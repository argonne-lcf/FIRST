use std::{
    cell::LazyCell,
    collections::{HashMap, hash_map::Entry},
    fs::{self, File},
    io::{self, BufWriter, Cursor, Read, Write},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use backhand::{FilesystemCompressor, FilesystemWriter, NodeHeader, compression::Compressor};
use clap::Parser;
use memmap2::Mmap;
use polars::{
    df,
    error::PolarsResult,
    frame::{DataFrame, UniqueKeepStrategy},
    prelude::{
        FileWriteFormat, IntoLazy, JoinCoalesce, JoinType, LazyFileListReader, LazyFrame,
        LazyJsonLineReader, ParquetWriteOptions, PlRefPath, ScanArgsParquet, SinkDestination,
        SinkTarget, UnifiedSinkArgs, all, col, cols,
    },
};
use regex::regex;
use sonic_rs::JsonValueTrait;

const STREAMS: &[&str] = &[
    "access_log",
    "app",
    "request_log",
    "request_metrics",
    "user",
];

#[derive(Parser)]
struct Args {
    logs: Vec<PathBuf>,
    large_requests: Option<PathBuf>,
}

struct LazyMmap<P>
where
    P: AsRef<Path>,
{
    path: P,
    cur: CursorStatus,
}

enum CursorStatus {
    Uninit,
    Mapped(Cursor<Mmap>),
    Dropped,
}

impl<P> LazyMmap<P>
where
    P: AsRef<Path>,
{
    fn new(path: P) -> Self {
        Self {
            path,
            cur: CursorStatus::Uninit,
        }
    }
}

impl<P> Read for LazyMmap<P>
where
    P: AsRef<Path>,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.cur {
            CursorStatus::Uninit => {
                let file = File::open(&self.path)?;
                let mmap = unsafe { Mmap::map(&file) }?;

                self.cur = CursorStatus::Mapped(Cursor::new(mmap));
                self.read(buf)
            }
            CursorStatus::Mapped(ref mut c) => {
                let bytes = c.read(buf)?;
                if bytes == 0 && !buf.is_empty() {
                    self.cur = CursorStatus::Dropped;
                }

                Ok(bytes)
            }
            CursorStatus::Dropped => Ok(0),
        }
    }
}

fn mmap_outdated(path: &Path) -> io::Result<Option<Mmap>> {
    let file = File::open(path)?;

    let mtime = file.metadata()?.mtime();
    let mut stream_mtimes = STREAMS
        .iter()
        .filter_map(|s| {
            let p = path.with_added_extension(s);
            p.with_extension("ndjson");
            fs::metadata(p).ok().map(|m| m.mtime())
        })
        .peekable();

    Ok(
        if stream_mtimes.peek().is_none() || stream_mtimes.any(|m| mtime > m) {
            unsafe {
                // SAFETY we assume the file is never modified at runtime
                Some(Mmap::map(&file)?)
            }
        } else {
            None
        },
    )
}

fn split_log(path: &Path, buf: &[u8]) -> io::Result<HashMap<String, PathBuf>> {
    let mut streams: HashMap<String, BufWriter<File>> = HashMap::new();
    let mut partitions: HashMap<String, PathBuf> = HashMap::new();

    let mut write_to_partition = |line| -> io::Result<()> {
        // get stream and append to partition if wanted
        match sonic_rs::get(line, &["stream"]).as_str() {
            Some(stream) => {
                if STREAMS.contains(&stream) {
                    let mut e = match streams.entry(stream.to_string()) {
                        Entry::Occupied(e) => e,
                        Entry::Vacant(e) => {
                            let mut p = path.with_added_extension(stream);
                            p.add_extension("ndjson");

                            let e = e.insert_entry(BufWriter::new(File::create(&p)?));

                            partitions.insert(stream.to_string(), p);
                            e
                        }
                    };

                    e.get_mut().write_all(line)?;
                }
            }
            None => {
                if let Some(l) = str::from_utf8(&line).ok() {
                    println!("Malformed msg: {}", l);
                }
            }
        };

        Ok(())
    };

    let mut start = 0;
    for end in memchr::memchr_iter(b'\n', buf) {
        let line = &buf[start..=end];
        write_to_partition(line)?;
        start = end + 1;
    }
    if start < buf.len() {
        let line = &buf[start..]; // trailing non-\n data
        write_to_partition(line)?;
    }

    // flush streams
    streams.values_mut().try_for_each(BufWriter::flush)?;

    Ok(partitions)
}

fn partitions_to_parquet(
    mut partitions: HashMap<String, PathBuf>,
) -> anyhow::Result<HashMap<String, PathBuf>> {
    for (stream, path) in &mut partitions {
        if matches!(stream.as_str(), "app" | "request_metrics") {
            continue; // skip request_metrics and app are written after
        }

        let lf = LazyJsonLineReader::new(PlRefPath::try_from_path(&path)?)
            .with_infer_schema_length(None)
            .finish()?;
        path.set_extension("parquet");

        let _ = lf
            .sink(
                SinkDestination::File {
                    target: SinkTarget::Path(PlRefPath::try_from_path(&path)?),
                },
                FileWriteFormat::Parquet(ParquetWriteOptions::default().into()),
                UnifiedSinkArgs::default(),
            )?
            .with_streaming(true)
            .collect()?;
    }

    if let Some(request_metrics) = partitions.get("request_metrics") {
        let request_metrics = if let Some(request_log) = partitions.get("request_log")
            && let Some(app) = partitions.get("app")
        {
            let streaming_metrics = pull_streaming_metrics(app)?;
            write_merged_request_metrics(streaming_metrics, request_metrics, request_log)?
        } else {
            let parquet = request_metrics.with_extension("parquet");
            let _ = LazyJsonLineReader::new(PlRefPath::try_from_path(&request_metrics)?)
                .with_infer_schema_length(None)
                .finish()?
                .sink(
                    SinkDestination::File {
                        target: SinkTarget::Path(PlRefPath::try_from_path(&parquet)?),
                    },
                    FileWriteFormat::Parquet(ParquetWriteOptions::default().into()),
                    UnifiedSinkArgs::default(),
                )?
                .with_streaming(true)
                .collect()?;
            parquet
        };

        partitions.insert("request_metrics".to_string(), request_metrics);
    }

    Ok(partitions)
}

fn pull_streaming_metrics(app: &Path) -> anyhow::Result<DataFrame> {
    let file = File::open(app)?;
    let mmap = unsafe {
        // SAFETY we assume the file is never modified at runtime
        Mmap::map(&file)?
    };

    let re = regex!(
        r"Token estimation for [0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}: ([0-9]+) total \(([0-9]+) completion, ([0-9]+) prompt\)"
    );
    let mut access_ids = Vec::new();
    let mut total_tokens: Vec<Option<u64>> = Vec::new();
    let mut completion_tokens: Vec<Option<u64>> = Vec::new();
    let mut prompt_tokens: Vec<Option<u64>> = Vec::new();
    let mut read_estimation = |line| {
        if let Some(msg) = sonic_rs::get(line, &["msg"]).as_str()
            && let Some(caps) = re.captures(msg)
        {
            let (_, [access_id, total, completion, prompt]) = caps.extract();
            access_ids.push(access_id.to_string());
            total_tokens.push(total.parse().ok());
            completion_tokens.push(completion.parse().ok());
            prompt_tokens.push(prompt.parse().ok());
        }
    };

    let mut start = 0;
    for end in memchr::memchr_iter(b'\n', &mmap) {
        let line = &mmap[start..=end];
        read_estimation(line);
        start = end + 1;
    }
    if start < mmap.len() {
        let line = &mmap[start..]; // trailing non-\n data
        read_estimation(line);
    }

    Ok(df!(
        "access_log_id" => access_ids,
        "total_tokens" => total_tokens,
        "completion_tokens" => completion_tokens,
        "prompt_tokens" => prompt_tokens,
    )?)
}

fn write_merged_request_metrics(
    streaming_metrics: DataFrame,
    request_metrics: &Path,
    request_log: &Path,
) -> PolarsResult<PathBuf> {
    let lf = LazyJsonLineReader::new(PlRefPath::try_from_path(&request_metrics)?)
        .with_infer_schema_length(None)
        .finish()?;

    // access_log->request_log mapping
    let mapping = LazyFrame::scan_parquet(
        PlRefPath::try_from_path(&request_log)?,
        ScanArgsParquet::default(),
    )?
    .select([col("access_log_id"), col("id").alias("request_id")]);

    // apply mapping on streaming_metrics
    let streaming_metrics = streaming_metrics
        .lazy()
        .join_builder()
        .with(mapping)
        .on([col("access_log_id")])
        .how(JoinType::Left)
        .finish()
        .select([all().exclude_cols(["access_log_id"]).as_expr()])
        .drop_nulls(Some(cols(["request_id"])));

    // merge with request_metrics
    let request_metrics = request_metrics.with_extension("parquet");
    let _ = lf
        .join_builder()
        .with(streaming_metrics)
        .how(JoinType::Left)
        .on([col("request_id")])
        .coalesce(JoinCoalesce::CoalesceColumns)
        .finish()
        .select([all().exclude_cols(["^*_right$"]).as_expr()])
        .sink(
            SinkDestination::File {
                target: SinkTarget::Path(PlRefPath::try_from_path(&request_metrics)?),
            },
            FileWriteFormat::Parquet(ParquetWriteOptions::default().into()),
            UnifiedSinkArgs::default(),
        )?
        .with_streaming(true)
        .collect()?;

    Ok(request_metrics)
}

fn bundle_requests(request_log: &Path, large_requests: &Path) -> anyhow::Result<Option<PathBuf>> {
    let uid = unsafe { libc::getuid() };
    let gid = unsafe { libc::getgid() };
    let mtime = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as u32;

    let mut squashfs = LazyFrame::scan_parquet(
        PlRefPath::try_from_path(&request_log)?,
        ScanArgsParquet::default(),
    )?
    .select([col("id")])
    .unique(None, UniqueKeepStrategy::Any)
    .collect()?
    .column("id")?
    .str()?
    .no_null_iter()
    .try_fold(
        LazyCell::new(|| {
            let mut fs = FilesystemWriter::default();
            fs.set_compressor(FilesystemCompressor::new(Compressor::Zstd, None).unwrap());
            fs.set_root_uid(uid);
            fs.set_root_gid(gid);
            fs.set_root_mode(0o755);
            fs.set_time(mtime);
            fs
        }),
        |mut fs, request_id| -> anyhow::Result<_> {
            let mut json = large_requests.join(request_id);
            json.set_extension("json");

            if json.is_file() {
                let mut path = Path::new(&request_id[0..2]).join(&request_id[2..4]);

                fs.push_dir_all(
                    &path,
                    NodeHeader {
                        permissions: 0o755,
                        uid,
                        gid,
                        mtime,
                    },
                )?;
                path.push(json.file_name().unwrap());

                fs.push_file(
                    LazyMmap::new(json),
                    path,
                    NodeHeader {
                        permissions: 0o644,
                        uid,
                        gid,
                        mtime,
                    },
                )?;
            }

            Ok(fs)
        },
    )?;

    match LazyCell::get_mut(&mut squashfs) {
        Some(fs) => {
            let path = request_log.with_extension("large_requests.squashfs");
            let mut file = File::create(&path)?;
            fs.write(&mut file)?;

            Ok(Some(path))
        }
        None => Ok(None),
    }
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    for p in args.logs {
        println!("Parsing {}...", p.display());
        let partitions = match mmap_outdated(&p)? {
            Some(b) => split_log(&p, &b)?,
            None => {
                println!("Skipped already parsed log {}", p.display());
                continue;
            }
        };

        let partitions = partitions_to_parquet(partitions)?;
        partitions
            .values()
            .for_each(|p| println!("Outputted frame {}", p.display()));

        if let Some(request_log) = partitions.get("request_log")
            && let Some(large_requests) = &args.large_requests
        {
            match bundle_requests(request_log, large_requests)? {
                Some(tarball) => println!("Dumped large requests to {}", tarball.display()),
                None => println!("No large requests to dump"),
            }
        }
    }

    Ok(())
}
