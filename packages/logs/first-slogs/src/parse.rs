use std::{
    collections::{HashMap, hash_map::Entry},
    fs::{self, File},
    io::{self, BufWriter, Cursor, Read, Write},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use backhand::{FilesystemCompressor, FilesystemWriter, NodeHeader, compression::Compressor};
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

use crate::files;

const STREAMS: &[&str] = &[
    "access_log",
    "app",
    "request_log",
    "request_metrics",
    "user",
];

/// Split `buf` on b"\n", each line including its trailing newline. Trailing
/// data without a newline is yielded as a final line as well.
fn lines(buf: &[u8]) -> impl Iterator<Item = &[u8]> {
    let mut pos = 0;
    std::iter::from_fn(move || match memchr::memchr(b'\n', &buf[pos..]) {
        Some(end) => {
            let line = &buf[pos..=pos + end];
            pos += end + 1;
            Some(line)
        }
        None if pos < buf.len() => {
            let line = &buf[pos..];
            pos = buf.len();
            Some(line)
        }
        None => None,
    })
}

/// Reader that mmaps its file on first read and drops the mapping once EOF
/// is reached (subsequent reads return `Ok(0)`).
struct LazyMmap {
    path: PathBuf,
    state: MmapState,
}

enum MmapState {
    Unmapped,
    Mapped(Cursor<Mmap>),
    Eof,
}

impl LazyMmap {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            state: MmapState::Unmapped,
        }
    }
}

impl Read for LazyMmap {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if matches!(self.state, MmapState::Unmapped) {
            let file = File::open(&self.path)?;
            // SAFETY: log files are not modified while first-slogs runs
            let mmap = unsafe { Mmap::map(&file) }?;
            self.state = MmapState::Mapped(Cursor::new(mmap));
        }

        let MmapState::Mapped(cursor) = &mut self.state else {
            return Ok(0);
        };

        let bytes = cursor.read(buf)?;
        if bytes == 0 && !buf.is_empty() {
            self.state = MmapState::Eof;
        }

        Ok(bytes)
    }
}

/// Path of `log`'s `stream` ndjson partition inside `dataset_dir`.
fn partition(log: &Path, dataset_dir: &Path, stream: &str) -> PathBuf {
    let mut p = dataset_dir
        .join(log.file_name().unwrap())
        .with_added_extension(stream);
    p.add_extension("ndjson");
    p
}

/// Map `path` iff it has no `.ndjson` partitions in `dataset_dir` yet or any of
/// them is older than the log itself (strict comparison).
fn mmap_if_stale(path: &Path, dataset_dir: &Path) -> io::Result<Option<Mmap>> {
    let file = File::open(path)?;
    let log_mtime = file.metadata()?.mtime();

    let partition_mtimes: Vec<_> = STREAMS
        .iter()
        .filter_map(|stream| {
            fs::metadata(partition(path, dataset_dir, stream))
                .ok()
                .map(|m| m.mtime())
        })
        .collect();

    if !partition_mtimes.is_empty() && partition_mtimes.iter().all(|&m| log_mtime <= m) {
        return Ok(None);
    }

    // SAFETY: log files are not modified while first-slogs runs
    Ok(Some(unsafe { Mmap::map(&file)? }))
}

fn split_log(path: &Path, dataset_dir: &Path, buf: &[u8]) -> io::Result<HashMap<String, PathBuf>> {
    let mut streams: HashMap<String, BufWriter<File>> = HashMap::new();
    let mut partitions: HashMap<String, PathBuf> = HashMap::new();

    for line in lines(buf) {
        match sonic_rs::get(line, &["stream"]).as_str() {
            Some(stream) if STREAMS.contains(&stream) => {
                let mut entry = match streams.entry(stream.to_string()) {
                    Entry::Occupied(e) => e,
                    Entry::Vacant(e) => {
                        let p = partition(path, dataset_dir, stream);

                        let e = e.insert_entry(BufWriter::new(File::create(&p)?));

                        partitions.insert(stream.to_string(), p);
                        e
                    }
                };

                entry.get_mut().write_all(line)?;
            }
            // unknown streams are silently dropped
            Some(_) => {}
            None => {
                if let Ok(msg) = str::from_utf8(line) {
                    println!("Malformed msg: {}", msg);
                }
            }
        }
    }

    for writer in streams.values_mut() {
        writer.flush()?;
    }

    Ok(partitions)
}

/// Sink an `.ndjson` partition next to `path` into a `.parquet` file of the
/// same stem, returning the parquet path.
fn ndjson_to_parquet(path: &Path) -> anyhow::Result<PathBuf> {
    let parquet = path.with_extension("parquet");

    LazyJsonLineReader::new(PlRefPath::try_from_path(path)?)
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

    Ok(parquet)
}

fn partitions_to_parquet(
    mut partitions: HashMap<String, PathBuf>,
) -> anyhow::Result<HashMap<String, PathBuf>> {
    for (stream, path) in &mut partitions {
        if matches!(stream.as_str(), "app" | "request_metrics") {
            continue; // request_metrics is merged with app/request_log below
        }

        *path = ndjson_to_parquet(path)?;
    }

    if let Some(request_metrics) = partitions.get("request_metrics") {
        let request_metrics = if let Some(request_log) = partitions.get("request_log")
            && let Some(app) = partitions.get("app")
        {
            let streaming_metrics = pull_streaming_metrics(app)?;
            write_merged_request_metrics(streaming_metrics, request_metrics, request_log)?
        } else {
            ndjson_to_parquet(request_metrics)?
        };

        partitions.insert("request_metrics".to_string(), request_metrics);
    }

    Ok(partitions)
}

fn pull_streaming_metrics(app: &Path) -> anyhow::Result<DataFrame> {
    let mmap = unsafe {
        // SAFETY: log files are not modified while first-slogs runs
        Mmap::map(&File::open(app)?)?
    };

    let re = regex!(
        r"Token estimation for ([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}): ([0-9]+) total \(([0-9]+) completion, ([0-9]+) prompt\)"
    );
    let mut access_ids = Vec::new();
    let mut total_tokens: Vec<Option<u64>> = Vec::new();
    let mut completion_tokens: Vec<Option<u64>> = Vec::new();
    let mut prompt_tokens: Vec<Option<u64>> = Vec::new();

    for line in lines(&mmap) {
        if let Some(msg) = sonic_rs::get(line, &["msg"]).as_str()
            && let Some(caps) = re.captures(msg)
        {
            let (_, [access_id, total, completion, prompt]) = caps.extract();
            access_ids.push(access_id.to_string());
            total_tokens.push(total.parse().ok());
            completion_tokens.push(completion.parse().ok());
            prompt_tokens.push(prompt.parse().ok());
        }
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
    let lf = LazyJsonLineReader::new(PlRefPath::try_from_path(request_metrics)?)
        .with_infer_schema_length(None)
        .finish()?;

    // access_log->request_log mapping
    let mapping = LazyFrame::scan_parquet(
        PlRefPath::try_from_path(request_log)?,
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
    lf.join_builder()
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

/// Distinct request ids referenced by a `request_log` parquet.
pub fn request_log_ids(request_log: &Path) -> anyhow::Result<Vec<String>> {
    Ok(LazyFrame::scan_parquet(
        PlRefPath::try_from_path(request_log)?,
        ScanArgsParquet::default(),
    )?
    .select([col("id")])
    .unique(None, UniqueKeepStrategy::Any)
    .collect()?
    .column("id")?
    .str()?
    .no_null_iter()
    .map(|id| id.to_string())
    .collect())
}

fn bundle_requests(request_log: &Path, large_requests: &Path) -> anyhow::Result<Option<PathBuf>> {
    let uid = unsafe { libc::getuid() };
    let gid = unsafe { libc::getgid() };
    let mtime = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as u32;

    let ids = request_log_ids(request_log)?;

    // the writer is only created once a matching file is found
    let mut squashfs: Option<FilesystemWriter> = None;
    for request_id in &ids {
        let mut json = large_requests.join(request_id);
        json.set_extension("json");

        if !json.is_file() {
            continue;
        }

        let writer = squashfs.get_or_insert_with(|| {
            let mut fs = FilesystemWriter::default();
            fs.set_compressor(FilesystemCompressor::new(Compressor::Zstd, None).unwrap());
            fs.set_root_uid(uid);
            fs.set_root_gid(gid);
            fs.set_root_mode(0o755);
            fs.set_time(mtime);
            fs
        });

        let mut path = Path::new(&request_id[0..2]).join(&request_id[2..4]);

        writer.push_dir_all(
            &path,
            NodeHeader {
                permissions: 0o755,
                uid,
                gid,
                mtime,
            },
        )?;
        path.push(json.file_name().unwrap());

        writer.push_file(
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

    match squashfs {
        Some(mut fs) => {
            let path = request_log.with_extension("large_requests.squashfs");
            let mut file = File::create(&path)?;
            fs.write(&mut file)?;

            Ok(Some(path))
        }
        None => Ok(None),
    }
}

pub fn parse_logs(large_requests: &Path, dataset_dir: &Path, logs: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(dataset_dir)?;

    for log in files(logs)? {
        println!("Parsing {}...", log.display());

        let partitions = match mmap_if_stale(&log, dataset_dir)? {
            Some(mmap) => split_log(&log, dataset_dir, &mmap)?,
            None => {
                println!("Skipped already parsed log {}", log.display());
                continue;
            }
        };

        let partitions = partitions_to_parquet(partitions)?;
        for partition in partitions.values() {
            println!("Outputted frame {}", partition.display());
        }

        if let Some(request_log) = partitions.get("request_log") {
            match bundle_requests(request_log, large_requests)? {
                Some(tarball) => println!("Dumped large requests to {}", tarball.display()),
                None => println!("No large requests to dump"),
            }
        }
    }

    Ok(())
}
