use std::{
    collections::{HashMap, hash_map::Entry},
    fs::{self, File},
    io::{self, BufWriter, Cursor, Read, Write},
    num::NonZeroUsize,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use backhand::{FilesystemCompressor, FilesystemWriter, NodeHeader, compression::Compressor};
use memmap2::Mmap;
use rayon::prelude::*;

use polars::{
    df,
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

/// Rows of each ndjson partition polars infers the parquet schema from.
/// Inferring from every row parses the whole file twice, but fields that only
/// appear after these rows are dropped from the schema.
const INFER_SCHEMA_ROWS: usize = 100_000;

/// Hive dirs placing partitions at null `year`/`month`/`day` values, which
/// polars scans as nulls.
const NULL_DIRS: &str = "year=__HIVE_DEFAULT_PARTITION__/month=__HIVE_DEFAULT_PARTITION__/day=__HIVE_DEFAULT_PARTITION__";

/// The partition raw, non-conforming entries are captured in verbatim.
const MALFORMED: &str = "malformed";

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

/// The hive dirs of `log`'s partitions: the `YYYY-MM-DD` date in its name,
/// or the default partition for names without one, which polars scans as
/// nulls.
fn date_dirs(log: &Path) -> PathBuf {
    let name = log.file_name().and_then(|name| name.to_str());
    let date = name.and_then(|name| regex!(r"[0-9]{4}-[0-9]{2}-[0-9]{2}$").find(name));

    let Some(date) = date else {
        return PathBuf::from(NULL_DIRS);
    };

    let (year, rest) = date.as_str().split_once('-').unwrap();
    let (month, day) = rest.split_once('-').unwrap();

    PathBuf::from(format!("year={year}/month={month}/day={day}"))
}

/// Path of `log`'s `stream` ndjson partition inside `dataset_dir`, under
/// `ndjson/<stream>/<dated>/`; the malformed partition is placed at null hive
/// values instead.
fn partition(log: &Path, dataset_dir: &Path, stream: &str, dated: &Path) -> PathBuf {
    let dirs = if stream == MALFORMED {
        Path::new(NULL_DIRS)
    } else {
        dated
    };
    let name = log.file_name().unwrap();
    let mut p = PathBuf::with_capacity(
        dataset_dir.as_os_str().len() + dirs.as_os_str().len() + name.len() + 64,
    );
    p.push(dataset_dir);
    p.push("ndjson");
    p.push(stream);
    p.push(dirs);
    p.push(name);
    p.add_extension(stream);
    p.add_extension("ndjson");
    p
}

/// Path of the parquet partition of the ndjson `partition`: the same dated
/// subpath rooted at `<dataset_dir>/<stream>/` instead of
/// `<dataset_dir>/ndjson/<stream>/`, with its directory created.
fn parquet_of(dataset_dir: &Path, partition: &Path) -> anyhow::Result<PathBuf> {
    let relative = partition
        .strip_prefix(dataset_dir)?
        .strip_prefix("ndjson")?;

    let mut parquet = dataset_dir.join(relative);
    parquet.set_extension("parquet");
    fs::create_dir_all(parquet.parent().unwrap())?;

    Ok(parquet)
}

/// Map `path` iff it has no `.ndjson` partitions in `dataset_dir` yet or any of
/// them is older than the log itself (strict comparison).
fn mmap_if_stale(path: &Path, dataset_dir: &Path) -> io::Result<Option<Mmap>> {
    let file = File::open(path)?;
    let log_mtime = file.metadata()?.mtime();

    let dated = date_dirs(path);
    let mut written = false;
    let mut stale = false;
    for stream in STREAMS.iter().chain(std::iter::once(&MALFORMED)) {
        // partitions of streams the log has no lines for are absent
        if let Ok(metadata) = fs::metadata(partition(path, dataset_dir, stream, &dated)) {
            written = true;
            stale |= log_mtime > metadata.mtime();
        }
    }

    if written && !stale {
        return Ok(None);
    }

    // SAFETY: log files are not modified while first-slogs runs
    Ok(Some(unsafe { Mmap::map(&file)? }))
}

fn split_log(
    path: &Path,
    dataset_dir: &Path,
    buf: &[u8],
) -> io::Result<HashMap<&'static str, PathBuf>> {
    let mut streams: HashMap<&'static str, BufWriter<File>> = HashMap::new();
    let mut partitions: HashMap<&'static str, PathBuf> = HashMap::new();

    let dated = date_dirs(path);

    for line in lines(buf) {
        // non-conforming entries are captured in the malformed partition
        let stream = sonic_rs::get(line, &["stream"])
            .as_str()
            .and_then(|stream| STREAMS.iter().copied().find(|known| *known == stream))
            .unwrap_or(MALFORMED);

        let mut entry = match streams.entry(stream) {
            Entry::Occupied(e) => e,
            Entry::Vacant(e) => {
                let p = partition(path, dataset_dir, stream, &dated);
                fs::create_dir_all(p.parent().unwrap())?;

                let e = e.insert_entry(BufWriter::with_capacity(64 * 1024, File::create(&p)?));

                partitions.insert(stream, p);
                e
            }
        };

        entry.get_mut().write_all(line)?;
    }

    for writer in streams.values_mut() {
        writer.flush()?;
    }

    Ok(partitions)
}

/// Sink an `.ndjson` partition next to `path` into a `.parquet` file of the
/// same stem, returning the parquet path.
fn ndjson_to_parquet(dataset_dir: &Path, path: &Path) -> anyhow::Result<PathBuf> {
    let parquet = parquet_of(dataset_dir, path)?;

    LazyJsonLineReader::new(PlRefPath::try_from_path(path)?)
        .with_infer_schema_length(NonZeroUsize::new(INFER_SCHEMA_ROWS))
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
    dataset_dir: &Path,
    mut partitions: HashMap<&'static str, PathBuf>,
) -> anyhow::Result<HashMap<&'static str, PathBuf>> {
    for (stream, path) in &mut partitions {
        if matches!(*stream, "app" | "malformed" | "request_metrics") {
            continue; // app and malformed stay ndjson, request_metrics is merged below
        }

        *path = ndjson_to_parquet(dataset_dir, path)?;
    }

    if let Some(request_metrics) = partitions.get("request_metrics") {
        let request_metrics = if let Some(request_log) = partitions.get("request_log")
            && let Some(app) = partitions.get("app")
        {
            let streaming_metrics = pull_streaming_metrics(app)?;
            write_merged_request_metrics(
                dataset_dir,
                streaming_metrics,
                request_metrics,
                request_log,
            )?
        } else {
            ndjson_to_parquet(dataset_dir, request_metrics)?
        };

        partitions.insert("request_metrics", request_metrics);
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
    dataset_dir: &Path,
    streaming_metrics: DataFrame,
    request_metrics: &Path,
    request_log: &Path,
) -> anyhow::Result<PathBuf> {
    let lf = LazyJsonLineReader::new(PlRefPath::try_from_path(request_metrics)?)
        .with_infer_schema_length(NonZeroUsize::new(INFER_SCHEMA_ROWS))
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
    let request_metrics = parquet_of(dataset_dir, request_metrics)?;
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

/// The sorted ids of the requests that have a source json in `large_requests`.
pub fn source_request_ids(large_requests: &Path) -> anyhow::Result<Vec<String>> {
    let mut ids = Vec::new();
    for entry in fs::read_dir(large_requests)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_file()
            && path.extension().is_some_and(|ext| ext == "json")
            && let Some(id) = path.file_stem().and_then(|id| id.to_str())
        {
            ids.push(id.to_string());
        }
    }
    ids.sort();
    Ok(ids)
}

fn bundle_requests(
    dataset_dir: &Path,
    request_log: &Path,
    large_requests: &Path,
) -> anyhow::Result<Option<PathBuf>> {
    let uid = unsafe { libc::getuid() };
    let gid = unsafe { libc::getgid() };
    let mtime = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as u32;

    let ids = request_log_ids(request_log)?;
    let sources = source_request_ids(large_requests)?;

    // the writer is only created once a matching file is found
    let mut squashfs: Option<FilesystemWriter> = None;
    for request_id in &ids {
        if sources.binary_search(request_id).is_err() {
            continue; // the request has no source json
        }

        let mut json = large_requests.join(request_id);
        json.set_extension("json");

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
            let relative = request_log
                .strip_prefix(dataset_dir)?
                .strip_prefix("request_log")?;

            let mut path = dataset_dir.join("squashfs");
            path.push(relative);
            path.set_extension("large_requests.squashfs");
            fs::create_dir_all(path.parent().unwrap())?;
            let mut file = File::create(&path)?;
            fs.write(&mut file)?;

            Ok(Some(path))
        }
        None => Ok(None),
    }
}

pub fn parse_logs(large_requests: &Path, dataset_dir: &Path, logs: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(dataset_dir)?;

    let logs: Vec<PathBuf> = files(logs)?
        .into_iter()
        .filter(|log| {
            log.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("out.log")
        })
        .collect();

    // each log is parsed into its own dated partitions, so logs are
    // independent and parsed in parallel
    logs.par_iter()
        .try_for_each(|log| parse_log(large_requests, dataset_dir, log))
}

fn parse_log(large_requests: &Path, dataset_dir: &Path, log: &Path) -> anyhow::Result<()> {
    println!("Parsing {}...", log.display());

    let partitions = match mmap_if_stale(log, dataset_dir)? {
        Some(mmap) => split_log(log, dataset_dir, &mmap)?,
        None => {
            println!("Skipped already parsed log {}", log.display());
            return Ok(());
        }
    };

    let partitions = partitions_to_parquet(dataset_dir, partitions)?;
    for partition in partitions.values() {
        println!("Outputted frame {}", partition.display());
    }

    if let Some(request_log) = partitions.get("request_log") {
        match bundle_requests(dataset_dir, request_log, large_requests)? {
            Some(tarball) => println!("Dumped large requests to {}", tarball.display()),
            None => println!("No large requests to dump"),
        }
    }

    Ok(())
}
