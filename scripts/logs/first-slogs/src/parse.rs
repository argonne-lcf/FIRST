use std::{
    collections::{HashMap, hash_map::Entry},
    fs::{self, File},
    io::{self, BufWriter, Cursor, Read, Write},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use backhand::{FilesystemCompressor, FilesystemWriter, NodeHeader, compression::Compressor};
use memmap2::Mmap;
use rayon::prelude::*;

use polars::{
    df,
    frame::{DataFrame, UniqueKeepStrategy},
    io::HiveOptions,
    prelude::{
        AsOfOptions, AsofStrategy, DataType, ExtraColumnsPolicy, FileWriteFormat, IntoLazy,
        JoinType, LazyFileListReader, LazyFrame, LazyJsonLineReader, MatchToSchemaPerColumn,
        MissingColumnsPolicy, MissingColumnsPolicyOrExpr, Null, ParquetWriteOptions, PlRefPath,
        ScanArgsParquet, Schema, Selector, SinkDestination, SinkTarget, SortMultipleOptions,
        UnifiedSinkArgs,
        UpcastOrForbid, by_name, col, lit,
    },
};
use regex::regex;
use sonic_rs::{JsonContainerTrait, JsonValueTrait, to_object_iter};

use crate::{
    artifacts, files,
    schema::{primary_key_of, schema_of},
};

const STREAMS: &[&str] = &[
    "access_log",
    "app",
    "batch_log",
    "batch_metrics",
    "request_log",
    "request_metrics",
    "user",
    // uvicorn's access line, the only record of a streamed response's final
    // status: the app's access row is written before the stream ends
    "uvicorn.access",
];

/// Hive dirs placing partitions at null `year`/`month`/`day` values, which
/// polars scans as nulls.
const NULL_DIRS: &str = "year=__HIVE_DEFAULT_PARTITION__/month=__HIVE_DEFAULT_PARTITION__/day=__HIVE_DEFAULT_PARTITION__";

/// The partition raw, non-conforming entries are captured in verbatim.
const MALFORMED: &str = "malformed";

/// The partition lines of streams outside `STREAMS` are captured in verbatim.
const UNKNOWN: &str = "unknown_stream";

/// The partitions lines are split into besides `STREAMS` themselves: lines of
/// unknown streams and non-conforming lines.
const NON_STREAMS: &[&str] = &[MALFORMED, UNKNOWN];

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

/// The metrics partition's ndjson path that belongs beside a request_log
/// parquet: the same input file's day, the metrics stream's name. The heal
/// names its output after it, and `parquet_of` turns it into the parquet
/// path the day's metrics rows belong at.
fn metrics_naming_path_of(request_log: &Path) -> PathBuf {
    let mut p = request_log.to_path_buf();
    p.set_extension("ndjson");
    let s = p
        .to_string_lossy()
        .replace("/request_log/", "/ndjson/request_metrics/");
    PathBuf::from(s.replace(".request_log.ndjson", ".request_metrics.ndjson"))
}

/// Map `path` iff it has no `.ndjson` partitions in `dataset_dir` yet or any of
/// them is older than the log itself (strict comparison).
fn mmap_if_stale(path: &Path, dataset_dir: &Path) -> io::Result<Option<Mmap>> {
    let file = File::open(path)?;
    let log_mtime = file.metadata()?.mtime();


    let dated = date_dirs(path);
    let mut written = false;
    let mut stale = false;
    for stream in STREAMS.iter().chain(NON_STREAMS) {
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

    // the streams' schemas name the fields a line of the stream may
    // carry; the free-form app holds none
    let schemas: HashMap<&str, Schema> = STREAMS
        .iter()
        .filter_map(|stream| schema_of(stream).map(|schema| (*stream, schema)))
        .collect();

    let dated = date_dirs(path);

    for line in lines(buf) {
        // lines of streams outside `STREAMS` land in the unknown_stream
        // partition; non-conforming lines are captured in the malformed
        // partition verbatim
        let stream = match sonic_rs::get(line, &["stream"]).as_str() {
            Some(stream) => match STREAMS.iter().copied().find(|known| *known == stream) {
                // a line of a known stream names only the fields its
                // schema holds
                Some(stream)
                    if schemas.get(stream).is_none_or(|schema| {
                        to_object_iter(line)
                            .all(|field| field.is_ok_and(|(name, _)| schema.contains(&name)))
                    }) =>
                {
                    stream
                }
                Some(_) => MALFORMED,
                None => UNKNOWN,
            },
            None => MALFORMED,
        };

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

/// Sink `lines`, conformed to their stream's schema, into the `.parquet`
/// twin of the ndjson partition at `path`, returning the parquet path:
/// the columns a partition's lines never carried insert as nulls, the
/// all-null ones take the dtype the valued partitions infer, and the ones
/// outside the schema drop.
fn to_parquet(dataset_dir: &Path, path: &Path, lines: LazyFrame) -> anyhow::Result<PathBuf> {
    let parquet = parquet_of(dataset_dir, path)?;

    // the partition's stream, which `partition` names as the extension
    // before the `ndjson` one
    let lines = match path
        .file_stem()
        .map(Path::new)
        .and_then(Path::extension)
        .and_then(|stream| stream.to_str())
        .and_then(schema_of)
    {
        Some(schema) => {
            // one policy per schema column: insert the missing, upcast
            // the numeric
            let per_column = std::iter::repeat_with(|| MatchToSchemaPerColumn {
                missing_columns: MissingColumnsPolicyOrExpr::Insert,
                missing_struct_fields: MissingColumnsPolicy::Insert,
                extra_struct_fields: ExtraColumnsPolicy::Ignore,
                integer_cast: UpcastOrForbid::Upcast,
                float_cast: UpcastOrForbid::Upcast,
            })
            .take(schema.len())
            .collect();

            lines.match_to_schema(Arc::new(schema), per_column, ExtraColumnsPolicy::Ignore)
        }
        None => lines,
    };

    lines
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

/// A stream log's latest line per key: the log is append-ordered, so a key's last
/// line is its latest upsert.
fn latest_lines(path: &Path, stream: &str) -> anyhow::Result<LazyFrame> {
    let lines = LazyJsonLineReader::new(PlRefPath::try_from_path(path)?)
        .with_infer_schema_length(None)
        .finish()?;
    Ok(match primary_key_of(stream) {
        Some(key) => lines.unique(Some(by_name([key], false, false)), UniqueKeepStrategy::Last),
        None => lines,
    })
}

/// Convert an `.ndjson` partition next to `path` into a `.parquet` file of
/// the same stem, returning the parquet path.
fn ndjson_to_parquet(dataset_dir: &Path, path: &Path) -> anyhow::Result<PathBuf> {
    // the partition's stream, which `partition` names as the extension
    // before the `ndjson` one
    let stream = path
        .file_stem()
        .map(Path::new)
        .and_then(Path::extension)
        .and_then(|stream| stream.to_str())
        .unwrap_or_default();

    to_parquet(dataset_dir, path, latest_lines(path, stream)?)
}

/// Match each transport line onto the access row it followed.
///
/// The two lines share no id, so the match is the route, the client, and
/// the order of the log. A streamed response's final status is only here:
/// uvicorn writes this line as the last byte goes out, while the app's
/// access row is written when the stream begins.
fn match_transport(
    dataset_dir: &Path,
    log: &Path,
    uvicorn_access: &Path,
    access_log: &Path,
) -> anyhow::Result<PathBuf> {
    // uvicorn's client address carries a port the app's does not
    let transport = LazyFrame::scan_parquet(
        PlRefPath::try_from_path(uvicorn_access)?,
        ScanArgsParquet::default(),
    )?
    .select([
        col("timestamp"),
        col("path").alias("api_route"),
        col("client_addr")
            .str()
            .split(lit(":"))
            .list()
            .first()
            .alias("origin_ip"),
        col("status_code").alias("transport_status"),
    ])
    .sort(["timestamp"], SortMultipleOptions::default());

    let access = LazyFrame::scan_parquet(
        PlRefPath::try_from_path(access_log)?,
        ScanArgsParquet::default(),
    )?
    .select([
        col("id"),
        col("api_route"),
        col("origin_ip"),
        col("timestamp_request"),
    ])
    .sort(["timestamp_request"], SortMultipleOptions::default());

    let matched = transport
        .join_builder()
        .with(access)
        .left_on([col("timestamp")])
        .right_on([col("timestamp_request")])
        .how(JoinType::AsOf(Box::new(AsOfOptions {
            strategy: AsofStrategy::Backward,
            tolerance: None,
            tolerance_str: None,
            left_by: Some(vec!["api_route".into(), "origin_ip".into()]),
            right_by: Some(vec!["api_route".into(), "origin_ip".into()]),
            allow_eq: true,
            check_sortedness: true,
        })))
        .finish()
        .select([
            col("id"),
            col("transport_status").alias("status_code"),
            col("timestamp").alias("timestamp_response"),
        ])
        // every uvicorn line matched its nearest access row, so an access
        // row the lines after its own response also matched carries several:
        // keep the earliest, the line closest to the row's own request
        .sort(["timestamp_response"], SortMultipleOptions::default())
        .unique_stable(
            Some(Selector::ByName {
                names: vec!["id".into()].into(),
                strict: true,
            }),
            UniqueKeepStrategy::First,
        );

    to_parquet(
        dataset_dir,
        &partition(log, dataset_dir, "access_log_transport", &date_dirs(log)),
        matched,
    )
}

/// Fill the status a metrics row is missing from the uvicorn line its
/// request's access row matched. The emitter writes the status only on the
/// request's final request_log line, and the rows that carry none -- the
/// emitter bug's class, responses the gateway never finalized -- have none
/// anywhere of their own: the transport match's line is the one record of
/// what the server actually sent.
fn heal_metrics_status(
    request_log: &Path,
    transport: &Path,
    request_metrics: &Path,
) -> anyhow::Result<()> {
    let scan = |path: &Path| -> anyhow::Result<LazyFrame> {
        Ok(LazyFrame::scan_parquet(
            PlRefPath::try_from_path(path)?,
            ScanArgsParquet {
                // the hive dirs around a partition must not add their
                // year/month/day columns to the frame
                hive_options: HiveOptions::new_disabled(),
                ..Default::default()
            },
        )?)
    };
    let mut metrics = scan(request_metrics)?;
    let Some(status) = metrics.collect_schema()?.get("status_code").cloned() else {
        return Ok(()); // nothing to fill
    };

    // the request's access row to the uvicorn line matched onto it
    let statuses = scan(request_log)?
        .select([col("id").alias("request_id"), col("access_log_id")])
        .join_builder()
        .with(
            scan(transport)?.select([
                col("id").alias("access_log_id"),
                col("status_code").alias("transport_status"),
            ]),
        )
        .on([col("access_log_id")])
        .how(JoinType::Inner)
        .finish()
        // a request cannot reach here twice; the guard keeps a fan-out from
        // duplicating metrics rows whatever the partitions hold
        .unique(
            Some(Selector::ByName {
                names: vec!["request_id".into()].into(),
                strict: true,
            }),
            UniqueKeepStrategy::Any,
        )
        .select([col("request_id"), col("transport_status")]);

    // fill only what the row itself does not carry, and drop the columns
    // the join brought in so the parquet keeps its schema
    let columns: Vec<_> = metrics
        .clone()
        .collect_schema()?
        .iter_names()
        .map(|name| col(name.to_string()))
        .collect();
    let healed = metrics
        .join_builder()
        .with(statuses)
        .how(JoinType::Left)
        .on([col("request_id")])
        .finish()
        .with_columns([col("status_code")
            .fill_null(col("transport_status").cast(status))])
        .select(columns);

    // the rename keeps the path valid for a reader that is not watching
    let tmp = request_metrics.with_extension("tmp");
    healed
        .sink(
            SinkDestination::File {
                target: SinkTarget::Path(PlRefPath::try_from_path(&tmp)?),
            },
            FileWriteFormat::Parquet(ParquetWriteOptions::default().into()),
            UnifiedSinkArgs::default(),
        )?
        .with_streaming(true)
        .collect()?;
    fs::rename(&tmp, request_metrics)?;
    Ok(())
}

fn partitions_to_parquet(
    dataset_dir: &Path,
    mut partitions: HashMap<&'static str, PathBuf>,
    large_requests: &Path,
) -> anyhow::Result<HashMap<&'static str, PathBuf>> {
    for (stream, path) in &mut partitions {
        if matches!(
            *stream,
            "app" | "malformed" | "unknown_stream" | "request_metrics"
        ) {
            continue; // app, unknown_stream, and malformed stay ndjson, request_metrics is merged below
        }

        *path = ndjson_to_parquet(dataset_dir, path)?;
    }

    if let Some(request_metrics) = partitions.get("request_metrics") {
        let request_metrics = if let Some(request_log) = partitions.get("request_log")
            && let Some(app) = partitions.get("app")
        {
            let streaming_metrics = pull_streaming_metrics(app)?;
            let prompt_estimates = pull_prompt_estimates(app)?;
            let merged = write_merged_request_metrics(
                dataset_dir,
                large_requests,
                streaming_metrics,
                prompt_estimates,
                request_metrics,
                request_log,
            )?;
            // a metrics row is missing for some requests and carries no token count
            // for others: the request's payload answers both
            heal_request_metrics(
                dataset_dir,
                large_requests,
                &merged,
                request_log,
                request_metrics,
            )?
        } else if let Some(request_log) = partitions.get("request_log") {
            // no app lines, so no estimation lines to stream a metrics row from:
            // the pool and the row's own text are still enough to fill the blanks
            // and to synthesize what the stream never wrote at all
            let plain = ndjson_to_parquet(dataset_dir, request_metrics)?;
            heal_request_metrics(
                dataset_dir,
                large_requests,
                &plain,
                request_log,
                request_metrics,
            )?
        } else {
            ndjson_to_parquet(dataset_dir, request_metrics)?
        };

        partitions.insert("request_metrics", request_metrics);
    } else if let Some(request_log) = partitions.get("request_log") {
        // the day's logs hold requests but no metrics lines at all -- the
        // metrics writer was not running yet, the era's first days were like
        // this. Every request is never-finalized, so synthesize a metrics row
        // for each from its own text, exactly as the heal does for the days
        // that wrote some: an empty metrics frame carries the schema, and the
        // heal fills it with the synthesized rows
        let naming = metrics_naming_path_of(request_log);
        let empty = LazyFrame::scan_parquet(PlRefPath::try_from_path(request_log)?, ScanArgsParquet::default())?
            .select([
                col("id").alias("request_id"),
                col("cluster"),
                col("framework"),
                col("model"),
                col("timestamp_compute_request"),
                col("timestamp_compute_response"),
                col("status_code"),
                lit(Null {}).cast(DataType::Int64).alias("prompt_tokens"),
                lit(Null {}).cast(DataType::Int64).alias("completion_tokens"),
                lit(Null {}).cast(DataType::Int64).alias("total_tokens"),
                lit(Null {}).cast(DataType::Float64).alias("response_time_sec"),
                lit(Null {}).cast(DataType::Float64).alias("throughput_tokens_per_sec"),
            ])
            .limit(0);
        let empty_path = to_parquet(dataset_dir, &naming, empty)?;
        partitions.insert(
            "request_metrics",
            heal_request_metrics(dataset_dir, large_requests, &empty_path, request_log, &naming)?,
        );
    }

    Ok(partitions)
}

/// Deduplicate the upsert streams' requests that landed in two adjacent day
/// partitions, keeping the later day's row. A request that starts before
/// midnight and finishes after is written to both days' logs, and the
/// per-partition upsert only collapses the rows within one partition. The
/// `user` stream is left alone: its rows are one snapshot per user per day.
pub fn dedup_adjacent(dataset_dir: &Path) -> anyhow::Result<()> {
    for stream in [
        "request_log",
        "request_metrics",
        "batch_log",
        "batch_metrics",
    ] {
        let Some(key) = primary_key_of(stream) else {
            continue;
        };
        let mut partitions: Vec<(String, PathBuf)> = artifacts(dataset_dir, stream)?
            .into_iter()
            .filter_map(|path| {
                let name = path.file_name()?.to_str()?;
                let date = regex!(r"[0-9]{4}-[0-9]{2}-[0-9]{2}").find(name)?;
                Some((date.as_str().to_string(), path))
            })
            .collect();
        partitions.sort();

        for pair in partitions.windows(2) {
            let [(_, prev), (_, next)] = pair else {
                continue;
            };

            // the hive dirs around a partition must not add their
            // year/month/day columns to the frame
            let next_ids = LazyFrame::scan_parquet(
                PlRefPath::try_from_path(next)?,
                ScanArgsParquet {
                    hive_options: HiveOptions::new_disabled(),
                    ..Default::default()
                },
            )?
            .select([col(key)])
            .unique(None, UniqueKeepStrategy::Any)
            .collect()?;
            if next_ids.height() == 0 {
                continue;
            }

            let prev_lines = LazyFrame::scan_parquet(
                PlRefPath::try_from_path(prev)?,
                ScanArgsParquet {
                    hive_options: HiveOptions::new_disabled(),
                    ..Default::default()
                },
            )?;

            // the requests the later partition already holds
            let superseded = prev_lines
                .clone()
                .select([col(key)])
                .join_builder()
                .with(next_ids.lazy().with_columns([lit(true).alias("_dup")]))
                .how(JoinType::Left)
                .on([col(key)])
                .finish()
                .filter(col("_dup").is_not_null())
                .unique(None, UniqueKeepStrategy::Any)
                .collect()?;
            let count = superseded.height();
            if count == 0 {
                continue;
            }

            // rewrite the earlier partition without them; the rename keeps
            // the path valid for a reader that is not watching
            let lines = prev_lines
                .join_builder()
                .with(superseded.lazy().with_columns([lit(true).alias("_dup")]))
                .how(JoinType::Left)
                .on([col(key)])
                .finish()
                .filter(col("_dup").is_null())
                .drop(by_name(["_dup"], false, false));

            let tmp = prev.with_extension("tmp");
            lines
                .sink(
                    SinkDestination::File {
                        target: SinkTarget::Path(PlRefPath::try_from_path(&tmp)?),
                    },
                    FileWriteFormat::Parquet(ParquetWriteOptions::default().into()),
                    UnifiedSinkArgs::default(),
                )?
                .with_streaming(true)
                .collect()?;
            fs::rename(&tmp, prev)?;
            println!("Deduplicated {count} {stream} rows from {}", prev.display());
        }
    }

    Ok(())
}

/// The token counts a response body reports.
#[derive(Clone, Copy, Default)]
struct Usage {
    prompt_tokens: Option<i64>,
    completion_tokens: Option<i64>,
    total_tokens: Option<i64>,
}

impl Usage {
    /// Whether the body reported any count at all.
    fn is_known(&self) -> bool {
        self.prompt_tokens.is_some()
            || self.completion_tokens.is_some()
            || self.total_tokens.is_some()
    }
}

/// The JSON bodies a response body holds.
///
/// A buffered response is one object; a streamed one is a run of SSE `data:`
/// lines, with the usage in its own chunk. Both shapes are read the same way.
fn bodies_of(result: &str) -> impl Iterator<Item = &str> {
    let buffered = result.trim_start().starts_with('{').then_some(result);
    buffered.into_iter().chain(
        result
            .lines()
            .filter_map(|line| line.strip_prefix("data:"))
            .map(str::trim)
            .filter(|body| body.starts_with('{')),
    )
}

/// Parse the token counts a response body reports.
///
/// A body the backend never measured reports none, so an absent count is
/// a real answer, not a parse failure.
fn usage_of(result: &str) -> Usage {
    let mut usage = Usage::default();
    for body in bodies_of(result) {
        let count = |key: &str| sonic_rs::get(body, &["usage", key]).as_i64();
        // the Responses API names them input/output; the others prompt/completion
        usage.prompt_tokens = count("prompt_tokens")
            .or_else(|| count("input_tokens"))
            .or(usage.prompt_tokens);
        usage.completion_tokens = count("completion_tokens")
            .or_else(|| count("output_tokens"))
            .or(usage.completion_tokens);
        usage.total_tokens = count("total_tokens").or(usage.total_tokens);
    }
    usage
}

/// The token counts the large request payloads report, keyed by request id.
///
/// Payloads are written only for requests too large to keep in the row; the
/// rest have their counts inline or nowhere. The key is the request id, so
/// the lookup is exact.
fn pull_payload_usage(large_requests: &Path, ids: &[String]) -> anyhow::Result<DataFrame> {
    // ids are independent files, read across the pool; the columns are
    // zipped from the results afterwards
    let counted: Vec<(&String, Usage)> = ids
        .par_iter()
        .filter_map(|id| {
            let payload = large_requests.join(id).with_extension("json");
            // no payload was written for this request
            let payload = fs::read_to_string(&payload).ok()?;
            let value = sonic_rs::get(&payload, &["result"]);
            let usage = usage_of(value.as_str().unwrap_or(""));
            // a payload that reports no counts heals nothing
            usage.is_known().then_some((id, usage))
        })
        .collect();

    let request_ids: Vec<&str> = counted.iter().map(|(id, _)| id.as_str()).collect();
    let prompt_tokens: Vec<_> = counted.iter().map(|(_, usage)| usage.prompt_tokens).collect();
    let completion_tokens: Vec<_> = counted.iter().map(|(_, usage)| usage.completion_tokens).collect();
    let total_tokens: Vec<_> = counted.iter().map(|(_, usage)| usage.total_tokens).collect();

    Ok(df!(
        "request_id" => request_ids,
        "prompt_tokens" => prompt_tokens,
        "completion_tokens" => completion_tokens,
        "total_tokens" => total_tokens,
    )?)
}

/// The chars the row's own truncation marker says the emitter cut.
fn dropped_chars(result: &str) -> i64 {
    let Some(rest) = result.split("<truncated ").nth(1) else {
        return 0;
    };
    let Some(chars) = rest.split(" chars>").next() else {
        return 0;
    };
    chars.parse().unwrap_or(0)
}

/// The text a streamed chunk carries, if it carries any.
fn delta_content(body: &str) -> String {
    let Ok(data) = sonic_rs::from_str::<sonic_rs::Value>(body) else {
        return String::new();
    };
    if let Some(delta) = data.get("delta").and_then(|delta| delta.as_str()) {
        return delta.to_string();
    }
    if let Some(text) = data.get("delta").and_then(|delta| delta.get("text")).and_then(|t| t.as_str()) {
        return text.to_string();
    }
    data.get("choices")
        .and_then(|choices| choices.as_array())
        .and_then(|choices| choices.iter().next())
        .and_then(|choice| choice.get("delta"))
        .and_then(|delta| delta.get("content"))
        .and_then(|content| content.as_str())
        .unwrap_or_default()
        .to_string()
}

/// The response's text: the deltas of a stream's chunks, or the message of
/// a single response.
fn completion_text(result: &str) -> String {
    if !result.trim_start().starts_with("data:") {
        let content = sonic_rs::from_str::<sonic_rs::Value>(result)
            .ok()
            .and_then(|data| data.get("choices").and_then(|c| c.as_array()).and_then(|c| c.iter().next().cloned()))
            .and_then(|choice| {
                choice
                    .get("message")
                    .and_then(|message| message.get("content"))
                    .and_then(|content| content.as_str())
                    .map(str::to_string)
            });
        return content.unwrap_or_else(|| result.to_string());
    }

    result
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(delta_content)
        .collect()
}

/// The request's text, as the gateway's estimator reads it: its messages' content.
fn prompt_text(prompt: &str) -> String {
    let Ok(parsed) = sonic_rs::from_str::<sonic_rs::Value>(prompt) else {
        return prompt.to_string();
    };
    let Some(messages) = parsed.as_array() else {
        return String::new();
    };
    messages
        .iter()
        .filter_map(|message| message.get("content").and_then(|content| content.as_str()))
        .collect::<Vec<_>>()
        .join(" ")
}

/// The gateway's estimate: the same arithmetic on the same text. The
/// truncation marker says how many chars were cut, so the char count reads
/// the original length.
fn estimated_counts(prompt: &str, result: &str) -> (i64, i64) {
    let content = completion_text(result);
    let chars = content.chars().count() as f64 + dropped_chars(result) as f64;
    let completion = 1.max((((chars / 4.0) + (content.split_whitespace().count() as f64 * 1.3)) / 2.0) as i64);

    let text = prompt_text(prompt);
    let prompt_tokens = if text.is_empty() {
        50
    } else {
        let chars = text.chars().count() as f64;
        10.max((((chars / 4.0) + (text.split_whitespace().count() as f64 * 1.3)) / 2.0) as i64)
    };

    (prompt_tokens, completion)
}

/// The counts for the requests whose payload reports none: the row's own text, read the way
/// the gateway's estimator reads it.
fn pull_text_estimates(requests: &DataFrame) -> anyhow::Result<DataFrame> {
    let ids = requests.column("id")?.str()?;
    let prompts = requests.column("prompt")?.str()?;
    let results = requests.column("result")?.str()?;

    let estimated: Vec<(String, i64, i64, i64)> = ids
        .iter()
        .zip(prompts.iter())
        .zip(results.iter())
        .filter_map(|((id, prompt), result)| {
            let id = id?;
            let (prompt_tokens, completion_tokens) =
                estimated_counts(prompt.unwrap_or_default(), result.unwrap_or_default());
            Some((
                id.to_string(),
                prompt_tokens,
                completion_tokens,
                prompt_tokens + completion_tokens,
            ))
        })
        .collect();

    Ok(df!(
        "request_id" => estimated.iter().map(|(id, ..)| id.as_str()).collect::<Vec<_>>(),
        "est_prompt_tokens" => estimated.iter().map(|(_, prompt, ..)| *prompt).collect::<Vec<_>>(),
        "est_completion_tokens" => estimated.iter().map(|(_, _, completion, _)| *completion).collect::<Vec<_>>(),
        "est_total_tokens" => estimated.iter().map(|(.., total)| *total).collect::<Vec<_>>(),
    )?)
}

/// Fill the token counts a request's metrics row has none, and add the row a
/// request never got.
///
/// The request log has the row's other fields and the payload the counts,
/// both keyed by request id.
fn heal_request_metrics(
    dataset_dir: &Path,
    large_requests: &Path,
    merged: &Path,
    request_log: &Path,
    request_metrics: &Path,
) -> anyhow::Result<PathBuf> {
    // both frames are the parquet the pass above wrote: the metrics stream's own
    // lines, merged, and the request log's rows, already normalized
    let metrics = LazyFrame::scan_parquet(
        PlRefPath::try_from_path(merged)?,
        ScanArgsParquet::default(),
    )?;
    let requests = LazyFrame::scan_parquet(
        PlRefPath::try_from_path(request_log)?,
        ScanArgsParquet::default(),
    )?;

    // the requests that need a payload: those whose row is blank and those it
    // never wrote. A row counts as carrying its counts only when all three are
    // present -- rows with a total and no split need the split too
    let counted = metrics
        .clone()
        .filter(
            col("prompt_tokens")
                .is_not_null()
                .and(col("completion_tokens").is_not_null())
                .and(col("total_tokens").is_not_null()),
        )
        .select([col("request_id")])
        .collect()?;
    let counted: Vec<&str> = counted
        .column("request_id")?
        .str()?
        .no_null_iter()
        .collect();

    let request_ids = requests.clone().select([col("id")]).collect()?;
    let ids: Vec<String> = request_ids
        .column("id")?
        .str()?
        .no_null_iter()
        .filter(|id| !counted.contains(id))
        .map(str::to_string)
        .collect();

    // the join's colliding columns take a `_right` suffix, so the payloads' counts
    // arrive beside the row's and only fill the blanks
    let usage = pull_payload_usage(large_requests, &ids)?.lazy();

    // the counts the rows themselves carry, for the requests no payload reports: the same
    // arithmetic the gateway estimates with, on the same text
    let estimates = pull_text_estimates(&requests.clone().collect()?)?.lazy();

    // the rows the payloads fill: the counts the metrics stream left blank
    let healed = metrics
        .clone()
        .join_builder()
        .with(usage.clone())
        .how(JoinType::Left)
        .on([col("request_id")])
        .finish()
        .with_columns([
            col("prompt_tokens").fill_null(col("prompt_tokens_right")),
            col("completion_tokens").fill_null(col("completion_tokens_right")),
            col("total_tokens").fill_null(col("total_tokens_right")),
        ])
        // whatever the payloads left blank, the row's own text estimates
        .join_builder()
        .with(estimates.clone())
        .how(JoinType::Left)
        .on([col("request_id")])
        .finish()
        .with_columns([
            col("prompt_tokens").fill_null(col("est_prompt_tokens")),
            col("completion_tokens").fill_null(col("est_completion_tokens")),
            col("total_tokens").fill_null(col("est_total_tokens")),
        ])
        .select([
            col("request_id"),
            col("cluster"),
            col("framework"),
            col("model"),
            col("timestamp_compute_request"),
            col("timestamp_compute_response"),
            col("status_code"),
            col("prompt_tokens"),
            col("completion_tokens"),
            col("total_tokens"),
            col("response_time_sec"),
            col("throughput_tokens_per_sec"),
        ]);

    // the rows the stream never wrote at all: the request's own fields, with the
    // counts the payload reports where it reports any
    // the join drops the right frame's key column, so the marker carries it;
    // the rows to synthesize are the ones whose marker is absent
    let synthesized = requests
        .join_builder()
        .with(metrics.select([
            col("request_id").alias("counted"),
            lit(true).alias("has_metrics"),
        ]))
        .left_on([col("id")])
        .right_on([col("counted")])
        .how(JoinType::Left)
        .finish()
        .filter(col("has_metrics").is_null())
        .join_builder()
        .with(usage)
        .left_on([col("id")])
        .right_on([col("request_id")])
        .how(JoinType::Left)
        .finish()
        // whatever the payloads left blank, the row's own text estimates
        .join_builder()
        .with(estimates)
        .left_on([col("id")])
        .right_on([col("request_id")])
        .how(JoinType::Left)
        .finish()
        .with_columns([
            col("prompt_tokens").fill_null(col("est_prompt_tokens")),
            col("completion_tokens").fill_null(col("est_completion_tokens")),
            col("total_tokens").fill_null(col("est_total_tokens")),
        ])
        .select([
            col("id").alias("request_id"),
            col("cluster"),
            col("framework"),
            col("model"),
            col("timestamp_compute_request"),
            col("timestamp_compute_response"),
            col("status_code"),
            col("prompt_tokens"),
            col("completion_tokens"),
            col("total_tokens"),
            lit(Null {})
                .cast(DataType::Float64)
                .alias("response_time_sec"),
            lit(Null {})
                .cast(DataType::Float64)
                .alias("throughput_tokens_per_sec"),
        ]);

    let healed = healed.collect()?.vstack(&synthesized.collect()?)?;

    to_parquet(dataset_dir, request_metrics, healed.lazy())
}

fn pull_streaming_metrics(app: &Path) -> anyhow::Result<DataFrame> {
    let mmap = unsafe {
        // SAFETY: log files are not modified while first-slogs runs
        Mmap::map(&File::open(app)?)?
    };

    let re = regex!(
        r"Token estimation for [0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}: ([0-9]+) total \(([0-9]+) completion, ([0-9]+) prompt\)"
    );
    let mut access_ids = Vec::new();
    // the token counts parse as i64, the dtype their schema column holds
    let mut total_tokens: Vec<Option<i64>> = Vec::new();
    let mut completion_tokens: Vec<Option<i64>> = Vec::new();
    let mut prompt_tokens: Vec<Option<i64>> = Vec::new();
    // the estimation runs after the response, so the line's own timestamp is the
    // completion time a streamed request's row otherwise lacks
    let mut timestamps: Vec<Option<String>> = Vec::new();

    for line in lines(&mmap) {
        // the message's uuid is the ephemeral streaming task id; the
        // line's access_id is the access_log_id the merge joins on
        if let Some(access_id) = sonic_rs::get(line, &["access_id"]).as_str()
            && let Some(msg) = sonic_rs::get(line, &["message"]).as_str()
            && let Some(caps) = re.captures(msg)
        {
            let (_, [total, completion, prompt]) = caps.extract();
            access_ids.push(access_id.to_string());
            total_tokens.push(total.parse().ok());
            completion_tokens.push(completion.parse().ok());
            prompt_tokens.push(prompt.parse().ok());
            timestamps.push(
                sonic_rs::get(line, &["timestamp"])
                    .as_str()
                    .map(str::to_string),
            );
        }
    }

    Ok(df!(
        "access_log_id" => access_ids,
        "total_tokens" => total_tokens,
        "completion_tokens" => completion_tokens,
        "prompt_tokens" => prompt_tokens,
        "timestamp_compute_response" => timestamps,
    )?
    // re-shipped estimation lines fan the merge out; the log is
    // append-ordered, so each access id keeps its last line
    .unique_stable(
        Some(&["access_log_id".to_string()]),
        UniqueKeepStrategy::Last,
        None,
    )?)
}

/// The prompt's own estimation line, for the requests whose stream never logged a total.
/// The prompt is estimated before the response, so a request that never
/// finished has the first line and not the second.
fn pull_prompt_estimates(app: &Path) -> anyhow::Result<DataFrame> {
    let mmap = unsafe {
        // SAFETY: log files are not modified while first-slogs runs
        Mmap::map(&File::open(app)?)?
    };

    let re = regex!(
        r"Prompt token estimation for [0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}: ([0-9]+) tokens from [0-9]+ chars"
    );
    let mut access_ids = Vec::new();
    let mut prompt_tokens: Vec<Option<i64>> = Vec::new();

    for line in lines(&mmap) {
        if let Some(access_id) = sonic_rs::get(line, &["access_id"]).as_str()
            && let Some(msg) = sonic_rs::get(line, &["message"]).as_str()
            && let Some(caps) = re.captures(msg)
        {
            let (_, [prompt]) = caps.extract();
            access_ids.push(access_id.to_string());
            prompt_tokens.push(prompt.parse().ok());
        }
    }

    Ok(df!(
        "access_log_id" => access_ids,
        "prompt_tokens" => prompt_tokens,
    )?
    .unique_stable(
        Some(&["access_log_id".to_string()]),
        UniqueKeepStrategy::Last,
        None,
    )?)
}

/// Merge the streaming estimations into the metrics rows, with the payloads'
/// own usage taking priority: a measured count fills before an estimated one.
fn write_merged_request_metrics(
    dataset_dir: &Path,
    large_requests: &Path,
    streaming_metrics: DataFrame,
    prompt_estimates: DataFrame,
    request_metrics: &Path,
    request_log: &Path,
) -> anyhow::Result<PathBuf> {
    let lines = latest_lines(request_metrics, "request_metrics")?;

    // only the requests missing a count get their payloads read
    let blank = lines
        .clone()
        .filter(
            col("prompt_tokens")
                .is_null()
                .or(col("completion_tokens").is_null())
                .or(col("total_tokens").is_null()),
        )
        .select([col("request_id")])
        .collect()?;
    let blank: Vec<String> = blank
        .column("request_id")?
        .str()?
        .no_null_iter()
        .map(str::to_string)
        .collect();
    let usage = pull_payload_usage(large_requests, &blank)?.lazy();

    // access_log->request_log mapping
    let mapping = LazyFrame::scan_parquet(
        PlRefPath::try_from_path(request_log)?,
        ScanArgsParquet::default(),
    )?
    .select([col("access_log_id"), col("id").alias("request_id")]);

    // the estimations map onto the requests of this log; their token
    // columns are renamed so the merge can fill the nulls streaming rows
    // leave behind
    let streaming_metrics = streaming_metrics
        .lazy()
        .join_builder()
        .with(mapping.clone())
        .on([col("access_log_id")])
        .how(JoinType::Inner)
        .finish()
        .select([
            col("request_id"),
            col("total_tokens").alias("est_total_tokens"),
            col("completion_tokens").alias("est_completion_tokens"),
            col("prompt_tokens").alias("est_prompt_tokens"),
            col("timestamp_compute_response").alias("est_timestamp_compute_response"),
        ]);

    // the payload's usage is measured, so it fills before the estimates
    let merged = lines
        .join_builder()
        .with(usage)
        .how(JoinType::Left)
        .on([col("request_id")])
        .finish()
        .with_columns([
            col("prompt_tokens").fill_null(col("prompt_tokens_right")),
            col("completion_tokens").fill_null(col("completion_tokens_right")),
            col("total_tokens").fill_null(col("total_tokens_right")),
        ])
        // the estimations fill the rows the payloads left blank
        .join_builder()
        .with(streaming_metrics)
        .how(JoinType::Left)
        .on([col("request_id")])
        .finish()
        .with_columns([
            col("total_tokens").fill_null(col("est_total_tokens")),
            col("completion_tokens").fill_null(col("est_completion_tokens")),
            col("prompt_tokens").fill_null(col("est_prompt_tokens")),
            col("timestamp_compute_response").fill_null(col("est_timestamp_compute_response")),
        ])
        // the prompt-only lines cover the requests the total's line never reached
        .join_builder()
        .with(
            prompt_estimates
                .lazy()
                .join_builder()
                .with(mapping)
                .on([col("access_log_id")])
                .how(JoinType::Inner)
                .finish()
                .select([
                    col("request_id"),
                    col("prompt_tokens").alias("est_prompt_tokens"),
                ]),
        )
        .how(JoinType::Left)
        .on([col("request_id")])
        .finish()
        .with_columns([col("prompt_tokens").fill_null(col("est_prompt_tokens"))]);

    to_parquet(dataset_dir, request_metrics, merged)
}

/// Distinct request ids referenced by a `request_log` parquet, as its `id`
/// column.
pub fn request_log_ids(request_log: &Path) -> anyhow::Result<DataFrame> {
    Ok(LazyFrame::scan_parquet(
        PlRefPath::try_from_path(request_log)?,
        ScanArgsParquet::default(),
    )?
    .select([col("id")])
    .unique(None, UniqueKeepStrategy::Any)
    .collect()?)
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
    ids.par_sort();
    Ok(ids)
}

fn bundle_requests(
    large_requests: &Path,
    sources: &[String],
    dataset_dir: &Path,
    request_log: &Path,
) -> anyhow::Result<Option<PathBuf>> {
    let uid = unsafe { libc::getuid() };
    let gid = unsafe { libc::getgid() };
    let mtime = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as u32;

    let ids = request_log_ids(request_log)?;
    // sorted fullpaths insert at the tail of backhand's node vec
    let ids = ids.sort(["id"], SortMultipleOptions::default())?;
    let ids = ids.column("id")?.str()?;

    // the writer is only created once a matching file is found
    let mut squashfs: Option<FilesystemWriter> = None;
    for request_id in ids.no_null_iter() {
        if sources
            .binary_search_by(|source| source.as_str().cmp(request_id))
            .is_err()
        {
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

pub fn parse_logs(
    dataset_dir: &Path,
    logs: &Path,
    skip: &[String],
    large_requests: &Path,
) -> anyhow::Result<()> {
    fs::create_dir_all(dataset_dir)?;

    let logs: Vec<PathBuf> = files(logs)?
        .into_iter()
        .filter(|log| {
            let name = log.file_name().unwrap().to_string_lossy();
            name.starts_with("out.log") && !skip.iter().any(|skip| *skip == name)
        })
        .collect();

    // each log is parsed into its own dated partitions, so logs are
    // independent and parsed in parallel
    logs.par_iter()
        .try_for_each(|log| parse_log(dataset_dir, log, large_requests))?;

    dedup_adjacent(dataset_dir)
}

fn parse_log(dataset_dir: &Path, log: &Path, large_requests: &Path) -> anyhow::Result<()> {
    println!("Parsing {}...", log.display());

    let partitions = match mmap_if_stale(log, dataset_dir)? {
        Some(mmap) => split_log(log, dataset_dir, &mmap)?,
        None => {
            println!("Skipped already parsed log {}", log.display());
            return Ok(());
        }
    };

    let partitions = partitions_to_parquet(dataset_dir, partitions, large_requests)?;
    if let (Some(uvicorn_access), Some(access_log)) = (
        partitions.get("uvicorn.access"),
        partitions.get("access_log"),
    ) {
        let transport = match_transport(dataset_dir, log, uvicorn_access, access_log)?;
        // a metrics row the emitter never finalized carries no status of its
        // own; the uvicorn line just matched onto the request's access row is
        // the one record of what the server sent
        if let (Some(request_log), Some(request_metrics)) =
            (partitions.get("request_log"), partitions.get("request_metrics"))
        {
            heal_metrics_status(request_log, &transport, request_metrics)?;
        }
        println!("Outputted frame {}", transport.display());
    }
    for partition in partitions.values() {
        println!("Outputted frame {}", partition.display());
    }

    Ok(())
}

/// Bundle the large requests of every request_log partition in
/// `dataset_dir` into a squashfs image mirroring its dated subpath under
/// `<dataset_dir>/squashfs/`.
pub fn dump_large(large_requests: &Path, dataset_dir: &Path) -> anyhow::Result<()> {
    // the source listing is shared by every partition
    let sources = source_request_ids(large_requests)?;

    let parquets = artifacts(dataset_dir, "request_log")?;
    parquets.par_iter().try_for_each(|request_log| {
        match bundle_requests(large_requests, &sources, dataset_dir, request_log)? {
            Some(image) => println!("Dumped large requests to {}", image.display()),
            None => println!("No large requests to dump"),
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sink(path: &Path, frame: DataFrame) -> anyhow::Result<()> {
        frame
            .lazy()
            .sink(
                SinkDestination::File {
                    target: SinkTarget::Path(PlRefPath::try_from_path(path)?),
                },
                FileWriteFormat::Parquet(ParquetWriteOptions::default().into()),
                UnifiedSinkArgs::default(),
            )?
            .with_streaming(true)
            .collect()?;
        Ok(())
    }

    #[test]
    fn heal_metrics_status_fills_only_the_missing_statuses() -> anyhow::Result<()> {
        let dir = std::env::temp_dir().join("first-slogs-heal-metrics-status");
        fs::create_dir_all(&dir)?;

        let request_log = dir.join("request_log.parquet");
        sink(
            &request_log,
            df!(
                "id" => ["a", "b", "c"],
                "access_log_id" => ["A", "B", "C"],
            )?,
        )?;

        let transport = dir.join("access_log_transport.parquet");
        sink(
            &transport,
            df!(
                "id" => ["A", "B", "C"],
                "status_code" => [Some(200i64), Some(429i64), Some(502i64)],
            )?,
        )?;

        let request_metrics = dir.join("request_metrics.parquet");
        sink(
            &request_metrics,
            df!(
                "request_id" => ["a", "b", "c"],
                "status_code" => [None, Some(200i64), None],
            )?,
        )?;

        heal_metrics_status(&request_log, &transport, &request_metrics)?;

        let healed = LazyFrame::scan_parquet(
            PlRefPath::try_from_path(&request_metrics)?,
            ScanArgsParquet::default(),
        )?
        .sort(["request_id"], SortMultipleOptions::default())
        .collect()?;
        let statuses = healed.column("status_code")?.i64()?;

        // a filled from its transport line, b keeps the status it carries
        // rather than taking the transport's 429, c filled from its own
        assert_eq!(statuses.get(0), Some(200));
        assert_eq!(statuses.get(1), Some(200));
        assert_eq!(statuses.get(2), Some(502));
        Ok(())
    }
}
