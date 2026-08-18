use std::{
    collections::{HashMap, hash_map::Entry},
    fs::{File, OpenOptions},
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
};

use clap::Parser;
use memmap2::Mmap;
use polars::{
    df,
    error::PolarsResult,
    frame::DataFrame,
    prelude::{
        FileWriteFormat, IntoLazy, JoinCoalesce, JoinType, LazyFileListReader, LazyFrame,
        LazyJsonLineReader, ParquetWriteOptions, PlRefPath, ScanArgsParquet, SinkDestination,
        SinkTarget, UnifiedSinkArgs, all, col, cols,
    },
};
use regex::regex;
use sonic_rs::JsonValueTrait;

#[derive(Parser)]
struct Args {
    logs: Vec<PathBuf>,
}

fn split_log(path: &Path) -> io::Result<HashMap<String, PathBuf>> {
    let file = File::open(path)?;
    let mmap = unsafe {
        // SAFETY we assume the file is never modified at runtime
        Mmap::map(&file)?
    };

    let mut streams: HashMap<String, BufWriter<File>> = HashMap::new();
    let mut partitions: HashMap<String, PathBuf> = HashMap::new();

    let mut write_to_partition = |line| -> io::Result<()> {
        // get stream and append to partition if wanted
        match sonic_rs::get(line, &["stream"]).as_str() {
            Some(stream) => {
                if matches!(
                    stream,
                    "access_log" | "app" | "request_log" | "request_metrics" | "user"
                ) {
                    let mut e = match streams.entry(stream.to_string()) {
                        Entry::Occupied(e) => e,
                        Entry::Vacant(e) => {
                            let mut p = path.with_added_extension(stream);
                            p.add_extension("ndjson");

                            let e = e.insert_entry(BufWriter::new(
                                OpenOptions::new().append(true).create(true).open(&p)?,
                            ));

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
    for end in memchr::memchr_iter(b'\n', &mmap) {
        let line = &mmap[start..=end];
        write_to_partition(line)?;
        start = end + 1;
    }
    if start < mmap.len() {
        let line = &mmap[start..]; // trailing non-\n data
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

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    for l in args.logs {
        println!("Parsing {}...", l.display());
        let partitions = split_log(&l)?;
        let partitions = partitions_to_parquet(partitions)?;
        partitions
            .values()
            .for_each(|p| println!("Outputted frame {}", p.display()));
    }

    Ok(())
}
