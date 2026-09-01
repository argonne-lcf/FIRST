use std::{
    fs,
    path::{Path, PathBuf},
};

use clap::{Parser, Subcommand};
use sonic_rs::Object;

mod parse;
mod validation;

#[derive(Parser)]
struct Args {
    /// Dataset dir the parsed ndjson, parquet, and squashfs files live in
    #[arg(long)]
    dataset_dir: PathBuf,

    /// Directory of large request payloads
    #[arg(long)]
    large_requests: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Split logs into parquet partitions, bundling large requests into a squashfs
    Parse {
        /// Logs directory to parse
        logs: PathBuf,
    },
    /// Write an index of the large request checksums of each squashfs image in the dataset dir
    Index,
    /// Verify the large requests bundled into each squashfs image in the dataset dir against their source files
    Vet,
}

/// Sorted paths of the regular files in `dir`.
fn files(dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut paths: Vec<PathBuf> = fs::read_dir(dir)?
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(|entry| entry.path())
        .filter(|path| path.is_file())
        .collect();
    paths.sort();
    Ok(paths)
}

/// Sorted paths of the files in `dataset_dir` whose name ends with `suffix`.
fn artifacts(dataset_dir: &Path, suffix: &str) -> anyhow::Result<Vec<PathBuf>> {
    Ok(files(dataset_dir)?
        .into_iter()
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(suffix))
        })
        .collect())
}

/// Write the large request checksums of `squashfs` into a json map next to it,
/// mapping each request uuid to its checksum, returning the index path.
fn index_squashfs(squashfs: &Path) -> anyhow::Result<PathBuf> {
    let mut table = Object::new();
    for (uuid, checksum) in validation::large_request_checksums(squashfs)? {
        table.insert(&uuid, checksum.to_string().as_str());
    }

    let index = squashfs.with_added_extension("index.json");
    fs::write(&index, sonic_rs::to_vec_pretty(&table)?)?;

    Ok(index)
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    match args.command {
        Command::Parse { logs } => {
            parse::parse_logs(&args.large_requests, &args.dataset_dir, &logs)
        }
        Command::Index => {
            for squashfs in artifacts(&args.dataset_dir, ".large_requests.squashfs")? {
                println!("Dumped index to {}", index_squashfs(&squashfs)?.display());
            }
            Ok(())
        }
        Command::Vet => {
            for request_log in artifacts(&args.dataset_dir, ".request_log.parquet")? {
                let squashfs = request_log.with_extension("large_requests.squashfs");
                if !squashfs.is_file() {
                    continue; // nothing was bundled for this log
                }

                let verified = validation::validate_bundled_requests(
                    &request_log,
                    &args.large_requests,
                    &squashfs,
                )?;
                println!(
                    "Verified {verified} large requests in {}",
                    squashfs.display()
                );
            }
            Ok(())
        }
    }
}
