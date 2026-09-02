use std::{
    collections::HashMap,
    fs, io,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

use clap::{Parser, Subcommand};
use rayon::prelude::*;
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

/// Sorted paths of the files under `<dataset_dir>/<sub>` and its
/// subdirectories. Only that tree of the dataset is walked; its artifacts are
/// all of one kind, so everything in it is returned.
fn artifacts(dataset_dir: &Path, sub: &str) -> anyhow::Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    walk(&dataset_dir.join(sub), &mut paths)?;
    paths.sort();
    Ok(paths)
}

/// Append the files under `dir` to `paths`. A missing `dir` is an empty tree.
fn walk(dir: &Path, paths: &mut Vec<PathBuf>) -> anyhow::Result<()> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };

    for entry in entries {
        let entry = entry?;
        let file_type = entry.file_type()?;

        if file_type.is_dir() {
            walk(&entry.path(), paths)?;
        } else if file_type.is_file() {
            paths.push(entry.path());
        }
    }
    Ok(())
}

/// The path of the index of `squashfs`: the same dated subpath rooted at
/// `<dataset_dir>/index/`.
fn index_of(dataset_dir: &Path, squashfs: &Path) -> anyhow::Result<PathBuf> {
    let relative = squashfs
        .strip_prefix(dataset_dir)?
        .strip_prefix("squashfs")?;
    let mut index = dataset_dir.join("index");
    index.push(relative);
    index.add_extension("index.json");
    Ok(index)
}

/// Whether the index at `index` is newer than `squashfs` and so already holds
/// its checksums.
fn fresh(index: &Path, squashfs: &Path) -> bool {
    matches!(
        (fs::metadata(index), fs::metadata(squashfs)),
        (Ok(index), Ok(image)) if image.mtime() <= index.mtime()
    )
}

/// The bundled checksums of `squashfs`, loaded from its index when the index
/// is newer than the image and computed from the image itself otherwise.
fn bundled_checksums(
    dataset_dir: &Path,
    squashfs: &Path,
) -> anyhow::Result<HashMap<String, blake3::Hash>> {
    let index = index_of(dataset_dir, squashfs)?;

    if fresh(&index, squashfs) {
        let table: HashMap<String, String> = sonic_rs::from_slice(&fs::read(&index)?)?;
        return table
            .into_iter()
            .map(|(uuid, checksum)| Ok((uuid, blake3::Hash::from_hex(&checksum)?)))
            .collect();
    }

    validation::large_request_checksums(squashfs)
}

/// Write the large request checksums of `squashfs` into a json map under
/// `<dataset_dir>/index/`, unless an index newer than the image already holds
/// them, returning the index path when written.
fn index_squashfs(dataset_dir: &Path, squashfs: &Path) -> anyhow::Result<Option<PathBuf>> {
    let index = index_of(dataset_dir, squashfs)?;

    // an index newer than the image already holds its checksums
    if fresh(&index, squashfs) {
        return Ok(None);
    }

    let mut table = Object::new();
    for (uuid, checksum) in validation::large_request_checksums(squashfs)? {
        table.insert(&uuid, checksum.to_string().as_str());
    }

    fs::create_dir_all(index.parent().unwrap())?;
    fs::write(&index, sonic_rs::to_vec_pretty(&table)?)?;

    Ok(Some(index))
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    match args.command {
        Command::Parse { logs } => {
            parse::parse_logs(&args.large_requests, &args.dataset_dir, &logs)
        }
        Command::Index => {
            let images = artifacts(&args.dataset_dir, "squashfs")?;
            images.par_iter().try_for_each(|squashfs| {
                match index_squashfs(&args.dataset_dir, squashfs)? {
                    Some(index) => println!("Dumped index to {}", index.display()),
                    None => println!("Skipped already indexed image {}", squashfs.display()),
                }
                Ok(())
            })
        }
        Command::Vet => {
            let parquets = artifacts(&args.dataset_dir, "request_log")?;
            parquets.par_iter().try_for_each(|request_log| {
                vet_log(&args.dataset_dir, &args.large_requests, request_log)
            })
        }
    }
}

/// Vet the large requests bundled for one request_log parquet against their
/// source files.
fn vet_log(dataset_dir: &Path, large_requests: &Path, request_log: &Path) -> anyhow::Result<()> {
    // the squashfs mirrors the request_log tree under <dataset_dir>/squashfs/
    let relative = request_log
        .strip_prefix(dataset_dir)?
        .strip_prefix("request_log")?;
    let mut squashfs = dataset_dir.join("squashfs");
    squashfs.push(relative);
    squashfs.set_extension("large_requests.squashfs");
    if !squashfs.is_file() {
        return Ok(()); // nothing was bundled for this log
    }

    let bundled = bundled_checksums(dataset_dir, &squashfs)?;
    let verified = validation::validate_bundled_requests(&bundled, request_log, large_requests)?;
    println!(
        "Verified {verified} large requests in {}",
        squashfs.display()
    );

    Ok(())
}
