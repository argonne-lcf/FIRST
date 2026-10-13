use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, Read},
    path::Path,
};

use backhand::{FilesystemReader, InnerNode};
use rayon::prelude::*;

use crate::parse::request_log_ids;

/// Map of every large request uuid inside `squashfs` to the checksum of its
/// payload.
///
/// `bundle_requests` stores requests as `<uuid[0..2]>/<uuid[2..4]>/<uuid>.json`,
/// so the file stem is the uuid.
/// The image's nodes are split across the rayon pool: the reader borrows the
/// filesystem, so it cannot be shared between threads and each shard opens its own.
/// A shard walks the whole node list and keeps every nth node, which keeps the
/// shards disjoint and still covers the image.
pub fn large_request_checksums(squashfs: &Path) -> anyhow::Result<HashMap<String, blake3::Hash>> {
    // each shard holds its own reader over the image, so memory scales with
    // the shard count rather than the cores
    let shards = rayon::current_num_threads().min(8);

    let checksums: Vec<HashMap<String, blake3::Hash>> = (0..shards)
        .into_par_iter()
        .map(|shard| {
            let image = FilesystemReader::from_reader(BufReader::new(File::open(squashfs)?))?;

            let mut checksums = HashMap::new();
            let mut buf = vec![0u8; 64 * 1024];
            for (index, node) in image.files().enumerate() {
                if index % shards != shard {
                    continue;
                }
                let InnerNode::File(file) = &node.inner else {
                    continue; // the root and the two levels of uuid prefix dirs
                };
                let Some(uuid) = node.fullpath.file_stem().and_then(|stem| stem.to_str()) else {
                    continue;
                };

                // payloads are hashed as they stream out of the image
                let mut reader = image.file(file).reader();
                let mut hasher = blake3::Hasher::new();
                let mut read = 0;
                loop {
                    let n = reader.read(&mut buf)?;
                    if n == 0 {
                        break;
                    }
                    hasher.update(&buf[..n]);
                    read += n;
                }

                // a truncated payload would otherwise silently checksum as empty
                anyhow::ensure!(
                    read == file.file_len(),
                    "short read of {}: {} of {} bytes",
                    node.fullpath.display(),
                    read,
                    file.file_len()
                );

                checksums.insert(uuid.to_string(), hasher.finalize());
            }

            Ok(checksums)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(checksums.into_iter().flatten().collect())
}

/// Verify that the large requests `bundle_requests` bundles from
/// `large_requests` for `request_log` made it into the squashfs unmodified,
/// returning how many requests were verified. `bundled` holds the checksums of
/// the image's contents.
///
/// Fails with one line per request that is missing from the squashfs or whose
/// source file no longer matches the bundled copy.
pub fn validate_bundled_requests(
    bundled: &HashMap<String, blake3::Hash>,
    sources: &[String],
    request_log: &Path,
    large_requests: &Path,
) -> anyhow::Result<usize> {
    let mut problems = Vec::new();
    let ids = request_log_ids(request_log)?;
    let ids = ids.column("id")?.str()?;

    // every id names an independent source file, so the hashing happens across the
    // pool and each one reports a line only when its copy is missing or differs
    let wanted: Vec<&str> = ids
        .no_null_iter()
        .filter(|id| sources.binary_search_by(|source| source.as_str().cmp(id)).is_ok())
        .collect();
    let checked: Vec<anyhow::Result<Option<String>>> = wanted
        .par_iter()
        .map(|id| {
            let json = large_requests.join(*id).with_extension("json");

            let Some(&checksum) = bundled.get(*id) else {
                return Ok(Some(format!("{id} is missing from the squashfs")));
            };

            // the source file is hashed as it streams in
            let mut file = File::open(&json)?;
            let mut hasher = blake3::Hasher::new();
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                let n = file.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }

            Ok((hasher.finalize() != checksum)
                .then(|| format!("{id} differs from the bundled copy")))
        })
        .collect();

    let mut verified = 0;
    for checked in checked {
        match checked? {
            Some(problem) => problems.push(problem),
            None => verified += 1,
        }
    }

    anyhow::ensure!(problems.is_empty(), "{}", problems.join("\n"));
    Ok(verified)
}
