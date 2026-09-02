use std::{
    collections::HashMap,
    fs::{self, File},
    io::{BufReader, Read},
    path::Path,
};

use backhand::{FilesystemReader, InnerNode};

use crate::parse::request_log_ids;

/// Map of every large request uuid inside `squashfs` to the checksum of its
/// payload.
///
/// `bundle_requests` stores requests as `<uuid[0..2]>/<uuid[2..4]>/<uuid>.json`,
/// so the file stem is the uuid.
pub fn large_request_checksums(squashfs: &Path) -> anyhow::Result<HashMap<String, blake3::Hash>> {
    let image = FilesystemReader::from_reader(BufReader::new(File::open(squashfs)?))?;

    let mut checksums = HashMap::new();
    let mut buf = vec![0u8; 64 * 1024];
    for node in image.files() {
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
            file.file_len(),
        );

        checksums.insert(uuid.to_string(), hasher.finalize());
    }

    Ok(checksums)
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
    let mut verified = 0;
    let mut buf = vec![0u8; 64 * 1024];
    for id in request_log_ids(request_log)? {
        if sources.binary_search(&id).is_err() {
            continue; // never bundled by `bundle_requests` either
        }

        let json = large_requests.join(&id).with_extension("json");

        let Some(&checksum) = bundled.get(&id) else {
            problems.push(format!("{id} is missing from the squashfs"));
            continue;
        };

        // the source file is hashed as it streams in
        let mut file = File::open(&json)?;
        let mut hasher = blake3::Hasher::new();
        loop {
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }

        if hasher.finalize() != checksum {
            problems.push(format!("{id} differs from the bundled copy"));
        } else {
            verified += 1;
        }
    }

    anyhow::ensure!(problems.is_empty(), "{}", problems.join("\n"));
    Ok(verified)
}
