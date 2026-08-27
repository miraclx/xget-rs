//! The download engine: probe, plan, fetch chunks in parallel with range validation, verify.

use std::path::Path;
use std::sync::Arc;

use futures::StreamExt as _;
use futures::stream::FuturesUnordered;
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncReadExt as _;

use crate::{ByteRange, Error, Source, plan};

/// The outcome of a completed download.
#[derive(Clone, Debug)]
pub struct Report {
    /// The verified total length in bytes.
    pub length: u64,
    /// The lowercase hex SHA-256 of the downloaded bytes.
    pub sha256: String,
}

/// Download the resource behind `source` into `output`, fetching up to `parts` chunks in parallel, and
/// return its verified length and SHA-256.
///
/// Every chunk's range is validated by the source, each chunk asserts it delivered exactly its bytes,
/// and the hash is computed over the written file, so the returned digest certifies the resource rather
/// than whatever bytes happened to arrive.
pub async fn download<S: Source>(source: &S, output: &Path, parts: u32) -> Result<Report, Error> {
    let probe = source.probe().await?;
    if !probe.supports_ranges && probe.length > 0 {
        return Err(detail(
            "resource does not support range requests (single-stream fetch is not yet implemented)",
        ));
    }
    let ranges = plan(probe.length, parts);

    let file = tokio::fs::File::create(output).await.map_err(io)?;
    file.set_len(probe.length).await.map_err(io)?;
    let file = Arc::new(file.into_std().await);

    // Structured concurrency: the source's futures are not `Send`, so chunks are driven together on
    // this task rather than spawned. Each writes its own file region, so there is no shared cursor.
    let mut chunks: FuturesUnordered<_> = ranges
        .into_iter()
        .map(|range| fetch_chunk(source, range, Arc::clone(&file)))
        .collect();
    while let Some(result) = chunks.next().await {
        result?;
    }

    // Every positioned write was awaited above, so the bytes are in the OS by now and reading the file
    // back hashes exactly what we wrote.
    let sha256 = hash_file(output).await?;
    Ok(Report {
        length: probe.length,
        sha256,
    })
}

/// Fetch one chunk and write it to its file region as it streams, asserting it delivered exactly its
/// range so a short or long chunk is a [`Error::LengthMismatch`], never silent corruption.
async fn fetch_chunk<S: Source>(
    source: &S,
    range: ByteRange,
    file: Arc<std::fs::File>,
) -> Result<(), Error> {
    use std::os::unix::fs::FileExt as _;

    let mut stream = source.fetch(range).await?;
    let mut offset = range.start;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        let at = offset;
        let file = Arc::clone(&file);
        let written = tokio::task::spawn_blocking(move || {
            file.write_all_at(&chunk, at).map(|()| chunk.len() as u64)
        })
        .await
        .map_err(join)?
        .map_err(io)?;
        offset += written;
    }
    let received = offset - range.start;
    if received != range.len() {
        return Err(Error::LengthMismatch {
            expected: range.len(),
            received,
        });
    }
    Ok(())
}

/// Read `path` back and compute its SHA-256, hex-encoded.
async fn hash_file(path: &Path) -> Result<String, Error> {
    let mut file = tokio::fs::File::open(path).await.map_err(io)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).await.map_err(io)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn io(error: std::io::Error) -> Error {
    Error::Transport(Box::new(error))
}

fn join(error: tokio::task::JoinError) -> Error {
    Error::Transport(Box::new(error))
}

fn detail(message: &str) -> Error {
    Error::Transport(Box::new(std::io::Error::other(message.to_owned())))
}
