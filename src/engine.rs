//! The download engine: probe, plan, fetch chunks in parallel, reassemble in order, hash while writing.
//!
//! The pipeline is one-shot. Parallel chunks stream into bounded per-chunk channels; a single
//! reassembler drains them in order and both hashes and writes each byte in the same pass. Nothing is
//! read back: the digest is computed as the resource is written, not over the finished file.

use core::time::Duration;
use std::path::Path;

use bytes::Bytes;
use futures::StreamExt as _;
use futures::stream::FuturesUnordered;
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncWriteExt as _;
use tokio::sync::mpsc;

use crate::{ByteRange, Error, Source, plan};

/// Buffers a chunk may hold before its fetch blocks on the reassembler. Peak memory is roughly
/// `parts * CHUNK_BUFFER` byte buffers, so parallelism cannot grow memory without bound.
const CHUNK_BUFFER: usize = 8;

/// The outcome of a completed download.
#[derive(Clone, Debug)]
pub struct Report {
    /// The verified total length in bytes.
    pub length: u64,
    /// The lowercase hex SHA-256 of the downloaded bytes.
    pub sha256: String,
}

/// Download the resource behind `source` into `output`, fetching up to `parts` chunks in parallel and
/// retrying a dropped chunk up to `retries` times, and return its verified length and SHA-256.
///
/// Chunks stream through in order and are hashed as they are written, in a single pass. Every chunk's
/// range is validated, a retry resumes from where it dropped, and the total length is gated, so the
/// returned digest certifies the resource rather than whatever bytes happened to arrive.
pub async fn download<S: Source>(
    source: &S,
    output: &Path,
    parts: u32,
    retries: u32,
) -> Result<Report, Error> {
    let probe = source.probe().await?;
    if !probe.supports_ranges && probe.length > 0 {
        return Err(detail(
            "resource does not support range requests (single-stream fetch is not yet implemented)",
        ));
    }
    let ranges = plan(probe.length, parts);

    // One bounded channel per chunk. The chunk fetches fill them in parallel; the reassembler drains
    // them in order, so the bytes leave this stage in resource order.
    let mut senders = Vec::with_capacity(ranges.len());
    let mut receivers = Vec::with_capacity(ranges.len());
    for _ in &ranges {
        let (tx, rx) = mpsc::channel::<Bytes>(CHUNK_BUFFER);
        senders.push(tx);
        receivers.push(rx);
    }

    let file = tokio::fs::File::create(output).await.map_err(io)?;

    // Structured concurrency on one task: the source's futures are not `Send`. The fetchers feed the
    // channels while the reassembler drains them, hashing and writing in a single pass.
    let mut fetchers: FuturesUnordered<_> = ranges
        .into_iter()
        .zip(senders)
        .map(|(range, tx)| fetch_into(source, range, tx, retries))
        .collect();
    let fetch_all = async {
        while let Some(result) = fetchers.next().await {
            result?;
        }
        Ok::<(), Error>(())
    };

    let (fetched, hashed) = tokio::join!(fetch_all, reassemble(receivers, file, probe.length));
    fetched?;
    let sha256 = hashed?;
    Ok(Report {
        length: probe.length,
        sha256,
    })
}

/// Fetch one chunk into its channel, resuming from its current offset with backoff if the connection
/// drops, and asserting it ultimately delivered exactly its range. Dropping the sender at the end
/// closes the channel, which signals the reassembler the chunk is complete.
async fn fetch_into<S: Source>(
    source: &S,
    range: ByteRange,
    tx: mpsc::Sender<Bytes>,
    retries: u32,
) -> Result<(), Error> {
    let mut offset = range.start;
    let mut last_error = None;
    for attempt in 0..=retries {
        if offset >= range.end {
            break;
        }
        if attempt > 0 {
            tokio::time::sleep(backoff(attempt)).await;
        }
        // Resume the remaining bytes; the source validates the resumed range starts at `offset`, so a
        // retry can neither duplicate nor gap bytes.
        match stream_into(source, offset, range.end, &tx, &mut offset).await {
            Ok(()) => {}
            Err(error) => last_error = Some(error),
        }
    }
    if offset < range.end {
        return Err(last_error.unwrap_or_else(|| Error::LengthMismatch {
            expected: range.len(),
            received: offset - range.start,
        }));
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

/// Stream the range `[start, end)` into the channel, advancing `offset` as bytes are sent. On a
/// mid-stream error `offset` reflects how far it got, so the caller can resume from there. The bounded
/// channel applies backpressure, so a fast chunk cannot outrun the reassembler.
async fn stream_into<S: Source>(
    source: &S,
    start: u64,
    end: u64,
    tx: &mpsc::Sender<Bytes>,
    offset: &mut u64,
) -> Result<(), Error> {
    let mut stream = source.fetch(ByteRange { start, end }).await?;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        let len = chunk.len() as u64;
        if *offset + len > end {
            return Err(detail("source sent more bytes than the requested range"));
        }
        tx.send(chunk)
            .await
            .map_err(|_| detail("reassembler stopped receiving"))?;
        *offset += len;
    }
    Ok(())
}

/// Drain the per-chunk channels in order, hashing and writing each byte in one pass, and return the
/// hex SHA-256. Gates the total length so a short assembly is an error, not a certified bad file.
async fn reassemble(
    receivers: Vec<mpsc::Receiver<Bytes>>,
    mut file: tokio::fs::File,
    total: u64,
) -> Result<String, Error> {
    let mut hasher = Sha256::new();
    let mut written = 0u64;
    for mut receiver in receivers {
        while let Some(bytes) = receiver.recv().await {
            hasher.update(&bytes);
            file.write_all(&bytes).await.map_err(io)?;
            written += bytes.len() as u64;
        }
    }
    file.flush().await.map_err(io)?;
    if written != total {
        return Err(Error::LengthMismatch {
            expected: total,
            received: written,
        });
    }
    Ok(hex::encode(hasher.finalize()))
}

/// Exponential backoff before a retry, capped at a few seconds.
fn backoff(attempt: u32) -> Duration {
    Duration::from_millis(100u64.saturating_mul(1u64 << attempt.min(6)))
}

fn io(error: std::io::Error) -> Error {
    Error::Transport(Box::new(error))
}

fn detail(message: &str) -> Error {
    Error::Transport(Box::new(std::io::Error::other(message.to_owned())))
}
