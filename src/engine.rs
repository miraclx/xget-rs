//! The download engine: probe, plan, fetch chunks in parallel, reassemble in order, hash while writing.
//!
//! The pipeline is one-shot. Parallel chunks stream into bounded per-chunk channels; a single
//! reassembler drains them in order and both hashes and writes each byte in the same pass. Nothing is
//! read back: the digest is computed as the resource is written, not over the finished file. A source
//! that cannot serve ranges is fetched as one stream instead of parallel chunks.

use core::time::Duration;
use std::path::Path;

use bytes::Bytes;
use futures::StreamExt as _;
use futures::stream::FuturesUnordered;
use tokio::io::AsyncWriteExt as _;
use tokio::sync::mpsc;

use crate::{ByteRange, Checksum, Error, Options, Progress, Source, plan};

/// Buffers a chunk may read ahead before its fetch blocks on the reassembler. This is the
/// memory-versus-parallelism knob (the JS `--cache-size`): larger lets a chunk download further ahead
/// of the in-order reassembler, so a slow early chunk head-of-line-blocks the others less, at the cost
/// of `parts * CHUNK_BUFFER` buffered chunks of memory. TODO: expose as `--cache-size`.
const CHUNK_BUFFER: usize = 64;

/// The outcome of a completed download.
#[derive(Clone, Debug)]
pub struct Report {
    /// The verified total length in bytes.
    pub length: u64,
    /// The lowercase hex checksum, or `None` if no checksum was requested.
    pub hash: Option<String>,
}

/// Download the resource behind `source` into `output` per `options`, reporting to `progress`, and
/// return its verified length and checksum.
///
/// A range-capable resource is fetched in parallel chunks, hashed as they are written in a single pass;
/// one that is not is fetched as a single stream. Every chunk's range is validated, a retry resumes
/// from where it dropped, and the total length is gated, so the returned digest certifies the resource.
pub async fn download<S: Source, P: Progress>(
    source: &S,
    output: &Path,
    options: Options,
    progress: &P,
) -> Result<Report, Error> {
    let probe = source.probe().await?;
    let file = tokio::fs::File::create(output).await.map_err(io)?;

    let hash = if probe.supports_ranges {
        fetch_ranged(source, probe.length, options, file, progress).await?
    } else {
        fetch_whole(source, probe.length, options, file, progress).await?
    };

    progress.finish();
    Ok(Report {
        length: probe.length,
        hash,
    })
}

/// Fetch a range-capable resource as `parts` parallel chunks, reassembled and hashed in order.
async fn fetch_ranged<S: Source, P: Progress>(
    source: &S,
    total: u64,
    options: Options,
    file: tokio::fs::File,
    progress: &P,
) -> Result<Option<String>, Error> {
    let ranges = plan(total, options.parts);
    let sizes: Vec<u64> = ranges.iter().map(ByteRange::len).collect();
    progress.start(&sizes);

    // One bounded channel per chunk. The chunk fetches fill them in parallel; the reassembler drains
    // them in order, so the bytes leave this stage in resource order.
    let mut senders = Vec::with_capacity(ranges.len());
    let mut receivers = Vec::with_capacity(ranges.len());
    for _ in &ranges {
        let (tx, rx) = mpsc::channel::<Bytes>(CHUNK_BUFFER);
        senders.push(tx);
        receivers.push(rx);
    }

    // Structured concurrency on one task: the source's futures are not `Send`. The fetchers feed the
    // channels while the reassembler drains them, hashing and writing in a single pass.
    let mut fetchers: FuturesUnordered<_> = ranges
        .into_iter()
        .zip(senders)
        .enumerate()
        .map(|(index, (range, tx))| fetch_into(source, index, range, tx, options, progress))
        .collect();
    let drive = async {
        while let Some(result) = fetchers.next().await {
            result?;
        }
        Ok::<(), Error>(())
    };

    let (fetched, hashed) =
        tokio::join!(drive, reassemble(receivers, file, total, options.checksum));
    fetched?;
    hashed
}

/// Fetch a resource that does not support ranges as one stream, hashed and written in a single pass.
async fn fetch_whole<S: Source, P: Progress>(
    source: &S,
    total: u64,
    options: Options,
    file: tokio::fs::File,
    progress: &P,
) -> Result<Option<String>, Error> {
    progress.start(&[total]);
    let (tx, rx) = mpsc::channel::<Bytes>(CHUNK_BUFFER);
    let fetch = fetch_all_into(source, tx, options.timeout, progress);
    let (fetched, hashed) =
        tokio::join!(fetch, reassemble(vec![rx], file, total, options.checksum));
    fetched?;
    hashed
}

/// Stream the whole resource into a single channel. No range validation and no resume: a source
/// without ranges cannot be resumed, so an error mid-stream fails the download.
async fn fetch_all_into<S: Source, P: Progress>(
    source: &S,
    tx: mpsc::Sender<Bytes>,
    timeout: Option<Duration>,
    progress: &P,
) -> Result<(), Error> {
    let mut stream = source.fetch_all().await?;
    while let Some(chunk) = next_chunk(&mut stream, timeout).await? {
        let len = chunk.len() as u64;
        tx.send(chunk)
            .await
            .map_err(|_| detail("reassembler stopped receiving"))?;
        progress.advance(0, len);
    }
    Ok(())
}

/// Read the next chunk from a stream, failing with a typed error if none arrives within `timeout`. A
/// timeout on a ranged chunk is caught by its retry loop, which resumes from where it stalled.
async fn next_chunk(
    stream: &mut crate::ByteStream,
    timeout: Option<Duration>,
) -> Result<Option<Bytes>, Error> {
    let next = match timeout {
        Some(limit) => tokio::time::timeout(limit, stream.next())
            .await
            .map_err(|_| detail("timed out waiting for data"))?,
        None => stream.next().await,
    };
    next.transpose()
}

/// Fetch one chunk into its channel, resuming from its current offset with backoff if the connection
/// drops, and asserting it ultimately delivered exactly its range. Dropping the sender at the end
/// closes the channel, which signals the reassembler the chunk is complete.
async fn fetch_into<S: Source, P: Progress>(
    source: &S,
    index: usize,
    range: ByteRange,
    tx: mpsc::Sender<Bytes>,
    options: Options,
    progress: &P,
) -> Result<(), Error> {
    let mut offset = range.start;
    let mut last_error = None;
    for attempt in 0..=options.retries {
        if offset >= range.end {
            break;
        }
        if attempt > 0 {
            tokio::time::sleep(backoff(attempt)).await;
        }
        // Resume the remaining bytes; the source validates the resumed range starts at `offset`, so a
        // retry can neither duplicate nor gap bytes.
        match stream_into(
            source,
            index,
            range.end,
            &tx,
            &mut offset,
            options.timeout,
            progress,
        )
        .await
        {
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

/// Stream the range `[*offset, end)` into the channel, advancing `offset` and reporting `progress` as
/// bytes are sent. On a mid-stream error `offset` reflects how far it got, so the caller can resume.
/// The bounded channel applies backpressure, so a fast chunk cannot outrun the reassembler.
async fn stream_into<S: Source, P: Progress>(
    source: &S,
    index: usize,
    end: u64,
    tx: &mpsc::Sender<Bytes>,
    offset: &mut u64,
    timeout: Option<Duration>,
    progress: &P,
) -> Result<(), Error> {
    let mut stream = source
        .fetch(ByteRange {
            start: *offset,
            end,
        })
        .await?;
    while let Some(chunk) = next_chunk(&mut stream, timeout).await? {
        let len = chunk.len() as u64;
        if *offset + len > end {
            return Err(detail("source sent more bytes than the requested range"));
        }
        tx.send(chunk)
            .await
            .map_err(|_| detail("reassembler stopped receiving"))?;
        *offset += len;
        progress.advance(index, len);
    }
    Ok(())
}

/// Drain the per-chunk channels in order, hashing and writing each byte in one pass, and return the
/// hex checksum. Gates the total length so a short assembly is an error, not a certified bad file.
async fn reassemble(
    receivers: Vec<mpsc::Receiver<Bytes>>,
    mut file: tokio::fs::File,
    total: u64,
    checksum: Checksum,
) -> Result<Option<String>, Error> {
    let mut hasher = checksum.hasher();
    let mut written = 0u64;
    for mut receiver in receivers {
        while let Some(bytes) = receiver.recv().await {
            if let Some(hasher) = hasher.as_mut() {
                hasher.update(&bytes);
            }
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
    Ok(hasher.map(|mut hasher| hex::encode(hasher.finalize_reset())))
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
