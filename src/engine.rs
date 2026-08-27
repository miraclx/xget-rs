//! The download engine: probe, plan, fetch chunks in parallel, reassemble in order, hash while writing.
//!
//! The pipeline is one-shot. Parallel chunks stream into bounded per-chunk channels; a single
//! reassembler drains them in order and both hashes and writes each byte in the same pass. The live
//! download is never read back: the digest is computed as the resource is written, not over the
//! finished file. A source that cannot serve ranges is fetched as one stream instead of parallel
//! chunks. The one exception is resuming: the bytes already on disk are read once to seed the hasher,
//! concurrently with the live fetch so the read overlaps the download rather than blocking it.

use core::time::Duration;
use std::path::Path;

use bytes::Bytes;
use futures::StreamExt as _;
use futures::stream::FuturesUnordered;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::mpsc;

use crate::plan::plan_range;
use crate::{ByteRange, Checksum, Error, Options, Progress, Source};

/// The bytes already present in the output when resuming: a read handle positioned at the start and the
/// number of bytes to fold into the checksum before hashing anything new.
struct Prefix {
    reader: tokio::fs::File,
    len: u64,
}

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
    let (file, start, prefix) = open_output(output, &probe, &options).await?;

    let hash = if start > 0 || probe.supports_ranges {
        // A resume, or a fresh range-capable fetch: plan `[start, total)` into parallel chunks.
        fetch_ranged(source, start, probe.length, options, file, prefix, progress).await?
    } else {
        fetch_whole(source, probe.length, options, file, progress).await?
    };

    progress.finish();
    Ok(Report {
        length: probe.length,
        hash,
    })
}

/// Open the output for writing and work out where the download should start. A fresh download truncates
/// the file and starts at zero; a resume keeps what is there, appends the rest, and returns a [`Prefix`]
/// handle so the existing bytes can be folded into the checksum.
async fn open_output(
    output: &Path,
    probe: &crate::Probe,
    options: &Options,
) -> Result<(tokio::fs::File, u64, Option<Prefix>), Error> {
    let existing = if options.resume {
        tokio::fs::metadata(output)
            .await
            .map(|meta| meta.len())
            .unwrap_or(0)
    } else {
        0
    };

    if existing == 0 {
        let file = tokio::fs::File::create(output).await.map_err(io)?;
        return Ok((file, 0, None));
    }
    if existing > probe.length {
        return Err(detail(
            "existing file is larger than the resource; refusing to resume",
        ));
    }
    if !probe.supports_ranges {
        return Err(detail(
            "cannot resume: the source does not support byte ranges",
        ));
    }
    let file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(output)
        .await
        .map_err(io)?;
    let reader = tokio::fs::File::open(output).await.map_err(io)?;
    Ok((
        file,
        existing,
        Some(Prefix {
            reader,
            len: existing,
        }),
    ))
}

/// Fetch a range-capable resource's `[start, total)` bytes as `parts` parallel chunks, reassembled and
/// hashed in order after any resumed `prefix`.
async fn fetch_ranged<S: Source, P: Progress>(
    source: &S,
    start: u64,
    total: u64,
    options: Options,
    file: tokio::fs::File,
    prefix: Option<Prefix>,
    progress: &P,
) -> Result<Option<String>, Error> {
    let ranges = plan_range(start, total, options.parts);
    let sizes: Vec<u64> = ranges.iter().map(ByteRange::len).collect();
    progress.start(&sizes);

    // One bounded channel per chunk. The chunk fetches fill them in parallel; the reassembler drains
    // them in order, so the bytes leave this stage in resource order.
    let cache = options.cache.max(1);
    let mut senders = Vec::with_capacity(ranges.len());
    let mut receivers = Vec::with_capacity(ranges.len());
    for _ in &ranges {
        let (tx, rx) = mpsc::channel::<Bytes>(cache);
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

    let (fetched, hashed) = tokio::join!(
        drive,
        reassemble(receivers, file, total, options.checksum, prefix)
    );
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
    let (tx, rx) = mpsc::channel::<Bytes>(options.cache.max(1));
    let fetch = fetch_all_into(source, tx, options.timeout, progress);
    let (fetched, hashed) = tokio::join!(
        fetch,
        reassemble(vec![rx], file, total, options.checksum, None)
    );
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

/// Fold the resumed `prefix` into the hasher, then drain the per-chunk channels in order, hashing and
/// writing each new byte in one pass, and return the hex checksum. Gates the total length so a short
/// assembly is an error, not a certified bad file.
///
/// Reading the prefix happens here while the fetchers are already pulling new bytes from the network
/// into the bounded channels, so the disk read overlaps the live download rather than blocking it. The
/// hasher still consumes prefix-then-new in order, which a single digest requires.
async fn reassemble(
    receivers: Vec<mpsc::Receiver<Bytes>>,
    mut file: tokio::fs::File,
    total: u64,
    checksum: Checksum,
    prefix: Option<Prefix>,
) -> Result<Option<String>, Error> {
    let mut hasher = checksum.hasher();
    let mut written = 0u64;

    if let Some(mut prefix) = prefix {
        let mut buffer = vec![0u8; 64 * 1024];
        let mut remaining = prefix.len;
        while remaining > 0 {
            let want = remaining.min(buffer.len() as u64) as usize;
            let read = prefix.reader.read(&mut buffer[..want]).await.map_err(io)?;
            if read == 0 {
                break;
            }
            if let Some(hasher) = hasher.as_mut() {
                hasher.update(&buffer[..read]);
            }
            written += read as u64;
            remaining -= read as u64;
        }
        if written != prefix.len {
            return Err(Error::LengthMismatch {
                expected: prefix.len,
                received: written,
            });
        }
    }

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
