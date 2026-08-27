//! The download engine: probe, plan, scatter chunks into a sparse file, verify in order.
//!
//! A range-capable resource is planned into contiguous chunks that download in parallel, each written
//! straight to its own offset in a preallocated sparse file. There is no in-memory reassembly and no
//! cross-chunk backpressure, so every connection streams continuously. A single verifier then reads the
//! file back from offset zero, hashing the contiguous, hole-free prefix as each chunk completes: that
//! prefix is what a returned digest certifies. So `received` marks bytes written anywhere in the file
//! (how much is downloaded) and `wrote` marks the verified prefix from zero (how much is exact). A
//! resume re-hashes the bytes already on disk as part of that same in-order pass. A source that cannot
//! serve ranges is fetched as one stream, hashed inline as it is written.

use core::time::Duration;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};

use futures::StreamExt as _;
use futures::stream::FuturesUnordered;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _, AsyncWriteExt as _};
use tokio::sync::oneshot;

use crate::checksum::Hasher;
use crate::plan::plan_range;
use crate::{ByteRange, Checksum, Error, Options, Progress, Source};

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
/// A range-capable resource is scattered into a sparse `.part` in parallel and verified in order; one
/// that is not is streamed and hashed inline. Every chunk's range is validated, a dropped chunk resumes
/// from its offset, and the total length is gated, so the returned digest certifies the resource.
pub async fn download<S: Source, P: Progress>(
    source: &S,
    output: &Path,
    options: Options,
    progress: &P,
) -> Result<Report, Error> {
    let probe = source.probe().await?;
    // Write to a sibling `.part` and only rename it into place once the length and hash gates pass, so
    // an interrupted download never leaves a truncated file masquerading as complete. The `.part` is
    // also what a `--continue` resume picks up.
    let part = part_path(output);

    let hash = if probe.supports_ranges {
        let start = resume_offset(&part, probe.length, options.resume).await?;
        allocate(&part, probe.length, start).await?;
        fetch_scatter(source, &part, start, probe.length, options, progress).await?
    } else {
        if options.resume && file_len(&part).await > 0 {
            return Err(detail(
                "cannot resume: the source does not support byte ranges",
            ));
        }
        fetch_stream(source, &part, probe.length, options, progress).await?
    };

    tokio::fs::rename(&part, output).await.map_err(io)?;
    progress.finish();
    Ok(Report {
        length: probe.length,
        hash,
    })
}

/// The sibling `.part` path a download writes to before it is renamed into place: the output name with
/// `.part` appended, so a multi-part extension like `.tar.gz` is preserved.
fn part_path(output: &Path) -> PathBuf {
    let mut name = output.as_os_str().to_owned();
    name.push(".part");
    PathBuf::from(name)
}

/// Where a resumed download should start: the length already on disk, or zero for a fresh download.
/// Refuses a partial larger than the resource.
async fn resume_offset(part: &Path, total: u64, resume: bool) -> Result<u64, Error> {
    if !resume {
        return Ok(0);
    }
    let existing = file_len(part).await;
    if existing > total {
        return Err(detail(
            "existing file is larger than the resource; refusing to resume",
        ));
    }
    Ok(existing)
}

/// The size of `part`, or zero if it does not exist.
async fn file_len(part: &Path) -> u64 {
    tokio::fs::metadata(part)
        .await
        .map(|meta| meta.len())
        .unwrap_or(0)
}

/// Preallocate `part` to `total` bytes as a sparse file so every chunk offset exists to be written. A
/// fresh download (`start == 0`) truncates first; a resume keeps the bytes already there and extends.
async fn allocate(part: &Path, total: u64, start: u64) -> Result<(), Error> {
    let file = if start == 0 {
        File::create(part).await.map_err(io)?
    } else {
        OpenOptions::new()
            .write(true)
            .open(part)
            .await
            .map_err(io)?
    };
    file.set_len(total).await.map_err(io)?;
    Ok(())
}

/// Fetch `[start, total)` as parallel chunks scattered into `part`, verified in order. The fetchers
/// write to their own offsets while a verifier reads the file back from zero, hashing each chunk's
/// region as it completes.
async fn fetch_scatter<S: Source, P: Progress>(
    source: &S,
    part: &Path,
    start: u64,
    total: u64,
    options: Options,
    progress: &P,
) -> Result<Option<String>, Error> {
    let ranges = plan_range(start, total, options.parts);
    let sizes: Vec<u64> = ranges.iter().map(ByteRange::len).collect();
    progress.start(&sizes);

    // One completion signal per chunk. A fetcher fires it once its whole region is on disk; the
    // verifier waits on them in order before reading each region back.
    let mut senders = Vec::with_capacity(ranges.len());
    let mut receivers = Vec::with_capacity(ranges.len());
    for _ in &ranges {
        let (tx, rx) = oneshot::channel::<()>();
        senders.push(tx);
        receivers.push(rx);
    }

    // Structured concurrency on one task: the source's futures are not `Send`. The fetchers scatter
    // into the file while the verifier reads it back in order and hashes.
    let mut fetchers: FuturesUnordered<_> = ranges
        .iter()
        .copied()
        .zip(senders)
        .enumerate()
        .map(|(index, (range, done))| {
            scatter_one(source, part, index, range, options, progress, done)
        })
        .collect();
    let drive = async {
        while let Some(result) = fetchers.next().await {
            result?;
        }
        Ok::<(), Error>(())
    };

    let (fetched, hashed) = tokio::join!(
        drive,
        verify(
            part,
            start,
            &ranges,
            receivers,
            options.checksum,
            total,
            progress
        )
    );
    fetched?;
    hashed
}

/// Download one chunk's range into `part` at its offset, resuming from where it dropped with backoff,
/// and signal completion once the whole region is written. Each chunk holds its own file handle, so
/// concurrent writes to different offsets never contend.
async fn scatter_one<S: Source, P: Progress>(
    source: &S,
    part: &Path,
    index: usize,
    range: ByteRange,
    options: Options,
    progress: &P,
    done: oneshot::Sender<()>,
) -> Result<(), Error> {
    let mut file = OpenOptions::new()
        .write(true)
        .open(part)
        .await
        .map_err(io)?;
    let mut offset = range.start;
    let mut last_error = None;
    for attempt in 0..=options.retries {
        if offset >= range.end {
            break;
        }
        if attempt > 0 {
            tokio::time::sleep(backoff(attempt)).await;
        }
        match write_range(
            source,
            index,
            &mut file,
            &mut offset,
            range.end,
            options.timeout,
            progress,
        )
        .await
        {
            Ok(()) => {}
            Err(error) => last_error = Some(error),
        }
    }
    let received = offset - range.start;
    if received != range.len() {
        return Err(last_error.unwrap_or(Error::LengthMismatch {
            expected: range.len(),
            received,
        }));
    }
    file.flush().await.map_err(io)?;
    // The region is durable; let the verifier read it. A gone verifier just means the download failed
    // elsewhere, so the error is ignored here.
    let _ = done.send(());
    Ok(())
}

/// Stream `[*offset, end)` from the source into `file` at that offset, advancing `offset` and reporting
/// `received` as bytes land. On a mid-stream error `offset` reflects how far it got, so the caller can
/// seek back and resume.
async fn write_range<S: Source, P: Progress>(
    source: &S,
    index: usize,
    file: &mut File,
    offset: &mut u64,
    end: u64,
    timeout: Option<Duration>,
    progress: &P,
) -> Result<(), Error> {
    file.seek(SeekFrom::Start(*offset)).await.map_err(io)?;
    let mut stream = source
        .fetch(Some(ByteRange {
            start: *offset,
            end,
        }))
        .await?;
    while let Some(chunk) = next_chunk(&mut stream, timeout).await? {
        let len = chunk.len() as u64;
        if *offset + len > end {
            return Err(detail("source sent more bytes than the requested range"));
        }
        file.write_all(&chunk).await.map_err(io)?;
        *offset += len;
        progress.received(index, len);
    }
    Ok(())
}

/// Read `part` back from offset zero and hash it in order: the resumed prefix first, then each chunk's
/// region once that chunk signals it is fully written. Reports `wrote` as the verified prefix grows and
/// gates the total length, so the returned digest certifies the whole file.
async fn verify<P: Progress>(
    part: &Path,
    start: u64,
    ranges: &[ByteRange],
    receivers: Vec<oneshot::Receiver<()>>,
    checksum: Checksum,
    total: u64,
    progress: &P,
) -> Result<Option<String>, Error> {
    let mut hasher = checksum.hasher();
    let mut reader = File::open(part).await.map_err(io)?;
    let mut buffer = vec![0u8; 128 * 1024];
    let mut written = 0u64;

    // The resumed prefix is already on disk; fold it in first (no progress: it is not a planned chunk).
    hash_region(&mut reader, &mut hasher, start, None, &mut buffer, progress).await?;
    written += start;

    // Each chunk's region, in order, once the chunk has written all of it.
    for (index, (range, done)) in ranges.iter().zip(receivers).enumerate() {
        done.await
            .map_err(|_| detail("a chunk failed before it could be verified"))?;
        hash_region(
            &mut reader,
            &mut hasher,
            range.len(),
            Some(index),
            &mut buffer,
            progress,
        )
        .await?;
        written += range.len();
    }

    if written != total {
        return Err(Error::LengthMismatch {
            expected: total,
            received: written,
        });
    }
    Ok(hasher.map(|hasher| hasher.finalize_hex()))
}

/// Read exactly `count` bytes from `reader` in order, folding them into `hasher` and, when `report` is
/// `Some(index)`, reporting them as `wrote` for that chunk so the verified prefix advances on screen.
async fn hash_region<P: Progress>(
    reader: &mut File,
    hasher: &mut Option<Box<dyn Hasher>>,
    count: u64,
    report: Option<usize>,
    buffer: &mut [u8],
    progress: &P,
) -> Result<(), Error> {
    let mut remaining = count;
    while remaining > 0 {
        let want = remaining.min(buffer.len() as u64) as usize;
        let read = reader.read(&mut buffer[..want]).await.map_err(io)?;
        if read == 0 {
            return Err(Error::LengthMismatch {
                expected: count,
                received: count - remaining,
            });
        }
        if let Some(hasher) = hasher.as_mut() {
            hasher.update(&buffer[..read]);
        }
        if let Some(index) = report {
            progress.wrote(index, read as u64);
        }
        remaining -= read as u64;
    }
    Ok(())
}

/// Fetch a resource that does not support ranges as one stream, hashing and writing each byte in one
/// in-order pass (no scatter, no re-read: a single stream is already in order).
async fn fetch_stream<S: Source, P: Progress>(
    source: &S,
    part: &Path,
    total: u64,
    options: Options,
    progress: &P,
) -> Result<Option<String>, Error> {
    progress.start(&[total]);
    let mut file = File::create(part).await.map_err(io)?;
    let mut hasher = options.checksum.hasher();
    let mut stream = source.fetch(None).await?;
    let mut written = 0u64;
    while let Some(chunk) = next_chunk(&mut stream, options.timeout).await? {
        let len = chunk.len() as u64;
        if let Some(hasher) = hasher.as_mut() {
            hasher.update(&chunk);
        }
        file.write_all(&chunk).await.map_err(io)?;
        written += len;
        progress.received(0, len);
        progress.wrote(0, len);
    }
    file.flush().await.map_err(io)?;
    if written != total {
        return Err(Error::LengthMismatch {
            expected: total,
            received: written,
        });
    }
    Ok(hasher.map(|hasher| hasher.finalize_hex()))
}

/// Read the next chunk from a stream, failing with a typed error if none arrives within `timeout`. A
/// timeout on a ranged chunk is caught by its retry loop, which resumes from where it stalled.
async fn next_chunk(
    stream: &mut crate::ByteStream,
    timeout: Option<Duration>,
) -> Result<Option<bytes::Bytes>, Error> {
    let next = match timeout {
        Some(limit) => tokio::time::timeout(limit, stream.next())
            .await
            .map_err(|_| detail("timed out waiting for data"))?,
        None => stream.next().await,
    };
    next.transpose()
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
