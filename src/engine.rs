//! The download engine: probe, plan, scatter chunks into a sparse file, verify in order.
//!
//! A range-capable resource is planned into contiguous chunks that download in parallel, each written
//! straight to its own offset in a preallocated sparse file. There is no in-memory reassembly and no
//! cross-chunk backpressure, so every connection streams continuously. A single verifier then reads the
//! file back from offset zero, hashing the contiguous, hole-free prefix as it grows: it follows the
//! earliest unfinished chunk, so the verified frontier advances the moment bytes fill in, not in
//! whole-chunk jumps. That prefix is what a returned digest certifies. So `received` marks bytes written
//! anywhere in the file (how much is downloaded) and `wrote` marks the verified prefix from zero (how
//! much is exact). A resume folds the bytes already on disk into that same pass. A source that cannot
//! serve ranges is fetched as one stream, hashed inline as it is written.

use core::cell::{Cell, RefCell};
use core::time::Duration;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use futures::StreamExt as _;
use futures::stream::FuturesUnordered;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _, AsyncWriteExt as _};
use tokio::sync::Notify;

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

/// Fetch `[start, total)` as parallel chunks scattered into `part`, verified continuously. The fetchers
/// write to their own offsets and report how far each has reached; the verifier reads the file back
/// from zero and hashes the contiguous, hole-free prefix as it grows, so the verified frontier follows
/// the earliest unfinished chunk rather than jumping a whole chunk at a time.
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

    let shared = Rc::new(Shared {
        received: RefCell::new(vec![0u64; ranges.len()]),
        notify: Notify::new(),
        failed: Cell::new(false),
    });

    // Structured concurrency on one task: the source's futures are not `Send`. The fetchers scatter
    // into the file and nudge the shared state while the verifier reads the contiguous prefix and hashes.
    let mut fetchers: FuturesUnordered<_> = ranges
        .iter()
        .copied()
        .enumerate()
        .map(|(index, range)| scatter_one(source, part, index, range, options, &shared, progress))
        .collect();
    let drive = async {
        let mut outcome = Ok(());
        while let Some(result) = fetchers.next().await {
            if let Err(error) = result {
                shared.failed.set(true);
                outcome = Err(error);
                break;
            }
        }
        // Wake the verifier for its final pass (or so it sees the failure).
        shared.notify.notify_one();
        outcome
    };

    let (fetched, hashed) = tokio::join!(
        drive,
        verify(
            part,
            start,
            &ranges,
            &shared,
            options.checksum,
            total,
            progress
        )
    );
    fetched?;
    hashed
}

/// State a scatter download shares between its fetchers and its verifier: how many bytes each chunk has
/// received, a nudge when that changes, and whether a chunk has permanently failed.
struct Shared {
    received: RefCell<Vec<u64>>,
    notify: Notify,
    failed: Cell<bool>,
}

/// One chunk's write target: its own file handle, its range, and the shared state it reports into.
struct Sink<'a> {
    index: usize,
    range: ByteRange,
    file: File,
    shared: &'a Rc<Shared>,
}

impl Sink<'_> {
    /// Stream `[*offset, range.end)` from `source` into the file at that offset, advancing `offset`,
    /// recording each write into the shared received count, and nudging the verifier. On a mid-stream
    /// error `offset` reflects how far it got, so the caller can seek back and resume.
    async fn fill<S: Source, P: Progress>(
        &mut self,
        source: &S,
        offset: &mut u64,
        timeout: Option<Duration>,
        progress: &P,
    ) -> Result<(), Error> {
        self.file.seek(SeekFrom::Start(*offset)).await.map_err(io)?;
        let mut stream = source
            .fetch(Some(ByteRange {
                start: *offset,
                end: self.range.end,
            }))
            .await?;
        while let Some(chunk) = next_chunk(&mut stream, timeout).await? {
            let len = chunk.len() as u64;
            if *offset + len > self.range.end {
                return Err(detail("source sent more bytes than the requested range"));
            }
            self.file.write_all(&chunk).await.map_err(io)?;
            // Flush before recording, so the bytes are visible to the verifier's separate read handle
            // (a buffered write would otherwise let it read a stale hole and hash the wrong bytes).
            self.file.flush().await.map_err(io)?;
            *offset += len;
            self.shared.received.borrow_mut()[self.index] = *offset - self.range.start;
            self.shared.notify.notify_one();
            progress.received(self.index, len);
        }
        Ok(())
    }
}

/// Download one chunk's range into `part` at its offset, resuming from where it dropped with backoff.
/// Each chunk holds its own file handle, so concurrent writes to different offsets never contend.
async fn scatter_one<S: Source, P: Progress>(
    source: &S,
    part: &Path,
    index: usize,
    range: ByteRange,
    options: Options,
    shared: &Rc<Shared>,
    progress: &P,
) -> Result<(), Error> {
    let file = OpenOptions::new()
        .write(true)
        .open(part)
        .await
        .map_err(io)?;
    let mut sink = Sink {
        index,
        range,
        file,
        shared,
    };
    let mut offset = range.start;
    let mut last_error = None;
    for attempt in 0..=options.retries {
        if offset >= range.end {
            break;
        }
        if attempt > 0 {
            tokio::time::sleep(backoff(attempt)).await;
        }
        match sink
            .fill(source, &mut offset, options.timeout, progress)
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
    Ok(())
}

/// Read `part` back from offset zero and hash the contiguous, hole-free prefix as it grows: the resumed
/// prefix first, then bytes as the earliest unfinished chunk receives them. Reports `wrote` per chunk so
/// the verified frontier advances continuously, and gates the total length so the digest certifies the
/// whole file.
async fn verify<P: Progress>(
    part: &Path,
    start: u64,
    ranges: &[ByteRange],
    shared: &Rc<Shared>,
    checksum: Checksum,
    total: u64,
    progress: &P,
) -> Result<Option<String>, Error> {
    let mut hasher = checksum.hasher();
    let mut reader = File::open(part).await.map_err(io)?;
    let mut buffer = vec![0u8; 128 * 1024];

    // The resumed prefix is already on disk; fold it in first (it is not a planned chunk, so no report).
    read_into(&mut reader, &mut hasher, start, &mut buffer).await?;
    let mut hashed = start;

    loop {
        if shared.failed.get() {
            return Err(detail("a chunk failed before it could be verified"));
        }
        let frontier = start + contiguous_run(&shared.received.borrow(), ranges);
        while hashed < frontier {
            // Read no further than the current chunk's end, so each read is one chunk's bytes to report.
            let index = chunk_of(ranges, hashed);
            let want = (frontier - hashed).min(ranges[index].end - hashed);
            let before = hashed;
            read_into(&mut reader, &mut hasher, want, &mut buffer).await?;
            progress.wrote(index, want);
            hashed = before + want;
        }
        if hashed >= total {
            break;
        }
        shared.notify.notified().await;
    }
    Ok(hasher.map(|hasher| hasher.finalize_hex()))
}

/// The number of contiguous bytes downloaded from the plan's start: every leading fully-received chunk,
/// plus the partial bytes of the first chunk that is not yet complete.
fn contiguous_run(received: &[u64], ranges: &[ByteRange]) -> u64 {
    let mut run = 0;
    for (got, range) in received.iter().zip(ranges) {
        run += got;
        if *got < range.len() {
            break;
        }
    }
    run
}

/// The index of the chunk whose range contains file offset `offset` (which is at or past the plan start).
fn chunk_of(ranges: &[ByteRange], offset: u64) -> usize {
    ranges
        .iter()
        .position(|range| offset < range.end)
        .unwrap_or(ranges.len().saturating_sub(1))
}

/// Read exactly `count` bytes from `reader` in order and fold them into `hasher`.
async fn read_into(
    reader: &mut File,
    hasher: &mut Option<Box<dyn Hasher>>,
    count: u64,
    buffer: &mut [u8],
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
