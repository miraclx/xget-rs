//! The download engine: probe, plan, fetch chunks in parallel, reassemble in order, hash while writing.
//!
//! The pipeline is one-shot. Parallel chunks stream into per-chunk channels, each with its own byte
//! budget that bounds how far ahead it may buffer; a single reassembler drains them in order and both
//! hashes and writes each byte in the same pass, releasing budget as it goes. The live
//! download is never read back: the digest is computed as the resource is written, not over the
//! finished file. A source that cannot serve ranges is fetched as one stream instead of parallel
//! chunks. The one exception is resuming: the bytes already on disk are read once to seed the hasher,
//! concurrently with the live fetch so the read overlaps the download rather than blocking it.

use core::time::Duration;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt as _;
use futures::stream::FuturesUnordered;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use xbytes::ByteSize;
use xbytes::sizes::MEBI_BYTE;

use crate::plan::plan_range;
use crate::{ByteRange, Checksum, Error, Options, Progress, Source};

/// A buffered chunk in transit, carrying the memory permit for its bytes; dropping the permit after the
/// bytes are written returns that much room to the chunk's budget, which is what applies backpressure.
type Buffered = (Bytes, OwnedSemaphorePermit);

/// Where a chunk's fetch sends its buffers: the channel to the reassembler and the byte budget that
/// bounds how far it may run ahead. The two always travel together.
struct Sink {
    tx: mpsc::UnboundedSender<Buffered>,
    budget: Arc<Semaphore>,
}

impl Sink {
    /// Reserve room for `len` bytes then hand them to the reassembler, keeping the permit alive with the
    /// bytes so the room is only freed once they are written.
    async fn send(&self, chunk: Bytes) -> Result<(), Error> {
        let len = chunk.len() as u64;
        let want = len.clamp(1, u64::from(u32::MAX)) as u32;
        let permit = Arc::clone(&self.budget)
            .acquire_many_owned(want)
            .await
            .map_err(|_| detail("memory budget closed"))?;
        self.tx
            .send((chunk, permit))
            .map_err(|_| detail("reassembler stopped receiving"))
    }
}

/// The smallest per-chunk memory budget, and so the largest single buffer that can ever be admitted.
/// Network reads are far smaller, so a buffer always fits and a chunk can never deadlock on its budget.
fn min_chunk_budget() -> u64 {
    ByteSize::of(4u64, MEBI_BYTE).byte_count() as u64
}

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
    // Write to a sibling `.part` and only rename it into place once the length and hash gates pass, so
    // an interrupted download never leaves a truncated file masquerading as complete. The `.part` is
    // also what a `--continue` resume picks up.
    let part = part_path(output);
    let (file, start, prefix) = open_output(&part, &probe, &options).await?;

    let hash = if start > 0 || probe.supports_ranges {
        // A resume, or a fresh range-capable fetch: plan `[start, total)` into parallel chunks.
        fetch_ranged(source, start, probe.length, options, file, prefix, progress).await?
    } else {
        fetch_whole(source, probe.length, options, file, progress).await?
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
fn part_path(output: &Path) -> std::path::PathBuf {
    let mut name = output.as_os_str().to_owned();
    name.push(".part");
    std::path::PathBuf::from(name)
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

    // One channel per chunk with its own byte budget. The chunk fetches fill them in parallel; the
    // reassembler drains them in order, so the bytes leave this stage in resource order. The budget is
    // per chunk, not shared, so a late chunk buffering ahead can never starve the frontier chunk the
    // reassembler is waiting on. The total memory ceiling is `parts * per_chunk`, about `cache`.
    let per_chunk = (options.cache / ranges.len().max(1) as u64).max(min_chunk_budget());
    let mut sinks = Vec::with_capacity(ranges.len());
    let mut receivers = Vec::with_capacity(ranges.len());
    for _ in &ranges {
        let (tx, rx) = mpsc::unbounded_channel::<Buffered>();
        receivers.push(rx);
        sinks.push(Sink {
            tx,
            budget: Arc::new(Semaphore::new(per_chunk as usize)),
        });
    }

    // Structured concurrency on one task: the source's futures are not `Send`. The fetchers feed the
    // channels while the reassembler drains them, hashing and writing in a single pass.
    let mut fetchers: FuturesUnordered<_> = ranges
        .into_iter()
        .zip(sinks)
        .enumerate()
        .map(|(index, (range, sink))| fetch_into(source, index, range, sink, options, progress))
        .collect();
    let drive = async {
        while let Some(result) = fetchers.next().await {
            result?;
        }
        Ok::<(), Error>(())
    };

    let (fetched, hashed) = tokio::join!(
        drive,
        reassemble(receivers, file, total, options.checksum, prefix, progress)
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
    let (tx, rx) = mpsc::unbounded_channel::<Buffered>();
    let sink = Sink {
        tx,
        budget: Arc::new(Semaphore::new(
            options.cache.max(min_chunk_budget()) as usize
        )),
    };
    let fetch = fetch_all_into(source, sink, options.timeout, progress);
    let (fetched, hashed) = tokio::join!(
        fetch,
        reassemble(vec![rx], file, total, options.checksum, None, progress)
    );
    fetched?;
    hashed
}

/// Stream the whole resource into a single channel. No range validation and no resume: a source
/// without ranges cannot be resumed, so an error mid-stream fails the download.
async fn fetch_all_into<S: Source, P: Progress>(
    source: &S,
    sink: Sink,
    timeout: Option<Duration>,
    progress: &P,
) -> Result<(), Error> {
    let mut stream = source.fetch(None).await?;
    while let Some(chunk) = next_chunk(&mut stream, timeout).await? {
        let len = chunk.len() as u64;
        sink.send(chunk).await?;
        progress.received(0, len);
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
    sink: Sink,
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
            &sink,
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
/// Each buffer reserves room from the chunk's `budget` first, so a fast chunk cannot outrun the
/// reassembler by more than its budget.
async fn stream_into<S: Source, P: Progress>(
    source: &S,
    index: usize,
    end: u64,
    sink: &Sink,
    offset: &mut u64,
    timeout: Option<Duration>,
    progress: &P,
) -> Result<(), Error> {
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
        sink.send(chunk).await?;
        *offset += len;
        progress.received(index, len);
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
async fn reassemble<P: Progress>(
    receivers: Vec<mpsc::UnboundedReceiver<Buffered>>,
    mut file: tokio::fs::File,
    total: u64,
    checksum: Checksum,
    prefix: Option<Prefix>,
    progress: &P,
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

    for (index, mut receiver) in receivers.into_iter().enumerate() {
        while let Some((bytes, permit)) = receiver.recv().await {
            let len = bytes.len() as u64;
            if let Some(hasher) = hasher.as_mut() {
                hasher.update(&bytes);
            }
            file.write_all(&bytes).await.map_err(io)?;
            written += len;
            progress.wrote(index, len);
            // The bytes are written, so return their room to the chunk's budget for the next buffer.
            drop(permit);
        }
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
