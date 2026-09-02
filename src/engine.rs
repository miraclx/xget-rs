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
use core::sync::atomic::{AtomicU64, Ordering};
use core::time::Duration;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Instant;

use futures::StreamExt as _;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _, AsyncWrite, AsyncWriteExt as _};
use tokio::sync::{Mutex, Notify};
use xbytes::ByteSize;
use xbytes::sizes::MEBI_BYTE;

use crate::checksum::Hasher;
use crate::control::Writer;
use crate::plan::{plan_range, plan_resume};
use crate::{ByteRange, Checksum, Error, Options, Output, Progress, Source, control};

/// Bumped for each temp scratch a `Writer`/`Discard` download needs, so concurrent downloads in one
/// process never collide on the same `.xget` scratch name.
static SCRATCH_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A chunk checkpoints its flushed prefix to the control trailer after this many freshly downloaded
/// bytes, so a fast large chunk records often without a checkpoint per write.
const CHECKPOINT_BYTES: u64 = ByteSize::of_int(4, MEBI_BYTE).byte_count_lossy() as u64;

/// ...and at least this often while it is making progress, so a small or slow chunk that never reaches
/// [`CHECKPOINT_BYTES`] still persists its partial progress and resumes near where it stopped rather
/// than from zero.
const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(1);

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
/// The verified bytes go where [`Output`] says: an [`Output::File`] scatters into a sibling `.xget` and
/// atomic-renames it into place (and alone may resume); an [`Output::Writer`] streams the confirmed bytes
/// to any [`tokio::io::AsyncWrite`]; an [`Output::Discard`] keeps nothing. A range-capable resource is
/// scattered into a seekable scratch in parallel and verified in order; one that is not is streamed and
/// hashed inline. Every chunk's range is validated, a dropped chunk resumes from its offset, and the
/// total length is gated, so the returned digest certifies the resource.
pub(crate) async fn download<S: Source, P: Progress + ?Sized>(
    source: &S,
    mut output: Output<'_>,
    options: Options,
    progress: &P,
) -> Result<Report, Error> {
    let probe = source.probe().await?;
    // The scratch is a seekable `.xget`: when the output persists a file it sits beside that file so it
    // can be renamed into place, carrying the resume control in a trailer past the data until finalized;
    // otherwise (writers or a discard) it is a throwaway under the temp dir. Resume comes back to that
    // persistent `.xget`, so any output that keeps a file (a lone file, or a tee that includes one) may
    // resume; a writer-only or discard output has nothing to return to and forces no-resume. Delivery to
    // writers happens after the download completes, so a resumed run simply delivers the full stream once.
    let part = scratch_path(&output);
    let can_resume = first_file(&output).is_some();
    // A throwaway temp scratch (a writer/discard output) is removed on any early exit, an error or a
    // dropped/cancelled future, so an interrupted stream does not leave a partial in the temp dir. A
    // file's `.xget` is the resume artifact and is never guarded.
    let _scratch_guard = (!can_resume).then(|| ScratchGuard(PathBuf::clone(&part)));
    let options = Options {
        resume: options.resume && can_resume,
        ..options
    };

    tracing::debug!(
        length = probe.length,
        supports_ranges = probe.supports_ranges,
        content_type = probe.content_type.as_deref(),
        "probed resource"
    );

    // A sequential download uses the single-stream path even for a range source: one ordered connection,
    // no scatter, no scratch for a writer/discard sink. It trades parallel speed for zero holding space.
    let hash = if probe.supports_ranges && !options.sequential {
        let plan = resume_plan(&part, probe.length, probe.validator.as_deref(), &options).await?;
        tracing::debug!(
            chunks = plan.ranges.len(),
            already_done = plan
                .received
                .iter()
                .zip(&plan.ranges)
                .filter(|(received, range)| **received >= range.len())
                .count(),
            resumed = plan.resumed,
            "planned download"
        );
        // The control writer lives in the `.xget` file itself: a resume reopens its trailer to append
        // to, a fresh run allocates the sparse data region and writes an empty trailer.
        let writer = if plan.resumed {
            Writer::open(&part, probe.length).await?
        } else {
            allocate_fresh(&part, probe.length).await?;
            Writer::create(
                &part,
                probe.length,
                probe.validator.as_deref(),
                source.identity().as_deref(),
                options.checksum,
            )
            .await?
        };
        let writer = Rc::new(Mutex::new(writer));
        let hash = fetch_scatter(
            source,
            &part,
            probe.length,
            &plan,
            options,
            progress,
            &writer,
        )
        .await?;
        // Strip the control trailer and footer, leaving the scratch a byte-exact image `[0, total)`.
        writer.lock().await.finish().await?;
        // Deliver the verified scratch to the sink: rename it to a file, copy it out to a writer, both
        // for a tee, or drop it for a discard. (A writer re-reads the scratch for now; it could later
        // stream during verify.)
        distribute(&part, probe.length, output).await?;
        hash
    } else {
        // Only a genuine non-range source is unresumable for this reason. A range-capable source taken
        // down this path was forced sequential (`--sequential`, or the CLI auto-switch for a huge pipe); a
        // single stream simply cannot resume, so it restarts and the truncating create below discards any
        // leftover partial.
        if options.resume && !probe.supports_ranges && file_len(&part).await > 0 {
            return Err(Error::detail(
                "cannot resume: the source does not support byte ranges",
            ));
        }
        // The single stream writes straight to the sink: a file or a tee to the scratch (then
        // delivered), a lone writer live with no scratch, a discard nowhere.
        let hash =
            fetch_stream(source, &mut output, &part, probe.length, options, progress).await?;
        if matches!(output, Output::File(_) | Output::Tee { .. }) {
            distribute(&part, probe.length, output).await?;
        }
        hash
    };

    progress.finish();
    Ok(Report {
        length: probe.length,
        hash,
    })
}

/// The `.xget` scratch a download scatters into: beside the output for a [`Output::File`] (so it can be
/// renamed into place), or a unique throwaway under the temp dir for a [`Output::Writer`] or
/// [`Output::Discard`] (which have no persistent artifact and so are copied out or dropped after verify).
fn scratch_path(output: &Output<'_>) -> PathBuf {
    match first_file(output) {
        // A file (alone or the file half of a tee) becomes the output by rename, so its `.xget` beside
        // it is the scratch and doubles as the resume control.
        Some(path) => part_path(path),
        // No file to persist: scatter into a throwaway under the temp dir, copied out then dropped.
        None => std::env::temp_dir().join(format!(
            "xget-{}-{}.xget",
            std::process::id(),
            SCRATCH_COUNTER.fetch_add(1, Ordering::Relaxed)
        )),
    }
}

/// The `File` sink in `output`, if any: a lone file or the file half of a tee. Its presence is what lets
/// a run persist (and so resume); its `.xget` is the scratch.
fn first_file<'a>(output: &Output<'a>) -> Option<&'a Path> {
    match output {
        Output::File(path) => Some(path),
        Output::Tee { file, .. } => Some(file),
        Output::Writer(_) | Output::Discard => None,
    }
}

/// Deliver the finalized scratch to `output`: rename it to a file, copy it out to a writer, do both for a
/// tee, or drop it for a discard. Called once the scatter or stream has left the scratch a byte-exact
/// image of `[0, total)`. A tee copies to its writer first, then renames the scratch to its file, so an
/// interrupt leaves at most the one `.xget`.
async fn distribute(part: &Path, total: u64, output: Output<'_>) -> Result<(), Error> {
    match output {
        Output::File(path) => tokio::fs::rename(part, path)
            .await
            .map_err(Error::transport)?,
        Output::Writer(writer) => {
            copy_out(part, total, writer).await?;
            let _ = tokio::fs::remove_file(part).await;
        }
        Output::Tee { file, writer } => {
            copy_out(part, total, writer).await?;
            tokio::fs::rename(part, file)
                .await
                .map_err(Error::transport)?;
        }
        Output::Discard => {
            let _ = tokio::fs::remove_file(part).await;
        }
    }
    Ok(())
}

/// Copy the finalized scratch's `[0, total)` to `sink` and flush it. Used to stream a range download's
/// verified image out to a writer once the scatter and verify have certified it.
async fn copy_out(
    part: &Path,
    total: u64,
    sink: &mut (dyn AsyncWrite + Unpin),
) -> Result<(), Error> {
    let mut reader = File::open(part).await.map_err(Error::transport)?;
    let mut buffer = vec![0u8; 128 * 1024];
    let mut remaining = total;
    while remaining > 0 {
        let want = remaining.min(buffer.len() as u64) as usize;
        let read = reader
            .read(&mut buffer[..want])
            .await
            .map_err(Error::transport)?;
        if read == 0 {
            return Err(Error::LengthMismatch {
                expected: total,
                received: total - remaining,
            });
        }
        sink.write_all(&buffer[..read])
            .await
            .map_err(Error::transport)?;
        remaining -= read as u64;
    }
    sink.flush().await.map_err(Error::transport)
}

/// The sibling `.xget` path a download writes to before it is renamed into place: the output name with
/// `.xget` appended, so a multi-part extension like `.tar.gz` is preserved. The file holds the sparse
/// data and, until it is finalized, the resume control in a trailer past the data.
pub(crate) fn part_path(output: &Path) -> PathBuf {
    let mut name = output.as_os_str().to_owned();
    name.push(".xget");
    PathBuf::from(name)
}

/// How a scatter download is laid out: the chunks tiling `[0, total)`, how many bytes of each are
/// already on disk from a previous run, and whether this continues an existing `.xget`.
struct ResumePlan {
    /// The chunks to fetch, tiling `[0, total)` in order, the same as a fresh run would use.
    ranges: Vec<ByteRange>,
    /// How many bytes of each chunk are already on disk, as a prefix from the chunk's start. A chunk
    /// whose received equals its length is complete and skipped; otherwise it resumes from that offset.
    received: Vec<u64>,
    /// Whether this continues an existing `.xget` (so it must not be truncated).
    resumed: bool,
}

/// Work out how a download is laid out. Fresh, it is the whole resource in `options.parts` chunks.
/// Resuming, the `.xget` file's control trailer lists the byte ranges already written, and the plan is
/// rebuilt around them with `options.parts` chunks, so a resume may use a different parallelism than the
/// run that started it. A file with no valid control cannot be safely resumed (its holes are unknown),
/// so it is an error rather than a corrupt result.
async fn resume_plan(
    part: &Path,
    total: u64,
    validator: Option<&str>,
    options: &Options,
) -> Result<ResumePlan, Error> {
    let fresh = || {
        let ranges = plan_range(0, total, options.parts);
        let received = vec![0u64; ranges.len()];
        ResumePlan {
            ranges,
            received,
            resumed: false,
        }
    };
    if !options.resume {
        return Ok(fresh());
    }
    if let Some(control) = control::read(part).await {
        // We wrote this control, so the partial is ours to reason about. Resume it only if it is still
        // the same resource: same length, and a validator that either matches or is absent on one side
        // (nothing to compare, so fall back to the length alone). A changed resource is discarded, not
        // stitched: the fresh plan's `allocate_fresh` truncates the stale bytes away. We never splice
        // old and new bytes into one file.
        if control.total == total && validators_agree(control.validator.as_deref(), validator) {
            let (ranges, received) = plan_resume(total, options.parts, &control.done);
            return Ok(ResumePlan {
                ranges,
                received,
                resumed: true,
            });
        }
        tracing::info!(
            expected_len = control.total,
            actual_len = total,
            "the resource changed since the partial was written; restarting from scratch"
        );
        return Ok(fresh());
    }
    if file_len(part).await == 0 {
        Ok(fresh())
    } else {
        Err(Error::detail(
            "cannot resume: no saved state in the partial file (use --restart to start over)",
        ))
    }
}

/// Whether a stored validator and the resource's current validator are compatible for resume. Two
/// present validators must be equal; if either side is absent there is nothing to compare, so this
/// yields `true` and resume falls back to the length check that gates it. Absent-on-both is the
/// no-validator case (a source that offers no `ETag`/`Last-Modified`): weaker, but the best available.
fn validators_agree(stored: Option<&str>, current: Option<&str>) -> bool {
    match (stored, current) {
        (Some(stored), Some(current)) => stored == current,
        _ => true,
    }
}

/// The size of `part`, or zero if it does not exist.
async fn file_len(part: &Path) -> u64 {
    tokio::fs::metadata(part)
        .await
        .map(|meta| meta.len())
        .unwrap_or(0)
}

/// Create `part` and preallocate it to `total` bytes as a sparse file, so every chunk offset exists to
/// be written. Only for a fresh download: a resume keeps its existing file (data and control trailer)
/// untouched, since truncating here would strip the trailer.
async fn allocate_fresh(part: &Path, total: u64) -> Result<(), Error> {
    let file = File::create(part).await.map_err(Error::transport)?;
    file.set_len(total).await.map_err(Error::transport)?;
    Ok(())
}

/// Fetch `[start, total)` as parallel chunks scattered into `part`, verified continuously. The fetchers
/// write to their own offsets and report how far each has reached; the verifier reads the file back
/// from zero and hashes the contiguous, hole-free prefix as it grows, so the verified frontier follows
/// the earliest unfinished chunk rather than jumping a whole chunk at a time.
async fn fetch_scatter<S: Source, P: Progress + ?Sized>(
    source: &S,
    part: &Path,
    total: u64,
    plan: &ResumePlan,
    options: Options,
    progress: &P,
    writer: &Rc<Mutex<Writer>>,
) -> Result<Option<String>, Error> {
    let ranges = &plan.ranges;
    // How much of each chunk is already on disk: its resume prefix (zero for a fresh run).
    let received = &plan.received;
    let sizes: Vec<u64> = ranges.iter().map(ByteRange::len).collect();
    progress.start(&sizes);

    // Show the resume where it starts: the bytes already on disk open as downloaded-but-unverified, so
    // the bar picks up where the last run stopped and the verify pass sweeps the confirmed frontier
    // through them. Only meaningful when resuming; a fresh run has nothing on disk.
    if plan.resumed {
        progress.restore(received);
    }
    let shared = Rc::new(Shared {
        received: RefCell::new(Vec::clone(received)),
        notify: Notify::new(),
        failed: Cell::new(false),
    });

    // Structured concurrency on one task: the source's futures are not `Send`. The fetchers scatter
    // into the file and nudge the shared state while the verifier reads the contiguous prefix and hashes.
    let fetchers = ranges.iter().copied().enumerate().map(|(index, range)| {
        let chunk = Chunk {
            index,
            range,
            resume_from: range.start + received[index],
        };
        let complete = received[index] >= range.len();
        let scatter = Scatter {
            shared: Rc::clone(&shared),
            writer: Rc::clone(writer),
        };
        async move {
            if complete {
                Ok(())
            } else {
                scatter_one(source, part, chunk, options, progress, &scatter).await
            }
        }
    });
    // A fresh run fetches every chunk at once: parallelism is the whole point. A resume instead fills its
    // holes low-offset first, one at a time, so the verifier's contiguous prefix advances steadily from
    // zero and the hash keeps pace with the download instead of bursting once the last hole closes. The
    // resume tail is usually a fraction of the file, so the parallelism traded away is a fair price.
    let concurrency = if plan.resumed { 1 } else { ranges.len().max(1) };
    let drive = async {
        let mut fetchers = futures::stream::iter(fetchers).buffer_unordered(concurrency);
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
        verify(part, ranges, &shared, options.checksum, total, progress)
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

/// The shared handles one fetching chunk needs: the state it reports into and the control writer it
/// records completed and checkpointed ranges through. Each fetcher gets its own clones of the shared
/// `Rc`s, all pointing at the one download's state.
struct Scatter {
    shared: Rc<Shared>,
    writer: Rc<Mutex<Writer>>,
}

/// One chunk to fetch: its position in the plan, the byte range it covers, and the offset to resume from
/// (its start for a fresh chunk, past its on-disk prefix for a resumed one).
#[derive(Clone, Copy)]
struct Chunk {
    index: usize,
    range: ByteRange,
    resume_from: u64,
}

/// One chunk's write target: its own file handle, its range, the shared state it reports into, and the
/// control writer it checkpoints its flushed prefix to.
struct Sink<'a> {
    index: usize,
    range: ByteRange,
    file: File,
    shared: &'a Rc<Shared>,
    writer: &'a Rc<Mutex<Writer>>,
    /// The offset last written to the control trailer, so this chunk checkpoints only every so often.
    checkpointed: u64,
    /// When it last checkpointed, so a slow chunk still records on a time basis, not only by bytes.
    checkpoint_at: Instant,
}

impl Sink<'_> {
    /// Stream `[*offset, range.end)` from `source` into the file at that offset, advancing `offset`,
    /// recording each write into the shared received count, nudging the verifier, and checkpointing the
    /// flushed prefix to the control trailer every so often. On a mid-stream error `offset` reflects how
    /// far it got, so the caller can seek back and resume.
    async fn fill<S: Source, P: Progress + ?Sized>(
        &mut self,
        source: &S,
        offset: &mut u64,
        timeout: Option<Duration>,
        progress: &P,
    ) -> Result<(), Error> {
        self.file
            .seek(SeekFrom::Start(*offset))
            .await
            .map_err(Error::transport)?;
        let mut stream = source
            .fetch(Some(ByteRange {
                start: *offset,
                end: self.range.end,
            }))
            .await?;
        while let Some(chunk) = next_chunk(&mut stream, timeout).await? {
            let len = chunk.len() as u64;
            // `*offset <= self.range.end` holds here, so subtract rather than add to avoid an overflow on
            // a hostile source streaming near `u64::MAX`.
            if len > self.range.end - *offset {
                return Err(Error::detail(
                    "source sent more bytes than the requested range",
                ));
            }
            self.file
                .write_all(&chunk)
                .await
                .map_err(Error::transport)?;
            // Flush before recording, so the bytes are visible to the verifier's separate read handle
            // (a buffered write would otherwise let it read a stale hole and hash the wrong bytes).
            self.file.flush().await.map_err(Error::transport)?;
            *offset += len;
            self.shared.received.borrow_mut()[self.index] = *offset - self.range.start;
            self.shared.notify.notify_one();
            progress.received(self.index, len);
            // Persist this chunk's flushed prefix now and then, so a resume keeps partial progress and
            // does not refetch it: after enough new bytes, or after enough time while progressing, so a
            // small or slow chunk records too. The bytes are already flushed, so the range is on disk.
            let progressed = *offset - self.checkpointed;
            if progressed >= CHECKPOINT_BYTES
                || (progressed > 0 && self.checkpoint_at.elapsed() >= CHECKPOINT_INTERVAL)
            {
                self.writer
                    .lock()
                    .await
                    .append(ByteRange {
                        start: self.range.start,
                        end: *offset,
                    })
                    .await?;
                self.checkpointed = *offset;
                self.checkpoint_at = Instant::now();
            }
        }
        Ok(())
    }
}

/// Download `chunk`'s range into `part` at its offset, starting from `chunk.resume_from` (past any
/// on-disk prefix) and resuming from where it drops with backoff. Each chunk holds its own file handle,
/// so concurrent writes to different offsets never contend.
async fn scatter_one<S: Source, P: Progress + ?Sized>(
    source: &S,
    part: &Path,
    chunk: Chunk,
    options: Options,
    progress: &P,
    scatter: &Scatter,
) -> Result<(), Error> {
    let Chunk {
        index,
        range,
        resume_from,
    } = chunk;
    let file = OpenOptions::new()
        .write(true)
        .open(part)
        .await
        .map_err(Error::transport)?;
    let mut sink = Sink {
        index,
        range,
        file,
        shared: &scatter.shared,
        writer: &scatter.writer,
        checkpointed: resume_from,
        checkpoint_at: Instant::now(),
    };
    let mut offset = resume_from;
    let mut last_error = None;
    tracing::debug!(
        chunk = index,
        start = range.start,
        end = range.end,
        resume_from,
        "opening chunk"
    );
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
            Err(error) => {
                tracing::warn!(chunk = index, attempt, resume_from = offset, error = %error, "chunk failed, retrying");
                // Surface the retry to the reporter only when another attempt follows, resuming from the
                // bytes already in this chunk (`offset - range.start`), so a display can explain a stall.
                if attempt < options.retries {
                    progress.retry(
                        index,
                        attempt + 1,
                        options.retries,
                        offset - range.start,
                        &short_error(&error),
                    );
                }
                last_error = Some(error);
            }
        }
    }
    let received = offset - range.start;
    if received != range.len() {
        return Err(last_error.unwrap_or(Error::LengthMismatch {
            expected: range.len(),
            received,
        }));
    }
    tracing::debug!(chunk = index, "chunk complete");
    // Record the whole chunk's byte range as on disk, so a later run skips it however it re-chunks.
    scatter.writer.lock().await.append(range).await?;
    Ok(())
}

/// Read `part` back from offset zero and hash the contiguous, hole-free prefix as it grows: the resumed
/// prefix first, then bytes as the earliest unfinished chunk receives them. Reports `wrote` per chunk so
/// the verified frontier advances continuously, and gates the total length so the digest certifies the
/// whole file.
async fn verify<P: Progress + ?Sized>(
    part: &Path,
    ranges: &[ByteRange],
    shared: &Rc<Shared>,
    checksum: Checksum,
    total: u64,
    progress: &P,
) -> Result<Option<String>, Error> {
    let mut hasher = checksum.hasher();
    let mut reader = File::open(part).await.map_err(Error::transport)?;
    let mut buffer = vec![0u8; 128 * 1024];

    // The plan tiles the whole resource from zero, so verifying is a single in-order sweep of the
    // contiguous prefix: chunks already on disk report their bytes as it passes over them, just like
    // freshly fetched ones.
    let mut hashed = 0;

    loop {
        if shared.failed.get() {
            return Err(Error::detail("a chunk failed before it could be verified"));
        }
        let frontier = contiguous_run(&shared.received.borrow(), ranges);
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
        let read = reader
            .read(&mut buffer[..want])
            .await
            .map_err(Error::transport)?;
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
/// in-order pass (no scatter, no re-read: a single stream is already in order). The bytes go straight to
/// the sink: a file to its scratch (renamed after by the caller), a writer live as they arrive, a discard
/// nowhere. Either way the stream is hashed inline and gated on the declared length.
async fn fetch_stream<S: Source, P: Progress + ?Sized>(
    source: &S,
    output: &mut Output<'_>,
    part: &Path,
    total: u64,
    options: Options,
    progress: &P,
) -> Result<Option<String>, Error> {
    progress.start(&[total]);
    // A file or a tee streams to the scratch (delivered after); a lone writer goes live; a
    // discard writes nowhere.
    let mut file = match output {
        Output::File(_) | Output::Tee { .. } => {
            Some(File::create(part).await.map_err(Error::transport)?)
        }
        Output::Writer(_) | Output::Discard => None,
    };
    let mut hasher = options.checksum.hasher();
    let mut stream = source.fetch(None).await?;
    let mut written = 0u64;
    while let Some(chunk) = next_chunk(&mut stream, options.timeout).await? {
        let len = chunk.len() as u64;
        if let Some(hasher) = hasher.as_mut() {
            hasher.update(&chunk);
        }
        match output {
            Output::File(_) | Output::Tee { .. } => {
                if let Some(file) = file.as_mut() {
                    file.write_all(&chunk).await.map_err(Error::transport)?;
                }
            }
            Output::Writer(sink) => sink.write_all(&chunk).await.map_err(Error::transport)?,
            Output::Discard => {}
        }
        written += len;
        progress.received(0, len);
        progress.wrote(0, len);
    }
    match output {
        Output::File(_) | Output::Tee { .. } => {
            if let Some(file) = file.as_mut() {
                file.flush().await.map_err(Error::transport)?;
            }
        }
        Output::Writer(sink) => sink.flush().await.map_err(Error::transport)?,
        Output::Discard => {}
    }
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
            .map_err(|_| Error::detail("timed out waiting for data"))?,
        None => stream.next().await,
    };
    next.transpose()
}

/// Exponential backoff before a retry, capped at a few seconds.
fn backoff(attempt: u32) -> Duration {
    Duration::from_millis(100u64.saturating_mul(1u64 << attempt.min(6)))
}

/// The message of the deepest source of `error`, so a retry line shows the concrete cause (e.g.
/// "connection reset by peer") rather than the generic top-level wrapper ("fetch failed").
fn short_error(error: &Error) -> String {
    let mut current: &dyn core::error::Error = error;
    while let Some(next) = core::error::Error::source(current) {
        current = next;
    }
    current.to_string()
}

/// Removes a throwaway temp scratch when a download drops without finishing (an error, or a cancelled
/// future). A successful download has already delivered and removed the scratch, so the extra remove is
/// a harmless no-op. Only ever guards a writer/discard scratch, never a file's `.xget` (the resume
/// artifact), so a partial file download stays resumable.
struct ScratchGuard(PathBuf);

impl Drop for ScratchGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Remove throwaway scratch files (`xget-<pid>-<n>.xget`) left in the temp dir by earlier runs whose
/// process is gone: best-effort startup housekeeping so interrupted writer/discard downloads (a Ctrl-C,
/// a kill, a crash: cases the in-process guard cannot catch) do not pile up. A live download's scratch,
/// including this process's own, is never touched. A no-op off unix, where the liveness check is
/// unavailable.
pub async fn sweep_orphans() {
    let Ok(mut entries) = tokio::fs::read_dir(std::env::temp_dir()).await else {
        return;
    };
    let me = std::process::id();
    while let Ok(Some(entry)) = entries.next_entry().await {
        let Some(pid) = entry.file_name().to_str().and_then(orphan_pid) else {
            continue;
        };
        if pid == me || pid_alive(pid) {
            continue;
        }
        let _ = tokio::fs::remove_file(entry.path()).await;
    }
}

/// The pid embedded in a throwaway scratch name `xget-<pid>-<n>.xget`, if the name matches exactly (both
/// fields numeric), so a user file that happens to sit in the temp dir is never matched.
fn orphan_pid(name: &str) -> Option<u32> {
    let (pid, counter) = name
        .strip_prefix("xget-")?
        .strip_suffix(".xget")?
        .split_once('-')?;
    if counter.parse::<u64>().is_err() {
        return None;
    }
    pid.parse().ok()
}

/// Whether a process is still running: unix `kill(pid, 0)` is alive if it succeeds, or fails with EPERM
/// (exists but owned by another user). ESRCH means gone.
#[cfg(unix)]
fn pid_alive(pid: u32) -> bool {
    let Ok(pid) = i32::try_from(pid) else {
        return true;
    };
    // SAFETY: `kill` with signal 0 runs the existence/permission check without delivering a signal.
    let checked = unsafe { libc::kill(pid, 0) };
    checked == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

/// Whether a process is still running on Windows: `OpenProcess` succeeds for a live process; a failure
/// with `ERROR_ACCESS_DENIED` means it exists but is not queryable (alive), while a missing pid fails
/// with `ERROR_INVALID_PARAMETER` (gone).
#[cfg(windows)]
fn pid_alive(pid: u32) -> bool {
    use windows_sys::Win32::Foundation::{CloseHandle, ERROR_ACCESS_DENIED, GetLastError};
    use windows_sys::Win32::System::Threading::{OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION};

    // SAFETY: OpenProcess returns a valid handle for a running process or null on failure; we only test
    // the handle and close it if it is valid.
    let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if !handle.is_null() {
        // SAFETY: closing a handle we just successfully opened.
        unsafe { CloseHandle(handle) };
        return true;
    }
    // SAFETY: reading the thread's last error immediately after the failed OpenProcess.
    unsafe { GetLastError() == ERROR_ACCESS_DENIED }
}

/// On any other platform there is no portable liveness check, so never sweep (never a wrongful delete).
#[cfg(not(any(unix, windows)))]
fn pid_alive(_pid: u32) -> bool {
    true
}
