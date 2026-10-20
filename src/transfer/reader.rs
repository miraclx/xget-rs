//! Borrowed, repeatable range access into the checked native stage.
//!
//! A reader is implemented for the particular borrow lifetime, not with a higher-ranked bound on
//! every possible lifetime. This lets bodies, opening futures and errors borrow application data.
//! No task is spawned for source work. Only the native backend submits bounded physical operations.
//!
//! The following complete program takes an absent output path as its first argument. Its parent must
//! already exist, be caller-owned and have no group/other write permission. Native publication requires
//! Unix hardlink support; see [`super::Transfer`] for the filesystem and process-interruption limits.
//! The runtime is caller-owned. This example is compile-checked; supply your own private destination
//! when running it as a program.
//!
//! ```no_run
//! use core::convert::Infallible;
//! use core::future::{Ready, ready};
//! use std::path::PathBuf;
//! use bytes::Bytes;
//! use futures::stream::{self, Iter};
//! use xget::Transfer;
//! use xget::transfer::{ByteCount, Measurement};
//! use xget::transfer::reader::{Evidence, RangeReader, ReadResponse, Window};
//!
//! struct Memory<'data>(&'data [u8]);
//! impl<'read, 'data: 'read> RangeReader<'read> for Memory<'data> {
//!     type Error = Infallible;
//!     type Body = Iter<core::array::IntoIter<Result<Bytes, Infallible>, 1>>;
//!     type Read = Ready<Result<ReadResponse<'read, Self::Body>, Self::Error>>;
//!     fn size(&self) -> ByteCount { ByteCount(self.0.len() as u64) }
//!     fn evidence(&'read self) -> Evidence<'read> {
//!         Evidence::Enforced { representation: "memory bytes", version: b"immutable borrow" }
//!     }
//!     fn read(&'read self, window: Window) -> Self::Read {
//!         let bytes = Bytes::copy_from_slice(&self.0[window.start().0 as usize..window.end().0 as usize]);
//!         ready(Ok(ReadResponse { window, evidence: self.evidence(),
//!             body: stream::iter([Ok(bytes)]) }))
//!     }
//! }
//! #[tokio::main(flavor = "current_thread")]
//! async fn main() -> Result<(), Box<dyn core::error::Error>> {
//!     let destination = std::env::args_os().nth(1).map(PathBuf::from).ok_or_else(|| {
//!         std::io::Error::new(std::io::ErrorKind::InvalidInput,
//!             "supply an absent output path in an existing caller-owned parent")
//!     })?;
//!     let data = vec![1, 2, 3];
//!     let reader = Memory(&data);
//!     let receipt = Transfer::from_reader(&reader)
//!         .measurement(Measurement::None).to_file(destination)?.run().await?;
//!     assert_eq!(receipt.native().partial().written, ByteCount(3));
//!     Ok(())
//! }
//! ```
//!
//! # Generic Send helpers
//!
//! Name one borrow's future/body/error families explicitly when returning a generic Send future.
//! Keeping them as named parameters avoids generalizing associated projections to unrelated lifetimes.
//! These bounds apply only to this helper, not to local reader execution:
//!
//! ```
//! use core::future::Future;
//! use std::path::PathBuf;
//! use bytes::Bytes;
//! use futures::Stream;
//! use xget::Transfer;
//! use xget::transfer::Failure;
//! use xget::transfer::reader::{RangeReader, ReadResponse, ReaderError, ReaderReceipt, ReaderRequestError};
//!
//! fn download_send<'read, R, F, B, E>(reader: &'read R, path: PathBuf)
//!     -> Result<impl Future<Output = Result<ReaderReceipt, Box<Failure<ReaderError<E>>>>> + Send + 'read, ReaderRequestError>
//! where
//!     R: RangeReader<'read, Read = F, Body = B, Error = E> + Sync,
//!     F: Future<Output = Result<ReadResponse<'read, B>, E>> + Send + 'read,
//!     B: Stream<Item = Result<Bytes, E>> + Send + 'read,
//!     E: Send + 'read,
//! {
//!     Ok(Transfer::from_reader(reader).to_file(path)?.run())
//! }
//! ```
//!
//! No higher-ranked or static bound is required of application data. A reader containing Rc remains
//! usable locally but cannot satisfy this helper's Send promise.

//! Local readers are accepted by `run`, but are intentionally not made Send by the engine:
//!
//! ```compile_fail
//! use core::convert::Infallible;
//! use core::future::{Ready, ready};
//! use std::rc::Rc;
//! use bytes::Bytes;
//! use futures::stream::{self, Iter};
//! use xget::Transfer;
//! use xget::transfer::ByteCount;
//! use xget::transfer::reader::{Evidence, RangeReader, ReadResponse, Window};
//! struct Local(Rc<Vec<u8>>);
//! impl<'read> RangeReader<'read> for Local {
//!     type Error = Infallible;
//!     type Body = Iter<core::array::IntoIter<Result<Bytes, Infallible>, 1>>;
//!     type Read = Ready<Result<ReadResponse<'read, Self::Body>, Infallible>>;
//!     fn size(&self) -> ByteCount { ByteCount(self.0.len() as u64) }
//!     fn evidence(&'read self) -> Evidence<'read> {
//!         Evidence::Enforced { representation: "bytes", version: b"immutable" }
//!     }
//!     fn read(&'read self, window: Window) -> Self::Read {
//!         let bytes = Bytes::copy_from_slice(&self.0[window.start().0 as usize..window.end().0 as usize]);
//!         ready(Ok(ReadResponse { window, evidence: self.evidence(), body: stream::iter([Ok(bytes)]) }))
//!     }
//! }
//! fn needs_send<T: Send>(_: T) {}
//! let reader = Local(Rc::new(vec![1]));
//! let pending = Transfer::from_reader(&reader).to_file("output").unwrap();
//! needs_send(pending.run()); // Rc is not Sync, so borrowing Local cannot be sent.
//! ```

use core::future::Future;
use core::pin::pin;
use core::sync::atomic::{AtomicBool, Ordering};
use std::path::Path;

use bytes::Bytes;
use futures::stream::FuturesUnordered;
use futures::{Stream, StreamExt as _};
use tokio::sync::Notify;

use super::native::Native;
use super::plan::Plan;
use super::{
    Algorithm, ByteCount, Conflict, Digest, Failure, FailureKind, Measurement, Partial, Provenance,
    Receipt, RequestError, StateRef, WriteExtent,
};
use crate::checks::{CheckPolicy, Requirements};

/// A validated nonempty half-open window within a reader's known size.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Window {
    start: ByteCount,
    end: ByteCount,
}

impl Window {
    /// Reject empty, inverted and out-of-object windows before calling a source.
    pub fn new(start: ByteCount, end: ByteCount, size: ByteCount) -> Option<Self> {
        (start < end && end <= size).then_some(Self { start, end })
    }

    /// Inclusive first byte offset.
    pub fn start(self) -> ByteCount {
        self.start
    }

    /// Exclusive final byte offset.
    pub fn end(self) -> ByteCount {
        self.end
    }
}

/// The exact opened representation and the source's session consistency contract.
/// Tokens must be safe application metadata, not credentials or renewable access URLs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Evidence<'read> {
    /// Every read is conditional on this version of these representation bytes. The implementer
    /// must fail a read if it cannot enforce this pin, including changes detected during the body.
    Enforced {
        /// Describes the byte representation, including decoding/transformation policy.
        representation: &'read str,
        /// An opaque source version, compared byte-for-byte, not interpreted as a checksum.
        version: &'read [u8],
    },
    /// No enforceable version. Assembly requires a strong whole-payload expected digest.
    Candidate {
        /// Describes the candidate representation to which the requirement applies.
        representation: &'read str,
    },
}

/// One borrow lifetime's concrete opening future, body and error family.
///
/// `size` and `evidence` describe one session and must remain stable for the borrow. `read` must
/// return exactly the requested window and session evidence. Each body must reach actual clean EOF;
/// returning all requested bytes followed by an error is a failure, not a successful range.
/// Bodies may emit arbitrary chunk boundaries. The engine retains at most one source chunk per
/// active range and submits at most 64 KiB per physical write. Source-internal buffering remains
/// the implementer's responsibility. The reader is borrowed and may be called concurrently.
pub trait RangeReader<'read>: 'read {
    /// Concrete caller-owned error, permitted to borrow and to be non-Send/non-Sync.
    type Error: 'read;
    /// Concrete body, with no Unpin, Send or static requirement.
    type Body: Stream<Item = Result<Bytes, Self::Error>> + 'read;
    /// Concrete, nameable opening future for this borrow lifetime.
    type Read: Future<Output = Result<ReadResponse<'read, Self::Body>, Self::Error>> + 'read;

    /// Known total representation size, not a content or version guarantee.
    fn size(&self) -> ByteCount;
    /// The consistency contract the implementer enforces for this session.
    fn evidence(&'read self) -> Evidence<'read>;
    /// Open repeatable access to a validated window. Never consume a hidden take-once body here.
    fn read(&'read self, window: Window) -> Self::Read;
}

/// Opened range metadata is checked before its body is polled.
pub struct ReadResponse<'read, B> {
    /// Actual response window. It must equal the request, not merely share its start or length.
    pub window: Window,
    /// Actual enforced session evidence. Any known disagreement ends the transfer.
    pub evidence: Evidence<'read>,
    /// Body of precisely this window, including terminal errors after its last data chunk.
    pub body: B,
}

/// A borrowed reader plus check policy, without filesystem effects or source opening.
#[must_use]
pub struct ReaderTransfer<'read, R> {
    reader: &'read R,
    requirements: Result<Requirements, Box<Conflict>>,
    measurement: Measurement,
}

impl<'read, R: RangeReader<'read>> ReaderTransfer<'read, R> {
    pub(super) fn from_reader(reader: &'read R) -> Self {
        Self {
            reader,
            requirements: Ok(Requirements::default()),
            measurement: Measurement::default(),
        }
    }

    /// Require a whole-payload digest. Measurement alone never authorizes candidate assembly.
    pub fn expect(self, digest: impl Into<Digest>) -> Self {
        self.require(digest.into(), Provenance::Caller)
    }

    /// Add an agreeing requirement with bounded provenance; a conflict stays sticky.
    pub fn require(mut self, digest: Digest, provenance: Provenance) -> Self {
        self.requirements = self.requirements.and_then(|mut requirements| {
            requirements.insert(digest, provenance)?;
            Ok(requirements)
        });
        self
    }

    /// Select reporting hash work independently of required checks and version enforcement.
    pub fn measurement(mut self, measurement: Measurement) -> Self {
        self.measurement = measurement;
        self
    }

    /// Validate the session policy and destination without opening ranges or acquiring native leases.
    /// Candidate sessions require SHA-256, SHA-512 or BLAKE3 as an expectation, not a measurement.
    pub fn to_file(
        self,
        destination: impl AsRef<Path>,
    ) -> Result<PendingReader<'read, R>, ReaderRequestError> {
        let requirements = self
            .requirements
            .map_err(RequestError::ConflictingRequirements)?;
        let evidence = self.reader.evidence();
        if matches!(evidence, Evidence::Candidate { .. })
            && !requirements.iter().any(|requirement| {
                matches!(
                    requirement.digest.algorithm(),
                    Algorithm::Sha256 | Algorithm::Sha512 | Algorithm::Blake3
                )
            })
        {
            return Err(ReaderRequestError::Unpinned);
        }
        let size = self.reader.size();
        let plan = Plan::new(
            destination.as_ref(),
            CheckPolicy {
                requirements,
                measurement: self.measurement,
            },
        )?;
        Ok(PendingReader {
            reader: self.reader,
            size,
            evidence,
            plan,
        })
    }
}

/// Policy rejection occurs before body opening and native effects.
#[derive(Debug, thiserror::Error)]
pub enum ReaderRequestError {
    /// Existing destination and required-check validation vocabulary.
    #[error(transparent)]
    Request(#[from] RequestError),
    /// Size and optional or weak hashes cannot establish consistent range assembly.
    #[error(
        "range session is unpinned; supply a strong whole-file expectation or enforce a source version"
    )]
    Unpinned,
}

/// Reader protocol failures remain distinct from the original concrete caller cause.
#[derive(Debug, thiserror::Error)]
pub enum ReaderError<E> {
    /// Error from opening or polling the source, including late errors.
    #[error("range source failed")]
    Source(#[source] E),
    /// Response did not cover the entire requested window.
    #[error("range response window differs from the request; open a fresh session")]
    Window,
    /// A known representation or version mismatch always terminates this session.
    #[error("range session changed; open a fresh session")]
    SessionChanged,
}

/// What made range assembly admissible, separate from measured or matched digest results.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Consistency {
    /// Every response matched the reader's enforced version contract.
    EnforcedVersion,
    /// An unversioned candidate passed at least one strong required whole-payload check.
    ExpectedContent,
}

/// Successful range assembly plus the shared native checked-publication receipt.
#[derive(Debug)]
pub struct ReaderReceipt {
    native: Receipt,
    consistency: Consistency,
}

impl ReaderReceipt {
    /// Shared native counts, checks and retained publication evidence.
    pub fn native(&self) -> &Receipt {
        &self.native
    }
    /// Session consistency, not authenticity or a measured-hash claim.
    pub fn consistency(&self) -> Consistency {
        self.consistency
    }
}

/// Frozen session and native plan. This borrows the reader through all body completion and I/O.
#[must_use]
pub struct PendingReader<'read, R> {
    reader: &'read R,
    size: ByteCount,
    evidence: Evidence<'read>,
    plan: Plan,
}

impl<'read, R: RangeReader<'read>> PendingReader<'read, R> {
    /// Advisory retained-state location; possession is not ownership or reopening authority.
    pub fn state(&self) -> &StateRef {
        self.plan.state()
    }

    /// Run up to four concurrent ranges on the calling task. No implicit source spawn or runtime,
    /// retry, reopening, overwrite, cleanup or power-loss durability is promised.
    /// On failure, source admission stops and already-submitted physical writes settle before the
    /// partial checkpoint. Dropping the future instead leaves registrations owning their leases
    /// until queued/running native operations finish.
    ///
    /// Drop is not rollback: a submitted publication can still create the destination without returning
    /// a receipt. Retain a clone of [`Self::state`] before starting if the caller may drop execution,
    /// then investigate with [`StateRef::reconcile`] rather than blindly restarting. Reconciliation
    /// can report an unknown or not-revalidatable outcome; it does not reopen this reader session.
    pub fn run<F, B, E>(
        self,
    ) -> impl Future<Output = Result<ReaderReceipt, Box<Failure<ReaderError<E>>>>> + 'read
    where
        R: RangeReader<'read, Read = F, Body = B, Error = E>,
        F: Future<Output = Result<ReadResponse<'read, B>, E>> + 'read,
        B: Stream<Item = Result<Bytes, E>> + 'read,
        E: 'read,
    {
        execute::<R, F, B, E>(self)
    }
}

// Name the three family members once at the public boundary. Carrying projections through nested
// opaque async futures can make Send checking quantify over other source lifetimes, requiring static
// despite the caller having proved the members for this borrow. These equalities select one family.
async fn execute<'read, R, F, B, E>(
    pending: PendingReader<'read, R>,
) -> Result<ReaderReceipt, Box<Failure<ReaderError<E>>>>
where
    R: RangeReader<'read, Read = F, Body = B, Error = E>,
    F: Future<Output = Result<ReadResponse<'read, B>, E>> + 'read,
    B: Stream<Item = Result<Bytes, E>> + 'read,
    E: 'read,
{
    let PendingReader {
        reader,
        size,
        evidence,
        plan,
    } = pending;
    let state = StateRef::clone(plan.state());
    // Unsupported hosts reject here without calling read or polling a body.
    let native = Native::acquire(plan)
        .await
        .map_err(|failure| Failure::native(failure.error, Partial::default(), failure.state))?;
    let mut partial = Partial::default();
    let result = receive::<R, F, B, E>(reader, size, evidence, &native, &mut partial).await;
    let result = match result {
        Ok(checks) => match native.seal(size).await {
            Ok(()) => match checks.finish() {
                Ok(results) => {
                    partial.set_checks(results);
                    Ok(results)
                }
                Err(mismatch) => {
                    partial.set_checks(mismatch.results);
                    Err(FailureKind::DigestMismatch(mismatch))
                }
            },
            Err(error) => Err(error.into_kind()),
        },
        Err(kind) => Err(kind),
    };
    let results = match result {
        Ok(results) => results,
        Err(mut kind) => {
            if let Err(error) = native.checkpoint(partial).await {
                kind = FailureKind::CheckpointFailed {
                    cause: Box::new(kind),
                    checkpoint: Box::new(error.into_kind()),
                };
            }
            return Err(Box::new(Failure {
                kind,
                partial,
                state: Some(state),
            }));
        }
    };
    native
        .publish(partial)
        .await
        .map_err(|error| Failure::native(error, partial, Some(StateRef::clone(&state))))?;
    Ok(ReaderReceipt {
        native: Receipt::completed(partial, results, state),
        consistency: match evidence {
            Evidence::Enforced { .. } => Consistency::EnforcedVersion,
            Evidence::Candidate { .. } => Consistency::ExpectedContent,
        },
    })
}

async fn receive<'read, R, F, B, E>(
    reader: &'read R,
    size: ByteCount,
    evidence: Evidence<'read>,
    native: &Native,
    partial: &mut Partial,
) -> Result<crate::checks::Checks, FailureKind<ReaderError<E>>>
where
    R: RangeReader<'read, Read = F, Body = B, Error = E>,
    F: Future<Output = Result<ReadResponse<'read, B>, E>> + 'read,
    B: Stream<Item = Result<Bytes, E>> + 'read,
    E: 'read,
{
    let cancellation = Cancellation::default();
    let ranges = crate::plan::plan(size.0, 4);
    let mut settled: Vec<u64> = ranges.iter().map(|range| range.start).collect();
    let mut work = FuturesUnordered::new();
    for (index, range) in ranges.iter().enumerate() {
        let window = Window {
            start: ByteCount(range.start),
            end: ByteCount(range.end),
        };
        work.push(advance::<R, F, B, E>(
            Task {
                reader,
                window,
                evidence,
                index,
                offset: window.start.0,
                body: None,
                chunk: Bytes::new(),
            },
            native,
            &cancellation,
        ));
    }
    let mut checks = native.policy().start();
    let mut frontier = 0;
    let mut failure = None;
    while let Some(step) = work.next().await {
        let Step {
            task,
            received,
            accepted,
            extent,
            error,
            complete,
        } = step;
        partial.received.0 = match partial.received.0.checked_add(received) {
            Some(count) => count,
            None => {
                failure.get_or_insert(FailureKind::LengthOverflow);
                cancellation.cancel();
                partial.received.0
            }
        };
        partial.written.0 += accepted;
        if extent == WriteExtent::Unconfirmed {
            partial.write_extent = extent;
        }
        settled[task.index] = task.offset;
        if let Some(error) = error {
            failure.get_or_insert(error);
            cancellation.cancel();
        }
        if failure.is_none() {
            // Only settled prefixes are visible. Tail writes never let the verifier jump a hole.
            let mut available = 0;
            for (range, end) in ranges.iter().zip(&settled) {
                if range.start > available {
                    break;
                }
                available = *end;
                if *end != range.end {
                    break;
                }
            }
            if checks.active() {
                while frontier < available {
                    let end = available.min(frontier.saturating_add(64 * 1024));
                    match native
                        .read_prefix(ByteCount(frontier), ByteCount(end))
                        .await
                    {
                        Ok(bytes) => {
                            checks.update(&bytes);
                            frontier = end;
                            partial.hashed = ByteCount(frontier);
                            #[cfg(test)]
                            if let Some(notify) = FRONTIER_TEST
                                .with(|hook| hook.borrow().as_ref().map(std::sync::Arc::clone))
                            {
                                notify.notify_one();
                            }
                        }
                        Err(error) => {
                            failure = Some(error.into_kind());
                            cancellation.cancel();
                            break;
                        }
                    }
                }
            }
        }
        if !complete && failure.is_none() {
            work.push(advance::<R, F, B, E>(task, native, &cancellation));
            if received == 0 && accepted == 0 {
                // Empty ready chunks bypass native I/O, so they must not form an unbounded ready loop.
                tokio::task::yield_now().await;
            }
        }
        // Remaining advance futures are drained after cancellation. Their read/body waits are
        // cancellable, but a write once submitted is not: its accepted prefix must be accounted.
    }
    if let Some(error) = failure {
        return Err(error);
    }
    if partial.received != size || partial.written != size {
        return Err(FailureKind::LengthMismatch);
    }
    Ok(checks)
}

#[derive(Default)]
struct Cancellation {
    // Shared between concurrently-polled source steps. Notify wakes read waits; the atomic makes
    // cancellation sticky and closes registration races without imposing Send on source values.
    cancelled: AtomicBool,
    wake: Notify,
}

impl Cancellation {
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.wake.notify_waiters();
    }

    async fn wait(&self) {
        let notified = self.wake.notified();
        let mut notified = pin!(notified);
        notified.as_mut().enable();
        if self.cancelled.load(Ordering::Acquire) {
            return;
        }
        notified.await;
    }
}

struct Task<'read, R, B> {
    reader: &'read R,
    window: Window,
    evidence: Evidence<'read>,
    index: usize,
    offset: u64,
    body: Option<core::pin::Pin<Box<B>>>,
    chunk: Bytes,
}

struct Step<'read, R, B, E> {
    task: Task<'read, R, B>,
    received: u64,
    accepted: u64,
    extent: WriteExtent,
    error: Option<FailureKind<ReaderError<E>>>,
    complete: bool,
}

async fn advance<'read, R, F, B, E>(
    mut task: Task<'read, R, B>,
    native: &Native,
    cancellation: &Cancellation,
) -> Step<'read, R, B, E>
where
    R: RangeReader<'read, Read = F, Body = B, Error = E>,
    F: Future<Output = Result<ReadResponse<'read, B>, E>> + 'read,
    B: Stream<Item = Result<Bytes, E>> + 'read,
    E: 'read,
{
    let mut received = 0;
    let mut accepted = 0;
    let mut extent = WriteExtent::Confirmed;
    let result = async {
        // A queued slot is not source admission. Do not even construct an opening future after
        // cancellation: a synchronous read method may itself acquire a resource or submit work.
        if cancellation.cancelled.load(Ordering::Acquire) {
            return Ok(true);
        }
        if task.body.is_none() {
            let reader: &'read R = task.reader;
            let opening: F = <R as RangeReader<'read>>::read(reader, task.window);
            let response: ReadResponse<'read, B> = tokio::select! {
                biased;
                () = cancellation.wait() => return Ok(true),
                response = opening => response.map_err(|error| FailureKind::Body(ReaderError::Source(error)))?,
            };
            if response.evidence != task.evidence {
                return Err(FailureKind::Body(ReaderError::SessionChanged));
            }
            if response.window != task.window {
                return Err(FailureKind::Body(ReaderError::Window));
            }
            task.body = Some(Box::pin(response.body));
        }
        if task.chunk.is_empty() {
            let Some(body) = task.body.as_mut() else { return Err(FailureKind::InvalidState); };
            let next = tokio::select! {
                biased;
                () = cancellation.wait() => return Ok(true),
                next = body.next() => next,
            };
            match next {
                None if task.offset == task.window.end.0 => return Ok(true),
                None => return Err(FailureKind::LengthMismatch),
                Some(Err(error)) => return Err(FailureKind::Body(ReaderError::Source(error))),
                Some(Ok(bytes)) => {
                    received = bytes.len() as u64;
                    if received > task.window.end.0 - task.offset {
                        return Err(FailureKind::LengthMismatch);
                    }
                    task.chunk = bytes;
                }
            }
        }
        if cancellation.cancelled.load(Ordering::Acquire) { return Ok(true); }
        if !task.chunk.is_empty() {
            let bytes = task.chunk.split_to(task.chunk.len().min(64 * 1024));
            match native.write(ByteCount(task.offset), bytes).await {
                Ok(count) => { accepted = count.0; task.offset += count.0; }
                Err(error) => {
                    match error.accepted {
                        Some(count) => { accepted = count.0; task.offset += count.0; }
                        None => extent = WriteExtent::Unconfirmed,
                    }
                    return Err(error.error.into_kind());
                }
            }
        }
        Ok(false)
    }.await;
    let (complete, error) = match result {
        Ok(complete) => (complete, None),
        Err(error) => (true, Some(error)),
    };
    Step {
        task,
        received,
        accepted,
        extent,
        error,
        complete,
    }
}

#[cfg(test)]
std::thread_local! {
    // Deterministic current-thread regression observation, never part of the public observer API.
    static FRONTIER_TEST: core::cell::RefCell<Option<std::sync::Arc<Notify>>> = const { core::cell::RefCell::new(None) };
}

#[cfg(test)]
#[path = "reader_tests.rs"]
mod tests;
