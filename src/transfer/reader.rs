//! Borrowed, repeatable range access into the checked native stage.
//!
//! A reader is implemented for the particular borrow lifetime, not with a higher-ranked bound on
//! every possible lifetime. This lets bodies, opening futures and errors borrow application data.
//! No task is spawned for source work. Only the native backend submits bounded physical operations.
//!
//! The following complete program takes an absent output path as its first argument and optionally
//! a retained reader state path as its second. It prints the state before execution. Interrupt a
//! sufficiently large fresh run, then supply that printed state with the same output to continue.
//! Changed session metadata, corrupt prefixes or sealed states produce an intended typed refusal,
//! not a fresh attempt. Its parent must already exist, be caller-owned and have no group/other write
//! permission. Native publication requires
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
//! use xget::transfer::{ByteCount, StateRef};
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
//!     let data = vec![1; 16 * 1024 * 1024];
//!     let reader = Memory(&data);
//!     let request = Transfer::from_reader(&reader); // default SHA-256 work supplies prefix anchors
//!     let request = match std::env::args_os().nth(2) {
//!         Some(path) => request.resume_from(StateRef::from(PathBuf::from(path))),
//!         None => request,
//!     };
//!     let pending = request.to_file(destination)?;
//!     eprintln!("reader state: {}", pending.state().path().display());
//!     let receipt = pending.run().await?;
//!     assert_eq!(receipt.native().partial().written, ByteCount(data.len() as u64));
//!     eprintln!("current attempt: {:?}", receipt.stats());
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
use super::observation::{Observation, Observer};
use super::plan::Plan;
use super::{
    Algorithm, ByteCount, Conflict, Digest, Failure, FailureKind, Measurement, Partial, Provenance,
    ReaderSession as Session, Receipt, RequestError, StateRef, WriteExtent,
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
    resume: Option<StateRef>,
}

impl<'read, R: RangeReader<'read>> ReaderTransfer<'read, R> {
    pub(super) fn from_reader(reader: &'read R) -> Self {
        Self {
            reader,
            requirements: Ok(Requirements::default()),
            measurement: Measurement::default(),
            resume: None,
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

    /// Continue an owned receiving reader checkpoint using this caller-supplied repeatable access.
    /// The destination, checks, size, representation and version must still agree. Only a recorded
    /// strong proper-prefix anchor authorizes reuse; old one-shot or sealed states are refused.
    /// No reader is reopened or renewed on the caller's behalf.
    pub fn resume_from(mut self, state: StateRef) -> Self {
        self.resume = Some(state);
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
        let (representation, version) = match evidence {
            Evidence::Enforced {
                representation,
                version,
            } => (representation, Some(version)),
            Evidence::Candidate { representation } => (representation, None),
        };
        if representation.is_empty()
            || representation.len() > 256
            || version.is_some_and(|version| version.is_empty() || version.len() > 256)
        {
            return Err(ReaderRequestError::SessionMetadata);
        }
        if self.resume.is_some()
            && (CheckPolicy {
                requirements,
                measurement: self.measurement,
            })
            .prefix_algorithm()
            .is_none()
        {
            return Err(ReaderRequestError::NoResumeAnchor);
        }
        let session = Session {
            size,
            representation: representation.to_owned(),
            version: version.map(<[u8]>::to_vec),
        };
        let mut plan = Plan::new(
            destination.as_ref(),
            CheckPolicy {
                requirements,
                measurement: self.measurement,
            },
        )?;
        if let Some(state) = &self.resume {
            plan.state = StateRef::clone(state);
        }
        Ok(PendingReader {
            reader: self.reader,
            size,
            evidence,
            plan,
            session,
            resume: self.resume,
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
    /// Persisted safe metadata is deliberately bounded, not a capability URL store.
    #[error(
        "range session metadata is empty or exceeds 256 bytes; supply bounded safe pin metadata"
    )]
    SessionMetadata,
    /// No hidden recovery-only hash is introduced for disabled or weak-only checks.
    #[error("reader continuation needs an active strong check; start a fresh attempt instead")]
    NoResumeAnchor,
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
    stats: ReaderStats,
}

/// Current attempt's physical work, separate from the native receipt's logical assembled counts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReaderStats {
    /// Previously acknowledged bytes rehashed against the recorded strong prefix anchor.
    pub restored: ByteCount,
    /// Bytes admitted from this attempt's source bodies, never including restored bytes.
    pub source: ByteCount,
    /// Bytes acknowledged by this attempt's physical payload writes.
    pub written: ByteCount,
}

impl ReaderReceipt {
    /// Shared native counts, checks and retained publication evidence.
    pub fn native(&self) -> &Receipt {
        &self.native
    }
    /// Current attempt's restored, source and physical-write counts. The native receipt counts the
    /// logical assembled object, including the prefix's earlier acknowledged receive/write work.
    pub fn stats(&self) -> ReaderStats {
        self.stats
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
    session: Session,
    resume: Option<StateRef>,
}

impl<'read, R: RangeReader<'read>> PendingReader<'read, R> {
    pub(crate) fn protect_targets(mut self, targets: std::sync::Arc<[std::path::PathBuf]>) -> Self {
        self.plan.targets = targets;
        self
    }

    pub(crate) fn authorized(
        mut self,
        publication: super::Publication,
        restart: Option<StateRef>,
    ) -> Self {
        self.plan.publication = publication;
        self.plan.restart = restart;
        self
    }

    /// Advisory retained-state location; possession is not ownership or reopening authority.
    pub fn state(&self) -> &StateRef {
        self.plan.state()
    }

    /// Run up to four concurrent ranges on the calling task. No implicit source spawn or runtime,
    /// retry, reopening, overwrite, cleanup or power-loss durability is promised.
    /// On failure, source admission stops and already-submitted physical writes settle before the
    /// partial checkpoint. Reader failure partials count logical assembled bytes, including prior
    /// acknowledged receive/write work restored from a prefix, not current-attempt network traffic.
    /// Dropping the future instead leaves registrations owning their leases until queued/running
    /// native operations finish.
    ///
    /// Drop is not rollback: a submitted publication can still create the destination without returning
    /// a receipt. Retain a clone of [`Self::state`] before starting if the caller may drop execution,
    /// then investigate publication with [`StateRef::reconcile`] rather than blindly restarting.
    /// Receiving v3 states with a recorded strong proper-prefix anchor may instead be supplied to
    /// [`ReaderTransfer::resume_from`]. That operation checks the frozen session and rehashes the
    /// prefix before source admission. Reconciliation itself never reopens a reader.
    pub fn run<F, B, E>(
        self,
    ) -> impl Future<Output = Result<ReaderReceipt, Box<Failure<ReaderError<E>>>>> + 'read
    where
        R: RangeReader<'read, Read = F, Body = B, Error = E>,
        F: Future<Output = Result<ReadResponse<'read, B>, E>> + 'read,
        B: Stream<Item = Result<Bytes, E>> + 'read,
        E: 'read,
    {
        self.run_registered::<F, B, E>(None)
    }

    pub(crate) async fn run_registered<F, B, E>(
        self,
        owner: Option<super::OperationOwner>,
    ) -> Result<ReaderReceipt, Box<Failure<ReaderError<E>>>>
    where
        R: RangeReader<'read, Read = F, Body = B, Error = E>,
        F: Future<Output = Result<ReadResponse<'read, B>, E>> + 'read,
        B: Stream<Item = Result<Bytes, E>> + 'read,
        E: 'read,
    {
        self.run_registered_observed::<F, B, E, _>(owner, &mut ())
            .await
    }

    pub(crate) async fn run_registered_observed<F, B, E, O>(
        self,
        owner: Option<super::OperationOwner>,
        observer: &mut O,
    ) -> Result<ReaderReceipt, Box<Failure<ReaderError<E>>>>
    where
        R: RangeReader<'read, Read = F, Body = B, Error = E>,
        F: Future<Output = Result<ReadResponse<'read, B>, E>> + 'read,
        B: Stream<Item = Result<Bytes, E>> + 'read,
        E: 'read,
        O: Observer,
    {
        self.run_registered_until_observed::<F, B, E, _, O>(
            owner,
            core::future::pending(),
            observer,
        )
        .await
    }

    pub(crate) async fn run_registered_until_observed<F, B, E, S, O>(
        self,
        owner: Option<super::OperationOwner>,
        stop: S,
        observer: &mut O,
    ) -> Result<ReaderReceipt, Box<Failure<ReaderError<E>>>>
    where
        R: RangeReader<'read, Read = F, Body = B, Error = E>,
        F: Future<Output = Result<ReadResponse<'read, B>, E>> + 'read,
        B: Stream<Item = Result<Bytes, E>> + 'read,
        E: 'read,
        S: Future<Output = ()> + 'read,
        O: Observer,
    {
        execute::<R, F, B, E, S, O>(self, stop, owner, observer).await
    }

    /// Stop source admission when the supplied signal completes and settle submitted physical work.
    /// A receiving state with a recorded strong proper-prefix witness may be continued through
    /// [`ReaderTransfer::resume_from`], which revalidates that prefix. A stop during sealing settles
    /// the seal without starting publication, but the closed state cannot resume writable.
    /// A stop after publication is submitted cannot undo it. `FailureKind::Stopped` is not a
    /// completed receipt or an unconditional promise of resumability.
    pub fn run_until<F, B, E, S>(
        self,
        stop: S,
    ) -> impl Future<Output = Result<ReaderReceipt, Box<Failure<ReaderError<E>>>>> + 'read
    where
        R: RangeReader<'read, Read = F, Body = B, Error = E>,
        F: Future<Output = Result<ReadResponse<'read, B>, E>> + 'read,
        B: Stream<Item = Result<Bytes, E>> + 'read,
        E: 'read,
        S: Future<Output = ()> + 'read,
    {
        execute_unobserved::<R, F, B, E, S>(self, stop, None)
    }
}

// Name the three family members once at the public boundary. Carrying projections through nested
// opaque async futures can make Send checking quantify over other source lifetimes, requiring static
// despite the caller having proved the members for this borrow. These equalities select one family.
async fn execute_unobserved<'read, R, F, B, E, S>(
    pending: PendingReader<'read, R>,
    stop: S,
    owner: Option<super::OperationOwner>,
) -> Result<ReaderReceipt, Box<Failure<ReaderError<E>>>>
where
    R: RangeReader<'read, Read = F, Body = B, Error = E>,
    F: Future<Output = Result<ReadResponse<'read, B>, E>> + 'read,
    B: Stream<Item = Result<Bytes, E>> + 'read,
    E: 'read,
    S: Future<Output = ()> + 'read,
{
    execute::<R, F, B, E, S, _>(pending, stop, owner, &mut ()).await
}

async fn execute<'read, R, F, B, E, S, O>(
    pending: PendingReader<'read, R>,
    stop: S,
    owner: Option<super::OperationOwner>,
    observer: &mut O,
) -> Result<ReaderReceipt, Box<Failure<ReaderError<E>>>>
where
    R: RangeReader<'read, Read = F, Body = B, Error = E>,
    F: Future<Output = Result<ReadResponse<'read, B>, E>> + 'read,
    B: Stream<Item = Result<Bytes, E>> + 'read,
    E: 'read,
    S: Future<Output = ()> + 'read,
    O: Observer,
{
    let mut observation = Observation::start(observer, super::Length::Known(pending.size));
    let result = execute_observed::<R, F, B, E, S, O>(pending, stop, owner, &mut observation).await;
    observation.emit();
    result
}

async fn execute_observed<'read, R, F, B, E, S, O>(
    pending: PendingReader<'read, R>,
    stop: S,
    owner: Option<super::OperationOwner>,
    observation: &mut Observation<'_, O>,
) -> Result<ReaderReceipt, Box<Failure<ReaderError<E>>>>
where
    R: RangeReader<'read, Read = F, Body = B, Error = E>,
    F: Future<Output = Result<ReadResponse<'read, B>, E>> + 'read,
    B: Stream<Item = Result<Bytes, E>> + 'read,
    E: 'read,
    S: Future<Output = ()> + 'read,
    O: Observer,
{
    let PendingReader {
        reader,
        size,
        evidence,
        plan,
        session,
        resume,
    } = pending;
    let state = StateRef::clone(plan.state());
    // Unsupported hosts reject here without calling read or polling a body.
    let native = Native::acquire_reader(plan, session, resume, owner)
        .await
        .map_err(|failure| Failure::native(failure.error, Partial::default(), failure.state))?;
    #[cfg(all(test, unix))]
    SEAL_TEST.with(|hook| {
        if let Some(sender) = hook.borrow_mut().take() {
            let _ = sender.send(native.hooks().seal.install());
        }
    });
    // Revalidation failures leave the last record intact, not a new zero-prefix checkpoint.
    let (checks, restored) = native.restore_reader().await.map_err(|error| {
        Failure::native(error, Partial::default(), Some(StateRef::clone(&state)))
    })?;
    let mut partial = Partial {
        received: restored,
        written: restored,
        hashed: restored,
        ..Partial::default()
    };
    observation.update(&partial, restored);
    let mut stop = pin!(stop);
    let result = receive::<R, F, B, E, _, O>(
        reader,
        size,
        evidence,
        &native,
        &mut partial,
        checks,
        stop.as_mut(),
        observation,
    )
    .await;
    let result = match result {
        Ok(checks) => match native.seal(size).await {
            // Sealing must settle even when stop is ready. Retain the signal until the last
            // pre-publication decision, rather than losing it when source reception ends.
            Ok(()) => tokio::select! {
                biased;
                () = &mut stop => Err(FailureKind::Stopped),
                () = core::future::ready(()) => match checks.finish() {
                    Ok(results) => {
                        partial.set_checks(results);
                        Ok(results)
                    }
                    Err(mismatch) => {
                        partial.set_checks(mismatch.results);
                        Err(FailureKind::DigestMismatch(mismatch))
                    }
                },
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
        stats: ReaderStats {
            restored,
            source: ByteCount(partial.received.0 - restored.0),
            written: ByteCount(partial.written.0 - restored.0),
        },
        consistency: match evidence {
            Evidence::Enforced { .. } => Consistency::EnforcedVersion,
            Evidence::Candidate { .. } => Consistency::ExpectedContent,
        },
    })
}

// This one-task private pipeline threads its borrowed checker and observation explicitly.
#[allow(clippy::too_many_arguments)]
async fn receive<'read, R, F, B, E, S, O>(
    reader: &'read R,
    size: ByteCount,
    evidence: Evidence<'read>,
    native: &Native,
    partial: &mut Partial,
    mut checks: crate::checks::Checks,
    stop: S,
    observation: &mut Observation<'_, O>,
) -> Result<crate::checks::Checks, FailureKind<ReaderError<E>>>
where
    R: RangeReader<'read, Read = F, Body = B, Error = E>,
    F: Future<Output = Result<ReadResponse<'read, B>, E>> + 'read,
    B: Stream<Item = Result<Bytes, E>> + 'read,
    E: 'read,
    S: Future<Output = ()>,
    O: Observer,
{
    let cancellation = Cancellation::default();
    let restored = partial.written.0;
    let ranges = crate::plan::plan_range(restored, size.0, 4);
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
    let mut frontier = restored;
    let mut failure = None;
    let mut stop = pin!(stop);
    loop {
        let step = tokio::select! {
            biased;
            () = &mut stop, if failure.is_none() => {
                failure = Some(FailureKind::Stopped);
                cancellation.cancel();
                continue;
            }
            step = work.next() => step,
        };
        let Some(step) = step else {
            break;
        };
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
        observation.update(partial, ByteCount(restored));
        settled[task.index] = task.offset;
        if let Some(error) = error {
            failure.get_or_insert(error);
            cancellation.cancel();
        }
        if failure.is_none() {
            // Only settled prefixes are visible. Tail writes never let the verifier jump a hole.
            let mut available = restored;
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
                    let mut end = available.min(frontier.saturating_add(64 * 1024));
                    // Never persist the last byte as cached EOF authority. Even a fully received
                    // late-error body must be reopened for at least a proper suffix on continuation.
                    if frontier < size.0.saturating_sub(1) {
                        end = end.min(size.0 - 1);
                    }
                    match native
                        .read_prefix(ByteCount(frontier), ByteCount(end))
                        .await
                    {
                        Ok(bytes) => {
                            checks.update(&bytes);
                            frontier = end;
                            partial.hashed = ByteCount(frontier);
                            observation.update(partial, ByteCount(restored));
                            if frontier < size.0
                                && let Some(anchor) = checks.prefix_anchor()
                                && let Err(error) = native
                                    .checkpoint_prefix(*partial, ByteCount(frontier), anchor)
                                    .await
                            {
                                failure = Some(error.into_kind());
                                cancellation.cancel();
                                break;
                            }
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

#[cfg(all(test, unix))]
std::thread_local! {
    // Hand the fixture its own physical seal barrier after this task acquires the native stage.
    static SEAL_TEST: core::cell::RefCell<Option<tokio::sync::oneshot::Sender<super::native::Held>>> = const { core::cell::RefCell::new(None) };
}

#[cfg(test)]
#[path = "reader_tests.rs"]
mod tests;
