//! Checked, one-shot native file publication, separate from the legacy download engine.
//!
//! Required SHA-256, clean body EOF and settled writes precede atomic no-replace publication.
//! The native backend requires a caller-owned Tokio runtime, an existing caller-owned parent without
//! group/other write permission, and Unix regular-file hardlink support. Published files retain the
//! private stage's mode 0600. Other hosts return a typed unsupported capability before filesystem work.
//! The retained private stage is an identity witness, not a resumable source. State is flushed to the
//! operating system without fsync: recovery covers process interruption, not power loss. Leases
//! exclude cooperating new writers only, not the legacy CLI or arbitrary same-user filesystem edits.

use core::convert::Infallible;
use core::fmt;
use core::future::{Future, pending};
use core::str::FromStr;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use futures::Stream;

use crate::checksum::OwnedSha256;

#[cfg(unix)]
mod native;
#[cfg(not(unix))]
#[path = "transfer/unsupported.rs"]
mod native;
mod pipeline;
mod plan;
#[cfg(unix)]
mod record;

use native::{Native, NativeError};
use plan::Plan;

#[cfg(all(test, unix))]
#[path = "transfer_tests.rs"]
mod tests;

/// A consumed ordered body and its declared length. This entrance has no hidden reopening behavior.
#[must_use]
pub struct Transfer<B> {
    body: B,
    length: Length,
    required: Result<Option<RequiredSha256>, RequestError>,
}

impl<B> Transfer<B> {
    /// Consume an already-open ordered body. The body is polled only on the task running this transfer.
    pub fn from_body(body: B, length: Length) -> Self {
        Self {
            body,
            length,
            required: Ok(None),
        }
    }

    /// Require a whole-file SHA-256 check. Agreeing requirements are idempotent; conflicts cannot
    /// discard an earlier requirement, even if another call later repeats it.
    pub fn expect(mut self, required: RequiredSha256) -> Self {
        self.required = match self.required {
            Ok(Some(previous)) if previous != required => {
                Err(RequestError::ConflictingRequirements)
            }
            Ok(_) => Ok(Some(required)),
            Err(error) => Err(error),
        };
        self
    }

    /// Plan an absent-only file publication without filesystem effects.
    ///
    /// An existing destination is deliberately checked by the publication primitive, after hashing.
    /// Missing checks and invalid destinations fail here, before a stage or lock can be created.
    pub fn to_file(self, destination: impl AsRef<Path>) -> Result<PendingFile<B>, RequestError> {
        let required = self.required?.ok_or(RequestError::MissingRequirement)?;
        let plan = Plan::new(destination.as_ref(), required)?;
        Ok(PendingFile {
            body: self.body,
            length: self.length,
            plan,
        })
    }
}

/// A planned native transfer. Its state location is advisory until execution acquires ownership.
#[must_use]
pub struct PendingFile<B> {
    body: B,
    length: Length,
    plan: Plan,
}

impl<B> PendingFile<B> {
    /// The planned private workspace, available before awaiting. A path grants no ownership authority.
    pub fn state(&self) -> &StateRef {
        self.plan.state()
    }

    /// Execute without a cooperative stop signal. Dropping the future stops further body polling.
    pub async fn run<E>(self) -> Result<Receipt, Failure<E>>
    where
        B: Stream<Item = Result<Bytes, E>>,
    {
        self.run_until(pending()).await
    }

    /// Stop body admission when `stop` completes, settle submitted I/O, and checkpoint retained data.
    ///
    /// A stop during commit cannot undo it: commit settles and reports publication instead. Dropping
    /// this future can leave the directory Busy until an already-submitted physical operation settles.
    /// The body, error and stop future need neither Send nor static; owned Send components yield Send.
    #[tracing::instrument(skip_all, name = "checked_native_file")]
    pub async fn run_until<E, S>(self, stop: S) -> Result<Receipt, Failure<E>>
    where
        B: Stream<Item = Result<Bytes, E>>,
        S: Future<Output = ()>,
    {
        let Self { body, length, plan } = self;
        let state = StateRef::clone(plan.state());
        let native = Native::acquire(plan)
            .await
            .map_err(|failure| Failure::native(failure.error, Partial::default(), failure.state))?;
        pipeline::Run {
            native,
            state,
            length,
        }
        .execute(body, stop)
        .await
    }
}

/// Exact byte counts, independent of terminal formatting and legacy engine counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct ByteCount(pub u64);

/// Whether the consumed body has a declared byte length.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Length {
    /// Gate completion on this exact number of bytes, in addition to actual EOF.
    Known(ByteCount),
    /// Only actual EOF establishes the length.
    Unknown,
}

/// A parsed whole-file SHA-256 requirement. Other algorithms are not supported by this entrance yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequiredSha256(Sha256Digest);

impl RequiredSha256 {
    /// The required value, distinct from an actual measured digest.
    pub fn digest(self) -> Sha256Digest {
        let Self(digest) = self;
        digest
    }
}

impl FromStr for RequiredSha256 {
    type Err = RequestError;

    /// Parse `sha256:` followed by exactly 64 hexadecimal digits.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (algorithm, digest) = value
            .split_once(':')
            .ok_or(RequestError::InvalidRequirement)?;
        if algorithm != "sha256" {
            return Err(RequestError::UnsupportedRequirement);
        }
        let mut bytes = [0; 32];
        hex::decode_to_slice(digest, &mut bytes).map_err(|_| RequestError::InvalidRequirement)?;
        Ok(Self(Sha256Digest(bytes)))
    }
}

/// An actual or required whole-file digest, never a source version or an ETag.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Sha256Digest(pub(crate) [u8; 32]);

impl fmt::Display for Sha256Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(bytes) = self;
        for byte in bytes {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Whether written counts fully describe the settled physical extent.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WriteExtent {
    /// Every submitted write returned its accepted prefix.
    #[default]
    Confirmed,
    /// A blocking worker failed without returning its extent. Counts are a confirmed lower bound.
    Unconfirmed,
}

/// Last observed counts. `actual` exists only after clean EOF and complete settled hashing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Partial {
    /// Bytes admitted from successful body chunks, including a rejected overlong chunk.
    pub received: ByteCount,
    /// Bytes physically written, including the successful prefix of a failed physical write.
    pub written: ByteCount,
    /// An exceptional lost worker result cannot be represented as all-or-zero physical progress.
    pub write_extent: WriteExtent,
    /// Written bytes folded into the concrete incremental SHA-256 owner.
    pub hashed: ByteCount,
    /// Whole-file measurement, not an assertion that requirements matched.
    pub actual: Option<Sha256Digest>,
}

/// Evidence of checked publication. The owned stage hardlink remains for offline reconciliation.
#[derive(Debug)]
pub struct Receipt {
    /// All bytes were received, written and hashed, with a clean EOF.
    pub partial: Partial,
    /// The matched whole-file requirement.
    pub required: RequiredSha256,
    /// A locator for retained evidence, not a transferable lease.
    pub state: StateRef,
}

/// A locator for one private workspace. Merely cloning or possessing it does not acquire a lease.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StateRef(PathBuf);

impl StateRef {
    /// The designated private workspace, not a legacy `.xget` file.
    pub fn path(&self) -> &Path {
        &self.0
    }

    /// Reacquire ownership and rehash anchored stage content before recognizing publication.
    ///
    /// No network or refetch occurs. Missing or conflicting publication evidence is never adopted.
    #[tracing::instrument(skip_all, name = "reconcile_native_file")]
    pub async fn reconcile(&self) -> Result<Reconciliation, Failure<Infallible>> {
        Native::reconcile(Self::clone(self))
            .await
            .map_err(|error| Failure::native(error, Partial::default(), Some(Self::clone(self))))
    }
}

/// A persisted path remains only a locator; reconciliation must reacquire and validate ownership.
impl From<PathBuf> for StateRef {
    fn from(path: PathBuf) -> Self {
        Self(path)
    }
}

impl From<&Path> for StateRef {
    fn from(path: &Path) -> Self {
        Self(path.to_path_buf())
    }
}

#[derive(Debug)]
struct AcquisitionFailure {
    error: NativeError,
    state: Option<StateRef>,
}

/// Offline recovery of the last valid checkpoint, not automatic resumption of a one-shot body.
#[derive(Debug)]
pub enum Reconciliation {
    /// A fresh body and fresh attempt are required. Counts describe only the last valid checkpoint.
    NeedsBody {
        /// Retained evidence that must not be appended to using an unrelated body.
        state: StateRef,
        /// Last atomically recorded counts, possibly behind writes interrupted after that checkpoint.
        partial: Partial,
    },
    /// Rehashed stage and destination share the recorded file identity and required content.
    Published(Receipt),
    /// An intent exists, but evidence is absent or incomplete. Do not blindly republish or refetch.
    PublicationUnknown(StateRef),
    /// Destination evidence disagrees, even if another inode contains identical bytes.
    Conflict(StateRef),
}

/// Request errors are resolved before any native filesystem effects.
#[derive(Debug, thiserror::Error)]
pub enum RequestError {
    /// This first entrance is checked-only.
    #[error("supply a whole-file sha256 requirement before creating a native file")]
    MissingRequirement,
    /// Different required values cannot replace or cancel one another.
    #[error("supply agreeing sha256 requirements for one native file")]
    ConflictingRequirements,
    /// The caller requested an algorithm this slice does not implement.
    #[error("use sha256 for checked native file publication")]
    UnsupportedRequirement,
    /// Digest syntax does not encode one SHA-256 value.
    #[error("use sha256 followed by a colon and 64 hexadecimal digits")]
    InvalidRequirement,
    /// Destination must name a file, not one of the private backend's reserved roles.
    #[error("choose a nonreserved filename in an existing parent directory")]
    InvalidDestination,
}

/// Which physical operation failed. Causes remain typed I/O errors, not formatted strings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// Opening and leasing the existing parent and private workspace.
    Acquire,
    /// Bounded payload write.
    Write,
    /// Settling and checking the physical data file.
    Seal,
    /// Atomic state replacement, without fsync.
    Checkpoint,
    /// Atomic no-replace link publication.
    Publish,
    /// Offline ownership reacquisition and revalidation.
    Reconcile,
}

impl fmt::Display for Phase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Acquire => "acquire",
            Self::Write => "write",
            Self::Seal => "seal",
            Self::Checkpoint => "checkpoint",
            Self::Publish => "publish",
            Self::Reconcile => "reconcile",
        })
    }
}

/// Typed terminal reason. A caller-owned source error is retained by value without global bounds.
#[derive(Debug, thiserror::Error)]
pub enum FailureKind<E> {
    /// The body failed, even if all declared bytes had previously arrived.
    #[error("body failed")]
    Body(E),
    /// Actual body length disagrees with its declaration.
    #[error("body length does not match its declared length")]
    LengthMismatch,
    /// Exact counts cannot represent the admitted body.
    #[error("body byte count exceeds the supported range")]
    LengthOverflow,
    /// A clean whole-file measurement did not satisfy the requirement.
    #[error("whole-file sha256 requirement did not match")]
    DigestMismatch,
    /// Cooperative stop successfully settled and checkpointed the one-shot partial.
    #[error("transfer stopped; supply a fresh body for a new attempt")]
    Stopped,
    /// Checkpointing failed while retaining the original interruption or body failure.
    #[error("checkpoint failed; retained data is not a successfully paused transfer")]
    CheckpointFailed {
        /// Original reason, including a caller-owned body error if present.
        cause: Box<FailureKind<E>>,
        /// Typed native checkpoint failure with its original I/O cause.
        checkpoint: Box<FailureKind<E>>,
    },
    /// Cooperating ownership has not yet settled.
    #[error("native file ownership is busy; wait for the active operation to settle")]
    Busy,
    /// The actual no-replace publication observed an incumbent.
    #[error("destination exists; choose an absent destination")]
    DestinationExists,
    /// No caller-owned Tokio runtime is available for native physical submissions.
    #[error("run checked native file operations inside a tokio runtime")]
    RuntimeUnavailable,
    /// Native host or filesystem cannot supply the requested guarantee.
    #[error("native file publication is unsupported on this host or filesystem")]
    Unsupported,
    /// A role, ownership, version or identity check refused foreign or damaged state.
    #[error("native state is foreign or invalid; preserve it and choose a fresh destination")]
    InvalidState,
    /// Intent exists and commit acknowledgement was lost; only leased reconciliation may decide it.
    #[error("publication outcome is unknown; reconcile the retained state before retrying")]
    PublicationUnknown {
        /// Failure after publication may have happened, without claiming no effect.
        cause: std::io::Error,
    },
    /// A native operation failed, preserving its phase and original I/O error.
    #[error("native {phase} operation failed")]
    Io {
        /// The physical phase, separate from body failures.
        phase: Phase,
        /// Original native cause.
        cause: std::io::Error,
    },
}

/// Failure plus exact in-task progress and optional owned-state locator.
#[derive(Debug)]
pub struct Failure<E> {
    /// A matchable reason, including the original caller-owned body error.
    pub kind: FailureKind<E>,
    /// Exact counts observed before this failure, not a fabricated success receipt.
    pub partial: Partial,
    /// Retained owned workspace, once create-new ownership was established.
    pub state: Option<StateRef>,
}

impl<E> Failure<E> {
    fn native(error: NativeError, partial: Partial, state: Option<StateRef>) -> Self {
        Self {
            kind: error.into_kind(),
            partial,
            state,
        }
    }
}

impl<E: fmt::Debug> fmt::Display for Failure<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.kind, f)
    }
}

// Borrowed E is still retained by value. Only static errors can participate in Error::source.
impl<E: core::error::Error + 'static> core::error::Error for Failure<E> {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match &self.kind {
            FailureKind::Body(cause) => Some(cause),
            FailureKind::CheckpointFailed { checkpoint, .. } => checkpoint.native_source(),
            FailureKind::Io { cause, .. } | FailureKind::PublicationUnknown { cause } => {
                Some(cause)
            }
            _ => None,
        }
    }
}

impl<E: core::error::Error + 'static> FailureKind<E> {
    fn native_source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Body(cause) => Some(cause),
            Self::Io { cause, .. } | Self::PublicationUnknown { cause } => Some(cause),
            Self::CheckpointFailed { checkpoint, .. } => checkpoint.native_source(),
            _ => None,
        }
    }
}
