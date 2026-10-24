//! Checked native publication with required checks and optional measurement.
//!
//! Clean body EOF, settled writes and every required digest precede atomic no-replace publication.
//! Optional measurement defaults to SHA-256; explicit none never creates a hidden recovery hash.
//! The native backend requires a caller-owned Tokio runtime, an existing caller-owned parent without
//! group/other write permission, and Unix regular-file hardlink support. Published files retain the
//! private stage's mode 0600. Other hosts return a typed unsupported capability before filesystem work.
//! The retained private stage is an identity witness, not a resumable source. State is flushed to the
//! operating system without fsync: recovery covers process interruption, not power loss. Leases
//! exclude cooperating writers only, not arbitrary same-user filesystem edits.
//!
//! Required checks are independent of optional measurement. Run native operations on a caller-owned
//! Tokio runtime, with an absent destination in a suitable existing parent:
//!
//! ```no_run
//! use bytes::Bytes;
//! use futures::stream;
//! use xget::Transfer;
//! use xget::transfer::{Digest, Length, Measurement};
//!
//! # async fn example() -> Result<(), Box<dyn core::error::Error>> {
//! let required: Digest =
//!     "blake3:6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85".parse()?;
//! let body = stream::iter([Ok::<_, std::io::Error>(Bytes::from_static(b"abc"))]);
//! let receipt = Transfer::from_body(body, Length::Unknown)
//!     .expect(required)
//!     .measurement(Measurement::None)
//!     .to_file("download.bin")?
//!     .run().await?;
//! assert_eq!(receipt.checks().matched_requirements().count(), 1);
//! assert_eq!(receipt.checks().measured(), None);
//! # Ok(())
//! # }
//! ```
//!
//! Omitting requirements is ordinary file publication. With `Measurement::None` as well, no hash is
//! computed and later reconciliation returns `NotRevalidatable` when identities still agree. It does
//! not compute a recovery-only fingerprint, adopt foreign bytes, or republish a missing destination.

use core::convert::Infallible;
use core::fmt;
use core::future::{Future, pending};
use core::str::FromStr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use futures::Stream;

pub use crate::checks::{
    Algorithm, Conflict, Digest, Measurement, Mismatch, Origins, Outcome as CheckResults,
    ParseDigestError, Provenance, Requirement,
};
use crate::checks::{CheckPolicy, Requirements};
#[cfg(all(test, unix))]
use crate::checksum::OwnedSha256;

pub mod live;
#[cfg(unix)]
mod native;
#[cfg(not(unix))]
#[path = "transfer/unsupported.rs"]
mod native;
pub(crate) mod observation;
mod pipeline;
mod plan;
pub mod reader;
#[cfg(unix)]
mod record;

use native::{Native, NativeError};
use plan::Plan;
pub(crate) use plan::validate_file_spelling;

/// Pure frozen reader metadata shared by the coordinator and private native codec.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ReaderSession {
    size: ByteCount,
    representation: String,
    version: Option<Vec<u8>>,
}

#[cfg(unix)]
#[derive(Clone, Debug, PartialEq, Eq)]
struct ReaderCheckpoint {
    session: ReaderSession,
    // Irreversible even when whole-file checks fail after sealing.
    closed: bool,
    prefix: Option<(ByteCount, Digest)>,
}

/// Ordinary coordinator authority only. Advanced public entrances always construct Absent plans.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum Publication {
    #[default]
    Absent,
    Replace,
}

// Private physical-operation registration. Lower layers know only an owned lifetime, not its caller.
pub(crate) type OperationOwner = Arc<dyn Send + Sync>;
pub(crate) const OWNED_RUN_STORE: &str = ".xget-runs";
#[cfg(unix)]
pub(crate) const OWNED_LEASE_MARKER: &[u8] = b"XGET-OWNED-DIRECTORY-1\n";

fn is_run_store(path: &Path) -> bool {
    path.components().any(|component| {
        component
            .as_os_str()
            .to_str()
            .is_some_and(|name| name.eq_ignore_ascii_case(OWNED_RUN_STORE))
    })
}

#[cfg_attr(not(unix), allow(dead_code))] // Unsupported hosts retain the private registration signature.
struct RecoveryBinding {
    destination: PathBuf,
    checks: CheckPolicy,
}

pub(crate) async fn lease_directory(
    path: PathBuf,
) -> Result<OperationOwner, Box<Failure<Infallible>>> {
    Native::lease_directory(path)
        .await
        .map_err(|error| Failure::native(error, Partial::default(), None))
}

pub(crate) async fn lease_existing_directory(
    path: PathBuf,
) -> Result<OperationOwner, Box<Failure<Infallible>>> {
    Native::lease_existing_directory(path)
        .await
        .map_err(|error| Failure::native(error, Partial::default(), None))
}

#[cfg(all(test, unix))]
#[path = "transfer_tests.rs"]
mod tests;

/// A consumed ordered body and its declared length. This entrance has no hidden reopening behavior.
#[must_use]
pub struct Transfer<B> {
    body: B,
    length: Length,
    requirements: Result<Requirements, Box<Conflict>>,
    measurement: Measurement,
}

impl Transfer<()> {
    /// Borrow a repeatable, known-size range session for checked native publication.
    /// See [`reader::RangeReader`] for lifetime-specific future/body/error families and version policy.
    pub fn from_reader<'read, R>(reader: &'read R) -> reader::ReaderTransfer<'read, R>
    where
        R: reader::RangeReader<'read>,
    {
        reader::ReaderTransfer::from_reader(reader)
    }
}

impl<B> Transfer<B> {
    /// Consume an already-open ordered body. The body is polled only on the task running this transfer.
    pub fn from_body(body: B, length: Length) -> Self {
        Self {
            body,
            length,
            requirements: Ok(Requirements::default()),
            measurement: Measurement::default(),
        }
    }

    /// Require a whole-file digest from the caller. Agreeing values merge; conflicts stay sticky.
    /// The original RequiredSha256 value also converts into this general digest entrance.
    pub fn expect(self, required: impl Into<Digest>) -> Self {
        self.require(required.into(), Provenance::Caller)
    }

    /// Require a digest with bounded provenance. Provenance records origin, not authenticity.
    pub fn require(mut self, required: Digest, provenance: Provenance) -> Self {
        self.requirements = self.requirements.and_then(|mut requirements| {
            requirements.insert(required, provenance)?;
            Ok(requirements)
        });
        self
    }

    /// Select optional measurement independently of required checks. None performs no extra hashing.
    pub fn measurement(mut self, measurement: Measurement) -> Self {
        self.measurement = measurement;
        self
    }

    /// Plan an absent-only file publication without filesystem effects.
    ///
    /// An existing destination is deliberately checked by the publication primitive, after hashing.
    /// Conflicting requirements and invalid destinations fail before a stage or lock is created.
    /// With no requirement this is ordinary absent-only publication, not expectation verification.
    pub fn to_file(self, destination: impl AsRef<Path>) -> Result<PendingFile<B>, RequestError> {
        let requirements = self
            .requirements
            .map_err(RequestError::ConflictingRequirements)?;
        let checks = CheckPolicy {
            requirements,
            measurement: self.measurement,
        };
        if is_run_store(destination.as_ref()) {
            return Err(RequestError::InvalidDestination);
        }
        let plan = Plan::new(destination.as_ref(), checks)?;
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
    pub(crate) fn protect_targets(mut self, targets: Arc<[PathBuf]>) -> Self {
        self.plan.targets = targets;
        self
    }

    pub(crate) fn authorized(
        mut self,
        publication: Publication,
        restart: Option<StateRef>,
    ) -> Self {
        self.plan.publication = publication;
        self.plan.restart = restart;
        self
    }

    /// The planned private workspace, available before awaiting. A path grants no ownership authority.
    pub fn state(&self) -> &StateRef {
        self.plan.state()
    }

    /// Execute without a cooperative stop signal. Dropping the future stops further body polling.
    pub async fn run<E>(self) -> Result<Receipt, Box<Failure<E>>>
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
    pub async fn run_until<E, S>(self, stop: S) -> Result<Receipt, Box<Failure<E>>>
    where
        B: Stream<Item = Result<Bytes, E>>,
        S: Future<Output = ()>,
    {
        self.run_owned(stop, None, &mut ()).await
    }

    pub(crate) async fn run_registered_observed<E, O>(
        self,
        owner: OperationOwner,
        observer: &mut O,
    ) -> Result<Receipt, Box<Failure<E>>>
    where
        B: Stream<Item = Result<Bytes, E>>,
        O: observation::Observer,
    {
        self.run_registered_until_observed(owner, pending(), observer)
            .await
    }

    pub(crate) async fn run_registered_until_observed<E, S, O>(
        self,
        owner: OperationOwner,
        stop: S,
        observer: &mut O,
    ) -> Result<Receipt, Box<Failure<E>>>
    where
        B: Stream<Item = Result<Bytes, E>>,
        S: Future<Output = ()>,
        O: observation::Observer,
    {
        self.run_owned(stop, Some(owner), observer).await
    }

    async fn run_owned<E, S, O>(
        self,
        stop: S,
        owner: Option<OperationOwner>,
        observer: &mut O,
    ) -> Result<Receipt, Box<Failure<E>>>
    where
        B: Stream<Item = Result<Bytes, E>>,
        S: Future<Output = ()>,
        O: observation::Observer,
    {
        let Self { body, length, plan } = self;
        let mut observation = observation::Observation::start(observer, length);
        let result = async {
            let state = StateRef::clone(plan.state());
            let native = Native::acquire_registered(plan, owner)
                .await
                .map_err(|failure| {
                    Failure::native(failure.error, Partial::default(), failure.state)
                })?;
            pipeline::Run {
                native,
                state,
                length,
            }
            .execute_observed(body, stop, &mut observation)
            .await
        }
        .await;
        observation.emit();
        result
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

/// Compatibility whole-file SHA-256 requirement. Use Digest for the general algorithm entrance.
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

impl From<RequiredSha256> for Digest {
    fn from(required: RequiredSha256) -> Self {
        let RequiredSha256(Sha256Digest(bytes)) = required;
        Self::sha256(bytes)
    }
}

impl From<RequiredSha256> for CheckPolicy {
    fn from(required: RequiredSha256) -> Self {
        Self {
            requirements: Requirements::single(required.into(), Provenance::Caller),
            measurement: Measurement::default(),
        }
    }
}

/// An actual or required whole-file digest, never a source version or an ETag.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Sha256Digest(pub(crate) [u8; 32]);

impl Sha256Digest {
    fn from_digest(digest: Digest) -> Option<Self> {
        if digest.algorithm() != Algorithm::Sha256 {
            return None;
        }
        digest.as_bytes().try_into().ok().map(Self)
    }
}

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

/// Last observed counts. Finished hash facts do not themselves assert publication or remote freshness.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Partial {
    /// Bytes admitted from successful body chunks, including a rejected overlong chunk.
    pub received: ByteCount,
    /// Bytes physically written, including the successful prefix of a failed physical write.
    pub written: ByteCount,
    /// An exceptional lost worker result cannot be represented as all-or-zero physical progress.
    pub write_extent: WriteExtent,
    /// Payload bytes fed to the active digest set, counted once, not once per algorithm.
    /// Zero when no requirements and Measurement::None requested no hashing.
    pub hashed: ByteCount,
    /// Compatibility SHA-256 value when present in the finished measured/matched results.
    /// This is not an independently requested measurement or a verification claim.
    pub actual: Option<Sha256Digest>,
    /// Finished optional measurement and individual matching requirements, only after clean EOF.
    pub checks: Option<CheckResults>,
}

/// Evidence of native publication after the configured required checks, if any.
/// Public advanced entrances are absent-only. Ordinary Client execution may carry separately
/// granted replacement permission; this receipt never transfers that execution authority.
/// Read-only views keep final counts and check results consistent. The owned stage hardlink remains
/// for offline reconciliation, which may be NotRevalidatable; this value is not a transferable lease.
#[derive(Debug)]
pub struct Receipt {
    partial: Partial,
    required: Option<RequiredSha256>,
    state: StateRef,
}

impl Partial {
    fn set_checks(&mut self, results: CheckResults) {
        self.actual = results
            .digest_for(Algorithm::Sha256)
            .and_then(Sha256Digest::from_digest);
        self.checks = Some(results);
    }

    #[cfg(unix)]
    fn results_fit(self, policy: CheckPolicy) -> bool {
        match self.checks {
            None => self.actual.is_none(),
            Some(results) => {
                self.write_extent == WriteExtent::Confirmed
                    && self.received == self.written
                    && self.hashed
                        == (if policy.hashing() {
                            self.written
                        } else {
                            ByteCount(0)
                        })
                    && policy.describes(results)
                    && self.actual
                        == results
                            .digest_for(Algorithm::Sha256)
                            .and_then(Sha256Digest::from_digest)
            }
        }
    }

    #[cfg(unix)]
    fn complete_for(self, policy: CheckPolicy) -> bool {
        self.results_fit(policy)
            && self
                .checks
                .is_some_and(|results| policy.satisfied_by(results))
    }
}

impl Receipt {
    /// Clean EOF and settled counts. Hash work is counted once per payload byte, not per algorithm.
    pub fn partial(&self) -> &Partial {
        &self.partial
    }

    /// Matched requirements and the separate optional measurement, stored once with the final counts.
    pub fn checks(&self) -> &CheckResults {
        match &self.partial.checks {
            Some(checks) => checks,
            None => unreachable!("completed receipts always contain finished check results"),
        }
    }

    /// Compatibility view of a matched SHA-256 requirement, absent for an unrequired file.
    /// Use [`Self::checks`] to inspect algorithm-general requirements.
    pub fn required(&self) -> Option<RequiredSha256> {
        self.required
    }

    /// A locator for retained evidence, not a transferable lease.
    pub fn state(&self) -> &StateRef {
        &self.state
    }

    fn completed(mut partial: Partial, checks: CheckResults, state: StateRef) -> Self {
        // Keeping one result set prevents the receipt's views from diverging or doubling its size.
        partial.set_checks(checks);
        let required = checks
            .matched_requirements()
            .find(|requirement| requirement.digest.algorithm() == Algorithm::Sha256)
            .and_then(|requirement| Sha256Digest::from_digest(requirement.digest))
            .map(RequiredSha256);
        Self {
            partial,
            required,
            state,
        }
    }
}

/// A locator for one private workspace. Merely cloning or possessing it does not acquire a lease.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StateRef(PathBuf);

impl StateRef {
    /// The designated private workspace, not a legacy `.xget` file.
    pub fn path(&self) -> &Path {
        &self.0
    }

    /// Reacquire ownership and rehash only an already-recorded strong content anchor.
    /// Without one, return NotRevalidatable rather than computing an unrequested fingerprint.
    ///
    /// No network or refetch occurs. Missing or conflicting publication evidence is never adopted.
    #[tracing::instrument(skip_all, name = "reconcile_native_file")]
    pub async fn reconcile(&self) -> Result<Reconciliation, Box<Failure<Infallible>>> {
        Native::reconcile(Self::clone(self))
            .await
            .map_err(|error| Failure::native(error, Partial::default(), Some(Self::clone(self))))
    }

    pub(crate) async fn reconcile_registered(
        &self,
        owner: OperationOwner,
        destination: PathBuf,
        checks: CheckPolicy,
    ) -> Result<Reconciliation, Box<Failure<Infallible>>> {
        Native::reconcile_registered(
            Self::clone(self),
            Some(owner),
            Some(RecoveryBinding {
                destination,
                checks,
            }),
        )
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
    /// Stage and destination share the recorded identity and revalidated strong content anchor.
    /// This checks recorded completed content, not authenticity or the current remote version.
    Published(Receipt),
    /// An intent exists, but evidence is absent or incomplete. Do not blindly republish or refetch.
    PublicationUnknown(StateRef),
    /// Destination evidence disagrees, even if another inode contains identical bytes.
    Conflict(StateRef),
    /// There is no already-recorded strong content anchor. Identity alone cannot certify completion.
    /// No payload hash, refetch, adoption or republication is performed for this result.
    NotRevalidatable(StateRef),
}

/// Request errors are resolved before body polling, writer calls or native filesystem effects.
#[derive(Debug, thiserror::Error)]
pub enum RequestError {
    /// Compatibility error retained for callers of the original checked-only entrance.
    /// Ordinary unrequired publication no longer produces this error.
    #[error("supply a whole-file sha256 requirement before creating a native file")]
    MissingRequirement,
    /// Different required values cannot replace or cancel one another.
    #[error("conflicting body requirements; supply agreeing digests for each algorithm")]
    ConflictingRequirements(#[source] Box<Conflict>),
    /// The compatibility RequiredSha256 parser was given a different algorithm.
    #[error("use sha256 in a requiredsha256 value or use digest for other algorithms")]
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
    /// Bounded in-order readback of an acknowledged stage prefix for configured digest work.
    ReadStage,
    /// Settling and checking the physical data file.
    Seal,
    /// Atomic state replacement, without fsync.
    Checkpoint,
    /// Atomic no-replace link publication or separately authorized guarded replacement.
    /// See [`crate::client::Overwrite`] for the cooperating-writer replacement contract;
    /// replacement is not an inode compare-and-swap against arbitrary filesystem writers.
    Publish,
    /// Offline ownership reacquisition and revalidation.
    Reconcile,
}

impl fmt::Display for Phase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Acquire => "acquire",
            Self::Write => "write",
            Self::ReadStage => "read stage",
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
    /// At least one required check failed, independently of optional measurement.
    #[error("whole-file requirement did not match")]
    DigestMismatch(#[source] Box<Mismatch>),
    /// Cooperative stop successfully settled and checkpointed the retained partial.
    /// A one-shot body still needs fresh access; a reader may have an anchored resumable prefix.
    #[error("transfer stopped; inspect the retained state before continuing")]
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
/// Returned behind a concrete Box to keep multi-check failures off the success path, without
/// erasing the caller cause or requiring it to be Send, Sync or static.
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
    fn native(error: NativeError, partial: Partial, state: Option<StateRef>) -> Box<Self> {
        Box::new(Self {
            kind: error.into_kind(),
            partial,
            state,
        })
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
            FailureKind::DigestMismatch(cause) => Some(cause.as_ref()),
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
            Self::DigestMismatch(cause) => Some(cause.as_ref()),
            Self::Io { cause, .. } | Self::PublicationUnknown { cause } => Some(cause),
            Self::CheckpointFailed { checkpoint, .. } => checkpoint.native_source(),
            _ => None,
        }
    }
}
