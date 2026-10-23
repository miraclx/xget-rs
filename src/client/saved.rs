//! Frozen aggregate continuation. V3 adds exact HTTP reader mode to V2 member bindings.
//! V4 adds genuine S3 key selectors and optional versionId without changing V1/V2/V3 encoding.
//! S3 stays whole mode, with fresh conditional GETs, no listing/HEAD on reentry and no byte reuse.
//! This codec is not a public manifest API.

use core::convert::Infallible;
use core::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use zeroize::Zeroizing;

use super::{Client, Error, Plan, PlannedFile, Report, RunFailure, Target};
use crate::checks::{CheckPolicy, Requirements};
use crate::checksum::OwnedSha256;
use crate::input::{Input, InputOccurrence, InputScope, MemberToken, Namespace};
use crate::inventory::{Description, File, FileId, Selected};
use crate::selection::Selection;
use crate::transfer::{Length, Measurement, OperationOwner, Receipt, Reconciliation, StateRef};

mod codec;
mod disk;
mod key;

pub use key::{BindingKeyParseError, RunId};

#[cfg(all(test, unix))]
#[path = "saved_tests.rs"]
mod tests;

const STORE: &str = crate::transfer::OWNED_RUN_STORE;
const MAX_ATTEMPTS: usize = 32;
const MAX_RECORD: usize = 1024 * 1024;

/// Offline frozen run inspection. This value grants no executable ownership and holds no credentials.
///
/// Continue by re-supplying each inspected binding key. Only the exact original reference intent is
/// supported, including its query and spelling. SwissTransfer reuses the original share reference
/// plus its recorded member token, path and any first-open strong pin, never a persisted signed URL.
/// A different reference is not inferred to be the same member. Continuation rechecks completed native
/// evidence. Frozen V3 exact HTTP reader members may rehash an owned strong receiving prefix and open
/// only missing suffix windows. Native validation refuses missing/unanchored/closed reader evidence;
/// there is no fallback after refusal. Other members open fresh full ordered bodies. V1/V2 retain
/// their original whole-body meaning. No transport session capability or credential is persisted.
///
/// ```no_run
/// use xget::Client;
/// use xget::client::SavedRun;
/// # async fn example() -> Result<(), Box<dyn core::error::Error>> {
/// let saved = SavedRun::inspect("downloads/.xget-runs/run-123").await?;
/// let key = saved.members().next().ok_or("empty saved run")?.key();
/// let client = Client::new()?;
/// let report = client.resume(saved)
///     .input(key, "https://example.org/abc.bin")
///     .run().await?;
/// assert_eq!(report.completed().len(), 1);
/// # Ok(())
/// # }
/// ```
#[must_use]
pub struct SavedRun {
    path: PathBuf,
    record: Record,
    snapshot: Fingerprint,
}

impl SavedRun {
    /// Reads bounded private metadata only, without a Client, network access or a lease.
    /// Missing state, corrupt state, unsupported versions, host limitations and I/O stay distinct.
    /// Requires a caller-owned Tokio runtime. No filesystem modifications or payload hashing occur.
    /// Parent (`..`) path components are refused rather than collapsed across possible symlinks.
    pub async fn inspect(path: impl AsRef<Path>) -> Result<Self, SavedError> {
        disk::inspect(path.as_ref().to_path_buf()).await
    }

    /// The actual run workspace to pass to a later inspection, not a reopen capability.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Safe frozen member and binding keys; no remote reference or request fingerprint is exposed.
    pub fn members(&self) -> impl ExactSizeIterator<Item = SavedMember<'_>> {
        let run = Fingerprint::bytes(self.path.as_os_str().as_encoded_bytes());
        self.record
            .members
            .iter()
            .enumerate()
            .map(move |(index, member)| SavedMember {
                key: BindingKey {
                    run,
                    index: index as u32,
                },
                member,
            })
    }

    pub(super) fn bind_request<'a>(
        self,
        client: &'a Client,
        inputs: Vec<Input>,
    ) -> Result<Resume<'a>, SavedError> {
        if self.record.version == 1 && self.record.members.len() != inputs.len() {
            return Err(SavedError::Binding);
        }
        let mut bindings = Vec::with_capacity(self.record.members.len());
        for (inspected, member) in self.members().zip(&self.record.members) {
            let input = inputs
                .get(member.occurrence.0 as usize)
                .ok_or(SavedError::Binding)?;
            if !member.accepts_selected(input) {
                return Err(SavedError::Binding);
            }
            let mut supplied = input
                .duplicate_reference()
                .map_err(|_| SavedError::Binding)?;
            supplied.occurrence = member.occurrence;
            bindings.push((inspected.key(), supplied));
        }
        let mut resume = client.resume(self);
        for (key, input) in bindings {
            resume = resume.input(key, input);
        }
        Ok(resume)
    }

    /// Fully acknowledged aggregate completion is not an unfinished automatic re-entry match.
    pub fn is_complete(&self) -> bool {
        self.record.complete
    }
}

impl fmt::Debug for SavedRun {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SavedRun")
            .field("members", &self.record.members.len())
            .field("complete", &self.record.complete)
            .finish()
    }
}

/// A run-local frozen member binding, not a remote identity, authorization token or row selector.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct BindingKey {
    run: Fingerprint,
    index: u32,
}

impl fmt::Debug for BindingKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BindingKey")
            .field("member", &self.index)
            .finish()
    }
}

/// Read-only safe aggregate member metadata. Remote names and validators are intentionally omitted.
pub struct SavedMember<'a> {
    key: BindingKey,
    member: &'a Member,
}

impl SavedMember<'_> {
    /// Rebuilds frozen nonsecret binding intent from a caller-supplied original reference.
    ///
    /// This pure operation performs no discovery, selector matching, filesystem work or lease
    /// acquisition. V4 intent restores scope, namespace and ordered patterns, then checks both the
    /// exact original-reference fingerprint and the existing full binding. Frozen member/version,
    /// checks, mode and assignment remain authoritative in Resume::input. No URL or credentials are
    /// recovered from disk. Older records can reconstruct a default parsed input only when its full
    /// binding agrees; complex older intent requires the caller to supply the original Input explicitly.
    pub fn binding_input(&self, reference: &str) -> Result<Input, SavedError> {
        match &self.member.intent {
            Some(intent) => intent.input(reference, self.member.binding),
            None => {
                let input = Input::parse(reference).map_err(|_| SavedError::Binding)?;
                if Fingerprint::reference(&input) != self.member.binding {
                    return Err(SavedError::BindingIntentRequired);
                }
                Ok(input)
            }
        }
    }

    /// Key accepted by the consumed continuation builder.
    pub fn key(&self) -> BindingKey {
        self.key
    }
    /// The approved native assignment, explicitly requested rather than included in diagnostics.
    pub fn destination(&self) -> &Path {
        &self.member.destination
    }
    /// Frozen advisory length, not authority to adopt current bytes.
    pub fn length(&self) -> Length {
        self.member.length
    }
    /// Frozen version category. The opaque validator itself is never diagnostic data.
    pub fn version(&self) -> crate::inventory::VersionEvidence {
        if self.member.etag.is_some() {
            crate::inventory::VersionEvidence::StrongEtag
        } else {
            crate::inventory::VersionEvidence::Unpinned
        }
    }
    /// Frozen whole-payload requirements, with their bounded origins preserved.
    pub fn requirements(&self) -> impl Iterator<Item = crate::transfer::Requirement> {
        self.member.requirements.iter()
    }
    /// Frozen optional measurement; None never creates a recovery-only hash.
    pub fn measurement(&self) -> Measurement {
        self.member.measurement
    }
    /// Last aggregate acknowledgement only. Complete still requires native strong-content and inode
    /// revalidation before reuse; Submitted may have a lost native publication acknowledgement.
    pub fn status(&self) -> SavedStatus {
        match self.member.attempts.last().map(|attempt| attempt.status) {
            None => SavedStatus::Pending,
            Some(AttemptStatus::Submitted) => SavedStatus::Submitted,
            Some(AttemptStatus::Retained) => SavedStatus::Retained,
            Some(AttemptStatus::Complete) => SavedStatus::Complete,
        }
    }
    /// All retained native attempt locators, oldest first. No attempt is deleted automatically.
    pub fn attempts(&self) -> impl ExactSizeIterator<Item = &StateRef> {
        self.member.attempts.iter().map(|attempt| &attempt.state)
    }
}

impl fmt::Debug for SavedMember<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SavedMember")
            .field("key", &self.key)
            .finish()
    }
}

/// Last recorded aggregate member state, not a lease or a fresh content certification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SavedStatus {
    /// No native attempt was admitted yet.
    Pending,
    /// The advisory native StateRef was checkpointed before submission; its outcome needs inspection.
    Submitted,
    /// A native failure was acknowledged and its attempt retained.
    Retained,
    /// Native publication was acknowledged. Continuation still revalidates its recorded evidence.
    Complete,
}

/// Explicitly bypass automatic unfinished-request continuation, leaving all old state untouched.
/// This grants neither overwrite permission nor authority over another live owner.
#[derive(Clone, Copy, Debug)]
pub struct Fresh;

/// Consumed exact-reference rebinding of an inspected frozen run. No selection or HEAD is performed.
///
/// # Ownership and cancellation
///
/// Execution excludes other aggregate owners of the same designated output root. Submitted manifest
/// and native physical operations retain that ownership until they settle, including work queued
/// before acquisition returns. Dropping a waiter can therefore leave the root Busy after drop.
/// Different output roots do not share this aggregate lease.
///
/// Drop is not rollback or a successful pause: a submitted publication can finish without returning
/// its receipt. Retain the actual [`SavedRun::path`] or [`Report::saved_run`] path, wait for submitted
/// operations to settle, then inspect and continue that run rather than blindly fetching again.
/// Directory preparation precedes aggregate admission and may leave empty directories or submitted
/// directory work after drop; those partial effects are not a saved-run or settlement receipt.
#[must_use]
pub struct Resume<'a> {
    client: &'a Client,
    saved: SavedRun,
    bindings: Vec<Option<Input>>,
    error: Option<SavedError>,
    retained_bytes: usize,
}

impl<'a> Resume<'a> {
    pub(super) fn new(client: &'a Client, saved: SavedRun) -> Self {
        let bindings = (0..saved.record.members.len()).map(|_| None).collect();
        Self {
            client,
            saved,
            bindings,
            error: None,
            retained_bytes: 0,
        }
    }

    /// Supplies the exact original reference intent for one frozen member, including its namespace
    /// and ordered include/exclusion spellings if configured. A share with several members uses the
    /// same original share Input for each inspected key, without rediscovery or path reevaluation.
    /// Wrong, repeated and
    /// foreign keys fail; renamed/renewed references are not accepted by guessing from paths or ETags.
    /// A supplied selected File must agree with the frozen version and advisory length, and each of
    /// its required digest values must already be frozen. It cannot replace or add execution policy.
    pub fn input<T>(mut self, key: BindingKey, reference: T) -> Self
    where
        T: TryInto<Input>,
    {
        if self.error.is_some() {
            return self;
        }
        let mut input = match reference.try_into() {
            Ok(input) => input,
            Err(_) => {
                self.error = Some(SavedError::Binding);
                return self;
            }
        };
        self.retained_bytes = self.retained_bytes.saturating_add(input.retained_bytes());
        if self.retained_bytes > crate::inventory::MAX_METADATA_BYTES {
            self.error = Some(SavedError::Limit);
            return self;
        }
        if key.run != Fingerprint::bytes(self.saved.path.as_os_str().as_encoded_bytes()) {
            self.error = Some(SavedError::Binding);
            return self;
        }
        let index = key.index as usize;
        let Some(member) = self.saved.record.members.get(index) else {
            self.error = Some(SavedError::Binding);
            return self;
        };
        if self.bindings[index].is_some()
            || Client::supported(&input).is_err()
            || Fingerprint::reference(&input) != member.binding
            || !member.accepts_selected(&input)
        {
            self.error = Some(SavedError::Binding);
            return self;
        }
        input.occurrence = member.occurrence;
        input.member = member
            .remote
            .as_ref()
            .map(|remote| MemberToken::clone(&remote.token));
        self.bindings[index] = Some(input);
        self
    }

    /// Reacquires ownership, validates frozen roles/root and native evidence, then continues serially.
    /// Completed content without a recorded strong anchor fails NotRevalidatable without a hidden hash.
    /// See [ownership and cancellation](Resume#ownership-and-cancellation) for Busy after drop,
    /// publication without a returned receipt, and how to retain the actual recovery path.
    pub async fn run(self) -> Result<Report, Box<RunFailure>> {
        self.run_with(&mut ()).await
    }

    /// Continues frozen work with synchronous borrowed caller-task events.
    /// Revalidated publications emit completion without inventing current-attempt progress.
    /// Drop promises no terminal event, successful pause or rollback; see [`super::Observer`].
    pub async fn run_with<O: super::Observer>(
        self,
        observer: &mut O,
    ) -> Result<Report, Box<RunFailure>> {
        let result = self.run_observed(observer).await;
        observer.observe(super::Event::returned(&result));
        result
    }

    pub(super) async fn run_observed<O: super::Observer>(
        self,
        observer: &mut O,
    ) -> Result<Report, Box<RunFailure>> {
        let Self {
            client,
            saved,
            bindings,
            error,
            retained_bytes: _,
        } = self;
        let path = PathBuf::clone(&saved.path);
        if error.is_none() {
            for input in bindings.iter().flatten() {
                if let Err(error) = client.capability(input) {
                    return Err(Box::new(RunFailure {
                        error,
                        report: Report {
                            completed: Vec::new(),
                            saved_run: Some(path),
                        },
                        failed: None,
                    }));
                }
            }
        }
        let result = async {
            if let Some(error) = error {
                return Err(error);
            }
            if bindings.iter().any(Option::is_none) {
                return Err(SavedError::NeedsInput);
            }
            let owned = Owned::resume(saved).await?;
            let plan = owned.plan(bindings)?;
            Ok((owned, plan))
        }
        .await;
        match result {
            Ok((owned, plan)) => client.execute_owned(plan, owned, observer).await,
            Err(error) => Err(Box::new(RunFailure {
                error: Error::Saved(error),
                report: Report {
                    completed: Vec::new(),
                    saved_run: Some(path),
                },
                failed: None,
            })),
        }
    }
}

impl fmt::Debug for Resume<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Resume([redacted bindings])")
    }
}

/// Safe typed inspection/continuation failures. Arbitrary foreign errors and references are not stored.
#[derive(Debug, thiserror::Error)]
pub enum SavedError {
    /// No owned run record exists at the requested workspace.
    #[error("saved run is missing; inspect the actual run workspace path")]
    Missing,
    /// Bounds, normalization, roles or recorded root evidence failed validation.
    #[error("saved run is corrupt or its roles changed; retain it and choose a fresh destination")]
    Corrupt,
    /// The private record belongs to an unsupported codec version.
    #[error("saved run version is unsupported; use a compatible xget version")]
    Version,
    /// Native owned run persistence is not implemented on this host.
    #[error("saved runs are unsupported on this host; use checked live output")]
    Unsupported,
    /// No hidden runtime is created for offline filesystem inspection.
    #[error("runtime unavailable; inspect or continue inside a tokio runtime")]
    Runtime,
    /// Filesystem causes do not contain remote references.
    #[error("saved run io failed; retain the run and check its directory permissions")]
    Io(#[source] std::io::Error),
    /// Initial checkpoint failed after workspace creation, before native effects were admitted.
    #[error("saved run checkpoint failed; retain its workspace and inspect the cause")]
    Checkpoint {
        /// Actual workspace, possibly lacking a valid first record. This is not a successful pause.
        saved_run: PathBuf,
        /// Typed safe persistence cause.
        #[source]
        cause: Box<SavedError>,
    },
    /// Cooperating work still owns the store or native evidence.
    #[error("saved run is busy; wait for its physical operations to settle")]
    Busy,
    /// Original exact references must be re-supplied; they are never persisted as capabilities.
    #[error("saved run needs input; supply every inspected binding key")]
    NeedsInput,
    /// A reference/key cannot prove the exact frozen member binding.
    #[error(
        "input does not bind this frozen member; supply its original exact reference and namespace"
    )]
    Binding,
    /// Duplicate unfinished requests require explicit selection, never timestamp guessing.
    #[error("multiple unfinished runs match; inspect resume_paths and explicitly resume one")]
    Ambiguous {
        /// Actual owned run locators, not access-bearing references.
        resume_paths: Vec<PathBuf>,
    },
    /// A competing request became visible between the initial scan and owned admission.
    #[error(
        "unfinished work appeared during planning; explicitly inspect and resume its actual path"
    )]
    RequestAppeared {
        /// The existing run, never the now-stale newly discovered plan.
        resume_path: PathBuf,
    },
    /// Snapshot changes between inspection and ownership do not silently change execution authority.
    #[error("saved run changed after inspection; inspect it again before continuing")]
    Changed,
    /// Parent components could change the chosen root when combined with filesystem symlinks.
    #[error(
        "parent path components are unsupported; supply a resolved output root without dot-dot"
    )]
    ParentPath,
    /// The bounded store/record/attempt budget cannot be silently truncated.
    #[error(
        "saved run limit exceeded; use a new owned destination or retain fewer pending requests"
    )]
    Limit,
    /// Older records lack a reconstructable nonsecret recipe, or the supplied default binding differs.
    #[error(
        "saved binding needs original intent; supply the original scope, namespace and patterns explicitly"
    )]
    BindingIntentRequired,
    /// Missing attempt/intent or publication evidence cannot authorize a replacement or refetch.
    #[error("native publication is uncertain; retain the evidence and inspect the native attempt")]
    Uncertain,
    /// Equal foreign content, changed inode or wrong destination is not adopted.
    #[error("saved completion conflicts with its native evidence; choose a fresh destination")]
    Conflict,
    /// No hidden recovery hash is computed for nohash or weak-only completions.
    #[error(
        "saved completion is not revalidatable; use an authorized fresh fetch at a new destination"
    )]
    NotRevalidatable,
    /// Native reconciliation retains its concrete safe typed failure.
    #[error("saved native evidence failed; inspect the retained native failure")]
    Native(#[source] Box<crate::transfer::Failure<Infallible>>),
}

impl SavedError {
    /// Actual workspace for initial-checkpoint failure or a request that appeared during admission.
    /// A failed first checkpoint may not yet be inspectable; a directory alone is not a pause receipt.
    pub fn saved_run(&self) -> Option<&Path> {
        match self {
            Self::Checkpoint { saved_run, .. } => Some(saved_run),
            Self::RequestAppeared { resume_path } => Some(resume_path),
            _ => None,
        }
    }
}

// A request fingerprint compares intent only. It is neither resource identity nor a reopen reference.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) struct Fingerprint([u8; 32]);

impl Fingerprint {
    fn bytes(bytes: &[u8]) -> Self {
        let mut hash = OwnedSha256::default();
        hash.update(bytes);
        Self(hash.finish())
    }
    fn field(hash: &mut OwnedSha256, bytes: &[u8]) {
        hash.update(&(bytes.len() as u64).to_le_bytes());
        hash.update(bytes);
    }
    fn original(input: &Input) -> Self {
        let mut hash = OwnedSha256::default();
        Self::field(&mut hash, b"xget original reference only 1");
        Self::field(&mut hash, input.original_reference().as_bytes());
        Self(hash.finish())
    }

    fn reference(input: &Input) -> Self {
        let mut hash = OwnedSha256::default();
        Self::field(&mut hash, b"xget exact original binding 1");
        if input.kind() == crate::input::InputKind::S3 {
            Self::field(&mut hash, b"s3 scope 1");
            Self::field(
                &mut hash,
                match input.scope() {
                    crate::input::InputScope::Exact => b"exact",
                    crate::input::InputScope::Tree => b"tree",
                    _ => b"unsupported",
                },
            );
        }
        if input.kind() == crate::input::InputKind::Http && input.scope() == InputScope::Tree {
            Self::field(&mut hash, b"webdav tree scope 1");
        }
        Self::field(&mut hash, input.original_reference().as_bytes());
        Self::field(
            &mut hash,
            input
                .namespace()
                .map_or("", |value| value.as_str())
                .as_bytes(),
        );
        if let Some(selection) = &input.selection
            && selection.is_configured()
        {
            // Keep the old reference/request fingerprints byte-for-byte unchanged when no patterns
            // exist. Ordered role/count/length framing binds intent, not mutable discovery results.
            Self::field(&mut hash, b"original path selection intent 1");
            Self::field(&mut hash, b"includes");
            Self::field(
                &mut hash,
                &(selection.includes().count() as u64).to_le_bytes(),
            );
            for spelling in selection.includes() {
                Self::field(&mut hash, spelling.as_bytes());
            }
            Self::field(&mut hash, b"excludes");
            Self::field(
                &mut hash,
                &(selection.excludes().count() as u64).to_le_bytes(),
            );
            for spelling in selection.excludes() {
                Self::field(&mut hash, spelling.as_bytes());
            }
        }
        Self(hash.finish())
    }
    pub(super) fn request(
        inputs: &[Input],
        target: &Target,
        requirements: Requirements,
        measurement: Measurement,
    ) -> Result<Self, SavedError> {
        let mut hash = OwnedSha256::default();
        Self::field(&mut hash, b"xget exact request intent 1");
        for (index, input) in inputs.iter().enumerate() {
            Self::field(&mut hash, &Self::reference(input).0);
            if (matches!(
                input.kind(),
                crate::input::InputKind::SwissTransfer | crate::input::InputKind::S3
            ) || input.scope() == InputScope::Tree)
                && input.selected.is_some()
            {
                let group = inputs[..index]
                    .iter()
                    .position(|previous| Arc::ptr_eq(&previous.group, &input.group))
                    .unwrap_or(index);
                Self::field(&mut hash, b"frozen collection grouping 1");
                Self::field(&mut hash, &(group as u32).to_le_bytes());
            }
            if let Some(token) = &input.member {
                Self::field(
                    &mut hash,
                    if token.is_s3() {
                        b"s3 object key 1"
                    } else if token.is_webdav() {
                        b"webdav scoped href 1"
                    } else {
                        b"swiss member token 1"
                    },
                );
                Self::field(&mut hash, token.as_str().as_bytes());
            }
            // Selected-file requests carry a frozen session; do not merge with mutable discovery.
            if let Some(selected) = &input.selected {
                Self::field(&mut hash, b"selected");
                if input.kind() == crate::input::InputKind::S3 {
                    Self::field(&mut hash, b"s3 frozen version 1");
                    Self::field(
                        &mut hash,
                        input
                            .s3_version
                            .as_deref()
                            .map_or("", String::as_str)
                            .as_bytes(),
                    );
                    Self::field(&mut hash, selected.description.source_path.as_bytes());
                }
                // Advisory range support is not caller intent. Matching must still find an older
                // run when fresh metadata advertises different transport capabilities.
                match selected.description.length {
                    Length::Unknown => Self::field(&mut hash, b"unknown length"),
                    Length::Known(length) => Self::field(&mut hash, &length.0.to_le_bytes()),
                }
                Self::field(
                    &mut hash,
                    selected
                        .description
                        .etag
                        .as_deref()
                        .map_or("", String::as_str)
                        .as_bytes(),
                );
                Self::field(
                    &mut hash,
                    &codec::policy(selected.description.requirements, measurement),
                );
            } else {
                Self::field(&mut hash, b"input");
            }
        }
        let (tag, path) = match target {
            Target::Default => (0, Path::new(".")),
            Target::File(path) => (1, path.as_path()),
            Target::Directory(path) => (2, path.as_path()),
        };
        Self::field(&mut hash, &[tag]);
        Self::field(
            &mut hash,
            disk::absolute(path)?.as_os_str().as_encoded_bytes(),
        );
        Self::field(&mut hash, &codec::policy(requirements, measurement));
        Ok(Self(hash.finish()))
    }
}

struct Record {
    // V1 remains one exact HTTP file per input. It is read and checkpointed as V1, never
    // reinterpreted as collection state. V2 freezes N-input -> M-member bindings; V3 additionally
    // freezes reader capability without changing the meaning or encoding of either older version.
    version: u32,
    root: PathBuf,
    identity: RootIdentity,
    store_identity: RootIdentity,
    workspace_identity: RootIdentity,
    request: Option<Fingerprint>,
    complete: bool,
    members: Vec<Member>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct RootIdentity {
    device: RootDevice,
    inode: RootInode,
}
#[derive(Clone, Copy, Eq, PartialEq)]
struct RootDevice(u64);
#[derive(Clone, Copy, Eq, PartialEq)]
struct RootInode(u64);

struct FrozenMember {
    token: MemberToken,
    source_path: Zeroizing<String>,
    // V4 only: an S3 HEAD versionId, separate from opaque conditional ETag and key selector.
    s3_version: Option<Zeroizing<String>>,
}

struct FrozenIntent {
    original: Fingerprint,
    scope: InputScope,
    namespace: Option<Namespace>,
    selection: Option<Arc<Selection>>,
}

impl FrozenIntent {
    fn capture(input: &Input) -> Result<Option<Self>, SavedError> {
        if input.kind() != crate::input::InputKind::S3
            && input.scope() != InputScope::Tree
            && input.namespace().is_none()
            && input
                .selection
                .as_ref()
                .is_none_or(|selection| !selection.is_configured())
        {
            return Ok(None);
        }
        let namespace = input
            .namespace()
            .map(|namespace| {
                if namespace.as_str().len() > 4096 || namespace.as_str().split('/').count() > 64 {
                    return Err(SavedError::Limit);
                }
                Namespace::parse(namespace.as_str()).map_err(|_| SavedError::Corrupt)
            })
            .transpose()?;
        Ok(Some(Self {
            original: Fingerprint::original(input),
            scope: input.scope(),
            namespace,
            selection: input
                .selection
                .as_ref()
                .filter(|selection| selection.is_configured())
                .map(Arc::clone),
        }))
    }

    fn input(&self, reference: &str, binding: Fingerprint) -> Result<Input, SavedError> {
        let mut input = if self.scope == InputScope::Tree {
            Input::tree(reference)
        } else {
            Input::parse(reference)
        }
        .map_err(|_| SavedError::Binding)?;
        if Fingerprint::original(&input) != self.original || input.scope() != self.scope {
            return Err(SavedError::Binding);
        }
        if let Some(namespace) = &self.namespace {
            input = input
                .under(namespace.as_str())
                .map_err(|_| SavedError::Binding)?;
        }
        input.selection = self.selection.as_ref().map(Arc::clone);
        if Fingerprint::reference(&input) != binding {
            return Err(SavedError::Binding);
        }
        Ok(input)
    }
}

struct Member {
    occurrence: InputOccurrence,
    intent: Option<FrozenIntent>,
    remote: Option<FrozenMember>,
    binding: Fingerprint,
    destination: PathBuf,
    length: Length,
    etag: Option<Zeroizing<String>>,
    requirements: Requirements,
    measurement: Measurement,
    mode: super::range::Mode,
    attempts: Vec<Attempt>,
}

impl Member {
    fn accepts_selected(&self, input: &Input) -> bool {
        let token = self.remote.as_ref().map(|remote| &remote.token);
        if input.member.is_some() && input.member.as_ref() != token {
            return false;
        }
        let provider_matches = match &self.remote {
            Some(remote) if remote.token.is_s3() => input.kind() == crate::input::InputKind::S3,
            Some(remote) if remote.token.is_webdav() => {
                input.kind() == crate::input::InputKind::Http && input.scope() == InputScope::Tree
            }
            Some(_) => input.kind() == crate::input::InputKind::SwissTransfer,
            None => input.kind() == crate::input::InputKind::Http,
        };
        if !provider_matches {
            return false;
        }
        let Some(selected) = &input.selected else {
            return true;
        };
        let supplied = &selected.description;
        // Exact-reference equality alone does not authorize discarding a supplied frozen session.
        // Preserve the stored policy, rejecting newer or additional selected evidence before effects.
        // Fresh range hints do not change authority: reconstruction restores the recorded mode.
        self.remote
            .as_ref()
            .is_none_or(|remote| supplied.source_path.as_str() == remote.source_path.as_str())
            && self.remote.as_ref().is_none_or(|remote| {
                !remote.token.is_s3() || input.s3_version.as_deref() == remote.s3_version.as_deref()
            })
            && supplied.length == self.length
            && supplied.etag.as_deref() == self.etag.as_deref()
            && supplied.requirements.iter().all(|required| {
                self.requirements
                    .iter()
                    .any(|frozen| frozen.digest == required.digest)
            })
    }
}

struct Attempt {
    state: StateRef,
    status: AttemptStatus,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AttemptStatus {
    Submitted,
    Retained,
    Complete,
}

pub(super) struct Owned {
    saved: SavedRun,
    registration: OperationOwner,
    #[cfg(test)]
    before_completed_checkpoint: Option<fn(&Path)>,
}

impl Owned {
    pub(super) async fn create(
        plan: &Plan,
        request: Option<Fingerprint>,
        reentry: super::Reentry,
    ) -> Result<Self, SavedError> {
        disk::create(plan, request, reentry).await
    }
    async fn resume(saved: SavedRun) -> Result<Self, SavedError> {
        disk::resume(saved).await
    }
    pub(super) fn path(&self) -> &Path {
        &self.saved.path
    }
    fn plan(&self, bindings: Vec<Option<Input>>) -> Result<Plan, SavedError> {
        let mut files = Vec::with_capacity(bindings.len());
        for (input, member) in bindings.into_iter().zip(&self.saved.record.members) {
            let mut input = input.ok_or(SavedError::NeedsInput)?;
            // Resume::input already rejected incompatible selected evidence before ownership.
            // The stored session remains authoritative; compatible metadata grants no policy change.
            input.selected = None;
            input.s3_version = member
                .remote
                .as_ref()
                .and_then(|remote| Option::clone(&remote.s3_version));
            input.ranges = match member.mode {
                super::range::Mode::Whole => crate::input::RangeSupport::Unknown,
                super::range::Mode::Ranges => crate::input::RangeSupport::Bytes,
            };
            // Exact-reference equality was checked before ownership. Reconstitute only advisory
            // display metadata from that same supplied reference, never identity or selection.
            let remote_path = input.parsed_reference().path();
            let source_path = match &member.remote {
                Some(remote) => Zeroizing::clone(&remote.source_path),
                None => Zeroizing::new(
                    remote_path
                        .strip_prefix('/')
                        .unwrap_or(remote_path)
                        .to_owned(),
                ),
            };
            let name = remote_path
                .rsplit('/')
                .next()
                .filter(|name| !name.is_empty())
                .and_then(|name| {
                    percent_encoding::percent_decode_str(name)
                        .decode_utf8()
                        .ok()
                })
                .map(|name| Zeroizing::new(name.into_owned()));
            let name = if let Some(remote) = &member.remote {
                remote
                    .source_path
                    .rsplit('/')
                    .next()
                    .map(|name| Zeroizing::new(name.to_owned()))
            } else {
                name
            };
            files.push(PlannedFile {
                file: File {
                    input,
                    selected: Selected {
                        id: FileId::fresh(),
                        description: Description {
                            source_path,
                            name,
                            length: member.length,
                            etag: Option::clone(&member.etag),
                            requirements: member.requirements,
                        },
                    },
                },
                destination: PathBuf::clone(&member.destination),
                requirements: member.requirements,
                measurement: member.measurement,
            });
        }
        Ok(Plan {
            files,
            directory_root: None,
            directories: Vec::new(),
        })
    }
    pub(super) fn registration(&self) -> OperationOwner {
        Arc::clone(&self.registration)
    }
    pub(super) fn destinations(&self) -> impl Iterator<Item = &Path> {
        self.saved
            .record
            .members
            .iter()
            .map(|member| member.destination.as_path())
    }
    pub(super) async fn opened(&mut self, index: usize, file: &File) -> Result<(), SavedError> {
        let member = &mut self.saved.record.members[index];
        if member.remote.is_none() {
            return Ok(());
        }
        if member
            .remote
            .as_ref()
            .is_some_and(|remote| remote.token.is_webdav())
        {
            // Unpinned DAV is honestly a fresh whole response, not first-open snapshot evidence.
            // A discovered strong pin is already frozen before any native submission.
            if member.etag.as_deref() != file.selected.description.etag.as_deref()
                || member.length != file.length()
            {
                return Err(SavedError::Binding);
            }
            return Ok(());
        }
        let etag = file
            .selected
            .description
            .etag
            .as_ref()
            .ok_or(SavedError::Binding)?;
        if let Some(frozen) = &member.etag {
            if frozen != etag || file.length() != member.length {
                return Err(SavedError::Binding);
            }
            return Ok(());
        }
        member.etag = Some(Zeroizing::clone(etag));
        self.checkpoint().await
    }

    pub(super) async fn completed(&mut self, index: usize) -> Result<(), SavedError> {
        #[cfg(test)]
        if let Some(hook) = self.before_completed_checkpoint.take() {
            hook(self.path());
        }
        self.settled(index, AttemptStatus::Complete).await
    }
    pub(super) async fn retained(&mut self, index: usize) -> Result<(), SavedError> {
        self.settled(index, AttemptStatus::Retained).await
    }
    async fn checkpoint(&mut self) -> Result<(), SavedError> {
        let bytes = codec::encode(&self.saved.record)?;
        disk::checkpoint(PathBuf::clone(&self.saved.path), bytes, self.registration()).await
    }
    pub(super) async fn previous(&self, index: usize) -> Result<Option<Receipt>, SavedError> {
        let member = &self.saved.record.members[index];
        let Some(attempt) = member.attempts.last() else {
            return Ok(None);
        };
        let result = attempt
            .state
            .reconcile_registered(
                self.registration(),
                PathBuf::clone(&member.destination),
                CheckPolicy {
                    requirements: member.requirements,
                    measurement: member.measurement,
                },
            )
            .await
            .map_err(|failure| match &failure.kind {
                crate::transfer::FailureKind::Io { cause, .. }
                    if cause.kind() == io::ErrorKind::NotFound =>
                {
                    SavedError::Uncertain
                }
                crate::transfer::FailureKind::Busy => SavedError::Busy,
                _ => SavedError::Native(failure),
            })?;
        match result {
            // Return the acknowledged receipt before any aggregate checkpoint can fail. The
            // coordinator first places it in the partial report, just as for a fresh publication.
            Reconciliation::Published(receipt) => Ok(Some(receipt)),
            Reconciliation::NeedsBody { .. } if attempt.status != AttemptStatus::Complete => {
                Ok(None)
            }
            Reconciliation::NeedsBody { .. } | Reconciliation::PublicationUnknown(_) => {
                Err(SavedError::Uncertain)
            }
            Reconciliation::Conflict(_) => Err(SavedError::Conflict),
            Reconciliation::NotRevalidatable(_) => Err(SavedError::NotRevalidatable),
        }
    }
    pub(super) fn reader_state(&self, index: usize) -> Option<StateRef> {
        let member = &self.saved.record.members[index];
        if member.mode != super::range::Mode::Ranges {
            return None;
        }
        member
            .attempts
            .last()
            .map(|attempt| StateRef::clone(&attempt.state))
    }

    pub(super) async fn submit_reader(
        &mut self,
        index: usize,
        state: &StateRef,
    ) -> Result<(), SavedError> {
        let member = &mut self.saved.record.members[index];
        if member.mode != super::range::Mode::Ranges {
            return Err(SavedError::Binding);
        }
        if let Some(attempt) = member.attempts.last_mut() {
            if attempt.state != *state || attempt.status == AttemptStatus::Complete {
                return Err(SavedError::Uncertain);
            }
            // Continuation is the same owned native attempt, not a duplicate locator or authority
            // to replace older attempts. Record intent again before submitting any physical work.
            attempt.status = AttemptStatus::Submitted;
            return self.checkpoint().await;
        }
        self.submit(index, state).await
    }

    pub(super) async fn submit(
        &mut self,
        index: usize,
        state: &StateRef,
    ) -> Result<(), SavedError> {
        let attempts = &mut self.saved.record.members[index].attempts;
        if attempts.len() == MAX_ATTEMPTS {
            return Err(SavedError::Limit);
        }
        attempts.push(Attempt {
            state: StateRef::clone(state),
            status: AttemptStatus::Submitted,
        });
        self.checkpoint().await
    }
    async fn settled(&mut self, index: usize, status: AttemptStatus) -> Result<(), SavedError> {
        self.saved.record.members[index]
            .attempts
            .last_mut()
            .ok_or(SavedError::Corrupt)?
            .status = status;
        self.checkpoint().await
    }
    pub(super) async fn finish(&mut self) -> Result<(), SavedError> {
        self.saved.record.complete = true;
        self.checkpoint().await
    }
}

pub(super) async fn matching(
    root: PathBuf,
    request: Fingerprint,
) -> Result<Option<SavedRun>, SavedError> {
    disk::matching(root, request).await
}

pub(super) fn request_root(target: &Target) -> Result<PathBuf, SavedError> {
    disk::absolute(match target {
        Target::Default => Path::new("."),
        Target::Directory(path) => path,
        Target::File(path) => path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    })
}
