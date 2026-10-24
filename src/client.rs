//! Ordinary acquisition through the common inventory, planning and checked publication path.
//!
//! Exact HTTP inputs and, with `swisstransfer`, bounded SwissTransfer collections use the same serial
//! native Transfer implementation. Exact HTTP with known nonzero length, a bounded strong HEAD ETag
//! and explicit byte-range support uses Transfer's bounded reader coordinator. Other exact HTTP and
//! SwissTransfer retain ordered whole-body access. With `s3` and a caller-configured SDK, exact S3
//! objects and explicit literal bucket/prefix trees use ordered whole bodies and the same coordinator.
//! Client clones share reusable transports and bounded control/body admission, not a provider-owned
//! scheduler or the legacy range engine. Owned run metadata stays in the destination's `.xget-runs`
//! store. Unsupported redirects, catalogues and trees fail explicitly. S3 listing is not a snapshot:
//! each object's conditional HEAD/GET continuity is independent of other listed objects. Tree scopes
//! retain zero-byte slash-ending objects as inspection-only directory metadata. Nonempty slash-key
//! payloads fail explicitly rather than being dropped as folder markers. No empty directories are
//! restored; directory-only scopes have zero file jobs and create no output. Exact slash keys work
//! with an explicit file target. S3 and
//! SwissTransfer have no parallel ranges or partial-byte resume in this entrance.
//!
//! ```no_run
//! use xget::Client;
//! use xget::transfer::{Digest, Measurement};
//!
//! # async fn example() -> Result<(), Box<dyn core::error::Error>> {
//! let client = Client::new()?;
//! let required: Digest =
//!     "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".parse()?;
//! let report = client.download("https://example.org/abc.bin")
//!     .expect(required)
//!     .measurement(Measurement::None)
//!     .to_file("abc.bin")
//!     .run().await?;
//! assert_eq!(report.completed().len(), 1);
//! # Ok(())
//! # }
//! ```
//!
//! Run asynchronous calls on a caller-owned Tokio runtime with I/O and time drivers enabled.
//! A missing runtime handle is a typed error; Tokio cannot expose disabled driver capabilities through
//! Handle inspection, so configuring an active runtime correctly remains the caller's responsibility.
//! Explicit file parents must already be
//! suitable caller-owned directories; directory targets create their validated hierarchy at execution.
//! No preview GET is used when resolving metadata or planning output assignments.
//!
//! [`Download::run_with`], [`Client::execute_with`] and [`Resume::run_with`] borrow an [`Observer`]
//! on the executing task. Their default counterparts use the same pipeline with a no-op observer.
//! [`Event`] separates frozen metadata, exact [`ProgressFacts`] and terminal outcomes.
//! Counts reaching a declared length do not establish EOF, matched requirements or publication.
//! Reconciled publications do not invent current-attempt progress. Drop promises no terminal event.

use core::convert::Infallible;
use core::fmt;
use core::num::NonZeroUsize;
use std::io;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt as _;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use crate::checks::Requirements;
use crate::input::{Input, InputError, InputKind, InputScope};
use crate::inventory::{
    Discovered, File, FileId, Inventory, InventoryError, MAX_FILES, MAX_METADATA_BYTES, Selected,
};
pub use crate::transfer::live::{BodyError, BodyFailure, FinishError, Progress as LiveProgress};
use crate::transfer::{Conflict, Digest, Measurement, Provenance, Receipt, Requirement, Transfer};

mod builtin;
#[cfg(feature = "github")]
mod github;
mod http;
#[cfg(feature = "ipfs")]
mod ipfs;
mod layout;
mod live;
mod observation;
mod range;
#[cfg(feature = "s3")]
mod s3;
mod saved;
#[cfg(feature = "swisstransfer")]
mod swiss;
#[cfg(feature = "webdav")]
mod webdav;

pub use builtin::SourceError;
#[cfg(feature = "github")]
use github::Github;
#[cfg(feature = "github")]
pub use github::GithubError;
use http::Http;
pub use http::{HttpError, HttpErrorKind, RequestStage};
#[cfg(feature = "ipfs")]
use ipfs::Ipfs;
#[cfg(feature = "ipfs")]
pub use ipfs::IpfsError;
pub use live::{CheckedBody, LiveReceipt, OpenedFile};
use observation::FileProgress;
pub use observation::{Event, Observer, ProgressFacts};
#[cfg(feature = "s3")]
use s3::S3;
#[cfg(feature = "s3")]
pub use s3::{S3Error, S3ErrorKind};
pub use saved::{
    BindingKey, BindingKeyParseError, Fresh, Resume, RunId, SavedError, SavedMember, SavedRun,
    SavedStatus,
};
#[cfg(feature = "swisstransfer")]
use swiss::Swiss;
#[cfg(feature = "webdav")]
use webdav::WebDav;
#[cfg(feature = "webdav")]
pub use webdav::WebDavError;

#[cfg(test)]
#[path = "client/discovery_tests.rs"]
mod discovery_tests;
#[cfg(test)]
#[path = "client_tests.rs"]
mod tests;

/// Reusable native HTTP transport and a shared bound on active requests/response bodies.
///
/// Construction performs no network discovery and loads no `.env`, proxy or credential profile.
/// A configured S3 SDK retains its caller-selected credential/endpoint/retry policies; xget does not
/// replace those with HTTP defaults. See `ClientBuilder::s3_client` when `s3` is enabled.
/// A caller-owned Tokio runtime is required only when performing asynchronous transport/native work.
///
/// # Request deadlines
///
/// Each request has a fixed 15-second deadline covering shared-admission waiting, connection and
/// response headers together. Retained open bodies can consume all admission slots; drop or finish
/// consuming them before opening more work. Expiry can occur before any request is sent.
/// Payload consumption has no total or inactivity timeout. A caller may bound its own consumption
/// future; dropping it follows the documented body/directory/native-operation ownership rules.
pub struct Client {
    http: Arc<Http>,
}

impl Client {
    /// Builds a client without issuing requests. The default admits at most four active requests/bodies.
    pub fn new() -> Result<Self, Error> {
        Self::builder().build()
    }

    /// Starts deliberate configuration without discovery or run ownership.
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    /// Starts consumed continuation of an offline-inspected frozen run. Supply each member binding
    /// with [`Resume::input`]; credentials/reopen references are never loaded from saved metadata.
    pub fn resume(&self, saved: SavedRun) -> Resume<'_> {
        Resume::new(self, saved)
    }

    /// Starts one pending input. Conversion failures remain sticky until plan/run is awaited.
    /// Materialization collections select all members by default, unless a literal member token is
    /// requested. The optional `github` capability resolves public release alternatives without an
    /// implicit first-asset choice. Acquisition requires member/path selection or explicit all.
    pub fn download<T>(&self, input: T) -> Download<'_>
    where
        T: TryInto<Input>,
        T::Error: Into<Error>,
    {
        self.download_all(core::iter::once(input))
    }

    /// Adds requested work, never inferring mirrors or silently merging duplicate occurrences.
    ///
    /// Empty input iterators are invalid requests, not manufactured complete empty inventories.
    /// Input count and retained request metadata are bounded before any metadata request is sent.
    pub fn download_all<I, T>(&self, inputs: I) -> Download<'_>
    where
        I: IntoIterator<Item = T>,
        T: TryInto<Input>,
        T::Error: Into<Error>,
    {
        let inputs = Self::collect_inputs(inputs);
        Download {
            client: self,
            inputs,
            target: Target::Default,
            requirements: Ok(Requirements::default()),
            measurement: Measurement::default(),
            reentry: Reentry::Continue,
            publication: crate::transfer::Publication::Absent,
        }
    }

    fn collect_inputs<I, T>(inputs: I) -> Result<Vec<Input>, Error>
    where
        I: IntoIterator<Item = T>,
        T: TryInto<Input>,
        T::Error: Into<Error>,
    {
        let mut retained = 0_usize;
        let mut collected = Vec::new();
        for input in inputs {
            if collected.len() == MAX_FILES {
                return Err(Error::Inventory(InventoryError::Limit));
            }
            let input = input.try_into().map_err(Into::into)?;
            retained = retained.saturating_add(input.retained_bytes());
            if retained > MAX_METADATA_BYTES {
                return Err(Error::Inventory(InventoryError::Limit));
            }
            collected.push(input);
        }
        if collected.is_empty() {
            return Err(Error::NoInputs);
        }
        Ok(collected)
    }

    /// Resolves bounded advisory metadata, never a preview payload GET.
    /// The [request deadline](Client#request-deadlines) includes admission waiting.
    ///
    /// Selected Files preserve their frozen metadata. Exact HTTP uses HEAD; the optional SwissTransfer
    /// adapter uses a bounded share-page metadata GET and preserves Collection at every cardinality.
    /// Configured S3 uses HEAD for exact objects and bounded ListObjectsV2 followed by conditional
    /// per-object HEADs for explicit trees. It preserves known versionId and opaque strong ETag
    /// separately from key selectors and digests. Listing has no point-in-time snapshot claim.
    /// Tree source paths are relative to the prefix's last complete slash ancestor: `dir/fo` lists
    /// `dir/foo` as `foo`, while selectors always retain `dir/foo`. No key normalization occurs.
    /// At most 128 pages are accepted, with nonempty nonrepeating continuation tokens up to 8192 bytes;
    /// object keys/version IDs allow 1024 bytes and ETags 8192 bytes. All calls share body admission.
    /// Unsupported providers/trees return errors, not fake inventories. HTTP 405/501 leaves a usable
    /// exact unpinned reference without probing GET. Limits are 128 combined files/directory entries
    /// and 1 MiB accounted inventory text; exceeding either fails discovery, never returning a prefix.
    /// Zero-byte S3 slash markers remain inspectable through Inventory::directories, not file jobs.
    /// File path patterns do not remove directory facts; directory-only scopes are zero-file no-ops.
    /// Input-local includes/exclusions apply to original source-relative paths only after complete
    /// discovery and accounting. Every positive criterion must match the complete file scope before
    /// exclusions, including duplicate criteria. A positive on an empty scope or nonempty files
    /// filtered to none returns [`Error::PathSelection`]. Exclusion-only/unconfigured genuine empty
    /// collections remain empty. See [`Input::include`] for syntax and fixed budgets.
    pub async fn resolve<T>(&self, input: T) -> Result<Inventory, Error>
    where
        T: TryInto<Input>,
        T::Error: Into<Error>,
    {
        self.resolve_all(core::iter::once(input)).await
    }

    /// Resolves a complete multi-input inventory under one original-input, metadata and time budget.
    ///
    /// At most 128 original inputs and 128 combined discovered file/directory entries are accepted,
    /// with 1 MiB accounted text and one 15-second complete-discovery deadline. Inputs retain separate
    /// original occurrences and grouping, even when references are equal. No payload GET or output
    /// creation occurs. Each positive input-local include must match its complete executable-file
    /// scope before exclusions; a positive criterion on an empty scope is unmatched. Directory facts
    /// are inspection-only and cannot satisfy a file include. See [`Self::resolve`] for transport limits.
    pub async fn resolve_all<I, T>(&self, inputs: I) -> Result<Inventory, Error>
    where
        I: IntoIterator<Item = T>,
        T: TryInto<Input>,
        T::Error: Into<Error>,
    {
        self.resolve_inputs(Self::collect_inputs(inputs)?).await
    }

    async fn resolve_input(&self, input: Input) -> Result<Inventory, Error> {
        self.resolve_inputs(Self::collect_inputs(core::iter::once(input))?)
            .await
    }

    async fn resolve_inputs(&self, inputs: Vec<Input>) -> Result<Inventory, Error> {
        let retained = inputs.iter().fold(0_usize, |bytes, input| {
            bytes.saturating_add(input.retained_bytes())
        });
        let mut budget = DiscoveryBudget {
            files: MAX_FILES,
            bytes: MAX_METADATA_BYTES
                .checked_sub(retained)
                .ok_or(Error::Inventory(InventoryError::Limit))?,
        };
        for input in &inputs {
            Self::supported(input)?;
            self.capability(input)?;
        }
        let needs_metadata = inputs.iter().any(|input| input.selected.is_none());
        if needs_metadata {
            Http::runtime()?;
        }
        let discovery = async {
            let mut discovered = Discovered::default();
            let mut intents = Vec::new();
            let mut members = Vec::new();
            for (index, mut input) in inputs.into_iter().enumerate() {
                input.occurrence = crate::input::InputOccurrence(index as u32);
                if let Some(selection) = input.pending_includes()? {
                    intents.push(PathIntent {
                        occurrence: input.occurrence,
                        selection,
                    });
                }
                if input.selected.is_none()
                    && let Some(token) = &input.member
                {
                    members.push((input.occurrence, crate::input::MemberToken::clone(token)));
                }
                budget.bytes += input.retained_bytes();
                let scope = self.resolve_bounded(input, &mut budget).await?;
                discovered.files.extend(scope.files);
                discovered.directories.extend(scope.directories);
            }
            Ok::<_, Error>((discovered, intents, members))
        };
        let (discovered, intents, members) = if needs_metadata {
            tokio::time::timeout(core::time::Duration::from_secs(15), discovery)
                .await
                .map_err(|_| Error::DiscoveryTimeout)??
        } else {
            discovery.await?
        };
        Self::select_paths(discovered, intents, members)
    }

    // Resolve, live opening and Download planning share this one post-discovery selection seam.
    // Account the COMPLETE scope before filtering; never manufacture an executable truncated scope.
    fn select_paths(
        discovered: Discovered,
        intents: Vec<PathIntent>,
        members: Vec<(crate::input::InputOccurrence, crate::input::MemberToken)>,
    ) -> Result<Inventory, Error> {
        let mut inventory = if discovered.directories.is_empty() {
            Inventory::complete(discovered.files)?
        } else {
            Inventory::discovered(discovered)?
        };
        // Resolve literal identity against complete metadata, not a path-filtered subset. An
        // existing member excluded by another criterion is unmatched intent, not a stale identity.
        for (occurrence, token) in &members {
            let file_match = inventory.files().any(|file| {
                file.input.occurrence == *occurrence && file.member_token() == Some(token)
            });
            let marker_match = token.is_s3()
                && inventory.directories().any(|directory| {
                    directory.input.occurrence == *occurrence
                        && directory.member_token() == Some(token)
                });
            if !file_match && !marker_match {
                return Err(Error::Selection);
            }
        }
        let mut requested = Vec::new();
        for file in inventory.files() {
            if file
                .input
                .selection
                .as_ref()
                .is_some_and(|selection| selection.is_configured())
            {
                if matches!(
                    file.input.kind(),
                    InputKind::Http | InputKind::S3 | InputKind::Ipfs
                ) && file.input.scope() == InputScope::Exact
                {
                    // Exact remote ancestors are routing data, not local destination components.
                    if !crate::selection::Selection::path_is_bounded(file.source_path()) {
                        return Err(Error::Inventory(InventoryError::Limit));
                    }
                } else {
                    layout::Layout::source_path(file.source_path())?;
                }
                if !requested.contains(&file.input.occurrence) {
                    requested.push(file.input.occurrence);
                }
            }
        }
        // Keep original immutable intent even when discovery produced no files. Test every positive
        // criterion on the complete per-occurrence file scope, before exclusions remove any matches.
        for intent in intents {
            if !intent.selection.all_includes_match(
                inventory
                    .files()
                    .filter(|file| file.input.occurrence == intent.occurrence)
                    .map(File::source_path),
            ) {
                return Err(Error::PathSelection);
            }
        }
        inventory.retain(|file| {
            file.input
                .selection
                .as_ref()
                .is_none_or(|selection| selection.matches(file.source_path()))
        });
        for (occurrence, token) in members {
            inventory.retain(|file| {
                file.input.occurrence != occurrence || file.member_token() == Some(&token)
            });
        }
        // A genuine empty discovery contributed no requested occurrence. In a mixed request, one
        // input's matches cannot hide another nonempty input whose local intent selected nothing.
        if requested.iter().any(|occurrence| {
            !inventory
                .files()
                .any(|file| file.input.occurrence == *occurrence)
        }) {
            return Err(Error::PathSelection);
        }
        inventory.freeze_path_selection();
        Ok(inventory)
    }

    async fn resolve_bounded(
        &self,
        mut input: Input,
        budget: &mut DiscoveryBudget,
    ) -> Result<Discovered, Error> {
        Self::supported(&input)?;
        self.capability(&input)?;
        if input.retained_bytes() > MAX_METADATA_BYTES {
            return Err(Error::Inventory(InventoryError::Limit));
        }
        if let Some(selected) = input.selected.take() {
            let file = File {
                input,
                selected: *selected,
            };
            budget.reserve(&file)?;
            return Ok(vec![file].into());
        }
        #[cfg(feature = "github")]
        if input.kind() == InputKind::GithubRelease {
            return Github::resolve(&self.http, input, budget).await;
        }
        #[cfg(feature = "swisstransfer")]
        if input.kind() == InputKind::SwissTransfer {
            return Swiss::resolve(&self.http, input, budget)
                .await
                .map(Into::into);
        }
        #[cfg(feature = "ipfs")]
        if input.kind() == InputKind::Ipfs {
            let ipfs = self.http.ipfs.as_ref().ok_or(Error::IpfsUnavailable)?;
            return ipfs.resolve(&self.http, input, budget).await;
        }
        if input.kind() == InputKind::S3 {
            #[cfg(feature = "s3")]
            {
                let s3 = self.http.s3.as_ref().ok_or(Error::S3Unavailable)?;
                return s3.resolve(&self.http, input, budget).await;
            }
            #[cfg(not(feature = "s3"))]
            return Err(Error::S3Unavailable);
        }
        #[cfg(feature = "webdav")]
        if input.kind() == InputKind::Http && input.scope() == InputScope::Tree {
            return WebDav::resolve(&self.http, input, budget).await;
        }
        let description = self.http.describe(&mut input).await?;
        let file = File {
            input,
            selected: Selected {
                id: FileId::fresh(),
                description,
            },
        };
        budget.reserve(&file)?;
        Ok(vec![file].into())
    }

    fn capability(&self, input: &Input) -> Result<(), Error> {
        if input.kind() == InputKind::Ipfs {
            #[cfg(feature = "ipfs")]
            if self.http.ipfs.is_some() {
                return Ok(());
            }
            return Err(Error::IpfsUnavailable);
        }
        if input.kind() != InputKind::S3 {
            return Ok(());
        }
        #[cfg(feature = "s3")]
        if self.http.s3.is_some() {
            return Ok(());
        }
        Err(Error::S3Unavailable)
    }

    fn supported(input: &Input) -> Result<(), Error> {
        #[cfg(feature = "github")]
        if input.kind() == InputKind::GithubRelease && input.scope() == InputScope::Catalogue {
            return Ok(());
        }
        if matches!(input.kind(), InputKind::S3 | InputKind::Ipfs) {
            return Ok(());
        }
        #[cfg(feature = "swisstransfer")]
        if input.kind() == InputKind::SwissTransfer && input.scope() == InputScope::Collection {
            return Ok(());
        }
        #[cfg(feature = "webdav")]
        if input.kind() == InputKind::Http && input.scope() == InputScope::Tree {
            return Ok(());
        }
        if input.kind() != InputKind::Http || input.scope() != InputScope::Exact {
            return Err(Error::UnsupportedInput {
                kind: input.kind(),
                scope: input.scope(),
            });
        }
        Ok(())
    }

    /// Consumes one exact GET response. Opening does not publish or poll payload bytes.
    /// The [request deadline](Client#request-deadlines) bounds admission/connect/headers, not payload.
    ///
    /// The owning body holds request admission until EOF, source failure or drop. A caller that stops
    /// consuming must drop the opened file/body to release capacity. No hidden reopen occurs.
    pub async fn open<T>(&self, input: T) -> Result<OpenedFile, Error>
    where
        T: TryInto<Input>,
        T::Error: Into<Error>,
    {
        let input = input.try_into().map_err(Into::into)?;
        layout::Layout::alternatives(&input)?;
        let mut files: Vec<_> = self.resolve_input(input).await?.into_files().collect();
        if files.len() != 1 {
            return Err(Error::FileCardinality);
        }
        let mut file = files.pop().ok_or(Error::FileCardinality)?;
        let opened = self.open_file(&mut file).await?;
        Ok(OpenedFile::new(file, opened))
    }

    async fn open_file(&self, file: &mut File) -> Result<builtin::Opened, Error> {
        #[cfg(feature = "github")]
        if file.input.kind() == InputKind::GithubRelease {
            return Github::open(&self.http, file).await.map(Into::into);
        }
        #[cfg(feature = "ipfs")]
        if file.input.kind() == InputKind::Ipfs {
            return self
                .http
                .ipfs
                .as_ref()
                .ok_or(Error::IpfsUnavailable)?
                .open(&self.http, file)
                .await
                .map(Into::into);
        }
        if file.input.kind() == InputKind::S3 {
            #[cfg(feature = "s3")]
            {
                return self
                    .http
                    .s3
                    .as_ref()
                    .ok_or(Error::S3Unavailable)?
                    .open(&self.http, file)
                    .await;
            }
            #[cfg(not(feature = "s3"))]
            return Err(Error::S3Unavailable);
        }
        #[cfg(feature = "swisstransfer")]
        if file.input.kind() == InputKind::SwissTransfer {
            return Swiss::open(&self.http, file).await.map(Into::into);
        }
        #[cfg(feature = "webdav")]
        if file.input.kind() == InputKind::Http && file.input.scope() == InputScope::Tree {
            return WebDav::open(&self.http, file).await.map(Into::into);
        }
        self.http.open(file).await.map(Into::into)
    }

    /// Executes frozen assignments serially using actual checked native Transfer publication.
    ///
    /// All lexical layout/check conflicts are resolved before this call can issue its first GET.
    /// Output directories may be created here, never during planning. Existing roots/components
    /// must be nonsymlink directories owned by the effective user, with owner rwx and no group/other
    /// write bits. New directories request mode 0700. The caller must control the hierarchy against
    /// concurrent replacement; these static checks are not a same-user sandbox. Publication is absent-only;
    /// no overwrite is granted. Execution accounts frozen members and advisory native attempts in an
    /// owned aggregate SavedRun before native effects. Eligible exact HTTP uses up to four ranges;
    /// continuation rehashes an owned strong prefix before suffix admission. Unsupported/unpinned
    /// exact HTTP and SwissTransfer keep fresh full ordered bodies. Missing anchors and closed reader
    /// stages refuse continuation, never silently refetch. Checkpoints are atomic, not fsync-durable.
    /// Dropping during directory preparation can leave empty directories and submitted Tokio filesystem
    /// work settling after drop; there is no high-level pause or directory-settlement receipt.
    /// See [`Resume`'s ownership and cancellation contract](Resume#ownership-and-cancellation) for
    /// same-root aggregate exclusion and native/manifest ownership through submitted-work settlement.
    pub async fn execute(&self, plan: Plan) -> Result<Report, Box<RunFailure>> {
        self.execute_with(plan, &mut ()).await
    }

    /// Executes frozen assignments with synchronous borrowed caller-task events.
    /// The observer may borrow local state. Drop promises no terminal event or rollback;
    /// see [`Observer`] and [ownership and cancellation](Resume#ownership-and-cancellation).
    pub async fn execute_with<O: Observer>(
        &self,
        plan: Plan,
        observer: &mut O,
    ) -> Result<Report, Box<RunFailure>> {
        let result = self
            .execute_with_request(plan, None, Reentry::Fresh, Authority::default(), observer)
            .await;
        observer.observe(Event::returned(&result));
        result
    }

    async fn execute_with_request<O: Observer>(
        &self,
        mut plan: Plan,
        request: Option<saved::Fingerprint>,
        reentry: Reentry,
        authority: Authority,
        observer: &mut O,
    ) -> Result<Report, Box<RunFailure>> {
        let report = Report::default();
        if let Err(error) = Http::runtime() {
            return Err(Box::new(RunFailure {
                error,
                report,
                failed: None,
            }));
        }
        if cfg!(not(unix)) {
            return Err(Box::new(RunFailure {
                error: Error::NativeUnsupported,
                report,
                failed: None,
            }));
        }
        if plan.files.is_empty() {
            observer.observe(Event::RunStarted {
                run: None,
                saved_run: None,
                files: 0,
            });
            return Ok(report);
        }
        for planned in &plan.files {
            if let Err(error) = self.capability(&planned.file.input) {
                return Err(Box::new(RunFailure {
                    error,
                    report,
                    failed: None,
                }));
            }
        }
        if let Err(error) = Self::prepare_directories(&plan).await {
            return Err(Box::new(RunFailure {
                error,
                report,
                failed: None,
            }));
        }
        let owned = saved::Owned::create(&plan, request, reentry)
            .await
            .map_err(|error| {
                let report = Report {
                    completed: Vec::new(),
                    saved_run: error.saved_run().map(Path::to_path_buf),
                };
                Box::new(RunFailure {
                    error: Error::Saved(error),
                    report,
                    failed: None,
                })
            })?;
        // Persistence freezes canonical absolute assignments, so later cwd changes cannot redirect
        // either native work or inspection. This does not repeat remote discovery or local layout.
        for (file, member) in plan.files.iter_mut().zip(owned.destinations()) {
            file.destination = member.to_path_buf();
        }
        self.execute_owned(plan, owned, authority, observer).await
    }

    async fn execute_owned<O: Observer>(
        &self,
        plan: Plan,
        mut owned: saved::Owned,
        authority: Authority,
        observer: &mut O,
    ) -> Result<Report, Box<RunFailure>> {
        let mut report = Report {
            completed: Vec::new(),
            saved_run: Some(owned.path().to_path_buf()),
        };
        let run = RunId::for_path(owned.path());
        observer.observe(Event::RunStarted {
            run: Some(run),
            saved_run: Some(owned.path()),
            files: plan.files.len(),
        });
        let targets: Arc<[PathBuf]> = owned.destinations().map(Path::to_path_buf).collect();
        for (index, planned) in plan.files.into_iter().enumerate() {
            let Some(key) = BindingKey::for_run_member(run, index) else {
                return Err(Box::new(RunFailure {
                    error: Error::Saved(SavedError::Limit),
                    report,
                    failed: Some(planned),
                }));
            };
            observer.observe(Event::FileStarted {
                key,
                planned: &planned,
            });
            let PlannedFile {
                mut file,
                destination,
                requirements,
                measurement,
            } = planned;
            match owned.previous(index).await {
                Ok(Some(receipt)) => {
                    report.completed.push(PublishedFile {
                        file,
                        destination,
                        version: VersionConsistency::LocallyRevalidated,
                        receipt: PublicationReceipt::Whole(receipt),
                    });
                    observer.observe(Event::FileCompleted {
                        key,
                        published: &report.completed[index],
                    });
                    if let Err(error) = owned.completed(index).await {
                        return Err(Box::new(RunFailure {
                            error: Error::Saved(error),
                            report,
                            failed: None,
                        }));
                    }
                    continue;
                }
                Ok(None) => {}
                Err(error) => {
                    return Err(Box::new(RunFailure {
                        error: Error::Saved(error),
                        report,
                        failed: Some(PlannedFile {
                            file,
                            destination,
                            requirements,
                            measurement,
                        }),
                    }));
                }
            }
            if range::Mode::of(&file) == range::Mode::Ranges {
                let receipt = match range::run(
                    &self.http,
                    &file,
                    &destination,
                    requirements,
                    measurement,
                    &mut owned,
                    index,
                    authority,
                    Arc::clone(&targets),
                    &mut FileProgress {
                        observer: &mut *observer,
                        key,
                    },
                )
                .await
                {
                    Ok(receipt) => receipt,
                    Err(error) => {
                        return Err(Box::new(RunFailure {
                            error,
                            report,
                            failed: Some(PlannedFile {
                                file,
                                destination,
                                requirements,
                                measurement,
                            }),
                        }));
                    }
                };
                // Preserve the native receipt and attempt statistics before completion checkpointing.
                report.completed.push(PublishedFile {
                    file,
                    destination,
                    version: VersionConsistency::StrongEtagMatched,
                    receipt: PublicationReceipt::Ranges(receipt),
                });
                observer.observe(Event::FileCompleted {
                    key,
                    published: &report.completed[index],
                });
                if let Err(error) = owned.completed(index).await {
                    return Err(Box::new(RunFailure {
                        error: Error::Saved(error),
                        report,
                        failed: None,
                    }));
                }
                continue;
            }
            let opened = match self.open_file(&mut file).await {
                Ok(opened) => opened,
                Err(error) => {
                    return Err(Box::new(RunFailure {
                        error,
                        report,
                        failed: Some(PlannedFile {
                            file,
                            destination,
                            requirements,
                            measurement,
                        }),
                    }));
                }
            };
            // The first actual provider response establishes the frozen pin. Checkpoint it before
            // any native request can create state, so a later lease renewal cannot choose latest.
            if let Err(error) = owned.opened(index, &file).await {
                return Err(Box::new(RunFailure {
                    error: Error::Saved(error),
                    report,
                    failed: Some(PlannedFile {
                        file,
                        destination,
                        requirements,
                        measurement,
                    }),
                }));
            }
            let version = opened.version;
            let mut transfer =
                Transfer::from_body(opened.body, opened.length).measurement(measurement);
            for requirement in requirements.iter() {
                for provenance in requirement.origins.iter() {
                    transfer = transfer.require(requirement.digest, provenance);
                }
            }
            let pending = match transfer.to_file(&destination) {
                Ok(pending) => pending,
                Err(cause) => {
                    return Err(Box::new(RunFailure {
                        error: Error::NativeRequest(cause),
                        report,
                        failed: Some(PlannedFile {
                            file,
                            destination,
                            requirements,
                            measurement,
                        }),
                    }));
                }
            };
            let restart = match authority.attempt {
                AttemptPolicy::Continue => None,
                AttemptPolicy::Restart => match owned.restart_state(index) {
                    Ok(state) => state,
                    Err(error) => {
                        return Err(Box::new(RunFailure {
                            error: Error::Saved(error),
                            report,
                            failed: Some(PlannedFile {
                                file,
                                destination,
                                requirements,
                                measurement,
                            }),
                        }));
                    }
                },
            };
            let pending = pending
                .authorized(authority.publication, restart)
                .protect_targets(Arc::clone(&targets));
            if let Err(error) = owned.submit(index, pending.state()).await {
                return Err(Box::new(RunFailure {
                    error: Error::Saved(error),
                    report,
                    failed: Some(PlannedFile {
                        file,
                        destination,
                        requirements,
                        measurement,
                    }),
                }));
            }
            match pending
                .run_registered_observed(
                    owned.registration(),
                    &mut FileProgress {
                        observer: &mut *observer,
                        key,
                    },
                )
                .await
            {
                Ok(receipt) => {
                    report.completed.push(PublishedFile {
                        file,
                        destination,
                        version,
                        receipt: PublicationReceipt::Whole(receipt),
                    });
                    observer.observe(Event::FileCompleted {
                        key,
                        published: &report.completed[index],
                    });
                    if let Err(error) = owned.completed(index).await {
                        return Err(Box::new(RunFailure {
                            error: Error::Saved(error),
                            report,
                            failed: None,
                        }));
                    }
                }
                Err(failure) => {
                    if let Err(error) = owned.retained(index).await {
                        return Err(Box::new(RunFailure {
                            error: Error::RunCheckpoint {
                                transfer: failure,
                                checkpoint: error,
                            },
                            report,
                            failed: Some(PlannedFile {
                                file,
                                destination,
                                requirements,
                                measurement,
                            }),
                        }));
                    }
                    return Err(Box::new(RunFailure {
                        error: Error::Transfer(failure),
                        report,
                        failed: Some(PlannedFile {
                            file,
                            destination,
                            requirements,
                            measurement,
                        }),
                    }));
                }
            }
        }
        if let Err(error) = owned.finish().await {
            return Err(Box::new(RunFailure {
                error: Error::Saved(error),
                report,
                failed: None,
            }));
        }
        Ok(report)
    }

    async fn prepare_directories(plan: &Plan) -> Result<(), Error> {
        let Some(root) = &plan.directory_root else {
            for file in &plan.files {
                let parent = file
                    .destination
                    .parent()
                    .filter(|parent| !parent.as_os_str().is_empty())
                    .unwrap_or(Path::new("."));
                let metadata = tokio::fs::symlink_metadata(parent)
                    .await
                    .map_err(Error::Directory)?;
                Self::directory_kind(&metadata)?;
            }
            return Ok(());
        };
        match tokio::fs::symlink_metadata(root).await {
            Ok(metadata) => Self::directory_kind(&metadata)?,
            Err(cause) if cause.kind() == io::ErrorKind::NotFound => {
                // The explicit root is caller authority. Only namespace components below it are
                // untrusted relative assignments, and those are created one checked component at a time.
                let mut builder = tokio::fs::DirBuilder::new();
                builder.recursive(true);
                #[cfg(unix)]
                builder.mode(0o700);
                builder.create(root).await.map_err(Error::Directory)?;
                let metadata = tokio::fs::symlink_metadata(root)
                    .await
                    .map_err(Error::Directory)?;
                Self::directory_kind(&metadata)?;
            }
            Err(cause) => return Err(Error::Directory(cause)),
        }
        for directory in &plan.directories {
            let relative = directory
                .strip_prefix(root)
                .map_err(|_| Error::UnsafeDirectory)?;
            let mut prefix = PathBuf::clone(root);
            for component in relative.components() {
                let Component::Normal(component) = component else {
                    return Err(Error::UnsafeDirectory);
                };
                prefix.push(component);
                match tokio::fs::symlink_metadata(&prefix).await {
                    Ok(metadata) => Self::directory_kind(&metadata)?,
                    Err(cause) if cause.kind() == io::ErrorKind::NotFound => {
                        let mut builder = tokio::fs::DirBuilder::new();
                        builder.recursive(false);
                        #[cfg(unix)]
                        builder.mode(0o700);
                        match builder.create(&prefix).await {
                            Ok(()) => {}
                            Err(cause) if cause.kind() == io::ErrorKind::AlreadyExists => {}
                            Err(cause) => return Err(Error::Directory(cause)),
                        }
                        let metadata = tokio::fs::symlink_metadata(&prefix)
                            .await
                            .map_err(Error::Directory)?;
                        Self::directory_kind(&metadata)?;
                    }
                    Err(cause) => return Err(Error::Directory(cause)),
                }
            }
        }
        Ok(())
    }

    fn directory_kind(metadata: &std::fs::Metadata) -> Result<(), Error> {
        if !metadata.file_type().is_dir() {
            return Err(Error::UnsafeDirectory);
        }
        #[cfg(unix)]
        {
            // SAFETY: geteuid reads process identity and takes no pointers or resources.
            let uid = unsafe { libc::geteuid() };
            if metadata.uid() != uid
                || metadata.mode() & 0o022 != 0
                || metadata.mode() & 0o700 != 0o700
            {
                return Err(Error::UnsafeDirectory);
            }
        }
        // These preflight checks refuse existing symlink roles. The caller must still control the
        // hierarchy against concurrent replacement; this is not a descriptor-anchored sandbox.
        Ok(())
    }
}

impl Clone for Client {
    fn clone(&self) -> Self {
        Self {
            http: Arc::clone(&self.http),
        }
    }
}

impl fmt::Debug for Client {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Client([shared http pool and admission])")
    }
}

/// Deliberate shared request/body capacity configuration, not a process-global scheduler.
#[must_use]
pub struct ClientBuilder {
    max_requests: NonZeroUsize,
    #[cfg(feature = "s3")]
    s3: Option<S3>,
    #[cfg(feature = "ipfs")]
    ipfs: Option<Ipfs>,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            max_requests: NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
            #[cfg(feature = "s3")]
            s3: None,
            #[cfg(feature = "ipfs")]
            ipfs: None,
        }
    }
}

impl ClientBuilder {
    /// Caps active metadata/control requests and open payload bodies together across clones.
    /// Waiting for capacity uses the same 15-second deadline as connection and headers;
    /// see [request deadlines](Client#request-deadlines). This does not add a payload timeout.
    pub fn max_requests(mut self, maximum: NonZeroUsize) -> Self {
        self.max_requests = maximum;
        self
    }

    /// Installs a caller-configured reusable S3 SDK client without loading AWS configuration.
    ///
    /// Credentials, endpoint, retry, checksum-validation and connection policies remain the caller
    /// SDK's policies, not the HTTP adapter's defaults. Each SDK metadata call/page and GET opening
    /// has a fixed 15-second async deadline including common admission. Complete discovery also has
    /// a 15-second deadline. These cancel awaited futures, not synchronous parser CPU work or a
    /// caller credential provider's separately spawned work. Payload reading has no added deadline.
    /// A separate fixed 2 MiB metadata HTTP-body limit is installed before the SDK's collector/XML
    /// parser, including chunked pages. Oversized declared listing/error lengths fail before polling.
    /// HEAD Content-Length describes the object and is not mistaken for a metadata body length.
    /// Successful GET payloads remain unbounded streaming. Common decoded count/text limits still
    /// apply independently; neither bound claims total SDK/transport RSS or parser CPU preemption.
    /// Each listing asks for 129 keys, but that request hint is not trusted as a response-size bound.
    /// No SDK is constructed implicitly and no profiles, environment or IMDS are consulted here.
    #[cfg(feature = "s3")]
    pub fn s3_client(mut self, client: aws_sdk_s3::Client) -> Self {
        self.s3 = Some(S3::from(client));
        self
    }

    /// Sets the exact-IPFS HTTP gateway without reading environment or local daemon configuration.
    /// Gateway access is independent of content identity. Redirects and IPFS tree traversal are not
    /// enabled; raw CID requirements remain mandatory even with measurement disabled.
    #[cfg(feature = "ipfs")]
    pub fn ipfs_gateway(mut self, gateway: &str) -> Result<Self, IpfsError> {
        self.ipfs = Some(Ipfs::new(gateway)?);
        Ok(self)
    }

    /// Builds reusable transports without network or filesystem effects.
    pub fn build(self) -> Result<Client, Error> {
        #[allow(unused_mut)] // Optional source configuration is installed only with its capability.
        let mut http = Http::new(self.max_requests)?;
        #[cfg(feature = "s3")]
        {
            http.s3 = self.s3.map(Arc::new);
        }
        #[cfg(feature = "ipfs")]
        {
            let gateway = match self.ipfs {
                Some(gateway) => gateway,
                None => Ipfs::default_gateway()?,
            };
            http.ipfs = Some(Arc::new(gateway));
        }
        Ok(Client {
            http: Arc::new(http),
        })
    }
}

/// Pending request configuration, separate from the legacy root Download engine.
#[must_use]
pub struct Download<'a> {
    client: &'a Client,
    inputs: Result<Vec<Input>, Error>,
    target: Target,
    requirements: Result<Requirements, Box<Conflict>>,
    measurement: Measurement,
    reentry: Reentry,
    publication: crate::transfer::Publication,
}

/// Explicitly starts a new attempt for pending receiving work in the selected frozen run.
/// Old partials remain untouched; this is not cleanup, fresh discovery or overwrite authority.
pub struct Restart;

/// Allows checked atomic replacement of an incumbent captured under the native destination lease.
/// This is a cooperating-writer guarantee, not an inode CAS against arbitrary filesystem writers.
/// Aliases of other frozen run targets are refused, even when their bytes match. Captured absence
/// remains no-replace if a foreign file appears before commit. The captured incumbent descriptor and
/// all native leases remain owned through submitted physical work after future drop. A later
/// continuation must be explicitly granted this permission again; saved state is not authority.
pub struct Overwrite;

#[derive(Clone, Copy)]
enum Reentry {
    Continue,
    Fresh,
    Restart,
    Conflict,
}

#[derive(Clone, Copy, Default)]
enum AttemptPolicy {
    #[default]
    Continue,
    Restart,
}

#[derive(Clone, Copy, Default)]
struct Authority {
    attempt: AttemptPolicy,
    publication: crate::transfer::Publication,
}

impl Download<'_> {
    /// Selects one literal repeatable adapter token for a single original input.
    ///
    /// Intent is retained before automatic unfinished-run matching, so continuation never evaluates
    /// a mutable manifest again. Literal token intent is independent of include/exclude path intent.
    pub fn select_id(mut self, token: &str) -> Self {
        self.inputs = self.inputs.and_then(|mut inputs| {
            if inputs.len() != 1 {
                return Err(Error::SelectionScope);
            }
            let input = inputs.pop().ok_or(Error::NoInputs)?;
            Ok(vec![input.select_id(token)?])
        });
        self
    }

    /// Explicitly chooses all alternatives for one original input. Mixed requests attach
    /// [`Input::all`] to their catalogue occurrence instead; there is no global platform guess.
    pub fn all(mut self) -> Self {
        self.inputs = self.inputs.and_then(|mut inputs| {
            if inputs.len() != 1 {
                return Err(Error::SelectionScope);
            }
            let input = inputs.pop().ok_or(Error::NoInputs)?;
            Ok(vec![input.all()])
        });
        self
    }

    /// Adds an original-path include for one original input; see [`Input::include`] for the fixed
    /// grammar and budgets. Parse/scope failures remain sticky until plan/run is awaited.
    /// For mixed inputs, attach intent to each Input in Rust instead of applying a global pattern.
    /// Intent is fingerprinted before discovery; an unfinished frozen match wins before reevaluation.
    pub fn include(mut self, pattern: &str) -> Self {
        self.inputs = self.inputs.and_then(|mut inputs| {
            if inputs.len() != 1 {
                return Err(Error::SelectionScope);
            }
            let input = inputs.pop().ok_or(Error::NoInputs)?;
            let input = input.include(pattern).map_err(InputError::from)?;
            Client::collect_inputs(core::iter::once(input))
        });
        self
    }

    /// Adds an original-path exclusion for one original input, after include union.
    /// See [`Input::include`] for grammar/bounds and [`Download::include`] for request scope.
    pub fn exclude(mut self, pattern: &str) -> Self {
        self.inputs = self.inputs.and_then(|mut inputs| {
            if inputs.len() != 1 {
                return Err(Error::SelectionScope);
            }
            let input = inputs.pop().ok_or(Error::NoInputs)?;
            let input = input.exclude(pattern).map_err(InputError::from)?;
            Client::collect_inputs(core::iter::once(input))
        });
        self
    }

    /// Intentionally discovers new work instead of matching unfinished owned runs. Old run artifacts
    /// remain untouched; Fresh does not grant overwrite or restart any existing attempt.
    pub fn fresh(mut self, _policy: Fresh) -> Self {
        self.reentry = match self.reentry {
            Reentry::Restart | Reentry::Conflict => Reentry::Conflict,
            Reentry::Continue | Reentry::Fresh => Reentry::Fresh,
        };
        self
    }

    /// Restarts pending receiving work only after unique frozen-run matching, without discovery.
    /// No match is an error. Completed outputs are revalidated, not deleted or fetched again.
    /// A new owned attempt is made; old data/control and uncertain extents remain untouched.
    /// Fresh and Restart together are a sticky request error. Preview plans carry no authority.
    pub fn restart(mut self, _authority: Restart) -> Self {
        self.reentry = match self.reentry {
            Reentry::Fresh | Reentry::Conflict => Reentry::Conflict,
            Reentry::Continue | Reentry::Restart => Reentry::Restart,
        };
        self
    }

    /// Grants replacement only to this consumed run, independently of Fresh or Restart.
    /// Checks and sealing still precede replacement. Preview plans carry no authority; executing
    /// a separately inspected Plan retains the ordinary absent-only policy.
    pub fn overwrite(mut self, _authority: Overwrite) -> Self {
        self.publication = crate::transfer::Publication::Replace;
        self
    }

    /// Asserts exactly one selected file and explicit native output name.
    /// Aggregate execution refuses parent (`..`) components; supply a resolved parent path instead
    /// of relying on lexical collapse across symlinks. Directory-shaped file spellings are invalid.
    pub fn to_file(mut self, destination: impl AsRef<Path>) -> Self {
        self.target = Target::File(destination.as_ref().to_path_buf());
        self
    }

    /// Assigns selected files beneath a caller-chosen native directory root.
    pub fn to_directory(mut self, directory: impl AsRef<Path>) -> Self {
        self.target = Target::Directory(directory.as_ref().to_path_buf());
        self
    }

    /// Requires a caller digest for exactly one selected file; conflicts stay sticky.
    pub fn expect(self, digest: impl Into<Digest>) -> Self {
        self.require(digest, Provenance::Caller)
    }

    /// Requires a digest for exactly one selected file with its declared origin.
    /// Agreeing values merge origins; a conflict remains sticky before discovery or output effects.
    /// Provenance records the caller's attribution, not authentication of the digest.
    pub fn require(mut self, digest: impl Into<Digest>, provenance: Provenance) -> Self {
        self.requirements = self.requirements.and_then(|mut requirements| {
            requirements.insert(digest.into(), provenance)?;
            Ok(requirements)
        });
        self
    }

    /// Optional measurement never cancels required checks.
    pub fn measurement(mut self, measurement: Measurement) -> Self {
        self.measurement = measurement;
        self
    }

    /// Resolves bounded metadata and freezes selections, requirements and output assignments.
    /// Discovery has a shared 15-second deadline, count and text budget across all input occurrences.
    /// There is no payload GET, output creation or lasting ownership. SwissTransfer uses readable
    /// `SwissTransfer` grouping where a group is needed because no real title field is established.
    /// Equal groups conflict; use explicit Input namespaces rather than relying on suffixes.
    pub async fn plan(self) -> Result<Plan, Error> {
        let Self {
            client,
            inputs,
            target,
            requirements,
            measurement,
            reentry,
            publication: _,
        } = self;
        if matches!(reentry, Reentry::Conflict | Reentry::Restart) {
            return Err(Error::Saved(SavedError::Authority));
        }
        let inputs = inputs?;
        let input_count = inputs
            .iter()
            .enumerate()
            .filter(|(index, input)| {
                !inputs[..*index]
                    .iter()
                    .any(|previous| Arc::ptr_eq(&previous.group, &input.group))
            })
            .count();
        let requirements = requirements.map_err(Error::ConflictingRequirements)?;
        let inventory = client.resolve_inputs(inputs).await?;
        let files: Vec<_> = inventory.into_files().collect();
        if requirements.iter().next().is_some() && files.len() != 1 {
            return Err(Error::GlobalRequirementCardinality);
        }
        layout::Layout::freeze(files, target, requirements, measurement, input_count)
    }

    /// Continues uniquely matching unfinished owned work before mutable HEAD/discovery. No match
    /// discovers a new frozen plan. Fully completed prior runs are never unfinished matches.
    /// See [`Resume`'s ownership and cancellation contract](Resume#ownership-and-cancellation): drop
    /// can leave Busy until submitted work settles, and is neither rollback nor a pause receipt.
    pub async fn run(self) -> Result<Report, Box<RunFailure>> {
        self.run_with(&mut ()).await
    }

    /// Runs with a synchronous observer borrowing caller-local state on the executing task.
    /// Returned success or failure emits one terminal event. Drop promises none; see [`Observer`].
    pub async fn run_with<O: Observer>(self, observer: &mut O) -> Result<Report, Box<RunFailure>> {
        let result = self.run_observed(observer).await;
        observer.observe(Event::returned(&result));
        result
    }

    async fn run_observed<O: Observer>(self, observer: &mut O) -> Result<Report, Box<RunFailure>> {
        let client = self.client;
        let Self {
            inputs,
            target,
            requirements,
            measurement,
            reentry,
            publication,
            ..
        } = self;
        let setup = (|| {
            if matches!(reentry, Reentry::Conflict) {
                return Err(Error::Saved(SavedError::Authority));
            }
            let inputs = inputs?;
            for input in &inputs {
                Client::supported(input)?;
            }
            let requirements = requirements.map_err(Error::ConflictingRequirements)?;
            if let Target::File(path) = &target {
                layout::Layout::explicit_file(path)?;
            }
            let request = saved::Fingerprint::request(&inputs, &target, requirements, measurement)
                .map_err(Error::Saved)?;
            let root = saved::request_root(&target).map_err(Error::Saved)?;
            Ok::<_, Error>((inputs, requirements, request, root))
        })();
        let (inputs, requirements, request, root) = setup.map_err(RunFailure::before_execution)?;
        Http::runtime().map_err(RunFailure::before_execution)?;
        if cfg!(not(unix)) {
            return Err(RunFailure::before_execution(Error::NativeUnsupported));
        }
        match tokio::fs::symlink_metadata(&root).await {
            Ok(metadata) => {
                Client::directory_kind(&metadata).map_err(RunFailure::before_execution)?
            }
            Err(cause) if cause.kind() == io::ErrorKind::NotFound => {}
            Err(cause) => return Err(RunFailure::before_execution(Error::Directory(cause))),
        }
        let authority = Authority {
            attempt: if matches!(reentry, Reentry::Restart) {
                AttemptPolicy::Restart
            } else {
                AttemptPolicy::Continue
            },
            publication,
        };
        if matches!(reentry, Reentry::Continue | Reentry::Restart)
            && let Some(saved) = saved::matching(root, request)
                .await
                .map_err(|error| RunFailure::before_execution(Error::Saved(error)))?
        {
            return saved
                .bind_request(client, inputs)
                .map_err(|error| RunFailure::before_execution(Error::Saved(error)))?
                .authorized(authority)
                .run_observed(observer)
                .await;
        }
        if matches!(reentry, Reentry::Restart) {
            return Err(RunFailure::before_execution(Error::Saved(
                SavedError::RestartMissing,
            )));
        }
        let plan = Download {
            client,
            inputs: Ok(inputs),
            target,
            requirements: Ok(requirements),
            measurement,
            reentry,
            publication,
        }
        .plan()
        .await
        .map_err(RunFailure::before_execution)?;
        // Fresh bypasses admission matching, but its own unfinished work remains discoverable later.
        client
            .execute_with_request(plan, Some(request), reentry, authority, observer)
            .await
    }
}

impl fmt::Debug for Download<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Download([pending redacted request])")
    }
}

// One request-wide retained inventory budget, debited before selection. A selector cannot hide a
// larger discovery, and later inputs cannot independently each retain the entire common allowance.
struct PathIntent {
    occurrence: crate::input::InputOccurrence,
    selection: Arc<crate::selection::Selection>,
}

struct DiscoveryBudget {
    files: usize,
    bytes: usize,
}
impl Default for DiscoveryBudget {
    fn default() -> Self {
        Self {
            files: MAX_FILES,
            bytes: MAX_METADATA_BYTES,
        }
    }
}
impl DiscoveryBudget {
    fn reserve(&mut self, file: &File) -> Result<(), Error> {
        self.reserve_entry(file.retained_bytes())
    }

    #[cfg(any(feature = "s3", feature = "webdav"))]
    fn reserve_directory(&mut self, directory: &crate::inventory::Directory) -> Result<(), Error> {
        self.reserve_entry(directory.retained_bytes())
    }

    fn reserve_entry(&mut self, retained_bytes: usize) -> Result<(), Error> {
        // The historical field name counts all discovered entries, including inspection-only markers.
        self.files = self
            .files
            .checked_sub(1)
            .ok_or(Error::Inventory(InventoryError::Limit))?;
        self.bytes = self
            .bytes
            .checked_sub(retained_bytes)
            .ok_or(Error::Inventory(InventoryError::Limit))?;
        Ok(())
    }
}

enum Target {
    Default,
    File(PathBuf),
    Directory(PathBuf),
}

/// Frozen read-only assignments. Consuming execution is the only mutation path.
/// A plan grants neither filesystem permission nor exactly-once execution authority.
#[must_use]
pub struct Plan {
    files: Vec<PlannedFile>,
    directory_root: Option<PathBuf>,
    directories: Vec<PathBuf>,
}

impl Plan {
    /// Inspects final assignments without mutating selected scope or paths.
    pub fn files(&self) -> impl ExactSizeIterator<Item = &PlannedFile> {
        self.files.iter()
    }
}

impl fmt::Debug for Plan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Plan")
            .field("files", &self.files.len())
            .finish()
    }
}

/// One frozen local assignment, separate from its remote path, hint and version.
pub struct PlannedFile {
    file: File,
    destination: PathBuf,
    requirements: Requirements,
    measurement: Measurement,
}

impl PlannedFile {
    /// The selected remote member and original occurrence context.
    pub fn file(&self) -> &File {
        &self.file
    }

    /// Explicit inspection of the approved local assignment, not a remote identity.
    pub fn destination(&self) -> &Path {
        &self.destination
    }

    /// Frozen requirements, separately from optional reporting work.
    pub fn requirements(&self) -> impl Iterator<Item = Requirement> {
        self.requirements.iter()
    }

    /// Frozen optional measurement.
    pub fn measurement(&self) -> Measurement {
        self.measurement
    }
}

impl fmt::Debug for PlannedFile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PlannedFile([reference and assignment redacted])")
    }
}

/// Response version consistency or local publication revalidation, not authenticity or a content hash.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VersionConsistency {
    /// The first actual provider response established a strong pin before body/native effects.
    StrongEtagEstablished,
    /// No strong version was available; this is a fresh unpinned ordered response.
    Unpinned,
    /// Conditional GET and its returned strong ETag agreed with the frozen pin, whether that pin
    /// came from HEAD metadata or an earlier actual provider response.
    StrongEtagMatched,
    /// An existing publication's recorded strong content anchor and filesystem identity were
    /// revalidated locally. No current response was opened and no remote version check was performed.
    /// Historical source version evidence remains available through [`File::version`].
    LocallyRevalidated,
}

/// Completed files only. A run failure retains this report plus the failing file's native state.
#[derive(Default)]
pub struct Report {
    completed: Vec<PublishedFile>,
    saved_run: Option<PathBuf>,
}

impl Report {
    /// Actual aggregate workspace, including on partial failure. A path grants no executable lease.
    pub fn saved_run(&self) -> Option<&Path> {
        self.saved_run.as_deref()
    }

    /// Acknowledged native publications, including revalidated saved completions. Checks, version
    /// consistency and retained state remain separate; this is not a claim of current remote freshness.
    pub fn completed(&self) -> impl ExactSizeIterator<Item = &PublishedFile> {
        self.completed.iter()
    }
}

impl fmt::Debug for Report {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Report")
            .field("published_files", &self.completed.len())
            .finish()
    }
}

/// One acknowledged native publication, not an assertion about other files in a run.
pub struct PublishedFile {
    file: File,
    destination: PathBuf,
    version: VersionConsistency,
    receipt: PublicationReceipt,
}

// Preserve the consuming reader receipt without cloning or discarding its physical-work facts.
// Reconciled publications have no new source attempt and therefore carry only the native receipt.
enum PublicationReceipt {
    Whole(Receipt),
    Ranges(crate::transfer::reader::ReaderReceipt),
}

impl PublishedFile {
    /// Selected original source context.
    pub fn file(&self) -> &File {
        &self.file
    }

    /// Local assignment, accessible explicitly rather than emitted in default diagnostics.
    pub fn destination(&self) -> &Path {
        &self.destination
    }

    /// The actual native publication/check receipt, including its retained state locator.
    pub fn receipt(&self) -> &Receipt {
        match &self.receipt {
            PublicationReceipt::Whole(receipt) => receipt,
            PublicationReceipt::Ranges(receipt) => receipt.native(),
        }
    }

    /// Current reader attempt's restored, source and physical-write facts. Absent for whole-body
    /// attempts and offline-reconciled publications, which did not run a reader in this invocation.
    /// Logical assembled counts remain available through [`Self::receipt`].
    pub fn range_stats(&self) -> Option<crate::transfer::reader::ReaderStats> {
        match &self.receipt {
            PublicationReceipt::Whole(_) => None,
            PublicationReceipt::Ranges(receipt) => Some(receipt.stats()),
        }
    }

    /// Response version consistency or local revalidation, independent of matched requirements
    /// and measured hashes. Local revalidation does not check current remote freshness.
    pub fn version(&self) -> VersionConsistency {
        self.version
    }
}

impl fmt::Debug for PublishedFile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PublishedFile")
            .field("version", &self.version)
            .finish()
    }
}

/// Failure retains completed native publications and, for native transfer errors, partial/state facts.
#[derive(Debug, thiserror::Error)]
#[error("{error}")]
pub struct RunFailure {
    /// Typed terminal reason, not a successful empty or truncated run.
    #[source]
    pub error: Error,
    /// Files published before the terminal failure.
    pub report: Report,
    /// Frozen failing member/assignment when execution had reached one, without automatic retry.
    /// Its native partial/state facts remain in the typed transfer error.
    pub failed: Option<PlannedFile>,
}

impl RunFailure {
    fn before_execution(error: Error) -> Box<Self> {
        Box::new(Self {
            error,
            report: Report::default(),
            failed: None,
        })
    }
}

/// Built-in errors expose only safe typed context, never a bearer reference or unsafe remote name.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Exact IPFS gateway acquisition requires its compiled capability and configured transport.
    #[error("ipfs capability unavailable; enable ipfs and configure an http gateway")]
    IpfsUnavailable,
    /// Gateway/CID failures retain safe typed categories rather than access references.
    #[cfg(feature = "ipfs")]
    #[error("ipfs acquisition failed; inspect the typed identity or gateway category")]
    Ipfs(#[from] IpfsError),
    /// Per-input access headers were invalid or crossed their original authority.
    #[error("request header configuration failed; correct the input access headers")]
    RequestHeader(#[from] crate::input::HeaderError),
    /// Bounded namespace-aware DAV discovery failure, without remote diagnostic text.
    #[cfg(feature = "webdav")]
    #[error("webdav discovery failed; inspect the typed metadata category")]
    WebDav(#[from] WebDavError),
    /// Exact S3 and explicit S3 trees require the feature and a caller-configured SDK client.
    #[error(
        "s3 capability unavailable; enable s3 and configure the client builder with an sdk client"
    )]
    S3Unavailable,
    /// Concrete SDK failure or bounded S3 protocol category, with remote diagnostics redacted.
    #[cfg(feature = "s3")]
    #[error("s3 operation failed; inspect the safe transport category")]
    S3(#[from] S3Error),
    /// Offline owned persistence and frozen continuation errors remain matchable.
    #[error("saved run operation failed; inspect the typed recovery error")]
    Saved(#[from] SavedError),
    /// A terminal native failure and failed aggregate checkpoint both remain available.
    #[error("native transfer and aggregate checkpoint failed; retain both state references")]
    RunCheckpoint {
        /// Original transfer failure, including confirmed or unconfirmed extents.
        #[source]
        transfer: Box<crate::transfer::Failure<SourceError>>,
        /// Aggregate persistence failure; it is not a successful pause.
        checkpoint: SavedError,
    },
    /// Input conversion failed before transport effects.
    #[error("invalid input; inspect the typed input error")]
    Input(#[from] InputError),
    /// Unsupported scopes never produce fake successful inventories.
    #[error("unsupported input scope; use exact http or enable a supported collection adapter")]
    UnsupportedInput {
        /// Safe recognized family.
        kind: InputKind,
        /// Requested scope, not inferred from lookup failure.
        scope: InputScope,
    },
    /// Empty requests differ from genuine complete empty inventories.
    #[error("no inputs; supply at least one reference or selected file")]
    NoInputs,
    /// Provider protocol/gate failures contain no remote text, identifiers or bearer values.
    #[cfg(feature = "swisstransfer")]
    #[error(
        "swisstransfer metadata or access is invalid or unsupported; check the share and access"
    )]
    SwissProtocol,
    /// A request-level literal selector cannot accidentally apply to several independent inputs.
    #[error("selector needs one original input; use input-local selection for mixed inputs")]
    SelectionScope,
    /// Literal selector intent never falls back to the first file or a matching path.
    #[error("member selection matched no files; list the share and choose a current member token")]
    Selection,
    /// Complete nonempty discovery for an input cannot silently disappear under its path intent.
    #[error("path selection matched no files; list the input and adjust includes or exclusions")]
    PathSelection,
    /// Bounded metadata failure does not expose a truncated inventory.
    #[error("metadata limit exceeded; request a smaller scope")]
    Inventory(#[from] InventoryError),
    /// The requested semaphore capacity must fit Tokio's supported bound.
    #[error("invalid request capacity; choose a smaller nonzero request limit")]
    AdmissionLimit,
    /// No implicit Tokio runtime is created.
    #[error("runtime unavailable; perform http and native file work inside a tokio runtime")]
    RuntimeUnavailable,
    /// Transport causes are retained inside a redacted audited wrapper.
    #[error("http operation failed; inspect the safe transport category")]
    Http(#[from] HttpError),
    /// The combined 15-second admission, connection and response-header deadline expired.
    /// This can happen while waiting for a retained body's admission slot, before sending a request.
    #[error(
        "http request admission or headers timed out; release retained bodies or check the endpoint"
    )]
    HeaderTimeout,
    /// The complete transport-neutral metadata scope exceeded its shared discovery deadline.
    #[error("metadata discovery timed out; release retained bodies or request a smaller scope")]
    DiscoveryTimeout,
    /// Redirect targets can change origin/credential authority and are not followed in this slice.
    #[error("http redirects are unsupported; supply the authorized final reference explicitly")]
    Redirect,
    /// A response failed without embedding its URL or untrusted status text.
    #[error("http {stage} returned status {status}; check access and the reference")]
    Status {
        /// Metadata or payload opening, without reference text.
        stage: RequestStage,
        /// Numeric status only, not a server-provided reason phrase.
        status: u16,
    },
    /// Malformed/conflicting header declarations cannot establish payload length/version.
    #[error("invalid http metadata; check the endpoint's response headers")]
    InvalidMetadata,
    /// Identity content encoding is required; no silent representation transformation.
    #[error("unsupported content encoding; use an identity representation")]
    ContentEncoding,
    /// A frozen strong ETag disagreed, disappeared or was refused by the conditional request.
    #[error("remote version changed; discover a fresh request rather than reusing this plan")]
    VersionChanged,
    /// Optional public release discovery/opening retains sanitized provider failure categories.
    #[cfg(feature = "github")]
    #[error("github release failed; inspect the safe provider category")]
    Github(#[from] GithubError),
    /// Catalogues never infer the first asset or a native-platform match.
    #[error(
        "release alternatives require selection; choose a member, include path or explicit all"
    )]
    CatalogueChoice,
    /// Caller expectations bind exactly one actual selected file.
    #[error("global expectation needs exactly one selected file; select one member first")]
    GlobalRequirementCardinality,
    /// All conflicting proposals fail before output/body effects.
    #[error("conflicting required checks; resolve the expected digests")]
    ConflictingRequirements(#[source] Box<Conflict>),
    /// An exact native file target cannot silently discard an input namespace.
    #[error("namespace conflicts with an exact file target; choose a directory target")]
    NamespaceWithFile,
    /// Actual selected count controls exact-file cardinality.
    #[error("file target needs exactly one selected file; choose one member or a directory target")]
    FileCardinality,
    /// Unsafe names are not normalized, flattened or silently replaced.
    #[error("unsafe output name; choose an explicit safe name or namespace")]
    InvalidName,
    /// Safe readable fallback is not guessed from a bearer query or arbitrary response body.
    #[error("no usable output name; choose an explicit file target")]
    MissingName,
    /// Local assignment/key bytes and hierarchy depth are bounded before prefix retention.
    #[error("output layout exceeds its limit; choose a shorter or shallower namespace and root")]
    LayoutLimit,
    /// File/directory, canonical Unicode and case-equivalent assignments all conflict.
    #[error("output assignments conflict; give inputs distinct explicit namespaces or names")]
    LayoutConflict,
    /// Existing output roots/components must be nonsymlink caller-owned writable directories.
    #[error(
        "unsafe output directory role or permissions; choose a caller-owned non-shared directory"
    )]
    UnsafeDirectory,
    /// Directory inspection/creation is execution-only and does not grant overwrite authority.
    #[error("output directory preparation failed; choose a suitable caller-owned directory")]
    Directory(#[source] std::io::Error),
    /// Native checked publication is not implemented on this host.
    #[error("native publication is unsupported on this host; use the checked live body instead")]
    NativeUnsupported,
    /// Native request validation remains typed and precedes native effects.
    #[error("invalid native file request; inspect the request error")]
    NativeRequest(#[source] crate::transfer::RequestError),
    /// Reader session/check validation fails before native effects or range admission.
    #[error("invalid http range request; inspect the reader request error")]
    RangeRequest(#[source] crate::transfer::reader::ReaderRequestError),
    /// Concrete reader failures retain HTTP, protocol, native and logical partial-state facts.
    #[error("http range transfer failed; inspect its partial report and retained state")]
    RangeTransfer(
        #[source] Box<crate::transfer::Failure<crate::transfer::reader::ReaderError<Error>>>,
    ),
    /// Both reader failure and aggregate checkpoint failure remain available.
    #[error("http range transfer and checkpoint failed; retain both state references")]
    RangeCheckpoint {
        /// Original failure, not erased into a foreign cause string.
        #[source]
        transfer: Box<crate::transfer::Failure<crate::transfer::reader::ReaderError<Error>>>,
        /// Failed aggregate acknowledgement, not a successful pause.
        checkpoint: SavedError,
    },
    /// Actual native partial counts, settled extents and retained state survive failure.
    #[error("native transfer failed; inspect its partial report and retained state")]
    Transfer(#[source] Box<crate::transfer::Failure<SourceError>>),
}

impl From<Infallible> for Error {
    fn from(value: Infallible) -> Self {
        match value {}
    }
}
