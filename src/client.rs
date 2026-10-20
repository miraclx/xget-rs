//! Ordinary acquisition through the common inventory, planning and checked publication path.
//!
//! Exact HTTP inputs and, with `swisstransfer`, bounded SwissTransfer collections use the same serial
//! native Transfer implementation. Exact HTTP with known nonzero length, a bounded strong HEAD ETag
//! and explicit byte-range support uses Transfer's bounded reader coordinator. Other exact HTTP and
//! SwissTransfer retain ordered whole-body access. Client clones share reusable HTTP connections and
//! bounded control/body admission, not a provider-owned scheduler or the legacy range engine.
//! Owned run metadata stays in the designated destination's `.xget-runs` store. Redirects, catalogues
//! and unsupported tree/provider inputs fail explicitly. SwissTransfer members currently open whole
//! ordered responses, not parallel ranges or byte-resume sessions.
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
    File, FileId, Inventory, InventoryError, MAX_FILES, MAX_METADATA_BYTES, Selected,
};
pub use crate::transfer::live::{BodyError, BodyFailure, FinishError, Progress as LiveProgress};
use crate::transfer::{Conflict, Digest, Measurement, Provenance, Receipt, Requirement, Transfer};

mod http;
mod layout;
mod live;
mod range;
mod saved;
#[cfg(feature = "swisstransfer")]
mod swiss;

use http::Http;
pub use http::{HttpError, HttpErrorKind, RequestStage};
pub use live::{CheckedBody, LiveReceipt, OpenedFile};
pub use saved::{BindingKey, Fresh, Resume, SavedError, SavedMember, SavedRun, SavedStatus};
#[cfg(feature = "swisstransfer")]
use swiss::Swiss;

#[cfg(test)]
#[path = "client_tests.rs"]
mod tests;

/// Reusable native HTTP transport and a shared bound on active requests/response bodies.
///
/// Construction performs no network discovery and loads no `.env`, proxy or credential profile.
/// A caller-owned Tokio runtime is required only when performing asynchronous HTTP/native work.
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
    /// requested. Alternative catalogues remain unsupported; there is no implicit first-asset choice.
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
    /// Unsupported providers/trees return errors, not fake inventories. HTTP 405/501 leaves a usable
    /// exact unpinned reference without probing GET. Limits are 128 discovered files and 1 MiB retained
    /// inventory text; exceeding either fails the entire discovery, never returning a truncated value.
    pub async fn resolve<T>(&self, input: T) -> Result<Inventory, Error>
    where
        T: TryInto<Input>,
        T::Error: Into<Error>,
    {
        let files = self
            .resolve_input(input.try_into().map_err(Into::into)?)
            .await?;
        Inventory::complete(files).map_err(Error::Inventory)
    }

    async fn resolve_input(&self, input: Input) -> Result<Vec<File>, Error> {
        self.resolve_bounded(input, &mut DiscoveryBudget::default())
            .await
    }

    async fn resolve_bounded(
        &self,
        mut input: Input,
        budget: &mut DiscoveryBudget,
    ) -> Result<Vec<File>, Error> {
        Self::supported(&input)?;
        if input.retained_bytes() > MAX_METADATA_BYTES {
            return Err(Error::Inventory(InventoryError::Limit));
        }
        if let Some(selected) = input.selected.take() {
            let file = File {
                input,
                selected: *selected,
            };
            budget.reserve(&file)?;
            return Ok(vec![file]);
        }
        #[cfg(feature = "swisstransfer")]
        if input.kind() == InputKind::SwissTransfer {
            return Swiss::resolve(&self.http, input, budget).await;
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
        Ok(vec![file])
    }

    fn supported(input: &Input) -> Result<(), Error> {
        #[cfg(feature = "swisstransfer")]
        if input.kind() == InputKind::SwissTransfer && input.scope() == InputScope::Collection {
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
        let mut files = self
            .resolve_input(input.try_into().map_err(Into::into)?)
            .await?;
        if files.len() != 1 {
            return Err(Error::FileCardinality);
        }
        let mut file = files.pop().ok_or(Error::FileCardinality)?;
        let opened = self.open_file(&mut file).await?;
        Ok(OpenedFile::new(file, opened))
    }

    async fn open_file(&self, file: &mut File) -> Result<http::Opened, Error> {
        #[cfg(feature = "swisstransfer")]
        if file.input.kind() == InputKind::SwissTransfer {
            return Swiss::open(&self.http, file).await;
        }
        self.http.open(file).await
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
        self.execute_with_request(plan, None, Reentry::Fresh).await
    }

    async fn execute_with_request(
        &self,
        mut plan: Plan,
        request: Option<saved::Fingerprint>,
        reentry: Reentry,
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
            return Ok(report);
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
        self.execute_owned(plan, owned).await
    }

    async fn execute_owned(
        &self,
        plan: Plan,
        mut owned: saved::Owned,
    ) -> Result<Report, Box<RunFailure>> {
        let mut report = Report {
            completed: Vec::new(),
            saved_run: Some(owned.path().to_path_buf()),
        };
        for (
            index,
            PlannedFile {
                mut file,
                destination,
                requirements,
                measurement,
            },
        ) in plan.files.into_iter().enumerate()
        {
            match owned.previous(index).await {
                Ok(Some(receipt)) => {
                    let version = if file.selected.description.etag.is_some() {
                        VersionConsistency::StrongEtagMatched
                    } else {
                        VersionConsistency::Unpinned
                    };
                    report.completed.push(PublishedFile {
                        file,
                        destination,
                        version,
                        receipt: PublicationReceipt::Whole(receipt),
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
            match pending.run_registered(owned.registration()).await {
                Ok(receipt) => {
                    report.completed.push(PublishedFile {
                        file,
                        destination,
                        version,
                        receipt: PublicationReceipt::Whole(receipt),
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
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            max_requests: NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
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

    /// Builds reusable transports without network or filesystem effects.
    pub fn build(self) -> Result<Client, Error> {
        let http = Http::new(self.max_requests)?;
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
}

#[derive(Clone, Copy)]
enum Reentry {
    Continue,
    Fresh,
}

impl Download<'_> {
    /// Selects one literal repeatable adapter token for a single original input.
    ///
    /// Intent is retained before automatic unfinished-run matching, so continuation never evaluates
    /// a mutable manifest again. This initial API supports ID selection only; path globs and exclusions
    /// are not accepted here. Use Inventory iteration for one-shot in-memory path selection.
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

    /// Intentionally discovers new work instead of matching unfinished owned runs. Old run artifacts
    /// remain untouched; Fresh does not grant overwrite or restart any existing attempt.
    pub fn fresh(mut self, _policy: Fresh) -> Self {
        self.reentry = Reentry::Fresh;
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
    pub fn expect(mut self, digest: impl Into<Digest>) -> Self {
        self.requirements = self.requirements.and_then(|mut requirements| {
            requirements.insert(digest.into(), Provenance::Caller)?;
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
            reentry: _,
        } = self;
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
            Client::supported(input)?;
        }
        let needs_metadata = inputs.iter().any(|input| input.selected.is_none());
        if needs_metadata {
            Http::runtime()?;
        }
        let discovery = async {
            let mut files = Vec::new();
            for (index, mut input) in inputs.into_iter().enumerate() {
                input.occurrence = crate::input::InputOccurrence(index as u32);
                budget.bytes += input.retained_bytes();
                files.extend(client.resolve_bounded(input, &mut budget).await?);
            }
            Ok::<_, Error>(files)
        };
        let files = if needs_metadata {
            tokio::time::timeout(core::time::Duration::from_secs(15), discovery)
                .await
                .map_err(|_| Error::HeaderTimeout)??
        } else {
            discovery.await?
        };
        let inventory = Inventory::complete(files).map_err(Error::Inventory)?;
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
        let client = self.client;
        let Self {
            inputs,
            target,
            requirements,
            measurement,
            reentry,
            ..
        } = self;
        let setup = (|| {
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
        if matches!(reentry, Reentry::Continue)
            && let Some(saved) = saved::matching(root, request)
                .await
                .map_err(|error| RunFailure::before_execution(Error::Saved(error)))?
        {
            return saved
                .bind_request(client, inputs)
                .map_err(|error| RunFailure::before_execution(Error::Saved(error)))?
                .run()
                .await;
        }
        let plan = Download {
            client,
            inputs: Ok(inputs),
            target,
            requirements: Ok(requirements),
            measurement,
            reentry,
        }
        .plan()
        .await
        .map_err(RunFailure::before_execution)?;
        // Fresh bypasses admission matching, but its own unfinished work remains discoverable later.
        client
            .execute_with_request(plan, Some(request), reentry)
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
        self.files = self
            .files
            .checked_sub(1)
            .ok_or(Error::Inventory(InventoryError::Limit))?;
        self.bytes = self
            .bytes
            .checked_sub(file.retained_bytes())
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

/// Observed response version consistency, not authenticity or a content hash.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VersionConsistency {
    /// The first actual provider response established a strong pin before body/native effects.
    StrongEtagEstablished,
    /// No strong version was available; this is a fresh unpinned ordered response.
    Unpinned,
    /// Conditional GET and its returned strong ETag agreed with the frozen pin, whether that pin
    /// came from HEAD metadata or an earlier actual provider response.
    StrongEtagMatched,
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

/// One absent-only native publication, not an assertion about other files in a run.
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

    /// GET consistency, independent of matched requirements and measured hashes.
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
    /// Offline owned persistence and frozen continuation errors remain matchable.
    #[error("saved run operation failed; inspect the typed recovery error")]
    Saved(#[from] SavedError),
    /// A terminal native failure and failed aggregate checkpoint both remain available.
    #[error("native transfer and aggregate checkpoint failed; retain both state references")]
    RunCheckpoint {
        /// Original transfer failure, including confirmed or unconfirmed extents.
        #[source]
        transfer: Box<crate::transfer::Failure<HttpError>>,
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
    #[error("member selector needs one original input; use input-local selection for mixed inputs")]
    SelectionScope,
    /// Literal selector intent never falls back to the first file or a matching path.
    #[error("member selection matched no files; list the share and choose a current member token")]
    Selection,
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
    Transfer(#[source] Box<crate::transfer::Failure<HttpError>>),
}

impl From<Infallible> for Error {
    fn from(value: Infallible) -> Self {
        match value {}
    }
}
