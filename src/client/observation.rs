//! Caller-task event orchestration over the common native progress facts.

use std::path::Path;

use super::{BindingKey, PlannedFile, PublishedFile, Report, RunFailure, RunId};
use crate::transfer::observation::Observer as NativeObserver;
pub use crate::transfer::observation::ProgressFacts;

/// Receives synchronous events on the task executing a Client run.
///
/// Borrowed/local state is supported. The library adds no reporting task, queue or runtime and does
/// not require Send, Sync, Clone or static observers. Slow callbacks delay that task deliberately;
/// they are not covered by a payload inactivity timeout. A dropped run emits no promised terminal
/// event. A callback panic unwinds execution under the same physical-work ownership rules as drop.
/// Progress facts alone never establish clean EOF, matched requirements or publication.
///
/// ```no_run
/// use xget::{Client, client::{Event, Observer}};
///
/// struct CountFiles<'a>(&'a mut usize);
/// impl Observer for CountFiles<'_> {
///     fn observe(&mut self, event: Event<'_>) {
///         if let Event::FileCompleted { .. } = event {
///             *self.0 += 1;
///         }
///     }
/// }
///
/// # async fn example() -> Result<(), Box<dyn core::error::Error>> {
/// let client = Client::new()?;
/// let mut completed = 0;
/// let mut observer = CountFiles(&mut completed);
/// let report = client.download("https://example.org/file.bin")
///     .to_file("file.bin").run_with(&mut observer).await?;
/// assert_eq!(completed, report.completed().len());
/// # Ok(())
/// # }
/// ```
pub trait Observer {
    /// Observe one borrowed event without retaining its temporary references.
    fn observe(&mut self, event: Event<'_>);
}

impl Observer for () {
    fn observe(&mut self, _event: Event<'_>) {}
}

/// Run/item events whose metadata remains separate from payload and publication authority.
///
/// Saved identifiers correlate an actual frozen workspace. An empty no-op has no saved identifier.
/// Names, source paths and destinations are caller-inspected metadata; machine/terminal renderers
/// must choose their redaction and escaping rather than dumping Debug representations.
pub enum Event<'event> {
    /// Execution has its frozen assignments and, when needed, saved-run ownership.
    RunStarted {
        /// Saved-workspace correlation; not an execution lease or remote identity.
        run: Option<RunId>,
        /// Actual retained workspace, available before payload execution; not reopen authority.
        saved_run: Option<&'event Path>,
        /// Number of frozen executable file jobs, not directory metadata entries.
        files: usize,
    },
    /// One frozen file is about to reconcile or execute through the common coordinator.
    FileStarted {
        /// Correlation with the frozen saved member, never a new selector.
        key: BindingKey,
        /// Read-only selected metadata and assignment; no payload body is acquired by this borrow.
        planned: &'event PlannedFile,
    },
    /// Logical and current-attempt counters from settled native/source work.
    Progress {
        /// Frozen member to which the facts belong.
        key: BindingKey,
        /// Exact typed facts; hashing is not a whole-file match or authenticity statement.
        facts: ProgressFacts,
    },
    /// An acknowledged or revalidated native publication is present in the partial report.
    /// A later aggregate checkpoint may still fail; this is not whole-run completion.
    FileCompleted {
        /// Frozen saved member correlation.
        key: BindingKey,
        /// Receipt, checks and optional attempt facts retained by the common coordinator.
        published: &'event PublishedFile,
    },
    /// Every requested executable job completed, or a genuine empty scope was a no-op.
    RunCompleted {
        /// Completed report; not a filesystem-tree replication or power-loss guarantee.
        report: &'event Report,
    },
    /// Execution returned a failure with its acknowledged partial report and retained state facts.
    /// Abrupt future drop has no such callback or successfully paused-transfer promise.
    RunFailed {
        /// Typed failure; a renderer must preserve the library's diagnostic redaction boundary.
        failure: &'event RunFailure,
    },
}

impl<'event> Event<'event> {
    pub(super) fn returned(result: &'event Result<Report, Box<RunFailure>>) -> Self {
        match result {
            Ok(report) => Self::RunCompleted { report },
            Err(failure) => Self::RunFailed { failure },
        }
    }
}

pub(super) struct FileProgress<'observer, O: ?Sized> {
    pub(super) observer: &'observer mut O,
    pub(super) key: BindingKey,
}

impl<O: Observer + ?Sized> NativeObserver for FileProgress<'_, O> {
    fn observe(&mut self, facts: ProgressFacts) {
        self.observer.observe(Event::Progress {
            key: self.key,
            facts,
        });
    }
}
