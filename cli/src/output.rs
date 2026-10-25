//! Versioned CLI envelopes around observed facts, not synthetic progress or verification.

use core::cell::{Cell, RefCell};
use core::sync::atomic::{AtomicU64, Ordering};
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use xget::transfer::{CheckResults, Length, Phase, WriteExtent};

use crate::display::DisplayFormat;
use crate::failure::{Diagnostic, Failure};
use crate::human::{FailureContext, Human};

/// Correlation for this invocation only. Persistent core run/member IDs remain a library seam.
/// The schema explicitly says so rather than confusing these IDs with recovery authority.
pub(crate) struct RunId(String);

impl RunId {
    pub(crate) fn fresh() -> Self {
        // This counter distinguishes invocations in one process; it grants no execution authority.
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let serial = NEXT.fetch_add(1, Ordering::Relaxed);
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        Self(format!("invocation-{}-{time}-{serial}", std::process::id()))
    }
}

pub(crate) struct ItemId(pub(crate) usize);

pub(crate) enum Channel {
    Metadata,
    Diagnostic,
}

#[derive(Clone, Copy, clap::ValueEnum, Default)]
pub(crate) enum Format {
    #[default]
    Plain,
    Json,
    None,
}

pub(crate) struct Output {
    run: RunId,
    format: Format,
    channel: Channel,
    started: Instant,
    // Task-local diagnostic context only, with no executable lease or shared-worker ownership.
    saved_id: Cell<Option<xget::client::RunId>>,
    saved_path: RefCell<Option<PathBuf>>,
    human: Human,
    human_failure: RefCell<FailureContext>,
    // Suppress retries only on the failed sink; metadata stdout cannot disable healthy stderr.
    metadata_failed: Cell<bool>,
    diagnostic_failed: Cell<bool>,
}

impl Output {
    pub(crate) fn new(run: RunId, format: Format, channel: Channel) -> Self {
        Self {
            run,
            format,
            channel,
            started: Instant::now(),
            saved_id: Cell::new(None),
            saved_path: RefCell::new(None),
            human: Human {
                units: DisplayFormat::HumanBytes,
                cwd: std::env::current_dir().ok(),
            },
            human_failure: RefCell::new(FailureContext::default()),
            metadata_failed: Cell::new(false),
            diagnostic_failed: Cell::new(false),
        }
    }

    pub(crate) fn with_units(mut self, units: DisplayFormat) -> Self {
        self.human.units = units;
        self
    }

    /// Human output is built directly from typed facts, never decoded from a machine envelope.
    pub(crate) fn human(&self, render: impl FnOnce(&Human) -> String) -> Result<(), Failure> {
        if !matches!(self.format, Format::Plain) {
            return Ok(());
        }
        self.write_line(&self.human_text(render), &self.channel)
    }

    pub(crate) fn human_text(&self, render: impl FnOnce(&Human) -> String) -> String {
        render(&self.human)
    }

    pub(crate) fn human_run_started(&self, files: usize) {
        self.human_failure.borrow_mut().total = Some(files);
    }

    pub(crate) fn human_run_failed(&self, failure: &xget::client::RunFailure) {
        let mut context = self.human_failure.borrow_mut();
        context.completed = Some(failure.report.completed().len());
        context.state = failure.report.saved_run().map(Path::to_path_buf);
    }

    pub(crate) fn human_recovery(&self, state: Option<&Path>, choices: &[PathBuf]) {
        if !matches!(self.format, Format::Plain) {
            return;
        }
        let mut context = self.human_failure.borrow_mut();
        if let Some(state) = state {
            context.state = Some(state.to_path_buf());
        }
        context.choices = choices.to_vec();
    }

    pub(crate) fn explicit_resume(&self) {
        self.human_failure.borrow_mut().explicit_resume = true;
    }

    pub(crate) fn live(&self) {
        self.human_failure.borrow_mut().live = true;
    }

    pub(crate) fn human_done(&self, files: usize) -> Result<(), Failure> {
        self.human(|human| {
            human.done(
                files,
                self.started.elapsed(),
                self.saved_path
                    .borrow()
                    .as_deref()
                    .filter(|_| self.human_failure.borrow().explicit_resume),
            )
        })
    }

    pub(crate) fn emit(
        &self,
        kind: &str,
        item: Option<ItemId>,
        facts: Value,
    ) -> Result<(), Failure> {
        self.emit_at(kind, item, facts, &self.channel)
    }

    fn emit_at(
        &self,
        kind: &str,
        item: Option<ItemId>,
        facts: Value,
        channel: &Channel,
    ) -> Result<(), Failure> {
        if !matches!(self.format, Format::Json) {
            return Ok(());
        }
        let envelope = self.envelope(kind, item, &facts);
        self.write_json(&envelope, channel)
    }

    pub(crate) fn emit_saved(
        &self,
        kind: &str,
        run: xget::client::RunId,
        item: Option<xget::client::BindingKey>,
        facts: Value,
    ) -> Result<(), Failure> {
        if !matches!(self.format, Format::Json) {
            return Ok(());
        }
        let mut envelope = self.envelope(kind, None, &facts);
        envelope["run_id"] = json!(run.to_string());
        envelope["item_id"] = json!(item.map(|key| key.to_string()));
        envelope["correlation_scope"] = json!("SavedWorkspace");
        self.write_json(&envelope, &self.channel)
    }

    fn write_json(&self, envelope: &Value, channel: &Channel) -> Result<(), Failure> {
        let line = serde_json::to_string(envelope)
            .map_err(|_| Failure::runtime("cannot encode output; no completion is claimed"))?;
        self.write_line(&line, channel)
    }

    fn write_line(&self, line: &str, channel: &Channel) -> Result<(), Failure> {
        let failed = match channel {
            Channel::Metadata => &self.metadata_failed,
            Channel::Diagnostic => &self.diagnostic_failed,
        };
        if failed.get() {
            return Err(Failure::runtime(
                "command output previously failed; transferred bytes or published files are not rolled back",
            ));
        }
        let result = match channel {
            Channel::Metadata => writeln!(io::stdout().lock(), "{line}"),
            Channel::Diagnostic => writeln!(io::stderr().lock(), "{line}"),
        };
        result.map_err(|cause| {
            failed.set(true);
            Failure::io(cause, "cannot write command output; transferred bytes or published files are not rolled back")
        })
    }

    fn envelope(&self, kind: &str, item: Option<ItemId>, facts: &Value) -> Value {
        json!({
            "schema_version": 1,
            "kind": kind,
            "run_id": self.run.0,
            "invocation_id": self.run.0,
            "item_id": item.map(|ItemId(index)| format!("item-{index}")),
            "correlation_scope": "Invocation",
            "elapsed_ms": self.started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
            "facts": facts,
        })
    }

    pub(crate) fn bind_saved(&self, run: Option<xget::client::RunId>, path: Option<&Path>) {
        self.saved_id.set(run);
        let mut stored = self.saved_path.borrow_mut();
        if stored.as_deref() != path {
            *stored = path.map(Path::to_path_buf);
        }
    }

    pub(crate) fn failure(&self, failure: &Failure) -> Result<(), Failure> {
        match self.format {
            Format::Plain => {
                return self.write_line(
                    &self.human.failure(
                        failure,
                        &self.human_failure.borrow(),
                        self.saved_path.borrow().as_deref(),
                    ),
                    &Channel::Diagnostic,
                );
            }
            Format::None => return Ok(()),
            Format::Json => {}
        }
        let facts = self.failure_facts(failure);
        let mut envelope = self.envelope("Error", None, &facts);
        if let Some(run) = self.saved_id.get() {
            envelope["run_id"] = json!(run.to_string());
            envelope["correlation_scope"] = json!("SavedWorkspace");
        }
        self.write_json(&envelope, &Channel::Diagnostic)
    }

    fn failure_facts(&self, failure: &Failure) -> Value {
        let mut facts = failure
            .diagnostic()
            .map_or_else(|| json!({"message": failure.message()}), Self::diagnostic);
        facts["exit_code"] = json!(failure.code());
        facts["state_path"] = self
            .saved_path
            .borrow()
            .as_deref()
            .map_or(Value::Null, Self::path);
        facts["pause_confirmed"] = json!(false);
        facts
    }

    /// Failure counts are observations, not a publication receipt or an acknowledged pause.
    pub(crate) fn diagnostic(diagnostic: &Diagnostic) -> Value {
        json!({
            "reason": diagnostic.reason.name(),
            "message": diagnostic.message,
            "phase": diagnostic.phase.map(|phase| match phase {
                Phase::Acquire => "Acquire",
                Phase::Write => "Write",
                Phase::ReadStage => "ReadStage",
                Phase::Seal => "Seal",
                Phase::Checkpoint => "Checkpoint",
                Phase::Publish => "Publish",
                Phase::Reconcile => "Reconcile",
            }),
            "partial": diagnostic.partial.map(|partial| json!({
                "logical_received_bytes": partial.received.0,
                "logical_written_bytes": partial.written.0,
                "hashed_bytes": partial.hashed.0,
                "write_extent": match partial.write_extent {
                    WriteExtent::Confirmed => "Confirmed",
                    WriteExtent::Unconfirmed => "Unconfirmed",
                },
                "completion_evidence": false,
            })),
            "native_state_path": diagnostic.native_state.as_deref().map(Self::path),
            "checkpoint_failed": diagnostic.checkpoint_failed,
        })
    }

    pub(crate) fn human_progress(&self, line: &str) -> Result<(), Failure> {
        self.write_line(line, &Channel::Diagnostic)
    }

    pub(crate) fn path(path: &Path) -> Value {
        match path.to_str() {
            Some(path) => json!(path),
            None => {
                json!({"encoding": "NativeEncodedBytes", "bytes": path.as_os_str().as_encoded_bytes()})
            }
        }
    }

    pub(crate) fn length(length: Length) -> Value {
        match length {
            Length::Known(bytes) => json!(bytes.0),
            Length::Unknown => Value::Null,
        }
    }

    pub(crate) fn published(published: &xget::client::PublishedFile) -> Value {
        json!({
            "destination": Self::path(published.destination()),
            "length_bytes": published.receipt().partial().written.0,
            "logical_received_bytes": published.receipt().partial().received.0,
            "logical_written_bytes": published.receipt().partial().written.0,
            "hashed_bytes": published.receipt().partial().hashed.0,
            "range_attempt": published.range_stats().map(|stats| json!({
                "restored_bytes": stats.restored.0,
                "source_bytes": stats.source.0,
                "written_bytes": stats.written.0,
            })),
            "checks": Self::checks(*published.receipt().checks()),
            "version": Self::version(published.version()),
        })
    }

    pub(crate) fn measurement(measurement: xget::transfer::Measurement) -> Value {
        match measurement {
            xget::transfer::Measurement::None => json!({"kind": "None"}),
            xget::transfer::Measurement::Algorithm(algorithm) => {
                json!({"kind": "Algorithm", "algorithm": algorithm.to_string()})
            }
        }
    }

    /// LocallyRevalidated describes leased local content/identity evidence only, not a new remote
    /// ETag match. Keep that distinction in machine and plain publication records.
    pub(crate) fn version(version: xget::client::VersionConsistency) -> &'static str {
        match version {
            xget::client::VersionConsistency::StrongEtagEstablished => "StrongEtagEstablished",
            xget::client::VersionConsistency::StrongEtagMatched => "StrongEtagMatched",
            xget::client::VersionConsistency::LocallyRevalidated => "LocallyRevalidated",
            xget::client::VersionConsistency::Unpinned => "Unpinned",
        }
    }

    pub(crate) fn evidence(evidence: xget::inventory::VersionEvidence) -> &'static str {
        match evidence {
            xget::inventory::VersionEvidence::StrongEtag => "StrongEtag",
            xget::inventory::VersionEvidence::Unpinned => "Unpinned",
        }
    }

    pub(crate) fn status(status: xget::client::SavedStatus) -> &'static str {
        match status {
            xget::client::SavedStatus::Pending => "Pending",
            xget::client::SavedStatus::Submitted => "Submitted",
            xget::client::SavedStatus::Retained => "Retained",
            xget::client::SavedStatus::Complete => "Complete",
        }
    }

    pub(crate) fn checks(checks: CheckResults) -> Value {
        json!({
            "measured_digest": checks.measured().map(|digest| digest.to_string()),
            "matched_requirements": checks.matched_requirements().map(|requirement| {
                json!({"digest": requirement.digest.to_string(),
                    "origins": requirement.origins.iter().map(|origin| match origin {
                        xget::transfer::Provenance::Caller => "Caller",
                        xget::transfer::Provenance::Source => "Source",
                        xget::transfer::Provenance::Sidecar => "Sidecar",
                    }).collect::<Vec<_>>()})
            }).collect::<Vec<_>>()
        })
    }
}

#[cfg(test)]
#[path = "output_tests.rs"]
mod tests;
