//! Pure human presentation from allowlisted typed facts, separate from machine envelopes.

use core::fmt::Write as _;
use core::time::Duration;
use std::path::{Path, PathBuf};

use xget::client::{PlannedFile, PublishedFile, SavedMember, SavedStatus, VersionConsistency};
use xget::inventory::{Directory, File, VersionEvidence};
use xget::transfer::{
    Algorithm, ByteCount, CheckResults, Length, Measurement, Phase, Provenance, Requirement,
    WriteExtent,
};

use crate::display::DisplayFormat;
use crate::failure::Failure;
use crate::selection::Checks;

mod text;

use text::{DiagnosticText, Operand, PathText, Shell, Text};

pub(crate) struct Human {
    pub(crate) units: DisplayFormat,
    pub(crate) cwd: Option<PathBuf>,
}

/// Invocation-local failure context is advisory, never a recovery or publication capability.
#[derive(Default)]
pub(crate) struct FailureContext {
    pub(crate) total: Option<usize>,
    pub(crate) completed: Option<usize>,
    pub(crate) state: Option<PathBuf>,
    pub(crate) choices: Vec<PathBuf>,
    pub(crate) explicit_resume: bool,
    pub(crate) live: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum SelectorContext {
    Selectable,
    ExactInput,
    MixedInputs,
}

#[derive(Clone, Copy)]
pub(crate) struct Position {
    pub(crate) current: usize,
    pub(crate) total: usize,
}

impl Human {
    pub(crate) fn header(&self, destination: &Path, length: Length, position: Position) -> String {
        let destination = self.destination(destination);
        let ordinal = if position.total > 1 {
            format!("[{}/{}] ", position.current, position.total)
        } else {
            String::new()
        };
        format!(
            "{ordinal}{}  {}",
            PathText(destination),
            self.length(length)
        )
    }

    pub(crate) fn published(&self, published: &PublishedFile) -> String {
        self.native_receipt(
            published.destination(),
            published.receipt().partial().written,
            *published.receipt().checks(),
            published.version(),
        )
    }

    fn native_receipt(
        &self,
        destination: &Path,
        written: ByteCount,
        checks: CheckResults,
        version: VersionConsistency,
    ) -> String {
        let action = if version == VersionConsistency::LocallyRevalidated {
            "revalidated"
        } else {
            "saved"
        };
        let mut text = format!(
            "{action} {}  {}",
            PathText(self.destination(destination)),
            self.size(written.0)
        );
        self.receipt_checks(&mut text, checks, version);
        text
    }

    pub(crate) fn live_start(&self, length: Length) -> String {
        format!(
            "stdout  {}\n  live output; delivered prefixes cannot be retracted",
            self.length(length)
        )
    }

    pub(crate) fn delivered(
        &self,
        released: ByteCount,
        checks: CheckResults,
        version: VersionConsistency,
    ) -> String {
        let mut text = format!("delivered {} to stdout", self.size(released.0));
        self.receipt_checks(&mut text, checks, version);
        text
    }

    fn receipt_checks(&self, text: &mut String, checks: CheckResults, version: VersionConsistency) {
        let recorded = if version == VersionConsistency::LocallyRevalidated {
            text.push_str("\n  local content and file identity matched recorded evidence; remote version not checked");
            "recorded "
        } else {
            ""
        };
        let measured = checks.measured();
        for requirement in checks.matched_requirements() {
            // Formatting into String cannot fail; only Output owns fallible I/O.
            let _ = write!(
                text,
                "\n  {recorded}required {} matched ({})",
                requirement.digest,
                Self::origins(requirement)
            );
            if measured == Some(requirement.digest) {
                text.push_str("; also measured");
            }
        }
        if let Some(digest) = measured
            && !checks
                .matched_requirements()
                .any(|required| required.digest == digest)
        {
            let _ = write!(text, "\n  {recorded}measured {digest}");
        }
        if version == VersionConsistency::Unpinned
            && !checks
                .matched_requirements()
                .any(|required| Self::strong(required.digest.algorithm()))
        {
            text.push_str("\nwarning: source version unpinned; supply a trusted required checksum when available");
        }
    }

    pub(crate) fn done(
        &self,
        files: usize,
        elapsed: Duration,
        resume_state: Option<&Path>,
    ) -> String {
        let mut text = match files {
            0 => "no files to download".to_owned(),
            _ => format!(
                "done {files} {} in {}.{:02}s",
                Self::plural(files, "file", "files"),
                elapsed.as_secs(),
                elapsed.subsec_millis() / 10
            ),
        };
        if let Some(state) = resume_state {
            let _ = write!(
                text,
                "\n{}\ncontinued recorded assignments and checks, without rediscovering latest",
                Self::state_operand(state)
            );
        }
        text
    }

    pub(crate) fn member(&self, file: &File, context: SelectorContext) -> String {
        let mut text = format!(
            "file {}  {}",
            Text(file.source_path()),
            self.length(file.length())
        );
        if let Some(namespace) = file.namespace() {
            let _ = write!(text, "\n  namespace: {}", Text(namespace.as_str()));
        }
        match (file.member_token(), context) {
            (Some(token), SelectorContext::Selectable) => Self::selector(&mut text, token.as_str()),
            (Some(token), SelectorContext::ExactInput) => {
                let _ = write!(text, "\n  member token: {}", Text(token.as_str()));
                text.push_str("\n  select: exact input already selects this file");
            }
            (Some(token), SelectorContext::MixedInputs) => {
                let _ = write!(
                    text,
                    "\n  member token: {} (identity only; original selection scope not identified)",
                    Text(token.as_str())
                );
            }
            (None, _) => text.push_str("\n  --select-id: unavailable (no literal member token)"),
        }
        let _ = write!(
            text,
            "\n  version: {} (metadata only)",
            Self::evidence(file.version())
        );
        for required in file.requirements() {
            let _ = write!(
                text,
                "\n  required {} ({})",
                required.digest,
                Self::origins(required)
            );
        }
        text
    }

    fn selector(text: &mut String, token: &str) {
        let operand = Operand::text(token);
        match operand.shell() {
            Shell::Posix | Shell::Bash => {
                let _ = write!(
                    text,
                    "\n  select{}: --select-id={operand}",
                    Self::shell_label(&operand)
                );
            }
            Shell::NotCopyable | Shell::Unsupported => {
                let _ = write!(
                    text,
                    "\n  selector {}{}",
                    Text(token),
                    Self::shell_label(&operand)
                );
            }
        }
    }

    pub(crate) fn directory(&self, directory: &Directory) -> String {
        let mut text = format!(
            "directory {}  inspection only",
            Text(directory.source_path())
        );
        if let Some(namespace) = directory.namespace() {
            let _ = write!(text, "\n  namespace: {}", Text(namespace.as_str()));
        }
        text
    }

    pub(crate) fn listed(&self, files: usize, directories: usize, selectors: bool) -> String {
        let mut text = format!(
            "{files} {}, {directories} {}; metadata only",
            Self::plural(files, "file", "files"),
            Self::plural(directories, "directory", "directories")
        );
        if selectors {
            text.push_str("\nuse each selector with its original input");
        }
        text
    }

    pub(crate) fn planned(&self, planned: &PlannedFile, replacement_requested: bool) -> String {
        let mut text = format!(
            "would save {} -> {}  {}",
            Text(planned.file().source_path()),
            PathText(self.destination(planned.destination())),
            self.length(planned.file().length())
        );
        self.policy(&mut text, planned.requirements(), planned.measurement());
        if replacement_requested {
            text.push_str("\n  replacement requested; preview grants no replacement authority");
        }
        text
    }

    pub(crate) fn planned_live(
        &self,
        file: &File,
        checks: &Checks,
        measurement: Measurement,
    ) -> String {
        let mut text = format!(
            "would stream {} -> stdout  {}",
            Text(file.source_path()),
            self.length(file.length())
        );
        let mut proposals = checks.iter().peekable();
        if proposals.peek().is_none() {
            text.push_str("\n  required: none");
        }
        for (digest, origin) in proposals {
            let _ = write!(text, "\n  required {digest} ({})", Self::origin(origin));
        }
        let _ = write!(text, "\n  measurement: {}", Self::measurement(measurement));
        text
    }

    pub(crate) fn planned_end(&self, files: usize) -> String {
        format!(
            "planned {files} {}; not executed",
            Self::plural(files, "file", "files")
        )
    }

    pub(crate) fn info_header(&self, state: &Path, complete: bool) -> String {
        format!(
            "{}\nrecorded complete: {}",
            Self::state_operand(state),
            if complete { "yes" } else { "no" }
        )
    }

    pub(crate) fn saved_member(&self, member: &SavedMember<'_>) -> String {
        let mut text = format!(
            "member {}  {}\n  status: {}\n  key: {}",
            PathText(member.destination()),
            self.length(member.length()),
            Self::status(member.status()),
            member.key()
        );
        self.policy(&mut text, member.requirements(), member.measurement());
        text
    }

    pub(crate) fn info_end(
        &self,
        members: usize,
        statuses: impl Iterator<Item = SavedStatus>,
    ) -> String {
        let mut text = format!(
            "{members} {}; offline metadata only; content not revalidated",
            Self::plural(members, "member", "members")
        );
        let mut seen = [false; 4];
        for status in statuses {
            let (index, description) = match status {
                SavedStatus::Pending => (0, "pending: no attempt recorded"),
                SavedStatus::Submitted => (1, "submitted: outcome needs inspection"),
                SavedStatus::Retained => (2, "retained: failed attempt retained"),
                SavedStatus::Complete => (
                    3,
                    "complete: publication recorded; continuation revalidates",
                ),
            };
            if !seen[index] {
                let _ = write!(text, "\n{description}");
                seen[index] = true;
            }
        }
        text.push_str("\nresume needs --input KEY=REFERENCE for every member\nre-supply the exact original references; they are not stored here");
        text
    }

    fn policy(
        &self,
        text: &mut String,
        required: impl Iterator<Item = Requirement>,
        measurement: Measurement,
    ) {
        let mut required = required.peekable();
        if required.peek().is_none() {
            text.push_str("\n  required: none");
        }
        for requirement in required {
            let _ = write!(
                text,
                "\n  required {} ({})",
                requirement.digest,
                Self::origins(requirement)
            );
        }
        let _ = write!(text, "; measurement: {}", Self::measurement(measurement));
    }

    pub(crate) fn failure(
        &self,
        failure: &Failure,
        context: &FailureContext,
        known_state: Option<&Path>,
    ) -> String {
        let mut text = format!("error: {}", DiagnosticText(failure.message()));
        if let Some(completed) = context.completed {
            match context.total {
                Some(total) => {
                    let _ = write!(text, "\ncompleted: {completed} of {total} files");
                }
                None => {
                    let _ = write!(text, "\ncompleted: {completed} files");
                }
            }
        }
        if let Some(diagnostic) = failure.diagnostic() {
            if let Some(phase) = diagnostic.phase {
                let _ = write!(text, "\nphase: {}", Self::phase(phase));
            }
            if let Some(partial) = diagnostic.partial {
                let written = match partial.write_extent {
                    WriteExtent::Confirmed => "written",
                    WriteExtent::Unconfirmed => "written at least",
                };
                let _ = write!(
                    text,
                    "\npartial: received {}; {written} {}; hashed {}",
                    self.size(partial.received.0),
                    self.size(partial.written.0),
                    self.size(partial.hashed.0)
                );
                text.push_str(match partial.write_extent {
                    WriteExtent::Confirmed => "\nthese counts are not completion evidence",
                    WriteExtent::Unconfirmed => {
                        "\nwrite extent unconfirmed; these counts are not completion evidence"
                    }
                });
            }
            if diagnostic.checkpoint_failed {
                text.push_str(
                    "\ncheckpoint failed; retained state is not a successful pause receipt",
                );
            }
        }
        let state = context.state.as_deref().or(known_state);
        if let Some(state) = state {
            let _ = write!(text, "\nstate: {}", PathText(state));
        }
        if let Some(native) = failure
            .diagnostic()
            .and_then(|diagnostic| diagnostic.native_state.as_deref())
        {
            let _ = write!(text, "\nnative state: {}", PathText(native));
        }
        if let Some(state) = state {
            Self::inspect(&mut text, state);
        }
        for candidate in &context.choices {
            let _ = write!(text, "\ncandidate state: {}", PathText(candidate));
            Self::inspect(&mut text, candidate);
        }
        if context.live {
            text.push_str("\nstdout may contain irreversible prefixes; completion not confirmed");
        }
        text
    }

    fn state_operand(state: &Path) -> String {
        let operand = Operand::path(state);
        match operand.shell() {
            Shell::Posix | Shell::Bash => {
                format!("state{}: {operand}", Self::shell_label(&operand))
            }
            Shell::NotCopyable | Shell::Unsupported => {
                format!("state{}: {}", Self::shell_label(&operand), PathText(state))
            }
        }
    }

    fn inspect(text: &mut String, state: &Path) {
        let operand = Operand::path(state);
        match operand.shell() {
            Shell::Posix | Shell::Bash => {
                let _ = write!(
                    text,
                    "\ninspect{}: xget info {operand}",
                    Self::shell_label(&operand)
                );
            }
            Shell::NotCopyable | Shell::Unsupported => {
                let _ = write!(
                    text,
                    "\ninspect{}: no shell command available",
                    Self::shell_label(&operand)
                );
            }
        }
    }

    fn shell_label(operand: &Operand<'_>) -> &'static str {
        match operand.shell() {
            Shell::Posix => "",
            Shell::Bash => " (bash/zsh)",
            Shell::NotCopyable => " (not shell-copyable)",
            Shell::Unsupported => " (unsupported native encoding)",
        }
    }

    pub(crate) fn size(&self, bytes: u64) -> String {
        match self.units {
            DisplayFormat::RawBytes => format!("{} B", self.units.size(bytes)),
            DisplayFormat::HumanBytes | DisplayFormat::HumanBits => self.units.size(bytes),
        }
    }

    pub(crate) fn speed(&self, bytes_per_second: u64) -> String {
        match self.units {
            DisplayFormat::RawBytes => format!("bytes/s: {}", self.units.size(bytes_per_second)),
            DisplayFormat::HumanBytes | DisplayFormat::HumanBits => {
                self.units.speed(bytes_per_second)
            }
        }
    }

    fn length(&self, length: Length) -> String {
        match length {
            Length::Known(bytes) => self.size(bytes.0),
            Length::Unknown => "unknown size".to_owned(),
        }
    }

    fn destination<'a>(&self, destination: &'a Path) -> &'a Path {
        self.cwd
            .as_deref()
            .and_then(|cwd| destination.strip_prefix(cwd).ok())
            .filter(|relative| !relative.as_os_str().is_empty())
            .unwrap_or(destination)
    }

    fn origins(requirement: Requirement) -> String {
        let mut text = String::new();
        for origin in requirement.origins.iter() {
            if !text.is_empty() {
                text.push_str(", ");
            }
            text.push_str(Self::origin(origin));
        }
        text
    }

    fn origin(origin: Provenance) -> &'static str {
        match origin {
            Provenance::Caller => "caller",
            Provenance::Source => "source",
            Provenance::Sidecar => "sidecar",
        }
    }

    fn measurement(measurement: Measurement) -> String {
        match measurement {
            Measurement::None => "none".to_owned(),
            Measurement::Algorithm(algorithm) => algorithm.to_string(),
        }
    }

    fn evidence(evidence: VersionEvidence) -> &'static str {
        match evidence {
            VersionEvidence::StrongEtag => "strong etag evidence",
            VersionEvidence::Unpinned => "unpinned",
        }
    }

    fn status(status: SavedStatus) -> &'static str {
        match status {
            SavedStatus::Pending => "pending",
            SavedStatus::Submitted => "submitted",
            SavedStatus::Retained => "retained",
            SavedStatus::Complete => "complete",
        }
    }

    fn phase(phase: Phase) -> &'static str {
        match phase {
            Phase::Acquire => "acquire",
            Phase::Write => "write",
            Phase::ReadStage => "read stage",
            Phase::Seal => "seal",
            Phase::Checkpoint => "checkpoint",
            Phase::Publish => "publish",
            Phase::Reconcile => "reconcile",
        }
    }

    fn strong(algorithm: Algorithm) -> bool {
        match algorithm {
            Algorithm::Sha256 | Algorithm::Sha512 | Algorithm::Blake3 => true,
            Algorithm::Md5 | Algorithm::Sha1 => false,
        }
    }

    fn plural(count: usize, singular: &'static str, plural: &'static str) -> &'static str {
        if count == 1 { singular } else { plural }
    }
}

#[cfg(test)]
#[path = "human_tests.rs"]
mod tests;
