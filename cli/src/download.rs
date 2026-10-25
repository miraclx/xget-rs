//! Ordinary downloads compose common Client operations; no provider scheduling lives here.

use std::path::{Path, PathBuf};

use clap::Args;
use futures::StreamExt as _;
use serde_json::json;
use tokio::io;
use tokio::io::AsyncWriteExt as _;
use xget::client::{Fresh, Overwrite, Restart};
use xget::transfer::{Algorithm, Digest, Measurement, Provenance};
use xget::{Checksum, Client, Input};

use crate::args::Inputs;
use crate::failure::Failure;
use crate::output::{ItemId, Output, RunId};
use crate::reporting::{Presentation, Reporter};
use crate::selection::{Checks, OneFile};
use crate::sidecar::LocalSidecar;

#[derive(Args, Default)]
pub(crate) struct Command {
    #[command(flatten)]
    inputs: Inputs,
    #[command(flatten)]
    transport: crate::args::Transport,
    /// Exact file target, asserting one selected file; '-' streams irreversible checked output.
    #[arg(short = 'o', long, value_name = "FILE", conflicts_with = "directory")]
    output: Option<PathBuf>,
    /// Explicit directory authority; never inferred from another positional operand.
    #[arg(short = 'D', long, value_name = "DIR")]
    directory: Option<PathBuf>,
    /// Inspect the real frozen native plan; no payload, native stage or run-store effects.
    #[arg(long)]
    dry_run: bool,
    /// Discover anew instead of continuing matching unfinished work; grants no overwrite authority.
    #[arg(long, conflicts_with = "restart")]
    fresh: bool,
    /// Start a new owned attempt for pending receiving work in a matching frozen run, retaining old partials.
    /// Completed outputs are revalidated, not refetched. No match is an error, not fresh discovery.
    /// Closed, foreign or uncertain attempts are refused. No overwrite or cleanup authority.
    #[arg(long, conflicts_with_all = ["fresh", "dry_run", "expect_file"])]
    restart: bool,
    /// Permit checked guarded replacement of a captured incumbent, after required checks and sealing.
    /// This never permits duplicate/aliased targets within a run. Fresh does not imply this permission;
    /// previews grant no replacement authority, and arbitrary-writer inode CAS is not promised.
    #[arg(short = 'f', long)]
    overwrite: bool,
    /// Optional measured checksum, separate from requirements; none creates no hidden recovery hash.
    #[arg(short = 's', long, default_value = "sha256")]
    checksum: Checksum,
    /// Required whole-file algorithm:hex; checked before publication or successful live EOF.
    #[arg(long, value_name = "ALGORITHM:HEX")]
    expect: Option<Digest>,
    /// LOCAL regular checksum sidecar, at most 65536 bytes and 256 records. Each record is
    /// algorithm:hex or bare hex, optionally two spaces or space-star plus the EXACT original member
    /// path. No basename, output-rename, Unicode/percent normalization, BSD or escaped GNU matching.
    /// Select exactly one file before using any sidecar, including labelled multi-entry documents.
    /// Native sidecars separately require --fresh and one original input so mutable matching/layout
    /// are not silently changed. Remote sidecars and stdin are unsupported.
    #[arg(long, value_name = "FILE")]
    expect_file: Option<PathBuf>,
    /// Algorithm for bare hex in --expect-file only (sha256 when omitted); independent of -s.
    #[arg(long, value_name = "ALGORITHM", requires = "expect_file")]
    expect_file_algorithm: Option<Algorithm>,
    #[command(flatten)]
    presentation: Presentation,
}

pub(crate) struct Prepared {
    inputs: Vec<Input>,
    transport: crate::args::Transport,
    target: Target,
    dry_run: bool,
    fresh: bool,
    restart: bool,
    overwrite: bool,
    measurement: Measurement,
    expect: Option<Digest>,
    sidecar: Option<LocalSidecar>,
    presentation: Presentation,
}

enum Target {
    Default,
    File(PathBuf),
    Directory(PathBuf),
    Stdout,
}

impl Command {
    pub(crate) fn output(&self, run: RunId) -> Output {
        self.presentation.output(run)
    }

    pub(crate) fn prepare(self) -> Result<Prepared, Failure> {
        if self.restart && (self.fresh || self.dry_run || self.expect_file.is_some()) {
            return Err(Failure::request(
                "restart needs frozen execution without fresh, preview or a local sidecar; inspect the saved run and continue its recorded check policy",
            ));
        }
        let inputs = self.inputs.prepare_execution()?;
        self.transport.validate(&inputs)?;
        let target = match (self.output, self.directory) {
            (Some(file), None) if file == Path::new("-") => Target::Stdout,
            (Some(file), None) => Target::File(file),
            (None, Some(directory)) => Target::Directory(directory),
            (None, None) => Target::Default,
            (Some(_), Some(_)) => {
                return Err(Failure::request("choose -o FILE or -D DIR, not both"));
            }
        };
        if matches!(target, Target::Stdout) && (self.fresh || self.restart || self.overwrite) {
            return Err(Failure::request(
                "fresh, restart and overwrite apply to native file work, not irreversible stdout; remove those flags for live output",
            ));
        }
        if self.expect_file.as_deref() == Some(Path::new("-")) {
            return Err(Failure::request(
                "checksum sidecar stdin is unsupported; supply a bounded regular local file",
            ));
        }
        if self.expect_file.is_some()
            && !matches!(target, Target::Stdout)
            && (!self.fresh || inputs.len() != 1)
        {
            return Err(Failure::request(
                "native local sidecars currently need --fresh and one original input; choose a new destination or use a literal expectation for automatic continuation",
            ));
        }
        let sidecar = self.expect_file.map(|path| LocalSidecar {
            path,
            algorithm: self.expect_file_algorithm.unwrap_or(Algorithm::Sha256),
        });
        Ok(Prepared {
            inputs,
            transport: self.transport,
            target,
            dry_run: self.dry_run,
            fresh: self.fresh,
            restart: self.restart,
            overwrite: self.overwrite,
            measurement: measurement(self.checksum),
            expect: self.expect,
            sidecar,
            presentation: self.presentation,
        })
    }
}

impl Prepared {
    pub(crate) fn gateway(&self) -> Option<&str> {
        self.transport.gateway()
    }

    pub(crate) fn needs_s3(&self) -> bool {
        self.inputs
            .iter()
            .any(|input| input.kind() == xget::input::InputKind::S3)
    }

    pub(crate) async fn run(self, client: &Client, output: &Output) -> Result<(), Failure> {
        self.execute(client, output).await
    }

    async fn execute(self, client: &Client, output: &Output) -> Result<(), Failure> {
        if matches!(self.target, Target::Stdout) {
            return self.stream(client, output).await;
        }
        let (inputs, checks) = if self.sidecar.is_some() {
            // Preparation requires one original input and explicit Fresh. Moving this one frozen
            // File back into Input preserves collection context without dropping other occurrences.
            let selected = OneFile::resolve(client, self.inputs, self.expect, self.sidecar).await?;
            (vec![Input::from(selected.file)], selected.checks)
        } else {
            let mut checks = Checks::default();
            if let Some(required) = self.expect {
                checks.add(required, Provenance::Caller)?;
            }
            (self.inputs, checks)
        };
        let mut pending = client.download_all(inputs).measurement(self.measurement);
        // Keep the origin of every required check when configuring the common coordinator.
        for (digest, origin) in checks.iter() {
            pending = pending.require(digest, origin);
        }
        if self.fresh {
            pending = pending.fresh(Fresh);
        }
        if self.restart {
            pending = pending.restart(Restart);
        }
        if self.overwrite {
            pending = pending.overwrite(Overwrite);
        }
        pending = match self.target {
            Target::File(path) => pending.to_file(path),
            Target::Directory(path) => pending.to_directory(path),
            Target::Default => pending,
            Target::Stdout => return Err(Failure::request("invalid native output target")),
        };
        if self.dry_run {
            let plan = pending.plan().await.map_err(Failure::library)?;
            for (index, planned) in plan.files().enumerate() {
                output.human(|human| human.planned(planned, self.overwrite))?;
                output.emit("Planned", Some(ItemId(index)), json!({
                    "source_path": planned.file().source_path(),
                    "destination": Output::path(planned.destination()),
                    "length_bytes": Output::length(planned.file().length()),
                    "required_digests": planned.requirements().map(|required| required.digest.to_string()).collect::<Vec<_>>(),
                    "measurement": Output::measurement(planned.measurement()),
                    "replacement_requested": self.overwrite,
                    "execution_authority": false,
                }))?;
            }
            output.human(|human| human.planned_end(plan.files().len()))?;
            return output.emit(
                "End",
                None,
                json!({"planned_files": plan.files().len(), "executed": false}),
            );
        }
        let mut reporter = Reporter::new(output, self.presentation);
        // The common coordinator owns discovery, frozen continuation, checks and publication.
        let result = pending.run_with(&mut reporter).await;
        match result {
            Ok(_) => reporter.complete(),
            Err(failure) => {
                reporter.finish()?;
                Self::recovery(&failure.error, output)?;
                Err(Failure::library(failure.error))
            }
        }
    }

    async fn stream(self, client: &Client, output: &Output) -> Result<(), Failure> {
        let selected = OneFile::resolve(client, self.inputs, self.expect, self.sidecar).await?;
        if self.dry_run {
            output.human(|human| {
                human.planned_live(&selected.file, &selected.checks, self.measurement)
            })?;
            output.emit(
                "PlannedLive",
                Some(ItemId(0)),
                selected.live_inspection(self.measurement),
            )?;
            output.human(|human| human.planned_end(1))?;
            return output.emit("End", None, json!({
                "planned_files": 1, "executed": false, "payload_opened": false, "output_created": false,
            }));
        }
        let mut opened = client
            .open(selected.file)
            .await
            .map_err(Failure::library)?
            .measurement(self.measurement);
        // Preflight prevents conflicts before opening; the common body still enforces every
        // proposal with its original provenance, including Sidecar rather than Caller.
        for (digest, origin) in selected.checks.iter() {
            opened = opened.require(digest, origin).map_err(Failure::library)?;
        }
        let length = opened.length();
        let mut body = opened.into_body();
        let mut stdout = io::stdout();
        let mut reporter = Reporter::new(output, self.presentation);
        output.live();
        reporter.live_started(length)?;
        while let Some(chunk) = body.next().await {
            let bytes = chunk.map_err(|_| Failure::runtime(
                "live source, length or required-check failure; stdout may contain irreversible prefixes and is not verified complete"
            ))?;
            stdout.write_all(&bytes).await.map_err(|_| Failure::runtime(
                "stdout write failed; delivered prefixes cannot be retracted and completion is not verified"
            ))?;
            // Tokio's stdout wraps the process line buffer. Binary prefixes without newlines must
            // reach a waiting consumer before the next source poll, not wait until source EOF.
            stdout.flush().await.map_err(|_| Failure::runtime(
                "stdout flush failed; delivered prefixes cannot be retracted and completion is not verified"
            ))?;
            let progress = body.progress();
            reporter.live_progress(length, progress, progress.released)?;
        }
        let receipt = body.finish().map_err(|_| {
            Failure::runtime(
                "live body did not finish successfully; no completion receipt is available",
            )
        })?;
        stdout.flush().await.map_err(|_| {
            Failure::runtime(
                "stdout flush failed; body checks do not prove successful sink delivery",
            )
        })?;
        reporter.finish()?;
        output.human(|human| {
            human.delivered(
                receipt.body().progress().released,
                receipt.body().checks(),
                receipt.version(),
            )
        })?;
        output.emit(
            "Delivered",
            Some(ItemId(0)),
            json!({
                "published": false,
                "received_bytes": receipt.body().progress().received.0,
                "released_bytes": receipt.body().progress().released.0,
                "hashed_bytes": receipt.body().progress().hashed.0,
                "checks": Output::checks(receipt.body().checks()),
                "version": Output::version(receipt.version()),
            }),
        )?;
        output.human_done(1)?;
        output.emit(
            "End",
            None,
            json!({"complete": true, "delivered_files": 1, "published_files": 0}),
        )
    }

    pub(crate) fn recovery(error: &xget::client::Error, output: &Output) -> Result<(), Failure> {
        if let xget::client::Error::Saved(saved) = error {
            let choices = match saved {
                xget::client::SavedError::Ambiguous { resume_paths } => resume_paths.as_slice(),
                _ => &[],
            };
            output.human_recovery(saved.saved_run(), choices);
            if let xget::client::SavedError::Ambiguous { resume_paths } = saved {
                output.emit("ResumeChoices", None, json!({
                    "state_paths": resume_paths.iter().map(|path| Output::path(path)).collect::<Vec<_>>()
                }))?;
            }
            if let Some(path) = saved.saved_run() {
                output.emit(
                    "RetainedState",
                    None,
                    json!({"state_path": Output::path(path)}),
                )?;
            }
        }
        Ok(())
    }
}

fn measurement(checksum: Checksum) -> Measurement {
    match checksum {
        Checksum::None => Measurement::None,
        Checksum::Md5 => Measurement::Algorithm(Algorithm::Md5),
        Checksum::Sha1 => Measurement::Algorithm(Algorithm::Sha1),
        Checksum::Sha256 => Measurement::Algorithm(Algorithm::Sha256),
        Checksum::Sha512 => Measurement::Algorithm(Algorithm::Sha512),
        Checksum::Blake3 => Measurement::Algorithm(Algorithm::Blake3),
    }
}

#[cfg(test)]
#[path = "download_tests.rs"]
mod tests;
