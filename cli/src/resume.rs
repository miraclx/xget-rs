//! Explicit continuation of frozen owned members. Rebinding is not rediscovery or selection.

use std::path::PathBuf;

use clap::Args;
use xget::client::{BindingKey, Overwrite, Restart, SavedRun};
use xget::{Client, Input};
use zeroize::Zeroizing;

use crate::args::Reference;
use crate::download;
use crate::failure::Failure;
use crate::output::{Output, RunId};
use crate::reporting::{Presentation, Reporter};

#[derive(Args)]
pub(crate) struct Command {
    /// Actual owned run path printed by the original invocation or info.
    state: PathBuf,
    #[command(flatten)]
    transport: crate::args::Transport,
    /// Re-supply every required binding from info; keys are not row positions or selectors.
    #[arg(long = "input", value_name = "KEY=REFERENCE", required = true)]
    bindings: Vec<Reference>,
    /// Renew access headers only when all bindings resupply the same original HTTP/WebDAV reference.
    /// Headers do not replace frozen versions/checks and are not persisted. Use the Rust API for mixed access.
    #[arg(short = 'H', long = "header", value_name = "NAME: VALUE")]
    headers: Vec<xget::input::RequestHeader>,
    /// Start a new attempt only for this frozen run's pending receiving work. Old attempts remain;
    /// completed outputs are revalidated, not deleted or fetched again. Closed, foreign or uncertain
    /// attempts are refused. No discovery or cleanup.
    #[arg(long)]
    restart: bool,
    /// Explicit checked replacement authority for this continuation only, not loaded from records.
    /// Required checks and sealing precede replacement; intra-run aliases remain forbidden.
    #[arg(short = 'f', long)]
    overwrite: bool,
    #[command(flatten)]
    presentation: Presentation,
}

pub(crate) struct Prepared {
    saved: SavedRun,
    transport: crate::args::Transport,
    bindings: Vec<(BindingKey, Input)>,
    restart: bool,
    overwrite: bool,
    presentation: Presentation,
}

impl Command {
    pub(crate) fn output(&self, run: RunId) -> Output {
        let output = self.presentation.output(run);
        output.explicit_resume();
        output.human_recovery(Some(&self.state), &[]);
        output
    }

    pub(crate) async fn prepare(self, output: &Output) -> Result<Prepared, Failure> {
        // Inspection is offline, before transport construction or credential discovery.
        let saved = SavedRun::inspect(self.state)
            .await
            .map_err(Failure::saved)?;
        output.human_recovery(Some(saved.path()), &[]);
        let mut bindings = Vec::new();
        // A shared CLI convenience must not broadcast credentials across different original inputs.
        let mut header_reference: Option<Zeroizing<String>> = None;
        for spelling in self.bindings {
            let (key, reference) = spelling.as_str().split_once('=').ok_or_else(|| {
                Failure::request(
                    "invalid input binding; use KEY=REFERENCE with the key printed by info",
                )
            })?;
            // Use the public binding encoding, never Debug or row positions.
            let key: BindingKey = key.parse().map_err(|_| {
                Failure::request("invalid binding key; copy the complete key printed by info")
            })?;
            let member = saved
                .members()
                .find(|member| member.key() == key)
                .ok_or_else(|| {
                    Failure::request(
                        "binding key does not belong to this saved run; inspect this state path",
                    )
                })?;
            if bindings.iter().any(|(existing, _)| *existing == key) {
                return Err(Failure::request(
                    "duplicate binding key; supply each saved member exactly once",
                ));
            }
            // Reconstruct frozen intent without reevaluating selectors or guessing from fingerprints.
            // Older records without the recipe may require the original explicit intent.
            let mut input = member.binding_input(reference).map_err(Failure::saved)?;
            if !self.headers.is_empty() {
                if header_reference
                    .as_ref()
                    .is_some_and(|original| original.as_str() != reference)
                {
                    return Err(Failure::request(
                        "resume access headers need one original reference spelling; use input-local Rust bindings for mixed credentials",
                    ));
                }
                if header_reference.is_none() {
                    header_reference = Some(Zeroizing::new(reference.to_owned()));
                }
                for header in &self.headers {
                    input = input
                        .request_header(xget::input::RequestHeader::clone(header))
                        .map_err(|error| Failure::request(&error.to_string()))?;
                }
            }
            bindings.push((key, input));
        }
        if bindings.len() != saved.members().len() {
            return Err(Failure::request(
                "missing input bindings; inspect info and supply every frozen member key",
            ));
        }
        self.transport
            .validate(bindings.iter().map(|(_, input)| input))?;
        Ok(Prepared {
            saved,
            transport: self.transport,
            bindings,
            restart: self.restart,
            overwrite: self.overwrite,
            presentation: self.presentation,
        })
    }
}

impl Prepared {
    pub(crate) fn gateway(&self) -> Option<&str> {
        self.transport.gateway()
    }

    pub(crate) fn needs_s3(&self) -> bool {
        self.bindings
            .iter()
            .any(|(_, input)| input.kind() == xget::input::InputKind::S3)
    }

    pub(crate) async fn run(self, client: &Client, output: &Output) -> Result<(), Failure> {
        output.bind_saved(Some(self.saved.id()), Some(self.saved.path()));
        let mut resume = client.resume(self.saved);
        if self.restart {
            resume = resume.restart(Restart);
        }
        if self.overwrite {
            resume = resume.overwrite(Overwrite);
        }
        for (key, input) in self.bindings {
            resume = resume.input(key, input);
        }
        let mut reporter = Reporter::new(output, self.presentation);
        // Observe the existing coordinator without adding another recovery path.
        match resume.run_with(&mut reporter).await {
            Ok(_) => reporter.complete(),
            Err(failure) => {
                reporter.finish()?;
                download::Prepared::recovery(&failure.error, output)?;
                Err(Failure::library(failure.error))
            }
        }
    }
}
