//! One selected-file preflight shared by live execution and metadata-only live inspection.

use serde_json::{Value, json};
use xget::inventory::File;
use xget::transfer::{Digest, Measurement, Provenance};
use xget::{Client, Input};

use crate::failure::Failure;
use crate::output::Output;
use crate::sidecar::LocalSidecar;

pub(crate) struct OneFile {
    pub(crate) file: File,
    pub(crate) checks: Checks,
    metadata_bytes: usize,
    directories: usize,
}

impl OneFile {
    pub(crate) async fn resolve(
        client: &Client,
        inputs: Vec<Input>,
        expected: Option<Digest>,
        sidecar: Option<LocalSidecar>,
    ) -> Result<Self, Failure> {
        let inventory = client.resolve_all(inputs).await.map_err(Failure::library)?;
        let metadata_bytes = inventory.metadata_bytes();
        let directories = inventory.directories().len();
        let file = inventory.select_one(|_| true).map_err(|_| Failure::request(
            "output or global expectation needs exactly one selected file; list the complete scope and select a member"
        ))?;
        let mut checks = Checks::default();
        for required in file.requirements() {
            for origin in required.origins.iter() {
                checks.add(required.digest, origin)?;
            }
        }
        if let Some(expected) = expected {
            checks.add(expected, Provenance::Caller)?;
        }
        if let Some(sidecar) = sidecar {
            for digest in sidecar.read().await?.bind(file.source_path())? {
                checks.add(digest, Provenance::Sidecar)?;
            }
        }
        Ok(Self {
            file,
            checks,
            metadata_bytes,
            directories,
        })
    }

    pub(crate) fn live_inspection(&self, measurement: Measurement) -> Value {
        json!({
            "target": "Stdout",
            "source_path": self.file.source_path(),
            "length_bytes": Output::length(self.file.length()),
            "version_evidence": Output::evidence(self.file.version()),
            "measurement": Output::measurement(measurement),
            "check_proposals": self.checks.inspection(),
            "accounted_metadata_bytes": self.metadata_bytes,
            "inspection_only_directories": self.directories,
            "payload_opened": false,
            "execution_authority": false,
        })
    }
}

/// This bounded metadata conflict preflight does no hashing or byte admission. The common checked
/// body/native coordinator remains the authority that merges and enforces these typed proposals.
#[derive(Default)]
pub(crate) struct Checks(Vec<Proposal>);

struct Proposal {
    digest: Digest,
    origin: Provenance,
}

impl Checks {
    pub(crate) fn add(&mut self, digest: Digest, origin: Provenance) -> Result<(), Failure> {
        if self.0.iter().any(|existing| {
            existing.digest.algorithm() == digest.algorithm() && existing.digest != digest
        }) {
            return Err(Failure::request(
                "required checks conflict; correct the literal, source or local sidecar expectation before opening payload",
            ));
        }
        if !self
            .0
            .iter()
            .any(|existing| existing.digest == digest && existing.origin == origin)
        {
            // Five algorithms and three closed provenance variants bound agreeing proposals to 15.
            self.0.push(Proposal { digest, origin });
        }
        Ok(())
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (Digest, Provenance)> {
        self.0
            .iter()
            .map(|proposal| (proposal.digest, proposal.origin))
    }

    pub(crate) fn inspection(&self) -> Value {
        json!(self.iter().map(|(digest, origin)| json!({
            "digest": digest.to_string(),
            "origin": match origin { Provenance::Caller => "Caller", Provenance::Source => "Source", Provenance::Sidecar => "Sidecar" },
        })).collect::<Vec<_>>())
    }
}

#[cfg(test)]
#[path = "selection_tests.rs"]
mod tests;
