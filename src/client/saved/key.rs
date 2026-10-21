//! Versioned non-secret correlation keys, separate from execution authority and remote selectors.

use core::fmt;
use core::str::FromStr;
use std::path::Path;

use super::super::{PublishedFile, Report};
use super::{BindingKey, Fingerprint, SavedRun};
use crate::inventory::MAX_FILES;

const RUN_PREFIX: &str = "xget-run-v1:";
const MEMBER_PREFIX: &str = "xget-binding-v1:";

/// Correlates one canonical saved-run workspace without exposing its path.
///
/// This is not a remote identity, credential or execution lease. Moving or copying a workspace does
/// not promise to preserve the key: inspection still validates the recorded filesystem bindings.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct RunId(Fingerprint);

impl RunId {
    pub(crate) fn for_path(path: &Path) -> Self {
        Self(Fingerprint::bytes(path.as_os_str().as_encoded_bytes()))
    }
}

impl fmt::Display for RunId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(RUN_PREFIX)?;
        write_hash(formatter, self.0)
    }
}

impl fmt::Debug for RunId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RunId([saved workspace])")
    }
}

impl FromStr for RunId {
    type Err = BindingKeyParseError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let hash = text.strip_prefix(RUN_PREFIX).ok_or(BindingKeyParseError)?;
        Ok(Self(parse_hash(hash)?))
    }
}

impl SavedRun {
    /// Stable correlation for this inspected canonical workspace, not execution authority.
    pub fn id(&self) -> RunId {
        RunId::for_path(self.path())
    }
}

impl Report {
    /// Saved-workspace correlation when this execution acquired or matched a frozen run.
    /// An unowned failure or genuine empty no-op has no saved-run identifier.
    pub fn run_id(&self) -> Option<RunId> {
        self.saved_run.as_deref().map(RunId::for_path)
    }

    /// Acknowledged publications paired with their frozen member keys, in execution order.
    ///
    /// Files currently execute serially and stop on the first failure, so completed entries are a
    /// prefix of the saved member sequence. These keys also cover revalidated earlier publications.
    /// No key is invented when the report has no saved-run context.
    pub fn bound_completed(
        &self,
    ) -> Option<impl ExactSizeIterator<Item = (BindingKey, &PublishedFile)> + '_> {
        let run = self.run_id()?;
        Some(
            self.completed
                .iter()
                .enumerate()
                .map(move |(index, published)| {
                    // The common coordinator bounds the complete inventory to MAX_FILES before execution.
                    let Some(key) = BindingKey::for_run_member(run, index) else {
                        unreachable!("completed publications are a bounded saved-member prefix");
                    };
                    (key, published)
                }),
        )
    }
}

impl BindingKey {
    /// Internal construction is bounded just like parsed keys; it grants no execution authority.
    pub(crate) fn for_run_member(run: RunId, index: usize) -> Option<Self> {
        if index >= MAX_FILES {
            return None;
        }
        Some(Self {
            run: run.0,
            index: index as u32,
        })
    }

    /// Saved-workspace correlation carried by this frozen member key.
    pub fn run_id(self) -> RunId {
        RunId(self.run)
    }
}

impl fmt::Display for BindingKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(MEMBER_PREFIX)?;
        write_hash(formatter, self.run)?;
        write!(formatter, ":{:08x}", self.index)
    }
}

impl FromStr for BindingKey {
    type Err = BindingKeyParseError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        // Exact widths and lowercase hexadecimal provide one spelling before any ownership lookup.
        let payload = text
            .strip_prefix(MEMBER_PREFIX)
            .ok_or(BindingKeyParseError)?;
        if payload.len() != 73 {
            return Err(BindingKeyParseError);
        }
        let (hash, index) = payload.split_once(':').ok_or(BindingKeyParseError)?;
        let run = parse_hash(hash)?;
        if index.len() != 8 || !index.bytes().all(canonical_hex) {
            return Err(BindingKeyParseError);
        }
        let index = u32::from_str_radix(index, 16).map_err(|_| BindingKeyParseError)?;
        if index as usize >= MAX_FILES {
            return Err(BindingKeyParseError);
        }
        Ok(Self { run, index })
    }
}

/// Invalid or unsupported textual saved-run/member correlation, without retaining rejected text.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error(
    "invalid saved binding key; copy the complete versioned key from inspected run information"
)]
pub struct BindingKeyParseError;

// These helpers encode one fixed-width metadata hash, not payload bytes or bearer access.
fn write_hash(formatter: &mut fmt::Formatter<'_>, hash: Fingerprint) -> fmt::Result {
    for byte in hash.0 {
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}

fn parse_hash(text: &str) -> Result<Fingerprint, BindingKeyParseError> {
    if text.len() != 64 || !text.bytes().all(canonical_hex) {
        return Err(BindingKeyParseError);
    }
    let mut bytes = [0; 32];
    hex::decode_to_slice(text, &mut bytes).map_err(|_| BindingKeyParseError)?;
    Ok(Fingerprint(bytes))
}

fn canonical_hex(byte: u8) -> bool {
    byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
}

#[cfg(test)]
#[path = "key_tests.rs"]
mod tests;
