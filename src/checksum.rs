//! Algorithm selection for optional checksum measurement, separate from required checks.

use core::fmt;
use core::str::FromStr;

/// Which optional checksum to measure over the resource as it streams.
///
/// This selection does not disable required checks. See [`crate::transfer::Measurement`] for the
/// transfer policy: hashing is skipped only when measurement is disabled and no checks are required.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Checksum {
    /// Disable optional measurement; required checks still hash and must match.
    None,
    /// MD5.
    Md5,
    /// SHA-1.
    Sha1,
    /// SHA-256, the default.
    #[default]
    Sha256,
    /// SHA-512.
    Sha512,
    /// BLAKE3, fast and parallelizable.
    Blake3,
}

impl Checksum {
    /// The lowercase algorithm name, or `none`.
    pub const fn name(self) -> &'static str {
        match self {
            Checksum::None => "none",
            Checksum::Md5 => "md5",
            Checksum::Sha1 => "sha1",
            Checksum::Sha256 => "sha256",
            Checksum::Sha512 => "sha512",
            Checksum::Blake3 => "blake3",
        }
    }
}

/// A concrete incremental owner for checked native transfers, preserving conditional Send.
/// No hasher internals are persisted.
#[derive(Clone, Default)]
pub(crate) struct OwnedSha256(sha2::Sha256);

impl OwnedSha256 {
    pub(crate) fn update(&mut self, bytes: &[u8]) {
        use sha2::Digest as _;
        self.0.update(bytes);
    }

    pub(crate) fn finish(self) -> [u8; 32] {
        use sha2::Digest as _;
        self.0.finalize().into()
    }
}

impl fmt::Display for Checksum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// The error from parsing an unknown checksum name.
#[derive(Debug, thiserror::Error)]
#[error("unknown checksum (use none, md5, sha1, sha256, sha512, or blake3)")]
pub struct UnknownChecksum;

impl FromStr for Checksum {
    type Err = UnknownChecksum;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "none" => Ok(Checksum::None),
            "md5" => Ok(Checksum::Md5),
            "sha1" => Ok(Checksum::Sha1),
            "sha256" => Ok(Checksum::Sha256),
            "sha512" => Ok(Checksum::Sha512),
            "blake3" => Ok(Checksum::Blake3),
            _ => Err(UnknownChecksum),
        }
    }
}
