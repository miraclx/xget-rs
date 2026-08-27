//! The checksum computed over the downloaded bytes.

use core::fmt;
use core::str::FromStr;

/// Which checksum to compute over the resource as it streams. `None` skips hashing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Checksum {
    /// Skip hashing.
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
        }
    }

    /// A fresh boxed hasher, or `None` for [`Checksum::None`].
    pub(crate) fn hasher(self) -> Option<Box<dyn digest::DynDigest>> {
        match self {
            Checksum::None => Option::None,
            Checksum::Md5 => Some(Box::new(md5::Md5::default())),
            Checksum::Sha1 => Some(Box::new(sha1::Sha1::default())),
            Checksum::Sha256 => Some(Box::new(sha2::Sha256::default())),
            Checksum::Sha512 => Some(Box::new(sha2::Sha512::default())),
        }
    }
}

impl fmt::Display for Checksum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// The error from parsing an unknown checksum name.
#[derive(Debug, thiserror::Error)]
#[error("unknown checksum (use none, md5, sha1, sha256, or sha512)")]
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
            _ => Err(UnknownChecksum),
        }
    }
}
