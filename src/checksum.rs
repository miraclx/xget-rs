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

    /// A fresh boxed hasher, or `None` for [`Checksum::None`].
    pub(crate) fn hasher(self) -> Option<Box<dyn Hasher>> {
        match self {
            Checksum::None => Option::None,
            Checksum::Md5 => Some(Box::new(Digest(Box::new(md5::Md5::default())))),
            Checksum::Sha1 => Some(Box::new(Digest(Box::new(sha1::Sha1::default())))),
            Checksum::Sha256 => Some(Box::new(Digest(Box::new(sha2::Sha256::default())))),
            Checksum::Sha512 => Some(Box::new(Digest(Box::new(sha2::Sha512::default())))),
            Checksum::Blake3 => Some(Box::new(blake3::Hasher::new())),
        }
    }
}

/// A streaming hasher the engine feeds bytes and finalizes to a lowercase hex digest. This abstracts
/// over the RustCrypto `digest` families and BLAKE3 (whose own API sidesteps a `digest` version clash),
/// so the engine never names either.
pub(crate) trait Hasher {
    /// Fold `bytes` into the running digest.
    fn update(&mut self, bytes: &[u8]);
    /// Consume the hasher and return its lowercase hex digest.
    fn finalize_hex(self: Box<Self>) -> String;
}

/// Adapts any boxed RustCrypto [`digest::DynDigest`] to [`Hasher`].
struct Digest(Box<dyn digest::DynDigest>);

impl Hasher for Digest {
    fn update(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    fn finalize_hex(self: Box<Self>) -> String {
        hex::encode(self.0.finalize())
    }
}

impl Hasher for blake3::Hasher {
    fn update(&mut self, bytes: &[u8]) {
        blake3::Hasher::update(self, bytes);
    }

    fn finalize_hex(self: Box<Self>) -> String {
        self.finalize().to_hex().to_string()
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
