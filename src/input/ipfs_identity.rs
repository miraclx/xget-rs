//! Pure bounded IPFS member identity, independent of gateway access and native storage.

use core::fmt;

use cid::Cid;
use zeroize::Zeroizing;

use crate::checks::{Algorithm, Digest};

/// Validated content-address/path spelling, not a gateway URL or a claim of DAG reconstruction.
pub(crate) struct IpfsIdentity {
    spelling: Zeroizing<String>,
    source_path: Zeroizing<String>,
    required: Option<Digest>,
}

impl IpfsIdentity {
    pub(crate) fn parse(spelling: &str) -> Result<Self, IpfsIdentityError> {
        if spelling.is_empty() || spelling.len() > 4096 {
            return Err(IpfsIdentityError::Limit);
        }
        let (cid_text, path) = match spelling.split_once('/') {
            Some((cid, path)) => (cid, Some(path)),
            None => (spelling, None),
        };
        if cid_text.is_empty()
            || cid_text.len() > 512
            || !cid_text.bytes().all(|byte| byte.is_ascii_alphanumeric())
        {
            return Err(IpfsIdentityError::InvalidCid);
        }
        // URL host parsing lowercases text. CIDv0 is case-sensitive, so parse the original authority.
        let cid = Cid::try_from(cid_text).map_err(|_| IpfsIdentityError::InvalidCid)?;
        if !matches!(cid.codec(), 0x55 | 0x70) {
            return Err(IpfsIdentityError::UnsupportedCodec);
        }
        if cid.codec() == 0x55 && path.is_some() {
            return Err(IpfsIdentityError::RawPath);
        }
        let canonical = cid.to_string();
        let source_path = match path {
            None => String::clone(&canonical),
            Some(path) => Self::decode_path(path)?,
        };
        let required = if cid.codec() == 0x55 {
            let algorithm = match cid.hash().code() {
                0x11 => Algorithm::Sha1,
                0x12 => Algorithm::Sha256,
                0x13 => Algorithm::Sha512,
                0x1e => Algorithm::Blake3,
                _ => return Err(IpfsIdentityError::UnsupportedDigest),
            };
            Some(
                Digest::parse(algorithm, &hex::encode(cid.hash().digest()))
                    .map_err(|_| IpfsIdentityError::UnsupportedDigest)?,
            )
        } else {
            // A UnixFS/DAG root hashes a node, not the file representation returned by a gateway.
            None
        };
        let spelling = match path {
            Some(path) => format!("{canonical}/{path}"),
            None => canonical,
        };
        if spelling.len() > 4096 || source_path.len() > 4096 {
            return Err(IpfsIdentityError::Limit);
        }
        Ok(Self {
            spelling: Zeroizing::new(spelling),
            source_path: Zeroizing::new(source_path),
            required,
        })
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.spelling
    }
    pub(crate) fn source_path(&self) -> &str {
        &self.source_path
    }
    pub(crate) fn required(&self) -> Option<Digest> {
        self.required
    }

    fn decode_path(path: &str) -> Result<String, IpfsIdentityError> {
        if path.is_empty() || path.split('/').count() > 64 {
            return Err(IpfsIdentityError::Path);
        }
        let mut decoded_path = String::with_capacity(path.len());
        for component in path.split('/') {
            if component.is_empty()
                || !component.bytes().all(|byte| {
                    byte.is_ascii_alphanumeric() || b"-._~%!$&'()*+,;=:@".contains(&byte)
                })
            {
                return Err(IpfsIdentityError::Path);
            }
            let mut bytes = component.bytes();
            while let Some(byte) = bytes.next() {
                if byte == b'%'
                    && !(bytes.next().is_some_and(|byte| byte.is_ascii_hexdigit())
                        && bytes.next().is_some_and(|byte| byte.is_ascii_hexdigit()))
                {
                    return Err(IpfsIdentityError::Path);
                }
            }
            let decoded = percent_encoding::percent_decode_str(component)
                .decode_utf8()
                .map_err(|_| IpfsIdentityError::Path)?;
            if matches!(decoded.as_ref(), "." | "..")
                || decoded.contains(['/', '\\', '%'])
                || decoded.chars().any(char::is_control)
            {
                return Err(IpfsIdentityError::Path);
            }
            if !decoded_path.is_empty() {
                decoded_path.push('/');
            }
            decoded_path.push_str(&decoded);
        }
        Ok(decoded_path)
    }
}

impl fmt::Debug for IpfsIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("IpfsIdentity([content address and member path])")
    }
}

/// Invalid or unsupported bounded IPFS identity, without retaining the rejected reference.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum IpfsIdentityError {
    /// Reference spelling exceeded the bounded metadata allowance.
    #[error("ipfs identity limit exceeded; use at most 4096 bytes and 64 path components")]
    Limit,
    /// A CID must be parsed from its original case-sensitive spelling.
    #[error(
        "invalid ipfs cid; supply the original content identifier without credentials or a port"
    )]
    InvalidCid,
    /// Only raw blocks and gateway-provided UnixFS representations are supported.
    #[error("unsupported ipfs codec; use a raw block or a unixfs dag-pb file reference")]
    UnsupportedCodec,
    /// A raw block has no child namespace.
    #[error("raw ipfs blocks have no member path; request the cid itself")]
    RawPath,
    /// The raw CID needs a supported complete digest before acquisition can succeed.
    #[error("unsupported raw cid digest; use complete sha1, sha256, sha512 or blake3 evidence")]
    UnsupportedDigest,
    /// Ambiguous or directory-shaped paths cannot become exact file references.
    #[error(
        "invalid ipfs file path; use nonempty literal components without traversal or a trailing slash"
    )]
    Path,
}

#[cfg(test)]
#[path = "ipfs_identity_tests.rs"]
mod tests;
