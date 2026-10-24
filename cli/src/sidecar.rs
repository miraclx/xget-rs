//! Strict bounded local checksum metadata. Paths are member text, never filesystem instructions.

use std::path::PathBuf;

#[cfg(unix)]
use tokio::io::AsyncReadExt as _;
use xget::transfer::{Algorithm, Digest};
use zeroize::Zeroizing;

use crate::failure::Failure;

#[cfg(any(unix, test))]
const MAX_BYTES: u64 = 65_536;
#[cfg(any(unix, test))]
const MAX_RECORDS: usize = 256;
#[cfg(any(unix, test))]
const MAX_PATH_BYTES: usize = 4096;

pub(crate) struct LocalSidecar {
    pub(crate) path: PathBuf,
    pub(crate) algorithm: Algorithm,
}

pub(crate) struct Parsed {
    records: Vec<Record>,
}

struct Record {
    digest: Digest,
    path: Option<Zeroizing<String>>,
}

impl LocalSidecar {
    /// Bound actual reads independently of advisory metadata. Unix nonblocking/no-follow opening
    /// rejects special/symlink roles without waiting for a FIFO writer. This is not a path sandbox.
    #[cfg(unix)]
    pub(crate) async fn read(self) -> Result<Parsed, Failure> {
        let mut options = tokio::fs::OpenOptions::new();
        options
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        let file = options.open(self.path).await.map_err(|cause| Failure::io(cause,
            "cannot open local checksum metadata; choose a readable regular non-symlink sidecar"
        ))?;
        if !file
            .metadata()
            .await
            .map_err(|cause| {
                Failure::io(
                    cause,
                    "cannot inspect local checksum metadata; check its permissions",
                )
            })?
            .file_type()
            .is_file()
        {
            return Err(Failure::request(
                "checksum metadata must be a regular file, not a device, directory or pipe",
            ));
        }
        let mut bytes = Zeroizing::new(Vec::new());
        file.take(MAX_BYTES + 1)
            .read_to_end(&mut bytes)
            .await
            .map_err(|cause| {
                Failure::io(
                    cause,
                    "cannot read local checksum metadata; check the sidecar file",
                )
            })?;
        if bytes.len() > MAX_BYTES as usize {
            return Err(Failure::request(
                "checksum metadata exceeds 65536 bytes; supply a smaller sidecar",
            ));
        }
        let text = core::str::from_utf8(&bytes).map_err(|_| {
            Failure::request("checksum metadata is not utf-8; supply supported checksum text")
        })?;
        Parsed::parse(text, self.algorithm)
    }

    #[cfg(not(unix))]
    pub(crate) async fn read(self) -> Result<Parsed, Failure> {
        let _request = (self.path, self.algorithm);
        Err(Failure::request(
            "bounded nonblocking local sidecar opening is unavailable on this host; use a literal algorithm:hex expectation",
        ))
    }
}

impl Parsed {
    #[cfg(any(unix, test))]
    fn parse(text: &str, algorithm: Algorithm) -> Result<Self, Failure> {
        if text.len() > MAX_BYTES as usize {
            return Err(Failure::request(
                "checksum metadata exceeds 65536 bytes; supply a smaller sidecar",
            ));
        }
        let mut records: Vec<Record> = Vec::new();
        for line in text.lines() {
            if line.is_empty() {
                continue;
            }
            if records.len() == MAX_RECORDS {
                return Err(Failure::request(
                    "checksum metadata exceeds 256 records; supply a smaller sidecar",
                ));
            }
            let (token, path) = match line.find(' ') {
                Some(index) => {
                    let (digest, rest) = line.split_at(index);
                    let path = rest
                        .strip_prefix("  ")
                        .or_else(|| rest.strip_prefix(" *"))
                        .ok_or_else(invalid)?;
                    (digest, Some(path))
                }
                None => (line, None),
            };
            let digest = if token.contains(':') {
                token.parse().map_err(|_| invalid())?
            } else {
                Digest::parse(algorithm, token).map_err(|_| invalid())?
            };
            let path = match path {
                Some(path) => {
                    if path.is_empty()
                        || path.len() > MAX_PATH_BYTES
                        || path.chars().any(char::is_control)
                        || path.contains('\\')
                    {
                        return Err(invalid());
                    }
                    Some(Zeroizing::new(path.to_owned()))
                }
                None => None,
            };
            if records.iter().any(|previous| {
                previous.path.as_ref().map(|path| path.as_str())
                    == path.as_ref().map(|path| path.as_str())
                    && previous.digest.algorithm() == digest.algorithm()
            }) {
                return Err(Failure::request(
                    "duplicate or conflicting checksum record; keep one record per original path and algorithm",
                ));
            }
            records.push(Record { digest, path });
        }
        if records.is_empty() {
            return Err(Failure::request(
                "checksum metadata has no records; supply a nonempty supported sidecar",
            ));
        }
        if records.len() != 1 && records.iter().any(|record| record.path.is_none()) {
            return Err(Failure::request(
                "unlabelled checksum is ambiguous; use one digest only or label every original member path",
            ));
        }
        Ok(Self { records })
    }

    /// Exact original source-path equality only. No output-name, basename, decoding or normalization.
    pub(crate) fn bind(self, source_path: &str) -> Result<Vec<Digest>, Failure> {
        let mut selected = Vec::new();
        for record in self.records {
            if record
                .path
                .as_ref()
                .is_none_or(|path| path.as_str() == source_path)
            {
                selected.push(record.digest);
            }
        }
        if selected.is_empty() {
            return Err(Failure::request(
                "checksum metadata has no exact original-member path match; do not match the output rename or a basename",
            ));
        }
        Ok(selected)
    }
}

#[cfg(any(unix, test))]
fn invalid() -> Failure {
    Failure::request(
        "malformed checksum metadata; use algorithm:hex or exact-width hex, optionally followed by two spaces or space-star and the original member path",
    )
}

#[cfg(test)]
#[path = "sidecar_tests.rs"]
mod tests;

#[cfg(all(test, unix))]
#[path = "sidecar_io_tests.rs"]
mod io_tests;
