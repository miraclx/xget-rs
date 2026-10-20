//! Pure whole-payload checks, independent of byte acquisition and publication.
//!
//! Required expectations and optional measurement share hash work but never share policy authority.

use core::fmt;
use core::str::FromStr;

use digest::Digest as _;

use crate::checksum::OwnedSha256;

#[cfg(test)]
#[path = "checks_tests.rs"]
mod tests;

/// A real digest algorithm; absence belongs to measurement policy, not a digest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Algorithm {
    /// Compatibility checking only, not a strong content anchor.
    Md5,
    /// Compatibility checking only, not a strong content anchor.
    Sha1,
    /// A strong whole-payload content anchor.
    Sha256,
    /// A strong whole-payload content anchor.
    Sha512,
    /// A strong whole-payload content anchor.
    Blake3,
}

impl Algorithm {
    const ALL: [Self; 5] = [
        Self::Md5,
        Self::Sha1,
        Self::Sha256,
        Self::Sha512,
        Self::Blake3,
    ];

    /// Stable bounded slot order, independent of external spellings or stored tags.
    pub(crate) fn index(self) -> usize {
        match self {
            Self::Md5 => 0,
            Self::Sha1 => 1,
            Self::Sha256 => 2,
            Self::Sha512 => 3,
            Self::Blake3 => 4,
        }
    }

    /// Digest width comes from the algorithm, never from an untrusted stored length.
    pub(crate) fn byte_len(self) -> usize {
        match self {
            Self::Md5 => 16,
            Self::Sha1 => 20,
            Self::Sha256 | Self::Blake3 => 32,
            Self::Sha512 => 64,
        }
    }

    fn is_strong(self) -> bool {
        match self {
            Self::Md5 | Self::Sha1 => false,
            Self::Sha256 | Self::Sha512 | Self::Blake3 => true,
        }
    }
}

impl FromStr for Algorithm {
    type Err = ParseDigestError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "md5" => Ok(Self::Md5),
            "sha1" => Ok(Self::Sha1),
            "sha256" => Ok(Self::Sha256),
            "sha512" => Ok(Self::Sha512),
            "blake3" => Ok(Self::Blake3),
            _ => Err(ParseDigestError::Algorithm),
        }
    }
}

impl fmt::Display for Algorithm {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Md5 => "md5",
            Self::Sha1 => "sha1",
            Self::Sha256 => "sha256",
            Self::Sha512 => "sha512",
            Self::Blake3 => "blake3",
        })
    }
}

/// An algorithm and exactly its digest bytes, parsed once at the input boundary.
///
/// The unused tail is always zero, so equality cannot depend on hidden padding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Digest {
    algorithm: Algorithm,
    bytes: [u8; 64],
}

impl Digest {
    /// Parses exact hexadecimal without retaining input text in either result or error.
    pub fn parse(algorithm: Algorithm, value: &str) -> Result<Self, ParseDigestError> {
        if value.len() != algorithm.byte_len() * 2 {
            return Err(ParseDigestError::Length { algorithm });
        }
        let mut bytes = [0; 64];
        hex::decode_to_slice(value, &mut bytes[..algorithm.byte_len()])
            .map_err(|_| ParseDigestError::Hex { algorithm })?;
        Ok(Self { algorithm, bytes })
    }

    /// Identifies the algorithm without reparsing text.
    pub fn algorithm(self) -> Algorithm {
        self.algorithm
    }

    /// Borrows only the meaningful digest bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.algorithm.byte_len()]
    }

    #[cfg(unix)]
    /// Stored readers must reject malformed lengths instead of reaching a slicing panic.
    pub(crate) fn from_raw(algorithm: Algorithm, value: &[u8]) -> Result<Self, ParseDigestError> {
        if value.len() != algorithm.byte_len() {
            return Err(ParseDigestError::Length { algorithm });
        }
        Ok(Self::from_bytes(algorithm, value))
    }

    /// The compatibility array establishes its exact width in the type.
    pub(crate) fn sha256(bytes: [u8; 32]) -> Self {
        Self::from_bytes(Algorithm::Sha256, &bytes)
    }

    fn from_bytes(algorithm: Algorithm, value: &[u8]) -> Self {
        let mut bytes = [0; 64];
        bytes[..algorithm.byte_len()].copy_from_slice(value);
        Self { algorithm, bytes }
    }
}

impl FromStr for Digest {
    type Err = ParseDigestError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (algorithm, value) = value.split_once(':').ok_or(ParseDigestError::Syntax)?;
        Self::parse(algorithm.parse()?, value)
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:", self.algorithm)?;
        for byte in self.as_bytes() {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Input failures contain no raw input, URL, token or foreign decoder character.
#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum ParseDigestError {
    /// Expected the documented algorithm:hex form.
    #[error("invalid digest syntax; use algorithm:hex")]
    Syntax,
    /// The spelling does not identify a supported algorithm.
    #[error("unsupported digest algorithm; use md5, sha1, sha256, sha512, or blake3")]
    Algorithm,
    /// The length disagrees with the named algorithm.
    #[error("invalid {algorithm} digest length; provide its exact hexadecimal digest")]
    Length {
        /// The algorithm whose exact byte length is required.
        algorithm: Algorithm,
    },
    /// The exact-length input was not hexadecimal.
    #[error("invalid {algorithm} digest; provide hexadecimal bytes")]
    Hex {
        /// The algorithm whose digest contained non-hexadecimal input.
        algorithm: Algorithm,
    },
}

/// Where an expectation came from, not evidence that it is authentic.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Provenance {
    /// Supplied by the caller.
    Caller,
    /// Supplied by source metadata.
    Source,
    /// Resolved from a checksum sidecar.
    Sidecar,
}

impl Provenance {
    fn bit(self) -> u8 {
        match self {
            Self::Caller => 1,
            Self::Source => 2,
            Self::Sidecar => 4,
        }
    }
}

/// A nonempty bounded set of origins; repetitions consume no additional storage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Origins(u8);

impl Origins {
    #[cfg(unix)]
    /// Storage uses bounded enum bits, never caller-supplied origin strings.
    pub(crate) fn bits(self) -> u8 {
        self.0
    }

    #[cfg(unix)]
    /// Empty or unrecognized origin sets cannot enter the normalized domain.
    pub(crate) fn from_bits(bits: u8) -> Option<Self> {
        (bits != 0 && bits & !7 == 0).then_some(Self(bits))
    }

    /// Enumerates origins without retaining arbitrary source strings.
    pub fn iter(self) -> impl Iterator<Item = Provenance> {
        [Provenance::Caller, Provenance::Source, Provenance::Sidecar]
            .into_iter()
            .filter(move |origin| self.0 & origin.bit() != 0)
    }
}

/// An agreed expectation and its bounded, unauthenticated origins.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Requirement {
    /// The expected whole-payload digest.
    pub digest: Digest,
    /// All agreeing origins.
    pub origins: Origins,
}

/// A conflicting proposal cannot replace an existing requirement.
#[derive(Debug, thiserror::Error, Eq, PartialEq)]
#[error("conflicting {algorithm} requirements; resolve the conflicting expectations", algorithm = .existing.digest.algorithm)]
pub struct Conflict {
    /// The previously agreed value and origins, unchanged by insertion failure.
    pub existing: Requirement,
    /// The incompatible value proposed for the same algorithm.
    pub proposed: Digest,
    /// The origin of the incompatible proposal.
    pub provenance: Provenance,
}

/// At most one requirement for each supported algorithm.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct Requirements {
    entries: [Option<Requirement>; Algorithm::ALL.len()],
}

impl Requirements {
    /// A single typed proposal cannot conflict; compatibility decoding uses this entrance.
    pub(crate) fn single(digest: Digest, provenance: Provenance) -> Self {
        let mut entries = [None; Algorithm::ALL.len()];
        entries[digest.algorithm.index()] = Some(Requirement {
            digest,
            origins: Origins(provenance.bit()),
        });
        Self { entries }
    }

    #[cfg(any(unix, test))]
    /// Canonical encoding and policy checks share one fixed algorithm order.
    pub(crate) fn iter(self) -> impl Iterator<Item = Requirement> {
        self.entries.into_iter().flatten()
    }

    #[cfg(any(unix, test))]
    /// A supported algorithm needs no dynamic registry lookup.
    pub(crate) fn get(self, algorithm: Algorithm) -> Option<Requirement> {
        self.entries[algorithm.index()]
    }

    /// Merges agreeing origins or rejects disagreement without changing the set.
    pub(crate) fn insert(
        &mut self,
        digest: Digest,
        provenance: Provenance,
    ) -> Result<(), Box<Conflict>> {
        let entry = &mut self.entries[digest.algorithm.index()];
        match entry {
            Some(existing) if existing.digest != digest => Err(Box::new(Conflict {
                existing: *existing,
                proposed: digest,
                provenance,
            })),
            Some(existing) => {
                existing.origins.0 |= provenance.bit();
                Ok(())
            }
            None => {
                *entry = Some(Requirement {
                    digest,
                    origins: Origins(provenance.bit()),
                });
                Ok(())
            }
        }
    }

    /// Freezes the required set and creates only the requested distinct hashers.
    pub(crate) fn start(self, measurement: Measurement) -> Checks {
        let hashers = Algorithm::ALL.map(|algorithm| {
            let required = self.entries[algorithm.index()].is_some();
            let measured = measurement == Measurement::Algorithm(algorithm);
            (required || measured).then(|| ActiveCheck {
                hasher: Hasher::for_algorithm(algorithm),
                requirement: self.entries[algorithm.index()],
            })
        });
        Checks {
            measurement,
            hashers,
        }
    }
}

/// Optional reporting is independent of required checks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Measurement {
    /// No optional hash, including no implicit recovery fingerprint.
    None,
    /// Measure this algorithm, sharing work with a matching requirement.
    Algorithm(Algorithm),
}

impl Default for Measurement {
    fn default() -> Self {
        Self::Algorithm(Algorithm::Sha256)
    }
}

/// A one-pass accumulator. The caller supplies ordered payload bytes exactly once.
pub(crate) struct Checks {
    measurement: Measurement,
    hashers: [Option<ActiveCheck>; Algorithm::ALL.len()],
}

// A requirement cannot outlive or lose its concrete hasher. Empty slots mean no requested work.
struct ActiveCheck {
    hasher: Hasher,
    requirement: Option<Requirement>,
}

impl Checks {
    /// Byte accounting distinguishes actual requested hash work from received/written bytes.
    pub(crate) fn active(&self) -> bool {
        self.hashers.iter().any(Option::is_some)
    }

    /// Feeds every active algorithm from this same borrowed payload slice.
    pub(crate) fn update(&mut self, bytes: &[u8]) {
        for check in self.hashers.iter_mut().flatten() {
            check.hasher.update(bytes);
        }
    }

    /// Finalize every requested hash, retaining optional measurement even when a requirement fails.
    /// This says nothing about EOF, settled writes or publication; the transfer owns those gates.
    pub(crate) fn finish(self) -> Result<Outcome, Box<Mismatch>> {
        let mut results = Outcome::default();
        let mut mismatch = None;
        for ActiveCheck {
            hasher,
            requirement,
        } in self.hashers.into_iter().flatten()
        {
            let actual = hasher.finish();
            if self.measurement == Measurement::Algorithm(actual.algorithm) {
                results.measured = Some(actual);
            }
            if let Some(requirement) = requirement {
                if actual == requirement.digest {
                    results.matched[actual.algorithm.index()] = Some(requirement);
                } else if mismatch.is_none() {
                    mismatch = Some((requirement, actual));
                }
            }
        }
        match mismatch {
            Some((requirement, actual)) => Err(Box::new(Mismatch {
                requirement,
                actual,
                results,
            })),
            None => Ok(results),
        }
    }
}

/// A failed requirement with typed expected and actual digests of the same algorithm.
#[derive(Debug, thiserror::Error, Eq, PartialEq)]
#[error("{algorithm} digest mismatch; check the payload and expected digest", algorithm = .actual.algorithm)]
pub struct Mismatch {
    /// The expected digest and all agreeing origins.
    pub requirement: Requirement,
    /// The computed whole-payload digest.
    pub actual: Digest,
    /// Finished measurement and all agreeing requirements, not an all-requirements success.
    pub results: Outcome,
}

/// Hash results only, not a transfer or publication success receipt.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Outcome {
    matched: [Option<Requirement>; Algorithm::ALL.len()],
    measured: Option<Digest>,
}

impl Outcome {
    #[cfg(any(unix, test))]
    /// Private stored-data conversion; the codec separately validates the frozen policy binding.
    pub(crate) fn from_parts(matched: Requirements, measured: Option<Digest>) -> Self {
        Self {
            matched: matched.entries,
            measured,
        }
    }

    #[cfg(unix)]
    /// Encoding retains matched values without storing independent hasher state.
    pub(crate) fn requirements(self) -> Requirements {
        Requirements {
            entries: self.matched,
        }
    }

    /// Compatibility views may expose only an already-finished value.
    pub(crate) fn digest_for(self, algorithm: Algorithm) -> Option<Digest> {
        self.measured
            .filter(|digest| digest.algorithm == algorithm)
            .or_else(|| self.matched[algorithm.index()].map(|requirement| requirement.digest))
    }

    /// Required checks that matched, independently of optional measurement.
    pub fn matched_requirements(self) -> impl Iterator<Item = Requirement> {
        self.matched.into_iter().flatten()
    }

    /// The optional measured value, even when its work was shared with a requirement.
    pub fn measured(self) -> Option<Digest> {
        self.measured
    }

    /// Selects existing strong evidence without computing a recovery-only hash.
    ///
    /// Preference is SHA-256, SHA-512, then BLAKE3. An anchor describes content,
    /// not authenticity, destination identity or permission to adopt foreign bytes.
    pub fn strong_anchor(self) -> Option<Digest> {
        for algorithm in Algorithm::ALL
            .into_iter()
            .filter(|algorithm| algorithm.is_strong())
        {
            if let Some(requirement) = self.matched[algorithm.index()] {
                return Some(requirement.digest);
            }
            if let Some(measured) = self.measured.filter(|digest| digest.algorithm == algorithm) {
                return Some(measured);
            }
        }
        None
    }
}

/// Frozen bounded check configuration, separate from its computed evidence and stored encoding.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct CheckPolicy {
    /// Required expectations frozen before acquiring a workspace or admitting a body.
    pub(crate) requirements: Requirements,
    /// Optional hash selection never replaces or cancels required work.
    pub(crate) measurement: Measurement,
}

impl CheckPolicy {
    /// Freeze the requested work; required and optional uses share each concrete state.
    pub(crate) fn start(self) -> Checks {
        self.requirements.start(self.measurement)
    }

    #[cfg(unix)]
    /// Native checkpoint counts must be zero when no hash work was requested.
    pub(crate) fn hashing(self) -> bool {
        self.measurement != Measurement::None || self.requirements.iter().next().is_some()
    }

    #[cfg(any(unix, test))]
    /// A partial/mismatch report may contain only configured requirements and measurement.
    pub(crate) fn describes(self, results: Outcome) -> bool {
        results
            .matched_requirements()
            .all(|matched| self.requirements.get(matched.digest.algorithm) == Some(matched))
            && match self.measurement {
                Measurement::None => results.measured.is_none(),
                Measurement::Algorithm(algorithm) => results.measured.is_some_and(|digest| {
                    digest.algorithm == algorithm
                        && results.matched[algorithm.index()]
                            .is_none_or(|matched| matched.digest == digest)
                }),
            }
    }

    #[cfg(any(unix, test))]
    /// Publication needs the entire required set, not just individually agreeing checks.
    pub(crate) fn satisfied_by(self, results: Outcome) -> bool {
        self.describes(results) && self.requirements.entries == results.matched
    }
}

// Inline ownership keeps the maximum five states allocation-free and conditionally
// Send without changing the legacy boxed hasher's bounds. BLAKE3's state is larger.
#[allow(clippy::large_enum_variant)]
enum Hasher {
    Md5(md5::Md5),
    Sha1(sha1::Sha1),
    Sha256(OwnedSha256),
    Sha512(sha2::Sha512),
    Blake3(blake3::Hasher),
}

impl Hasher {
    fn for_algorithm(algorithm: Algorithm) -> Self {
        match algorithm {
            Algorithm::Md5 => Self::Md5(md5::Md5::default()),
            Algorithm::Sha1 => Self::Sha1(sha1::Sha1::default()),
            Algorithm::Sha256 => Self::Sha256(OwnedSha256::default()),
            Algorithm::Sha512 => Self::Sha512(sha2::Sha512::default()),
            Algorithm::Blake3 => Self::Blake3(blake3::Hasher::new()),
        }
    }

    fn update(&mut self, bytes: &[u8]) {
        match self {
            Self::Md5(hasher) => hasher.update(bytes),
            Self::Sha1(hasher) => hasher.update(bytes),
            Self::Sha256(hasher) => hasher.update(bytes),
            Self::Sha512(hasher) => hasher.update(bytes),
            Self::Blake3(hasher) => {
                hasher.update(bytes);
            }
        }
    }

    fn finish(self) -> Digest {
        match self {
            Self::Md5(hasher) => Digest::from_bytes(Algorithm::Md5, &hasher.finalize()),
            Self::Sha1(hasher) => Digest::from_bytes(Algorithm::Sha1, &hasher.finalize()),
            Self::Sha256(hasher) => Digest::sha256(hasher.finish()),
            Self::Sha512(hasher) => Digest::from_bytes(Algorithm::Sha512, &hasher.finalize()),
            Self::Blake3(hasher) => {
                Digest::from_bytes(Algorithm::Blake3, hasher.finalize().as_bytes())
            }
        }
    }
}
