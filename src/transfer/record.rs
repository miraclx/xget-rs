//! Bounded versioned native state. No source references, credentials, errors or hasher internals.

use super::{
    Algorithm, ByteCount, CheckResults, Digest, Measurement, Origins, Partial, Provenance,
    ReaderCheckpoint as Checkpoint, ReaderSession as Session, RequiredSha256, Sha256Digest,
    WriteExtent,
};
use crate::checks::{CheckPolicy, Requirements};

const V3_MAGIC: &[u8; 8] = b"XGETNF03";
const MAGIC: &[u8; 8] = b"XGETNF02";
const V1_MAGIC: &[u8; 8] = b"XGETNF01";
pub(super) const MAX_RECORD: u64 = 4096;

/// Stored independently of the public report, with explicit conversion for each supported version.
pub(super) struct Record {
    pub(super) binding: Binding,
    pub(super) status: Status,
    pub(super) partial: Partial,
    pub(super) reader: Option<Checkpoint>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Binding {
    pub(super) parent: Identity,
    pub(super) workspace: Identity,
    pub(super) data: Identity,
    pub(super) destination: Vec<u8>,
    pub(super) checks: CheckPolicy,
}

/// Identity is pure evidence; only the native boundary obtains it from filesystem metadata.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Identity {
    pub(super) device: DeviceId,
    pub(super) inode: InodeId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct DeviceId(pub(super) u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct InodeId(pub(super) u64);

/// Invalid stored bytes have no native I/O cause. The native reader maps this at its own seam.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("invalid native file record")]
pub(super) struct InvalidRecord;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Status {
    NeedsBody,
    Sealed,
    Publishing,
    Published,
}

impl Record {
    pub(super) fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(1024);
        bytes.extend_from_slice(if self.reader.is_some() {
            V3_MAGIC
        } else {
            MAGIC
        });
        bytes.push(match self.status {
            Status::NeedsBody => 0,
            Status::Sealed => 1,
            Status::Publishing => 2,
            Status::Published => 3,
        });
        for Identity {
            device: DeviceId(device),
            inode: InodeId(inode),
        } in [
            self.binding.parent,
            self.binding.workspace,
            self.binding.data,
        ] {
            bytes.extend_from_slice(&device.to_le_bytes());
            bytes.extend_from_slice(&inode.to_le_bytes());
        }
        Self::write_requirements(&mut bytes, self.binding.checks.requirements);
        bytes.push(match self.binding.checks.measurement {
            Measurement::None => 0,
            Measurement::Algorithm(algorithm) => Self::tag(algorithm),
        });
        bytes.push(match self.partial.write_extent {
            WriteExtent::Confirmed => 0,
            WriteExtent::Unconfirmed => 1,
        });
        for ByteCount(count) in [
            self.partial.received,
            self.partial.written,
            self.partial.hashed,
        ] {
            bytes.extend_from_slice(&count.to_le_bytes());
        }
        match self.partial.checks {
            None => bytes.push(0),
            Some(results) => {
                bytes.push(1);
                Self::write_requirements(&mut bytes, results.requirements());
                match results.measured() {
                    None => bytes.push(0),
                    Some(digest) => Self::write_digest(&mut bytes, digest),
                }
            }
        }
        if let Some(reader) = &self.reader {
            bytes.extend_from_slice(&reader.session.size.0.to_le_bytes());
            bytes.push(u8::from(reader.closed));
            bytes.extend_from_slice(&(reader.session.representation.len() as u16).to_le_bytes());
            bytes.extend_from_slice(reader.session.representation.as_bytes());
            match &reader.session.version {
                None => bytes.push(0),
                Some(version) => {
                    bytes.push(1);
                    bytes.extend_from_slice(&(version.len() as u16).to_le_bytes());
                    bytes.extend_from_slice(version);
                }
            }
            match reader.prefix {
                None => bytes.push(0),
                Some((length, digest)) => {
                    bytes.push(1);
                    bytes.extend_from_slice(&length.0.to_le_bytes());
                    Self::write_digest(&mut bytes, digest);
                }
            }
        }
        // The request boundary and decoder bound a destination to one component of at most 255 bytes.
        bytes.extend_from_slice(&(self.binding.destination.len() as u16).to_le_bytes());
        bytes.extend_from_slice(&self.binding.destination);
        bytes
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<Self, InvalidRecord> {
        if bytes.len() as u64 > MAX_RECORD {
            return Err(InvalidRecord);
        }
        let mut input = Input(bytes);
        let magic = input.take::<8>()?;
        if magic != *MAGIC && magic != *V1_MAGIC && magic != *V3_MAGIC {
            return Err(InvalidRecord);
        }
        let status = match input.take::<1>()? {
            [0] => Status::NeedsBody,
            [1] => Status::Sealed,
            [2] => Status::Publishing,
            [3] => Status::Published,
            _ => return Err(InvalidRecord),
        };
        let parent = input.identity()?;
        let workspace = input.identity()?;
        let data = input.identity()?;
        let (checks, partial) = if magic == *V1_MAGIC {
            input.v1()?
        } else {
            input.v2()?
        };
        let reader = if magic == *V3_MAGIC {
            Some(input.reader(checks, partial, status)?)
        } else {
            None
        };
        let length = usize::from(u16::from_le_bytes(input.take::<2>()?));
        let Input(destination) = input;
        if length == 0
            || length > 255
            || destination.len() != length
            || destination.contains(&0)
            || destination.contains(&b'/')
            || destination == b"."
            || destination == b".."
            || destination.starts_with(b".xget-native-")
            || destination == b".xget-native.lock"
            || partial.hashed > partial.written
            || partial.written > partial.received
            || (!checks.hashing() && partial.hashed != ByteCount(0))
            || !partial.results_fit(checks)
            || (status != Status::NeedsBody && !partial.complete_for(checks))
        {
            return Err(InvalidRecord);
        }
        Ok(Self {
            binding: Binding {
                parent,
                workspace,
                data,
                destination: destination.to_vec(),
                checks,
            },
            status,
            partial,
            reader,
        })
    }

    fn write_requirements(bytes: &mut Vec<u8>, requirements: Requirements) {
        bytes.push(requirements.iter().count() as u8);
        for requirement in requirements.iter() {
            Self::write_digest(bytes, requirement.digest);
            bytes.push(requirement.origins.bits());
        }
    }

    fn write_digest(bytes: &mut Vec<u8>, digest: Digest) {
        bytes.push(Self::tag(digest.algorithm()));
        bytes.extend_from_slice(digest.as_bytes());
    }

    fn tag(algorithm: Algorithm) -> u8 {
        match algorithm {
            Algorithm::Md5 => 1,
            Algorithm::Sha1 => 2,
            Algorithm::Sha256 => 3,
            Algorithm::Sha512 => 4,
            Algorithm::Blake3 => 5,
        }
    }
}

struct Input<'a>(&'a [u8]);

impl<'a> Input<'a> {
    fn reader(
        &mut self,
        checks: CheckPolicy,
        partial: Partial,
        status: Status,
    ) -> Result<Checkpoint, InvalidRecord> {
        let size = ByteCount(u64::from_le_bytes(self.take::<8>()?));
        let closed = match self.take::<1>()? {
            [0] => false,
            [1] => true,
            _ => return Err(InvalidRecord),
        };
        let length = usize::from(u16::from_le_bytes(self.take::<2>()?));
        if length == 0 || length > 256 {
            return Err(InvalidRecord);
        }
        let representation = core::str::from_utf8(self.bytes(length)?)
            .map_err(|_| InvalidRecord)?
            .to_owned();
        let version = match self.take::<1>()? {
            [0] => None,
            [1] => {
                let length = usize::from(u16::from_le_bytes(self.take::<2>()?));
                if length == 0 || length > 256 {
                    return Err(InvalidRecord);
                }
                Some(self.bytes(length)?.to_vec())
            }
            _ => return Err(InvalidRecord),
        };
        let prefix = match self.take::<1>()? {
            [0] => None,
            [1] => {
                let length = ByteCount(u64::from_le_bytes(self.take::<8>()?));
                let [tag] = self.take::<1>()?;
                let digest = self.digest_with_tag(tag)?;
                if length == ByteCount(0)
                    || length >= size
                    || length > partial.hashed
                    || !matches!(
                        digest.algorithm(),
                        Algorithm::Sha256 | Algorithm::Sha512 | Algorithm::Blake3
                    )
                    || checks.prefix_algorithm() != Some(digest.algorithm())
                {
                    return Err(InvalidRecord);
                }
                Some((length, digest))
            }
            _ => return Err(InvalidRecord),
        };
        if (status != Status::NeedsBody && (!closed || partial.written != size))
            || partial.written > size
            || (version.is_none()
                && !checks.requirements.iter().any(|requirement| {
                    matches!(
                        requirement.digest.algorithm(),
                        Algorithm::Sha256 | Algorithm::Sha512 | Algorithm::Blake3
                    )
                }))
        {
            return Err(InvalidRecord);
        }
        Ok(Checkpoint {
            session: Session {
                size,
                representation,
                version,
            },
            closed,
            prefix,
        })
    }

    fn bytes(&mut self, count: usize) -> Result<&'a [u8], InvalidRecord> {
        let Self(bytes) = self;
        let input = *bytes;
        let Some((head, tail)) = input.split_at_checked(count) else {
            return Err(InvalidRecord);
        };
        *bytes = tail;
        Ok(head)
    }

    fn take<const N: usize>(&mut self) -> Result<[u8; N], InvalidRecord> {
        self.bytes(N)?.try_into().map_err(|_| InvalidRecord)
    }

    fn identity(&mut self) -> Result<Identity, InvalidRecord> {
        Ok(Identity {
            device: DeviceId(u64::from_le_bytes(self.take::<8>()?)),
            inode: InodeId(u64::from_le_bytes(self.take::<8>()?)),
        })
    }

    fn algorithm(tag: u8) -> Result<Algorithm, InvalidRecord> {
        match tag {
            1 => Ok(Algorithm::Md5),
            2 => Ok(Algorithm::Sha1),
            3 => Ok(Algorithm::Sha256),
            4 => Ok(Algorithm::Sha512),
            5 => Ok(Algorithm::Blake3),
            _ => Err(InvalidRecord),
        }
    }

    fn digest_with_tag(&mut self, tag: u8) -> Result<Digest, InvalidRecord> {
        let algorithm = Self::algorithm(tag)?;
        Digest::from_raw(algorithm, self.bytes(algorithm.byte_len())?).map_err(|_| InvalidRecord)
    }

    fn requirements(&mut self) -> Result<Requirements, InvalidRecord> {
        let [count] = self.take::<1>()?;
        if count > 5 {
            return Err(InvalidRecord);
        }
        let mut requirements = Requirements::default();
        let mut previous = None;
        for _ in 0..count {
            let [tag] = self.take::<1>()?;
            let digest = self.digest_with_tag(tag)?;
            let index = digest.algorithm().index();
            if previous.is_some_and(|previous| previous >= index) {
                return Err(InvalidRecord);
            }
            previous = Some(index);
            let [bits] = self.take::<1>()?;
            let origins = Origins::from_bits(bits).ok_or(InvalidRecord)?;
            for origin in origins.iter() {
                requirements
                    .insert(digest, origin)
                    .map_err(|_| InvalidRecord)?;
            }
        }
        Ok(requirements)
    }

    fn extent(&mut self) -> Result<WriteExtent, InvalidRecord> {
        match self.take::<1>()? {
            [0] => Ok(WriteExtent::Confirmed),
            [1] => Ok(WriteExtent::Unconfirmed),
            _ => Err(InvalidRecord),
        }
    }

    fn counts(&mut self, write_extent: WriteExtent) -> Result<Partial, InvalidRecord> {
        Ok(Partial {
            received: ByteCount(u64::from_le_bytes(self.take::<8>()?)),
            written: ByteCount(u64::from_le_bytes(self.take::<8>()?)),
            hashed: ByteCount(u64::from_le_bytes(self.take::<8>()?)),
            write_extent,
            ..Partial::default()
        })
    }

    fn v2(&mut self) -> Result<(CheckPolicy, Partial), InvalidRecord> {
        let requirements = self.requirements()?;
        let [measurement] = self.take::<1>()?;
        let measurement = if measurement == 0 {
            Measurement::None
        } else {
            Measurement::Algorithm(Self::algorithm(measurement)?)
        };
        let extent = self.extent()?;
        let mut partial = self.counts(extent)?;
        match self.take::<1>()? {
            [0] => {}
            [1] => {
                let matched = self.requirements()?;
                let [measured] = self.take::<1>()?;
                let measured = if measured == 0 {
                    None
                } else {
                    Some(self.digest_with_tag(measured)?)
                };
                partial.set_checks(CheckResults::from_parts(matched, measured));
            }
            _ => return Err(InvalidRecord),
        }
        Ok((
            CheckPolicy {
                requirements,
                measurement,
            },
            partial,
        ))
    }

    fn v1(&mut self) -> Result<(CheckPolicy, Partial), InvalidRecord> {
        let required = RequiredSha256(Sha256Digest(self.take::<32>()?));
        let [tag] = self.take::<1>()?;
        let bytes = self.take::<32>()?;
        let actual = match tag {
            0 if bytes == [0; 32] => None,
            1 => Some(Digest::sha256(bytes)),
            _ => return Err(InvalidRecord),
        };
        let extent = self.extent()?;
        let mut partial = self.counts(extent)?;
        let checks = CheckPolicy::from(required);
        if let Some(actual) = actual {
            let matched = if actual == Digest::from(required) {
                Requirements::single(actual, Provenance::Caller)
            } else {
                Requirements::default()
            };
            partial.set_checks(CheckResults::from_parts(matched, Some(actual)));
        }
        Ok((checks, partial))
    }
}

#[cfg(test)]
#[path = "record_tests.rs"]
mod tests;
