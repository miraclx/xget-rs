//! Private bounded storage encoding. No source identity, error, credential or hasher state belongs here.

use super::{ByteCount, Partial, RequiredSha256, Sha256Digest, WriteExtent};

const MAGIC: &[u8; 8] = b"XGETNF01";
pub(super) const MAX_RECORD: u64 = 1024;

/// Stored independently of the public report so storage evolution does not change public types.
pub(super) struct Record {
    pub(super) binding: Binding,
    pub(super) status: Status,
    pub(super) partial: Partial,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Binding {
    pub(super) parent: Identity,
    pub(super) workspace: Identity,
    pub(super) data: Identity,
    pub(super) destination: Vec<u8>,
    pub(super) required: RequiredSha256,
}

/// Identity is pure evidence; the native boundary obtains it from filesystem metadata.
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
        let mut bytes = Vec::with_capacity(512);
        bytes.extend_from_slice(MAGIC);
        bytes.push(match self.status {
            Status::NeedsBody => 0,
            Status::Sealed => 1,
            Status::Publishing => 2,
            Status::Published => 3,
        });
        for identity in [
            self.binding.parent,
            self.binding.workspace,
            self.binding.data,
        ] {
            let Identity {
                device: DeviceId(device),
                inode: InodeId(inode),
            } = identity;
            bytes.extend_from_slice(&device.to_le_bytes());
            bytes.extend_from_slice(&inode.to_le_bytes());
        }
        let RequiredSha256(Sha256Digest(required)) = self.binding.required;
        bytes.extend_from_slice(&required);
        match self.partial.actual {
            Some(Sha256Digest(actual)) => {
                bytes.push(1);
                bytes.extend_from_slice(&actual);
            }
            None => {
                bytes.push(0);
                bytes.extend_from_slice(&[0; 32]);
            }
        }
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
        // Destination names were bounded to 255 bytes at the request boundary.
        bytes.extend_from_slice(&(self.binding.destination.len() as u16).to_le_bytes());
        bytes.extend_from_slice(&self.binding.destination);
        bytes
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<Self, InvalidRecord> {
        let mut input = Input(bytes);
        if input.take::<8>()? != *MAGIC {
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
        let required = RequiredSha256(Sha256Digest(input.take::<32>()?));
        let actual_tag = input.take::<1>()?;
        let digest = input.take::<32>()?;
        let actual = match actual_tag {
            [0] if digest == [0; 32] => None,
            [1] => Some(Sha256Digest(digest)),
            _ => return Err(InvalidRecord),
        };
        let write_extent = match input.take::<1>()? {
            [0] => WriteExtent::Confirmed,
            [1] => WriteExtent::Unconfirmed,
            _ => return Err(InvalidRecord),
        };
        let received = ByteCount(u64::from_le_bytes(input.take::<8>()?));
        let written = ByteCount(u64::from_le_bytes(input.take::<8>()?));
        let hashed = ByteCount(u64::from_le_bytes(input.take::<8>()?));
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
            || hashed > written
            || written > received
        {
            return Err(InvalidRecord);
        }
        if status != Status::NeedsBody
            && (actual != Some(required.digest())
                || received != written
                || written != hashed
                || write_extent != WriteExtent::Confirmed)
        {
            return Err(InvalidRecord);
        }
        Ok(Self {
            binding: Binding {
                parent,
                workspace,
                data,
                required,
                destination: destination.to_vec(),
            },
            status,
            partial: Partial {
                received,
                written,
                hashed,
                actual,
                write_extent,
            },
        })
    }
}

struct Input<'a>(&'a [u8]);

impl Input<'_> {
    fn take<const N: usize>(&mut self) -> Result<[u8; N], InvalidRecord> {
        let Self(bytes) = self;
        let Some((head, tail)) = bytes.split_at_checked(N) else {
            return Err(InvalidRecord);
        };
        let value = head.try_into().map_err(|_| InvalidRecord)?;
        *bytes = tail;
        Ok(value)
    }

    fn identity(&mut self) -> Result<Identity, InvalidRecord> {
        Ok(Identity {
            device: DeviceId(u64::from_le_bytes(self.take::<8>()?)),
            inode: InodeId(u64::from_le_bytes(self.take::<8>()?)),
        })
    }
}

#[cfg(test)]
#[path = "record_tests.rs"]
mod tests;
