#![allow(clippy::unwrap_used)]

use super::{Binding, DeviceId, Identity, InodeId, InvalidRecord, Record, Status};
use crate::transfer::tests::{complete, requirement};
use crate::transfer::{ByteCount, Partial, WriteExtent};

#[test]
fn bounded_versioned_state_roundtrips_each_lifecycle_status() {
    for status in [
        Status::NeedsBody,
        Status::Sealed,
        Status::Publishing,
        Status::Published,
    ] {
        let record = sample(status);
        let decoded = Record::decode(&record.encode()).unwrap();
        assert_eq!(decoded.binding, record.binding);
        assert_eq!(decoded.partial, record.partial);
        assert_eq!(decoded.status, record.status);
    }
}

#[test]
fn every_truncated_record_is_refused_without_partial_adoption() {
    let bytes = sample(Status::Publishing).encode();
    for end in 0..bytes.len() {
        assert!(Record::decode(&bytes[..end]).is_err());
    }
}

#[test]
fn a_future_storage_version_is_not_interpreted_as_current_state() {
    let mut bytes = sample(Status::Publishing).encode();
    bytes[7] = b'2';
    assert!(matches!(Record::decode(&bytes), Err(InvalidRecord)));
}

#[test]
fn noncanonical_trailing_bytes_and_paths_are_not_accepted() {
    let mut bytes = sample(Status::Publishing).encode();
    bytes.push(0);
    assert!(Record::decode(&bytes).is_err());
    for name in [
        b"../outside".as_slice(),
        b".",
        b"..",
        b".xget-native.lock",
        b".xget-native-foreign",
        b"bad\0name",
        b"",
    ] {
        let mut record = sample(Status::NeedsBody);
        record.binding.destination = name.to_vec();
        assert!(Record::decode(&record.encode()).is_err());
    }
}

#[test]
fn publication_records_require_a_matched_whole_digest_and_equal_confirmed_counts() {
    let mut record = sample(Status::Publishing);
    record.partial.actual = None;
    assert!(Record::decode(&record.encode()).is_err());
    record.partial = complete(b"abc");
    record.partial.actual = Some(requirement(b"different").digest());
    assert!(Record::decode(&record.encode()).is_err());
    record.partial = complete(b"abc");
    record.partial.written = ByteCount(2);
    assert!(Record::decode(&record.encode()).is_err());
    record.partial = complete(b"abc");
    record.partial.write_extent = WriteExtent::Unconfirmed;
    assert!(Record::decode(&record.encode()).is_err());
}

#[test]
fn one_shot_evidence_can_retain_an_unknown_extent_without_promising_reuse() {
    let mut record = sample(Status::NeedsBody);
    record.partial = Partial {
        write_extent: WriteExtent::Unconfirmed,
        ..Partial::default()
    };
    let decoded = Record::decode(&record.encode()).unwrap();
    assert_eq!(decoded.partial.write_extent, WriteExtent::Unconfirmed);
    assert_eq!(decoded.status, Status::NeedsBody);
}

fn sample(status: Status) -> Record {
    Record {
        binding: Binding {
            parent: Identity {
                device: DeviceId(1),
                inode: InodeId(2),
            },
            workspace: Identity {
                device: DeviceId(1),
                inode: InodeId(3),
            },
            data: Identity {
                device: DeviceId(1),
                inode: InodeId(4),
            },
            destination: b"output".to_vec(),
            required: requirement(b"abc"),
        },
        partial: complete(b"abc"),
        status,
    }
}
