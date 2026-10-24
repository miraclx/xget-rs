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
    bytes[7] = b'3';
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
    record.partial.checks = None;
    assert!(Record::decode(&record.encode()).is_err());
    record.partial = complete(b"abc");
    record.partial = complete(b"different");
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
            checks: requirement(b"abc").into(),
        },
        partial: complete(b"abc"),
        status,
    }
}

#[test]
fn v1_published_state_decodes_with_its_original_sha256_anchor_and_upgrades_on_write() {
    let bytes = v1_fixture(Status::Published, complete(b"abc"));
    let decoded = Record::decode(&bytes).unwrap();
    assert_eq!(decoded.binding.checks, requirement(b"abc").into());
    assert_eq!(decoded.partial, complete(b"abc"));
    assert_eq!(decoded.status, Status::Published);
    assert_eq!(&decoded.encode()[..8], b"XGETNF02");
}

#[test]
fn v1_interrupted_and_mismatched_one_shot_evidence_is_still_readable() {
    let interrupted = Partial {
        received: ByteCount(3),
        written: ByteCount(1),
        hashed: ByteCount(1),
        write_extent: WriteExtent::Unconfirmed,
        ..Partial::default()
    };
    let decoded = Record::decode(&v1_fixture(Status::NeedsBody, interrupted)).unwrap();
    assert_eq!(decoded.partial, interrupted);
    assert_eq!(decoded.status, Status::NeedsBody);
    let decoded = Record::decode(&v1_fixture(Status::NeedsBody, complete(b"different"))).unwrap();
    assert_eq!(
        decoded.partial.actual,
        Some(requirement(b"different").digest())
    );
    assert_eq!(
        decoded
            .partial
            .checks
            .unwrap()
            .matched_requirements()
            .count(),
        0
    );
}

#[test]
fn v2_no_hash_completion_is_bounded_and_cannot_fabricate_a_strong_anchor() {
    let mut record = sample(Status::Published);
    record.binding.checks = crate::checks::CheckPolicy {
        requirements: crate::checks::Requirements::default(),
        measurement: crate::transfer::Measurement::None,
    };
    let results = record.binding.checks.start().finish().unwrap();
    record.partial = Partial {
        received: ByteCount(3),
        written: ByteCount(3),
        ..Partial::default()
    };
    record.partial.set_checks(results);
    let bytes = record.encode();
    assert!(bytes.len() as u64 <= super::MAX_RECORD);
    let decoded = Record::decode(&bytes).unwrap();
    assert_eq!(decoded.partial.hashed, ByteCount(0));
    assert_eq!(decoded.partial.actual, None);
    assert_eq!(decoded.partial.checks.unwrap().strong_anchor(), None);
}

#[test]
fn v2_rejects_invalid_provenance_duplicate_algorithms_and_unrequested_measurement() {
    let mut bytes = sample(Status::Published).encode();
    // Header/status/three identities are 57 bytes, then one requirement count, tag and digest.
    let origins = 57 + 1 + 1 + 32;
    bytes[origins] = 8;
    assert!(matches!(Record::decode(&bytes), Err(InvalidRecord)));
    let mut bytes = sample(Status::Published).encode();
    let entry = bytes[58..origins + 1].to_vec();
    bytes[57] = 2;
    bytes.splice(origins + 1..origins + 1, entry);
    assert!(matches!(Record::decode(&bytes), Err(InvalidRecord)));
    let mut record = sample(Status::Published);
    record.binding.checks.measurement = crate::transfer::Measurement::None;
    assert!(matches!(
        Record::decode(&record.encode()),
        Err(InvalidRecord)
    ));
}

fn v1_fixture(status: Status, partial: Partial) -> Vec<u8> {
    let mut bytes = b"XGETNF01".to_vec();
    bytes.push(match status {
        Status::NeedsBody => 0,
        Status::Sealed => 1,
        Status::Publishing => 2,
        Status::Published => 3,
    });
    for value in [1u64, 2, 1, 3, 1, 4] {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes.extend_from_slice(&requirement(b"abc").digest().0);
    match partial.actual {
        Some(actual) => {
            bytes.push(1);
            bytes.extend_from_slice(&actual.0);
        }
        None => {
            bytes.push(0);
            bytes.extend_from_slice(&[0; 32]);
        }
    }
    bytes.push(match partial.write_extent {
        WriteExtent::Confirmed => 0,
        WriteExtent::Unconfirmed => 1,
    });
    for count in [partial.received, partial.written, partial.hashed] {
        bytes.extend_from_slice(&count.0.to_le_bytes());
    }
    bytes.extend_from_slice(&6u16.to_le_bytes());
    bytes.extend_from_slice(b"output");
    bytes
}
