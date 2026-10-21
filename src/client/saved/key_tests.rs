#![allow(clippy::unwrap_used)]

use std::path::Path;

use super::{BindingKey, BindingKeyParseError, Fingerprint, RunId};

#[test]
fn versioned_keys_roundtrip_without_exposing_paths_or_remote_references() {
    for hash in [
        [0; 32],
        [0xff; 32],
        core::array::from_fn(|index| index as u8),
    ] {
        let run = RunId(Fingerprint(hash));
        assert_eq!(run.to_string().parse::<RunId>().unwrap(), run);
        for index in [0, 127] {
            let member = BindingKey {
                run: Fingerprint(hash),
                index,
            };
            let text = member.to_string();
            assert_eq!(text.len(), 89);
            assert_eq!(text.parse::<BindingKey>().unwrap(), member);
            assert_eq!(member.run_id(), run);
            assert!(!format!("{member:?}").contains(&text));
        }
    }
}

#[test]
fn malformed_future_noncanonical_and_out_of_range_keys_fail_without_truncation() {
    let valid = BindingKey {
        run: Fingerprint([0xab; 32]),
        index: 0,
    }
    .to_string();
    for end in 0..valid.len() {
        assert_eq!(
            valid[..end].parse::<BindingKey>(),
            Err(BindingKeyParseError)
        );
    }
    for invalid in [
        format!("{valid} "),
        format!(" {valid}"),
        format!("{valid}:extra"),
        valid.replace("-v1:", "-v2:"),
        valid.replace("ab", "AB"),
        valid.replace("00000000", "00000080"),
        valid.replace("00000000", "ffffffff"),
        valid.replace("00000000", "-0000001"),
        valid.replace("00000000", "0000000g"),
    ] {
        assert_eq!(invalid.parse::<BindingKey>(), Err(BindingKeyParseError));
    }
    assert!("xget-run-v1:00".parse::<RunId>().is_err());
    assert!(valid.parse::<RunId>().is_err());
}

#[test]
fn workspace_correlation_is_not_a_remote_identity_or_portable_moved_workspace_claim() {
    let first = RunId::for_path(Path::new("/caller/first/.xget-runs/run-a"));
    let second = RunId::for_path(Path::new("/caller/second/.xget-runs/run-a"));
    assert_ne!(first, second);
    let text = first.to_string();
    assert_eq!(text.len(), 76);
    assert!(!text.contains("caller"));
    assert!(!text.contains("run-a"));
    assert_eq!(text.parse::<RunId>().unwrap(), first);
}
