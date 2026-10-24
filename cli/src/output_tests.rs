use serde_json::json;
use xget::client::Error;
use xget::transfer::{ByteCount, FailureKind, Length, Partial, Phase, StateRef, WriteExtent};

use super::{Channel, Format, ItemId, Output, RunId};

#[test]
fn all_control_characters_are_escaped_in_one_machine_record() {
    let controls: String = (0_u8..=31).map(char::from).collect();
    let output = Output::new(RunId::fresh(), Format::Json, Channel::Diagnostic);
    let event = output.envelope("Member", Some(ItemId(0)), &json!({"source_path": controls}));
    let encoded = serde_json::to_string(&event).unwrap();
    assert!(!encoded.bytes().any(|byte| byte < 32));
    let decoded: serde_json::Value = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded["facts"]["source_path"], controls);
    assert_eq!(decoded["kind"], "Member");
}

#[test]
fn invocation_ids_are_stable_across_events_without_claiming_persisted_identity() {
    let output = Output::new(RunId::fresh(), Format::Json, Channel::Diagnostic);
    let first = output.envelope("Planned", Some(ItemId(2)), &json!({}));
    let second = output.envelope("Published", Some(ItemId(2)), &json!({}));
    assert_eq!(first["run_id"], second["run_id"]);
    assert_eq!(first["item_id"], second["item_id"]);
    assert_eq!(first["correlation_scope"], "Invocation");
    assert_eq!(first["schema_version"], 1);
    assert!(first["elapsed_ms"].is_u64());
}

#[test]
fn byte_lengths_remain_exact_including_zero_and_unknown() {
    assert_eq!(Output::length(Length::Known(ByteCount(0))), json!(0));
    assert_eq!(
        Output::length(Length::Known(ByteCount(u64::MAX))),
        json!(u64::MAX)
    );
    assert_eq!(Output::length(Length::Unknown), serde_json::Value::Null);
}

#[test]
fn a_safe_failure_envelope_does_not_serialize_its_foreign_cause() {
    let failure = crate::failure::Failure::io(
        std::io::Error::other(
            "https://user:password@example.test/object?signature=secret; cookie=secret",
        ),
        "output failed; check the destination",
    );
    let output = Output::new(RunId::fresh(), Format::Json, Channel::Diagnostic);
    let event = output.envelope("Error", None, &output.failure_facts(&failure));
    let encoded = serde_json::to_string(&event).unwrap();
    for secret in ["password", "signature", "cookie", "secret"] {
        assert!(!encoded.contains(secret));
    }
}

#[test]
fn native_failure_facts_keep_exact_lower_bounds_phase_and_locator_without_foreign_text() {
    let error = Error::Transfer(Box::new(xget::transfer::Failure {
        kind: FailureKind::Io {
            phase: Phase::Write,
            cause: std::io::Error::other(
                "https://user:password@example.test/?signature=secret; cookie=secret",
            ),
        },
        partial: Partial {
            received: ByteCount(u64::MAX),
            written: ByteCount(7),
            hashed: ByteCount(3),
            write_extent: WriteExtent::Unconfirmed,
            ..Partial::default()
        },
        state: Some(StateRef::from(std::path::Path::new(
            "owned/native-workspace",
        ))),
    }));
    let failure = crate::failure::Failure::library(error);
    let output = Output::new(RunId::fresh(), Format::Json, Channel::Diagnostic);
    let facts = output.failure_facts(&failure);
    assert_eq!(facts["reason"], "NativeIo");
    assert_eq!(facts["phase"], "Write");
    assert_eq!(facts["native_state_path"], "owned/native-workspace");
    assert_eq!(facts["partial"]["logical_received_bytes"], u64::MAX);
    assert_eq!(facts["partial"]["logical_written_bytes"], 7);
    assert_eq!(facts["partial"]["hashed_bytes"], 3);
    assert_eq!(facts["partial"]["write_extent"], "Unconfirmed");
    assert_eq!(facts["partial"]["completion_evidence"], false);
    assert_eq!(facts["pause_confirmed"], false);
    assert_eq!(facts["exit_code"], 1);
    let encoded = serde_json::to_string(&facts).unwrap();
    for secret in ["password", "signature", "cookie", "secret", "example.test"] {
        assert!(!encoded.contains(secret));
    }
}

#[test]
fn known_native_reasons_do_not_fabricate_a_physical_phase_or_completion() {
    for (kind, reason) in [
        (FailureKind::LengthMismatch, "LengthMismatch"),
        (FailureKind::LengthOverflow, "LengthOverflow"),
        (FailureKind::DestinationExists, "DestinationExists"),
        (FailureKind::Stopped, "Stopped"),
        (FailureKind::Busy, "Busy"),
        (FailureKind::RuntimeUnavailable, "RuntimeUnavailable"),
        (FailureKind::Unsupported, "Unsupported"),
        (FailureKind::InvalidState, "InvalidState"),
        (
            FailureKind::PublicationUnknown {
                cause: std::io::Error::other("private-cause-marker"),
            },
            "PublicationUnknown",
        ),
    ] {
        let failure =
            crate::failure::Failure::library(Error::Transfer(Box::new(xget::transfer::Failure {
                kind,
                partial: Partial::default(),
                state: None,
            })));
        let output = Output::new(RunId::fresh(), Format::Json, Channel::Diagnostic);
        let facts = output.failure_facts(&failure);
        assert_eq!(facts["reason"], reason);
        assert!(facts["phase"].is_null());
        assert!(facts["native_state_path"].is_null());
        assert_eq!(facts["partial"]["completion_evidence"], false);
        assert!(!facts.to_string().contains("private-cause-marker"));
        assert_eq!(failure.message(), failure.message().to_lowercase());
        assert!(!failure.message().ends_with('.'));
    }
}

#[test]
fn local_revalidation_is_not_reported_as_a_new_remote_etag_match() {
    use xget::client::VersionConsistency;
    assert_eq!(
        Output::version(VersionConsistency::LocallyRevalidated),
        "LocallyRevalidated"
    );
    assert_eq!(
        Output::version(VersionConsistency::StrongEtagMatched),
        "StrongEtagMatched"
    );
    assert_eq!(
        Output::version(VersionConsistency::StrongEtagEstablished),
        "StrongEtagEstablished"
    );
    assert_eq!(Output::version(VersionConsistency::Unpinned), "Unpinned");
}
