//! Tests for the resume splice guard's deliberately length-only fallback.

use super::validators_agree;

#[test]
fn matching_validators_allow_resume() {
    assert!(validators_agree(Some("\"v1\""), Some("\"v1\"")));
}

#[test]
fn different_validators_prevent_resume() {
    assert!(!validators_agree(Some("\"v1\""), Some("\"v2\"")));
}

#[test]
fn a_missing_stored_validator_falls_back_to_length() {
    assert!(validators_agree(None, Some("\"v1\"")));
}

#[test]
fn a_missing_current_validator_falls_back_to_length() {
    assert!(validators_agree(Some("\"v1\""), None));
}

#[test]
fn no_validators_falls_back_to_length() {
    assert!(validators_agree(None, None));
}
