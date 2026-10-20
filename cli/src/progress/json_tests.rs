use std::process::Command;

use xget::Progress as _;

use super::{JsonProgress, JsonString};

const PROBE: &str = "XGET_TEST_JSON_RETRY_CONTROLS";

fn cause() -> String {
    (0_u8..=31)
        .map(char::from)
        .chain("quoted \" slash \\ café 日本語".chars())
        .collect()
}

#[test]
fn json_strings_round_trip_empty_plain_unicode_and_every_control_character() {
    for value in [String::new(), String::from("ordinary"), cause()] {
        let encoded = JsonString(&value).to_string();
        let decoded: String = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, value);
        assert!(!encoded.chars().any(|character| character <= '\u{001f}'));
    }
}

#[test]
fn retry_callback_probe() {
    if std::env::var_os(PROBE).is_some() {
        JsonProgress::new().retry(3, 2, 5, 1024, &cause());
    }
}

#[test]
fn retry_callback_emits_one_parseable_event_without_splitting_control_characters() {
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "progress::json::tests::retry_callback_probe",
            "--nocapture",
        ])
        .env(PROBE, "1")
        .output()
        .unwrap();
    assert!(output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert_eq!(stderr.lines().count(), 1, "{stderr:?}");
    let event: serde_json::Value = serde_json::from_str(&stderr).unwrap();
    assert_eq!(event["event"], "retry");
    assert_eq!(event["chunk"], 3);
    assert_eq!(event["retry"], 2);
    assert_eq!(event["max"], 5);
    assert_eq!(event["resume_from"], 1024);
    assert_eq!(event["error"].as_str(), Some(cause().as_str()));
}
