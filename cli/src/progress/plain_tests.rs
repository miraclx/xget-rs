use std::process::Command;

use xget::Progress as _;

use super::PlainProgress;
use crate::DisplayFormat;

const PROBE: &str = "XGET_TEST_CAPTURED_PLAIN";

#[test]
fn captured_plain_callback_probe() {
    if std::env::var_os(PROBE).is_some() {
        let progress = PlainProgress::new(DisplayFormat::RawBytes);
        progress.start(&[3]);
        progress.restore(&[1]);
        progress.received(0, 2);
        progress.retry(0, 1, 2, 1, "remote\nline\r\x1b[31m\tend");
        progress.finish();
    }
}

#[test]
fn captured_plain_callbacks_are_complete_lines_without_cursor_or_remote_controls() {
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "progress::plain::tests::captured_plain_callback_probe",
            "--nocapture",
        ])
        .env(PROBE, "1")
        .output()
        .unwrap();
    assert!(output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.ends_with('\n'), "{stderr:?}");
    assert!(
        !stderr
            .chars()
            .any(|character| character.is_control() && character != '\n'),
        "{stderr:?}"
    );
    let lines: Vec<_> = stderr.lines().collect();
    assert!(lines.len() >= 4, "{stderr:?}");
    assert!(lines[0].contains("0/3"), "{stderr:?}");
    assert!(lines[1].contains("1/3"), "{stderr:?}");
    assert!(
        lines
            .iter()
            .any(|line| line.contains("remote\\nline\\r\\u{1b}[31m\\tend")),
        "{stderr:?}"
    );
    assert!(lines.last().unwrap().contains("3/3"), "{stderr:?}");
}
