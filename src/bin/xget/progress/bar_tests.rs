use core::cell::RefCell;
use std::process::{Command, Stdio};

use xget::Progress as _;
use xprogress::{Bar, DrawTarget};

use super::super::Meter;
use super::{BarProgress, Live, MAX_NOTICES, frame, locale_is_utf8};
use crate::DisplayFormat;

/// A `Live` far from any terminal: `DrawTarget::hidden()` draws nothing, so the test never touches
/// stderr, and `Meter::new` starts the readout from zero.
fn live(total: u64) -> Live {
    Live {
        bar: Bar::new([total]),
        target: DrawTarget::hidden(),
        width: 20,
        multi: false,
        prev_lines: 0,
        meter: Meter::new(total),
        format: DisplayFormat::HumanBytes,
        chunks: 1,
        unicode: false,
        notices: Vec::new(),
    }
}

fn started(progress: &BarProgress, total: u64) {
    *progress.inner.borrow_mut() = Some(live(total));
}

/// `frame` needs `&mut Live` for the meter, so the borrow of `inner` must end before the read.
fn render(progress: &BarProgress) -> String {
    let mut state = progress.inner.borrow_mut();
    frame(state.as_mut().expect("started"))
}

#[test]
fn an_inherited_dump_setting_cannot_write_progress_into_payload_stdout() {
    // Child-only environment avoids mutating process-global state while other tests are running.
    for value in ["", "1"] {
        let output = Command::new(std::env::current_exe().expect("current test executable"))
            .args([
                "--exact",
                "progress::bar::bar_tests::payload_target_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("XGET_TEST_BAR_PAYLOAD_CHILD", "1")
            .env("XPROGRESS_DUMP", value)
            .stdin(Stdio::null())
            .output()
            .expect("isolated child");
        let stdout = String::from_utf8(output.stdout).expect("test output utf-8");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "child failed: {stdout:?}; {stderr}"
        );
        let payload = stdout
            .split_once("PAYLOAD-BEGIN\n")
            .expect("payload start")
            .1
            .split_once("PAYLOAD-END\n")
            .expect("payload end")
            .0;
        assert_eq!(
            payload, "",
            "progress entered stdout with XPROGRESS_DUMP={value:?}"
        );
    }
}

#[test]
fn payload_target_child() {
    if std::env::var_os("XGET_TEST_BAR_PAYLOAD_CHILD").is_none() {
        return;
    }
    assert!(
        matches!(DrawTarget::from_env(), DrawTarget::Dump),
        "fixture must exercise the dump override"
    );
    let progress = BarProgress::new(DisplayFormat::HumanBytes);
    println!("PAYLOAD-BEGIN");
    progress.start(&[60, 40]);
    progress.received(0, 10);
    progress.wrote(0, 10);
    progress.finish();
    println!("PAYLOAD-END");
    assert!(
        matches!(
            progress.inner.borrow().as_ref().expect("started").target,
            DrawTarget::Terminal | DrawTarget::Hidden
        ),
        "payload reporter must never choose stdout Dump"
    );
}

#[test]
fn utf8_locales_are_recognized() {
    assert!(locale_is_utf8("en_US.UTF-8"));
    assert!(locale_is_utf8("C.utf8"));
    assert!(!locale_is_utf8("C"));
    assert!(!locale_is_utf8("en_US.ISO8859-1"));
}

#[test]
fn a_bar_without_retries_draws_no_notice_lines() {
    let progress = BarProgress {
        inner: RefCell::new(None),
        format: DisplayFormat::HumanBytes,
    };
    started(&progress, 100);
    assert_eq!(render(&progress).lines().count(), 1);
}

#[test]
fn a_retry_is_parked_above_the_bar_and_stays_there() {
    let progress = BarProgress {
        inner: RefCell::new(None),
        format: DisplayFormat::HumanBytes,
    };
    started(&progress, 100);
    progress.retry(0, 1, 10, 40, "connection reset");
    // A later redraw (as `received` would trigger) must still carry the notice: it is parked, not
    // painted once.
    progress.received(0, 10);
    let block = render(&progress);
    let lines: Vec<&str> = block.lines().collect();
    assert_eq!(lines.len(), 2, "got {lines:?}");
    assert!(lines[0].contains("retry 1/10"), "got {:?}", lines[0]);
    assert!(lines[0].contains("connection reset"), "got {:?}", lines[0]);
    assert!(
        lines[0].contains("40 B"),
        "resume point missing: {:?}",
        lines[0]
    );
    assert!(lines[1].contains('['), "bar line missing: {:?}", lines[1]);
}

#[test]
fn notices_cap_so_a_mass_retry_cannot_push_the_bar_off_screen() {
    let progress = BarProgress {
        inner: RefCell::new(None),
        format: DisplayFormat::HumanBytes,
    };
    started(&progress, 100);
    for attempt in 1..=(MAX_NOTICES as u32 + 3) {
        progress.retry(0, attempt, 50, 0, "timed out");
    }
    let state = progress.inner.borrow();
    let live = state.as_ref().expect("started");
    assert_eq!(live.notices.len(), MAX_NOTICES);
    // The oldest is dropped, so the surviving first notice is attempt 4 of MAX_NOTICES + 3.
    let first = &live.notices[0];
    assert!(
        first.contains("retry 4/50"),
        "oldest notice should be attempt 4, got {first:?}"
    );
}
