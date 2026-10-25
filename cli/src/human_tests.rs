use core::time::Duration;
use std::path::{Path, PathBuf};

use xget::client::{Error, SavedStatus, VersionConsistency};
use xget::transfer::{
    ByteCount, CheckResults, FailureKind, Length, Partial, Phase, StateRef, WriteExtent,
};

use super::{FailureContext, Human, Position};
use crate::display::DisplayFormat;
use crate::failure::Failure;

fn human() -> Human {
    Human {
        units: DisplayFormat::HumanBytes,
        cwd: Some(PathBuf::from("/downloads")),
    }
}

#[test]
fn headers_keep_distinct_destinations_zero_unknown_and_serial_ordinals() {
    let human = human();
    assert_eq!(
        human.header(
            Path::new("/downloads/a/file"),
            Length::Known(ByteCount(0)),
            Position {
                current: 1,
                total: 1
            }
        ),
        "a/file  0 B"
    );
    assert_eq!(
        human.header(
            Path::new("/elsewhere/b/file"),
            Length::Unknown,
            Position {
                current: 2,
                total: 3
            }
        ),
        "[2/3] /elsewhere/b/file  unknown size"
    );
    assert_eq!(
        human.header(
            Path::new("/downloads/a/odd\nname"),
            Length::Known(ByteCount(3)),
            Position {
                current: 1,
                total: 1
            }
        ),
        "\"a/odd\\nname\"  3 B"
    );
}

#[test]
fn no_hash_receipts_are_saved_not_verified_and_local_revalidation_is_not_new_fetching() {
    let human = human();
    let saved = human.native_receipt(
        Path::new("/downloads/archive"),
        ByteCount(3),
        CheckResults::default(),
        VersionConsistency::StrongEtagMatched,
    );
    assert_eq!(saved, "saved archive  3 B");
    let local = human.native_receipt(
        Path::new("/downloads/archive"),
        ByteCount(3),
        CheckResults::default(),
        VersionConsistency::LocallyRevalidated,
    );
    assert_eq!(
        local,
        "revalidated archive  3 B\n  local content and file identity matched recorded evidence; remote version not checked"
    );
    let unpinned = human.delivered(
        ByteCount(3),
        CheckResults::default(),
        VersionConsistency::Unpinned,
    );
    assert!(unpinned.starts_with("delivered 3 B to stdout\nwarning:"));
    for forbidden in [
        "verified",
        "measurement: none",
        "checks: none",
        "StrongEtag",
        "source bytes",
        "restored",
    ] {
        assert!(!saved.contains(forbidden));
        assert!(!local.contains(forbidden));
    }
}

#[test]
fn completed_summaries_are_zero_one_many_and_explicit_resume_only() {
    let human = human();
    assert_eq!(human.done(0, Duration::ZERO, None), "no files to download");
    assert_eq!(
        human.done(1, Duration::from_millis(687), None),
        "done 1 file in 0.68s"
    );
    assert_eq!(
        human.done(3, Duration::from_millis(2400), None),
        "done 3 files in 2.40s"
    );
    let resumed = human.done(
        1,
        Duration::ZERO,
        Some(Path::new("/downloads/.xget-runs/run")),
    );
    assert!(resumed.contains("state: '/downloads/.xget-runs/run'"));
    assert!(resumed.contains("without rediscovering latest"));
}

#[test]
fn human_failure_keeps_partial_lower_bounds_and_paths_without_foreign_causes() {
    let failure = Failure::library(Error::Transfer(Box::new(xget::transfer::Failure {
        kind: FailureKind::Io {
            phase: Phase::Write,
            cause: std::io::Error::other(
                "https://user:password@example.test/?signature=secret; cookie=secret",
            ),
        },
        partial: Partial {
            received: ByteCount(6),
            written: ByteCount(5),
            hashed: ByteCount(4),
            write_extent: WriteExtent::Unconfirmed,
            ..Partial::default()
        },
        state: Some(StateRef::from(Path::new("/downloads/native"))),
    })));
    let context = FailureContext {
        total: Some(2),
        completed: Some(1),
        ..FailureContext::default()
    };
    let text = human().failure(&failure, &context, Some(Path::new("/downloads/it's state")));
    assert_eq!(text.matches("error:").count(), 1);
    for required in [
        "completed: 1 of 2 files",
        "phase: write",
        "partial: received 6 B; written at least 5 B; hashed 4 B",
        "write extent unconfirmed; these counts are not completion evidence",
        "native state: /downloads/native",
        "inspect: xget info '/downloads/it'\\''s state'",
    ] {
        assert!(text.contains(required), "{text}");
    }
    for forbidden in [
        "password",
        "signature",
        "cookie",
        "secret",
        "example.test",
        "\"partial\"",
        "paused",
        "sha256:",
    ] {
        assert!(!text.contains(forbidden), "{text}");
    }
}

#[test]
fn absent_failure_facts_are_not_invented_and_all_ambiguity_paths_are_copyable() {
    let failure = Failure::request("cannot choose retained work; inspect each candidate");
    let context = FailureContext {
        choices: vec![PathBuf::from("/one/run"), PathBuf::from("/two/odd\nrun")],
        ..FailureContext::default()
    };
    let text = human().failure(&failure, &context, None);
    assert!(!text.contains("completed:"));
    assert!(!text.contains("partial:"));
    assert!(!text.contains("phase:"));
    assert!(text.contains("inspect: xget info '/one/run'"));
    assert!(text.contains("inspect (bash/zsh): xget info $'/two/odd\\x0arun'"));
    assert!(!text.contains('\r'));
    assert_eq!(text.matches("candidate state:").count(), 2);
}

#[test]
fn policy_inspection_explains_recorded_statuses_without_content_certification() {
    let text = human().info_end(
        4,
        [
            SavedStatus::Pending,
            SavedStatus::Submitted,
            SavedStatus::Retained,
            SavedStatus::Complete,
            SavedStatus::Complete,
        ]
        .into_iter(),
    );
    assert!(text.starts_with("4 members; offline metadata only; content not revalidated"));
    assert_eq!(
        text.matches("complete: publication recorded; continuation revalidates")
            .count(),
        1
    );
    assert!(text.ends_with("re-supply the exact original references; they are not stored here"));
    assert_eq!(
        human().listed(0, 0, false),
        "0 files, 0 directories; metadata only"
    );
    assert_eq!(human().planned_end(0), "planned 0 files; not executed");
}

#[test]
fn selector_actions_attach_option_looking_values_without_changing_parser_policy() {
    use clap::Parser as _;

    use crate::args::Cli;

    for token in ["-asset.bin", "--help", "ordinary.bin"] {
        let mut text = String::new();
        Human::selector(&mut text, token);
        assert_eq!(text, format!("\n  select: --select-id='{token}'"));
        let attached = format!("--select-id={token}");
        assert!(
            Cli::try_parse_from([
                "xget",
                "--tree",
                "s3://fixture-bucket/",
                "-D",
                "out",
                &attached,
            ])
            .is_ok()
        );
    }
}

#[test]
fn raw_units_are_explicit_and_bits_change_only_human_speed() {
    let bytes = human();
    let bits = Human {
        units: DisplayFormat::HumanBits,
        cwd: None,
    };
    let raw = Human {
        units: DisplayFormat::RawBytes,
        cwd: None,
    };
    assert_eq!(bytes.size(1024), bits.size(1024));
    assert_eq!(bits.speed(1024), "8.19 kbps");
    assert_eq!(raw.size(1024), "1024 B");
    assert_eq!(raw.speed(1024), "bytes/s: 1024");
}
