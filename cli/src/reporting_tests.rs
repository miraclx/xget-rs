use core::time::Duration;

use serde_json::json;
use xget::client::{Error, Event, Observer as _, Report, RunFailure, SavedError};
use xget::transfer::{ByteCount, FailureKind, Length, Partial, Phase, StateRef, WriteExtent};

use super::{BytesPerSecond, Delivery, Mode, Presentation, Rate, Reporter, Snapshot};
use crate::failure::Failure;
use crate::output::{Channel, Format, Output, RunId};

#[test]
fn zero_unknown_overrun_and_live_acceptance_keep_distinct_human_counts_without_completion_claims() {
    let mut snapshot = Snapshot {
        length: Length::Known(ByteCount(0)),
        received: ByteCount(0),
        settled: ByteCount(0),
        hashed: ByteCount(0),
        source: ByteCount(0),
        delivery: Delivery::Native {
            restored: ByteCount(0),
            written: ByteCount(0),
            extent: WriteExtent::Confirmed,
        },
    };
    let zero = snapshot.human(Presentation::default().units(), Some(BytesPerSecond(2)));
    assert!(zero.percent.is_none());
    assert!(zero.eta.is_none());
    assert!(!zero.plain().contains('%'));
    snapshot.length = Length::Unknown;
    let unknown = snapshot.human(Presentation::default().units(), Some(BytesPerSecond(2)));
    assert!(unknown.percent.is_none());
    assert!(unknown.eta.is_none());
    assert!(unknown.plain().contains("unknown size"));
    snapshot.length = Length::Known(ByteCount(3));
    snapshot.received = ByteCount(u64::MAX);
    let overrun = snapshot
        .human(crate::display::DisplayFormat::RawBytes, None)
        .plain();
    assert!(overrun.contains("18446744073709551615 B/3 B"));
    assert!(!overrun.contains("100%"));
    assert!(overrun.contains("awaiting completion"));
    snapshot.length = Length::Known(ByteCount(10));
    snapshot.received = ByteCount(9);
    snapshot.settled = ByteCount(3);
    snapshot.delivery = Delivery::Live {
        released: ByteCount(3),
    };
    let live = snapshot.human(Presentation::default().units(), Some(BytesPerSecond(2)));
    assert!(live.plain().contains("30% accepted 3 B/10 B"));
    assert_eq!(live.eta.as_deref(), Some("ETA 1s"));
    assert!(!live.plain().contains("verified"));
}

#[test]
fn restored_logical_jumps_never_enter_the_source_rate() {
    let restored = Snapshot {
        length: Length::Known(ByteCount(1_000_000)),
        received: ByteCount(900_000),
        settled: ByteCount(900_000),
        hashed: ByteCount(900_000),
        source: ByteCount(0),
        delivery: Delivery::Native {
            restored: ByteCount(900_000),
            written: ByteCount(0),
            extent: WriteExtent::Confirmed,
        },
    };
    let mut rate = Rate::default();
    assert_eq!(
        rate.sample(restored.source, Duration::from_secs(10)),
        BytesPerSecond(0)
    );
    let facts = restored.machine(BytesPerSecond(0));
    assert_eq!(facts["restored_bytes"], 900_000);
    assert_eq!(facts["attempt_received_bytes"], 0);
    assert_eq!(facts["rate_bytes_per_second"], 0);
    assert!(facts["eta_seconds"].is_null());
    assert_eq!(facts["completion_evidence"], false);
}

#[test]
fn rate_uses_exact_integer_current_attempt_bytes_and_floors_once() {
    let mut rate = Rate::at(Duration::from_secs(4));
    assert_eq!(
        rate.sample(ByteCount(0), Duration::from_secs(4)),
        BytesPerSecond(0)
    );
    assert_eq!(
        rate.sample(ByteCount(11), Duration::from_secs(7)),
        BytesPerSecond(3)
    );
    assert_eq!(
        rate.sample(ByteCount(11), Duration::from_secs(7)),
        BytesPerSecond(3)
    );
    let mut fast = Rate::at(Duration::from_secs(7));
    assert_eq!(
        fast.sample(ByteCount(u64::MAX), Duration::from_nanos(7_000_000_001)),
        BytesPerSecond(u64::MAX)
    );
}

#[test]
fn a_reset_attempt_counter_starts_a_new_rate_baseline() {
    let mut rate = Rate::default();
    assert_eq!(
        rate.sample(ByteCount(100), Duration::from_secs(1)),
        BytesPerSecond(100)
    );
    assert_eq!(
        rate.sample(ByteCount(3), Duration::from_secs(2)),
        BytesPerSecond(0)
    );
    assert_eq!(
        rate.sample(ByteCount(8), Duration::from_secs(3)),
        BytesPerSecond(5)
    );
}

#[test]
fn human_bits_do_not_change_sizes_and_raw_counts_win() {
    let bytes = Presentation::default().units();
    let bits = Presentation {
        bits: true,
        ..Presentation::default()
    }
    .units();
    let raw = Presentation {
        bits: true,
        raw_sizes: true,
        ..Presentation::default()
    }
    .units();
    assert_eq!(bytes.speed(10 * 1024 * 1024), "10 MiB/s");
    assert_eq!(bits.speed(10 * 1024 * 1024), "83.89 Mbps");
    assert_eq!(bits.size(1024 * 1024), "1 MiB");
    assert_eq!(raw.speed(1024 * 1024), "1048576/s");
}

#[test]
fn every_captured_bar_policy_resolves_to_plain_without_dump_or_cursor_controls() {
    assert!(Presentation::default().resolved(false) == Mode::Plain);
    assert!(
        Presentation {
            progress: Mode::Bar,
            ..Presentation::default()
        }
        .resolved(false)
            == Mode::Plain
    );
    assert!(
        Presentation {
            json: true,
            ..Presentation::default()
        }
        .resolved(false)
            == Mode::Json
    );
    assert!(
        Presentation {
            progress: Mode::None,
            ..Presentation::default()
        }
        .resolved(false)
            == Mode::None
    );
}

#[test]
fn live_facts_describe_sink_acceptance_not_native_publication_or_write_extents() {
    let snapshot = Snapshot {
        length: Length::Unknown,
        received: ByteCount(3),
        settled: ByteCount(3),
        hashed: ByteCount(3),
        source: ByteCount(3),
        delivery: Delivery::Live {
            released: ByteCount(3),
        },
    };
    let facts = snapshot.machine(BytesPerSecond(2));
    assert_eq!(facts["kind"], "Live");
    assert_eq!(facts["accepted_bytes"], 3);
    assert_eq!(facts["published"], false);
    assert!(facts.get("logical_written_bytes").is_none());
    assert!(facts.get("write_extent").is_none());
    assert!(facts["length_bytes"].is_null());
    assert!(facts["eta_seconds"].is_null());
    let line = snapshot
        .human(Presentation::default().units(), Some(BytesPerSecond(2)))
        .plain();
    assert!(line.contains("accepted"));
    assert!(!line.contains("verified"));
    assert!(!line.chars().any(char::is_control));
}

#[test]
fn native_unconfirmed_extents_remain_explicit_without_verified_claims() {
    let snapshot = Snapshot {
        length: Length::Known(ByteCount(10)),
        received: ByteCount(10),
        settled: ByteCount(3),
        hashed: ByteCount(2),
        source: ByteCount(10),
        delivery: Delivery::Native {
            restored: ByteCount(0),
            written: ByteCount(3),
            extent: WriteExtent::Unconfirmed,
        },
    };
    let facts = snapshot.machine(BytesPerSecond(5));
    assert_eq!(facts["write_extent"], "Unconfirmed");
    assert_eq!(facts["logical_written_bytes"], 3);
    assert_eq!(facts["hashed_bytes"], 2);
    assert_eq!(facts["completion_evidence"], false);
    assert_eq!(facts["eta_seconds"], 0);
    let line = snapshot
        .human(Presentation::default().units(), Some(BytesPerSecond(5)))
        .plain();
    assert!(line.contains("100% received"));
    assert!(line.contains("awaiting completion"));
    assert!(!line.contains("verified"));
    // Extent lower bounds remain explicit in the final human diagnostic, not seven progress columns.
    let failure = Failure::library(Error::Transfer(Box::new(xget::transfer::Failure {
        kind: FailureKind::Stopped,
        partial: Partial {
            received: snapshot.received,
            written: snapshot.settled,
            hashed: snapshot.hashed,
            write_extent: WriteExtent::Unconfirmed,
            ..Partial::default()
        },
        state: None,
    })));
    let human = crate::human::Human {
        units: Presentation::default().units(),
        cwd: None,
    };
    let diagnostic = human.failure(&failure, &crate::human::FailureContext::default(), None);
    assert!(diagnostic.contains("written at least 3 B"));
    assert!(diagnostic.contains("write extent unconfirmed"));
}

#[test]
fn machine_counts_are_exact_integers_with_fixed_units_and_no_human_policy() {
    let snapshot = Snapshot {
        length: Length::Known(ByteCount(u64::MAX)),
        received: ByteCount(u64::MAX),
        settled: ByteCount(u64::MAX),
        hashed: ByteCount(0),
        source: ByteCount(u64::MAX),
        delivery: Delivery::Live {
            released: ByteCount(u64::MAX),
        },
    };
    let encoded = serde_json::to_vec(&snapshot.machine(BytesPerSecond(u64::MAX))).unwrap();
    let decoded: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded["received_bytes"], json!(u64::MAX));
    assert_eq!(decoded["rate_bytes_per_second"], json!(u64::MAX));
    assert!(decoded["rate_bytes_per_second"].is_u64());
    assert!(decoded["eta_seconds"].is_u64());
}

#[test]
fn a_sticky_reporter_error_suppresses_later_success_and_returns_runtime_failure() {
    let output = Output::new(RunId::fresh(), Format::None, Channel::Diagnostic);
    let mut reporter = Reporter::new(
        &output,
        Presentation {
            progress: Mode::None,
            ..Presentation::default()
        },
    );
    reporter.record(Err(Failure::runtime(
        "fixture reporting failure; no output success is claimed",
    )));
    reporter.observe(Event::RunCompleted {
        report: &Report::default(),
    });
    assert!(reporter.terminal == super::Terminal::Pending);
    assert_eq!(reporter.complete().err().unwrap().code(), 1);
}

#[test]
fn a_missing_common_terminal_callback_is_not_manufactured_into_success() {
    let output = Output::new(RunId::fresh(), Format::None, Channel::Diagnostic);
    let reporter = Reporter::new(
        &output,
        Presentation {
            progress: Mode::None,
            ..Presentation::default()
        },
    );
    assert_eq!(reporter.complete().err().unwrap().code(), 1);
}

#[test]
fn run_failed_and_error_share_the_audited_projection_even_when_checkpointing_also_fails() {
    let run_failure = RunFailure {
        error: Error::RunCheckpoint {
            transfer: Box::new(xget::transfer::Failure {
                kind: FailureKind::Io {
                    phase: Phase::Seal,
                    cause: std::io::Error::other(
                        "https://user:secret@example.test/?signature=private-marker",
                    ),
                },
                partial: Partial {
                    received: ByteCount(3),
                    written: ByteCount(2),
                    hashed: ByteCount(1),
                    write_extent: WriteExtent::Unconfirmed,
                    ..Partial::default()
                },
                state: Some(StateRef::from(std::path::Path::new("owned/native"))),
            }),
            checkpoint: SavedError::Io(std::io::Error::other("cookie=private-marker")),
        },
        report: Report::default(),
        failed: None,
    };
    let facts = Reporter::failure_facts(&run_failure);
    let failure = Failure::library(run_failure.error);
    let diagnostic = Output::diagnostic(failure.diagnostic().unwrap());
    for key in [
        "reason",
        "message",
        "phase",
        "partial",
        "native_state_path",
        "checkpoint_failed",
    ] {
        assert_eq!(facts[key], diagnostic[key]);
    }
    assert_eq!(facts["reason"], "NativeIo");
    assert_eq!(facts["phase"], "Seal");
    assert_eq!(facts["checkpoint_failed"], true);
    assert_eq!(facts["completed_files"], 0);
    assert_eq!(facts["complete"], false);
    assert_eq!(facts["pause_confirmed"], false);
    assert!(facts["state_path"].is_null());
    for secret in [
        "private-marker",
        "secret",
        "signature",
        "cookie",
        "example.test",
    ] {
        assert!(!facts.to_string().contains(secret));
    }
}

#[test]
fn a_range_protocol_failure_keeps_its_native_partial_without_inventing_remote_or_physical_facts() {
    let failure = RunFailure {
        error: Error::RangeTransfer(Box::new(xget::transfer::Failure {
            kind: FailureKind::Body(xget::transfer::reader::ReaderError::Window),
            partial: Partial {
                received: ByteCount(2),
                written: ByteCount(1),
                ..Partial::default()
            },
            state: Some(StateRef::from(std::path::Path::new("owned/range"))),
        })),
        report: Report::default(),
        failed: None,
    };
    let facts = Reporter::failure_facts(&failure);
    assert_eq!(facts["reason"], "RangeWindow");
    assert_eq!(facts["partial"]["logical_received_bytes"], 2);
    assert_eq!(facts["partial"]["logical_written_bytes"], 1);
    assert_eq!(facts["native_state_path"], "owned/range");
    assert!(facts["phase"].is_null());
    assert_eq!(facts["complete"], false);
    assert_eq!(facts["pause_confirmed"], false);
}

#[test]
fn ribbon_percent_is_exact_tenths_and_a_full_received_frontier_still_awaits_completion() {
    let mut snapshot = Snapshot {
        length: Length::Known(ByteCount(256)),
        received: ByteCount(160),
        settled: ByteCount(32),
        hashed: ByteCount(0),
        source: ByteCount(160),
        delivery: Delivery::Native {
            restored: ByteCount(0),
            written: ByteCount(32),
            extent: WriteExtent::Confirmed,
        },
    };
    let units = Presentation::default().units();
    let partial = snapshot.human(units, None);
    assert_eq!(partial.percent.as_deref(), Some("62.5%"));
    assert!(!partial.plain().contains("verified"));
    snapshot.length = Length::Known(ByteCount(u64::MAX));
    snapshot.received = ByteCount(u64::MAX - 1);
    assert_eq!(
        snapshot.human(units, None).percent.as_deref(),
        Some("99.9%")
    );
    snapshot.received = ByteCount(u64::MAX);
    let full = snapshot.human(units, None);
    assert_eq!(full.percent.as_deref(), Some("100.0%"));
    assert!(full.count.contains("awaiting completion"));
    assert!(!full.plain().contains("saved"));
    snapshot.length = Length::Known(ByteCount(3));
    assert!(snapshot.human(units, None).percent.is_none());
}

#[test]
fn glyph_presets_are_one_presentation_option_not_new_progress_modes() {
    use clap::Parser as _;

    use crate::args::Cli;

    for style in ["classic", "fine", "solid"] {
        assert!(Cli::try_parse_from(["xget", "https://example.test/a", "--style", style]).is_ok());
    }
    assert!(Cli::try_parse_from(["xget", "https://example.test/a", "--progress", "fine"]).is_err());
    assert!(Cli::try_parse_from(["xget", "https://example.test/a", "--style", "unknown"]).is_err());
}
