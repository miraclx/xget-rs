use clap::Parser as _;
use xget::transfer::{Algorithm, Measurement};

use super::{Target, measurement};
use crate::args::Cli;

#[test]
fn measurement_does_not_substitute_for_required_checks() {
    assert_eq!(measurement(xget::Checksum::None), Measurement::None);
    assert_eq!(
        measurement(xget::Checksum::Sha256),
        Measurement::Algorithm(Algorithm::Sha256)
    );
    let cli = Cli::try_parse_from([
        "xget",
        "https://example.test/file",
        "-s",
        "none",
        "--expect",
        "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
    ])
    .unwrap();
    let prepared = cli.download.prepare().unwrap();
    assert_eq!(prepared.measurement, Measurement::None);
    assert!(prepared.expect.is_some());
}

#[test]
fn stdout_is_a_distinct_entrance_not_a_native_filename_or_spool() {
    let cli = Cli::try_parse_from(["xget", "https://example.test/file", "-o", "-"]).unwrap();
    assert!(matches!(
        cli.download.prepare().unwrap().target,
        Target::Stdout
    ));
}

#[test]
fn fresh_owned_run_policy_is_not_silently_applied_to_live_output() {
    let cli =
        Cli::try_parse_from(["xget", "https://example.test/file", "-o", "-", "--fresh"]).unwrap();
    assert_eq!(cli.download.prepare().err().unwrap().code(), 2);
}

#[test]
fn live_preview_and_mixed_originals_wait_for_actual_selected_cardinality() {
    let cli = Cli::try_parse_from([
        "xget",
        "https://example.test/file",
        "--tree",
        "s3://bucket/empty/",
        "-o",
        "-",
        "--dry-run",
    ])
    .unwrap();
    assert!(cli.download.prepare().is_ok());
}

#[test]
fn local_sidecar_option_never_reinterprets_remote_expectations_or_stdin() {
    assert!(
        Cli::try_parse_from([
            "xget",
            "https://example.test/file",
            "--expect-file",
            "checks.sha256",
            "-s",
            "none",
            "-o",
            "-"
        ])
        .unwrap()
        .download
        .prepare()
        .is_ok()
    );
    assert!(
        Cli::try_parse_from([
            "xget",
            "https://example.test/file",
            "--expect-file-algorithm",
            "sha512"
        ])
        .is_err()
    );
    assert!(
        Cli::try_parse_from([
            "xget",
            "https://example.test/file",
            "--expect",
            "https://example.test/checks"
        ])
        .is_err()
    );
    assert!(
        Cli::try_parse_from(["xget", "https://example.test/file", "--expect-file", "-"])
            .unwrap()
            .download
            .prepare()
            .is_err()
    );
}

#[test]
fn native_authorities_are_explicit_and_independent() {
    let ordinary = Cli::try_parse_from(["xget", "https://example.test/file", "-o", "out"])
        .unwrap()
        .download
        .prepare()
        .unwrap();
    assert!(!ordinary.fresh && !ordinary.restart && !ordinary.overwrite);
    let restart = Cli::try_parse_from([
        "xget",
        "https://example.test/file",
        "--restart",
        "-f",
        "-o",
        "out",
    ])
    .unwrap()
    .download
    .prepare()
    .unwrap();
    assert!(restart.restart && restart.overwrite && !restart.fresh);
    let fresh = Cli::try_parse_from(["xget", "https://example.test/file", "--fresh", "-o", "out"])
        .unwrap()
        .download
        .prepare()
        .unwrap();
    assert!(fresh.fresh && !fresh.overwrite && !fresh.restart);
    let preview = Cli::try_parse_from([
        "xget",
        "https://example.test/file",
        "--dry-run",
        "--overwrite",
        "-o",
        "out",
    ])
    .unwrap()
    .download
    .prepare()
    .unwrap();
    assert!(preview.dry_run && preview.overwrite && !preview.restart);
}

#[test]
fn conflicting_restart_modes_fail_in_either_argument_order() {
    for arguments in [
        vec!["xget", "https://example.test/file", "--fresh", "--restart"],
        vec!["xget", "https://example.test/file", "--restart", "--fresh"],
        vec![
            "xget",
            "https://example.test/file",
            "--dry-run",
            "--restart",
        ],
        vec![
            "xget",
            "https://example.test/file",
            "--restart",
            "--expect-file",
            "checks",
        ],
    ] {
        assert!(Cli::try_parse_from(arguments).is_err());
    }
}

#[test]
fn live_output_refuses_native_attempt_and_replacement_authority() {
    for authority in ["--restart", "-f", "--overwrite"] {
        for preview in [false, true] {
            let mut arguments = vec!["xget", "https://example.test/file", "-o", "-", authority];
            if preview {
                arguments.push("--dry-run");
            }
            match Cli::try_parse_from(arguments) {
                Ok(cli) => assert_eq!(cli.download.prepare().err().unwrap().code(), 2),
                Err(error) => assert_eq!(error.exit_code(), 2),
            }
        }
    }
}

#[test]
fn a_missing_frozen_restart_and_conflicting_authority_are_request_errors() {
    for error in [
        xget::client::SavedError::RestartMissing,
        xget::client::SavedError::Authority,
    ] {
        assert_eq!(crate::failure::Failure::saved(error).code(), 2);
    }
}

#[test]
fn resume_accepts_only_its_explicit_frozen_attempt_and_publication_flags() {
    let binding = "key=https://example.test/file";
    assert!(matches!(
        Cli::try_parse_from([
            "xget",
            "resume",
            "state",
            "--input",
            binding,
            "--restart",
            "-f"
        ])
        .unwrap()
        .command,
        Some(crate::args::Command::Resume(_))
    ));
    for unsupported in ["--fresh", "--dry-run"] {
        assert!(
            Cli::try_parse_from(["xget", "resume", "state", "--input", binding, unsupported])
                .is_err()
        );
    }
}

#[test]
fn default_output_and_explicit_directory_have_distinct_authority() {
    let cli = Cli::try_parse_from(["xget", "https://example.test/file"]).unwrap();
    assert!(matches!(
        cli.download.prepare().unwrap().target,
        Target::Default
    ));
    let cli = Cli::try_parse_from(["xget", "https://example.test/file", "-D", "new-root"]).unwrap();
    assert!(matches!(
        cli.download.prepare().unwrap().target,
        Target::Directory(_)
    ));
}

#[test]
fn collision_and_frozen_binding_remedies_do_not_advertise_nonexistent_flags() {
    let collision = crate::failure::Failure::library(xget::client::Error::LayoutConflict);
    assert_eq!(collision.code(), 2);
    assert_eq!(
        collision.diagnostic().unwrap().reason.name(),
        "LayoutConflict"
    );
    assert!(
        collision
            .message()
            .contains("separate invocations with distinct -o paths")
    );
    assert!(
        collision
            .message()
            .contains("input namespaces through the rust api")
    );
    let binding = crate::failure::Failure::saved(xget::client::SavedError::BindingIntentRequired);
    assert_eq!(binding.code(), 1);
    assert_eq!(
        binding.diagnostic().unwrap().reason.name(),
        "BindingIntentRequired"
    );
    assert!(binding.message().contains("original request"));
    assert!(binding.message().contains("original scope and selection"));
    assert!(
        binding
            .message()
            .contains("namespace and pattern intent through the rust api")
    );
    for message in [collision.message(), binding.message()] {
        assert_eq!(message, message.to_lowercase());
        assert!(!message.contains("--namespace"));
        assert!(!message.contains("--scope"));
        assert!(!message.ends_with('.'));
    }
}
