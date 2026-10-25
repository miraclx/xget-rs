use clap::{CommandFactory as _, Parser as _};

use super::{Cli, Command};

#[test]
fn no_input_is_a_request_error_not_an_empty_success() {
    let cli = Cli::try_parse_from(["xget"]).expect("parser permits preparation diagnostic");
    let failure = cli
        .download
        .prepare()
        .err()
        .expect("no input fails preparation");
    assert_eq!(failure.code(), 2);
}

#[test]
fn malformed_expectations_explain_the_format_without_echoing_rejected_values() {
    for value in [
        "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
        "https://example.invalid/parse-secret-marker",
        "sha256:parse-secret-marker",
    ] {
        let error = Cli::try_parse_from(["xget", "--expect", value])
            .err()
            .unwrap();
        let failure = Cli::parse_failure(&error);
        assert_eq!(failure.code(), 2);
        assert!(failure.message().contains("invalid --expect value"));
        assert!(failure.message().contains("sha256:"));
        assert!(!failure.message().contains(value));
        assert!(!failure.message().contains("parse-secret-marker"));
    }
}

#[test]
fn unrelated_parser_errors_do_not_inherit_expectation_advice_or_reveal_values() {
    for arguments in [
        vec!["xget", "--parse-secret-marker"],
        vec!["xget", "-s", "parse-secret-marker"],
    ] {
        let error = Cli::try_parse_from(arguments).err().unwrap();
        let failure = Cli::parse_failure(&error);
        assert_eq!(failure.code(), 2);
        assert!(!failure.message().contains("invalid --expect value"));
        assert!(!failure.message().contains("parse-secret-marker"));
    }
}

#[test]
fn exact_share_and_mixed_inputs_use_one_explicit_front_door() {
    for arguments in [
        vec!["xget", "https://example.test/a"],
        vec![
            "xget",
            "https://www.swisstransfer.com/d/01234567-89ab-cdef-0123-456789abcdef",
            "-D",
            "files",
        ],
        vec![
            "xget",
            "https://example.test/a",
            "https://example.test/b",
            "-D",
            "files",
        ],
        vec![
            "xget",
            "https://example.test/a",
            "--tree",
            "s3://bucket/prefix",
            "-D",
            "files",
        ],
    ] {
        let cli = Cli::try_parse_from(arguments).expect("valid surface");
        assert!(
            cli.download.prepare().is_ok(),
            "typed input preparation succeeds without discovery"
        );
    }
}

#[test]
fn positional_operand_two_is_never_guessed_to_be_an_output() {
    let cli = Cli::try_parse_from(["xget", "https://example.test/a", "output.bin"])
        .expect("operands retain URL-list grammar");
    let failure = cli
        .download
        .prepare()
        .err()
        .expect("local output is not a reference");
    assert_eq!(failure.code(), 2);
    assert!(failure.message().contains("positional output"));
}

#[test]
fn file_and_directory_authority_are_mutually_exclusive() {
    assert!(
        Cli::try_parse_from(["xget", "https://example.test/a", "-o", "a", "-D", "root"]).is_err()
    );
}

#[test]
fn selectors_cannot_silently_apply_to_several_original_inputs() {
    for selector in ["--select", "--exclude", "--select-id"] {
        let cli = Cli::try_parse_from([
            "xget",
            "https://example.test/a",
            "https://example.test/b",
            selector,
            "a",
        ])
        .expect("scope is checked before discovery");
        assert_eq!(
            cli.download
                .prepare()
                .err()
                .expect("mixed selector rejected")
                .code(),
            2
        );
    }
}

#[test]
fn list_alias_info_and_resume_are_parser_commands_not_reference_heuristics() {
    assert!(matches!(
        Cli::try_parse_from(["xget", "ls", "https://example.test/a"])
            .unwrap()
            .command,
        Some(Command::List(_))
    ));
    assert!(matches!(
        Cli::try_parse_from(["xget", "info", "state"])
            .unwrap()
            .command,
        Some(Command::Info(_))
    ));
    assert!(matches!(
        Cli::try_parse_from([
            "xget",
            "resume",
            "state",
            "--input",
            "key=https://example.test/a"
        ])
        .unwrap()
        .command,
        Some(Command::Resume(_))
    ));
}

#[test]
fn unsupported_controls_and_removed_interfaces_are_not_successful_noops() {
    for flag in ["--sequential", "--chunks"] {
        assert!(Cli::try_parse_from(["xget", "https://example.test/a", flag]).is_err());
    }
    let removed = Cli::try_parse_from(["xget", "legacy", "https://example.test/a"]).unwrap();
    assert!(removed.download.prepare().is_err());
}

#[test]
fn catalogue_inspection_needs_no_choice_but_execution_and_live_preview_do() {
    let release = "https://github.com/fixture-owner/fixture-repo/releases/latest";
    let inspection = Cli::try_parse_from(["xget", "list", release]).unwrap();
    let Some(Command::List(command)) = inspection.command else {
        panic!("expected metadata command")
    };
    assert!(command.prepare().is_ok());
    for arguments in [
        vec!["xget", release],
        vec!["xget", release, "-o", "-"],
        vec!["xget", release, "-o", "-", "--dry-run"],
        vec!["xget", release, "--exclude", "*.sig"],
    ] {
        let failure = Cli::try_parse_from(arguments)
            .unwrap()
            .download
            .prepare()
            .err()
            .unwrap();
        assert_eq!(failure.code(), 2);
        assert!(failure.message().contains("explicit choice"));
    }
}

#[test]
fn all_is_one_original_input_intent_not_a_mixed_request_broadcast() {
    let release = "https://github.com/fixture-owner/fixture-repo/releases/latest";
    assert!(
        Cli::try_parse_from(["xget", release, "--all"])
            .unwrap()
            .download
            .prepare()
            .is_ok()
    );
    for arguments in [
        vec!["xget", "--all"],
        vec!["xget", release, "https://example.test/file", "--all"],
        vec!["xget", release, "--tree", "s3://bucket/prefix", "--all"],
        vec![
            "xget",
            "list",
            release,
            "https://example.test/file",
            "--all",
        ],
    ] {
        let parsed = Cli::try_parse_from(arguments).unwrap();
        let rejected = match parsed.command {
            Some(Command::List(command)) => command.prepare().is_err(),
            None => parsed.download.prepare().is_err(),
            _ => panic!("unexpected command"),
        };
        assert!(rejected);
    }
}

#[test]
fn all_keeps_literal_and_path_criteria_active_instead_of_ignoring_them() {
    let release = "https://github.com/fixture-owner/fixture-repo/releases/latest";
    for criteria in [
        vec!["--all", "--exclude", "*.sig"],
        vec!["--all", "--select", "tool-*"],
        vec!["--all", "--select-id", "17"],
        vec![
            "--select-id",
            "17",
            "--all",
            "--select",
            "tool-*",
            "--exclude",
            "*.sig",
        ],
        vec!["--select", "tool-*"],
        vec!["--select-id", "17"],
    ] {
        let arguments = [vec!["xget", release], criteria].concat();
        assert!(
            Cli::try_parse_from(arguments)
                .unwrap()
                .download
                .prepare()
                .is_ok()
        );
    }
    assert!(
        Cli::try_parse_from(["xget", release, "--all", "--select", "bad**pattern"])
            .unwrap()
            .download
            .prepare()
            .is_err()
    );
    assert!(
        Cli::try_parse_from(["xget", release, "--all", "--select-id", "not-an-asset-id"])
            .unwrap()
            .download
            .prepare()
            .is_err()
    );
    let inputs = super::Inputs {
        references: vec![release.parse::<super::Reference>().unwrap()],
        all: true,
        ..super::Inputs::default()
    }
    .prepare_execution()
    .unwrap();
    assert_eq!(inputs.len(), 1);
    assert_eq!(inputs[0].scope(), xget::input::InputScope::Catalogue);
    assert_eq!(inputs[0].kind(), xget::input::InputKind::GithubRelease);
}

#[test]
fn new_progress_policy_is_explicit_and_json_alias_conflicts_are_not_ignored() {
    for mode in ["auto", "bar", "plain", "json", "none"] {
        assert!(
            Cli::try_parse_from([
                "xget",
                "https://example.test/a",
                "--progress",
                mode,
                "--bits",
                "--raw-sizes"
            ])
            .is_ok()
        );
    }
    assert!(
        Cli::try_parse_from([
            "xget",
            "https://example.test/a",
            "--json",
            "--progress",
            "plain"
        ])
        .is_err()
    );
    assert!(Cli::try_parse_from(["xget", "https://example.test/a", "--json", "--bits"]).is_ok());
}

#[test]
fn checksum_remains_s_and_is_independent_of_a_full_required_digest() {
    let cli = Cli::try_parse_from([
        "xget",
        "https://example.test/a",
        "-s",
        "none",
        "--expect",
        "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
    ])
    .expect("independent measurement and expectation");
    assert!(cli.download.prepare().is_ok());
    assert!(
        Cli::try_parse_from(["xget", "https://example.test/a", "--expect", "sha256:abcd"]).is_err()
    );
    assert!(
        Cli::try_parse_from([
            "xget",
            "https://example.test/a",
            "--expect",
            "https://example.test/sums"
        ])
        .is_err()
    );
}

#[test]
fn help_teaches_the_default_download_and_explicit_authorities() {
    let help = Cli::command().render_long_help().to_string();
    let help = help.split_whitespace().collect::<Vec<_>>().join(" ");
    for spelling in [
        "Download files and inspect remote collections",
        "by default, downloads are saved in the current directory",
        "-o FILE to name one selected file",
        "-D DIR to choose a directory",
        "-o - for stdout",
        "pipefail",
        "list",
        "info",
        "resume",
        "Select exactly one file",
        "labelled multi-entry documents",
        "Native sidecars separately require --fresh",
        "one original input",
        "--all",
        "even for a singleton",
        "still filter",
        "never guesses a native asset",
        "--restart",
        "--overwrite",
        "No match is an error",
        "retaining old partials",
        "previews grant no replacement authority",
    ] {
        assert!(help.contains(spelling), "missing help contract: {spelling}");
    }
}
