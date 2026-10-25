//! Actual loopback HTTP fixtures; authored source, not execution evidence.
#![allow(clippy::expect_used)]

use core::cell::{Cell, RefCell};
use core::num::NonZeroUsize;
use core::sync::atomic::{AtomicU64, Ordering};
use core::task::Poll;
use core::time::Duration;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _, PermissionsExt as _, symlink};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use futures::StreamExt as _;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::oneshot;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{BodyError, BodyFailure, Client, Error, FinishError, VersionConsistency};
use crate::input::Input;
use crate::transfer::{Algorithm, Digest, Measurement, Provenance};

#[tokio::test]
async fn filtering_remote_http_ancestors_does_not_grant_them_local_filename_roles() {
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .and(path("/con/payload"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-length", "3")
                .insert_header("etag", "\"v1\""),
        )
        .mount(&server)
        .await;
    let reference = format!("{}/con/payload", server.uri());
    let root = Directory::new();
    let client = Client::new().expect("client");
    for mode in ["include", "exclude"] {
        let input = || {
            let input = Input::parse(&reference).expect("fixture reference");
            match mode {
                "include" => input.include("**").expect("include all"),
                _ => input.exclude("*.tmp").expect("unrelated exclusion"),
            }
        };
        let inventory = client
            .resolve(input())
            .await
            .expect("remote ancestors are not outputs");
        assert_eq!(inventory.files().len(), 1);
        assert_eq!(
            inventory.files().next().expect("member").source_path(),
            "con/payload"
        );
        let _plan = client
            .download(input())
            .to_file(root.path().join("safe.bin"))
            .plan()
            .await
            .expect("safe explicit assignment");
        let invalid = client
            .download(input())
            .to_file(root.path().join("con"))
            .plan()
            .await;
        assert!(matches!(invalid, Err(Error::InvalidName)));
    }
    assert_eq!(get_count(&server).await, 0);
}

const ABC_SHA256: &str = "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

async fn exact(server: &MockServer, source_path: &str) -> String {
    Mock::given(method("HEAD"))
        .and(path(source_path))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-length", "3")
                .insert_header("etag", "\"v1\""),
        )
        .expect(1)
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path(source_path))
        .and(header("if-match", "\"v1\""))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("etag", "\"v1\"")
                .set_body_bytes(b"abc"),
        )
        .expect(1)
        .mount(server)
        .await;
    format!("{}{source_path}", server.uri())
}

async fn get_count(server: &MockServer) -> usize {
    server
        .received_requests()
        .await
        .expect("request recording")
        .iter()
        .filter(|request| request.method.as_str() == "GET")
        .count()
}

#[tokio::test]
async fn construction_and_planning_do_not_spend_a_payload_get_or_create_outputs() {
    let server = MockServer::start().await;
    let client = Client::new().expect("client construction");
    assert!(
        server
            .received_requests()
            .await
            .expect("requests")
            .is_empty()
    );
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(200).insert_header("content-length", "3"))
        .expect(1)
        .mount(&server)
        .await;
    let reference = format!("{}/payload", server.uri());
    let directory = Directory::new();
    let destination = directory.path().join("not-created/planned-only.bin");
    let plan = client
        .download(reference.as_str())
        .to_file(&destination)
        .plan()
        .await
        .expect("plan");
    assert_eq!(plan.files().len(), 1);
    assert!(!destination.exists());
    assert_eq!(
        fs::read_dir(directory.path())
            .expect("empty planning fixture")
            .count(),
        0
    );
    assert_eq!(get_count(&server).await, 0);
}

#[tokio::test]
async fn resolve_all_keeps_duplicate_original_occurrences_and_selected_files_need_no_new_heads() {
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-length", "3")
                .insert_header("etag", "\"v1\""),
        )
        .expect(2)
        .mount(&server)
        .await;
    let reference = format!("{}/object", server.uri());
    let client = Client::new().expect("client");
    let inventory = client
        .resolve_all([reference.as_str(), reference.as_str()])
        .await
        .expect("complete multi input");
    assert_eq!(inventory.files().len(), 2);
    let mut files = inventory.into_files();
    let first = files.next().expect("first occurrence");
    let second = files.next().expect("second occurrence");
    assert_ne!(first.id(), second.id());
    assert!(first.input.occurrence != second.input.occurrence);
    let selected = client
        .resolve_all([first, second])
        .await
        .expect("frozen inputs");
    assert_eq!(selected.files().len(), 2);
    assert_eq!(get_count(&server).await, 0);
}

#[tokio::test]
async fn resolve_all_rejects_empty_or_overbudget_original_input_sets_before_discovery() {
    let client = Client::new().expect("client");
    assert!(matches!(
        client.resolve_all(core::iter::empty::<Input>()).await,
        Err(Error::NoInputs)
    ));
    let inputs =
        (0..129).map(|_| Input::parse("https://example.invalid/never-requested").expect("intent"));
    assert!(matches!(
        client.resolve_all(inputs).await,
        Err(Error::Inventory(_))
    ));
}

#[test]
fn missing_runtime_is_typed_before_http_or_native_io() {
    let client = Client::new().expect("construction needs no runtime");
    let result = futures::executor::block_on(
        client
            .download("https://example.test/object")
            .to_file("unused.bin")
            .plan(),
    );
    assert!(matches!(result, Err(Error::RuntimeUnavailable)));
}

#[test]
fn oversized_request_capacity_is_a_typed_error_not_a_semaphore_panic() {
    let maximum = NonZeroUsize::new(usize::MAX).expect("nonzero capacity");
    assert!(matches!(
        Client::builder().max_requests(maximum).build(),
        Err(Error::AdmissionLimit)
    ));
}

#[tokio::test]
async fn no_inputs_is_not_a_genuine_complete_empty_inventory() {
    let client = Client::new().expect("client");
    let result = client
        .download_all(core::iter::empty::<Input>())
        .plan()
        .await;
    assert!(matches!(result, Err(Error::NoInputs)));
}

#[tokio::test]
async fn unsupported_providers_and_trees_fail_without_fake_inventory_success() {
    let client = Client::new().expect("client");
    for input in [
        #[cfg(not(feature = "swisstransfer"))]
        Input::parse("https://swisstransfer.com/dl/aaaaaaaa-1111-2222-3333-444444444444")
            .expect("share intent"),
        #[cfg(not(feature = "github"))]
        Input::parse("https://github.com/owner/project/releases/latest").expect("catalogue intent"),
        #[cfg(not(feature = "webdav"))]
        Input::tree("https://example.test/dav/").expect("tree intent"),
    ] {
        assert!(matches!(
            client.resolve::<Input>(input).await,
            Err(Error::UnsupportedInput { .. })
        ));
    }
}

#[tokio::test]
async fn missing_configured_s3_is_actionable_for_exact_and_explicit_tree() {
    let client = Client::new().expect("client");
    for input in [
        Input::parse("s3://bucket/key").expect("exact"),
        Input::tree("s3://bucket/prefix/").expect("tree"),
    ] {
        assert!(matches!(
            client.resolve(input).await,
            Err(Error::S3Unavailable)
        ));
    }
}

#[tokio::test]
async fn selected_file_planning_preserves_frozen_metadata_without_another_head() {
    let server = MockServer::start().await;
    let client = Client::new().expect("client");
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(200).insert_header("etag", "\"version\""))
        .expect(1)
        .mount(&server)
        .await;
    let reference = format!("{}/original-name", server.uri());
    let inventory = client.resolve(reference.as_str()).await.expect("inventory");
    let selected = inventory
        .select_one(|file| file.source_path() == "original-name")
        .expect("selected member");
    let id = crate::inventory::FileId::clone(selected.id());
    let plan = client
        .download(selected)
        .to_file("explicit-name.bin")
        .plan()
        .await
        .expect("frozen plan");
    let entry = plan.files().next().expect("one assignment");
    assert_eq!(entry.file().id(), &id);
    assert_eq!(entry.file().source_path(), "original-name");
    assert_eq!(
        entry.destination(),
        std::path::Path::new("explicit-name.bin")
    );
    assert_eq!(get_count(&server).await, 0);
}

#[tokio::test]
async fn namespace_and_global_expectation_scope_conflicts_fail_before_get() {
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(200))
        .expect(3)
        .mount(&server)
        .await;
    let client = Client::new().expect("client");
    let first = format!("{}/one", server.uri());
    let second = format!("{}/two", server.uri());
    let grouped = Input::parse(&first)
        .expect("reference")
        .under("group")
        .expect("namespace");
    assert!(matches!(
        client.download(grouped).to_file("one.bin").plan().await,
        Err(Error::NamespaceWithFile)
    ));
    let required: Digest = ABC_SHA256.parse().expect("digest");
    assert!(matches!(
        client
            .download_all([first.as_str(), second.as_str()])
            .expect(required)
            .to_directory("unused")
            .plan()
            .await,
        Err(Error::GlobalRequirementCardinality)
    ));
    assert_eq!(get_count(&server).await, 0);
}

#[tokio::test]
async fn conflicting_pending_requirements_stay_sticky_before_metadata_requests() {
    let server = MockServer::start().await;
    let client = Client::new().expect("client");
    let reference = format!("{}/payload", server.uri());
    let first: Digest = ABC_SHA256.parse().expect("digest");
    let other = Digest::parse(Algorithm::Sha256, &"00".repeat(32)).expect("different digest");
    let result = client
        .download(reference.as_str())
        .expect(first)
        .require(other, Provenance::Sidecar)
        .require(first, Provenance::Source)
        .measurement(Measurement::None)
        .to_file("unused.bin")
        .plan()
        .await;
    assert!(matches!(result, Err(Error::ConflictingRequirements(_))));
    assert!(
        server
            .received_requests()
            .await
            .expect("recording")
            .is_empty()
    );
}

#[tokio::test]
async fn exact_file_target_checks_actual_selected_count_before_any_get() {
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(200))
        .expect(2)
        .mount(&server)
        .await;
    let client = Client::new().expect("client");
    let first = format!("{}/one", server.uri());
    let second = format!("{}/two", server.uri());
    let result = client
        .download_all([first.as_str(), second.as_str()])
        .to_file("unused.bin")
        .plan()
        .await;
    assert!(matches!(result, Err(Error::FileCardinality)));
    assert_eq!(get_count(&server).await, 0);
}

#[tokio::test]
async fn all_file_case_unicode_and_file_directory_collisions_precede_first_get() {
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let client = Client::new().expect("client");
    for (first, second) in [
        ("/first/same", "/second/same"),
        ("/A", "/a"),
        ("/%C3%A9", "/e%CC%81"),
        ("/Stra%C3%9Fe", "/STRASSE"),
    ] {
        let first = format!("{}{first}", server.uri());
        let second = format!("{}{second}", server.uri());
        assert!(matches!(
            client
                .download_all([first.as_str(), second.as_str()])
                .to_directory("not-created-by-planning")
                .plan()
                .await,
            Err(Error::LayoutConflict)
        ));
    }
    let direct = Input::parse(&format!("{}/same", server.uri())).expect("direct");
    let nested = Input::parse(&format!("{}/child", server.uri()))
        .expect("nested")
        .under("same")
        .expect("namespace");
    assert!(matches!(
        client
            .download_all([direct, nested])
            .to_directory("unused")
            .plan()
            .await,
        Err(Error::LayoutConflict)
    ));
    assert_eq!(get_count(&server).await, 0);
}

#[tokio::test]
async fn equivalent_directory_spellings_cannot_silently_merge_namespace_groups() {
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let client = Client::new().expect("client");
    for (first_group, second_group) in [("Group", "group"), ("é", "e\u{301}")] {
        let first = Input::parse(&format!("{}/first", server.uri()))
            .expect("first")
            .under(first_group)
            .expect("first namespace");
        let second = Input::parse(&format!("{}/second", server.uri()))
            .expect("second")
            .under(second_group)
            .expect("second namespace");
        assert!(matches!(
            client
                .download_all([first, second])
                .to_directory("unused")
                .plan()
                .await,
            Err(Error::LayoutConflict)
        ));
    }
    // Explicitly identical namespace spellings may contain distinct exact files.
    let first = Input::parse(&format!("{}/first", server.uri()))
        .expect("first")
        .under("shared")
        .expect("namespace");
    let second = Input::parse(&format!("{}/second", server.uri()))
        .expect("second")
        .under("shared")
        .expect("namespace");
    assert_eq!(
        client
            .download_all([first, second])
            .to_directory("unused")
            .plan()
            .await
            .expect("shared explicit namespace")
            .files()
            .len(),
        2
    );
    assert_eq!(get_count(&server).await, 0);
}

#[tokio::test]
async fn cumulative_metadata_limit_stops_discovery_before_more_head_requests() {
    let server = MockServer::start().await;
    let etag = format!("\"{}\"", "v".repeat(8190));
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(200).insert_header("etag", etag))
        .mount(&server)
        .await;
    let references: Vec<_> = (0..64)
        .map(|index| format!("{}/{index}/{}/payload", server.uri(), "x".repeat(3000)))
        .collect();
    let client = Client::new().expect("client");
    assert!(matches!(
        client
            .download_all(references)
            .to_directory("unused")
            .plan()
            .await,
        Err(Error::Inventory(_))
    ));
    let requests = server.received_requests().await.expect("request recording");
    assert!(!requests.is_empty());
    assert!(
        requests.len() < 64,
        "metadata budget must stop discovery before all inputs"
    );
    assert_eq!(get_count(&server).await, 0);
}

#[tokio::test]
async fn unsafe_hints_fail_assignment_but_explicit_safe_output_names_win() {
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let client = Client::new().expect("client");
    for suffix in [
        "unsafe%2Fname",
        "%2Exget-native.lock",
        "payload.xget",
        "C%3Adrive",
        "..%5Cescape",
    ] {
        let reference = format!("{}/{suffix}", server.uri());
        assert!(matches!(
            client
                .download(reference.as_str())
                .to_directory("unused")
                .plan()
                .await,
            Err(Error::InvalidName)
        ));
        let plan = client
            .download(reference.as_str())
            .to_file("explicit-safe.bin")
            .plan()
            .await
            .expect("explicit name wins");
        assert_eq!(plan.files().len(), 1);
    }
    assert_eq!(get_count(&server).await, 0);
}

#[tokio::test]
async fn a_stale_strong_version_is_refused_before_body_or_native_staging() {
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(200).insert_header("etag", "\"old\""))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(header("if-match", "\"old\""))
        .respond_with(ResponseTemplate::new(412))
        .expect(1)
        .mount(&server)
        .await;
    let client = Client::new().expect("client");
    let reference = format!("{}/payload", server.uri());
    assert!(matches!(
        client.open(reference.as_str()).await,
        Err(Error::VersionChanged)
    ));
}

#[tokio::test]
async fn an_ignored_conditional_request_cannot_silently_drop_or_change_the_frozen_version() {
    for returned_etag in [None, Some("\"new\"")] {
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .respond_with(ResponseTemplate::new(200).insert_header("etag", "\"old\""))
            .expect(1)
            .mount(&server)
            .await;
        let mut response = ResponseTemplate::new(200).set_body_bytes(b"abc");
        if let Some(etag) = returned_etag {
            response = response.insert_header("etag", etag);
        }
        Mock::given(method("GET"))
            .and(header("if-match", "\"old\""))
            .respond_with(response)
            .expect(1)
            .mount(&server)
            .await;
        let client = Client::new().expect("client");
        let reference = format!("{}/payload", server.uri());
        assert!(matches!(
            client.open(reference.as_str()).await,
            Err(Error::VersionChanged)
        ));
    }
}

#[tokio::test]
async fn redirects_and_content_transforms_are_typed_limitations_not_followed_authority() {
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .and(path("/redirect"))
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("location", "https://other.test/bearer-reference"),
        )
        .mount(&server)
        .await;
    Mock::given(method("HEAD"))
        .and(path("/encoded"))
        .respond_with(ResponseTemplate::new(200).insert_header("content-encoding", "gzip"))
        .mount(&server)
        .await;
    let client = Client::new().expect("client");
    assert!(matches!(
        client.resolve(format!("{}/redirect", server.uri())).await,
        Err(Error::Redirect)
    ));
    assert!(matches!(
        client.resolve(format!("{}/encoded", server.uri())).await,
        Err(Error::ContentEncoding)
    ));
    assert_eq!(get_count(&server).await, 0);
}

#[tokio::test]
async fn cap_one_clones_share_body_admission_and_drop_releases_it_without_deadlock() {
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"abc"))
        .mount(&server)
        .await;
    let client = Client::builder()
        .max_requests(NonZeroUsize::MIN)
        .build()
        .expect("cap one");
    let sibling = Client::clone(&client);
    let reference = format!("{}/payload", server.uri());
    let opened = client
        .open(reference.as_str())
        .await
        .expect("first consumed GET owner");
    let mut waiting = Box::pin(sibling.open(reference.as_str()));
    assert!(matches!(futures::poll!(waiting.as_mut()), Poll::Pending));
    assert_eq!(get_count(&server).await, 1);
    drop(opened);
    let second = tokio::time::timeout(Duration::from_secs(2), waiting)
        .await
        .expect("permit released")
        .expect("second open");
    let mut body = second.into_body();
    while let Some(chunk) = body.next().await {
        chunk.expect("body bytes");
    }
    // Retaining the completed body does not retain its admission permit.
    let third = tokio::time::timeout(Duration::from_secs(2), client.open(reference.as_str()))
        .await
        .expect("EOF released permit")
        .expect("third open");
    drop(third);
    assert_eq!(
        body.finish()
            .expect("streamed receipt")
            .body()
            .progress()
            .released
            .0,
        3
    );
}

#[tokio::test]
async fn live_completion_separates_streaming_checks_from_publication_and_rejects_early_finish() {
    let server = MockServer::start().await;
    let reference = exact(&server, "/payload").await;
    let client = Client::new().expect("client");
    let mut body = client
        .open(reference.as_str())
        .await
        .expect("opened")
        .expect(ABC_SHA256.parse::<Digest>().expect("digest"))
        .expect("required check")
        .require(
            ABC_SHA256.parse::<Digest>().expect("digest"),
            Provenance::Sidecar,
        )
        .expect("agreeing sidecar check")
        .measurement(Measurement::None)
        .into_body();
    let mut received = Vec::new();
    while let Some(chunk) = body.next().await {
        received.extend_from_slice(&chunk.expect("chunk"));
    }
    assert_eq!(received, b"abc");
    let receipt = body.finish().expect("clean EOF");
    let matched = receipt
        .body()
        .checks()
        .matched_requirements()
        .collect::<Vec<_>>();
    assert_eq!(matched.len(), 1);
    assert_eq!(
        matched[0].origins.iter().collect::<Vec<_>>(),
        [Provenance::Caller, Provenance::Sidecar]
    );
    assert_eq!(receipt.body().checks().measured(), None);
    assert_eq!(receipt.version(), VersionConsistency::StrongEtagMatched);

    let server = MockServer::start().await;
    let reference = exact(&server, "/payload").await;
    let body = client
        .open(reference.as_str())
        .await
        .expect("opened")
        .into_body();
    assert!(matches!(body.finish(), Err(FinishError::Incomplete(_))));
}

#[tokio::test]
async fn required_live_mismatch_arrives_before_clean_eof_even_with_measurement_none() {
    let server = MockServer::start().await;
    let reference = exact(&server, "/payload").await;
    let client = Client::new().expect("client");
    let wrong = Digest::parse(Algorithm::Sha256, &"00".repeat(32)).expect("wrong digest");
    let mut body = client
        .open(reference.as_str())
        .await
        .expect("opened")
        .require(wrong, Provenance::Sidecar)
        .expect("requirement")
        .measurement(Measurement::None)
        .into_body();
    let mut mismatch = false;
    while let Some(chunk) = body.next().await {
        match chunk {
            Ok(_) => {}
            Err(BodyFailure {
                error: BodyError::Mismatch(_),
                ..
            }) => {
                mismatch = true;
                break;
            }
            Err(error) => panic!("unexpected typed error: {error}"),
        }
    }
    assert!(mismatch);
    assert!(matches!(body.finish(), Err(FinishError::Failed(_))));
}

#[cfg(unix)]
#[tokio::test]
async fn exact_http_publishes_expected_bytes_once_with_required_and_measured_facts_separate() {
    let directory = Directory::new();
    let server = MockServer::start().await;
    let reference = exact(&server, "/payload").await;
    let client = Client::new().expect("client");
    let destination = directory.path().join("explicit.bin");
    let report = client
        .download(reference.as_str())
        .expect(ABC_SHA256.parse::<Digest>().expect("digest"))
        .require(
            ABC_SHA256.parse::<Digest>().expect("digest"),
            Provenance::Sidecar,
        )
        .measurement(Measurement::None)
        .to_file(&destination)
        .run()
        .await
        .expect("native publication");
    assert_eq!(fs::read(destination).expect("published payload"), b"abc");
    let published = report.completed().next().expect("one publication");
    assert_eq!(published.receipt().checks().measured(), None);
    assert_eq!(
        published.receipt().checks().matched_requirements().count(),
        1
    );
    let matched = published
        .receipt()
        .checks()
        .matched_requirements()
        .next()
        .expect("matched digest");
    assert_eq!(
        matched.origins.iter().collect::<Vec<_>>(),
        [Provenance::Caller, Provenance::Sidecar]
    );
    assert_eq!(published.version(), VersionConsistency::StrongEtagMatched);
    assert!(published.receipt().state().path().exists());
    assert_eq!(get_count(&server).await, 1);
}

#[cfg(unix)]
#[tokio::test]
async fn a_zero_byte_http_object_is_one_real_published_file_not_an_empty_inventory() {
    let directory = Directory::new();
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-length", "0")
                .insert_header("etag", "\"zero\""),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(header("if-match", "\"zero\""))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("etag", "\"zero\"")
                .set_body_bytes(b""),
        )
        .expect(1)
        .mount(&server)
        .await;
    let client = Client::new().expect("client");
    let reference = format!("{}/empty", server.uri());
    let destination = directory.path().join("empty");
    let report = client
        .download(reference.as_str())
        .to_file(&destination)
        .run()
        .await
        .expect("zero-byte publication");
    assert_eq!(report.completed().len(), 1);
    assert_eq!(fs::metadata(destination).expect("published file").len(), 0);
}

#[cfg(unix)]
#[tokio::test]
async fn ordinary_no_measurement_publication_does_not_invent_hash_or_recovery_evidence() {
    let directory = Directory::new();
    let server = MockServer::start().await;
    let reference = exact(&server, "/payload").await;
    let client = Client::new().expect("client");
    let report = client
        .download(reference.as_str())
        .measurement(Measurement::None)
        .to_file(directory.path().join("payload"))
        .run()
        .await
        .expect("ordinary native publication");
    let receipt = report
        .completed()
        .next()
        .expect("one publication")
        .receipt();
    assert_eq!(receipt.partial().hashed.0, 0);
    assert_eq!(receipt.checks().measured(), None);
    assert_eq!(receipt.checks().matched_requirements().count(), 0);
    assert!(matches!(
        receipt
            .state()
            .reconcile()
            .await
            .expect("owned reconciliation"),
        crate::transfer::Reconciliation::NotRevalidatable(_)
    ));
}

#[cfg(unix)]
#[tokio::test]
async fn multiple_inputs_keep_namespaces_and_prior_publications_when_a_later_file_fails() {
    let directory = Directory::new();
    let server = MockServer::start().await;
    let first = exact(&server, "/first/same").await;
    Mock::given(method("HEAD"))
        .and(path("/second/same"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/second/same"))
        .respond_with(ResponseTemplate::new(403))
        .expect(1)
        .mount(&server)
        .await;
    let first = Input::parse(&first)
        .expect("first")
        .under("first")
        .expect("namespace");
    let second = Input::parse(&format!("{}/second/same", server.uri()))
        .expect("second")
        .under("second")
        .expect("namespace");
    let client = Client::new().expect("client");
    let output = directory.path().join("outputs");
    let plan = client
        .download_all([first, second])
        .to_directory(&output)
        .plan()
        .await
        .expect("all assignments");
    assert!(!output.exists());
    assert_eq!(get_count(&server).await, 0);
    let failure = client.execute(plan).await.expect_err("second denied");
    assert!(matches!(failure.error, Error::Status { status: 403, .. }));
    assert_eq!(failure.report.completed().len(), 1);
    assert_eq!(
        fs::read(output.join("first/same")).expect("first retained"),
        b"abc"
    );
    assert_eq!(
        fs::metadata(&output).expect("new root metadata").mode() & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(output.join("first"))
            .expect("new namespace metadata")
            .mode()
            & 0o777,
        0o700
    );
    assert!(!output.join("second/same").exists());
    assert_eq!(
        failure
            .failed
            .as_ref()
            .expect("failed assignment")
            .file()
            .namespace()
            .expect("namespace")
            .as_str(),
        "second"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn existing_root_child_symlink_and_shared_directory_roles_fail_before_payload_get() {
    let directory = Directory::new();
    let outside = Directory::new();
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let reference = format!("{}/payload", server.uri());
    let client = Client::new().expect("client");

    let shared = directory.path().join("shared");
    fs::create_dir(&shared).expect("fixture shared root");
    fs::set_permissions(&shared, fs::Permissions::from_mode(0o770)).expect("shared fixture mode");
    let input = Input::parse(&reference)
        .expect("reference")
        .under("private/child")
        .expect("namespace");
    let error = client
        .download(input)
        .to_directory(&shared)
        .run()
        .await
        .expect_err("shared root refused");
    assert!(matches!(error.error, Error::UnsafeDirectory));
    assert!(!shared.join("private").exists());

    let root = directory.path().join("root");
    fs::DirBuilder::new()
        .mode(0o700)
        .create(&root)
        .expect("private root");
    symlink(outside.path(), root.join("link")).expect("fixture link");
    let input = Input::parse(&reference)
        .expect("reference")
        .under("link/nested")
        .expect("namespace");
    let error = client
        .download(input)
        .to_directory(&root)
        .run()
        .await
        .expect_err("symlink child refused");
    assert!(matches!(error.error, Error::UnsafeDirectory));
    assert_eq!(
        fs::read_dir(outside.path())
            .expect("outside fixture untouched")
            .count(),
        0
    );

    let child = root.join("shared-child");
    fs::create_dir(&child).expect("child");
    fs::set_permissions(&child, fs::Permissions::from_mode(0o772)).expect("shared child mode");
    let input = Input::parse(&reference)
        .expect("reference")
        .under("shared-child/nested")
        .expect("namespace");
    let error = client
        .download(input)
        .to_directory(&root)
        .run()
        .await
        .expect_err("shared child refused");
    assert!(matches!(error.error, Error::UnsafeDirectory));
    assert!(!child.join("nested").exists());
    assert_eq!(get_count(&server).await, 0);
}

#[tokio::test]
async fn layout_depth_limits_fail_without_retaining_unbounded_prefix_keys_or_fetching() {
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let client = Client::new().expect("client");
    let reference = format!("{}/payload", server.uri());
    let namespace = core::iter::repeat_n("part", 64)
        .collect::<Vec<_>>()
        .join("/");
    let input = Input::parse(&reference)
        .expect("reference")
        .under(&namespace)
        .expect("lexical namespace");
    assert!(matches!(
        client.download(input).to_directory("unused").plan().await,
        Err(Error::LayoutLimit)
    ));
    assert_eq!(get_count(&server).await, 0);
}

#[cfg(unix)]
#[tokio::test]
async fn native_digest_failure_preserves_partial_state_and_an_authorized_incumbent() {
    let directory = Directory::new();
    let server = MockServer::start().await;
    let reference = exact(&server, "/payload").await;
    let destination = directory.path().join("sentinel");
    fs::write(&destination, b"sentinel").expect("sentinel");
    let wrong = Digest::parse(Algorithm::Sha256, &"00".repeat(32)).expect("wrong digest");
    let client = Client::new().expect("client");
    let failure = client
        .download(reference.as_str())
        .overwrite(crate::client::Overwrite)
        .expect(wrong)
        .to_file(&destination)
        .run()
        .await
        .expect_err("mismatch");
    let Error::Transfer(native) = &failure.error else {
        panic!("expected native failure")
    };
    assert!(matches!(
        native.kind,
        crate::transfer::FailureKind::DigestMismatch(_)
    ));
    assert!(
        native
            .state
            .as_ref()
            .expect("retained state")
            .path()
            .exists()
    );
    assert_eq!(failure.report.completed().len(), 0);
    assert_eq!(
        fs::read(destination).expect("sentinel retained"),
        b"sentinel"
    );
}

#[tokio::test]
async fn missing_head_support_does_not_spend_a_get_during_planning() {
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(405))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"abc"))
        .expect(1)
        .mount(&server)
        .await;
    let client = Client::new().expect("client");
    let reference = format!("{}/payload", server.uri());
    let file = client
        .resolve(reference.as_str())
        .await
        .expect("exact unpinned metadata")
        .select_one(|_| true)
        .expect("one");
    assert_eq!(get_count(&server).await, 0);
    let mut body = client.open(file).await.expect("one GET").into_body();
    while let Some(chunk) = body.next().await {
        chunk.expect("payload");
    }
    assert_eq!(
        body.finish().expect("EOF").version(),
        VersionConsistency::Unpinned
    );
}

#[tokio::test]
async fn credential_markers_never_enter_default_diagnostics_or_builtin_source_chains() {
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(403))
        .mount(&server)
        .await;
    let authority = server
        .uri()
        .strip_prefix("http://")
        .expect("loopback authority")
        .to_owned();
    let reference =
        format!("http://marker-user:marker-password@{authority}/marker-path?token=marker-query");
    let client = Client::new().expect("client");
    let pending = client
        .download(reference.as_str())
        .to_file("explicit-safe.bin");
    safe(&format!("{pending:?}"));
    let plan = pending.plan().await.expect("metadata");
    safe(&format!("{plan:#?}"));
    safe(&format!("{:?}", plan.files().next().expect("one")));
    let error = client.open(reference.as_str()).await.expect_err("denied");
    safe_chain(&error);
    // A malformed response exercises a real reqwest cause without a released-port reuse race.
    let endpoint = Endpoint::malformed().await;
    let error = client
        .resolve(endpoint.reference.as_str())
        .await
        .expect_err("invalid response protocol");
    assert!(matches!(error, Error::Http(_)));
    safe_chain(&error);
    endpoint.settle().await;
}

fn safe(rendering: &str) {
    for marker in [
        "marker-user",
        "marker-password",
        "marker-path",
        "marker-query",
    ] {
        assert!(!rendering.contains(marker));
    }
}

fn safe_chain(error: &(dyn core::error::Error + 'static)) {
    let mut current = Some(error);
    while let Some(error) = current {
        safe(&format!("{error}"));
        safe(&format!("{error:?}"));
        current = error.source();
    }
}

// Owned loopback tasks are aborted if an assertion unwinds before explicit settlement. Accepts and
// reads are bounded, so a production regression cannot strand an unbounded blocking fixture thread.
struct Endpoint {
    reference: String,
    close: Option<oneshot::Sender<()>>,
    worker: Option<tokio::task::JoinHandle<()>>,
}

impl Endpoint {
    async fn late() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("loopback listener");
        let address = listener.local_addr().expect("address");
        let (close, closed) = oneshot::channel();
        let worker = tokio::spawn(async move {
            let (mut head, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
                .await
                .expect("HEAD accept deadline")
                .expect("HEAD connection");
            Self::request(&mut head).await;
            head.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nETag: \"v1\"\r\nConnection: close\r\n\r\n").await.expect("HEAD response");
            drop(head);
            let (mut get, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
                .await
                .expect("GET accept deadline")
                .expect("GET connection");
            Self::request(&mut get).await;
            get.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nETag: \"v1\"\r\nConnection: close\r\n\r\n3\r\nabc\r\n").await.expect("valid prefix");
            let _closed = closed.await;
            // Missing terminal zero chunk remains a body error even after the valid prefix.
        });
        Self {
            reference: format!("http://{address}/payload"),
            close: Some(close),
            worker: Some(worker),
        }
    }

    async fn malformed() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("loopback listener");
        let address = listener.local_addr().expect("address");
        let worker = tokio::spawn(async move {
            let (mut stream, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
                .await
                .expect("malformed fixture accept deadline")
                .expect("fixture connection");
            Self::request(&mut stream).await;
            stream
                .write_all(b"not-an-http-response\r\n\r\n")
                .await
                .expect("invalid protocol response");
        });
        Self {
            reference: format!(
                "http://marker-user:marker-password@{address}/marker-path?token=marker-query"
            ),
            close: None,
            worker: Some(worker),
        }
    }

    async fn request(stream: &mut tokio::net::TcpStream) {
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut bytes = Vec::new();
            let mut byte = [0];
            while !bytes.ends_with(b"\r\n\r\n") {
                assert!(bytes.len() < 16 * 1024, "fixture request bounded");
                stream
                    .read_exact(&mut byte)
                    .await
                    .expect("request header byte");
                bytes.push(byte[0]);
            }
        })
        .await
        .expect("fixture request deadline");
    }

    fn release(&mut self) {
        self.close
            .take()
            .expect("late fixture close handle")
            .send(())
            .expect("release late close");
    }

    async fn settle(mut self) {
        self.worker
            .take()
            .expect("fixture task")
            .await
            .expect("fixture settled");
    }
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            worker.abort();
        }
    }
}

#[tokio::test]
async fn late_http_failure_follows_a_consumed_prefix_and_releases_admission() {
    let mut endpoint = Endpoint::late().await;
    let client = Client::builder()
        .max_requests(NonZeroUsize::MIN)
        .build()
        .expect("client");
    let mut body = client
        .open(endpoint.reference.as_str())
        .await
        .expect("opened")
        .into_body();
    assert_eq!(
        body.next().await.expect("prefix").expect("valid bytes"),
        b"abc"[..]
    );
    endpoint.release();
    let failure = body
        .next()
        .await
        .expect("terminal body failure")
        .expect_err("late source error");
    assert!(matches!(failure.error, BodyError::Source(_)));
    assert_eq!(failure.progress.received.0, 3);
    endpoint.settle().await;
    let server = MockServer::start().await;
    let reference = exact(&server, "/payload").await;
    let opened = tokio::time::timeout(Duration::from_secs(2), client.open(reference.as_str()))
        .await
        .expect("failed body released permit while retained")
        .expect("new open");
    drop(opened);
    assert!(matches!(body.finish(), Err(FinishError::Failed(_))));
}

#[cfg(unix)]
#[tokio::test]
async fn native_late_body_error_retains_owned_state_and_never_publishes() {
    let directory = Directory::new();
    let mut endpoint = Endpoint::late().await;
    // The fixture's valid chunk is followed by truncated chunk framing, even though its payload
    // length agrees with the strong-version-bound HEAD. EOF is still required by the native engine.
    endpoint.release();
    let client = Client::new().expect("client");
    let destination = directory.path().join("payload");
    let failure = client
        .download(endpoint.reference.as_str())
        .to_file(&destination)
        .run()
        .await
        .expect_err("late body failure");
    let Error::Transfer(native) = &failure.error else {
        panic!("expected actual native failure")
    };
    assert!(matches!(native.kind, crate::transfer::FailureKind::Body(_)));
    assert!(
        native
            .state
            .as_ref()
            .expect("retained state")
            .path()
            .exists()
    );
    assert!(!destination.exists());
    assert_eq!(failure.report.completed().len(), 0);
    endpoint.settle().await;
}

#[tokio::test]
async fn resolve_and_planning_share_original_path_filtering_and_keep_selected_metadata() {
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .and(path("/nested/a.bin"))
        .respond_with(ResponseTemplate::new(200).insert_header("etag", "\"frozen\""))
        .expect(2)
        .mount(&server)
        .await;
    let reference = format!("{}/nested/a.bin", server.uri());
    let client = Client::new().expect("client");
    let input = Input::parse(&reference)
        .expect("input")
        .under("output-group")
        .expect("namespace")
        .include("nested/?.bin")
        .expect("include")
        .exclude("output-group/**")
        .expect("exclude uses original path");
    let inventory = client.resolve(input).await.expect("filtered inventory");
    let file = inventory.into_files().next().expect("one member");
    assert_eq!(file.source_path(), "nested/a.bin");
    let id = crate::inventory::FileId::clone(file.id());
    let plan = client
        .download(file)
        .to_directory("uncreated-root")
        .plan()
        .await
        .expect("selected metadata without another HEAD");
    let planned = plan.files().next().expect("one planned member");
    assert_eq!(planned.file().id(), &id);
    assert_eq!(
        planned.file().version(),
        crate::inventory::VersionEvidence::StrongEtag
    );
    assert_eq!(
        planned.destination(),
        Path::new("uncreated-root/output-group/a.bin")
    );
    let plan = client
        .download(reference.as_str())
        .include("nested/*.bin")
        .exclude("*.bin")
        .to_file("uncreated.bin")
        .plan()
        .await
        .expect("same matching seam");
    assert_eq!(plan.files().len(), 1);
    assert_eq!(get_count(&server).await, 0);
}

#[tokio::test]
async fn nonmatching_paths_are_typed_and_pattern_errors_and_global_scope_remain_sticky() {
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(200))
        .expect(2)
        .mount(&server)
        .await;
    let reference = format!("{}/payload", server.uri());
    let client = Client::new().expect("client");
    let input = Input::parse(&reference)
        .expect("input")
        .include("other")
        .expect("include");
    assert!(matches!(
        client.resolve(input).await,
        Err(Error::PathSelection)
    ));
    assert!(matches!(
        client
            .download(reference.as_str())
            .include("**")
            .exclude("payload")
            .plan()
            .await,
        Err(Error::PathSelection)
    ));
    assert!(matches!(
        client
            .download(reference.as_str())
            .include("**bad")
            .include("**")
            .plan()
            .await,
        Err(Error::Input(crate::input::InputError::Pattern(
            crate::input::PatternError::Syntax
        )))
    ));
    assert!(matches!(
        client
            .download_all([reference.as_str(), reference.as_str()])
            .exclude("*.tmp")
            .include("**")
            .plan()
            .await,
        Err(Error::SelectionScope)
    ));
    assert_eq!(get_count(&server).await, 0);
}

#[tokio::test]
async fn mixed_exact_inputs_keep_local_patterns_instead_of_a_global_selector() {
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(200))
        .expect(4)
        .mount(&server)
        .await;
    let first = format!("{}/one.bin", server.uri());
    let second = format!("{}/two.txt", server.uri());
    let client = Client::new().expect("client");
    let plan = client
        .download_all([
            Input::parse(&first)
                .expect("first")
                .include("*.bin")
                .expect("first intent"),
            Input::parse(&second)
                .expect("second")
                .exclude("*.bin")
                .expect("second intent"),
        ])
        .to_directory("uncreated-root")
        .plan()
        .await
        .expect("mixed local intent");
    assert_eq!(plan.files().len(), 2);
    assert!(matches!(
        client
            .download_all([
                Input::parse(&first)
                    .expect("first")
                    .include("*.bin")
                    .expect("first intent"),
                Input::parse(&second)
                    .expect("second")
                    .include("*.bin")
                    .expect("no second match"),
            ])
            .to_directory("uncreated-root")
            .plan()
            .await,
        Err(Error::PathSelection)
    ));
    assert_eq!(get_count(&server).await, 0);
}

#[tokio::test]
async fn pattern_spellings_share_request_text_admission_before_any_discovery() {
    let server = MockServer::start().await;
    let reference = format!("{}/payload", server.uri());
    let mut inputs = Vec::new();
    let spelling = "x".repeat(1024);
    for _ in 0..128 {
        let mut input = Input::parse(&reference).expect("reference");
        for _ in 0..16 {
            input = input
                .exclude(&spelling)
                .expect("within per-input spelling budget");
        }
        inputs.push(input);
    }
    let client = Client::new().expect("client");
    assert!(matches!(
        client.download_all(inputs).plan().await,
        Err(Error::Inventory(crate::inventory::InventoryError::Limit))
    ));
    assert!(
        server
            .received_requests()
            .await
            .expect("no metadata requests")
            .is_empty()
    );
}

#[cfg(feature = "swisstransfer")]
const PATH_SHARE: &str = "aaaaaaaa-1111-2222-3333-444444444444";
#[cfg(feature = "swisstransfer")]
const PATH_MEMBER_A: &str = "11111111-1111-2222-3333-444444444444";
#[cfg(feature = "swisstransfer")]
const PATH_MEMBER_B: &str = "22222222-1111-2222-3333-444444444444";

#[cfg(feature = "swisstransfer")]
fn path_share_input() -> Input {
    Input::parse(&format!("https://www.swisstransfer.com/dl/{PATH_SHARE}")).expect("share")
}

#[cfg(feature = "swisstransfer")]
fn path_share_client(server: &MockServer) -> Client {
    Client {
        http: std::sync::Arc::new(super::http::Http::fixture(&server.uri(), NonZeroUsize::MIN)),
    }
}

#[cfg(feature = "swisstransfer")]
async fn path_share_manifest(server: &MockServer, members: &[(&str, &str, u64)], requests: u64) {
    let files: Vec<_> = members
        .iter()
        .map(|(id, path, size)| serde_json::json!({"id":id,"path":path,"size":size}))
        .collect();
    let page = format!(
        "<script data-page=app type=application/json>{}</script>",
        serde_json::json!({"component":"link/show","props":{
            "link":{"id":PATH_SHARE},"transfer":{"files":files}
        }})
    );
    Mock::given(method("GET"))
        .and(path(format!("/dl/{PATH_SHARE}")))
        .respond_with(ResponseTemplate::new(200).set_body_string(page))
        .expect(requests)
        .mount(server)
        .await;
}

#[cfg(feature = "swisstransfer")]
#[tokio::test]
async fn collection_path_filters_preserve_singleton_context_multi_union_and_literal_token_intent() {
    let server = MockServer::start().await;
    path_share_manifest(
        &server,
        &[
            (PATH_MEMBER_A, "docs/a.txt", 0),
            (PATH_MEMBER_B, "images/b.bin", 3),
        ],
        5,
    )
    .await;
    let client = path_share_client(&server);
    let inventory = client
        .resolve(path_share_input().include("docs/**").expect("include"))
        .await
        .expect("one selected");
    assert_eq!(inventory.files().len(), 1);
    let accounted = inventory.metadata_bytes();
    let file = inventory.into_files().next().expect("member");
    assert!(
        accounted > file.retained_bytes(),
        "discarded discovery remains charged"
    );
    assert_eq!(file.input.scope(), crate::input::InputScope::Collection);
    assert_eq!(
        file.member_token().expect("literal token").as_str(),
        PATH_MEMBER_A
    );
    let plan = client
        .download(file)
        .plan()
        .await
        .expect("no manifest rediscovery");
    assert_eq!(
        plan.files()
            .next()
            .expect("singleton grouping")
            .destination(),
        Path::new("./SwissTransfer/docs/a.txt")
    );
    let plan = client
        .download(path_share_input())
        .include("docs/**")
        .include("images/**")
        .exclude("images/*.tmp")
        .to_directory("uncreated-root")
        .plan()
        .await
        .expect("include union");
    assert_eq!(plan.files().len(), 2);
    let input = path_share_input()
        .select_id(PATH_MEMBER_B)
        .expect("literal token")
        .include("docs/**")
        .expect("independent path intent");
    assert!(matches!(
        client.resolve(input).await,
        Err(Error::PathSelection)
    ));
    let input = path_share_input()
        .select_id("33333333-1111-2222-3333-444444444444")
        .expect("stale literal token")
        .include("**")
        .expect("all paths");
    assert!(matches!(client.resolve(input).await, Err(Error::Selection)));
    let input = path_share_input().exclude("**").expect("exclude all");
    assert!(matches!(
        client.resolve(input).await,
        Err(Error::PathSelection)
    ));
    assert_eq!(server.received_requests().await.expect("requests").len(), 5);
}

#[cfg(feature = "swisstransfer")]
#[tokio::test]
async fn mixed_exact_and_collection_inputs_keep_independent_original_path_intent_and_grouping() {
    let server = MockServer::start().await;
    path_share_manifest(
        &server,
        &[
            (PATH_MEMBER_A, "nested/a.bin", 3),
            (PATH_MEMBER_B, "nested/b.txt", 0),
        ],
        1,
    )
    .await;
    Mock::given(method("HEAD"))
        .and(path("/exact.txt"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;
    let client = path_share_client(&server);
    let share = path_share_input()
        .under("share-output")
        .expect("share namespace")
        .include("nested/**")
        .expect("include")
        .exclude("**/*.txt")
        .expect("exclude");
    let exact = Input::parse(&format!("{}/exact.txt", server.uri()))
        .expect("exact")
        .under("exact-output")
        .expect("exact namespace")
        .include("*.txt")
        .expect("exact include");
    let plan = client
        .download_all([share, exact])
        .to_directory("uncreated-root")
        .plan()
        .await
        .expect("mixed plan");
    let assignments: Vec<_> = plan.files().map(|file| file.destination()).collect();
    assert_eq!(
        assignments,
        [
            Path::new("uncreated-root/share-output/nested/a.bin"),
            Path::new("uncreated-root/exact-output/exact.txt")
        ]
    );
    assert_eq!(
        server
            .received_requests()
            .await
            .expect("metadata only")
            .len(),
        2
    );
}

#[cfg(feature = "swisstransfer")]
#[tokio::test]
async fn empty_collection_requires_positive_matches_but_exclusion_only_is_a_noop() {
    let server = MockServer::start().await;
    path_share_manifest(&server, &[], 3).await;
    let client = path_share_client(&server);
    assert!(matches!(
        client
            .resolve(path_share_input().include("nothing/**").expect("include"))
            .await,
        Err(Error::PathSelection)
    ));
    let inventory = client
        .resolve(path_share_input().exclude("**").expect("exclude"))
        .await
        .expect("genuinely empty exclusion-only inventory");
    assert_eq!(inventory.files().len(), 0);
    let plan = client
        .download(path_share_input())
        .exclude("**")
        .to_directory("uncreated-root")
        .plan()
        .await
        .expect("empty plan");
    assert_eq!(plan.files().len(), 0);
}

#[cfg(all(unix, feature = "swisstransfer"))]
#[tokio::test]
async fn genuinely_empty_exclusion_only_run_creates_no_owned_store_or_payload_outputs() {
    let server = MockServer::start().await;
    path_share_manifest(&server, &[], 1).await;
    let root = Directory::new();
    let events = Rc::new(RefCell::new(Vec::new()));
    let mut observer = Capture(&events);
    let report = path_share_client(&server)
        .download(path_share_input())
        .exclude("**")
        .to_directory(root.path())
        .run_with(&mut observer)
        .await
        .expect("genuine empty no-op");
    assert_eq!(
        *events.borrow(),
        [
            Observed::Started {
                run: None,
                saved: None,
                files: 0
            },
            Observed::Completed(0),
        ]
    );
    assert_eq!(report.completed().len(), 0);
    assert!(report.saved_run().is_none());
    assert_eq!(
        fs::read_dir(root.path())
            .expect("empty output root")
            .count(),
        0
    );
    assert_eq!(
        server
            .received_requests()
            .await
            .expect("manifest only")
            .len(),
        1
    );
}

#[cfg(feature = "swisstransfer")]
#[tokio::test]
async fn all_collection_metadata_is_bounded_and_validated_before_path_filtering() {
    let server = MockServer::start().await;
    let ids: Vec<_> = (0..129)
        .map(|index| format!("{index:08x}-1111-2222-3333-444444444444"))
        .collect();
    let names: Vec<_> = (0..129)
        .map(|index| format!("member-{index}.bin"))
        .collect();
    let members: Vec<_> = ids
        .iter()
        .zip(&names)
        .map(|(id, name)| (id.as_str(), name.as_str(), 0))
        .collect();
    path_share_manifest(&server, &members, 1).await;
    let client = path_share_client(&server);
    assert!(matches!(
        client
            .resolve(
                path_share_input()
                    .include("member-0.bin")
                    .expect("one path")
            )
            .await,
        Err(Error::Inventory(crate::inventory::InventoryError::Limit))
    ));
    server.reset().await;
    path_share_manifest(&server, &members[..128], 1).await;
    let mut input = path_share_input().include("**").expect("include");
    // Shared immutable spellings are conservatively charged per discovered input reference too.
    for _ in 0..15 {
        input = input
            .exclude(&"x".repeat(1024))
            .expect("bounded local pattern");
    }
    input = input
        .exclude("**")
        .expect("exclude all cannot hide discovery text overflow");
    assert!(matches!(
        client.resolve(input).await,
        Err(Error::Inventory(crate::inventory::InventoryError::Limit))
    ));
    server.reset().await;
    path_share_manifest(
        &server,
        &[(PATH_MEMBER_A, "safe", 0), (PATH_MEMBER_B, "../unsafe", 0)],
        1,
    )
    .await;
    assert!(matches!(
        client
            .resolve(path_share_input().include("safe").expect("include"))
            .await,
        Err(Error::InvalidName)
    ));
}

#[cfg(all(unix, feature = "swisstransfer"))]
#[tokio::test]
async fn frozen_pattern_request_reentry_uses_recorded_members_without_changed_manifest_discovery() {
    let server = MockServer::start().await;
    path_share_manifest(&server, &[(PATH_MEMBER_A, "docs/old.bin", 3)], 1).await;
    let access_path = format!("/api/1/links/{PATH_SHARE}/files/{PATH_MEMBER_A}");
    Mock::given(method("GET"))
        .and(path(access_path.as_str()))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&server)
        .await;
    let client = path_share_client(&server);
    let root = Directory::new();
    let failure = client
        .download(path_share_input())
        .include("docs/**")
        .exclude("**/*.tmp")
        .to_directory(root.path())
        .run()
        .await
        .expect_err("access failure leaves frozen intent");
    let saved = failure.report.saved_run().expect("saved run").to_path_buf();
    let state = fs::read(saved.join("state")).expect("bounded saved state");
    // V4 retains the nonsecret recipe for explicit reference-only rebinding, never access URLs.
    assert!(String::from_utf8_lossy(&state).contains("docs/**"));
    assert!(!String::from_utf8_lossy(&state).contains("https://"));
    server.reset().await;
    // If mutable discovery were repeated, the same glob would now select a different member.
    path_share_manifest(&server, &[(PATH_MEMBER_B, "docs/new.bin", 3)], 0).await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    let date = time::OffsetDateTime::now_utc()
        .format(time::macros::format_description!(
            "[year][month][day]T[hour][minute][second]Z"
        ))
        .expect("fixture signing date");
    let signed = format!(
        "https://objects.swisstransfer.infomaniak.cloud/old-object?X-Amz-Date={date}&X-Amz-Expires=3600&X-Amz-Signature=fixture-secret"
    );
    Mock::given(method("GET"))
        .and(path(access_path.as_str()))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data":{"url":signed}
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/old-object"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("etag", "\"old\"")
                .set_body_bytes(b"abc"),
        )
        .expect(1)
        .mount(&server)
        .await;
    let report = client
        .download(path_share_input())
        .include("docs/**")
        .exclude("**/*.tmp")
        .to_directory(root.path())
        .run()
        .await
        .expect("frozen continuation");
    assert_eq!(report.saved_run(), Some(saved.as_path()));
    assert_eq!(report.completed().len(), 1);
    let file = report.completed().next().expect("recorded member");
    assert_eq!(
        file.file().member_token().expect("frozen token").as_str(),
        PATH_MEMBER_A
    );
    assert_eq!(file.file().source_path(), "docs/old.bin");
    assert_eq!(
        fs::read(root.path().join("docs/old.bin")).expect("old assignment"),
        b"abc"
    );
    assert!(!root.path().join("docs/new.bin").exists());
    assert_eq!(server.received_requests().await.expect("requests").len(), 2);
}

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let sequence = NEXT.fetch_add(1, Ordering::Relaxed);
        let time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "xget-client-test-{}-{time:x}-{sequence:x}",
            std::process::id()
        ));
        let mut builder = fs::DirBuilder::new();
        builder.recursive(false);
        #[cfg(unix)]
        builder.mode(0o700);
        builder
            .create(&path)
            .expect("new empty private fixture directory");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).expect("remove only this fixture's uniquely created directory");
    }
}

// Mirror the bounded caller-task snapshot without introducing a different boxed fixture shape.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Observed {
    Started {
        run: Option<super::RunId>,
        saved: Option<PathBuf>,
        files: usize,
    },
    File {
        key: super::BindingKey,
        destination: PathBuf,
    },
    Progress {
        key: super::BindingKey,
        facts: super::ProgressFacts,
    },
    Published {
        key: super::BindingKey,
        written: crate::transfer::ByteCount,
    },
    Completed(usize),
    Failed,
}

pub(super) struct Capture<'a>(pub(super) &'a Rc<RefCell<Vec<Observed>>>);

impl super::Observer for Capture<'_> {
    fn observe(&mut self, event: super::Event<'_>) {
        let event = match event {
            super::Event::RunStarted {
                run,
                saved_run,
                files,
            } => {
                if let Some(path) = saved_run {
                    assert!(
                        path.is_dir(),
                        "run starts only after acquiring its saved workspace"
                    );
                }
                Observed::Started {
                    run,
                    saved: saved_run.map(Path::to_path_buf),
                    files,
                }
            }
            super::Event::FileStarted { key, planned } => Observed::File {
                key,
                destination: planned.destination().to_path_buf(),
            },
            super::Event::Progress { key, facts } => Observed::Progress { key, facts },
            super::Event::FileCompleted { key, published } => Observed::Published {
                key,
                written: published.receipt().partial().written,
            },
            super::Event::RunCompleted { report } => Observed::Completed(report.completed().len()),
            super::Event::RunFailed { .. } => Observed::Failed,
        };
        self.0.borrow_mut().push(event);
    }
}

#[cfg(unix)]
#[tokio::test]
async fn borrowed_local_observers_see_frozen_http_work_and_exact_nohash_progress_in_order() {
    use crate::transfer::ByteCount;
    for execute_plan in [false, true] {
        let root = Directory::new();
        let server = MockServer::start().await;
        let reference = exact(&server, "/observed").await;
        let client = Client::new().expect("client");
        let events = Rc::new(RefCell::new(Vec::new()));
        let mut observer = Capture(&events);
        let download = client
            .download(reference.as_str())
            .measurement(Measurement::None)
            .to_file(root.path().join("payload"));
        let report = if execute_plan {
            client
                .execute_with(download.plan().await.expect("plan"), &mut observer)
                .await
        } else {
            download.run_with(&mut observer).await
        }
        .expect("observed publication");
        let payload_gets = get_count(&server).await;
        let events = events.borrow();
        let key = report
            .bound_completed()
            .expect("saved context")
            .next()
            .expect("file")
            .0;
        assert_eq!(
            events.first(),
            Some(&Observed::Started {
                run: report.run_id(),
                saved: report.saved_run().map(Path::to_path_buf),
                files: 1,
            })
        );
        assert_eq!(
            events.get(1),
            Some(&Observed::File {
                key,
                destination: report
                    .completed()
                    .next()
                    .expect("published file")
                    .destination()
                    .to_path_buf(),
            })
        );
        assert!(events.iter().any(|event| matches!(event, Observed::Progress { key: actual, facts }
            if *actual == key && facts.received() == ByteCount(3) && facts.written() == ByteCount(3)
                && facts.attempt_received() == ByteCount(3) && facts.attempt_written() == ByteCount(3))));
        for event in events.iter() {
            if let Observed::Progress { key: actual, facts } = event {
                assert_eq!(*actual, key);
                assert_eq!(facts.hashed(), ByteCount(0));
                assert_eq!(facts.restored(), ByteCount(0));
            }
        }
        assert_eq!(
            events.get(events.len() - 2),
            Some(&Observed::Published {
                key,
                written: ByteCount(3)
            })
        );
        assert_eq!(events.last(), Some(&Observed::Completed(1)));
        assert!(!events.contains(&Observed::Failed));
        assert_eq!(payload_gets, 1);
    }
}

#[tokio::test]
async fn setup_failures_emit_one_typed_failure_without_started_or_completed_events() {
    let client = Client::new().expect("client");
    let events = Rc::new(RefCell::new(Vec::new()));
    let mut observer = Capture(&events);
    let failure = client
        .download("not a supported reference")
        .run_with(&mut observer)
        .await
        .expect_err("invalid input");
    assert_eq!(failure.report.completed().len(), 0);
    assert_eq!(*events.borrow(), [Observed::Failed]);
}

#[cfg(unix)]
#[tokio::test]
async fn native_check_failure_emits_progress_but_no_spurious_file_or_run_completion() {
    let root = Directory::new();
    let server = MockServer::start().await;
    let reference = exact(&server, "/bad-check").await;
    let client = Client::new().expect("client");
    let events = Rc::new(RefCell::new(Vec::new()));
    let mut observer = Capture(&events);
    let wrong: Digest = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
        .parse()
        .expect("digest");
    let failure = client
        .download(reference.as_str())
        .expect(wrong)
        .to_file(root.path().join("payload"))
        .run_with(&mut observer)
        .await
        .expect_err("mismatch");
    assert!(matches!(failure.error, Error::Transfer(_)));
    let events = events.borrow();
    assert_eq!(events.last(), Some(&Observed::Failed));
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, Observed::Failed))
            .count(),
        1
    );
    assert!(events.iter().any(|event| matches!(event, Observed::Progress { facts, .. } if facts.hashed() == crate::transfer::ByteCount(3))));
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, Observed::Published { .. } | Observed::Completed(_)))
    );
    assert!(!root.path().join("payload").exists());
}

#[test]
fn default_and_send_but_not_sync_observed_client_futures_remain_send() {
    struct SendObservation(Cell<usize>);
    impl super::Observer for SendObservation {
        fn observe(&mut self, _: super::Event<'_>) {
            self.0.set(self.0.get() + 1);
        }
    }
    fn needs_send(_: impl Send) {}
    let client = Client::new().expect("client");
    let mut observer = SendObservation(Cell::new(0));
    needs_send(client.download("https://example.test/payload").run());
    needs_send(
        client
            .download("https://example.test/payload")
            .run_with(&mut observer),
    );
    fn execute_send(client: &Client, plan: super::Plan) {
        let mut observer = SendObservation(Cell::new(0));
        needs_send(client.execute_with(plan, &mut observer));
    }
    fn resume_send(client: &Client, saved: super::SavedRun) {
        let mut observer = SendObservation(Cell::new(0));
        needs_send(client.resume(saved).run_with(&mut observer));
    }
    let _checks = (execute_send, resume_send);
}

#[cfg(unix)]
#[tokio::test]
async fn observed_received_length_is_not_eof_and_drop_does_not_emit_a_terminal_event() {
    struct PrefixObservation<'a> {
        capture: Capture<'a>,
        written: &'a tokio::sync::Notify,
    }
    impl super::Observer for PrefixObservation<'_> {
        fn observe(&mut self, event: super::Event<'_>) {
            if matches!(&event, super::Event::Progress { facts, .. }
                if facts.written() == crate::transfer::ByteCount(3))
            {
                self.written.notify_one();
            }
            super::Observer::observe(&mut self.capture, event);
        }
    }
    for abrupt in [false, true] {
        let directory = Directory::new();
        let mut endpoint = Endpoint::late().await;
        let client = Client::new().expect("client");
        let events = Rc::new(RefCell::new(Vec::new()));
        let written = tokio::sync::Notify::new();
        let mut observer = PrefixObservation {
            capture: Capture(&events),
            written: &written,
        };
        let output = directory.path().join("payload");
        let mut run = Box::pin(
            client
                .download(endpoint.reference.as_str())
                .to_file(&output)
                .run_with(&mut observer),
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                result = &mut run => panic!("clean EOF has not arrived: {result:?}"),
                () = written.notified() => {}
            }
        })
        .await
        .expect("settled prefix event");
        assert!(!events.borrow().iter().any(|event| matches!(
            event,
            Observed::Published { .. } | Observed::Completed(_) | Observed::Failed
        )));
        if abrupt {
            drop(run);
            let count = events.borrow().len();
            endpoint.release();
            endpoint.settle().await;
            assert_eq!(events.borrow().len(), count);
        } else {
            endpoint.release();
            let failure = run.await.expect_err("late body error");
            assert!(matches!(failure.error, Error::Transfer(_)));
            endpoint.settle().await;
            assert_eq!(events.borrow().last(), Some(&Observed::Failed));
        }
        assert!(!output.exists());
        assert!(
            !events
                .borrow()
                .iter()
                .any(|event| matches!(event, Observed::Published { .. } | Observed::Completed(_)))
        );
    }
}

#[cfg(unix)]
#[tokio::test]
async fn multiple_files_emit_distinct_frozen_keys_and_one_terminal_event() {
    let directory = Directory::new();
    let server = MockServer::start().await;
    let first = exact(&server, "/one").await;
    let second = exact(&server, "/two").await;
    let client = Client::new().expect("client");
    let events = Rc::new(RefCell::new(Vec::new()));
    let mut observer = Capture(&events);
    let report = client
        .download_all([first.as_str(), second.as_str()])
        .to_directory(directory.path())
        .run_with(&mut observer)
        .await
        .expect("two publications");
    let keys: Vec<_> = report
        .bound_completed()
        .expect("saved context")
        .map(|(key, _)| key)
        .collect();
    assert_eq!(keys.len(), 2);
    assert_ne!(keys[0], keys[1]);
    let payload_gets = get_count(&server).await;
    let events = events.borrow();
    assert!(matches!(
        events.first(),
        Some(Observed::Started { files: 2, .. })
    ));
    let started: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            Observed::File { key, .. } => Some(*key),
            _ => None,
        })
        .collect();
    let completed: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            Observed::Published { key, .. } => Some(*key),
            _ => None,
        })
        .collect();
    assert_eq!(started, keys);
    assert_eq!(completed, keys);
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, Observed::Completed(_) | Observed::Failed))
            .count(),
        1
    );
    assert_eq!(events.last(), Some(&Observed::Completed(2)));
    assert_eq!(payload_gets, 2);
}

#[cfg(unix)]
#[tokio::test]
async fn access_headers_stay_with_one_input_and_never_enter_owned_records() {
    let root = Directory::new();
    let server = MockServer::start().await;
    let private = exact(&server, "/private").await;
    let public = exact(&server, "/public").await;
    let secret = "Bearer scoped-secret-marker";
    let client = Client::new().expect("client");
    let report = client
        .download_all([
            Input::parse(&private)
                .expect("private")
                .header("Authorization", secret)
                .expect("access"),
            Input::parse(&public).expect("public"),
        ])
        .to_directory(root.path())
        .run()
        .await
        .expect("both inputs");
    assert_eq!(report.completed().len(), 2);
    let requests = server.received_requests().await.expect("requests");
    for request in requests {
        if request.url.path() == "/private" {
            assert_eq!(
                request
                    .headers
                    .get("authorization")
                    .expect("scoped header")
                    .to_str()
                    .expect("ascii"),
                secret
            );
        } else {
            assert!(!request.headers.contains_key("authorization"));
        }
    }
    let state = fs::read(report.saved_run().expect("saved").join("state")).expect("metadata");
    assert!(
        !state
            .windows(secret.len())
            .any(|bytes| bytes == secret.as_bytes())
    );
}

#[tokio::test]
async fn access_headers_are_not_forwarded_through_a_redirect() {
    let server = MockServer::start().await;
    let foreign = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(
            ResponseTemplate::new(302).insert_header("location", format!("{}/file", foreign.uri())),
        )
        .expect(1)
        .mount(&server)
        .await;
    let input = Input::parse(&format!("{}/file", server.uri()))
        .expect("reference")
        .header("Authorization", "Bearer scoped-secret-marker")
        .expect("access");
    assert!(matches!(
        Client::new().expect("client").resolve(input).await,
        Err(Error::Redirect)
    ));
    assert!(
        foreign
            .received_requests()
            .await
            .expect("foreign requests")
            .is_empty()
    );
}

#[cfg(unix)]
#[tokio::test]
async fn renewed_access_headers_bind_frozen_work_without_repeating_discovery() {
    let root = Directory::new();
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .and(header("authorization", "Bearer first-access"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-length", "3")
                .insert_header("etag", "\"v1\""),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(header("authorization", "Bearer first-access"))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&server)
        .await;
    let reference = format!("{}/payload", server.uri());
    let client = Client::new().expect("client");
    let first = Input::parse(&reference)
        .expect("reference")
        .header("Authorization", "Bearer first-access")
        .expect("access");
    let failure = client
        .download(first)
        .to_directory(root.path())
        .run()
        .await
        .expect_err("opening failed");
    let saved = failure.report.saved_run().expect("owned run").to_path_buf();
    server.verify().await;
    server.reset().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(header("authorization", "Bearer renewed-access"))
        .and(header("if-match", "\"v1\""))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("etag", "\"v1\"")
                .set_body_bytes(b"abc"),
        )
        .expect(1)
        .mount(&server)
        .await;
    let renewed = Input::parse(&reference)
        .expect("reference")
        .header("Authorization", "Bearer renewed-access")
        .expect("renewal");
    let report = client
        .download(renewed)
        .to_directory(root.path())
        .run()
        .await
        .expect("frozen continuation");
    assert_eq!(report.saved_run(), Some(saved.as_path()));
    assert_eq!(
        fs::read(root.path().join("payload")).expect("payload"),
        b"abc"
    );
    let state = fs::read(saved.join("state")).expect("metadata");
    for secret in ["first-access", "renewed-access"] {
        assert!(
            !state
                .windows(secret.len())
                .any(|bytes| bytes == secret.as_bytes())
        );
    }
}

#[cfg(not(feature = "ipfs"))]
#[tokio::test]
async fn ipfs_transport_disabled_is_a_capability_error_not_an_http_fallback() {
    let client = Client::new().expect("client");
    assert!(matches!(
        client.resolve("ipfs://bafyexample/path").await,
        Err(Error::IpfsUnavailable)
    ));
}
