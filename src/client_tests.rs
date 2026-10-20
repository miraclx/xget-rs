//! Actual loopback HTTP fixtures; authored source, not execution evidence.
#![allow(clippy::expect_used)]

use core::num::NonZeroUsize;
use core::sync::atomic::{AtomicU64, Ordering};
use core::task::Poll;
use core::time::Duration;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _, PermissionsExt as _, symlink};
use std::path::{Path, PathBuf};

use futures::StreamExt as _;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::oneshot;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{BodyError, BodyFailure, Client, Error, FinishError, VersionConsistency};
use crate::input::Input;
use crate::transfer::{Algorithm, Digest, Measurement};

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
        Input::parse("s3://bucket/key").expect("s3 intent"),
        Input::parse("ipfs://bafyexample/path").expect("ipfs intent"),
        Input::parse("https://swisstransfer.com/dl/aaaaaaaa-1111-2222-3333-444444444444")
            .expect("share intent"),
        Input::parse("https://github.com/owner/project/releases/latest").expect("catalogue intent"),
        Input::tree("https://example.test/dav/").expect("tree intent"),
    ] {
        assert!(matches!(
            client.resolve(input).await,
            Err(Error::UnsupportedInput { .. })
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
        .expect(other)
        .expect(first)
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
        .measurement(Measurement::None)
        .into_body();
    let mut received = Vec::new();
    while let Some(chunk) = body.next().await {
        received.extend_from_slice(&chunk.expect("chunk"));
    }
    assert_eq!(received, b"abc");
    let receipt = body.finish().expect("clean EOF");
    assert_eq!(receipt.body().checks().matched_requirements().count(), 1);
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
        .expect(wrong)
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
async fn native_digest_failure_preserves_partial_state_and_an_existing_destination() {
    let directory = Directory::new();
    let server = MockServer::start().await;
    let reference = exact(&server, "/payload").await;
    let destination = directory.path().join("sentinel");
    fs::write(&destination, b"sentinel").expect("sentinel");
    let wrong = Digest::parse(Algorithm::Sha256, &"00".repeat(32)).expect("wrong digest");
    let client = Client::new().expect("client");
    let failure = client
        .download(reference.as_str())
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
