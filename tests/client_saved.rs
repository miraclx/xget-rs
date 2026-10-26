//! Public exact-HTTP recovery usage. Authored fixtures are not a claim of execution evidence.
#![cfg(unix)]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use core::sync::atomic::{AtomicU64, Ordering};
use std::fs;
use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _, symlink};
use std::path::{Path, PathBuf};

use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};
use xget::Client;
use xget::client::{BindingKey, Error, Fresh, RunId, SavedError, SavedRun};
use xget::transfer::{Digest, Measurement};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let sequence = NEXT.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "xget-public-saved-{}-{sequence}",
            std::process::id()
        ));
        fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
        Self(path)
    }
    fn path(&self) -> &Path {
        &self.0
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn managed_run_store_names_cannot_be_namespace_or_native_payload_roles() {
    use bytes::Bytes;
    use futures::stream;
    use xget::Input;
    use xget::input::NamespaceError;
    use xget::transfer::{Length, RequestError, Transfer};

    assert!(matches!(
        Input::parse("https://example.org/payload")
            .unwrap()
            .under(".xget-runs"),
        Err(NamespaceError::ReservedRole)
    ));
    let body = stream::empty::<Result<Bytes, core::convert::Infallible>>();
    assert!(matches!(
        Transfer::from_body(body, Length::Unknown).to_file(".XGET-RUNS"),
        Err(RequestError::InvalidDestination)
    ));
}

async fn whole_fixture(server: &MockServer) {
    Mock::given(method("HEAD"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-length", "3")
                .insert_header("etag", "\"v1\""),
        )
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("etag", "\"v1\"")
                .set_body_bytes(b"abc"),
        )
        .mount(server)
        .await;
}

#[tokio::test]
async fn completed_history_admits_more_than_128_runs_without_removing_native_artifacts() {
    let output = Directory::new();
    let server = MockServer::start().await;
    whole_fixture(&server).await;
    let client = Client::new().unwrap();
    let mut runs = Vec::new();
    let mut native = Vec::new();
    let mut lease_identity = None;
    for index in 0..132 {
        let reference = format!("{}/payload-{index}", server.uri());
        let report = client
            .download(reference.as_str())
            .to_directory(output.path())
            .run()
            .await
            .unwrap();
        let path = report.saved_run().unwrap().to_path_buf();
        assert!(SavedRun::inspect(&path).await.unwrap().is_complete());
        runs.push(path);
        let state = report
            .completed()
            .next()
            .unwrap()
            .receipt()
            .state()
            .path()
            .join("state");
        native.push((PathBuf::clone(&state), fs::read(state).unwrap()));
        if index == 127 {
            // Completion keeps actual inspection paths until bounded capacity needs retirement.
            for path in &runs {
                assert!(SavedRun::inspect(path).await.unwrap().is_complete());
            }
            let metadata = fs::metadata(output.path().join(".xget-runs/lease")).unwrap();
            lease_identity = Some((metadata.dev(), metadata.ino()));
        }
    }
    assert_eq!(runs.iter().filter(|path| path.exists()).count(), 128);
    assert_eq!(
        fs::read_dir(output.path().join(".xget-runs"))
            .unwrap()
            .count(),
        129
    );
    let metadata = fs::metadata(output.path().join(".xget-runs/lease")).unwrap();
    assert_eq!(lease_identity, Some((metadata.dev(), metadata.ino())));
    for (state, bytes) in native {
        assert_eq!(fs::read(state).unwrap(), bytes);
    }
    for index in 0..132 {
        assert_eq!(
            fs::read(output.path().join(format!("payload-{index}"))).unwrap(),
            b"abc"
        );
    }
    for path in runs.iter().filter(|path| !path.exists()) {
        assert!(matches!(
            SavedRun::inspect(path).await,
            Err(SavedError::Missing)
        ));
    }
    assert_eq!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| request.method.as_str() == "GET")
            .count(),
        132
    );
}

#[tokio::test]
async fn existing_fresh_targets_refuse_without_get_or_owned_state_and_preview_stays_a_preview() {
    for fresh in [false, true] {
        for dangling in [false, true] {
            let output = Directory::new();
            let server = MockServer::start().await;
            whole_fixture(&server).await;
            let destination = output.path().join("chosen.bin");
            if dangling {
                symlink(output.path().join("missing"), &destination).unwrap();
            } else {
                fs::write(&destination, b"incumbent").unwrap();
            }
            let reference = format!("{}/payload", server.uri());
            let client = Client::new().unwrap();
            let preview = client
                .download(reference.as_str())
                .to_file(&destination)
                .plan()
                .await
                .unwrap();
            assert_eq!(preview.files().len(), 1);
            let request = client.download(reference.as_str()).to_file(&destination);
            let request = if fresh { request.fresh(Fresh) } else { request };
            let failure = request.run().await.unwrap_err();
            assert!(matches!(failure.error, Error::DestinationExists));
            assert_eq!(failure.failed.as_ref().unwrap().destination(), destination);
            assert!(
                failure
                    .error
                    .to_string()
                    .contains("fresh with explicit overwrite")
            );
            assert!(failure.report.saved_run().is_none());
            assert_eq!(fs::read_dir(output.path()).unwrap().count(), 1);
            assert_eq!(
                server
                    .received_requests()
                    .await
                    .unwrap()
                    .iter()
                    .filter(|request| request.method.as_str() == "GET")
                    .count(),
                0
            );
            if dangling {
                assert!(fs::symlink_metadata(destination).unwrap().is_symlink());
                assert!(!output.path().join("missing").exists());
            } else {
                assert_eq!(fs::read(destination).unwrap(), b"incumbent");
            }
        }
    }
}

#[tokio::test]
async fn one_existing_fresh_assignment_preflights_the_entire_set_before_any_get() {
    let output = Directory::new();
    let server = MockServer::start().await;
    whole_fixture(&server).await;
    fs::write(output.path().join("two"), b"incumbent").unwrap();
    let references = [
        format!("{}/one", server.uri()),
        format!("{}/two", server.uri()),
    ];
    let failure = Client::new()
        .unwrap()
        .download_all(references.iter().map(String::as_str))
        .to_directory(output.path())
        .run()
        .await
        .unwrap_err();
    assert!(matches!(failure.error, Error::DestinationExists));
    assert_eq!(
        failure.failed.as_ref().unwrap().destination(),
        output.path().join("two")
    );
    assert!(failure.report.saved_run().is_none());
    assert!(!output.path().join("one").exists());
    assert_eq!(fs::read_dir(output.path()).unwrap().count(), 1);
    assert_eq!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| request.method.as_str() == "GET")
            .count(),
        0
    );
}

#[tokio::test]
async fn metadata_selection_and_cardinality_errors_precede_existing_target_preflight() {
    let output = Directory::new();
    let server = MockServer::start().await;
    whole_fixture(&server).await;
    let destination = output.path().join("chosen.bin");
    fs::write(&destination, b"incumbent").unwrap();
    let references = [
        format!("{}/one", server.uri()),
        format!("{}/two", server.uri()),
    ];
    let client = Client::new().unwrap();
    let failure = client
        .download(references[0].as_str())
        .include("absent")
        .to_file(&destination)
        .run()
        .await
        .unwrap_err();
    assert!(matches!(failure.error, Error::PathSelection));
    let failure = client
        .download_all(references.iter().map(String::as_str))
        .to_file(&destination)
        .run()
        .await
        .unwrap_err();
    assert!(matches!(failure.error, Error::FileCardinality));
    let a: Digest = format!("sha256:{}", "00".repeat(32)).parse().unwrap();
    let b: Digest = format!("sha256:{}", "11".repeat(32)).parse().unwrap();
    let before = server.received_requests().await.unwrap().len();
    let failure = client
        .download(references[0].as_str())
        .expect(a)
        .expect(b)
        .to_file(&destination)
        .run()
        .await
        .unwrap_err();
    assert!(matches!(failure.error, Error::ConflictingRequirements(_)));
    assert_eq!(server.received_requests().await.unwrap().len(), before);
    assert_eq!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| request.method.as_str() == "GET")
            .count(),
        0
    );
    assert_eq!(fs::read_dir(output.path()).unwrap().count(), 1);
}

#[tokio::test]
async fn automatic_unfinished_reentry_revalidates_acknowledged_incumbents_before_pending_work() {
    let output = Directory::new();
    let server = MockServer::start().await;
    whole_fixture(&server).await;
    Mock::given(method("GET"))
        .and(path("/two"))
        .respond_with(ResponseTemplate::new(503))
        .with_priority(1)
        .mount(&server)
        .await;
    let references = [
        format!("{}/one", server.uri()),
        format!("{}/two", server.uri()),
    ];
    let client = Client::new().unwrap();
    let failure = client
        .download_all(references.iter().map(String::as_str))
        .to_directory(output.path())
        .run()
        .await
        .unwrap_err();
    assert_eq!(failure.report.completed().len(), 1);
    let saved = failure.report.saved_run().unwrap().to_path_buf();
    let inode = fs::metadata(output.path().join("one")).unwrap().ino();
    server.reset().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/one"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/two"))
        .and(header("if-match", "\"v1\""))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("etag", "\"v1\"")
                .set_body_bytes(b"abc"),
        )
        .expect(1)
        .mount(&server)
        .await;
    let report = client
        .download_all(references.iter().map(String::as_str))
        .to_directory(output.path())
        .run()
        .await
        .unwrap();
    assert_eq!(report.saved_run(), Some(saved.as_path()));
    assert_eq!(report.completed().len(), 2);
    assert!(matches!(
        report.completed().next().unwrap().version(),
        xget::client::VersionConsistency::LocallyRevalidated
    ));
    assert_eq!(
        fs::metadata(output.path().join("one")).unwrap().ino(),
        inode
    );
}

#[tokio::test]
async fn ordinary_failure_offline_inspection_and_consumed_explicit_rebinding_share_one_pipeline() {
    let output = Directory::new();
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(200).insert_header("etag", "\"revision-1\""))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;
    let reference = format!("{}/payload?authorization=not-persisted", server.uri());
    let client = Client::new().unwrap();
    let required: Digest =
        "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
            .parse()
            .unwrap();
    let failure = client
        .download(reference.as_str())
        .expect(required)
        .measurement(Measurement::None)
        .to_file(output.path().join("chosen.bin"))
        .run()
        .await
        .unwrap_err();
    assert_eq!(failure.report.completed().len(), 0);
    let saved = SavedRun::inspect(failure.report.saved_run().expect("actual owned run path"))
        .await
        .unwrap();
    let member = saved.members().next().unwrap();
    let binding = member.key().to_string().parse::<BindingKey>().unwrap();
    let run = saved.id();
    assert_eq!(run.to_string().parse::<RunId>().unwrap(), run);
    assert_eq!(binding.run_id(), run);
    assert_eq!(failure.report.run_id(), Some(run));
    assert_eq!(member.destination().file_name().unwrap(), "chosen.bin");
    assert_eq!(member.attempts().len(), 0); // Opening failed before native submission.
    let mut foreign = binding.to_string();
    foreign.truncate(foreign.len() - 8);
    foreign.push_str("00000001");
    let before = server.received_requests().await.unwrap().len();
    let rejected = client
        .resume(SavedRun::inspect(saved.path()).await.unwrap())
        .input(foreign.parse::<BindingKey>().unwrap(), reference.as_str())
        .run()
        .await
        .unwrap_err();
    assert!(matches!(rejected.error, Error::Saved(SavedError::Binding)));
    assert_eq!(server.received_requests().await.unwrap().len(), before);
    server.reset().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(header("if-match", "\"revision-1\""))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("etag", "\"revision-1\"")
                .set_body_bytes(b"abc"),
        )
        .expect(1)
        .mount(&server)
        .await;
    let report = client
        .resume(saved)
        .input(binding, reference.as_str())
        .run()
        .await
        .unwrap();
    let completed = report.completed().next().unwrap();
    let mut bound = report.bound_completed().unwrap();
    let (completed_key, bound_file) = bound.next().unwrap();
    assert_eq!(completed_key, binding);
    assert_eq!(bound_file.destination(), completed.destination());
    assert_eq!(report.run_id(), Some(run));
    assert_eq!(bound.len(), 0);
    assert_eq!(completed.receipt().checks().measured(), None);
    assert_eq!(
        completed.receipt().checks().matched_requirements().count(),
        1
    );
    assert!(
        SavedRun::inspect(report.saved_run().unwrap())
            .await
            .unwrap()
            .is_complete()
    );
    assert_eq!(fs::read(output.path().join("chosen.bin")).unwrap(), b"abc");
}

#[tokio::test]
async fn saved_keys_follow_partial_and_revalidated_publication_order() {
    let output = Directory::new();
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-length", "3")
                .insert_header("etag", "\"revision-1\""),
        )
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/one"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("etag", "\"revision-1\"")
                .set_body_bytes(b"abc"),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/two"))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&server)
        .await;
    let references = [
        format!("{}/one", server.uri()),
        format!("{}/two", server.uri()),
    ];
    let client = Client::new().unwrap();
    let failure = client
        .download_all(references.iter().map(String::as_str))
        .to_directory(output.path())
        .run()
        .await
        .unwrap_err();
    let saved = SavedRun::inspect(failure.report.saved_run().unwrap())
        .await
        .unwrap();
    let keys = saved
        .members()
        .map(|member| member.key())
        .collect::<Vec<_>>();
    let partial = failure
        .report
        .bound_completed()
        .unwrap()
        .collect::<Vec<_>>();
    assert_eq!(partial.len(), 1);
    assert_eq!(partial[0].0, keys[0]);
    assert_eq!(partial[0].1.destination().file_name().unwrap(), "one");
    server.verify().await;
    server.reset().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/one"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/two"))
        .and(header("if-match", "\"revision-1\""))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("etag", "\"revision-1\"")
                .set_body_bytes(b"xyz"),
        )
        .expect(1)
        .mount(&server)
        .await;
    let report = client
        .resume(saved)
        .input(keys[0], references[0].as_str())
        .input(keys[1], references[1].as_str())
        .run()
        .await
        .unwrap();
    let completed = report.bound_completed().unwrap().collect::<Vec<_>>();
    assert_eq!(completed.len(), 2);
    for (index, (key, published)) in completed.into_iter().enumerate() {
        assert_eq!(key, keys[index]);
        assert_eq!(key.run_id(), report.run_id().unwrap());
        assert_eq!(
            published.destination(),
            report.completed().nth(index).unwrap().destination()
        );
    }
    assert_eq!(fs::read(output.path().join("one")).unwrap(), b"abc");
    assert_eq!(fs::read(output.path().join("two")).unwrap(), b"xyz");
}

async fn failed_exact(
    server: &MockServer,
    output: &Directory,
    etag: Option<&str>,
) -> (String, PathBuf) {
    let mut metadata = ResponseTemplate::new(200).insert_header("content-length", "3");
    if let Some(etag) = etag {
        metadata = metadata.insert_header("etag", etag);
    }
    Mock::given(method("HEAD"))
        .respond_with(metadata)
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(503))
        .mount(server)
        .await;
    let reference = format!("{}/payload", server.uri());
    let failure = Client::new()
        .unwrap()
        .download(reference.as_str())
        .to_file(output.path().join("chosen.bin"))
        .run()
        .await
        .unwrap_err();
    (reference, failure.report.saved_run().unwrap().to_path_buf())
}

#[tokio::test]
async fn supplied_selected_versions_and_lengths_cannot_be_silently_discarded_on_resume() {
    for (frozen, supplied, length) in [
        (None, "\"v2\"", "3"),
        (Some("\"v1\""), "\"v2\"", "3"),
        (Some("\"v1\""), "\"v1\"", "4"),
    ] {
        let output = Directory::new();
        let server = MockServer::start().await;
        let (reference, path) = failed_exact(&server, &output, frozen).await;
        server.reset().await;
        Mock::given(method("HEAD"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-length", length)
                    .insert_header("etag", supplied),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header(
                        "etag",
                        if supplied == "\"v1\"" {
                            "\"v1\""
                        } else {
                            "\"v3\""
                        },
                    )
                    .set_body_bytes(b"abc"),
            )
            .expect(0)
            .mount(&server)
            .await;
        let client = Client::new().unwrap();
        let file = client
            .resolve(reference.as_str())
            .await
            .unwrap()
            .select_one(|_| true)
            .unwrap();
        let saved = SavedRun::inspect(path).await.unwrap();
        let key = saved.members().next().unwrap().key();
        let failure = client
            .resume(saved)
            .input(key, file)
            .run()
            .await
            .unwrap_err();
        assert!(matches!(failure.error, Error::Saved(SavedError::Binding)));
        assert!(!output.path().join("chosen.bin").exists());
        assert_eq!(
            server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .filter(|request| request.method.as_str() == "GET")
                .count(),
            0
        );
    }
}

#[tokio::test]
async fn compatible_selected_file_reentry_preserves_the_frozen_caller_requirement() {
    let output = Directory::new();
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-length", "3")
                .insert_header("etag", "\"v1\""),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;
    let reference = format!("{}/payload", server.uri());
    let client = Client::new().unwrap();
    let required: Digest =
        "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
            .parse()
            .unwrap();
    let failure = client
        .download(reference.as_str())
        .expect(required)
        .measurement(Measurement::None)
        .to_file(output.path().join("chosen.bin"))
        .run()
        .await
        .unwrap_err();
    let file = client
        .resolve(reference.as_str())
        .await
        .unwrap()
        .select_one(|_| true)
        .unwrap();
    let saved = SavedRun::inspect(failure.report.saved_run().unwrap())
        .await
        .unwrap();
    let key = saved.members().next().unwrap().key();
    server.reset().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(header("if-match", "\"v1\""))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("etag", "\"v1\"")
                .set_body_bytes(b"abc"),
        )
        .expect(1)
        .mount(&server)
        .await;
    let report = client.resume(saved).input(key, file).run().await.unwrap();
    let completed = report.completed().next().unwrap();
    assert_eq!(completed.receipt().checks().measured(), None);
    assert_eq!(
        completed
            .receipt()
            .checks()
            .matched_requirements()
            .next()
            .unwrap()
            .digest,
        required
    );
}

#[tokio::test]
async fn resume_accepts_a_conversion_error_without_any_into_client_error_implementation() {
    struct LocalReference;
    struct LocalConversionError;
    impl TryFrom<LocalReference> for xget::Input {
        type Error = LocalConversionError;
        fn try_from(_: LocalReference) -> Result<Self, Self::Error> {
            Err(LocalConversionError)
        }
    }
    let output = Directory::new();
    let server = MockServer::start().await;
    let (_, path) = failed_exact(&server, &output, None).await;
    let saved = SavedRun::inspect(path).await.unwrap();
    let key = saved.members().next().unwrap().key();
    let failure = Client::new()
        .unwrap()
        .resume(saved)
        .input(key, LocalReference)
        .run()
        .await
        .unwrap_err();
    assert!(matches!(failure.error, Error::Saved(SavedError::Binding)));
}

#[tokio::test]
async fn directory_shaped_file_targets_cannot_match_an_existing_valid_frozen_file_request() {
    for suffix in ["/", "/."] {
        let output = Directory::new();
        let server = MockServer::start().await;
        let (reference, _) = failed_exact(&server, &output, None).await;
        server.reset().await;
        Mock::given(method("HEAD"))
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(503))
            .expect(0)
            .mount(&server)
            .await;
        let mut raw = output.path().join("chosen.bin").into_os_string();
        raw.push(suffix);
        let failure = Client::new()
            .unwrap()
            .download(reference.as_str())
            .to_file(PathBuf::from(raw))
            .run()
            .await
            .unwrap_err();
        assert!(matches!(failure.error, Error::InvalidName));
        assert!(server.received_requests().await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn parent_components_through_a_symlink_are_refused_without_redirecting_the_output() {
    use std::os::unix::fs::symlink;
    let output = Directory::new();
    let a = output.path().join("a");
    let b = output.path().join("b");
    for path in [&a, &b, &a.join("out"), &b.join("out"), &b.join("sub")] {
        fs::DirBuilder::new().mode(0o700).create(path).unwrap();
    }
    symlink(b.join("sub"), a.join("link")).unwrap();
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"abc"))
        .expect(0)
        .mount(&server)
        .await;
    let reference = format!("{}/payload", server.uri());
    let failure = Client::new()
        .unwrap()
        .download(reference.as_str())
        .to_directory(a.join("link/../out"))
        .run()
        .await
        .unwrap_err();
    assert!(matches!(failure.error, Error::Saved(_)));
    assert!(!a.join("out/payload").exists());
    assert!(!b.join("out/payload").exists());
    assert_eq!(fs::read_dir(a.join("out")).unwrap().count(), 0);
    assert_eq!(fs::read_dir(b.join("out")).unwrap().count(), 0);
}

#[tokio::test]
async fn fresh_is_not_restart_or_overwrite_and_ambiguous_runs_expose_actual_resume_paths() {
    let output = Directory::new();
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;
    let reference = format!("{}/payload", server.uri());
    let client = Client::new().unwrap();
    let first = client
        .download(reference.as_str())
        .to_directory(output.path())
        .run()
        .await
        .unwrap_err();
    let second = client
        .download(reference.as_str())
        .fresh(Fresh)
        .to_directory(output.path())
        .run()
        .await
        .unwrap_err();
    assert_ne!(first.report.saved_run(), second.report.saved_run());
    let failure = client
        .download(reference.as_str())
        .to_directory(output.path())
        .run()
        .await
        .unwrap_err();
    match failure.error {
        Error::Saved(SavedError::Ambiguous { resume_paths }) => {
            assert_eq!(resume_paths.len(), 2);
            for path in resume_paths {
                assert!(SavedRun::inspect(path).await.is_ok());
            }
        }
        other => panic!("expected explicit ambiguity, got {other:?}"),
    }
    assert!(!output.path().join("payload").exists());
}
