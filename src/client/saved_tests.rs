//! Authored source fixtures only. The source-author session does not execute these tests.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use core::sync::atomic::{AtomicU64, Ordering};
use core::time::Duration;
use std::fs;
use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _, PermissionsExt as _, symlink};
use std::path::{Path, PathBuf};

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::{Notify, oneshot};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{AttemptStatus, Fingerprint, Fresh, Owned, STORE, SavedError, SavedRun, codec, disk};
use crate::client::{Client, Error};
use crate::transfer::{Algorithm, FailureKind, Measurement, lease_directory};

#[test]
fn requests_without_path_patterns_keep_the_existing_fingerprint_byte_stream() {
    let root = Fixture::new();
    let input = crate::Input::parse("https://example.test/payload?token=private")
        .unwrap()
        .under("namespace")
        .unwrap();
    let requirements = crate::checks::Requirements::default();
    let measurement = Measurement::default();
    let target = crate::client::Target::Directory(root.path().to_path_buf());
    let mut reference = crate::checksum::OwnedSha256::default();
    Fingerprint::field(&mut reference, b"xget exact original binding 1");
    Fingerprint::field(&mut reference, input.original_reference().as_bytes());
    Fingerprint::field(&mut reference, b"namespace");
    let old_binding = Fingerprint(reference.finish());
    assert!(Fingerprint::reference(&input) == old_binding);
    let mut request = crate::checksum::OwnedSha256::default();
    Fingerprint::field(&mut request, b"xget exact request intent 1");
    Fingerprint::field(&mut request, &old_binding.0);
    Fingerprint::field(&mut request, b"input");
    Fingerprint::field(&mut request, &[2]);
    Fingerprint::field(
        &mut request,
        disk::absolute(root.path())
            .unwrap()
            .as_os_str()
            .as_encoded_bytes(),
    );
    Fingerprint::field(&mut request, &codec::policy(requirements, measurement));
    assert!(
        Fingerprint::request(&[input], &target, requirements, measurement).unwrap()
            == Fingerprint(request.finish())
    );
}

#[test]
fn pattern_fingerprints_bind_order_duplicates_and_list_roles_with_unambiguous_framing() {
    let reference = "https://example.test/payload";
    let inputs = [
        crate::Input::parse(reference).unwrap(),
        crate::Input::parse(reference)
            .unwrap()
            .include("a")
            .unwrap()
            .include("bc")
            .unwrap(),
        crate::Input::parse(reference)
            .unwrap()
            .include("ab")
            .unwrap()
            .include("c")
            .unwrap(),
        crate::Input::parse(reference)
            .unwrap()
            .include("bc")
            .unwrap()
            .include("a")
            .unwrap(),
        crate::Input::parse(reference)
            .unwrap()
            .include("a")
            .unwrap()
            .exclude("bc")
            .unwrap(),
        crate::Input::parse(reference)
            .unwrap()
            .include("a")
            .unwrap()
            .include("bc")
            .unwrap()
            .include("bc")
            .unwrap(),
    ];
    let root = Fixture::new();
    let target = crate::client::Target::Directory(root.path().to_path_buf());
    for (index, input) in inputs.iter().enumerate() {
        let duplicate = input.duplicate_reference().unwrap();
        assert!(Fingerprint::reference(input) == Fingerprint::reference(&duplicate));
        let intent = Fingerprint::request(
            core::slice::from_ref(input),
            &target,
            crate::checks::Requirements::default(),
            Measurement::default(),
        )
        .unwrap();
        for other in &inputs[..index] {
            assert!(Fingerprint::reference(input) != Fingerprint::reference(other));
            assert!(
                intent
                    != Fingerprint::request(
                        core::slice::from_ref(other),
                        &target,
                        crate::checks::Requirements::default(),
                        Measurement::default()
                    )
                    .unwrap()
            );
        }
    }
}

#[tokio::test]
async fn frozen_pattern_binding_rejects_changed_intent_and_reenters_without_changed_head() {
    let root = Fixture::new();
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-length", "3")
                .insert_header("etag", "\"v1\""),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&server)
        .await;
    let reference = format!("{}/payload", server.uri());
    let client = Client::new().unwrap();
    let failure = client
        .download(reference.as_str())
        .include("payload")
        .exclude("*.tmp")
        .to_directory(root.path())
        .run()
        .await
        .unwrap_err();
    let path = failure.report.saved_run().unwrap().to_path_buf();
    let saved = SavedRun::inspect(&path).await.unwrap();
    let key = saved.members().next().unwrap().key();
    server.reset().await;
    Mock::given(method("HEAD"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-length", "999")
                .insert_header("etag", "\"changed\""),
        )
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
    let failure = client
        .resume(saved)
        .input(key, reference.as_str())
        .run()
        .await
        .unwrap_err();
    assert!(matches!(failure.error, Error::Saved(SavedError::Binding)));
    let changed = crate::Input::parse(&reference)
        .unwrap()
        .include("**")
        .unwrap()
        .exclude("*.tmp")
        .unwrap();
    let failure = client
        .resume(SavedRun::inspect(&path).await.unwrap())
        .input(key, changed)
        .run()
        .await
        .unwrap_err();
    assert!(matches!(failure.error, Error::Saved(SavedError::Binding)));
    assert_eq!(count(&server, "GET").await, 0);
    let report = client
        .download(reference.as_str())
        .include("payload")
        .exclude("*.tmp")
        .to_directory(root.path())
        .run()
        .await
        .unwrap();
    assert_eq!(report.saved_run(), Some(path.as_path()));
    assert_eq!(report.completed().len(), 1);
    assert_eq!(fs::read(root.path().join("payload")).unwrap(), b"abc");
    assert_eq!(count(&server, "HEAD").await, 0);
    assert_eq!(count(&server, "GET").await, 1);
}

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let sequence = NEXT.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("xget-saved-{}-{sequence}", std::process::id()));
        fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
        Self(path)
    }
    fn path(&self) -> &Path {
        &self.0
    }
    fn runs(&self) -> Vec<PathBuf> {
        fs::read_dir(self.path().join(STORE))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.file_name().unwrap() != "lease")
            .collect()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

async fn exact(server: &MockServer, name: &str) -> String {
    Mock::given(method("HEAD"))
        .and(path(name))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-length", "3")
                .insert_header("etag", "\"v1\""),
        )
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path(name))
        .and(header("if-match", "\"v1\""))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("etag", "\"v1\"")
                .set_body_bytes(b"abc"),
        )
        .mount(server)
        .await;
    format!("{}{name}", server.uri())
}

async fn count(server: &MockServer, verb: &str) -> usize {
    server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|request| request.method.as_str() == verb)
        .count()
}

// A real HTTP body sends one prefix and then waits behind explicit fixture authority. Cancelling
// the client is not simulated by a hand-written transport error or a manufactured native receipt.
struct Interrupted {
    reference: String,
    entered: std::sync::Arc<Notify>,
    stop: Option<oneshot::Sender<()>>,
    worker: Option<tokio::task::JoinHandle<()>>,
}
impl Interrupted {
    async fn new() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let reference = format!("http://{}/second", listener.local_addr().unwrap());
        let entered = std::sync::Arc::new(Notify::new());
        let signal = std::sync::Arc::clone(&entered);
        let (stop, mut stopped) = oneshot::channel();
        let worker = tokio::spawn(async move {
            loop {
                let (mut socket, _) = tokio::select! {
                    result = listener.accept() => result.unwrap(),
                    _ = &mut stopped => return,
                };
                let mut request = Vec::new();
                loop {
                    let mut byte = [0; 1];
                    if socket.read(&mut byte).await.unwrap() == 0 {
                        break;
                    }
                    request.push(byte[0]);
                    assert!(request.len() < 16384);
                    if request.ends_with(b"\r\n\r\n") {
                        break;
                    }
                }
                if request.starts_with(b"HEAD ") {
                    socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nETag: \"v1\"\r\nConnection: close\r\n\r\n").await.unwrap();
                } else {
                    assert!(request.starts_with(b"GET "));
                    assert!(
                        String::from_utf8_lossy(&request)
                            .to_ascii_lowercase()
                            .contains("if-match: \"v1\"")
                    );
                    socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nETag: \"v1\"\r\nConnection: close\r\n\r\na").await.unwrap();
                    signal.notify_one();
                    let _ = (&mut stopped).await;
                    return;
                }
            }
        });
        Self {
            reference,
            entered,
            stop: Some(stop),
            worker: Some(worker),
        }
    }
    async fn close(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(worker) = self.worker.take() {
            worker.await.unwrap();
        }
    }
}
impl Drop for Interrupted {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(worker) = self.worker.take() {
            worker.abort();
        }
    }
}

async fn wait_for_prefix(root: &Path) -> PathBuf {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(entries) = fs::read_dir(root.join(STORE)) {
                for entry in entries.flatten() {
                    if entry.file_name() == "lease" {
                        continue;
                    }
                    if let Ok(saved) = SavedRun::inspect(entry.path()).await
                        && let Some(state) = saved
                            .members()
                            .nth(1)
                            .and_then(|member| member.attempts().last().cloned())
                        && fs::metadata(state.path().join("data"))
                            .is_ok_and(|metadata| metadata.len() == 1)
                    {
                        return saved.path().to_path_buf();
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("native second prefix must reach disk")
}

async fn settled(root: &Path) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match lease_directory(root.join(STORE)).await {
                Ok(owner) => {
                    drop(owner);
                    return;
                }
                Err(failure) if matches!(failure.kind, FailureKind::Busy) => {
                    tokio::task::yield_now().await
                }
                Err(failure) => panic!("unexpected store probe: {failure:?}"),
            }
        }
    })
    .await
    .expect("submitted native work must settle")
}

#[tokio::test]
async fn first_publication_and_interrupted_second_body_keep_frozen_members_and_old_attempts() {
    let root = Fixture::new();
    let first = MockServer::start().await;
    let first_ref = exact(&first, "/first").await;
    let interrupted = Interrupted::new().await;
    let second_ref = String::clone(&interrupted.reference);
    let output = root.path().to_path_buf();
    let inputs = [String::clone(&first_ref), String::clone(&second_ref)];
    let task = tokio::spawn(async move {
        Client::new()
            .unwrap()
            .download_all(inputs)
            .to_directory(output)
            .run()
            .await
    });
    interrupted.entered.notified().await;
    let path = wait_for_prefix(root.path()).await;
    assert_eq!(fs::read(root.path().join("first")).unwrap(), b"abc");
    let first_inode = fs::metadata(root.path().join("first")).unwrap().ino();
    task.abort();
    let _ = task.await;
    settled(root.path()).await;
    let saved = SavedRun::inspect(&path).await.unwrap();
    assert!(!saved.is_complete());
    assert_eq!(saved.members().len(), 2);
    let old_attempt = saved
        .members()
        .nth(1)
        .unwrap()
        .attempts()
        .last()
        .unwrap()
        .path()
        .to_path_buf();
    assert_eq!(fs::read(old_attempt.join("data")).unwrap(), b"a");

    // Swap only the endpoint's live behavior, retaining the exact original reference intent. No HEAD
    // may be consulted on continuation; first completion must be reused by digest plus inode.
    interrupted.close().await;
    let address = url::Url::parse(&second_ref).unwrap();
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", address.port().unwrap()))
        .await
        .unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        loop {
            let mut byte = [0; 1];
            socket.read_exact(&mut byte).await.unwrap();
            request.push(byte[0]);
            if request.ends_with(b"\r\n\r\n") {
                break;
            }
            assert!(request.len() < 16384);
        }
        assert!(request.starts_with(b"GET "));
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nETag: \"v1\"\r\nConnection: close\r\n\r\nabc").await.unwrap();
    });
    first.reset().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&first)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&first)
        .await;
    let report = Client::new()
        .unwrap()
        .download_all([first_ref, second_ref])
        .to_directory(root.path())
        .run()
        .await
        .unwrap();
    server.await.unwrap();
    assert_eq!(report.completed().len(), 2);
    assert_eq!(report.saved_run(), Some(path.as_path()));
    assert_eq!(
        fs::metadata(root.path().join("first")).unwrap().ino(),
        first_inode
    );
    assert_eq!(fs::read(root.path().join("second")).unwrap(), b"abc");
    assert_eq!(fs::read(old_attempt.join("data")).unwrap(), b"a");
    let saved = SavedRun::inspect(path).await.unwrap();
    assert!(saved.is_complete());
    assert_eq!(saved.members().nth(1).unwrap().attempts().len(), 2);
}

async fn failed_get(root: &Fixture, server: &MockServer) -> (String, PathBuf) {
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(200).insert_header("etag", "\"old\""))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(503))
        .mount(server)
        .await;
    let reference = format!("{}/payload?secret=marker-query", server.uri());
    let error = Client::new()
        .unwrap()
        .download(reference.as_str())
        .to_directory(root.path())
        .run()
        .await
        .unwrap_err();
    assert_eq!(error.report.completed().len(), 0);
    (reference, error.report.saved_run().unwrap().to_path_buf())
}

#[tokio::test]
async fn explicit_rebind_is_real_and_missing_wrong_duplicate_and_foreign_keys_are_typed() {
    let root = Fixture::new();
    let server = MockServer::start().await;
    let (reference, path) = failed_get(&root, &server).await;
    let client = Client::new().unwrap();
    let error = client
        .resume(SavedRun::inspect(&path).await.unwrap())
        .run()
        .await
        .unwrap_err();
    assert!(matches!(error.error, Error::Saved(SavedError::NeedsInput)));
    let saved = SavedRun::inspect(&path).await.unwrap();
    let key = saved.members().next().unwrap().key();
    let error = client
        .resume(saved)
        .input(key, "https://other.invalid/payload")
        .run()
        .await
        .unwrap_err();
    assert!(matches!(error.error, Error::Saved(SavedError::Binding)));
    let saved = SavedRun::inspect(&path).await.unwrap();
    let error = client
        .resume(saved)
        .input(key, reference.as_str())
        .input(key, reference.as_str())
        .run()
        .await
        .unwrap_err();
    assert!(matches!(error.error, Error::Saved(SavedError::Binding)));
    let foreign = Fixture::new();
    let (_, foreign_path) = failed_get(&foreign, &server).await;
    let foreign_run = SavedRun::inspect(foreign_path).await.unwrap();
    let foreign_key = foreign_run.members().next().unwrap().key();
    let error = client
        .resume(SavedRun::inspect(&path).await.unwrap())
        .input(foreign_key, reference.as_str())
        .run()
        .await
        .unwrap_err();
    assert!(matches!(error.error, Error::Saved(SavedError::Binding)));
    server.reset().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(header("if-match", "\"old\""))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("etag", "\"old\"")
                .set_body_bytes(b"abc"),
        )
        .expect(1)
        .mount(&server)
        .await;
    let report = client
        .resume(SavedRun::inspect(path).await.unwrap())
        .input(key, reference.as_str())
        .run()
        .await
        .unwrap();
    assert_eq!(report.completed().len(), 1);
}

#[tokio::test]
async fn frozen_etag_refusal_does_not_rehead_or_publish() {
    let root = Fixture::new();
    let server = MockServer::start().await;
    let (reference, _) = failed_get(&root, &server).await;
    server.reset().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(200).insert_header("etag", "\"new\""))
        .expect(0)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(header("if-match", "\"old\""))
        .respond_with(ResponseTemplate::new(412))
        .expect(1)
        .mount(&server)
        .await;
    let failure = Client::new()
        .unwrap()
        .download(reference.as_str())
        .to_directory(root.path())
        .run()
        .await
        .unwrap_err();
    assert!(matches!(failure.error, Error::VersionChanged));
    assert!(!root.path().join("payload").exists());
}

#[tokio::test]
async fn complete_prior_run_is_not_automatically_matched_and_fresh_preserves_old_state() {
    let root = Fixture::new();
    let server = MockServer::start().await;
    let reference = exact(&server, "/payload").await;
    let client = Client::new().unwrap();
    let report = client
        .download(reference.as_str())
        .to_directory(root.path())
        .run()
        .await
        .unwrap();
    let old = report.saved_run().unwrap().to_path_buf();
    let failure = client
        .download(reference.as_str())
        .to_directory(root.path())
        .run()
        .await
        .unwrap_err();
    assert!(
        matches!(failure.error, Error::Transfer(ref failure) if matches!(failure.kind, FailureKind::DestinationExists))
    );
    assert_eq!(count(&server, "HEAD").await, 2);
    assert_eq!(count(&server, "GET").await, 2);
    assert_ne!(failure.report.saved_run(), Some(old.as_path()));
    let failed_path = failure.report.saved_run().unwrap().to_path_buf();
    let failure = client
        .download(reference.as_str())
        .fresh(Fresh)
        .to_directory(root.path())
        .run()
        .await
        .unwrap_err();
    assert!(matches!(failure.error, Error::Transfer(_)));
    assert!(old.exists() && failed_path.exists());
    assert_eq!(root.runs().len(), 3);
}

#[tokio::test]
async fn nohash_completed_entries_refuse_hidden_recovery_fingerprints() {
    let root = Fixture::new();
    let server = MockServer::start().await;
    let reference = exact(&server, "/payload").await;
    let client = Client::new().unwrap();
    let report = client
        .download(reference.as_str())
        .measurement(Measurement::None)
        .to_directory(root.path())
        .run()
        .await
        .unwrap();
    let saved = SavedRun::inspect(report.saved_run().unwrap())
        .await
        .unwrap();
    let key = saved.members().next().unwrap().key();
    let failure = client
        .resume(saved)
        .input(key, reference.as_str())
        .run()
        .await
        .unwrap_err();
    assert!(matches!(
        failure.error,
        Error::Saved(SavedError::NotRevalidatable)
    ));
    assert_eq!(count(&server, "GET").await, 1);
}

#[tokio::test]
async fn weak_only_measurements_do_not_become_strong_recovery_guards() {
    for algorithm in [Algorithm::Md5, Algorithm::Sha1] {
        let root = Fixture::new();
        let server = MockServer::start().await;
        let reference = exact(&server, "/payload").await;
        let client = Client::new().unwrap();
        let report = client
            .download(reference.as_str())
            .measurement(Measurement::Algorithm(algorithm))
            .to_directory(root.path())
            .run()
            .await
            .unwrap();
        let saved = SavedRun::inspect(report.saved_run().unwrap())
            .await
            .unwrap();
        let key = saved.members().next().unwrap().key();
        let failure = client
            .resume(saved)
            .input(key, reference.as_str())
            .run()
            .await
            .unwrap_err();
        assert!(matches!(
            failure.error,
            Error::Saved(SavedError::NotRevalidatable)
        ));
        assert_eq!(count(&server, "GET").await, 1);
    }
}

#[tokio::test]
async fn equal_foreign_bytes_changed_inode_and_missing_destinations_are_never_adopted_or_republished()
 {
    for missing in [false, true] {
        let root = Fixture::new();
        let server = MockServer::start().await;
        let reference = exact(&server, "/payload").await;
        let client = Client::new().unwrap();
        let report = client
            .download(reference.as_str())
            .to_directory(root.path())
            .run()
            .await
            .unwrap();
        let destination = root.path().join("payload");
        let inode = fs::metadata(&destination).unwrap().ino();
        fs::remove_file(&destination).unwrap();
        if !missing {
            fs::write(&destination, b"abc").unwrap();
            assert_ne!(fs::metadata(&destination).unwrap().ino(), inode);
        }
        let saved = SavedRun::inspect(report.saved_run().unwrap())
            .await
            .unwrap();
        let key = saved.members().next().unwrap().key();
        let failure = client
            .resume(saved)
            .input(key, reference.as_str())
            .run()
            .await
            .unwrap_err();
        assert!(matches!(
            failure.error,
            Error::Saved(SavedError::Conflict | SavedError::Uncertain)
        ));
        assert_eq!(count(&server, "GET").await, 1);
        assert_eq!(destination.exists(), !missing);
    }
}

#[tokio::test]
async fn offline_inspection_rejects_versions_corruption_bounds_symlinks_and_root_replacement_without_network()
 {
    let root = Fixture::new();
    let server = MockServer::start().await;
    let (_, path) = failed_get(&root, &server).await;
    let state = path.join("state");
    let original = fs::read(&state).unwrap();
    let mut unsupported = Vec::clone(&original);
    unsupported[8..12].copy_from_slice(&999_u32.to_le_bytes());
    fs::write(&state, unsupported).unwrap();
    assert!(matches!(
        SavedRun::inspect(&path).await,
        Err(SavedError::Version)
    ));
    fs::write(&state, b"bad").unwrap();
    assert!(matches!(
        SavedRun::inspect(&path).await,
        Err(SavedError::Corrupt)
    ));
    fs::write(&state, vec![0; super::MAX_RECORD + 1]).unwrap();
    assert!(matches!(
        SavedRun::inspect(&path).await,
        Err(SavedError::Corrupt)
    ));
    fs::write(&state, &original).unwrap();
    fs::remove_file(&state).unwrap();
    assert!(matches!(
        SavedRun::inspect(&path).await,
        Err(SavedError::Missing)
    ));
    let foreign = root.path().join("foreign");
    fs::write(&foreign, &original).unwrap();
    symlink(&foreign, &state).unwrap();
    assert!(matches!(
        SavedRun::inspect(&path).await,
        Err(SavedError::Corrupt)
    ));
    fs::remove_file(&state).unwrap();
    fs::write(&state, &original).unwrap();
    fs::set_permissions(&state, fs::Permissions::from_mode(0o600)).unwrap();
    let mut record = codec::decode(&original).unwrap();
    record.identity.inode.0 ^= 1;
    fs::write(&state, codec::encode(&record).unwrap()).unwrap();
    assert!(matches!(
        SavedRun::inspect(&path).await,
        Err(SavedError::Corrupt)
    ));
}

#[tokio::test]
async fn stored_bytes_and_default_diagnostics_do_not_persist_bearer_references() {
    let root = Fixture::new();
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(403))
        .mount(&server)
        .await;
    let authority = server.uri().trim_start_matches("http://").to_owned();
    let reference =
        format!("http://marker-user:marker-pass@{authority}/marker-path?cookie=marker-query");
    let failure = Client::new()
        .unwrap()
        .download(reference.as_str())
        .to_file(root.path().join("safe.bin"))
        .run()
        .await
        .unwrap_err();
    let path = failure.report.saved_run().unwrap();
    let bytes = fs::read(path.join("state")).unwrap();
    let saved = SavedRun::inspect(path).await.unwrap();
    let renderings = format!(
        "{failure:?} {failure} {saved:?} {:?}",
        saved.members().next().unwrap()
    );
    for marker in [
        "marker-user",
        "marker-pass",
        "marker-path",
        "marker-query",
        reference.as_str(),
    ] {
        assert!(
            !bytes
                .windows(marker.len())
                .any(|window| window == marker.as_bytes())
        );
        assert!(!renderings.contains(marker));
    }
    let mut cause: Option<&(dyn core::error::Error + 'static)> = Some(failure.as_ref());
    while let Some(error) = cause {
        assert!(!format!("{error:?} {error}").contains("marker-"));
        cause = error.source();
    }
}

#[tokio::test]
async fn snapshot_changes_and_atomic_checkpoint_failure_do_not_claim_a_pause_or_modify_the_valid_record()
 {
    let root = Fixture::new();
    let server = MockServer::start().await;
    let (_, path) = failed_get(&root, &server).await;
    let saved = SavedRun::inspect(&path).await.unwrap();
    let original = fs::read(path.join("state")).unwrap();
    let mut record = codec::decode(&original).unwrap();
    record.request = Some(Fingerprint::bytes(b"changed"));
    fs::write(path.join("state"), codec::encode(&record).unwrap()).unwrap();
    assert!(matches!(
        Owned::resume(saved).await,
        Err(SavedError::Changed)
    ));
    fs::write(path.join("state"), &original).unwrap();
    let registration = lease_directory(root.path().join(STORE)).await.unwrap();
    // An invalid target role fails before replacing it. The retained last-valid file is moved by
    // the fixture, not deleted by production cleanup, and remains independently inspectable bytes.
    fs::rename(path.join("state"), path.join("last-valid")).unwrap();
    fs::create_dir(path.join("state")).unwrap();
    assert!(
        disk::checkpoint(PathBuf::clone(&path), Vec::clone(&original), registration)
            .await
            .is_err()
    );
    assert_eq!(fs::read(path.join("last-valid")).unwrap(), original);
}

#[tokio::test]
async fn missing_native_attempt_evidence_is_uncertainty_not_permission_to_refetch() {
    let root = Fixture::new();
    let server = MockServer::start().await;
    let (reference, path) = failed_get(&root, &server).await;
    let mut record = codec::decode(&fs::read(path.join("state")).unwrap()).unwrap();
    record.members[0].attempts.push(super::Attempt {
        state: fs::canonicalize(root.path())
            .unwrap()
            .join(".xget-native-1-dead-1")
            .into(),
        status: AttemptStatus::Submitted,
    });
    fs::write(path.join("state"), codec::encode(&record).unwrap()).unwrap();
    let failure = Client::new()
        .unwrap()
        .download(reference.as_str())
        .to_directory(root.path())
        .run()
        .await
        .unwrap_err();
    assert!(matches!(failure.error, Error::Saved(SavedError::Uncertain)));
    assert_eq!(count(&server, "GET").await, 1);
}

#[tokio::test]
async fn reader_publication_receipt_and_statistics_survive_a_real_completion_checkpoint_failure() {
    fn exchange_state_role(path: &Path) {
        fs::rename(path.join("state"), path.join("last-valid")).unwrap();
        fs::create_dir(path.join("state")).unwrap();
    }
    let root = Fixture::new();
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-length", "3")
                .insert_header("etag", "\"v1\"")
                .insert_header("accept-ranges", "bytes"),
        )
        .mount(&server)
        .await;
    for (offset, byte) in b"abc".iter().enumerate() {
        Mock::given(method("GET"))
            .and(header("range", format!("bytes={offset}-{offset}")))
            .and(header("if-match", "\"v1\""))
            .respond_with(
                ResponseTemplate::new(206)
                    .insert_header("etag", "\"v1\"")
                    .insert_header("content-range", format!("bytes {offset}-{offset}/3"))
                    .set_body_bytes([*byte]),
            )
            .mount(&server)
            .await;
    }
    let client = Client::new().unwrap();
    let reference = format!("{}/payload", server.uri());
    // This fixture calls the private coordinator directly, so supply the canonical assignment that
    // the public execute path installs after creating the owned record.
    let canonical = fs::canonicalize(root.path()).unwrap();
    let plan = client
        .download(reference.as_str())
        .to_directory(&canonical)
        .plan()
        .await
        .unwrap();
    let mut owned = Owned::create(&plan, None, crate::client::Reentry::Fresh)
        .await
        .unwrap();
    owned.before_completed_checkpoint = Some(exchange_state_role);
    let failure = client.execute_owned(plan, owned).await.unwrap_err();
    assert!(matches!(failure.error, Error::Saved(SavedError::Corrupt)));
    let published = failure.report.completed().next().unwrap();
    assert_eq!(
        published.range_stats().unwrap().source,
        crate::transfer::ByteCount(3)
    );
    assert_eq!(fs::read(published.destination()).unwrap(), b"abc");
    let native_state = published.receipt().state().path().to_path_buf();
    let path = failure.report.saved_run().unwrap();
    fs::remove_dir(path.join("state")).unwrap();
    fs::rename(path.join("last-valid"), path.join("state")).unwrap();
    let saved = SavedRun::inspect(path).await.unwrap();
    let key = saved.members().next().unwrap().key();
    let report = client
        .resume(saved)
        .input(key, reference.as_str())
        .run()
        .await
        .unwrap();
    let reconciled = report.completed().next().unwrap();
    assert_eq!(reconciled.receipt().state().path(), native_state);
    assert!(reconciled.range_stats().is_none());
    assert_eq!(count(&server, "GET").await, 3);
}

#[tokio::test]
async fn a_real_checkpoint_role_failure_after_publication_retains_the_partial_report_and_native_intent()
 {
    fn exchange_state_role(path: &Path) {
        fs::rename(path.join("state"), path.join("last-valid")).unwrap();
        fs::create_dir(path.join("state")).unwrap();
    }
    let root = Fixture::new();
    let server = MockServer::start().await;
    let reference = exact(&server, "/payload").await;
    let client = Client::new().unwrap();
    let destination = fs::canonicalize(root.path()).unwrap().join("payload");
    let plan = client
        .download(reference.as_str())
        .to_file(&destination)
        .plan()
        .await
        .unwrap();
    let mut owned = Owned::create(&plan, None, crate::client::Reentry::Fresh)
        .await
        .unwrap();
    owned.before_completed_checkpoint = Some(exchange_state_role);
    let failure = client.execute_owned(plan, owned).await.unwrap_err();
    assert!(matches!(failure.error, Error::Saved(SavedError::Corrupt)));
    assert_eq!(failure.report.completed().len(), 1);
    assert_eq!(fs::read(&destination).unwrap(), b"abc");
    let path = failure.report.saved_run().unwrap();
    assert!(matches!(
        SavedRun::inspect(path).await,
        Err(SavedError::Corrupt)
    ));
    fs::remove_dir(path.join("state")).unwrap();
    fs::rename(path.join("last-valid"), path.join("state")).unwrap();
    let saved = SavedRun::inspect(path).await.unwrap();
    assert_eq!(
        saved.record.members[0].attempts[0].status,
        AttemptStatus::Submitted
    );
    let key = saved.members().next().unwrap().key();
    let report = client
        .resume(saved)
        .input(key, reference.as_str())
        .run()
        .await
        .unwrap();
    assert_eq!(report.completed().len(), 1);
    assert_eq!(count(&server, "GET").await, 1);
}

#[tokio::test]
async fn a_live_store_lease_refuses_continuation_before_network_access() {
    let root = Fixture::new();
    let server = MockServer::start().await;
    let (reference, path) = failed_get(&root, &server).await;
    let saved = SavedRun::inspect(&path).await.unwrap();
    let key = saved.members().next().unwrap().key();
    let owner = lease_directory(root.path().join(STORE)).await.unwrap();
    let failure = Client::new()
        .unwrap()
        .resume(saved)
        .input(key, reference.as_str())
        .run()
        .await
        .unwrap_err();
    assert!(matches!(failure.error, Error::Saved(SavedError::Busy)));
    assert_eq!(count(&server, "GET").await, 1);
    drop(owner);
}

#[tokio::test]
async fn reconciled_publication_enters_the_partial_report_before_a_real_checkpoint_failure() {
    let root = Fixture::new();
    let server = MockServer::start().await;
    let reference = exact(&server, "/payload").await;
    let client = Client::new().unwrap();
    let report = client
        .download(reference.as_str())
        .to_directory(root.path())
        .run()
        .await
        .unwrap();
    let original = report.completed().next().unwrap();
    let native_state = crate::transfer::StateRef::clone(original.receipt().state());
    let destination = original.destination().to_path_buf();
    let inode = fs::metadata(&destination).unwrap().ino();
    let saved = SavedRun::inspect(report.saved_run().unwrap())
        .await
        .unwrap();
    let owned = Owned::resume(saved).await.unwrap();
    let plan = owned
        .plan(vec![Some(crate::input::Input::parse(&reference).unwrap())])
        .unwrap();
    let path = owned.path().to_path_buf();
    // Native evidence is independent of this aggregate record. Make its next real replacement fail
    // after ownership, without changing or mocking native revalidation or manufacturing an error.
    fs::rename(path.join("state"), path.join("last-valid")).unwrap();
    fs::create_dir(path.join("state")).unwrap();
    let failure = client.execute_owned(plan, owned).await.unwrap_err();
    assert!(matches!(failure.error, Error::Saved(SavedError::Corrupt)));
    assert_eq!(failure.report.completed().len(), 1);
    let acknowledged = failure.report.completed().next().unwrap();
    assert_eq!(acknowledged.receipt().state(), &native_state);
    assert_eq!(acknowledged.destination(), destination);
    assert_eq!(failure.report.saved_run(), Some(path.as_path()));
    assert_eq!(fs::metadata(&destination).unwrap().ino(), inode);
    assert_eq!(fs::read(destination).unwrap(), b"abc");
    assert_eq!(count(&server, "GET").await, 1);
    assert!(path.join("last-valid").is_file());
}

#[tokio::test]
async fn stored_directory_shaped_file_spellings_are_corrupt_before_any_new_get() {
    for suffix in ["/", "/."] {
        let root = Fixture::new();
        let server = MockServer::start().await;
        let (reference, path) = failed_get(&root, &server).await;
        let mut record = codec::decode(&fs::read(path.join("state")).unwrap()).unwrap();
        let mut raw = record.members[0].destination.as_os_str().to_os_string();
        raw.push(suffix);
        record.members[0].destination = PathBuf::from(raw);
        fs::write(path.join("state"), codec::encode(&record).unwrap()).unwrap();
        assert!(matches!(
            SavedRun::inspect(&path).await,
            Err(SavedError::Corrupt)
        ));
        let failure = Client::new()
            .unwrap()
            .download(reference.as_str())
            .to_directory(root.path())
            .run()
            .await
            .unwrap_err();
        assert!(matches!(failure.error, Error::Saved(SavedError::Corrupt)));
        assert_eq!(count(&server, "GET").await, 1);
        assert!(!root.path().join("payload").exists());
    }
}

#[tokio::test]
async fn a_competing_unfinished_request_between_lookup_and_admission_refuses_the_stale_plan() {
    let root = Fixture::new();
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
    let inputs = vec![crate::input::Input::parse(&reference).unwrap()];
    let request = Fingerprint::request(
        &inputs,
        &crate::client::Target::Directory(root.path().to_path_buf()),
        crate::checks::Requirements::default(),
        Measurement::default(),
    )
    .unwrap();
    assert!(
        super::matching(root.path().to_path_buf(), request)
            .await
            .unwrap()
            .is_none()
    );
    let stale = client
        .download(reference.as_str())
        .to_directory(root.path())
        .plan()
        .await
        .unwrap();
    let competitor = client
        .download(reference.as_str())
        .to_directory(root.path())
        .run()
        .await
        .unwrap_err();
    let actual_path = competitor.report.saved_run().unwrap();
    let failure = client
        .execute_with_request(stale, Some(request), crate::client::Reentry::Continue)
        .await
        .unwrap_err();
    match failure.error {
        Error::Saved(SavedError::RequestAppeared { resume_path }) => {
            assert_eq!(resume_path.as_path(), actual_path)
        }
        other => panic!("expected actual-path admission refusal, got {other:?}"),
    }
    assert_eq!(root.runs().len(), 1);
    assert_eq!(count(&server, "GET").await, 1);
    assert_eq!(count(&server, "HEAD").await, 2);
    assert!(!root.path().join(".xget-native.lock").exists());
    assert_eq!(
        SavedRun::inspect(actual_path)
            .await
            .unwrap()
            .members()
            .next()
            .unwrap()
            .attempts()
            .len(),
        0
    );
}

#[tokio::test]
async fn valid_native_evidence_for_a_different_member_or_policy_is_not_adopted() {
    use bytes::Bytes;
    use futures::stream;

    use crate::transfer::{Length, StateRef, Transfer};
    for wrong_destination in [true, false] {
        let root = Fixture::new();
        let server = MockServer::start().await;
        let reference = exact(&server, "/payload").await;
        let client = Client::new().unwrap();
        let report = client
            .download(reference.as_str())
            .to_directory(root.path())
            .run()
            .await
            .unwrap();
        let original = report.completed().next().unwrap();
        let inode = fs::metadata(original.destination()).unwrap().ino();
        let path = report.saved_run().unwrap();
        let mut record = codec::decode(&fs::read(path.join("state")).unwrap()).unwrap();
        let other = Transfer::from_body(
            stream::iter([Ok::<_, core::convert::Infallible>(Bytes::from_static(
                b"abc",
            ))]),
            Length::Unknown,
        )
        .to_file(fs::canonicalize(root.path()).unwrap().join("other"))
        .unwrap()
        .run()
        .await
        .unwrap();
        let other_state = fs::read(other.state().path().join("state")).unwrap();
        if wrong_destination {
            record.members[0].attempts[0].state = StateRef::clone(other.state());
        } else {
            record.members[0].measurement = Measurement::None;
        }
        fs::write(path.join("state"), codec::encode(&record).unwrap()).unwrap();
        let saved = SavedRun::inspect(path).await.unwrap();
        let key = saved.members().next().unwrap().key();
        let failure = client
            .resume(saved)
            .input(key, reference.as_str())
            .run()
            .await
            .unwrap_err();
        assert!(
            matches!(failure.error, Error::Saved(SavedError::Native(ref native))
            if matches!(native.kind, FailureKind::InvalidState))
        );
        assert_eq!(failure.report.completed().len(), 0);
        assert_eq!(fs::metadata(original.destination()).unwrap().ino(), inode);
        assert_eq!(fs::read(original.destination()).unwrap(), b"abc");
        assert_eq!(fs::read(root.path().join("other")).unwrap(), b"abc");
        assert_eq!(
            fs::read(other.state().path().join("state")).unwrap(),
            other_state
        );
        assert_eq!(count(&server, "GET").await, 1);
    }
}

#[tokio::test]
async fn additional_or_conflicting_selected_requirements_are_refused_before_ownership() {
    use crate::transfer::{Digest, Provenance};
    for conflicting in [false, true] {
        let root = Fixture::new();
        let server = MockServer::start().await;
        let (reference, path) = failed_get(&root, &server).await;
        let client = Client::new().unwrap();
        let mut selected = client
            .resolve(reference.as_str())
            .await
            .unwrap()
            .select_one(|_| true)
            .unwrap();
        let supplied: Digest =
            "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
                .parse()
                .unwrap();
        selected
            .selected
            .description
            .requirements
            .insert(supplied, Provenance::Source)
            .unwrap();
        if conflicting {
            let mut record = codec::decode(&fs::read(path.join("state")).unwrap()).unwrap();
            record.members[0]
                .requirements
                .insert(
                    Digest::parse(Algorithm::Sha256, &"00".repeat(32)).unwrap(),
                    Provenance::Caller,
                )
                .unwrap();
            fs::write(path.join("state"), codec::encode(&record).unwrap()).unwrap();
        }
        let saved = SavedRun::inspect(&path).await.unwrap();
        let key = saved.members().next().unwrap().key();
        let owner = lease_directory(root.path().join(STORE)).await.unwrap();
        let failure = client
            .resume(saved)
            .input(key, selected)
            .run()
            .await
            .unwrap_err();
        assert!(matches!(failure.error, Error::Saved(SavedError::Binding)));
        assert_eq!(count(&server, "GET").await, 1);
        drop(owner);
    }
}

#[tokio::test]
async fn stored_member_attempt_path_and_validator_bounds_fail_before_execution_authority() {
    let root = Fixture::new();
    let server = MockServer::start().await;
    let (_, path) = failed_get(&root, &server).await;
    let bytes = fs::read(path.join("state")).unwrap();
    let mut record = codec::decode(&bytes).unwrap();
    record.members[0].destination = PathBuf::from("x".repeat(4097));
    assert!(matches!(codec::encode(&record), Err(SavedError::Limit)));
    let mut record = codec::decode(&bytes).unwrap();
    record.members[0].etag = Some(zeroize::Zeroizing::new("x".repeat(8193)));
    assert!(matches!(codec::encode(&record), Err(SavedError::Limit)));
    let mut record = codec::decode(&bytes).unwrap();
    for index in 0..=super::MAX_ATTEMPTS {
        record.members[0].attempts.push(super::Attempt {
            state: root
                .path()
                .join(format!(".xget-native-1-{index:x}-1"))
                .into(),
            status: AttemptStatus::Retained,
        });
    }
    assert!(matches!(codec::encode(&record), Err(SavedError::Limit)));
    let mut record = codec::decode(&bytes).unwrap();
    let member = record.members.pop().unwrap();
    record.members = (0..=crate::inventory::MAX_FILES)
        .map(|index| super::Member {
            occurrence: crate::input::InputOccurrence(index as u32),
            remote: None,
            binding: member.binding,
            destination: root.path().join(format!("file-{index}")),
            length: member.length,
            etag: None,
            requirements: member.requirements,
            measurement: member.measurement,
            mode: crate::client::range::Mode::Whole,
            attempts: Vec::new(),
        })
        .collect();
    assert!(matches!(codec::encode(&record), Err(SavedError::Limit)));
}

#[tokio::test]
async fn changed_selected_range_advertisement_cannot_bypass_an_unanchored_run() {
    use core::num::NonZeroUsize;
    use std::sync::Arc;

    let root = Fixture::new();
    let server = MockServer::start().await;
    let heads = Arc::new(AtomicU64::new(0));
    let observed = Arc::clone(&heads);
    Mock::given(method("HEAD"))
        .respond_with(move |_: &wiremock::Request| {
            let response = ResponseTemplate::new(200)
                .insert_header("content-length", "3")
                .insert_header("etag", "\"v1\"");
            if observed.fetch_add(1, Ordering::Relaxed) == 0 {
                response.insert_header("accept-ranges", "bytes")
            } else {
                response
            }
        })
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;
    let client = Client::builder()
        .max_requests(NonZeroUsize::MIN)
        .build()
        .unwrap();
    let reference = format!("{}/payload", server.uri());
    let file = client
        .resolve(crate::Input::parse(&reference).unwrap())
        .await
        .unwrap()
        .select_one(|_| true)
        .unwrap();
    let failure = client
        .download(file)
        .measurement(Measurement::None)
        .to_directory(root.path())
        .run()
        .await
        .unwrap_err();
    assert!(matches!(failure.error, Error::RangeTransfer(_)));
    let saved = failure.report.saved_run().unwrap().to_path_buf();
    let reads = count(&server, "GET").await;
    let file = client
        .resolve(crate::Input::parse(&reference).unwrap())
        .await
        .unwrap()
        .select_one(|_| true)
        .unwrap();
    let failure = client
        .download(file)
        .measurement(Measurement::None)
        .to_directory(root.path())
        .run()
        .await
        .unwrap_err();
    assert!(
        matches!(
            failure.error,
            Error::RangeRequest(crate::transfer::reader::ReaderRequestError::NoResumeAnchor)
        ),
        "{failure:?}"
    );
    assert_eq!(failure.report.saved_run(), Some(saved.as_path()));
    assert_eq!(count(&server, "GET").await, reads);
    assert_eq!(root.runs().len(), 1);
}

#[tokio::test]
async fn old_selected_whole_runs_keep_their_mode_when_new_metadata_advertises_ranges() {
    use std::sync::Arc;

    for version in [1, 2] {
        for explicit in [false, true] {
            let root = Fixture::new();
            let server = MockServer::start().await;
            Mock::given(method("HEAD"))
                .respond_with(
                    ResponseTemplate::new(200)
                        .insert_header("content-length", "3")
                        .insert_header("etag", "\"v1\"")
                        .insert_header("accept-ranges", "bytes"),
                )
                .expect(2)
                .mount(&server)
                .await;
            let gets = Arc::new(AtomicU64::new(0));
            let observed = Arc::clone(&gets);
            Mock::given(method("GET"))
                .respond_with(move |request: &wiremock::Request| {
                    if request.headers.contains_key("range") {
                        return ResponseTemplate::new(400);
                    }
                    let status = if observed.fetch_add(1, Ordering::Relaxed) == 0 {
                        503
                    } else {
                        200
                    };
                    ResponseTemplate::new(status)
                        .insert_header("etag", "\"v1\"")
                        .set_body_bytes(b"abc")
                })
                .expect(2)
                .mount(&server)
                .await;
            let client = Client::new().unwrap();
            let reference = format!("{}/payload", server.uri());
            let mut file = client
                .resolve(crate::Input::parse(&reference).unwrap())
                .await
                .unwrap()
                .select_one(|_| true)
                .unwrap();
            // Accepted V1/V2 selected Files did not carry this newly introduced capability hint.
            file.input.ranges = crate::input::RangeSupport::Unknown;
            let failure = client
                .download(file)
                .to_directory(root.path())
                .run()
                .await
                .unwrap_err();
            let saved_path = failure.report.saved_run().unwrap().to_path_buf();
            let mut record = codec::decode(&fs::read(saved_path.join("state")).unwrap()).unwrap();
            record.version = version;
            fs::write(saved_path.join("state"), codec::encode(&record).unwrap()).unwrap();
            let file = client
                .resolve(crate::Input::parse(&reference).unwrap())
                .await
                .unwrap()
                .select_one(|_| true)
                .unwrap();
            let report = if explicit {
                let saved = SavedRun::inspect(&saved_path).await.unwrap();
                let key = saved.members().next().unwrap().key();
                client.resume(saved).input(key, file).run().await.unwrap()
            } else {
                client
                    .download(file)
                    .to_directory(root.path())
                    .run()
                    .await
                    .unwrap()
            };
            assert_eq!(report.saved_run(), Some(saved_path.as_path()));
            let published = report.completed().next().unwrap();
            assert!(published.range_stats().is_none());
            assert_eq!(fs::read(published.destination()).unwrap(), b"abc");
            assert_eq!(
                SavedRun::inspect(&saved_path).await.unwrap().record.version,
                version
            );
            assert_eq!(root.runs().len(), 1);
        }
    }
}

#[tokio::test]
async fn reader_mode_is_a_v3_extension_and_older_records_keep_their_whole_body_meaning() {
    use crate::client::range::Mode;
    use crate::transfer::{ByteCount, Length};

    let root = Fixture::new();
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-length", "3")
                .insert_header("etag", "\"v1\"")
                .insert_header("accept-ranges", "bytes"),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("etag", "\"v1\"")
                .set_body_bytes(b"abc"),
        )
        .expect(1)
        .mount(&server)
        .await;
    let reference = format!("{}/payload", server.uri());
    let client = Client::new().unwrap();
    let plan = client
        .download(reference.as_str())
        .to_directory(root.path())
        .plan()
        .await
        .unwrap();
    let owned = Owned::create(&plan, None, crate::client::Reentry::Fresh)
        .await
        .unwrap();
    let path = owned.path().to_path_buf();
    drop(owned);
    let bytes = fs::read(path.join("state")).unwrap();
    let mut record = codec::decode(&bytes).unwrap();
    assert_eq!(record.version, 3);
    assert_eq!(record.members[0].mode, Mode::Ranges);
    assert_eq!(codec::encode(&record).unwrap(), bytes);
    record.version = 2;
    assert!(matches!(codec::encode(&record), Err(SavedError::Corrupt)));
    record.version = 3;
    record.members[0].length = Length::Unknown;
    assert!(matches!(codec::encode(&record), Err(SavedError::Corrupt)));
    record.members[0].length = Length::Known(ByteCount(3));
    record.members[0].etag = None;
    assert!(matches!(codec::encode(&record), Err(SavedError::Corrupt)));
    let mut record = codec::decode(&bytes).unwrap();
    record.version = 2;
    record.members[0].mode = Mode::Whole;
    let old_bytes = codec::encode(&record).unwrap();
    assert_eq!(
        codec::encode(&codec::decode(&old_bytes).unwrap()).unwrap(),
        old_bytes
    );
    fs::write(path.join("state"), &old_bytes).unwrap();
    let saved = SavedRun::inspect(&path).await.unwrap();
    let key = saved.members().next().unwrap().key();
    let report = client
        .resume(saved)
        .input(key, reference.as_str())
        .run()
        .await
        .unwrap();
    assert!(report.completed().next().unwrap().range_stats().is_none());
    let stored = codec::decode(&fs::read(path.join("state")).unwrap()).unwrap();
    assert_eq!(stored.version, 2);
    assert_eq!(stored.members[0].mode, Mode::Whole);
    assert_eq!(count(&server, "HEAD").await, 1);
    for request in server.received_requests().await.unwrap() {
        assert!(!request.headers.contains_key("range"));
    }
}

#[tokio::test]
async fn version_one_exact_runs_remain_readable_and_checkpoint_as_version_one() {
    let root = Fixture::new();
    let server = MockServer::start().await;
    let (reference, path) = failed_get(&root, &server).await;
    let mut record = codec::decode(&fs::read(path.join("state")).unwrap()).unwrap();
    record.version = 1;
    let version_one = codec::encode(&record).unwrap();
    assert_eq!(&version_one[8..12], &1_u32.to_le_bytes());
    fs::write(path.join("state"), &version_one).unwrap();
    assert_eq!(
        codec::encode(&codec::decode(&version_one).unwrap()).unwrap(),
        version_one
    );
    server.reset().await;
    Mock::given(method("GET"))
        .and(header("if-match", "\"old\""))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("etag", "\"old\"")
                .set_body_bytes(b"abc"),
        )
        .expect(1)
        .mount(&server)
        .await;
    let report = Client::new()
        .unwrap()
        .download(reference)
        .to_directory(root.path())
        .run()
        .await
        .unwrap();
    assert_eq!(report.completed().len(), 1);
    assert_eq!(count(&server, "HEAD").await, 0);
    let bytes = fs::read(path.join("state")).unwrap();
    assert_eq!(&bytes[8..12], &1_u32.to_le_bytes());
}

#[tokio::test]
async fn version_two_mapping_rejects_duplicate_exact_occurrences_before_authority() {
    let root = Fixture::new();
    let server = MockServer::start().await;
    let (_, path) = failed_get(&root, &server).await;
    let mut record = codec::decode(&fs::read(path.join("state")).unwrap()).unwrap();
    assert_eq!(record.version, 2);
    let first = &record.members[0];
    record.members.push(super::Member {
        occurrence: first.occurrence,
        remote: None,
        binding: first.binding,
        destination: root.path().join("another"),
        length: first.length,
        etag: None,
        requirements: first.requirements,
        measurement: first.measurement,
        mode: crate::client::range::Mode::Whole,
        attempts: Vec::new(),
    });
    assert!(matches!(codec::encode(&record), Err(SavedError::Corrupt)));
}

#[test]
fn malformed_private_records_are_bounded_and_normalized() {
    for bytes in [
        Vec::new(),
        b"XGETRUN\0".to_vec(),
        vec![0xff; super::MAX_RECORD + 1],
    ] {
        assert!(matches!(codec::decode(&bytes), Err(SavedError::Corrupt)));
    }
}

#[test]
fn native_owned_http_run_and_resume_futures_are_send() {
    fn send<T: Send>(_: T) {}
    let client = Client::new().unwrap();
    send(
        client
            .download("https://example.org/payload")
            .to_file("safe.bin")
            .run(),
    );
    send(SavedRun::inspect("downloads/.xget-runs/run-1"));
    // Concrete native HTTP components are Send. No bound is imposed on generic body/local errors.
    fn resume_send(client: &Client, saved: SavedRun) {
        send(client.resume(saved).run());
    }
    let _check = resume_send;
}
