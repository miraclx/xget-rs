//! Real loopback source fixtures only. These authored tests are not execution evidence.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use core::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use core::time::Duration;
use std::os::unix::fs::DirBuilderExt as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::{fs, io};

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::{Barrier, Notify, oneshot};
use wiremock::matchers::{header, method};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::Mode;
use crate::checksum::OwnedSha256;
use crate::client::{Client, Error, Fresh, SavedError, SavedRun};
use crate::transfer::reader::ReaderError;
use crate::transfer::{Algorithm, ByteCount, Digest, FailureKind, Measurement, lease_directory};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "xget-http-reader-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
        Self(fs::canonicalize(path).unwrap())
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

#[derive(Clone, Copy, Debug)]
enum Fault {
    None,
    Ignored,
    Start,
    End,
    Total,
    Changed,
    Missing,
    Encoded,
    Declared,
    Short,
    Overlong,
    Late,
    HeldHead,
}

struct Fixture {
    reference: String,
    // The mutex owns only the fixture's bounded request log, never socket I/O or an await.
    requests: Arc<Mutex<Vec<String>>>,
    resumed: Arc<AtomicBool>,
    tails: Arc<Barrier>,
    head: Arc<Notify>,
    stop: Option<oneshot::Sender<()>>,
    worker: Option<tokio::task::JoinHandle<()>>,
}
impl Fixture {
    async fn new(size: usize, fault: Fault) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let reference = format!("http://{}/payload", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let resumed = Arc::new(AtomicBool::new(false));
        // Three tail responses and the test rendezvous before the head is allowed to send bytes.
        let tails = Arc::new(Barrier::new(4));
        let head = Arc::new(Notify::new());
        let log = Arc::clone(&requests);
        let phase = Arc::clone(&resumed);
        let arrived = Arc::clone(&tails);
        let release = Arc::clone(&head);
        let (stop, mut stopped) = oneshot::channel();
        let worker = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        let (socket, _) = result.unwrap();
                        let log = Arc::clone(&log);
                        let phase = Arc::clone(&phase);
                        let arrived = Arc::clone(&arrived);
                        let release = Arc::clone(&release);
                        connections.spawn(async move {
                            // A rejected/cancelled response can close its socket while this fixture
                            // writes. Protocol assertions still panic and are joined by the owner.
                            let _ = serve(socket, size, fault, log, phase, arrived, release).await;
                        });
                    }
                    result = connections.join_next(), if !connections.is_empty() => {
                        if let Some(Err(error)) = result { panic!("fixture handler failed: {error}"); }
                    }
                    _ = &mut stopped => break,
                }
            }
            connections.abort_all();
            while let Some(result) = connections.join_next().await {
                if let Err(error) = result {
                    assert!(error.is_cancelled(), "fixture handler failed: {error}");
                }
            }
        });
        Self {
            reference,
            requests,
            resumed,
            tails,
            head,
            stop: Some(stop),
            worker: Some(worker),
        }
    }
    fn requests(&self) -> Vec<String> {
        Vec::clone(&self.requests.lock().unwrap())
    }
    fn reads(&self) -> Vec<(u64, u64)> {
        self.requests()
            .iter()
            .filter(|request| request.starts_with("get "))
            .map(|request| requested(request))
            .collect()
    }
    async fn finish(mut self) {
        let _ = self.stop.take().unwrap().send(());
        self.worker.take().unwrap().await.unwrap();
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(worker) = self.worker.take() {
            worker.abort();
        }
    }
}

fn requested(request: &str) -> (u64, u64) {
    let line = request
        .lines()
        .find_map(|line| line.strip_prefix("range: bytes="))
        .expect("every payload request must use an explicit byte range");
    let (start, end) = line.split_once('-').unwrap();
    (start.parse().unwrap(), end.parse::<u64>().unwrap() + 1)
}

async fn chunk(socket: &mut tokio::net::TcpStream, bytes: &[u8]) -> io::Result<()> {
    socket
        .write_all(format!("{:x}\r\n", bytes.len()).as_bytes())
        .await?;
    socket.write_all(bytes).await?;
    socket.write_all(b"\r\n").await
}

async fn serve(
    mut socket: tokio::net::TcpStream,
    size: usize,
    fault: Fault,
    requests: Arc<Mutex<Vec<String>>>,
    resumed: Arc<AtomicBool>,
    tails: Arc<Barrier>,
    head: Arc<Notify>,
) -> io::Result<()> {
    let mut bytes = Vec::new();
    loop {
        let mut byte = [0; 1];
        socket.read_exact(&mut byte).await?;
        bytes.push(byte[0]);
        assert!(bytes.len() < 16384);
        if bytes.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    let request = String::from_utf8(bytes).unwrap().to_ascii_lowercase();
    requests.lock().unwrap().push(String::clone(&request));
    if request.starts_with("head ") {
        return socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {size}\r\nETag: \"v1\"\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n").as_bytes()).await;
    }
    assert!(request.starts_with("get "));
    // Log casing is normalized above, so request method matching uses lower case too.
    let (start, end) = requested(&request);
    assert!(request.contains("if-match: \"v1\"\r\n"));
    assert!(request.contains("accept-encoding: identity\r\n"));
    assert!(!request.contains("if-range:"));
    let fault = if resumed.load(Ordering::Acquire) {
        Fault::None
    } else {
        fault
    };
    let status = if matches!(fault, Fault::Ignored) {
        200
    } else {
        206
    };
    let actual_start = start + u64::from(matches!(fault, Fault::Start));
    let actual_end = end - 1 + u64::from(matches!(fault, Fault::End));
    let total = size as u64 + u64::from(matches!(fault, Fault::Total));
    let tag = match fault {
        Fault::Changed => "ETag: \"v2\"\r\n",
        Fault::Missing => "",
        _ => "ETag: \"v1\"\r\n",
    };
    let framing = if matches!(fault, Fault::Declared) {
        format!("Content-Length: {}\r\n", end - start + 1)
    } else {
        "Transfer-Encoding: chunked\r\n".to_owned()
    };
    let encoding = if matches!(fault, Fault::Encoded) {
        "Content-Encoding: gzip\r\n"
    } else {
        ""
    };
    socket.write_all(format!("HTTP/1.1 {status} fixture\r\nContent-Range: bytes {actual_start}-{actual_end}/{total}\r\n{tag}{framing}{encoding}Connection: close\r\n\r\n").as_bytes()).await?;
    let body = vec![b'a'; (end - start) as usize];
    if matches!(fault, Fault::HeldHead) {
        if start != 0 {
            chunk(&mut socket, &body).await?;
            socket.write_all(b"0\r\n\r\n").await?;
            tails.wait().await;
            return Ok(());
        }
        head.notified().await;
        chunk(&mut socket, &body[..body.len() / 2]).await?;
        // The test drops the client after observing an actual native prefix checkpoint.
        core::future::pending::<()>().await;
    }
    if matches!(fault, Fault::Declared) {
        return socket.write_all(&body).await;
    }
    if !matches!(fault, Fault::Short) {
        chunk(&mut socket, &body).await?;
    }
    if matches!(fault, Fault::Overlong) {
        chunk(&mut socket, b"!").await?;
    }
    if matches!(fault, Fault::Late) {
        // All requested data has arrived, but the chunked framing fails instead of clean EOF.
        return socket.write_all(b"invalid-chunk-size\r\n").await;
    }
    socket.write_all(b"0\r\n\r\n").await
}

fn expected(size: usize) -> Digest {
    let mut hash = OwnedSha256::default();
    hash.update(&vec![b'a'; size]);
    Digest::sha256(hash.finish())
}

#[tokio::test]
async fn exact_http_uses_multiple_conditional_windows_and_keeps_physical_statistics() {
    let server = Fixture::new(12, Fault::None).await;
    let root = Directory::new();
    let report = Client::new()
        .unwrap()
        .download(server.reference.as_str())
        .to_directory(root.path())
        .run()
        .await
        .unwrap();
    let published = report.completed().next().unwrap();
    assert_eq!(fs::read(published.destination()).unwrap(), vec![b'a'; 12]);
    assert_eq!(server.reads().len(), 4);
    let stats = published.range_stats().unwrap();
    assert_eq!(stats.restored, ByteCount(0));
    assert_eq!(stats.source, ByteCount(12));
    assert_eq!(stats.written, ByteCount(12));
    assert_eq!(published.receipt().partial().hashed, ByteCount(12));
    assert_eq!(
        server
            .requests()
            .iter()
            .filter(|request| request.starts_with("head "))
            .count(),
        1
    );
    server.finish().await;
}

#[tokio::test]
async fn one_admission_slot_still_drains_all_four_range_windows() {
    use core::num::NonZeroUsize;

    let server = Fixture::new(12, Fault::None).await;
    let root = Directory::new();
    let client = Client::builder()
        .max_requests(NonZeroUsize::MIN)
        .build()
        .unwrap();
    let report = tokio::time::timeout(
        Duration::from_secs(5),
        client
            .download(server.reference.as_str())
            .expect(expected(12))
            .to_directory(root.path())
            .run(),
    )
    .await
    .unwrap()
    .unwrap();
    let published = report.completed().next().unwrap();
    assert_eq!(published.range_stats().unwrap().source, ByteCount(12));
    assert_eq!(server.reads().len(), 4);
    assert_eq!(fs::read(published.destination()).unwrap(), vec![b'a'; 12]);
    server.finish().await;
}

#[tokio::test]
async fn wrong_windows_ignored_ranges_bad_encoding_and_terminal_errors_never_publish_or_fall_back()
{
    for fault in [
        Fault::Ignored,
        Fault::Start,
        Fault::End,
        Fault::Total,
        Fault::Changed,
        Fault::Missing,
        Fault::Encoded,
        Fault::Declared,
        Fault::Short,
        Fault::Overlong,
        Fault::Late,
    ] {
        let server = Fixture::new(12, fault).await;
        let root = Directory::new();
        let failure = Client::new()
            .unwrap()
            .download(server.reference.as_str())
            .expect(expected(12))
            .to_directory(root.path())
            .run()
            .await
            .unwrap_err();
        assert!(
            matches!(failure.error, Error::RangeTransfer(_)),
            "{fault:?}: {failure:?}"
        );
        assert_eq!(failure.report.completed().len(), 0);
        assert!(!root.path().join("payload").exists(), "{fault:?}");
        assert!(!server.reads().is_empty());
        // requested() asserts that every GET in this log is a range, including rejected responses.
        assert_eq!(
            server
                .requests()
                .iter()
                .filter(|request| request.starts_with("head "))
                .count(),
            1
        );
        server.finish().await;
    }
}

#[tokio::test]
async fn known_pin_mismatch_refuses_even_when_the_required_content_would_match() {
    let server = Fixture::new(12, Fault::Changed).await;
    let root = Directory::new();
    let failure = Client::new()
        .unwrap()
        .download(server.reference.as_str())
        .expect(expected(12))
        .to_directory(root.path())
        .run()
        .await
        .unwrap_err();
    assert!(matches!(failure.error, Error::RangeTransfer(ref failure)
        if matches!(failure.kind, FailureKind::Body(ReaderError::Source(Error::VersionChanged)))));
    assert!(!root.path().join("payload").exists());
    server.finish().await;
}

async fn native_state(root: &Path) -> (PathBuf, PathBuf) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(entries) = fs::read_dir(root.join(".xget-runs")) {
                for entry in entries.flatten() {
                    if let Ok(saved) = SavedRun::inspect(entry.path()).await
                        && let Some(state) = saved
                            .members()
                            .next()
                            .and_then(|member| member.attempts().last().cloned())
                        && state.path().join("state").is_file()
                    {
                        return (saved.path().to_path_buf(), state.path().to_path_buf());
                    }
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("submitted native intent and initial record")
}

async fn settled(root: &Path) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match lease_directory(root.join(".xget-runs")).await {
                Ok(owner) => {
                    drop(owner);
                    return;
                }
                Err(failure) if matches!(failure.kind, FailureKind::Busy) => {
                    tokio::task::yield_now().await
                }
                Err(failure) => panic!("unexpected lease result: {failure:?}"),
            }
        }
    })
    .await
    .expect("submitted native work settles after drop");
}

#[tokio::test]
async fn tails_before_a_partial_head_checkpoint_reenter_as_only_suffix_windows_without_a_new_head()
{
    for required in [false, true] {
        const SIZE: usize = 256 * 1024;
        let server = Fixture::new(SIZE, Fault::HeldHead).await;
        let root = Directory::new();
        let client = Client::new().unwrap();
        let mut download = client.download(server.reference.as_str());
        if required {
            download = download
                .expect(expected(SIZE))
                .measurement(Measurement::None);
        }
        let initial_events = std::rc::Rc::new(core::cell::RefCell::new(Vec::new()));
        let mut initial_observer = crate::client::tests::Capture(&initial_events);
        let mut run = Box::pin(
            download
                .to_directory(root.path())
                .run_with(&mut initial_observer),
        );
        tokio::select! {
            result = &mut run => panic!("head must remain withheld: {result:?}"),
            _ = server.tails.wait() => {},
        }
        let (aggregate, native) = native_state(root.path()).await;
        let before = fs::read(native.join("state")).unwrap();
        assert!(!root.path().join("payload").exists());
        server.head.notify_one();
        tokio::select! {
            result = &mut run => panic!("partial head must not finish: {result:?}"),
            result = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if fs::read(native.join("state")).is_ok_and(|bytes| bytes != before) { break; }
                    tokio::task::yield_now().await;
                }
            }) => result.expect("contiguous head is checkpointed while its body remains open"),
        }
        drop(run);
        assert!(!initial_events.borrow().iter().any(|event| matches!(
            event,
            crate::client::tests::Observed::Completed(_)
                | crate::client::tests::Observed::Failed
                | crate::client::tests::Observed::Published { .. }
        )));
        settled(root.path()).await;
        let retained = crate::transfer::StateRef::from(native.to_path_buf())
            .reconcile()
            .await
            .unwrap();
        let crate::transfer::Reconciliation::NeedsBody { partial, .. } = retained else {
            panic!("withheld head must retain receiving evidence");
        };
        let anchored = partial.hashed;
        assert!(anchored > ByteCount(0) && anchored <= ByteCount(32 * 1024));
        let old_reads = server.reads().len();
        server.resumed.store(true, Ordering::Release);
        let mut download = client.download(server.reference.as_str());
        if required {
            download = download
                .expect(expected(SIZE))
                .measurement(Measurement::None);
        }
        let events = std::rc::Rc::new(core::cell::RefCell::new(Vec::new()));
        let mut observer = crate::client::tests::Capture(&events);
        let report = download
            .to_directory(root.path())
            .run_with(&mut observer)
            .await
            .unwrap();
        assert_eq!(report.saved_run(), Some(aggregate.as_path()));
        let published = report.completed().next().unwrap();
        let stats = published.range_stats().unwrap();
        // TCP/HTTP delivery may split the fixture's one write into smaller checker chunks.
        assert_eq!(stats.restored, anchored);
        assert_eq!(stats.source.0 + stats.restored.0, SIZE as u64);
        assert_eq!(stats.written, stats.source);
        assert!(events.borrow().iter().any(|event| matches!(event,
            crate::client::tests::Observed::Progress { facts, .. }
                if facts.restored() == anchored && facts.received() == anchored
                    && facts.written() == anchored && facts.hashed() == anchored
                    && facts.attempt_received() == ByteCount(0)
                    && facts.attempt_written() == ByteCount(0))));
        assert!(events.borrow().iter().any(|event| matches!(event,
            crate::client::tests::Observed::Progress { facts, .. }
                if facts.restored() == stats.restored && facts.attempt_received() == stats.source
                    && facts.attempt_written() == stats.written
                    && facts.hashed() == ByteCount(SIZE as u64))));
        assert_eq!(
            events
                .borrow()
                .iter()
                .filter(|event| matches!(event, crate::client::tests::Observed::Started { .. }))
                .count(),
            1
        );
        assert_eq!(
            events
                .borrow()
                .iter()
                .filter(|event| matches!(event, crate::client::tests::Observed::Completed(1)))
                .count(),
            1
        );
        assert_eq!(
            events.borrow().last(),
            Some(&crate::client::tests::Observed::Completed(1))
        );
        assert_eq!(
            published.receipt().partial().written,
            ByteCount(SIZE as u64)
        );
        assert_eq!(published.receipt().state().path(), native);
        assert_eq!(fs::read(published.destination()).unwrap(), vec![b'a'; SIZE]);
        assert!(
            server.reads()[old_reads..]
                .iter()
                .all(|(start, _)| *start >= stats.restored.0)
        );
        assert_eq!(
            server
                .requests()
                .iter()
                .filter(|request| request.starts_with("head "))
                .count(),
            1
        );
        let saved = SavedRun::inspect(&aggregate).await.unwrap();
        assert!(saved.is_complete());
        assert_eq!(saved.members().next().unwrap().attempts().len(), 1);
        server.finish().await;
    }
}

#[tokio::test]
async fn weak_and_disabled_hashes_can_transfer_pinned_ranges_but_cannot_invent_recovery_anchors() {
    for measurement in [
        Measurement::None,
        Measurement::Algorithm(Algorithm::Md5),
        Measurement::Algorithm(Algorithm::Sha1),
    ] {
        let server = Fixture::new(12, Fault::None).await;
        let root = Directory::new();
        let client = Client::new().unwrap();
        let report = client
            .download(server.reference.as_str())
            .measurement(measurement)
            .to_directory(root.path())
            .run()
            .await
            .unwrap();
        let file = report.completed().next().unwrap();
        assert_eq!(file.range_stats().unwrap().source, ByteCount(12));
        if measurement == Measurement::None {
            assert_eq!(file.receipt().partial().hashed, ByteCount(0));
        }
        let saved = SavedRun::inspect(report.saved_run().unwrap())
            .await
            .unwrap();
        let key = saved.members().next().unwrap().key();
        let reads = server.reads().len();
        let failure = client
            .resume(saved)
            .input(key, server.reference.as_str())
            .run()
            .await
            .unwrap_err();
        assert!(matches!(
            failure.error,
            Error::Saved(SavedError::NotRevalidatable)
        ));
        assert_eq!(server.reads().len(), reads);
        server.finish().await;
    }
}

#[tokio::test]
async fn a_sealed_reader_with_a_failed_requirement_is_never_reopened_writable_or_refetched() {
    let server = Fixture::new(12, Fault::None).await;
    let root = Directory::new();
    let client = Client::new().unwrap();
    let wrong = Digest::sha256([0; 32]);
    let failure = client
        .download(server.reference.as_str())
        .expect(wrong)
        .to_directory(root.path())
        .run()
        .await
        .unwrap_err();
    let Error::RangeTransfer(native_failure) = failure.error else {
        panic!("expected range failure");
    };
    assert!(matches!(
        native_failure.kind,
        FailureKind::DigestMismatch(_)
    ));
    let native = native_failure.state.unwrap();
    let data = fs::read(native.path().join("data")).unwrap();
    let record = fs::read(native.path().join("state")).unwrap();
    let reads = server.reads().len();
    let failure = client
        .download(server.reference.as_str())
        .expect(wrong)
        .to_directory(root.path())
        .run()
        .await
        .unwrap_err();
    assert!(
        matches!(failure.error, Error::RangeTransfer(ref failure) if matches!(failure.kind, FailureKind::InvalidState))
    );
    assert_eq!(server.reads().len(), reads);
    assert_eq!(
        server
            .requests()
            .iter()
            .filter(|request| request.starts_with("head "))
            .count(),
        1
    );
    assert_eq!(fs::read(native.path().join("data")).unwrap(), data);
    assert_eq!(fs::read(native.path().join("state")).unwrap(), record);
    assert!(!root.path().join("payload").exists());
    server.finish().await;
}

#[tokio::test]
async fn unsupported_unknown_size_unpinned_and_zero_size_metadata_keep_whole_body_execution() {
    for (length, etag, ranges) in [
        (Some("3"), Some("\"v1\""), None),
        (None, Some("\"v1\""), Some("bytes")),
        (Some("3"), None, Some("bytes")),
        (Some("0"), Some("\"v1\""), Some("bytes")),
    ] {
        let server = MockServer::start().await;
        let mut head = ResponseTemplate::new(200);
        if let Some(length) = length {
            head = head.insert_header("content-length", length);
        }
        if let Some(etag) = etag {
            head = head.insert_header("etag", etag);
        }
        if let Some(ranges) = ranges {
            head = head.insert_header("accept-ranges", ranges);
        }
        Mock::given(method("HEAD"))
            .respond_with(head)
            .expect(1)
            .mount(&server)
            .await;
        let body: &[u8] = if length == Some("0") { b"" } else { b"abc" };
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"v1\"")
                    .set_body_bytes(body),
            )
            .expect(1)
            .mount(&server)
            .await;
        let root = Directory::new();
        let report = Client::new()
            .unwrap()
            .download(format!("{}/payload", server.uri()))
            .to_directory(root.path())
            .run()
            .await
            .unwrap();
        assert!(report.completed().next().unwrap().range_stats().is_none());
        for request in server.received_requests().await.unwrap() {
            assert!(!request.headers.contains_key("range"));
        }
    }
}

#[tokio::test]
async fn live_open_stays_one_whole_get_even_with_explicit_range_support() {
    use futures::StreamExt as _;
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
    let client = Client::new().unwrap();
    let opened = client
        .open(format!("{}/payload", server.uri()))
        .await
        .unwrap();
    let mut body = opened.into_body();
    assert_eq!(body.next().await.unwrap().unwrap(), b"abc".as_slice());
    assert!(body.next().await.is_none());
    for request in server.received_requests().await.unwrap() {
        assert!(!request.headers.contains_key("range"));
    }
}

#[tokio::test]
async fn unanchored_receiving_ranges_refuse_reuse_and_fresh_keeps_the_old_attempt_untouched() {
    for measurement in [
        Measurement::None,
        Measurement::Algorithm(Algorithm::Md5),
        Measurement::Algorithm(Algorithm::Sha1),
    ] {
        const SIZE: usize = 256 * 1024;
        let server = Fixture::new(SIZE, Fault::HeldHead).await;
        let root = Directory::new();
        let client = Client::new().unwrap();
        let mut run = Box::pin(
            client
                .download(server.reference.as_str())
                .measurement(measurement)
                .to_directory(root.path())
                .run(),
        );
        tokio::select! {
            result = &mut run => panic!("head must remain withheld: {result:?}"),
            _ = server.tails.wait() => {},
        }
        let (aggregate, native) = native_state(root.path()).await;
        server.head.notify_one();
        tokio::select! {
            result = &mut run => panic!("head must remain partial: {result:?}"),
            result = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if fs::read(native.join("data")).is_ok_and(|bytes| bytes.starts_with(&vec![b'a'; 32 * 1024])) { break; }
                    tokio::task::yield_now().await;
                }
            }) => result.expect("actual settled head prefix"),
        }
        drop(run);
        settled(root.path()).await;
        let data = fs::read(native.join("data")).unwrap();
        let record = fs::read(native.join("state")).unwrap();
        let reads = server.reads().len();
        server.resumed.store(true, Ordering::Release);
        let failure = client
            .download(server.reference.as_str())
            .measurement(measurement)
            .to_directory(root.path())
            .run()
            .await
            .unwrap_err();
        assert!(matches!(
            failure.error,
            Error::RangeRequest(crate::transfer::reader::ReaderRequestError::NoResumeAnchor)
        ));
        assert_eq!(server.reads().len(), reads);
        assert_eq!(
            server
                .requests()
                .iter()
                .filter(|request| request.starts_with("head "))
                .count(),
            1
        );
        let report = client
            .download(server.reference.as_str())
            .measurement(measurement)
            .fresh(Fresh)
            .to_directory(root.path())
            .run()
            .await
            .unwrap();
        assert_ne!(report.saved_run(), Some(aggregate.as_path()));
        assert_eq!(
            report
                .completed()
                .next()
                .unwrap()
                .range_stats()
                .unwrap()
                .restored,
            ByteCount(0)
        );
        assert_eq!(fs::read(native.join("data")).unwrap(), data);
        assert_eq!(fs::read(native.join("state")).unwrap(), record);
        server.finish().await;
    }
}

#[test]
fn a_queued_reader_acquisition_retains_the_aggregate_lease_after_waiter_drop() {
    use std::os::fd::AsRawFd as _;
    use std::sync::mpsc;

    use crate::client::saved::Owned;
    use crate::transfer::{OperationOwner, Transfer};

    struct SettledRegistration {
        owner: Option<OperationOwner>,
        settled: Option<oneshot::Sender<()>>,
    }
    impl Drop for SettledRegistration {
        fn drop(&mut self) {
            // Arc::strong_count can become zero before its payload destructor releases the flock.
            // Signal only after the actual inner owner has finished that cleanup.
            drop(self.owner.take());
            if let Some(settled) = self.settled.take() {
                let _ = settled.send(());
            }
        }
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let server = Fixture::new(12, Fault::None).await;
        let root = Directory::new();
        let client = Client::new().unwrap();
        let plan = client
            .download(server.reference.as_str())
            .to_directory(root.path())
            .plan()
            .await
            .unwrap();
        let mut owned = Owned::create(&plan, None, crate::client::Reentry::Fresh)
            .await
            .unwrap();
        let file = &plan.files[0].file;
        let reader = super::Reader {
            http: &client.http,
            file,
            size: ByteCount(12),
            etag: "\"v1\"",
        };
        let pending = Transfer::from_reader(&reader)
            .to_file(&plan.files[0].destination)
            .unwrap();
        let state = pending.state().path().to_path_buf();
        owned.submit_reader(0, pending.state()).await.unwrap();
        let (settled, complete) = oneshot::channel();
        let registration: OperationOwner = Arc::new(SettledRegistration {
            owner: Some(owned.registration()),
            settled: Some(settled),
        });
        let (entered, occupied) = oneshot::channel();
        let (release, blocked) = mpsc::channel::<()>();
        let blocker = tokio::task::spawn_blocking(move || {
            let _ = entered.send(());
            let _ = blocked.recv();
        });
        occupied.await.unwrap();
        let mut waiter = Box::pin(pending.run_registered(Some(registration)));
        assert!(futures::poll!(waiter.as_mut()).is_pending());
        drop(waiter);
        drop(owned);
        let lease = fs::File::open(root.path().join(".xget-runs/lease")).unwrap();
        // SAFETY: this independent descriptor tests only the fixture's stable store lock.
        assert_eq!(
            unsafe { libc::flock(lease.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
            -1
        );
        assert_eq!(io::Error::last_os_error().kind(), io::ErrorKind::WouldBlock);
        assert!(!state.exists());
        release.send(()).unwrap();
        blocker.await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), complete)
            .await
            .expect("queued acquisition owns registration through cleanup")
            .expect("registration cleanup signal");
        // SAFETY: acquisition has settled, so this independent probe now acquires its own lease.
        assert_eq!(
            unsafe { libc::flock(lease.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
            0
        );
        assert!(state.join("state").is_file());
        assert!(server.reads().is_empty());
        server.finish().await;
    });
}

#[test]
fn only_a_known_nonzero_size_and_bounded_pin_can_select_reader_mode() {
    use crate::transfer::Length;
    assert!(!Mode::supports(Length::Unknown, Some("\"v1\"")));
    assert!(!Mode::supports(Length::Known(ByteCount(0)), Some("\"v1\"")));
    assert!(!Mode::supports(Length::Known(ByteCount(1)), None));
    assert!(!Mode::supports(
        Length::Known(ByteCount(1)),
        Some(&"x".repeat(257))
    ));
    assert!(Mode::supports(Length::Known(ByteCount(1)), Some("\"v1\"")));
}

#[tokio::test]
async fn explicit_restart_of_a_receiving_reader_fetches_all_frozen_windows_without_resetting_old_data()
 {
    const SIZE: usize = 256 * 1024;
    let server = Fixture::new(SIZE, Fault::HeldHead).await;
    let root = Directory::new();
    let client = Client::new().unwrap();
    let mut run = Box::pin(
        client
            .download(server.reference.as_str())
            .to_directory(root.path())
            .run(),
    );
    tokio::select! {
        result = &mut run => panic!("head must remain held: {result:?}"),
        _ = server.tails.wait() => {}
    }
    let (aggregate, native) = native_state(root.path()).await;
    drop(run);
    settled(root.path()).await;
    let old_control = fs::read(native.join("state")).unwrap();
    let old_data = fs::read(native.join("data")).unwrap();
    let old_reads = server.reads().len();
    server.resumed.store(true, Ordering::Release);
    let report = client
        .download(server.reference.as_str())
        .restart(crate::client::Restart)
        .to_directory(root.path())
        .run()
        .await
        .unwrap();
    assert_eq!(report.saved_run(), Some(aggregate.as_path()));
    let published = report.completed().next().unwrap();
    assert_ne!(published.receipt().state().path(), native.as_path());
    assert_eq!(published.range_stats().unwrap().restored, ByteCount(0));
    assert_eq!(
        published.range_stats().unwrap().source,
        ByteCount(SIZE as u64)
    );
    assert_eq!(fs::read(native.join("state")).unwrap(), old_control);
    assert_eq!(fs::read(native.join("data")).unwrap(), old_data);
    assert_eq!(
        server.reads()[old_reads..]
            .iter()
            .map(|(start, end)| end - start)
            .sum::<u64>(),
        SIZE as u64
    );
    assert_eq!(
        server
            .requests()
            .iter()
            .filter(|request| request.starts_with("head "))
            .count(),
        1
    );
    server.head.notify_one();
    server.finish().await;
}

#[tokio::test]
async fn explicit_restart_refuses_a_changed_reader_data_inode_without_any_refetch_fallback() {
    let server = Fixture::new(256 * 1024, Fault::HeldHead).await;
    let root = Directory::new();
    let client = Client::new().unwrap();
    let mut run = Box::pin(
        client
            .download(server.reference.as_str())
            .to_directory(root.path())
            .run(),
    );
    tokio::select! {
        result = &mut run => panic!("head must remain held: {result:?}"),
        _ = server.tails.wait() => {}
    }
    let (_, native) = native_state(root.path()).await;
    drop(run);
    settled(root.path()).await;
    fs::rename(native.join("data"), native.join("old-data")).unwrap();
    fs::write(native.join("data"), b"foreign").unwrap();
    let old_reads = server.reads().len();
    server.resumed.store(true, Ordering::Release);
    let failure = client
        .download(server.reference.as_str())
        .restart(crate::client::Restart)
        .to_directory(root.path())
        .run()
        .await
        .unwrap_err();
    assert!(matches!(failure.error, Error::Saved(SavedError::Native(_))));
    assert_eq!(server.reads().len(), old_reads);
    assert!(!root.path().join("payload").exists());
    assert_eq!(fs::read(native.join("data")).unwrap(), b"foreign");
    server.head.notify_one();
    server.finish().await;
}
