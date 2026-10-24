//! Source-authored executable fixtures. These have not been run by the cutover author.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use core::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use core::time::Duration;
use std::io::{self, Read as _, Write as _};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::{Arc, Mutex};
use std::thread;

use serde_json::Value;

struct Directory(PathBuf);

impl Directory {
    fn fresh() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "xget-cli-cutover-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        std::fs::create_dir(path.join("tmp")).unwrap();
        Self(path)
    }

    #[cfg(feature = "s3")]
    fn run_sdk(&self, server: &Server, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_xget"))
            .args(args)
            .current_dir(&self.0)
            .env("TMPDIR", self.0.join("tmp"))
            .env("AWS_ACCESS_KEY_ID", "fixture-access")
            .env("AWS_SECRET_ACCESS_KEY", "fixture-secret")
            .env("AWS_SESSION_TOKEN", "fixture-session")
            .env("AWS_REGION", "us-east-1")
            .env("AWS_DEFAULT_REGION", "us-east-1")
            .env("AWS_ENDPOINT_URL", &server.url)
            .env("AWS_ENDPOINT_URL_S3", &server.url)
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .env("AWS_MAX_ATTEMPTS", "1")
            .env("AWS_CONFIG_FILE", self.0.join("absent-config"))
            .env(
                "AWS_SHARED_CREDENTIALS_FILE",
                self.0.join("absent-credentials"),
            )
            .env_remove("AWS_PROFILE")
            .env_remove("AWS_ROLE_ARN")
            .env_remove("AWS_WEB_IDENTITY_TOKEN_FILE")
            .env_remove("AWS_CONTAINER_CREDENTIALS_FULL_URI")
            .env_remove("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")
            .env_remove("RUST_LOG")
            .output()
            .unwrap()
    }

    fn path(&self) -> &Path {
        &self.0
    }

    fn run(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_xget"))
            .args(args)
            .current_dir(&self.0)
            .env("TMPDIR", self.0.join("tmp"))
            .env_remove("RUST_LOG")
            .env_remove("XPROGRESS_DUMP")
            .output()
            .unwrap()
    }

    #[cfg(unix)]
    fn run_after_native_start(&self, server: &Server, args: &[&str]) -> Output {
        use std::io::{BufRead as _, BufReader};
        use std::process::Stdio;
        use std::sync::mpsc;

        let mut child = Command::new(env!("CARGO_BIN_EXE_xget"))
            .args(args)
            .current_dir(&self.0)
            .env("TMPDIR", self.0.join("tmp"))
            .env_remove("RUST_LOG")
            .env_remove("XPROGRESS_DUMP")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let stderr = child.stderr.take().unwrap();
        let (ready, observed) = mpsc::channel();
        let reader = thread::spawn(move || {
            let mut reader = BufReader::new(stderr);
            let mut captured = Vec::new();
            let mut reported = false;
            loop {
                let start = captured.len();
                if reader.read_until(b'\n', &mut captured).unwrap() == 0 {
                    break;
                }
                let line = &captured[start..];
                let native_progress = line.starts_with(b"logical received ")
                    || serde_json::from_slice::<Value>(line).is_ok_and(|event| {
                        event["kind"] == "Progress" && event["facts"]["kind"] == "Native"
                    });
                if !reported && native_progress {
                    let _ = ready.send(());
                    reported = true;
                }
            }
            captured
        });
        let started = observed.recv_timeout(Duration::from_secs(3));
        // This progress follows accepted GET headers/body ownership and entry into native transfer,
        // not completed native acquisition. The short response cannot fail during header opening.
        server.release_prefix.store(true, Ordering::Relaxed);
        if started.is_err() {
            let _ = child.kill();
            let _ = child.wait();
            let _ = reader.join();
            panic!("owned child did not begin native work before short-body release");
        }
        let mut completed = child.wait_with_output().unwrap();
        completed.stderr = reader.join().unwrap();
        completed
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[derive(Clone, Copy)]
enum Behavior {
    Complete,
    ShortBody,
    #[cfg(unix)]
    ShortBodyAfterHeaders,
    #[cfg(unix)]
    FailSecondOnce,
    GatedBody,
    #[cfg(unix)]
    ShortFirstWhole,
    #[cfg(unix)]
    GatedIncumbent,
    #[cfg(all(feature = "ipfs", unix))]
    WrongContent,
    #[cfg(feature = "s3")]
    S3Empty,
    #[cfg(all(feature = "s3", unix))]
    S3One,
    #[cfg(feature = "s3")]
    S3Many,
    #[cfg(feature = "s3")]
    S3Directory,
}

struct Server {
    url: String,
    stopping: Arc<AtomicBool>,
    release_tail: Arc<AtomicBool>,
    #[cfg(unix)]
    release_prefix: Arc<AtomicBool>,
    requests: Arc<Mutex<Vec<String>>>,
    worker: Option<thread::JoinHandle<()>>,
}

impl Server {
    fn start(behavior: Behavior) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let stopping = Arc::new(AtomicBool::new(false));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let release_tail = Arc::new(AtomicBool::new(false));
        let release = Arc::clone(&release_tail);
        #[cfg(unix)]
        let release_prefix = Arc::new(AtomicBool::new(false));
        #[cfg(unix)]
        let prefix = Arc::clone(&release_prefix);
        let stop = Arc::clone(&stopping);
        let log = Arc::clone(&requests);
        let worker = thread::spawn(move || {
            #[cfg(unix)]
            let mut denied = false;
            #[cfg(unix)]
            let mut shortened = false;
            while !stop.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        // Darwin can inherit the listener's nonblocking flag on accepted sockets.
                        // These bounded fixture reads are blocking, unlike the accept loop.
                        stream.set_nonblocking(false).unwrap();
                        stream
                            .set_read_timeout(Some(Duration::from_secs(2)))
                            .unwrap();
                        stream
                            .set_write_timeout(Some(Duration::from_secs(2)))
                            .unwrap();
                        if let Some(request) = read_request(&mut stream) {
                            // Test request recording crosses the fixture worker/test assertion boundary.
                            log.lock().unwrap().push(String::clone(&request));
                            let head = request.starts_with("HEAD ");
                            #[cfg(feature = "s3")]
                            if request.contains("list-type=2") {
                                let keys: &[&str] = match behavior {
                                    Behavior::S3Empty => &[],
                                    #[cfg(unix)]
                                    Behavior::S3One => &["a.bin"],
                                    Behavior::S3Many => &["a.bin", "b.bin"],
                                    Behavior::S3Directory => &["a.bin", "folder/"],
                                    _ => panic!("unexpected fixture listing"),
                                };
                                let mut xml = String::from(
                                    "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>fixture-bucket</Name><IsTruncated>false</IsTruncated>",
                                );
                                for key in keys {
                                    let size = if key.ends_with('/') { 0 } else { 3 };
                                    xml.push_str(&format!("<Contents><Key>{key}</Key><Size>{size}</Size><ETag>&quot;fixture&quot;</ETag></Contents>"));
                                }
                                xml.push_str("</ListBucketResult>");
                                write!(stream, "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/xml\r\nConnection: close\r\n\r\n{xml}", xml.len()).unwrap();
                                continue;
                            }
                            #[cfg(unix)]
                            if matches!(behavior, Behavior::FailSecondOnce)
                                && request.starts_with("GET /b.bin ")
                                && !denied
                            {
                                denied = true;
                                stream.write_all(b"HTTP/1.1 500 fixture\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
                                continue;
                            }
                            let length = match behavior {
                                Behavior::ShortBody => 4,
                                #[cfg(unix)]
                                Behavior::ShortBodyAfterHeaders => 4,
                                Behavior::GatedBody => 6,
                                #[cfg(unix)]
                                Behavior::ShortFirstWhole | Behavior::GatedIncumbent => 6,
                                #[cfg(feature = "s3")]
                                Behavior::S3Directory if head && request.contains("/folder/") => 0,
                                _ => 3,
                            };
                            write!(stream, "HTTP/1.1 200 OK\r\nContent-Length: {length}\r\nETag: \"fixture\"\r\nConnection: close\r\n\r\n").unwrap();
                            if !head {
                                #[cfg(unix)]
                                if matches!(
                                    behavior,
                                    Behavior::GatedIncumbent | Behavior::ShortBodyAfterHeaders
                                ) {
                                    stream.flush().unwrap();
                                    let deadline =
                                        std::time::Instant::now() + Duration::from_secs(5);
                                    while !prefix.load(Ordering::Relaxed)
                                        && !stop.load(Ordering::Relaxed)
                                        && std::time::Instant::now() < deadline
                                    {
                                        thread::sleep(Duration::from_millis(2));
                                    }
                                }
                                #[cfg(all(feature = "ipfs", unix))]
                                let bytes = if matches!(behavior, Behavior::WrongContent) {
                                    b"abd"
                                } else {
                                    b"abc"
                                };
                                #[cfg(not(all(feature = "ipfs", unix)))]
                                let bytes = b"abc";
                                stream.write_all(bytes).unwrap();
                                #[cfg(unix)]
                                if matches!(behavior, Behavior::ShortFirstWhole) {
                                    if shortened {
                                        stream.write_all(b"def").unwrap();
                                    }
                                    shortened = true;
                                }
                                let gated_tail = matches!(behavior, Behavior::GatedBody);
                                #[cfg(unix)]
                                let gated_tail =
                                    gated_tail || matches!(behavior, Behavior::GatedIncumbent);
                                if gated_tail {
                                    stream.flush().unwrap();
                                    let deadline =
                                        std::time::Instant::now() + Duration::from_secs(5);
                                    while !release.load(Ordering::Relaxed)
                                        && !stop.load(Ordering::Relaxed)
                                        && std::time::Instant::now() < deadline
                                    {
                                        thread::sleep(Duration::from_millis(2));
                                    }
                                    if release.load(Ordering::Relaxed) {
                                        let _ = stream.write_all(b"def");
                                    }
                                }
                            }
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2))
                    }
                    Err(error) => panic!("fixture accept failed: {error}"),
                }
            }
        });
        Self {
            url,
            stopping,
            release_tail,
            #[cfg(unix)]
            release_prefix,
            requests,
            worker: Some(worker),
        }
    }

    fn reference(&self, path: &str) -> String {
        format!("{}{path}", self.url)
    }

    fn requests(&self) -> Vec<String> {
        // The worker owns recording; tests take a bounded snapshot between completed child runs.
        Vec::clone(&self.requests.lock().unwrap())
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Relaxed);
        if let Some(worker) = self.worker.take() {
            worker.join().unwrap();
        }
    }
}

fn read_request(stream: &mut TcpStream) -> Option<String> {
    let text = read_request_headers(stream)?;
    text.lines().next().map(str::to_owned)
}

fn read_request_headers(stream: &mut TcpStream) -> Option<String> {
    let mut request = Vec::new();
    let mut byte = [0];
    while request.len() < 16384 {
        if stream.read(&mut byte).ok()? == 0 {
            return None;
        }
        request.push(byte[0]);
        if request.ends_with(b"\r\n\r\n") {
            return String::from_utf8(request).ok();
        }
    }
    None
}

fn events(bytes: &[u8]) -> Vec<Value> {
    String::from_utf8_lossy(bytes)
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[test]
#[cfg(feature = "ipfs")]
fn ipfs_gateway_flags_drive_listing_native_checks_and_live_delivery() {
    let directory = Directory::fresh();
    let server = Server::start(Behavior::Complete);
    let cid = "bafkreif2pall7dybz7vecqka3zo24irdwabwdi4wc55jznaq75q7eaavvu";
    let reference = format!("ipfs://{cid}");
    let listed = directory.run(&["list", &reference, "--ipfs-gateway", &server.url, "--json"]);
    assert_eq!(
        listed.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&listed.stderr)
    );
    let members = events(&listed.stdout);
    let member = members
        .iter()
        .find(|event| event["kind"] == "Member")
        .unwrap();
    assert_eq!(member["facts"]["selector_token"], cid);
    assert_eq!(member["facts"]["source_path"], cid);
    assert!(
        server
            .requests()
            .iter()
            .all(|request| request.starts_with("HEAD "))
    );
    assert!(!directory.0.join(".xget-runs").exists());

    let downloaded = directory.run(&[
        &reference,
        "--ipfs-gateway",
        &server.url,
        "--select-id",
        cid,
        "-o",
        "raw.bin",
        "-s",
        "none",
        "--json",
    ]);
    assert_eq!(
        downloaded.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&downloaded.stderr)
    );
    assert_eq!(std::fs::read(directory.0.join("raw.bin")).unwrap(), b"abc");
    let records = events(&downloaded.stderr);
    let published = records
        .iter()
        .find(|event| event["kind"] == "Published")
        .unwrap();
    assert!(published["facts"]["checks"]["measured_digest"].is_null());
    assert_eq!(
        published["facts"]["checks"]["matched_requirements"][0]["origins"],
        serde_json::json!(["Source"])
    );

    let live = directory.run(&[
        &reference,
        "--ipfs-gateway",
        &server.url,
        "-o",
        "-",
        "-s",
        "none",
        "--json",
    ]);
    assert_eq!(live.status.code(), Some(0));
    assert_eq!(live.stdout, b"abc");
    assert!(
        events(&live.stderr)
            .iter()
            .any(|event| event["kind"] == "Delivered")
    );
    assert!(
        server
            .requests()
            .iter()
            .all(|request| request.contains(&format!(" /ipfs/{cid} ")))
    );
}

#[test]
#[cfg(all(feature = "ipfs", unix))]
fn ipfs_raw_requirement_and_short_eof_preserve_overwrite_incumbent_without_measurement() {
    for behavior in [Behavior::WrongContent, Behavior::ShortBody] {
        let directory = Directory::fresh();
        let server = Server::start(behavior);
        let reference = "ipfs://bafkreif2pall7dybz7vecqka3zo24irdwabwdi4wc55jznaq75q7eaavvu";
        let path = directory.0.join("raw.bin");
        std::fs::write(&path, b"sentinel").unwrap();
        let original = FileIdentity::of(&path);
        let failed = directory.run(&[
            reference,
            "--ipfs-gateway",
            &server.url,
            "-o",
            "raw.bin",
            "--overwrite",
            "-s",
            "none",
            "--json",
        ]);
        assert_eq!(
            failed.status.code(),
            Some(1),
            "{}",
            String::from_utf8_lossy(&failed.stderr)
        );
        assert_eq!(std::fs::read(&path).unwrap(), b"sentinel");
        assert_eq!(FileIdentity::of(&path), original);
        assert!(
            !events(&failed.stderr)
                .iter()
                .any(|event| event["kind"] == "Published" || event["kind"] == "End")
        );
        assert!(
            server
                .requests()
                .iter()
                .any(|request| request.starts_with("GET "))
        );
    }
}

#[test]
fn scoped_header_flags_reach_metadata_and_payload_without_entering_events_or_records() {
    let directory = Directory::fresh();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let reference = format!("http://{}/a.bin", listener.local_addr().unwrap());
    let worker = thread::spawn(move || {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut requests = Vec::new();
        for _ in 0..2 {
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error)
                        if error.kind() == io::ErrorKind::WouldBlock
                            && std::time::Instant::now() < deadline =>
                    {
                        thread::sleep(Duration::from_millis(2))
                    }
                    Err(error) => panic!("header fixture accept: {error}"),
                }
            };
            stream.set_nonblocking(false).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let request = read_request_headers(&mut stream).unwrap();
            let head = request.starts_with("HEAD ");
            stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nETag: \"fixture\"\r\nConnection: close\r\n\r\n").unwrap();
            if !head {
                stream.write_all(b"abc").unwrap();
            }
            requests.push(request);
        }
        requests
    });
    let result = directory.run(&[
        &reference,
        "-o",
        "a.bin",
        "-H",
        "Authorization: Bearer cli-header-marker",
        "--json",
    ]);
    assert_eq!(
        result.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let requests = worker.join().unwrap();
    assert_eq!(requests.len(), 2);
    for request in requests {
        let (_, value) = request
            .lines()
            .filter_map(|line| line.split_once(':'))
            .find(|(name, _)| name.eq_ignore_ascii_case("authorization"))
            .unwrap();
        assert_eq!(value.trim(), "Bearer cli-header-marker");
    }
    assert_eq!(
        std::fs::read(directory.path().join("a.bin")).unwrap(),
        b"abc"
    );
    assert!(!String::from_utf8_lossy(&result.stderr).contains("cli-header-marker"));
    assert!(!String::from_utf8_lossy(&result.stdout).contains("cli-header-marker"));
    for entry in std::fs::read_dir(directory.path().join(".xget-runs")).unwrap() {
        let state = entry.unwrap().path().join("state");
        if state.is_file() {
            let bytes = std::fs::read(state).unwrap();
            assert!(
                !bytes
                    .windows(b"cli-header-marker".len())
                    .any(|window| window == b"cli-header-marker")
            );
        }
    }
}

#[test]
fn explicit_resume_renews_headers_without_rediscovery_or_storing_access() {
    let directory = Directory::fresh();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let reference = format!("http://{}/a.bin", listener.local_addr().unwrap());
    let worker = thread::spawn(move || {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut requests = Vec::new();
        for index in 0..3 {
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error)
                        if error.kind() == io::ErrorKind::WouldBlock
                            && std::time::Instant::now() < deadline =>
                    {
                        thread::sleep(Duration::from_millis(2))
                    }
                    Err(error) => panic!("resume header fixture accept: {error}"),
                }
            };
            stream.set_nonblocking(false).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let request = read_request_headers(&mut stream).unwrap();
            if index == 1 {
                stream
                    .write_all(
                        b"HTTP/1.1 503 fixture\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .unwrap();
            } else {
                stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nETag: \"fixture\"\r\nConnection: close\r\n\r\n").unwrap();
                if index == 2 {
                    stream.write_all(b"abc").unwrap();
                }
            }
            requests.push(request);
        }
        requests
    });
    let failed = directory.run(&[
        &reference,
        "-o",
        "a.bin",
        "-H",
        "Authorization: Bearer first-access-marker",
        "--json",
    ]);
    assert_eq!(failed.status.code(), Some(1));
    let first = events(&failed.stderr);
    let state = first
        .iter()
        .find_map(|event| event["facts"]["state_path"].as_str())
        .unwrap();
    let info = directory.run(&["info", state, "--json"]);
    assert_eq!(info.status.code(), Some(0));
    let metadata = events(&info.stdout);
    let key = metadata
        .iter()
        .find(|event| event["kind"] == "SavedMember")
        .unwrap()["facts"]["binding_key"]
        .as_str()
        .unwrap();
    let binding = format!("{key}={reference}");
    let resumed = directory.run(&[
        "resume",
        state,
        "--input",
        &binding,
        "-H",
        "Authorization: Bearer renewed-access-marker",
        "--json",
    ]);
    assert_eq!(
        resumed.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&resumed.stderr)
    );
    assert_eq!(
        std::fs::read(directory.path().join("a.bin")).unwrap(),
        b"abc"
    );
    let requests = worker.join().unwrap();
    assert!(requests[0].starts_with("HEAD "));
    assert!(requests[1].starts_with("GET "));
    assert!(
        requests[2].starts_with("GET "),
        "explicit continuation must not HEAD again"
    );
    for (index, request) in requests.iter().enumerate() {
        let (_, value) = request
            .lines()
            .filter_map(|line| line.split_once(':'))
            .find(|(name, _)| name.eq_ignore_ascii_case("authorization"))
            .unwrap();
        assert_eq!(
            value.trim(),
            if index < 2 {
                "Bearer first-access-marker"
            } else {
                "Bearer renewed-access-marker"
            }
        );
    }
    assert!(
        requests[2]
            .lines()
            .any(|line| line.eq_ignore_ascii_case("if-match: \"fixture\""))
    );
    let record = std::fs::read(Path::new(state).join("state")).unwrap();
    for marker in ["first-access-marker", "renewed-access-marker"] {
        assert!(
            !record
                .windows(marker.len())
                .any(|window| window == marker.as_bytes())
        );
        for bytes in [&failed.stderr, &info.stdout, &resumed.stderr] {
            assert!(!String::from_utf8_lossy(bytes).contains(marker));
        }
    }
}

#[test]
fn invalid_or_broadcast_access_headers_fail_before_network_and_hide_values() {
    let directory = Directory::fresh();
    let server = Server::start(Behavior::Complete);
    let first = server.reference("/a.bin");
    let second = server.reference("/b.bin");
    for arguments in [
        vec![first.as_str(), "-H", "Range: bytes=0-2", "--json"],
        vec![
            first.as_str(),
            second.as_str(),
            "-H",
            "Authorization: Bearer cli-header-marker",
            "--json",
        ],
        vec![
            "s3://fixture-bucket/a",
            "-H",
            "Authorization: Bearer cli-header-marker",
            "--json",
        ],
    ] {
        let result = directory.run(&arguments);
        assert_eq!(result.status.code(), Some(2));
        assert!(result.stdout.is_empty());
        assert!(!String::from_utf8_lossy(&result.stderr).contains("cli-header-marker"));
    }
    assert!(server.requests().is_empty());
    assert!(!directory.path().join(".xget-runs").exists());
}

#[test]
fn invalid_inputs_exit_two_without_echoing_secrets_or_creating_outputs() {
    let directory = Directory::fresh();
    for arguments in [
        vec!["--json"],
        vec![
            "https://user:password@example.test/file?signature=secret",
            "out.bin",
            "--json",
        ],
        vec![
            "--expect",
            "https://example.test/sums?secret=token",
            "--json",
        ],
    ] {
        let result = directory.run(&arguments);
        assert_eq!(result.status.code(), Some(2));
        assert!(result.stdout.is_empty());
        let text = String::from_utf8(result.stderr).unwrap();
        for secret in ["password", "signature", "secret", "token"] {
            assert!(!text.contains(secret));
        }
        assert!(!directory.0.join("out.bin").exists());
        assert!(!directory.0.join(".xget-runs").exists());
    }
}

#[test]
fn malformed_expectations_explain_the_format_before_network_or_output_effects() {
    let directory = Directory::fresh();
    let server = Server::start(Behavior::Complete);
    let reference = server.reference("/a.bin?signature=parse-query-marker");
    for value in [
        "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
        "sha256:parse-value-marker",
    ] {
        let result = directory.run(&[&reference, "-o", "out.bin", "--expect", value, "--json"]);
        assert_eq!(result.status.code(), Some(2));
        assert!(result.stdout.is_empty());
        let records = events(&result.stderr);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["kind"], "Error");
        let message = records[0]["facts"]["message"].as_str().unwrap();
        assert!(message.contains("invalid --expect value"));
        assert!(message.contains("sha256:"));
        assert!(!message.contains(value));
        let text = String::from_utf8(result.stderr).unwrap();
        assert!(!text.contains("parse-query-marker"));
        assert!(!text.contains("parse-value-marker"));
        assert!(server.requests().is_empty());
        assert!(!directory.path().join("out.bin").exists());
        assert!(!directory.path().join(".xget-runs").exists());
    }
}

#[test]
fn list_and_real_dryrun_use_metadata_only_without_native_effects() {
    let directory = Directory::fresh();
    let server = Server::start(Behavior::Complete);
    let reference = server.reference("/a.bin");
    let list = directory.run(&["list", &reference, "--json"]);
    assert_eq!(list.status.code(), Some(0));
    let listed = events(&list.stdout);
    assert!(listed.iter().any(|event| event["kind"] == "Member"));
    let dry = directory.run(&[&reference, "-o", "renamed.bin", "--dry-run", "--json"]);
    assert_eq!(dry.status.code(), Some(0));
    assert!(
        events(&dry.stderr)
            .iter()
            .any(|event| event["kind"] == "Planned")
    );
    assert!(dry.stdout.is_empty());
    assert!(
        server
            .requests()
            .iter()
            .all(|request| request.starts_with("HEAD "))
    );
    assert!(!directory.0.join("renamed.bin").exists());
    assert!(!directory.0.join(".xget-runs").exists());
}

#[test]
fn stdout_success_requires_clean_eof_checks_and_sink_flush() {
    let directory = Directory::fresh();
    let server = Server::start(Behavior::Complete);
    let reference = server.reference("/a.bin");
    let result = directory.run(&[
        &reference,
        "-o",
        "-",
        "-s",
        "none",
        "--expect",
        "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        "--json",
    ]);
    assert_eq!(result.status.code(), Some(0));
    assert_eq!(result.stdout, b"abc");
    let records = events(&result.stderr);
    let delivered = records
        .iter()
        .find(|event| event["kind"] == "Delivered")
        .unwrap();
    assert!(delivered["facts"]["checks"]["measured_digest"].is_null());
    assert_eq!(
        delivered["facts"]["checks"]["matched_requirements"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(delivered["facts"]["released_bytes"], 3);
    assert!(!directory.0.join(".xget-runs").exists());
    assert_eq!(
        std::fs::read_dir(directory.0.join("tmp")).unwrap().count(),
        0
    );
}

#[test]
fn stdout_releases_a_prefix_while_the_remote_body_is_still_open() {
    use std::process::Stdio;
    use std::sync::mpsc;

    let directory = Directory::fresh();
    let server = Server::start(Behavior::GatedBody);
    let reference = server.reference("/a.bin");
    let mut child = Command::new(env!("CARGO_BIN_EXE_xget"))
        .args([&reference, "-o", "-", "--json"])
        .current_dir(&directory.0)
        .env("TMPDIR", directory.0.join("tmp"))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdout = child.stdout.take().unwrap();
    let (ready, observed) = mpsc::channel();
    let reader = thread::spawn(move || {
        let mut prefix = [0; 3];
        let result = stdout.read_exact(&mut prefix);
        ready.send(result.map(|()| prefix)).unwrap();
        let mut tail = Vec::new();
        stdout.read_to_end(&mut tail).unwrap();
        tail
    });
    let prefix = observed.recv_timeout(Duration::from_secs(3));
    server.release_tail.store(true, Ordering::Relaxed);
    if !matches!(prefix, Ok(Ok(_))) {
        let _ = child.kill();
        let _ = child.wait();
        let _ = reader.join();
        panic!("stdout did not release the prefix before source EOF: {prefix:?}");
    }
    assert_eq!(prefix.unwrap().unwrap(), *b"abc");
    let completed = child.wait_with_output().unwrap();
    assert_eq!(completed.status.code(), Some(0));
    assert_eq!(reader.join().unwrap(), b"def");
    assert_eq!(
        std::fs::read_dir(directory.0.join("tmp")).unwrap().count(),
        0
    );
}

#[test]
fn late_stdout_length_or_digest_failure_never_emits_a_completion() {
    let directory = Directory::fresh();
    for behavior in [Behavior::Complete, Behavior::ShortBody] {
        let server = Server::start(behavior);
        let reference = server.reference("/a.bin");
        let result = directory.run(&[
            &reference,
            "-o",
            "-",
            "--expect",
            "sha256:0000000000000000000000000000000000000000000000000000000000000000",
            "--json",
        ]);
        assert_eq!(result.status.code(), Some(1));
        assert!(b"abc".starts_with(&result.stdout));
        let records = events(&result.stderr);
        assert!(records.iter().any(|event| event["kind"] == "Error"));
        assert!(
            !records
                .iter()
                .any(|event| event["kind"] == "Delivered" || event["kind"] == "End")
        );
        assert_eq!(
            std::fs::read_dir(directory.0.join("tmp")).unwrap().count(),
            0
        );
    }
}

#[test]
fn a_closed_stdout_consumer_is_failure_not_verified_delivery() {
    use std::process::Stdio;

    let directory = Directory::fresh();
    let server = Server::start(Behavior::GatedBody);
    let reference = server.reference("/a.bin");
    let mut child = Command::new(env!("CARGO_BIN_EXE_xget"))
        .args([&reference, "-o", "-", "--json"])
        .current_dir(&directory.0)
        .env("TMPDIR", directory.0.join("tmp"))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    drop(child.stdout.take());
    let completed = child.wait_with_output().unwrap();
    server.release_tail.store(true, Ordering::Relaxed);
    assert_eq!(completed.status.code(), Some(1));
    let records = events(&completed.stderr);
    assert!(records.iter().any(|event| event["kind"] == "Error"));
    assert!(
        !records
            .iter()
            .any(|event| event["kind"] == "Delivered" || event["kind"] == "End")
    );
}

#[test]
#[cfg(unix)]
fn user_interrupt_exits_130_without_claiming_pause_or_completion() {
    use std::process::Stdio;

    let directory = Directory::fresh();
    let server = Server::start(Behavior::GatedBody);
    let reference = server.reference("/a.bin");
    let mut child = Command::new(env!("CARGO_BIN_EXE_xget"))
        .args([&reference, "-o", "out.bin", "--json"])
        .current_dir(&directory.0)
        .env("TMPDIR", directory.0.join("tmp"))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while !server
        .requests()
        .iter()
        .any(|request| request.starts_with("GET "))
        && std::time::Instant::now() < deadline
    {
        thread::sleep(Duration::from_millis(2));
    }
    if !server
        .requests()
        .iter()
        .any(|request| request.starts_with("GET "))
    {
        let _ = child.kill();
        let _ = child.wait();
        panic!("owned child did not reach the gated body");
    }
    // SAFETY: this PID belongs to the still-owned child spawned above; no unrelated process is targeted.
    assert_eq!(
        unsafe { libc::kill(i32::try_from(child.id()).unwrap(), libc::SIGINT) },
        0
    );
    let completed = child.wait_with_output().unwrap();
    server.release_tail.store(true, Ordering::Relaxed);
    assert_eq!(completed.status.code(), Some(130));
    let records = events(&completed.stderr);
    assert!(
        !records
            .iter()
            .any(|event| event["kind"] == "End" || event["kind"] == "Published")
    );
    let started = records
        .iter()
        .find(|event| event["kind"] == "RunStarted")
        .unwrap();
    let interrupted = records
        .iter()
        .find(|event| event["kind"] == "Error" && event["facts"]["exit_code"] == 130)
        .unwrap();
    assert_eq!(started["run_id"], interrupted["run_id"]);
    assert!(started["facts"]["state_path"].as_str().is_some());
    assert_eq!(
        started["facts"]["state_path"],
        interrupted["facts"]["state_path"]
    );
    assert_eq!(interrupted["facts"]["pause_confirmed"], false);
}

#[test]
fn many_selected_files_reject_one_file_target_before_any_get() {
    let directory = Directory::fresh();
    let server = Server::start(Behavior::Complete);
    let a = server.reference("/a.bin");
    let b = server.reference("/b.bin");
    let result = directory.run(&[&a, &b, "-o", "out.bin", "--json"]);
    assert_eq!(result.status.code(), Some(2));
    assert!(
        server
            .requests()
            .iter()
            .all(|request| request.starts_with("HEAD "))
    );
    assert!(!directory.0.join("out.bin").exists());
    assert!(!directory.0.join(".xget-runs").exists());
}

#[test]
#[cfg(unix)]
fn offline_info_and_actual_frozen_resume_never_rediscover_members() {
    let directory = Directory::fresh();
    let server = Server::start(Behavior::FailSecondOnce);
    let a = server.reference("/a.bin");
    let b = server.reference("/b.bin");
    let first = directory.run(&[&a, &b, "-D", "downloads", "--json"]);
    assert_eq!(first.status.code(), Some(1));
    let first_events = events(&first.stderr);
    let state = first_events
        .iter()
        .find_map(|event| event["facts"]["state_path"].as_str())
        .unwrap();
    let before_info = server.requests();
    let inspected = directory.run(&["info", state, "--json"]);
    assert_eq!(inspected.status.code(), Some(0));
    assert_eq!(server.requests(), before_info);
    let info = events(&inspected.stdout);
    let mut bindings = Vec::new();
    for event in info.iter().filter(|event| event["kind"] == "SavedMember") {
        let destination = Path::new(event["facts"]["destination"].as_str().unwrap());
        let reference = if destination.file_name().unwrap() == "a.bin" {
            &a
        } else {
            &b
        };
        bindings.push(format!(
            "{}={reference}",
            event["facts"]["binding_key"].as_str().unwrap()
        ));
    }
    assert_eq!(bindings.len(), 2);
    let resumed = directory.run(&[
        "resume",
        state,
        "--input",
        &bindings[0],
        "--input",
        &bindings[1],
        "--json",
    ]);
    assert_eq!(resumed.status.code(), Some(0));
    assert!(
        server.requests()[before_info.len()..]
            .iter()
            .all(|request| !request.starts_with("HEAD "))
    );
    assert_eq!(
        std::fs::read(directory.0.join("downloads/a.bin")).unwrap(),
        b"abc"
    );
    assert_eq!(
        std::fs::read(directory.0.join("downloads/b.bin")).unwrap(),
        b"abc"
    );
    let resumed_events = events(&resumed.stderr);
    assert!(resumed_events.iter().any(|event| event["kind"] == "End"));
    let original = first_events
        .iter()
        .find(|event| event["kind"] == "Published")
        .unwrap();
    assert_eq!(original["correlation_scope"], "SavedWorkspace");
    let inspected_member = info
        .iter()
        .find(|event| event["item_id"] == original["item_id"])
        .unwrap();
    let reused = resumed_events
        .iter()
        .find(|event| event["kind"] == "Published" && event["item_id"] == original["item_id"])
        .unwrap();
    assert_eq!(original["run_id"], inspected_member["run_id"]);
    assert_eq!(original["run_id"], reused["run_id"]);
    assert_ne!(original["invocation_id"], reused["invocation_id"]);
    assert_eq!(reused["facts"]["version"], "LocallyRevalidated");
    assert_eq!(original["facts"]["version"], "StrongEtagMatched");
}

#[test]
#[cfg(all(feature = "s3", unix))]
fn actual_sdk_empty_one_many_and_mixed_tree_flows_share_the_common_cli() {
    for (behavior, count) in [
        (Behavior::S3Empty, 0),
        (Behavior::S3One, 1),
        (Behavior::S3Many, 2),
    ] {
        let directory = Directory::fresh();
        let server = Server::start(behavior);
        let listed = directory.run_sdk(
            &server,
            &["list", "--tree", "s3://fixture-bucket/", "--json"],
        );
        assert_eq!(listed.status.code(), Some(0));
        assert_eq!(
            events(&listed.stdout)
                .iter()
                .filter(|event| event["kind"] == "Member")
                .count(),
            count
        );
        assert!(!directory.0.join("downloads").exists());
        let downloaded = directory.run_sdk(
            &server,
            &[
                "--tree",
                "s3://fixture-bucket/",
                "-D",
                "downloads",
                "--json",
            ],
        );
        assert_eq!(downloaded.status.code(), Some(0));
        assert_eq!(
            events(&downloaded.stderr)
                .iter()
                .filter(|event| event["kind"] == "Published")
                .count(),
            count
        );
        assert_eq!(
            directory.0.join("downloads").exists(),
            count != 0,
            "genuine empty tree creates no output"
        );
        for secret in ["fixture-access", "fixture-secret", "fixture-session"] {
            assert!(!String::from_utf8_lossy(&downloaded.stderr).contains(secret));
        }
    }
    let directory = Directory::fresh();
    let server = Server::start(Behavior::S3Many);
    let exact = server.reference("/direct.bin");
    let mixed = directory.run_sdk(
        &server,
        &[
            &exact,
            "--tree",
            "s3://fixture-bucket/",
            "-D",
            "mixed",
            "--json",
        ],
    );
    assert_eq!(mixed.status.code(), Some(0));
    let records = events(&mixed.stderr);
    let published: Vec<_> = records
        .iter()
        .filter(|event| event["kind"] == "Published")
        .collect();
    assert_eq!(published.len(), 3);
    for event in published {
        assert_eq!(
            std::fs::read(event["facts"]["destination"].as_str().unwrap()).unwrap(),
            b"abc"
        );
    }
}

#[test]
fn captured_new_bar_and_plain_modes_keep_payload_stdout_clean_with_dump_overrides() {
    let directory = Directory::fresh();
    let server = Server::start(Behavior::Complete);
    let reference = server.reference("/a.bin");
    for mode in ["auto", "bar", "plain"] {
        for dump in ["", "1"] {
            let result = Command::new(env!("CARGO_BIN_EXE_xget"))
                .args([&reference, "-o", "-", "--progress", mode, "--bits"])
                .current_dir(&directory.0)
                .env("TMPDIR", directory.0.join("tmp"))
                .env("XPROGRESS_DUMP", dump)
                .output()
                .unwrap();
            assert_eq!(result.status.code(), Some(0));
            assert_eq!(result.stdout, b"abc");
            assert!(!result.stderr.contains(&0x1b));
            assert!(!result.stderr.contains(&13));
            assert!(result.stderr.ends_with(b"\n"));
            assert!(String::from_utf8_lossy(&result.stderr).contains("accepted"));
        }
    }
}

#[test]
fn new_json_progress_uses_fixed_units_even_with_human_flags() {
    let directory = Directory::fresh();
    let server = Server::start(Behavior::Complete);
    let reference = server.reference("/a.bin");
    let result = directory.run(&[
        &reference,
        "-o",
        "-",
        "--progress",
        "json",
        "--bits",
        "--raw-sizes",
    ]);
    assert_eq!(result.status.code(), Some(0));
    assert_eq!(result.stdout, b"abc");
    let records = events(&result.stderr);
    let progress = records
        .iter()
        .find(|event| event["kind"] == "Progress")
        .unwrap();
    assert_eq!(progress["schema_version"], 1);
    assert_eq!(progress["facts"]["kind"], "Live");
    assert_eq!(progress["facts"]["accepted_bytes"], 3);
    assert!(progress["facts"]["rate_bytes_per_second"].is_u64());
    assert!(progress["elapsed_ms"].is_u64());
    assert_eq!(progress["facts"]["completion_evidence"], false);
    assert_eq!(progress["facts"]["published"], false);
    assert!(!String::from_utf8_lossy(&result.stderr).contains("\"event\":\"verified\""));
}

#[test]
#[cfg(unix)]
fn a_reporter_pipe_failure_is_nonzero_even_if_core_publication_finishes() {
    use std::io::{BufRead as _, BufReader};
    use std::process::Stdio;

    let directory = Directory::fresh();
    let server = Server::start(Behavior::GatedBody);
    let reference = server.reference("/a.bin");
    let mut child = Command::new(env!("CARGO_BIN_EXE_xget"))
        .args([&reference, "-o", "out.bin", "--progress", "json"])
        .current_dir(&directory.0)
        .env("TMPDIR", directory.0.join("tmp"))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut reader = BufReader::new(child.stderr.take().unwrap());
    let mut started = false;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap() == 0 {
            break;
        }
        let event: Value = serde_json::from_str(&line).unwrap();
        if event["kind"] == "RunStarted" {
            started = true;
            break;
        }
    }
    drop(reader);
    server.release_tail.store(true, Ordering::Relaxed);
    let completed = child.wait_with_output().unwrap();
    assert!(started, "state notification precedes the gated payload");
    assert_eq!(completed.status.code(), Some(1));
    assert_eq!(
        std::fs::read(directory.0.join("out.bin")).unwrap(),
        b"abcdef",
        "an infallible observer cannot promise cancellation or rollback of native work"
    );
}

#[test]
fn multi_original_list_and_live_dryrun_share_complete_metadata_without_payload_or_native_effects() {
    let directory = Directory::fresh();
    let server = Server::start(Behavior::Complete);
    let a = server.reference("/a.bin");
    let b = server.reference("/b.bin");
    let listed = directory.run(&["list", &a, &b, "--json"]);
    assert_eq!(listed.status.code(), Some(0));
    assert_eq!(
        events(&listed.stdout)
            .iter()
            .filter(|event| event["kind"] == "Member")
            .count(),
        2
    );
    let dry = directory.run(&[&a, "-o", "-", "--dry-run", "--json"]);
    assert_eq!(dry.status.code(), Some(0));
    assert!(dry.stdout.is_empty());
    let inspected = events(&dry.stderr);
    let planned = inspected
        .iter()
        .find(|event| event["kind"] == "PlannedLive")
        .unwrap();
    assert_eq!(planned["facts"]["target"], "Stdout");
    assert_eq!(planned["facts"]["execution_authority"], false);
    assert_eq!(planned["facts"]["payload_opened"], false);
    let many = directory.run(&[&a, &b, "-o", "-", "--dry-run", "--json"]);
    assert_eq!(many.status.code(), Some(2));
    assert!(
        server
            .requests()
            .iter()
            .all(|request| request.starts_with("HEAD "))
    );
    assert!(!directory.0.join("-").exists());
    assert!(!directory.0.join(".xget-runs").exists());
    assert_eq!(
        std::fs::read_dir(directory.0.join("tmp")).unwrap().count(),
        0
    );
}

#[test]
fn local_sidecar_live_binding_uses_original_path_and_preserves_provenance_without_measurement() {
    let directory = Directory::fresh();
    let server = Server::start(Behavior::Complete);
    let reference = server.reference("/nested/original.bin");
    let checksum = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
    std::fs::write(
        directory.0.join("checks"),
        format!("{checksum}  nested/original.bin\n{checksum}  private-unselected-marker\n"),
    )
    .unwrap();
    let dry = directory.run(&[
        &reference,
        "-o",
        "-",
        "--expect-file",
        "checks",
        "-s",
        "none",
        "--dry-run",
        "--json",
    ]);
    assert_eq!(dry.status.code(), Some(0));
    assert!(
        server
            .requests()
            .iter()
            .all(|request| request.starts_with("HEAD "))
    );
    let live = directory.run(&[
        &reference,
        "-o",
        "-",
        "--expect-file",
        "checks",
        "-s",
        "none",
        "--json",
    ]);
    assert_eq!(live.status.code(), Some(0));
    assert_eq!(live.stdout, b"abc");
    let records = events(&live.stderr);
    let delivered = records
        .iter()
        .find(|event| event["kind"] == "Delivered")
        .unwrap();
    assert!(delivered["facts"]["checks"]["measured_digest"].is_null());
    assert_eq!(
        delivered["facts"]["checks"]["matched_requirements"][0]["origins"],
        serde_json::json!(["Sidecar"])
    );
    for output in [&dry.stderr, &live.stderr] {
        assert!(!String::from_utf8_lossy(output).contains("private-unselected-marker"));
    }
}

#[test]
fn bad_sidecar_text_bounds_wrong_paths_and_check_conflicts_never_open_selected_payload() {
    let checksum = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
    for text in [
        format!("{checksum}  renamed.bin\n"),
        format!("{checksum}  original.bin\n"),
        format!("{checksum}  nested/original.bin\nnot-a-digest  secret-unmatched-marker\n"),
        format!("{checksum}  nested/original.bin\n{checksum}  nested/original.bin\n"),
        "x".repeat(65_537),
    ] {
        let directory = Directory::fresh();
        let server = Server::start(Behavior::Complete);
        let reference = server.reference("/nested/original.bin");
        std::fs::write(directory.0.join("checks"), text).unwrap();
        let result = directory.run(&[&reference, "-o", "-", "--expect-file", "checks", "--json"]);
        assert_eq!(result.status.code(), Some(2));
        assert!(result.stdout.is_empty());
        assert!(
            server
                .requests()
                .iter()
                .all(|request| request.starts_with("HEAD "))
        );
        assert!(!String::from_utf8_lossy(&result.stderr).contains("secret-unmatched-marker"));
        assert!(!directory.0.join(".xget-runs").exists());
    }
    let directory = Directory::fresh();
    let server = Server::start(Behavior::Complete);
    let reference = server.reference("/nested/original.bin");
    std::fs::write(
        directory.0.join("checks"),
        format!("{checksum}  nested/original.bin\n"),
    )
    .unwrap();
    let result = directory.run(&[
        &reference,
        "-o",
        "-",
        "--expect-file",
        "checks",
        "--expect",
        "sha256:0000000000000000000000000000000000000000000000000000000000000000",
        "--json",
    ]);
    assert_eq!(result.status.code(), Some(2));
    assert!(
        server
            .requests()
            .iter()
            .all(|request| request.starts_with("HEAD "))
    );
}

#[test]
#[cfg(feature = "s3")]
fn empty_scope_plus_one_file_is_live_and_directories_remain_inspection_only() {
    let directory = Directory::fresh();
    let empty = Server::start(Behavior::S3Empty);
    let exact = empty.reference("/direct.bin");
    let listed = directory.run_sdk(
        &empty,
        &["list", &exact, "--tree", "s3://fixture-bucket/", "--json"],
    );
    assert_eq!(listed.status.code(), Some(0));
    assert_eq!(
        events(&listed.stdout)
            .iter()
            .filter(|event| event["kind"] == "Member")
            .count(),
        1
    );
    let dry = directory.run_sdk(
        &empty,
        &[
            &exact,
            "--tree",
            "s3://fixture-bucket/",
            "-o",
            "-",
            "--dry-run",
            "--json",
        ],
    );
    assert_eq!(dry.status.code(), Some(0));
    assert!(dry.stdout.is_empty());
    assert!(
        empty
            .requests()
            .iter()
            .all(|request| request.starts_with("HEAD ") || request.contains("list-type=2"))
    );
    let live = directory.run_sdk(
        &empty,
        &[
            &exact,
            "--tree",
            "s3://fixture-bucket/",
            "-o",
            "-",
            "--json",
        ],
    );
    assert_eq!(live.status.code(), Some(0));
    assert_eq!(live.stdout, b"abc");
    let zero = directory.run_sdk(
        &empty,
        &[
            "--tree",
            "s3://fixture-bucket/",
            "-o",
            "-",
            "--dry-run",
            "--json",
        ],
    );
    assert_eq!(zero.status.code(), Some(2));
    let metadata = Server::start(Behavior::S3Directory);
    let inspected = directory.run_sdk(
        &metadata,
        &["list", "--tree", "s3://fixture-bucket/", "--json"],
    );
    assert_eq!(inspected.status.code(), Some(0));
    let records = events(&inspected.stdout);
    let folder = records
        .iter()
        .find(|event| event["kind"] == "Directory")
        .unwrap();
    assert_eq!(folder["facts"]["source_path"], "folder");
    assert_eq!(folder["facts"]["inspection_only"], true);
    assert_eq!(folder["facts"]["executable"], false);
    assert!(
        metadata
            .requests()
            .iter()
            .all(|request| request.starts_with("HEAD ") || request.contains("list-type=2"))
    );
    assert!(!directory.0.join(".xget-runs").exists());
    assert!(!directory.0.join("folder").exists());
}

#[test]
#[cfg(unix)]
fn fresh_native_sidecar_binds_before_output_rename_without_replanning_or_caller_provenance() {
    let directory = Directory::fresh();
    let server = Server::start(Behavior::Complete);
    let reference = server.reference("/nested/original.bin");
    let checksum = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
    std::fs::write(
        directory.0.join("checks"),
        format!("{checksum}  nested/original.bin\n"),
    )
    .unwrap();
    let automatic = directory.run(&[
        &reference,
        "--expect-file",
        "checks",
        "-o",
        "renamed.bin",
        "--json",
    ]);
    assert_eq!(automatic.status.code(), Some(2));
    assert!(
        server.requests().is_empty(),
        "unsupported native sidecar re-entry is rejected before discovery"
    );
    let dry = directory.run(&[
        &reference,
        "--expect-file",
        "checks",
        "--fresh",
        "-o",
        "renamed.bin",
        "--dry-run",
        "--json",
    ]);
    assert_eq!(dry.status.code(), Some(0));
    assert!(
        server
            .requests()
            .iter()
            .all(|request| request.starts_with("HEAD "))
    );
    assert!(!directory.0.join("renamed.bin").exists());
    assert!(!directory.0.join(".xget-runs").exists());
    let completed = directory.run(&[
        &reference,
        "--expect-file",
        "checks",
        "--fresh",
        "-o",
        "renamed.bin",
        "-s",
        "none",
        "--json",
    ]);
    assert_eq!(completed.status.code(), Some(0));
    assert_eq!(
        std::fs::read(directory.0.join("renamed.bin")).unwrap(),
        b"abc"
    );
    let records = events(&completed.stderr);
    let published = records
        .iter()
        .find(|event| event["kind"] == "Published")
        .unwrap();
    assert!(published["facts"]["checks"]["measured_digest"].is_null());
    assert_eq!(
        published["facts"]["checks"]["matched_requirements"][0]["origins"],
        serde_json::json!(["Sidecar"])
    );
    assert_eq!(
        server
            .requests()
            .iter()
            .filter(|request| request.starts_with("GET "))
            .count(),
        1
    );
    assert_eq!(
        server
            .requests()
            .iter()
            .filter(|request| request.starts_with("HEAD "))
            .count(),
        2
    );
}

#[test]
fn malformed_progress_preserves_following_json_option_and_respects_terminator() {
    let directory = Directory::fresh();
    for reference in [
        "https://example.test/file",
        "https://user:password@example.test/file?signature=secret",
    ] {
        let result = directory.run(&[reference, "--progress", "--json"]);
        assert_eq!(result.status.code(), Some(2));
        assert!(result.stdout.is_empty());
        let records = events(&result.stderr);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["schema_version"], 1);
        assert_eq!(records[0]["kind"], "Error");
        assert_eq!(records[0]["facts"]["exit_code"], 2);
        let text = String::from_utf8_lossy(&result.stderr);
        for secret in ["password", "signature", "secret"] {
            assert!(!text.contains(secret));
        }
    }
    let terminated = directory.run(&["https://example.test/file", "--progress", "--", "--json"]);
    assert_eq!(terminated.status.code(), Some(2));
    assert!(terminated.stdout.is_empty());
    assert!(
        terminated.stderr.starts_with(b"error "),
        "a token after -- is not a JSON presentation option"
    );
}

#[test]
#[cfg(feature = "s3")]
fn labelled_sidecars_still_require_one_selected_collection_member() {
    let directory = Directory::fresh();
    let server = Server::start(Behavior::S3Many);
    let checksum = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
    std::fs::write(
        directory.0.join("checks"),
        format!("{checksum}  a.bin\n{checksum}  b.bin\n"),
    )
    .unwrap();
    for arguments in [
        vec![
            "--tree",
            "s3://fixture-bucket/",
            "-o",
            "-",
            "--expect-file",
            "checks",
            "--json",
        ],
        vec![
            "--tree",
            "s3://fixture-bucket/",
            "-D",
            "files",
            "--expect-file",
            "checks",
            "--fresh",
            "--json",
        ],
    ] {
        let result = directory.run_sdk(&server, &arguments);
        assert_eq!(result.status.code(), Some(2));
        assert!(result.stdout.is_empty());
        assert!(events(&result.stderr).iter().any(|event| {
            event["kind"] == "Error"
                && event["facts"]["message"]
                    .as_str()
                    .unwrap()
                    .contains("exactly one selected file")
        }));
    }
    assert!(
        server
            .requests()
            .iter()
            .all(|request| request.starts_with("HEAD ") || request.contains("list-type=2"))
    );
    assert!(!directory.0.join("files").exists());
    assert!(!directory.0.join(".xget-runs").exists());
}

#[test]
fn all_materialization_intent_keeps_positive_and_negative_original_path_filters_active() {
    let directory = Directory::fresh();
    let server = Server::start(Behavior::Complete);
    let reference = server.reference("/a.bin");
    let selected = directory.run(&[
        &reference,
        "--all",
        "--select",
        "a.bin",
        "--exclude",
        "*.sig",
        "-o",
        "-",
        "--json",
    ]);
    assert_eq!(selected.status.code(), Some(0));
    assert_eq!(selected.stdout, b"abc");
    let before = server.requests();
    for arguments in [
        vec![
            &reference,
            "--all",
            "--select",
            "missing-*",
            "-o",
            "-",
            "--json",
        ],
        vec![
            &reference,
            "--all",
            "--select",
            "a.bin",
            "--exclude",
            "a.bin",
            "-o",
            "-",
            "--json",
        ],
    ] {
        let failed = directory.run(&arguments);
        assert_eq!(failed.status.code(), Some(2));
        assert!(failed.stdout.is_empty());
    }
    assert!(
        server.requests()[before.len()..]
            .iter()
            .all(|request| request.starts_with("HEAD "))
    );
    assert!(!directory.0.join(".xget-runs").exists());
}

#[cfg(unix)]
#[derive(Debug, PartialEq, Eq)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

#[cfg(unix)]
impl FileIdentity {
    fn of(path: &Path) -> Self {
        use std::os::unix::fs::MetadataExt as _;

        let metadata = std::fs::metadata(path).unwrap();
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
        }
    }
}

#[test]
#[cfg(unix)]
fn restart_conflicts_live_authority_and_missing_match_never_start_fresh_work() {
    let directory = Directory::fresh();
    let server = Server::start(Behavior::Complete);
    let reference = server.reference("/a.bin");
    for arguments in [
        vec![
            &reference,
            "--restart",
            "--fresh",
            "-o",
            "out.bin",
            "--json",
        ],
        vec![
            &reference,
            "--fresh",
            "--restart",
            "-o",
            "out.bin",
            "--json",
        ],
        vec![
            &reference,
            "--restart",
            "--dry-run",
            "-o",
            "out.bin",
            "--json",
        ],
        vec![
            &reference,
            "--restart",
            "--expect-file",
            "absent-checks",
            "-o",
            "out.bin",
            "--json",
        ],
        vec![&reference, "--restart", "-o", "-", "--json"],
        vec![&reference, "--overwrite", "-o", "-", "--json"],
        vec![&reference, "--overwrite", "--dry-run", "-o", "-", "--json"],
        vec![&reference, "--restart", "-o", "out.bin", "--json"],
    ] {
        let result = directory.run(&arguments);
        assert_eq!(result.status.code(), Some(2));
        assert!(result.stdout.is_empty());
        let records = events(&result.stderr);
        assert!(
            !records
                .iter()
                .any(|event| event["kind"] == "Published" || event["kind"] == "End")
        );
    }
    assert!(
        server.requests().is_empty(),
        "restart without a match is not a discovery/fetch fallback"
    );
    assert!(!directory.0.join("out.bin").exists());
    assert!(!directory.0.join(".xget-runs").exists());
}

#[test]
#[cfg(unix)]
fn native_destination_digest_and_short_body_failures_have_distinct_actionable_diagnostics() {
    let wrong_sha = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
    for (behavior, incumbent, expected_sha, reason, message) in [
        (
            Behavior::Complete,
            Some(b"sentinel".as_slice()),
            None,
            "DestinationExists",
            "destination exists",
        ),
        (
            Behavior::Complete,
            None,
            Some(wrong_sha),
            "DigestMismatch",
            "required digest does not match",
        ),
        // Response-frame decode failures fall under the public HttpErrorKind::Request fallback.
        // The gate proves accepted headers/native ownership, not a different transport category
        // or clean EOF from which the coordinator could report LengthMismatch.
        (
            Behavior::ShortBodyAfterHeaders,
            None,
            None,
            "HttpRequest",
            "http response failed before clean completion",
        ),
    ] {
        let directory = Directory::fresh();
        let server = Server::start(behavior);
        let reference = server.reference("/a.bin?signature=uxdoc-query-marker");
        if let Some(bytes) = incumbent {
            std::fs::write(directory.0.join("out.bin"), bytes).unwrap();
        }
        let mut arguments = vec![
            reference.as_str(),
            "-o",
            "out.bin",
            "--json",
            "-H",
            "Authorization: Bearer uxdoc-access-marker",
        ];
        if let Some(digest) = expected_sha {
            arguments.extend(["--expect", digest]);
        }
        let failed = match behavior {
            Behavior::ShortBodyAfterHeaders => {
                directory.run_after_native_start(&server, &arguments)
            }
            _ => directory.run(&arguments),
        };
        assert_eq!(
            failed.status.code(),
            Some(1),
            "{}",
            String::from_utf8_lossy(&failed.stderr)
        );
        assert!(failed.stdout.is_empty());
        let records = events(&failed.stderr);
        let run_failed = records
            .iter()
            .find(|event| event["kind"] == "RunFailed")
            .unwrap();
        let error = records
            .iter()
            .find(|event| event["kind"] == "Error")
            .unwrap();
        for record in [run_failed, error] {
            let facts = &record["facts"];
            assert_eq!(facts["reason"], reason, "{facts}");
            assert!(
                facts["message"].as_str().unwrap().contains(message),
                "{facts}"
            );
            assert!(facts["phase"].is_null(), "{facts}");
            assert_eq!(
                facts["message"].as_str().unwrap(),
                facts["message"].as_str().unwrap().to_lowercase()
            );
            assert!(facts["partial"]["logical_received_bytes"].is_u64());
            assert!(facts["partial"]["logical_written_bytes"].is_u64());
            assert!(facts["partial"]["hashed_bytes"].is_u64());
            assert_eq!(facts["partial"]["completion_evidence"], false);
            assert_eq!(facts["pause_confirmed"], false);
            assert!(facts["state_path"].as_str().is_some());
        }
        for field in [
            "reason",
            "message",
            "phase",
            "partial",
            "native_state_path",
            "checkpoint_failed",
        ] {
            assert_eq!(run_failed["facts"][field], error["facts"][field]);
        }
        assert_eq!(run_failed["facts"]["complete"], false);
        assert_eq!(run_failed["facts"]["completed_files"], 0);
        assert_eq!(error["facts"]["exit_code"], 1);
        assert_eq!(run_failed["run_id"], error["run_id"]);
        if incumbent.is_none() {
            assert!(error["facts"]["native_state_path"].as_str().is_some());
            assert!(!directory.0.join("out.bin").exists());
        } else {
            assert_eq!(
                std::fs::read(directory.0.join("out.bin")).unwrap(),
                b"sentinel"
            );
        }
        assert!(
            !records
                .iter()
                .any(|event| event["kind"] == "Published" || event["kind"] == "End")
        );
        let text = String::from_utf8_lossy(&failed.stderr);
        for secret in [
            "uxdoc-query-marker",
            "uxdoc-access-marker",
            reference.as_str(),
        ] {
            assert!(!text.contains(secret));
        }
    }
}

#[test]
#[cfg(unix)]
fn captured_plain_failure_keeps_actionable_reason_and_partial_on_stderr() {
    let directory = Directory::fresh();
    let server = Server::start(Behavior::ShortBodyAfterHeaders);
    let reference = server.reference("/a.bin?signature=uxdoc-plain-marker");
    let failed = directory.run_after_native_start(
        &server,
        &[&reference, "-o", "out.bin", "--progress", "plain"],
    );
    assert_eq!(failed.status.code(), Some(1));
    assert!(failed.stdout.is_empty());
    let text = String::from_utf8(failed.stderr).unwrap();
    assert!(
        text.contains("http response failed before clean completion"),
        "{text}"
    );
    assert!(text.contains("\"reason\":\"HttpRequest\""));
    assert!(text.contains("\"phase\":null"));
    assert!(text.contains("\"partial\":"));
    assert!(text.contains("\"logical_received_bytes\":"));
    assert!(text.contains("\"completion_evidence\":false"));
    assert!(!text.contains("uxdoc-plain-marker"));
    assert!(!text.contains('\u{1b}'));
    assert!(!text.contains('\r'));
    assert!(text.ends_with('\n'));
    assert!(!directory.0.join("out.bin").exists());
}

#[test]
#[cfg(unix)]
fn overwrite_checks_and_late_eof_failures_preserve_incumbent_bytes_and_identity() {
    for behavior in [Behavior::Complete, Behavior::ShortBody] {
        let directory = Directory::fresh();
        let server = Server::start(behavior);
        let reference = server.reference("/a.bin");
        let output = directory.0.join("out.bin");
        std::fs::write(&output, b"sentinel").unwrap();
        let original = FileIdentity::of(&output);
        let mut arguments = vec![&reference, "-o", "out.bin", "--overwrite", "--json"];
        if matches!(behavior, Behavior::Complete) {
            arguments.extend([
                "--expect",
                "sha256:0000000000000000000000000000000000000000000000000000000000000000",
            ]);
        }
        let failed = directory.run(&arguments);
        assert_eq!(failed.status.code(), Some(1));
        assert_eq!(std::fs::read(&output).unwrap(), b"sentinel");
        assert_eq!(FileIdentity::of(&output), original);
        assert!(
            !events(&failed.stderr)
                .iter()
                .any(|event| event["kind"] == "Published" || event["kind"] == "End")
        );
    }
}

#[test]
#[cfg(unix)]
fn fresh_is_not_overwrite_but_explicit_checked_replacement_succeeds() {
    let directory = Directory::fresh();
    let server = Server::start(Behavior::Complete);
    let reference = server.reference("/a.bin");
    let output = directory.0.join("out.bin");
    std::fs::write(&output, b"sentinel").unwrap();
    let original = FileIdentity::of(&output);
    let preview = directory.run(&[
        &reference,
        "-o",
        "out.bin",
        "--overwrite",
        "--dry-run",
        "--json",
    ]);
    assert_eq!(preview.status.code(), Some(0));
    let planned = events(&preview.stderr);
    let file = planned
        .iter()
        .find(|event| event["kind"] == "Planned")
        .unwrap();
    assert_eq!(file["facts"]["replacement_requested"], true);
    assert_eq!(file["facts"]["execution_authority"], false);
    assert!(
        server
            .requests()
            .iter()
            .all(|request| request.starts_with("HEAD "))
    );
    assert_eq!(FileIdentity::of(&output), original);
    assert!(!directory.0.join(".xget-runs").exists());
    for authority in [vec![], vec!["--fresh"]] {
        let arguments = [
            vec![reference.as_str(), "-o", "out.bin", "--json"],
            authority,
        ]
        .concat();
        let denied = directory.run(&arguments);
        assert_eq!(denied.status.code(), Some(1));
        assert_eq!(std::fs::read(&output).unwrap(), b"sentinel");
        assert_eq!(FileIdentity::of(&output), original);
    }
    let replaced = directory.run(&[
        &reference,
        "-o",
        "out.bin",
        "--fresh",
        "-f",
        "--expect",
        "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        "--json",
    ]);
    assert_eq!(replaced.status.code(), Some(0));
    assert_eq!(std::fs::read(&output).unwrap(), b"abc");
    assert_ne!(FileIdentity::of(&output), original);
    assert!(
        events(&replaced.stderr)
            .iter()
            .any(|event| event["kind"] == "Published")
    );
}

#[test]
#[cfg(unix)]
fn overwrite_does_not_authorize_duplicate_or_aliased_targets_within_one_run() {
    let directory = Directory::fresh();
    let server = Server::start(Behavior::Complete);
    let a = server.reference("/a.bin");
    let b = server.reference("/b.bin");
    let a_path = directory.0.join("a.bin");
    let b_path = directory.0.join("b.bin");
    std::fs::write(&a_path, b"sentinel").unwrap();
    std::fs::hard_link(&a_path, &b_path).unwrap();
    let original = FileIdentity::of(&a_path);
    let duplicate = directory.run(&[&a, &a, "-D", ".", "--overwrite", "--json"]);
    assert_eq!(duplicate.status.code(), Some(2));
    let duplicate_events = events(&duplicate.stderr);
    let diagnostic = &duplicate_events
        .iter()
        .find(|event| event["kind"] == "Error")
        .unwrap()["facts"];
    assert_eq!(diagnostic["reason"], "LayoutConflict");
    let message = diagnostic["message"].as_str().unwrap();
    assert!(message.contains("separate invocations with distinct -o paths"));
    assert!(message.contains("input namespaces through the rust api"));
    assert!(!message.contains("--namespace"));
    assert!(
        server
            .requests()
            .iter()
            .all(|request| request.starts_with("HEAD "))
    );
    let aliases = directory.run(&[&a, &b, "-D", ".", "--overwrite", "--json"]);
    assert!(!aliases.status.success());
    assert!(
        !events(&aliases.stderr)
            .iter()
            .any(|event| event["kind"] == "Published")
    );
    assert_eq!(std::fs::read(&a_path).unwrap(), b"sentinel");
    assert_eq!(std::fs::read(&b_path).unwrap(), b"sentinel");
    assert_eq!(FileIdentity::of(&a_path), original);
    assert_eq!(FileIdentity::of(&b_path), original);
}

#[test]
#[cfg(unix)]
fn restart_uses_frozen_receiving_work_without_new_head_or_mutating_old_attempts() {
    let directory = Directory::fresh();
    let server = Server::start(Behavior::ShortFirstWhole);
    let reference = server.reference("/a.bin");
    let digest = "sha256:bef57ec7f53a6d40beb640a780a639c83bc29ac8a9816f1fc6c5c6dcd93c4721";
    let first = directory.run(&[&reference, "-o", "out.bin", "--expect", digest, "--json"]);
    assert_eq!(first.status.code(), Some(1));
    assert!(!directory.0.join("out.bin").exists());
    let records = events(&first.stderr);
    let state = records
        .iter()
        .find_map(|event| event["facts"]["state_path"].as_str())
        .unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let saved = runtime
        .block_on(xget::client::SavedRun::inspect(state))
        .unwrap();
    let member = saved.members().next().unwrap();
    assert_eq!(member.attempts().len(), 1);
    let old_workspace = member.attempts().next().unwrap().path().to_path_buf();
    let snapshot = || {
        let mut files = std::fs::read_dir(&old_workspace)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                (entry.file_name(), std::fs::read(entry.path()).unwrap())
            })
            .collect::<Vec<_>>();
        files.sort();
        files
    };
    let old_files = snapshot();
    let before = server.requests();
    let restarted = directory.run(&[
        &reference,
        "-o",
        "out.bin",
        "--expect",
        digest,
        "--restart",
        "--json",
    ]);
    assert_eq!(restarted.status.code(), Some(0));
    assert_eq!(
        std::fs::read(directory.0.join("out.bin")).unwrap(),
        b"abcdef"
    );
    assert_eq!(snapshot(), old_files);
    assert!(
        server.requests()[before.len()..]
            .iter()
            .all(|request| !request.starts_with("HEAD "))
    );
    let complete = runtime
        .block_on(xget::client::SavedRun::inspect(state))
        .unwrap();
    assert!(complete.is_complete());
    assert_eq!(complete.members().next().unwrap().attempts().len(), 2);
    let before_missing = server.requests();
    let completed_identity = FileIdentity::of(&directory.0.join("out.bin"));
    let missing = directory.run(&[
        &reference,
        "-o",
        "out.bin",
        "--expect",
        digest,
        "--restart",
        "--json",
    ]);
    assert_eq!(
        missing.status.code(),
        Some(2),
        "a complete prior run is not unfinished restart work"
    );
    assert_eq!(server.requests(), before_missing);
    assert_eq!(
        FileIdentity::of(&directory.0.join("out.bin")),
        completed_identity
    );
}

#[test]
#[cfg(unix)]
fn explicit_resume_restart_overwrite_reconciles_completed_files_without_fetching_them() {
    let directory = Directory::fresh();
    let server = Server::start(Behavior::FailSecondOnce);
    let a = server.reference("/a.bin");
    let b = server.reference("/b.bin");
    let first = directory.run(&[&a, &b, "-D", "downloads", "--json"]);
    assert_eq!(first.status.code(), Some(1));
    let records = events(&first.stderr);
    let state = records
        .iter()
        .find_map(|event| event["facts"]["state_path"].as_str())
        .unwrap();
    let a_path = directory.0.join("downloads/a.bin");
    let identity = FileIdentity::of(&a_path);
    let b_path = directory.0.join("downloads/b.bin");
    std::fs::write(&b_path, b"sentinel").unwrap();
    let b_identity = FileIdentity::of(&b_path);
    let inspected = directory.run(&["info", state, "--json"]);
    assert_eq!(inspected.status.code(), Some(0));
    let info = events(&inspected.stdout);
    let bindings = info
        .iter()
        .filter(|event| event["kind"] == "SavedMember")
        .map(|event| {
            let name = Path::new(event["facts"]["destination"].as_str().unwrap())
                .file_name()
                .unwrap()
                .to_str()
                .unwrap();
            let reference = match name {
                "a.bin" => &a,
                "b.bin" => &b,
                _ => panic!("unexpected fixture member"),
            };
            format!(
                "{}={reference}",
                event["facts"]["binding_key"].as_str().unwrap()
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(bindings.len(), 2);
    let before = server.requests();
    let resumed = directory.run(&[
        "resume",
        state,
        "--input",
        &bindings[0],
        "--input",
        &bindings[1],
        "--restart",
        "-f",
        "--json",
    ]);
    assert_eq!(resumed.status.code(), Some(0));
    assert_eq!(FileIdentity::of(&a_path), identity);
    assert_eq!(std::fs::read(&a_path).unwrap(), b"abc");
    assert_eq!(std::fs::read(&b_path).unwrap(), b"abc");
    assert_ne!(FileIdentity::of(&b_path), b_identity);
    assert_eq!(server.requests()[before.len()..].len(), 1);
    assert!(server.requests()[before.len()].starts_with("GET /b.bin "));
    let completed_a = records
        .iter()
        .find(|event| event["kind"] == "Published")
        .unwrap();
    let resumed_records = events(&resumed.stderr);
    assert!(
        !resumed_records
            .iter()
            .any(|event| event["kind"] == "Progress" && event["item_id"] == completed_a["item_id"])
    );
    let before_complete = server.requests();
    let b_complete = FileIdentity::of(&b_path);
    let again = directory.run(&[
        "resume",
        state,
        "--input",
        &bindings[0],
        "--input",
        &bindings[1],
        "--restart",
        "--overwrite",
        "--json",
    ]);
    assert_eq!(again.status.code(), Some(0));
    assert_eq!(server.requests(), before_complete);
    assert_eq!(FileIdentity::of(&a_path), identity);
    assert_eq!(FileIdentity::of(&b_path), b_complete);
    assert!(
        !events(&again.stderr)
            .iter()
            .any(|event| event["kind"] == "Progress")
    );
}

#[test]
#[cfg(unix)]
fn overwrite_refuses_an_incumbent_changed_after_capture_even_with_equal_incoming_bytes() {
    use std::io::{BufRead as _, BufReader};
    use std::process::Stdio;

    let directory = Directory::fresh();
    let server = Server::start(Behavior::GatedIncumbent);
    let reference = server.reference("/a.bin");
    let output = directory.0.join("out.bin");
    std::fs::write(&output, b"oldold").unwrap();
    let original = FileIdentity::of(&output);
    let mut child = Command::new(env!("CARGO_BIN_EXE_xget"))
        .args([
            &reference,
            "-o",
            "out.bin",
            "-f",
            "--expect",
            "sha256:bef57ec7f53a6d40beb640a780a639c83bc29ac8a9816f1fc6c5c6dcd93c4721",
            "--json",
        ])
        .current_dir(&directory.0)
        .env("TMPDIR", directory.0.join("tmp"))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut reader = BufReader::new(child.stderr.take().unwrap());
    let mut recorded = Vec::new();
    let mut opened_prefix = false;
    let mut captured = false;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap() == 0 {
            break;
        }
        let event: Value = serde_json::from_str(&line).unwrap();
        if event["kind"] == "Progress" {
            if event["facts"]["logical_received_bytes"].as_u64().unwrap() > 0 {
                recorded.push(event);
                captured = true;
                break;
            }
            if !opened_prefix {
                // Wait only for CLI rendering cadence. The source prefix and tail gates below,
                // plus actual positive native receive facts, establish operation ordering.
                thread::sleep(Duration::from_millis(150));
                server.release_prefix.store(true, Ordering::Relaxed);
                opened_prefix = true;
            }
        }
        recorded.push(event);
    }
    if !captured {
        server.release_prefix.store(true, Ordering::Relaxed);
        server.release_tail.store(true, Ordering::Relaxed);
        let _ = child.kill();
        let _ = child.wait();
        panic!("owned child did not reach received work after native incumbent capture");
    }
    let foreign = directory.0.join("new-incumbent");
    std::fs::write(&foreign, b"abcdef").unwrap();
    std::fs::rename(&foreign, &output).unwrap();
    let changed = FileIdentity::of(&output);
    assert_ne!(changed, original);
    server.release_tail.store(true, Ordering::Relaxed);
    let completed = child.wait_with_output().unwrap();
    let mut remaining = Vec::new();
    reader.read_to_end(&mut remaining).unwrap();
    recorded.extend(events(&remaining));
    assert_eq!(completed.status.code(), Some(1));
    assert_eq!(std::fs::read(&output).unwrap(), b"abcdef");
    assert_eq!(FileIdentity::of(&output), changed);
    assert!(
        !recorded
            .iter()
            .any(|event| event["kind"] == "Published" || event["kind"] == "End")
    );
}

#[test]
fn removed_interfaces_do_not_dispatch_a_second_engine_or_touch_the_network() {
    let directory = Directory::fresh();
    let server = Server::start(Behavior::Complete);
    let reference = server.reference("/a.bin");
    for arguments in [
        vec!["legacy", reference.as_str(), "--json"],
        vec![reference.as_str(), "out.bin", "--json"],
    ] {
        let rejected = directory.run(&arguments);
        assert_eq!(rejected.status.code(), Some(2));
        assert!(rejected.stdout.is_empty());
    }
    assert!(server.requests().is_empty());
    let help = directory.run(&["--help"]);
    assert_eq!(help.status.code(), Some(0));
    assert!(!String::from_utf8_lossy(&help.stdout).contains("legacy"));
}
