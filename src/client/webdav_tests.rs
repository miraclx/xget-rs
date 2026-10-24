//! Owned loopback public-Client fixtures. Authored source only, not execution evidence.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use core::num::NonZeroUsize;
#[cfg(unix)]
use core::sync::atomic::{AtomicU64, Ordering};
use core::time::Duration;
#[cfg(unix)]
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::DirBuilderExt as _;
#[cfg(unix)]
use std::path::PathBuf;

use futures::StreamExt as _;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{Listing, WebDavError};
#[cfg(unix)]
use crate::client::SavedRun;
use crate::client::{Client, Error, VersionConsistency};
use crate::input::Input;
use crate::inventory::{DirectoryKind, VersionEvidence};
use crate::transfer::{ByteCount, Length};

fn response(href: &str, kind: &str, length: &str, etag: &str) -> String {
    format!(
        "<x:response><x:href>{href}</x:href><x:propstat><x:prop><x:resourcetype>{kind}</x:resourcetype>{length}{etag}</x:prop><x:status>HTTP/1.1 200 OK</x:status></x:propstat></x:response>"
    )
}

fn directory(href: &str) -> String {
    response(href, "<x:collection/>", "", "")
}

fn file(href: &str, length: u64, etag: Option<&str>) -> String {
    response(
        href,
        "",
        &format!("<x:getcontentlength>{length}</x:getcontentlength>"),
        &etag
            .map(|etag| format!("<x:getetag>{etag}</x:getetag>"))
            .unwrap_or_default(),
    )
}

fn listing(entries: impl IntoIterator<Item = String>) -> String {
    format!(
        "<?xml version=\"1.0\"?><x:multistatus xmlns:x=\"DAV:\">{}</x:multistatus>",
        entries.into_iter().collect::<String>()
    )
}

async fn mount(server: &MockServer, resource: &str, body: String) {
    Mock::given(method("PROPFIND"))
        .and(path(resource))
        .and(header("Depth", "1"))
        .and(header("Accept-Encoding", "identity"))
        .respond_with(ResponseTemplate::new(207).set_body_string(body))
        .mount(server)
        .await;
}

fn tree(server: &MockServer) -> Input {
    Input::tree(&format!("{}/dav/", server.uri())).expect("tree")
}

#[test]
fn namespace_uris_not_prefixes_define_properties_and_dtd_is_refused() {
    let body = listing([directory("/dav/"), file("/dav/a", 0, None)]);
    assert_eq!(
        Listing::parse(
            body.replace("x:", "z:")
                .replace("xmlns:x", "xmlns:z")
                .as_bytes()
        )
        .unwrap()
        .len(),
        2
    );
    assert!(Listing::parse(body.replace("DAV:", "urn:not-dav").as_bytes()).is_err());
    let dtd = "<!DOCTYPE x:multistatus [<!ENTITY href '/dav/'>]><x:multistatus xmlns:x=\"DAV:\"><x:response><x:href>&href;</x:href></x:response></x:multistatus>";
    assert!(matches!(
        Listing::parse(dtd.as_bytes()),
        Err(Error::WebDav(WebDavError::Malformed))
    ));
}

#[tokio::test]
async fn credential_origin_and_original_root_are_separate_from_repeatable_member_paths() {
    let server = MockServer::start().await;
    mount(
        &server,
        "/dav/",
        listing([
            directory("/dav/"),
            file(&format!("{}/dav/a", server.uri()), 0, None),
        ]),
    )
    .await;
    let reference = server
        .uri()
        .replacen("http://", "http://fixture-user:fixture-password@", 1)
        + "/dav/";
    let client = Client::new().unwrap();
    let inventory = client
        .resolve(Input::tree(&reference).unwrap())
        .await
        .unwrap();
    let member = inventory.files().next().unwrap();
    assert_eq!(member.input.original_reference(), reference);
    assert_eq!(member.member_token().unwrap().as_str(), "/dav/a");
    assert!(!format!("{member:?}").contains("fixture-password"));
    assert!(
        server.received_requests().await.unwrap()[0]
            .headers
            .get("authorization")
            .is_some()
    );
    server.reset().await;
    mount(
        &server,
        "/dav/",
        listing([
            directory("/dav/"),
            file(
                &format!("http://other:password@{}/dav/a", server.address()),
                0,
                None,
            ),
        ]),
    )
    .await;
    assert!(matches!(
        client.resolve(Input::tree(&reference).unwrap()).await,
        Err(Error::WebDav(WebDavError::Scope))
    ));
    assert!(
        client
            .resolve(Input::tree(&(reference + "?bearer=secret")).unwrap())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn two_levels_preserve_real_href_tokens_paths_directory_facts_and_zero_length() {
    let server = MockServer::start().await;
    mount(
        &server,
        "/dav/",
        listing([
            directory("/dav/"),
            directory("/dav/sub/"),
            file("/dav/a%20b", 0, Some("&quot;v1&quot;")),
        ]),
    )
    .await;
    mount(
        &server,
        "/dav/sub/",
        listing([directory("/dav/sub/"), file("/dav/sub/c", 3, None)]),
    )
    .await;
    let client = Client::new().unwrap();
    let inventory = client.resolve(tree(&server)).await.unwrap();
    assert_eq!(inventory.files().count(), 2);
    assert_eq!(inventory.directories().count(), 2);
    assert!(
        inventory
            .directories()
            .all(|directory| directory.kind() == DirectoryKind::WebDavCollection)
    );
    let first = inventory.files().next().unwrap();
    assert_eq!(first.source_path(), "a b");
    assert_eq!(first.member_token().unwrap().as_str(), "/dav/a%20b");
    assert_eq!(first.length(), Length::Known(ByteCount(0)));
    assert_eq!(first.version(), VersionEvidence::StrongEtag);
    assert_eq!(
        inventory.files().nth(1).unwrap().version(),
        VersionEvidence::Unpinned
    );
    assert!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.method.as_str() == "PROPFIND")
    );
}

#[tokio::test]
async fn genuine_empty_and_directory_only_trees_have_no_executable_files() {
    for child in [false, true] {
        let server = MockServer::start().await;
        let mut entries = vec![directory("/dav/")];
        if child {
            entries.push(directory("/dav/empty/"));
            mount(&server, "/dav/empty/", listing([directory("/dav/empty/")])).await;
        }
        mount(&server, "/dav/", listing(entries)).await;
        let client = Client::new().unwrap();
        let inventory = client.resolve(tree(&server)).await.unwrap();
        assert_eq!(inventory.files().count(), 0);
        assert_eq!(inventory.directories().count(), if child { 2 } else { 1 });
        let rejected = client.resolve(tree(&server).include("**").unwrap()).await;
        assert!(matches!(rejected, Err(Error::PathSelection)));
    }
}

#[tokio::test]
async fn incomplete_denied_missing_duplicate_alias_and_escaped_scope_are_not_inventories() {
    let cases = [
        listing([file("/dav/a", 1, None)]),
        listing([directory("/dav/"), "<x:response><x:href>/dav/a</x:href><x:status>HTTP/1.1 403 Forbidden</x:status></x:response>".to_owned()]),
        listing([directory("/dav/"), "<x:response><x:href>/dav/a</x:href><x:status>HTTP/1.1 404 Missing</x:status></x:response>".to_owned()]),
        listing([directory("/dav/"), file("/dav/a", 1, None), file("/dav/a", 1, None)]),
        listing([directory("/dav/"), file("/dav/a", 1, None), file("/dav/%61", 1, None)]),
        listing([directory("/dav/"), file("https://foreign.invalid/dav/a", 1, None)]),
        "<!DOCTYPE x:multistatus [<!ENTITY href '/dav/'>]><x:multistatus xmlns:x=\"DAV:\">&href;</x:multistatus>".to_owned(),
        listing([directory("/dav/"), file("/dav/a?token=secret", 1, None)]),
        listing([directory("/dav/"), file("/dav/a#fragment", 1, None)]),
        listing([directory("/dav/"), file("/dav/%2e%2e/a", 1, None)]),
        listing([directory("/dav/"), file("/dav/a%2fb", 1, None)]),
        listing([directory("/dav/"), file("/dav/sub/a", 1, None)]),
        listing([directory("/dav/"), "<x:response><x:href>/dav/a</x:href><x:propstat><x:prop><x:resourcetype/></x:prop><x:status>HTTP/1.1 206 Partial</x:status></x:propstat></x:response>".to_owned()]),
        format!("{}<x:limit xmlns:x=\"DAV:\"/>", listing([directory("/dav/")])),
    ];
    for body in cases {
        let server = MockServer::start().await;
        mount(&server, "/dav/", body).await;
        let result = Client::new().unwrap().resolve(tree(&server)).await;
        assert!(result.is_err(), "malformed/incomplete listing must fail");
        assert!(
            server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .all(|request| request.method.as_str() != "GET")
        );
    }
}

#[tokio::test]
async fn child_listing_cannot_loop_back_to_its_parent() {
    let server = MockServer::start().await;
    mount(
        &server,
        "/dav/",
        listing([directory("/dav/"), directory("/dav/sub/")]),
    )
    .await;
    mount(
        &server,
        "/dav/sub/",
        listing([directory("/dav/sub/"), directory("/dav/")]),
    )
    .await;
    assert!(Client::new().unwrap().resolve(tree(&server)).await.is_err());
}

#[tokio::test]
async fn case_and_file_directory_collisions_fail_before_selection() {
    for entries in [
        vec![
            directory("/dav/"),
            file("/dav/A", 1, None),
            file("/dav/a", 1, None),
        ],
        vec![
            directory("/dav/"),
            file("/dav/a", 1, None),
            directory("/dav/a/"),
        ],
    ] {
        let server = MockServer::start().await;
        mount(&server, "/dav/", listing(entries)).await;
        mount(&server, "/dav/a/", listing([directory("/dav/a/")])).await;
        let result = Client::new()
            .unwrap()
            .resolve(tree(&server).include("missing").unwrap())
            .await;
        assert!(matches!(result, Err(Error::LayoutConflict)));
    }
}

#[tokio::test]
async fn literal_tokens_and_each_positive_criterion_use_complete_original_scope() {
    let server = MockServer::start().await;
    mount(
        &server,
        "/dav/",
        listing([
            directory("/dav/"),
            file("/dav/a", 1, None),
            file("/dav/b", 1, None),
        ]),
    )
    .await;
    let client = Client::new().unwrap();
    let all = client
        .resolve(tree(&server))
        .await
        .unwrap()
        .metadata_bytes();
    let selected = client
        .resolve(
            tree(&server)
                .include("a")
                .unwrap()
                .include("b")
                .unwrap()
                .select_id("/dav/a")
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(selected.files().count(), 1);
    assert!(selected.metadata_bytes() >= all);
    let selected = selected.select_one(|_| true).unwrap();
    let subset = client.resolve(selected).await.unwrap();
    assert_eq!(
        subset.files().count(),
        1,
        "prior positive proof is not reevaluated on a subset"
    );
    assert!(matches!(
        client
            .resolve(
                tree(&server)
                    .include("a")
                    .unwrap()
                    .include("never")
                    .unwrap()
            )
            .await,
        Err(Error::PathSelection)
    ));
    assert!(matches!(
        client
            .resolve(tree(&server).select_id("/dav/stale").unwrap())
            .await,
        Err(Error::Selection)
    ));
}

#[tokio::test]
async fn raw_control_and_shared_entry_limits_fail_without_payloads() {
    for body in [
        "x".repeat(super::MAX_CONTROL + 1),
        listing(
            (0..129)
                .map(|index| file(&format!("/dav/{index}"), 0, None))
                .chain([directory("/dav/")]),
        ),
    ] {
        let server = MockServer::start().await;
        mount(&server, "/dav/", body).await;
        assert!(Client::new().unwrap().resolve(tree(&server)).await.is_err());
    }
}

#[tokio::test]
async fn excessive_tree_depth_and_incomplete_optional_property_status_are_typed_failures() {
    let server = MockServer::start().await;
    let mut current = "/dav/".to_owned();
    for _ in 0..=super::MAX_DEPTH {
        let next = format!("{current}d/");
        mount(
            &server,
            &current,
            listing([directory(&current), directory(&next)]),
        )
        .await;
        current = next;
    }
    assert!(matches!(
        Client::new().unwrap().resolve(tree(&server)).await,
        Err(Error::WebDav(WebDavError::Limit))
    ));
    server.reset().await;
    let body = listing([directory("/dav/"), file("/dav/a", 0, None)]).replace(
        "</x:propstat></x:response>",
        "</x:propstat><x:propstat><x:prop/><x:status>HTTP/1.1 403 Forbidden</x:status></x:propstat></x:response>");
    mount(&server, "/dav/", body).await;
    assert!(matches!(
        Client::new().unwrap().resolve(tree(&server)).await,
        Err(Error::WebDav(WebDavError::Denied))
    ));
}

#[tokio::test]
async fn outer_http_status_and_redirect_never_become_empty_or_cross_origin_discovery() {
    for status in [200, 301, 401, 403, 404, 500] {
        let server = MockServer::start().await;
        Mock::given(method("PROPFIND"))
            .respond_with(
                ResponseTemplate::new(status)
                    .insert_header("location", "https://foreign.invalid/dav/"),
            )
            .mount(&server)
            .await;
        assert!(Client::new().unwrap().resolve(tree(&server)).await.is_err());
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }
}

#[tokio::test]
async fn exact_missing_http_never_tries_propfind_and_mixed_http_webdav_use_common_layout() {
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .and(path("/missing"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    let client = Client::new().unwrap();
    assert!(
        client
            .resolve(format!("{}/missing", server.uri()))
            .await
            .is_err()
    );
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
    mount(
        &server,
        "/dav/",
        listing([directory("/dav/"), file("/dav/a", 0, None)]),
    )
    .await;
    Mock::given(method("HEAD"))
        .and(path("/exact"))
        .respond_with(ResponseTemplate::new(200).insert_header("content-length", "0"))
        .mount(&server)
        .await;
    let plan = client
        .download_all([
            tree(&server).under("tree").unwrap(),
            Input::parse(&format!("{}/exact", server.uri())).unwrap(),
        ])
        .to_directory("unused-webdav-fixture-output")
        .plan()
        .await
        .unwrap();
    assert_eq!(plan.files().count(), 2);
}

#[tokio::test]
async fn cap_one_is_held_by_the_selected_body_and_drop_releases_it() {
    let server = MockServer::start().await;
    mount(
        &server,
        "/dav/",
        listing([directory("/dav/"), file("/dav/a", 3, None)]),
    )
    .await;
    Mock::given(method("GET"))
        .and(path("/dav/a"))
        .respond_with(ResponseTemplate::new(200).set_body_string("abc"))
        .mount(&server)
        .await;
    let client = Client::builder()
        .max_requests(NonZeroUsize::new(1).unwrap())
        .build()
        .unwrap();
    let opened = client.open(tree(&server)).await.unwrap();
    let wait = client.resolve(tree(&server));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), wait)
            .await
            .is_err()
    );
    drop(opened);
    assert!(client.resolve(tree(&server)).await.is_ok());
}

#[tokio::test]
async fn unpinned_metadata_length_is_enforced_and_known_pin_cannot_disappear() {
    for (etag, payload, response_etag) in [
        (None, "abcd", None),
        (Some("&quot;v1&quot;"), "abc", None),
        (Some("&quot;v1&quot;"), "abc", Some("\"v2\"")),
    ] {
        let server = MockServer::start().await;
        mount(
            &server,
            "/dav/",
            listing([directory("/dav/"), file("/dav/a", 3, etag)]),
        )
        .await;
        let mut response = ResponseTemplate::new(200).set_body_string(payload);
        if let Some(etag) = response_etag {
            response = response.insert_header("etag", etag);
        }
        Mock::given(method("GET"))
            .respond_with(response)
            .mount(&server)
            .await;
        assert!(matches!(
            Client::new().unwrap().open(tree(&server)).await,
            Err(Error::VersionChanged)
        ));
    }
}

#[cfg(unix)]
#[tokio::test]
async fn directory_only_execution_creates_nothing_and_unpinned_empty_files_are_real_publications() {
    let root = Scratch::new();
    let server = MockServer::start().await;
    mount(&server, "/dav/", listing([directory("/dav/")])).await;
    let client = Client::new().unwrap();
    let destination = root.0.join("no-directory-restoration");
    let report = client
        .download(tree(&server))
        .to_directory(&destination)
        .run()
        .await
        .unwrap();
    assert_eq!(report.completed().len(), 0);
    assert!(!destination.exists());
    server.reset().await;
    mount(
        &server,
        "/dav/",
        listing([directory("/dav/"), file("/dav/empty", 0, None)]),
    )
    .await;
    Mock::given(method("GET"))
        .and(path("/dav/empty"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(Vec::<u8>::new()))
        .mount(&server)
        .await;
    let report = client
        .download(tree(&server))
        .to_directory(&root.0)
        .run()
        .await
        .unwrap();
    assert_eq!(report.completed().len(), 1);
    assert_eq!(fs::metadata(root.0.join("empty")).unwrap().len(), 0);
    let saved = SavedRun::inspect(report.saved_run().unwrap())
        .await
        .unwrap();
    assert_eq!(
        saved.members().next().unwrap().version(),
        VersionEvidence::Unpinned
    );
}

#[cfg(unix)]
#[tokio::test]
async fn frozen_continuation_restores_href_pin_and_recipe_without_propfind_or_head() {
    let root = Scratch::new();
    let server = MockServer::start().await;
    mount(
        &server,
        "/dav/",
        listing([
            directory("/dav/"),
            file("/dav/a", 3, Some("&quot;v1&quot;")),
            file("/dav/b", 3, Some("&quot;v1&quot;")),
        ]),
    )
    .await;
    Mock::given(method("GET"))
        .and(path("/dav/a"))
        .and(header("If-Match", "\"v1\""))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("etag", "\"v1\"")
                .set_body_string("abc"),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/dav/b"))
        .respond_with(ResponseTemplate::new(412))
        .mount(&server)
        .await;
    let client = Client::new().unwrap();
    let reference = format!(
        "http://credential-user:credential-secret-marker@{}/dav/",
        server.address()
    );
    let failure = client
        .download(Input::tree(&reference).unwrap().include("*").unwrap())
        .to_directory(&root.0)
        .run()
        .await
        .unwrap_err();
    assert!(matches!(failure.error, Error::VersionChanged));
    assert_eq!(fs::read(root.0.join("a")).unwrap(), b"abc");
    let saved = SavedRun::inspect(failure.report.saved_run().unwrap())
        .await
        .unwrap();
    let state = fs::read(saved.path().join("state")).unwrap();
    for forbidden in [
        "credential-user",
        "credential-secret-marker",
        reference.as_str(),
    ] {
        assert!(
            !state
                .windows(forbidden.len())
                .any(|bytes| bytes == forbidden.as_bytes())
        );
    }
    let before = server.received_requests().await.unwrap().len();
    let rotated = reference.replace("credential-secret-marker", "rotated-secret-marker");
    assert!(matches!(
        saved.members().next().unwrap().binding_input(&rotated),
        Err(crate::client::SavedError::Binding)
    ));
    assert_eq!(server.received_requests().await.unwrap().len(), before);
    assert!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.headers.contains_key("authorization"))
    );
    let bindings: Vec<_> = saved
        .members()
        .map(|member| (member.key(), member.binding_input(&reference).unwrap()))
        .collect();
    server.reset().await;
    Mock::given(method("GET"))
        .and(path("/dav/b"))
        .and(header("If-Match", "\"v1\""))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("etag", "\"v1\"")
                .set_body_string("def"),
        )
        .mount(&server)
        .await;
    let mut resume = client.resume(saved);
    for (key, input) in bindings {
        resume = resume.input(key, input);
    }
    let report = resume.run().await.unwrap();
    assert_eq!(report.completed().len(), 2);
    assert_eq!(fs::read(root.0.join("b")).unwrap(), b"def");
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method.as_str(), "GET");
    assert!(requests[0].headers.contains_key("authorization"));
}

#[tokio::test]
async fn live_unpinned_zero_length_requires_actual_clean_eof() {
    let server = MockServer::start().await;
    mount(
        &server,
        "/dav/",
        listing([directory("/dav/"), file("/dav/empty", 0, None)]),
    )
    .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(Vec::<u8>::new()))
        .mount(&server)
        .await;
    let client = Client::new().unwrap();
    let opened = client.open(tree(&server)).await.unwrap();
    assert_eq!(opened.version(), VersionConsistency::Unpinned);
    let mut body = opened.into_body();
    assert!(body.next().await.is_none());
    assert!(body.finish().is_ok());
}

#[tokio::test]
async fn a_late_transport_error_after_all_expected_bytes_is_not_successful_eof() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (release, released) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut control, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buffer = [0_u8; 1024];
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let count = control.read(&mut buffer).await.unwrap();
            assert_ne!(count, 0);
            request.extend_from_slice(&buffer[..count]);
        }
        assert!(request.starts_with(b"PROPFIND /dav/ "));
        let body = listing([directory("/dav/"), file("/dav/a", 3, None)]);
        control.write_all(format!("HTTP/1.1 207 Multi-Status\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
        drop(control);
        let (mut payload, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let count = payload.read(&mut buffer).await.unwrap();
            assert_ne!(count, 0);
            request.extend_from_slice(&buffer[..count]);
        }
        assert!(request.starts_with(b"GET /dav/a "));
        payload.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n3\r\nabc\r\n").await.unwrap();
        released.await.unwrap();
        payload.write_all(b"not-a-chunk\r\n").await.unwrap();
    });
    let client = Client::new().unwrap();
    let mut body = client
        .open(Input::tree(&format!("http://{address}/dav/")).unwrap())
        .await
        .unwrap()
        .into_body();
    assert_eq!(body.next().await.unwrap().unwrap(), b"abc"[..]);
    release.send(()).unwrap();
    assert!(body.next().await.unwrap().is_err());
    assert!(body.finish().is_err());
    task.await.unwrap();
}

#[cfg(feature = "s3")]
#[tokio::test]
async fn mixed_http_s3_webdav_members_use_one_common_plan_without_provider_scheduling() {
    use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
    use aws_smithy_types::retry::RetryConfig;
    let server = MockServer::start().await;
    mount(
        &server,
        "/dav/",
        listing([directory("/dav/"), file("/dav/a", 0, None)]),
    )
    .await;
    for resource in ["/exact", "/bucket/object"] {
        Mock::given(method("HEAD"))
            .and(path(resource))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-length", "0")
                    .insert_header("etag", "\"v1\""),
            )
            .mount(&server)
            .await;
    }
    let sdk = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(Credentials::new(
                "owned-fixture",
                "owned-fixture",
                None,
                None,
                "webdav-mixed-fixture",
            ))
            .region(Region::new("us-east-1"))
            .endpoint_url(server.uri())
            .force_path_style(true)
            .retry_config(RetryConfig::disabled())
            .build(),
    );
    let client = Client::builder().s3_client(sdk).build().unwrap();
    let plan = client
        .download_all([
            tree(&server).under("dav").unwrap(),
            Input::parse(&format!("{}/exact", server.uri())).unwrap(),
            Input::parse("s3://bucket/object").unwrap(),
        ])
        .to_directory("unused-owned-mixed-fixture-output")
        .plan()
        .await
        .unwrap();
    assert_eq!(plan.files().count(), 3);
}

#[cfg(unix)]
struct Scratch(PathBuf);

#[cfg(unix)]
impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "xget-owned-webdav-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let mut builder = fs::DirBuilder::new();
        builder
            .mode(0o700)
            .create(&path)
            .expect("fresh owned fixture directory");
        Self(path)
    }
}

#[cfg(unix)]
impl Drop for Scratch {
    fn drop(&mut self) {
        let _cleanup = fs::remove_dir_all(&self.0);
    }
}
