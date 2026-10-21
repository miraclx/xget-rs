#![allow(clippy::expect_used)]

use core::num::NonZeroUsize;
use core::time::Duration;

use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{Client, Error};
use crate::Input;

#[tokio::test]
async fn shared_discovery_deadline_spans_inputs_and_releases_admission() {
    let server = MockServer::start().await;
    for name in ["first", "second"] {
        Mock::given(method("HEAD"))
            .and(path(format!("/{name}")))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-length", "3")
                    .insert_header("etag", "\"v1\"")
                    .set_delay(Duration::from_secs(8)),
            )
            .expect(1)
            .mount(&server)
            .await;
    }
    Mock::given(method("HEAD"))
        .and(path("/third"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;
    Mock::given(method("HEAD"))
        .and(path("/after"))
        .respond_with(ResponseTemplate::new(200).insert_header("content-length", "3"))
        .expect(1)
        .mount(&server)
        .await;
    let client = Client::builder()
        .max_requests(NonZeroUsize::MIN)
        .build()
        .expect("client");
    let inputs = ["first", "second", "third"]
        .map(|name| Input::parse(&format!("{}/{name}", server.uri())).expect("fixture input"));
    let failure = tokio::time::timeout(Duration::from_secs(20), client.resolve_all(inputs))
        .await
        .expect("bounded whole discovery")
        .expect_err("two timely steps exceed one budget");
    assert!(matches!(failure, Error::DiscoveryTimeout));
    assert!(!failure.to_string().contains("http"));
    let after = Input::parse(&format!("{}/after", server.uri())).expect("successor input");
    let inventory = tokio::time::timeout(Duration::from_secs(3), client.resolve(after))
        .await
        .expect("cancelled discovery releases its application admission slot")
        .expect("successor metadata");
    assert_eq!(inventory.files().len(), 1);
    assert!(
        server
            .received_requests()
            .await
            .expect("requests")
            .iter()
            .all(|request| request.method.as_str() == "HEAD")
    );
}
