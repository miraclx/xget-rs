#![allow(clippy::unwrap_used)]

use core::convert::Infallible;
use core::task::Poll;

use bytes::Bytes;
use futures::{FutureExt as _, stream};

use crate::transfer::{FailureKind, Length, Measurement, Transfer};

#[test]
fn unsupported_native_planning_needs_no_clock_runtime_filesystem_or_body_poll() {
    for measurement in [Measurement::default(), Measurement::None] {
        let body = stream::poll_fn(|_| -> Poll<Option<Result<Bytes, Infallible>>> {
            panic!("unsupported native execution must not poll the body");
        });
        let pending = Transfer::from_body(body, Length::Unknown)
            .measurement(measurement)
            .to_file("output")
            .unwrap();
        assert_eq!(
            pending.state().path().file_name().unwrap(),
            ".xget-native-unsupported"
        );
        let failure = pending.run().now_or_never().unwrap().unwrap_err();
        assert!(matches!(failure.kind, FailureKind::Unsupported));
        assert_eq!(failure.state, None);
        assert_eq!(failure.partial.hashed.0, 0);
        assert_eq!(failure.partial.checks, None);
    }
}
