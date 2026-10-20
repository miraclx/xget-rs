//! Public lifetime-specific source/helper shapes, using the real native range path.
#![allow(clippy::unwrap_used)]

use core::future::{Future, Ready, ready};
use std::path::PathBuf;

use bytes::Bytes;
use futures::Stream;
use futures::stream::{self, Iter};
use xget::Transfer;
use xget::transfer::reader::{
    Evidence, RangeReader, ReadResponse, ReaderError, ReaderReceipt, Window,
};
use xget::transfer::{ByteCount, Failure};

#[derive(Debug, PartialEq, Eq)]
struct BorrowedError<'data>(&'data str);

impl core::fmt::Display for BorrowedError<'_> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(self.0)
    }
}

impl core::error::Error for BorrowedError<'_> {}

struct Borrowed<'data> {
    bytes: &'data [u8],
    label: &'data str,
}

impl<'read, 'data: 'read> RangeReader<'read> for Borrowed<'data> {
    type Error = BorrowedError<'read>;
    type Body = Iter<core::array::IntoIter<Result<Bytes, Self::Error>, 1>>;
    type Read = Ready<Result<ReadResponse<'read, Self::Body>, Self::Error>>;
    fn size(&self) -> ByteCount {
        ByteCount(self.bytes.len() as u64)
    }
    fn evidence(&'read self) -> Evidence<'read> {
        Evidence::Enforced {
            representation: self.label,
            version: b"immutable borrow",
        }
    }
    fn read(&'read self, window: Window) -> Self::Read {
        let bytes =
            Bytes::copy_from_slice(&self.bytes[window.start().0 as usize..window.end().0 as usize]);
        ready(Ok(ReadResponse {
            window,
            evidence: self.evidence(),
            body: stream::iter([Ok(bytes)]),
        }))
    }
}

// Name one borrow's families, avoiding projection generalization and a for<'a> static requirement.
fn send_helper<'read, R, F, B, E>(
    reader: &'read R,
    destination: PathBuf,
) -> impl Future<Output = Result<ReaderReceipt, Box<Failure<ReaderError<E>>>>> + Send + 'read
where
    R: RangeReader<'read, Read = F, Body = B, Error = E> + Sync,
    F: Future<Output = Result<ReadResponse<'read, B>, E>> + Send + 'read,
    B: Stream<Item = Result<Bytes, E>> + Send + 'read,
    E: Send + 'read,
{
    Transfer::from_reader(reader)
        .to_file(destination)
        .unwrap()
        .run()
}

fn assert_send<T: Send>(value: T) -> T {
    value
}

#[cfg(unix)]
#[tokio::test]
async fn actual_public_generic_send_helper_accepts_nonstatic_source_and_error_families() {
    use std::fs;
    use std::os::unix::fs::DirBuilderExt as _;
    use std::time::{SystemTime, UNIX_EPOCH};

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root =
        std::env::temp_dir().join(format!("xget-reader-public-{}-{now:x}", std::process::id()));
    fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
    let label = String::from("application-owned bytes");
    let data = vec![b'a', b'b', b'c'];
    let reader = Borrowed {
        bytes: &data,
        label: &label,
    };
    let receipt = assert_send(send_helper(&reader, root.join("borrowed")))
        .await
        .unwrap();
    assert_eq!(receipt.native().partial().written, ByteCount(3));
    assert_eq!(fs::read(root.join("borrowed")).unwrap(), data);
    // Owned Send values moved into a caller-spawnable future remain Send as well.
    let destination = root.join("owned");
    let owned = assert_send(async move {
        let data = vec![b'a', b'b', b'c'];
        let label = String::from("owned bytes");
        let reader = Borrowed {
            bytes: &data,
            label: &label,
        };
        Transfer::from_reader(&reader)
            .to_file(destination)
            .unwrap()
            .run()
            .await
            .unwrap()
    });
    owned.await;
    fs::remove_dir_all(root).unwrap();
}

#[cfg(not(unix))]
#[tokio::test]
async fn generic_borrowed_send_path_is_typed_unsupported_without_source_polling() {
    let label = String::from("application-owned bytes");
    let data = vec![b'a'];
    let reader = Borrowed {
        bytes: &data,
        label: &label,
    };
    let failure = assert_send(send_helper(&reader, PathBuf::from("output")))
        .await
        .unwrap_err();
    assert!(matches!(
        failure.kind,
        xget::transfer::FailureKind::Unsupported
    ));
}
