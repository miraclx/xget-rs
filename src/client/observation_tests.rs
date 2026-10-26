#![allow(clippy::unwrap_used)]

use core::cell::RefCell;
use core::convert::Infallible;
use core::future::{Ready, ready};
use core::sync::atomic::{AtomicU64, Ordering};
use std::fs;
use std::os::unix::fs::DirBuilderExt as _;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::stream::{self, Iter};

use super::{BindingKey, Event, FileProgress, Observer, ProgressFacts, RunId};
use crate::transfer::observation::Reception;
use crate::transfer::reader::{Evidence, RangeReader, ReadResponse, Window};
use crate::transfer::{ByteCount, Length, Measurement, Transfer};

struct Directory(PathBuf);

impl Directory {
    fn create() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let next = NEXT.fetch_add(1, Ordering::Relaxed);
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "xget-observation-{}-{time:x}-{next:x}",
            std::process::id()
        ));
        fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
        Self(path)
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

struct Local<'a>(&'a Rc<RefCell<Vec<(BindingKey, ProgressFacts)>>>);

impl Observer for Local<'_> {
    fn observe(&mut self, event: Event<'_>) {
        let Event::Progress { key, facts } = event else {
            panic!("native adapter adds no file or terminal event");
        };
        self.0.borrow_mut().push((key, facts));
    }
}

#[tokio::test]
async fn common_progress_preserves_whole_available_without_synthetic_range_windows() {
    let directory = Directory::create();
    let key = BindingKey::for_run_member(RunId::for_path(&directory.0), 0).unwrap();
    let facts = Rc::new(RefCell::new(Vec::new()));
    let mut observer = Local(&facts);
    let mut file = FileProgress {
        observer: &mut observer,
        key,
    };
    let receipt = Transfer::from_body(
        stream::iter([Ok::<_, Infallible>(Bytes::from_static(b"abc"))]),
        Length::Known(ByteCount(3)),
    )
    .measurement(Measurement::None)
    .to_file(directory.0.join("output"))
    .unwrap()
    .run_registered_observed(std::sync::Arc::new(()), &mut file)
    .await
    .unwrap();
    let facts = facts.borrow();
    assert!(
        facts
            .iter()
            .all(|(actual, facts)| *actual == key && facts.windows().count() == 0)
    );
    let (_, terminal) = *facts.last().unwrap();
    assert_eq!(terminal.available(), receipt.partial().written);
    assert_eq!(terminal.available(), ByteCount(3));
    assert_eq!(terminal.hashed(), ByteCount(0));
    assert_eq!(terminal.reception(), Reception::CleanEof);
}

#[tokio::test]
async fn common_progress_preserves_actual_unequal_windows_with_a_borrowed_local_reader_and_observer()
 {
    struct Memory<'data>(&'data Rc<Vec<u8>>);
    impl<'read, 'data: 'read> RangeReader<'read> for Memory<'data> {
        type Error = Infallible;
        type Body = Iter<core::array::IntoIter<Result<Bytes, Self::Error>, 1>>;
        type Read = Ready<Result<ReadResponse<'read, Self::Body>, Self::Error>>;
        fn size(&self) -> ByteCount {
            ByteCount(self.0.len() as u64)
        }
        fn evidence(&'read self) -> Evidence<'read> {
            Evidence::Enforced {
                representation: "bytes",
                version: b"immutable borrow",
            }
        }
        fn read(&'read self, window: Window) -> Self::Read {
            ready(Ok(ReadResponse {
                window,
                evidence: self.evidence(),
                body: stream::iter([Ok(Bytes::copy_from_slice(
                    &self.0[window.start().0 as usize..window.end().0 as usize],
                ))]),
            }))
        }
    }
    let directory = Directory::create();
    let key = BindingKey::for_run_member(RunId::for_path(&directory.0), 0).unwrap();
    let bytes = Rc::new(vec![0; 19]);
    let reader = Memory(&bytes);
    let facts = Rc::new(RefCell::new(Vec::new()));
    let mut observer = Local(&facts);
    let mut file = FileProgress {
        observer: &mut observer,
        key,
    };
    Transfer::from_reader(&reader)
        .measurement(Measurement::None)
        .to_file(directory.0.join("output"))
        .unwrap()
        .run_registered_observed(None, &mut file)
        .await
        .unwrap();
    let facts = facts.borrow();
    assert!(
        facts
            .iter()
            .all(|(actual, facts)| *actual == key && facts.windows().count() <= 4)
    );
    let (_, terminal) = *facts.last().unwrap();
    assert_eq!(terminal.available(), ByteCount(19));
    assert_eq!(terminal.hashed(), ByteCount(0));
    assert_eq!(terminal.restored(), ByteCount(0));
    assert_eq!(terminal.attempt_received(), ByteCount(19));
    assert_eq!(
        terminal
            .windows()
            .map(|facts| (facts.window().start().0, facts.window().end().0))
            .collect::<Vec<_>>(),
        [(0, 5), (5, 10), (10, 15), (15, 19)]
    );
    assert_eq!(
        terminal
            .windows()
            .map(|facts| facts.received().0)
            .collect::<Vec<_>>(),
        [5, 5, 5, 4]
    );
    assert!(
        terminal
            .windows()
            .all(|facts| facts.reception() == Reception::CleanEof)
    );
    assert_eq!(terminal.reception(), Reception::CleanEof);
}
