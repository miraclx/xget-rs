//! Physical manifest registration evidence. These tests are source-authored, not author-run evidence.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use core::sync::atomic::{AtomicU64, Ordering};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io;
use std::os::fd::AsRawFd as _;
use std::os::unix::fs::{DirBuilderExt as _, symlink};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex, mpsc};

use tokio::sync::oneshot;

use super::{Disk, checkpoint};
use crate::checks::Requirements;
use crate::client::Reentry;
use crate::client::saved::{Fingerprint, Member, STORE, codec};
use crate::transfer::{Length, Measurement, OperationOwner, lease_directory};

// The mutex protects per-path test configuration only. A worker removes its entry and releases the
// mutex before waiting, so parallel tests never hold this lock across a physical operation.
static GATES: LazyLock<Mutex<BTreeMap<PathBuf, Gate>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Gate {
    entered: oneshot::Sender<()>,
    release: mpsc::Receiver<()>,
}

pub(super) fn enter_checkpoint(path: &Path) {
    let gate = GATES.lock().unwrap().remove(path);
    if let Some(gate) = gate {
        let _ = gate.entered.send(());
        // The fixture's sender also releases this worker on panic/drop.
        let _ = gate.release.recv();
    }
}

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let sequence = NEXT.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "xget-manifest-owner-{}-{sequence}",
            std::process::id()
        ));
        fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
        Self(fs::canonicalize(path).unwrap())
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct ObservedOwner {
    lease: Option<OperationOwner>,
    released: Option<oneshot::Sender<()>>,
}
impl Drop for ObservedOwner {
    fn drop(&mut self) {
        drop(self.lease.take());
        if let Some(released) = self.released.take() {
            let _ = released.send(());
        }
    }
}

async fn seed(root: &Path) -> (PathBuf, Vec<u8>, OperationOwner, oneshot::Receiver<()>) {
    let record = Disk::prepare(
        root.to_path_buf(),
        None,
        vec![Member {
            occurrence: crate::input::InputOccurrence(0),
            remote: None,
            binding: Fingerprint::bytes(b"owned manifest fixture"),
            destination: root.join("payload"),
            length: Length::Unknown,
            etag: None,
            requirements: Requirements::default(),
            measurement: Measurement::default(),
            attempts: Vec::new(),
        }],
    )
    .unwrap();
    let lease = lease_directory(root.join(STORE)).await.unwrap();
    let saved = Disk::create(record, Reentry::Fresh).unwrap();
    let bytes = codec::encode(&saved.record).unwrap();
    let (released, settled) = oneshot::channel();
    let owner: OperationOwner = Arc::new(ObservedOwner {
        lease: Some(lease),
        released: Some(released),
    });
    (saved.path, bytes, owner, settled)
}

enum Expected {
    Busy,
    Free,
}

fn probe(alias: &Path, expected: Expected) {
    let file = File::open(alias.join("lease")).unwrap();
    // SAFETY: the independent probe owns this descriptor. It never unlocks the operation's lease.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if matches!(expected, Expected::Busy) {
        assert_eq!(result, -1);
        assert_eq!(
            std::io::Error::last_os_error().kind(),
            io::ErrorKind::WouldBlock
        );
    } else {
        assert_eq!(result, 0);
    }
    // A successful probe releases only its own independent flock by closing its descriptor.
}

#[test]
fn queued_manifest_work_keeps_the_real_lease_after_its_waiter_is_dropped() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let fixture = Fixture::new();
        let (path, bytes, owner, settled) = seed(&fixture.0).await;
        let alias = fixture.0.join("store-alias");
        symlink(fixture.0.join(STORE), &alias).unwrap();
        let (entered, occupied) = oneshot::channel();
        let (release, blocked) = mpsc::channel::<()>();
        let blocker = tokio::task::spawn_blocking(move || {
            let _ = entered.send(());
            let _ = blocked.recv();
        });
        occupied.await.unwrap();
        let mut waiter = Box::pin(checkpoint(PathBuf::clone(&path), Vec::clone(&bytes), owner));
        assert!(futures::poll!(waiter.as_mut()).is_pending());
        drop(waiter);
        probe(&alias, Expected::Busy);
        release.send(()).unwrap();
        blocker.await.unwrap();
        settled.await.unwrap();
        probe(&alias, Expected::Free);
        assert_eq!(fs::read(path.join("state")).unwrap(), bytes);
    });
}

#[test]
fn running_manifest_work_keeps_the_real_lease_after_its_waiter_is_dropped() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let fixture = Fixture::new();
        let (path, bytes, owner, settled) = seed(&fixture.0).await;
        let alias = fixture.0.join("store-alias");
        symlink(fixture.0.join(STORE), &alias).unwrap();
        let (entered, running) = oneshot::channel();
        let (release, blocked) = mpsc::channel::<()>();
        GATES.lock().unwrap().insert(
            PathBuf::clone(&path),
            Gate {
                entered,
                release: blocked,
            },
        );
        let mut waiter = Box::pin(checkpoint(PathBuf::clone(&path), Vec::clone(&bytes), owner));
        tokio::select! {
            result = &mut waiter => panic!("physical checkpoint must be held: {result:?}"),
            result = running => result.unwrap(),
        }
        drop(waiter);
        probe(&alias, Expected::Busy);
        release.send(()).unwrap();
        settled.await.unwrap();
        probe(&alias, Expected::Free);
        assert_eq!(fs::read(path.join("state")).unwrap(), bytes);
    });
}
