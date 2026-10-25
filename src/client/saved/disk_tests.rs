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

use super::{Disk, admit, checkpoint};
use crate::checks::Requirements;
use crate::client::Reentry;
use crate::client::saved::{
    Attempt, AttemptStatus, Fingerprint, Member, Record, STORE, SavedError, codec,
};
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

pub(super) fn enter_work(path: &Path) {
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

fn pending(root: &Path, name: &str) -> Record {
    Disk::prepare(
        root.to_path_buf(),
        None,
        vec![Member {
            occurrence: crate::input::InputOccurrence(0),
            intent: None,
            remote: None,
            binding: Fingerprint::bytes(b"owned manifest fixture"),
            destination: root.join(name),
            length: Length::Unknown,
            etag: None,
            requirements: Requirements::default(),
            measurement: Measurement::default(),
            mode: crate::client::range::Mode::Whole,
            attempts: Vec::new(),
        }],
    )
    .unwrap()
}

async fn seed(root: &Path) -> (PathBuf, Vec<u8>, OperationOwner, oneshot::Receiver<()>) {
    let record = pending(root, "payload");
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

struct Pressure {
    record: Record,
    owner: OperationOwner,
    settled: oneshot::Receiver<()>,
    paths: Vec<PathBuf>,
}

async fn pressure(root: &Path) -> Pressure {
    let record = pending(root, "next");
    let lease = lease_directory(root.join(STORE)).await.unwrap();
    let mut paths = Vec::new();
    for index in 0..128 {
        let mut completed = pending(root, &format!("payload-{index}"));
        // These are metadata-boundary fixtures, not manufactured native reconciliation evidence.
        // Public integration tests exercise retirement after actual checked publications.
        completed.complete = true;
        completed.members[0].attempts.push(Attempt {
            state: root.join(format!(".xget-native-{index:x}")).into(),
            status: AttemptStatus::Complete,
        });
        paths.push(Disk::create(completed, Reentry::Fresh).unwrap().path);
    }
    let (released, settled) = oneshot::channel();
    Pressure {
        record,
        owner: Arc::new(ObservedOwner {
            lease: Some(lease),
            released: Some(released),
        }),
        settled,
        paths,
    }
}

#[test]
fn queued_retirement_keeps_the_store_lease_after_its_waiter_is_dropped() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let fixture = Fixture::new();
        let Pressure {
            record,
            owner,
            settled,
            paths,
        } = pressure(&fixture.0).await;
        let store = fixture.0.join(STORE);
        let (entered, occupied) = oneshot::channel();
        let (release, blocked) = mpsc::channel::<()>();
        let blocker = tokio::task::spawn_blocking(move || {
            let _ = entered.send(());
            let _ = blocked.recv();
        });
        occupied.await.unwrap();
        let mut waiter = Box::pin(admit(record, Reentry::Fresh, owner));
        assert!(futures::poll!(waiter.as_mut()).is_pending());
        drop(waiter);
        probe(&store, Expected::Busy);
        assert!(paths.iter().all(|path| path.join("state").is_file()));
        release.send(()).unwrap();
        blocker.await.unwrap();
        settled.await.unwrap();
        probe(&store, Expected::Free);
        assert_eq!(paths.iter().filter(|path| path.exists()).count(), 127);
        assert_eq!(fs::read_dir(&store).unwrap().count(), 129);
        assert!(!store.join("retired").exists());
    });
}

#[test]
fn running_retirement_keeps_the_store_lease_after_its_waiter_is_dropped() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let fixture = Fixture::new();
        let Pressure {
            record,
            owner,
            settled,
            paths,
        } = pressure(&fixture.0).await;
        let store = fixture.0.join(STORE);
        let (entered, running) = oneshot::channel();
        let (release, blocked) = mpsc::channel::<()>();
        GATES.lock().unwrap().insert(
            PathBuf::clone(&store),
            Gate {
                entered,
                release: blocked,
            },
        );
        let mut waiter = Box::pin(admit(record, Reentry::Fresh, owner));
        tokio::select! {
            result = &mut waiter => panic!("retirement must be held: {result:?}"),
            result = running => result.unwrap(),
        }
        drop(waiter);
        probe(&store, Expected::Busy);
        assert!(paths.iter().all(|path| path.join("state").is_file()));
        release.send(()).unwrap();
        settled.await.unwrap();
        probe(&store, Expected::Free);
        assert_eq!(paths.iter().filter(|path| path.exists()).count(), 127);
        assert!(!store.join("retired").exists());
    });
}

#[tokio::test]
async fn retirement_refuses_a_replaced_store_without_deleting_either_directorys_records() {
    let fixture = Fixture::new();
    let Pressure {
        record,
        owner,
        settled,
        paths,
    } = pressure(&fixture.0).await;
    let store = fixture.0.join(STORE);
    let detached = fixture.0.join("detached-store");
    let (entered, running) = oneshot::channel();
    let (release, blocked) = mpsc::channel::<()>();
    GATES.lock().unwrap().insert(
        PathBuf::clone(&store),
        Gate {
            entered,
            release: blocked,
        },
    );
    let mut waiter = Box::pin(admit(record, Reentry::Fresh, owner));
    tokio::select! {
        result = &mut waiter => panic!("retirement must be held: {result:?}"),
        result = running => result.unwrap(),
    }
    fs::rename(&store, &detached).unwrap();
    fs::DirBuilder::new().mode(0o700).create(&store).unwrap();
    fs::copy(detached.join("lease"), store.join("lease")).unwrap();
    fs::write(store.join("sentinel"), b"foreign replacement").unwrap();
    probe(&detached, Expected::Busy);
    release.send(()).unwrap();
    assert!(matches!(waiter.await, Err(SavedError::Corrupt)));
    settled.await.unwrap();
    probe(&detached, Expected::Free);
    for path in paths {
        assert!(
            detached
                .join(path.file_name().unwrap())
                .join("state")
                .is_file()
        );
    }
    assert_eq!(
        fs::read(store.join("sentinel")).unwrap(),
        b"foreign replacement"
    );
    assert_eq!(fs::read_dir(&store).unwrap().count(), 2);
}

#[tokio::test]
async fn retirement_refuses_a_replaced_completed_workspace() {
    let fixture = Fixture::new();
    let Pressure {
        record,
        owner,
        settled,
        mut paths,
    } = pressure(&fixture.0).await;
    paths.sort();
    let selected = &paths[0];
    let bytes = fs::read(selected.join("state")).unwrap();
    let store = fixture.0.join(STORE);
    let (entered, running) = oneshot::channel();
    let (release, blocked) = mpsc::channel::<()>();
    GATES.lock().unwrap().insert(
        PathBuf::clone(&store),
        Gate {
            entered,
            release: blocked,
        },
    );
    let mut waiter = Box::pin(admit(record, Reentry::Fresh, owner));
    tokio::select! {
        result = &mut waiter => panic!("retirement must be held: {result:?}"),
        result = running => result.unwrap(),
    }
    let detached = fixture.0.join("detached-run");
    fs::rename(selected, &detached).unwrap();
    fs::DirBuilder::new().mode(0o700).create(selected).unwrap();
    fs::copy(detached.join("state"), selected.join("state")).unwrap();
    release.send(()).unwrap();
    assert!(matches!(waiter.await, Err(SavedError::Corrupt)));
    settled.await.unwrap();
    assert_eq!(fs::read(selected.join("state")).unwrap(), bytes);
    assert_eq!(fs::read(detached.join("state")).unwrap(), bytes);
    assert!(!store.join("retired").exists());
}

#[tokio::test]
async fn interrupted_retirement_resumes_only_known_record_or_empty_directory_removal() {
    for recorded in [false, true] {
        let fixture = Fixture::new();
        let Pressure {
            record,
            owner,
            settled,
            paths,
        } = pressure(&fixture.0).await;
        let store = fixture.0.join(STORE);
        let retired = store.join("retired");
        fs::rename(&paths[0], &retired).unwrap();
        if !recorded {
            fs::remove_file(retired.join("state")).unwrap();
        }
        // A quarantine is outside active matching/capacity, but remains bounded to one fixed slot.
        let _ = Disk::create(record, Reentry::Fresh).unwrap();
        assert!(retired.is_dir());
        let record = pending(&fixture.0, "after-interruption");
        let _ = admit(record, Reentry::Fresh, owner).await.unwrap();
        settled.await.unwrap();
        assert!(!retired.exists());
        assert_eq!(fs::read_dir(store).unwrap().count(), 129);
    }
}

#[tokio::test]
async fn corrupt_or_nonempty_unknown_quarantine_is_never_ignored_or_recursively_removed() {
    for unknown in [false, true] {
        let fixture = Fixture::new();
        let Pressure {
            record,
            owner,
            settled,
            paths,
        } = pressure(&fixture.0).await;
        let retired = fixture.0.join(STORE).join("retired");
        fs::rename(&paths[0], &retired).unwrap();
        if unknown {
            fs::write(retired.join("unexpected"), b"owner evidence").unwrap();
        } else {
            fs::write(retired.join("state"), b"corrupt").unwrap();
        }
        assert!(matches!(
            admit(record, Reentry::Fresh, owner).await,
            Err(SavedError::Corrupt)
        ));
        settled.await.unwrap();
        assert!(retired.join("state").exists());
        if unknown {
            assert_eq!(
                fs::read(retired.join("unexpected")).unwrap(),
                b"owner evidence"
            );
        }
    }
}

#[tokio::test]
async fn a_store_full_of_unfinished_records_refuses_without_retiring_evidence() {
    let fixture = Fixture::new();
    let Pressure {
        record,
        owner,
        settled,
        paths,
    } = pressure(&fixture.0).await;
    for path in &paths {
        let mut record = codec::decode(&fs::read(path.join("state")).unwrap()).unwrap();
        record.complete = false;
        record.members[0].attempts.clear();
        Disk::checkpoint(path, &codec::encode(&record).unwrap()).unwrap();
    }
    let error = admit(record, Reentry::Fresh, owner).await.unwrap_err();
    assert!(matches!(error, SavedError::StoreFull));
    assert!(
        error
            .to_string()
            .contains("unfinished or uncertain evidence")
    );
    settled.await.unwrap();
    assert!(paths.iter().all(|path| path.join("state").is_file()));
    assert_eq!(fs::read_dir(fixture.0.join(STORE)).unwrap().count(), 129);
}

#[tokio::test]
async fn capacity_scan_finishes_integrity_validation_before_retiring_a_complete_candidate() {
    let fixture = Fixture::new();
    let Pressure {
        record,
        owner,
        settled,
        mut paths,
    } = pressure(&fixture.0).await;
    paths.sort();
    fs::write(paths.last().unwrap().join("state"), b"corrupt").unwrap();
    assert!(matches!(
        admit(record, Reentry::Fresh, owner).await,
        Err(SavedError::Corrupt)
    ));
    settled.await.unwrap();
    assert!(paths.iter().all(|path| path.exists()));
    assert!(!fixture.0.join(STORE).join("retired").exists());
}

#[tokio::test]
async fn fully_complete_records_with_historical_attempt_ambiguity_are_not_retired() {
    let fixture = Fixture::new();
    let Pressure {
        record,
        owner,
        settled,
        paths,
    } = pressure(&fixture.0).await;
    for path in &paths {
        let mut record = codec::decode(&fs::read(path.join("state")).unwrap()).unwrap();
        record.members[0].attempts.insert(
            0,
            Attempt {
                state: fixture.0.join(".xget-native-ffff").into(),
                status: AttemptStatus::Retained,
            },
        );
        Disk::checkpoint(path, &codec::encode(&record).unwrap()).unwrap();
    }
    assert!(matches!(
        admit(record, Reentry::Fresh, owner).await,
        Err(SavedError::StoreFull)
    ));
    settled.await.unwrap();
    assert!(paths.iter().all(|path| path.join("state").is_file()));
    assert!(!fixture.0.join(STORE).join("retired").exists());
}
