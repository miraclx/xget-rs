#![allow(clippy::expect_used, clippy::unwrap_used)]

use core::cell::Cell;
use core::convert::Infallible;
use core::future::pending;
use core::pin::Pin;
use core::sync::atomic::Ordering;
use core::task::{Context, Poll};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Read as _;
use std::os::fd::{AsRawFd as _, FromRawFd as _, RawFd};
use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _, symlink};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::rc::Rc;
use std::sync::{Arc, Barrier, LazyLock, Mutex, mpsc};
use std::{io, thread};

use bytes::Bytes;
use futures::{Stream, stream};

use super::{
    DATA, Directory, DirectoryLease, FlockLease, LockAccess, Native, NativeError, ProcessId, STATE,
    WORKSPACE_MARKER,
};
use crate::transfer::pipeline::Run;
use crate::transfer::plan::Plan;
use crate::transfer::tests::{Fixture, complete, requirement};
use crate::transfer::{
    ByteCount, FailureKind, Length, Partial, Phase, Reconciliation, StateRef, Transfer, WriteExtent,
};

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum PhysicalOperation {
    Acquire,
    ReaderAcquire,
    Reconcile,
}

// Only per-state fixture configuration is guarded. Remove the entry before waiting on the existing
// physical barrier, so unrelated native operations and other tests cannot be held by this mutex.
type RegisteredGates = BTreeMap<(PathBuf, PhysicalOperation), Arc<super::Gate>>;
static REGISTERED_WORK: LazyLock<Mutex<RegisteredGates>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));

pub(super) fn enter_registered_work(state: &Path, operation: PhysicalOperation) {
    let gate = REGISTERED_WORK
        .lock()
        .unwrap()
        .remove(&(state.to_path_buf(), operation));
    if let Some(gate) = gate {
        gate.enter();
    }
}

struct ObservedRegistration {
    lease: Option<crate::transfer::OperationOwner>,
    released: Option<tokio::sync::oneshot::Sender<()>>,
}
impl Drop for ObservedRegistration {
    fn drop(&mut self) {
        drop(self.lease.take());
        if let Some(released) = self.released.take() {
            let _ = released.send(());
        }
    }
}

enum SubmissionStage {
    Queued,
    Running,
}
enum HeldSubmission {
    Queued {
        release: mpsc::Sender<()>,
        blocker: tokio::task::JoinHandle<()>,
    },
    Running(super::Held),
}

fn dropped_registered_work(operation: PhysicalOperation, stage: SubmissionStage) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let fixture = Fixture::new();
        let store = fixture.path().join("owned-store");
        fs::create_dir(&store).unwrap();
        fs::set_permissions(&store, fs::Permissions::from_mode(0o700)).unwrap();
        let alias = fixture.path().join("store-alias");
        symlink(&store, &alias).unwrap();
        let plan = Plan::new(&fixture.output(), requirement(b"abc")).unwrap();
        let state = match operation {
            PhysicalOperation::Acquire | PhysicalOperation::ReaderAcquire => {
                StateRef::clone(plan.state())
            }
            PhysicalOperation::Reconcile => StateRef::clone(
                Transfer::from_body(body(), Length::Unknown)
                    .to_file(fixture.output())
                    .unwrap()
                    .run()
                    .await
                    .unwrap()
                    .state(),
            ),
        };
        let lease = Native::lease_directory(store).await.unwrap();
        let (released, settled) = tokio::sync::oneshot::channel();
        let owner: crate::transfer::OperationOwner = Arc::new(ObservedRegistration {
            lease: Some(lease),
            released: Some(released),
        });
        let held = match stage {
            SubmissionStage::Queued => {
                let (entered, occupied) = tokio::sync::oneshot::channel();
                let (release, blocked) = mpsc::channel::<()>();
                let blocker = tokio::task::spawn_blocking(move || {
                    let _ = entered.send(());
                    let _ = blocked.recv();
                });
                occupied.await.unwrap();
                HeldSubmission::Queued { release, blocker }
            }
            SubmissionStage::Running => {
                let gate = Arc::new(super::Gate::default());
                let held = gate.install();
                REGISTERED_WORK.lock().unwrap().insert(
                    (
                        state.path().to_path_buf(),
                        match operation {
                            PhysicalOperation::ReaderAcquire => PhysicalOperation::Acquire,
                            operation => operation,
                        },
                    ),
                    gate,
                );
                HeldSubmission::Running(held)
            }
        };
        let operation_state = StateRef::clone(&state);
        let mut waiter = Box::pin(async move {
            match operation {
                PhysicalOperation::Acquire => {
                    let _native = Native::acquire_registered(plan, Some(owner)).await.unwrap();
                }
                PhysicalOperation::ReaderAcquire => {
                    let _native = Native::acquire_reader(
                        plan,
                        super::Session {
                            size: ByteCount(3),
                            representation: "bytes".to_owned(),
                            version: Some(b"one".to_vec()),
                        },
                        None,
                        Some(owner),
                    )
                    .await
                    .unwrap();
                }
                PhysicalOperation::Reconcile => {
                    let _result = Native::reconcile_registered(operation_state, Some(owner), None)
                        .await
                        .unwrap();
                }
            }
        });
        match &held {
            HeldSubmission::Queued { .. } => assert!(futures::poll!(waiter.as_mut()).is_pending()),
            HeldSubmission::Running(barrier) => tokio::select! {
                () = &mut waiter => panic!("physical registered operation must be held"),
                () = barrier.entered.notified() => {}
            },
        }
        drop(waiter);
        // A separately executed child opens the stable lock through an alias. This is an actual
        // primitive proof, not a second high-level Busy result or an inspection of Arc counts.
        probe(&alias, "owned-busy");
        if matches!(
            operation,
            PhysicalOperation::Acquire | PhysicalOperation::ReaderAcquire
        ) {
            assert!(!state.path().exists());
        }
        match held {
            HeldSubmission::Queued { release, blocker } => {
                release.send(()).unwrap();
                blocker.await.unwrap();
            }
            HeldSubmission::Running(barrier) => barrier.release(),
        }
        settled.await.unwrap();
        probe(&alias, "owned-free");
        probe(fixture.path(), "free");
        assert!(state.path().join("state").is_file());
        match operation {
            PhysicalOperation::Acquire | PhysicalOperation::ReaderAcquire => {
                assert!(!fixture.output().exists())
            }
            PhysicalOperation::Reconcile => assert_eq!(fs::read(fixture.output()).unwrap(), b"abc"),
        }
    });
}

#[test]
fn queued_acquisition_retains_the_outer_lease_before_a_native_owner_is_returned() {
    dropped_registered_work(PhysicalOperation::Acquire, SubmissionStage::Queued);
}
#[test]
fn running_acquisition_retains_the_outer_lease_before_a_native_owner_is_returned() {
    dropped_registered_work(PhysicalOperation::Acquire, SubmissionStage::Running);
}
#[test]
fn queued_reader_acquisition_retains_the_outer_lease_after_waiter_drop() {
    dropped_registered_work(PhysicalOperation::ReaderAcquire, SubmissionStage::Queued);
}
#[test]
fn running_reader_acquisition_retains_the_outer_lease_after_waiter_drop() {
    dropped_registered_work(PhysicalOperation::ReaderAcquire, SubmissionStage::Running);
}
#[test]
fn queued_reconciliation_retains_the_outer_lease_after_waiter_drop() {
    dropped_registered_work(PhysicalOperation::Reconcile, SubmissionStage::Queued);
}
#[test]
fn running_reconciliation_retains_the_outer_lease_after_waiter_drop() {
    dropped_registered_work(PhysicalOperation::Reconcile, SubmissionStage::Running);
}

#[tokio::test]
async fn old_one_shot_receiving_records_never_acquire_reader_write_authority() {
    let fixture = Fixture::new();
    let plan = Plan::new(&fixture.output(), requirement(b"abc")).unwrap();
    let state = StateRef::clone(plan.state());
    let native = Native::acquire(plan).await.unwrap();
    native
        .write(ByteCount(0), Bytes::from_static(b"a"))
        .await
        .unwrap();
    drop(native);
    let before = fs::read(state.path().join("state")).unwrap();
    assert_eq!(&before[..8], b"XGETNF02");
    let session = super::Session {
        size: ByteCount(3),
        representation: "bytes".to_owned(),
        version: Some(b"one".to_vec()),
    };
    let result = Native::acquire_reader(
        Plan::new(&fixture.output(), requirement(b"abc")).unwrap(),
        session,
        Some(StateRef::clone(&state)),
        None,
    )
    .await;
    assert!(matches!(
        result,
        Err(crate::transfer::AcquisitionFailure {
            error: NativeError::InvalidState,
            ..
        })
    ));
    assert_eq!(fs::read(state.path().join("state")).unwrap(), before);
    assert_eq!(fs::read(state.path().join("data")).unwrap(), b"a");
}

#[tokio::test]
async fn reader_sealing_is_persisted_before_checks_and_cannot_be_reset_by_failed_checkpoint() {
    let fixture = Fixture::new();
    let session = super::Session {
        size: ByteCount(3),
        representation: "bytes".to_owned(),
        version: Some(b"one".to_vec()),
    };
    let plan = Plan::new(&fixture.output(), requirement(b"abc")).unwrap();
    let state = StateRef::clone(plan.state());
    let native = Native::acquire_reader(plan, super::Session::clone(&session), None, None)
        .await
        .unwrap();
    native
        .write(ByteCount(0), Bytes::from_static(b"abc"))
        .await
        .unwrap();
    let mut checks = native.policy().start();
    checks.update(b"ab");
    let partial = Partial {
        received: ByteCount(3),
        written: ByteCount(3),
        hashed: ByteCount(2),
        ..Partial::default()
    };
    native
        .checkpoint_prefix(partial, ByteCount(2), checks.prefix_anchor().unwrap())
        .await
        .unwrap();
    native.seal(ByteCount(3)).await.unwrap();
    native
        .hooks()
        .fail_checkpoint
        .store(true, Ordering::Release);
    assert!(native.checkpoint(partial).await.is_err());
    drop(native);
    let record = super::Record::decode(&fs::read(state.path().join("state")).unwrap()).unwrap();
    assert!(record.reader.unwrap().closed);
    let result = Native::acquire_reader(
        Plan::new(&fixture.output(), requirement(b"abc")).unwrap(),
        session,
        Some(state),
        None,
    )
    .await;
    assert!(matches!(
        result,
        Err(crate::transfer::AcquisitionFailure {
            error: NativeError::InvalidState,
            ..
        })
    ));
    assert!(!fixture.output().exists());
}

#[tokio::test]
async fn dropped_prefix_revalidation_retains_both_leases_and_the_aggregate_owner() {
    let fixture = Fixture::new();
    let session = super::Session {
        size: ByteCount(3),
        representation: "bytes".to_owned(),
        version: Some(b"one".to_vec()),
    };
    let plan = Plan::new(&fixture.output(), requirement(b"abc")).unwrap();
    let state = StateRef::clone(plan.state());
    let native = Native::acquire_reader(plan, super::Session::clone(&session), None, None)
        .await
        .unwrap();
    native
        .write(ByteCount(0), Bytes::from_static(b"a"))
        .await
        .unwrap();
    let mut checks = native.policy().start();
    checks.update(b"a");
    native
        .checkpoint_prefix(
            Partial {
                received: ByteCount(1),
                written: ByteCount(1),
                hashed: ByteCount(1),
                ..Partial::default()
            },
            ByteCount(1),
            checks.prefix_anchor().unwrap(),
        )
        .await
        .unwrap();
    drop(native);
    let store = fixture.path().join("owned-store");
    fs::create_dir(&store).unwrap();
    fs::set_permissions(&store, fs::Permissions::from_mode(0o700)).unwrap();
    let owner = Native::lease_directory(PathBuf::clone(&store))
        .await
        .unwrap();
    let (released, settled) = tokio::sync::oneshot::channel();
    let owner: crate::transfer::OperationOwner = Arc::new(ObservedRegistration {
        lease: Some(owner),
        released: Some(released),
    });
    let native = Native::acquire_reader(
        Plan::new(&fixture.output(), requirement(b"abc")).unwrap(),
        session,
        Some(StateRef::clone(&state)),
        Some(owner),
    )
    .await
    .unwrap();
    let barrier = native.hooks().read.install();
    let mut restore = Box::pin(native.restore_reader());
    tokio::select! {
        result = &mut restore => panic!("revalidation escaped its barrier: {}", result.is_ok()),
        () = barrier.entered.notified() => {}
    }
    drop(restore);
    drop(native);
    probe(&store, "owned-busy");
    probe(fixture.path(), "busy");
    barrier.release();
    settled.await.unwrap();
    probe(&store, "owned-free");
    probe(fixture.path(), "free");
    assert!(state.path().join("state").is_file());
}

#[tokio::test]
async fn every_native_entrance_reserves_the_aggregate_store_before_acquisition_effects() {
    let fixture = Fixture::new();
    for destination in [
        fixture.path().join(".xget-runs"),
        fixture.path().join(".xget-runs/payload"),
    ] {
        let failure =
            match Native::acquire(Plan::new(&destination, requirement(b"abc")).unwrap()).await {
                Ok(_) => panic!("managed store role must fail before acquisition"),
                Err(failure) => failure,
            };
        assert!(matches!(failure.error, NativeError::InvalidState));
        assert!(failure.state.is_none());
    }
    assert!(!fixture.path().join(".xget-runs").exists());
    assert!(!fixture.path().join(".xget-native.lock").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_outer_workspace_lease_survives_waiter_drop_until_native_physical_work_settles() {
    let fixture = Fixture::new();
    let store = fixture.path().join("owned-store");
    fs::create_dir(&store).unwrap();
    fs::set_permissions(&store, fs::Permissions::from_mode(0o700)).unwrap();
    let registration = Native::lease_directory(store.to_path_buf()).await.unwrap();
    let native = Native::acquire_registered(
        Plan::new(&fixture.output(), requirement(b"abc")).unwrap(),
        Some(registration),
    )
    .await
    .unwrap();
    let barrier = native.hooks().write.install();
    let released = Arc::clone(&native.hooks().released);
    let mut waiter =
        Box::pin(async move { native.write(ByteCount(0), Bytes::from_static(b"abc")).await });
    tokio::select! {
        result = &mut waiter => panic!("physical write should be held, got {result:?}"),
        () = barrier.entered.notified() => {}
    }
    drop(waiter);
    assert!(matches!(
        Native::lease_directory(store.to_path_buf()).await,
        Err(NativeError::Busy)
    ));
    barrier.release();
    released.notified().await;
    let registration = Native::lease_directory(store).await.unwrap();
    drop(registration);
}

#[tokio::test]
async fn a_real_stage_read_failure_keeps_its_phase_and_io_cause() {
    let fixture = Fixture::new();
    let (run, state) = owned_run(&fixture).await;
    run.native
        .write(ByteCount(0), Bytes::from_static(b"abc"))
        .await
        .unwrap();
    fs::OpenOptions::new()
        .write(true)
        .open(state.path().join("data"))
        .unwrap()
        .set_len(0)
        .unwrap();
    let error = run
        .native
        .read_prefix(ByteCount(0), ByteCount(3))
        .await
        .unwrap_err();
    match error {
        NativeError::Io { phase, cause } => {
            assert_eq!(phase, Phase::ReadStage);
            assert_eq!(cause.kind(), io::ErrorKind::UnexpectedEof);
        }
        other => panic!("expected a real stage read error, got {other:?}"),
    }
    assert!(!fixture.output().exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_stage_read_future_keeps_both_leases_until_physical_settlement() {
    let fixture = Fixture::new();
    let (run, state) = owned_run(&fixture).await;
    let native = run.native;
    native
        .write(ByteCount(0), Bytes::from_static(b"abc"))
        .await
        .unwrap();
    let barrier = native.hooks().read.install();
    let released = Arc::clone(&native.hooks().released);
    let mut future = Box::pin(async move { native.read_prefix(ByteCount(0), ByteCount(3)).await });
    tokio::select! {
        result = &mut future => panic!("physical stage read should be held, got {result:?}"),
        () = barrier.entered.notified() => {}
    }
    drop(future);
    probe(fixture.path(), "busy");
    probe(state.path(), "workspace-busy");
    assert!(matches!(
        state.reconcile().await.unwrap_err().kind,
        FailureKind::Busy
    ));
    barrier.release();
    released.notified().await;
    probe(fixture.path(), "free");
    probe(state.path(), "workspace-free");
    assert_eq!(fs::read(state.path().join("data")).unwrap(), b"abc");
    assert!(!fixture.output().exists());
}

#[tokio::test]
async fn a_partial_physical_write_error_reports_its_actual_accepted_prefix() {
    let fixture = Fixture::new();
    let (run, state) = owned_run(&fixture).await;
    run.native
        .hooks()
        .fail_write_prefix
        .store(true, Ordering::Release);
    let failure = run.execute(body(), pending()).await.unwrap_err();
    assert!(matches!(
        failure.kind,
        FailureKind::Io {
            phase: Phase::Write,
            ..
        }
    ));
    assert_eq!(failure.partial.received, ByteCount(3));
    assert_eq!(failure.partial.written, ByteCount(1));
    assert_eq!(failure.partial.hashed, ByteCount(1));
    assert_eq!(failure.partial.write_extent, WriteExtent::Confirmed);
    assert_eq!(failure.partial.actual, None);
    assert_eq!(fs::read(state.path().join("data")).unwrap(), b"a");
    assert!(!fixture.output().exists());
}

#[tokio::test]
async fn a_lost_worker_result_after_a_real_prefix_retains_an_unconfirmed_extent() {
    let fixture = Fixture::new();
    let (run, state) = owned_run(&fixture).await;
    run.native
        .hooks()
        .lose_write_result
        .store(true, Ordering::Release);
    let failure = run.execute(body(), pending()).await.unwrap_err();
    match &failure.kind {
        FailureKind::Io {
            phase: Phase::Write,
            cause,
        } => {
            let worker = cause
                .get_ref()
                .and_then(|cause| cause.downcast_ref::<tokio::task::JoinError>())
                .expect("the original worker failure remains typed");
            assert!(worker.is_panic());
        }
        other => panic!("expected lost physical write result, got {other:?}"),
    }
    assert_eq!(failure.partial.received, ByteCount(3));
    assert_eq!(failure.partial.written, ByteCount(0));
    assert_eq!(failure.partial.hashed, ByteCount(0));
    assert_eq!(failure.partial.write_extent, WriteExtent::Unconfirmed);
    assert_eq!(failure.partial.actual, None);
    // Zero is only the confirmed lower bound: a real byte reached the retained stage.
    assert_eq!(fs::read(state.path().join("data")).unwrap(), b"a");
    assert!(!fixture.output().exists());
    match state.reconcile().await.unwrap() {
        Reconciliation::NeedsBody { partial, .. } => assert_eq!(partial, failure.partial),
        other => panic!("expected restart-required unknown extent, got {other:?}"),
    }
}

#[tokio::test]
async fn a_real_create_new_collision_keeps_its_checkpoint_phase_and_os_cause() {
    let fixture = Fixture::new();
    let (run, state) = owned_run(&fixture).await;
    let record = state.path().join("state");
    let before = fs::read(&record).unwrap();
    let inode = fs::metadata(&record).unwrap().ino();
    // The existing leaf is private to this fixture. No global sequence observation or race is needed.
    let error = run
        .native
        .submit(Phase::Checkpoint, |lease| {
            lease
                .workspace
                .open(
                    STATE,
                    libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
                    0o600,
                    Phase::Checkpoint,
                )
                .map(drop)
        })
        .await
        .unwrap_err();
    match error {
        NativeError::Io {
            phase: Phase::Checkpoint,
            cause,
        } => {
            assert_eq!(cause.kind(), io::ErrorKind::AlreadyExists);
            assert_eq!(cause.raw_os_error(), Some(libc::EEXIST));
        }
        other => panic!("expected a real checkpoint create-new collision, got {other:?}"),
    }
    assert_eq!(fs::read(&record).unwrap(), before);
    assert_eq!(fs::metadata(&record).unwrap().ino(), inode);
}

#[tokio::test]
async fn a_real_checkpoint_record_read_failure_is_not_mislabeled_reconcile_or_acquire() {
    let fixture = Fixture::new();
    let (run, state) = owned_run(&fixture).await;
    fs::remove_file(state.path().join("state")).unwrap();
    let error = run.native.checkpoint(Partial::default()).await.unwrap_err();
    match error {
        NativeError::Io {
            phase: Phase::Checkpoint,
            cause,
        } => {
            assert_eq!(cause.kind(), io::ErrorKind::NotFound);
            assert_eq!(cause.raw_os_error(), Some(libc::ENOENT));
        }
        other => panic!("expected a real checkpoint read failure, got {other:?}"),
    }
    assert!(!state.path().join("state").exists());
}

#[tokio::test]
async fn a_real_reconciliation_record_open_failure_keeps_its_reconcile_phase() {
    let fixture = Fixture::new();
    let (run, state) = owned_run(&fixture).await;
    drop(run);
    fs::remove_file(state.path().join("state")).unwrap();
    let failure = state.reconcile().await.unwrap_err();
    match failure.kind {
        FailureKind::Io {
            phase: Phase::Reconcile,
            cause,
        } => {
            assert_eq!(cause.kind(), io::ErrorKind::NotFound);
            assert_eq!(cause.raw_os_error(), Some(libc::ENOENT));
        }
        other => panic!("expected a real reconciliation read failure, got {other:?}"),
    }
    assert!(!state.path().join("state").exists());
}

#[tokio::test]
async fn settled_native_registration_releases_both_locks_despite_inherited_descriptors() {
    let fixture = Fixture::new();
    let (run, state) = owned_run(&fixture).await;
    let released = Arc::clone(&run.native.hooks().released);
    let directory_lock = run.native.0.directory._lock.as_raw_fd();
    let workspace_lock = run.native.0._workspace_lock.as_raw_fd();
    let mut child = InheritedLockChild::hold(directory_lock, workspace_lock)
        .expect("native lock descriptors must be close-on-exec before the controlled fork");
    let ready = child.acknowledgement(b'R');
    drop(run);
    released.notified().await;

    // One attempt while the child deliberately retains both incidental copies. No retry may hide
    // whether the settled registration actually relinquished its cooperating-writer authority.
    let while_inherited = state.reconcile().await;
    let child_result = child.release_and_wait();
    let after_close = state.reconcile().await;

    // Child release and waitpid happen before any post-fork assertion, including protocol failures.
    assert!(
        ready.is_ok() && matches!(&child_result, Ok(status) if status.successful()),
        "inherited-descriptor protocol failed: ready={ready:?}, child={child_result:?}, \
         while inherited={while_inherited:?}, after close={after_close:?}"
    );
    assert!(
        matches!(&while_inherited, Ok(Reconciliation::NeedsBody { .. }))
            && matches!(&after_close, Ok(Reconciliation::NeedsBody { .. })),
        "settled native registration must release both locks independently of incidental fd copies: \
         while inherited={while_inherited:?}, after close={after_close:?}, child={child_result:?}"
    );
}

#[tokio::test]
async fn an_owner_pid_mismatch_cannot_unlock_the_live_directory_or_workspace_lease() {
    let fixture = Fixture::new();
    let (run, state) = owned_run(&fixture).await;
    // Exercise the inherited-child owner mismatch without executing Rust destructors after fork.
    // SAFETY: getppid has no arguments or memory-safety preconditions.
    let foreign_owner = ProcessId(unsafe { libc::getppid() });
    assert!(foreign_owner != ProcessId::current());
    for original in [&run.native.0.directory._lock, &run.native.0._workspace_lock] {
        let inherited = FlockLease {
            file: original.file.try_clone().unwrap(),
            acquired_by: foreign_owner,
        };
        drop(inherited);
    }
    let directory = DirectoryLease::reacquire(fixture.path()).map(drop);
    let workspace = run
        .native
        .0
        .workspace
        .lock(
            c"lease",
            WORKSPACE_MARKER,
            LockAccess::Existing,
            Phase::Reconcile,
        )
        .map(drop);
    drop(run);
    let after_release = state.reconcile().await;
    assert!(
        matches!(&directory, Err(NativeError::Busy))
            && matches!(&workspace, Err(NativeError::Busy)),
        "dropping a non-acquiring copy must not unlock the live owner: \
         directory={directory:?}, workspace={workspace:?}"
    );
    assert!(
        matches!(&after_release, Ok(Reconciliation::NeedsBody { .. })),
        "the acquiring owner must still release both leases: {after_release:?}"
    );
}

#[tokio::test]
async fn cooperative_stop_retains_checkpointed_needs_body_without_polling_the_body() {
    let fixture = Fixture::new();
    let polls = Rc::new(Cell::new(0));
    let dropped = Rc::new(Cell::new(false));
    let body = ObservedBody {
        polls: Rc::clone(&polls),
        dropped: Rc::clone(&dropped),
        after_first: AfterFirst::Pending,
    };
    let local_signal = Rc::new(Cell::new(17));
    let borrowed_signal = &local_signal;
    let pending = Transfer::from_body(body, Length::Unknown)
        .expect(requirement(b"abc"))
        .to_file(fixture.output())
        .unwrap();
    let failure = pending
        .run_until(async {
            assert_eq!(borrowed_signal.get(), 17);
        })
        .await
        .unwrap_err();
    assert!(matches!(failure.kind, FailureKind::Stopped));
    assert_eq!(polls.get(), 0);
    assert!(dropped.get());
    assert_eq!(failure.partial, Partial::default());
    assert!(matches!(
        failure.state.unwrap().reconcile().await.unwrap(),
        Reconciliation::NeedsBody { .. }
    ));
}

#[tokio::test]
async fn checkpoint_failure_cannot_be_reported_as_a_successful_pause() {
    let fixture = Fixture::new();
    let (run, state) = owned_run(&fixture).await;
    run.native
        .hooks()
        .fail_checkpoint
        .store(true, Ordering::Release);
    let failure = run
        .execute(body(), core::future::ready(()))
        .await
        .unwrap_err();
    match failure.kind {
        FailureKind::CheckpointFailed { cause, checkpoint } => {
            assert!(matches!(*cause, FailureKind::Stopped));
            assert!(matches!(
                *checkpoint,
                FailureKind::Io {
                    phase: Phase::Checkpoint,
                    ..
                }
            ));
        }
        other => panic!("expected failed stop checkpoint, got {other:?}"),
    }
    assert!(matches!(
        state.reconcile().await.unwrap(),
        Reconciliation::NeedsBody { .. }
    ));
}

#[tokio::test]
async fn checkpoint_failure_keeps_the_original_body_cause_by_value_too() {
    let fixture = Fixture::new();
    let (run, _) = owned_run(&fixture).await;
    run.native
        .hooks()
        .fail_checkpoint
        .store(true, Ordering::Release);
    let cause = String::from("borrowed body value");
    let failure = run
        .execute(stream::iter([Err::<Bytes, _>(cause.as_str())]), pending())
        .await
        .unwrap_err();
    match failure.kind {
        FailureKind::CheckpointFailed {
            cause: body,
            checkpoint,
        } => {
            assert!(
                matches!(*body, FailureKind::Body(value) if core::ptr::eq(value.as_ptr(), cause.as_ptr()))
            );
            assert!(matches!(
                *checkpoint,
                FailureKind::Io {
                    phase: Phase::Checkpoint,
                    ..
                }
            ));
        }
        other => panic!("expected both original causes, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_write_future_keeps_both_leases_until_physical_settlement() {
    let fixture = Fixture::new();
    fs::create_dir(fixture.path().join("alias")).unwrap();
    let (run, state) = owned_run(&fixture).await;
    let barrier = run.native.hooks().write.install();
    let released = Arc::clone(&run.native.hooks().released);
    let polls = Rc::new(Cell::new(0));
    let dropped = Rc::new(Cell::new(false));
    let observed = ObservedBody {
        polls: Rc::clone(&polls),
        dropped: Rc::clone(&dropped),
        after_first: AfterFirst::Pending,
    };
    let mut future = Box::pin(run.execute(observed, pending()));
    tokio::select! {
        result = &mut future => panic!("physical write should be held, got {result:?}"),
        () = barrier.entered.notified() => {}
    }
    drop(future);
    assert!(dropped.get());
    assert_eq!(polls.get(), 1);
    probe(fixture.path(), "busy");
    probe(&fixture.path().join("alias/.."), "busy");
    probe(state.path(), "workspace-busy");
    assert!(matches!(
        state.reconcile().await.unwrap_err().kind,
        FailureKind::Busy
    ));
    barrier.release();
    released.notified().await;
    probe(fixture.path(), "free");
    probe(state.path(), "workspace-free");
    assert_eq!(polls.get(), 1);
    assert_eq!(fs::read(state.path().join("data")).unwrap(), b"abc");
    assert!(!fixture.output().exists());
    // Abrupt drop promises the last valid checkpoint, not the just-finished unacknowledged write.
    match state.reconcile().await.unwrap() {
        Reconciliation::NeedsBody { partial, .. } => assert_eq!(partial.written, ByteCount(0)),
        other => panic!("expected one-shot restart evidence, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_commit_future_stays_busy_then_reconciles_the_real_effect() {
    let fixture = Fixture::new();
    fs::create_dir(fixture.path().join("alias")).unwrap();
    let (run, state) = owned_run(&fixture).await;
    let barrier = run.native.hooks().commit.install();
    let released = Arc::clone(&run.native.hooks().released);
    run.native
        .hooks()
        .lose_publish_ack
        .store(true, Ordering::Release);
    let polls = Rc::new(Cell::new(0));
    let dropped = Rc::new(Cell::new(false));
    let observed = ObservedBody {
        polls: Rc::clone(&polls),
        dropped: Rc::clone(&dropped),
        after_first: AfterFirst::Eof,
    };
    let mut future = Box::pin(run.execute(observed, pending()));
    tokio::select! {
        result = &mut future => panic!("physical commit should be held, got {result:?}"),
        () = barrier.entered.notified() => {}
    }
    assert!(dropped.get());
    assert_eq!(polls.get(), 2);
    drop(future);
    assert!(!fixture.output().exists());
    probe(fixture.path(), "busy");
    probe(&fixture.path().join("alias/.."), "busy");
    probe(state.path(), "workspace-busy");
    assert!(matches!(
        state.reconcile().await.unwrap_err().kind,
        FailureKind::Busy
    ));
    barrier.release();
    released.notified().await;
    assert_eq!(polls.get(), 2);
    probe(state.path(), "workspace-free");
    assert_eq!(fs::read(fixture.output()).unwrap(), b"abc");
    assert!(matches!(
        state.reconcile().await.unwrap(),
        Reconciliation::Published(_)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_during_commit_settles_and_reports_publication_instead_of_pause() {
    let fixture = Fixture::new();
    let (run, _) = owned_run(&fixture).await;
    let barrier = run.native.hooks().commit.install();
    let (stop, signal) = tokio::sync::oneshot::channel();
    let mut future = Box::pin(run.execute(body(), async {
        let _ = signal.await;
    }));
    tokio::select! {
        result = &mut future => panic!("physical commit should be held, got {result:?}"),
        () = barrier.entered.notified() => {}
    }
    stop.send(()).unwrap();
    barrier.release();
    let receipt = future.await.unwrap();
    assert_eq!(receipt.partial.written, ByteCount(3));
    assert_eq!(fs::read(fixture.output()).unwrap(), b"abc");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cooperative_stop_during_write_settles_and_checkpoints_the_accepted_prefix() {
    let fixture = Fixture::new();
    let (run, state) = owned_run(&fixture).await;
    let barrier = run.native.hooks().write.install();
    let (stop, signal) = tokio::sync::oneshot::channel();
    let mut future = Box::pin(run.execute(body(), async {
        let _ = signal.await;
    }));
    tokio::select! {
        result = &mut future => panic!("physical write should be held, got {result:?}"),
        () = barrier.entered.notified() => {}
    }
    stop.send(()).unwrap();
    barrier.release();
    let failure = future.await.unwrap_err();
    assert!(matches!(failure.kind, FailureKind::Stopped));
    assert_eq!(failure.partial.written, ByteCount(3));
    assert!(!fixture.output().exists());
    match state.reconcile().await.unwrap() {
        Reconciliation::NeedsBody { partial, .. } => assert_eq!(partial.written, ByteCount(3)),
        other => panic!("expected stopped one-shot evidence, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_during_seal_settles_before_checkpointing_without_starting_publication() {
    let fixture = Fixture::new();
    let (run, state) = owned_run(&fixture).await;
    let barrier = run.native.hooks().seal.install();
    let (stop, signal) = tokio::sync::oneshot::channel();
    let mut future = Box::pin(run.execute(body(), async {
        let _ = signal.await;
    }));
    tokio::select! {
        result = &mut future => panic!("physical seal should be held, got {result:?}"),
        () = barrier.entered.notified() => {}
    }
    stop.send(()).unwrap();
    barrier.release();
    let failure = future.await.unwrap_err();
    assert!(matches!(failure.kind, FailureKind::Stopped));
    assert!(!fixture.output().exists());
    assert!(matches!(
        state.reconcile().await.unwrap(),
        Reconciliation::NeedsBody { .. }
    ));
}

#[tokio::test]
async fn lost_acknowledgement_after_actual_link_is_unknown_until_leased_reconciliation() {
    let fixture = Fixture::new();
    let state = lose_ack(&fixture).await;
    let output = fs::metadata(fixture.output()).unwrap();
    assert_eq!(
        output.ino(),
        fs::metadata(state.path().join("data")).unwrap().ino()
    );
    assert_eq!(fs::read(fixture.output()).unwrap(), b"abc");
    assert!(matches!(
        state.reconcile().await.unwrap(),
        Reconciliation::Published(_)
    ));
    assert!(matches!(
        state.reconcile().await.unwrap(),
        Reconciliation::Published(_)
    ));
}

#[tokio::test]
async fn identical_foreign_bytes_are_not_adopted_as_the_published_inode() {
    let fixture = Fixture::new();
    let state = lose_ack(&fixture).await;
    fs::remove_file(fixture.output()).unwrap();
    fs::write(fixture.output(), b"abc").unwrap();
    let foreign = fs::metadata(fixture.output()).unwrap().ino();
    assert_ne!(
        foreign,
        fs::metadata(state.path().join("data")).unwrap().ino()
    );
    assert!(matches!(
        state.reconcile().await.unwrap(),
        Reconciliation::Conflict(_)
    ));
    assert_eq!(fs::metadata(fixture.output()).unwrap().ino(), foreign);
    assert_eq!(fs::read(fixture.output()).unwrap(), b"abc");
}

#[tokio::test]
async fn missing_destination_after_intent_stays_unknown_without_republication() {
    let fixture = Fixture::new();
    let state = lose_ack(&fixture).await;
    fs::remove_file(fixture.output()).unwrap();
    assert!(matches!(
        state.reconcile().await.unwrap(),
        Reconciliation::PublicationUnknown(_)
    ));
    assert!(!fixture.output().exists());
}

#[tokio::test]
async fn missing_stage_after_intent_stays_unknown_without_adopting_the_destination() {
    let fixture = Fixture::new();
    let state = lose_ack(&fixture).await;
    fs::remove_file(state.path().join("data")).unwrap();
    assert!(matches!(
        state.reconcile().await.unwrap(),
        Reconciliation::PublicationUnknown(_)
    ));
    assert_eq!(fs::read(fixture.output()).unwrap(), b"abc");
}

#[tokio::test]
async fn a_symlink_destination_after_intent_is_a_conflict_not_adopted() {
    let fixture = Fixture::new();
    let state = lose_ack(&fixture).await;
    fs::remove_file(fixture.output()).unwrap();
    symlink(state.path().join("data"), fixture.output()).unwrap();
    let before = fs::symlink_metadata(fixture.output()).unwrap().ino();
    assert!(matches!(
        state.reconcile().await.unwrap(),
        Reconciliation::Conflict(_)
    ));
    assert_eq!(
        fs::symlink_metadata(fixture.output()).unwrap().ino(),
        before
    );
}

#[tokio::test]
async fn changed_stage_content_is_rehashed_and_refused_not_trusted_by_inode() {
    let fixture = Fixture::new();
    let state = lose_ack(&fixture).await;
    fs::write(fixture.output(), b"xyz").unwrap();
    assert!(matches!(
        state.reconcile().await.unwrap(),
        Reconciliation::Conflict(_)
    ));
    assert_eq!(fs::read(fixture.output()).unwrap(), b"xyz");
}

#[tokio::test]
async fn sealed_and_published_data_cannot_return_to_receiving_or_reset_state() {
    let fixture = Fixture::new();
    let (run, _) = owned_run(&fixture).await;
    let native = run.native;
    assert_eq!(
        native
            .write(ByteCount(0), Bytes::from_static(b"abc"))
            .await
            .unwrap(),
        ByteCount(3)
    );
    native.seal(ByteCount(3)).await.unwrap();
    assert!(matches!(
        native.write(ByteCount(0), Bytes::from_static(b"bad")).await,
        Err(super::WriteFailure {
            error: NativeError::InvalidState,
            ..
        })
    ));
    native.publish(complete(b"abc")).await.unwrap();
    assert!(matches!(
        native.write(ByteCount(0), Bytes::from_static(b"bad")).await,
        Err(super::WriteFailure {
            error: NativeError::InvalidState,
            ..
        })
    ));
    assert!(matches!(
        native.checkpoint(Partial::default()).await,
        Err(NativeError::InvalidState)
    ));
    assert!(matches!(
        native.seal(ByteCount(3)).await,
        Err(NativeError::InvalidState)
    ));
    assert_eq!(fs::read(fixture.output()).unwrap(), b"abc");
}

#[tokio::test]
async fn create_new_workspace_collision_preserves_foreign_contents() {
    let fixture = Fixture::new();
    let plan = Plan::new(&fixture.output(), requirement(b"abc")).unwrap();
    fs::create_dir(plan.state().path()).unwrap();
    let sentinel = plan.state().path().join("foreign");
    fs::write(&sentinel, b"foreign").unwrap();
    let before = fs::metadata(&sentinel).unwrap().ino();
    assert!(Native::acquire(plan).await.is_err());
    assert_eq!(fs::read(&sentinel).unwrap(), b"foreign");
    assert_eq!(fs::metadata(&sentinel).unwrap().ino(), before);
}

#[tokio::test]
async fn foreign_state_is_not_replaced_by_checkpointing() {
    let fixture = Fixture::new();
    let (run, state) = owned_run(&fixture).await;
    let file = state.path().join("state");
    fs::remove_file(&file).unwrap();
    fs::write(&file, b"foreign").unwrap();
    fs::set_permissions(&file, fs::Permissions::from_mode(0o600)).unwrap();
    let before = fs::metadata(&file).unwrap().ino();
    assert!(matches!(
        run.native.checkpoint(Partial::default()).await,
        Err(NativeError::InvalidState)
    ));
    assert_eq!(fs::read(&file).unwrap(), b"foreign");
    assert_eq!(fs::metadata(&file).unwrap().ino(), before);
}

#[tokio::test]
async fn foreign_symlink_lock_is_preserved_without_touching_its_target() {
    let fixture = Fixture::new();
    let target = fixture.path().join("foreign");
    fs::write(&target, b"foreign").unwrap();
    symlink(&target, fixture.path().join(".xget-native.lock")).unwrap();
    let before = fs::symlink_metadata(fixture.path().join(".xget-native.lock"))
        .unwrap()
        .ino();
    let pending = Transfer::from_body(body(), Length::Unknown)
        .expect(requirement(b"abc"))
        .to_file(fixture.output())
        .unwrap();
    assert!(pending.run().await.is_err());
    assert_eq!(fs::read(&target).unwrap(), b"foreign");
    assert_eq!(
        fs::symlink_metadata(fixture.path().join(".xget-native.lock"))
            .unwrap()
            .ino(),
        before
    );
}

#[tokio::test]
async fn two_sealed_stages_race_the_actual_no_replace_link_primitive() {
    // Each stage has independent ownership. Admission Busy is not evidence of atomic publication.
    let left_fixture = Fixture::new();
    let right_fixture = Fixture::new();
    let target = Fixture::new();
    let (left, _) = owned_run(&left_fixture).await;
    let (right, _) = owned_run(&right_fixture).await;
    for native in [&left.native, &right.native] {
        native
            .write(ByteCount(0), Bytes::from_static(b"abc"))
            .await
            .unwrap();
        native.seal(ByteCount(3)).await.unwrap();
    }
    let destination = Arc::new(Directory(File::open(target.path()).unwrap()));
    let barrier = Arc::new(Barrier::new(3));
    let jobs: Vec<_> = [left.native, right.native]
        .into_iter()
        .map(|native| {
            let destination = Arc::clone(&destination);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                destination.publish(&native.0.workspace, DATA, c"output")
            })
        })
        .collect();
    barrier.wait();
    let results: Vec<_> = jobs.into_iter().map(|job| job.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(NativeError::DestinationExists)))
            .count(),
        1
    );
    assert_eq!(fs::read(target.output()).unwrap(), b"abc");
    let identity = fs::metadata(target.output()).unwrap().ino();
    let stages = [left_fixture.path(), right_fixture.path()]
        .into_iter()
        .map(|root| {
            fs::read_dir(root)
                .unwrap()
                .map(Result::unwrap)
                .find(|entry| entry.file_type().unwrap().is_dir())
                .unwrap()
                .path()
                .join("data")
        })
        .collect::<Vec<_>>();
    assert_eq!(
        stages
            .iter()
            .filter(|path| fs::metadata(path).unwrap().ino() == identity)
            .count(),
        1
    );
}

#[test]
fn subprocess_ownership_probe() {
    let Some(parent) = std::env::var_os("XGET_NATIVE_TEST_PARENT") else {
        return;
    };
    let expected = std::env::var("XGET_NATIVE_TEST_EXPECTED").unwrap();
    if expected.starts_with("owned-") {
        let workspace = Directory(File::open(&parent).unwrap());
        let result = workspace.lock(
            c"lease",
            crate::transfer::OWNED_LEASE_MARKER,
            LockAccess::Existing,
            Phase::Acquire,
        );
        match expected.as_str() {
            "owned-busy" => assert!(matches!(result, Err(NativeError::Busy))),
            "owned-free" => assert!(result.is_ok()),
            other => panic!("unexpected aggregate probe mode {other}"),
        }
        return;
    }
    if expected.starts_with("workspace-") {
        let workspace = Directory(File::open(&parent).unwrap());
        let result = workspace.lock(
            c"lease",
            WORKSPACE_MARKER,
            LockAccess::Existing,
            Phase::Reconcile,
        );
        match expected.as_str() {
            "workspace-busy" => assert!(matches!(result, Err(NativeError::Busy))),
            "workspace-free" => assert!(result.is_ok()),
            other => panic!("unexpected workspace probe mode {other}"),
        }
        return;
    }
    let result = DirectoryLease::acquire(Path::new(&parent));
    match expected.as_str() {
        "busy" => assert!(matches!(result, Err(NativeError::Busy))),
        "free" => assert!(result.is_ok()),
        other => panic!("unexpected owned subprocess probe mode {other}"),
    }
}

#[tokio::test]
async fn native_reconciliation_reads_a_real_v1_publishing_record_without_replacing_the_output() {
    let fixture = Fixture::new();
    let state = lose_ack(&fixture).await;
    let inode = fs::metadata(fixture.output()).unwrap().ino();
    // Independent old-format fixture, using only this run's actual anchored file identities.
    let mut bytes = b"XGETNF01".to_vec();
    bytes.push(2);
    for path in [
        fixture.path().to_path_buf(),
        state.path().to_path_buf(),
        state.path().join("data"),
    ] {
        let metadata = fs::metadata(path).unwrap();
        bytes.extend_from_slice(&metadata.dev().to_le_bytes());
        bytes.extend_from_slice(&metadata.ino().to_le_bytes());
    }
    let digest = requirement(b"abc").digest();
    bytes.extend_from_slice(&digest.0);
    bytes.push(1);
    bytes.extend_from_slice(&digest.0);
    bytes.push(0);
    for _ in 0..3 {
        bytes.extend_from_slice(&3u64.to_le_bytes());
    }
    bytes.extend_from_slice(&6u16.to_le_bytes());
    bytes.extend_from_slice(b"output");
    fs::write(state.path().join("state"), bytes).unwrap();
    let outcome = state.reconcile().await.unwrap();
    match outcome {
        Reconciliation::Published(receipt) => {
            assert_eq!(receipt.required, Some(requirement(b"abc")));
            assert_eq!(receipt.checks().matched_requirements().count(), 1);
            assert_eq!(
                receipt.checks().measured(),
                Some(requirement(b"abc").into())
            );
        }
        other => panic!("expected anchored v1 publication evidence, got {other:?}"),
    }
    assert_eq!(fs::metadata(fixture.output()).unwrap().ino(), inode);
    assert_eq!(
        &fs::read(state.path().join("state")).unwrap()[..8],
        b"XGETNF02"
    );
}

#[tokio::test]
async fn no_hash_lost_ack_stays_not_revalidatable_without_a_recovery_fingerprint_or_receipt() {
    let fixture = Fixture::new();
    let policy = crate::checks::CheckPolicy {
        requirements: crate::checks::Requirements::default(),
        measurement: crate::transfer::Measurement::None,
    };
    let plan = Plan::new(&fixture.output(), policy).unwrap();
    let state = StateRef::clone(plan.state());
    let native = Native::acquire(plan).await.unwrap();
    native
        .hooks()
        .lose_publish_ack
        .store(true, Ordering::Release);
    let failure = Run {
        native,
        state: StateRef::clone(&state),
        length: Length::Known(ByteCount(3)),
    }
    .execute(body(), pending())
    .await
    .unwrap_err();
    assert!(matches!(
        failure.kind,
        FailureKind::PublicationUnknown { .. }
    ));
    assert_eq!(failure.partial.hashed, ByteCount(0));
    assert_eq!(failure.partial.actual, None);
    let before = fs::read(state.path().join("state")).unwrap();
    assert_eq!(before[8], 2);
    assert!(matches!(
        state.reconcile().await.unwrap(),
        Reconciliation::NotRevalidatable(_)
    ));
    assert_eq!(fs::read(state.path().join("state")).unwrap(), before);
    assert_eq!(fs::read(fixture.output()).unwrap(), b"abc");
}

async fn owned_run(fixture: &Fixture) -> (Run, StateRef) {
    let plan = Plan::new(&fixture.output(), requirement(b"abc")).unwrap();
    let state = StateRef::clone(plan.state());
    let native = Native::acquire(plan).await.unwrap();
    (
        Run {
            native,
            state: StateRef::clone(&state),
            length: Length::Known(ByteCount(3)),
        },
        state,
    )
}

async fn lose_ack(fixture: &Fixture) -> StateRef {
    let (run, state) = owned_run(fixture).await;
    run.native
        .hooks()
        .lose_publish_ack
        .store(true, Ordering::Release);
    let failure = run.execute(body(), pending()).await.unwrap_err();
    assert!(matches!(
        failure.kind,
        FailureKind::PublicationUnknown { .. }
    ));
    assert_eq!(failure.partial.actual, Some(requirement(b"abc").digest()));
    state
}

fn body() -> impl Stream<Item = Result<Bytes, Infallible>> {
    stream::iter([Ok(Bytes::from_static(b"abc"))])
}

fn probe(parent: &Path, expected: &str) {
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "transfer::native::tests::subprocess_ownership_probe",
            "--nocapture",
        ])
        .env("XGET_NATIVE_TEST_PARENT", parent)
        .env("XGET_NATIVE_TEST_EXPECTED", expected)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "ownership probe failed: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("1 passed"),
        "probe must actually execute one test"
    );
}

/// Own the diagnostic child and parent pipe ends so cancellation/unwinding cannot strand the child.
struct InheritedLockChild {
    pid: Option<ChildPid>,
    release: Option<File>,
    events: File,
}

struct ChildPid(libc::pid_t);

#[derive(Debug)]
struct ChildStatus(libc::c_int);

impl InheritedLockChild {
    fn hold(directory_lock: RawFd, workspace_lock: RawFd) -> io::Result<Self> {
        Self::require_close_on_exec(directory_lock)?;
        Self::require_close_on_exec(workspace_lock)?;
        let [release_read, release_write] = Self::pipe()?;
        let [event_read, event_write] = Self::pipe()?;
        let release_read_fd = release_read.as_raw_fd();
        let release_write_fd = release_write.as_raw_fd();
        let event_read_fd = event_read.as_raw_fd();
        let event_write_fd = event_write.as_raw_fd();
        let ready = *b"R";
        let mut release = [0u8];
        let closed = *b"C";
        let ready_ptr = ready.as_ptr().cast();
        let release_ptr = release.as_mut_ptr().cast();
        let closed_ptr = closed.as_ptr().cast();
        // SAFETY: all child-side buffers and descriptor numbers exist before fork. The child branch
        // uses only primitive control flow and async-signal-safe libc read/write/close/_exit, then
        // exits without running Rust destructors, allocation, formatting, panics or the runtime.
        let pid = unsafe { libc::fork() };
        if pid == 0 {
            // SAFETY: these are the live inherited descriptors and preallocated stack buffers. EOF
            // on the control pipe is the release barrier, so even parent unwinding releases it.
            unsafe {
                if libc::close(release_write_fd) != 0 || libc::close(event_read_fd) != 0 {
                    libc::_exit(11);
                }
                if libc::write(event_write_fd, ready_ptr, 1) != 1 {
                    libc::_exit(12);
                }
                if libc::read(release_read_fd, release_ptr, 1) != 0 {
                    libc::_exit(13);
                }
                let directory_closed = libc::close(directory_lock);
                let workspace_closed = libc::close(workspace_lock);
                if directory_closed != 0 || workspace_closed != 0 {
                    libc::_exit(14);
                }
                if libc::write(event_write_fd, closed_ptr, 1) != 1 {
                    libc::_exit(15);
                }
                libc::close(release_read_fd);
                libc::close(event_write_fd);
                libc::_exit(0);
            }
        }
        if pid < 0 {
            return Err(io::Error::last_os_error());
        }
        let child = Self {
            pid: Some(ChildPid(pid)),
            release: Some(release_write),
            events: event_read,
        };
        drop(release_read);
        drop(event_write);
        Ok(child)
    }

    fn require_close_on_exec(fd: RawFd) -> io::Result<()> {
        // SAFETY: the actual Native registration still owns this descriptor before the fork.
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        if flags < 0 {
            return Err(io::Error::last_os_error());
        }
        if flags & libc::FD_CLOEXEC == 0 {
            return Err(io::Error::other(
                "native lock descriptor lacks close-on-exec",
            ));
        }
        Ok(())
    }

    fn pipe() -> io::Result<[File; 2]> {
        let mut fds = [-1; 2];
        // SAFETY: pipe initializes exactly these two descriptor slots on success.
        if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let [read, write] = fds;
        // SAFETY: both descriptors are newly created and each receives exactly one File owner.
        let files = unsafe { [File::from_raw_fd(read), File::from_raw_fd(write)] };
        for file in &files {
            // Prevent unrelated exec children from retaining this diagnostic's pipe ends. These
            // parent-side calls occur before our fork and do not alter the Native lock descriptors.
            // SAFETY: File owns the descriptor throughout both fcntl calls.
            let flags = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFD) };
            if flags < 0 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: F_SETFD accepts these descriptor flags and does not transfer ownership.
            if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETFD, flags | libc::FD_CLOEXEC) }
                != 0
            {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(files)
    }

    fn acknowledgement(&mut self, expected: u8) -> io::Result<()> {
        let mut actual = [0];
        self.events.read_exact(&mut actual)?;
        if actual != [expected] {
            return Err(io::Error::other(
                "unexpected diagnostic child acknowledgement",
            ));
        }
        Ok(())
    }

    fn release_and_wait(mut self) -> io::Result<ChildStatus> {
        drop(self.release.take());
        let closed = self.acknowledgement(b'C');
        // Always reap before returning even when the acknowledgement failed.
        let status = self.wait();
        closed.and(status)
    }

    fn wait(&mut self) -> io::Result<ChildStatus> {
        let Some(ChildPid(pid)) = self.pid.take() else {
            return Err(io::Error::other("diagnostic child was already reaped"));
        };
        let mut status = 0;
        loop {
            // SAFETY: this guard owns exactly this child, and status is writable for waitpid.
            let result = unsafe { libc::waitpid(pid, &mut status, 0) };
            if result == pid {
                return Ok(ChildStatus(status));
            }
            let cause = io::Error::last_os_error();
            if cause.kind() != io::ErrorKind::Interrupted {
                return Err(cause);
            }
        }
    }
}

impl Drop for InheritedLockChild {
    fn drop(&mut self) {
        // EOF releases a child still at its barrier. It writes at most two acknowledgement bytes,
        // so the live events pipe cannot fill even if unwinding skipped the parent's reads.
        drop(self.release.take());
        if self.pid.is_some() {
            let _ = self.wait();
        }
    }
}

impl ChildStatus {
    fn successful(&self) -> bool {
        let Self(status) = *self;
        libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0
    }
}

struct ObservedBody {
    polls: Rc<Cell<u32>>,
    dropped: Rc<Cell<bool>>,
    after_first: AfterFirst,
}

#[derive(Clone, Copy)]
enum AfterFirst {
    Pending,
    Eof,
}

impl Stream for ObservedBody {
    type Item = Result<Bytes, Infallible>;
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let polls = self.polls.get();
        self.polls.set(polls + 1);
        if polls == 0 {
            Poll::Ready(Some(Ok(Bytes::from_static(b"abc"))))
        } else {
            match self.after_first {
                AfterFirst::Pending => Poll::Pending,
                AfterFirst::Eof => Poll::Ready(None),
            }
        }
    }
}

impl Drop for ObservedBody {
    fn drop(&mut self) {
        self.dropped.set(true);
    }
}
