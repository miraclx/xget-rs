//! Authored owned-fixture scenarios, not execution evidence.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use core::convert::Infallible;
use core::sync::atomic::Ordering;
use std::fs;
use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _, symlink};
use std::sync::Arc;

use bytes::Bytes;
use futures::stream;

use super::{Directory, DirectoryLease, Native, NativeError};
use crate::transfer::pipeline::Run;
use crate::transfer::plan::Plan;
use crate::transfer::tests::{Fixture, complete, requirement};
use crate::transfer::{
    ByteCount, FailureKind, Length, Measurement, Partial, Publication, Reconciliation, StateRef,
    Transfer, WriteExtent,
};

fn replacement(fixture: &Fixture) -> Plan {
    let mut plan = Plan::new(&fixture.output(), requirement(b"abc")).unwrap();
    plan.publication = Publication::Replace;
    plan
}

async fn sealed(fixture: &Fixture) -> Native {
    let native = Native::acquire(replacement(fixture)).await.unwrap();
    native
        .write(ByteCount(0), Bytes::from_static(b"abc"))
        .await
        .unwrap();
    native.seal(ByteCount(3)).await.unwrap();
    native
}

#[tokio::test]
async fn explicit_replacement_changes_the_final_inode_only_after_checked_completion() {
    let fixture = Fixture::new();
    fs::write(fixture.output(), b"sentinel").unwrap();
    let incumbent = fs::metadata(fixture.output()).unwrap().ino();
    let pending = Transfer::from_body(
        stream::iter([Ok::<_, Infallible>(Bytes::from_static(b"abc"))]),
        Length::Known(ByteCount(3)),
    )
    .expect(requirement(b"abc"))
    .to_file(fixture.output())
    .unwrap()
    .authorized(Publication::Replace, None);
    let state = StateRef::clone(pending.state());
    let receipt = pending.run().await.unwrap();
    assert_eq!(fs::read(fixture.output()).unwrap(), b"abc");
    assert_ne!(fs::metadata(fixture.output()).unwrap().ino(), incumbent);
    assert_eq!(
        fs::metadata(fixture.output()).unwrap().ino(),
        fs::metadata(state.path().join("data")).unwrap().ino()
    );
    assert!(matches!(
        receipt.state().reconcile().await.unwrap(),
        Reconciliation::Published(_)
    ));
}

#[tokio::test]
async fn equal_bytes_do_not_weaken_default_absent_only_publication() {
    let fixture = Fixture::new();
    fs::write(fixture.output(), b"abc").unwrap();
    let incumbent = fs::metadata(fixture.output()).unwrap().ino();
    let failure = Transfer::from_body(
        stream::iter([Ok::<_, Infallible>(Bytes::from_static(b"abc"))]),
        Length::Unknown,
    )
    .expect(requirement(b"abc"))
    .to_file(fixture.output())
    .unwrap()
    .run()
    .await
    .unwrap_err();
    assert!(matches!(failure.kind, FailureKind::DestinationExists));
    assert_eq!(fs::metadata(fixture.output()).unwrap().ino(), incumbent);
}

#[tokio::test]
async fn replacement_preserves_sentinel_bytes_and_inode_on_wrong_digest_late_error_and_stop() {
    for scenario in 0..3 {
        let fixture = Fixture::new();
        fs::write(fixture.output(), b"sentinel").unwrap();
        let inode = fs::metadata(fixture.output()).unwrap().ino();
        let body = stream::iter(if scenario == 1 {
            vec![
                Ok(Bytes::from_static(b"abc")),
                Err(std::io::Error::other("late body error")),
            ]
        } else {
            vec![Ok(Bytes::from_static(b"abc"))]
        });
        let pending = Transfer::from_body(body, Length::Known(ByteCount(3)))
            .expect(requirement(if scenario == 0 { b"wrong" } else { b"abc" }))
            .to_file(fixture.output())
            .unwrap()
            .authorized(Publication::Replace, None);
        let failure = pending
            .run_until(async move {
                if scenario != 2 {
                    core::future::pending::<()>().await;
                }
            })
            .await
            .unwrap_err();
        assert!(matches!(
            failure.kind,
            FailureKind::DigestMismatch(_) | FailureKind::Body(_) | FailureKind::Stopped
        ));
        assert_eq!(fs::read(fixture.output()).unwrap(), b"sentinel");
        assert_eq!(fs::metadata(fixture.output()).unwrap().ino(), inode);
    }
}

#[tokio::test]
async fn replacement_refuses_symlinks_directories_and_hardlink_aliases_before_payload_polling() {
    for role in 0..3 {
        let fixture = Fixture::new();
        let foreign = fixture.path().join("foreign");
        fs::write(&foreign, b"sentinel").unwrap();
        match role {
            0 => symlink(&foreign, fixture.output()).unwrap(),
            1 => fs::create_dir(fixture.output()).unwrap(),
            _ => fs::hard_link(&foreign, fixture.output()).unwrap(),
        }
        let mut plan = replacement(&fixture);
        plan.targets = Arc::from([fixture.output(), foreign.to_path_buf()]);
        let result = Native::acquire(plan).await;
        assert!(result.is_err());
        assert_eq!(fs::read(&foreign).unwrap(), b"sentinel");
        assert!(!fs::read_dir(fixture.path()).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".xget-native-")
        }));
    }
}

#[tokio::test]
async fn a_changed_incumbent_is_not_replaced_after_acquisition() {
    let fixture = Fixture::new();
    fs::write(fixture.output(), b"sentinel").unwrap();
    let native = sealed(&fixture).await;
    let foreign = fixture.path().join("foreign");
    fs::write(&foreign, b"foreign").unwrap();
    let inode = fs::metadata(&foreign).unwrap().ino();
    fs::rename(&foreign, fixture.output()).unwrap();
    assert!(matches!(
        native.publish(complete(b"abc")).await,
        Err(NativeError::InvalidState)
    ));
    assert_eq!(fs::read(fixture.output()).unwrap(), b"foreign");
    assert_eq!(fs::metadata(fixture.output()).unwrap().ino(), inode);
}

#[tokio::test]
async fn replacing_an_earlier_native_output_retains_its_old_witness_without_adopting_equal_bytes() {
    let fixture = Fixture::new();
    let old = Transfer::from_body(
        stream::iter([Ok::<_, Infallible>(Bytes::from_static(b"old"))]),
        Length::Unknown,
    )
    .expect(requirement(b"old"))
    .to_file(fixture.output())
    .unwrap()
    .run()
    .await
    .unwrap();
    let inode = fs::metadata(fixture.output()).unwrap().ino();
    assert_eq!(fs::metadata(fixture.output()).unwrap().nlink(), 2);
    Transfer::from_body(
        stream::iter([Ok::<_, Infallible>(Bytes::from_static(b"abc"))]),
        Length::Unknown,
    )
    .expect(requirement(b"abc"))
    .to_file(fixture.output())
    .unwrap()
    .authorized(Publication::Replace, None)
    .run()
    .await
    .unwrap();
    assert_eq!(fs::read(old.state().path().join("data")).unwrap(), b"old");
    assert_eq!(
        fs::metadata(old.state().path().join("data")).unwrap().ino(),
        inode
    );
    assert_ne!(fs::metadata(fixture.output()).unwrap().ino(), inode);
    assert!(matches!(
        old.state().reconcile().await.unwrap(),
        Reconciliation::Conflict(_)
    ));
}

#[tokio::test]
async fn an_alias_added_to_another_frozen_target_before_commit_preserves_the_incumbent() {
    let fixture = Fixture::new();
    fs::write(fixture.output(), b"sentinel").unwrap();
    let inode = fs::metadata(fixture.output()).unwrap().ino();
    let other = fixture.path().join("other-target");
    let mut plan = replacement(&fixture);
    plan.targets = Arc::from([fixture.output(), other.to_path_buf()]);
    let native = Native::acquire(plan).await.unwrap();
    native
        .write(ByteCount(0), Bytes::from_static(b"abc"))
        .await
        .unwrap();
    native.seal(ByteCount(3)).await.unwrap();
    let held = native.hooks().commit.install();
    let mut publication = Box::pin(native.publish(complete(b"abc")));
    tokio::select! {
        result = &mut publication => panic!("commit must be held: {result:?}"),
        () = held.entered.notified() => {}
    }
    fs::hard_link(fixture.output(), &other).unwrap();
    held.release();
    assert!(matches!(publication.await, Err(NativeError::InvalidState)));
    assert_eq!(fs::metadata(fixture.output()).unwrap().ino(), inode);
    assert_eq!(fs::read(fixture.output()).unwrap(), b"sentinel");
    assert_eq!(fs::read(other).unwrap(), b"sentinel");
}

#[tokio::test]
async fn lost_replacement_ack_reconciles_only_exact_new_inode_and_recorded_strong_content() {
    for measurement in [Measurement::None, Measurement::default()] {
        let fixture = Fixture::new();
        fs::write(fixture.output(), b"sentinel").unwrap();
        let mut plan = Plan::new(
            &fixture.output(),
            crate::checks::CheckPolicy {
                requirements: crate::checks::Requirements::default(),
                measurement,
            },
        )
        .unwrap();
        plan.publication = Publication::Replace;
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
        .execute(
            stream::iter([Ok::<_, Infallible>(Bytes::from_static(b"abc"))]),
            core::future::pending(),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            failure.kind,
            FailureKind::PublicationUnknown { .. }
        ));
        let result = state.reconcile().await.unwrap();
        assert!(matches!(
            (measurement, result),
            (Measurement::None, Reconciliation::NotRevalidatable(_))
                | (Measurement::Algorithm(_), Reconciliation::Published(_))
        ));
        if measurement != Measurement::None {
            let foreign = fixture.path().join("equal-foreign");
            fs::write(&foreign, b"abc").unwrap();
            fs::rename(foreign, fixture.output()).unwrap();
            assert!(matches!(
                state.reconcile().await.unwrap(),
                Reconciliation::Conflict(_)
            ));
            fs::remove_file(fixture.output()).unwrap();
            assert!(matches!(
                state.reconcile().await.unwrap(),
                Reconciliation::PublicationUnknown(_)
            ));
            assert!(!fixture.output().exists());
        }
    }
}

#[tokio::test]
async fn restart_creates_a_distinct_attempt_without_rewriting_old_control_or_unknown_extents() {
    let fixture = Fixture::new();
    let old_plan = Plan::new(&fixture.output(), requirement(b"abc")).unwrap();
    let old_state = StateRef::clone(old_plan.state());
    let old = Native::acquire(old_plan).await.unwrap();
    old.write(ByteCount(0), Bytes::from_static(b"a"))
        .await
        .unwrap();
    old.checkpoint(Partial {
        write_extent: WriteExtent::Unconfirmed,
        ..Partial::default()
    })
    .await
    .unwrap();
    let old_control = fs::read(old_state.path().join("state")).unwrap();
    let old_inode = fs::metadata(old_state.path().join("data")).unwrap().ino();
    let mut plan = Plan::new(&fixture.output(), requirement(b"abc")).unwrap();
    plan.restart = Some(StateRef::clone(&old_state));
    assert!(matches!(
        Native::acquire(plan).await.err().unwrap().error,
        NativeError::Busy
    ));
    drop(old);
    let mut plan = Plan::new(&fixture.output(), requirement(b"abc")).unwrap();
    plan.restart = Some(StateRef::clone(&old_state));
    let new_state = StateRef::clone(plan.state());
    let new = Native::acquire(plan).await.unwrap();
    assert_ne!(old_state, new_state);
    assert_eq!(
        fs::read(old_state.path().join("state")).unwrap(),
        old_control
    );
    assert_eq!(
        fs::metadata(old_state.path().join("data")).unwrap().ino(),
        old_inode
    );
    assert_eq!(fs::read(old_state.path().join("data")).unwrap(), b"a");
    assert_eq!(
        fs::metadata(new_state.path().join("data")).unwrap().len(),
        0
    );
    drop(new);
}

#[tokio::test]
async fn restart_refuses_closed_published_legacy_wrong_binding_and_changed_data_witnesses() {
    for scenario in 0..9 {
        let fixture = Fixture::new();
        let old_plan = Plan::new(&fixture.output(), requirement(b"abc")).unwrap();
        let state = StateRef::clone(old_plan.state());
        let old = Native::acquire(old_plan).await.unwrap();
        old.write(ByteCount(0), Bytes::from_static(b"abc"))
            .await
            .unwrap();
        match scenario {
            0 => old.seal(ByteCount(3)).await.unwrap(),
            1 => {
                old.seal(ByteCount(3)).await.unwrap();
                old.publish(complete(b"abc")).await.unwrap();
            }
            3 => {
                fs::rename(state.path().join("data"), state.path().join("old-data")).unwrap();
                fs::write(state.path().join("data"), b"abc").unwrap();
            }
            4 => {
                fs::hard_link(state.path().join("data"), fixture.path().join("alias")).unwrap();
            }
            7 => {
                fs::set_permissions(state.path().join("data"), fs::Permissions::from_mode(0o644))
                    .unwrap();
            }
            _ => {}
        }
        drop(old);
        if scenario == 2 {
            // Construct a delivered V2 fixture after releasing its owner. The production checkpoint
            // path correctly refuses changing any frozen binding, including receiving authority.
            let control = state.path().join("state");
            let mut record =
                crate::transfer::record::Record::decode(&fs::read(&control).unwrap()).unwrap();
            record.binding.receiving_authority = false;
            let bytes = record.encode();
            assert_eq!(&bytes[..8], b"XGETNF02");
            fs::write(control, bytes).unwrap();
        }
        let destination = if scenario == 5 {
            fixture.path().join("different")
        } else {
            fixture.output()
        };
        let checks = requirement(if scenario == 6 { b"different" } else { b"abc" });
        let mut plan = Plan::new(&destination, checks).unwrap();
        let rejected = StateRef::clone(plan.state());
        plan.restart = Some(StateRef::clone(&state));
        let result = if scenario == 8 {
            Native::acquire_reader(
                plan,
                super::Session {
                    size: ByteCount(3),
                    representation: "bytes".to_owned(),
                    version: Some(b"v1".to_vec()),
                },
                None,
                None,
            )
            .await
        } else {
            Native::acquire(plan).await
        };
        assert!(matches!(
            result.err().unwrap().error,
            NativeError::InvalidState
        ));
        assert!(!rejected.path().exists());
    }
}

#[tokio::test]
async fn dropped_replacement_commit_keeps_descriptors_and_leases_until_guard_cleanup() {
    let fixture = Fixture::new();
    fs::write(fixture.output(), b"sentinel").unwrap();
    let native = sealed(&fixture).await;
    // Alias an ancestor, not the final directory component refused by O_NOFOLLOW.
    let alias_link = fixture.path().join("parent-alias");
    symlink(fixture.path().parent().unwrap(), &alias_link).unwrap();
    let alias = alias_link.join(fixture.path().file_name().unwrap());
    let released = Arc::clone(&native.hooks().released);
    let held = native.hooks().commit.install();
    let mut operation = Box::pin(native.publish(complete(b"abc")));
    tokio::select! {
        result = &mut operation => panic!("commit must be held: {result:?}"),
        () = held.entered.notified() => {}
    }
    drop(operation);
    drop(native);
    assert!(matches!(
        DirectoryLease::acquire(fixture.path()),
        Err(NativeError::Busy)
    ));
    super::tests::probe(&alias, "busy");
    held.release();
    released.notified().await;
    super::tests::probe(&alias, "free");
    assert_eq!(fs::read(fixture.output()).unwrap(), b"abc");
    let lease = DirectoryLease::acquire(fixture.path()).unwrap();
    drop(lease);
}

#[test]
fn queued_replacement_after_waiter_drop_keeps_the_real_destination_lease_until_cleanup() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let fixture = Fixture::new();
        fs::write(fixture.output(), b"sentinel").unwrap();
        let native = sealed(&fixture).await;
        // Alias an ancestor, not the final directory component refused by O_NOFOLLOW.
        let alias_link = fixture.path().join("parent-alias");
        symlink(fixture.path().parent().unwrap(), &alias_link).unwrap();
        let alias = alias_link.join(fixture.path().file_name().unwrap());
        let released = Arc::clone(&native.hooks().released);
        let (entered, occupied) = tokio::sync::oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel::<()>();
        let blocker = tokio::task::spawn_blocking(move || {
            entered.send(()).unwrap();
            wait.recv().unwrap();
        });
        occupied.await.unwrap();
        let mut publication = Box::pin(native.publish(complete(b"abc")));
        assert!(futures::poll!(publication.as_mut()).is_pending());
        drop(publication);
        drop(native);
        assert!(matches!(
            DirectoryLease::acquire(fixture.path()),
            Err(NativeError::Busy)
        ));
        super::tests::probe(&alias, "busy");
        assert_eq!(fs::read(fixture.output()).unwrap(), b"sentinel");
        release.send(()).unwrap();
        blocker.await.unwrap();
        released.notified().await;
        super::tests::probe(&alias, "free");
        let lease = DirectoryLease::acquire(fixture.path()).unwrap();
        assert_eq!(fs::read(fixture.output()).unwrap(), b"abc");
        drop(lease);
    });
}

#[tokio::test]
async fn abrupt_drop_during_physical_v4_seal_retains_all_real_leases_and_closed_intent() {
    let fixture = Fixture::new();
    fs::write(fixture.output(), b"sentinel").unwrap();
    let inode = fs::metadata(fixture.output()).unwrap().ino();
    // Alias an ancestor, not the final directory component refused by O_NOFOLLOW.
    let alias_link = fixture.path().join("parent-alias");
    symlink(fixture.path().parent().unwrap(), &alias_link).unwrap();
    let alias = alias_link.join(fixture.path().file_name().unwrap());
    let store = fixture.path().join("owned-store");
    fs::create_dir(&store).unwrap();
    fs::set_permissions(&store, fs::Permissions::from_mode(0o700)).unwrap();
    let registration = Native::lease_directory(store).await.unwrap();
    let plan = replacement(&fixture);
    let state = StateRef::clone(plan.state());
    let native = Native::acquire_registered(plan, Some(registration))
        .await
        .unwrap();
    native
        .write(ByteCount(0), Bytes::from_static(b"abc"))
        .await
        .unwrap();
    let released = Arc::clone(&native.hooks().released);
    let held = native.hooks().seal.install();
    let mut sealing = Box::pin(native.seal(ByteCount(3)));
    tokio::select! {
        result = &mut sealing => panic!("physical seal must remain held: {result:?}"),
        () = held.entered.notified() => {}
    }
    // Abruptly drop the waiter, without a cooperative stop or a failure-checkpoint path.
    drop(sealing);
    drop(native);
    let control = fs::read(state.path().join("state")).unwrap();
    let data = fs::read(state.path().join("data")).unwrap();
    let record = crate::transfer::record::Record::decode(&control).unwrap();
    assert_eq!(&control[..8], b"XGETNF04");
    assert_eq!(record.status, crate::transfer::record::Status::Closed);
    assert!(record.binding.receiving_authority);
    let workspace_alias = alias.join(state.path().file_name().unwrap());
    super::tests::probe(&alias, "busy");
    super::tests::probe(&workspace_alias, "workspace-busy");
    super::tests::probe(&alias.join("owned-store"), "owned-busy");
    assert_eq!(fs::metadata(fixture.output()).unwrap().ino(), inode);
    assert_eq!(fs::read(fixture.output()).unwrap(), b"sentinel");
    held.release();
    // Notification follows final guard/descriptors cleanup, not an Arc weak-count observation.
    released.notified().await;
    super::tests::probe(&alias, "free");
    super::tests::probe(&workspace_alias, "workspace-free");
    super::tests::probe(&alias.join("owned-store"), "owned-free");
    assert_eq!(fs::read(state.path().join("state")).unwrap(), control);
    assert_eq!(fs::read(state.path().join("data")).unwrap(), data);
    assert_eq!(fs::metadata(fixture.output()).unwrap().ino(), inode);
    let mut retry = replacement(&fixture);
    let rejected = StateRef::clone(retry.state());
    retry.restart = Some(StateRef::clone(&state));
    assert!(matches!(
        Native::acquire(retry).await.err().unwrap().error,
        NativeError::InvalidState
    ));
    assert!(!rejected.path().exists());
    assert_eq!(fs::read(state.path().join("state")).unwrap(), control);
    assert_eq!(fs::read(state.path().join("data")).unwrap(), data);
    assert_eq!(fs::read(fixture.output()).unwrap(), b"sentinel");
    assert_eq!(fs::metadata(fixture.output()).unwrap().ino(), inode);
}

#[test]
fn abrupt_drop_of_a_queued_v4_seal_retains_authority_until_closed_intent_and_cleanup() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let fixture = Fixture::new();
        fs::write(fixture.output(), b"sentinel").unwrap();
        let inode = fs::metadata(fixture.output()).unwrap().ino();
        // Alias an ancestor, not the final directory component refused by O_NOFOLLOW.
        let alias_link = fixture.path().join("parent-alias");
        symlink(fixture.path().parent().unwrap(), &alias_link).unwrap();
        let alias = alias_link.join(fixture.path().file_name().unwrap());
        let plan = replacement(&fixture);
        let state = StateRef::clone(plan.state());
        let native = Native::acquire(plan).await.unwrap();
        native
            .write(ByteCount(0), Bytes::from_static(b"abc"))
            .await
            .unwrap();
        let data = fs::read(state.path().join("data")).unwrap();
        let released = Arc::clone(&native.hooks().released);
        let (entered, occupied) = tokio::sync::oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel::<()>();
        let blocker = tokio::task::spawn_blocking(move || {
            entered.send(()).unwrap();
            wait.recv().unwrap();
        });
        occupied.await.unwrap();
        let mut sealing = Box::pin(native.seal(ByteCount(3)));
        assert!(futures::poll!(sealing.as_mut()).is_pending());
        drop(sealing);
        drop(native);
        super::tests::probe(&alias, "busy");
        assert_eq!(fs::metadata(fixture.output()).unwrap().ino(), inode);
        assert_eq!(fs::read(fixture.output()).unwrap(), b"sentinel");
        release.send(()).unwrap();
        blocker.await.unwrap();
        released.notified().await;
        super::tests::probe(&alias, "free");
        let control = fs::read(state.path().join("state")).unwrap();
        let record = crate::transfer::record::Record::decode(&control).unwrap();
        assert_eq!(&control[..8], b"XGETNF04");
        assert_eq!(record.status, crate::transfer::record::Status::Closed);
        assert_eq!(fs::read(state.path().join("data")).unwrap(), data);
        let mut retry = replacement(&fixture);
        retry.restart = Some(StateRef::clone(&state));
        assert!(matches!(
            Native::acquire(retry).await.err().unwrap().error,
            NativeError::InvalidState
        ));
        assert_eq!(fs::read(state.path().join("state")).unwrap(), control);
        assert_eq!(fs::read(state.path().join("data")).unwrap(), data);
        assert_eq!(fs::metadata(fixture.output()).unwrap().ino(), inode);
        assert_eq!(fs::read(fixture.output()).unwrap(), b"sentinel");
    });
}

#[tokio::test]
async fn overwrite_captured_absence_never_upgrades_to_replacement_when_a_foreign_inode_appears() {
    let fixture = Fixture::new();
    let plan = replacement(&fixture);
    let state = StateRef::clone(plan.state());
    let native = Native::acquire(plan).await.unwrap();
    assert!(native.0.incumbent.is_none());
    assert_eq!(native.0.binding.incumbent, None);
    native
        .write(ByteCount(0), Bytes::from_static(b"abc"))
        .await
        .unwrap();
    native.seal(ByteCount(3)).await.unwrap();
    let held = native.hooks().commit.install();
    let mut publication = Box::pin(native.publish(complete(b"abc")));
    tokio::select! {
        result = &mut publication => panic!("commit must remain held: {result:?}"),
        () = held.entered.notified() => {}
    }
    fs::write(fixture.output(), b"foreign").unwrap();
    let inode = fs::metadata(fixture.output()).unwrap().ino();
    held.release();
    assert!(matches!(
        publication.await,
        Err(NativeError::DestinationExists)
    ));
    assert_eq!(fs::read(fixture.output()).unwrap(), b"foreign");
    assert_eq!(fs::metadata(fixture.output()).unwrap().ino(), inode);
    assert!(!state.path().join("commit").exists());
    assert_eq!(fs::read(state.path().join("data")).unwrap(), b"abc");
    let record = native
        .0
        .workspace
        .read_record(crate::transfer::Phase::Reconcile)
        .unwrap();
    assert_eq!(record.binding.incumbent, None);
    assert_eq!(record.status, crate::transfer::record::Status::Publishing);
}

#[tokio::test]
async fn stopped_sealing_does_not_replace_and_retained_closed_data_cannot_restart() {
    let fixture = Fixture::new();
    fs::write(fixture.output(), b"sentinel").unwrap();
    let inode = fs::metadata(fixture.output()).unwrap().ino();
    let plan = replacement(&fixture);
    let state = StateRef::clone(plan.state());
    let native = Native::acquire(plan).await.unwrap();
    let held = native.hooks().seal.install();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let mut execution = Box::pin(
        Run {
            native,
            state: StateRef::clone(&state),
            length: Length::Known(ByteCount(3)),
        }
        .execute(
            stream::iter([Ok::<_, Infallible>(Bytes::from_static(b"abc"))]),
            async {
                let _ = stopped.await;
            },
        ),
    );
    tokio::select! {
        result = &mut execution => panic!("seal must be held: {result:?}"),
        () = held.entered.notified() => {}
    }
    stop.send(()).unwrap();
    held.release();
    assert!(matches!(
        execution.as_mut().await.unwrap_err().kind,
        FailureKind::Stopped
    ));
    drop(execution);
    assert_eq!(fs::metadata(fixture.output()).unwrap().ino(), inode);
    assert_eq!(fs::read(fixture.output()).unwrap(), b"sentinel");
    let mut plan = replacement(&fixture);
    plan.restart = Some(state);
    assert!(matches!(
        Native::acquire(plan).await.err().unwrap().error,
        NativeError::InvalidState
    ));
}

#[test]
fn below_admission_atomic_replacing_rename_is_not_an_incumbent_inode_cas() {
    let fixture = Fixture::new();
    fs::write(fixture.output(), b"sentinel").unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let workers: Vec<_> = [b"abc".as_slice(), b"xyz".as_slice()]
        .into_iter()
        .enumerate()
        .map(|(index, bytes)| {
            let stage = fixture.path().join(format!("stage-{index}"));
            fs::create_dir(&stage).unwrap();
            fs::write(stage.join("data"), bytes).unwrap();
            fs::hard_link(stage.join("data"), stage.join("commit")).unwrap();
            let source = Directory(fs::File::open(&stage).unwrap());
            let parent = Directory(fs::File::open(fixture.path()).unwrap());
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                // This deliberately bypasses cooperating admission and incumbent validation. Both
                // atomic renames may succeed; the real API makes no arbitrary inode-CAS promise.
                parent.replace(&source, c"commit", c"output").unwrap();
            })
        })
        .collect();
    barrier.wait();
    for worker in workers {
        worker.join().unwrap();
    }
    let bytes = fs::read(fixture.output()).unwrap();
    assert!(bytes == b"abc" || bytes == b"xyz");
    let inode = fs::metadata(fixture.output()).unwrap().ino();
    assert!((0..2).any(|index| {
        fs::metadata(fixture.path().join(format!("stage-{index}/data")))
            .unwrap()
            .ino()
            == inode
    }));
}

#[tokio::test]
async fn below_admission_a_second_captured_incumbent_cannot_replace_the_first_new_inode() {
    let fixture = Fixture::new();
    fs::write(fixture.output(), b"sentinel").unwrap();
    let directory = DirectoryLease::acquire(fixture.path()).unwrap();
    let plan = replacement(&fixture);
    let first = super::Incumbent::capture(&directory.parent, &plan)
        .unwrap()
        .unwrap();
    let second = super::Incumbent::capture(&directory.parent, &plan)
        .unwrap()
        .unwrap();
    let replacement_path = fixture.path().join("replacement");
    fs::write(&replacement_path, b"abc").unwrap();
    let descriptor = Directory(fs::File::open(fixture.path()).unwrap());
    first.validate(&descriptor, c"output").unwrap();
    descriptor
        .replace(&descriptor, c"replacement", c"output")
        .unwrap();
    let inode = fs::metadata(fixture.output()).unwrap().ino();
    assert!(matches!(
        second.validate(&descriptor, c"output"),
        Err(NativeError::InvalidState)
    ));
    assert_eq!(fs::metadata(fixture.output()).unwrap().ino(), inode);
    assert_eq!(fs::read(fixture.output()).unwrap(), b"abc");
}
