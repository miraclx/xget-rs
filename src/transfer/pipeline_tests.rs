#![allow(clippy::expect_used, clippy::unwrap_used)]

use core::cell::{Cell, RefCell};
use core::convert::Infallible;
use core::future::{Future as _, pending as pending_stop};
use core::pin::{Pin, pin};
use core::task::{Context, Poll};
use std::rc::Rc;

use bytes::Bytes;
use futures::task::noop_waker_ref;
use futures::{Stream, stream};

use super::{Native, Observation, Run};
use crate::transfer::observation::{Observer, ProgressFacts, Reception};
use crate::transfer::tests::Fixture;
use crate::transfer::{
    ByteCount, FailureKind, Length, Measurement, Partial, StateRef, Transfer, WriteExtent,
};

struct Snapshots<'a>(&'a RefCell<Vec<ProgressFacts>>);

impl Observer for Snapshots<'_> {
    fn observe(&mut self, facts: ProgressFacts) {
        self.0.borrow_mut().push(facts);
    }
}

#[tokio::test]
async fn ordered_available_excludes_unknown_extents_but_keeps_confirmed_partial_write_ends() {
    use core::sync::atomic::Ordering;

    for lose_result in [false, true] {
        let fixture = Fixture::new();
        let length = Length::Known(ByteCount(3));
        let pending = Transfer::from_body(stream::empty::<Result<Bytes, Infallible>>(), length)
            .measurement(Measurement::None)
            .to_file(fixture.output())
            .unwrap();
        let state = StateRef::clone(pending.state());
        let native = Native::acquire(pending.plan).await.unwrap();
        if lose_result {
            native
                .hooks()
                .lose_write_result
                .store(true, Ordering::Release);
        } else {
            native
                .hooks()
                .fail_write_prefix
                .store(true, Ordering::Release);
        }
        let facts = RefCell::new(Vec::new());
        let mut observer = Snapshots(&facts);
        let mut observation = Observation::start(&mut observer, length);
        let failure = Run {
            native,
            state,
            length,
        }
        .execute_observed(
            stream::iter([Ok::<_, Infallible>(Bytes::from_static(b"abc"))]),
            pending_stop(),
            &mut observation,
        )
        .await
        .unwrap_err();
        observation.emit();
        let terminal = *facts.borrow().last().unwrap();
        assert_eq!(
            terminal.available(),
            if lose_result {
                ByteCount(0)
            } else {
                ByteCount(1)
            }
        );
        assert_eq!(terminal.available(), failure.partial.written);
        assert_eq!(
            terminal.write_extent(),
            if lose_result {
                WriteExtent::Unconfirmed
            } else {
                WriteExtent::Confirmed
            }
        );
        assert_eq!(terminal.hashed(), ByteCount(0));
        assert_eq!(terminal.reception(), Reception::Failed);
        assert_eq!(terminal.windows().count(), 0);
        assert!(!fixture.output().exists());
    }
}

#[tokio::test]
async fn all_ordered_bytes_are_available_before_actual_eof_and_a_late_error_still_fails() {
    let fixture = Fixture::new();
    let length = Length::Known(ByteCount(3));
    let pending = Transfer::from_body(stream::empty::<Result<Bytes, &'static str>>(), length)
        .measurement(Measurement::None)
        .to_file(fixture.output())
        .unwrap();
    let state = StateRef::clone(pending.state());
    let native = Native::acquire(pending.plan).await.unwrap();
    let facts = RefCell::new(Vec::new());
    let mut observer = Snapshots(&facts);
    let mut observation = Observation::start(&mut observer, length);
    let failure = Run {
        native,
        state,
        length,
    }
    .execute_observed(
        stream::iter([Ok(Bytes::from_static(b"abc")), Err("late source failure")]),
        pending_stop(),
        &mut observation,
    )
    .await
    .unwrap_err();
    observation.emit();
    assert!(matches!(
        failure.kind,
        FailureKind::Body("late source failure")
    ));
    let facts = facts.borrow();
    assert!(facts.iter().any(
        |facts| facts.available() == ByteCount(3) && facts.reception() == Reception::Receiving
    ));
    assert!(
        facts
            .iter()
            .all(|facts| facts.reception() != Reception::CleanEof)
    );
    let terminal = *facts.last().unwrap();
    assert_eq!(terminal.available(), ByteCount(3));
    assert_eq!(terminal.reception(), Reception::Failed);
    assert_eq!(terminal.hashed(), ByteCount(0));
    assert!(!fixture.output().exists());
}

struct EmptyBody {
    polls: Rc<Cell<usize>>,
    dropped: Rc<Cell<bool>>,
}

impl Stream for EmptyBody {
    type Item = Result<Bytes, Infallible>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.polls.set(self.polls.get() + 1);
        // Cap the fixture itself: a timeout cannot preempt an unbounded single poll.
        if self.polls.get() <= 256 {
            Poll::Ready(Some(Ok(Bytes::new())))
        } else {
            Poll::Pending
        }
    }
}

impl Drop for EmptyBody {
    fn drop(&mut self) {
        self.dropped.set(true);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn ready_empty_native_body_returns_control_after_a_bounded_source_poll_count() {
    for length in [Length::Unknown, Length::Known(ByteCount(0))] {
        let fixture = Fixture::new();
        let pending = Transfer::from_body(stream::empty::<Result<Bytes, Infallible>>(), length)
            .to_file(fixture.output())
            .unwrap();
        let state = StateRef::clone(pending.state());
        let native = Native::acquire(pending.plan).await.unwrap();
        let run = Run {
            native,
            state,
            length,
        };
        let polls = Rc::new(Cell::new(0));
        let dropped = Rc::new(Cell::new(false));
        let body = EmptyBody {
            polls: Rc::clone(&polls),
            dropped: Rc::clone(&dropped),
        };
        let mut stop = pin!(pending_stop::<()>());
        let mut partial = Partial::default();
        let mut observer = ();
        let mut observation = Observation::start(&mut observer, length);
        let mut receiving =
            Box::pin(run.receive(body, stop.as_mut(), &mut partial, &mut observation));
        let mut context = Context::from_waker(noop_waker_ref());
        assert!(receiving.as_mut().poll(&mut context).is_pending());
        let count = polls.get();
        drop(receiving);
        assert!(dropped.get(), "dropping reception must destroy its source");
        assert!(
            count > 0 && count <= 4,
            "source polled {count} times before yielding"
        );
        assert_eq!(partial.received, ByteCount(0));
        assert_eq!(partial.written, ByteCount(0));
        assert!(!fixture.output().exists());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn ready_empty_native_body_allows_a_sibling_stop_producer_to_run() {
    let fixture = Fixture::new();
    let length = Length::Unknown;
    let pending = Transfer::from_body(stream::empty::<Result<Bytes, Infallible>>(), length)
        .to_file(fixture.output())
        .unwrap();
    let state = StateRef::clone(pending.state());
    let native = Native::acquire(pending.plan).await.unwrap();
    let run = Run {
        native,
        state,
        length,
    };
    let polls = Rc::new(Cell::new(0));
    let dropped = Rc::new(Cell::new(false));
    let body = EmptyBody {
        polls: Rc::clone(&polls),
        dropped: Rc::clone(&dropped),
    };
    let (send, receive) = tokio::sync::oneshot::channel();
    let execute = run.execute(body, async {
        let _ = receive.await;
    });
    let producer = async {
        tokio::task::yield_now().await;
        send.send(()).unwrap();
    };
    let (result, ()) = tokio::join!(execute, producer);
    let failure = result.expect_err("cooperative stop must not publish an endless empty body");
    assert!(matches!(failure.kind, FailureKind::Stopped));
    assert!(
        polls.get() > 0 && polls.get() <= 4,
        "empty body starved the sibling: {} polls",
        polls.get()
    );
    assert!(dropped.get());
    assert_eq!(failure.partial.received, ByteCount(0));
    assert_eq!(failure.partial.written, ByteCount(0));
    assert!(!fixture.output().exists());
}
