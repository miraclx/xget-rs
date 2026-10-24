#![allow(clippy::expect_used, clippy::unwrap_used)]

use core::cell::Cell;
use core::convert::Infallible;
use core::future::{Future as _, pending as pending_stop};
use core::pin::{Pin, pin};
use core::task::{Context, Poll};
use std::rc::Rc;

use bytes::Bytes;
use futures::task::noop_waker_ref;
use futures::{Stream, stream};

use super::{Native, Observation, Run};
use crate::transfer::tests::Fixture;
use crate::transfer::{ByteCount, FailureKind, Length, Partial, StateRef, Transfer};

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
