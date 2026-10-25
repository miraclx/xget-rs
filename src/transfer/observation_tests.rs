#![allow(clippy::unwrap_used)]

use core::cell::Cell;
use std::rc::Rc;

use super::{Observation, Observer, ProgressFacts, Reception};
use crate::transfer::reader::Window;
use crate::transfer::{ByteCount, Length, Partial, WriteExtent};

struct Borrowed<'a>(&'a Rc<Cell<Option<ProgressFacts>>>);

impl Observer for Borrowed<'_> {
    fn observe(&mut self, facts: ProgressFacts) {
        self.0.set(Some(facts));
    }
}

#[test]
fn borrowed_local_observers_distinguish_restored_logical_and_attempt_work() {
    let latest = Rc::new(Cell::new(None));
    let mut observer = Borrowed(&latest);
    let mut observation = Observation::start(&mut observer, Length::Known(ByteCount(100)));
    let initial = latest.get().unwrap();
    assert_eq!(initial.received(), ByteCount(0));
    assert_eq!(initial.length(), Length::Known(ByteCount(100)));
    let partial = Partial {
        received: ByteCount(80),
        written: ByteCount(70),
        hashed: ByteCount(60),
        write_extent: WriteExtent::Unconfirmed,
        ..Partial::default()
    };
    observation.update(&partial, ByteCount(40), ByteCount(50));
    let facts = latest.get().unwrap();
    assert_eq!(facts.received(), ByteCount(80));
    assert_eq!(facts.written(), ByteCount(70));
    assert_eq!(facts.hashed(), ByteCount(60));
    assert_eq!(facts.restored(), ByteCount(40));
    assert_eq!(facts.attempt_received(), ByteCount(40));
    assert_eq!(facts.attempt_written(), ByteCount(30));
    assert_eq!(facts.write_extent(), WriteExtent::Unconfirmed);
    assert_eq!(facts.available(), ByteCount(50));
    assert_eq!(facts.windows().count(), 0);
    observation.emit();
    assert_eq!(latest.get(), Some(facts));
}

#[test]
fn unknown_length_and_disabled_hashing_remain_exact_without_a_hidden_hash() {
    let latest = Rc::new(Cell::new(None));
    let mut observer = Borrowed(&latest);
    let mut observation = Observation::start(&mut observer, Length::Unknown);
    observation.update(
        &Partial {
            received: ByteCount(u64::MAX),
            written: ByteCount(u64::MAX),
            ..Partial::default()
        },
        ByteCount(0),
        ByteCount(u64::MAX),
    );
    let facts = latest.get().unwrap();
    assert_eq!(facts.length(), Length::Unknown);
    assert_eq!(facts.attempt_received(), ByteCount(u64::MAX));
    assert_eq!(facts.attempt_written(), ByteCount(u64::MAX));
    assert_eq!(facts.hashed(), ByteCount(0));
    assert_eq!(facts.restored(), ByteCount(0));
    assert_eq!(facts.write_extent(), WriteExtent::Confirmed);
    assert_eq!(facts.available(), ByteCount(u64::MAX));
    assert_eq!(facts.windows().count(), 0);
}

#[test]
fn window_facts_keep_actual_suffix_bounds_and_distinguish_full_counts_from_eof() {
    let latest = Rc::new(Cell::new(None));
    let mut observer = Borrowed(&latest);
    let mut observation = Observation::start(&mut observer, Length::Known(ByteCount(13)));
    let ranges = [(4, 7), (7, 9), (9, 11), (11, 13)]
        .map(|(start, end)| Window::new(ByteCount(start), ByteCount(end), ByteCount(13)).unwrap());
    observation.ranges(&ranges);
    observation.range_step(
        1,
        ByteCount(2),
        ByteCount(9),
        WriteExtent::Confirmed,
        Reception::Receiving,
    );
    observation.update(
        &Partial {
            received: ByteCount(6),
            written: ByteCount(6),
            hashed: ByteCount(4),
            ..Partial::default()
        },
        ByteCount(4),
        ByteCount(4),
    );
    let facts = latest.get().unwrap();
    let windows: Vec<_> = facts.windows().collect();
    assert_eq!(windows.len(), 4);
    assert_eq!(
        windows
            .iter()
            .map(|facts| facts.window())
            .collect::<Vec<_>>(),
        ranges
    );
    assert_eq!(windows[1].received(), ByteCount(2));
    assert_eq!(windows[1].written(), ByteCount(2));
    assert_eq!(windows[1].reception(), Reception::Receiving);
    assert_eq!(windows[0].reception(), Reception::Pending);
    assert_eq!(facts.reception(), Reception::Receiving);
    assert_eq!(facts.available(), ByteCount(4));
    assert_eq!(facts.attempt_received(), ByteCount(2));
    observation.range_step(
        1,
        ByteCount(0),
        ByteCount(9),
        WriteExtent::Confirmed,
        Reception::CleanEof,
    );
    observation.emit();
    assert_eq!(
        latest.get().unwrap().windows().nth(1).unwrap().reception(),
        Reception::CleanEof
    );
    assert_eq!(latest.get().unwrap().reception(), Reception::Receiving);
}

#[test]
fn window_failure_and_cooperative_stop_never_invent_eof_for_unfinished_windows() {
    use crate::transfer::FailureKind;

    for stopped in [false, true] {
        let latest = Rc::new(Cell::new(None));
        let mut observer = Borrowed(&latest);
        let mut observation = Observation::start(&mut observer, Length::Known(ByteCount(2)));
        observation.ranges(&[
            Window::new(ByteCount(0), ByteCount(1), ByteCount(2)).unwrap(),
            Window::new(ByteCount(1), ByteCount(2), ByteCount(2)).unwrap(),
        ]);
        observation.range_step(
            0,
            ByteCount(1),
            ByteCount(1),
            WriteExtent::Confirmed,
            Reception::CleanEof,
        );
        let result: Result<(), FailureKind<()>> = Err(if stopped {
            FailureKind::Stopped
        } else {
            FailureKind::Body(())
        });
        observation.end_reception(&result);
        observation.emit();
        let facts = latest.get().unwrap();
        assert_eq!(
            facts.reception(),
            if stopped {
                Reception::Stopped
            } else {
                Reception::Failed
            }
        );
        let windows: Vec<_> = facts.windows().collect();
        assert_eq!(windows[0].reception(), Reception::CleanEof);
        assert_eq!(windows[1].reception(), Reception::Stopped);
        assert_eq!(windows[1].written(), ByteCount(0));
    }
}
