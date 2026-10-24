#![allow(clippy::unwrap_used)]

use core::cell::Cell;
use std::rc::Rc;

use super::{Observation, Observer, ProgressFacts};
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
    observation.update(&partial, ByteCount(40));
    let facts = latest.get().unwrap();
    assert_eq!(facts.received(), ByteCount(80));
    assert_eq!(facts.written(), ByteCount(70));
    assert_eq!(facts.hashed(), ByteCount(60));
    assert_eq!(facts.restored(), ByteCount(40));
    assert_eq!(facts.attempt_received(), ByteCount(40));
    assert_eq!(facts.attempt_written(), ByteCount(30));
    assert_eq!(facts.write_extent(), WriteExtent::Unconfirmed);
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
    );
    let facts = latest.get().unwrap();
    assert_eq!(facts.length(), Length::Unknown);
    assert_eq!(facts.attempt_received(), ByteCount(u64::MAX));
    assert_eq!(facts.attempt_written(), ByteCount(u64::MAX));
    assert_eq!(facts.hashed(), ByteCount(0));
    assert_eq!(facts.restored(), ByteCount(0));
    assert_eq!(facts.write_extent(), WriteExtent::Confirmed);
}
