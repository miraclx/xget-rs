use super::{ByteCount, Length, Partial, WriteExtent};

/// Immutable byte-work facts, not completion, publication or authenticity evidence.
///
/// Logical counts include a validated restored prefix. Current-attempt counts exclude it.
/// Hashing counts payload bytes once across all active algorithms and does not assert a matched
/// requirement. An unconfirmed write extent makes acknowledged writes a lower bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProgressFacts {
    length: Length,
    received: ByteCount,
    written: ByteCount,
    hashed: ByteCount,
    restored: ByteCount,
    attempt_received: ByteCount,
    attempt_written: ByteCount,
    write_extent: WriteExtent,
}

impl ProgressFacts {
    /// Declared representation length. Reaching it does not establish actual EOF.
    pub fn length(self) -> Length {
        self.length
    }

    /// Logical received source bytes, including a validated restored prefix and rejected overlong chunks.
    pub fn received(self) -> ByteCount {
        self.received
    }

    /// Logical acknowledged payload writes, including a validated restored prefix.
    pub fn written(self) -> ByteCount {
        self.written
    }

    /// Logical contiguous bytes fed to active checks, including prefix revalidation.
    /// Zero when there are no required checks and measurement is disabled.
    pub fn hashed(self) -> ByteCount {
        self.hashed
    }

    /// Previously acknowledged bytes validated against the recorded strong proper-prefix anchor.
    pub fn restored(self) -> ByteCount {
        self.restored
    }

    /// Bytes admitted from successful source chunks in this attempt, excluding restored bytes.
    pub fn attempt_received(self) -> ByteCount {
        self.attempt_received
    }

    /// Payload bytes acknowledged by physical writes in this attempt, excluding restored bytes.
    pub fn attempt_written(self) -> ByteCount {
        self.attempt_written
    }

    /// Whether acknowledged counts describe every settled write's extent.
    pub fn write_extent(self) -> WriteExtent {
        self.write_extent
    }
}

/// A synchronous, borrowed caller-task hook. Implementations should return promptly.
/// No callback runs in a physical worker or after execution is dropped. Facts alone do not
/// announce a file/end event; orchestration and terminal outcomes belong to the caller.
/// Panics unwind the consuming future, with the same physical-work ownership rules as dropping it.
pub(crate) trait Observer {
    /// Observe a small copy of current facts. Duplicate snapshots are permitted.
    fn observe(&mut self, facts: ProgressFacts);
}

impl Observer for () {
    fn observe(&mut self, _: ProgressFacts) {}
}

// This value stays on the consuming task, never in a physical-operation registration.
pub(super) struct Observation<'observer, O> {
    observer: &'observer mut O,
    facts: ProgressFacts,
}

impl<'observer, O: Observer> Observation<'observer, O> {
    pub(super) fn start(observer: &'observer mut O, length: Length) -> Self {
        let mut observation = Self {
            observer,
            facts: ProgressFacts {
                length,
                received: ByteCount(0),
                written: ByteCount(0),
                hashed: ByteCount(0),
                restored: ByteCount(0),
                attempt_received: ByteCount(0),
                attempt_written: ByteCount(0),
                write_extent: WriteExtent::Confirmed,
            },
        };
        observation.emit();
        observation
    }

    pub(super) fn update(&mut self, partial: &Partial, restored: ByteCount) {
        self.facts = ProgressFacts {
            length: self.facts.length,
            received: partial.received,
            written: partial.written,
            hashed: partial.hashed,
            restored,
            // Restored counts enter Partial only after prefix revalidation, before source admission.
            attempt_received: ByteCount(partial.received.0 - restored.0),
            attempt_written: ByteCount(partial.written.0 - restored.0),
            write_extent: partial.write_extent,
        };
        self.emit();
    }

    pub(super) fn emit(&mut self) {
        self.observer.observe(self.facts);
    }
}

#[cfg(test)]
#[path = "observation_tests.rs"]
mod tests;
