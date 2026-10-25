use super::reader::Window;
use super::{ByteCount, FailureKind, Length, Partial, WriteExtent};

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
    available: ByteCount,
    reception: Reception,
    windows: [Option<WindowFacts>; 4],
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

    /// Greatest contiguous acknowledged settled payload prefix, as an exclusive byte offset.
    ///
    /// Includes a validated restored prefix, independently of optional hashing. Tail writes beyond
    /// a hole and unknown write extents do not advance it. This is an observation on the consuming
    /// task, not a path, read lease, integrity match, EOF, publication or permission to reuse bytes.
    /// It does not promise safety against concurrent owner mutation or power loss.
    pub fn available(self) -> ByteCount {
        self.available
    }

    /// Observed reception state, separate from checks, sealing and publication.
    /// A fully received byte count can still be Receiving, Failed or Stopped.
    pub fn reception(self) -> Reception {
        self.reception
    }

    /// At most four actual current-attempt reader windows, in increasing offset order.
    ///
    /// Whole ordered bodies have no windows. Restored bytes are reported by [`Self::restored`],
    /// never invented as a new window. Each snapshot is updated only when the caller task consumes
    /// a source/write step; physical work can be ahead of the last acknowledged snapshot.
    pub fn windows(self) -> impl Iterator<Item = WindowFacts> {
        self.windows.into_iter().flatten()
    }
}

/// Observed reception, not a transfer outcome or a successfully paused state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reception {
    /// No body step has been observed yet. A zero-size reader opens no windows and stays Pending.
    Pending,
    /// Reception is ongoing; reaching the requested byte count does not change this state.
    Receiving,
    /// The ordered body, or every requested reader window, reached actual EOF with valid length.
    /// Required checks and native publication may still fail afterwards.
    CleanEof,
    /// Reception failed, including a source, response, length or physical-write failure.
    /// Foreign causes and remote diagnostics are deliberately absent from these facts.
    Failed,
    /// Reception was cooperatively stopped, or this window was cancelled after another failed.
    /// Dropping execution has no promised final snapshot.
    Stopped,
}

/// One actual half-open reader window's current-attempt observations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WindowFacts {
    window: Window,
    received: ByteCount,
    written: ByteCount,
    write_extent: WriteExtent,
    reception: Reception,
}

impl WindowFacts {
    /// Inclusive requested start and exclusive requested end, not a division of aggregate counts.
    pub fn window(self) -> Window {
        self.window
    }

    /// Successful body-chunk bytes observed in this attempt, including rejected overlong chunks.
    /// This can exceed the requested window length on failure; it is not settled local coverage.
    /// Count overflow fails reception and retains the last representable exact count.
    pub fn received(self) -> ByteCount {
        self.received
    }

    /// Acknowledged settled prefix length within this window, excluding any restored bytes.
    /// Add it to [`Window::start`] for the exclusive settled offset. Unknown extents are excluded.
    pub fn written(self) -> ByteCount {
        self.written
    }

    /// Whether this window's settled physical writes all returned their exact accepted prefix.
    pub fn write_extent(self) -> WriteExtent {
        self.write_extent
    }

    /// Actual EOF, failure and cancellation are distinct from receiving all requested bytes.
    pub fn reception(self) -> Reception {
        self.reception
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
                available: ByteCount(0),
                reception: Reception::Pending,
                windows: [None; 4],
            },
        };
        observation.emit();
        observation
    }

    pub(super) fn update(&mut self, partial: &Partial, restored: ByteCount, available: ByteCount) {
        self.facts.received = partial.received;
        self.facts.written = partial.written;
        self.facts.hashed = partial.hashed;
        self.facts.restored = restored;
        // Restored counts enter Partial only after prefix revalidation, before source admission.
        self.facts.attempt_received = ByteCount(partial.received.0 - restored.0);
        self.facts.attempt_written = ByteCount(partial.written.0 - restored.0);
        self.facts.write_extent = partial.write_extent;
        self.facts.available = available;
        self.emit();
    }

    pub(super) fn receiving(&mut self) {
        self.facts.reception = Reception::Receiving;
    }

    pub(super) fn ranges(&mut self, ranges: &[Window]) {
        // The coordinator's partition is bounded independently of representation size.
        assert!(
            ranges.len() <= self.facts.windows.len(),
            "reader admits at most four windows"
        );
        for (slot, &window) in self.facts.windows.iter_mut().zip(ranges) {
            *slot = Some(WindowFacts {
                window,
                received: ByteCount(0),
                written: ByteCount(0),
                write_extent: WriteExtent::Confirmed,
                reception: Reception::Pending,
            });
        }
    }

    pub(super) fn range_step(
        &mut self,
        index: usize,
        received: ByteCount,
        end: ByteCount,
        extent: WriteExtent,
        reception: Reception,
    ) {
        let Some(window) = self.facts.windows[index].as_mut() else {
            unreachable!("every reader step belongs to an admitted window");
        };
        // The coordinator terminates on aggregate overflow. Retain the last representable exact
        // window count too, rather than inventing a wrapped or saturated observation.
        if let Some(count) = window.received.0.checked_add(received.0) {
            window.received.0 = count;
        }
        window.written = ByteCount(end.0 - window.window.start().0);
        if extent == WriteExtent::Unconfirmed {
            window.write_extent = extent;
        }
        window.reception = reception;
        if self.facts.reception != Reception::Failed {
            self.facts.reception = match reception {
                Reception::Failed => Reception::Failed,
                Reception::Stopped => Reception::Stopped,
                _ if self
                    .facts
                    .windows
                    .iter()
                    .flatten()
                    .all(|window| window.reception == Reception::CleanEof) =>
                {
                    Reception::CleanEof
                }
                _ if self.facts.reception == Reception::Stopped => Reception::Stopped,
                _ => Reception::Receiving,
            };
        }
    }

    pub(super) fn end_reception<T, E>(&mut self, result: &Result<T, FailureKind<E>>) {
        self.facts.reception = match result {
            Ok(_) if self.facts.reception == Reception::Pending => Reception::Pending,
            Ok(_) => Reception::CleanEof,
            Err(FailureKind::Stopped) => Reception::Stopped,
            Err(_) => Reception::Failed,
        };
        if result.is_err() {
            for window in self.facts.windows.iter_mut().flatten() {
                if matches!(window.reception, Reception::Pending | Reception::Receiving) {
                    // Dropped queued steps and a failed verifier stop unfinished window admission;
                    // neither implies a source error or EOF for those windows.
                    window.reception = Reception::Stopped;
                }
            }
        }
        // The existing outer return snapshot exposes this state. Do not introduce another progress
        // callback here: renderers retain their existing event ordering and sampling opportunities.
    }

    pub(super) fn emit(&mut self) {
        self.observer.observe(self.facts);
    }
}

#[cfg(test)]
#[path = "observation_tests.rs"]
mod tests;
