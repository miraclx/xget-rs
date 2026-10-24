//! Caller-task observation of common work with fixed-unit machine facts.

use core::time::Duration;
use std::io::IsTerminal as _;
use std::time::Instant;

use clap::Args;
use serde_json::{Value, json};
use xget::client::{BindingKey, Event, Observer, ProgressFacts};
use xget::transfer::{ByteCount, Length, WriteExtent};

use crate::display::DisplayFormat;
use crate::failure::{Diagnostic, Failure};
use crate::output::{Channel, Format, ItemId, Output, RunId};

mod bar;

use bar::TerminalBar;

/// Front-door presentation policy. These are not source concurrency or core timing knobs.
#[derive(Args, Clone, Copy, Default)]
pub(crate) struct Presentation {
    /// Progress: auto (stderr bar on a terminal, complete plain snapshots otherwise), bar, plain,
    /// versioned json, or none. None suppresses progress, not final results or failures.
    #[arg(long, value_enum, default_value_t = Mode::Auto, conflicts_with = "json")]
    progress: Mode,
    /// Versioned events on stderr, equivalent to --progress json.
    #[arg(long)]
    json: bool,
    /// Human speed in bits/s only; machine counters and rates remain bytes and bytes/s.
    #[arg(long)]
    bits: bool,
    /// Raw human byte counts and bytes/s; takes precedence over --bits.
    #[arg(long)]
    raw_sizes: bool,
}

#[derive(Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
enum Mode {
    #[default]
    Auto,
    Bar,
    Plain,
    Json,
    None,
}

impl Presentation {
    pub(crate) fn output(self, run: RunId) -> Output {
        let format = if self.json || self.progress == Mode::Json {
            Format::Json
        } else {
            Format::Plain
        };
        Output::new(run, format, Channel::Diagnostic)
    }

    fn resolved(self, terminal: bool) -> Mode {
        if self.json {
            return Mode::Json;
        }
        match (self.progress, terminal) {
            (Mode::Auto | Mode::Bar, false) => Mode::Plain,
            (Mode::Auto, true) => Mode::Bar,
            (mode, _) => mode,
        }
    }

    fn units(self) -> DisplayFormat {
        if self.raw_sizes {
            DisplayFormat::RawBytes
        } else if self.bits {
            DisplayFormat::HumanBits
        } else {
            DisplayFormat::HumanBytes
        }
    }
}

/// No renderer task, mutex or callback panic is needed. The first write failure is sticky because
/// Observer is infallible; the caller must consume finish/complete after the library returns.
/// A reporting failure cannot cancel the current core run through this API or undo publication.
pub(crate) struct Reporter<'output> {
    output: &'output Output,
    mode: Mode,
    units: DisplayFormat,
    started: Instant,
    rate: Rate,
    last_render: Option<Duration>,
    bar: Option<TerminalBar>,
    failure: Option<Failure>,
    terminal: Terminal,
}

#[derive(PartialEq, Eq)]
enum Terminal {
    Pending,
    Complete,
    Failed,
}

impl<'output> Reporter<'output> {
    pub(crate) fn new(output: &'output Output, presentation: Presentation) -> Self {
        let mode = presentation.resolved(std::io::stderr().is_terminal());
        Self {
            output,
            mode,
            units: presentation.units(),
            started: Instant::now(),
            rate: Rate::default(),
            last_render: None,
            bar: (mode == Mode::Bar).then(TerminalBar::new),
            failure: None,
            terminal: Terminal::Pending,
        }
    }

    pub(crate) fn finish(mut self) -> Result<(), Failure> {
        let cleared = self.clear_bar();
        self.record(cleared);
        match self.failure.take() {
            Some(failure) => Err(failure),
            None => Ok(()),
        }
    }

    pub(crate) fn complete(mut self) -> Result<(), Failure> {
        let cleared = self.clear_bar();
        self.record(cleared);
        if let Some(failure) = self.failure.take() {
            return Err(failure);
        }
        if self.terminal != Terminal::Complete {
            return Err(Failure::runtime(
                "run returned without a complete observer outcome; inspect retained work before continuing",
            ));
        }
        Ok(())
    }

    pub(crate) fn live_progress(
        &mut self,
        length: Length,
        progress: xget::client::LiveProgress,
        accepted: ByteCount,
    ) -> Result<(), Failure> {
        let snapshot = Snapshot {
            length,
            received: progress.received,
            settled: accepted,
            hashed: progress.hashed,
            source: progress.received,
            delivery: Delivery::Live {
                released: progress.released,
            },
        };
        self.progress(None, snapshot);
        match self.failure.take() {
            Some(failure) => Err(failure),
            None => Ok(()),
        }
    }

    fn progress(&mut self, key: Option<BindingKey>, snapshot: Snapshot) {
        if self.failure.is_some() {
            return;
        }
        let elapsed = self.started.elapsed();
        let rate = self.rate.sample(snapshot.source, elapsed);
        // Bounded presentation frequency belongs here, not in the source or native worker. The
        // first snapshot is immediate; terminal publication receipts retain final exact counters.
        let unconfirmed = matches!(
            snapshot.delivery,
            Delivery::Native {
                extent: WriteExtent::Unconfirmed,
                ..
            }
        );
        if !unconfirmed
            && self
                .last_render
                .is_some_and(|last| elapsed.saturating_sub(last) < Duration::from_millis(100))
        {
            return;
        }
        self.last_render = Some(elapsed);
        let result = match self.mode {
            Mode::Json => {
                let facts = snapshot.machine(rate);
                match key {
                    Some(key) => self
                        .output
                        .emit_saved("Progress", key.run_id(), Some(key), facts),
                    None => self.output.emit("Progress", Some(ItemId(0)), facts),
                }
            }
            Mode::Plain | Mode::Auto => self
                .output
                .human_progress(&snapshot.human(self.units, rate)),
            Mode::Bar => match &mut self.bar {
                Some(bar) => bar.render(
                    snapshot.length,
                    snapshot.received,
                    snapshot.settled,
                    &snapshot.human(self.units, rate),
                ),
                None => self
                    .output
                    .human_progress(&snapshot.human(self.units, rate)),
            },
            Mode::None => Ok(()),
        };
        self.record(result);
    }

    fn clear_bar(&mut self) -> Result<(), Failure> {
        match &mut self.bar {
            Some(bar) => bar.clear(),
            None => Ok(()),
        }
    }

    fn record(&mut self, result: Result<(), Failure>) {
        if self.failure.is_none()
            && let Err(failure) = result
        {
            self.failure = Some(failure);
        }
    }

    fn failure_facts(failure: &xget::client::RunFailure) -> Value {
        let diagnostic = Diagnostic::library(&failure.error);
        let mut facts = Output::diagnostic(&diagnostic);
        facts["complete"] = json!(false);
        facts["completed_files"] = json!(failure.report.completed().len());
        facts["state_path"] = failure.report.saved_run().map_or(Value::Null, Output::path);
        facts["pause_confirmed"] = json!(false);
        facts
    }

    fn event(&mut self, event: Event<'_>) -> Result<(), Failure> {
        match event {
            Event::RunStarted {
                run,
                saved_run,
                files,
            } => {
                if self.mode == Mode::None {
                    return Ok(());
                }
                let facts = json!({"state_path": saved_run.map(Output::path), "files": files, "pause_confirmed": false});
                match run {
                    Some(run) => self.output.emit_saved("RunStarted", run, None, facts),
                    None => self.output.emit("RunStarted", None, facts),
                }
            }
            Event::FileStarted { key, planned } => {
                self.clear_bar()?;
                self.rate = Rate::at(self.started.elapsed());
                self.last_render = None;
                if self.mode == Mode::None {
                    return Ok(());
                }
                self.output.emit_saved("FileStarted", key.run_id(), Some(key), json!({
                    "destination": Output::path(planned.destination()),
                    "length_bytes": Output::length(planned.file().length()),
                    "measurement": Output::measurement(planned.measurement()),
                    "required_digests": planned.requirements().map(|required| required.digest.to_string()).collect::<Vec<_>>(),
                }))
            }
            Event::Progress { key, facts } => {
                self.progress(Some(key), Snapshot::native(facts));
                Ok(())
            }
            Event::FileCompleted { key, published } => {
                self.clear_bar()?;
                self.output.emit_saved(
                    "Published",
                    key.run_id(),
                    Some(key),
                    Output::published(published),
                )
            }
            Event::RunCompleted { report } => {
                self.clear_bar()?;
                self.terminal = Terminal::Complete;
                let facts = json!({"complete": true, "completed_files": report.completed().len(), "state_path": report.saved_run().map(Output::path)});
                match report.run_id() {
                    Some(run) => self.output.emit_saved("End", run, None, facts),
                    None => self.output.emit("End", None, facts),
                }
            }
            Event::RunFailed { failure } => {
                self.clear_bar()?;
                self.terminal = Terminal::Failed;
                let facts = Self::failure_facts(failure);
                match failure.report.run_id() {
                    Some(run) => self.output.emit_saved("RunFailed", run, None, facts),
                    None => self.output.emit("RunFailed", None, facts),
                }
            }
        }
    }
}

impl Drop for Reporter<'_> {
    fn drop(&mut self) {
        // Explicit finish/complete handles fallible cleanup on success. Cancellation/body-error
        // paths can only attempt cursor cleanup here, never announce pause or completion.
        if let Some(bar) = &mut self.bar {
            let _ = bar.clear();
        }
    }
}

impl Observer for Reporter<'_> {
    fn observe(&mut self, event: Event<'_>) {
        if let Event::RunStarted { run, saved_run, .. } = &event {
            // Preserve safe state notification even if an earlier diagnostic write already failed.
            self.output.bind_saved(*run, *saved_run);
        }
        if self.failure.is_some() {
            return;
        }
        let result = self.event(event);
        self.record(result);
    }
}

struct Snapshot {
    length: Length,
    received: ByteCount,
    settled: ByteCount,
    hashed: ByteCount,
    source: ByteCount,
    delivery: Delivery,
}

#[derive(Clone, Copy)]
enum Delivery {
    Native {
        restored: ByteCount,
        written: ByteCount,
        extent: WriteExtent,
    },
    Live {
        released: ByteCount,
    },
}

impl Snapshot {
    fn native(facts: ProgressFacts) -> Self {
        Self {
            length: facts.length(),
            received: facts.received(),
            settled: facts.written(),
            hashed: facts.hashed(),
            source: facts.attempt_received(),
            delivery: Delivery::Native {
                restored: facts.restored(),
                written: facts.attempt_written(),
                extent: facts.write_extent(),
            },
        }
    }

    fn eta(&self, BytesPerSecond(rate): BytesPerSecond) -> Option<u64> {
        let Length::Known(total) = self.length else {
            return None;
        };
        if rate == 0 {
            return None;
        }
        Some(total.0.saturating_sub(self.received.0).div_ceil(rate))
    }

    fn machine(&self, rate: BytesPerSecond) -> Value {
        let mut facts = json!({
            "length_bytes": Output::length(self.length),
            "hashed_bytes": self.hashed.0,
            "attempt_received_bytes": self.source.0,
            "rate_bytes_per_second": rate.0,
            "eta_seconds": self.eta(rate),
            "completion_evidence": false,
        });
        match self.delivery {
            Delivery::Native {
                restored,
                written,
                extent,
            } => {
                facts["kind"] = json!("Native");
                facts["logical_received_bytes"] = json!(self.received.0);
                facts["logical_written_bytes"] = json!(self.settled.0);
                facts["restored_bytes"] = json!(restored.0);
                facts["attempt_written_bytes"] = json!(written.0);
                facts["write_extent"] = json!(match extent {
                    WriteExtent::Confirmed => "Confirmed",
                    WriteExtent::Unconfirmed => "Unconfirmed",
                });
            }
            Delivery::Live { released } => {
                facts["kind"] = json!("Live");
                facts["received_bytes"] = json!(self.received.0);
                facts["released_bytes"] = json!(released.0);
                facts["accepted_bytes"] = json!(self.settled.0);
                facts["published"] = json!(false);
            }
        }
        facts
    }

    fn human(&self, units: DisplayFormat, rate: BytesPerSecond) -> String {
        let length = match self.length {
            Length::Known(total) => units.size(total.0),
            Length::Unknown => "unknown".to_owned(),
        };
        let operation = match self.delivery {
            Delivery::Native { .. } => "written",
            Delivery::Live { .. } => "accepted",
        };
        let received = match self.delivery {
            Delivery::Native { .. } => "logical received",
            Delivery::Live { .. } => "received",
        };
        let restored = match self.delivery {
            Delivery::Native { restored, .. } => format!("; restored {}", units.size(restored.0)),
            Delivery::Live { .. } => String::new(),
        };
        let extent = match self.delivery {
            Delivery::Native {
                extent: WriteExtent::Unconfirmed,
                ..
            } => " (write extent unconfirmed)",
            _ => "",
        };
        let eta = self
            .eta(rate)
            .map_or_else(|| "unknown".to_owned(), |seconds| format!("{seconds}s"));
        format!(
            "{received} {}/{length}; {operation} {}; hashed {}; source {}{restored}; {}; eta {eta}{extent}",
            units.size(self.received.0),
            units.size(self.settled.0),
            units.size(self.hashed.0),
            units.size(self.source.0),
            units.speed(rate.0)
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BytesPerSecond(u64);

#[derive(Default)]
struct Rate {
    start: Duration,
    base: ByteCount,
    previous: ByteCount,
}

impl Rate {
    fn at(start: Duration) -> Self {
        Self {
            start,
            base: ByteCount(0),
            previous: ByteCount(0),
        }
    }

    fn sample(&mut self, current: ByteCount, elapsed: Duration) -> BytesPerSecond {
        if current.0 < self.previous.0 {
            self.start = elapsed;
            self.base = current;
        }
        self.previous = current;
        let nanos = elapsed.saturating_sub(self.start).as_nanos();
        if nanos == 0 {
            return BytesPerSecond(0);
        }
        // Exact integer floor in bytes/s. A restored logical frontier never enters this numerator.
        let bytes = current.0.saturating_sub(self.base.0);
        let scaled = u128::from(bytes) * 1_000_000_000 / nanos;
        BytesPerSecond(scaled.min(u128::from(u64::MAX)) as u64)
    }
}

#[cfg(test)]
#[path = "reporting_tests.rs"]
mod tests;
