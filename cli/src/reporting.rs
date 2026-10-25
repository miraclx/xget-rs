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
use crate::human::{Human, Position};
use crate::output::{Channel, Format, ItemId, Output, RunId};

mod bar;
mod rate;

use bar::{Decoration, RibbonStyle, Row, TerminalBar};
use rate::RecentRate;

/// Front-door presentation policy. These are not source concurrency or core timing knobs.
#[derive(Args, Clone, Copy, Default)]
pub(crate) struct Presentation {
    /// Progress: auto (stderr bar on a terminal, complete plain snapshots otherwise), bar, plain,
    /// versioned json, or none. None suppresses progress, not final results or failures.
    #[arg(long, value_enum, default_value_t = Mode::Auto, conflicts_with = "json")]
    progress: Mode,
    /// Terminal ribbon glyphs and palette; does not change progress facts or captured output.
    #[arg(long, value_enum, default_value_t = RibbonStyle::Classic)]
    style: RibbonStyle,
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
        Output::new(run, format, Channel::Diagnostic).with_units(self.units())
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
    human_rate: RecentRate,
    last_human: Option<Duration>,
    last_count: Option<ByteCount>,
    restored_shown: bool,
    position: Position,
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
        let terminal = std::io::stderr().is_terminal();
        let mode = presentation.resolved(terminal);
        Self {
            output,
            mode,
            units: presentation.units(),
            started: Instant::now(),
            rate: Rate::default(),
            last_render: None,
            human_rate: RecentRate::at(Duration::ZERO),
            last_human: None,
            last_count: None,
            restored_shown: false,
            position: Position {
                current: 0,
                total: 0,
            },
            bar: match (mode, terminal) {
                (Mode::Bar, true) => Some(TerminalBar::new(Decoration::Bar, presentation.style)),
                (Mode::Plain, true) => {
                    Some(TerminalBar::new(Decoration::Plain, presentation.style))
                }
                _ => None,
            },
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

    pub(crate) fn live_started(&mut self, length: Length) -> Result<(), Failure> {
        if matches!(self.mode, Mode::None | Mode::Json) {
            return Ok(());
        }
        let heading = self.output.human_text(|human| human.live_start(length));
        match &mut self.bar {
            Some(bar) => {
                let (heading, warning) = heading.split_once('\n').unwrap_or((&heading, ""));
                self.output.human(|_| warning.to_owned())?;
                bar.start(heading.to_owned())
            }
            None => self.output.human(|_| heading),
        }
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
        // The machine sampler, including its unconfirmed-extent bypass, is deliberately unchanged.
        let rate = self.rate.sample(snapshot.source, elapsed);
        if self.mode == Mode::Json {
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
            let facts = snapshot.machine(rate);
            let result = match key {
                Some(key) => self
                    .output
                    .emit_saved("Progress", key.run_id(), Some(key), facts),
                None => self.output.emit("Progress", Some(ItemId(0)), facts),
            };
            self.record(result);
            return;
        }
        if self.mode == Mode::None {
            return;
        }
        self.human_rate.sample(snapshot.source, elapsed);
        if let Delivery::Native { restored, .. } = snapshot.delivery
            && restored.0 != 0
            && !self.restored_shown
        {
            let text = self
                .output
                .human_text(|human| format!("restored {}", human.size(restored.0)));
            let result = match &mut self.bar {
                Some(bar) => {
                    bar.restored(text);
                    Ok(())
                }
                None => self.output.human(|_| format!("  {text}")),
            };
            self.record(result);
            if self.failure.is_some() {
                return;
            }
            self.restored_shown = true;
        }
        let count = snapshot.count();
        if self.last_count == Some(count)
            || self
                .last_human
                .is_some_and(|last| elapsed.saturating_sub(last) < Duration::from_millis(200))
        {
            return;
        }
        self.last_count = Some(count);
        self.last_human = Some(elapsed);
        let row = snapshot.human(self.units, self.human_rate.rate(elapsed));
        let result = match &mut self.bar {
            Some(bar) => bar.render(snapshot.length, count, &row),
            None => self.output.human_progress(&format!("  {}", row.plain())),
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
                self.position = Position {
                    current: 0,
                    total: files,
                };
                self.output.human_run_started(files);
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
                let elapsed = self.started.elapsed();
                self.rate = Rate::at(elapsed);
                self.last_render = None;
                self.human_rate = RecentRate::at(elapsed);
                self.last_human = None;
                self.last_count = None;
                self.restored_shown = false;
                self.position.current += 1;
                if self.mode == Mode::None {
                    return Ok(());
                }
                let heading = |human: &Human| {
                    human.header(
                        planned.destination(),
                        planned.file().length(),
                        self.position,
                    )
                };
                match &mut self.bar {
                    Some(bar) => bar.start(self.output.human_text(heading))?,
                    None => self.output.human(heading)?,
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
                match &mut self.bar {
                    Some(bar) => {
                        let receipt = self.output.human_text(|human| human.published(published));
                        bar.receipt(&receipt)?;
                    }
                    None => self.output.human(|human| human.published(published))?,
                }
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
                self.output.human_done(report.completed().len())?;
                let facts = json!({"complete": true, "completed_files": report.completed().len(), "state_path": report.saved_run().map(Output::path)});
                match report.run_id() {
                    Some(run) => self.output.emit_saved("End", run, None, facts),
                    None => self.output.emit("End", None, facts),
                }
            }
            Event::RunFailed { failure } => {
                if let Some(bar) = &mut self.bar {
                    bar.failed()?;
                }
                self.terminal = Terminal::Failed;
                self.output.human_run_failed(failure);
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

    fn count(&self) -> ByteCount {
        match self.delivery {
            Delivery::Native { .. } => self.received,
            Delivery::Live { .. } => self.settled,
        }
    }

    fn human(&self, units: DisplayFormat, rate: Option<BytesPerSecond>) -> Row {
        let human = Human { units, cwd: None };
        let operation = match self.delivery {
            Delivery::Native { .. } => "received",
            Delivery::Live { .. } => "accepted",
        };
        let current = self.count().0;
        let (count, detailed) = match self.length {
            Length::Known(total) => {
                let count = format!(
                    "{operation} {}/{}",
                    human.size(current),
                    human.size(total.0)
                );
                if total.0 == 0 {
                    let waiting = format!("{count}; awaiting completion");
                    (String::clone(&waiting), waiting)
                } else if current >= total.0 {
                    let percent = if current == total.0 { "100% " } else { "" };
                    let waiting = format!("{percent}{count}; awaiting completion");
                    (format!("{count}; awaiting completion"), waiting)
                } else {
                    let percent = u128::from(current) * 100 / u128::from(total.0);
                    let detailed = format!("{percent}% {count}");
                    (count, detailed)
                }
            }
            Length::Unknown => {
                let count = format!("{operation} {} (unknown size)", human.size(current));
                (String::clone(&count), count)
            }
        };
        let eta = match (self.length, rate) {
            (Length::Known(total), Some(BytesPerSecond(rate)))
                if rate > 0 && self.received.0 < total.0 =>
            {
                Some(format!(
                    "ETA {}s",
                    total.0.saturating_sub(self.received.0).div_ceil(rate)
                ))
            }
            _ => None,
        };
        let percent = match self.length {
            Length::Known(total) if total.0 > 0 && current <= total.0 => {
                // Exact integer tenths, rounded down once; no floating-point byte arithmetic.
                let tenths = u128::from(current) * 1000 / u128::from(total.0);
                Some(format!("{}.{:01}%", tenths / 10, tenths % 10))
            }
            _ => None,
        };
        Row {
            count: count.replace('/', " / "),
            detailed,
            percent,
            rate: rate.map(|BytesPerSecond(rate)| human.speed(rate)),
            eta,
        }
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
