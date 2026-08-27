//! xget: download a URL in parallel, verified, with a live segmented progress bar.

use core::cell::RefCell;
use core::time::Duration;
use std::io::IsTerminal as _;
use std::path::{Path, PathBuf};
use std::time::Instant;

use clap::Parser;
use libxget::{Checksum, HttpSource, Probe, Progress, Source as _};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use xbytes::prelude::*;
use xprogress::{Bar, Color, DrawTarget, Style};

/// The bar takes at most this fraction of the terminal width, so it scales with the screen instead of
/// stretching across it. Capped by [`BAR_MAX`] and floored so it never vanishes.
const BAR_MAX_PCT: u16 = 40;
/// The bar never grows past this many cells, however wide the terminal, keeping the stats in focus.
const BAR_MAX: u16 = 48;

// ANSI colors for the live readout. Each code has zero display width, so it never affects the bar's
// own width accounting; it only tints the text that follows the bar.
const GREEN: &str = "\x1b[32m";
const CYAN: &str = "\x1b[36m";
const YELLOW: &str = "\x1b[33m";
const DIM: &str = "\x1b[2m";
const RESET: &str = "\x1b[0m";

/// Download a URL in parallel chunks, verify it, and print its SHA-256.
#[derive(Parser)]
#[command(name = "xget", version, about)]
struct Cli {
    /// The URL to download.
    url: String,
    /// Where to write the file. If omitted or a directory, the name is taken from the URL.
    output: Option<PathBuf>,
    /// Maximum number of concurrent chunk connections.
    #[arg(short = 'n', long, default_value_t = 5)]
    chunks: u32,
    /// Retries for each chunk, resuming from its offset. `inf` for unlimited.
    #[arg(short = 't', long, default_value = "10", value_parser = parse_tries)]
    tries: u32,
    /// Save the file under this directory prefix.
    #[arg(short = 'D', long)]
    directory_prefix: Option<PathBuf>,
    /// Do not create missing directories.
    #[arg(long)]
    no_directories: bool,
    /// Overwrite an existing output file.
    #[arg(short = 'f', long)]
    overwrite: bool,
    /// Resume a partial download: keep the existing file, fetch only what remains, still verified.
    #[arg(short = 'c', long = "continue")]
    resume: bool,
    /// Set a request header, e.g. `Authorization: Bearer x` (repeatable).
    #[arg(short = 'H', long = "header")]
    headers: Vec<String>,
    /// Checksum to verify the download with: none, md5, sha1, sha256, sha512, or blake3.
    #[arg(short = 's', long, default_value_t = Checksum::Sha256)]
    checksum: Checksum,
    /// Require the download to match this checksum: `algo:hex`, or bare `hex` using `--checksum`'s
    /// algorithm. Exits non-zero on mismatch.
    #[arg(long, value_name = "[ALGO:]HEX")]
    expect: Option<String>,
    /// Fail a chunk if no data arrives for this many seconds, so its retry can resume it.
    #[arg(long, value_name = "SECS")]
    timeout: Option<f64>,
    /// Buffers each chunk may read ahead of the reassembler; trades memory for smoother throughput.
    #[arg(long, value_name = "N", default_value_t = 64)]
    cache_size: usize,
    /// Progress output: auto (a bar on a terminal, else plain lines), bar, plain, json, or none.
    #[arg(long, value_enum, default_value_t = ProgressMode::Auto)]
    progress: ProgressMode,
    /// Disable the live bar (same as `--progress plain`).
    #[arg(long)]
    no_bar: bool,
    /// Report raw byte counts instead of human-readable sizes.
    #[arg(long)]
    raw_sizes: bool,
}

/// How to report progress while downloading.
#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum ProgressMode {
    /// A live bar on a terminal, otherwise plain lines.
    Auto,
    /// A live segmented bar.
    Bar,
    /// A single updating text line.
    Plain,
    /// One JSON event per update, to stderr.
    Json,
    /// No progress, only the final result.
    None,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let cli = Cli::parse();
    let source = HttpSource::new(&cli.url, parse_headers(&cli.headers)?)?;
    let mode = resolve_mode(&cli);

    // Probe once up front for the preamble and the output name; the download re-probes authoritatively.
    let probe = source.probe().await.ok();
    let output = resolve_output(&cli, probe.as_ref())?;
    if mode == ProgressMode::Bar {
        preamble(&cli, probe.as_ref(), &output);
    }

    // `--expect` is a literal digest or a URL to a checksum file; resolve it (fetching the sidecar if
    // needed) to an optional pinned algorithm and the expected hex.
    let expected = match cli.expect.as_deref() {
        Some(value) => Some(resolve_expect(parse_expect(value)?).await?),
        None => None,
    };
    let checksum = match &expected {
        Some((Some(algo), _)) => *algo,
        Some((None, _)) if matches!(cli.checksum, Checksum::None) => {
            eyre::bail!("--expect needs an algorithm: pass --checksum or use algo:hex")
        }
        _ => cli.checksum,
    };

    let reporter = Reporter::new(mode, cli.raw_sizes);
    let options = libxget::Options {
        parts: cli.chunks,
        retries: cli.tries,
        checksum,
        timeout: cli.timeout.map(Duration::from_secs_f64),
        resume: cli.resume,
        cache: cli.cache_size,
    };
    let started = Instant::now();
    let report = libxget::download(&source, &output, options, &reporter).await?;

    if let Some((_, want)) = &expected {
        match &report.hash {
            Some(got) if got.eq_ignore_ascii_case(want) => {}
            Some(got) => {
                eyre::bail!("checksum mismatch: expected {checksum}:{want}, got {checksum}:{got}")
            }
            None => eyre::bail!("no checksum was computed to verify against"),
        }
    }

    summary(mode, &cli, checksum, &report, started.elapsed());
    Ok(())
}

/// An `--expect` value: either a checksum given inline, or a URL to a checksum file to fetch. Both
/// carry an optional pinned algorithm (from an `algo:` prefix inline, or the sidecar's extension).
enum Expect {
    /// A digest given on the command line.
    Literal {
        /// The algorithm, if pinned by an `algo:` prefix.
        algo: Option<Checksum>,
        /// The expected lowercase hex digest.
        hex: String,
    },
    /// A URL to a checksum file, as published beside many releases (`file.tar.gz.sha256`).
    Sidecar {
        /// The algorithm, if inferred from the URL's extension.
        algo: Option<Checksum>,
        /// The checksum file's URL.
        url: String,
    },
}

/// Parse an `--expect` value: a URL (contains `://`) becomes a [`Expect::Sidecar`] with the algorithm
/// inferred from its extension; otherwise an `algo:hex` or bare `hex` [`Expect::Literal`].
fn parse_expect(value: &str) -> eyre::Result<Expect> {
    if value.contains("://") {
        return Ok(Expect::Sidecar {
            algo: algo_from_extension(value),
            url: value.to_owned(),
        });
    }
    match value.split_once(':') {
        Some((algo, hex)) => Ok(Expect::Literal {
            algo: Some(algo.parse()?),
            hex: hex.to_ascii_lowercase(),
        }),
        None => Ok(Expect::Literal {
            algo: None,
            hex: value.to_ascii_lowercase(),
        }),
    }
}

/// Resolve an [`Expect`] to a pinned algorithm and the expected hex, fetching and parsing a checksum
/// file if the value was a URL. A checksum file's first hex-looking token is taken, so both a bare
/// digest and the usual `<hex>  <filename>` line both work.
async fn resolve_expect(expect: Expect) -> eyre::Result<(Option<Checksum>, String)> {
    match expect {
        Expect::Literal { algo, hex } => Ok((algo, hex)),
        Expect::Sidecar { algo, url } => {
            let body = reqwest::get(&url).await?.error_for_status()?.text().await?;
            let hex = body
                .split_whitespace()
                .find(|token| is_hex_digest(token))
                .ok_or_else(|| eyre::eyre!("no checksum found at {url}"))?
                .to_ascii_lowercase();
            Ok((algo, hex))
        }
    }
}

/// Infer a checksum algorithm from a checksum file's extension, e.g. `.sha256` or `.sha256sum`.
fn algo_from_extension(url: &str) -> Option<Checksum> {
    let lower = url.to_ascii_lowercase();
    let matches = |suffixes: &[&str]| suffixes.iter().any(|suffix| lower.ends_with(suffix));
    if matches(&[".sha256", ".sha256sum"]) {
        Some(Checksum::Sha256)
    } else if matches(&[".sha512", ".sha512sum"]) {
        Some(Checksum::Sha512)
    } else if matches(&[".sha1", ".sha1sum"]) {
        Some(Checksum::Sha1)
    } else if matches(&[".md5", ".md5sum"]) {
        Some(Checksum::Md5)
    } else {
        None
    }
}

/// Whether `token` is a hex string of a checksum's length (md5, sha1, sha256, or sha512).
fn is_hex_digest(token: &str) -> bool {
    matches!(token.len(), 32 | 40 | 64 | 128) && token.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Print the block shown before a live bar: the URL, how many chunks, the length and media type, and
/// where the file is being written.
fn preamble(cli: &Cli, probe: Option<&Probe>, output: &Path) {
    eprintln!("{DIM}URL:{RESET} {}", cli.url);
    let chunks = match probe {
        Some(probe) if probe.supports_ranges => {
            libxget::plan(probe.length, cli.chunks).len().max(1)
        }
        Some(_) => 1,
        None => cli.chunks as usize,
    };
    eprintln!("{DIM}Chunks:{RESET} {chunks}");
    match probe {
        Some(probe) => {
            let size = fmt_size(probe.length, cli.raw_sizes);
            match &probe.content_type {
                Some(kind) => eprintln!("{DIM}Length:{RESET} {size} [{kind}]"),
                None => eprintln!("{DIM}Length:{RESET} {size}"),
            }
        }
        None => eprintln!("{DIM}Length:{RESET} unknown"),
    }
    eprintln!("{DIM}Saving:{RESET} '{}'", output.display());
}

/// Print the closing summary: how much was fetched in how long, and the verified checksum if one was
/// requested. Skipped for `json` (which emits an `end` event instead).
fn summary(
    mode: ProgressMode,
    cli: &Cli,
    checksum: Checksum,
    report: &libxget::Report,
    elapsed: Duration,
) {
    if mode == ProgressMode::Json {
        return;
    }
    let size = fmt_size(report.length, cli.raw_sizes);
    println!("Downloaded {size} in {}", fmt_elapsed(elapsed));
    if let Some(hash) = &report.hash {
        println!("Hash({checksum}): {hash}");
    }
}

/// Resolve the effective progress mode: `auto` becomes a bar on a terminal and plain lines otherwise,
/// and `--no-bar` downgrades a bar to plain.
fn resolve_mode(cli: &Cli) -> ProgressMode {
    let mut mode = cli.progress;
    if mode == ProgressMode::Auto {
        mode = if std::io::stderr().is_terminal() {
            ProgressMode::Bar
        } else {
            ProgressMode::Plain
        };
    }
    if cli.no_bar && mode == ProgressMode::Bar {
        mode = ProgressMode::Plain;
    }
    mode
}

/// Format a byte count, either raw or human-readable.
fn fmt_size(bytes: u64, raw: bool) -> String {
    if raw {
        bytes.to_string()
    } else {
        ByteSize::of(bytes, BYTE).iec().to_string()
    }
}

/// A compact wall-clock duration for the closing summary: `1h2m`, `3m4s`, or `4.29s`.
fn fmt_elapsed(elapsed: Duration) -> String {
    let seconds = elapsed.as_secs_f64();
    if seconds >= 3600.0 {
        format!(
            "{}h{}m",
            (seconds / 3600.0) as u64,
            (seconds % 3600.0 / 60.0) as u64
        )
    } else if seconds >= 60.0 {
        format!("{}m{:.0}s", (seconds / 60.0) as u64, seconds % 60.0)
    } else {
        format!("{seconds:.2}s")
    }
}

/// Parse repeated `Name: Value` header arguments into a [`HeaderMap`].
fn parse_headers(raw: &[String]) -> eyre::Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    for entry in raw {
        let (name, value) = entry
            .split_once(':')
            .ok_or_else(|| eyre::eyre!("header must be `Name: Value`: {entry}"))?;
        headers.insert(
            name.trim().parse::<HeaderName>()?,
            value.trim().parse::<HeaderValue>()?,
        );
    }
    Ok(headers)
}

/// Resolve where to write: an explicit file, a name inside an explicit directory, or a name inferred
/// from the resource (its `Content-Disposition`, else the URL) under `--directory-prefix`. Refuses to
/// clobber an existing file without `-f`, and creates missing parents unless `--no-directories`.
fn resolve_output(cli: &Cli, probe: Option<&Probe>) -> eyre::Result<PathBuf> {
    let path = match &cli.output {
        Some(dir) if dir.is_dir() => dir.join(infer_name(cli, probe)?),
        Some(file) => file.to_path_buf(),
        None => {
            let name = infer_name(cli, probe)?;
            match &cli.directory_prefix {
                Some(prefix) => prefix.join(name),
                None => PathBuf::from(name),
            }
        }
    };
    if path.exists() && !cli.overwrite && !cli.resume {
        eyre::bail!(
            "{} already exists (use -f to overwrite or -c to resume)",
            path.display()
        );
    }
    if !cli.no_directories {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
    }
    Ok(path)
}

/// Infer an output filename: the resource's `Content-Disposition` if the server offered one in the
/// probe, otherwise the URL's last path segment.
fn infer_name(cli: &Cli, probe: Option<&Probe>) -> eyre::Result<String> {
    if let Some(name) = probe.and_then(|probe| probe.filename.clone()) {
        return Ok(name);
    }
    url_basename(&cli.url)
}

/// The last path segment of the URL, for naming a downloaded file.
fn url_basename(url: &str) -> eyre::Result<String> {
    let parsed = reqwest::Url::parse(url)?;
    let name = parsed
        .path_segments()
        .and_then(Iterator::last)
        .filter(|segment| !segment.is_empty());
    match name {
        Some(name) => Ok(name.to_owned()),
        None => eyre::bail!("cannot infer a filename from {url}; give an output path"),
    }
}

/// Parse a retry count, accepting `inf`/`infinite` as unlimited.
fn parse_tries(value: &str) -> Result<u32, String> {
    match value.to_ascii_lowercase().as_str() {
        "inf" | "infinite" => Ok(u32::MAX),
        _ => value
            .parse()
            .map_err(|_| format!("expected a number or `inf`, got `{value}`")),
    }
}

/// The chosen progress reporter, dispatching to whichever mode was selected. Every variant reports the
/// same download; they differ only in how they render it.
enum Reporter {
    /// A live segmented bar with a size, speed, and ETA readout.
    Bar(BarProgress),
    /// A single updating text line.
    Plain(PlainProgress),
    /// One JSON event per update.
    Json(JsonProgress),
    /// No progress output.
    Silent,
}

impl Reporter {
    fn new(mode: ProgressMode, raw: bool) -> Self {
        match mode {
            ProgressMode::Bar | ProgressMode::Auto => Self::Bar(BarProgress::new(raw)),
            ProgressMode::Plain => Self::Plain(PlainProgress::new(raw)),
            ProgressMode::Json => Self::Json(JsonProgress::new()),
            ProgressMode::None => Self::Silent,
        }
    }
}

impl Progress for Reporter {
    fn start(&self, chunks: &[u64]) {
        match self {
            Self::Bar(bar) => bar.start(chunks),
            Self::Plain(plain) => plain.start(chunks),
            Self::Json(json) => json.start(chunks),
            Self::Silent => {}
        }
    }

    fn received(&self, index: usize, bytes: u64) {
        match self {
            Self::Bar(bar) => bar.received(index, bytes),
            Self::Plain(plain) => plain.received(index, bytes),
            Self::Json(json) => json.received(index, bytes),
            Self::Silent => {}
        }
    }

    fn wrote(&self, index: usize, bytes: u64) {
        match self {
            Self::Bar(bar) => bar.wrote(index, bytes),
            Self::Plain(plain) => plain.wrote(index, bytes),
            Self::Json(json) => json.wrote(index, bytes),
            Self::Silent => {}
        }
    }

    fn finish(&self) {
        match self {
            Self::Bar(bar) => bar.finish(),
            Self::Plain(plain) => plain.finish(),
            Self::Json(json) => json.finish(),
            Self::Silent => {}
        }
    }
}

/// Running totals shared by the text-based reporters: how much of how many bytes have arrived, and a
/// throttle so a fast download does not spend its time printing.
struct Meter {
    total: u64,
    done: u64,
    started: Instant,
    last: Instant,
}

impl Meter {
    fn new(total: u64) -> Self {
        let now = Instant::now();
        Self {
            total,
            done: 0,
            started: now,
            last: now,
        }
    }

    /// Record `bytes` more of confirmed progress.
    fn add(&mut self, bytes: u64) {
        self.done += bytes;
    }

    /// Whether enough time has passed since the last redraw to draw again.
    fn ready(&mut self, every: Duration) -> bool {
        let now = Instant::now();
        if now.duration_since(self.last) < every {
            return false;
        }
        self.last = now;
        true
    }

    /// Record `bytes` more and report whether enough time has passed to redraw.
    fn advance(&mut self, bytes: u64, every: Duration) -> bool {
        self.add(bytes);
        self.ready(every)
    }

    /// Bytes per second so far.
    fn rate(&self) -> u64 {
        let elapsed = self.started.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            (self.done as f64 / elapsed) as u64
        } else {
            0
        }
    }

    /// Seconds until completion at the current rate, or `0` if unknown.
    fn eta(&self) -> u64 {
        let rate = self.rate();
        if rate > 0 && self.total > self.done {
            (self.total - self.done) / rate
        } else {
            0
        }
    }

    /// Percent complete, `0` when the total is unknown.
    fn percent(&self) -> u64 {
        self.done
            .saturating_mul(100)
            .checked_div(self.total)
            .unwrap_or(0)
    }

    /// Percent complete as a fraction, `0.0` when the total is unknown.
    fn percent_f64(&self) -> f64 {
        if self.total > 0 {
            self.done as f64 * 100.0 / self.total as f64
        } else {
            0.0
        }
    }

    /// The ETA as a `MM:SS` clock (minutes may exceed 59 for a long transfer).
    fn eta_clock(&self) -> String {
        let eta = self.eta();
        format!("{:02}:{:02}", eta / 60, eta % 60)
    }

    /// Milliseconds since the transfer started.
    fn elapsed_ms(&self) -> u128 {
        self.started.elapsed().as_millis()
    }
}

/// A single updating text line: `done / total (pct%)  speed  eta`, drawn in place on stderr and
/// throttled, with a final newline so the shell prompt lands cleanly.
struct PlainProgress {
    meter: RefCell<Option<Meter>>,
    raw: bool,
}

impl PlainProgress {
    fn new(raw: bool) -> Self {
        Self {
            meter: RefCell::new(None),
            raw,
        }
    }

    fn line(&self, meter: &Meter) -> String {
        format!(
            "\r\x1b[K{:>5.1}%  {}/{}  {}/s  ETA {}",
            meter.percent_f64(),
            fmt_size(meter.done, self.raw),
            fmt_size(meter.total, self.raw),
            fmt_size(meter.rate(), self.raw),
            meter.eta_clock(),
        )
    }
}

impl Progress for PlainProgress {
    fn start(&self, chunks: &[u64]) {
        let meter = Meter::new(chunks.iter().sum());
        eprint!("{}", self.line(&meter));
        let _ = std::io::Write::flush(&mut std::io::stderr());
        *self.meter.borrow_mut() = Some(meter);
    }

    fn wrote(&self, _index: usize, bytes: u64) {
        if let Some(meter) = self.meter.borrow_mut().as_mut() {
            if meter.advance(bytes, Duration::from_millis(200)) {
                eprint!("{}", self.line(meter));
                let _ = std::io::Write::flush(&mut std::io::stderr());
            }
        }
    }

    fn finish(&self) {
        if let Some(meter) = self.meter.borrow().as_ref() {
            eprintln!("{}", self.line(meter));
        }
    }
}

/// One JSON event per update, to stderr: a `start`, throttled `progress` events, and a `done`. The
/// numbers are always raw bytes so a consumer can format them itself.
struct JsonProgress {
    meter: RefCell<Option<Meter>>,
}

impl JsonProgress {
    fn new() -> Self {
        Self {
            meter: RefCell::new(None),
        }
    }
}

impl Progress for JsonProgress {
    fn start(&self, chunks: &[u64]) {
        *self.meter.borrow_mut() = Some(Meter::new(chunks.iter().sum()));
    }

    fn wrote(&self, _index: usize, bytes: u64) {
        if let Some(meter) = self.meter.borrow_mut().as_mut() {
            if meter.advance(bytes, Duration::from_millis(200)) {
                eprintln!(
                    r#"{{"event":"progress","bytes":{},"total":{},"percent":{:.1},"speed":{},"eta":{}}}"#,
                    meter.done,
                    meter.total,
                    meter.percent_f64(),
                    meter.rate(),
                    meter.eta()
                );
            }
        }
    }

    fn finish(&self) {
        if let Some(meter) = self.meter.borrow().as_ref() {
            eprintln!(
                r#"{{"event":"end","bytes":{},"total":{},"elapsed":{}}}"#,
                meter.done,
                meter.total,
                meter.elapsed_ms()
            );
        }
    }
}

/// The live bar. With a handful of chunks it draws an aggregate header line over a per-chunk line, both
/// two-tone (done vs received-ahead); with one chunk or many it collapses to a single aggregate bar.
/// The lead comes from bytes received off the network, the done from bytes written and hashed, so the
/// shaded gap between them is the buffered-ahead window. Drawn in place and throttled.
struct BarProgress {
    inner: RefCell<Option<Live>>,
    raw: bool,
}

struct Live {
    bar: Bar,
    target: DrawTarget,
    width: u16,
    multi: bool,
    prev_lines: usize,
    meter: Meter,
    raw: bool,
}

impl BarProgress {
    fn new(raw: bool) -> Self {
        Self {
            inner: RefCell::new(None),
            raw,
        }
    }
}

impl Progress for BarProgress {
    fn start(&self, chunks: &[u64]) {
        // A two-line view is worth it for a few chunks; one chunk has nothing to aggregate and twenty
        // would not fit, so both collapse to a single aggregate bar.
        let multi = (2..20).contains(&chunks.len());
        // The libxget-js look: a thin solid line for done capped with a head, dashed for lead and
        // empty, cyan fill.
        let style = Style::default()
            .with_filler('━')
            .with_header('╸')
            .with_leader('┅')
            .with_blank('┅')
            .with_separator('┆')
            .with_color(Color::Cyan);
        let bar = Bar::new(chunks.iter().copied()).with_style(style);
        let target = DrawTarget::from_env();
        // Fill the space beside the stats, but never past a fraction of the screen, so the bar scales
        // with the terminal up to a cap and only shrinks past that when the stats would clip.
        let columns = target.columns().unwrap_or(80);
        let reserve = if multi { 34 } else { 52 };
        let cap = (columns.saturating_mul(BAR_MAX_PCT) / 100).min(BAR_MAX);
        let width = cap.min(columns.saturating_sub(reserve)).max(8);
        let mut slot = self.inner.borrow_mut();
        *slot = Some(Live {
            bar,
            target,
            width,
            multi,
            prev_lines: 0,
            meter: Meter::new(chunks.iter().sum()),
            raw: self.raw,
        });
        if let Some(live) = slot.as_mut() {
            redraw(live);
        }
    }

    fn received(&self, index: usize, bytes: u64) {
        if let Some(live) = self.inner.borrow_mut().as_mut() {
            live.bar.advance_lead(index, bytes);
            if live.meter.ready(Duration::from_millis(60)) {
                redraw(live);
            }
        }
    }

    fn wrote(&self, index: usize, bytes: u64) {
        if let Some(live) = self.inner.borrow_mut().as_mut() {
            live.bar.advance(index, bytes);
            live.meter.add(bytes);
            if live.meter.ready(Duration::from_millis(60)) {
                redraw(live);
            }
        }
    }

    fn finish(&self) {
        if let Some(live) = self.inner.borrow_mut().as_mut() {
            let block = frame(live);
            let _ = live.target.finish_block(&block, live.prev_lines);
        }
    }
}

/// Redraw the bar block in place, remembering how many lines it drew for the next redraw.
fn redraw(live: &mut Live) {
    let block = frame(live);
    if let Ok(lines) = live.target.draw_block(&block, live.prev_lines) {
        live.prev_lines = lines;
    }
}

/// Compose the bar block: an aggregate header line over the per-chunk line when multi, otherwise a
/// single aggregate bar. The readout (percent, speed, eta, sizes) is xget's to write; xprogress only
/// renders the bars.
fn frame(live: &Live) -> String {
    let meter = &live.meter;
    let pct = meter.percent();
    let speed = fmt_size(meter.rate(), live.raw);
    let eta = format_eta(meter.eta());
    let done = fmt_size(meter.done, live.raw);
    let total = fmt_size(meter.total, live.raw);
    let rate = format!("{GREEN}{pct:>3}%{RESET}  {CYAN}{speed}/s{RESET}  {YELLOW}{eta}{RESET}");
    if live.multi {
        format!(
            "  {DIM}┏{RESET} {} {DIM}┓{RESET}  {rate}\n  {DIM}┗{RESET} {} {DIM}┛{RESET}  {done}/{total}",
            live.bar.render_aggregate(live.width),
            live.bar.render(live.width),
        )
    } else {
        format!(
            "  {DIM}[{RESET}{}{DIM}]{RESET}  {rate}  {done}/{total}",
            live.bar.render_aggregate(live.width),
        )
    }
}

/// A compact `1h2m` / `3m4s` / `5s` duration for the ETA readout.
fn format_eta(seconds: u64) -> String {
    if seconds >= 3600 {
        format!("{}h{}m", seconds / 3600, (seconds % 3600) / 60)
    } else if seconds >= 60 {
        format!("{}m{}s", seconds / 60, seconds % 60)
    } else {
        format!("{seconds}s")
    }
}
