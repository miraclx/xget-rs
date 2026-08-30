//! xget: download a URL in parallel, verified, with a live segmented progress bar.

mod expect;
mod progress;

use core::time::Duration;
use std::io::IsTerminal as _;
use std::path::{Path, PathBuf};
use std::time::Instant;

use clap::Parser;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use xbytes::ByteSize;
use xbytes::sizes::BYTE;
use xget::{ByteRange, ByteStream, Checksum, Error, HttpSource, Mirrors, Probe, Source};

use crate::expect::{Expect, parse_expect, resolve_expect};
use crate::progress::{DIM, Reporter};

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
    /// Overwrite an existing output file. Grants permission to replace the destination; it does not by
    /// itself discard a resumable partial (see --restart), so a corrupt finished file can be replaced by
    /// resuming an interrupted download of it.
    #[arg(short = 'f', long)]
    overwrite: bool,
    /// Force resuming a partial download. A partial left by an interrupted run resumes automatically, so
    /// this is only needed to override.
    #[arg(short = 'c', long = "continue", conflicts_with = "restart")]
    resume: bool,
    /// Ignore any resumable partial and download from scratch.
    #[arg(long)]
    restart: bool,
    /// Print what a `.xget` control file records (source, size, progress) and exit, without downloading
    /// or touching the network. The argument may be the `.xget` file or the output it belongs to.
    #[arg(long)]
    info: bool,
    /// Set a request header, e.g. `Authorization: Bearer x` (repeatable).
    #[arg(short = 'H', long = "header", value_parser = parse_header)]
    headers: Vec<(HeaderName, HeaderValue)>,
    /// A mirror URL for the same resource, tried when the primary fails a chunk (repeatable).
    #[arg(long = "mirror", value_name = "URL")]
    mirrors: Vec<String>,
    /// Endpoint for an `s3://` URL, so an S3-compatible store (R2, MinIO, Backblaze) works. Ignored
    /// for HTTP URLs.
    #[arg(long, value_name = "URL")]
    endpoint_url: Option<String>,
    /// Gateway for an `ipfs://` URL (e.g. http://127.0.0.1:8080). Defaults to $IPFS_GATEWAY, then the
    /// local daemon's ~/.ipfs/gateway, then a public gateway. Ignored for other URLs.
    #[arg(long, value_name = "URL")]
    ipfs_gateway: Option<String>,
    /// Checksum to verify the download with: none, md5, sha1, sha256, sha512, or blake3.
    #[arg(short = 's', long, default_value_t = Checksum::Sha256)]
    checksum: Checksum,
    /// Require the download to match this checksum: `algo:hex`, or bare `hex` using `--checksum`'s
    /// algorithm, or a URL to a checksum file. Exits non-zero on mismatch.
    #[arg(long, value_name = "[ALGO:]HEX", value_parser = parse_expect)]
    expect: Option<Expect>,
    /// Fail a chunk if no data arrives for this many seconds, so its retry can resume it.
    #[arg(long, value_name = "SECS", value_parser = parse_timeout)]
    timeout: Option<Duration>,
    /// Progress output: auto (a bar on a terminal, else plain lines), bar, plain, json, or none.
    #[arg(long, value_enum, default_value_t = ProgressMode::Auto)]
    progress: ProgressMode,
    /// Disable the live bar (same as `--progress plain`).
    #[arg(long)]
    no_bar: bool,
    /// Report raw byte counts instead of human-readable sizes.
    #[arg(long)]
    raw_sizes: bool,
    /// Verbose diagnostics on stderr: `-v` for chunk ranges, retries, and errors as they happen; `-vv`
    /// for more. Off by default.
    #[arg(short = 'v', long, action = clap::ArgAction::Count)]
    verbose: u8,
}

/// Turn on diagnostic tracing to stderr at a level chosen by `-v` count. Off when zero, so a normal run
/// emits nothing. `RUST_LOG` overrides the level if set.
fn init_tracing(verbose: u8) {
    if verbose == 0 {
        return;
    }
    let level = if verbose == 1 { "debug" } else { "trace" };
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(format!("xget={level}")));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_target(false)
        .without_time()
        .init();
}

/// How to report progress while downloading.
#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum ProgressMode {
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

fn main() {
    if let Err(error) = run() {
        eprintln!("xget: {error:#}");
        std::process::exit(1);
    }
}

/// Run the download, returning any error for `main` to print. Kept apart so a failure surfaces as one
/// concise line rather than the default multi-line debug report with a source location.
#[tokio::main]
async fn run() -> eyre::Result<()> {
    // Load a .env from the working directory if present, so credentials (e.g. for an s3:// URL) can
    // live there instead of being exported by hand. A missing file is not an error.
    let _ = dotenvy::dotenv();
    let mut cli = Cli::parse();
    // Inspect a control file and exit, before any network setup: `xget --info file.xget` reads the local
    // partial and prints what it records, nothing more.
    if cli.info {
        return show_info(&cli).await;
    }
    // Resume straight from a control file: `xget path/to/file.xget` reads the URL saved inside the
    // partial, rebuilds the source, and finishes writing `path/to/file`, with no URL retyped.
    if let Some((url, output)) = control_resume_target(&cli).await {
        cli.url = url;
        cli.output = Some(output);
        cli.resume = true;
    }
    init_tracing(cli.verbose);
    // The headers were validated into typed pairs at parse time; assemble them into a map.
    let mut headers = HeaderMap::new();
    for (name, value) in &cli.headers {
        headers.insert(name.clone(), value.clone());
    }
    let source = build_source(&cli, &headers).await?;
    let mode = resolve_mode(&cli);

    // Probe once up front for the preamble and the output name; the download re-probes authoritatively.
    let probe = source.probe().await.ok();
    // Where the bytes go: `-` streams to stdout, `/dev/null` discards (a verify-only speed test), anything
    // else is a file whose name is resolved and inferred as before. Only a file has a persistent artifact
    // to resume, so `-` and `/dev/null` never resume.
    let sink = resolve_sink(&cli);
    let output = match sink {
        Sink::File => Some(resolve_output(&cli, probe.as_ref())?),
        // A stream sink writes to the special file exactly as given: no name inference, no clobber check
        // (writing to the pipe or device is the point), no `.xget` beside it.
        Sink::Stream => cli.output.clone(),
        Sink::Stdout | Sink::Discard => None,
    };
    // Auto-resume: an interrupted download leaves a `.xget` control file beside the output, and its
    // presence is the signal to continue where the last run stopped, so a re-run resumes without needing
    // -c. A partial actually on disk is what we resume; -c only asks to, so with nothing to come back to
    // there is nothing to resume (and no "resuming" notice). Resume is independent of -f (which only
    // permits replacing the destination); --restart forces a fresh start. Only a file resumes; `-`,
    // `/dev/null`, and a stream have nothing to come back to.
    let partial = match (sink, &output) {
        (Sink::File, Some(output)) => !cli.restart && xget::resumable(output).await,
        _ => false,
    };
    let resume = partial || (matches!(sink, Sink::File) && cli.resume && !cli.restart);
    if mode == ProgressMode::Bar {
        // For a file, print the block before the bar as usual. For `-`, still print it (to stderr, since
        // stdout is the data). A path label makes sense only for a file; show the destination stream
        // otherwise. The "resuming" notice keys off a real partial, not the -c flag.
        preamble(&cli, probe.as_ref(), output.as_deref(), sink, partial);
    }

    // `--expect` was parsed to a literal digest or a checksum-file URL at parse time; resolve it now,
    // fetching the sidecar if needed, to an optional pinned algorithm and the expected hex.
    let mut expected = match &cli.expect {
        Some(expect) => Some(resolve_expect(expect.clone(), cli.endpoint_url.as_deref()).await?),
        None => None,
    };
    // With no explicit --expect, adopt a checksum the source vouches for (e.g. an S3 stored checksum),
    // so the download is verified against it for free.
    if expected.is_none()
        && let Some((algo, hex)) = probe.as_ref().and_then(|probe| probe.checksum.clone())
    {
        if mode != ProgressMode::Json {
            eprintln!(
                "{}Verifying against the source's stored {algo} checksum{}",
                DIM.render(),
                DIM.render_reset()
            );
        }
        expected = Some((Some(algo), hex));
    }
    let checksum = match &expected {
        Some((Some(algo), _)) => *algo,
        Some((None, _)) if matches!(cli.checksum, Checksum::None) => {
            eyre::bail!("--expect needs an algorithm: pass --checksum or use algo:hex")
        }
        _ => cli.checksum,
    };

    let reporter = Reporter::new(mode, cli.raw_sizes);
    let options = xget::Options {
        parts: cli.chunks,
        retries: cli.tries,
        checksum,
        timeout: cli.timeout,
        resume,
    };
    let started = Instant::now();
    // Build the sink and run. Stdout is held in a binding so a `Writer` can borrow it for the whole call.
    let mut stdout = tokio::io::stdout();
    // A stream sink (pipe, device, or process substitution) is opened for writing up front and held in a
    // binding so the download can borrow it for the whole call, the same way stdout is.
    let mut stream_sink = match (sink, &output) {
        (Sink::Stream, Some(path)) => Some(
            tokio::fs::OpenOptions::new()
                .write(true)
                .open(path)
                .await
                .map_err(|err| eyre::eyre!("cannot open {} for writing: {err}", path.display()))?,
        ),
        _ => None,
    };
    let out = match (sink, &output) {
        (Sink::File, Some(path)) => xget::Output::File(path),
        (Sink::Stdout, _) => xget::Output::Writer(&mut stdout),
        (Sink::Discard, _) => xget::Output::Discard,
        (Sink::Stream, _) => match stream_sink.as_mut() {
            Some(file) => xget::Output::Writer(file),
            None => eyre::bail!("no stream sink opened for a stream download"),
        },
        // A file sink always resolves an output above; this arm is unreachable but keeps the match total.
        (Sink::File, None) => eyre::bail!("no output path resolved for a file download"),
    };
    let report = xget::download(&source, out, options, &reporter).await?;

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

/// The byte source for this run, chosen from the URL scheme. `Source` is not object-safe (it has async
/// methods), so a `dyn Source` is impossible; this enum dispatches to whichever concrete source the URL
/// selected while still being one type the engine can be generic over.
enum AnySource {
    /// An HTTP(S) URL, with any `--mirror` URLs tried in order on failure.
    Http(Mirrors<HttpSource>),
    /// An object in an S3 (or S3-compatible) bucket.
    #[cfg(feature = "s3")]
    S3(xget::S3Source),
    /// Content-addressed data behind an `ipfs://` URL, fetched through a gateway.
    #[cfg(feature = "ipfs")]
    Ipfs(xget::IpfsSource),
}

impl Source for AnySource {
    fn identity(&self) -> Option<String> {
        match self {
            Self::Http(source) => source.identity(),
            #[cfg(feature = "s3")]
            Self::S3(source) => source.identity(),
            #[cfg(feature = "ipfs")]
            Self::Ipfs(source) => source.identity(),
        }
    }

    async fn probe(&self) -> Result<Probe, Error> {
        match self {
            Self::Http(source) => source.probe().await,
            #[cfg(feature = "s3")]
            Self::S3(source) => source.probe().await,
            #[cfg(feature = "ipfs")]
            Self::Ipfs(source) => source.probe().await,
        }
    }

    async fn fetch(&self, range: Option<ByteRange>) -> Result<ByteStream, Error> {
        match self {
            Self::Http(source) => source.fetch(range).await,
            #[cfg(feature = "s3")]
            Self::S3(source) => source.fetch(range).await,
            #[cfg(feature = "ipfs")]
            Self::Ipfs(source) => source.fetch(range).await,
        }
    }
}

/// Build the source for `cli.url`: an `s3://bucket/key` object when the scheme is `s3`, otherwise the
/// HTTP primary plus any `--mirror` URLs tried in order on failure. Mirrors are HTTP-only; an `s3://`
/// URL ignores them.
async fn build_source(cli: &Cli, headers: &HeaderMap) -> eyre::Result<AnySource> {
    if let Some(rest) = cli.url.strip_prefix("s3://") {
        return build_s3_source(rest, cli.endpoint_url.clone()).await;
    }
    if let Some(rest) = cli.url.strip_prefix("ipfs://") {
        return build_ipfs_source(rest, cli.ipfs_gateway.clone());
    }
    // The primary plus any --mirror URLs, tried in order on failure. With no mirrors this is just the
    // primary, so there is one download path.
    let primary = HttpSource::new(&cli.url, headers.clone())?;
    let mirrors = cli
        .mirrors
        .iter()
        .map(|url| HttpSource::new(url, headers.clone()))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(AnySource::Http(Mirrors::new(primary, mirrors)))
}

/// Build an S3 source from the part of an `s3://` URL after the scheme: the first path segment is the
/// bucket and the rest is the key. Only available when built with `--features s3`.
#[cfg(feature = "s3")]
async fn build_s3_source(rest: &str, endpoint_url: Option<String>) -> eyre::Result<AnySource> {
    let (bucket, key) = rest.split_once('/').unwrap_or((rest, ""));
    if bucket.is_empty() || key.is_empty() {
        eyre::bail!("s3 URL must be s3://bucket/key");
    }
    let source = xget::S3Source::new(bucket, key, endpoint_url).await;
    Ok(AnySource::S3(source))
}

/// Reject an `s3://` URL when the `s3` feature was not compiled in, so the failure names the fix rather
/// than the URL falling through to the HTTP path and failing obscurely.
#[cfg(not(feature = "s3"))]
async fn build_s3_source(_rest: &str, _endpoint_url: Option<String>) -> eyre::Result<AnySource> {
    eyre::bail!("s3:// requires building with --features s3")
}

/// Build an IPFS source from the part of an `ipfs://` URL after the scheme: a CID, optionally followed
/// by a path. Only available when built with `--features ipfs`.
#[cfg(feature = "ipfs")]
fn build_ipfs_source(rest: &str, gateway: Option<String>) -> eyre::Result<AnySource> {
    Ok(AnySource::Ipfs(xget::IpfsSource::new(rest, gateway)?))
}

/// Reject an `ipfs://` URL when the `ipfs` feature was not compiled in.
#[cfg(not(feature = "ipfs"))]
fn build_ipfs_source(_rest: &str, _gateway: Option<String>) -> eyre::Result<AnySource> {
    eyre::bail!("ipfs:// requires building with --features ipfs")
}

/// Print the block shown before a live bar: the URL, how many chunks, the length and media type, and
/// where the bytes are going, noting when the run is continuing a partial. Always to stderr, so it does
/// not corrupt a `-` download whose data is on stdout.
fn preamble(cli: &Cli, probe: Option<&Probe>, output: Option<&Path>, sink: Sink, resume: bool) {
    eprintln!("{}URL:{} {}", DIM.render(), DIM.render_reset(), cli.url);
    let chunks = match probe {
        Some(probe) if probe.supports_ranges => xget::plan(probe.length, cli.chunks).len().max(1),
        Some(_) => 1,
        None => cli.chunks as usize,
    };
    eprintln!("{}Chunks:{} {chunks}", DIM.render(), DIM.render_reset());
    match probe {
        Some(probe) => {
            let size = fmt_size(probe.length, cli.raw_sizes);
            match &probe.content_type {
                Some(kind) => eprintln!(
                    "{}Length:{} {size} [{kind}]",
                    DIM.render(),
                    DIM.render_reset()
                ),
                None => eprintln!("{}Length:{} {size}", DIM.render(), DIM.render_reset()),
            }
        }
        None => eprintln!("{}Length:{} unknown", DIM.render(), DIM.render_reset()),
    }
    match (sink, output) {
        (Sink::File, Some(output)) => eprintln!(
            "{}Saving:{} '{}'",
            DIM.render(),
            DIM.render_reset(),
            output.display()
        ),
        (Sink::Stream, Some(output)) => eprintln!(
            "{}Saving:{} '{}' <stream>",
            DIM.render(),
            DIM.render_reset(),
            output.display()
        ),
        (Sink::Stdout, _) => eprintln!("{}Saving:{} <stdout>", DIM.render(), DIM.render_reset()),
        (Sink::Discard, _) | (Sink::File, None) | (Sink::Stream, None) => {
            eprintln!("{}Saving:{} <discarded>", DIM.render(), DIM.render_reset())
        }
    }
    if resume {
        eprintln!(
            "{}Resuming a previous download{}",
            DIM.render(),
            DIM.render_reset()
        );
    }
}

/// Print the closing summary: how much was fetched in how long, and the verified checksum if one was
/// requested. Skipped for `json` (which emits an `end` event instead).
fn summary(
    mode: ProgressMode,
    cli: &Cli,
    checksum: Checksum,
    report: &xget::Report,
    elapsed: Duration,
) {
    if mode == ProgressMode::Json {
        return;
    }
    let size = fmt_size(report.length, cli.raw_sizes);
    // The summary is chatter, not data, so it goes to stderr, leaving stdout clean for a `-` stream.
    eprintln!("Downloaded {size} in {}", fmt_elapsed(elapsed));
    if let Some(hash) = &report.hash {
        eprintln!("Hash({checksum}): {hash}");
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
pub(crate) fn fmt_size(bytes: u64, raw: bool) -> String {
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

/// Parse one `Name: Value` header argument into a typed name and value. A `clap` value parser, so a
/// malformed header is rejected at parse time rather than mid-run.
fn parse_header(raw: &str) -> Result<(HeaderName, HeaderValue), String> {
    let (name, value) = raw
        .split_once(':')
        .ok_or_else(|| format!("header must be `Name: Value`: {raw}"))?;
    let name: HeaderName = name
        .trim()
        .parse()
        .map_err(|_| format!("invalid header name: {name}"))?;
    let value: HeaderValue = value
        .trim()
        .parse()
        .map_err(|_| format!("invalid header value: {value}"))?;
    Ok((name, value))
}

/// Where a run's bytes go, chosen from the output argument: a file (the default), stdout for `-`, or
/// discarded for `/dev/null`, or streamed to a special file (a pipe or device). Only a regular file
/// leaves a persistent artifact, so only it resumes.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Sink {
    /// Write to a regular file whose path is resolved and inferred as usual.
    File,
    /// Stream to stdout (the output argument was `-`).
    Stdout,
    /// Verify and keep nothing (the output argument was `/dev/null`).
    Discard,
    /// Stream to an existing special file: a named pipe, a device, or a shell process substitution
    /// (`>(cmd)`, seen as `/dev/fd/N`). It cannot host a sibling `.xget` or be finalized by rename, so it
    /// is written sequentially like stdout, with no resume.
    Stream,
}

/// Read the output argument to decide the sink: `-` is stdout, the exact path `/dev/null` is a discard,
/// an existing special file (pipe/device/process-substitution) is a stream, anything else (or no
/// argument) is a regular file.
/// Print a summary of the `.xget` control file named by the argument (the `.xget` itself, or the output
/// it belongs to) and exit. Offline: reads only the local file. Powers `xget --info`.
async fn show_info(cli: &Cli) -> eyre::Result<()> {
    let path = Path::new(&cli.url);
    let control_path = if path.extension().and_then(|ext| ext.to_str()) == Some("xget") {
        path.to_path_buf()
    } else {
        let mut with_suffix = path.as_os_str().to_owned();
        with_suffix.push(".xget");
        PathBuf::from(with_suffix)
    };
    let Some(info) = xget::inspect(&control_path).await else {
        eyre::bail!(
            "{} is not a resumable xget control file",
            control_path.display()
        );
    };

    let raw = cli.raw_sizes;
    let percent = if info.total > 0 {
        info.downloaded as f64 * 100.0 / info.total as f64
    } else {
        0.0
    };
    println!(
        "Source:     {}",
        info.source.as_deref().unwrap_or("(unknown)")
    );
    println!("Size:       {}", fmt_size(info.total, raw));
    println!(
        "Downloaded: {}  ({percent:.1}%)",
        fmt_size(info.downloaded, raw)
    );
    if let Some(validator) = &info.validator {
        println!("Validator:  {validator}");
    }
    // The output the partial belongs to is the control path without its `.xget` suffix.
    let output = control_path.with_extension("");
    match info.source {
        Some(_) => println!("Status:     resumable   ->  xget {}", output.display()),
        None => println!("Status:     resumable (re-run with the original URL to resume)"),
    }
    Ok(())
}

/// If the URL argument is actually a `.xget` control file (and no separate output was given), resolve
/// the resume it describes: the source URL saved inside it, and the output it belongs to (its path
/// without the `.xget` suffix). Returns `None` for a normal download. This powers `xget path/to/file.xget`.
async fn control_resume_target(cli: &Cli) -> Option<(String, PathBuf)> {
    if cli.output.is_some() {
        return None;
    }
    let path = Path::new(&cli.url);
    if path.extension().and_then(|ext| ext.to_str()) != Some("xget") {
        return None;
    }
    let url = xget::control_source(path).await?;
    Some((url, path.with_extension("")))
}

fn resolve_sink(cli: &Cli) -> Sink {
    match cli.output.as_deref() {
        Some(path) if path == Path::new("-") => Sink::Stdout,
        Some(path) if path == Path::new("/dev/null") => Sink::Discard,
        Some(path) if is_special_file(path) => Sink::Stream,
        _ => Sink::File,
    }
}

/// Whether `path` names an existing special file: not a regular file or directory, but a FIFO, a
/// character or block device, or a socket. Such a target cannot host a sibling `.xget` scratch or be
/// finalized by an atomic rename, so a download streams to it (like stdout) with no resume, instead of
/// the scatter-and-rename a regular file gets. This is what routes a named pipe or a shell process
/// substitution (`>(cmd)`, which the shell passes as `/dev/fd/N`) to the stream path.
#[cfg(unix)]
fn is_special_file(path: &Path) -> bool {
    use std::os::unix::fs::FileTypeExt as _;
    std::fs::metadata(path).is_ok_and(|meta| {
        let file_type = meta.file_type();
        file_type.is_fifo()
            || file_type.is_char_device()
            || file_type.is_block_device()
            || file_type.is_socket()
    })
}

#[cfg(not(unix))]
fn is_special_file(_path: &Path) -> bool {
    false
}

/// Resolve where to write: an explicit file, a name inside an explicit directory, or a name inferred
/// from the resource (its `Content-Disposition`, else the URL) under `--directory-prefix`. Refuses to
/// clobber an existing complete file without `-f`, and creates missing parents unless `--no-directories`.
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
    // A complete file at the destination is never clobbered without -f. A resume, by contrast, works on
    // the sibling `.part` (which is not this path), so it is not gated here.
    if path.exists() && !cli.overwrite {
        eyre::bail!("{} already exists (use -f to overwrite)", path.display());
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

/// Parse an inactivity timeout given in seconds into a [`Duration`].
fn parse_timeout(value: &str) -> Result<Duration, String> {
    let seconds: f64 = value
        .parse()
        .map_err(|_| format!("invalid seconds: `{value}`"))?;
    if !seconds.is_finite() || seconds < 0.0 {
        return Err(format!("invalid seconds: `{value}`"));
    }
    Ok(Duration::from_secs_f64(seconds))
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
