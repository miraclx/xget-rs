//! `--expect`: parse a checksum expectation (a literal `algo:hex`, a bare hex, or a checksum-file URL)
//! and resolve it to the pinned algorithm and expected digest the download must match.

use xget::Checksum;

/// An `--expect` value: either a checksum given inline, or a URL to a checksum file to fetch. Both
/// carry an optional pinned algorithm (from an `algo:` prefix inline, or the sidecar's extension).
#[derive(Clone)]
pub(crate) enum Expect {
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
/// inferred from its extension; otherwise an `algo:hex` or bare `hex` [`Expect::Literal`]. A `clap`
/// value parser, so a bad `algo:` prefix is rejected at parse time; the sidecar it names is fetched
/// later, at run time.
pub(crate) fn parse_expect(value: &str) -> Result<Expect, String> {
    if value.contains("://") {
        return Ok(Expect::Sidecar {
            algo: algo_from_extension(value),
            url: value.to_owned(),
        });
    }
    match value.split_once(':') {
        Some((algo, hex)) => Ok(Expect::Literal {
            algo: Some(algo.parse().map_err(|error| format!("{error}"))?),
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
pub(crate) async fn resolve_expect(
    expect: Expect,
    endpoint_url: Option<&str>,
) -> eyre::Result<(Option<Checksum>, String)> {
    match expect {
        Expect::Literal { algo, hex } => Ok((algo, hex)),
        Expect::Sidecar { algo, url } => {
            let body = fetch_sidecar(&url, endpoint_url).await?;
            let hex = body
                .split_whitespace()
                .find(|token| is_hex_digest(token))
                .ok_or_else(|| eyre::eyre!("no checksum found at {url}"))?
                .to_ascii_lowercase();
            Ok((algo, hex))
        }
    }
}

/// Fetch a checksum file as text, over HTTP or, for an `s3://` URL, through the S3 source (so a sidecar
/// published in the same bucket works). The file is small, so it is read whole.
async fn fetch_sidecar(url: &str, endpoint_url: Option<&str>) -> eyre::Result<String> {
    match url.strip_prefix("s3://") {
        Some(rest) => fetch_s3_text(rest, endpoint_url).await,
        None => Ok(reqwest::get(url).await?.error_for_status()?.text().await?),
    }
}

/// Read a small `s3://bucket/key` object whole and decode it as UTF-8. Only available with `--features
/// s3`.
#[cfg(feature = "s3")]
async fn fetch_s3_text(rest: &str, endpoint_url: Option<&str>) -> eyre::Result<String> {
    use futures::StreamExt as _;
    use xget::{S3Source, Source};

    let (bucket, key) = rest.split_once('/').unwrap_or((rest, ""));
    if bucket.is_empty() || key.is_empty() {
        eyre::bail!("s3 checksum URL must be s3://bucket/key");
    }
    let source = S3Source::new(bucket, key, endpoint_url.map(str::to_owned)).await;
    let mut stream = source.fetch(None).await?;
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        bytes.extend_from_slice(&chunk?);
    }
    Ok(String::from_utf8(bytes)?)
}

/// Reject an `s3://` checksum URL when the `s3` feature was not compiled in.
#[cfg(not(feature = "s3"))]
async fn fetch_s3_text(_rest: &str, _endpoint_url: Option<&str>) -> eyre::Result<String> {
    eyre::bail!("s3:// checksum URL requires building with --features s3")
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
