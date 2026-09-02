//! An IPFS byte source: fetch content-addressed data (`ipfs://<cid>`) through an HTTP gateway.
//!
//! Like curl's `ipfs://` support, this resolves the CID to an HTTP gateway request rather than speaking
//! the peer-to-peer protocol: `ipfs://<cid>[/path]` becomes `GET <gateway>/ipfs/<cid>[/path]`, so a
//! trustless gateway's byte-range support carries the engine's parallel scatter, resume, and verify
//! unchanged. The gateway is taken from `--ipfs-gateway`, else the `IPFS_GATEWAY` environment variable,
//! else the running daemon's `~/.ipfs/gateway` file, else a public fallback, mirroring how curl finds one.
//!
//! Unlike curl, which trusts the gateway's bytes, a CID that addresses raw bytes carries the content's
//! own hash, so the download is verified against its own address for free, no checksum supplied out of
//! band. A CID that wraps a file in a UnixFS DAG (the usual case above one block) addresses the DAG root
//! rather than the raw bytes. Verifying one would mean fetching the whole DAG as a CAR and checking each
//! block against its CID, which is a single-stream download with no byte ranges. Rather than give up the
//! parallel, resumable range fetch this engine is built on, a DAG CID is trusted the way curl trusts the
//! gateway, and only a raw CID is verified against its address.

use std::env;
use std::path::PathBuf;

use cid::Cid;
use reqwest::header::HeaderMap;

use crate::{ByteRange, ByteStream, Checksum, Error, HttpSource, Probe, Source};

/// A public gateway used only when no local gateway is configured, so an `ipfs://` URL works out of the
/// box while still preferring a local daemon when one is present.
const DEFAULT_GATEWAY: &str = "https://dweb.link";

/// The multicodec for a raw block, whose CID hashes the content bytes directly (so it is verifiable).
const RAW_CODEC: u64 = 0x55;

/// An IPFS source backed by an HTTP gateway. It delegates transport to an [`HttpSource`] over the
/// gateway URL and, when the CID vouches for the raw bytes, offers that hash for verification.
pub struct IpfsSource {
    http: HttpSource,
    checksum: Option<(Checksum, String)>,
    /// The content-addressed identity of this resource (`<cid>[/path]`), used as the resume validator.
    /// Being the address itself, it is immutable and gateway-independent, so a partial resumes across a
    /// change of gateway and can never stitch a differently-addressed resource.
    identity: String,
}

impl IpfsSource {
    /// Build a source for `reference` (a `<cid>` or `<cid>/path`) through `gateway`, or a gateway
    /// resolved from the environment when `gateway` is `None`.
    pub fn new(reference: &str, gateway: Option<String>) -> Result<Self, Error> {
        let (cid_text, path) = match reference.split_once('/') {
            Some((cid, rest)) => (cid, format!("/{rest}")),
            None => (reference, String::new()),
        };
        let cid = Cid::try_from(cid_text)
            .map_err(|error| Error::detail(&format!("invalid IPFS CID: {error}")))?;
        let gateway = resolve_gateway(gateway);
        let url = format!("{gateway}/ipfs/{cid_text}{path}");
        let http = HttpSource::new(&url, HeaderMap::new())?;
        Ok(Self {
            http,
            checksum: checksum_from_cid(&cid),
            identity: format!("{cid}{path}"),
        })
    }
}

impl Source for IpfsSource {
    fn identity(&self) -> Option<String> {
        Some(format!("ipfs://{}", self.identity))
    }

    async fn probe(&self) -> Result<Probe, Error> {
        let mut probe = self.http.probe().await?;
        // The CID is the content's address; when it hashes the raw bytes, adopt it so the download is
        // verified against its own address (unless the gateway already vouched for something).
        if probe.checksum.is_none() {
            probe.checksum = Option::clone(&self.checksum);
        }
        // The CID addresses the content itself, so it is a stronger, gateway-independent validator than
        // whatever ETag a particular gateway happens to return.
        probe.validator = Some(String::clone(&self.identity));
        Ok(probe)
    }

    async fn fetch(&self, range: Option<ByteRange>) -> Result<ByteStream, Error> {
        self.http.fetch(range).await
    }
}

/// Resolve the gateway base URL, preferring an explicit one, then `IPFS_GATEWAY`, then the local
/// daemon's `~/.ipfs/gateway`, then a public fallback. The trailing slash is trimmed so the path joins
/// cleanly.
pub(crate) fn resolve_gateway(explicit: Option<String>) -> String {
    let chosen = explicit
        .filter(|gateway| !gateway.is_empty())
        .or_else(|| {
            env::var("IPFS_GATEWAY")
                .ok()
                .filter(|gateway| !gateway.is_empty())
        })
        .or_else(gateway_file)
        .unwrap_or_else(|| DEFAULT_GATEWAY.to_owned());
    chosen.trim_end_matches('/').to_owned()
}

/// The gateway a running Kubo daemon advertises in `~/.ipfs/gateway`, if present and non-empty.
fn gateway_file() -> Option<String> {
    let home = env::var_os("HOME")?;
    let text = std::fs::read_to_string(PathBuf::from(home).join(".ipfs/gateway")).ok()?;
    let gateway = text.trim();
    (!gateway.is_empty()).then(|| gateway.to_owned())
}

/// The checksum a CID vouches for, when it addresses raw bytes with a hash the engine can verify. A
/// DAG-wrapped CID (the usual case for a file above one block) addresses the DAG root, not the bytes, so
/// it yields `None`.
pub(crate) fn checksum_from_cid(cid: &Cid) -> Option<(Checksum, String)> {
    if cid.codec() != RAW_CODEC {
        return None;
    }
    let multihash = cid.hash();
    let algorithm = match multihash.code() {
        0x11 => Checksum::Sha1,
        0x12 => Checksum::Sha256,
        0x13 => Checksum::Sha512,
        0x1e => Checksum::Blake3,
        _ => return None,
    };
    Some((algorithm, hex::encode(multihash.digest())))
}
