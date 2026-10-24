//! Public GitHub release alternatives. Every asset is independently observed, not a release snapshot.

use std::collections::BTreeSet;
use std::sync::Arc;

use zeroize::Zeroizing;

use super::http::{GithubAsset, GithubControl, Http, Opened};
use super::{DiscoveryBudget, Error};
use crate::checks::Requirements;
use crate::input::{GithubEvidence, Input, MemberToken};
use crate::inventory::{Description, Discovered, File, FileId, Selected};
use crate::transfer::{Algorithm, ByteCount, Digest, Length, Provenance};

#[cfg(test)]
#[path = "github_tests.rs"]
mod tests;

const PAGE_SIZE: usize = 30;
const MAX_PAGES: usize = 128;
const MAX_CONTROL: usize = 256 * 1024;
const MAX_TOTAL: usize = 1024 * 1024;

pub(super) struct Github;

impl Github {
    pub(super) async fn resolve(
        http: &Http,
        input: Input,
        budget: &mut DiscoveryBudget,
    ) -> Result<Discovered, Error> {
        let address = Address::input(&input)?;
        let mut controls = Controls {
            http,
            remaining: MAX_TOTAL,
        };
        let release: ReleaseJson = controls.json(&address.release()?).await?;
        if release.id == 0
            || release.draft
            || release.tag_name.is_empty()
            || release.tag_name.len() > 1024
            || release.tag_name.chars().any(char::is_control)
            || matches!(&address.revision, Revision::Tag(tag) if tag.as_str() != release.tag_name)
            || !address.api_matches(&release.url, &format!("/releases/{}", release.id))
            || !address.api_matches(
                &release.assets_url,
                &format!("/releases/{}/assets", release.id),
            )
        {
            return Err(GithubError::Malformed.into());
        }
        let mut discovered = Discovered::default();
        let mut ids = BTreeSet::new();
        // Never trust Release.assets: GitHub embeds a page, not a completeness certificate. A full
        // terminal page is followed by an empty page even if Link is absent. No silent truncation.
        for page in 1..=MAX_PAGES {
            let url = address.page(release.id, page)?;
            let GithubControl { bytes, link } = controls.bytes(&url).await?;
            let assets: Vec<AssetJson> = Json::parse(&bytes)?;
            let next = address.links(
                release.id,
                page,
                assets.len(),
                link.as_deref().map(String::as_str),
            )?;
            if assets.len() > PAGE_SIZE || (assets.len() < PAGE_SIZE && next) {
                return Err(GithubError::Incomplete.into());
            }
            let terminal = assets.len() < PAGE_SIZE;
            for asset in assets {
                let converted = asset.convert(&address, release.id)?;
                if !ids.insert(MemberToken::clone(&converted.token)) {
                    return Err(GithubError::Duplicate.into());
                }
                let mut member = input.duplicate_reference()?;
                member.member = Some(converted.token);
                member.github = Some(Arc::new(converted.evidence));
                let file = File {
                    input: member,
                    selected: Selected {
                        id: FileId::fresh(),
                        description: converted.description,
                    },
                };
                budget.reserve(&file)?;
                discovered.files.push(file);
            }
            if terminal {
                super::layout::Layout::source_paths(
                    discovered.files.iter().map(File::source_path),
                )?;
                return Ok(discovered);
            }
        }
        Err(GithubError::Limit.into())
    }

    pub(super) async fn open(http: &Http, file: &File) -> Result<Opened, Error> {
        let address = Address::input(&file.input)?;
        let token = file
            .input
            .member
            .as_ref()
            .filter(|token| token.is_github())
            .ok_or(GithubError::Scope)?;
        let evidence = file.input.github.as_ref().ok_or(GithubError::Evidence)?;
        let Length::Known(size) = file.length() else {
            return Err(GithubError::Evidence.into());
        };
        let api = address.asset(token.as_str())?;
        let mut controls = Controls {
            http,
            remaining: MAX_CONTROL,
        };
        // Reentry uses the actual frozen asset route, never latest/tag discovery or filename search.
        // A live denial is final. No credential or expired-link login bypass is attempted.
        let asset: AssetJson = controls.json(&api).await?;
        let current = asset.convert(&address, evidence.release.0)?;
        if current.token != *token
            || &current.evidence != evidence.as_ref()
            || current.description.source_path != file.selected.description.source_path
            || current.description.length != file.length()
            || !same_requirements(
                current.description.requirements,
                file.selected.description.requirements,
            )
        {
            return Err(Error::VersionChanged);
        }
        // The signed locator lives only for this one opening; never stored in Input, File or records.
        // Both hops use unauthenticated requests with no cookie jar, automatic redirects or referrer.
        match http.github_asset(&api, size).await? {
            GithubAsset::Body(opened) => Ok(opened),
            GithubAsset::Redirect(location) => {
                let access = address.redirect(&location)?;
                match http.github_asset(&access, size).await? {
                    GithubAsset::Body(opened) => Ok(opened),
                    GithubAsset::Redirect(_) => Err(GithubError::Redirect.into()),
                }
            }
        }
    }
}

fn same_requirements(left: Requirements, right: Requirements) -> bool {
    let source = |required: &crate::transfer::Requirement| {
        required
            .origins
            .iter()
            .any(|origin| origin == Provenance::Source)
    };
    left.iter().filter(source).count() == right.iter().filter(source).count()
        && left.iter().filter(source).all(|required| {
            right
                .iter()
                .filter(source)
                .any(|other| other.digest == required.digest)
        })
}

struct Address {
    owner: String,
    repository: String,
    revision: Revision,
}
enum Revision {
    Latest,
    Tag(Zeroizing<String>),
}

impl Address {
    fn input(input: &Input) -> Result<Self, Error> {
        let url = input.parsed_reference();
        if url.scheme() != "https"
            || url.host_str() != Some("github.com")
            || url.port().is_some()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || input.original_reference() != url.as_str()
        {
            return Err(GithubError::Scope.into());
        }
        let parts: Vec<_> = url.path().trim_end_matches('/').split('/').collect();
        if parts.len() < 5 || parts[3] != "releases" {
            return Err(GithubError::Scope.into());
        }
        let valid = |part: &str| {
            !part.is_empty()
                && part.len() <= 100
                && !matches!(part, "." | "..")
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
        };
        if !valid(parts[1]) || !valid(parts[2]) {
            return Err(GithubError::Scope.into());
        }
        let revision = match parts[4] {
            "latest" if parts.len() == 5 => Revision::Latest,
            "tag" if parts.len() > 5 => {
                let raw = parts[5..].join("/");
                let tag = percent_encoding::percent_decode_str(&raw)
                    .decode_utf8()
                    .map_err(|_| GithubError::Scope)?;
                if tag.is_empty() || tag.len() > 1024 || tag.chars().any(char::is_control) {
                    return Err(GithubError::Scope.into());
                }
                Revision::Tag(Zeroizing::new(tag.into_owned()))
            }
            _ => return Err(GithubError::Scope.into()),
        };
        Ok(Self {
            owner: parts[1].to_owned(),
            repository: parts[2].to_owned(),
            revision,
        })
    }

    fn api(&self, suffix: &str) -> Result<url::Url, Error> {
        url::Url::parse(&format!(
            "https://api.github.com/repos/{}/{}{}",
            self.owner, self.repository, suffix
        ))
        .map_err(|_| GithubError::Scope.into())
    }
    // Repository names are ASCII case-insensitive; authority, route, IDs and query remain exact.
    fn api_matches(&self, value: &str, suffix: &str) -> bool {
        value
            .strip_prefix("https://api.github.com")
            .is_some_and(|path| self.api_path_matches(path, suffix))
    }

    fn api_path_matches(&self, path: &str, suffix: &str) -> bool {
        let Some(path) = path.strip_prefix("/repos/") else {
            return false;
        };
        let Some((owner, path)) = path.split_once('/') else {
            return false;
        };
        let Some((repository, tail)) = path.split_once('/') else {
            return false;
        };
        owner.eq_ignore_ascii_case(&self.owner)
            && repository.eq_ignore_ascii_case(&self.repository)
            && suffix.strip_prefix('/') == Some(tail)
    }

    fn release(&self) -> Result<url::Url, Error> {
        match &self.revision {
            Revision::Latest => self.api("/releases/latest"),
            Revision::Tag(tag) => {
                // A tag is one API path parameter, including embedded slashes.
                let mut encoder =
                    url::Url::parse("https://api.github.com/").map_err(|_| GithubError::Scope)?;
                encoder
                    .path_segments_mut()
                    .map_err(|_| GithubError::Scope)?
                    .push(tag);
                self.api(&format!("/releases/tags/{}", &encoder.path()[1..]))
            }
        }
    }
    fn asset(&self, id: &str) -> Result<url::Url, Error> {
        self.api(&format!("/releases/assets/{id}"))
    }
    fn page(&self, id: u64, page: usize) -> Result<url::Url, Error> {
        self.api(&format!(
            "/releases/{id}/assets?per_page={PAGE_SIZE}&page={page}"
        ))
    }

    fn links(
        &self,
        release: u64,
        page: usize,
        entries: usize,
        link: Option<&str>,
    ) -> Result<bool, Error> {
        if page == 0 || page > MAX_PAGES {
            return Err(GithubError::Limit.into());
        }
        if entries > PAGE_SIZE {
            return Err(GithubError::Incomplete.into());
        }
        let Some(link) = link else { return Ok(false) };
        let mut seen = BTreeSet::new();
        let mut next = false;
        let mut later = false;
        for part in link.split(',') {
            let (url, relation) = part
                .trim()
                .split_once("; rel=\"")
                .ok_or(GithubError::Incomplete)?;
            let relation = relation.strip_suffix('"').ok_or(GithubError::Incomplete)?;
            if !seen.insert(relation) {
                return Err(GithubError::Incomplete.into());
            }
            let url = url
                .strip_prefix('<')
                .and_then(|url| url.strip_suffix('>'))
                .ok_or(GithubError::Incomplete)?;
            let parsed = url::Url::parse(url).map_err(|_| GithubError::Scope)?;
            if parsed.scheme() != "https"
                || parsed.host_str() != Some("api.github.com")
                || parsed.port().is_some()
                || !parsed.username().is_empty()
                || parsed.password().is_some()
                || parsed.fragment().is_some()
                || !self.api_path_matches(parsed.path(), &format!("/releases/{release}/assets"))
            {
                return Err(GithubError::Scope.into());
            }
            let mut destination = None;
            let mut per_page = false;
            for (key, value) in parsed.query_pairs() {
                match key.as_ref() {
                    "page" if destination.is_none() => {
                        destination = Some(
                            value
                                .parse::<usize>()
                                .map_err(|_| GithubError::Incomplete)?,
                        )
                    }
                    "per_page" if !per_page && value == "30" => per_page = true,
                    _ => return Err(GithubError::Incomplete.into()),
                }
            }
            let destination = destination
                .filter(|value| *value > 0 && *value <= MAX_PAGES)
                .ok_or(GithubError::Limit)?;
            if !per_page || parsed.as_str() != url {
                return Err(GithubError::Scope.into());
            }
            match relation {
                "next" if destination == page + 1 => next = true,
                "prev" if page > 1 && destination == page - 1 => {}
                "first" if destination == 1 => {}
                // A probe after a full terminal page can report the prior populated last page.
                // Nonempty short pages cannot use an earlier last page to certify completeness.
                "last"
                    if destination >= page
                        || (entries == 0 && page > 1 && destination == page - 1) =>
                {
                    later = destination > page;
                }
                _ => return Err(GithubError::Incomplete.into()),
            }
        }
        if (later && !next) || (entries < PAGE_SIZE && next) {
            return Err(GithubError::Incomplete.into());
        }
        Ok(next)
    }

    fn redirect(&self, location: &str) -> Result<url::Url, Error> {
        if location.len() > 8192
            || location.chars().any(char::is_control)
            || location.contains('\\')
        {
            return Err(GithubError::Redirect.into());
        }
        let url = url::Url::parse(location).map_err(|_| GithubError::Redirect)?;
        // This is the GitHub release-asset API's dedicated signed-content authority, not arbitrary
        // github.com subdomains, browser download routes, object storage or suffix matching.
        if url.scheme() != "https"
            || url.host_str() != Some("release-assets.githubusercontent.com")
            || url.port().is_some()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
            || url.as_str() != location
            || url.query().is_none()
            || !url.path().starts_with("/github-production-release-asset/")
        {
            return Err(GithubError::Redirect.into());
        }
        let suffix = &url.path()["/github-production-release-asset/".len()..];
        if suffix.is_empty()
            || suffix.len() > 2048
            || suffix.split('/').count() > 4
            || suffix.split('/').any(|part| {
                part.is_empty()
                    || matches!(part, "." | "..")
                    || !part
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
            })
        {
            return Err(GithubError::Redirect.into());
        }
        Ok(url)
    }
}

struct Controls<'a> {
    http: &'a Http,
    remaining: usize,
}
impl Controls<'_> {
    async fn bytes(&mut self, url: &url::Url) -> Result<GithubControl, Error> {
        if self.remaining == 0 {
            return Err(GithubError::Limit.into());
        }
        let result = self
            .http
            .github_control(url, MAX_CONTROL.min(self.remaining))
            .await?;
        self.remaining = self
            .remaining
            .checked_sub(result.bytes.len())
            .ok_or(GithubError::Limit)?;
        Json::bounded(&result.bytes)?;
        Ok(result)
    }
    async fn json<T: serde::de::DeserializeOwned>(&mut self, url: &url::Url) -> Result<T, Error> {
        let GithubControl { bytes, link } = self.bytes(url).await?;
        if link.is_some() {
            return Err(GithubError::Incomplete.into());
        }
        Json::parse(&bytes)
    }
}

struct Json;
impl Json {
    fn bounded(bytes: &[u8]) -> Result<(), Error> {
        if bytes.len() > MAX_CONTROL {
            return Err(GithubError::Limit.into());
        }
        let page = bytes.iter().find(|byte| !byte.is_ascii_whitespace()) == Some(&b'[');
        let mut entries = 0_usize;
        let mut element = true;
        let mut depth = 0_u8;
        let mut quoted = false;
        let mut escaped = false;
        let mut text = 0_usize;
        for byte in bytes {
            if quoted {
                text += 1;
                if text > 16384 {
                    return Err(GithubError::Limit.into());
                }
                if escaped {
                    escaped = false;
                } else if *byte == b'\\' {
                    escaped = true;
                } else if *byte == b'"' {
                    quoted = false;
                }
            } else {
                if page && depth == 1 {
                    if *byte == b',' {
                        element = true;
                    } else if *byte != b']' && !byte.is_ascii_whitespace() && element {
                        entries += 1;
                        if entries > PAGE_SIZE {
                            return Err(GithubError::Limit.into());
                        }
                        element = false;
                    }
                }
                match byte {
                    b'"' => {
                        quoted = true;
                        text = 0;
                    }
                    b'[' | b'{' => {
                        depth = depth
                            .checked_add(1)
                            .filter(|depth| *depth <= 32)
                            .ok_or(GithubError::Limit)?;
                    }
                    b']' | b'}' => {
                        depth = depth.checked_sub(1).ok_or(GithubError::Malformed)?;
                    }
                    _ => {}
                }
            }
        }
        if depth != 0 || quoted {
            return Err(GithubError::Malformed.into());
        }
        Ok(())
    }
    fn parse<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
        Self::bounded(bytes)?;
        serde_json::from_slice(bytes).map_err(|_| GithubError::Malformed.into())
    }
}

#[derive(serde::Deserialize)]
struct ReleaseJson {
    id: u64,
    url: String,
    assets_url: String,
    tag_name: String,
    draft: bool,
}

#[derive(serde::Deserialize)]
struct AssetJson {
    id: u64,
    url: String,
    name: String,
    size: u64,
    state: String,
    updated_at: String,
    digest: Option<String>,
}
struct Converted {
    token: MemberToken,
    evidence: GithubEvidence,
    description: Description,
}
impl AssetJson {
    fn convert(self, address: &Address, release: u64) -> Result<Converted, Error> {
        let token = MemberToken::github(&self.id.to_string()).ok_or(GithubError::Malformed)?;
        let evidence =
            GithubEvidence::parse(release, &self.updated_at).ok_or(GithubError::Malformed)?;
        if !address.api_matches(&self.url, &format!("/releases/assets/{}", token.as_str()))
            || self.state != "uploaded"
            || self.name.contains('/')
            || self.name.len() > 255
        {
            return Err(GithubError::Malformed.into());
        }
        super::layout::Layout::source_path(&self.name)?;
        let mut requirements = Requirements::default();
        if let Some(digest) = self.digest {
            if digest.len() != 71 || !digest.starts_with("sha256:") {
                return Err(GithubError::Malformed.into());
            }
            let digest: Digest = digest.parse().map_err(|_| GithubError::Malformed)?;
            if digest.algorithm() != Algorithm::Sha256 {
                return Err(GithubError::Malformed.into());
            }
            requirements
                .insert(digest, Provenance::Source)
                .map_err(Error::ConflictingRequirements)?;
        }
        Ok(Converted {
            token,
            evidence,
            description: Description {
                source_path: Zeroizing::new(String::clone(&self.name)),
                name: Some(Zeroizing::new(self.name)),
                length: Length::Known(ByteCount(self.size)),
                etag: None,
                requirements,
            },
        })
    }
}

/// Public catalogue failures are sanitized and never retain API bodies, signed URLs or foreign errors.
#[derive(Debug, thiserror::Error)]
pub enum GithubError {
    /// Only explicit public latest/tag references and repository-scoped API routes are supported.
    #[error(
        "unsupported github release scope; use a public https github release latest or tag reference"
    )]
    Scope,
    /// A complete bounded response must match the actual release/asset API model.
    #[error("invalid github release metadata; inspect the public release and retry explicitly")]
    Malformed,
    /// Enumeration cannot safely be treated as complete.
    #[error("incomplete github asset pagination; use a smaller complete release")]
    Incomplete,
    /// The bounded complete catalogue ceiling was exceeded.
    #[error("github catalogue limit exceeded; use a smaller complete release")]
    Limit,
    /// Stable IDs must not be repeated across pages.
    #[error("duplicate github asset identity; inspect the release before retrying")]
    Duplicate,
    /// Frozen reentry is missing genuine asset/revision facts.
    #[error("missing frozen github evidence; use a fresh request at a new destination")]
    Evidence,
    /// Exactly one dedicated GitHub signed-content redirect is supported.
    #[error("unsupported github asset redirect; use a supported public release endpoint")]
    Redirect,
}
