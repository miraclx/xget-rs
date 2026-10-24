//! Parsed request intent only, with no discovery, credentials lookup or destination authority.
//!
//! References are access-bearing secrets, not member identities or version evidence. Classification
//! recognizes supported URL shapes; it does not establish that a remote object exists.

use core::fmt;
use core::str::FromStr;
use std::sync::Arc;

use url::Url;
use zeroize::Zeroizing;

#[cfg(test)]
#[path = "input_tests.rs"]
mod tests;

/// One input occurrence, retaining its original reference separately from local grouping intent.
///
/// This value performs no discovery or member selection. It deliberately is not Clone: selected-file
/// re-entry consumes its private provenance instead of implicitly duplicating an input occurrence.
pub struct Input {
    reference: Reference,
    kind: InputKind,
    scope: InputScope,
    namespace: Option<Namespace>,
    // Selected-file re-entry owns metadata only when present, keeping ordinary parsed inputs small.
    pub(crate) selected: Option<Box<crate::inventory::Selected>>,
    pub(crate) member: Option<MemberToken>,
    pub(crate) occurrence: InputOccurrence,
    // Advisory HEAD capability, retained with selected metadata, never inferred from a URL or digest.
    pub(crate) ranges: RangeSupport,
    // Consumed Files from one resolved inventory retain grouping, while separately parsed identical
    // references remain distinct requested occurrences. This is not a persistent member identifier.
    pub(crate) group: Arc<()>,
}

impl Input {
    /// Parses one known reference without network or credential actions.
    ///
    /// HTTP(S), S3 and IPFS references request exact acquisition. Recognized SwissTransfer shares
    /// and GitHub release catalogues retain collection intent even if discovery later finds one file.
    /// Fragments are unsupported, including legacy fragment-based member selectors.
    pub fn parse(reference: &str) -> Result<Self, InputError> {
        // The URL parser can discard whitespace and normalize separators. Reject those spellings
        // rather than classifying a different reference from the one supplied by the caller.
        if reference.is_empty()
            || reference.trim() != reference
            || reference.chars().any(char::is_control)
            || reference.contains('\\')
        {
            return Err(InputError::InvalidReference);
        }
        let reference = Reference::parse(reference)?;
        let parsed = reference.parsed();
        if parsed.cannot_be_a_base() || parsed.host_str().is_none_or(str::is_empty) {
            return Err(InputError::MissingAuthority);
        }
        if !reference
            .original
            .get(parsed.scheme().len()..)
            .and_then(|rest| rest.strip_prefix("://"))
            .is_some_and(|authority| !authority.starts_with('/'))
        {
            return Err(InputError::InvalidReference);
        }
        if matches!(parsed.scheme(), "s3" | "ipfs") && reference.has_dot_path() {
            // URL dot removal cannot silently turn a literal object key/prefix into another one.
            return Err(InputError::UnsupportedObjectPath);
        }
        if parsed.fragment().is_some() {
            return Err(InputError::UnsupportedFragment);
        }
        let (kind, scope) = InputKind::recognize(parsed)?;
        Ok(Self {
            reference,
            kind,
            scope,
            namespace: None,
            selected: None,
            member: None,
            occurrence: InputOccurrence(0),
            ranges: RangeSupport::Unknown,
            group: Arc::new(()),
        })
    }

    /// Requests a namespace/tree explicitly, never as fallback after a missing exact object.
    ///
    /// HTTP(S) means WebDAV listing intent, not website crawling. S3 means literal bucket/prefix
    /// listing intent. IPFS trees and tree scope for provider catalogues/shares are unsupported.
    pub fn tree(reference: &str) -> Result<Self, InputError> {
        let mut input = Self::parse(reference)?;
        match input.kind {
            InputKind::Http | InputKind::S3 => input.scope = InputScope::Tree,
            InputKind::Ipfs | InputKind::SwissTransfer | InputKind::GithubRelease => {
                return Err(InputError::UnsupportedTree { kind: input.kind });
            }
        }
        Ok(input)
    }

    /// Assigns a root-relative output namespace to this occurrence without changing its reference.
    ///
    /// This is grouping intent only. A coordinator must reject its combination with an exact local
    /// file target rather than ignore it. Repeated calls replace the previous local namespace.
    pub fn under(mut self, namespace: &str) -> Result<Self, NamespaceError> {
        self.namespace = Some(Namespace::parse(namespace)?);
        Ok(self)
    }

    /// Requests one repeatable SwissTransfer member token within this input occurrence.
    ///
    /// This is not authorization or an occurrence-local FileId. Fresh discovery must contain exactly
    /// this member; a stale token fails without a path, position or filename fallback. A selected File
    /// already carries its frozen member and cannot be redirected by this method.
    pub fn select_id(mut self, token: &str) -> Result<Self, InputError> {
        if self.kind != InputKind::SwissTransfer || self.selected.is_some() {
            return Err(InputError::UnsupportedMemberSelection);
        }
        self.member = Some(MemberToken::parse(token).ok_or(InputError::InvalidMemberToken)?);
        Ok(self)
    }

    pub(crate) fn duplicate_reference(&self) -> Result<Self, InputError> {
        Ok(Self {
            reference: Reference::parse(self.original_reference())?,
            kind: self.kind,
            scope: self.scope,
            namespace: self
                .namespace
                .as_ref()
                .map(|namespace| Namespace(String::clone(&namespace.0))),
            selected: None,
            member: self.member,
            occurrence: self.occurrence,
            ranges: self.ranges,
            group: Arc::clone(&self.group),
        })
    }

    /// Returns the recognized reference family, not a discovered remote object type.
    pub fn kind(&self) -> InputKind {
        self.kind
    }

    /// Returns the requested exact, collection or explicit tree intent.
    pub fn scope(&self) -> InputScope {
        self.scope
    }

    /// Returns the validated local grouping request, if supplied.
    pub fn namespace(&self) -> Option<&Namespace> {
        self.namespace.as_ref()
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        self.original_reference()
            .len()
            .saturating_add(self.reference.parsed().as_str().len())
            .saturating_add(
                self.namespace
                    .as_ref()
                    .map_or(0, |namespace| namespace.as_str().len()),
            )
            .saturating_add(
                self.selected
                    .as_ref()
                    .map_or(0, |selected| selected.description.retained_bytes()),
            )
    }

    // Deliberately crate-private: transport integration must not turn this into diagnostic data.
    pub(crate) fn parsed_reference(&self) -> &Url {
        self.reference.parsed()
    }

    // Preserve the caller's spelling as well as the parsed URL. Neither is a stable identity.
    pub(crate) fn original_reference(&self) -> &str {
        &self.reference.original
    }
}

impl FromStr for Input {
    type Err = InputError;

    fn from_str(reference: &str) -> Result<Self, Self::Err> {
        Self::parse(reference)
    }
}

impl TryFrom<&str> for Input {
    type Error = InputError;

    fn try_from(reference: &str) -> Result<Self, Self::Error> {
        Self::parse(reference)
    }
}

impl TryFrom<String> for Input {
    type Error = InputError;

    fn try_from(reference: String) -> Result<Self, Self::Error> {
        let reference = Zeroizing::new(reference);
        Self::parse(&reference)
    }
}

impl fmt::Debug for Input {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Input")
            .field("kind", &self.kind)
            .field("scope", &self.scope)
            .field("reference", &"[redacted]")
            .field("has_namespace", &self.namespace.is_some())
            .finish()
    }
}

impl fmt::Display for Input {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("input [redacted]")
    }
}

/// Explicit metadata evidence for identity byte windows, not a promise that any GET is valid.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum RangeSupport {
    /// No explicit accepted HEAD byte-range declaration. This includes unsupported responses.
    #[default]
    Unknown,
    /// HEAD explicitly advertised byte windows; every actual response still needs validation.
    Bytes,
}

/// A recognized reference family, independent of a remote member, version or local name.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InputKind {
    /// HTTP or HTTPS acquisition; explicit tree scope requests WebDAV.
    Http,
    /// S3 exact key or explicit literal bucket/prefix intent.
    S3,
    /// IPFS reference; CID validation and payload/DAG interpretation belong to its adapter.
    Ipfs,
    /// A recognized SwissTransfer share page, not a synthetic selected-file locator.
    SwissTransfer,
    /// A GitHub release catalogue requiring later visible selection/all policy.
    GithubRelease,
}

impl InputKind {
    fn recognize(reference: &Url) -> Result<(Self, InputScope), InputError> {
        match reference.scheme() {
            "http" | "https" => Self::recognize_http(reference),
            "s3" => Ok((Self::S3, InputScope::Exact)),
            "ipfs" => Ok((Self::Ipfs, InputScope::Exact)),
            _ => Err(InputError::UnsupportedScheme),
        }
    }

    fn recognize_http(reference: &Url) -> Result<(Self, InputScope), InputError> {
        let host = reference.host_str();
        if matches!(host, Some("swisstransfer.com" | "www.swisstransfer.com"))
            && (reference.path() == "/dl" || reference.path().starts_with("/dl/"))
        {
            if reference.scheme() != "https"
                || reference.port().is_some()
                || !reference.username().is_empty()
                || reference.password().is_some()
                || reference.query().is_some()
            {
                return Err(InputError::UnsupportedProviderReference);
            }
            let share = reference.path().strip_prefix("/dl/").unwrap_or("");
            let share = share.strip_suffix('/').unwrap_or(share);
            if !Self::is_share_id(share) {
                return Err(InputError::InvalidShare);
            }
            return Ok((Self::SwissTransfer, InputScope::Collection));
        }
        if host == Some("github.com") && Self::is_release_catalogue(reference.path()) {
            if reference.scheme() != "https" || reference.port().is_some() {
                return Err(InputError::UnsupportedProviderReference);
            }
            return Ok((Self::GithubRelease, InputScope::Catalogue));
        }
        Ok((Self::Http, InputScope::Exact))
    }

    fn is_share_id(value: &str) -> bool {
        value.len() == 36
            && value.bytes().enumerate().all(|(index, byte)| match index {
                8 | 13 | 18 | 23 => byte == b'-',
                _ => byte.is_ascii_hexdigit(),
            })
    }

    fn is_release_catalogue(path: &str) -> bool {
        let mut components = path.strip_prefix('/').unwrap_or(path).split('/');
        let Some(owner) = components.next() else {
            return false;
        };
        let Some(repository) = components.next() else {
            return false;
        };
        if owner.is_empty() || repository.is_empty() || components.next() != Some("releases") {
            return false;
        }
        match components.next() {
            None | Some("") => components.next().is_none(),
            Some("latest") => {
                matches!(components.next(), None | Some("")) && components.next().is_none()
            }
            Some("tag") => {
                // Tags may contain slashes. This is only catalogue recognition, not revision proof.
                let Some(tag) = components.next() else {
                    return false;
                };
                !tag.is_empty() && components.all(|component| !component.is_empty())
            }
            _ => false,
        }
    }
}

/// A repeatable SwissTransfer member token. Its meaning is scoped by the original share input.
///
/// It contains only a canonical file UUID, never access credentials. It is not a global identity or
/// the occurrence-local [`crate::inventory::FileId`]. Default diagnostics redact it.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct MemberToken([u8; 36]);

impl MemberToken {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        if !InputKind::is_share_id(value) {
            return None;
        }
        let mut token = [0; 36];
        for (output, byte) in token.iter_mut().zip(value.bytes()) {
            *output = byte.to_ascii_lowercase();
        }
        Some(Self(token))
    }

    /// Canonical literal selector to use with the same share input, not a URL or access capability.
    pub fn as_str(&self) -> &str {
        match core::str::from_utf8(&self.0) {
            Ok(value) => value,
            Err(_) => unreachable!("member tokens contain only validated ASCII UUID bytes"),
        }
    }
}

impl fmt::Debug for MemberToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MemberToken([share-scoped])")
    }
}

/// Frozen request occurrence index, not a remote member identity.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct InputOccurrence(pub(crate) u32);

/// Requested resolution scope, never inferred from a failed exact lookup or inventory cardinality.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InputScope {
    /// One exact reference, including S3 keys with a trailing slash.
    Exact,
    /// Materialization share intent, preserving its context at every cardinality.
    Collection,
    /// Alternatives that require later explicit selection/all policy.
    Catalogue,
    /// Explicit bounded namespace listing intent, not permission for generic crawling.
    Tree,
}

/// A nonempty slash-separated local namespace, not a filesystem path or remote selector.
///
/// No normalization or percent decoding is performed. Managed `.xget*` components and legacy
/// `*.xget` partial names are reserved case-insensitively to prevent local role collisions.
#[derive(Eq, PartialEq)]
pub struct Namespace(String);

impl Namespace {
    fn parse(value: &str) -> Result<Self, NamespaceError> {
        if value.is_empty() {
            return Err(NamespaceError::Empty);
        }
        if value.starts_with('/') {
            return Err(NamespaceError::Absolute);
        }
        if value.contains('\\') || value.contains(':') || value.chars().any(char::is_control) {
            return Err(NamespaceError::InvalidCharacter);
        }
        for component in value.split('/') {
            if component.is_empty() {
                return Err(NamespaceError::EmptyComponent);
            }
            if matches!(component, "." | "..") {
                return Err(NamespaceError::Traversal);
            }
            if Self::is_reserved(component) {
                return Err(NamespaceError::ReservedRole);
            }
        }
        Ok(Self(value.to_owned()))
    }

    fn is_reserved(component: &str) -> bool {
        component
            .get(..5)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(".xget"))
            || component
                .get(component.len().saturating_sub(5)..)
                .is_some_and(|suffix| suffix.eq_ignore_ascii_case(".xget"))
    }

    /// Returns literal root-relative slash components; callers must not URL-decode them.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Namespace {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Namespace([redacted])")
    }
}

/// Reference parsing failures retain only safe typed causes, never raw reference text.
#[derive(Debug, thiserror::Error)]
pub enum InputError {
    /// Empty, padded, control-bearing or backslash-bearing input is not silently normalized.
    #[error("invalid reference spelling; supply an unpadded url without controls or backslashes")]
    InvalidReference,
    /// The URL parser's typed cause contains no caller-supplied text.
    #[error("invalid reference url; check the url syntax")]
    InvalidUrl(#[source] url::ParseError),
    /// Relative/opaque URLs cannot name a supported remote authority.
    #[error("reference needs a remote authority; supply an absolute supported url")]
    MissingAuthority,
    /// No implicit file, arbitrary scheme or provider registry exists.
    #[error("unsupported reference scheme; use http, https, s3 or ipfs")]
    UnsupportedScheme,
    /// URL-normalized dot segments cannot represent a literal S3 key or IPFS path safely here.
    #[error("dot segments in object references are unsupported; supply a literal non-dot path")]
    UnsupportedObjectPath,
    /// A fragment must not accidentally turn a selected member into an entire collection.
    #[error("reference fragments are unsupported; use explicit member selection after resolution")]
    UnsupportedFragment,
    /// A recognized share route must carry a correctly spelled share identifier.
    #[error("invalid share reference; supply a swisstransfer share page url")]
    InvalidShare,
    /// Provider catalogue recognition is restricted to its HTTPS service endpoint.
    #[error("unsupported provider endpoint; use its standard https url")]
    UnsupportedProviderReference,
    /// Only a materialization adapter with stable member tokens supports ID intent.
    #[error("member selection is unsupported for this input; select from its resolved inventory")]
    UnsupportedMemberSelection,
    /// Tokens are literal UUIDs, never row indices or paths.
    #[error("invalid member token; supply the literal token from this share's inventory")]
    InvalidMemberToken,
    /// Tree intent is supported only for HTTP(S)/WebDAV and S3 in this draft.
    #[error("unsupported tree scope; use an exact or collection reference for this input kind")]
    UnsupportedTree {
        /// The safe recognized family, without its access-bearing reference.
        kind: InputKind,
    },
}

/// Namespace errors contain no caller text, even for invalid local grouping requests.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum NamespaceError {
    /// A namespace needs at least one component.
    #[error("empty namespace; supply at least one relative component")]
    Empty,
    /// Grouping never grants root authority.
    #[error("absolute namespace; supply root-relative slash components")]
    Absolute,
    /// Backslashes, colons and controls can change path meaning across hosts.
    #[error("invalid namespace character; omit controls, colons and backslashes")]
    InvalidCharacter,
    /// Leading, repeated and trailing separators are not normalized away.
    #[error("empty namespace component; omit repeated or trailing slashes")]
    EmptyComponent,
    /// Dot components must not acquire meaning at a later filesystem boundary.
    #[error("dot namespace component; supply literal names without dot or traversal components")]
    Traversal,
    /// Managed names cannot double as payload grouping roles.
    #[error("reserved namespace role; choose a name outside the xget-managed family")]
    ReservedRole,
}

// Both retained allocations are secret-bearing. Url owns a normalized serialization, while original
// preserves the caller's exact spelling. Moving Url into String transfers that same allocation in
// url 2; wrapping it before any validation also wipes it on all later error paths. This does not
// promise to wipe the caller's input or temporary allocations internal to the foreign parser.
struct Reference {
    original: Zeroizing<String>,
    parsed: Option<Url>,
}

impl Reference {
    fn parse(value: &str) -> Result<Self, InputError> {
        let parsed = Url::parse(value).map_err(InputError::InvalidUrl)?;
        Ok(Self {
            original: Zeroizing::new(value.to_owned()),
            parsed: Some(parsed),
        })
    }

    fn has_dot_path(&self) -> bool {
        let Some((_, remainder)) = self.original.split_once("://") else {
            return false;
        };
        let hierarchy = remainder.split(['?', '#']).next().unwrap_or("");
        let Some((_, path)) = hierarchy.split_once('/') else {
            return false;
        };
        path.split('/').any(|component| {
            [".", "..", "%2e", ".%2e", "%2e.", "%2e%2e"]
                .into_iter()
                .any(|dot| component.eq_ignore_ascii_case(dot))
        })
    }

    fn parsed(&self) -> &Url {
        match &self.parsed {
            Some(parsed) => parsed,
            None => unreachable!("parsed reference is present until its owner is dropped"),
        }
    }
}

impl Drop for Reference {
    fn drop(&mut self) {
        if let Some(parsed) = self.parsed.take() {
            let _serialization = Zeroizing::new(String::from(parsed));
        }
    }
}
