//! Explicit bounded DAV traversal. Listings never grant a cross-file snapshot or byte-reuse claim.

use roxmltree::{Document, Node, ParsingOptions};
use zeroize::Zeroizing;

use super::http::{Http, Opened};
use super::{DiscoveryBudget, Error};
use crate::checks::Requirements;
use crate::input::{Input, MemberToken};
use crate::inventory::{Description, Directory, Discovered, File, FileId, Selected};
use crate::transfer::{ByteCount, Length};

#[cfg(test)]
#[path = "webdav_tests.rs"]
mod tests;

const MAX_PAGES: usize = 128;
const MAX_DEPTH: usize = 64;
const MAX_CONTROL: usize = 256 * 1024;
const MAX_CONTROL_TOTAL: usize = 1024 * 1024;
const DAV: &str = "DAV:";

pub(super) struct WebDav;

impl WebDav {
    pub(super) async fn resolve(
        http: &Http,
        input: Input,
        budget: &mut DiscoveryBudget,
    ) -> Result<Discovered, Error> {
        let root = Address::root(&input)?;
        let mut pending = vec![MemberToken::clone(&root)];
        let mut seen: Vec<Zeroizing<String>> = Vec::new();
        // Decoded paths detect distinct URI spellings that would assign the same original member.
        // Neither comparison changes the actual frozen href used by transport or selection.
        let mut aliases = Vec::new();
        seen.push(Zeroizing::new(root.as_str().to_owned()));
        aliases.push(Address::decoded(root.as_str())?);
        let mut discovered = Discovered::default();
        let mut pages = 0_usize;
        let mut controls = 0_usize;
        while let Some(directory) = pending.pop() {
            if pages == MAX_PAGES {
                return Err(Error::WebDav(WebDavError::Limit));
            }
            pages += 1;
            let access = Address::access(&input, &directory)?;
            let bytes = http
                .propfind(&access, MAX_CONTROL.min(MAX_CONTROL_TOTAL - controls))
                .await?;
            controls += bytes.len();
            let entries = Listing::parse(&bytes)?;
            let mut self_seen = false;
            for entry in entries {
                let token = Address::href(&input, &root, &entry.href)?;
                if token == directory {
                    if self_seen || entry.kind != Kind::Directory {
                        return Err(Error::WebDav(WebDavError::Incomplete));
                    }
                    self_seen = true;
                    // The requested root has no parent listing to contribute its inspection fact.
                    if token != root {
                        continue;
                    }
                } else {
                    if !Address::immediate(directory.as_str(), token.as_str()) {
                        return Err(Error::WebDav(WebDavError::Scope));
                    }
                    let decoded = Address::decoded(token.as_str())?;
                    if seen.iter().any(|prior| prior.as_str() == token.as_str())
                        || aliases.iter().any(|prior| prior == &decoded)
                    {
                        return Err(Error::WebDav(WebDavError::Duplicate));
                    }
                    seen.push(Zeroizing::new(token.as_str().to_owned()));
                    aliases.push(decoded);
                }
                if (entry.kind == Kind::Directory) != token.as_str().ends_with('/') {
                    return Err(Error::WebDav(WebDavError::Malformed));
                }
                let relative = token
                    .as_str()
                    .strip_prefix(root.as_str())
                    .ok_or(Error::WebDav(WebDavError::Scope))?;
                let path = Address::decoded(relative.strip_suffix('/').unwrap_or(relative))?;
                if !path.is_empty() && path.split('/').count() > MAX_DEPTH {
                    return Err(Error::WebDav(WebDavError::Limit));
                }
                let mut member = input.duplicate_reference()?;
                member.member = Some(MemberToken::clone(&token));
                let name = path
                    .rsplit('/')
                    .next()
                    .filter(|part| !part.is_empty())
                    .map(|part| Zeroizing::new(part.to_owned()));
                let description = Description {
                    source_path: path,
                    name,
                    length: entry.length,
                    etag: entry.etag,
                    requirements: Requirements::default(),
                };
                match entry.kind {
                    Kind::Directory => {
                        if token != root {
                            pending.push(token);
                        }
                        let directory = Directory {
                            input: member,
                            description,
                        };
                        budget.reserve_directory(&directory)?;
                        discovered.directories.push(directory);
                    }
                    Kind::File => {
                        let file = File {
                            input: member,
                            selected: Selected {
                                id: FileId::fresh(),
                                description,
                            },
                        };
                        budget.reserve(&file)?;
                        discovered.files.push(file);
                    }
                }
            }
            if !self_seen {
                return Err(Error::WebDav(WebDavError::Incomplete));
            }
        }
        super::layout::Layout::source_tree(
            discovered.files.iter().map(File::source_path),
            discovered.directories.iter().map(Directory::source_path),
        )?;
        // Literal selection is applied at the common post-discovery seam, alongside path criteria.
        // This keeps every positive criterion's proof over the full original file scope.
        Ok(discovered)
    }

    pub(super) async fn open(http: &Http, file: &File) -> Result<Opened, Error> {
        let root = Address::root(&file.input)?;
        let token = file
            .input
            .member
            .as_ref()
            .filter(|token| token.is_webdav())
            .ok_or(Error::WebDav(WebDavError::Scope))?;
        if !token.as_str().starts_with(root.as_str()) || token.as_str().ends_with('/') {
            return Err(Error::WebDav(WebDavError::Scope));
        }
        let relative = token
            .as_str()
            .strip_prefix(root.as_str())
            .ok_or(Error::WebDav(WebDavError::Scope))?;
        if Address::decoded(relative)?.as_str() != file.source_path() {
            return Err(Error::WebDav(WebDavError::Scope));
        }
        let access = Address::access(&file.input, token)?;
        http.open_reference(file, &access)
            .await
            .map(|(opened, _)| opened)
    }
}

struct Address;

impl Address {
    fn root(input: &Input) -> Result<MemberToken, Error> {
        let url = input.parsed_reference();
        if url.query().is_some() || !url.path().ends_with('/') {
            return Err(Error::WebDav(WebDavError::Scope));
        }
        // Reject URL parser dot removal and path spelling changes, rather than silently selecting
        // another namespace. Credentials remain in the original access reference, never the token.
        let raw = input
            .original_reference()
            .split_once("://")
            .and_then(|(_, rest)| rest.split_once('/'))
            .map(|(_, path)| path)
            .ok_or(Error::WebDav(WebDavError::Scope))?;
        if raw != &url.path()[1..] {
            return Err(Error::WebDav(WebDavError::Scope));
        }
        MemberToken::webdav(url.path()).ok_or(Error::WebDav(WebDavError::Scope))
    }

    fn href(input: &Input, root: &MemberToken, href: &str) -> Result<MemberToken, Error> {
        if href.trim() != href || href.chars().any(char::is_control) || href.contains('\\') {
            return Err(Error::WebDav(WebDavError::Scope));
        }
        let path = if href.starts_with('/') && !href.starts_with("//") {
            href
        } else {
            let url = url::Url::parse(href).map_err(|_| Error::WebDav(WebDavError::Scope))?;
            if url.origin() != input.parsed_reference().origin()
                || !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
            {
                return Err(Error::WebDav(WebDavError::Scope));
            }
            let raw = href
                .split_once("://")
                .and_then(|(_, rest)| rest.split_once('/'))
                .map(|(_, path)| path)
                .ok_or(Error::WebDav(WebDavError::Scope))?;
            if raw != &url.path()[1..] {
                return Err(Error::WebDav(WebDavError::Scope));
            }
            // Borrow the original serialization, not a URL-normalized replacement.
            &href[href.len() - raw.len() - 1..]
        };
        let token = MemberToken::webdav(path).ok_or(Error::WebDav(WebDavError::Scope))?;
        if !token.as_str().starts_with(root.as_str()) {
            return Err(Error::WebDav(WebDavError::Scope));
        }
        Ok(token)
    }

    fn access(input: &Input, token: &MemberToken) -> Result<Input, Error> {
        let mut url = url::Url::clone(input.parsed_reference());
        url.set_path(token.as_str());
        // The ephemeral transport request is not the member's public Input or stored binding.
        let serialization = Zeroizing::new(String::from(url));
        Input::parse(&serialization).map_err(Into::into)
    }

    fn immediate(parent: &str, child: &str) -> bool {
        child.strip_prefix(parent).is_some_and(|suffix| {
            let suffix = suffix.strip_suffix('/').unwrap_or(suffix);
            !suffix.is_empty() && !suffix.contains('/')
        })
    }

    fn decoded(path: &str) -> Result<Zeroizing<String>, Error> {
        percent_encoding::percent_decode_str(path)
            .decode_utf8()
            .map(|path| Zeroizing::new(path.into_owned()))
            .map_err(|_| Error::WebDav(WebDavError::Malformed))
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Kind {
    File,
    Directory,
}

struct Entry {
    href: Zeroizing<String>,
    kind: Kind,
    length: Length,
    etag: Option<Zeroizing<String>>,
}

struct Listing;

impl Listing {
    fn parse(bytes: &[u8]) -> Result<Vec<Entry>, Error> {
        let text =
            core::str::from_utf8(bytes).map_err(|_| Error::WebDav(WebDavError::Malformed))?;
        // roxmltree resolves namespace URIs, not prefix spellings. DTD parsing is disabled explicitly;
        // no external entity resolver or network facility exists at this boundary.
        let document = Document::parse_with_options(
            text,
            ParsingOptions {
                allow_dtd: false,
                nodes_limit: 16384,
                ..ParsingOptions::default()
            },
        )
        .map_err(|_| Error::WebDav(WebDavError::Malformed))?;
        if document.descendants().filter(Node::is_element).any(|node| {
            node.attribute(("http://www.w3.org/XML/1998/namespace", "base"))
                .is_some()
                || node.children().filter(Node::is_text).any(|text| {
                    text.text().is_some_and(|text| !text.trim().is_empty())
                        && node.children().any(|child| child.is_element())
                })
        }) {
            return Err(Error::WebDav(WebDavError::Malformed));
        }
        let root = document.root_element();
        if !Self::dav(root, "multistatus") {
            return Err(Error::WebDav(WebDavError::Malformed));
        }
        let mut entries = Vec::new();
        for child in root.children().filter(Node::is_element) {
            if Self::dav(child, "responsedescription") {
                continue;
            }
            if !Self::dav(child, "response") {
                return Err(Error::WebDav(WebDavError::Incomplete));
            }
            if entries.len() == 129 {
                return Err(Error::WebDav(WebDavError::Limit));
            }
            entries.push(Self::response(child)?);
        }
        Ok(entries)
    }

    fn response(node: Node<'_, '_>) -> Result<Entry, Error> {
        let mut href = None;
        let mut kind = None;
        let mut length = None;
        let mut missing_length = false;
        let mut etag = None;
        let mut etag_seen = false;
        let mut properties = false;
        for child in node.children().filter(Node::is_element) {
            if Self::dav(child, "href") {
                if href.is_some() {
                    return Err(Error::WebDav(WebDavError::Malformed));
                }
                href = Some(Zeroizing::new(Self::scalar(child)?.to_owned()));
                continue;
            }
            if Self::dav(child, "responsedescription") {
                continue;
            }
            if Self::dav(child, "status") {
                Self::status(Self::scalar(child)?)?;
                // Status-only success lacks the required resource facts.
                return Err(Error::WebDav(WebDavError::Incomplete));
            }
            if !Self::dav(child, "propstat") {
                return Err(Error::WebDav(WebDavError::Incomplete));
            }
            properties = true;
            let mut status = None;
            let mut prop = None;
            for item in child.children().filter(Node::is_element) {
                if Self::dav(item, "status") && status.is_none() {
                    status = Some(Self::scalar(item)?);
                } else if Self::dav(item, "prop") && prop.is_none() {
                    prop = Some(item);
                } else if !Self::dav(item, "responsedescription") {
                    return Err(Error::WebDav(WebDavError::Malformed));
                }
            }
            let status = status.ok_or(Error::WebDav(WebDavError::Incomplete))?;
            let prop = prop.ok_or(Error::WebDav(WebDavError::Incomplete))?;
            let code = Self::code(status)?;
            let success = code == 200;
            if !success && (code != 404 || !prop.children().any(|node| node.is_element())) {
                Self::status(status)?;
            }
            for value in prop.children().filter(Node::is_element) {
                // Optional getetag may be absent. Failure of a required property, or an unknown
                // response extension, cannot establish a complete executable listing.
                if !success {
                    if Self::dav(value, "getetag") && Self::code(status)? == 404 && !etag_seen {
                        etag_seen = true;
                        continue;
                    }
                    if Self::dav(value, "getcontentlength")
                        && code == 404
                        && length.is_none()
                        && !missing_length
                    {
                        missing_length = true;
                        continue;
                    }
                    Self::status(status)?;
                    return Err(Error::WebDav(WebDavError::Incomplete));
                }
                if Self::dav(value, "resourcetype") && kind.is_none() {
                    let mut children = value.children().filter(Node::is_element);
                    kind = Some(match children.next() {
                        None => Kind::File,
                        Some(collection)
                            if Self::dav(collection, "collection") && children.next().is_none() =>
                        {
                            Kind::Directory
                        }
                        _ => return Err(Error::WebDav(WebDavError::Malformed)),
                    });
                } else if Self::dav(value, "getcontentlength")
                    && length.is_none()
                    && !missing_length
                {
                    let text = Self::scalar(value)?;
                    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
                        return Err(Error::WebDav(WebDavError::Malformed));
                    }
                    length = Some(ByteCount(
                        text.parse()
                            .map_err(|_| Error::WebDav(WebDavError::Malformed))?,
                    ));
                } else if Self::dav(value, "getetag") && !etag_seen {
                    etag_seen = true;
                    etag = Self::etag(Self::scalar(value)?)?;
                } else {
                    return Err(Error::WebDav(WebDavError::Malformed));
                }
            }
        }
        let kind = kind
            .filter(|_| properties)
            .ok_or(Error::WebDav(WebDavError::Incomplete))?;
        let length = match (kind, length) {
            (Kind::File, Some(length)) => Length::Known(length),
            (Kind::File, None) => return Err(Error::WebDav(WebDavError::Incomplete)),
            (Kind::Directory, _) => Length::Unknown,
        };
        Ok(Entry {
            href: href.ok_or(Error::WebDav(WebDavError::Incomplete))?,
            kind,
            length,
            etag,
        })
    }

    fn dav(node: Node<'_, '_>, name: &str) -> bool {
        node.tag_name().namespace() == Some(DAV) && node.tag_name().name() == name
    }

    fn scalar<'a>(node: Node<'a, '_>) -> Result<&'a str, Error> {
        let mut children = node.children();
        let text = children
            .next()
            .filter(Node::is_text)
            .and_then(|node| node.text())
            .ok_or(Error::WebDav(WebDavError::Malformed))?;
        if children.next().is_some() {
            return Err(Error::WebDav(WebDavError::Malformed));
        }
        Ok(text)
    }

    fn code(status: &str) -> Result<u16, Error> {
        let mut parts = status.splitn(3, ' ');
        if !matches!(parts.next(), Some("HTTP/1.1" | "HTTP/1.0")) {
            return Err(Error::WebDav(WebDavError::Malformed));
        }
        let code = parts
            .next()
            .filter(|code| code.len() == 3 && code.bytes().all(|byte| byte.is_ascii_digit()))
            .ok_or(Error::WebDav(WebDavError::Malformed))?;
        code.parse()
            .map_err(|_| Error::WebDav(WebDavError::Malformed))
    }

    fn status(status: &str) -> Result<(), Error> {
        match Self::code(status)? {
            200 => Ok(()),
            401 | 403 => Err(Error::WebDav(WebDavError::Denied)),
            404 | 410 => Err(Error::WebDav(WebDavError::Missing)),
            _ => Err(Error::WebDav(WebDavError::Incomplete)),
        }
    }

    fn etag(value: &str) -> Result<Option<Zeroizing<String>>, Error> {
        let tag = value.strip_prefix("W/").unwrap_or(value);
        if tag.len() > 8192
            || tag.len() < 2
            || !tag.starts_with('"')
            || !tag.ends_with('"')
            || tag.as_bytes()[1..tag.len() - 1]
                .iter()
                .any(|byte| !(*byte == 0x21 || (0x23..=0x7e).contains(byte)))
        {
            return Err(Error::WebDav(WebDavError::Malformed));
        }
        Ok((!value.starts_with("W/")).then(|| Zeroizing::new(value.to_owned())))
    }
}

/// Safe WebDAV discovery category, without raw XML, credentials, hrefs or server diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum WebDavError {
    /// Requested or returned URI cannot preserve the explicit authorized origin/path scope.
    #[error("webdav scope is invalid; use a slash-ending tree root without query and scoped hrefs")]
    Scope,
    /// A listing repeats a member or introduces an alternate URI spelling of the same path.
    #[error("webdav listing repeats a member; request a nonaliased complete tree")]
    Duplicate,
    /// Untrusted XML or required property syntax cannot establish valid metadata.
    #[error("invalid webdav metadata; check the server's namespace-aware multistatus response")]
    Malformed,
    /// A directory is missing its self entry or has partial/unsupported status or pagination facts.
    #[error("incomplete webdav listing; request a complete depth-one multistatus scope")]
    Incomplete,
    /// Discovery bounds do not authorize a truncated inventory.
    #[error("webdav discovery limit exceeded; request a smaller or shallower tree")]
    Limit,
    /// A multistatus member or required property was denied.
    #[error("webdav member access denied; check authorization for the complete tree")]
    Denied,
    /// A multistatus member or required property is missing, not a successful empty tree.
    #[error("webdav member is missing; discover a current complete tree")]
    Missing,
}
