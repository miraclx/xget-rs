//! Bounded per-input request access headers for HTTP and WebDAV transport.
//!
//! These values never become stored intent, source identity, a fingerprint or a resume key. They
//! are supplied for one original input occurrence, applied to that input, and never persisted in
//! saved runs. Header values are access secrets.
//!
//! Construction validates every field at the edge, so downstream transport code receives already
//! bounded canonical names and printable values. Names are canonical lowercase HTTP token names.
//! Transport-managed names are rejected because range, conditional, framing and content decisions
//! belong to the client and engine, not to per-input access intent.
//!
//! Values are owned in zeroizing allocations. This module cannot wipe the caller's original string
//! or copies made by a foreign parser or request builder; only the allocation it retains is wiped.

use core::fmt;
use core::str::FromStr;
use std::sync::Arc;

use zeroize::Zeroizing;

/// Longest accepted canonical header name, in bytes.
const MAX_NAME_BYTES: usize = 128;

/// Longest accepted header value, in bytes.
const MAX_VALUE_BYTES: usize = 8_192;

/// Most distinct header names one set may hold.
const MAX_HEADERS: usize = 32;

/// Most combined name and value bytes one set may hold.
const MAX_SET_BYTES: usize = 32_768;

/// Names the transport already owns, which per-input access intent may not replace.
const MANAGED_NAMES: [&str; 20] = [
    "host",
    "range",
    "if-range",
    "content-range",
    "if-match",
    "if-none-match",
    "if-modified-since",
    "if-unmodified-since",
    "accept-encoding",
    "content-encoding",
    "content-length",
    "transfer-encoding",
    "content-type",
    "connection",
    "proxy-connection",
    "proxy-authorization",
    "upgrade",
    "te",
    "trailer",
    "depth",
];

#[cfg(test)]
#[path = "headers_tests.rs"]
mod tests;

/// One validated per-input request header.
///
/// The name is a canonical lowercase ASCII HTTP token of at most 128 bytes. The value is printable
/// ASCII from space through tilde, at most 8192 bytes. Values are access secrets: they live in a
/// shared zeroizing allocation, `Clone` shares that allocation, and `Debug` always redacts the
/// value. This type is never stored intent or identity evidence.
pub struct RequestHeader {
    name: HeaderName,
    value: Arc<Zeroizing<String>>,
}

impl RequestHeader {
    /// Builds a header from a name and an exact value.
    ///
    /// The name is canonicalized to lowercase. The value is retained exactly as supplied,
    /// including leading or trailing spaces; only printable ASCII is accepted. The `FromStr`
    /// implementation parses the CLI `Name: value` spelling and omits surrounding ASCII spaces.
    ///
    /// # Errors
    ///
    /// Returns [`HeaderError`] when the name is empty, invalid, too long or transport managed, or
    /// when the value contains a non-printable byte or exceeds 8192 bytes.
    pub fn new(name: &str, value: &str) -> Result<Self, HeaderError> {
        let name = HeaderName::parse(name)?;
        validate_value(value)?;
        Ok(Self {
            name,
            value: Arc::new(Zeroizing::new(value.to_owned())),
        })
    }

    /// Returns the canonical lowercase header name, never its value.
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Borrows the secret value for transport integration; never expose it in diagnostics.
    pub(crate) fn value(&self) -> &str {
        self.value.as_str()
    }

    /// Accounted name and value bytes, used by a bounded set without rescanning.
    pub(crate) fn text_bytes(&self) -> usize {
        self.name.len() + self.value.len()
    }
}

impl Clone for RequestHeader {
    fn clone(&self) -> Self {
        Self {
            name: HeaderName::clone(&self.name),
            value: Arc::clone(&self.value),
        }
    }
}

impl FromStr for RequestHeader {
    type Err = HeaderError;

    /// Parses the CLI spelling `Name: value`.
    ///
    /// The first colon separates name from value, so later colons remain in the value. ASCII spaces
    /// surrounding the name and value are omitted. An empty value is allowed. Name canonicalization
    /// and both bounds still apply.
    ///
    /// # Errors
    ///
    /// Returns [`HeaderError::MissingColon`] when no colon is present, and the same name and value
    /// errors as [`RequestHeader::new`].
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let (name, value) = input.split_once(':').ok_or(HeaderError::MissingColon)?;
        Self::new(name.trim_matches(' '), value.trim_matches(' '))
    }
}

impl fmt::Debug for RequestHeader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RequestHeader")
            .field("name", &self.name.as_str())
            .field("value", &"[redacted]")
            .finish()
    }
}

/// A bounded set of per-input request headers with distinct canonical names.
///
/// A set holds at most 32 distinct headers and at most 32768 combined name and value bytes.
/// [`HeaderSet::set`] consumes the set, replaces a same-name header in place, or appends a new one.
/// An update that would cross either bound returns an error rather than truncating or silently
/// overflowing. Iteration is borrowed and insertion ordered.
#[derive(Clone, Debug, Default)]
pub(crate) struct HeaderSet {
    headers: Vec<RequestHeader>,
    text_bytes: usize,
}

impl HeaderSet {
    /// Sets one header, replacing a same-name entry in place.
    ///
    /// The text check subtracts the previous header's accounted bytes before adding the new
    /// header, so a replacement cannot appear to fit when it does not. A same-name replacement is
    /// allowed at the 32-header count; a new 33rd distinct name is not. No bound is silently
    /// exceeded.
    ///
    /// # Errors
    ///
    /// Returns [`HeaderError::TooManyHeaders`] for a new 33rd distinct header, or
    /// [`HeaderError::TextLimit`] when the combined name and value bytes would exceed 32768.
    pub(crate) fn set(mut self, header: RequestHeader) -> Result<Self, HeaderError> {
        let replacement = header.text_bytes();
        if let Some(index) = self
            .headers
            .iter()
            .position(|existing| existing.name() == header.name())
        {
            let previous = self.headers[index].text_bytes();
            let updated = self
                .text_bytes
                .checked_sub(previous)
                .and_then(|remaining| remaining.checked_add(replacement))
                .ok_or(HeaderError::TextLimit)?;
            if updated > MAX_SET_BYTES {
                return Err(HeaderError::TextLimit);
            }
            self.headers[index] = header;
            self.text_bytes = updated;
            return Ok(self);
        }
        if self.headers.len() >= MAX_HEADERS {
            return Err(HeaderError::TooManyHeaders);
        }
        let updated = self
            .text_bytes
            .checked_add(replacement)
            .ok_or(HeaderError::TextLimit)?;
        if updated > MAX_SET_BYTES {
            return Err(HeaderError::TextLimit);
        }
        self.headers.push(header);
        self.text_bytes = updated;
        Ok(self)
    }

    /// Iterates headers in insertion order; a replaced name keeps its original position.
    pub(crate) fn iter(&self) -> core::slice::Iter<'_, RequestHeader> {
        self.headers.iter()
    }

    /// Returns the number of distinct header names.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.headers.len()
    }

    /// Returns whether the set holds no headers.
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.headers.is_empty()
    }

    /// Returns the accounted combined name and value bytes.
    pub(crate) fn text_bytes(&self) -> usize {
        self.text_bytes
    }
}

/// A validated canonical lowercase HTTP token name.
#[derive(Clone)]
struct HeaderName(String);

impl HeaderName {
    fn parse(spelling: &str) -> Result<Self, HeaderError> {
        if spelling.is_empty() {
            return Err(HeaderError::EmptyName);
        }
        if spelling.len() > MAX_NAME_BYTES {
            return Err(HeaderError::NameTooLong);
        }
        if !spelling.bytes().all(is_token_byte) {
            return Err(HeaderError::InvalidName);
        }
        let mut canonical = String::with_capacity(spelling.len());
        for byte in spelling.bytes() {
            canonical.push(char::from(byte.to_ascii_lowercase()));
        }
        if MANAGED_NAMES.contains(&canonical.as_str()) {
            return Err(HeaderError::ManagedName);
        }
        Ok(Self(canonical))
    }

    fn as_str(&self) -> &str {
        &self.0
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

/// Returns whether `byte` is an HTTP token character.
fn is_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

/// Accepts printable ASCII only, which excludes controls, DEL, newlines and non-ASCII bytes.
fn validate_value(value: &str) -> Result<(), HeaderError> {
    if value.len() > MAX_VALUE_BYTES {
        return Err(HeaderError::ValueTooLong);
    }
    if !value.bytes().all(|byte| matches!(byte, b' '..=b'~')) {
        return Err(HeaderError::InvalidValue);
    }
    Ok(())
}

/// Typed request-header failures. No variant retains the rejected name or value.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum HeaderError {
    /// A header needs at least one token character.
    #[error("empty header name; supply an http token name")]
    EmptyName,
    /// Names accept only HTTP token characters.
    #[error("invalid header name; use ascii token characters")]
    InvalidName,
    /// A bounded name keeps transport parsing and accounting predictable.
    #[error("header name too long; names are limited to 128 bytes")]
    NameTooLong,
    /// Values must not carry controls, DEL, newlines or non-ASCII bytes.
    #[error("invalid header value; use printable ascii without controls or newlines")]
    InvalidValue,
    /// A bounded value keeps per-input secret retention predictable.
    #[error("header value too long; values are limited to 8192 bytes")]
    ValueTooLong,
    /// Range, conditional, framing, proxy and WebDAV depth names belong to the transport.
    #[error("transport-managed header; choose a header the transport does not own")]
    ManagedName,
    /// A set holds only a bounded number of distinct names.
    #[error("too many distinct headers; remove one before adding another")]
    TooManyHeaders,
    /// A set holds only a bounded amount of combined name and value text.
    #[error("header text limit reached; remove or shorten headers")]
    TextLimit,
    /// Per-input headers do not configure a provider-owned access protocol.
    #[error("request headers need an http or webdav input; configure provider access separately")]
    UnsupportedInput,
    /// Explicit Authorization must not compete with URL-userinfo authentication.
    #[error("authorization conflicts with url credentials; use a clean reference with the header")]
    ConflictingAuthorization,
    /// Access headers must not follow a transport projection to another origin.
    #[error("header access changed origin; keep credentials scoped to the original input")]
    OriginChanged,
    /// The CLI spelling needs the colon that separates a name from its value.
    #[error("missing header separator; use `name: value`")]
    MissingColon,
}
