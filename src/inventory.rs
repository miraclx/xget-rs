//! Complete bounded metadata and selected-file provenance, without transport or filesystem effects.

use core::fmt;
use std::sync::Arc;

use zeroize::Zeroizing;

use crate::checks::Requirements;
use crate::input::{Input, Namespace};
use crate::transfer::{Length, Requirement};

#[cfg(test)]
#[path = "inventory_tests.rs"]
mod tests;

pub(crate) const MAX_FILES: usize = 128;
pub(crate) const MAX_METADATA_BYTES: usize = 1024 * 1024;

/// A complete finite inventory. An incomplete discovery cannot construct this value.
///
/// Exact HTTP returns one file; the optional SwissTransfer adapter returns its complete bounded
/// member inventory. Empty values remain distinct from invalid input and zero-byte file jobs.
/// Unsupported providers do not manufacture successful empty results.
pub struct Inventory {
    files: Vec<File>,
    directories: Vec<Directory>,
    metadata_bytes: usize,
}

// Discovery carries inspection-only directory facts separately from executable file selections.
// No provider scheduler or public discovery framework is introduced.
#[derive(Default)]
pub(crate) struct Discovered {
    pub(crate) files: Vec<File>,
    pub(crate) directories: Vec<Directory>,
}

impl From<Vec<File>> for Discovered {
    fn from(files: Vec<File>) -> Self {
        Self {
            files,
            directories: Vec::new(),
        }
    }
}

impl Inventory {
    pub(crate) fn complete(files: Vec<File>) -> Result<Self, InventoryError> {
        Self::discovered(files.into())
    }

    pub(crate) fn discovered(discovered: Discovered) -> Result<Self, InventoryError> {
        let Discovered { files, directories } = discovered;
        let metadata_bytes = files
            .iter()
            .map(File::retained_bytes)
            .chain(directories.iter().map(Directory::retained_bytes))
            .fold(0_usize, usize::saturating_add);
        if files.len().saturating_add(directories.len()) > MAX_FILES
            || metadata_bytes > MAX_METADATA_BYTES
        {
            return Err(InventoryError::Limit);
        }
        Ok(Self {
            files,
            directories,
            metadata_bytes,
        })
    }

    pub(crate) fn freeze_path_selection(&mut self) {
        for file in &mut self.files {
            file.input.checked_selection = file.input.selection.as_ref().map(Arc::clone);
        }
    }

    /// Only complete, accounted discovery may be filtered; its original charge remains visible.
    pub(crate) fn retain(&mut self, predicate: impl FnMut(&File) -> bool) {
        self.files.retain(predicate);
    }

    /// Inspects selected-file metadata without acquiring a payload body.
    pub fn files(&self) -> impl ExactSizeIterator<Item = &File> {
        self.files.iter()
    }

    /// Inspects genuine directory metadata separately from executable files.
    ///
    /// These entries do not create local directories or file jobs. In particular, an inventory
    /// containing only directory metadata has zero files and executes as a no-op. Directory entries
    /// share the 128-entry and 1 MiB accounted-text limits with files, before file selection. File
    /// include/exclude intent does not remove these inspection facts or create a directory job.
    pub fn directories(&self) -> impl ExactSizeIterator<Item = &Directory> {
        self.directories.iter()
    }

    /// Consumes executable files, preserving each file's private origin/group context.
    /// Inspection-only directory metadata is discarded, never converted to executable file jobs.
    pub fn into_files(self) -> impl ExactSizeIterator<Item = File> {
        self.files.into_iter()
    }

    /// Selects exactly one matching file; neither zero nor multiple matches picks an arbitrary row.
    pub fn select_one(
        self,
        mut predicate: impl FnMut(&File) -> bool,
    ) -> Result<File, SelectionError> {
        let mut selected = None;
        for file in self.files {
            if !predicate(&file) {
                continue;
            }
            if selected.is_some() {
                return Err(SelectionError::MultipleMatches);
            }
            selected = Some(file);
        }
        selected.ok_or(SelectionError::NoMatch)
    }

    /// Accounted discovery reference, pattern, path, hint and validator bytes before literal-member
    /// and input-path filtering, not total process RSS. Neither selection erases the complete scope's
    /// charge, including inspection-only directories. This is the common complete-discovery receipt.
    pub fn metadata_bytes(&self) -> usize {
        self.metadata_bytes
    }
}

impl fmt::Debug for Inventory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Inventory")
            .field("files", &self.files.len())
            .field("directories", &self.directories.len())
            .field("metadata_bytes", &self.metadata_bytes)
            .finish()
    }
}

/// Genuine inspection-only directory metadata, not an executable file or local directory promise.
///
/// S3 folder markers require a zero-byte slash-ending object and conditional HEAD agreement with
/// the listing. WebDAV collections come from complete scoped PROPFIND evidence. Neither establishes
/// an atomic tree snapshot or authorizes restoring empty directories. Original input/group, scoped
/// selector, decoded source path and discovered version evidence remain separate. Default diagnostics
/// redact those names and references.
pub struct Directory {
    pub(crate) input: Input,
    pub(crate) description: Description,
}

impl Directory {
    /// The evidenced directory metadata kind, not a filesystem operation or payload type guess.
    pub fn kind(&self) -> DirectoryKind {
        if self
            .input
            .member
            .as_ref()
            .is_some_and(crate::input::MemberToken::is_webdav)
        {
            DirectoryKind::WebDavCollection
        } else {
            DirectoryKind::S3FolderMarker
        }
    }

    /// Original source-relative directory path without its marker's terminal slash.
    /// Empty means the explicitly requested prefix root, not a normalized absolute path.
    /// This explicit accessor may expose sensitive names; default formatting does not.
    pub fn source_path(&self) -> &str {
        &self.description.source_path
    }

    /// Provider-scoped directory selector: an S3 full key or a WebDAV collection href.
    /// It identifies inspection metadata, not an executable File or authorization capability.
    pub fn member_token(&self) -> Option<&crate::input::MemberToken> {
        self.input.member.as_ref()
    }

    /// Caller-requested original-occurrence grouping, independent of the remote marker key.
    pub fn namespace(&self) -> Option<&Namespace> {
        self.input.namespace()
    }

    /// Discovered strong-validator evidence, not a content hash or atomic tree snapshot.
    /// S3 markers use conditional HEAD evidence; WebDAV collections use PROPFIND evidence.
    pub fn version(&self) -> VersionEvidence {
        if self.description.etag.is_some() {
            VersionEvidence::StrongEtag
        } else {
            VersionEvidence::Unpinned
        }
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        self.input
            .retained_bytes()
            .saturating_add(self.description.retained_bytes())
    }
}

impl fmt::Debug for Directory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Directory")
            .field("kind", &self.kind())
            .field("version", &self.version())
            .field("reference_and_names", &"[redacted]")
            .finish()
    }
}

/// Source evidence behind inspection-only directory metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DirectoryKind {
    /// An actual S3 zero-byte slash-ending object, conditionally checked against listing metadata.
    /// Nonempty slash-ending object payload is not classified as directory metadata.
    S3FolderMarker,
    /// A DAV:collection reported by complete explicit Depth:1 traversal, not an empty-directory job.
    WebDavCollection,
}

/// A selected remote file with its original occurrence and advisory metadata intact.
///
/// A file is not Clone. Moving it back into Input preserves its frozen metadata rather than
/// resolving a mutable reference again. No local output assignment is stored on the remote file.
pub struct File {
    pub(crate) input: Input,
    pub(crate) selected: Selected,
}

impl File {
    /// Occurrence-local correlation only, never a repeatable CLI selector or authorization token.
    pub fn id(&self) -> &FileId {
        &self.selected.id
    }

    /// Inventory source-relative path, independent of local naming or output grouping.
    /// HTTP uses the URL parser's serialized path spelling, not the original full reference.
    /// This explicit accessor can expose sensitive remote names; default formatting does not.
    pub fn source_path(&self) -> &str {
        &self.selected.description.source_path
    }

    /// Advisory URL- or member-path-derived readable name, not a local assignment.
    pub fn name_hint(&self) -> Option<&str> {
        self.selected
            .description
            .name
            .as_deref()
            .map(String::as_str)
    }

    /// Advisory HEAD or manifest length, not an independently pinned representation size.
    ///
    /// Execution checks the GET declaration when present. If GET omits it but repeats the enforced
    /// strong HEAD ETag, the HEAD length remains the effective exact length. See
    /// [`crate::client::OpenedFile::length`] for the consumed response's effective length.
    pub fn length(&self) -> Length {
        self.selected.description.length
    }

    /// States whether metadata or an actual opened provider response supplied a strong ETag.
    /// Subsequent openings must enforce that pin, including after access renewal.
    /// The validator itself is intentionally not a diagnostic/public identity string.
    pub fn version(&self) -> VersionEvidence {
        if self.selected.description.etag.is_some() {
            VersionEvidence::StrongEtag
        } else {
            VersionEvidence::Unpinned
        }
    }

    /// Repeatable adapter member selector, scoped by the original input. Exact HTTP has none.
    /// Reordering a manifest cannot redirect this token to another member.
    pub fn member_token(&self) -> Option<&crate::input::MemberToken> {
        self.input.member.as_ref()
    }

    /// Input-local grouping, distinct from the remote member path.
    pub fn namespace(&self) -> Option<&Namespace> {
        self.input.namespace()
    }

    /// Required whole-file checks, not optional measurement or version evidence.
    pub fn requirements(&self) -> impl Iterator<Item = Requirement> {
        self.selected.description.requirements.iter()
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        self.input
            .retained_bytes()
            .saturating_add(self.selected.description.retained_bytes())
    }
}

impl fmt::Debug for File {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("File")
            .field("reference_and_names", &"[redacted]")
            .field("length", &self.length())
            .field("version", &self.version())
            .finish()
    }
}

impl From<File> for Input {
    fn from(file: File) -> Self {
        let File {
            mut input,
            selected,
        } = file;
        input.selected = Some(Box::new(selected));
        if input.kind() == crate::input::InputKind::GithubRelease {
            input.alternatives = crate::input::Alternatives::Chosen;
        }
        input
    }
}

/// An opaque in-memory occurrence identity. Equality does not compare names, references or bytes.
pub struct FileId(Arc<()>);

impl FileId {
    pub(crate) fn fresh() -> Self {
        Self(Arc::new(()))
    }
}

impl Clone for FileId {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl PartialEq for FileId {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for FileId {}

impl fmt::Debug for FileId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("FileId([occurrence-local])")
    }
}

/// Available version evidence, not a payload digest or authenticity statement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VersionEvidence {
    /// A fresh ordered GET has no continuity claim with earlier metadata.
    Unpinned,
    /// A strong ETag is frozen from HEAD metadata or an actual opened provider response.
    /// Subsequent GET responses must enforce and repeat that pin.
    StrongEtag,
}

/// Selection cardinality failures contain no source names or bearer references.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SelectionError {
    /// No selected member matched the caller's predicate.
    #[error("selection matched no files; inspect the inventory and choose a matching member")]
    NoMatch,
    /// Choosing the first match would silently change requested work.
    #[error("selection matched multiple files; choose one unambiguous member")]
    MultipleMatches,
}

/// A metadata limit produces no executable truncated inventory.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InventoryError {
    /// Retained metadata is bounded by both file count and owned text bytes.
    #[error("inventory exceeds its metadata limit; request a smaller supported scope")]
    Limit,
}

pub(crate) struct Selected {
    pub(crate) id: FileId,
    pub(crate) description: Description,
}

// Paths and validators can contain bearer material too, so retained derivatives are also wiped.
pub(crate) struct Description {
    pub(crate) source_path: Zeroizing<String>,
    pub(crate) name: Option<Zeroizing<String>>,
    pub(crate) length: Length,
    pub(crate) etag: Option<Zeroizing<String>>,
    pub(crate) requirements: Requirements,
}

impl Description {
    pub(crate) fn retained_bytes(&self) -> usize {
        self.source_path
            .len()
            .saturating_add(self.name.as_ref().map_or(0, |name| name.len()))
            .saturating_add(self.etag.as_ref().map_or(0, |etag| etag.len()))
    }
}
