//! Exit policy and audited diagnostics. Foreign causes are retained, not rendered.

use core::convert::Infallible;
use std::path::PathBuf;

use xget::client::{Error, HttpErrorKind, SavedError, SourceError};
use xget::transfer::reader::ReaderError;
use xget::transfer::{ByteCount, FailureKind, Phase, WriteExtent};

#[derive(Debug)]
pub(crate) struct Failure {
    code: u8,
    message: String,
    diagnostic: Option<Box<Diagnostic>>,
    _cause: Option<eyre::Report>,
}

impl Failure {
    pub(crate) fn request(message: &str) -> Self {
        Self {
            code: 2,
            message: message.to_owned(),
            diagnostic: None,
            _cause: None,
        }
    }

    pub(crate) fn runtime(message: &str) -> Self {
        Self {
            code: 1,
            message: message.to_owned(),
            diagnostic: None,
            _cause: None,
        }
    }

    pub(crate) fn io(cause: std::io::Error, message: &str) -> Self {
        Self {
            code: 1,
            message: message.to_owned(),
            diagnostic: None,
            _cause: Some(eyre::Report::new(cause)),
        }
    }

    pub(crate) fn interrupted() -> Self {
        Self {
            code: 130,
            message: "interrupted; submitted work may still settle, inspect retained state before continuing; live bytes cannot be retracted".to_owned(),
            diagnostic: None,
            _cause: None,
        }
    }

    pub(crate) fn signal() -> Self {
        Self::runtime("cannot listen for interrupts; retry in a supported terminal environment")
    }

    pub(crate) fn saved(cause: SavedError) -> Self {
        let code = match cause {
            SavedError::Binding
            | SavedError::NeedsInput
            | SavedError::ParentPath
            | SavedError::Limit
            | SavedError::StoreFull
            | SavedError::Authority
            | SavedError::RestartMissing => 2,
            _ => 1,
        };
        let diagnostic = Diagnostic::saved(&cause);
        Self {
            code,
            message: String::clone(&diagnostic.message),
            diagnostic: Some(Box::new(diagnostic)),
            _cause: Some(eyre::Report::new(cause)),
        }
    }

    pub(crate) fn library(cause: Error) -> Self {
        let cause = match cause {
            Error::Saved(cause) => return Self::saved(cause),
            cause => cause,
        };
        let code = match &cause {
            Error::Input(_)
            | Error::RequestHeader(_)
            | Error::UnsupportedInput { .. }
            | Error::NoInputs
            | Error::SelectionScope
            | Error::Selection
            | Error::PathSelection
            | Error::Inventory(_)
            | Error::AdmissionLimit
            | Error::GlobalRequirementCardinality
            | Error::ConflictingRequirements(_)
            | Error::NamespaceWithFile
            | Error::FileCardinality
            | Error::InvalidName
            | Error::MissingName
            | Error::LayoutLimit
            | Error::LayoutConflict
            | Error::NativeRequest(_)
            | Error::RangeRequest(_) => 2,
            _ => 1,
        };
        let diagnostic = Diagnostic::library(&cause);
        Self {
            code,
            message: String::clone(&diagnostic.message),
            diagnostic: Some(Box::new(diagnostic)),
            _cause: Some(eyre::Report::new(cause)),
        }
    }

    pub(crate) fn message(&self) -> &str {
        &self.message
    }

    pub(crate) fn diagnostic(&self) -> Option<&Diagnostic> {
        self.diagnostic.as_deref()
    }

    pub(crate) fn code(&self) -> i32 {
        i32::from(self.code)
    }
}

/// Presentation-only facts, never executable recovery authority. Both terminal event paths use
/// this allowlist: no Error::source traversal, foreign Display/Debug, URLs or mismatch values.
#[derive(Debug)]
pub(crate) struct Diagnostic {
    pub(crate) reason: Reason,
    pub(crate) message: String,
    pub(crate) phase: Option<Phase>,
    pub(crate) partial: Option<NativePartial>,
    pub(crate) native_state: Option<PathBuf>,
    pub(crate) checkpoint_failed: bool,
}

/// Copy only counts and extent certainty, never finished digest/check values or a receipt.
#[derive(Clone, Copy, Debug)]
pub(crate) struct NativePartial {
    pub(crate) received: ByteCount,
    pub(crate) written: ByteCount,
    pub(crate) hashed: ByteCount,
    pub(crate) write_extent: WriteExtent,
}

impl Diagnostic {
    fn plain(reason: Reason, message: impl Into<String>) -> Self {
        Self {
            reason,
            message: message.into(),
            phase: None,
            partial: None,
            native_state: None,
            checkpoint_failed: false,
        }
    }

    pub(crate) fn library(error: &Error) -> Self {
        let reason = match error {
            Error::Transfer(failure) => return Self::native(failure, Self::source),
            Error::RunCheckpoint { transfer, .. } => {
                let mut diagnostic = Self::native(transfer, Self::source);
                diagnostic.checkpoint_failed = true;
                diagnostic.message.push_str("; aggregate checkpoint also failed, retain both workspaces and inspect them before continuing");
                return diagnostic;
            }
            Error::RangeTransfer(failure) => return Self::native(failure, Self::reader),
            Error::RangeCheckpoint { transfer, .. } => {
                let mut diagnostic = Self::native(transfer, Self::reader);
                diagnostic.checkpoint_failed = true;
                diagnostic.message.push_str("; aggregate checkpoint also failed, retain both workspaces and inspect them before continuing");
                return diagnostic;
            }
            Error::Saved(error) => return Self::saved(error),
            Error::Http(error) => return Self::http(error.kind()),
            #[cfg(feature = "s3")]
            Error::S3(error) => return Self::s3(error.kind()),
            #[cfg(feature = "ipfs")]
            Error::Ipfs(error) => {
                use xget::client::IpfsError;
                let reason = match error {
                    IpfsError::Gateway => Reason::ProviderConfiguration,
                    IpfsError::Reference | IpfsError::Identity(_) => Reason::InvalidInput,
                    IpfsError::Evidence => Reason::ProviderEvidence,
                };
                // This provider enum has only local fixed text, including its Identity arm.
                return Self::plain(reason, error.to_string());
            }
            #[cfg(feature = "webdav")]
            Error::WebDav(error) => {
                use xget::client::WebDavError;
                let reason = match error {
                    WebDavError::Scope => Reason::ProviderScope,
                    WebDavError::Duplicate => Reason::ProviderDuplicate,
                    WebDavError::Malformed => Reason::InvalidMetadata,
                    WebDavError::Incomplete => Reason::ProviderIncomplete,
                    WebDavError::Limit => Reason::InventoryLimit,
                    WebDavError::Denied => Reason::AccessDenied,
                    WebDavError::Missing => Reason::SourceMissing,
                };
                return Self::plain(reason, error.to_string());
            }
            #[cfg(feature = "github")]
            Error::Github(error) => {
                use xget::client::GithubError;
                let reason = match error {
                    GithubError::Scope => Reason::ProviderScope,
                    GithubError::Malformed => Reason::InvalidMetadata,
                    GithubError::Incomplete => Reason::ProviderIncomplete,
                    GithubError::Limit => Reason::InventoryLimit,
                    GithubError::Duplicate => Reason::ProviderDuplicate,
                    GithubError::Evidence => Reason::ProviderEvidence,
                    GithubError::Redirect => Reason::Redirect,
                };
                return Self::plain(reason, error.to_string());
            }
            Error::LayoutConflict => {
                return Self::plain(
                    Reason::LayoutConflict,
                    "output assignments collide; use separate invocations with distinct -o paths, or assign distinct input namespaces through the rust api",
                );
            }
            Error::Input(_) => Reason::InvalidInput,
            Error::RequestHeader(_) => Reason::InvalidHeaders,
            Error::UnsupportedInput { .. } => Reason::UnsupportedInput,
            Error::NoInputs => Reason::NoInputs,
            Error::SelectionScope => Reason::SelectionScope,
            Error::Selection => Reason::Selection,
            Error::PathSelection => Reason::PathSelection,
            Error::Inventory(_) => Reason::InventoryLimit,
            Error::AdmissionLimit => Reason::AdmissionLimit,
            Error::GlobalRequirementCardinality => Reason::RequirementCardinality,
            Error::ConflictingRequirements(_) => Reason::ConflictingRequirements,
            Error::NamespaceWithFile => Reason::NamespaceWithFile,
            Error::FileCardinality => Reason::FileCardinality,
            Error::InvalidName => Reason::InvalidName,
            Error::MissingName => Reason::MissingName,
            Error::LayoutLimit => Reason::LayoutLimit,
            Error::NativeRequest(_) => Reason::InvalidNativeRequest,
            Error::RangeRequest(_) => Reason::InvalidRangeRequest,
            Error::RuntimeUnavailable => Reason::RuntimeUnavailable,
            Error::NativeUnsupported => Reason::Unsupported,
            Error::UnsafeDirectory => Reason::UnsafeDirectory,
            Error::DestinationExists => {
                return Self::plain(
                    Reason::DestinationExists,
                    "destination exists; choose another target, or use --fresh --overwrite only if checked replacement is intended",
                );
            }
            Error::Directory(_) => Reason::DirectoryIo,
            Error::IpfsUnavailable | Error::S3Unavailable => Reason::ProviderUnavailable,
            #[cfg(feature = "swisstransfer")]
            Error::SwissProtocol => Reason::InvalidMetadata,
            Error::HeaderTimeout => Reason::HeaderTimeout,
            Error::DiscoveryTimeout => Reason::DiscoveryTimeout,
            Error::Redirect => Reason::Redirect,
            Error::Status { .. } => Reason::HttpStatus,
            Error::InvalidMetadata => Reason::InvalidMetadata,
            Error::ContentEncoding => Reason::ContentEncoding,
            Error::VersionChanged => Reason::VersionChanged,
            Error::CatalogueChoice => Reason::CatalogueChoice,
        };
        // Only the enumerated local Client leaves above reach Display. Each has audited fixed
        // text (Status adds only its own stage enum and u16); none formats an embedded cause.
        Self::plain(reason, error.to_string())
    }

    fn saved(error: &SavedError) -> Self {
        let reason = match error {
            SavedError::Native(failure) => {
                return Self::native(failure, |never: &Infallible| match *never {});
            }
            SavedError::BindingIntentRequired => {
                return Self::plain(
                    Reason::BindingIntentRequired,
                    "saved binding needs original intent; rerun the original request with its original scope and selection, or reconstruct its namespace and pattern intent through the rust api",
                );
            }
            SavedError::Checkpoint { cause, .. } => {
                let mut diagnostic = Self::saved(cause);
                diagnostic.checkpoint_failed = true;
                diagnostic
                    .message
                    .push_str("; saved checkpoint failed, retain the workspace before continuing");
                return diagnostic;
            }
            SavedError::Authority => Reason::RecoveryAuthority,
            SavedError::RestartMissing => Reason::RestartMissing,
            SavedError::Missing => Reason::SavedMissing,
            SavedError::Corrupt => Reason::InvalidState,
            SavedError::Version => Reason::SavedVersion,
            SavedError::Unsupported => Reason::Unsupported,
            SavedError::Runtime => Reason::RuntimeUnavailable,
            SavedError::Io(_) => Reason::SavedIo,
            SavedError::Busy => Reason::Busy,
            SavedError::NeedsInput => Reason::NeedsInput,
            SavedError::Binding => Reason::Binding,
            SavedError::Ambiguous { .. } => Reason::AmbiguousRun,
            SavedError::RequestAppeared { .. } => Reason::RequestAppeared,
            SavedError::Changed => Reason::SavedChanged,
            SavedError::ParentPath => Reason::ParentPath,
            SavedError::Limit | SavedError::StoreFull => Reason::SavedLimit,
            SavedError::Uncertain => Reason::PublicationUnknown,
            SavedError::Conflict => Reason::SavedConflict,
            SavedError::NotRevalidatable => Reason::NotRevalidatable,
        };
        // SavedError leaves render no I/O cause, path or reference.
        Self::plain(reason, error.to_string())
    }

    fn native<E>(failure: &xget::transfer::Failure<E>, body: fn(&E) -> Self) -> Self {
        let mut diagnostic = Self::native_kind(&failure.kind, body);
        diagnostic.partial = Some(NativePartial {
            received: failure.partial.received,
            written: failure.partial.written,
            hashed: failure.partial.hashed,
            write_extent: failure.partial.write_extent,
        });
        diagnostic.native_state = failure
            .state
            .as_ref()
            .map(|state| state.path().to_path_buf());
        diagnostic
    }

    fn native_kind<E>(kind: &FailureKind<E>, body: fn(&E) -> Self) -> Self {
        match kind {
            FailureKind::Body(error) => body(error),
            FailureKind::LengthMismatch => Self::plain(
                Reason::LengthMismatch,
                "body length does not match its declaration; retain the partial and check the source before continuing",
            ),
            FailureKind::LengthOverflow => Self::plain(
                Reason::LengthOverflow,
                "body exceeds the supported byte count; use a smaller source",
            ),
            FailureKind::DigestMismatch(_) => Self::plain(
                Reason::DigestMismatch,
                "required digest does not match; retain the partial, check the expected checksum and source, and use a new destination for a fresh request",
            ),
            FailureKind::Stopped => Self::plain(
                Reason::Stopped,
                "transfer stopped; inspect retained state before continuing, this is not a completion receipt",
            ),
            FailureKind::CheckpointFailed { cause, .. } => {
                let mut diagnostic = Self::native_kind(cause, body);
                diagnostic.checkpoint_failed = true;
                diagnostic.message.push_str("; native checkpoint also failed, retained counts are not a successful pause receipt");
                diagnostic
            }
            FailureKind::Busy => Self::plain(
                Reason::Busy,
                "native file ownership is busy; wait for active physical work to settle before continuing",
            ),
            FailureKind::DestinationExists => Self::plain(
                Reason::DestinationExists,
                "destination exists; choose an absent target, or use --fresh --overwrite only if checked replacement is intended; retain the old attempt",
            ),
            FailureKind::RuntimeUnavailable => Self::plain(
                Reason::RuntimeUnavailable,
                "native runtime is unavailable; run inside a supported tokio runtime",
            ),
            FailureKind::Unsupported => Self::plain(
                Reason::Unsupported,
                "native publication is unsupported on this host or filesystem; use checked live output with -o -",
            ),
            FailureKind::InvalidState => Self::plain(
                Reason::InvalidState,
                "native state is foreign or invalid; preserve the evidence and choose a new destination",
            ),
            FailureKind::PublicationUnknown { .. } => Self::plain(
                Reason::PublicationUnknown,
                "publication outcome is unknown; inspect and reconcile retained state before retrying, do not assume no file was published",
            ),
            FailureKind::Io { phase, .. } => {
                let mut diagnostic = Self::plain(
                    Reason::NativeIo,
                    "native file operation failed; retain the state and check destination permissions and available space before continuing",
                );
                diagnostic.phase = Some(*phase);
                diagnostic
            }
        }
    }

    fn source(error: &SourceError) -> Self {
        match error {
            SourceError::Http(error) => {
                let kind = error.kind();
                if kind == HttpErrorKind::Request {
                    // Request is the public fallback category, including consumed response-frame
                    // decode failures. This owning body wrapper, not a foreign chain, proves context.
                    return Self::plain(
                        Reason::Http(kind),
                        "http response failed before clean completion; the declared body may be short, retain the partial and check the source before continuing",
                    );
                }
                Self::http(kind)
            }
            #[cfg(feature = "s3")]
            SourceError::S3(error) => Self::s3(error.kind()),
        }
    }

    fn reader(error: &ReaderError<Error>) -> Self {
        match error {
            ReaderError::Source(error) => Self::library(error),
            ReaderError::Window => Self::plain(
                Reason::RangeWindow,
                "range response differs from the requested window; retain the partial and start a fresh request at a new destination",
            ),
            ReaderError::SessionChanged => Self::plain(
                Reason::VersionChanged,
                "range representation or version changed; retain the partial and start a fresh request at a new destination",
            ),
        }
    }

    fn http(kind: HttpErrorKind) -> Self {
        let message = match kind {
            HttpErrorKind::Configuration => {
                "http configuration failed; check the client and access configuration"
            }
            HttpErrorKind::Connect => {
                "http connection failed; check connectivity and the endpoint before continuing"
            }
            HttpErrorKind::Timeout => {
                "http transport timed out; check connectivity and the endpoint before continuing"
            }
            HttpErrorKind::Body => {
                "http body ended with a transport error before clean completion; the declared body may be short, retain the partial and check the source before continuing"
            }
            HttpErrorKind::Request => {
                "http request or response failed; check access and the endpoint before continuing"
            }
        };
        Self::plain(Reason::Http(kind), message)
    }

    #[cfg(feature = "s3")]
    fn s3(kind: xget::client::S3ErrorKind) -> Self {
        use xget::client::S3ErrorKind;
        let (reason, message) = match kind {
            S3ErrorKind::Missing => (
                Reason::SourceMissing,
                "s3 object or bucket is missing; check the exact reference",
            ),
            S3ErrorKind::Denied => (
                Reason::AccessDenied,
                "s3 access denied; check credentials and object permissions",
            ),
            S3ErrorKind::VersionChanged => (
                Reason::VersionChanged,
                "s3 version changed; use a fresh request at a new destination",
            ),
            S3ErrorKind::Transport => (
                Reason::ProviderTransport,
                "s3 transport failed; check sdk configuration and connectivity before continuing",
            ),
            S3ErrorKind::Body => (
                Reason::ProviderBody,
                "s3 body failed before clean completion; retain the partial and check the source before continuing",
            ),
            S3ErrorKind::Deadline => (
                Reason::HeaderTimeout,
                "s3 admission or metadata timed out; release retained bodies or check the endpoint",
            ),
            S3ErrorKind::MetadataLimit => (
                Reason::InventoryLimit,
                "s3 metadata limit exceeded; request a smaller scope",
            ),
            S3ErrorKind::Protocol => (
                Reason::InvalidMetadata,
                "s3 metadata or response is unsupported; check the source and request scope",
            ),
            S3ErrorKind::DirectoryKey => (
                Reason::ProviderScope,
                "s3 tree contains a nonempty directory key; request exact objects instead",
            ),
        };
        Self::plain(reason, message)
    }
}

/// Stable CLI reason spellings come from local variants, never foreign diagnostic text.
#[derive(Debug)]
pub(crate) enum Reason {
    InvalidInput,
    InvalidHeaders,
    UnsupportedInput,
    NoInputs,
    SelectionScope,
    Selection,
    PathSelection,
    InventoryLimit,
    AdmissionLimit,
    RequirementCardinality,
    ConflictingRequirements,
    NamespaceWithFile,
    FileCardinality,
    InvalidName,
    MissingName,
    LayoutLimit,
    LayoutConflict,
    InvalidNativeRequest,
    InvalidRangeRequest,
    RuntimeUnavailable,
    Unsupported,
    UnsafeDirectory,
    DirectoryIo,
    ProviderUnavailable,
    #[cfg(feature = "ipfs")]
    ProviderConfiguration,
    #[cfg(any(feature = "s3", feature = "webdav", feature = "github"))]
    ProviderScope,
    #[cfg(any(feature = "webdav", feature = "github"))]
    ProviderDuplicate,
    #[cfg(any(feature = "webdav", feature = "github"))]
    ProviderIncomplete,
    #[cfg(any(feature = "ipfs", feature = "github"))]
    ProviderEvidence,
    #[cfg(any(feature = "s3", feature = "webdav"))]
    AccessDenied,
    #[cfg(any(feature = "s3", feature = "webdav"))]
    SourceMissing,
    #[cfg(feature = "s3")]
    ProviderTransport,
    #[cfg(feature = "s3")]
    ProviderBody,
    Http(HttpErrorKind),
    HeaderTimeout,
    DiscoveryTimeout,
    Redirect,
    HttpStatus,
    InvalidMetadata,
    ContentEncoding,
    VersionChanged,
    CatalogueChoice,
    RecoveryAuthority,
    RestartMissing,
    SavedMissing,
    SavedVersion,
    SavedIo,
    NeedsInput,
    Binding,
    BindingIntentRequired,
    AmbiguousRun,
    RequestAppeared,
    SavedChanged,
    ParentPath,
    SavedLimit,
    SavedConflict,
    NotRevalidatable,
    LengthMismatch,
    LengthOverflow,
    DigestMismatch,
    Stopped,
    Busy,
    DestinationExists,
    InvalidState,
    PublicationUnknown,
    NativeIo,
    RangeWindow,
}

impl Reason {
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Self::InvalidInput => "InvalidInput",
            Self::InvalidHeaders => "InvalidHeaders",
            Self::UnsupportedInput => "UnsupportedInput",
            Self::NoInputs => "NoInputs",
            Self::SelectionScope => "SelectionScope",
            Self::Selection => "Selection",
            Self::PathSelection => "PathSelection",
            Self::InventoryLimit => "InventoryLimit",
            Self::AdmissionLimit => "AdmissionLimit",
            Self::RequirementCardinality => "RequirementCardinality",
            Self::ConflictingRequirements => "ConflictingRequirements",
            Self::NamespaceWithFile => "NamespaceWithFile",
            Self::FileCardinality => "FileCardinality",
            Self::InvalidName => "InvalidName",
            Self::MissingName => "MissingName",
            Self::LayoutLimit => "LayoutLimit",
            Self::LayoutConflict => "LayoutConflict",
            Self::InvalidNativeRequest => "InvalidNativeRequest",
            Self::InvalidRangeRequest => "InvalidRangeRequest",
            Self::RuntimeUnavailable => "RuntimeUnavailable",
            Self::Unsupported => "Unsupported",
            Self::UnsafeDirectory => "UnsafeDirectory",
            Self::DirectoryIo => "DirectoryIo",
            Self::ProviderUnavailable => "ProviderUnavailable",
            #[cfg(feature = "ipfs")]
            Self::ProviderConfiguration => "ProviderConfiguration",
            #[cfg(any(feature = "s3", feature = "webdav", feature = "github"))]
            Self::ProviderScope => "ProviderScope",
            #[cfg(any(feature = "webdav", feature = "github"))]
            Self::ProviderDuplicate => "ProviderDuplicate",
            #[cfg(any(feature = "webdav", feature = "github"))]
            Self::ProviderIncomplete => "ProviderIncomplete",
            #[cfg(any(feature = "ipfs", feature = "github"))]
            Self::ProviderEvidence => "ProviderEvidence",
            #[cfg(any(feature = "s3", feature = "webdav"))]
            Self::AccessDenied => "AccessDenied",
            #[cfg(any(feature = "s3", feature = "webdav"))]
            Self::SourceMissing => "SourceMissing",
            #[cfg(feature = "s3")]
            Self::ProviderTransport => "ProviderTransport",
            #[cfg(feature = "s3")]
            Self::ProviderBody => "ProviderBody",
            Self::Http(kind) => match kind {
                HttpErrorKind::Configuration => "HttpConfiguration",
                HttpErrorKind::Connect => "HttpConnect",
                HttpErrorKind::Timeout => "HttpTimeout",
                HttpErrorKind::Body => "HttpBody",
                HttpErrorKind::Request => "HttpRequest",
            },
            Self::HeaderTimeout => "HeaderTimeout",
            Self::DiscoveryTimeout => "DiscoveryTimeout",
            Self::Redirect => "Redirect",
            Self::HttpStatus => "HttpStatus",
            Self::InvalidMetadata => "InvalidMetadata",
            Self::ContentEncoding => "ContentEncoding",
            Self::VersionChanged => "VersionChanged",
            Self::CatalogueChoice => "CatalogueChoice",
            Self::RecoveryAuthority => "RecoveryAuthority",
            Self::RestartMissing => "RestartMissing",
            Self::SavedMissing => "SavedMissing",
            Self::SavedVersion => "SavedVersion",
            Self::SavedIo => "SavedIo",
            Self::NeedsInput => "NeedsInput",
            Self::Binding => "Binding",
            Self::BindingIntentRequired => "BindingIntentRequired",
            Self::AmbiguousRun => "AmbiguousRun",
            Self::RequestAppeared => "RequestAppeared",
            Self::SavedChanged => "SavedChanged",
            Self::ParentPath => "ParentPath",
            Self::SavedLimit => "SavedLimit",
            Self::SavedConflict => "SavedConflict",
            Self::NotRevalidatable => "NotRevalidatable",
            Self::LengthMismatch => "LengthMismatch",
            Self::LengthOverflow => "LengthOverflow",
            Self::DigestMismatch => "DigestMismatch",
            Self::Stopped => "Stopped",
            Self::Busy => "Busy",
            Self::DestinationExists => "DestinationExists",
            Self::InvalidState => "InvalidState",
            Self::PublicationUnknown => "PublicationUnknown",
            Self::NativeIo => "NativeIo",
            Self::RangeWindow => "RangeWindow",
        }
    }
}
