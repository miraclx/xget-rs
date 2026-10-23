//! Filesystem boundary. Submitted blocking closures own their registration through settlement.

use std::path::{Component, Path, PathBuf};

#[cfg(unix)]
use super::MAX_RECORD;
use super::{Fingerprint, Member, Owned, Plan, Record, STORE, SavedError, SavedRun};
use crate::transfer::{OperationOwner, lease_directory, lease_existing_directory};

pub(super) fn absolute(path: &Path) -> Result<PathBuf, SavedError> {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().map_err(SavedError::Io)?.join(path)
    };
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            // Erasing `..` before filesystem resolution can cross a symlink into another root.
            // This entrance deliberately refuses that spelling instead of guessing its authority.
            Component::ParentDir => return Err(SavedError::ParentPath),
            component => normalized.push(component.as_os_str()),
        }
    }
    if normalized.as_os_str().as_encoded_bytes().len() > 4096 {
        return Err(SavedError::Limit);
    }
    Ok(normalized)
}

async fn work<T, F>(operation: F) -> Result<T, SavedError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, SavedError> + Send + 'static,
{
    let runtime = tokio::runtime::Handle::try_current().map_err(|_| SavedError::Runtime)?;
    runtime
        .spawn_blocking(operation)
        .await
        .map_err(|_| SavedError::Io(std::io::Error::other("saved run worker failed")))?
}

pub(super) async fn inspect(path: PathBuf) -> Result<SavedRun, SavedError> {
    work(move || Disk::inspect(path)).await
}

pub(super) async fn checkpoint(
    path: PathBuf,
    bytes: Vec<u8>,
    registration: OperationOwner,
) -> Result<(), SavedError> {
    work(move || {
        let _registration = registration;
        #[cfg(all(test, unix))]
        tests::enter_checkpoint(&path);
        Disk::checkpoint(&path, &bytes)
    })
    .await
}

pub(super) async fn matching(
    root: PathBuf,
    request: Fingerprint,
) -> Result<Option<SavedRun>, SavedError> {
    work(move || Disk::matches(root, request)).await
}

pub(super) async fn create(
    plan: &Plan,
    request: Option<Fingerprint>,
    reentry: crate::client::Reentry,
) -> Result<Owned, SavedError> {
    if cfg!(not(unix)) {
        return Err(SavedError::Unsupported);
    }
    let root = plan.directory_root.as_deref().unwrap_or_else(|| {
        plan.files
            .first()
            .and_then(|file| file.destination.parent())
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new("."))
    });
    let members = plan
        .files
        .iter()
        .map(|file| {
            Ok(Member {
                occurrence: file.file.input.occurrence,
                intent: super::FrozenIntent::capture(&file.file.input)?,
                remote: file
                    .file
                    .input
                    .member
                    .as_ref()
                    .map(|token| super::FrozenMember {
                        token: crate::input::MemberToken::clone(token),
                        source_path: zeroize::Zeroizing::new(file.file.source_path().to_owned()),
                        s3_version: Option::clone(&file.file.input.s3_version),
                        github: file.file.input.github.as_ref().map(std::sync::Arc::clone),
                    }),
                binding: Fingerprint::reference(&file.file.input),
                destination: PathBuf::clone(&file.destination),
                length: file.file.length(),
                etag: Option::clone(&file.file.selected.description.etag),
                requirements: file.requirements,
                measurement: file.measurement,
                mode: crate::client::range::Mode::of(&file.file),
                attempts: Vec::new(),
            })
        })
        .collect::<Result<Vec<_>, SavedError>>()?;
    let root = absolute(root)?;
    let record = work(move || Disk::prepare(root, request, members)).await?;
    let registration = lease_directory(record.root.join(STORE))
        .await
        .map_err(native_error)?;
    let operation_owner = std::sync::Arc::clone(&registration);
    let saved = work(move || {
        let _registration = operation_owner;
        Disk::create(record, reentry)
    })
    .await?;
    Ok(Owned {
        saved,
        registration,
        #[cfg(test)]
        before_completed_checkpoint: None,
    })
}

pub(super) async fn resume(saved: SavedRun) -> Result<Owned, SavedError> {
    let registration = lease_existing_directory(saved.record.root.join(STORE))
        .await
        .map_err(native_error)?;
    let operation_owner = std::sync::Arc::clone(&registration);
    let saved = work(move || {
        let _registration = operation_owner;
        let current = Disk::inspect(saved.path)?;
        if current.snapshot != saved.snapshot {
            return Err(SavedError::Changed);
        }
        Ok(current)
    })
    .await?;
    Ok(Owned {
        saved,
        registration,
        #[cfg(test)]
        before_completed_checkpoint: None,
    })
}

fn native_error(failure: Box<crate::transfer::Failure<core::convert::Infallible>>) -> SavedError {
    match &failure.kind {
        crate::transfer::FailureKind::Busy => SavedError::Busy,
        crate::transfer::FailureKind::Unsupported => SavedError::Unsupported,
        crate::transfer::FailureKind::RuntimeUnavailable => SavedError::Runtime,
        _ => SavedError::Native(failure),
    }
}

struct Disk;

#[cfg(all(test, unix))]
#[path = "disk_tests.rs"]
mod tests;

#[cfg(unix)]
mod unix {
    use core::sync::atomic::{AtomicU64, Ordering};
    use std::fs::{self, File, OpenOptions};
    use std::io::{self, Read as _, Write as _};
    use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _, OpenOptionsExt as _};
    use std::path::{Component, Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{
        Disk, Fingerprint, MAX_RECORD, Member, Record, STORE, SavedError, SavedRun, absolute,
    };
    use crate::client::layout::Layout;
    use crate::client::saved::{RootDevice, RootIdentity, RootInode, codec};

    const MAX_RUNS: usize = 128;
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);

    impl Disk {
        pub(super) fn inspect(path: PathBuf) -> Result<SavedRun, SavedError> {
            let path = absolute(&path)?;
            Self::directory(&path, DirectoryRole::Private)?;
            let path = fs::canonicalize(path).map_err(SavedError::Io)?;
            if !Self::run_name(
                path.file_name()
                    .and_then(|name| name.to_str())
                    .ok_or(SavedError::Corrupt)?,
            ) {
                return Err(SavedError::Corrupt);
            }
            let store = path.parent().ok_or(SavedError::Corrupt)?;
            if store.file_name() != Some(std::ffi::OsStr::new(STORE)) {
                return Err(SavedError::Corrupt);
            }
            let root = store.parent().ok_or(SavedError::Corrupt)?;
            Self::directory(root, DirectoryRole::Root)?;
            Self::directory(store, DirectoryRole::Private)?;
            Self::lease_marker(store)?;
            let file = Self::read_file(&path.join("state"))?;
            let mut bytes = Vec::new();
            file.take(MAX_RECORD as u64 + 1)
                .read_to_end(&mut bytes)
                .map_err(SavedError::Io)?;
            let record = codec::decode(&bytes)?;
            Self::validate(&path, &record)?;
            Ok(SavedRun {
                path,
                record,
                snapshot: Fingerprint::bytes(&bytes),
            })
        }

        pub(super) fn prepare(
            root: PathBuf,
            request: Option<Fingerprint>,
            mut members: Vec<Member>,
        ) -> Result<Record, SavedError> {
            Self::directory(&root, DirectoryRole::Root)?;
            let root = fs::canonicalize(root).map_err(SavedError::Io)?;
            let identity = Self::identity(&root)?;
            for member in &mut members {
                crate::transfer::validate_file_spelling(&member.destination)
                    .map_err(|_| SavedError::Corrupt)?;
                let path = absolute(&member.destination)?;
                let parent = fs::canonicalize(path.parent().ok_or(SavedError::Corrupt)?)
                    .map_err(SavedError::Io)?;
                member.destination = parent.join(path.file_name().ok_or(SavedError::Corrupt)?);
            }
            let store = root.join(STORE);
            Self::create_directory(&store)?;
            if !Self::entries(&store)?.is_empty() {
                Self::lease_marker(&store)?;
            }
            let store_identity = Self::identity(&store)?;
            Ok(Record {
                version: if members.iter().any(|member| {
                    member.intent.is_some()
                        || member
                            .remote
                            .as_ref()
                            .is_some_and(|remote| remote.token.is_s3() || remote.token.is_webdav())
                }) {
                    4
                } else if members
                    .iter()
                    .any(|member| member.mode == crate::client::range::Mode::Ranges)
                {
                    3
                } else {
                    2
                },
                root,
                identity,
                store_identity,
                workspace_identity: RootIdentity {
                    device: RootDevice(0),
                    inode: RootInode(0),
                },
                request,
                complete: false,
                members,
            })
        }

        pub(super) fn create(
            mut record: Record,
            reentry: crate::client::Reentry,
        ) -> Result<SavedRun, SavedError> {
            if let Some(request) = record
                .request
                .filter(|_| matches!(reentry, crate::client::Reentry::Continue))
                && let Some(existing) = Self::matches(PathBuf::clone(&record.root), request)?
            {
                // Another owner may have admitted this request after the read-only pre-discovery
                // scan. Do not create a duplicate or execute the now-stale fresh plan.
                return Err(SavedError::RequestAppeared {
                    resume_path: existing.path,
                });
            }
            let store = record.root.join(STORE);
            if Self::entries(&store)?.len() >= MAX_RUNS {
                return Err(SavedError::Limit);
            }
            // Uniqueness is enforced by mkdir, not by time/counter identity. The name is not authority.
            let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let path = store.join(format!("run-{}-{time:x}-{sequence:x}", std::process::id()));
            fs::DirBuilder::new()
                .mode(0o700)
                .create(&path)
                .map_err(SavedError::Io)?;
            record.workspace_identity = Self::identity(&path)?;
            Self::validate(&path, &record)?;
            let bytes = codec::encode(&record)?;
            Self::checkpoint(&path, &bytes).map_err(|cause| SavedError::Checkpoint {
                saved_run: PathBuf::clone(&path),
                cause: Box::new(cause),
            })?;
            Ok(SavedRun {
                path,
                record,
                snapshot: Fingerprint::bytes(&bytes),
            })
        }

        pub(super) fn matches(
            root: PathBuf,
            request: Fingerprint,
        ) -> Result<Option<SavedRun>, SavedError> {
            let root = match fs::canonicalize(root) {
                Ok(root) => root,
                Err(cause) if cause.kind() == io::ErrorKind::NotFound => return Ok(None),
                Err(cause) => return Err(SavedError::Io(cause)),
            };
            Self::directory(&root, DirectoryRole::Root)?;
            let store = root.join(STORE);
            match fs::symlink_metadata(&store) {
                Err(cause) if cause.kind() == io::ErrorKind::NotFound => return Ok(None),
                Err(cause) => return Err(SavedError::Io(cause)),
                Ok(_) => Self::directory(&store, DirectoryRole::Private)?,
            }
            let mut found: Option<SavedRun> = None;
            let mut ambiguous = Vec::new();
            for path in Self::entries(&store)? {
                let saved = Self::inspect(path)?;
                if saved.record.complete || saved.record.request != Some(request) {
                    continue;
                }
                if found.is_none() && ambiguous.is_empty() {
                    found = Some(saved);
                    continue;
                }
                // Retain only locators once ambiguity is known, not a record-sized allocation for
                // every matching run. Scanning stays bounded even when every run is unfinished.
                if let Some(first) = found.take() {
                    ambiguous.push(first.path);
                }
                ambiguous.push(saved.path);
            }
            if !ambiguous.is_empty() {
                return Err(SavedError::Ambiguous {
                    resume_paths: ambiguous,
                });
            }
            Ok(found)
        }

        fn entries(store: &Path) -> Result<Vec<PathBuf>, SavedError> {
            let mut entries = Vec::new();
            let mut scanned = 0;
            for entry in fs::read_dir(store).map_err(SavedError::Io)? {
                scanned += 1;
                if scanned > MAX_RUNS + 1 {
                    return Err(SavedError::Limit);
                }
                let entry = entry.map_err(SavedError::Io)?;
                let name = entry.file_name();
                if name == "lease" {
                    continue;
                }
                if !name.to_str().is_some_and(Self::run_name) {
                    return Err(SavedError::Corrupt);
                }
                entries.push(entry.path());
            }
            entries.sort();
            Ok(entries)
        }

        fn run_name(name: &str) -> bool {
            name.strip_prefix("run-").is_some_and(|suffix| {
                !suffix.is_empty()
                    && suffix.len() < 100
                    && suffix
                        .bytes()
                        .all(|byte| byte.is_ascii_hexdigit() || byte == b'-')
            })
        }

        fn validate(path: &Path, record: &Record) -> Result<(), SavedError> {
            let root = path
                .parent()
                .and_then(Path::parent)
                .ok_or(SavedError::Corrupt)?;
            if record.root != root
                || absolute(root)? != root
                || Self::identity(root)? != record.identity
                || Self::identity(path.parent().ok_or(SavedError::Corrupt)?)?
                    != record.store_identity
                || Self::identity(path)? != record.workspace_identity
            {
                return Err(SavedError::Corrupt);
            }
            Layout::frozen_paths(
                root,
                record
                    .members
                    .iter()
                    .map(|member| member.destination.as_path()),
            )
            .map_err(|_| SavedError::Corrupt)?;
            let mut attempts = std::collections::BTreeSet::new();
            for member in &record.members {
                let relative = member
                    .destination
                    .strip_prefix(root)
                    .map_err(|_| SavedError::Corrupt)?;
                let mut parent = root.to_path_buf();
                for component in relative.parent().ok_or(SavedError::Corrupt)?.components() {
                    let Component::Normal(component) = component else {
                        return Err(SavedError::Corrupt);
                    };
                    parent.push(component);
                    Self::directory(&parent, DirectoryRole::Root)?;
                }
                for attempt in &member.attempts {
                    let state = attempt.state.path();
                    if state.parent() != member.destination.parent()
                        || !attempts.insert(state.to_path_buf())
                        || !state
                            .file_name()
                            .and_then(|value| value.to_str())
                            .is_some_and(|name| {
                                name.strip_prefix(".xget-native-").is_some_and(|suffix| {
                                    !suffix.is_empty()
                                        && suffix.len() < 100
                                        && suffix
                                            .bytes()
                                            .all(|byte| byte.is_ascii_hexdigit() || byte == b'-')
                                })
                            })
                    {
                        return Err(SavedError::Corrupt);
                    }
                }
            }
            Ok(())
        }

        pub(super) fn checkpoint(path: &Path, bytes: &[u8]) -> Result<(), SavedError> {
            if bytes.len() > MAX_RECORD {
                return Err(SavedError::Limit);
            }
            Self::directory(path, DirectoryRole::Private)?;
            let state = path.join("state");
            match fs::symlink_metadata(&state) {
                Ok(_) => {
                    Self::read_file(&state)?;
                }
                Err(cause) if cause.kind() == io::ErrorKind::NotFound => {}
                Err(cause) => return Err(SavedError::Io(cause)),
            }
            let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let temporary = path.join(format!("checkpoint-{}-{sequence:x}", std::process::id()));
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(&temporary)
                .map_err(SavedError::Io)?;
            file.write_all(bytes).map_err(SavedError::Io)?;
            file.flush().map_err(SavedError::Io)?;
            // Same-directory atomic replacement, deliberately without an fsync durability promise.
            fs::rename(temporary, state).map_err(SavedError::Io)
        }

        fn read_file(path: &Path) -> Result<File, SavedError> {
            let file = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
                .open(path)
                .map_err(|cause| {
                    if cause.kind() == io::ErrorKind::NotFound {
                        SavedError::Missing
                    } else if cause.raw_os_error() == Some(libc::ELOOP) {
                        SavedError::Corrupt
                    } else {
                        SavedError::Io(cause)
                    }
                })?;
            let metadata = file.metadata().map_err(SavedError::Io)?;
            if !metadata.is_file()
                || metadata.uid() != Self::uid()
                || metadata.nlink() != 1
                || metadata.mode() & 0o777 != 0o600
                || metadata.len() > MAX_RECORD as u64
            {
                return Err(SavedError::Corrupt);
            }
            Ok(file)
        }

        fn lease_marker(store: &Path) -> Result<(), SavedError> {
            let file = Self::read_file(&store.join("lease")).map_err(|error| match error {
                SavedError::Missing => SavedError::Corrupt,
                error => error,
            })?;
            let mut marker = Vec::new();
            file.take(128)
                .read_to_end(&mut marker)
                .map_err(SavedError::Io)?;
            if marker != crate::transfer::OWNED_LEASE_MARKER {
                return Err(SavedError::Corrupt);
            }
            Ok(())
        }

        fn create_directory(path: &Path) -> Result<(), SavedError> {
            match fs::DirBuilder::new().mode(0o700).create(path) {
                Ok(()) => {}
                Err(cause) if cause.kind() == io::ErrorKind::AlreadyExists => {}
                Err(cause) => return Err(SavedError::Io(cause)),
            }
            Self::directory(path, DirectoryRole::Private)
        }

        fn directory(path: &Path, role: DirectoryRole) -> Result<(), SavedError> {
            let metadata = fs::symlink_metadata(path).map_err(|cause| {
                if cause.kind() == io::ErrorKind::NotFound {
                    SavedError::Missing
                } else {
                    SavedError::Io(cause)
                }
            })?;
            if !metadata.is_dir()
                || metadata.uid() != Self::uid()
                || metadata.mode() & 0o700 != 0o700
                || metadata.mode() & 0o022 != 0
                || matches!(role, DirectoryRole::Private) && metadata.mode() & 0o777 != 0o700
            {
                return Err(SavedError::Corrupt);
            }
            Ok(())
        }

        fn identity(path: &Path) -> Result<RootIdentity, SavedError> {
            let metadata = fs::symlink_metadata(path).map_err(SavedError::Io)?;
            Ok(RootIdentity {
                device: RootDevice(metadata.dev()),
                inode: RootInode(metadata.ino()),
            })
        }

        fn uid() -> u32 {
            // SAFETY: geteuid reads process identity without pointers or side effects.
            unsafe { libc::geteuid() }
        }
    }
    enum DirectoryRole {
        Root,
        Private,
    }
}

#[cfg(not(unix))]
impl Disk {
    fn inspect(_path: PathBuf) -> Result<SavedRun, SavedError> {
        Err(SavedError::Unsupported)
    }
    fn checkpoint(_path: &Path, _bytes: &[u8]) -> Result<(), SavedError> {
        Err(SavedError::Unsupported)
    }
    fn matches(_root: PathBuf, _request: Fingerprint) -> Result<Option<SavedRun>, SavedError> {
        Err(SavedError::Unsupported)
    }
    fn prepare(
        _root: PathBuf,
        _request: Option<Fingerprint>,
        _members: Vec<Member>,
    ) -> Result<Record, SavedError> {
        Err(SavedError::Unsupported)
    }
    fn create(_record: Record, _reentry: crate::client::Reentry) -> Result<SavedRun, SavedError> {
        Err(SavedError::Unsupported)
    }
}
