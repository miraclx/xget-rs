//! One physical-operation owner. Every submission moves an Arc registration into the blocking
//! closure itself; aborting an async waiter cannot release its descriptors or either lease early.

use core::ffi::CStr;
use core::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::ffi::{CString, OsStr};
use std::fs::{File, Metadata, OpenOptions};
use std::io::{self, Read};
use std::os::fd::{AsRawFd, FromRawFd as _, RawFd};
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::{FileExt as _, MetadataExt as _, OpenOptionsExt as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use tokio::runtime::Handle;

use super::plan::Plan;
use super::record::{
    Binding, DeviceId, Identity, InodeId, InvalidRecord, MAX_RECORD, Record, Status,
};
use super::{
    AcquisitionFailure, ByteCount, FailureKind, Measurement, OperationOwner, Partial, Phase,
    Receipt, Reconciliation, RecoveryBinding, StateRef,
};
use crate::checks::{CheckPolicy, Requirements};

const DIRECTORY_LOCK: &CStr = c".xget-native.lock";
const DIRECTORY_MARKER: &[u8] = b"XGETNF-DIRECTORY-1\n";
const WORKSPACE_MARKER: &[u8] = b"XGETNF-WORKSPACE-1\n";
const DATA: &CStr = c"data";
const STATE: &CStr = c"state";
const RECEIVING: u8 = 0;
const SEALED: u8 = 1;
const PUBLISHING: u8 = 2;
const PUBLISHED: u8 = 3;
static CHECKPOINT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub(super) struct Native(Arc<Lease>);

impl Native {
    pub(super) async fn acquire(plan: Plan) -> Result<Self, AcquisitionFailure> {
        Self::acquire_registered(plan, None).await
    }

    pub(super) async fn acquire_registered(
        plan: Plan,
        registration: Option<OperationOwner>,
    ) -> Result<Self, AcquisitionFailure> {
        // Shared managed roles cannot become payloads through another native entrance. Reader plans
        // are also checked here without changing the reader implementation or acquiring any authority.
        if super::is_run_store(&plan.parent) || super::is_run_store(Path::new(&plan.destination)) {
            return Err(AcquisitionFailure {
                error: NativeError::InvalidState,
                state: None,
            });
        }
        // Do not panic or create a hidden runtime when a caller uses a different executor.
        let runtime = Handle::try_current().map_err(|_| AcquisitionFailure {
            error: NativeError::RuntimeUnavailable,
            state: None,
        })?;
        let owner_runtime = Handle::clone(&runtime);
        runtime
            .spawn_blocking(move || Self::open(plan, owner_runtime, registration))
            .await
            .map_err(|cause| AcquisitionFailure {
                error: NativeError::io(Phase::Acquire, io::Error::other(cause)),
                state: None,
            })?
    }

    fn open(
        plan: Plan,
        runtime: Handle,
        registration: Option<OperationOwner>,
    ) -> Result<Self, AcquisitionFailure> {
        let mut retained = None;
        let result = (|| {
            #[cfg(test)]
            tests::enter_registered_work(plan.state.path(), tests::PhysicalOperation::Acquire);
            let directory = DirectoryLease::acquire(&plan.parent)?;
            let workspace_name = Name::from_os(&plan.workspace)?;
            directory.parent.mkdir(&workspace_name)?;
            retained = Some(StateRef::clone(&plan.state));
            let workspace = directory
                .parent
                .directory(&workspace_name, Phase::Acquire)?;
            workspace.check_private_directory(Phase::Acquire)?;
            let workspace_lock =
                workspace.lock(c"lease", WORKSPACE_MARKER, LockAccess::New, Phase::Acquire)?;
            let data = workspace.open(
                DATA,
                libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
                0o600,
                Phase::Acquire,
            )?;
            let data_metadata = data
                .metadata()
                .map_err(|cause| NativeError::io(Phase::Acquire, cause))?;
            Role::Data.check(&data_metadata)?;
            let binding = Binding {
                parent: directory.parent.identity(Phase::Acquire)?,
                workspace: workspace.identity(Phase::Acquire)?,
                data: Identity::of(&data_metadata),
                destination: plan.destination.as_os_str().as_bytes().to_vec(),
                checks: plan.checks,
            };
            #[cfg(test)]
            let released = Arc::new(tokio::sync::Notify::new());
            let native = Self(Arc::new(Lease {
                directory,
                workspace,
                _workspace_lock: workspace_lock,
                data,
                binding,
                runtime,
                mode: AtomicU8::new(RECEIVING),
                initialized: AtomicBool::new(false),
                _registration: registration,
                #[cfg(test)]
                hooks: Hooks {
                    released: Arc::clone(&released),
                    ..Hooks::default()
                },
                #[cfg(test)]
                _released: Released(released),
            }));
            native.0.checkpoint(Status::NeedsBody, Partial::default())?;
            Ok(native)
        })();
        result.map_err(|error| AcquisitionFailure {
            error,
            state: retained,
        })
    }

    /// Callers submit disjoint settled-prefix writes, bounded before submission, and settle every
    /// submission before sealing. A published witness is never reopened for writable recovery.
    pub(super) async fn write(
        &self,
        offset: ByteCount,
        bytes: Bytes,
    ) -> Result<ByteCount, WriteFailure> {
        let lease = Arc::clone(&self.0);
        self.0
            .runtime
            .spawn_blocking(move || {
                let ByteCount(offset) = offset;
                if lease.mode.load(Ordering::Acquire) != RECEIVING
                    || bytes.len() > 64 * 1024
                    || offset.checked_add(bytes.len() as u64).is_none()
                {
                    return Err(WriteFailure {
                        accepted: Some(ByteCount(0)),
                        error: NativeError::InvalidState,
                    });
                }
                #[cfg(test)]
                lease.hooks.write.enter();
                let mut accepted = 0;
                while accepted < bytes.len() {
                    let end = bytes.len();
                    #[cfg(test)]
                    let end = if lease.hooks.fail_write_prefix.load(Ordering::Acquire)
                        || lease.hooks.lose_write_result.load(Ordering::Acquire)
                    {
                        accepted + 1
                    } else {
                        end
                    };
                    match lease
                        .data
                        .write_at(&bytes[accepted..end], offset + accepted as u64)
                    {
                        Ok(0) => {
                            return Err(WriteFailure {
                                accepted: Some(ByteCount(accepted as u64)),
                                error: NativeError::io(
                                    Phase::Write,
                                    io::ErrorKind::WriteZero.into(),
                                ),
                            });
                        }
                        Ok(count) => accepted += count,
                        Err(cause) if cause.kind() == io::ErrorKind::Interrupted => continue,
                        Err(cause) => {
                            return Err(WriteFailure {
                                accepted: Some(ByteCount(accepted as u64)),
                                error: NativeError::io(Phase::Write, cause),
                            });
                        }
                    }
                    #[cfg(test)]
                    if lease.hooks.lose_write_result.load(Ordering::Acquire) {
                        // A real prefix reached the file, but no accepted count reaches the waiter.
                        panic!("injected lost physical write result");
                    }
                    #[cfg(test)]
                    if lease.hooks.fail_write_prefix.load(Ordering::Acquire) {
                        return Err(WriteFailure {
                            accepted: Some(ByteCount(accepted as u64)),
                            error: NativeError::io(
                                Phase::Write,
                                io::Error::other("injected write failure"),
                            ),
                        });
                    }
                }
                Ok(ByteCount(accepted as u64))
            })
            .await
            .map_err(|cause| WriteFailure {
                // A panicked blocking worker has no confirmed extent; recovery requires a fresh body.
                accepted: None,
                error: NativeError::io(Phase::Write, io::Error::other(cause)),
            })?
    }

    /// Read only an already-settled contiguous stage prefix. The range coordinator supplies this
    /// authority after write acknowledgement, never from received-byte counters or sparse file size.
    /// The registration owns the descriptor and both leases through queued/running reads and drop.
    pub(super) async fn read_prefix(
        &self,
        start: ByteCount,
        end: ByteCount,
    ) -> Result<Vec<u8>, NativeError> {
        self.submit(Phase::ReadStage, move |lease| {
            #[cfg(test)]
            lease.hooks.read.enter();
            let length = end
                .0
                .checked_sub(start.0)
                .ok_or(NativeError::InvalidState)?;
            if lease.mode.load(Ordering::Acquire) != RECEIVING || length == 0 || length > 64 * 1024
            {
                return Err(NativeError::InvalidState);
            }
            let mut bytes = vec![0; length as usize];
            lease
                .data
                .read_exact_at(&mut bytes, start.0)
                .map_err(|cause| NativeError::io(Phase::ReadStage, cause))?;
            Ok(bytes)
        })
        .await
    }

    pub(super) async fn seal(&self, length: ByteCount) -> Result<(), NativeError> {
        self.submit(Phase::Seal, move |lease| {
            lease
                .mode
                .compare_exchange(RECEIVING, SEALED, Ordering::AcqRel, Ordering::Acquire)
                .map_err(|_| NativeError::InvalidState)?;
            #[cfg(test)]
            lease.hooks.seal.enter();
            lease.validate_data(length)?;
            Ok(())
        })
        .await
    }

    pub(super) async fn checkpoint(&self, partial: Partial) -> Result<(), NativeError> {
        self.submit(Phase::Checkpoint, move |lease| {
            lease.checkpoint(Status::NeedsBody, partial)
        })
        .await
    }

    pub(super) async fn publish(&self, partial: Partial) -> Result<(), NativeError> {
        self.submit(Phase::Publish, move |lease| {
            if lease.mode.load(Ordering::Acquire) != SEALED
                || !partial.complete_for(lease.binding.checks)
            {
                return Err(NativeError::InvalidState);
            }
            lease.validate_data(partial.written)?;
            lease.checkpoint(Status::Sealed, partial)?;
            // Once intent is attempted no API returns this inode to the receive/reset state.
            lease.mode.store(PUBLISHING, Ordering::Release);
            lease.checkpoint(Status::Publishing, partial)?;
            #[cfg(test)]
            lease.hooks.commit.enter();
            lease.link()?;
            #[cfg(test)]
            if lease.hooks.lose_publish_ack.load(Ordering::Acquire) {
                return Err(NativeError::PublicationUnknown(io::Error::other(
                    "injected lost publication acknowledgement",
                )));
            }
            lease
                .checkpoint(Status::Published, partial)
                .map_err(|error| NativeError::PublicationUnknown(error.into_io()))?;
            lease.mode.store(PUBLISHED, Ordering::Release);
            Ok(())
        })
        .await
    }

    async fn submit<T, F>(&self, phase: Phase, operation: F) -> Result<T, NativeError>
    where
        T: Send + 'static,
        F: FnOnce(&Lease) -> Result<T, NativeError> + Send + 'static,
    {
        let lease = Arc::clone(&self.0);
        self.0
            .runtime
            .spawn_blocking(move || operation(&lease))
            .await
            .map_err(|cause| {
                let cause = io::Error::other(cause);
                if phase == Phase::Publish {
                    NativeError::PublicationUnknown(cause)
                } else {
                    NativeError::io(phase, cause)
                }
            })?
    }

    pub(super) async fn lease_directory(path: PathBuf) -> Result<OperationOwner, NativeError> {
        Self::lease_directory_with(path, LockAccess::Create).await
    }

    pub(super) async fn lease_existing_directory(
        path: PathBuf,
    ) -> Result<OperationOwner, NativeError> {
        Self::lease_directory_with(path, LockAccess::Existing).await
    }

    async fn lease_directory_with(
        path: PathBuf,
        access: LockAccess,
    ) -> Result<OperationOwner, NativeError> {
        let runtime = Handle::try_current().map_err(|_| NativeError::RuntimeUnavailable)?;
        runtime
            .spawn_blocking(move || {
                // This is a workspace lease, not the destination-directory lock used by file attempts.
                // Reuse the final-owner acquiring-PID guard and stable marker validation.
                let file = OpenOptions::new()
                    .read(true)
                    .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
                    .open(path)
                    .map_err(|cause| NativeError::io(Phase::Acquire, cause))?;
                let directory = Directory(file);
                directory.check_private_directory(Phase::Acquire)?;
                let lock =
                    directory.lock(c"lease", super::OWNED_LEASE_MARKER, access, Phase::Acquire)?;
                Ok(Arc::new(OwnedDirectory {
                    _directory: directory,
                    _lock: lock,
                }) as OperationOwner)
            })
            .await
            .map_err(|cause| NativeError::io(Phase::Acquire, io::Error::other(cause)))?
    }

    pub(super) async fn reconcile(state: StateRef) -> Result<Reconciliation, NativeError> {
        Self::reconcile_registered(state, None, None).await
    }

    pub(super) async fn reconcile_registered(
        state: StateRef,
        registration: Option<OperationOwner>,
        binding: Option<RecoveryBinding>,
    ) -> Result<Reconciliation, NativeError> {
        let runtime = Handle::try_current().map_err(|_| NativeError::RuntimeUnavailable)?;
        runtime
            .spawn_blocking(move || {
                let _registration = registration;
                #[cfg(test)]
                tests::enter_registered_work(state.path(), tests::PhysicalOperation::Reconcile);
                Self::reconcile_bound(state, binding)
            })
            .await
            .map_err(|cause| NativeError::io(Phase::Reconcile, io::Error::other(cause)))?
    }

    fn reconcile_bound(
        state: StateRef,
        expected: Option<RecoveryBinding>,
    ) -> Result<Reconciliation, NativeError> {
        let parent = state.path().parent().ok_or(NativeError::InvalidState)?;
        let workspace_name = state.path().file_name().ok_or(NativeError::InvalidState)?;
        if !workspace_name.as_bytes().starts_with(b".xget-native-") {
            return Err(NativeError::InvalidState);
        }
        let directory = DirectoryLease::reacquire(parent)?;
        let workspace = directory
            .parent
            .directory(&Name::from_os(workspace_name)?, Phase::Reconcile)?;
        workspace.check_private_directory(Phase::Reconcile)?;
        let _workspace_lock = workspace.lock(
            c"lease",
            WORKSPACE_MARKER,
            LockAccess::Existing,
            Phase::Reconcile,
        )?;
        let record = workspace.read_record(Phase::Reconcile)?;
        if let Some(expected) = expected
            && (expected.destination.parent() != Some(parent)
                || expected.destination.file_name().map(OsStr::as_bytes)
                    != Some(record.binding.destination.as_slice())
                || expected.checks != record.binding.checks)
        {
            return Err(NativeError::InvalidState);
        }
        if record.binding.parent != directory.parent.identity(Phase::Reconcile)?
            || record.binding.workspace != workspace.identity(Phase::Reconcile)?
        {
            return Err(NativeError::InvalidState);
        }
        let has_intent = matches!(record.status, Status::Publishing | Status::Published);
        // Recovery opens the stage read-only. A final hardlink must never become a writable partial.
        let mut data = match workspace.open(DATA, libc::O_RDONLY, 0, Phase::Reconcile) {
            Ok(data) => data,
            Err(NativeError::Io { cause, .. })
                if has_intent && cause.kind() == io::ErrorKind::NotFound =>
            {
                return Ok(Reconciliation::PublicationUnknown(state));
            }
            Err(NativeError::Io { cause, .. })
                if has_intent && cause.raw_os_error() == Some(libc::ELOOP) =>
            {
                return Ok(Reconciliation::Conflict(state));
            }
            Err(error) => return Err(error),
        };
        let metadata = data
            .metadata()
            .map_err(|cause| NativeError::io(Phase::Reconcile, cause))?;
        if Role::Data.check(&metadata).is_err() || record.binding.data != Identity::of(&metadata) {
            return if has_intent {
                Ok(Reconciliation::Conflict(state))
            } else {
                Err(NativeError::InvalidState)
            };
        }
        if matches!(record.status, Status::NeedsBody | Status::Sealed) {
            return Ok(Reconciliation::NeedsBody {
                state,
                partial: record.partial,
            });
        }
        if metadata.len() != record.partial.written.0 {
            return Ok(Reconciliation::PublicationUnknown(state));
        }
        let destination = Name::from_bytes(&record.binding.destination)?;
        let metadata = match directory.parent.metadata(&destination, Phase::Reconcile) {
            Ok(metadata) => metadata,
            Err(NativeError::Io { cause, .. }) if cause.kind() == io::ErrorKind::NotFound => {
                return Ok(Reconciliation::PublicationUnknown(state));
            }
            Err(NativeError::Io { cause, .. }) if cause.raw_os_error() == Some(libc::ELOOP) => {
                return Ok(Reconciliation::Conflict(state));
            }
            Err(error) => return Err(error),
        };
        if !metadata.is_file() || Identity::of(&metadata) != record.binding.data {
            return Ok(Reconciliation::Conflict(state));
        }
        let Some(results) = record.partial.checks else {
            return Err(NativeError::InvalidState);
        };
        let Some(anchor) = results.strong_anchor() else {
            return Ok(Reconciliation::NotRevalidatable(state));
        };
        // Only revalidate an already-recorded strong digest. No recovery-only measurement is added.
        let mut checks = Requirements::default().start(Measurement::Algorithm(anchor.algorithm()));
        let mut buffer = [0; 64 * 1024];
        let mut remaining = record.partial.written.0;
        while remaining > 0 {
            let limit = remaining.min(buffer.len() as u64) as usize;
            let count = data
                .read(&mut buffer[..limit])
                .map_err(|cause| NativeError::io(Phase::Reconcile, cause))?;
            if count == 0 {
                return Ok(Reconciliation::Conflict(state));
            }
            checks.update(&buffer[..count]);
            remaining -= count as u64;
        }
        // An externally growing witness cannot turn reconciliation into an unbounded read.
        if data
            .read(&mut buffer[..1])
            .map_err(|cause| NativeError::io(Phase::Reconcile, cause))?
            != 0
        {
            return Ok(Reconciliation::Conflict(state));
        }
        let actual = checks
            .finish()
            .map_err(|_| NativeError::InvalidState)?
            .measured();
        if actual != Some(anchor) {
            return Ok(Reconciliation::Conflict(state));
        }
        // Recheck the retained name after hashing, before converting intent into an acknowledged receipt.
        if Identity::of(&workspace.metadata(DATA, Phase::Reconcile)?) != record.binding.data {
            return Ok(Reconciliation::Conflict(state));
        }
        match directory.parent.metadata(&destination, Phase::Reconcile) {
            Ok(metadata)
                if metadata.is_file() && Identity::of(&metadata) == record.binding.data => {}
            Ok(_) => return Ok(Reconciliation::Conflict(state)),
            Err(NativeError::Io { cause, .. }) if cause.kind() == io::ErrorKind::NotFound => {
                return Ok(Reconciliation::PublicationUnknown(state));
            }
            Err(NativeError::Io { cause, .. }) if cause.raw_os_error() == Some(libc::ELOOP) => {
                return Ok(Reconciliation::Conflict(state));
            }
            Err(error) => return Err(error),
        }
        workspace.replace_record(
            &Record {
                binding: Binding::clone(&record.binding),
                status: Status::Published,
                partial: record.partial,
            },
            RecordAccess::Existing,
        )?;
        Ok(Reconciliation::Published(Receipt::completed(
            record.partial,
            results,
            state,
        )))
    }

    pub(super) fn policy(&self) -> CheckPolicy {
        self.0.binding.checks
    }

    #[cfg(test)]
    pub(super) fn hooks(&self) -> &Hooks {
        &self.0.hooks
    }
}

struct Lease {
    directory: DirectoryLease,
    workspace: Directory,
    _workspace_lock: FlockLease,
    data: File,
    binding: Binding,
    runtime: Handle,
    // These atomics enforce a monotonic inode lifecycle and distinguish initial create from replace.
    mode: AtomicU8,
    initialized: AtomicBool,
    // The outer workspace stays owned until the final queued/running physical operation settles.
    _registration: Option<OperationOwner>,
    #[cfg(test)]
    hooks: Hooks,
    // Last field: notification follows descriptor and lease destruction, not public waiter drop.
    #[cfg(test)]
    _released: Released,
}

impl Lease {
    fn validate_data(&self, length: ByteCount) -> Result<(), NativeError> {
        let metadata = self
            .data
            .metadata()
            .map_err(|cause| NativeError::io(Phase::Seal, cause))?;
        Role::Data.check(&metadata)?;
        if Identity::of(&metadata) != self.binding.data
            || metadata.len() != length.0
            || Identity::of(&self.workspace.metadata(DATA, Phase::Seal)?) != self.binding.data
        {
            return Err(NativeError::InvalidState);
        }
        Ok(())
    }

    fn checkpoint(&self, status: Status, partial: Partial) -> Result<(), NativeError> {
        let mode = self.mode.load(Ordering::Acquire);
        match status {
            Status::NeedsBody if mode >= PUBLISHING => return Err(NativeError::InvalidState),
            Status::Sealed if mode != SEALED => return Err(NativeError::InvalidState),
            Status::Publishing if mode != PUBLISHING => return Err(NativeError::InvalidState),
            Status::Published if mode != PUBLISHING && mode != PUBLISHED => {
                return Err(NativeError::InvalidState);
            }
            _ => {}
        }
        #[cfg(test)]
        if self.hooks.fail_checkpoint.load(Ordering::Acquire) {
            return Err(NativeError::io(
                Phase::Checkpoint,
                io::Error::other("injected checkpoint failure"),
            ));
        }
        let access = if self.initialized.load(Ordering::Acquire) {
            RecordAccess::Existing
        } else {
            RecordAccess::Create
        };
        self.workspace.replace_record(
            &Record {
                binding: Binding::clone(&self.binding),
                status,
                partial,
            },
            access,
        )?;
        self.initialized.store(true, Ordering::Release);
        Ok(())
    }

    fn link(&self) -> Result<(), NativeError> {
        let destination = Name::from_bytes(&self.binding.destination)?;
        self.directory
            .parent
            .publish(&self.workspace, DATA, &destination)
    }
}

struct OwnedDirectory {
    _directory: Directory,
    _lock: FlockLease,
}

struct DirectoryLease {
    parent: Directory,
    _lock: FlockLease,
}

impl DirectoryLease {
    fn acquire(path: &Path) -> Result<Self, NativeError> {
        Self::open(path, LockAccess::Create, Phase::Acquire)
    }

    fn reacquire(path: &Path) -> Result<Self, NativeError> {
        Self::open(path, LockAccess::Existing, Phase::Reconcile)
    }

    fn open(path: &Path, access: LockAccess, phase: Phase) -> Result<Self, NativeError> {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)
            .map_err(|cause| NativeError::io(phase, cause))?;
        let parent = Directory(file);
        let metadata = parent
            .0
            .metadata()
            .map_err(|cause| NativeError::io(phase, cause))?;
        // Other users must not be able to exchange the stable cooperating lock directory entry.
        if !metadata.is_dir() || metadata.uid() != effective_uid() || metadata.mode() & 0o022 != 0 {
            return Err(NativeError::InvalidState);
        }
        let lock = parent.lock(DIRECTORY_LOCK, DIRECTORY_MARKER, access, phase)?;
        Ok(Self {
            parent,
            _lock: lock,
        })
    }
}

struct Directory(File);

impl Directory {
    /// Atomic no-replace publication, deliberately below cooperating-writer admission so its race
    /// semantics can be exercised by two independently sealed stages.
    fn publish(
        &self,
        source: &Self,
        source_name: &CStr,
        destination: &CStr,
    ) -> Result<(), NativeError> {
        // SAFETY: both descriptors and NUL-terminated names are owned through the syscall.
        let result = unsafe {
            libc::linkat(
                source.0.as_raw_fd(),
                source_name.as_ptr(),
                self.0.as_raw_fd(),
                destination.as_ptr(),
                0,
            )
        };
        if result == 0 {
            return Ok(());
        }
        let cause = io::Error::last_os_error();
        if cause.kind() == io::ErrorKind::AlreadyExists {
            return Err(NativeError::DestinationExists);
        }
        if matches!(
            cause.raw_os_error(),
            Some(libc::EXDEV | libc::EOPNOTSUPP | libc::ENOSYS)
        ) {
            return Err(NativeError::Unsupported);
        }
        Err(NativeError::io(Phase::Publish, cause))
    }

    fn directory(&self, name: &CStr, phase: Phase) -> Result<Self, NativeError> {
        self.open(name, libc::O_RDONLY | libc::O_DIRECTORY, 0, phase)
            .map(Self)
    }

    fn mkdir(&self, name: &CStr) -> Result<(), NativeError> {
        // SAFETY: both the parent descriptor and name are live and owned by this operation.
        if unsafe { libc::mkdirat(self.0.as_raw_fd(), name.as_ptr(), 0o700) } != 0 {
            return Err(NativeError::io(Phase::Acquire, io::Error::last_os_error()));
        }
        Ok(())
    }

    /// A shared syscall helper cannot infer whether its caller is acquiring, checkpointing or
    /// reconciling. Carry the caller's semantic phase without replacing the original I/O cause.
    fn open(
        &self,
        name: &CStr,
        flags: i32,
        mode: libc::c_uint,
        phase: Phase,
    ) -> Result<File, NativeError> {
        // SAFETY: the descriptor and C string are live; mode is supplied even when unused.
        let fd = unsafe {
            libc::openat(
                self.0.as_raw_fd(),
                name.as_ptr(),
                flags | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
                mode,
            )
        };
        if fd < 0 {
            return Err(NativeError::io(phase, io::Error::last_os_error()));
        }
        // SAFETY: openat returned one new owned descriptor and no other File owns it.
        Ok(unsafe { File::from_raw_fd(fd) })
    }

    fn metadata(&self, name: &CStr, phase: Phase) -> Result<Metadata, NativeError> {
        // O_NOFOLLOW refuses a symlink before opening. O_NONBLOCK avoids blocking on foreign FIFOs.
        let file = self.open(name, libc::O_RDONLY, 0, phase)?;
        file.metadata()
            .map_err(|cause| NativeError::io(phase, cause))
    }

    fn identity(&self, phase: Phase) -> Result<Identity, NativeError> {
        self.0
            .metadata()
            .map(|m| Identity::of(&m))
            .map_err(|cause| NativeError::io(phase, cause))
    }

    fn check_private_directory(&self, phase: Phase) -> Result<(), NativeError> {
        let metadata = self
            .0
            .metadata()
            .map_err(|cause| NativeError::io(phase, cause))?;
        if !metadata.is_dir()
            || metadata.uid() != effective_uid()
            || metadata.mode() & 0o777 != 0o700
        {
            return Err(NativeError::InvalidState);
        }
        Ok(())
    }

    fn lock(
        &self,
        name: &CStr,
        marker: &[u8],
        access: LockAccess,
        phase: Phase,
    ) -> Result<FlockLease, NativeError> {
        let (file, created) = match access {
            LockAccess::New => (
                self.open(
                    name,
                    libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
                    0o600,
                    phase,
                )?,
                true,
            ),
            LockAccess::Create => {
                match self.open(
                    name,
                    libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
                    0o600,
                    phase,
                ) {
                    Ok(file) => (file, true),
                    Err(NativeError::Io { cause, .. })
                        if cause.kind() == io::ErrorKind::AlreadyExists =>
                    {
                        (self.open(name, libc::O_RDWR, 0, phase)?, false)
                    }
                    Err(error) => return Err(error),
                }
            }
            LockAccess::Existing => (self.open(name, libc::O_RDWR, 0, phase)?, false),
        };
        Role::Lock.check(
            &file
                .metadata()
                .map_err(|cause| NativeError::io(phase, cause))?,
        )?;
        // The operation-owning registration keeps this lease until physical settlement. Closing its
        // descriptor alone is insufficient when an incidental fork/dup copy shares the description.
        // SAFETY: file owns this descriptor; LOCK_NB makes admission nonblocking.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            let cause = io::Error::last_os_error();
            return if cause.kind() == io::ErrorKind::WouldBlock {
                Err(NativeError::Busy)
            } else {
                Err(NativeError::io(phase, cause))
            };
        }
        // Wrap immediately after acquisition so every later validation or marker I/O failure also
        // relinquishes the lease, even when another process inherited an incidental descriptor copy.
        let lease = FlockLease {
            file,
            acquired_by: ProcessId::current(),
        };
        if created {
            lease
                .file
                .write_all_at(marker, 0)
                .map_err(|cause| NativeError::io(phase, cause))?;
        } else {
            let metadata = lease
                .file
                .metadata()
                .map_err(|cause| NativeError::io(phase, cause))?;
            if metadata.len() != marker.len() as u64 {
                return Err(NativeError::InvalidState);
            }
            let mut actual = vec![0; marker.len()];
            lease
                .file
                .read_exact_at(&mut actual, 0)
                .map_err(|cause| NativeError::io(phase, cause))?;
            if actual != marker {
                return Err(NativeError::InvalidState);
            }
        }
        if Identity::of(&self.metadata(name, phase)?)
            != Identity::of(
                &lease
                    .file
                    .metadata()
                    .map_err(|cause| NativeError::io(phase, cause))?,
            )
        {
            return Err(NativeError::InvalidState);
        }
        Ok(lease)
    }

    fn read_record(&self, phase: Phase) -> Result<Record, NativeError> {
        let file = self.open(STATE, libc::O_RDONLY, 0, phase)?;
        let metadata = file
            .metadata()
            .map_err(|cause| NativeError::io(phase, cause))?;
        Role::Record.check(&metadata)?;
        if metadata.len() > MAX_RECORD {
            return Err(NativeError::InvalidState);
        }
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        file.take(MAX_RECORD + 1)
            .read_to_end(&mut bytes)
            .map_err(|cause| NativeError::io(phase, cause))?;
        if bytes.len() as u64 > MAX_RECORD {
            return Err(NativeError::InvalidState);
        }
        Record::decode(&bytes).map_err(|InvalidRecord| NativeError::InvalidState)
    }

    fn replace_record(&self, record: &Record, access: RecordAccess) -> Result<(), NativeError> {
        let previous = match access {
            RecordAccess::Existing => {
                let previous = self.read_record(Phase::Checkpoint)?;
                if previous.binding != record.binding {
                    return Err(NativeError::InvalidState);
                }
                Some(Identity::of(&self.metadata(STATE, Phase::Checkpoint)?))
            }
            RecordAccess::Create => {
                match self.metadata(STATE, Phase::Checkpoint) {
                    Err(NativeError::Io { cause, .. })
                        if cause.kind() == io::ErrorKind::NotFound => {}
                    Ok(_) => return Err(NativeError::InvalidState),
                    Err(error) => return Err(error),
                }
                None
            }
        };
        // Private, versioned metadata only. No payload or credential can expand this allocation.
        let bytes = record.encode();
        if bytes.len() as u64 > MAX_RECORD {
            return Err(NativeError::InvalidState);
        }
        let sequence = CHECKPOINT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let name =
            Name::from_bytes(format!("state-next-{}-{sequence}", std::process::id()).as_bytes())?;
        let file = self.open(
            &name,
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
            0o600,
            Phase::Checkpoint,
        )?;
        file.write_all_at(&bytes, 0)
            .map_err(|cause| NativeError::io(Phase::Checkpoint, cause))?;
        if let Some(previous) = previous {
            if Identity::of(&self.metadata(STATE, Phase::Checkpoint)?) != previous {
                return Err(NativeError::InvalidState);
            }
            // Replacing rename is restricted to this already-validated private metadata record.
            // SAFETY: names and workspace descriptor are live and refer to owned private metadata.
            if unsafe {
                libc::renameat(
                    self.0.as_raw_fd(),
                    name.0.as_ptr(),
                    self.0.as_raw_fd(),
                    STATE.as_ptr(),
                )
            } != 0
            {
                return Err(NativeError::io(
                    Phase::Checkpoint,
                    io::Error::last_os_error(),
                ));
            }
        } else {
            // Initial record creation has absent-only semantics too; never replace a foreign role.
            // SAFETY: names and descriptor are owned across the syscall.
            if unsafe {
                libc::linkat(
                    self.0.as_raw_fd(),
                    name.0.as_ptr(),
                    self.0.as_raw_fd(),
                    STATE.as_ptr(),
                    0,
                )
            } != 0
            {
                return Err(NativeError::io(
                    Phase::Checkpoint,
                    io::Error::last_os_error(),
                ));
            }
            // SAFETY: only the create-new temporary made by this operation is removed.
            if unsafe { libc::unlinkat(self.0.as_raw_fd(), name.0.as_ptr(), 0) } != 0 {
                return Err(NativeError::io(
                    Phase::Checkpoint,
                    io::Error::last_os_error(),
                ));
            }
        }
        Ok(())
    }
}

/// A lease belongs to the acquiring process and the final operation-owning registration, not to
/// incidental descriptor references. An inherited child may close its copy but must not unlock the
/// acquiring parent's shared open file description.
struct FlockLease {
    file: File,
    acquired_by: ProcessId,
}

impl AsRawFd for FlockLease {
    fn as_raw_fd(&self) -> RawFd {
        self.file.as_raw_fd()
    }
}

impl Drop for FlockLease {
    fn drop(&mut self) {
        if self.acquired_by != ProcessId::current() {
            return;
        }
        // SAFETY: this guard still owns the live descriptor. Explicit unlock relinquishes authority
        // even if another process retains a fork/dup reference. File closes after this Drop body.
        // Drop cannot report an unlock error; closing the descriptor remains the cleanup fallback.
        let _ = unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct ProcessId(libc::pid_t);

impl ProcessId {
    fn current() -> Self {
        // SAFETY: getpid has no arguments or memory-safety preconditions and is safe after fork.
        Self(unsafe { libc::getpid() })
    }
}

impl Identity {
    fn of(metadata: &Metadata) -> Self {
        Self {
            device: DeviceId(metadata.dev()),
            inode: InodeId(metadata.ino()),
        }
    }
}

struct Name(CString);

impl Name {
    fn from_os(value: &OsStr) -> Result<Self, NativeError> {
        Self::from_bytes(value.as_bytes())
    }
    fn from_bytes(value: &[u8]) -> Result<Self, NativeError> {
        if value.is_empty()
            || value.len() > 255
            || value.contains(&b'/')
            || value == b"."
            || value == b".."
        {
            return Err(NativeError::InvalidState);
        }
        CString::new(value)
            .map(Self)
            .map_err(|_| NativeError::InvalidState)
    }
}

impl core::ops::Deref for Name {
    type Target = CStr;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

enum LockAccess {
    New,
    Create,
    Existing,
}
enum RecordAccess {
    Create,
    Existing,
}
enum Role {
    Lock,
    Data,
    Record,
}

impl Role {
    fn check(self, metadata: &Metadata) -> Result<(), NativeError> {
        if !metadata.is_file()
            || metadata.uid() != effective_uid()
            || metadata.mode() & 0o777 != 0o600
        {
            return Err(NativeError::InvalidState);
        }
        let links = metadata.nlink();
        match self {
            Self::Lock | Self::Record if links != 1 => Err(NativeError::InvalidState),
            Self::Data if !(1..=2).contains(&links) => Err(NativeError::InvalidState),
            _ => Ok(()),
        }
    }
}

fn effective_uid() -> u32 {
    // SAFETY: geteuid has no arguments or memory-safety preconditions.
    unsafe { libc::geteuid() }
}

#[derive(Debug)]
pub(super) struct WriteFailure {
    pub(super) accepted: Option<ByteCount>,
    pub(super) error: NativeError,
}

#[derive(Debug)]
pub(super) enum NativeError {
    Busy,
    DestinationExists,
    Unsupported,
    RuntimeUnavailable,
    InvalidState,
    Io { phase: Phase, cause: io::Error },
    PublicationUnknown(io::Error),
}

impl NativeError {
    fn io(phase: Phase, cause: io::Error) -> Self {
        if matches!(
            cause.raw_os_error(),
            Some(libc::EOPNOTSUPP | libc::ENOSYS | libc::EXDEV)
        ) {
            return Self::Unsupported;
        }
        Self::Io { phase, cause }
    }
    fn into_io(self) -> io::Error {
        match self {
            Self::Io { cause, .. } | Self::PublicationUnknown(cause) => cause,
            _ => io::Error::other("publication receipt could not be recorded"),
        }
    }
    pub(super) fn into_kind<E>(self) -> FailureKind<E> {
        match self {
            Self::Busy => FailureKind::Busy,
            Self::DestinationExists => FailureKind::DestinationExists,
            Self::Unsupported => FailureKind::Unsupported,
            Self::RuntimeUnavailable => FailureKind::RuntimeUnavailable,
            Self::InvalidState => FailureKind::InvalidState,
            Self::Io { phase, cause } => FailureKind::Io { phase, cause },
            Self::PublicationUnknown(cause) => FailureKind::PublicationUnknown { cause },
        }
    }
}

#[cfg(test)]
#[derive(Default)]
pub(super) struct Hooks {
    pub(super) released: Arc<tokio::sync::Notify>,
    pub(super) write: Gate,
    pub(super) read: Gate,
    pub(super) seal: Gate,
    pub(super) commit: Gate,
    pub(super) lose_publish_ack: AtomicBool,
    pub(super) fail_checkpoint: AtomicBool,
    pub(super) fail_write_prefix: AtomicBool,
    pub(super) lose_write_result: AtomicBool,
}

#[cfg(test)]
#[derive(Default)]
pub(super) struct Gate {
    // Test barriers only. The mutex pairs notification with a physical worker's explicit release.
    inner: std::sync::Mutex<Option<Arc<Barrier>>>,
}

#[cfg(test)]
pub(super) struct Barrier {
    pub(super) entered: tokio::sync::Notify,
    // The controlled physical worker waits here; release is idempotent and never blocks the driver.
    released: std::sync::Mutex<bool>,
    wake: std::sync::Condvar,
}

#[cfg(test)]
pub(super) struct Held(Arc<Barrier>);

#[cfg(test)]
impl core::ops::Deref for Held {
    type Target = Barrier;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
impl Drop for Held {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
impl Barrier {
    pub(super) fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }

    fn wait(&self) {
        let released = self.released.lock().unwrap();
        drop(
            self.wake
                .wait_while(released, |released| !*released)
                .unwrap(),
        );
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
impl Gate {
    pub(super) fn install(&self) -> Held {
        let barrier = Arc::new(Barrier {
            entered: tokio::sync::Notify::new(),
            released: std::sync::Mutex::new(false),
            wake: std::sync::Condvar::new(),
        });
        *self.inner.lock().unwrap() = Some(Arc::clone(&barrier));
        Held(barrier)
    }
    fn enter(&self) {
        let barrier = self.inner.lock().unwrap().take();
        if let Some(barrier) = barrier {
            barrier.entered.notify_one();
            barrier.wait();
        }
    }
}

#[cfg(test)]
#[path = "native_tests.rs"]
mod tests;

#[cfg(test)]
struct Released(Arc<tokio::sync::Notify>);

#[cfg(test)]
impl Drop for Released {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}
