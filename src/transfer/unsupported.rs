//! The legacy API still builds on other hosts. This new backend makes no weaker publication promise.

use bytes::Bytes;

use super::plan::Plan;
use super::{
    AcquisitionFailure, ByteCount, FailureKind, OperationOwner, Partial, ReaderSession as Session,
    Reconciliation, RecoveryBinding, StateRef,
};
use crate::checks::CheckPolicy;

pub(super) struct Native {
    checks: CheckPolicy,
}

impl Native {
    pub(super) async fn acquire_registered(
        plan: Plan,
        _registration: Option<OperationOwner>,
    ) -> Result<Self, AcquisitionFailure> {
        let Plan {
            parent,
            destination,
            workspace,
            checks,
            state,
            publication,
            restart,
            targets,
        } = plan;
        drop((
            parent,
            destination,
            workspace,
            checks,
            state,
            publication,
            restart,
            targets,
        ));
        Err(AcquisitionFailure {
            error: NativeError,
            state: None,
        })
    }
    pub(super) async fn acquire_reader(
        plan: Plan,
        _session: Session,
        _resume: Option<StateRef>,
        registration: Option<OperationOwner>,
    ) -> Result<Self, AcquisitionFailure> {
        Self::acquire_registered(plan, registration).await
    }
    pub(super) async fn restore_reader(
        &self,
    ) -> Result<(crate::checks::Checks, ByteCount), NativeError> {
        Err(NativeError)
    }
    pub(super) async fn checkpoint_prefix(
        &self,
        _partial: Partial,
        _length: ByteCount,
        _digest: super::Digest,
    ) -> Result<(), NativeError> {
        Err(NativeError)
    }
    pub(super) async fn write(
        &self,
        _offset: ByteCount,
        _bytes: Bytes,
    ) -> Result<ByteCount, WriteFailure> {
        Err(WriteFailure {
            accepted: Some(ByteCount(0)),
            error: NativeError,
        })
    }
    pub(super) async fn read_prefix(
        &self,
        _start: ByteCount,
        _end: ByteCount,
    ) -> Result<Vec<u8>, NativeError> {
        Err(NativeError)
    }
    pub(super) async fn seal(&self, _length: ByteCount) -> Result<(), NativeError> {
        Err(NativeError)
    }
    pub(super) async fn checkpoint(&self, _partial: Partial) -> Result<(), NativeError> {
        Err(NativeError)
    }
    pub(super) async fn publish(&self, _partial: Partial) -> Result<(), NativeError> {
        Err(NativeError)
    }
    pub(super) fn policy(&self) -> CheckPolicy {
        self.checks
    }
    pub(super) async fn reconcile(_state: StateRef) -> Result<Reconciliation, NativeError> {
        Err(NativeError)
    }
    pub(super) async fn reconcile_registered(
        _state: StateRef,
        _registration: Option<OperationOwner>,
        _binding: Option<RecoveryBinding>,
    ) -> Result<Reconciliation, NativeError> {
        Err(NativeError)
    }
    pub(super) async fn lease_directory(
        _path: std::path::PathBuf,
    ) -> Result<OperationOwner, NativeError> {
        Err(NativeError)
    }
    pub(super) async fn lease_existing_directory(
        _path: std::path::PathBuf,
    ) -> Result<OperationOwner, NativeError> {
        Err(NativeError)
    }
}

#[derive(Debug)]
pub(super) struct WriteFailure {
    pub(super) accepted: Option<ByteCount>,
    pub(super) error: NativeError,
}

#[derive(Debug)]
pub(super) struct NativeError;

impl NativeError {
    pub(super) fn into_kind<E>(self) -> FailureKind<E> {
        FailureKind::Unsupported
    }
}

#[cfg(test)]
#[path = "unsupported_tests.rs"]
mod tests;
