//! The legacy API still builds on other hosts. This new backend makes no weaker publication promise.

use bytes::Bytes;

use super::plan::Plan;
use super::{AcquisitionFailure, ByteCount, FailureKind, Partial, Reconciliation, StateRef};
use crate::checks::CheckPolicy;

pub(super) struct Native {
    checks: CheckPolicy,
}

impl Native {
    pub(super) async fn acquire(plan: Plan) -> Result<Self, AcquisitionFailure> {
        let Plan {
            parent,
            destination,
            workspace,
            checks,
            state,
        } = plan;
        drop((parent, destination, workspace, checks, state));
        Err(AcquisitionFailure {
            error: NativeError,
            state: None,
        })
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
