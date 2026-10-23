use core::future::Future;
use core::pin::{Pin, pin};

use bytes::Bytes;
use futures::{Stream, StreamExt as _};

use super::native::{Native, NativeError};
use super::observation::{Observation, Observer};
use super::{ByteCount, Failure, FailureKind, Length, Partial, Receipt, StateRef, WriteExtent};
use crate::checks::Checks;

/// One consuming task owns the body. Native submissions own only descriptors and bounded byte slices.
pub(super) struct Run {
    pub(super) native: Native,
    pub(super) state: StateRef,
    pub(super) length: Length,
}

impl Run {
    // Native fault fixtures enter the same pipeline after installing physical-operation barriers.
    #[cfg(all(test, unix))]
    pub(super) async fn execute<B, E, S>(self, body: B, stop: S) -> Result<Receipt, Box<Failure<E>>>
    where
        B: Stream<Item = Result<Bytes, E>>,
        S: Future<Output = ()>,
    {
        let mut observer = ();
        let mut observation = Observation::start(&mut observer, self.length);
        let result = self.execute_observed(body, stop, &mut observation).await;
        observation.emit();
        result
    }

    pub(super) async fn execute_observed<B, E, S, O>(
        self,
        body: B,
        stop: S,
        observation: &mut Observation<'_, O>,
    ) -> Result<Receipt, Box<Failure<E>>>
    where
        B: Stream<Item = Result<Bytes, E>>,
        S: Future<Output = ()>,
        O: Observer,
    {
        let mut partial = Partial::default();
        let mut stop = pin!(stop);
        // The consuming helper drops the body before sealing/publication; no detached body pump exists.
        let outcome = match self
            .receive(body, stop.as_mut(), &mut partial, observation)
            .await
        {
            Ok(checks) => match self.native.seal(partial.written).await {
                Ok(()) => tokio::select! {
                    biased;
                    () = &mut stop => Err(FailureKind::Stopped),
                    () = core::future::ready(()) => {
                        match checks.finish() {
                            Ok(results) => { partial.set_checks(results); Ok(()) }
                            Err(mismatch) => {
                                partial.set_checks(mismatch.results);
                                Err(FailureKind::DigestMismatch(mismatch))
                            }
                        }
                    }
                },
                Err(error) => Err(error.into_kind()),
            },
            Err(kind) => Err(kind),
        };
        if let Err(kind) = outcome {
            // On normal failure and cooperative stop, checkpoint failure takes precedence over a
            // successful-pause claim. A body cause is retained separately if checkpointing also fails.
            if let Err(error) = self.native.checkpoint(partial).await {
                return Err(Box::new(Failure {
                    kind: FailureKind::CheckpointFailed {
                        cause: Box::new(kind),
                        checkpoint: Box::new(error.into_kind()),
                    },
                    partial,
                    state: Some(self.state),
                }));
            }
            return Err(Box::new(Failure {
                kind,
                partial,
                state: Some(self.state),
            }));
        }
        let results = partial.checks.ok_or_else(|| {
            Box::new(Failure {
                kind: FailureKind::InvalidState,
                partial,
                state: Some(StateRef::clone(&self.state)),
            })
        })?;
        self.native
            .publish(partial)
            .await
            .map_err(|error| self.failure(error, partial))?;
        Ok(Receipt::completed(partial, results, self.state))
    }

    async fn receive<B, E, S, O>(
        &self,
        body: B,
        mut stop: Pin<&mut S>,
        partial: &mut Partial,
        observation: &mut Observation<'_, O>,
    ) -> Result<Checks, FailureKind<E>>
    where
        B: Stream<Item = Result<Bytes, E>>,
        S: Future<Output = ()>,
        O: Observer,
    {
        let mut body = pin!(body);
        let mut checks = self.native.policy().start();
        let hashing = checks.active();
        loop {
            // Stop wins simultaneous readiness and is checked between every bounded physical write.
            let next = tokio::select! {
                biased;
                () = &mut stop => return Err(FailureKind::Stopped),
                next = body.next() => next,
            };
            let Some(next) = next else {
                break;
            };
            let chunk = next.map_err(FailureKind::Body)?;
            partial.received = ByteCount(
                partial
                    .received
                    .0
                    .checked_add(chunk.len() as u64)
                    .ok_or(FailureKind::LengthOverflow)?,
            );
            observation.update(partial, ByteCount(0));
            if let Length::Known(expected) = self.length
                && partial.received > expected
            {
                return Err(FailureKind::LengthMismatch);
            }
            let mut offset = 0;
            while offset < chunk.len() {
                // No source polling occurs while a physical write is pending. Once submitted it must
                // settle before a cooperative stop can checkpoint the exact accepted prefix.
                tokio::select! {
                    biased;
                    () = &mut stop => return Err(FailureKind::Stopped),
                    () = core::future::ready(()) => {}
                }
                let end = offset.saturating_add(64 * 1024).min(chunk.len());
                let bytes = chunk.slice(offset..end);
                match self
                    .native
                    .write(partial.written, Bytes::clone(&bytes))
                    .await
                {
                    Ok(accepted) => {
                        checks.update(&bytes[..accepted.0 as usize]);
                        partial.written.0 += accepted.0;
                        if hashing {
                            partial.hashed.0 += accepted.0;
                        }
                    }
                    Err(error) => {
                        if let Some(accepted) = error.accepted {
                            checks.update(&bytes[..accepted.0 as usize]);
                            partial.written.0 += accepted.0;
                            if hashing {
                                partial.hashed.0 += accepted.0;
                            }
                        } else {
                            partial.write_extent = WriteExtent::Unconfirmed;
                        }
                        observation.update(partial, ByteCount(0));
                        return Err(error.error.into_kind());
                    }
                }
                observation.update(partial, ByteCount(0));
                offset = end;
            }
        }
        if let Length::Known(expected) = self.length
            && partial.received != expected
        {
            return Err(FailureKind::LengthMismatch);
        }
        Ok(checks)
    }

    fn failure<E>(&self, error: NativeError, partial: Partial) -> Box<Failure<E>> {
        Failure::native(error, partial, Some(StateRef::clone(&self.state)))
    }
}
