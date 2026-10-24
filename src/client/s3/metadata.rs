//! Local wire-byte admission before the SDK's body collector and XML deserializer.

use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::interceptors::Intercept;
use aws_smithy_runtime_api::client::interceptors::context::{
    BeforeDeserializationInterceptorContextMut, FinalizerInterceptorContextMut,
};
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::config_bag::ConfigBag;
use http_body_04::Limited;

// Separate from the common 1 MiB decoded retained-text budget. XML encoding/structure has its own
// finite allowance; max_keys is only a request hint and is never trusted as a local response bound.
pub(super) const MAX_WIRE_BYTES: usize = 2 * 1024 * 1024;

#[derive(Clone, Copy, Debug)]
pub(super) enum MetadataWire {
    Listing,
    Head,
    PayloadErrors,
}

impl Intercept for MetadataWire {
    fn name(&self) -> &'static str {
        "xget metadata wire bound"
    }

    fn modify_before_deserialization(
        &self,
        context: &mut BeforeDeserializationInterceptorContextMut<'_>,
        _runtime: &RuntimeComponents,
        _config: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let response = context.response_mut();
        if matches!(self, Self::PayloadErrors) && response.status().is_success() {
            // A successful GetObject owns an unbounded streaming payload, not metadata XML.
            return Ok(());
        }
        // A successful HEAD's Content-Length describes the OBJECT, not metadata response bytes.
        // Its actual (normally empty) body is still wrapped, including SDK-parsed error responses.
        if !matches!(self, Self::Head)
            && response
                .headers()
                .get("content-length")
                .and_then(|value| value.parse::<u64>().ok())
                .is_some_and(|length| length > MAX_WIRE_BYTES as u64)
        {
            // The retained SDK error may keep its raw response. Destroy the rejected stream before
            // returning the error, while the operation still holds common request admission.
            *response.body_mut() = SdkBody::empty();
            return Err(Box::new(WireLimit));
        }
        let original = core::mem::replace(response.body_mut(), SdkBody::taken());
        // Smithy runtime 1.9.4 invokes this hook BEFORE read_body's accumulating Vec and before
        // streaming/nonstreaming deserialization. Limited admits at most this many data bytes and
        // errors rather than truncating; size hints are clipped too. The SDK remains the XML parser.
        *response.body_mut() = SdkBody::from_body_0_4(Limited::new(original, MAX_WIRE_BYTES));
        Ok(())
    }

    fn modify_before_completion(
        &self,
        context: &mut FinalizerInterceptorContextMut<'_>,
        _runtime: &RuntimeComponents,
        _config: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        if context
            .output_or_error()
            .is_some_and(|result| result.is_err())
            && let Some(response) = context.response_mut()
        {
            // SDK typed causes/status survive, not a rejected raw stream outliving its permit.
            *response.body_mut() = SdkBody::empty();
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("s3 metadata response exceeds its wire byte limit")]
struct WireLimit;

pub(super) fn is_limit(error: &(dyn core::error::Error + 'static)) -> bool {
    let mut current = Some(error);
    // Only classification inspects the retained foreign chain, never formatting or exposing it.
    // A fixed walk bound cannot loop forever if a caller-configured foreign cause is cyclic.
    for _ in 0..16 {
        let Some(error) = current else { return false };
        if error.is::<WireLimit>() || error.is::<http_body_04::LengthLimitError>() {
            return true;
        }
        current = error.source();
    }
    false
}
