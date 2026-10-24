//! Human units only; machine records keep fixed byte/second units.

use xbytes::sizes::BYTE;
use xbytes::{ByteSize, Mode};

/// Human size/rate policy; raw counts win over the optional human bit-rate view.
#[derive(Clone, Copy)]
pub(crate) enum DisplayFormat {
    /// IEC byte sizes and rates.
    HumanBytes,
    /// IEC sizes with decimal bit rates.
    HumanBits,
    /// Unscaled byte sizes and rates.
    RawBytes,
}

impl DisplayFormat {
    /// Renders a byte count without changing machine-record units.
    pub(crate) fn size(self, bytes: u64) -> String {
        match self {
            Self::RawBytes => bytes.to_string(),
            Self::HumanBytes | Self::HumanBits => ByteSize::of(bytes, BYTE).iec().to_string(),
        }
    }

    /// Renders an exact bytes-per-second measurement in the selected human denomination.
    pub(crate) fn speed(self, bytes_per_second: u64) -> String {
        match self {
            Self::HumanBits => format!(
                "{}ps",
                ByteSize::of(bytes_per_second, BYTE).repr(Mode::Bits | Mode::Decimal)
            ),
            Self::HumanBytes | Self::RawBytes => format!("{}/s", self.size(bytes_per_second)),
        }
    }
}
