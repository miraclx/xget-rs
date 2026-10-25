//! Display escaping and shell operands are different contracts; neither accepts foreign diagnostics.

use core::fmt;
use std::path::Path;

pub(crate) struct Text<'a>(pub(crate) &'a str);
pub(crate) struct DiagnosticText<'a>(pub(crate) &'a str);

impl fmt::Display for Text<'_> {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(text) = *self;
        let quoted = text.is_empty()
            || text.chars().any(|ch| {
                ch.is_whitespace()
                    || matches!(
                        ch,
                        '"' | '\''
                            | '\\'
                            | ';'
                            | '|'
                            | '&'
                            | '$'
                            | '`'
                            | '('
                            | ')'
                            | '<'
                            | '>'
                            | '*'
                            | '?'
                            | '['
                            | ']'
                            | '{'
                            | '}'
                    )
                    || escaped(ch)
            });
        if quoted {
            out.write_str("\"")?;
        }
        DiagnosticText(text).fmt(out)?;
        if quoted {
            out.write_str("\"")?;
        }
        Ok(())
    }
}

impl fmt::Display for DiagnosticText<'_> {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        for ch in self.0.chars() {
            match ch {
                '\\' => out.write_str("\\\\")?,
                '"' => out.write_str("\\\"")?,
                '\n' => out.write_str("\\n")?,
                '\r' => out.write_str("\\r")?,
                '\t' => out.write_str("\\t")?,
                ch if escaped(ch) => write!(out, "\\u{{{:x}}}", u32::from(ch))?,
                ch => write!(out, "{ch}")?,
            }
        }
        Ok(())
    }
}

pub(crate) struct PathText<'a>(pub(crate) &'a Path);

impl fmt::Display for PathText<'_> {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.to_str() {
            Some(text) => Text(text).fmt(out),
            None => {
                out.write_str("native bytes \"")?;
                for byte in self.0.as_os_str().as_encoded_bytes() {
                    match byte {
                        b' '..=b'~' if !matches!(byte, b'\\' | b'"') => {
                            write!(out, "{}", char::from(*byte))?
                        }
                        _ => write!(out, "\\x{byte:02x}")?,
                    }
                }
                out.write_str("\"")
            }
        }
    }
}

/// A complete operand, never an escaped display label masquerading as a shell recipe.
pub(crate) struct Operand<'a> {
    bytes: &'a [u8],
    encoding: Encoding,
}

#[derive(Clone, Copy)]
pub(crate) enum Shell {
    Posix,
    Bash,
    NotCopyable,
    Unsupported,
}

#[derive(Clone, Copy)]
enum Encoding {
    Utf8,
    Native,
}

impl<'a> Operand<'a> {
    pub(crate) fn text(text: &'a str) -> Self {
        Self {
            bytes: text.as_bytes(),
            encoding: Encoding::Utf8,
        }
    }

    pub(crate) fn path(path: &'a Path) -> Self {
        Self {
            bytes: path.as_os_str().as_encoded_bytes(),
            encoding: if path.to_str().is_some() {
                Encoding::Utf8
            } else {
                Encoding::Native
            },
        }
    }

    pub(crate) fn shell(&self) -> Shell {
        if self.bytes.contains(&0) {
            return Shell::NotCopyable;
        }
        if matches!(self.encoding, Encoding::Native) {
            return if cfg!(unix) {
                Shell::Bash
            } else {
                Shell::Unsupported
            };
        }
        if core::str::from_utf8(self.bytes).is_ok_and(|text| text.chars().any(escaped)) {
            Shell::Bash
        } else {
            Shell::Posix
        }
    }
}

impl fmt::Display for Operand<'_> {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.shell() {
            Shell::NotCopyable => return out.write_str("<contains NUL; not shell-copyable>"),
            Shell::Unsupported => return out.write_str("<unsupported native encoding>"),
            Shell::Posix => {
                out.write_str("'")?;
                // POSIX mode is admitted only for printable UTF-8, including ordinary Unicode.
                if let Ok(text) = core::str::from_utf8(self.bytes) {
                    for ch in text.chars() {
                        if ch == '\'' {
                            out.write_str("'\\''")?;
                        } else {
                            write!(out, "{ch}")?;
                        }
                    }
                }
                return out.write_str("'");
            }
            Shell::Bash => {}
        }
        out.write_str("$'")?;
        // ANSI-C operands preserve each native Unix byte and never contain literal terminal controls.
        for byte in self.bytes {
            match byte {
                b'\\' => out.write_str("\\\\")?,
                b'\'' => out.write_str("\\'")?,
                b' '..=b'~' => write!(out, "{}", char::from(*byte))?,
                _ => write!(out, "\\x{byte:02x}")?,
            }
        }
        out.write_str("'")
    }
}

fn escaped(ch: char) -> bool {
    ch.is_control()
        || matches!(ch, '\u{061c}' | '\u{200e}' | '\u{200f}' | '\u{2028}'..='\u{202e}' | '\u{2066}'..='\u{206f}' | '\u{feff}')
}

#[cfg(test)]
#[path = "text_tests.rs"]
mod tests;
