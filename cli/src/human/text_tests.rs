use std::path::Path;

use super::{DiagnosticText, Operand, PathText, Shell, Text};

#[test]
fn display_strings_escape_controls_and_bidi_without_normalizing_unicode_or_percent_spelling() {
    let input = "café/%2F\\\"\n\r\t\u{1b}\u{7f}\u{85}\u{202e}end";
    let shown = Text(input).to_string();
    assert!(shown.starts_with('"'));
    assert!(shown.contains("café/%2F"));
    for spelling in [
        "\\\\",
        "\\\"",
        "\\n",
        "\\r",
        "\\t",
        "\\u{1b}",
        "\\u{7f}",
        "\\u{85}",
        "\\u{202e}",
    ] {
        assert!(shown.contains(spelling), "{shown}");
    }
    assert!(!shown.chars().any(char::is_control));
    assert!(!shown.contains('\u{202e}'));
    assert_eq!(Text("plain-name").to_string(), "plain-name");
    assert_eq!(Text("").to_string(), "\"\"");
    assert_eq!(Text("two words").to_string(), "\"two words\"");
    assert_eq!(
        DiagnosticText("one safe diagnosis; take action").to_string(),
        "one safe diagnosis; take action"
    );
}

#[test]
fn printable_operands_use_posix_quoting_and_control_operands_use_real_ansi_c_escapes() {
    let ordinary = Operand::text("café/$HOME/it's %2F\\safe");
    assert!(matches!(ordinary.shell(), Shell::Posix));
    assert_eq!(ordinary.to_string(), "'café/$HOME/it'\\''s %2F\\safe'");
    let controls = Operand::text("a\n\u{202e}\\'$x");
    assert!(matches!(controls.shell(), Shell::Bash));
    assert_eq!(controls.to_string(), "$'a\\x0a\\xe2\\x80\\xae\\\\\\'$x'");
    assert!(!controls.to_string().chars().any(char::is_control));
    let nul = Operand::text("a\0b");
    assert!(matches!(nul.shell(), Shell::NotCopyable));
    assert_eq!(nul.to_string(), "<contains NUL; not shell-copyable>");
}

#[test]
fn all_c0_del_and_c1_characters_are_visible_not_terminal_controls() {
    let controls: String = (0..=0x9f)
        .filter(|value| *value < 32 || *value >= 127)
        .filter_map(char::from_u32)
        .collect();
    let shown = Text(&controls).to_string();
    assert!(!shown.chars().any(char::is_control));
    assert_eq!(
        PathText(Path::new("normal/space name")).to_string(),
        "\"normal/space name\""
    );
}

#[cfg(unix)]
#[test]
fn native_non_utf8_paths_preserve_bytes_in_display_and_copy_operands() {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt as _;

    let path = Path::new(OsStr::from_bytes(b"/odd\xff/it's\\name\n"));
    assert_eq!(
        PathText(path).to_string(),
        "native bytes \"/odd\\xff/it's\\x5cname\\x0a\""
    );
    let operand = Operand::path(path);
    assert!(matches!(operand.shell(), Shell::Bash));
    assert_eq!(operand.to_string(), "$'/odd\\xff/it\\'s\\\\name\\x0a'");
    assert!(!PathText(path).to_string().contains('\u{fffd}'));
}
