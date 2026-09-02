use super::locale_is_utf8;

#[test]
fn utf8_locales_are_recognized() {
    assert!(locale_is_utf8("en_US.UTF-8"));
    assert!(locale_is_utf8("C.utf8"));
    assert!(!locale_is_utf8("C"));
    assert!(!locale_is_utf8("en_US.ISO8859-1"));
}
