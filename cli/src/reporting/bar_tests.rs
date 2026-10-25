use super::{Decoration, Row};

fn row() -> Row {
    Row {
        count: "received 1 KiB/2 KiB".to_owned(),
        detailed: "50% received 1 KiB/2 KiB".to_owned(),
        rate: Some("5 KiB/s".to_owned()),
        eta: Some("ETA 1s".to_owned()),
        determinate: true,
    }
}

#[test]
fn complete_row_budget_shrinks_on_resize_without_a_bar_minimum() {
    let row = row();
    for columns in [200, 80, 40, 20, 80] {
        let layout = row.layout(columns, Decoration::Bar);
        let cells = layout.text.len()
            + 2
            + if layout.bar_cells == 0 {
                0
            } else {
                usize::from(layout.bar_cells) + 3
            };
        assert!(layout.bar_cells <= 48);
        assert!(layout.bar_cells <= columns * 40 / 100);
        if layout.updating {
            assert!(cells <= usize::from(columns));
        }
        if columns < 40 {
            assert_eq!(layout.bar_cells, 0);
            assert!(!layout.text.contains('%'));
        }
    }
    assert!(row.layout(80, Decoration::Bar).bar_cells > 0);
    assert_eq!(row.layout(20, Decoration::Bar).bar_cells, 0);
}

#[test]
fn eta_then_rate_are_dropped_and_oversized_required_counts_are_complete_snapshots() {
    let row = row();
    let narrow = row.layout(40, Decoration::Bar);
    assert!(!narrow.text.contains("ETA"));
    assert!(narrow.text.contains("KiB/s"));
    assert!((1..8).contains(&narrow.bar_cells));
    let narrower = row.layout(28, Decoration::Bar);
    assert!(!narrower.text.contains("ETA"));
    assert!(!narrower.text.contains("KiB/s"));
    let huge = Row {
        count: "received 18446744073709551615 B/18446744073709551615 B".to_owned(),
        detailed:
            "100% received 18446744073709551615 B/18446744073709551615 B; awaiting completion"
                .to_owned(),
        rate: None,
        eta: None,
        determinate: true,
    };
    let layout = huge.layout(20, Decoration::Bar);
    assert!(!layout.updating);
    assert_eq!(layout.text, huge.count);
    assert_eq!(layout.bar_cells, 0);
}

#[test]
fn unknown_and_plain_rows_have_no_determinate_bar() {
    let mut row = row();
    assert_eq!(row.layout(80, Decoration::Plain).bar_cells, 0);
    row.determinate = false;
    assert_eq!(row.layout(80, Decoration::Bar).bar_cells, 0);
    assert_eq!(row.plain(), "50% received 1 KiB/2 KiB  5 KiB/s  ETA 1s");
}
