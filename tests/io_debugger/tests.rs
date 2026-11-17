use crate::io_debugger::order::{Matcher, Order};

#[test]
#[should_panic(expected = "matches left")]
fn order_panics_when_matches_left() {
    let _order = Order::from([Matcher::default()]);
}

#[test]
#[should_panic(expected = "foo")]
fn order_does_not_double_panic() {
    let _order = Order::from([Matcher::default()]);
    panic!("foo");
}
