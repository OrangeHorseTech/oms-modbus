// SPDX-License-Identifier: MIT OR Apache-2.0
//! Unit tests for ClientOptions — builder defaults, getters, combinations.

use std::sync::Arc;
use std::time::Duration;

use oms_modbus::*;

#[test]
fn default_timeout_is_one_second() {
    let opts = ClientOptions::default();
    assert_eq!(opts.timeout(), Duration::from_secs(1));
}

#[test]
fn with_timeout_overrides_default() {
    let opts = ClientOptions::default().with_timeout(Duration::from_secs(5));
    assert_eq!(opts.timeout(), Duration::from_secs(5));
}

#[test]
fn default_bus_timing_is_35t_at_9600() {
    let opts = ClientOptions::default();
    assert!(
        opts.bus_timing().is_some(),
        "default should include BusTiming"
    );
    assert_eq!(
        opts.bus_timing().unwrap().min_spacing(),
        BusTiming::rtu_35t(9600).min_spacing()
    );
}

#[test]
fn with_bus_timing_overrides_default() {
    let custom = BusTiming::custom(Duration::from_millis(100));
    let opts = ClientOptions::default().with_bus_timing(custom);
    assert_eq!(
        opts.bus_timing().unwrap().min_spacing(),
        Duration::from_millis(100)
    );
}

#[test]
fn no_reconnect_by_default() {
    let opts = ClientOptions::default();
    assert!(opts.reconnect().is_none());
}

#[test]
fn with_reconnect_sets_config() {
    let opts = ClientOptions::default().with_reconnect(3, Duration::from_millis(500));
    let cfg = opts.reconnect().unwrap();
    assert_eq!(cfg.max_retries(), 3);
    assert_eq!(cfg.interval(), Duration::from_millis(500));
}

#[test]
fn no_tap_by_default() {
    let opts = ClientOptions::default();
    assert!(opts.tap().is_none());
}

#[test]
fn with_tap_sets_tap() {
    let cap = Arc::new(BusCapture::unbounded());
    let opts = ClientOptions::default().with_tap(cap);
    assert!(opts.tap().is_some());
}

#[test]
fn new_equals_default() {
    assert_eq!(
        ClientOptions::new().timeout(),
        ClientOptions::default().timeout()
    );
}

#[test]
fn builder_can_chain_all_options() {
    let cap = Arc::new(BusCapture::unbounded());
    let timing = BusTiming::custom(Duration::from_millis(10));
    let opts = ClientOptions::default()
        .with_timeout(Duration::from_secs(2))
        .with_tap(cap)
        .with_reconnect(5, Duration::from_millis(100))
        .with_bus_timing(timing);
    assert_eq!(opts.timeout(), Duration::from_secs(2));
    assert!(opts.tap().is_some());
    assert!(opts.reconnect().is_some());
    assert_eq!(opts.reconnect().unwrap().max_retries(), 5);
    assert!(opts.bus_timing().is_some());
}
