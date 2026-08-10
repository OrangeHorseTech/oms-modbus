// SPDX-License-Identifier: MIT OR Apache-2.0
//! Integration tests for [`BusTiming`].

use oms_modbus::BusTiming;
use std::time::Duration;

#[test]
fn rtu_35t_values() {
    // ≤ 19200: uses the raw formula
    let t = BusTiming::rtu_35t(9600);
    assert_eq!(t.min_spacing().as_micros() as u64, 3_500_000 * 11 / 9600); // ~4010 µs

    let t = BusTiming::rtu_35t(19200);
    assert_eq!(t.min_spacing().as_micros() as u64, 3_500_000 * 11 / 19200); // ~2005 µs

    // >19200: spec caps at 1750 µs
    let t = BusTiming::rtu_35t(38400);
    assert_eq!(t.min_spacing().as_micros() as u64, 1750);

    let t = BusTiming::rtu_35t(115200);
    assert_eq!(t.min_spacing().as_micros() as u64, 1750);

    let t = BusTiming::rtu_35t(921600);
    assert_eq!(t.min_spacing().as_micros() as u64, 1750);
}

#[test]
fn rtu_35t_raw_no_cap() {
    // Raw formula is always used, even at high baud rates
    let t = BusTiming::rtu_35t_raw(9600);
    assert_eq!(t.min_spacing().as_micros() as u64, 3_500_000 * 11 / 9600);

    let t = BusTiming::rtu_35t_raw(115200);
    assert_eq!(t.min_spacing().as_micros() as u64, 3_500_000 * 11 / 115200); // ~334 µs

    let t = BusTiming::rtu_35t_raw(921600);
    assert_eq!(t.min_spacing().as_micros() as u64, 3_500_000 * 11 / 921600); // ~41 µs
}

#[test]
fn custom_values() {
    let t = BusTiming::custom(Duration::from_millis(50));
    assert_eq!(t.min_spacing().as_micros() as u64, 50_000);
}

#[tokio::test]
async fn first_call_returns_immediately() {
    let t = BusTiming::custom(Duration::from_secs(999)); // huge gap
                                                         // No touch() called — should return immediately (first-call fast path)
    let start = tokio::time::Instant::now();
    t.wait_if_needed().await;
    assert!(start.elapsed() < Duration::from_millis(1));
}

#[tokio::test]
async fn touch_then_wait_respects_spacing() {
    // After touch(), wait_if_needed() must delay at least min_spacing.
    let spacing = Duration::from_millis(10);
    let t = BusTiming::custom(spacing);
    t.touch();

    let start = tokio::time::Instant::now();
    t.wait_if_needed().await;
    let elapsed = start.elapsed();

    assert!(
        elapsed >= spacing,
        "wait_if_needed must delay ≥{:?} after touch(), actual: {elapsed:?}",
        spacing
    );
}

#[test]
fn ascii_35t_same_as_rtu_35t() {
    // ASCII 3.5T delegates to the same formula as RTU
    let rtu = BusTiming::rtu_35t(9600);
    let ascii = BusTiming::ascii_35t(9600);
    assert_eq!(rtu.min_spacing(), ascii.min_spacing());

    // Also applies the spec cap above 19200 baud
    let rtu = BusTiming::rtu_35t(115200);
    let ascii = BusTiming::ascii_35t(115200);
    assert_eq!(rtu.min_spacing(), ascii.min_spacing());
}

#[test]
fn min_spacing_returns_correct_duration() {
    let t = BusTiming::rtu_35t(9600);
    assert_eq!(
        t.min_spacing(),
        Duration::from_micros(3_500_000 * 11 / 9600)
    );

    let t = BusTiming::custom(Duration::from_millis(100));
    assert_eq!(t.min_spacing(), Duration::from_millis(100));
}

// ── P2-4: Quantitative accuracy & robustness ────────────────────────────

#[tokio::test]
async fn wait_if_needed_actually_sleeps() {
    // After touch, wait_if_needed must sleep for at least min_spacing.
    let t = BusTiming::custom(Duration::from_millis(80));
    t.touch();
    // Small delay so internal elapsed is non-zero
    tokio::time::sleep(Duration::from_millis(1)).await;
    let start = std::time::Instant::now();
    t.wait_if_needed().await;
    let elapsed = start.elapsed();
    // With 1ms already passed, remaining ≈ 79ms. Allow 5ms tolerance.
    assert!(
        elapsed >= Duration::from_millis(75),
        "expected >=75ms elapsed, got {elapsed:?}"
    );
}

#[tokio::test]
async fn concurrent_touch_multiple_tasks() {
    use std::sync::Arc;
    let t = Arc::new(BusTiming::custom(Duration::from_millis(10)));

    let t1 = t.clone();
    let t2 = t.clone();
    let h1 = tokio::spawn(async move {
        for _ in 0..50 {
            t1.touch();
            t1.wait_if_needed().await;
        }
    });
    let h2 = tokio::spawn(async move {
        for _ in 0..50 {
            t2.touch();
            t2.wait_if_needed().await;
        }
    });
    // Must complete without panic or hang
    h1.await.unwrap();
    h2.await.unwrap();
}

#[test]
fn rtu_35t_zero_baud_clamped() {
    // Baud=0 → clamped to 1 via `baud_rate.max(1)`. Must not panic.
    let t = BusTiming::rtu_35t(0);
    let spacing = t.min_spacing();
    // With baud=1: 3_500_000 * 11 / 1 = 38_500_000 µs = 38.5 s
    assert_eq!(spacing.as_micros() as u64, 3_500_000 * 11);

    let t = BusTiming::rtu_35t_raw(0);
    let spacing = t.min_spacing();
    assert_eq!(spacing.as_micros() as u64, 3_500_000 * 11);
}

#[test]
fn rtu_35t_max_baud_no_overflow() {
    // Baud=u32::MAX → formula divides by u32::MAX, cast to u64 → no overflow.
    let t = BusTiming::rtu_35t(u32::MAX);
    // >19200 so spec cap applies → 1750 µs
    assert_eq!(t.min_spacing().as_micros() as u64, 1750);

    // Raw formula: 3_500_000 * 11 / u32::MAX ≈ 38_500_000 / 4_294_967_295 ≈ 0
    let t = BusTiming::rtu_35t_raw(u32::MAX);
    let spacing = t.min_spacing();
    assert_eq!(spacing.as_micros(), 0, "raw formula at max baud → 0 µs");
}

#[test]
fn now_micros_monotonic_1000_calls() {
    // Public now_micros() uses Instant::elapsed() + epoch offset → monotonic.
    // 1000 consecutive calls must be non-decreasing.
    let mut prev = oms_modbus::now_micros();
    for _ in 0..1000 {
        let cur = oms_modbus::now_micros();
        assert!(
            cur >= prev,
            "now_micros must be non-decreasing: {prev} → {cur}"
        );
        prev = cur;
    }
}

#[test]
fn custom_zero_duration_does_not_panic() {
    // Zero-length spacing — touch + wait should return immediately.
    let t = BusTiming::custom(Duration::ZERO);
    t.touch();
    // touch sets last_event to now_micros().max(1), so last > 0.
    // wait_if_needed computes elapsed = now - last.
    // If now ≈ last (common case: touch + wait in same microsecond),
    // elapsed < 0 (saturating_sub → 0), so remaining = 0, returns immediately.
    // This must not panic.
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    rt.block_on(async {
        t.wait_if_needed().await;
    });
}
