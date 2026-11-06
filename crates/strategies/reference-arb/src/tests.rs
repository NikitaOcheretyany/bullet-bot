#![allow(clippy::unwrap_used, clippy::expect_used, clippy::float_cmp)]
use std::sync::Arc;

use bb_core::broker::BrokerRegistry;
use bb_core::clock::{Clock, TestClock};
use bb_core::events::{OrderLifecycle, Trade};
use bb_core::harness::MockBroker;
use bb_core::types::{Order, OrderResult, Position};

use super::*;
use crate::config::ImpulseConfig;

fn d(s: &str) -> Decimal {
    s.parse().unwrap()
}
fn config() -> ReferenceArbConfig {
    ReferenceArbConfig {
        exchange: "bullet".into(),
        symbol: "LIT-USD".into(),
        binance_symbol: "litusdt".into(),
        binance_market: "perp".into(),
        order_notional: d("1000"),
        basis_half_life_secs: 30.0,
        entry_threshold_bps: 1.0,
        entry_reserve_bps: 0.0,
        exit: ExitConfig::Fair { remaining_bps: -0.25 },
        stop_loss_bps: 5.0,
        max_drawdown_usd: None,
        max_hold_secs: 60,
        reference_stale_ms: 1000,
        impulse: Some(ImpulseConfig { window_ms: 50, threshold_bps: 1.0 }),
        market_slippage_bps: d("50"),
        dry_run: false,
    }
}
#[allow(clippy::default_trait_access)]
fn context() -> (Arc<MockBroker>, TestClock, ActorContext) {
    let broker = MockBroker::shared("bullet");
    let clock = TestClock::new(1_000_000);
    let mut registry = BrokerRegistry::new();
    registry.insert(Arc::from("bullet"), broker.clone()).unwrap();
    let cx = ActorContext::with_clock(
        Arc::from("test"),
        Arc::new(registry),
        Arc::new(clock.clone()),
        Default::default(),
    );
    (broker, clock, cx)
}
fn book(bid: &str, ask: &str, qty: &str, at: Instant) -> BookUpdate {
    BookUpdate {
        exchange: "bullet".into(),
        symbol: "LIT-USD".into(),
        received_at: at,
        orderbook: OrderBook {
            bids: [(d(bid), d(qty))].into(),
            asks: [(d(ask), d(qty))].into(),
            last_update_id: 1,
        },
    }
}
fn reference(midpoint: &str, micro: &str, at: Instant) -> ReferencePriceUpdate {
    ReferencePriceUpdate {
        symbol: "LITUSDT".into(),
        midpoint: d(midpoint),
        mid: d(micro),
        received_at: at,
    }
}
fn ready(clock: &TestClock) -> ReferenceArbActor {
    let mut a =
        ReferenceArbActor::new(config(), Decimal::ONE, tokio::sync::mpsc::unbounded_channel().0);
    a.bullet_book = Some(book("0.9999", "1.0001", "2000", clock.now()).orderbook);
    a.bullet_last_seen = Some(clock.now());
    a.binance = Some(reference("1", "1", clock.now()));
    a.basis = Some(0.0);
    a.basis_at = Some(clock.now());
    a
}
fn confirmation(
    a: &ReferenceArbActor,
    status: OrderStatus,
    fill: Option<(Side, &str, &str)>,
    at: Instant,
) -> ExecutionUpdate {
    // Recovery rotates tracked orders; the latest submitted one need not be last.
    let client_id =
        &a.tracked.iter().max_by_key(|t| t.client_id.parse::<u64>().unwrap()).unwrap().client_id;
    confirmation_for(a, client_id, status, fill, at)
}
fn confirmation_for(
    a: &ReferenceArbActor,
    client_id: &str,
    status: OrderStatus,
    fill: Option<(Side, &str, &str)>,
    at: Instant,
) -> ExecutionUpdate {
    let tracked = a.tracked.iter().find(|t| t.client_id == client_id).unwrap();
    let client_id = client_id.to_owned();
    ExecutionUpdate {
        stream_started_at: None,
        received_at: at,
        trade: fill.map(|(side, price, quantity)| Trade {
            exchange: "bullet".into(),
            symbol: "LIT-USD".into(),
            order_id: "1".into(),
            trade_id: Some(format!("fill-{}", a.inventory.total_fills)),
            client_id: Some(client_id.clone()),
            side,
            price: d(price),
            quantity: d(quantity),
            fee: Decimal::ZERO,
            realized_pnl: None,
            timestamp: None,
        }),
        lifecycle: OrderLifecycle {
            exchange: "bullet".into(),
            order: Order {
                id: "1".into(),
                client_id: Some(client_id),
                symbol: "LIT-USD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: d("1.0011"),
                quantity: if status == OrderStatus::Filled {
                    tracked.filled + fill.map_or(Decimal::ZERO, |(_, _, q)| d(q))
                } else {
                    tracked.quantity
                },
                // Bullet cancellation messages do not supply a cumulative count.
                filled_quantity: if matches!(status, OrderStatus::Cancelled | OrderStatus::Rejected)
                {
                    Decimal::ZERO
                } else {
                    tracked.filled + fill.map_or(Decimal::ZERO, |(_, _, q)| d(q))
                },
                status,
            },
        },
    }
}
fn position(size: Decimal) -> Vec<Position> {
    if size.is_zero() {
        vec![]
    } else {
        vec![Position {
            symbol: "LIT-USD".into(),
            side: Some(if size.is_sign_negative() { Side::Sell } else { Side::Buy }),
            size: size.abs(),
            ..Default::default()
        }]
    }
}
async fn settle_submissions(a: &mut ReferenceArbActor, cx: &ActorContext) {
    while let Some(t) = a.tracked.iter().find(|t| t.submission.is_some()) {
        let event = OrderTaskReady::Submission(t.client_id.clone());
        a.finish_task(Some(&event), cx).await;
    }
}
fn task_events(a: &mut ReferenceArbActor) -> tokio::sync::mpsc::UnboundedReceiver<OrderTaskReady> {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    a.task_tx = tx;
    rx
}
async fn deliver_recovery(
    a: &mut ReferenceArbActor,
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<OrderTaskReady>,
    cx: &ActorContext,
) {
    loop {
        let event = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await.unwrap().unwrap();
        let recovered = matches!(event, OrderTaskReady::Recovery(_));
        a.on_event(event, cx).await.unwrap();
        settle_submissions(a, cx).await;
        if recovered {
            break;
        }
    }
}
fn holding(clock: &TestClock, side: Side) -> ReferenceArbActor {
    let mut a = ready(clock);
    a.inventory.record_fill(side, Decimal::ONE, d("20"));
    a.entry = Some(Entry { side, basis: 0.0, filled_at: Some(clock.now()), best_close: None });

    a
}

#[test]
fn config_allows_requested_edge_without_invented_fee_floor() {
    config().validate().unwrap();
    let mut c = config();
    c.impulse = None;
    c.entry_threshold_bps = 0.0;
    c.exit = ExitConfig::Fair { remaining_bps: 0.0 };
    c.validate().unwrap();
    c.basis_half_life_secs = f64::NAN;
    assert!(c.validate().is_err());
    c.basis_half_life_secs = 30.0;
    c.market_slippage_bps = d("10000");
    assert!(c.validate().is_err());
}

#[test]
fn exit_config_selects_one_mode_and_rejects_obsolete_fields() {
    let mut value = serde_json::to_value(config()).unwrap();
    for exit in [
        serde_json::json!({"mode": "fair", "remaining_bps": -1.5}),
        serde_json::json!({"mode": "trailing", "drawdown_bps": 2.0}),
    ] {
        value["exit"] = exit;
        serde_json::from_value::<ReferenceArbConfig>(value.clone()).unwrap().validate().unwrap();
    }
    for exit in [
        serde_json::json!({"mode": "fair", "remaining_bps": 1.0, "drawdown_bps": 2.0}),
        serde_json::json!({"mode": "trailing", "drawdown_bps": 2.0, "remaining_bps": -1.5}),
        serde_json::json!({"mode": "trailing"}),
        serde_json::json!({"mode": "unknown"}),
    ] {
        value["exit"] = exit;
        assert!(serde_json::from_value::<ReferenceArbConfig>(value.clone()).is_err());
    }
    for (key, obsolete) in [
        ("subtract_spread", serde_json::json!(false)),
        ("entry_price_offset", serde_json::json!("0.0003")),
        ("exit_threshold_bps", serde_json::json!(1.5)),
    ] {
        let mut value = serde_json::to_value(config()).unwrap();
        value[key] = obsolete;
        assert!(serde_json::from_value::<ReferenceArbConfig>(value).is_err(), "{key}");
    }
    let mut c = config();
    for invalid in [-0.1, f64::NAN, f64::INFINITY] {
        c.entry_reserve_bps = invalid;
        assert!(c.validate().is_err());
    }
    c.entry_reserve_bps = 0.0;
    for invalid in [0.0, -1.0, f64::NAN, f64::INFINITY] {
        c.exit = ExitConfig::Trailing { drawdown_bps: invalid };
        assert!(c.validate().is_err());
    }
}

#[tokio::test]
async fn fair_ioc_limit_and_basis_are_frozen_at_signal_for_both_sides() {
    for side in [Side::Buy, Side::Sell] {
        let (broker, clock, cx) = context();
        let mut a = ready(&clock);
        a.config.entry_threshold_bps = 4.0;
        a.config.entry_reserve_bps = 1.0;
        a.config.impulse = None;
        let (bid, ask) = if side == Side::Buy { ("99.94", "99.95") } else { ("100.05", "100.06") };
        a.bullet_book = Some(book(bid, ask, "2000", clock.now()).orderbook);
        a.binance = Some(reference("100", "100", clock.now()));
        a.evaluate(&cx, true).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        let submitted = broker.last_placed_orders().await.pop().unwrap();
        assert_eq!(submitted.side, side);
        let expected = 100.0 * (-sign(side) / BPS).exp();
        assert!((submitted.price.to_f64().unwrap() - expected).abs() < 1e-12);
        assert!(
            sign(side) * (a.touch_price(side).unwrap() - submitted.price).to_f64().unwrap() < 0.0
        );
        let frozen = a.entry.unwrap().basis;
        clock.advance(Duration::from_millis(50));
        let moved = if side == Side::Buy { "101" } else { "99" };
        a.on_event(reference(moved, moved, clock.now()), &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        assert_eq!(broker.placed_count().await, 1);
        assert_eq!(broker.last_placed_orders().await[0].price, submitted.price);
        assert_eq!(a.entry.unwrap().basis, frozen);
        assert_ne!(a.basis, Some(frozen));
    }
}

#[tokio::test]
async fn reentry_reset_uses_executable_edge_not_midpoint_distance() {
    let (_, clock, cx) = context();
    for side in [Side::Buy, Side::Sell] {
        let mut a = ready(&clock);
        a.bullet_book = Some(book("99.99", "100.01", "2000", clock.now()).orderbook);
        a.binance = Some(reference(
            "100",
            if side == Side::Buy { "100.015" } else { "99.985" },
            clock.now(),
        ));
        a.reentry[side_index(side)] = Reentry { reset: false, confirmed_at: None };
        assert!(a.remaining(side, 0.0).unwrap() > a.config.entry_threshold_bps);
        assert!(a.edge(side).unwrap() < a.config.entry_threshold_bps);
        a.evaluate(&cx, false).await.unwrap();
        assert!(a.reentry[side_index(side)].reset);
    }
}

#[test]
fn fair_exit_uses_direct_signed_r() {
    let (_, clock, _) = context();
    for side in [Side::Buy, Side::Sell] {
        let mut a = holding(&clock, side);
        let ahead = if side == Side::Buy { "1.0002" } else { "0.9998" };
        a.binance = Some(reference(ahead, ahead, clock.now()));
        a.config.exit = ExitConfig::Fair { remaining_bps: 1.0 };
        assert!(a.exit_reason(clock.now()).is_none());
        a.config.exit = ExitConfig::Fair { remaining_bps: 3.0 };
        assert!(matches!(a.exit_reason(clock.now()), Some(ExitReason::Fair)));
        a.config.exit = ExitConfig::Fair { remaining_bps: -1.5 };
        assert!(a.exit_reason(clock.now()).is_none());
        let behind = if side == Side::Buy { "0.9998" } else { "1.0002" };
        a.binance = Some(reference(behind, behind, clock.now()));
        assert!(matches!(a.exit_reason(clock.now()), Some(ExitReason::Fair)));
    }
}

#[tokio::test]
async fn trailing_starts_at_fill_survives_partial_duplicates_and_closes_unknown_ioc() {
    for side in [Side::Buy, Side::Sell] {
        let (broker, clock, cx) = context();
        let mut a = ready(&clock);
        a.config.exit = ExitConfig::Trailing { drawdown_bps: 2.0 };
        let old_extreme = if side == Side::Buy { "1.01" } else { "0.99" };
        a.bullet_book = Some(book(old_extreme, old_extreme, "2000", clock.now()).orderbook);
        a.place_entry(&cx, side).unwrap();
        settle_submissions(&mut a, &cx).await;
        let entry_id = a.tracked[0].client_id.clone();
        let requested = a.tracked[0].quantity;
        a.evaluate(&cx, false).await.unwrap();
        assert!(a.entry.unwrap().best_close.is_none());
        assert!(a.exit_requested.is_none()); // no fair exit in trailing mode
        a.on_event(book("1", "1", "2000", clock.now()), &cx).await.unwrap();
        let first =
            confirmation(&a, OrderStatus::PartiallyFilled, Some((side, "1", "5")), clock.now());
        a.on_event(first, &cx).await.unwrap();
        let filled_at = a.entry.unwrap().filled_at;
        assert_eq!(a.entry.unwrap().best_close, Some(d("1")));
        clock.advance(Duration::from_millis(10));
        let (bid, ask, best) = if side == Side::Buy {
            ("1.001", "1.0011", "1.001")
        } else {
            ("0.9989", "0.999", "0.999")
        };
        a.on_event(book(bid, ask, "2000", clock.now()), &cx).await.unwrap();
        let second =
            confirmation(&a, OrderStatus::PartiallyFilled, Some((side, "1", "5")), clock.now());
        a.on_event(second.clone(), &cx).await.unwrap();
        a.on_event(second, &cx).await.unwrap();
        assert_eq!(a.inventory.net_position.abs(), d("10"));
        assert_eq!(a.inventory.total_fills, 2);
        assert_eq!(a.entry.unwrap().filled_at, filled_at);
        assert_eq!(a.entry.unwrap().best_close, Some(d(best)));
        let (bid, ask) = if side == Side::Buy { ("1.0009", "1.001") } else { ("0.999", "0.9991") };
        a.on_event(book(bid, ask, "2000", clock.now()), &cx).await.unwrap();
        assert!(a.exit_requested.is_none());
        assert_eq!(a.entry.unwrap().best_close, Some(d(best)));
        let (bid, ask) =
            if side == Side::Buy { ("1.0007", "1.0008") } else { ("0.9992", "0.9993") };
        a.on_event(book(bid, ask, "2000", clock.now()), &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        assert!(matches!(a.exit_requested, Some(ExitReason::Trailing)));
        assert_eq!(broker.placed_count().await, 2);
        let close = broker.last_placed_orders().await.pop().unwrap();
        assert!(close.reduce_only);
        assert_eq!(close.side, side.opposite());
        assert_eq!(close.quantity, requested); // unknown residual must not block protection
        let cancel = confirmation_for(&a, &entry_id, OrderStatus::Cancelled, None, clock.now());
        a.on_event(cancel, &cx).await.unwrap();
        let partial_close = confirmation(
            &a,
            OrderStatus::Cancelled,
            Some((side.opposite(), "1", "5")),
            clock.now(),
        );
        a.on_event(partial_close, &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        assert_eq!(broker.placed_count().await, 3);
        assert_eq!(broker.last_placed_orders().await[0].quantity, d("5"));
        a.on_event(book(best, best, "2000", clock.now()), &cx).await.unwrap();
        assert!(matches!(a.exit_requested, Some(ExitReason::Trailing)));
        let last =
            confirmation(&a, OrderStatus::Filled, Some((side.opposite(), "1", "5")), clock.now());
        a.on_event(last, &cx).await.unwrap();
        assert!(a.is_flat());
        assert!(a.entry.is_none());
        a.place_entry(&cx, side).unwrap();
        assert!(a.entry.unwrap().best_close.is_none());
    }
}

#[tokio::test]
async fn trailing_keeps_stop_and_timeout_protection_through_market_disconnect() {
    for side in [Side::Buy, Side::Sell] {
        for stop in [true, false] {
            let (broker, clock, cx) = context();
            let mut a = if stop { holding(&clock, side) } else { ready(&clock) };
            a.config.exit = ExitConfig::Trailing { drawdown_bps: 100.0 };
            if !stop {
                broker.queue_place_response(Err(BotError::exchange("response lost", true))).await;
                a.place_entry(&cx, side).unwrap();
                settle_submissions(&mut a, &cx).await;
                assert!(a.entry.unwrap().best_close.is_none());
            }
            clock.advance(Duration::from_secs(if stop { 2 } else { 60 }));
            broker.flag_reconnecting(clock.now());
            if stop {
                a.bullet_book = Some(book("0.9995", "1.0005", "100", clock.now()).orderbook);
            }
            assert!(!a.quotes_ready(&cx).unwrap());
            a.evaluate(&cx, false).await.unwrap();
            settle_submissions(&mut a, &cx).await;
            assert!(matches!(
                (stop, a.exit_requested),
                (true, Some(ExitReason::StopLoss)) | (false, Some(ExitReason::Timeout))
            ));
            let close = broker.last_placed_orders().await.pop().unwrap();
            assert!(close.reduce_only);
            assert_eq!(close.side, side.opposite());
            assert!(close.quantity > Decimal::ZERO);
        }
    }
}

#[tokio::test]
async fn recovered_entry_initializes_trailing_from_current_quote_not_old_extremes() {
    let (_, clock, cx) = context();
    let mut a = ready(&clock);
    a.config.exit = ExitConfig::Trailing { drawdown_bps: 2.0 };
    a.place_entry(&cx, Side::Buy).unwrap();
    settle_submissions(&mut a, &cx).await;
    let id = a.tracked[0].client_id.clone();
    a.on_event(book("1.01", "1.0101", "100", clock.now()), &cx).await.unwrap();
    clock.advance(Duration::from_secs(5));
    a.on_event(book("1.001", "1.0011", "100", clock.now()), &cx).await.unwrap();
    let recovered = confirmation(&a, OrderStatus::Filled, Some((Side::Buy, "1", "5")), clock.now());
    a.apply_recovery(
        &id,
        OrderRecovery {
            order: Some(recovered.lifecycle.order),
            trades: vec![recovered.trade.unwrap()],
            submission_expired: false,
        },
        &cx,
    );
    assert_eq!(a.entry.unwrap().best_close, Some(d("1.001")));
    assert_eq!(a.trailing_drawdown_bps(), Some(0.0));
    assert!(a.exit_reason(clock.now()).is_none());
}

#[test]
fn drawdown_limit_is_optional_and_requires_a_positive_amount() {
    let mut c = config();
    let mut legacy = serde_json::to_value(&c).unwrap();
    legacy.as_object_mut().unwrap().remove("max_drawdown_usd");
    assert!(
        serde_json::from_value::<ReferenceArbConfig>(legacy).unwrap().max_drawdown_usd.is_none()
    );
    for amount in ["0", "-20"] {
        c.max_drawdown_usd = Some(d(amount));
        assert!(c.validate().unwrap_err().contains("max_drawdown_usd"));
    }
    c.max_drawdown_usd = Some(d("20"));
    c.validate().unwrap();
}

#[tokio::test]
async fn realized_drawdown_follows_the_profit_peak_and_never_rearms_this_run() {
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    a.config.max_drawdown_usd = Some(d("20"));
    for (profit, peak, stopped) in [
        ("12", "12", false),
        ("58", "70", false),
        ("-15", "70", false),
        ("-4.9999", "70", false),
        ("-0.0001", "70", true),
    ] {
        a.inventory.record_fill(Side::Buy, d("1"), d("100"));
        a.inventory.record_fill(Side::Sell, d("1") + d(profit) / d("100"), d("100"));
        a.check_drawdown(&cx);
        assert_eq!(a.peak_pnl, d(peak));
        assert_eq!(a.drawdown_triggered, stopped);
    }
    assert_eq!(a.inventory.realized_pnl, d("50"));
    assert_eq!(d(a.status()["drawdown"].as_str().unwrap()), d("20"));
    // Even a later recovered profit or queued signal cannot resume a stopped run.
    a.inventory.record_fill(Side::Buy, d("1"), d("100"));
    a.inventory.record_fill(Side::Sell, d("2"), d("100"));
    a.check_drawdown(&cx);
    a.config.impulse = None;
    a.on_event(reference("1.001", "1.001", clock.now()), &cx).await.unwrap();
    assert!(a.drawdown_triggered);
    assert_eq!(a.state(), "stopping");
    assert!(broker.history().await.is_empty(), "no balance polling or reentry");
    let next_run = ready(&clock);
    assert_eq!(next_run.peak_pnl, Decimal::ZERO);
    assert!(!next_run.drawdown_triggered);
}

#[tokio::test]
async fn quotes_do_not_change_realized_pnl_peak_or_trigger_drawdown() {
    for side in [Side::Buy, Side::Sell] {
        let (broker, clock, cx) = context();
        let mut a = holding(&clock, side);
        a.config.max_drawdown_usd = Some(d("20"));
        a.config.stop_loss_bps = 100_000.0;
        a.config.exit = ExitConfig::Fair { remaining_bps: -100_000.0 };
        a.inventory.realized_pnl = d("70");
        a.check_drawdown(&cx);
        for (bid, ask) in [("3", "4"), ("0.1", "0.2"), ("1", "1.1")] {
            a.on_event(book(bid, ask, "100", clock.now()), &cx).await.unwrap();
            a.on_event(Tick::now(), &cx).await.unwrap();
            assert_eq!(a.inventory.realized_pnl, d("70"));
            assert_eq!(a.peak_pnl, d("70"));
            assert!(!a.drawdown_triggered);
        }
        assert!(broker.history().await.is_empty());
    }
}

#[tokio::test]
async fn venue_realized_pnl_and_fees_are_counted_once_across_ws_and_recovery() {
    let (broker, clock, cx) = context();
    let mut a = holding(&clock, Side::Buy);
    a.config.max_drawdown_usd = Some(d("20"));
    a.place_exit(&cx, ExitReason::StopLoss).unwrap();
    settle_submissions(&mut a, &cx).await;
    let client_id = a.tracked[0].client_id.clone();
    let mut first =
        confirmation(&a, OrderStatus::PartiallyFilled, Some((Side::Sell, "0.9", "5")), clock.now());
    // Venue PnL is authoritative even when the local entry VWAP is incomplete.
    first.trade.as_mut().unwrap().realized_pnl = Some(d("-5"));
    first.trade.as_mut().unwrap().fee = d("0.25");
    a.on_event(first.clone(), &cx).await.unwrap();
    a.on_event(first.clone(), &cx).await.unwrap();
    assert_eq!(a.inventory.realized_pnl, d("-5.25"));
    assert!(!a.drawdown_triggered);
    let mut last =
        confirmation(&a, OrderStatus::Filled, Some((Side::Sell, "0.9", "15")), clock.now());
    last.trade.as_mut().unwrap().realized_pnl = Some(d("-14.25"));
    last.trade.as_mut().unwrap().fee = d("0.5");
    a.apply_recovery(
        &client_id,
        OrderRecovery {
            submission_expired: false,
            order: Some(last.lifecycle.order.clone()),
            trades: vec![first.trade.clone().unwrap(), last.trade.clone().unwrap()],
        },
        &cx,
    );
    a.on_event(last, &cx).await.unwrap();
    a.on_event(first, &cx).await.unwrap();
    assert!(a.is_flat());
    assert_eq!(a.inventory.total_fills, 3);
    assert_eq!(a.inventory.realized_pnl, d("-20"));
    assert!(a.drawdown_triggered, "initial peak is zero even before any profit");
    assert_eq!(broker.placed_count().await, 1);
}

#[tokio::test(start_paused = true)]
async fn realized_drawdown_does_not_wait_for_an_unknown_entry_to_send_protection() {
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    a.config.max_drawdown_usd = Some(d("20"));
    broker.set_place_delay(Duration::from_secs(5)).await;
    a.place_entry(&cx, Side::Buy).unwrap();
    tokio::task::yield_now().await;
    let mut fill =
        confirmation(&a, OrderStatus::PartiallyFilled, Some((Side::Buy, "1", "7")), clock.now());
    fill.trade.as_mut().unwrap().fee = d("20");
    fill.trade.as_mut().unwrap().realized_pnl = Some(Decimal::ZERO);
    tokio::time::timeout(Duration::from_millis(10), a.on_event(fill.clone(), &cx))
        .await
        .unwrap()
        .unwrap();
    tokio::task::yield_now().await;
    a.on_event(fill, &cx).await.unwrap();
    assert!(a.drawdown_triggered);
    assert!(matches!(a.exit_requested, Some(ExitReason::Drawdown)));
    assert_eq!(a.inventory.realized_pnl, d("-20"));
    assert_eq!(broker.placed_count().await, 2);
    let close = broker.last_placed_orders().await.pop().unwrap();
    assert!(close.reduce_only);
    assert_eq!(close.quantity, d("999"));
    assert!(a.tracked.iter().all(|t| t.submission.is_some()));
}

#[tokio::test(start_paused = true)]
async fn realized_drawdown_shuts_down_the_harness_after_paper_close_without_account_queries() {
    use bb_core::harness::{ActorSpec, HarnessBuilder, TimedFeed};

    let (broker, clock, _) = context();
    let mut a = holding(&clock, Side::Buy);
    a.config.dry_run = true;
    a.config.max_drawdown_usd = Some(d("0.01"));
    // The price stop realizes -0.02 on 20 units, then the run's drawdown stops
    // the harness. The open loss alone must not update the realized peak.
    let update = book("0.999", "0.9992", "100", clock.now());
    let harness = HarnessBuilder::new()
        .with_clock(Arc::new(clock))
        .wire_broker("bullet", broker.clone())
        .wire_feed_named(
            "books",
            TimedFeed::new(vec![
                (Duration::ZERO, update.clone()),
                (Duration::from_secs(3600), update),
            ]),
        )
        .wire_actor(ActorSpec::new("arb", a).sub::<BookUpdate>())
        .build()
        .unwrap();
    let reason =
        tokio::time::timeout(Duration::from_millis(100), harness.run()).await.unwrap().unwrap();
    assert!(matches!(reason, WindDownReason::Signal));
    assert!(broker.history().await.is_empty());
}
#[test]
fn time_weighted_basis_uses_midpoint_and_half_life() {
    let (_, clock, cx) = context();
    let mut a = ready(&clock);
    a.binance = Some(reference("0.99", "1.01", clock.now()));
    a.update_basis(&cx).unwrap(); // zero elapsed: unchanged
    assert_eq!(a.basis, Some(0.0));
    clock.advance(Duration::from_secs(30));
    a.binance.as_mut().unwrap().received_at = clock.now();
    a.update_basis(&cx).unwrap(); // unchanged Bullet BBO is still valid after 30 s
    let expected = (1.0_f64 / 0.99).ln() / 2.0;
    assert!((a.basis.unwrap() - expected).abs() < 1e-12);
    // Frozen trade basis must not follow the global EMA.
    a.entry =
        Some(Entry { side: Side::Buy, basis: 0.0, filled_at: Some(clock.now()), best_close: None });
    assert!(
        (a.remaining(Side::Buy, a.entry.unwrap().basis).unwrap() - BPS * 1.01_f64.ln()).abs()
            < 1e-10
    );
}
#[test]
fn impulse_uses_last_received_at_or_before_cutoff_not_nearest() {
    let (_, clock, _) = context();
    let mut a = ready(&clock);
    let start = clock.now();
    a.history.extend([
        (start, d("1")),
        (start + Duration::from_millis(49), d("1.0002")),
        (start + Duration::from_millis(51), d("2")),
    ]);
    clock.advance(Duration::from_millis(100));
    a.binance = Some(reference("1.0004", "1.0009", clock.now()));
    a.prune_history(clock.now());
    assert_eq!(a.history.front().unwrap().1, d("1.0002"));
    let expected = BPS * (1.0004_f64 / 1.0002).ln();
    assert!((a.impulse(Side::Buy, clock.now()).unwrap() - expected).abs() < 1e-10);
    assert!((a.impulse(Side::Sell, clock.now()).unwrap() + expected).abs() < 1e-10);
}
#[tokio::test]
async fn entry_threshold_and_impulse_are_strict_and_filter_can_be_disabled() {
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    a.binance = Some(reference("1.0003", "1.0003", clock.now()));
    a.config.entry_threshold_bps = a.edge(Side::Buy).unwrap();
    a.config.impulse = None;
    a.evaluate(&cx, true).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(broker.placed_count().await, 0); // equality does not enter
    a.config.entry_threshold_bps = 1.0;
    a.config.impulse = config().impulse;
    a.evaluate(&cx, true).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(broker.placed_count().await, 0); // missing history is not zero/future data
    a.history.push_back((clock.now().checked_sub(Duration::from_millis(50)).unwrap(), d("1")));
    a.config.impulse.as_mut().unwrap().threshold_bps = a.impulse(Side::Buy, clock.now()).unwrap();
    a.evaluate(&cx, true).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(broker.placed_count().await, 0);
    a.config.impulse.as_mut().unwrap().threshold_bps = 1.0;
    a.evaluate(&cx, true).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(broker.placed_count().await, 1);
    let o = &broker.last_placed_orders().await[0];
    assert_eq!(o.side, Side::Buy);
    assert_eq!(o.price, d("1.0003"));
    assert_eq!(o.quantity, d("999"));
    assert!(o.quantity * o.price <= d("1000"));
    assert!(!o.reduce_only);
    assert_eq!(o.order_type, OrderType::Market);
    a.evaluate(&cx, true).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(broker.placed_count().await, 1); // pending IOC blocks averaging/re-entry
}
#[test]
fn edge_uses_executable_side_and_ioc_reserves_signal_fair() {
    for side in [Side::Buy, Side::Sell] {
        let (_, clock, _) = context();
        let mut a = ready(&clock);
        a.bullet_book = Some(book("99.95", "100.05", "2000", clock.now()).orderbook);
        let micro = if side == Side::Buy { "100.1" } else { "99.9" };
        a.binance = Some(reference("100", micro, clock.now()));
        a.config.entry_reserve_bps = 1.0;
        let touch = a.touch_price(side).unwrap();
        let expected = sign(side) * BPS * (d(micro) / touch).to_f64().unwrap().ln();
        assert!((a.edge(side).unwrap() - expected).abs() < 1e-9);
        let order = a.entry_order(side).unwrap();
        let limit_edge = sign(side) * BPS * log_ratio(d(micro), order.price).unwrap();
        assert!((limit_edge - 1.0).abs() < 1e-9);
        assert!(order.quantity * touch.max(order.price) <= d("1000"));
        assert_eq!(order.order_type, OrderType::Market); // adapter maps this to IOC
        assert!(!order.reduce_only);
    }
}
#[tokio::test]
async fn eth_budget_and_reduce_only_close_support_fractional_lots() {
    for dry_run in [false, true] {
        for side in [Side::Buy, Side::Sell] {
            let (broker, clock, cx) = context();
            let mut cfg = config();
            cfg.symbol = "ETH-USD".into();
            cfg.order_notional = d("20");
            cfg.dry_run = dry_run;
            let mut a =
                ReferenceArbActor::new(cfg, d("0.001"), tokio::sync::mpsc::unbounded_channel().0);
            a.bullet_book = Some(book("3000", "3000.1", "0.006", clock.now()).orderbook);
            a.basis = Some(0.0);
            let fair = if side == Side::Buy { "3000.6" } else { "2999.5" };
            a.binance = Some(reference(fair, fair, clock.now()));
            let order = a.entry_order(side).unwrap();
            assert_eq!(order.quantity, d("0.006"));
            assert_eq!(order.price, if side == Side::Buy { d("3000.6") } else { d("2999.5") });
            assert!(order.quantity * d("3000.6") <= d("20"));
            a.place_entry(&cx, side).unwrap();
            settle_submissions(&mut a, &cx).await;
            if !dry_run {
                let mut fill = confirmation(
                    &a,
                    OrderStatus::Filled,
                    Some((side, "3000", "0.006")),
                    clock.now(),
                );
                fill.lifecycle.order.symbol = "ETH-USD".into();
                fill.trade.as_mut().unwrap().symbol = "ETH-USD".into();
                a.apply_execution(&fill, &cx).unwrap();
            }
            assert_eq!(a.inventory.net_position.abs(), d("0.006"));
            a.place_exit(&cx, ExitReason::Timeout).unwrap();
            settle_submissions(&mut a, &cx).await;
            if !dry_run {
                let close = broker.last_placed_orders().await.pop().unwrap();
                assert!(close.reduce_only);
                assert_eq!(close.side, side.opposite());
                assert_eq!(close.quantity, d("0.006"));
                let mut fill = confirmation(
                    &a,
                    OrderStatus::Filled,
                    Some((side.opposite(), "3000", "0.006")),
                    clock.now(),
                );
                fill.lifecycle.order.symbol = "ETH-USD".into();
                fill.trade.as_mut().unwrap().symbol = "ETH-USD".into();
                a.apply_execution(&fill, &cx).unwrap();
            }
            assert!(a.is_flat());
            assert_eq!(a.inventory.total_fills, 2);
        }
    }
}
#[tokio::test]
async fn binance_age_includes_queue_delay_and_accepts_exactly_1000ms() {
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    a.config.impulse = None;
    a.binance = Some(reference("1.0003", "1.0003", clock.now()));
    clock.advance(Duration::from_millis(1000));
    assert!(a.quotes_ready(&cx).unwrap());
    clock.advance(Duration::from_millis(1));
    a.evaluate(&cx, true).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(broker.placed_count().await, 0);
    let basis = (a.basis, a.basis_at);
    a.reentry[1] = Reentry { reset: false, confirmed_at: Some(clock.now()) };
    // A just-processed Binance update was already too old while queued.
    let received_at = clock.now().checked_sub(Duration::from_millis(1001)).unwrap();
    a.on_event(reference("1.0003", "1.0003", received_at), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(broker.placed_count().await, 0);
    assert_eq!((a.basis, a.basis_at), basis);
    assert!(!a.reentry[1].reset);
    a.on_event(reference("1.0003", "1.0003", clock.now()), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(broker.placed_count().await, 1); // no new Bullet quote required
}
#[tokio::test]
async fn partial_entry_waits_for_cancel_and_never_tops_up() {
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    a.place_entry(&cx, Side::Buy).unwrap();
    settle_submissions(&mut a, &cx).await;
    let fill = confirmation(
        &a,
        OrderStatus::PartiallyFilled,
        Some((Side::Buy, "1.0001", "10")),
        clock.now(),
    );
    a.on_event(fill, &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(a.state(), "entering");
    let first = a.entry.unwrap().filled_at;
    clock.advance(Duration::from_millis(200));
    let fill = confirmation(
        &a,
        OrderStatus::PartiallyFilled,
        Some((Side::Buy, "1.0001", "5")),
        clock.now(),
    );
    a.on_event(fill, &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(a.entry.unwrap().filled_at, first);
    let cancel = confirmation(&a, OrderStatus::Cancelled, None, clock.now());
    a.on_event(cancel, &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(a.state(), "holding");
    assert_eq!(a.inventory.net_position, d("15"));
    assert_eq!(broker.placed_count().await, 1);
}
#[tokio::test]
async fn full_fill_and_lifecycle_in_one_message_are_not_double_counted() {
    let (_, clock, cx) = context();
    let mut a = ready(&clock);
    a.place_entry(&cx, Side::Sell).unwrap();
    settle_submissions(&mut a, &cx).await;
    let fill =
        confirmation(&a, OrderStatus::Filled, Some((Side::Sell, "0.9999", "100")), clock.now());
    a.on_event(fill.clone(), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    a.on_event(fill, &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await; // late duplicate after completion
    assert_eq!(a.inventory.net_position, d("-100"));
    assert_eq!(a.inventory.total_fills, 1);
    assert_eq!(a.state(), "holding");
}
#[tokio::test]
async fn empty_ioc_rearms_after_reset_or_ten_seconds_from_cancel() {
    let (_, clock, cx) = context();
    let mut a = ready(&clock);
    a.binance = Some(reference("1.0002", "1.0002", clock.now()));
    a.place_entry(&cx, Side::Buy).unwrap();
    settle_submissions(&mut a, &cx).await;
    clock.advance(Duration::from_secs(3));
    let cancel = confirmation(&a, OrderStatus::Cancelled, None, clock.now());
    a.on_event(cancel, &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(a.state(), "flat");
    assert!(!a.reentry[0].allowed(clock.now()));
    clock.advance(Duration::from_millis(9999));
    assert!(!a.reentry[0].allowed(clock.now()));
    clock.advance(Duration::from_millis(1));
    assert!(a.reentry[0].allowed(clock.now()));
    a.reentry[0].confirmed_at = Some(clock.now());
    a.binance = Some(reference("1", "1", clock.now()));
    a.evaluate(&cx, false).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert!(a.reentry[0].allowed(clock.now()));
    assert!(a.reentry[1].allowed(clock.now()));
}
#[tokio::test]
async fn frozen_basis_stop_touch_and_time_exit_work_for_both_sides() {
    let (_, clock, _) = context();
    for side in [Side::Buy, Side::Sell] {
        let mut a = holding(&clock, side);
        // Global basis can change without changing this position's gap.
        a.basis = Some(sign(side) * 0.01);
        a.binance = Some(if side == Side::Buy {
            reference("0.99997", "0.99997", clock.now())
        } else {
            reference("1.00003", "1.00003", clock.now())
        });
        assert!(matches!(a.exit_reason(clock.now()), Some(ExitReason::Fair)));
        a.binance = Some(reference("1", "1", clock.now()));
        a.bullet_book = Some(book("0.9995", "1.0005", "100", clock.now()).orderbook);
        assert!(matches!(a.exit_reason(clock.now()), Some(ExitReason::StopLoss))); // exact -5bp
    }
}
#[tokio::test]
async fn hold_deadline_survives_partial_exit_and_stale_quotes() {
    let (broker, clock, cx) = context();
    let mut a = holding(&clock, Side::Buy);
    let original_basis = a.entry.unwrap().basis;
    clock.advance(Duration::from_secs(60));
    a.on_event(Tick::now(), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(broker.placed_count().await, 1);
    let fill = confirmation(
        &a,
        OrderStatus::PartiallyFilled,
        Some((Side::Sell, "0.9999", "5")),
        clock.now(),
    );
    a.on_event(fill, &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(a.state(), "exiting");
    let cancel = confirmation(&a, OrderStatus::Cancelled, None, clock.now());
    a.on_event(cancel, &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(broker.placed_count().await, 2); // retry residual after confirmation
    let o = &broker.last_placed_orders().await[0];
    assert!(o.reduce_only);
    assert_eq!(o.quantity, d("15"));
    assert_eq!(a.entry.unwrap().basis, original_basis);
    assert_eq!(a.state(), "exiting");
    assert!(matches!(a.exit_requested, Some(ExitReason::Timeout)));
    let fill =
        confirmation(&a, OrderStatus::Filled, Some((Side::Sell, "0.9999", "15")), clock.now());
    a.on_event(fill, &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert!(a.inventory.is_flat());
    assert_eq!(a.state(), "flat");
    assert_eq!(a.reentry[0].confirmed_at, Some(clock.now()));
}
#[tokio::test]
async fn exit_decision_stays_latched_if_gap_recovers_after_partial_close() {
    let (broker, clock, cx) = context();
    let mut a = holding(&clock, Side::Buy);
    a.binance = Some(reference("0.9999", "0.9999", clock.now()));
    a.evaluate(&cx, false).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    a.binance = Some(reference("1.0002", "1.0002", clock.now()));
    let fill = confirmation(
        &a,
        OrderStatus::PartiallyFilled,
        Some((Side::Sell, "0.9999", "1")),
        clock.now(),
    );
    a.on_event(fill, &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    let cancel = confirmation(&a, OrderStatus::Cancelled, None, clock.now());
    a.on_event(cancel, &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(broker.placed_count().await, 2);
    assert_eq!(a.state(), "exiting");
    assert!(matches!(a.exit_requested, Some(ExitReason::Fair)));
}
#[tokio::test]
async fn unknown_entry_times_out_with_reduce_only_without_duplicating_entry() {
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    broker.queue_place_response(Err(BotError::exchange("response lost", true))).await;
    a.place_entry(&cx, Side::Buy).unwrap();
    settle_submissions(&mut a, &cx).await;
    let requested = broker.last_placed_orders().await[0].quantity;
    clock.advance(Duration::from_millis(59_999));
    a.on_event(Tick::now(), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(broker.placed_count().await, 1);
    clock.advance(Duration::from_millis(1));
    a.on_event(Tick::now(), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert!(matches!(a.exit_requested, Some(ExitReason::Timeout)));
    assert_eq!(broker.placed_count().await, 2);
    let close = &broker.last_placed_orders().await[0];
    assert!(close.reduce_only);
    assert_eq!(close.side, Side::Sell);
    assert_eq!(close.quantity, requested);
    assert!(a.inventory.is_flat());
    assert_eq!(a.inventory.total_fills, 0);
}
#[tokio::test]
async fn dry_run_uses_visible_prices_accepts_partial_fill_and_never_touches_account() {
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    a.config.dry_run = true;
    a.init(&cx).await.unwrap();
    broker.flag_reconcile();
    a.on_event(Tick::now(), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    a.bullet_book = Some(book("0.9999", "1.0001", "7", clock.now()).orderbook);
    a.binance = Some(reference("1", "1.0003", clock.now()));
    a.place_entry(&cx, Side::Buy).unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(a.inventory.net_position, d("7"));
    assert_eq!(a.inventory.avg_entry_price, d("1.0001"));
    let (_, _, live_cx) = context();
    let mut live = ready(&clock);
    live.place_entry(&live_cx, Side::Buy).unwrap();
    let real = confirmation(&live, OrderStatus::Filled, Some((Side::Buy, "1", "100")), clock.now());
    a.on_event(real, &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(a.inventory.net_position, d("7"));
    a.wind_down(&WindDownReason::Signal, &cx).await.unwrap();
    assert!(a.inventory.is_flat());
    assert_eq!(a.inventory.realized_pnl, d("-0.0014"));
    assert!(broker.history().await.is_empty());
}
#[tokio::test]
async fn market_updates_drive_entry_with_causal_basis_and_impulse() {
    let (broker, clock, cx) = context();
    let mut a =
        ReferenceArbActor::new(config(), Decimal::ONE, tokio::sync::mpsc::unbounded_channel().0);
    a.on_event(book("0.9999", "1.0001", "2000", clock.now()), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    a.on_event(reference("1", "1", clock.now()), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(broker.placed_count().await, 0);
    assert_eq!(a.basis, Some(0.0));
    clock.advance(Duration::from_millis(50));
    a.on_event(reference("1.0003", "1.0003", clock.now()), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(broker.placed_count().await, 1);
    assert_eq!(a.entry.unwrap().basis, a.basis.unwrap());
}

#[tokio::test]
async fn holding_time_starts_at_first_fill_not_submission_or_new_signal() {
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    a.place_entry(&cx, Side::Buy).unwrap();
    settle_submissions(&mut a, &cx).await;
    clock.advance(Duration::from_secs(10));
    let fill = confirmation(&a, OrderStatus::Filled, Some((Side::Buy, "1", "10")), clock.now());
    a.on_event(fill, &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    let first_fill = a.entry.unwrap().filled_at;
    clock.advance(Duration::from_secs(50));
    // Already 60s since submission, but only 50s since the entry fill.
    a.on_event(Tick::now(), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(broker.placed_count().await, 1);
    a.on_event(reference("1.0002", "1.0002", clock.now()), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(a.entry.unwrap().filled_at, first_fill);
    clock.advance(Duration::from_secs(10));
    a.on_event(Tick::now(), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(broker.placed_count().await, 2);
    assert_eq!(a.state(), "exiting");
    assert!(matches!(a.exit_requested, Some(ExitReason::Timeout)));
}

#[tokio::test]
async fn invalid_book_blocks_entry_but_timeout_can_use_last_valid_touch() {
    let (broker, clock, cx) = context();
    let mut a = holding(&clock, Side::Buy);
    a.on_event(book("1.0002", "0.9999", "10", clock.now()), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert!(!a.quotes_ready(&cx).unwrap());
    assert_eq!(a.touch_price(Side::Sell), Some(d("0.9999")));
    clock.advance(Duration::from_secs(60));
    a.on_event(Tick::now(), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(broker.placed_count().await, 1);
}

#[tokio::test]
async fn gap_exit_boundary_is_inclusive_and_both_symbols_are_filtered() {
    let (broker, clock, cx) = context();
    let mut a = holding(&clock, Side::Buy);
    a.binance = Some(reference("0.99997", "0.99997", clock.now()));
    a.config.exit = ExitConfig::Fair { remaining_bps: a.remaining(Side::Buy, 0.0).unwrap() };
    assert!(matches!(a.exit_reason(clock.now()), Some(ExitReason::Fair)));

    a.inventory = InventoryTracker::new();
    a.entry = None;
    let mut wrong = book("0.5", "0.6", "10", clock.now());
    wrong.symbol = "BTC-USD".into();
    a.on_event(wrong, &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    let mut wrong = reference("2", "2", clock.now());
    wrong.symbol = "BTCUSDT".into();
    a.on_event(wrong, &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(broker.placed_count().await, 0);
    assert_eq!(a.bullet_mid(), Some(d("1")));
    assert_eq!(a.binance.as_ref().unwrap().midpoint, d("0.99997"));
}

#[tokio::test]
async fn harness_preserves_private_fill_cancel_ordering() {
    use bb_core::harness::{ActorSpec, HarnessBuilder, TimedFeed};
    // Capture the actor's final state without making shutdown requests in this
    // ordering test; shutdown recovery is covered separately below.
    struct Observed {
        actor: ReferenceArbActor,
        result: Arc<std::sync::Mutex<Decimal>>,
    }
    #[async_trait]
    impl Actor for Observed {
        async fn wind_down(
            &mut self,
            _: &WindDownReason,
            _: &ActorContext,
        ) -> Result<(), BotError> {
            *self.result.lock().unwrap() = self.actor.inventory.net_position;
            Ok(())
        }
    }
    #[async_trait]
    impl EventHandler<ExecutionUpdate> for Observed {
        async fn on_event(
            &mut self,
            event: ExecutionUpdate,
            cx: &ActorContext,
        ) -> Result<(), BotError> {
            self.actor.on_event(event, cx).await
        }
    }
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    a.place_entry(&cx, Side::Buy).unwrap();
    settle_submissions(&mut a, &cx).await;
    let fill =
        confirmation(&a, OrderStatus::PartiallyFilled, Some((Side::Buy, "1", "5")), clock.now());
    let cancel = confirmation(&a, OrderStatus::Cancelled, None, clock.now());
    let late = fill.clone();
    let feed = TimedFeed::new(vec![
        (Duration::from_millis(1), fill),
        (Duration::from_millis(1), cancel),
        (Duration::from_millis(1), late),
    ]);
    let result = Arc::new(std::sync::Mutex::new(Decimal::ZERO));
    HarnessBuilder::new()
        .with_clock(Arc::new(clock))
        .wire_broker("bullet", broker.clone())
        .wire_feed_named("execution", feed)
        .wire_actor(
            ActorSpec::new("arb", Observed { actor: a, result: result.clone() })
                .sub_critical::<ExecutionUpdate>(),
        )
        .build()
        .unwrap()
        .run()
        .await
        .unwrap();
    assert_eq!(*result.lock().unwrap(), d("5"));
    assert_eq!(broker.placed_count().await, 1);
}

#[tokio::test]
async fn paper_partial_exit_cannot_reuse_consumed_quote_liquidity() {
    let (broker, clock, cx) = context();
    let mut a = holding(&clock, Side::Buy);
    a.config.dry_run = true;
    a.bullet_book = Some(book("0.9999", "1.0001", "7", clock.now()).orderbook);
    a.place_exit(&cx, ExitReason::Fair).unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(a.inventory.net_position, d("13"));
    let fills = a.inventory.total_fills;
    a.evaluate(&cx, false).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    a.on_event(Tick::now(), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(a.inventory.total_fills, fills);
    assert_eq!(a.inventory.net_position, d("13"));
    clock.advance(Duration::from_millis(1));
    a.on_event(book("0.9999", "1.0001", "5", clock.now()), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(a.inventory.net_position, d("8"));
    assert!(broker.history().await.is_empty());
}

#[tokio::test(start_paused = true)]
async fn harness_recovery_triggers_stop_without_ticks_or_market_updates() {
    use bb_core::harness::{ActorSpec, HarnessBuilder, MpscFeed};

    struct Observed(ReferenceArbActor);
    #[async_trait]
    impl Actor for Observed {
        async fn init(&mut self, cx: &ActorContext) -> Result<(), BotError> {
            self.0.recover_if_needed(cx)
        }
    }
    #[async_trait]
    impl EventHandler<OrderTaskReady> for Observed {
        async fn on_event(
            &mut self,
            event: OrderTaskReady,
            cx: &ActorContext,
        ) -> Result<(), BotError> {
            let recovered = matches!(event, OrderTaskReady::Recovery(_));
            self.0.on_event(event, cx).await?;
            if !recovered {
                return Ok(());
            }
            settle_submissions(&mut self.0, cx).await;
            assert_eq!(self.0.inventory.net_position, d("10"));
            assert_eq!(self.0.inventory.total_fills, 1);
            assert!(matches!(self.0.exit_requested, Some(ExitReason::StopLoss)));
            cx.request_shutdown();
            Ok(())
        }
    }

    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    let recovery_rx = task_events(&mut a);
    a.bullet_book = Some(book("0.9994", "1.0006", "100", clock.now()).orderbook);
    a.place_entry(&cx, Side::Buy).unwrap();
    settle_submissions(&mut a, &cx).await;
    let filled = confirmation(&a, OrderStatus::Filled, Some((Side::Buy, "1", "10")), clock.now());
    broker
        .queue_recovery(Ok(OrderRecovery {
            submission_expired: false,
            order: Some(filled.lifecycle.order),
            trades: vec![filled.trade.unwrap()],
        }))
        .await;
    broker.set_recovery_delay(Duration::from_millis(100)).await;
    clock.advance(RECOVERY_INTERVAL);
    let harness = HarnessBuilder::new()
        .with_clock(Arc::new(clock))
        .wire_broker("bullet", broker.clone())
        .wire_feed_named("recovery", MpscFeed::new(recovery_rx))
        .wire_actor(ActorSpec::new("arb", Observed(a)).sub_critical::<OrderTaskReady>())
        .build()
        .unwrap();
    // No timer, quote or execution feed can wake the actor in this test.
    let reason =
        tokio::time::timeout(Duration::from_millis(500), harness.run()).await.unwrap().unwrap();
    assert!(matches!(reason, WindDownReason::Signal));
    assert_eq!(broker.placed_count().await, 2);
    let close = &broker.last_placed_orders().await[0];
    assert!(close.reduce_only);
    assert_eq!(close.side, Side::Sell);
    assert_eq!(close.quantity, d("10"));
}

#[tokio::test(start_paused = true)]
async fn queued_recovery_notification_cannot_consume_another_orders_result() {
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    let mut recovery_rx = task_events(&mut a);
    a.place_entry(&cx, Side::Buy).unwrap();
    settle_submissions(&mut a, &cx).await;
    let filled = confirmation(&a, OrderStatus::Filled, Some((Side::Buy, "1", "10")), clock.now());
    broker
        .queue_recovery(Ok(OrderRecovery {
            submission_expired: false,
            order: Some(filled.lifecycle.order.clone()),
            trades: vec![filled.trade.clone().unwrap()],
        }))
        .await;
    clock.advance(RECOVERY_INTERVAL);
    a.on_event(Tick::now(), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    let stale_event = loop {
        let event = recovery_rx.recv().await.unwrap();
        if matches!(event, OrderTaskReady::Recovery(_)) {
            break event;
        }
    };
    // WS wins the race with an already queued recovery notification.
    a.on_event(filled, &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    a.place_exit(&cx, ExitReason::StopLoss).unwrap();
    settle_submissions(&mut a, &cx).await;
    broker.set_recovery_delay(Duration::from_secs(2)).await;
    clock.advance(RECOVERY_INTERVAL);
    a.recover_if_needed(&cx).unwrap();
    let close_id = a.recovery_task.as_ref().unwrap().0.clone();
    tokio::time::timeout(Duration::from_millis(10), a.on_event(stale_event, &cx))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(a.recovery_task.as_ref().unwrap().0, close_id);
    assert_eq!(a.inventory.total_fills, 1);
    assert_eq!(a.inventory.net_position, d("10"));
    assert_eq!(broker.placed_count().await, 2);
}

#[tokio::test(start_paused = true)]
async fn shutdown_consumes_rest_result_immediately_with_notification_feed_closed() {
    // The longer read crosses the retry wake-up: selecting that timer must
    // preserve the pending handle, not lose its response or start another read.
    for delay in [Duration::from_millis(100), Duration::from_millis(1500)] {
        let (broker, clock, cx) = context();
        let mut a = holding(&clock, Side::Buy);
        assert!(a.task_tx.is_closed());
        broker.queue_place_response(Err(BotError::exchange("response lost", true))).await;
        a.place_exit(&cx, ExitReason::StopLoss).unwrap();
        settle_submissions(&mut a, &cx).await;
        let filled =
            confirmation(&a, OrderStatus::Filled, Some((Side::Sell, "1", "20")), clock.now());
        broker
            .queue_recovery(Ok(OrderRecovery {
                submission_expired: false,
                order: Some(filled.lifecycle.order),
                trades: vec![filled.trade.unwrap()],
            }))
            .await;
        broker.set_recovery_delay(delay).await;
        a.recover_if_needed(&cx).unwrap();
        let started = tokio::time::Instant::now();
        a.wind_down(&WindDownReason::Signal, &cx).await.unwrap();
        assert!(started.elapsed() >= delay);
        assert!(started.elapsed() < delay + Duration::from_millis(50));
        assert!(a.inventory.is_flat());
        assert!(a.tracked.is_empty());
        assert_eq!(broker.placed_count().await, 1);
        assert_eq!(
            broker.history().await.iter().filter(|c| c.method == "recover_order").count(),
            1
        );
    }
}

#[tokio::test(start_paused = true)]
async fn shutdown_keeps_closing_order_when_rest_terminal_precedes_fills() {
    for status in [OrderStatus::Filled, OrderStatus::Cancelled, OrderStatus::Rejected] {
        let (broker, clock, cx) = context();
        let mut a = holding(&clock, Side::Buy);
        a.place_exit(&cx, ExitReason::WindDown).unwrap();
        settle_submissions(&mut a, &cx).await;
        assert!(a.task_tx.is_closed());
        let close =
            confirmation(&a, OrderStatus::Filled, Some((Side::Sell, "0.998", "20")), clock.now());
        // The terminal row and fills can become visible on different REST reads.
        // Even Cancelled/zero was observed before the actual closing fills.
        let mut unindexed = close.lifecycle.order.clone();
        unindexed.status = status;
        unindexed.filled_quantity = Decimal::ZERO;
        broker
            .queue_recovery(Ok(OrderRecovery { order: Some(unindexed), ..Default::default() }))
            .await;
        let mut first = close.trade.clone().unwrap();
        first.trade_id = Some("close-first".into());
        first.quantity = d("7");
        first.price = d("0.999");
        let mut last = close.trade.clone().unwrap();
        last.trade_id = Some("close-last".into());
        last.quantity = d("13");
        broker
            .queue_recovery(Ok(OrderRecovery {
                order: Some(close.lifecycle.order.clone()),
                trades: vec![first.clone()],
                ..Default::default()
            }))
            .await;
        broker
            .queue_recovery(Ok(OrderRecovery {
                order: Some(close.lifecycle.order),
                trades: vec![first, last],
                ..Default::default()
            }))
            .await;
        broker.set_recovery_delay(Duration::from_millis(100)).await;
        a.wind_down(&WindDownReason::Signal, &cx).await.unwrap();
        assert!(a.is_flat());
        assert_eq!(a.inventory.total_fills, 3, "entry plus two unique closing fills");
        assert_eq!(a.inventory.realized_pnl, d("-0.033"));
        assert_eq!(
            broker.placed_count().await,
            1,
            "do not forget the original close and submit another"
        );
        assert_eq!(
            broker.history().await.iter().filter(|c| c.method == "recover_order").count(),
            3
        );
    }
}

#[tokio::test]
async fn zero_rest_close_keeps_protection_and_retires_after_another_close_fills() {
    let (broker, clock, cx) = context();
    let mut a = holding(&clock, Side::Buy);
    a.place_exit(&cx, ExitReason::StopLoss).unwrap();
    settle_submissions(&mut a, &cx).await;
    let empty = confirmation(&a, OrderStatus::Cancelled, None, clock.now()).lifecycle.order;
    let id = empty.client_id.clone().unwrap();
    a.apply_recovery(&id, OrderRecovery { order: Some(empty.clone()), ..Default::default() }, &cx);
    assert_eq!(a.tracked.len(), 1);
    clock.advance(RECOVERY_INTERVAL);
    a.place_exit(&cx, ExitReason::StopLoss).unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(broker.placed_count().await, 2);
    assert!(broker.last_placed_orders().await[0].reduce_only);
    let fill = confirmation(&a, OrderStatus::Filled, Some((Side::Sell, "1", "20")), clock.now());
    a.apply_execution(&fill, &cx).unwrap();
    assert!(a.inventory.is_flat());
    a.apply_recovery(&id, OrderRecovery { order: Some(empty), ..Default::default() }, &cx);
    a.apply_execution(&fill, &cx).unwrap();
    assert!(a.is_flat(), "the old empty close must not block later entries");
    assert_eq!(a.inventory.total_fills, 2);
}

#[tokio::test]
async fn lost_submit_and_fill_recover_once_without_restarting_hold_deadline() {
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    let mut recovery_rx = task_events(&mut a);
    broker.queue_place_response(Err(BotError::exchange("timeout", true))).await;
    a.place_entry(&cx, Side::Buy).unwrap();
    settle_submissions(&mut a, &cx).await;
    let basis = a.entry.unwrap().basis;
    clock.advance(Duration::from_secs(2));
    let first_fill_at = clock.now();
    let mut fill =
        confirmation(&a, OrderStatus::Filled, Some((Side::Buy, "1", "10")), first_fill_at);
    fill.trade.as_mut().unwrap().timestamp = Some(clock.unix_ms());
    clock.advance(Duration::from_secs(10));
    broker
        .queue_recovery(Ok(OrderRecovery {
            submission_expired: false,
            order: Some(fill.lifecycle.order.clone()),
            trades: vec![fill.trade.clone().unwrap()],
        }))
        .await;
    broker.set_positions(position(d("10"))).await;
    a.on_event(Tick::now(), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    deliver_recovery(&mut a, &mut recovery_rx, &cx).await;
    assert_eq!(a.state(), "holding");
    assert_eq!(a.entry.unwrap().basis, basis);
    assert_eq!(a.entry.unwrap().filled_at, Some(first_fill_at));
    a.on_event(fill, &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(a.inventory.total_fills, 1);
    assert_eq!(a.inventory.net_position, d("10"));
    assert_eq!(broker.placed_count().await, 1);
    clock.advance(Duration::from_secs(50));
    a.on_event(Tick::now(), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(a.state(), "exiting");
    assert!(matches!(a.exit_requested, Some(ExitReason::Timeout)));
}

#[tokio::test]
async fn missing_fill_is_recovered_without_waiting_for_position_index() {
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    let mut recovery_rx = task_events(&mut a);
    a.place_entry(&cx, Side::Sell).unwrap();
    settle_submissions(&mut a, &cx).await;
    let mut first =
        confirmation(&a, OrderStatus::PartiallyFilled, Some((Side::Sell, "1", "4")), clock.now());
    first.trade.as_mut().unwrap().trade_id = Some("first".into());
    let mut last = confirmation(&a, OrderStatus::Filled, Some((Side::Sell, "1", "6")), clock.now());
    last.lifecycle.order.quantity = d("10");
    last.lifecycle.order.filled_quantity = d("10");
    // Only the last websocket fill arrives. REST has also not caught up yet.
    a.on_event(last.clone(), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(a.state(), "entering");
    assert_eq!(a.inventory.net_position, d("-6"));
    let recovery = OrderRecovery {
        submission_expired: false,
        order: Some(last.lifecycle.order.clone()),
        trades: vec![first.trade.clone().unwrap(), last.trade.clone().unwrap()],
    };
    broker.queue_recovery(Ok(recovery)).await;
    clock.advance(RECOVERY_INTERVAL);
    a.on_event(Tick::now(), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    deliver_recovery(&mut a, &mut recovery_rx, &cx).await;
    // The account endpoint still says flat: complete order history is sufficient.
    assert_eq!(a.inventory.net_position, d("-10"));
    a.on_event(first, &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await; // late original is ignored
    assert!(!a.recovering());
    assert_eq!(a.state(), "holding");
    assert_eq!(a.inventory.total_fills, 2);
    assert_eq!(broker.placed_count().await, 1);
}

#[tokio::test]
async fn reconnect_before_cancel_recovers_a_lost_partial_fill() {
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    let mut recovery_rx = task_events(&mut a);
    a.place_entry(&cx, Side::Buy).unwrap();
    settle_submissions(&mut a, &cx).await;
    let fill =
        confirmation(&a, OrderStatus::PartiallyFilled, Some((Side::Buy, "1", "7")), clock.now());
    let cancel = confirmation(&a, OrderStatus::Cancelled, None, clock.now());
    let mut order = cancel.lifecycle.order.clone();
    order.filled_quantity = d("7");
    broker
        .queue_recovery(Ok(OrderRecovery {
            submission_expired: false,
            order: Some(order),
            trades: vec![fill.trade.clone().unwrap()],
        }))
        .await;
    broker.set_positions(position(d("7"))).await;
    broker.flag_reconcile();
    a.on_event(cancel, &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(a.state(), "entering");
    deliver_recovery(&mut a, &mut recovery_rx, &cx).await;
    a.on_event(fill, &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(a.state(), "holding");
    assert_eq!(a.inventory.net_position, d("7"));
    assert_eq!(a.inventory.total_fills, 1);
    assert_eq!(broker.placed_count().await, 1);
}

#[tokio::test]
async fn absent_history_is_unknown_and_recovery_errors_retry() {
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    let mut recovery_rx = task_events(&mut a);
    broker.queue_place_response(Err(BotError::exchange("response lost", true))).await;
    a.place_entry(&cx, Side::Buy).unwrap();
    settle_submissions(&mut a, &cx).await;
    broker.queue_recovery(Err(BotError::exchange("503", true))).await;
    a.on_event(Tick::now(), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    deliver_recovery(&mut a, &mut recovery_rx, &cx).await;
    for _ in 0..3 {
        clock.advance(RECOVERY_INTERVAL);
        a.on_event(Tick::now(), &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        deliver_recovery(&mut a, &mut recovery_rx, &cx).await;
    }
    assert_eq!(a.state(), "entering");
    assert!(a.recovering());
    assert_eq!(broker.placed_count().await, 1);
    assert_eq!(broker.history().await.iter().filter(|c| c.method == "recover_order").count(), 4);
}

#[tokio::test]
async fn definite_rejection_rearms_entry_and_throttles_rejected_closes() {
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    let reject = || {
        vec![OrderResult {
            order_id: None,
            client_id: None,
            success: false,
            error: Some("insufficient margin".into()),
        }]
    };
    broker.queue_place_results(reject()).await;
    a.place_entry(&cx, Side::Buy).unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(a.state(), "flat");
    assert!(!a.reentry[0].allowed(clock.now()));
    assert!(!a.recovering());
    a = holding(&clock, Side::Buy);
    broker.queue_place_results(reject()).await;
    a.place_exit(&cx, ExitReason::StopLoss).unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(a.state(), "exiting");
    for _ in 0..5 {
        a.on_event(Tick::now(), &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
    }
    assert_eq!(broker.placed_count().await, 2);
    clock.advance(RECOVERY_INTERVAL);
    a.on_event(Tick::now(), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(broker.placed_count().await, 3);
    assert!(broker.last_placed_orders().await[0].reduce_only);
}

#[tokio::test]
async fn idle_reconnect_needs_no_rest_and_snapshot_sequence_may_reset() {
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    a.bullet_book.as_mut().unwrap().last_update_id = 1000;
    clock.advance(Duration::from_millis(100));
    broker.flag_reconnecting(clock.now());
    clock.advance(Duration::from_millis(1));
    a.on_event(book("1.0999", "1.1001", "20", clock.now()), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(a.bullet_mid(), Some(d("1.1")));
    assert!(!a.recovering());
    a.on_event(Tick::now(), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert!(!a.recovering());
    assert_eq!(broker.placed_count().await, 0);
}

#[tokio::test]
async fn connection_maintenance_keeps_a_known_position_and_public_bbo() {
    let (broker, clock, cx) = context();
    let mut a = holding(&clock, Side::Buy);
    a.inventory.realized_pnl = d("12");
    a.peak_pnl = d("15");
    let entry = a.entry;
    let old_book_at = a.bullet_last_seen;
    broker.set_replacement_ready(true);
    a.evaluate(&cx, true).await.unwrap();
    assert_eq!(a.inventory.realized_pnl, d("12"));
    assert_eq!(a.inventory.net_position, d("20"));
    assert_eq!(a.peak_pnl, d("15"));
    assert_eq!(a.entry.unwrap().filled_at, entry.unwrap().filled_at);
    assert_eq!(a.basis, Some(0.0));
    assert_eq!(a.bullet_last_seen, old_book_at);
    assert!(a.quotes_ready(&cx).unwrap());
    assert_eq!(broker.placed_count().await, 0);
    assert_eq!(broker.history().await.len(), 1);
    assert_eq!(broker.history().await[0].method, "refresh_connection_if_idle");
}

#[tokio::test]
async fn new_private_stream_cancel_cannot_hide_earlier_partial_fill() {
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    let old_stream = clock.now();
    a.place_entry(&cx, Side::Buy).unwrap();
    settle_submissions(&mut a, &cx).await;
    clock.advance(Duration::from_millis(10));
    let mut cancel = confirmation(&a, OrderStatus::Cancelled, None, clock.now());
    cancel.stream_started_at = Some(clock.now());
    a.on_event(cancel.clone(), &cx).await.unwrap();
    assert_eq!(a.tracked.len(), 1, "new source does not prove empty IOC");
    assert!(!a.recovering(), "continuous peer still covers the order");
    let mut fill =
        confirmation(&a, OrderStatus::PartiallyFilled, Some((Side::Buy, "1", "7")), clock.now());
    fill.stream_started_at = Some(old_stream);
    a.on_event(fill.clone(), &cx).await.unwrap();
    fill.stream_started_at = Some(clock.now());
    a.on_event(fill, &cx).await.unwrap(); // Duplicate via the reconnected peer.
    a.on_event(cancel.clone(), &cx).await.unwrap();
    assert_eq!(a.tracked.len(), 1);
    assert_eq!(a.inventory.net_position, d("7"));
    assert_eq!(a.inventory.total_fills, 1);
    cancel.stream_started_at = Some(old_stream);
    a.on_event(cancel, &cx).await.unwrap();
    assert!(a.tracked.is_empty());
    assert_eq!(a.state(), "holding");
    assert!(broker.history().await.iter().all(|c| c.method != "recover_order"));
}

#[tokio::test]
async fn new_stream_terminal_partial_with_all_fills_needs_no_rest() {
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    a.place_entry(&cx, Side::Buy).unwrap();
    settle_submissions(&mut a, &cx).await;
    clock.advance(Duration::from_millis(10));
    // Same decoded shape as PARTIALLY_FILLED + rs=0: terminal remainder,
    // with a cumulative size proving that the first/only fill is complete.
    let mut fill =
        confirmation(&a, OrderStatus::Cancelled, Some((Side::Buy, "1", "7")), clock.now());
    fill.lifecycle.order.filled_quantity = d("7");
    fill.stream_started_at = Some(clock.now());
    a.on_event(fill.clone(), &cx).await.unwrap();
    a.on_event(fill, &cx).await.unwrap();
    assert!(a.tracked.is_empty());
    assert_eq!(a.inventory.net_position, d("7"));
    assert_eq!(a.inventory.total_fills, 1);
    assert!(broker.history().await.iter().all(|c| c.method != "recover_order"));
}

#[tokio::test]
async fn subscribed_peer_can_confirm_empty_ioc_and_unknown_entry_keeps_protection() {
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    let subscribed = clock.now();
    clock.advance(Duration::from_millis(10));
    a.place_entry(&cx, Side::Buy).unwrap();
    settle_submissions(&mut a, &cx).await;
    let mut cancel = confirmation(&a, OrderStatus::Cancelled, None, clock.now());
    cancel.stream_started_at = Some(subscribed);
    a.on_event(cancel, &cx).await.unwrap();
    assert!(a.tracked.is_empty());
    assert!(a.inventory.is_flat());
    a.place_entry(&cx, Side::Buy).unwrap();
    settle_submissions(&mut a, &cx).await;
    clock.advance(Duration::from_millis(10));
    let mut cancel = confirmation(&a, OrderStatus::Cancelled, None, clock.now());
    cancel.stream_started_at = Some(clock.now());
    a.on_event(cancel, &cx).await.unwrap();
    // Still-unconfirmed entry must not prevent a time-based protective order.
    clock.advance(Duration::from_secs(61));
    a.on_event(Tick::now(), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert!(broker.last_placed_orders().await[0].reduce_only);
    assert!(a.tracked.iter().any(|t| !t.reduce_only));
}

#[tokio::test]
async fn connection_handover_never_delays_protection_or_discards_unknown_entry() {
    for unknown_entry in [false, true] {
        let (broker, clock, cx) = context();
        let mut a = if unknown_entry { ready(&clock) } else { holding(&clock, Side::Buy) };
        if unknown_entry {
            broker.queue_place_response(Err(BotError::exchange("lost response", true))).await;
            a.place_entry(&cx, Side::Buy).unwrap();
            settle_submissions(&mut a, &cx).await;
            // Even zero known inventory is not permission to drop an unknown entry.
            broker.set_replacement_ready(true);
            a.evaluate(&cx, true).await.unwrap();
            assert!(
                broker.history().await.iter().all(|c| c.method != "refresh_connection_if_idle")
            );
            let fill = confirmation(
                &a,
                OrderStatus::PartiallyFilled,
                Some((Side::Buy, "1", "7")),
                clock.now(),
            );
            a.on_event(fill, &cx).await.unwrap();
        }
        broker.set_replacement_ready(true);
        a.on_event(book("0.999", "0.9991", "20", clock.now()), &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        assert!(broker.last_placed_orders().await[0].reduce_only);
        assert!(broker.history().await.iter().all(|c| c.method != "refresh_connection_if_idle"));
    }
}

#[tokio::test(start_paused = true)]
async fn shutdown_closes_known_exposure_despite_an_unknown_entry() {
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    broker.queue_place_response(Err(BotError::exchange("response lost", true))).await;
    a.place_entry(&cx, Side::Buy).unwrap();
    settle_submissions(&mut a, &cx).await;
    // A partial entry was seen but its terminal status is still unknown.
    let fill =
        confirmation(&a, OrderStatus::PartiallyFilled, Some((Side::Buy, "1", "7")), clock.now());
    a.on_event(fill, &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    broker.set_positions(position(d("7"))).await;
    assert!(a.wind_down(&WindDownReason::Signal, &cx).await.is_err());
    assert_eq!(broker.placed_count().await, 2);
    assert_eq!(broker.last_placed_orders().await[0].quantity, d("999"));
    assert!(broker.last_placed_orders().await[0].reduce_only);
    assert_eq!(a.tracked.len(), 2);
}

#[tokio::test(start_paused = true)]
async fn confirmed_flat_shutdown_does_not_wait_for_the_position_index() {
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    // A lagging account snapshot must not delay our confirmed local fills.
    broker.set_positions(position(d("20"))).await;
    a.wind_down(&WindDownReason::Signal, &cx).await.unwrap();
    assert!(broker.history().await.is_empty());
}

#[tokio::test]
async fn earlier_or_other_client_orders_cannot_complete_the_current_order() {
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    a.place_entry(&cx, Side::Buy).unwrap();
    settle_submissions(&mut a, &cx).await;
    let fill = confirmation(&a, OrderStatus::Filled, Some((Side::Buy, "1", "7")), clock.now());
    let old_cancel = confirmation(&a, OrderStatus::Cancelled, None, clock.now());
    a.on_event(fill.clone(), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert!(a.tracked.is_empty());
    a.place_exit(&cx, ExitReason::StopLoss).unwrap();
    settle_submissions(&mut a, &cx).await;
    let close_id = broker.last_placed_orders().await[0].client_id.clone().unwrap();
    a.on_event(fill.clone(), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await; // replay of our earlier entry
    a.on_event(old_cancel, &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    let mut another = fill;
    another.lifecycle.order.client_id = Some("manual-order".into());
    another.trade.as_mut().unwrap().client_id = Some("manual-order".into());
    a.on_event(another, &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(a.inventory.net_position, d("7"));
    assert_eq!(a.inventory.total_fills, 1);
    assert_eq!(a.tracked.back().unwrap().client_id, close_id);
    assert_eq!(a.state(), "exiting");
    assert_eq!(broker.placed_count().await, 2);
    let close = confirmation(&a, OrderStatus::Filled, Some((Side::Sell, "1", "7")), clock.now());
    a.on_event(close, &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert!(a.inventory.is_flat());
    assert!(a.tracked.is_empty());
}

#[tokio::test]
async fn reconnect_without_pending_order_does_not_block_stop_or_timeout() {
    for timeout in [false, true] {
        let (broker, clock, cx) = context();
        let mut a = holding(&clock, Side::Buy);
        if !timeout {
            a.bullet_book = Some(book("0.9994", "1.0006", "100", clock.now()).orderbook);
        }
        clock.advance(Duration::from_millis(1));
        broker.flag_reconnecting(clock.now());
        if timeout {
            clock.advance(Duration::from_secs(60));
        }
        assert!(!a.quotes_ready(&cx).unwrap());
        // No new BBO: the last valid touch must remain available for protection.
        a.on_event(Tick::now(), &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        assert_eq!(
            broker.placed_count().await,
            1,
            "confirmed position must close without a REST precondition"
        );
        assert!(broker.last_placed_orders().await[0].reduce_only);
        assert!(!broker.history().await.iter().any(|c| c.method == "recover_order"));
    }
}

#[tokio::test]
async fn stop_closes_partial_entry_and_late_entry_fill_without_new_entry() {
    for side in [Side::Buy, Side::Sell] {
        let (broker, clock, cx) = context();
        let mut a = ready(&clock);
        broker.queue_place_response(Err(BotError::exchange("response lost", true))).await;
        a.place_entry(&cx, side).unwrap();
        settle_submissions(&mut a, &cx).await;
        let entry_id = a.tracked.back().unwrap().client_id.clone();
        let requested = broker.last_placed_orders().await[0].quantity;
        let first =
            confirmation(&a, OrderStatus::PartiallyFilled, Some((side, "1", "5")), clock.now());
        a.on_event(first.clone(), &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        let entry = a.entry.unwrap();
        a.on_event(book("0.9994", "1.0006", "100", clock.now()), &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        assert_eq!(broker.placed_count().await, 2, "stop must not wait for entry completion");
        assert_eq!(broker.last_placed_orders().await[0].quantity, requested);
        assert!(broker.last_placed_orders().await[0].reduce_only);
        let close =
            confirmation(&a, OrderStatus::Filled, Some((side.opposite(), "1", "5")), clock.now());
        a.on_event(close.clone(), &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        assert!(a.inventory.is_flat());
        assert!(a.state() != "flat", "unresolved entry still blocks re-entry");
        assert!(a.entry.is_some());
        // A fresh signal cannot start a new trade while the original entry is unresolved.
        a.config.impulse = None;
        a.on_event(reference("1.001", "1.001", clock.now()), &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        assert_eq!(broker.placed_count().await, 2);
        let late = confirmation_for(
            &a,
            &entry_id,
            OrderStatus::Filled,
            Some((side, "1.0002", "5")),
            clock.now(),
        );
        a.on_event(late.clone(), &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        assert_eq!(a.entry.unwrap().basis, entry.basis);
        assert_eq!(a.entry.unwrap().filled_at, entry.filled_at);
        assert!(matches!(a.exit_requested, Some(ExitReason::StopLoss)));
        assert_eq!(broker.placed_count().await, 3);
        assert_eq!(broker.last_placed_orders().await[0].quantity, d("5"));
        assert_eq!(broker.last_placed_orders().await[0].side, side.opposite());
        for replay in [first, close, late] {
            a.on_event(replay, &cx).await.unwrap();
            settle_submissions(&mut a, &cx).await;
        }
        assert_eq!(a.inventory.total_fills, 3);
        let last = confirmation(
            &a,
            OrderStatus::Filled,
            Some((side.opposite(), "1.0001", "5")),
            clock.now(),
        );
        a.on_event(last, &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        assert!(a.inventory.is_flat());
        assert_eq!(a.state(), "flat");
        assert_eq!(a.inventory.total_fills, 4);
        assert_eq!(
            a.inventory.realized_pnl,
            if side == Side::Buy { d("-0.0005") } else { d("0.0005") }
        );
    }
}

#[tokio::test(start_paused = true)]
async fn slow_rest_cannot_block_protective_close_or_discard_entry_recovery() {
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    let mut recovery_rx = task_events(&mut a);
    broker.set_recovery_delay(Duration::from_secs(2)).await;
    a.place_entry(&cx, Side::Buy).unwrap();
    settle_submissions(&mut a, &cx).await;
    let entry_id = a.tracked.back().unwrap().client_id.clone();
    let first =
        confirmation(&a, OrderStatus::PartiallyFilled, Some((Side::Buy, "1", "5")), clock.now());
    a.on_event(first.clone(), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    let full = confirmation(&a, OrderStatus::Filled, Some((Side::Buy, "1", "5")), clock.now());
    broker
        .queue_recovery(Ok(OrderRecovery {
            submission_expired: false,
            order: Some(full.lifecycle.order.clone()),
            trades: vec![first.trade.unwrap(), full.trade.clone().unwrap()],
        }))
        .await;
    clock.advance(RECOVERY_INTERVAL);
    tokio::time::timeout(Duration::from_millis(10), a.on_event(Tick::now(), &cx))
        .await
        .unwrap()
        .unwrap();
    tokio::task::yield_now().await;
    assert_eq!(a.recovery_task.as_ref().unwrap().0, entry_id);
    a.on_event(book("0.9994", "1.0006", "100", clock.now()), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(broker.placed_count().await, 2);
    assert_eq!(a.recovery_task.as_ref().unwrap().0, entry_id, "close must preserve entry recovery");
    let close =
        confirmation(&a, OrderStatus::Filled, Some((Side::Sell, "0.9994", "5")), clock.now());
    a.on_event(close, &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert!(a.inventory.is_flat());
    assert_eq!(
        a.recovery_task.as_ref().unwrap().0,
        entry_id,
        "completing close must not abort entry read"
    );
    tokio::time::advance(Duration::from_secs(2)).await;
    deliver_recovery(&mut a, &mut recovery_rx, &cx).await;
    assert_eq!(broker.placed_count().await, 3, "recovered late entry residual closes immediately");
    assert_eq!(broker.last_placed_orders().await[0].quantity, d("5"));
    a.on_event(full, &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    let last =
        confirmation(&a, OrderStatus::Filled, Some((Side::Sell, "0.9994", "5")), clock.now());
    a.on_event(last, &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert!(a.inventory.is_flat());
    assert!(a.tracked.is_empty());
    assert_eq!(a.inventory.total_fills, 4);
    assert_eq!(a.inventory.realized_pnl, d("-0.006"));
}

#[tokio::test(start_paused = true)]
async fn unknown_close_retries_once_per_second_despite_slow_recovery() {
    let (broker, clock, cx) = context();
    let mut a = holding(&clock, Side::Buy);
    broker.set_recovery_delay(Duration::from_secs(10)).await;
    broker.queue_place_response(Err(BotError::exchange("response lost", true))).await;
    a.place_exit(&cx, ExitReason::StopLoss).unwrap();
    settle_submissions(&mut a, &cx).await;
    for second in 0..3 {
        for _ in 0..10 {
            a.on_event(Tick::now(), &cx).await.unwrap();
            settle_submissions(&mut a, &cx).await;
            a.on_event(book("0.9994", "1.0006", "100", clock.now()), &cx).await.unwrap();
            settle_submissions(&mut a, &cx).await;
            tokio::task::yield_now().await;
        }
        assert_eq!(broker.placed_count().await, second + 1);
        clock.advance(RECOVERY_INTERVAL);
    }
    let calls = broker.history().await;
    assert!(calls.iter().flat_map(|c| &c.orders).all(|o| o.reduce_only && o.side == Side::Sell));
    assert_eq!(calls.iter().filter(|c| c.method == "recover_order").count(), 1);
}

#[tokio::test]
async fn recovery_starts_after_one_second_not_on_every_tick() {
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    let mut recovery_rx = task_events(&mut a);
    a.place_entry(&cx, Side::Buy).unwrap();
    settle_submissions(&mut a, &cx).await;
    clock.advance(Duration::from_millis(999));
    a.on_event(Tick::now(), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert!(!broker.history().await.iter().any(|c| c.method == "recover_order"));
    clock.advance(Duration::from_millis(1));
    a.on_event(Tick::now(), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    deliver_recovery(&mut a, &mut recovery_rx, &cx).await;
    assert_eq!(broker.history().await.iter().filter(|c| c.method == "recover_order").count(), 1);
    assert_eq!(broker.placed_count().await, 1);
}

#[tokio::test]
async fn normal_partial_ioc_close_retries_only_the_residual_without_rest() {
    for side in [Side::Buy, Side::Sell] {
        let (broker, clock, cx) = context();
        let mut a = holding(&clock, side);
        a.place_exit(&cx, ExitReason::StopLoss).unwrap();
        settle_submissions(&mut a, &cx).await;
        let fill = confirmation(
            &a,
            OrderStatus::PartiallyFilled,
            Some((side.opposite(), "1", "5")),
            clock.now(),
        );
        a.on_event(fill, &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        let mut cancel = confirmation(&a, OrderStatus::Cancelled, None, clock.now());
        cancel.lifecycle.order.filled_quantity = Decimal::ZERO; // actual SDK cancel carries no cumulative qty
        a.on_event(cancel, &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        assert_eq!(
            broker.placed_count().await,
            2,
            "normal cancel must release the confirmed residual immediately"
        );
        assert_eq!(broker.last_placed_orders().await[0].quantity, d("15"));
        assert!(broker.last_placed_orders().await[0].reduce_only);
        tokio::task::yield_now().await;
        assert!(!broker.history().await.iter().any(|c| c.method == "recover_order"));
    }
}

#[tokio::test]
async fn normal_empty_or_partial_entry_cancel_completes_without_rest() {
    for qty in ["0", "7"] {
        let (broker, clock, cx) = context();
        let mut a = ready(&clock);
        a.place_entry(&cx, Side::Buy).unwrap();
        settle_submissions(&mut a, &cx).await;
        if qty != "0" {
            let fill = confirmation(
                &a,
                OrderStatus::PartiallyFilled,
                Some((Side::Buy, "1", qty)),
                clock.now(),
            );
            a.on_event(fill, &cx).await.unwrap();
            settle_submissions(&mut a, &cx).await;
        }
        let mut cancel = confirmation(&a, OrderStatus::Cancelled, None, clock.now());
        cancel.lifecycle.order.filled_quantity = Decimal::ZERO;
        a.on_event(cancel, &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        assert!(a.tracked.is_empty());
        assert_eq!(a.inventory.net_position, d(qty));
        assert_eq!(broker.placed_count().await, 1);
        tokio::task::yield_now().await;
        assert!(!broker.history().await.iter().any(|c| c.method == "recover_order"));
    }
}

#[tokio::test]
async fn cumulative_fill_gap_requires_recovery_after_cancel() {
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    let mut recovery_rx = task_events(&mut a);
    a.place_entry(&cx, Side::Buy).unwrap();
    settle_submissions(&mut a, &cx).await;
    let mut missing =
        confirmation(&a, OrderStatus::PartiallyFilled, Some((Side::Buy, "1", "3")), clock.now());
    missing.trade.as_mut().unwrap().trade_id = Some("missing".into());
    let mut received =
        confirmation(&a, OrderStatus::PartiallyFilled, Some((Side::Buy, "1", "5")), clock.now());
    received.lifecycle.order.filled_quantity = d("8");
    a.on_event(received.clone(), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert!(a.tracked.back().unwrap().stream_gap);
    let cancel = confirmation(&a, OrderStatus::Cancelled, None, clock.now());
    a.on_event(cancel.clone(), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(a.state(), "entering");
    assert_eq!(broker.placed_count().await, 1);
    let mut order = cancel.lifecycle.order;
    order.filled_quantity = d("8");
    broker
        .queue_recovery(Ok(OrderRecovery {
            submission_expired: false,
            order: Some(order),
            trades: vec![missing.trade.unwrap(), received.trade.unwrap()],
        }))
        .await;
    deliver_recovery(&mut a, &mut recovery_rx, &cx).await;
    assert_eq!(a.state(), "holding");
    assert_eq!(a.inventory.net_position, d("8"));
    assert_eq!(a.inventory.total_fills, 2);
    assert_eq!(broker.placed_count().await, 1);
}

#[tokio::test]
async fn terminal_partial_fill_closes_only_the_actual_size_without_rest() {
    for side in [Side::Buy, Side::Sell] {
        let (broker, clock, cx) = context();
        let mut a = ready(&clock);
        a.place_entry(&cx, side).unwrap();
        settle_submissions(&mut a, &cx).await;
        let first =
            confirmation(&a, OrderStatus::PartiallyFilled, Some((side, "1", "5")), clock.now());
        a.on_event(first.clone(), &cx).await.unwrap();
        clock.advance(Duration::from_millis(10));
        // Adapter normalizes PARTIALLY_FILLED + rs=0 to a cancelled remainder,
        // with the last Trade and cumulative count in the same ExecutionUpdate.
        let mut last =
            confirmation(&a, OrderStatus::Cancelled, Some((side, "1", "2")), clock.now());
        last.lifecycle.order.filled_quantity = d("7");
        a.on_event(last.clone(), &cx).await.unwrap();
        a.on_event(first.clone(), &cx).await.unwrap();
        a.on_event(last, &cx).await.unwrap();
        assert!(a.tracked.is_empty());
        assert_eq!(a.inventory.net_position.abs(), d("7"));
        assert_eq!(a.inventory.total_fills, 2);
        assert_eq!(a.entry.unwrap().filled_at, Some(first.received_at));
        clock.advance(Duration::from_secs(60));
        a.on_event(Tick::now(), &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        let close = broker.last_placed_orders().await.pop().unwrap();
        assert!(close.reduce_only);
        assert_eq!(close.quantity, d("7"));
        assert_eq!(close.side, side.opposite());
        let fill =
            confirmation(&a, OrderStatus::Filled, Some((side.opposite(), "1", "7")), clock.now());
        a.on_event(fill.clone(), &cx).await.unwrap();
        a.on_event(fill, &cx).await.unwrap();
        clock.advance(RECOVERY_INTERVAL);
        a.on_event(Tick::now(), &cx).await.unwrap();
        assert!(a.is_flat());
        assert_eq!(a.inventory.total_fills, 3);
        assert_eq!(broker.placed_count().await, 2);
        assert!(!broker.history().await.iter().any(|c| c.method == "recover_order"));
    }
}

#[tokio::test]
async fn terminal_partial_fill_with_missing_trades_keeps_protection_and_recovers_once() {
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    a.place_entry(&cx, Side::Buy).unwrap();
    settle_submissions(&mut a, &cx).await;
    let entry_id = a.tracked[0].client_id.clone();
    let mut missing =
        confirmation(&a, OrderStatus::PartiallyFilled, Some((Side::Buy, "1", "3")), clock.now());
    missing.trade.as_mut().unwrap().trade_id = Some("missing".into());
    let mut last =
        confirmation(&a, OrderStatus::Cancelled, Some((Side::Buy, "1", "5")), clock.now());
    last.lifecycle.order.filled_quantity = d("8");
    a.on_event(last.clone(), &cx).await.unwrap();
    assert!(a.tracked[0].stream_gap);
    assert!(a.recovering());
    // A missing earlier fill still needs recovery, but must not delay protection.
    a.on_event(reference("0.9999", "0.9999", clock.now()), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(broker.placed_count().await, 2);
    assert!(broker.last_placed_orders().await[0].reduce_only);
    a.apply_recovery(
        &entry_id,
        OrderRecovery {
            submission_expired: false,
            order: Some(last.lifecycle.order.clone()),
            trades: vec![missing.trade.unwrap(), last.trade.clone().unwrap()],
        },
        &cx,
    );
    a.on_event(last, &cx).await.unwrap();
    assert_eq!(a.inventory.net_position, d("8"));
    assert_eq!(a.inventory.total_fills, 2);
    assert!(a.tracked.iter().all(|t| t.reduce_only));
    let close = confirmation(&a, OrderStatus::Filled, Some((Side::Sell, "1", "8")), clock.now());
    a.on_event(close, &cx).await.unwrap();
    assert!(a.is_flat());
    assert_eq!(a.inventory.total_fills, 3);
    assert_eq!(broker.placed_count().await, 2);
}

#[tokio::test(start_paused = true)]
async fn normal_entry_cancel_aborts_only_its_recovery_and_preserves_protective_close() {
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    broker.set_recovery_delay(Duration::from_secs(10)).await;
    a.place_entry(&cx, Side::Buy).unwrap();
    settle_submissions(&mut a, &cx).await;
    let entry_id = a.tracked.back().unwrap().client_id.clone();
    let fill =
        confirmation(&a, OrderStatus::PartiallyFilled, Some((Side::Buy, "1", "5")), clock.now());
    a.on_event(fill, &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    clock.advance(RECOVERY_INTERVAL);
    a.on_event(Tick::now(), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    tokio::task::yield_now().await;
    assert!(a.recovery_task.is_some());
    a.on_event(book("0.9994", "1.0006", "100", clock.now()), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(broker.placed_count().await, 2);
    // The transient stop must survive even if prices recover before cancel.
    a.on_event(book("0.9999", "1.0001", "100", clock.now()), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    let cancel = confirmation_for(&a, &entry_id, OrderStatus::Cancelled, None, clock.now());
    tokio::time::timeout(Duration::from_millis(10), a.on_event(cancel, &cx))
        .await
        .unwrap()
        .unwrap();
    assert!(a.recovery_task.is_none());
    assert_eq!(a.state(), "exiting");
    assert!(matches!(a.exit_requested, Some(ExitReason::StopLoss)));
    assert_eq!(broker.placed_count().await, 2);
    assert_eq!(broker.last_placed_orders().await[0].quantity, d("999"));
    let close = confirmation(&a, OrderStatus::Filled, Some((Side::Sell, "1", "5")), clock.now());
    a.on_event(close, &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    tokio::time::advance(Duration::from_secs(10)).await;
    a.on_event(Tick::now(), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert!(a.inventory.is_flat());
    assert_eq!(broker.placed_count().await, 2);
}

#[tokio::test]
async fn both_close_orders_count_ws_and_rest_fills_once_and_wait_before_reentry() {
    for side in [Side::Buy, Side::Sell] {
        let (broker, clock, cx) = context();
        let mut a = holding(&clock, side);
        broker.queue_place_response(Err(BotError::exchange("response lost", true))).await;
        a.place_exit(&cx, ExitReason::StopLoss).unwrap();
        settle_submissions(&mut a, &cx).await;
        let first_id = a.tracked.back().unwrap().client_id.clone();
        let first = confirmation(
            &a,
            OrderStatus::PartiallyFilled,
            Some((side.opposite(), "0.9994", "5")),
            clock.now(),
        );
        a.on_event(first.clone(), &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        let missing = confirmation_for(
            &a,
            &first_id,
            OrderStatus::PartiallyFilled,
            Some((side.opposite(), "0.9996", "5")),
            clock.now(),
        );
        // First recovery sees only the already received fill; the order remains pending.
        a.apply_recovery(
            &first_id,
            OrderRecovery {
                submission_expired: false,
                order: Some(first.lifecycle.order.clone()),
                trades: vec![first.trade.clone().unwrap()],
            },
            &cx,
        );
        clock.advance(RECOVERY_INTERVAL);
        a.on_event(Tick::now(), &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        assert_eq!(broker.placed_count().await, 2);
        assert_eq!(broker.last_placed_orders().await[0].quantity, d("15"));
        let second_id = a.tracked.back().unwrap().client_id.clone();
        let second = confirmation_for(
            &a,
            &second_id,
            OrderStatus::PartiallyFilled,
            Some((side.opposite(), "0.9998", "10")),
            clock.now(),
        );
        a.on_event(second.clone(), &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        a.on_event(first.clone(), &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        a.on_event(second.clone(), &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        a.apply_recovery(
            &second_id,
            OrderRecovery {
                submission_expired: false,
                order: Some(second.lifecycle.order.clone()),
                trades: vec![second.trade.clone().unwrap()],
            },
            &cx,
        );
        assert_eq!(a.inventory.total_fills, 3); // original entry + two distinct closes
        let mut terminal = missing.lifecycle.order.clone();
        terminal.status = OrderStatus::Cancelled;
        terminal.filled_quantity = d("10");
        a.apply_recovery(
            &first_id,
            OrderRecovery {
                submission_expired: false,
                order: Some(terminal),
                trades: vec![first.trade.unwrap(), missing.trade.clone().unwrap()],
            },
            &cx,
        );
        assert!(a.inventory.is_flat());
        assert_eq!(a.tracked.len(), 1);
        assert_ne!(a.state(), "flat");
        a.config.impulse = None;
        a.on_event(reference("1.001", "1.001", clock.now()), &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        assert_eq!(broker.placed_count().await, 2);
        a.on_event(missing, &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await; // late WS copy of the REST fill
        let cancel = confirmation_for(&a, &second_id, OrderStatus::Cancelled, None, clock.now());
        a.on_event(cancel, &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        assert!(a.tracked.is_empty());
        assert_eq!(a.state(), "flat");
        assert_eq!(a.inventory.total_fills, 4);
        assert_eq!(
            a.inventory.realized_pnl,
            if side == Side::Buy { d("-0.007") } else { d("0.007") }
        );
    }
}

#[tokio::test]
async fn close_fills_before_missing_entry_never_reverse_protective_direction() {
    for side in [Side::Buy, Side::Sell] {
        let (broker, clock, cx) = context();
        let mut a = ready(&clock);
        a.place_entry(&cx, side).unwrap();
        settle_submissions(&mut a, &cx).await;
        let entry_id = a.tracked.back().unwrap().client_id.clone();
        let first =
            confirmation(&a, OrderStatus::PartiallyFilled, Some((side, "1", "5")), clock.now());
        a.on_event(first, &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        a.on_event(book("0.9994", "1.0006", "100", clock.now()), &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        let first_close_id = a.tracked.back().unwrap().client_id.clone();
        clock.advance(RECOVERY_INTERVAL);
        a.on_event(Tick::now(), &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        let second_close_id = broker.last_placed_orders().await[0].client_id.clone().unwrap();
        assert_ne!(first_close_id, second_close_id);
        for id in [&second_close_id, &first_close_id] {
            let close = confirmation_for(
                &a,
                id,
                OrderStatus::Filled,
                Some((side.opposite(), "1", "5")),
                clock.now(),
            );
            a.on_event(close, &cx).await.unwrap();
            settle_submissions(&mut a, &cx).await;
        }
        // Both reducing fills were possible because another entry fill happened
        // on the venue, but its notification has not arrived yet.
        assert_eq!(a.inventory.net_position, if side == Side::Buy { d("-5") } else { d("5") });
        clock.advance(RECOVERY_INTERVAL);
        a.on_event(Tick::now(), &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        assert_eq!(broker.placed_count().await, 4);
        let pending_close_id = broker.last_placed_orders().await[0].client_id.clone().unwrap();
        let order = &broker.last_placed_orders().await[0];
        assert!(order.reduce_only);
        assert_eq!(order.side, side.opposite());
        let late = confirmation_for(
            &a,
            &entry_id,
            OrderStatus::Filled,
            Some((side, "1.0002", "8")),
            clock.now(),
        );
        a.on_event(late.clone(), &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        let cancel =
            confirmation_for(&a, &pending_close_id, OrderStatus::Cancelled, None, clock.now());
        a.on_event(cancel, &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        assert_eq!(broker.placed_count().await, 5);
        let remaining = &broker.last_placed_orders().await[0];
        assert!(remaining.reduce_only);
        assert_eq!(remaining.side, side.opposite());
        assert_eq!(remaining.quantity, d("3"));
        a.on_event(late, &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        let close = confirmation(
            &a,
            OrderStatus::Filled,
            Some((side.opposite(), "1.0001", "3")),
            clock.now(),
        );
        a.on_event(close, &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        assert!(a.inventory.is_flat());
        assert!(a.tracked.is_empty());
        assert_eq!(a.inventory.total_fills, 5);
        assert_eq!(
            a.inventory.realized_pnl,
            if side == Side::Buy { d("-0.0013") } else { d("0.0013") }
        );
    }
}

#[tokio::test]
async fn unresolved_entry_cannot_starve_recovery_of_an_unknown_close() {
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    let mut recovery_rx = task_events(&mut a);
    a.place_entry(&cx, Side::Buy).unwrap();
    settle_submissions(&mut a, &cx).await;
    let entry_id = a.tracked.back().unwrap().client_id.clone();
    let first =
        confirmation(&a, OrderStatus::PartiallyFilled, Some((Side::Buy, "1", "5")), clock.now());
    a.on_event(first, &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    a.place_exit(&cx, ExitReason::StopLoss).unwrap();
    settle_submissions(&mut a, &cx).await;
    let unknown_close_id = a.tracked.back().unwrap().client_id.clone();
    clock.advance(RECOVERY_INTERVAL);
    a.on_event(Tick::now(), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(a.recovery_task.as_ref().unwrap().0, entry_id);
    let second_close_id = broker.last_placed_orders().await[0].client_id.clone().unwrap();
    let filled = confirmation_for(
        &a,
        &second_close_id,
        OrderStatus::Filled,
        Some((Side::Sell, "1", "5")),
        clock.now(),
    );
    a.on_event(filled, &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    deliver_recovery(&mut a, &mut recovery_rx, &cx).await;
    for id in [&unknown_close_id, &entry_id, &unknown_close_id] {
        broker
            .queue_place_results(vec![OrderResult {
                order_id: None,
                client_id: None,
                success: false,
                error: Some("position already closed".into()),
            }])
            .await;
        clock.advance(RECOVERY_INTERVAL);
        a.on_event(Tick::now(), &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        assert_eq!(&a.recovery_task.as_ref().unwrap().0, id);
        deliver_recovery(&mut a, &mut recovery_rx, &cx).await;
    }
    assert_eq!(broker.history().await.iter().filter(|c| c.method == "recover_order").count(), 4);
    assert_eq!(broker.placed_count().await, 6);
    assert!(a.inventory.is_flat());
    assert_eq!(a.tracked.len(), 2);
    assert_ne!(a.state(), "flat");
}

#[tokio::test]
async fn timeout_closes_a_partial_entry_even_without_its_terminal_confirmation() {
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    a.place_entry(&cx, Side::Buy).unwrap();
    settle_submissions(&mut a, &cx).await;
    let first =
        confirmation(&a, OrderStatus::PartiallyFilled, Some((Side::Buy, "1", "5")), clock.now());
    a.on_event(first, &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    clock.advance(Duration::from_secs(60));
    a.on_event(Tick::now(), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert!(matches!(a.exit_requested, Some(ExitReason::Timeout)));
    assert_eq!(a.tracked.len(), 2);
    assert_eq!(broker.placed_count().await, 2);
    assert!(broker.last_placed_orders().await[0].reduce_only);
    assert_eq!(broker.last_placed_orders().await[0].quantity, d("999"));
}

#[tokio::test(start_paused = true)]
async fn shutdown_confirms_pending_entry_and_protective_close_independently() {
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    a.client_ids = ClientIdIssuer::starting_at(500);
    a.place_entry(&cx, Side::Buy).unwrap();
    settle_submissions(&mut a, &cx).await;
    let first =
        confirmation(&a, OrderStatus::PartiallyFilled, Some((Side::Buy, "1", "7")), clock.now());
    a.on_event(first.clone(), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    let mut entry_terminal = first.lifecycle.order.clone();
    entry_terminal.status = OrderStatus::Cancelled;
    broker
        .queue_recovery(Ok(OrderRecovery {
            submission_expired: false,
            order: Some(entry_terminal),
            trades: vec![first.trade.clone().unwrap()],
        }))
        .await;
    let mut close = first;
    close.lifecycle.order.client_id = Some("501".into());
    close.lifecycle.order.id = "2".into();
    close.lifecycle.order.side = Side::Sell;
    close.lifecycle.order.quantity = d("7");
    close.lifecycle.order.status = OrderStatus::Filled;
    let fill = close.trade.as_mut().unwrap();
    fill.client_id = Some("501".into());
    fill.order_id = "2".into();
    fill.trade_id = Some("close-fill".into());
    fill.side = Side::Sell;
    fill.price = d("0.9999");
    broker
        .queue_recovery(Ok(OrderRecovery {
            submission_expired: false,
            order: Some(close.lifecycle.order),
            trades: vec![close.trade.unwrap()],
        }))
        .await;
    a.wind_down(&WindDownReason::Signal, &cx).await.unwrap();
    assert!(a.inventory.is_flat());
    assert!(a.tracked.is_empty());
    assert_eq!(a.inventory.total_fills, 2);
    assert_eq!(a.inventory.realized_pnl, d("-0.0007"));
    assert_eq!(broker.placed_count().await, 2);
    assert!(broker.last_placed_orders().await[0].reduce_only);
    assert!(!broker.history().await.iter().any(|c| c.method == "cancel_all_orders"));
}

#[test]
fn config_accepts_signed_signal_thresholds_and_zero_exit_allowance() {
    let mut c = config();
    c.entry_threshold_bps = -1.0;
    c.exit = ExitConfig::Fair { remaining_bps: 0.25 };
    c.impulse.as_mut().unwrap().threshold_bps = -1.0;
    c.market_slippage_bps = Decimal::ZERO;
    c.validate().unwrap();
    c.entry_threshold_bps = f64::NAN;
    assert!(c.validate().is_err());
    c.entry_threshold_bps = -1.0;
    c.exit = ExitConfig::Fair { remaining_bps: f64::INFINITY };
    assert!(c.validate().is_err());
    c.exit = ExitConfig::Fair { remaining_bps: 0.25 };
    c.impulse.as_mut().unwrap().threshold_bps = f64::NEG_INFINITY;
    assert!(c.validate().is_err());
}

#[tokio::test]
async fn only_binance_has_a_quote_age_limit() {
    for (reference_age, exchange_age, entry_allowed) in [
        (0, 0, true),
        (1000, 1000, true),
        (1001, 0, false),
        (0, 1001, true),
        (0, 60_000, true),
        (1001, 60_000, false),
    ] {
        let (broker, clock, cx) = context();
        clock.advance(Duration::from_secs(60));
        let mut a = ready(&clock);
        a.config.impulse = None;
        a.binance = Some(reference(
            "1.0003",
            "1.0003",
            clock.now().checked_sub(Duration::from_millis(reference_age)).unwrap(),
        ));
        a.bullet_last_seen =
            Some(clock.now().checked_sub(Duration::from_millis(exchange_age)).unwrap());
        // A private execution gap alone must not invalidate the market quote.
        broker.flag_reconcile();
        a.evaluate(&cx, true).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        assert_eq!(broker.placed_count().await, usize::from(entry_allowed));
    }
}

#[tokio::test]
async fn reconnect_waits_for_new_bbo_before_entry_ema_and_signal_reset() {
    for dry_run in [false, true] {
        let (broker, clock, cx) = context();
        let mut a = ready(&clock);
        a.config.dry_run = dry_run;
        a.config.impulse = None;
        let before_reconnect = clock.now();
        let basis_before = (a.basis, a.basis_at);
        a.reentry = [Reentry { reset: false, confirmed_at: Some(clock.now()) }; 2];
        clock.advance(Duration::from_millis(1));
        broker.flag_reconnecting(clock.now());

        // This would reset the long signal and move EMA using the old Bullet BBO.
        a.on_event(reference("0.9998", "0.9998", clock.now()), &cx).await.unwrap();
        // Even after the reconcile flag was consumed, a queued old BBO is invalid.
        a.on_event(book("0.9999", "1.0001", "2000", before_reconnect), &cx).await.unwrap();
        assert_eq!((a.basis, a.basis_at), basis_before);
        assert!(a.reentry.iter().all(|r| !r.reset));

        // A quote from between two reconnects also cannot restore readiness.
        clock.advance(Duration::from_millis(1));
        let queued = book("0.9999", "1.0001", "2000", clock.now());
        clock.advance(Duration::from_millis(1));
        broker.flag_reconnecting(clock.now());
        a.on_event(queued, &cx).await.unwrap();
        a.on_event(book("1.001", "1", "2000", clock.now()), &cx).await.unwrap();
        assert!(!a.quotes_ready(&cx).unwrap());
        assert_eq!((a.basis, a.basis_at), basis_before);
        assert!(a.reentry.iter().all(|r| !r.reset));

        // The 10-second reentry delay can expire, but cannot bypass the new BBO.
        clock.advance(Duration::from_secs(10));
        a.on_event(reference("1.0003", "1.0003", clock.now()), &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        assert_eq!(broker.placed_count().await, 0);
        assert!(a.entry.is_none());
        assert_eq!((a.basis, a.basis_at), basis_before);
        assert!(a.reentry.iter().all(|r| !r.reset));

        a.on_event(book("0.9999", "1.0001", "2000", clock.now()), &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
        assert!(a.quotes_ready(&cx).unwrap());
        let elapsed = clock.now().duration_since(before_reconnect).as_secs_f64();
        let alpha = -(-std::f64::consts::LN_2 * elapsed / 30.0).exp_m1();
        assert!((a.basis.unwrap() - (1.0_f64 / 1.0003).ln() * alpha).abs() < 1e-12);
        assert!(a.reentry[1].reset); // the short signal reset on the new pair
        assert_eq!(a.entry.unwrap().side, Side::Buy);
        assert_eq!(a.entry.unwrap().basis, a.basis.unwrap());
        assert_eq!(broker.placed_count().await, usize::from(!dry_run));
        assert!(!broker.history().await.iter().any(|call| call.method == "recover_order"));
    }
}

#[tokio::test]
async fn binance_gap_exit_does_not_wait_for_post_reconnect_bbo() {
    let (broker, clock, cx) = context();
    let mut a = holding(&clock, Side::Buy);
    clock.advance(Duration::from_millis(1));
    broker.flag_reconnecting(clock.now());
    a.on_event(reference("0.9999", "0.9999", clock.now()), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert!(!a.quotes_ready(&cx).unwrap());
    assert!(matches!(a.exit_requested, Some(ExitReason::Fair)));
    assert_eq!(broker.placed_count().await, 1);
    assert!(broker.last_placed_orders().await[0].reduce_only);
}

#[tokio::test]
async fn price_exits_run_on_both_market_feeds_without_any_timer_tick() {
    for leader_update in [true, false] {
        let (broker, clock, cx) = context();
        let mut a = holding(&clock, Side::Buy);
        if leader_update {
            a.on_event(reference("0.9999", "0.9999", clock.now()), &cx).await.unwrap();
            settle_submissions(&mut a, &cx).await;
            assert!(matches!(a.exit_requested, Some(ExitReason::Fair)));
        } else {
            a.on_event(book("0.9994", "1.0006", "100", clock.now()), &cx).await.unwrap();
            settle_submissions(&mut a, &cx).await;
            assert!(matches!(a.exit_requested, Some(ExitReason::StopLoss)));
        }
        assert_eq!(broker.placed_count().await, 1);
        assert!(broker.last_placed_orders().await[0].reduce_only);
    }
}

#[tokio::test]
async fn missing_formula_inputs_do_not_become_zero_valued_signals() {
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    // Even a negative threshold must not turn missing history into a passing zero.
    a.config.impulse.as_mut().unwrap().threshold_bps = -1.0;
    a.binance = Some(reference("1.0002", "1.0002", clock.now()));
    a.evaluate(&cx, true).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(broker.placed_count().await, 0);
    a.config.impulse = None;
    a.basis = None;
    a.evaluate(&cx, true).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(broker.placed_count().await, 0);
    // No warmup timer: the first usable pair initializes EMA immediately.
    a.update_basis(&cx).unwrap();
    assert!(a.basis.is_some());
}

#[tokio::test]
async fn unknown_entry_can_close_before_any_entry_fill_and_late_fills_reconcile() {
    for side in [Side::Buy, Side::Sell] {
        for timeout in [false, true] {
            let (broker, clock, cx) = context();
            let mut a = ready(&clock);
            a.config.order_notional = d("10.011"); // ten LIT for either side
            broker.queue_place_response(Err(BotError::exchange("response lost", true))).await;
            a.place_entry(&cx, side).unwrap();
            settle_submissions(&mut a, &cx).await;
            let entry_id = a.tracked.back().unwrap().client_id.clone();
            assert_eq!(a.tracked.back().unwrap().quantity, d("10"));
            if timeout {
                clock.advance(Duration::from_secs(60));
                a.on_event(Tick::now(), &cx).await.unwrap();
                assert!(matches!(a.exit_requested, Some(ExitReason::Timeout)));
            } else {
                let price = if side == Side::Buy { "0.9999" } else { "1.0001" };
                a.on_event(reference(price, price, clock.now()), &cx).await.unwrap();
                assert!(matches!(a.exit_requested, Some(ExitReason::Fair)));
            }
            settle_submissions(&mut a, &cx).await;
            assert_eq!(broker.placed_count().await, 2);
            assert_eq!(broker.last_placed_orders().await[0].quantity, d("10"));
            assert!(broker.last_placed_orders().await[0].reduce_only);
            assert!(a.inventory.is_flat(), "submission must not invent fills");
            let close_id = broker.last_placed_orders().await[0].client_id.clone().unwrap();
            let close = confirmation_for(
                &a,
                &close_id,
                OrderStatus::PartiallyFilled,
                Some((side.opposite(), "0.9999", "7")),
                clock.now(),
            );
            a.on_event(close.clone(), &cx).await.unwrap();
            settle_submissions(&mut a, &cx).await;
            let cancel = confirmation_for(&a, &close_id, OrderStatus::Cancelled, None, clock.now());
            a.on_event(cancel, &cx).await.unwrap();
            settle_submissions(&mut a, &cx).await;
            assert_eq!(
                broker.placed_count().await,
                2,
                "unknown-size retry uses the one-second interval"
            );
            let entry = confirmation_for(
                &a,
                &entry_id,
                OrderStatus::Filled,
                Some((side, "1", "10")),
                clock.now(),
            );
            a.on_event(entry.clone(), &cx).await.unwrap();
            settle_submissions(&mut a, &cx).await;
            assert_eq!(broker.placed_count().await, 3);
            assert_eq!(broker.last_placed_orders().await[0].quantity, d("3"));
            assert_eq!(broker.last_placed_orders().await[0].side, side.opposite());
            a.on_event(close, &cx).await.unwrap();
            settle_submissions(&mut a, &cx).await;
            a.apply_recovery(
                &entry_id,
                OrderRecovery {
                    submission_expired: false,
                    order: Some(entry.lifecycle.order.clone()),
                    trades: vec![entry.trade.clone().unwrap()],
                },
                &cx,
            );
            a.on_event(entry, &cx).await.unwrap();
            settle_submissions(&mut a, &cx).await;
            let last = confirmation(
                &a,
                OrderStatus::Filled,
                Some((side.opposite(), "1", "3")),
                clock.now(),
            );
            a.on_event(last, &cx).await.unwrap();
            settle_submissions(&mut a, &cx).await;
            assert!(a.inventory.is_flat());
            assert!(a.tracked.is_empty());
            assert_eq!(a.inventory.total_fills, 3);
            assert_eq!(
                a.inventory.realized_pnl,
                if side == Side::Buy { d("-0.0007") } else { d("0.0007") }
            );
        }
    }
}

#[tokio::test]
async fn empty_reduce_only_close_does_not_spin_while_entry_size_is_unknown() {
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    a.place_entry(&cx, Side::Buy).unwrap();
    settle_submissions(&mut a, &cx).await;
    let entry_id = a.tracked.back().unwrap().client_id.clone();
    a.on_event(reference("0.9999", "0.9999", clock.now()), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    let cancel = confirmation(&a, OrderStatus::Cancelled, None, clock.now());
    a.on_event(cancel, &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    for _ in 0..10 {
        a.on_event(reference("0.9999", "0.9999", clock.now()), &cx).await.unwrap();
        settle_submissions(&mut a, &cx).await;
    }
    assert_eq!(broker.placed_count().await, 2);
    clock.advance(RECOVERY_INTERVAL);
    a.on_event(Tick::now(), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(broker.placed_count().await, 3);
    let close_id = broker.last_placed_orders().await[0].client_id.clone().unwrap();
    let entry_cancel = confirmation_for(&a, &entry_id, OrderStatus::Cancelled, None, clock.now());
    a.on_event(entry_cancel, &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    let close_cancel = confirmation_for(&a, &close_id, OrderStatus::Cancelled, None, clock.now());
    a.on_event(close_cancel, &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert!(a.tracked.is_empty());
    assert!(a.inventory.is_flat());
    assert_eq!(a.inventory.total_fills, 0);
    assert_eq!(a.state(), "flat");
    assert_eq!(broker.placed_count().await, 3);
}

#[tokio::test(start_paused = true)]
async fn slow_submission_does_not_block_harness_timer_or_reduce_only_exit() {
    use bb_core::harness::{ActorSpec, HarnessBuilder, MpscFeed};
    struct Observed(ReferenceArbActor);
    #[async_trait]
    impl Actor for Observed {}
    #[async_trait]
    impl EventHandler<ReferencePriceUpdate> for Observed {
        async fn on_event(
            &mut self,
            e: ReferencePriceUpdate,
            cx: &ActorContext,
        ) -> Result<(), BotError> {
            self.0.on_event(e, cx).await
        }
    }
    #[async_trait]
    impl EventHandler<Tick> for Observed {
        async fn on_event(&mut self, e: Tick, cx: &ActorContext) -> Result<(), BotError> {
            self.0.on_event(e, cx).await?;
            assert!(matches!(self.0.exit_requested, Some(ExitReason::Timeout)));
            assert_eq!(self.0.tracked.len(), 2);
            assert!(self.0.tracked.iter().all(|t| t.submission.is_some()));
            // Both HTTP responses are outstanding. Give spawned requests a turn
            // before dropping this test actor on shutdown.
            tokio::task::yield_now().await;
            cx.request_shutdown();
            Ok(())
        }
    }
    let (broker, clock, _) = context();
    broker.set_place_delay(Duration::from_secs(5)).await;
    let mut a = ready(&clock);
    a.config.impulse = None;
    let (ref_tx, ref_rx) = tokio::sync::mpsc::unbounded_channel();
    let (tick_tx, tick_rx) = tokio::sync::mpsc::unbounded_channel();
    let harness = HarnessBuilder::new()
        .with_clock(Arc::new(clock.clone()))
        .wire_broker("bullet", broker.clone())
        .wire_feed_named("ref", MpscFeed::new(ref_rx))
        .wire_feed_named("ticks", MpscFeed::new(tick_rx))
        .wire_actor(ActorSpec::new("arb", Observed(a)).sub::<ReferencePriceUpdate>().sub::<Tick>())
        .build()
        .unwrap();
    let run = tokio::spawn(harness.run());
    ref_tx.send(reference("1.001", "1.001", clock.now())).unwrap();
    tokio::time::timeout(Duration::from_millis(100), async {
        while broker.placed_count().await != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    clock.advance(Duration::from_secs(60));
    tick_tx.send(Tick::now()).unwrap();
    let reason =
        tokio::time::timeout(Duration::from_millis(100), run).await.unwrap().unwrap().unwrap();
    assert!(matches!(reason, WindDownReason::Signal));
    assert_eq!(broker.placed_count().await, 2);
    assert!(broker.last_placed_orders().await[0].reduce_only);
}

#[tokio::test(start_paused = true)]
async fn fills_and_price_exit_proceed_while_both_submissions_wait() {
    let (broker, clock, cx) = context();
    broker.set_place_delay(Duration::from_secs(5)).await;
    let mut a = ready(&clock);
    a.place_entry(&cx, Side::Buy).unwrap();
    tokio::task::yield_now().await;
    let entry_id = a.tracked.back().unwrap().client_id.clone();
    let first = confirmation_for(
        &a,
        &entry_id,
        OrderStatus::PartiallyFilled,
        Some((Side::Buy, "1", "5")),
        clock.now(),
    );
    tokio::time::timeout(Duration::from_millis(10), a.on_event(first, &cx)).await.unwrap().unwrap();
    tokio::time::timeout(
        Duration::from_millis(10),
        a.on_event(book("0.9994", "1.0006", "100", clock.now()), &cx),
    )
    .await
    .unwrap()
    .unwrap();
    tokio::task::yield_now().await;
    assert_eq!(broker.placed_count().await, 2);
    assert!(a.tracked.iter().all(|t| t.submission.is_some()));
    assert!(broker.last_placed_orders().await[0].reduce_only);
    let close_id = broker.last_placed_orders().await[0].client_id.clone().unwrap();
    let late = confirmation_for(
        &a,
        &entry_id,
        OrderStatus::PartiallyFilled,
        Some((Side::Buy, "1", "5")),
        clock.now(),
    );
    a.on_event(late.clone(), &cx).await.unwrap();
    a.on_event(late, &cx).await.unwrap(); // duplicate while HTTP is still pending
    let cancel = confirmation_for(&a, &entry_id, OrderStatus::Cancelled, None, clock.now());
    a.on_event(cancel, &cx).await.unwrap();
    let close = confirmation_for(
        &a,
        &close_id,
        OrderStatus::Filled,
        Some((Side::Sell, "0.9994", "10")),
        clock.now(),
    );
    a.on_event(close.clone(), &cx).await.unwrap();
    a.on_event(close, &cx).await.unwrap();
    assert!(a.is_flat());
    assert_eq!(a.inventory.total_fills, 3);
    assert_eq!(a.inventory.realized_pnl, d("-0.006"));
    assert_eq!(broker.placed_count().await, 2);
}

#[tokio::test]
async fn late_submission_response_cannot_change_completed_order_or_next_entry() {
    for response in [Ok(()), Err(BotError::exchange("response lost", true))] {
        let (broker, clock, cx) = context();
        let mut a = ready(&clock);
        let mut events = task_events(&mut a);
        broker.queue_place_response(response).await;
        a.place_entry(&cx, Side::Buy).unwrap();
        let stale = events.recv().await.unwrap();
        let cancel = confirmation(&a, OrderStatus::Cancelled, None, clock.now());
        a.on_event(cancel, &cx).await.unwrap();
        assert!(a.is_flat());
        a.place_entry(&cx, Side::Sell).unwrap();
        let new_id = a.tracked.back().unwrap().client_id.clone();
        a.on_event(stale, &cx).await.unwrap();
        assert_eq!(a.tracked.len(), 1);
        assert_eq!(a.tracked[0].client_id, new_id);
        assert!(!a.tracked[0].reconcile_needed);
        assert_eq!(a.inventory.total_fills, 0);
        assert_eq!(a.entry.unwrap().side, Side::Sell);
    }
}

#[tokio::test]
async fn ready_entry_response_precedes_price_or_timer_exit_without_waiting_for_notification() {
    for rejected in [true, false] {
        for trigger in ["bullet", "binance", "timer"] {
            let (broker, clock, cx) = context();
            let mut a = ready(&clock);
            let mut events = task_events(&mut a);
            if rejected {
                broker
                    .queue_place_results(vec![OrderResult {
                        order_id: None,
                        client_id: None,
                        success: false,
                        error: Some("IOC order received no fill".into()),
                    }])
                    .await;
            } else {
                broker.queue_place_response(Err(BotError::exchange("response lost", true))).await;
            }
            a.place_entry(&cx, Side::Buy).unwrap();
            // The result is ready, but its event is behind a market/timer event.
            let notification = events.recv().await.unwrap();
            assert!(a.tracked[0].submission.as_ref().unwrap().is_finished());
            match trigger {
                "bullet" => {
                    a.on_event(book("1.0001", "1.0003", "100", clock.now()), &cx).await.unwrap();
                }
                "binance" => {
                    a.on_event(reference("0.9999", "0.9999", clock.now()), &cx).await.unwrap();
                }
                _ => {
                    clock.advance(Duration::from_secs(60));
                    a.on_event(Tick::now(), &cx).await.unwrap();
                }
            }
            settle_submissions(&mut a, &cx).await;
            a.on_event(notification, &cx).await.unwrap();
            if rejected {
                assert!(a.is_flat());
                assert!(a.exit_requested.is_none());
                assert_eq!(broker.placed_count().await, 1, "no close for a known empty IOC");
                assert!(!broker.history().await.iter().any(|c| c.method == "recover_order"));
            } else {
                assert_eq!(broker.placed_count().await, 2, "unknown entry retains protection");
                let close = broker.last_placed_orders().await.pop().unwrap();
                assert!(close.reduce_only);
                assert_eq!(close.side, Side::Sell);
                assert_eq!(close.quantity, d("999"));
                assert_eq!(a.tracked.len(), 2);
            }
        }
    }
}

#[tokio::test]
async fn ready_close_recovery_precedes_retry_and_late_notifications_do_not_duplicate_fills() {
    let (broker, clock, cx) = context();
    let mut a = holding(&clock, Side::Buy);
    a.place_exit(&cx, ExitReason::StopLoss).unwrap();
    settle_submissions(&mut a, &cx).await;
    let close = confirmation(&a, OrderStatus::Filled, Some((Side::Sell, "1", "20")), clock.now());
    broker
        .queue_recovery(Ok(OrderRecovery {
            submission_expired: false,
            order: Some(close.lifecycle.order.clone()),
            trades: vec![close.trade.clone().unwrap()],
        }))
        .await;
    let mut events = task_events(&mut a);
    clock.advance(RECOVERY_INTERVAL);
    a.recover_if_needed(&cx).unwrap();
    let notification = events.recv().await.unwrap();
    assert!(a.recovery_task.as_ref().unwrap().1.is_finished());
    // The old close is eligible for retry, but its ready recovery proves flat.
    a.on_event(Tick::now(), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    a.on_event(notification, &cx).await.unwrap();
    a.on_event(close, &cx).await.unwrap();
    assert!(a.is_flat());
    assert_eq!(broker.placed_count().await, 1);
    assert_eq!(a.inventory.total_fills, 2);
}

#[tokio::test]
async fn acknowledgement_without_order_id_preserves_earlier_ws_id() {
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    let mut events = task_events(&mut a);
    broker
        .queue_place_results(vec![OrderResult {
            order_id: None,
            client_id: None,
            success: true,
            error: None,
        }])
        .await;
    a.place_entry(&cx, Side::Buy).unwrap();
    let ack = events.recv().await.unwrap();
    let partial =
        confirmation(&a, OrderStatus::PartiallyFilled, Some((Side::Buy, "1", "5")), clock.now());
    a.on_event(partial, &cx).await.unwrap();
    a.on_event(ack, &cx).await.unwrap();
    assert_eq!(a.tracked[0].order_id.as_deref(), Some("1"));
    assert_eq!(a.inventory.net_position, d("5"));
}

#[tokio::test(start_paused = true)]
async fn shutdown_collects_submission_results_after_event_feed_closed() {
    let (broker, clock, cx) = context();
    let mut a = holding(&clock, Side::Buy);
    assert!(a.task_tx.is_closed());
    broker.set_place_delay(Duration::from_millis(1500)).await;
    broker
        .queue_place_results(vec![OrderResult {
            order_id: None,
            client_id: None,
            success: false,
            error: Some("rejected".into()),
        }])
        .await;
    a.place_exit(&cx, ExitReason::StopLoss).unwrap();
    let id = a.tracked[0].client_id.clone();
    let started = tokio::time::Instant::now();
    // Avoid REST completing it first: its response deliberately arrives later.
    broker.set_recovery_delay(Duration::from_secs(20)).await;
    let result = a.wind_down(&WindDownReason::Signal, &cx).await;
    assert!(result.is_err());
    assert_eq!(started.elapsed(), Duration::from_secs(10));
    assert!(!a.tracked.iter().any(|t| t.client_id == id));
    assert_eq!(a.inventory.net_position, d("20"));
    assert!(a.retry_exit_at.is_some());
}

#[tokio::test]
async fn paper_and_ws_share_partial_ioc_accounting_and_exit_state() {
    let (broker, clock, cx) = context();
    let mut paper = ready(&clock);
    paper.config.dry_run = true;
    paper.bullet_book = Some(book("0.9999", "1.0001", "7", clock.now()).orderbook);
    let mut live = ready(&clock);
    paper.binance = Some(reference("1", "1.0003", clock.now()));
    live.binance.clone_from(&paper.binance);
    paper.place_entry(&cx, Side::Buy).unwrap();
    live.place_entry(&cx, Side::Buy).unwrap();
    settle_submissions(&mut live, &cx).await;
    let mut fill =
        confirmation(&live, OrderStatus::Cancelled, Some((Side::Buy, "1.0001", "7")), clock.now());
    fill.lifecycle.order.filled_quantity = d("7");
    live.apply_execution(&fill, &cx).unwrap();
    for a in [&paper, &live] {
        assert_eq!(a.inventory.net_position, d("7"));
        assert_eq!(a.inventory.avg_entry_price, d("1.0001"));
        assert_eq!(a.entry.unwrap().filled_at, Some(clock.now()));
        assert!(a.tracked.is_empty());
        assert_eq!(a.state(), "holding");
    }
    paper.bullet_book = Some(book("0.9994", "1.0006", "5", clock.now()).orderbook);
    live.bullet_book.clone_from(&paper.bullet_book);
    paper.place_exit(&cx, ExitReason::StopLoss).unwrap();
    live.place_exit(&cx, ExitReason::StopLoss).unwrap();
    settle_submissions(&mut live, &cx).await;
    let mut fill =
        confirmation(&live, OrderStatus::Cancelled, Some((Side::Sell, "0.9994", "5")), clock.now());
    fill.lifecycle.order.filled_quantity = d("5");
    live.apply_execution(&fill, &cx).unwrap();
    for a in [&paper, &live] {
        assert_eq!(a.inventory.net_position, d("2"));
        assert_eq!(a.inventory.realized_pnl, d("-0.0035"));
        assert_eq!(a.inventory.total_fills, 2);
        assert!(a.tracked.is_empty());
        assert_eq!(a.state(), "exiting");
        assert!(matches!(a.exit_requested, Some(ExitReason::StopLoss)));
    }
    assert_eq!(broker.placed_count().await, 2); // Only live sent requests.
}

#[tokio::test]
async fn dry_shutdown_reports_unfilled_residual_without_inventing_liquidity() {
    let (broker, clock, cx) = context();
    let mut a = holding(&clock, Side::Buy);
    a.config.dry_run = true;
    a.bullet_book = Some(book("0.9999", "1.0001", "5", clock.now()).orderbook);
    let error = a.wind_down(&WindDownReason::Signal, &cx).await.unwrap_err();
    assert!(error.to_string().contains("15"));
    assert_eq!(a.inventory.net_position, d("15"));
    assert!(a.tracked.is_empty());
    assert_eq!(a.state(), "exiting");
    assert!(broker.history().await.is_empty());
}

#[tokio::test]
async fn lost_old_exit_releases_entries_only_after_expiry_and_confirmed_flat_fills() {
    let (broker, clock, cx) = context();
    let mut a = holding(&clock, Side::Buy);
    let mut rx = task_events(&mut a);
    broker.queue_place_response(Err(BotError::exchange("lost during reconnect", true))).await;
    a.place_exit(&cx, ExitReason::Fair).unwrap();
    settle_submissions(&mut a, &cx).await;
    let old_id = a.tracked[0].client_id.clone();
    // Expiry is not evidence about past fills: keep protection while non-flat.
    a.apply_recovery(
        &old_id,
        OrderRecovery { submission_expired: true, ..Default::default() },
        &cx,
    );
    assert_eq!(a.tracked.len(), 1);
    clock.advance(Duration::from_secs(2));
    a.place_exit(&cx, ExitReason::Fair).unwrap();
    settle_submissions(&mut a, &cx).await;
    let fill =
        confirmation(&a, OrderStatus::Filled, Some((Side::Sell, "1.0001", "20")), clock.now());
    a.on_event(fill.clone(), &cx).await.unwrap();
    assert!(a.inventory.is_flat());
    assert!(!a.is_flat());
    a.apply_recovery(&old_id, OrderRecovery::default(), &cx);
    assert!(
        !a.is_flat(),
        "flat alone cannot retire an order that could execute on the next position"
    );
    broker
        .queue_recovery(Ok(OrderRecovery { submission_expired: true, ..Default::default() }))
        .await;
    clock.advance(RECOVERY_INTERVAL);
    a.recover_if_needed(&cx).unwrap();
    deliver_recovery(&mut a, &mut rx, &cx).await;
    assert!(a.is_flat());
    let pnl = a.inventory.realized_pnl;
    a.on_event(fill, &cx).await.unwrap();
    assert_eq!(a.inventory.realized_pnl, pnl);
    assert_eq!(broker.placed_count().await, 2, "no extra exit after flat");
    clock.advance(REENTRY_DELAY);
    a.config.impulse = None;
    a.on_event(book("0.9999", "1.0001", "2000", clock.now()), &cx).await.unwrap();
    a.on_event(reference("1", "1.0003", clock.now()), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(broker.placed_count().await, 3);
    assert!(!broker.last_placed_orders().await[0].reduce_only, "new entry is no longer stuck");
}

#[tokio::test]
async fn expiry_never_discards_an_unknown_entry_even_with_zero_local_inventory() {
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    a.place_entry(&cx, Side::Buy).unwrap();
    settle_submissions(&mut a, &cx).await;
    let entry_id = a.tracked[0].client_id.clone();
    a.apply_recovery(
        &entry_id,
        OrderRecovery { submission_expired: true, ..Default::default() },
        &cx,
    );
    assert_eq!(a.tracked.len(), 1);
    // Zero local inventory could still hide a lost entry fill; close remains allowed.
    a.place_exit(&cx, ExitReason::StopLoss).unwrap();
    settle_submissions(&mut a, &cx).await;
    let close_id = a.tracked.back().unwrap().client_id.clone();
    a.apply_recovery(
        &close_id,
        OrderRecovery { submission_expired: true, ..Default::default() },
        &cx,
    );
    assert_eq!(a.tracked.len(), 2);
    assert!(broker.last_placed_orders().await[0].reduce_only);
}

#[tokio::test(start_paused = true)]
async fn shutdown_resolves_expired_old_close_after_confirmed_flat_without_another_order() {
    let (broker, clock, cx) = context();
    let mut a = holding(&clock, Side::Buy);
    a.place_exit(&cx, ExitReason::Fair).unwrap();
    settle_submissions(&mut a, &cx).await;
    clock.advance(Duration::from_secs(2));
    a.place_exit(&cx, ExitReason::Fair).unwrap();
    settle_submissions(&mut a, &cx).await;
    let fill = confirmation(&a, OrderStatus::Filled, Some((Side::Sell, "1", "20")), clock.now());
    a.on_event(fill, &cx).await.unwrap();
    assert!(!a.is_flat());
    broker
        .queue_recovery(Ok(OrderRecovery { submission_expired: true, ..Default::default() }))
        .await;
    broker.set_positions(position(d("20"))).await;
    let started = tokio::time::Instant::now();
    a.wind_down(&WindDownReason::Signal, &cx).await.unwrap();
    assert!(a.is_flat());
    assert_eq!(broker.placed_count().await, 2);
    assert!(started.elapsed() < Duration::from_secs(1), "fills already confirm flat");
    assert!(!broker.history().await.iter().any(|c| c.method == "get_positions"));
}

#[tokio::test]
async fn shutdown_request_blocks_entry_but_not_protective_exit() {
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    a.config.impulse = None;
    a.binance = Some(reference("1.01", "1.01", clock.now()));
    cx.request_shutdown();
    a.evaluate(&cx, true).await.unwrap();
    assert_eq!(broker.placed_count().await, 0);
    let mut a = holding(&clock, Side::Buy);
    a.on_event(book("0.999", "0.9991", "20", clock.now()), &cx).await.unwrap();
    settle_submissions(&mut a, &cx).await;
    assert_eq!(broker.placed_count().await, 1);
    assert!(broker.last_placed_orders().await[0].reduce_only);
}

#[tokio::test(start_paused = true)]
async fn shutdown_accounts_queued_ws_fill_before_retry_without_rest() {
    let (broker, clock, cx) = context();
    let mut a = holding(&clock, Side::Buy);
    a.place_exit(&cx, ExitReason::StopLoss).unwrap();
    settle_submissions(&mut a, &cx).await;
    let fill =
        confirmation(&a, OrderStatus::Filled, Some((Side::Sell, "0.999", "20")), clock.now());
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tx.send(fill.clone()).unwrap();
    tx.send(fill).unwrap();
    a.retain_receiver(rx);
    clock.advance(Duration::from_secs(2)); // a retry would otherwise be eligible
    a.wind_down(&WindDownReason::Signal, &cx).await.unwrap();
    assert!(a.is_flat());
    assert_eq!(a.inventory.total_fills, 2);
    assert_eq!(a.inventory.realized_pnl, d("-0.02"));
    assert_eq!(broker.placed_count().await, 1);
    assert!(broker.history().await.iter().all(|c| c.method == "place_orders"));
}

#[tokio::test(start_paused = true)]
async fn shutdown_ws_partial_ioc_closes_only_residual_and_deduplicates_both_peers() {
    let (broker, clock, cx) = context();
    let mut a = holding(&clock, Side::Buy);
    a.client_ids = ClientIdIssuer::starting_at(500);
    a.place_exit(&cx, ExitReason::WindDown).unwrap();
    settle_submissions(&mut a, &cx).await;
    let mut partial =
        confirmation(&a, OrderStatus::Cancelled, Some((Side::Sell, "0.999", "7")), clock.now());
    partial.lifecycle.order.filled_quantity = d("7"); // partial IOC, rs=0
    partial.trade.as_mut().unwrap().trade_id = Some("close-first".into());
    let mut final_fill = partial.clone();
    final_fill.lifecycle.order.client_id = Some("501".into());
    final_fill.lifecycle.order.id = "2".into();
    final_fill.lifecycle.order.quantity = d("13");
    final_fill.lifecycle.order.filled_quantity = d("13");
    final_fill.lifecycle.order.status = OrderStatus::Filled;
    let trade = final_fill.trade.as_mut().unwrap();
    trade.client_id = Some("501".into());
    trade.order_id = "2".into();
    trade.quantity = d("13");
    trade.trade_id = Some("close-last".into());
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    a.retain_receiver(rx);
    let b = broker.clone();
    let producer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        tx.send(partial.clone()).unwrap();
        tx.send(partial).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let orders = b.last_placed_orders().await;
        assert_eq!(orders[0].quantity, d("13"));
        assert!(orders[0].reduce_only);
        tx.send(final_fill.clone()).unwrap();
        tx.send(final_fill).unwrap();
        tx.closed().await;
    });
    let start = tokio::time::Instant::now();
    a.wind_down(&WindDownReason::Signal, &cx).await.unwrap();
    producer.await.unwrap();
    assert!(start.elapsed() < Duration::from_millis(150));
    assert!(a.is_flat());
    assert_eq!(a.inventory.total_fills, 3);
    assert_eq!(a.inventory.realized_pnl, d("-0.02"));
    assert_eq!(broker.placed_count().await, 2);
    assert!(broker.history().await.iter().all(|c| c.method == "place_orders"));
}

#[tokio::test(start_paused = true)]
async fn shutdown_uses_rest_for_lost_ws_fill_or_closed_channel() {
    for closed in [false, true] {
        let (broker, clock, cx) = context();
        let mut a = holding(&clock, Side::Buy);
        a.place_exit(&cx, ExitReason::StopLoss).unwrap();
        settle_submissions(&mut a, &cx).await;
        let fill =
            confirmation(&a, OrderStatus::Filled, Some((Side::Sell, "1", "20")), clock.now());
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        if !closed {
            let mut missing = fill.clone();
            missing.trade = None;
            tx.send(missing).unwrap();
        }
        let keepalive = (!closed).then(|| tx.clone());
        drop(tx);
        a.retain_receiver(rx);
        broker
            .queue_recovery(Ok(OrderRecovery {
                order: Some(fill.lifecycle.order.clone()),
                trades: vec![fill.trade.clone().unwrap()],
                ..Default::default()
            }))
            .await;
        a.wind_down(&WindDownReason::Signal, &cx).await.unwrap();
        a.apply_execution(&fill, &cx).unwrap(); // delayed duplicate after REST
        assert!(a.is_flat());
        assert_eq!(a.inventory.total_fills, 2);
        assert_eq!(broker.placed_count().await, 1);
        assert_eq!(
            broker.history().await.iter().filter(|c| c.method == "recover_order").count(),
            1
        );
        drop(keepalive);
    }
}

#[tokio::test(start_paused = true)]
async fn shutdown_ws_fill_does_not_wait_for_slow_rest() {
    let (broker, clock, cx) = context();
    let mut a = holding(&clock, Side::Buy);
    a.place_exit(&cx, ExitReason::StopLoss).unwrap();
    settle_submissions(&mut a, &cx).await;
    a.tracked[0].reconcile_needed = true;
    broker.set_recovery_delay(Duration::from_secs(20)).await;
    let fill = confirmation(&a, OrderStatus::Filled, Some((Side::Sell, "1", "20")), clock.now());
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    a.retain_receiver(rx);
    let producer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        tx.send(fill).unwrap();
        tx.closed().await;
    });
    let started = tokio::time::Instant::now();
    a.wind_down(&WindDownReason::Signal, &cx).await.unwrap();
    producer.await.unwrap();
    assert!(started.elapsed() < Duration::from_millis(150));
    assert!(a.is_flat());
    assert!(a.recovery_task.is_none());
    assert_eq!(broker.placed_count().await, 1);
}

#[tokio::test(start_paused = true)]
async fn shutdown_drains_partial_entry_before_sizing_close() {
    let (broker, clock, cx) = context();
    let mut a = ready(&clock);
    a.client_ids = ClientIdIssuer::starting_at(500);
    a.place_entry(&cx, Side::Buy).unwrap();
    settle_submissions(&mut a, &cx).await;
    let mut entry =
        confirmation(&a, OrderStatus::Cancelled, Some((Side::Buy, "1", "7")), clock.now());
    entry.lifecycle.order.filled_quantity = d("7");
    let mut close = entry.clone();
    close.lifecycle.order.client_id = Some("501".into());
    close.lifecycle.order.status = OrderStatus::Filled;
    close.lifecycle.order.quantity = d("7");
    let trade = close.trade.as_mut().unwrap();
    trade.client_id = Some("501".into());
    trade.trade_id = Some("close".into());
    trade.side = Side::Sell;
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tx.send(entry.clone()).unwrap();
    tx.send(entry).unwrap();
    a.retain_receiver(rx);
    let b = broker.clone();
    let producer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let orders = b.last_placed_orders().await;
        assert!(orders[0].reduce_only);
        assert_eq!(orders[0].quantity, d("7"));
        tx.send(close).unwrap();
        tx.closed().await;
    });
    a.wind_down(&WindDownReason::Signal, &cx).await.unwrap();
    producer.await.unwrap();
    assert!(a.is_flat());
    assert_eq!(a.inventory.total_fills, 2);
    assert_eq!(broker.placed_count().await, 2);
    assert!(broker.history().await.iter().all(|c| c.method == "place_orders"));
}
