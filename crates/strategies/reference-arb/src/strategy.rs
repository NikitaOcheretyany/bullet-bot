//! Binance → Bullet lead–lag actor. Prices/quantities stay Decimal; only
//! dimensionless log ratios, EMA weights and bp signals use f64.
//! Unknown orders block new entries, never protective reduce-only exits.
//! All unresolved IOCs retain their fill accounting. No entry top-ups.
//! Only Trade values change inventory.

use std::collections::{HashSet, VecDeque};
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::task::Poll;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bb_core::broker::OrderRecovery;
use bb_core::error::BotError;
use bb_core::events::{BookUpdate, ExecutionUpdate, OrderLifecycle, Tick, Trade};
use bb_core::harness::{Actor, ActorContext, EventHandler, WindDownReason};
use bb_core::helpers::{ClientIdIssuer, InventoryTracker};
use bb_core::types::{NewOrder, Order, OrderBook, OrderResult, OrderStatus, OrderType, Side};
use bb_exchange_binance::ReferencePriceUpdate;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use crate::config::{ExitConfig, ReferenceArbConfig};

/// Completion notification for the existing background order tasks.
/// Results remain in their handles so shutdown can consume them after feeds stop.
#[derive(Debug, Clone)]
pub enum OrderTaskReady {
    Submission(String),
    Recovery(String),
}

const BPS: f64 = 10_000.0;
const RECOVERY_INTERVAL: Duration = Duration::from_secs(1);
const REENTRY_DELAY: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy)]
pub enum ExitReason {
    Fair,
    Trailing,
    StopLoss,
    Timeout,
    Drawdown,
    WindDown,
}

#[derive(Debug, Clone, Copy)]
struct Entry {
    side: Side,
    basis: f64,
    /// First confirmed fill, not order submission or the most recent signal.
    filled_at: Option<Instant>,
    /// Best executable closing quote since the first confirmed entry fill.
    best_close: Option<Decimal>,
}

#[derive(Debug, Clone, Copy)]
struct Reentry {
    reset: bool,
    confirmed_at: Option<Instant>,
}
impl Reentry {
    fn allowed(self, now: Instant) -> bool {
        self.reset
            || self.confirmed_at.is_some_and(|t| now.saturating_duration_since(t) >= REENTRY_DELAY)
    }
}

/// Evidence for each pending IOC, cleared on completion. REST and WS share
/// fill IDs so a recovered execution cannot be counted twice.
struct TrackedOrder {
    client_id: String,
    reduce_only: bool,
    order_id: Option<String>,
    submitted_at: Instant,
    submitted_ms: u64,
    quantity: Decimal,
    filled: Decimal,
    trade_ids: HashSet<String>,
    /// Observed interruption/missing fills, not merely a slow acknowledgement.
    stream_gap: bool,
    reconcile_needed: bool,
    submission: Option<tokio::task::JoinHandle<Result<Vec<OrderResult>, BotError>>>,
}

pub struct ReferenceArbActor {
    config: ReferenceArbConfig,
    quantity_step: Decimal,
    inventory: InventoryTracker,
    peak_pnl: Decimal,
    drawdown_triggered: bool,
    client_ids: ClientIdIssuer,
    bullet_book: Option<OrderBook>,
    bullet_last_seen: Option<Instant>,
    binance: Option<ReferencePriceUpdate>,
    history: VecDeque<(Instant, Decimal)>,
    basis: Option<f64>,
    basis_at: Option<Instant>,
    entry: Option<Entry>,
    exit_requested: Option<ExitReason>,
    reentry: [Reentry; 2],
    last_status_log: Option<Instant>,
    tracked: VecDeque<TrackedOrder>,
    last_reconcile: Option<Instant>,
    retry_exit_at: Option<Instant>,
    task_tx: tokio::sync::mpsc::UnboundedSender<OrderTaskReady>,
    execution_rx: Option<tokio::sync::mpsc::UnboundedReceiver<ExecutionUpdate>>,
    recovery_task: Option<(String, tokio::task::JoinHandle<Result<OrderRecovery, BotError>>)>,
}

fn side_index(side: Side) -> usize {
    usize::from(side == Side::Sell)
}
fn sign(side: Side) -> f64 {
    if side == Side::Buy { 1.0 } else { -1.0 }
}
fn log_ratio(a: Decimal, b: Decimal) -> Option<f64> {
    if a <= Decimal::ZERO || b <= Decimal::ZERO {
        return None;
    }
    let value = (a / b).to_f64()?.ln();
    value.is_finite().then_some(value)
}
fn valid_book(book: &OrderBook) -> bool {
    match (book.best_bid(), book.best_ask()) {
        (Some(b), Some(a)) => {
            b.price > Decimal::ZERO
                && a.price >= b.price
                && b.quantity > Decimal::ZERO
                && a.quantity > Decimal::ZERO
        }
        _ => false,
    }
}

impl ReferenceArbActor {
    pub fn new(
        config: ReferenceArbConfig,
        quantity_step: Decimal,
        task_tx: tokio::sync::mpsc::UnboundedSender<OrderTaskReady>,
    ) -> Self {
        Self::with_client_ids(config, quantity_step, ClientIdIssuer::session_seeded(), task_tx)
    }
    pub fn with_client_ids(
        config: ReferenceArbConfig,
        quantity_step: Decimal,
        client_ids: ClientIdIssuer,
        task_tx: tokio::sync::mpsc::UnboundedSender<OrderTaskReady>,
    ) -> Self {
        Self {
            config,
            quantity_step,
            client_ids,
            inventory: InventoryTracker::new(),
            peak_pnl: Decimal::ZERO,
            drawdown_triggered: false,
            bullet_book: None,
            bullet_last_seen: None,
            binance: None,
            history: VecDeque::new(),
            basis: None,
            basis_at: None,
            entry: None,
            exit_requested: None,
            reentry: [Reentry { reset: true, confirmed_at: None }; 2],
            last_status_log: None,
            tracked: VecDeque::new(),
            last_reconcile: None,
            retry_exit_at: None,
            task_tx,
            execution_rx: None,
            recovery_task: None,
        }
    }
    fn is_flat(&self) -> bool {
        self.inventory.is_flat() && self.tracked.is_empty()
    }
    fn state(&self) -> &'static str {
        if self.drawdown_triggered {
            "stopping"
        } else if self.is_flat() {
            "flat"
        } else if self.exit_requested.is_some() || self.tracked.iter().any(|t| t.reduce_only) {
            "exiting"
        } else if !self.tracked.is_empty() {
            "entering"
        } else {
            "holding"
        }
    }
    fn bullet_mid(&self) -> Option<Decimal> {
        self.bullet_book.as_ref()?.midpoint()
    }
    fn touch_price(&self, side: Side) -> Option<Decimal> {
        let book = self.bullet_book.as_ref()?;
        match side {
            Side::Buy => book.best_ask(),
            Side::Sell => book.best_bid(),
        }
        .map(|x| x.price)
    }
    fn quotes_ready(&self, cx: &ActorContext) -> Result<bool, BotError> {
        let broker = cx.broker(&self.config.exchange)?;
        let reconnect_at = broker.last_reconnect_at();
        Ok(!broker.is_disconnected()
            && self
                .bullet_last_seen
                .is_some_and(|at| reconnect_at.is_none_or(|cutoff| at >= cutoff))
            && self.binance.as_ref().is_some_and(|r| {
                cx.clock().now().saturating_duration_since(r.received_at)
                    <= Duration::from_millis(self.config.reference_stale_ms)
            }))
    }
    fn update_basis(&mut self, cx: &ActorContext) -> Result<(), BotError> {
        if !self.quotes_ready(cx)? {
            return Ok(());
        }
        let now = cx.clock().now();
        let Some(observed) =
            self.binance.as_ref().and_then(|r| log_ratio(self.bullet_mid()?, r.midpoint))
        else {
            return Ok(());
        };
        self.basis = Some(match (self.basis, self.basis_at) {
            (Some(previous), Some(at)) => {
                let elapsed = now.saturating_duration_since(at).as_secs_f64();
                let alpha = -(-std::f64::consts::LN_2 * elapsed / self.config.basis_half_life_secs)
                    .exp_m1();
                previous + alpha * (observed - previous)
            }
            _ => observed,
        });
        self.basis_at = Some(now);
        Ok(())
    }
    fn remaining(&self, side: Side, basis: f64) -> Option<f64> {
        Some(
            sign(side) * BPS * (log_ratio(self.binance.as_ref()?.mid, self.bullet_mid()?)? + basis),
        )
    }
    fn edge(&self, side: Side) -> Option<f64> {
        Some(
            sign(side)
                * BPS
                * (log_ratio(self.binance.as_ref()?.mid, self.touch_price(side)?)? + self.basis?),
        )
    }
    fn fair_price(&self, basis: f64) -> Option<Decimal> {
        self.binance.as_ref()?.mid.checked_mul(Decimal::from_f64_retain(basis.exp())?)
    }
    fn update_trailing(&mut self) {
        if !matches!(self.config.exit, ExitConfig::Trailing { .. }) {
            return;
        }
        let Some(entry) = self.entry.filter(|e| e.filled_at.is_some()) else { return };
        let Some(price) = self.touch_price(entry.side.opposite()) else { return };
        if let Some(entry) = &mut self.entry {
            entry.best_close = Some(entry.best_close.map_or(price, |best| match entry.side {
                Side::Buy => best.max(price),
                Side::Sell => best.min(price),
            }));
        }
    }
    fn trailing_drawdown_bps(&self) -> Option<f64> {
        let entry = self.entry?;
        Some(
            sign(entry.side)
                * BPS
                * log_ratio(entry.best_close?, self.touch_price(entry.side.opposite())?)?,
        )
    }
    fn prune_history(&mut self, now: Instant) {
        let Some(impulse) = &self.config.impulse else {
            self.history.clear();
            return;
        };
        let Some(cutoff) = now.checked_sub(Duration::from_millis(impulse.window_ms)) else {
            return;
        };
        // Retain the last quote at/before the cutoff, not the nearest future quote.
        while self.history.len() > 1 && self.history[1].0 <= cutoff {
            self.history.pop_front();
        }
    }
    fn impulse(&self, side: Side, now: Instant) -> Option<f64> {
        let config = self.config.impulse.as_ref()?;
        let cutoff = now.checked_sub(Duration::from_millis(config.window_ms))?;
        let (_, past) = self.history.iter().rev().find(|(at, _)| *at <= cutoff)?;
        Some(sign(side) * BPS * log_ratio(self.binance.as_ref()?.midpoint, *past)?)
    }
    fn close_pnl_bps(&self) -> Option<f64> {
        let entry = self.entry?;
        let price = self.touch_price(entry.side.opposite())?;
        let basis = self.inventory.avg_entry_price;
        if basis <= Decimal::ZERO {
            return None;
        }
        Some(sign(entry.side) * BPS * ((price - basis) / basis).to_f64()?)
    }
    /// Called only after a new fill. Initial balance cancels out of peak - `PnL`.
    fn check_drawdown(&mut self, cx: &ActorContext) {
        if self.drawdown_triggered {
            return;
        }
        let Some(limit) = self.config.max_drawdown_usd else {
            return;
        };
        let pnl = self.inventory.realized_pnl;
        self.peak_pnl = self.peak_pnl.max(pnl);
        let drawdown = self.peak_pnl - pnl;
        if drawdown >= limit {
            self.drawdown_triggered = true;
            self.exit_requested = Some(ExitReason::Drawdown);
            tracing::warn!(%pnl, peak_pnl = %self.peak_pnl, %drawdown, limit = %limit,
                "PnL drawdown limit reached; closing and stopping this run");
            cx.request_shutdown();
        }
    }
    fn exit_reason(&self, now: Instant) -> Option<ExitReason> {
        if let Some(reason) = self.exit_requested {
            return Some(reason);
        }
        let entry = self.entry?;
        // Quote age blocks entries only: cached prices and the hold deadline
        // remain usable for closing an existing position.
        match self.config.exit {
            ExitConfig::Fair { remaining_bps }
                if self
                    .remaining(entry.side, entry.basis)
                    .is_some_and(|remaining| remaining <= remaining_bps) =>
            {
                return Some(ExitReason::Fair);
            }
            ExitConfig::Trailing { drawdown_bps }
                if self
                    .trailing_drawdown_bps()
                    .is_some_and(|drawdown| drawdown >= drawdown_bps) =>
            {
                return Some(ExitReason::Trailing);
            }
            _ => {}
        }
        if self.close_pnl_bps().is_some_and(|pnl| pnl <= -self.config.stop_loss_bps) {
            Some(ExitReason::StopLoss)
        } else if entry
            .filled_at
            .or_else(|| {
                // If the entry fill is invisible, its submission time still bounds
                // the protective deadline. Normal H starts at the first known fill.
                self.tracked.iter().find(|t| !t.reduce_only).map(|t| t.submitted_at)
            })
            .is_some_and(|at| {
                now.saturating_duration_since(at) >= Duration::from_secs(self.config.max_hold_secs)
            })
        {
            Some(ExitReason::Timeout)
        } else {
            None
        }
    }
    fn observe_execution_gap(&mut self, cx: &ActorContext) -> Result<(), BotError> {
        if !self.config.dry_run && cx.broker(&self.config.exchange)?.take_reconcile_signal() {
            for tracked in &mut self.tracked {
                tracked.stream_gap = true;
                tracked.reconcile_needed = true;
            }
        }
        Ok(())
    }
    async fn evaluate(&mut self, cx: &ActorContext, allow_entry: bool) -> Result<(), BotError> {
        let allow_entry = allow_entry && !cx.is_shutting_down();
        // A quote can overtake the completion notification in the actor queue.
        // Consume all ready results before sizing a close; never wait for an
        // outstanding request. Its reduce-only protection must remain immediate.
        loop {
            tokio::select! {
                biased;
                () = self.finish_task(None, cx) => {}
                () = std::future::ready(()) => break,
            }
        }
        self.observe_execution_gap(cx)?;
        let now = cx.clock().now();
        self.prune_history(now);
        self.update_trailing();
        // An unresolved order never suppresses a protective exit. Preserve the
        // decision even through temporary flat inventory and late entry fills.
        if let Some(reason) = self.exit_reason(now) {
            self.exit_requested = Some(reason);
        }
        let quotes_ready = self.quotes_ready(cx)?;
        if quotes_ready {
            for side in [Side::Buy, Side::Sell] {
                if self.edge(side).is_some_and(|s| s <= self.config.entry_threshold_bps) {
                    self.reentry[side_index(side)].reset = true;
                }
            }
        }
        if let Some(reason) = self.exit_requested {
            self.place_exit(cx, reason)?;
        } else if !self.drawdown_triggered && self.is_flat() && allow_entry && quotes_ready {
            for side in [Side::Buy, Side::Sell] {
                if self.edge(side).is_some_and(|s| s > self.config.entry_threshold_bps)
                    && self.reentry[side_index(side)].allowed(now)
                    && self.config.impulse.as_ref().is_none_or(|i| {
                        self.impulse(side, now).is_some_and(|x| x > i.threshold_bps)
                    })
                {
                    self.place_entry(cx, side)?;
                    break;
                }
            }
        }
        // Physical replacement needs no unresolved orders, not flat inventory.
        // Route selection is independent and never asks the strategy to wait.
        // Decide/submit protective exits BEFORE this local maintenance command.
        if allow_entry
            && !self.config.dry_run
            && !self.drawdown_triggered
            && self.tracked.is_empty()
        {
            cx.broker(&self.config.exchange)?.refresh_connection_if_idle().await;
        }
        Ok(())
    }
    fn entry_order(&mut self, side: Side) -> Option<NewOrder> {
        let touch = self.touch_price(side)?;
        let price = self.fair_price(self.basis?)?.checked_mul(Decimal::from_f64_retain(
            (-sign(side) * self.config.entry_reserve_bps / BPS).exp(),
        )?)?;
        if price <= Decimal::ZERO {
            return None;
        }
        // Budget against the larger of touch/limit; reserve buy slippage up front.
        let quantity = (self.config.order_notional / touch.max(price) / self.quantity_step).floor()
            * self.quantity_step;
        if quantity <= Decimal::ZERO {
            return None;
        }
        Some(NewOrder {
            symbol: self.config.symbol.clone(),
            side,
            // Existing Bullet adapter maps Market to a priced IOC, never GTC.
            order_type: OrderType::Market,
            price,
            quantity,
            client_id: Some(self.client_ids.issue()),
            reduce_only: false,
        })
    }
    fn place_entry(&mut self, cx: &ActorContext, side: Side) -> Result<(), BotError> {
        let (Some(basis), Some(order)) = (self.basis, self.entry_order(side)) else {
            return Ok(());
        };
        tracing::info!(side = %side, edge_bps = ?self.edge(side), impulse_bps = ?self.impulse(side, cx.clock().now()),
            basis_entry = basis, fair_signal = ?self.fair_price(basis), price = %order.price, qty = %order.quantity,
            client_id = order.client_id.as_deref().unwrap_or_default(), reduce_only = order.reduce_only,
            dry_run = self.config.dry_run,
            "Entry signal: LIMIT IOC");
        self.entry = Some(Entry { side, basis, filled_at: None, best_close: None });
        self.reentry[side_index(side)] = Reentry { reset: false, confirmed_at: None };
        self.submit(cx, order)
    }
    fn place_exit(&mut self, cx: &ActorContext, reason: ExitReason) -> Result<(), BotError> {
        let now = cx.clock().now();
        if self.retry_exit_at.is_some_and(|at| now < at)
            || self.tracked.iter().any(|t| {
                t.reduce_only && now.saturating_duration_since(t.submitted_at) < RECOVERY_INTERVAL
            })
        {
            return Ok(());
        }
        let Some(entry) = self.entry else { return Ok(()) };
        // Bound possible exposure by the unconfirmed part of our sole entry.
        // Reduce-only on the venue decides how much can actually close, even
        // when fills arrive late and local inventory is zero or looks reversed.
        let known = match entry.side {
            Side::Buy => self.inventory.net_position,
            Side::Sell => -self.inventory.net_position,
        };
        let unconfirmed = self
            .tracked
            .iter()
            .find(|t| !t.reduce_only)
            .map_or(Decimal::ZERO, |t| (t.quantity - t.filled).max(Decimal::ZERO));
        let quantity = known + unconfirmed;
        if quantity <= Decimal::ZERO {
            return Ok(());
        }
        self.exit_requested = Some(reason);
        let side = entry.side.opposite();
        if self.config.dry_run {
            let available = self.bullet_book.as_ref().and_then(|book| match side {
                Side::Buy => book.best_ask(),
                Side::Sell => book.best_bid(),
            });
            // Paper fills consume this quote's visible liquidity. Wait for a
            // new BBO rather than repeatedly filling against the same quantity.
            if available.is_none_or(|level| level.quantity < self.quantity_step) {
                return Ok(());
            }
        }
        let Some(touch) = self.touch_price(side) else {
            return Ok(());
        };
        if unconfirmed > Decimal::ZERO {
            // An empty close must not spin on every cancel while the entry is
            // still unknown. Reuse the existing close-retry interval.
            self.retry_exit_at = Some(now + RECOVERY_INTERVAL);
        }
        let slip = self.config.market_slippage_bps / Decimal::from(10_000);
        let price = touch
            * match side {
                Side::Buy => Decimal::ONE + slip,
                Side::Sell => Decimal::ONE - slip,
            };
        let client_id = self.client_ids.issue();
        let order = NewOrder {
            symbol: self.config.symbol.clone(),
            side,
            order_type: OrderType::Market,
            price,
            quantity,
            client_id: Some(client_id.clone()),
            reduce_only: true,
        };
        tracing::info!(?reason, side = %side, qty = %order.quantity, price = %price, client_id, reduce_only = order.reduce_only,
            remaining_bps = ?self.entry.and_then(|e| self.remaining(e.side, e.basis)), trailing_drawdown_bps = ?self.trailing_drawdown_bps(), close_pnl_bps = ?self.close_pnl_bps(), dry_run = self.config.dry_run,
            "Exit signal: MARKET (protected IOC)");
        self.submit(cx, order)
    }
    fn submit(&mut self, cx: &ActorContext, order: NewOrder) -> Result<(), BotError> {
        let client_id = order.client_id.clone().unwrap_or_default();
        let reduce_only = order.reduce_only;
        self.tracked.push_back(TrackedOrder {
            client_id: client_id.clone(),
            reduce_only,
            order_id: None,
            submitted_at: cx.clock().now(),
            submitted_ms: cx.clock().unix_ms(),
            quantity: order.quantity,
            filled: Decimal::ZERO,
            trade_ids: HashSet::new(),
            stream_gap: false,
            reconcile_needed: false,
            submission: None,
        });
        if self.config.dry_run {
            let update = self.paper_execution(&order, cx.clock().now());
            self.apply_execution(&update, cx)?;
            tracing::info!(tag = "PAPER", side = %order.side,
                filled = %update.lifecycle.order.filled_quantity,
                cancelled = %(order.quantity - update.lifecycle.order.filled_quantity),
                net_pos = %self.inventory.net_position, realized_pnl = %self.inventory.realized_pnl,
                "PAPER IOC completed using visible BBO liquidity");
        } else {
            let broker = cx.broker(&self.config.exchange)?;
            let task_tx = self.task_tx.clone();
            let task = tokio::spawn(async move {
                let result = broker.place_orders(&[order]).await;
                let _ = task_tx.send(OrderTaskReady::Submission(client_id));
                result
            });
            let index = self.tracked.len() - 1;
            self.tracked[index].submission = Some(task);
        }
        Ok(())
    }
    fn apply_submission(
        &mut self,
        index: usize,
        result: Result<Result<Vec<OrderResult>, BotError>, tokio::task::JoinError>,
        cx: &ActorContext,
    ) {
        let tracked = &mut self.tracked[index];
        tracked.submission = None;
        match result {
            Ok(Ok(results)) if results.first().is_some_and(|r| r.success) => {
                // The private stream may have supplied the ID before the RPC acknowledgement.
                if let Some(id) = &results[0].order_id {
                    tracked.order_id = Some(id.clone());
                }
            }
            Ok(Ok(results)) if results.first().is_some_and(|r| !r.success) => {
                let error = results[0].error.as_deref().unwrap_or_default();
                // Only this explicit venue reason identifies an ordinary unfilled IOC.
                if !tracked.reduce_only
                    && tracked.filled.is_zero()
                    && error.contains("IOC order received no fill")
                {
                    tracing::info!(client_id = %tracked.client_id, order_id = ?tracked.order_id,
                        reduce_only = tracked.reduce_only, %error, "Entry IOC received no fill");
                } else {
                    tracing::warn!(client_id = %tracked.client_id, order_id = ?tracked.order_id,
                        reduce_only = tracked.reduce_only, %error, "Order rejected; no execution to wait for");
                }
                self.rejected(index, cx.clock().now());
            }
            Ok(Err(BotError::Config(e) | BotError::Strategy(e))) => {
                tracing::warn!(client_id = %tracked.client_id, order_id = ?tracked.order_id,
                    reduce_only = tracked.reduce_only, error = %e, "Order failed before submission");
                self.rejected(index, cx.clock().now());
            }
            other => {
                tracing::warn!(result = ?other, client_id = %tracked.client_id, order_id = ?tracked.order_id,
                    reduce_only = tracked.reduce_only,
                    "Order outcome unknown; new entries wait, reduce-only exits remain allowed");
                tracked.reconcile_needed = true;
            }
        }
    }
    fn rejected(&mut self, index: usize, now: Instant) {
        let reduce_only = self.tracked[index].reduce_only;
        self.complete_order(index, now);
        if reduce_only {
            self.retry_exit_at = Some(now + RECOVERY_INTERVAL);
        }
    }
    fn complete_order(&mut self, index: usize, now: Instant) {
        let Some(completed) = self.tracked.remove(index) else { return };
        tracing::info!(client_id = %completed.client_id, order_id = ?completed.order_id,
            reduce_only = completed.reduce_only, filled = %completed.filled,
            net_pos = %self.inventory.net_position, "Order tracking completed");
        if let Some(task) = completed.submission {
            task.abort();
        }
        if !completed.reduce_only {
            // The entry's actual size is now known; its late residual may close
            // immediately instead of waiting on the unknown-size retry delay.
            self.retry_exit_at = None;
        }
        if self.recovery_task.as_ref().is_some_and(|(id, _)| *id == completed.client_id) {
            self.stop_recovery();
        }
        if self.tracked.is_empty() {
            self.last_reconcile = None;
            if self.inventory.is_flat() {
                self.finish_attempt(now);
            }
        }
    }
    fn apply_trade(
        &mut self,
        index: usize,
        trade: &Trade,
        received_at: Instant,
        cx: &ActorContext,
    ) {
        let tracked = &mut self.tracked[index];
        let Some(id) = &trade.trade_id else {
            tracked.stream_gap = true;
            tracked.reconcile_needed = true;
            return;
        };
        if !tracked.trade_ids.insert(id.clone()) {
            return;
        }
        tracked.order_id = Some(trade.order_id.clone());
        tracked.filled += trade.quantity;
        let calculated = self.inventory.record_fill(trade.side, trade.price, trade.quantity);
        // Venue PnL remains correct even if an entry's fill arrives after its
        // reducing close. Paper fills use the existing inventory calculation.
        if let Some(realized) = trade.realized_pnl {
            self.inventory.realized_pnl += realized - calculated;
        }
        self.inventory.realized_pnl -= trade.fee;
        if !tracked.reduce_only
            && let Some(entry) = &mut self.entry
        {
            let at = received_at.max(tracked.submitted_at).min(cx.clock().now());
            entry.filled_at = Some(entry.filled_at.map_or(at, |old| old.min(at)));
        }
        tracing::info!(side = %trade.side, price = %trade.price, quantity = %trade.quantity,
            trade_id = %id, client_id = %tracked.client_id, order_id = %trade.order_id,
            reduce_only = tracked.reduce_only, exchange_time_ms = ?trade.timestamp,
            fee = %trade.fee, net_pos = %self.inventory.net_position, "Confirmed fill");
        self.update_trailing();
        self.check_drawdown(cx);
    }
    fn stop_recovery(&mut self) {
        if let Some((_, task)) = self.recovery_task.take() {
            task.abort();
        }
    }
    fn recovering(&self) -> bool {
        self.tracked.iter().any(|t| t.reconcile_needed)
    }
    fn apply_recovery(&mut self, client_id: &str, recovered: OrderRecovery, cx: &ActorContext) {
        let Some(index) = self.tracked.iter().position(|t| t.client_id == client_id) else {
            return; // A WS confirmation already completed this order.
        };
        for trade in &recovered.trades {
            let at = trade
                .timestamp
                .and_then(|ms| {
                    cx.clock()
                        .now()
                        .checked_sub(Duration::from_millis(cx.clock().unix_ms().saturating_sub(ms)))
                })
                .unwrap_or(cx.clock().now());
            self.apply_trade(index, trade, at, cx);
        }
        if let Some(order) = recovered.order {
            let tracked = &mut self.tracked[index];
            tracked.order_id = Some(order.id);
            tracked.stream_gap |= order.filled_quantity > tracked.filled;
            if matches!(
                order.status,
                OrderStatus::Filled | OrderStatus::Cancelled | OrderStatus::Rejected
            ) && tracked.filled == order.filled_quantity
                // REST has returned Cancelled/0 before the closing fills were
                // indexed. Keep that exit while exposure is known; reduce-only
                // retries can still close a genuinely empty IOC.
                && (tracked.filled > Decimal::ZERO
                    || !tracked.reduce_only
                    || self.inventory.is_flat())
            {
                self.complete_order(index, cx.clock().now());
                tracing::info!(client_id, status = ?order.status, filled = %order.filled_quantity,
                    net_pos = %self.inventory.net_position, "Order recovery confirmed");
                return;
            }
        }
        // A later protective close can finish the entire known position while
        // an older RPC has no answer. Release it only when it cannot execute
        // again AND all entry fills are known. Expiry alone says nothing about
        // past fills, so unresolved entries and non-flat inventory still recover.
        if recovered.submission_expired
            && self.inventory.is_flat()
            && self.tracked.iter().all(|t| t.reduce_only)
        {
            tracing::info!(
                client_id,
                "Retiring expired protective exit after confirmed flat fills"
            );
            self.complete_order(index, cx.clock().now());
        }
    }
    async fn finish_task(&mut self, only: Option<&OrderTaskReady>, cx: &ActorContext) {
        // Poll by reference so a shutdown retry wake-up cannot discard a result.
        // A notification selects one task; evaluation drains any ready task.
        poll_fn(|task_cx| {
            if let Some((client_id, task)) = &mut self.recovery_task
                && only.is_none_or(|e| matches!(e, OrderTaskReady::Recovery(id) if id == client_id))
                && let Poll::Ready(result) = Pin::new(task).poll(task_cx)
            {
                let client_id = client_id.clone();
                self.recovery_task = None;
                match result {
                    Ok(Ok(recovered)) => self.apply_recovery(&client_id, recovered, cx),
                    Ok(Err(e)) => {
                        tracing::warn!(client_id, error = %e, "Recovery incomplete; will retry");
                    }
                    Err(e) => {
                        tracing::warn!(client_id, error = %e, "Recovery task failed; will retry");
                    }
                }
                return Poll::Ready(());
            }
            for (index, tracked) in self.tracked.iter_mut().enumerate() {
                if only.is_none_or(
                    |e| matches!(e, OrderTaskReady::Submission(id) if *id == tracked.client_id),
                ) && let Some(task) = &mut tracked.submission
                    && let Poll::Ready(result) = Pin::new(task).poll(task_cx)
                {
                    self.apply_submission(index, result, cx);
                    return Poll::Ready(());
                }
            }
            Poll::Pending
        })
        .await;
    }
    fn recover_if_needed(&mut self, cx: &ActorContext) -> Result<(), BotError> {
        if self.config.dry_run {
            return Ok(());
        }
        self.observe_execution_gap(cx)?;
        let now = cx.clock().now();
        for tracked in &mut self.tracked {
            tracked.reconcile_needed |=
                now.saturating_duration_since(tracked.submitted_at) >= RECOVERY_INTERVAL;
        }
        if self.recovery_task.is_some()
            || self
                .last_reconcile
                .is_some_and(|at| now.saturating_duration_since(at) < RECOVERY_INTERVAL)
        {
            return Ok(());
        }
        let Some(index) = self.tracked.iter().position(|t| t.reconcile_needed) else {
            return Ok(());
        };
        // Reuse one read task, rotating unresolved orders so one missing result
        // cannot starve recovery of the protective close (or vice versa).
        self.tracked.rotate_left(index);
        let tracked = &self.tracked[0];
        let (client_id, order_id, submitted_ms) =
            (tracked.client_id.clone(), tracked.order_id.clone(), tracked.submitted_ms);
        self.tracked.rotate_left(1);
        self.last_reconcile = Some(now);
        let broker = cx.broker(&self.config.exchange)?;
        let symbol = self.config.symbol.clone();
        let task_id = client_id.clone();
        let task_tx = self.task_tx.clone();
        let task = tokio::spawn(async move {
            let result = tokio::time::timeout(
                Duration::from_secs(5),
                broker.recover_order(&symbol, &task_id, order_id.as_deref(), submitted_ms),
            )
            .await
            .unwrap_or_else(|_| Err(BotError::exchange("Order recovery timed out", true)));
            // During shutdown the feed is closed; wind_down awaits the handle.
            let _ = task_tx.send(OrderTaskReady::Recovery(task_id));
            result
        });
        self.recovery_task = Some((client_id, task));
        Ok(())
    }
    fn apply_execution(
        &mut self,
        event: &ExecutionUpdate,
        cx: &ActorContext,
    ) -> Result<bool, BotError> {
        let order = &event.lifecycle.order;
        if event.lifecycle.exchange != self.config.exchange || order.symbol != self.config.symbol {
            return Ok(false);
        }
        let Some(index) =
            self.tracked.iter().position(|t| order.client_id.as_deref() == Some(&t.client_id))
        else {
            return Ok(false);
        };
        // Consume reconnect/decoder evidence before accepting a terminal status.
        self.observe_execution_gap(cx)?;
        self.tracked[index].order_id = Some(order.id.clone());
        if let Some(trade) = &event.trade {
            self.apply_trade(index, trade, event.received_at, cx);
        }
        let tracked = &mut self.tracked[index];
        if order.filled_quantity > tracked.filled {
            tracked.stream_gap = true;
            tracked.reconcile_needed = true;
        }
        let complete = match order.status {
            OrderStatus::Filled => {
                tracked.filled > Decimal::ZERO
                    && tracked.filled == order.quantity.max(order.filled_quantity)
            }
            // A partial IOC with rs=0 supplies its cumulative size. Otherwise
            // cancellation needs a stream covering this order from submission.
            // A zero count on a plain cancel means "not supplied", not flat.
            OrderStatus::Cancelled | OrderStatus::Rejected => {
                (order.filled_quantity > Decimal::ZERO && order.filled_quantity == tracked.filled)
                    || (!tracked.stream_gap
                        && event.stream_started_at.is_none_or(|at| at <= tracked.submitted_at))
            }
            _ => false,
        };
        if complete {
            self.complete_order(index, event.received_at);
        } else if matches!(
            order.status,
            OrderStatus::Filled | OrderStatus::Cancelled | OrderStatus::Rejected
        ) && (tracked.stream_gap
            || event.stream_started_at.is_none_or(|at| at <= tracked.submitted_at))
        {
            tracked.reconcile_needed = true;
        }
        Ok(true)
    }
    fn paper_execution(&mut self, order: &NewOrder, received_at: Instant) -> ExecutionUpdate {
        let level = self.bullet_book.as_mut().and_then(|book| match order.side {
            Side::Buy => book.asks.first_entry(),
            Side::Sell => book.bids.last_entry(),
        });
        let (price, filled) = level.map_or((order.price, Decimal::ZERO), |mut level| {
            let price = *level.key();
            let crosses = match order.side {
                Side::Buy => price <= order.price,
                Side::Sell => price >= order.price,
            };
            let filled = if crosses {
                (order.quantity.min(*level.get()) / self.quantity_step).floor() * self.quantity_step
            } else {
                Decimal::ZERO
            };
            *level.get_mut() -= filled;
            (price, filled)
        });
        let id = format!("paper-{}", order.client_id.as_deref().unwrap_or_default());
        ExecutionUpdate {
            received_at,
            stream_started_at: None,
            trade: (filled > Decimal::ZERO).then(|| Trade {
                exchange: self.config.exchange.clone(),
                symbol: order.symbol.clone(),
                order_id: id.clone(),
                trade_id: Some(id.clone()),
                client_id: order.client_id.clone(),
                side: order.side,
                price,
                quantity: filled,
                fee: Decimal::ZERO,
                realized_pnl: None,
                timestamp: None,
            }),
            lifecycle: OrderLifecycle {
                exchange: self.config.exchange.clone(),
                order: Order {
                    id,
                    client_id: order.client_id.clone(),
                    symbol: order.symbol.clone(),
                    side: order.side,
                    order_type: order.order_type,
                    price: order.price,
                    quantity: order.quantity,
                    filled_quantity: filled,
                    status: if filled == order.quantity {
                        OrderStatus::Filled
                    } else {
                        OrderStatus::Cancelled
                    },
                },
            },
        }
    }
    fn finish_attempt(&mut self, now: Instant) {
        if let Some(entry) = self.entry.take() {
            self.reentry[side_index(entry.side)].confirmed_at = Some(now);
        }
        self.exit_requested = None;
        self.retry_exit_at = None;
    }
}

impl Drop for ReferenceArbActor {
    fn drop(&mut self) {
        self.stop_recovery();
        for tracked in &self.tracked {
            if let Some(task) = &tracked.submission {
                task.abort();
            }
        }
    }
}

#[async_trait]
impl Actor for ReferenceArbActor {
    async fn init(&mut self, cx: &ActorContext) -> Result<(), BotError> {
        self.config.validate().map_err(BotError::config)?;
        if !self.config.dry_run {
            let broker = cx.broker(&self.config.exchange)?;
            broker.cancel_all_orders(&self.config.symbol).await.ok();
            for p in broker.get_positions().await?.iter().filter(|p| p.symbol == self.config.symbol)
            {
                if !p.size.is_zero() {
                    return Err(BotError::config(
                        "reference-arb refuses to start with non-flat position",
                    ));
                }
            }
        }
        tracing::info!(config = ?self.config, "lead-lag initialized");
        Ok(())
    }
    async fn wind_down(
        &mut self,
        _reason: &WindDownReason,
        cx: &ActorContext,
    ) -> Result<(), BotError> {
        let exit_reason =
            if self.drawdown_triggered { ExitReason::Drawdown } else { ExitReason::WindDown };
        let result = if self.config.dry_run {
            if !self.inventory.is_flat() {
                self.place_exit(cx, exit_reason)?;
            }
            if self.is_flat() {
                Ok(())
            } else {
                Err(BotError::strategy(format!(
                    "Dry-run shutdown left an unfilled position: {}",
                    self.inventory.net_position
                )))
            }
        } else {
            // The direct subscription returned the original channel, including
            // unread messages. The adapter and its SDK sockets remain alive.
            let mut execution_rx = self.execution_rx.take();
            let cleanup = async {
                // All our orders are IOC: no resting orders to cancel, and a
                // cancel-all cannot resolve an earlier submission's unknown result.
                self.exit_requested = Some(exit_reason);
                loop {
                    // Account already received fills before sizing/retrying a
                    // close. Bound this drain to the current queue, not future traffic.
                    if let Some(rx) = &mut execution_rx {
                        for _ in 0..rx.len() {
                            let Ok(event) = rx.try_recv() else { break };
                            self.apply_execution(&event, cx)?;
                        }
                    }
                    if execution_rx.as_ref().is_none_or(|rx| rx.is_closed() && rx.is_empty()) {
                        execution_rx = None;
                        for tracked in &mut self.tracked {
                            tracked.reconcile_needed = true;
                        }
                    }
                    self.evaluate(cx, false).await?;
                    self.recover_if_needed(cx)?;
                    if self.is_flat() {
                        return Ok(());
                    }
                    tokio::select! {
                        biased;
                        event = async { execution_rx.as_mut()?.recv().await }, if execution_rx.is_some() => {
                            if let Some(event) = event {
                                self.apply_execution(&event, cx)?;
                            } else {
                                execution_rx = None;
                            }
                        }
                        () = self.finish_task(None, cx), if self.recovery_task.is_some() || self.tracked.iter().any(|t| t.submission.is_some()) => {}
                        () = tokio::time::sleep(RECOVERY_INTERVAL) => {
                            self.last_reconcile = None;
                        }
                    }
                }
            };
            tokio::time::timeout(Duration::from_secs(10), cleanup).await.unwrap_or_else(|_| {
                Err(BotError::strategy("Shutdown could not confirm a flat account within 10s; check account before restarting"))
            })
        };
        tracing::info!(net_pos = %self.inventory.net_position, realized_pnl = %self.inventory.realized_pnl,
            peak_pnl = %self.peak_pnl, drawdown_triggered = self.drawdown_triggered,
            fills = self.inventory.total_fills, pending = self.tracked.len(), confirmed = result.is_ok(), "reference-arb final stats");
        result
    }
    fn status(&self) -> serde_json::Value {
        serde_json::json!({
            "state": self.state(), "recovering": self.recovering(), "pending_orders": self.tracked.len(), "dry_run": self.config.dry_run, "symbol": self.config.symbol,
            "basis": self.basis, "basis_entry": self.entry.map(|e| e.basis),
            "long_edge_bps": self.edge(Side::Buy), "short_edge_bps": self.edge(Side::Sell),
            "exit": self.config.exit,
            "remaining_bps": self.entry.and_then(|e| self.remaining(e.side, e.basis)),
            "best_close": self.entry.and_then(|e| e.best_close),
            "trailing_drawdown_bps": self.trailing_drawdown_bps(),
            "bullet_mid": self.bullet_mid(), "binance_mid": self.binance.as_ref().map(|r| r.midpoint),
            "inventory": self.inventory,
            "peak_pnl": self.peak_pnl,
            "drawdown": (self.peak_pnl - self.inventory.realized_pnl).max(Decimal::ZERO),
            "max_drawdown_usd": self.config.max_drawdown_usd,
            "drawdown_triggered": self.drawdown_triggered,
        })
    }
}

#[async_trait]
impl EventHandler<BookUpdate> for ReferenceArbActor {
    async fn on_event(&mut self, event: BookUpdate, cx: &ActorContext) -> Result<(), BotError> {
        if event.exchange != self.config.exchange || event.symbol != self.config.symbol {
            return Ok(());
        }
        if valid_book(&event.orderbook) {
            self.bullet_book = Some(event.orderbook);
            self.bullet_last_seen = Some(event.received_at);
        } else {
            self.bullet_last_seen = None;
        }
        self.update_basis(cx)?;
        self.evaluate(cx, true).await
    }
}
#[async_trait]
impl EventHandler<ReferencePriceUpdate> for ReferenceArbActor {
    async fn on_event(
        &mut self,
        event: ReferencePriceUpdate,
        cx: &ActorContext,
    ) -> Result<(), BotError> {
        if !event.symbol.eq_ignore_ascii_case(&self.config.binance_symbol) {
            return Ok(());
        }
        // Binance validates prices and emits receive timestamps in feed order.
        if self.config.impulse.is_some() {
            self.history.push_back((event.received_at, event.midpoint));
        }
        self.binance = Some(event);
        self.update_basis(cx)?;
        self.evaluate(cx, true).await
    }
}
#[async_trait]
impl EventHandler<ExecutionUpdate> for ReferenceArbActor {
    fn retain_receiver(&mut self, rx: tokio::sync::mpsc::UnboundedReceiver<ExecutionUpdate>) {
        self.execution_rx = Some(rx);
    }

    async fn on_event(
        &mut self,
        event: ExecutionUpdate,
        cx: &ActorContext,
    ) -> Result<(), BotError> {
        if self.config.dry_run {
            return Ok(()); // Private account messages are not paper executions.
        }
        if !self.apply_execution(&event, cx)? {
            return Ok(());
        }
        self.evaluate(cx, false).await?;
        if self.recovering() {
            self.recover_if_needed(cx)?;
        }
        Ok(())
    }
}
#[async_trait]
impl EventHandler<OrderTaskReady> for ReferenceArbActor {
    async fn on_event(&mut self, event: OrderTaskReady, cx: &ActorContext) -> Result<(), BotError> {
        let pending = match &event {
            OrderTaskReady::Recovery(id) => {
                self.recovery_task.as_ref().is_some_and(|(current, _)| current == id)
            }
            OrderTaskReady::Submission(id) => {
                self.tracked.iter().any(|t| t.client_id == *id && t.submission.is_some())
            }
        };
        if !pending {
            return Ok(()); // WS/REST already completed this order.
        }
        self.finish_task(Some(&event), cx).await;
        self.evaluate(cx, false).await?;
        self.recover_if_needed(cx)
    }
}
#[async_trait]
impl EventHandler<Tick> for ReferenceArbActor {
    async fn on_event(&mut self, _event: Tick, cx: &ActorContext) -> Result<(), BotError> {
        if let Ok(broker) = cx.broker(&self.config.exchange)
            && broker.is_disconnected()
        {
            cx.request_shutdown();
            return Ok(());
        }
        self.evaluate(cx, false).await?;
        self.recover_if_needed(cx)?;
        let now = cx.clock().now();
        if self
            .last_status_log
            .is_none_or(|t| now.saturating_duration_since(t) >= Duration::from_secs(5))
        {
            self.last_status_log = Some(now);
            tracing::debug!(state = self.state(), basis = ?self.basis, long_edge_bps = ?self.edge(Side::Buy),
                short_edge_bps = ?self.edge(Side::Sell), impulse_long_bps = ?self.impulse(Side::Buy, now),
                quotes_ready = self.quotes_ready(cx)?, net_pos = %self.inventory.net_position,
                realized_pnl = %self.inventory.realized_pnl, fills = self.inventory.total_fills, "arb tick");
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
