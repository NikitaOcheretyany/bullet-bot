//! Canonical event types that flow through the harness bus.
//!
//! Split guarantees one canonical source per kind of fact:
//!
//! - **`Trade`** — an execution. Adapters emit one `Trade` per fill. This is the *only* event
//!   strategies use to update position/PnL. Double-counting bugs are structurally impossible: if a
//!   Bullet `OrderUpdateData::TradeFill` arrives, the adapter emits a `Trade` (for inventory) and a
//!   `OrderLifecycle` (for reconcile) as independent events. An HL `UserFills` message produces a
//!   `Trade`; the parallel `OrderUpdates` message produces only lifecycle info, never a duplicate
//!   `Trade`.
//!
//! - **`OrderLifecycle`** — status transitions (Open → `PartiallyFilled` → Filled / Cancelled /
//!   Rejected). Used by strategies for reconcile: "is this order still resting?", "did my cancel go
//!   through?". Never used for position updates.
//!
//! - **`BookUpdate`** — orderbook state. Strategies read the latest for pricing decisions.
//!
//! - **`MarkPriceUpdate`** — mark price + funding rate from the venue.
//!
//! - **`Tick`** — periodic heartbeat. Produced by a framework-provided `TickFeed` so periodic work
//!   fits the same event model as everything else.

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use rust_decimal::Decimal;
use serde::Serialize;

use crate::types::{Order, OrderBook, Side};

/// A single execution against our account.
///
/// Fees come from the execution itself, so accounting needs neither a
/// maker/taker guess nor a configured fee schedule.
#[derive(Debug, Clone, Serialize)]
pub struct Trade {
    pub exchange: String,
    pub symbol: String,
    pub order_id: String,
    /// Venue-assigned fill / trade ID. Use this for dedup across reconnects —
    /// the same physical fill may be replayed when the WS reconnects.
    /// `None` when the adapter doesn't expose a per-fill ID.
    pub trade_id: Option<String>,
    /// Set if the order was placed with a caller-assigned client id.
    pub client_id: Option<String>,
    pub side: Side,
    pub price: Decimal,
    pub quantity: Decimal,
    /// Actual fee in quote currency; negative for a rebate.
    pub fee: Decimal,
    /// Venue-reported realized `PnL` for this fill, before fees, in quote currency.
    /// None for simulated fills, where inventory accounting supplies the value.
    pub realized_pnl: Option<Decimal>,
    /// Venue fill timestamp in Unix milliseconds. `None` when the adapter
    /// doesn't expose a per-fill timestamp.
    pub timestamp: Option<u64>,
}

/// Status transition for one of our orders.
#[derive(Debug, Clone, Serialize)]
pub struct OrderLifecycle {
    pub exchange: String,
    pub order: Order,
}

/// One private venue message, preserving fill-before-status and each source's order.
/// Subscribe to this OR the separate Trade/OrderLifecycle feeds, never both.
/// Inventory still changes only from `trade`, not from lifecycle quantities.
#[derive(Debug, Clone)]
pub struct ExecutionUpdate {
    pub trade: Option<Trade>,
    pub lifecycle: OrderLifecycle,
    pub received_at: Instant,
    /// Start of this source's uninterrupted, acknowledged private subscription.
    /// A source joining after submission cannot alone prove an empty cancellation.
    /// None is used for local/paper events without a reconnectable upstream.
    pub stream_started_at: Option<Instant>,
}

/// Orderbook state.
#[derive(Debug, Clone, Serialize)]
pub struct BookUpdate {
    pub exchange: String,
    pub symbol: String,
    pub orderbook: OrderBook,
    /// Local adapter receive time; queue delay must not make a quote fresh.
    #[serde(skip)]
    pub received_at: Instant,
}

/// Mark price and/or funding rate.
#[derive(Debug, Clone, Serialize)]
pub struct MarkPriceUpdate {
    pub exchange: String,
    pub symbol: String,
    pub mark_price: Decimal,
    /// Funding rate for the current period. `None` means the adapter did not
    /// receive a funding rate in this update (e.g. `AllMids` only carries prices).
    /// `Some(Decimal::ZERO)` means the rate was explicitly reported as zero.
    pub funding_rate: Option<Decimal>,
}

/// Periodic heartbeat.
///
/// `at` is the monotonic instant the tick was generated. `unix_ms` is the
/// corresponding Unix timestamp in milliseconds — useful for replay pacing
/// and for actors that need wall-clock time without importing
/// `SystemTime::now()`.
///
/// Note: `unix_ms` is self-stamped from the real wall clock when the tick is
/// produced. Actors doing time-sensitive logic should prefer
/// `cx.clock().unix_ms()` over `tick.unix_ms`: under a `TestClock`
/// (deterministic replay) the clock is driven by the replayed event stream, so
/// the self-stamped `tick.unix_ms` and `cx.clock()` can disagree.
#[derive(Debug, Clone)]
pub struct Tick {
    pub at: Instant,
    pub unix_ms: u64,
}

impl Tick {
    pub fn now() -> Self {
        let at = Instant::now();
        let unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
        Self { at, unix_ms }
    }
}
