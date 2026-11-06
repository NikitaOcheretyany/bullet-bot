//! Request-response side of an exchange (REST or WS). Actors call into a `Broker`
//! from their event handlers to place orders, query state, etc.
//!
//! The streaming (WebSocket) side lives in typed [`EventFeed`](crate::harness::EventFeed)
//! implementations instead of being bundled here. That split is why the
//! framework can enforce "one canonical source per fact" at the type level
//! (e.g., `Trade` events are the only way position changes reach an actor).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;

use crate::error::BotError;
use crate::events::Trade;
use crate::types::{
    AmendOrder, Balance, CancelOrder, CancelResult, NewOrder, Order, OrderBook, OrderResult,
    Position,
};

/// REST evidence for one submitted order. `None` is unknown, not cancelled.
#[derive(Debug, Clone, Default)]
pub struct OrderRecovery {
    pub order: Option<Order>,
    pub trades: Vec<Trade>,
    /// The venue's replay window has advanced past this signed submission.
    /// It cannot execute in the future; this does NOT prove it never executed.
    pub submission_expired: bool,
}

#[async_trait]
pub trait Broker: Send + Sync + 'static {
    fn name(&self) -> &str;

    // -- Connection health ----------------------------------------------------
    //
    // The methods below default to no connection-health signals. A
    // broker backed by a simple REST client with no reconnect loop can leave
    // these as-is. Adapters that own a long-lived WebSocket (e.g. Bullet,
    // Hyperliquid) override them to surface reconnect + permanent-disconnect
    // signals without the strategy having to poll the WS directly.

    /// Drains any "user-data WebSocket reconnected" signal that the broker's
    /// connection layer has observed since the previous call. Strategies
    /// consume this in their refresh loop to force an immediate REST
    /// reconciliation: any state changes during the disconnect window
    /// (fills, cancels) are otherwise invisible until the next periodic
    /// reconcile fires.
    fn take_reconcile_signal(&self) -> bool {
        false
    }

    /// Reports whether the broker's connection has been permanently lost.
    /// Strategies should call `ActorContext::request_shutdown()` so the
    /// harness winds down cleanly rather than running blind.
    fn is_disconnected(&self) -> bool {
        false
    }

    /// Latest market-stream reconnect boundary. Cached books must have been
    /// received at or after it before they can be used for new entries.
    /// Reading this does not consume the order reconciliation signal.
    fn last_reconnect_at(&self) -> Option<Instant> {
        None
    }

    /// Permit transport maintenance when no orders remain unresolved. Inventory
    /// may be nonzero. Returns whether maintenance was requested, not completed.
    /// Never waits for a transport reply or network work; another socket serves orders.
    async fn refresh_connection_if_idle(&self) -> bool {
        false
    }

    // -- Market data ----------------------------------------------------------

    async fn get_orderbook(&self, symbol: &str, depth: usize) -> Result<OrderBook, BotError>;
    async fn get_balances(&self) -> Result<Vec<Balance>, BotError>;
    async fn get_positions(&self) -> Result<Vec<Position>, BotError>;
    async fn get_open_orders(&self, symbol: &str) -> Result<Vec<Order>, BotError>;

    /// Recover the status and actual fills of one order, including closed IOCs.
    /// Implementations must not infer rejection from an empty open-order list.
    async fn recover_order(
        &self,
        _symbol: &str,
        _client_id: &str,
        _order_id: Option<&str>,
        _submitted_ms: u64,
    ) -> Result<OrderRecovery, BotError> {
        Err(BotError::exchange("Order recovery is not supported by this broker", true))
    }

    // -- Order management -----------------------------------------------------
    //
    // Error semantics:
    //   - `Err(BotError)` on the outer `Result` means a transport/system failure — the whole call
    //     may have reached the venue. Recover before retrying an ambiguous submission.
    //   - `Ok(results)` with `OrderResult.success = false` means the venue rejected a specific
    //     order (bad price, insufficient margin, etc.). Other orders in the batch may have
    //     succeeded.
    //
    // `OrderResult.order_id` is `Some(id)` when the venue confirms the order
    // synchronously. `None` means outcome unknown — listen on the lifecycle
    // stream (`OrderLifecycle`) for confirmation.

    async fn place_orders(&self, orders: &[NewOrder]) -> Result<Vec<OrderResult>, BotError>;
    async fn cancel_orders(&self, cancels: &[CancelOrder]) -> Result<Vec<CancelResult>, BotError>;
    async fn cancel_all_orders(&self, symbol: &str) -> Result<(), BotError>;

    /// Amend live quotes. Each entry pairs a cancel with a new placement.
    ///
    /// Brokers that support a native atomic amend endpoint (e.g. Bullet)
    /// override this. The default fallback is sequential cancel-then-place:
    /// partial failure leaves you flat on the cancelled side, so override
    /// for venues where atomicity matters.
    async fn amend_orders(&self, amends: &[AmendOrder]) -> Result<Vec<OrderResult>, BotError> {
        if amends.is_empty() {
            return Ok(vec![]);
        }
        let cancels: Vec<CancelOrder> = amends.iter().map(|a| a.cancel.clone()).collect();
        let orders: Vec<NewOrder> = amends.iter().map(|a| a.new_order.clone()).collect();
        self.cancel_orders(&cancels).await?;
        self.place_orders(&orders).await
    }
}

/// Name → broker handle. Actors obtain this via `ActorContext::brokers()`.
#[derive(Default)]
pub struct BrokerRegistry {
    by_name: HashMap<Arc<str>, Arc<dyn Broker>>,
}

impl BrokerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, name: Arc<str>, broker: Arc<dyn Broker>) -> Result<(), BotError> {
        if self.by_name.contains_key(&name) {
            return Err(BotError::config(format!("duplicate broker name: {name}")));
        }
        self.by_name.insert(name, broker);
        Ok(())
    }

    pub fn get(&self, name: &str) -> Option<&Arc<dyn Broker>> {
        self.by_name.get(name)
    }
}

impl std::fmt::Debug for BrokerRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BrokerRegistry")
            .field("names", &self.by_name.keys().collect::<Vec<_>>())
            .finish()
    }
}
