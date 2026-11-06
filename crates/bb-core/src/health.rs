//! Shared connection-health state between an adapter's streaming task and its
//! REST broker.
//!
//! Adapters run a WebSocket muxer task that owns the live connection, while the
//! [`Broker`](crate::broker::Broker) handles request-response on a separate
//! path. `ConnectionHealth` is the small piece of state they share: the muxer
//! flips it, the broker reads it, and strategies observe it via
//! the broker's connection-health methods.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, PoisonError};
use std::time::Instant;

/// Cross-thread health flags shared between a muxer task and a `Broker`.
///
/// - `reconcile_pending` is raised when private execution continuity is lost (both Bullet
///   subscriptions), so a strategy can resync unresolved orders.
/// - `disconnected` is raised once the venue's reconnect loop gives up, so a strategy can request
///   shutdown.
/// - `last_reconnect_at` is the market invalidation boundary: for Bullet, loss of all public books;
///   independent of private order reconciliation.
///
/// Wrap in an `Arc` and share one instance between the muxer task and the
/// broker.
#[derive(Debug, Default)]
pub struct ConnectionHealth {
    reconcile_pending: AtomicBool,
    disconnected: AtomicBool,
    replacement_ready: AtomicBool,
    last_reconnect_at: Mutex<Option<Instant>>,
}

impl ConnectionHealth {
    /// Another subscribed socket can serve orders while a slow route is replaced.
    pub fn set_replacement_ready(&self, ready: bool) {
        self.replacement_ready.store(ready, Ordering::Release);
    }

    pub fn replacement_ready(&self) -> bool {
        self.replacement_ready.load(Ordering::Acquire)
    }

    /// Claim a maintenance opportunity without waiting for the transport loop.
    pub fn take_replacement_ready(&self) -> bool {
        self.replacement_ready.swap(false, Ordering::AcqRel)
    }

    /// Remember the reconnect boundary independently of the consumable order
    /// reconcile signal. A queued book received before it is no longer current.
    pub fn flag_reconnecting(&self, at: Instant) {
        self.flag_market_reset(at);
        self.flag_reconcile();
    }

    /// All public books were lost. Invalidate cached quotes without requesting
    /// private-order recovery; a public socket cannot lose private executions.
    pub fn flag_market_reset(&self, at: Instant) {
        *self.last_reconnect_at.lock().unwrap_or_else(PoisonError::into_inner) = Some(at);
    }

    pub fn last_reconnect_at(&self) -> Option<Instant> {
        *self.last_reconnect_at.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Raise the reconcile signal for a reconnect or missing private execution.
    pub fn flag_reconcile(&self) {
        self.reconcile_pending.store(true, Ordering::Release);
    }

    /// Raise the permanent-disconnect flag — call from the muxer once the
    /// connection has given up for good.
    pub fn flag_disconnected(&self) {
        self.disconnected.store(true, Ordering::Release);
    }

    /// Take-and-clear the pending order reconciliation signal.
    pub fn take_reconcile_signal(&self) -> bool {
        self.reconcile_pending.swap(false, Ordering::AcqRel)
    }

    /// Whether the connection has permanently disconnected.
    pub fn is_disconnected(&self) -> bool {
        self.disconnected.load(Ordering::Acquire)
    }
}
