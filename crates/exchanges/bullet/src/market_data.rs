//! Three public SDK sockets for ONE instrument. Reconstruct each book before
//! merging by depth.T; local update ids never cross connection boundaries.
//! Private events and order RPCs remain on the existing, independent muxer.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bb_core::events::BookUpdate;
use bb_core::health::ConnectionHealth;
use bb_core::recording::RecordingSender;
use bb_core::types::OrderBook;
use bullet_rust_sdk::types::{DepthUpdate, MessageType};
use bullet_rust_sdk::ws::models::{ServerMessage, TaggedMessage};
use bullet_rust_sdk::{Client, ManagedWebsocket, OrderbookDepth, Topic, WsEvent};
use rust_decimal::Decimal;
use serde::Serializer;
use serde::ser::SerializeMap;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::connection::RecentIds;
use crate::convert::parse_level_tuple;

const ROTATION_INTERVAL: Duration = Duration::from_secs(300);
const WARMUP: Duration = Duration::from_secs(5);
const HISTORY_CAPACITY: usize = 8_192;

/// State of the one-symbol pool, not a second strategy/order state machine.
struct MarketData {
    books: [Option<OrderBook>; 3],
    generation: [u64; 3],
    ready_at: [Option<Instant>; 3],
    current: Option<(u64, OrderBook)>,
    // Last accepted BBO and its first receipt, whether delivered yet or not.
    last_bbo: Option<(OrderBook, Instant)>,
    pending_bbo: Option<BookUpdate>,
    // Equal transaction timestamps only: do not compare publication clocks.
    arrivals: BTreeMap<u64, [Option<Instant>; 3]>,
    wins: [u64; 3],
    trades: RecentIds,
}

impl Default for MarketData {
    fn default() -> Self {
        Self {
            books: std::array::from_fn(|_| None),
            generation: [0; 3],
            ready_at: [None; 3],
            current: None,
            last_bbo: None,
            pending_bbo: None,
            arrivals: BTreeMap::new(),
            wins: [0; 3],
            trades: RecentIds::new(HISTORY_CAPACITY),
        }
    }
}

impl MarketData {
    fn reset(&mut self, slot: usize, at: Instant, health: &ConnectionHealth) {
        self.books[slot] = None;
        self.ready_at[slot] = None;
        self.generation[slot] += 1;
        self.wins[slot] = 0;
        for arrivals in self.arrivals.values_mut() {
            arrivals[slot] = None;
        }
        if self.books.iter().all(Option::is_none) {
            self.pending_bbo = None;
            health.flag_market_reset(at);
        }
        // Keep the global T watermark, accepted book and trade dedup across
        // reconnects. A new socket's old snapshot cannot roll the market back.
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    fn depth(
        &mut self,
        slot: usize,
        depth: &DepthUpdate,
        at: Instant,
        bbo: &mpsc::Sender<BookUpdate>,
        books: &mpsc::Sender<BookUpdate>,
        health: &ConnectionHealth,
        recording: Option<&RecordingSender>,
    ) -> Result<(), &'static str> {
        if matches!(depth.msg_type, MessageType::Snapshot) {
            self.books[slot] = Some(OrderBook::default());
        }
        let Some(book) = &mut self.books[slot] else {
            return Ok(()); // A reconnected stream needs its initial snapshot.
        };
        if !matches!(depth.msg_type, MessageType::Snapshot) {
            if depth.last_update_id == book.last_update_id {
                return Ok(()); // Exact sequence replay, not a new update.
            }
            if depth.prev_update_id != book.last_update_id {
                return Err("depth sequence gap");
            }
        }
        for (levels, updates) in [(&mut book.bids, &depth.bids), (&mut book.asks, &depth.asks)] {
            for level in updates {
                let (price, qty) = parse_level_tuple(level).ok_or("invalid depth number")?;
                if price <= Decimal::ZERO || qty < Decimal::ZERO {
                    return Err("invalid depth price/quantity");
                }
                if qty.is_zero() {
                    levels.remove(&price);
                } else {
                    levels.insert(price, qty);
                }
            }
        }
        book.last_update_id = depth.last_update_id;
        self.ready_at[slot].get_or_insert(at);

        // Apply even a globally obsolete delta: the next delta needs this book.
        let t = depth.transaction_time;
        if at.duration_since(self.ready_at[slot].unwrap_or(at)) >= WARMUP {
            self.arrivals.entry(t).or_insert([None; 3])[slot].get_or_insert(at);
            if self.arrivals.len() > HISTORY_CAPACITY {
                self.arrivals.pop_first();
            }
        }
        if let Some((last_t, current)) = &self.current {
            if t < *last_t {
                return Ok(());
            }
            if t == *last_t && (book.bids != current.bids || book.asks != current.asks) {
                tracing::warn!(slot, symbol = %depth.symbol, t, "Bullet public books disagree at equal T; retaining accepted book");
                return Ok(());
            }
        }

        let changed = self.last_bbo.as_ref().is_none_or(|(last, _)| {
            book.bids.last_key_value() != last.bids.last_key_value()
                || book.asks.first_key_value() != last.asks.first_key_value()
        });
        let recovering = self.last_bbo.as_ref().is_none_or(|(_, sent)| {
            health.last_reconnect_at().is_some_and(|cutoff| *sent < cutoff)
        });
        let newer = self.current.as_ref().is_none_or(|(last_t, _)| t > *last_t);
        if changed || recovering {
            let mut top =
                OrderBook { last_update_id: depth.last_update_id, ..OrderBook::default() };
            if let Some((&price, &qty)) = book.bids.last_key_value() {
                top.bids.insert(price, qty);
            }
            if let Some((&price, &qty)) = book.asks.first_key_value() {
                top.asks.insert(price, qty);
            }
            self.pending_bbo = Some(BookUpdate {
                exchange: "bullet".into(),
                symbol: depth.symbol.clone(),
                orderbook: top.clone(),
                received_at: at,
            });
            self.last_bbo = Some((top, at));
            if newer && self.ready_at[slot].is_some_and(|ready| at.duration_since(ready) >= WARMUP)
            {
                self.wins[slot] += 1;
            }
        }
        // Enqueue BEFORE full-book copying/recording. A duplicate may retry,
        // but cannot renew the accepted quote's timestamp or overwrite it.
        let bbo_forwarded = if let Some(event) = self.pending_bbo.take() {
            match bbo.try_send(event) {
                Ok(()) => true,
                Err(mpsc::error::TrySendError::Full(event)) => {
                    self.pending_bbo = Some(event);
                    false
                }
                Err(mpsc::error::TrySendError::Closed(_)) => false,
            }
        } else {
            false
        };
        if !newer {
            // Delivery retries and same-T recovery can refresh BBO, but neither
            // creates a new canonical book. Publish/record each T only once.
            return Ok(());
        }
        // A full snapshot is safe at every source switch. Never forward raw
        // deltas from different sockets as though they were one depth chain.
        let book_forwarded = if let Ok(permit) = books.try_reserve() {
            permit.send(BookUpdate {
                exchange: "bullet".into(),
                symbol: depth.symbol.clone(),
                orderbook: book.clone(),
                received_at: at,
            });
            true
        } else {
            false
        };
        self.current = Some((t, book.clone()));
        if let Some(recording) = recording {
            recording.record_at(at, "bullet", "depthUpdate", || {
                // Write directly into the output buffer. Borrow Decimal levels:
                // no per-price Strings or intermediate serde_json::Value tree.
                let mut json = serde_json::Serializer::new(Vec::new());
                let mut fields = json.serialize_map(None)?;
                fields.serialize_entry("e", "depthUpdate")?;
                fields.serialize_entry("s", &depth.symbol)?;
                fields.serialize_entry("E", &depth.event_time)?;
                fields.serialize_entry("T", &t)?;
                fields.serialize_entry("U", &depth.last_update_id)?;
                fields.serialize_entry("u", &depth.last_update_id)?;
                fields.serialize_entry("pu", &0)?;
                fields.serialize_entry("mt", "s")?;
                fields.serialize_entry("b", &book.bids.iter().rev().collect::<Vec<_>>())?;
                fields.serialize_entry("a", &book.asks.iter().collect::<Vec<_>>())?;
                fields.serialize_entry("connection", &slot)?;
                fields.serialize_entry("generation", &self.generation[slot])?;
                fields.serialize_entry("bbo_changed", &changed)?;
                fields.serialize_entry("bbo_forwarded", &bbo_forwarded)?;
                fields.serialize_entry("book_forwarded", &book_forwarded)?;
                fields.end()?;
                String::from_utf8(json.into_inner()).map_err(serde::ser::Error::custom)
            });
        }
        Ok(())
    }

    /// Median lag against the first copy of the SAME depth.T. Unmatched
    /// timestamps cannot tell us relative delivery latency and are excluded.
    fn delays(&self) -> [Vec<Duration>; 3] {
        let mut delays: [Vec<Duration>; 3] = std::array::from_fn(|_| Vec::new());
        for arrivals in self.arrivals.values() {
            if arrivals.iter().flatten().count() < 2 {
                continue;
            }
            if let Some(first) = arrivals.iter().flatten().min() {
                for (slot, at) in arrivals.iter().enumerate() {
                    if let Some(at) = at {
                        delays[slot].push(at.duration_since(*first));
                    }
                }
            }
        }
        for samples in &mut delays {
            if !samples.is_empty() {
                let middle = samples.len() / 2;
                samples.select_nth_unstable(middle);
            }
        }
        delays
    }

    fn rotation_deadline(&self) -> Option<Instant> {
        let [a, b, c] = self.ready_at;
        Some(a?.max(b?).max(c?) + ROTATION_INTERVAL)
    }

    fn worst(&self, delays: &[Vec<Duration>; 3]) -> Option<usize> {
        // Old fast samples must not protect a now-silent peer. Reuse the five-
        // second observation grace: an in-flight copy is not a stalled stream.
        // Compare peers, not wall time, so a quiet market penalizes nobody.
        let last: [Option<Instant>; 3] = std::array::from_fn(|slot| {
            self.arrivals.values().filter_map(|arrivals| arrivals[slot]).max()
        });
        if let Some(latest) = last.iter().flatten().max()
            && let Some(slot) = (0..3)
                .filter(|&slot| last[slot].is_none_or(|at| latest.duration_since(at) >= WARMUP))
                .min_by_key(|&slot| last[slot])
        {
            return Some(slot);
        }
        // Different publishers may have no common T in this window. In that
        // case use contribution to the merged BBO, not fabricated latency.
        if delays.iter().any(Vec::is_empty) {
            return (0..3).min_by_key(|&slot| self.wins[slot]);
        }
        (0..3).max_by_key(|&slot| {
            (delays[slot][delays[slot].len() / 2], std::cmp::Reverse(self.wins[slot]))
        })
    }
}

async fn receive(ws: &mut Option<ManagedWebsocket>) -> Option<WsEvent> {
    match ws {
        Some(ws) => ws.recv().await,
        None => std::future::pending().await,
    }
}

fn connect(
    connecting: &mut JoinSet<(usize, ManagedWebsocket)>,
    client: Arc<Client>,
    symbol: String,
    slot: usize,
    recording: bool,
) {
    connecting.spawn(async move {
        loop {
            match client.connect_ws_managed().call().await {
                Ok(ws) => {
                    let mut topics = vec![Topic::depth(&symbol, OrderbookDepth::D20).to_string()];
                    if recording {
                        topics.push(Topic::agg_trade(&symbol).to_string());
                    }
                    if ws.subscribe_raw(topics, None).is_ok() {
                        return (slot, ws);
                    }
                }
                Err(error) => {
                    tracing::warn!(slot, %error, "Bullet public connect failed; retrying");
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
}

#[allow(clippy::too_many_lines)]
pub(super) async fn run(
    client: Arc<Client>,
    symbol: String,
    bbo: mpsc::Sender<BookUpdate>,
    books: mpsc::Sender<BookUpdate>,
    health: Arc<ConnectionHealth>,
    recording: Option<RecordingSender>,
) {
    let mut sockets: [Option<ManagedWebsocket>; 3] = std::array::from_fn(|_| None);
    let mut data = MarketData::default();
    let mut connecting = JoinSet::new();
    for slot in 0..3 {
        connect(&mut connecting, client.clone(), symbol.clone(), slot, recording.is_some());
    }
    let rotation = tokio::time::sleep(ROTATION_INTERVAL);
    tokio::pin!(rotation);
    loop {
        let [a, b, c] = &mut sockets;
        let (slot, event) = tokio::select! {
            () = async { bbo.closed().await; books.closed().await; } => break,
            permit = bbo.reserve(), if data.pending_bbo.is_some() => {
                if let Some(event) = data.pending_bbo.take()
                    && let Ok(permit) = permit
                {
                    permit.send(event);
                }
                continue;
            }
            connected = connecting.join_next(), if !connecting.is_empty() => {
                if let Some(Ok((slot, ws))) = connected {
                    sockets[slot] = Some(ws);
                    tracing::info!(slot, %symbol, "Bullet public connected");
                }
                continue;
            }
            () = &mut rotation, if data.rotation_deadline().is_some() => {
                let now = Instant::now();
                let delays = data.delays();
                for (slot, samples) in delays.iter().enumerate() {
                    tracing::info!(slot, %symbol, samples = samples.len(), first_bbo = data.wins[slot],
                        median_lag_ms = samples.get(samples.len()/2).map(|d| d.as_secs_f64()*1000.0),
                        "Bullet public quality");
                }
                if let Some(slot) = data.worst(&delays) {
                    // Close BEFORE opening: two remaining public sockets continue,
                    // and no private/RPC connection is touched.
                    sockets[slot] = None;
                    data.reset(slot, now, &health);
                    connect(&mut connecting, client.clone(), symbol.clone(), slot, recording.is_some());
                    tracing::info!(slot, %symbol, "Bullet public rotating slowest connection");
                }
                continue;
            }
            event = receive(a) => (0, event),
            event = receive(b) => (1, event),
            event = receive(c) => (2, event),
        };
        let at = Instant::now(); // SDK queue precedes this application timestamp.
        let mut replace = false;
        match event {
            Some(WsEvent::Message(msg)) => match *msg {
                ServerMessage::DepthUpdate(ref depth) if depth.symbol == symbol => {
                    let was_ready = data.ready_at[slot].is_some();
                    if let Err(reason) =
                        data.depth(slot, depth, at, &bbo, &books, &health, recording.as_ref())
                    {
                        tracing::warn!(slot, %symbol, reason, "Bullet public book lost; replacing socket");
                        replace = true;
                    } else if !was_ready && let Some(deadline) = data.rotation_deadline() {
                        // One countdown, starting when the last/new socket has
                        // its book. Ordinary updates never postpone rotation.
                        data.arrivals.clear();
                        data.wins = [0; 3];
                        rotation.as_mut().reset(deadline.into());
                    }
                }
                ServerMessage::AggTrade(ref trade) if trade.symbol == symbol => {
                    if data.trades.insert(&trade.agg_trade_id.to_string())
                        && let Some(recording) = &recording
                    {
                        recording.record_at(at, "bullet", "aggTrade", || {
                            let mut value = serde_json::to_value(trade)?;
                            value["connection"] = slot.into();
                            value["generation"] = data.generation[slot].into();
                            serde_json::to_string(&value)
                        });
                    }
                }
                ServerMessage::Tagged(TaggedMessage::Error(ref error))
                | ServerMessage::Error(ref error) => {
                    tracing::warn!(
                        slot,
                        ?error,
                        "Bullet public subscription error; replacing socket"
                    );
                    replace = true;
                }
                ServerMessage::Unknown(ref error, ref raw) => {
                    tracing::warn!(slot, %error, "Bullet public decode failure");
                    // An unknown control/trade message does not damage depth.
                    // Only an undecodable delta requires a fresh book.
                    replace = serde_json::from_str::<serde_json::Value>(raw)
                        .is_ok_and(|value| value["e"] == "depthUpdate" && value["s"] == symbol);
                }
                _ => {}
            },
            Some(WsEvent::Reconnecting) => {
                data.reset(slot, at, &health);
                tracing::warn!(slot, %symbol, "Bullet public reconnecting; other books retained");
            }
            Some(WsEvent::Disconnected(_)) | None => {
                replace = true;
            }
        }
        if replace {
            sockets[slot] = None;
            data.reset(slot, at, &health);
            connect(&mut connecting, client.clone(), symbol.clone(), slot, recording.is_some());
        }
        // The SDK uses futures channels. Give other tasks a turn under a
        // continuous backlog, including the independent protective RPC path.
        tokio::task::consume_budget().await;
    }
}

#[cfg(test)]
#[path = "market_data_tests.rs"]
mod tests;
