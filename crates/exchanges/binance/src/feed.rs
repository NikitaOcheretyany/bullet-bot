//! Binance bookTicker feed → [`ReferencePriceUpdate`].
//!
//! Three sockets for one symbol, merged by Binance's order-book update id `u`.
//! Every bookTicker is a complete BBO, so no depth reconstruction is needed.
//! The first valid new update feeds the strategy and the existing recorder;
//! other copies measure delivery lag. Reconnects retain the global watermark.

use std::collections::BTreeMap;
use std::str::FromStr;
use std::time::{Duration, Instant};

use bb_core::harness::MpscFeed;
use bb_core::recording::RecordingSender;
use futures_util::StreamExt;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

/// Reference price update — a fair-value snapshot from an external venue.
///
/// `mid` is the **microprice** `(bid · ask_size + ask · bid_size) /
/// (bid_size + ask_size)`. The cross-weighting captures top-of-book imbalance
/// (small ask-size → next trade likely lifts the ask, so fair value sits
/// closer to ask), and unlike the simple `(bid+ask)/2` it updates on every
/// size change even when bid/ask prices are pinned. Empty, negative and crossed quotes are
/// rejected.
///
/// Emitted by Binance's bookTicker stream. Treat `received_at` as a local
/// monotonic timestamp for staleness checks; it is **not** exchange time.
#[derive(Debug, Clone)]
pub struct ReferencePriceUpdate {
    /// Upstream symbol as reported by Binance (e.g. `"BTCUSDT"`).
    pub symbol: String,
    /// Microprice (see struct doc).
    pub mid: Decimal,
    /// Arithmetic midpoint, used for basis and impulse (distinct from microprice).
    pub midpoint: Decimal,
    /// Local receive instant — used by strategies to detect stale feeds.
    pub received_at: Instant,
}

/// Incoming bookTicker payload. Binance sends unquoted decimals as strings.
#[derive(Debug, Deserialize)]
struct BookTicker<'a> {
    #[serde(rename = "u")]
    update_id: u64,
    #[serde(rename = "s")]
    symbol: &'a str,
    #[serde(rename = "b")]
    bid: &'a str,
    #[serde(rename = "B")]
    bid_size: &'a str,
    #[serde(rename = "a")]
    ask: &'a str,
    #[serde(rename = "A")]
    ask_size: &'a str,
}

/// Which Binance venue to reference.
///
/// For arbitrage against a perpetual DEX (Bullet), [`BinanceMarket::Perp`]
/// is almost always the right choice — comparing perps-to-perps removes the
/// funding-basis confound between spot and perp prices. Spot is retained as
/// an option for completeness (useful if you're comparing against a spot
/// venue on the other side).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinanceMarket {
    /// `stream.binance.com:9443` — spot order book.
    Spot,
    /// `fstream.binance.com` — USDT-margined futures (perps).
    Perp,
}

impl BinanceMarket {
    fn host(self) -> &'static str {
        match self {
            Self::Spot => "stream.binance.com:9443",
            Self::Perp => "fstream.binance.com",
        }
    }
}

impl std::str::FromStr for BinanceMarket {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "spot" => Ok(Self::Spot),
            "perp" | "perps" | "futures" | "fstream" => Ok(Self::Perp),
            other => Err(format!("unknown binance market '{other}' (want 'spot' or 'perp')")),
        }
    }
}

/// Strategy delivery never blocks socket reads on this bounded channel. The
/// latest unsent quote is retried when space returns, with its first receive time.
const REFERENCE_CHANNEL_CAPACITY: usize = 256;
// Same public-feed evaluation interval, warmup and bounded history as Bullet.
const ROTATION_INTERVAL: Duration = Duration::from_secs(300);
const WARMUP: Duration = Duration::from_secs(5);
const HISTORY_CAPACITY: usize = 8_192;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Open the bookTicker stream for `symbol` on the chosen Binance market.
/// `symbol` is passed lowercased into the URL (e.g. `"btcusdt"`). Spawns a
/// three-connection background task and returns the feed handle. Connection
/// attempts happen on the spawned task with 1s→30s exponential backoff —
/// this fn itself never fails.
pub fn connect_binance(symbol: &str, market: BinanceMarket) -> MpscFeed<ReferencePriceUpdate> {
    connect_binance_with_recording(symbol, market, None)
}

/// Same merged stream, with one copy per accepted update for recording.
pub fn connect_binance_with_recording(
    symbol: &str,
    market: BinanceMarket,
    recording: Option<RecordingSender>,
) -> MpscFeed<ReferencePriceUpdate> {
    let symbol = symbol.to_lowercase();
    let url = format!("wss://{}/ws/{symbol}@bookTicker", market.host());
    let (tx, rx) = mpsc::channel::<ReferencePriceUpdate>(REFERENCE_CHANNEL_CAPACITY);

    tokio::spawn(async move {
        run_ws_loop(url, symbol, tx, recording).await;
    });

    MpscFeed::bounded(rx)
}

#[derive(Default)]
struct MarketData {
    connected_at: [Option<Instant>; 3],
    generation: [u64; 3],
    arrivals: BTreeMap<u64, [Option<Instant>; 3]>,
    wins: [u64; 3],
    last_id: Option<u64>,
    // Only an undelivered current quote; retain its FIRST receive time on retry.
    pending: Option<ReferencePriceUpdate>,
}

impl MarketData {
    fn connected(&mut self, slot: usize, at: Instant) -> Option<Instant> {
        self.connected_at[slot] = Some(at);
        let deadline = self.rotation_deadline()?;
        self.arrivals.clear();
        self.wins = [0; 3];
        Some(deadline)
    }

    fn reset(&mut self, slot: usize) {
        self.connected_at[slot] = None;
        self.generation[slot] += 1;
        // Do not reset last_id or refresh a quote's timestamp on reconnect.
        // The next completed connection starts a fresh shared rating window.
    }

    fn ticker(
        &mut self,
        slot: usize,
        text: &str,
        at: Instant,
        symbol: &str,
        tx: &mpsc::Sender<ReferencePriceUpdate>,
        recording: Option<&RecordingSender>,
    ) -> Result<(), String> {
        let tick: BookTicker<'_> = serde_json::from_str(text).map_err(|e| format!("json: {e}"))?;
        if !tick.symbol.eq_ignore_ascii_case(symbol) {
            return Err(format!("unexpected Binance symbol {}", tick.symbol));
        }
        let newer = self.last_id.is_none_or(|last| tick.update_id > last);
        if newer {
            // Validate before advancing the watermark. A malformed first copy
            // must not suppress a correct copy from another socket.
            let event = tick.reference(at)?;
            self.last_id = Some(tick.update_id);
            self.pending = Some(event);
        }
        if self.last_id == Some(tick.update_id)
            && let Some(event) = self.pending.take()
            && let Err(mpsc::error::TrySendError::Full(event)) = tx.try_send(event)
        {
            self.pending = Some(event);
        }
        // Compare receive instants for the SAME update id, including losing
        // copies. BBO conversion and strategy delivery precede rating/recording.
        if self.connected_at[slot].is_some_and(|ready| at.duration_since(ready) >= WARMUP) {
            self.arrivals.entry(tick.update_id).or_insert([None; 3])[slot].get_or_insert(at);
            if self.arrivals.len() > HISTORY_CAPACITY {
                self.arrivals.pop_first();
            }
            if newer {
                self.wins[slot] += 1;
            }
        }
        if newer && let Some(recording) = recording {
            // Preserve the raw winning payload, including E/T and unknown fields.
            recording.record_at(at, "binance", "bookTicker", || Ok(text.to_owned()));
        }
        Ok(())
    }

    fn rotation_deadline(&self) -> Option<Instant> {
        let [a, b, c] = self.connected_at;
        Some(a?.max(b?).max(c?) + ROTATION_INTERVAL)
    }

    // Same scoring as the Bullet pool, keyed by global Binance u rather than T.
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

    fn worst(&self, delays: &[Vec<Duration>; 3]) -> usize {
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
            return slot;
        }
        if delays.iter().any(Vec::is_empty) {
            // No shared events means no measured latency. Prefer low contribution;
            // on equal wins, a silent socket loses to one still delivering copies.
            return (0..3).min_by_key(|&slot| (self.wins[slot], delays[slot].len())).unwrap_or(0);
        }
        (0..3)
            .max_by_key(|&slot| {
                (delays[slot][delays[slot].len() / 2], std::cmp::Reverse(self.wins[slot]))
            })
            .unwrap_or(0)
    }
}

async fn receive(ws: &mut Option<Socket>) -> Option<Result<Message, WsError>> {
    match ws {
        Some(ws) => ws.next().await,
        None => std::future::pending().await,
    }
}

fn connect(connecting: &mut JoinSet<(usize, Socket)>, url: String, slot: usize, delay: Duration) {
    connecting.spawn(async move {
        let mut backoff = delay;
        loop {
            tokio::time::sleep(backoff).await;
            match tokio::time::timeout(Duration::from_secs(10), connect_async(&url)).await {
                Ok(Ok((ws, _))) => return (slot, ws),
                error => tracing::warn!(slot, ?error, "Binance WS connect failed; retrying"),
            }
            backoff = (backoff * 2).clamp(Duration::from_secs(1), Duration::from_secs(30));
        }
    });
}

async fn run_ws_loop(
    url: String,
    symbol: String,
    tx: mpsc::Sender<ReferencePriceUpdate>,
    recording: Option<RecordingSender>,
) {
    let mut sockets: [Option<Socket>; 3] = std::array::from_fn(|_| None);
    let mut data = MarketData::default();
    let mut connecting = JoinSet::new();
    for slot in 0..3 {
        connect(&mut connecting, url.clone(), slot, Duration::ZERO);
    }
    let rotation = tokio::time::sleep(ROTATION_INTERVAL);
    tokio::pin!(rotation);
    loop {
        let [a, b, c] = &mut sockets;
        let (slot, event) = tokio::select! {
            () = tx.closed() => break,
            permit = tx.reserve(), if data.pending.is_some() => {
                let Ok(permit) = permit else { break };
                if let Some(event) = data.pending.take() {
                    permit.send(event);
                }
                continue;
            }
            result = connecting.join_next(), if !connecting.is_empty() => {
                if let Some(Ok((slot, ws))) = result {
                    sockets[slot] = Some(ws);
                    if let Some(deadline) = data.connected(slot, Instant::now()) {
                        rotation.as_mut().reset(deadline.into());
                    }
                    tracing::info!(slot, generation = data.generation[slot], %symbol, "Binance public connected");
                    if let Some(recording) = &recording {
                        recording.record("binance", "connected", || serde_json::to_string(
                            &serde_json::json!({"url":url,"connection":slot,"generation":data.generation[slot]})
                        ));
                    }
                }
                continue;
            }
            () = &mut rotation, if data.rotation_deadline().is_some() => {
                let delays = data.delays();
                for (slot, samples) in delays.iter().enumerate() {
                    tracing::info!(slot, %symbol, generation = data.generation[slot],
                        samples = samples.len(), first_bbo = data.wins[slot],
                        median_lag_ms = samples.get(samples.len()/2).map(|d| d.as_secs_f64()*1000.0),
                        "Binance public quality");
                }
                let slot = data.worst(&delays);
                sockets[slot] = None; // Close before opening; the other two keep serving.
                data.reset(slot);
                connect(&mut connecting, url.clone(), slot, Duration::ZERO);
                tracing::info!(slot, %symbol, "Binance public rotating slowest connection");
                continue;
            }
            event = receive(a) => (0, event),
            event = receive(b) => (1, event),
            event = receive(c) => (2, event),
        };
        let received_at = Instant::now(); // Before parsing, rating or recording.
        match event {
            Some(Ok(Message::Text(text))) => {
                if let Err(error) =
                    data.ticker(slot, &text, received_at, &symbol, &tx, recording.as_ref())
                {
                    tracing::warn!(slot, %error, "Binance bookTicker parse error");
                }
            }
            // Tungstenite queues the matching Pong automatically; polling the
            // next read flushes it without awaiting writes outside select!.
            Some(Ok(
                Message::Ping(_) | Message::Pong(_) | Message::Binary(_) | Message::Frame(_),
            )) => {}
            disconnected => {
                tracing::warn!(slot, ?disconnected, "Binance public disconnected; peers retained");
                sockets[slot] = None;
                data.reset(slot);
                if let Some(recording) = &recording {
                    recording.record_at(received_at, "binance", "reconnecting", || serde_json::to_string(
                        &serde_json::json!({"url":url,"connection":slot,"generation":data.generation[slot]})
                    ));
                }
                connect(&mut connecting, url.clone(), slot, Duration::from_secs(1));
            }
        }
        tokio::task::consume_budget().await;
    }
    // Dropping sockets and JoinSet also cancels idle sockets/connect attempts.
}

impl BookTicker<'_> {
    fn reference(&self, received_at: Instant) -> Result<ReferencePriceUpdate, String> {
        let bid = Decimal::from_str(self.bid).map_err(|e| format!("bid decimal: {e}"))?;
        let ask = Decimal::from_str(self.ask).map_err(|e| format!("ask decimal: {e}"))?;
        let bid_size = Decimal::from_str(self.bid_size).map_err(|e| format!("bid_size: {e}"))?;
        let ask_size = Decimal::from_str(self.ask_size).map_err(|e| format!("ask_size: {e}"))?;
        if bid <= Decimal::ZERO
            || ask < bid
            || bid_size <= Decimal::ZERO
            || ask_size <= Decimal::ZERO
        {
            return Err("invalid or empty Binance BBO".into());
        }
        let total = bid_size + ask_size;
        let mid = (bid * ask_size + ask * bid_size) / total;
        Ok(ReferencePriceUpdate {
            symbol: self.symbol.to_owned(),
            mid,
            midpoint: (bid + ask) / Decimal::from(2),
            received_at,
        })
    }
}

#[cfg(test)]
#[path = "feed_tests.rs"]
mod tests;
