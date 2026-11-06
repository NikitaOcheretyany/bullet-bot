//! Bullet transport: two private/RPC SDK sockets plus a three-socket public
//! pool in `market_data`. Both publish through the existing typed feeds.
//!
//! Each channel backs one `EventFeed` implementation via the generic
//! [`bb_core::harness::MpscFeed`]. The muxer task is spawned at connection
//! time. A terminal SDK disconnect replaces only its slot; the peer keeps working.
//!
//! Canonical source split — important for correctness:
//!   - `OrderUpdateData::TradeFill` emits BOTH a `Trade` (for inventory) and an `OrderLifecycle`
//!     (for reconcile). They are independent events so strategies that only handle `Trade` won't
//!     miss position updates, and strategies that only handle `OrderLifecycle` won't miss a
//!     transition.
//!   - `OrderUpdateData::PlaceOrder` / `Cancel` emit only `OrderLifecycle` — they carry no
//!     execution, so there's no `Trade` to emit.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bb_core::error::BotError;
use bb_core::events::{BookUpdate, ExecutionUpdate, MarkPriceUpdate, OrderLifecycle, Trade};
use bb_core::harness::MpscFeed;
use bb_core::health::ConnectionHealth;
use bb_core::recording::RecordingSender;
use bullet_rust_sdk::types::{ClientMessage, OrderUpdateData, RequestId};
use bullet_rust_sdk::ws::models::{ServerMessage, TaggedMessage};
use bullet_rust_sdk::{
    CallMessage, Client, ManagedWebsocket, Network, Topic, UserAction, UserActionDiscriminants,
    WsEvent,
};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;

use crate::broker::{BulletBroker, load_increments};

type WsReply = oneshot::Sender<Result<ServerMessage, BotError>>;
/// Commands and read-only inspection of the existing pending RPCs.
#[allow(clippy::large_enum_variant)] // Keep the normal submission free of another allocation.
pub(crate) enum WsRequest {
    Submit { command: ClientMessage, call: CallMessage, window: Option<u64>, reply: WsReply },
    SubmissionWindow { client_id: String, reply: oneshot::Sender<Option<u64>> },
    RefreshConnection,
}

/// `BookUpdate` / `MarkPriceUpdate` channels are bounded — the muxer uses
/// `try_send` and drops-newest on overflow, since the next tick supersedes
/// the previous one. `Trade` / `OrderLifecycle` stay unbounded: missing a fill
/// or state transition permanently corrupts position tracking.
const BOOK_CHANNEL_CAPACITY: usize = 4_096;
const MARK_CHANNEL_CAPACITY: usize = 256;
/// Cap on remembered fill ids for replay dedup. A reconnect replays only a
/// small recent window, so this is far more than enough while bounding memory.
const MAX_SEEN_TRADE_IDS: usize = 8_192;
// Healthy routes on this server measured ~9 ms; delayed routes ~200 ms.
// These are transport probes, not trading parameters or order timeouts.
const FAST_WS_RTT: Duration = Duration::from_millis(50);
const PING_TIMEOUT: Duration = Duration::from_secs(1);
const PING_SPACING: Duration = Duration::from_millis(200);
const PROBE_INTERVAL: Duration = Duration::from_secs(10);
const SEARCH_INTERVAL: Duration = Duration::from_secs(60);

/// Bounded set of recently-seen ids with FIFO eviction. Used to drop fills
/// replayed across a reconnect (which would otherwise double-count the
/// position) without growing memory without bound.
pub(super) struct RecentIds {
    set: HashSet<String>,
    order: VecDeque<String>,
    cap: usize,
}

impl RecentIds {
    pub(super) fn new(cap: usize) -> Self {
        Self { set: HashSet::new(), order: VecDeque::new(), cap }
    }

    /// Record `id`; returns `true` if it's new, `false` if already seen.
    pub(super) fn insert(&mut self, id: &str) -> bool {
        if !self.set.insert(id.to_string()) {
            return false;
        }
        self.order.push_back(id.to_string());
        if self.order.len() > self.cap
            && let Some(evicted) = self.order.pop_front()
        {
            self.set.remove(&evicted);
        }
        true
    }
}
use crate::config::BulletConfig;
use crate::convert;

pub struct BulletFeeds {
    /// Complete top-of-book quotes, without depth deltas.
    pub bbo: MpscFeed<BookUpdate>,
    /// Ordered private messages for strategies that must wait for IOC completion.
    pub execution: mpsc::UnboundedReceiver<ExecutionUpdate>,
    pub trade: MpscFeed<Trade>,
    pub book: MpscFeed<BookUpdate>,
    pub lifecycle: MpscFeed<OrderLifecycle>,
    pub mark_price: MpscFeed<MarkPriceUpdate>,
}

/// Connect to Bullet and set up the demux task. Returns the broker and a
/// bundle of typed feeds to wire into the harness.
pub async fn connect(
    config: &BulletConfig,
    symbol: &str,
) -> Result<(BulletBroker, BulletFeeds), BotError> {
    connect_with_recording(config, symbol, None).await
}

/// Record market messages from this same managed connection when requested.
#[allow(clippy::too_many_lines)] // Connection and typed channel wiring stay together.
pub async fn connect_with_recording(
    config: &BulletConfig,
    symbol: &str,
    recording: Option<RecordingSender>,
) -> Result<(BulletBroker, BulletFeeds), BotError> {
    let secret = bb_core::keys::resolve_key_string(
        config.key_file.as_deref(),
        secrecy::ExposeSecret::expose_secret(&config.private_key),
    )?
    .ok_or_else(|| {
        BotError::config(
            "Bullet: no key material — set [exchanges.bullet].key_file, \
             BB_BULLET_KEY_FILE, private_key, or BB_BULLET_PRIVATE_KEY"
                .to_string(),
        )
    })?;
    let keypair = crate::key::keypair_from_secret(&secret)?;
    let network = match config.network.as_str() {
        "mainnet" => Network::Mainnet,
        "testnet" => Network::Testnet,
        other => {
            return Err(BotError::config(format!(
                "Unknown Bullet network '{other}' — use 'mainnet' or 'testnet'"
            )));
        }
    };

    let client = Client::builder()
        .network(network)
        .keypair(keypair)
        .user_actions(vec![
            UserActionDiscriminants::PlaceOrders,
            UserActionDiscriminants::CancelOrders,
            UserActionDiscriminants::CancelMarketOrders,
            UserActionDiscriminants::AmendOrders,
        ])
        .build()
        .await
        .map_err(|e| BotError::exchange(e, true))?;

    let address = client.address().map_err(|e| BotError::exchange(e, false))?;
    let account_address = crate::delegate::resolve_account_address(client.url(), &address).await?;
    let increments = load_increments(&client).await?;
    let client = Arc::new(client);

    // Set up WS + subscriptions.
    let ws = client.connect_ws_managed().call().await.map_err(|e| BotError::exchange(e, true))?;
    // Bullet user-order stream: address-prefixed topic (no listenKey flow).
    let user_topic = Topic::user_orders(account_address.clone()).to_string();
    // Both private sockets serve user events and RPCs. Public depth/trades
    // have their own three connections; replacing one cannot delay an exit.
    let topics = [Topic::mark_price(symbol), Topic::user_orders(account_address.clone())];
    let topics: Vec<String> = topics.iter().map(ToString::to_string).collect();
    ws.subscribe_raw(topics.clone(), None).map_err(|e| BotError::exchange(e, false))?;

    tracing::info!(
        symbol,
        signer = %address,
        account = %account_address,
        user_topic,
        symbols_known = client.symbols().len(),
        "Bullet: connected + subscribed"
    );

    // Typed channels (one per feed).
    // Trade and OrderLifecycle are unbounded — missing fills corrupts position tracking.
    // BookUpdate and MarkPriceUpdate are bounded — newest tick supersedes previous.
    let (execution_tx, execution_rx) = mpsc::unbounded_channel::<ExecutionUpdate>();
    let (trade_tx, trade_rx) = mpsc::unbounded_channel::<Trade>();
    let (bbo_tx, bbo_rx) = mpsc::channel::<BookUpdate>(BOOK_CHANNEL_CAPACITY);
    let (book_tx, book_rx) = mpsc::channel::<BookUpdate>(BOOK_CHANNEL_CAPACITY);
    let (life_tx, life_rx) = mpsc::unbounded_channel::<OrderLifecycle>();
    let (mark_tx, mark_rx) = mpsc::channel::<MarkPriceUpdate>(MARK_CHANNEL_CAPACITY);

    // Connection health flags shared with the broker. Muxer task flips them
    // on WS reconnect / permanent disconnect; strategies poll via the
    // `Broker::take_reconcile_signal` / `is_disconnected` trait methods.
    let health = Arc::new(ConnectionHealth::default());
    let (request_tx, request_rx) = mpsc::unbounded_channel();

    // Muxer task — reads WS, classifies, forwards. Holds `ws` so the connection
    // stays alive for the lifetime of the task.
    if let Some(recording) = &recording {
        recording.record("bullet", "connected", || {
            serde_json::to_string(
                &serde_json::json!({"network": config.network, "symbol": symbol, "depth": 20}),
            )
        });
    }
    let symbol = symbol.to_owned();
    let market_client = Arc::clone(&client);
    let market_health = Arc::clone(&health);
    let market_recording = recording.clone();
    let private_client = Arc::clone(&client);
    let private_health = Arc::clone(&health);
    tokio::spawn(async move {
        // Dropping this owner aborts the public pool and all SDK socket handles.
        let mut market = JoinSet::new();
        market.spawn(crate::market_data::run(
            market_client,
            symbol,
            bbo_tx,
            book_tx,
            market_health,
            market_recording,
        ));
        let private = muxer_loop(
            ws,
            FeedSenders {
                trade: trade_tx,
                execution: execution_tx,
                lifecycle: life_tx,
                mark: mark_tx,
            },
            private_health.clone(),
            recording,
            request_rx,
            private_client,
            topics,
        );
        tokio::pin!(private);
        tokio::select! {
            () = &mut private => {}
            result = market.join_next() => {
                // Loss of the public task must invalidate entries, while the
                // private transport remains available for protective closing.
                private_health.flag_market_reset(Instant::now());
                if let Some(Err(error)) = result {
                    tracing::error!(%error, "Bullet public task failed; private WS retained");
                }
                private.await;
            }
        }
    });

    let broker =
        BulletBroker::new(Arc::clone(&client), account_address, increments, health, request_tx);
    let feeds = BulletFeeds {
        bbo: MpscFeed::bounded(bbo_rx),
        execution: execution_rx,
        trade: MpscFeed::new(trade_rx),
        book: MpscFeed::bounded(book_rx),
        lifecycle: MpscFeed::new(life_rx),
        mark_price: MpscFeed::bounded(mark_rx),
    };
    Ok((broker, feeds))
}

struct FeedSenders {
    trade: mpsc::UnboundedSender<Trade>,
    execution: mpsc::UnboundedSender<ExecutionUpdate>,
    lifecycle: mpsc::UnboundedSender<OrderLifecycle>,
    mark: mpsc::Sender<MarkPriceUpdate>,
}

#[allow(clippy::too_many_lines)] // Two SDK handles, one request table and one fill consumer.
async fn muxer_loop(
    ws: ManagedWebsocket,
    tx: FeedSenders,
    health: Arc<ConnectionHealth>,
    recording: Option<RecordingSender>,
    mut requests: mpsc::UnboundedReceiver<WsRequest>,
    client: Arc<Client>,
    topics: Vec<String>,
) {
    let mut sockets = [Some(ws), None];
    let mut subscribed_at = [None; 2];
    let mut generation = [0_u64; 2];
    let mut last_order_event_time = [0_u64; 2];
    let mut seen_fills = RecentIds::new(MAX_SEEN_TRADE_IDS);
    let mut next_request_id = 0_u64;
    // Keep the original socket and SDK epoch. A reply on a later connection
    // may be an identical replay; an old queued reconnect cannot taint a NEW RPC.
    let mut pending: HashMap<RequestId, (WsReply, u64, CallMessage, Option<u64>, usize)> =
        HashMap::new();
    let mut active = 0;
    let mut ping: [Option<(RequestId, Instant)>; 2] = [None; 2];
    let mut ping_timed_out = [false; 2];
    let mut samples: [Vec<Duration>; 2] = std::array::from_fn(|_| Vec::with_capacity(4));
    let mut medians = [None; 2];
    let mut ping_at = [Instant::now() + PING_SPACING; 2];
    let mut search_at = Instant::now();
    let mut connecting = JoinSet::new();
    connect_private(&mut connecting, client.clone(), topics.clone(), 1);
    loop {
        // Use actual SDK transport state; old queued messages cannot revive a
        // disconnected route or delay one that has already reconnected.
        let online =
            sockets.each_ref().map(|ws| ws.as_ref().is_some_and(ManagedWebsocket::is_connected));
        pending.retain(|_, (reply, ..)| !reply.is_closed());
        for slot in 0..2 {
            if samples[slot].len() == 4 {
                let median = median_ping(&samples[slot]);
                medians[slot] = Some(median);
                samples[slot].clear();
                ping_at[slot] = Instant::now() + PROBE_INTERVAL;
                tracing::info!(
                    slot,
                    rtt_ms = median.as_secs_f64() * 1000.0,
                    "Bullet private WS ping median"
                );
            }
        }
        let other = 1 - active;
        if online[other]
            && (!online[active]
                || (!ping_timed_out[other]
                    && (ping_timed_out[active]
                        || medians[active]
                            .is_some_and(|rtt| medians[other].is_some_and(|other| other < rtt)))))
        {
            active = other;
            tracing::info!(
                slot = active,
                "Bullet private send route changed; pending RPCs retained"
            );
        }
        // Reuse the existing idle-maintenance command. It permits physical
        // replacement only; sending via the other route never waits for it.
        let replaceable = (0..2)
            .filter(|&slot| {
                medians[slot].is_some_and(|rtt| rtt > FAST_WS_RTT)
                    && online[1 - slot]
                    && subscribed_at[1 - slot].is_some()
                    && medians[1 - slot].is_some()
                    && !ping_timed_out[1 - slot]
                    && !pending.values().any(|(_, _, _, _, owner)| *owner == slot)
            })
            .max_by_key(|&slot| medians[slot]);
        let can_replace =
            replaceable.is_some() && connecting.is_empty() && Instant::now() >= search_at;
        health.set_replacement_ready(can_replace);
        let next_ping = (0..2)
            .filter(|&slot| online[slot])
            .map(|slot| ping_at[slot])
            .min()
            .unwrap_or_else(|| Instant::now() + PROBE_INTERVAL);
        let [a, b] = &mut sockets;
        let (slot, event) = tokio::select! {
            () = tokio::time::sleep_until(next_ping.into()) => {
                for slot in 0..2 {
                    if !online[slot] || Instant::now() < ping_at[slot] { continue; }
                    if ping[slot].take().is_some() {
                        // Route new commands away immediately, without closing
                        // this socket or disturbing any outstanding request.
                        // Forget its old speed: one later pong is not a new median.
                        ping_timed_out[slot] = true;
                        medians[slot] = None;
                        samples[slot].push(PING_TIMEOUT);
                        ping_at[slot] = Instant::now() + PING_SPACING;
                    } else {
                        next_request_id += 1;
                        let id = RequestId::new(next_request_id);
                        if sockets[slot].as_ref().is_some_and(|ws| ws.is_connected() && ws.ping(Some(id)).is_ok()) {
                            ping[slot] = Some((id, Instant::now()));
                            ping_at[slot] = Instant::now() + PING_TIMEOUT;
                        } else {
                            ping_at[slot] = Instant::now() + PROBE_INTERVAL;
                        }
                    }
                }
                continue;
            }
            result = connecting.join_next(), if !connecting.is_empty() => {
                if let Some(Ok((slot, ws))) = result {
                    sockets[slot] = Some(ws);
                    ping_at[slot] = Instant::now() + PING_SPACING;
                    tracing::info!(slot, generation = generation[slot], "Bullet private WS connected");
                }
                continue;
            }
            request = requests.recv() => {
                let Some(request) = request else { break };
                let (command, call, window, reply) = match request {
                    WsRequest::Submit { command, call, window, reply } => (command, call, window, reply),
                    WsRequest::RefreshConnection => {
                        // Caller has no unresolved orders; a known position is fine.
                        if can_replace
                            && let Some(slot) = replaceable
                            && sockets[1-slot].as_ref().is_some_and(ManagedWebsocket::is_connected)
                        {
                            active = 1-slot;
                            sockets[slot] = None; // At most TWO private sockets.
                            subscribed_at[slot] = None;
                            ping[slot] = None;
                            ping_timed_out[slot] = false;
                            samples[slot].clear();
                            medians[slot] = None;
                            last_order_event_time[slot] = 0;
                            generation[slot] += 1;
                            search_at = Instant::now() + SEARCH_INTERVAL;
                            health.set_replacement_ready(false);
                            connect_private(&mut connecting, client.clone(), topics.clone(), slot);
                            tracing::info!(slot, "Bullet private slow route replaced; other socket retained");
                            if let Some(recording) = &recording {
                                recording.record("bullet", "reconnecting", || serde_json::to_string(
                                    &serde_json::json!({"reason":"faster_ws", "connection":slot, "generation":generation[slot]})
                                ));
                            }
                        }
                        continue;
                    }
                    WsRequest::SubmissionWindow { client_id, reply } => {
                        let window = pending.values().find_map(|(reply, _, call, window, _)| {
                            if !reply.is_closed()
                                && let CallMessage::User(UserAction::PlaceOrders { orders, .. }) = call
                                && orders.iter().any(|o| o.reduce_only && o.client_order_id.as_ref().is_some_and(|id| id.0.to_string() == client_id))
                            { *window } else { None }
                        });
                        let _ = reply.send(window);
                        continue;
                    }
                };
                if reply.is_closed() { continue; }
                next_request_id += 1;
                let id = RequestId::new(next_request_id);
                let sent_at = Instant::now();
                let mut sent = Err("No connected private WS; request not queued".to_string());
                let mut owner = active;
                let mut sent_epoch = 0;
                for slot in [active, 1-active] {
                    let Some(ws) = &sockets[slot] else { continue };
                    // Recheck here: the SDK can detect a break while select is
                    // waiting, before its Reconnecting reaches this consumer.
                    let Some(epoch) = ws.connected_epoch() else { continue };
                    let result = match &command {
                        ClientMessage::OrderPlace { params, .. } => ws.order_place(params.tx.clone(), Some(id)),
                        ClientMessage::OrderCancel { params, .. } => ws.order_cancel(params.tx.clone(), Some(id)),
                        ClientMessage::OrderAmend { params, .. } => ws.order_amend(params.tx.clone(), Some(id)),
                        ClientMessage::OrderCancelAll { params, .. } => ws.order_cancel_all(params.tx.clone(), Some(id)),
                        _ => break,
                    };
                    owner = slot;
                    sent_epoch = epoch;
                    match result {
                        Ok(()) => { sent = Ok(()); break; }
                        // Busy/Stopped means NOT queued. Only this definite
                        // failure permits trying the exact same tx on the peer.
                        Err(error) => sent = Err(error.to_string()),
                    }
                }
                if let CallMessage::User(UserAction::PlaceOrders { market_id, orders, .. }) = &call {
                    for order in orders {
                        tracing::info!(connection = owner, generation = generation[owner], request_id = %id, market_id = ?market_id,
                            client_id = order.client_order_id.as_ref().map(|id| id.0),
                            reduce_only = order.reduce_only, side = ?order.side, order_type = ?order.order_type,
                            price = %order.price, qty = %order.size, window, queued = sent.is_ok(), "Bullet RPC request");
                    }
                } else {
                    tracing::info!(connection = owner, request_id = %id, ?call, queued = sent.is_ok(), "Bullet RPC request");
                }
                if let Some(recording) = &recording {
                    recording.record_at(sent_at, "bullet", "rpc.request", || serde_json::to_string(
                        &serde_json::json!({"id":id, "call":call, "window":window, "queued":sent.is_ok(),
                            "connection":owner, "generation":generation[owner], "error":sent.as_ref().err()})
                    ));
                }
                match sent {
                    Ok(()) => { pending.insert(id, (reply, sent_epoch, call, window, owner)); }
                    Err(error) => { let _ = reply.send(Err(BotError::strategy(format!("Bullet WS request not queued: {error}")))); }
                }
                continue;
            }
            event = receive_private(a) => (0, event),
            event = receive_private(b) => (1, event),
        };
        let received_at = Instant::now();
        // Check AFTER dequeue, before accepting a terminal status. A full SDK
        // queue may have dropped an earlier fill even on an open connection.
        let dropped = sockets[slot].as_ref().map_or(0, ManagedWebsocket::take_dropped_events);
        if dropped > 0 {
            health.flag_reconcile();
            // Data loss needs accounting recovery, but does not imply a replay
            // of a command. RPC epochs independently identify replies after a break.
            tracing::warn!(
                slot,
                dropped,
                "Bullet private SDK queue lost messages; requesting recovery"
            );
        }
        match event {
            Some(WsEvent::Message(msg)) => {
                if matches!(&*msg, ServerMessage::Tagged(TaggedMessage::Pong(_)))
                    && let Some((id, sent)) = ping[slot]
                    && msg.request_id() == Some(id)
                {
                    samples[slot].push(sent.elapsed());
                    ping[slot] = None;
                    // The last series may have ended with a timeout but a low
                    // median. Recovery needs a fresh series before speed ranking.
                    if ping_timed_out[slot] {
                        medians[slot] = None;
                    }
                    ping_timed_out[slot] = false;
                    ping_at[slot] = Instant::now() + PING_SPACING;
                    continue;
                }
                if let ServerMessage::Tagged(TaggedMessage::Subscribe(ack)) = &*msg
                    && ack.result == "success"
                {
                    subscribed_at[slot].get_or_insert(received_at);
                }
                let request_id =
                    msg.request_id().filter(|id| pending.get(id).is_some_and(|p| p.4 == slot));
                if let Some((reply, sent_epoch, ..)) = request_id.and_then(|id| pending.remove(&id))
                {
                    let received_epoch =
                        sockets[slot].as_ref().map_or(0, ManagedWebsocket::received_epoch);
                    let result = if received_epoch > sent_epoch && msg.is_error() {
                        Err(BotError::exchange(
                            format!(
                                "Bullet WS rejection after stream interruption; original outcome unknown: {msg:?}"
                            ),
                            true,
                        ))
                    } else {
                        // Keep the original for the audit record, but wake the
                        // request waiter BEFORE serialization/logging.
                        Ok((*msg).clone())
                    };
                    let _ = reply.send(result);
                } else {
                    match &*msg {
                        ServerMessage::MarkPrice(mp) => {
                            if slot == active
                                && let Ok(permit) = tx.mark.try_reserve()
                                && let Some(event) = convert::mark_price_to_event(mp)
                            {
                                permit.send(event);
                            }
                        }
                        ServerMessage::OrderUpdate(update) => {
                            let trade = convert::order_update_to_trade(update);
                            if matches!(update.order, OrderUpdateData::TradeFill(_))
                                && trade.is_none()
                            {
                                health.flag_reconcile();
                            }
                            let trade = trade.filter(|trade| {
                                trade.trade_id.as_ref().is_none_or(|id| seen_fills.insert(id))
                            });
                            let lifecycle = convert::order_update_to_lifecycle(update);
                            if let Some(trade) = &trade {
                                let _ = tx.trade.send(trade.clone());
                            }
                            let _ = tx.lifecycle.send(lifecycle.clone());
                            let _ = tx.execution.send(ExecutionUpdate {
                                trade,
                                lifecycle,
                                received_at,
                                // Unacknowledged subscription cannot prove
                                // completeness for an already-submitted order.
                                stream_started_at: Some(subscribed_at[slot].unwrap_or(received_at)),
                            });
                            let common = match &update.order {
                                OrderUpdateData::TradeFill(data) => &data.common,
                                OrderUpdateData::PlaceOrder(data) => &data.common,
                                OrderUpdateData::Cancel(data) => &data.common,
                            };
                            tracing::info!(connection = slot, generation = generation[slot],
                                client_id = common.client_order_id, order_id = common.order_id,
                                symbol = %common.symbol, status = %common.status, execution_type = %common.execution_type,
                                event_time_us = update.event_time, transaction_time_us = common.transaction_time,
                                "Bullet private order status");
                            if update.event_time < last_order_event_time[slot] {
                                tracing::warn!(
                                    slot,
                                    previous = last_order_event_time[slot],
                                    current = update.event_time,
                                    "OrderUpdate event_time regressed on source — out-of-order or replay"
                                );
                            }
                            last_order_event_time[slot] =
                                last_order_event_time[slot].max(update.event_time);
                        }
                        ServerMessage::Unknown(error, raw)
                            if serde_json::from_str::<serde_json::Value>(raw)
                                .is_ok_and(|value| value["e"] == "ORDER_TRADE_UPDATE") =>
                        {
                            health.flag_reconcile();
                            tracing::warn!(slot, %error, "Bullet private update could not be decoded; requesting recovery");
                        }
                        _ => {}
                    }
                }
                log_rpc_reply(&msg);
                if let Some(recording) = &recording {
                    record_ws_message_at(
                        recording,
                        &msg,
                        received_at,
                        Some((slot, generation[slot])),
                    );
                }
            }
            disconnected => {
                subscribed_at[slot] = None;
                ping[slot] = None;
                ping_timed_out[slot] = false;
                samples[slot].clear();
                medians[slot] = None;
                last_order_event_time[slot] = 0;
                generation[slot] += 1;
                // One continuously subscribed peer preserves private coverage.
                // If both are lost, existing recovery resolves affected orders.
                if subscribed_at.iter().all(Option::is_none) {
                    health.flag_reconcile();
                }
                if let Some(recording) = &recording {
                    recording.record_at(received_at, "bullet", "reconnecting", || {
                        serde_json::to_string(
                            &serde_json::json!({"connection":slot,"generation":generation[slot]}),
                        )
                    });
                }
                tracing::warn!(
                    slot,
                    "Bullet private reconnecting; peer and protective RPC path retained"
                );
                if !matches!(disconnected, Some(WsEvent::Reconnecting)) {
                    sockets[slot] = None;
                    connect_private(&mut connecting, client.clone(), topics.clone(), slot);
                }
                ping_at[slot] = Instant::now() + PING_SPACING;
            }
        }
        tokio::task::consume_budget().await;
    }
    health.set_replacement_ready(false);
}

async fn receive_private(ws: &mut Option<ManagedWebsocket>) -> Option<WsEvent> {
    match ws {
        Some(ws) => ws.recv().await,
        None => std::future::pending().await,
    }
}

fn connect_private(
    connecting: &mut JoinSet<(usize, ManagedWebsocket)>,
    client: Arc<Client>,
    topics: Vec<String>,
    slot: usize,
) {
    connecting.spawn(async move {
        loop {
            if let Ok(Ok(ws)) =
                tokio::time::timeout(Duration::from_secs(5), client.connect_ws_managed().call())
                    .await
                && ws.subscribe_raw(topics.clone(), None).is_ok()
            {
                return (slot, ws);
            }
            tracing::warn!(slot, "Bullet private connect failed; peer retained, retrying");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
}

/// Discard the warmup sample; a single spike must not cause a handover.
fn median_ping(samples: &[Duration]) -> Duration {
    let mut measured = [samples[1], samples[2], samples[3]];
    measured.sort_unstable();
    measured[1]
}

fn log_rpc_reply(msg: &ServerMessage) {
    match msg {
        ServerMessage::Tagged(
            TaggedMessage::OrderPlace(r)
            | TaggedMessage::OrderCancel(r)
            | TaggedMessage::OrderAmend(r)
            | TaggedMessage::OrderCancelAll(r),
        ) => {
            tracing::info!(request_id = r.id.as_ref().map(RequestId::as_u64), event_time_us = r.event_time,
                tx_id = %r.results.tx_id, status = ?r.results.status,
                client_ids = ?r.results.client_order_ids, order_ids = ?r.results.order_ids,
                "Bullet RPC reply");
        }
        ServerMessage::Tagged(TaggedMessage::Error(e)) | ServerMessage::Error(e) => {
            // Strategy classifies the outcome; transport logging also keeps late replies.
            tracing::info!(request_id = e.id.as_ref().map(RequestId::as_u64), event_time = e.event_time,
                code = e.error.code(), error = %e.error, "Bullet RPC error reply");
        }
        _ => {}
    }
}

#[cfg(test)]
fn record_ws_message(recording: &RecordingSender, msg: &ServerMessage) {
    record_ws_message_at(recording, msg, Instant::now(), None);
}

fn record_ws_message_at(
    recording: &RecordingSender,
    msg: &ServerMessage,
    received_at: Instant,
    source: Option<(usize, u64)>,
) {
    let kind = match msg {
        ServerMessage::DepthUpdate(_) => "depthUpdate",
        ServerMessage::BookTicker(_) => "bookTicker",
        ServerMessage::AggTrade(_) => "aggTrade",
        ServerMessage::OrderUpdate(_) => "ORDER_TRADE_UPDATE",
        ServerMessage::Tagged(TaggedMessage::OrderPlace(_)) => "order.place",
        ServerMessage::Tagged(TaggedMessage::OrderCancel(_)) => "order.cancel",
        ServerMessage::Tagged(TaggedMessage::OrderAmend(_)) => "order.amend",
        ServerMessage::Tagged(TaggedMessage::OrderCancelAll(_)) => "order.cancelAll",
        ServerMessage::Tagged(TaggedMessage::Error(_)) | ServerMessage::Error(_) => "error",
        ServerMessage::Unknown(..) => "unknown",
        _ => return,
    };
    recording.record_at(received_at, "bullet", kind, || {
        // Borrow the SDK message and stream it directly into the existing JSON
        // layout; no intermediate Value tree or copies of its strings.
        #[derive(serde::Serialize)]
        struct Record<'a> {
            #[serde(flatten)]
            message: Option<&'a ServerMessage>,
            #[serde(skip_serializing_if = "Option::is_none")]
            error: Option<&'a str>,
            #[serde(skip_serializing_if = "Option::is_none")]
            raw: Option<&'a str>,
            #[serde(skip_serializing_if = "Option::is_none")]
            connection: Option<usize>,
            #[serde(skip_serializing_if = "Option::is_none")]
            generation: Option<u64>,
        }
        let (message, error, raw) = match msg {
            ServerMessage::Unknown(error, raw) => (None, Some(error.as_str()), Some(raw.as_str())),
            _ => (Some(msg), None, None),
        };
        serde_json::to_string(&Record {
            message,
            error,
            raw,
            connection: source.map(|(slot, _)| slot),
            generation: source.map(|(_, generation)| generation),
        })
    });
}

#[cfg(test)]
#[path = "ws_tests.rs"]
mod ws_tests;

#[cfg(test)]
mod tests {
    use super::{
        Instant, RecentIds, RecordingSender, ServerMessage, record_ws_message, record_ws_message_at,
    };

    #[tokio::test]
    async fn sdk_market_messages_retain_times_prices_and_quantities() {
        let (sender, mut records) = RecordingSender::channel(8);
        let frames = [
            r#"{"e":"depthUpdate","E":1779600413218736,"T":1779600413211110,"s":"BTC-USD","U":1,"u":2,"pu":0,"b":[["67001","0.01"]],"a":[["67002","0.02"]],"mt":"s"}"#,
            r#"{"e":"bookTicker","E":1779600413218736,"T":1779600413211110,"s":"BTC-USD","u":2,"b":"67001","B":"0.01","a":"67002","A":"0.02","mt":"u"}"#,
            r#"{"e":"aggTrade","E":1779600413218736,"T":1779600413211110,"s":"BTC-USD","a":42,"p":"67001","q":"0.01","f":1,"l":1,"m":false,"th":"test"}"#,
        ];
        for raw in frames {
            let msg: ServerMessage = serde_json::from_str(raw).unwrap();
            record_ws_message(&sender, &msg);
            let record = records.try_recv().unwrap().unwrap();
            let original: serde_json::Value = serde_json::from_str(raw).unwrap();
            let saved: serde_json::Value = serde_json::from_str(&record.payload).unwrap();
            assert_eq!(record.source, "bullet");
            assert_eq!(record.kind, original["e"]);
            assert_eq!(saved, original);
        }
        let control: ServerMessage = serde_json::from_str(r#"{"e":"pong","id":1,"E":1}"#).unwrap();
        record_ws_message(&sender, &control);
        assert!(records.try_recv().is_err());
    }

    #[tokio::test]
    async fn rpc_replies_and_undecodable_messages_retain_details() {
        let (sender, mut records) = RecordingSender::channel(8);
        for event in ["order.place", "order.cancel", "order.amend", "order.cancelAll", "error"] {
            let raw = if event == "error" {
                serde_json::json!({"e":event,"id":42,"E":1_779_600_413_218_736_u64,
                    "error":{"code":-2010,"msg":"size_must_be_positive"}})
            } else {
                serde_json::json!({"e":event,"id":42,"E":1_779_600_413_218_736_u64,
                    "results":{"tx_id":"test-tx","status":"submitted",
                        "order_ids":[100],"client_order_ids":[10]}})
            };
            let msg: ServerMessage = serde_json::from_value(raw.clone()).unwrap();
            for source in [None, Some((1, 7))] {
                record_ws_message_at(&sender, &msg, Instant::now(), source);
                let record = records.try_recv().unwrap().unwrap();
                assert_eq!(record.kind, event);
                let mut expected = raw.clone();
                if let Some((slot, generation)) = source {
                    expected["connection"] = slot.into();
                    expected["generation"] = generation.into();
                }
                assert_eq!(
                    serde_json::from_str::<serde_json::Value>(&record.payload).unwrap(),
                    expected
                );
            }
        }
        let raw = r#"{"e":"ORDER_TRADE_UPDATE","o":{"X":"EXPIRED","new_schema":true}}"#;
        for source in [None, Some((0, 9))] {
            record_ws_message_at(
                &sender,
                &ServerMessage::Unknown("decode failure\n\"quoted\"".into(), raw.into()),
                Instant::now(),
                source,
            );
            let record = records.try_recv().unwrap().unwrap();
            assert_eq!(record.kind, "unknown");
            let mut expected = serde_json::json!({"raw":raw,"error":"decode failure\n\"quoted\""});
            if let Some((slot, generation)) = source {
                expected["connection"] = slot.into();
                expected["generation"] = generation.into();
            }
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&record.payload).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn recent_ids_dedups_exact_repeats() {
        let mut seen = RecentIds::new(4);
        assert!(seen.insert("a"), "first sighting is new");
        assert!(!seen.insert("a"), "exact repeat is a duplicate");
        assert!(seen.insert("b"), "different id is new");
    }

    #[test]
    fn recent_ids_evicts_oldest_past_cap() {
        let mut seen = RecentIds::new(2);
        seen.insert("a");
        seen.insert("b");
        seen.insert("c"); // evicts "a"
        assert!(seen.insert("a"), "evicted id is treated as new again");
        // memory stays bounded at the cap
        assert!(seen.set.len() <= 2);
    }
}
