//! One-off MAINNET measurement: 20 BTC buys at 60,000, each immediately cancelled.
//! Uses the minimum `LOT_SIZE` and the existing `BB_BULLET_KEY_FILE` variable.
//! Run `cargo run -p bb-exchange-bullet --example order_latency -- rest` (or `ws`).
//! JSONL preserves local microsecond timestamps, full replies and exchange order history.
//! `ws-taker` runs four minimum-size IOC buys, each followed by a reduce-only IOC sell.
//! `ws-ping` only measures SDK connections and public messages; it never loads a key.

use std::error::Error;
use std::path::Path;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bb_core::helpers::ClientIdIssuer;
use bb_core::types::OrderStatus;
use bb_exchange_bullet::{convert, delegate, key};
use bullet_rust_sdk::codegen::types::{Filter, SubmitTxResponse, TxResult, TxStatus};
use bullet_rust_sdk::types::{OrderUpdateData, RequestId, UserAction};
use bullet_rust_sdk::ws::models::{ServerMessage, TaggedMessage};
use bullet_rust_sdk::{
    CallMessage, CancelOrderArgs, Client, ClientOrderId, Network, NewOrderArgs, NewOrderExt,
    OrderId, PositiveDecimal, Side, Topic, Transaction, UserActionDiscriminants, WebsocketHandle,
};
use rust_decimal::Decimal;
use serde_json::json;

const SYMBOL: &str = "BTC-USD";
const SAMPLES: usize = 20;

// One order at a time. The receipt and the fill stream are timed independently;
// neither acknowledgement nor successful submission is treated as a fill.
#[allow(clippy::too_many_lines)]
async fn taker_order(
    client: &Client,
    ws: &mut WebsocketHandle,
    client_id: &str,
    quantity: Decimal,
    tick: Decimal,
    close: bool,
) -> Result<(Decimal, u64), Box<dyn Error>> {
    let book = client.order_book(Some(5), SYMBOL).await?.into_inner();
    let touch: Decimal = if close { &book.bids } else { &book.asks }
        .first()
        .and_then(|level| level.first())
        .ok_or("Empty BTC BBO")?
        .parse()?;
    // Same protected-market convention as the bot: IOC, 50 bp price allowance.
    let price = touch * if close { Decimal::new(995, 3) } else { Decimal::new(1005, 3) };
    let price = if close { (price / tick).ceil() } else { (price / tick).floor() } * tick;
    let cid = ClientOrderId(client_id.parse()?);
    let request_id = RequestId::new(cid.0);
    let mut order = NewOrderArgs::ioc(
        PositiveDecimal::try_from(price)?,
        PositiveDecimal::try_from(quantity)?,
        if close { Side::Ask } else { Side::Bid },
    );
    order.client_order_id = Some(cid);
    order.reduce_only = close;
    let message = CallMessage::User(UserAction::PlaceOrders {
        market_id: client.market_id(SYMBOL).ok_or("BTC market missing")?,
        orders: vec![order],
        replace: false,
        sub_account_index: None,
    });
    println!(
        "{}",
        json!({"event":"ioc_intent", "operation":if close {"close"} else {"open"},
        "client_id":client_id,"request_id":request_id,"quantity":quantity,"touch":touch,
        "limit_price":price,"reduce_only":close,"unix_us":unix_us()})
    );
    let begin_us = unix_us();
    let start = Instant::now();
    let signed = Transaction::builder().call_message(message).client(client).build()?;
    let signed_us = unix_us();
    ws.place_order(&signed, Some(request_id)).await?;
    let sent_us = unix_us();
    println!(
        "{}",
        json!({"event":"ioc_sent", "client_id":client_id,"request_id":request_id,
        "begin_unix_us":begin_us,"signed_unix_us":signed_us,"ws_sent_unix_us":sent_us,
        "elapsed_ms":start.elapsed().as_secs_f64()*1000.0})
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut acknowledged = false;
    let mut terminal = false;
    let mut order_id = None;
    let mut filled = Decimal::ZERO;
    let mut seen = Vec::new();
    while !acknowledged || !terminal {
        let event = tokio::time::timeout_at(deadline, ws.recv()).await??;
        let received_us = unix_us();
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        println!(
            "{}",
            json!({"event":"ioc_ws", "client_id":client_id,
            "received_unix_us":received_us,"elapsed_ms":elapsed_ms,"message":event})
        );
        if let ServerMessage::OrderUpdate(update) = &event {
            let lifecycle = convert::order_update_to_lifecycle(update);
            if lifecycle.order.client_id.as_deref() == Some(client_id) {
                order_id = Some(lifecycle.order.id.parse()?);
                terminal = matches!(
                    lifecycle.order.status,
                    OrderStatus::Filled | OrderStatus::Cancelled | OrderStatus::Rejected
                );
                if let OrderUpdateData::TradeFill(fill) = &update.order
                    && !seen.contains(&fill.trade_id)
                {
                    seen.push(fill.trade_id);
                    filled += fill.last_filled_qty.parse::<Decimal>()?;
                    if fill.is_maker {
                        return Err("Expected a taker fill".into());
                    }
                }
            }
        }
        if event.request_id() == Some(request_id) {
            match &event {
                ServerMessage::Tagged(TaggedMessage::OrderPlace(reply))
                    if reply.results.status.is_success() =>
                {
                    acknowledged = true;
                    order_id = order_id.or_else(|| reply.results.order_ids.first().copied());
                }
                _ => return Err(format!("IOC submission not confirmed: {event:?}").into()),
            }
        }
    }
    Ok((filled, order_id.ok_or("IOC has no exchange order ID")?))
}

#[allow(clippy::too_many_lines)]
async fn taker_series(
    client: &Client,
    account: &str,
    quantity: Decimal,
    tick: Decimal,
) -> Result<(), Box<dyn Error>> {
    let before = client.account_info(account).await?.into_inner();
    let position_before = before
        .positions
        .iter()
        .find(|p| p.symbol == SYMBOL)
        .map_or(Decimal::ZERO, |p| p.position_amt);
    if !position_before.is_zero()
        || !client.query_open_orders(account, Some(SYMBOL)).await?.is_empty()
    {
        return Err("BTC must be flat and have no existing orders before this experiment".into());
    }
    println!(
        "{}",
        json!({"event":"taker_start","unix_us":unix_us(),"symbol":SYMBOL,
        "quantity":quantity,"round_trips":4,"transport":"ws","build":if cfg!(debug_assertions){"dev"}else{"release"},
        "position_before":position_before,"path":"SDK raw WS measurement example, not the strategy/harness"})
    );
    clock_samples(client, "before").await?;
    let mut ws = client.connect_ws().call().await?;
    ws.subscribe([Topic::user_orders(account)], Some(RequestId::new(1))).await?;
    loop {
        let event = tokio::time::timeout(Duration::from_secs(10), ws.recv()).await??;
        println!(
            "{}",
            json!({"event":"subscription","received_unix_us":unix_us(),"message":event})
        );
        if event.request_id() == Some(RequestId::new(1)) {
            if event.is_error() {
                return Err("User-order subscription failed".into());
            }
            break;
        }
    }
    let mut ids = ClientIdIssuer::session_seeded();
    let mut venue_ids = Vec::new();
    let result: Result<(),Box<dyn Error>> = async {
        for sample in 1..=4 {
            let cid = ids.issue();
            let (filled, id) = taker_order(client, &mut ws, &cid, quantity, tick, false).await?;
            venue_ids.push(id);
            if filled.is_zero() { return Err("Opening IOC was empty; no additional entry will be sent".into()); }
            let cid = ids.issue();
            let (closed, id) = taker_order(client, &mut ws, &cid, filled, tick, true).await?;
            venue_ids.push(id);
            println!("{}",json!({"event":"round_trip","sample":sample,"opened":filled,"closed":closed,"unix_us":unix_us()}));
            if closed != filled { return Err("Closing IOC left a remainder".into()); }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Ok(())
    }.await;

    // On failure, do not open again. Inspect the account before returning so the
    // operator can close a test remainder; never silently declare success.
    let mut rows = Vec::new();
    let mut trades = Vec::new();
    let mut position_after = Decimal::ZERO;
    for _ in 0..20 {
        rows = client
            .all_orders(Some(account), None, Some(1000), None, None, Some(SYMBOL))
            .await?
            .into_inner()
            .data
            .into_iter()
            .filter(|o| venue_ids.contains(&o.order_id))
            .collect::<Vec<_>>();
        trades = client
            .user_trades(Some(account), None, Some(false), Some(1000), None, Some(SYMBOL))
            .await?
            .into_inner()
            .data
            .into_iter()
            .filter(|t| venue_ids.contains(&t.order_id))
            .collect::<Vec<_>>();
        let after = client.account_info(account).await?.into_inner();
        position_after = after
            .positions
            .iter()
            .find(|p| p.symbol == SYMBOL)
            .map_or(Decimal::ZERO, |p| p.position_amt);
        if rows.len() == venue_ids.len()
            && venue_ids.iter().all(|id| trades.iter().any(|t| t.order_id == *id))
            && position_after.is_zero()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let remaining = client.query_open_orders(account, Some(SYMBOL)).await?.len();
    println!(
        "{}",
        json!({"event":"taker_verification","position_before":position_before,
        "position_after":position_after,"remaining_open":remaining,"orders":rows,"trades":trades,
        "result":result.as_ref().map_err(ToString::to_string)})
    );
    clock_samples(client, "after").await?;
    result?;
    if rows.len() != 8
        || !position_after.is_zero()
        || remaining != 0
        || rows.iter().any(|o| o.status != "FILLED")
        || trades.iter().any(|t| t.maker)
    {
        return Err("Expected eight filled IOC orders and a flat BTC account".into());
    }
    Ok(())
}

fn successful(response: &SubmitTxResponse) -> bool {
    response.status != TxStatus::Dropped
        && response.receipt.as_ref().is_some_and(|r| r.result == TxResult::Successful)
}

fn unix_us() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_micros()
}

// Use the same managed transport as the bot, without signing or trading.
async fn ws_ping_series() -> Result<(), Box<dyn Error>> {
    let client = Client::mainnet().await?;
    for connection in 0..8_u64 {
        let started = Instant::now();
        let mut ws = client.connect_ws_managed().call().await?;
        ws.subscribe([Topic::book_ticker("LIT-USD")], None)?;
        println!(
            "{}",
            json!({"event":"connected", "connection":connection,
            "pid":std::process::id(), "unix_us":unix_us(),
            "connect_ms":started.elapsed().as_secs_f64()*1000.0})
        );
        for sample in 0..10_u64 {
            let id = RequestId::new(10_000 + connection * 100 + sample);
            let sent_us = unix_us();
            let start = Instant::now();
            ws.ping(Some(id))?;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                let event = tokio::time::timeout_at(deadline, ws.recv())
                    .await?
                    .ok_or("SDK probe connection ended")?;
                let received_us = unix_us();
                match event {
                    bullet_rust_sdk::WsEvent::Message(message) => {
                        let matched = message.request_id() == Some(id);
                        println!(
                            "{}",
                            json!({"event":"probe_message", "connection":connection,
                            "sample":sample, "received_unix_us":received_us,
                            "sent_unix_us":sent_us, "rtt_ms":matched.then(|| start.elapsed().as_secs_f64()*1000.0),
                            "message":message})
                        );
                        if matched {
                            break;
                        }
                    }
                    event => return Err(format!("SDK probe interrupted: {event:?}").into()),
                }
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        // Socket diagnostics belong to this one-off experiment, never the trading loop.
        let sockets = std::process::Command::new("ss").args(["-ntpi"]).output()?;
        let sockets = String::from_utf8_lossy(&sockets.stdout);
        let mut lines = sockets.lines();
        let mut own = Vec::new();
        while let Some(line) = lines.next() {
            if line.contains(&format!("pid={},", std::process::id())) {
                own.push(line);
                if let Some(tcp_info) = lines.next() {
                    own.push(tcp_info);
                }
            }
        }
        println!(
            "{}",
            json!({"event":"tcp_info", "connection":connection,
            "unix_us":unix_us(), "sockets":own})
        );
        drop(ws);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(())
}

// Keep clock samples as observations, not an assumed exact one-way latency correction.
async fn clock_samples(client: &Client, phase: &str) -> Result<(), Box<dyn Error>> {
    for _ in 0..5 {
        let begin_us = unix_us();
        let start = Instant::now();
        let server_us = client.time().await?.into_inner().server_time;
        let elapsed_us = start.elapsed().as_micros();
        let received_us = unix_us();
        println!(
            "{}",
            json!({"event": "clock", "phase": phase,
            "begin_unix_us": begin_us, "received_unix_us": received_us,
            "server_unix_us": server_us, "elapsed_us": elapsed_us})
        );
    }
    Ok(())
}

// Same SDK signing path for both transports. WS send completion alone is not an acknowledgement.
async fn submit(
    client: &Client,
    ws: &mut Option<WebsocketHandle>,
    message: CallMessage,
    request_id: RequestId,
) -> Result<(bool, Option<u64>), Box<dyn Error>> {
    let cancel = matches!(&message, CallMessage::User(UserAction::CancelOrders { .. }));
    let begin_us = unix_us();
    let start = Instant::now();
    let signed = Transaction::builder().call_message(message).client(client).build()?;
    let signed_us = unix_us();
    let sign_ms = start.elapsed().as_secs_f64() * 1000.0;
    let mut received_us = 0;
    let mut elapsed_ms = 0.0;
    let mut ws_sent_us = None;
    let result: Result<_, Box<dyn Error>> = async {
        if let Some(ws) = ws {
            if cancel {
                ws.cancel_order(&signed, Some(request_id)).await?;
            } else {
                ws.place_order(&signed, Some(request_id)).await?;
            }
            ws_sent_us = Some(unix_us());
            loop {
                let reply = tokio::time::timeout(Duration::from_secs(10), ws.recv()).await??;
                let at = unix_us();
                let elapsed = start.elapsed().as_secs_f64() * 1000.0;
                if let ServerMessage::Unknown(error, raw) = &reply {
                    println!(
                        "{}",
                        json!({"event": "unparsed_ws", "received_unix_us": at,
                        "error": error, "raw": raw})
                    );
                    return Err("Unparsed WS response".into());
                }
                if reply.request_id() != Some(request_id) {
                    continue;
                }
                received_us = at;
                elapsed_ms = elapsed;
                let (ok, id) = match &reply {
                    ServerMessage::Tagged(TaggedMessage::OrderPlace(r)) if !cancel => {
                        (r.results.status.is_success(), r.results.order_ids.first().copied())
                    }
                    ServerMessage::Tagged(TaggedMessage::OrderCancel(r)) if cancel => {
                        (r.results.status.is_success(), r.results.order_ids.first().copied())
                    }
                    _ => (false, None),
                };
                return Ok((ok, id, serde_json::to_value(reply)?));
            }
        }
        let reply = client.send_transaction(&signed).await?;
        received_us = unix_us();
        elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        let id = reply
            .events
            .iter()
            .filter_map(|e| convert::place_order_ids(&e.value))
            .find_map(|(_, id)| id.parse::<u64>().ok());
        Ok((successful(&reply), id, serde_json::to_value(reply)?))
    }
    .await;
    let finished_us = unix_us();
    if received_us == 0 {
        elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    }
    println!(
        "{}",
        json!({"event": "operation", "operation": if cancel {"cancel"} else {"place"},
        "request_id": request_id, "begin_unix_us": begin_us, "signed_unix_us": signed_us,
        "ws_sent_unix_us": ws_sent_us, "received_unix_us": (received_us != 0).then_some(received_us),
        "finished_unix_us": finished_us,
        "sign_ms": sign_ms, "elapsed_ms": elapsed_ms,
        "result": result.as_ref().map(|(_, _, raw)| raw).map_err(ToString::to_string)})
    );
    result.map(|(ok, id, _)| (ok, id))
}

#[tokio::main]
#[allow(clippy::too_many_lines)] // Keep this one-off sequential experiment in one place.
async fn main() -> Result<(), Box<dyn Error>> {
    let transport = std::env::args().nth(1).ok_or("Specify rest, ws, ws-taker or ws-ping")?;
    if transport == "ws-ping" {
        return ws_ping_series().await;
    }
    if !["rest", "ws", "ws-taker"].contains(&transport.as_str()) {
        return Err("Specify rest, ws, ws-taker or ws-ping".into());
    }
    let key_file = std::env::var("BB_BULLET_KEY_FILE")?;
    let client = Client::builder()
        .network(Network::Mainnet)
        .keypair(key::keypair_from_key_file(Path::new(&key_file))?)
        .user_actions(vec![
            UserActionDiscriminants::PlaceOrders,
            UserActionDiscriminants::CancelOrders,
            UserActionDiscriminants::CancelMarketOrders,
            UserActionDiscriminants::AmendOrders,
        ])
        .build()
        .await?;
    let account = delegate::resolve_account_address(client.url(), &client.address()?).await?;
    let market_id = client.market_id(SYMBOL).ok_or("BTC market missing")?;
    let info = client.exchange_info().await?.into_inner();
    let symbol = info.symbols.iter().find(|s| s.symbol == SYMBOL).ok_or("BTC filters missing")?;
    let quantity = symbol
        .filters
        .iter()
        .find_map(|f| match f {
            Filter::LotSize { min_qty, step_size, .. } => Some((*min_qty).max(*step_size)),
            _ => None,
        })
        .ok_or("LOT_SIZE missing")?;
    if transport == "ws-taker" {
        let tick = symbol
            .filters
            .iter()
            .find_map(|f| match f {
                Filter::PriceFilter { tick_size, .. } => Some(*tick_size),
                _ => None,
            })
            .ok_or("PRICE_FILTER missing")?;
        return taker_series(&client, &account, quantity, tick).await;
    }
    let price = Decimal::from(60_000);
    let before = client.account_info(&account).await?.into_inner();
    let position_before =
        before.positions.iter().find(|p| p.symbol == SYMBOL).map(|p| p.position_amt);
    println!(
        "{}",
        json!({
            "event": "start", "unix_us": unix_us(),
            "transport": transport, "sdk": "0.0.43", "symbol": SYMBOL, "price": price,
            "quantity": quantity, "samples": SAMPLES, "position_before": position_before,
            "timing": "SDK Transaction::builder through matching complete response; reused connection",
            "build": if cfg!(debug_assertions) { "dev" } else { "release" },
        })
    );

    clock_samples(&client, "before").await?;
    let mut ws = if transport == "ws" { Some(client.connect_ws().call().await?) } else { None };

    let mut ids = ClientIdIssuer::session_seeded();
    let mut issued = Vec::new();
    let mut venue_ids = Vec::new();
    let result: Result<(), Box<dyn Error>> = async {
        for sample in 1..=SAMPLES {
            // Outside the timed region: confirm the requested buy still rests below the market.
            let book = client.order_book(Some(5), SYMBOL).await?.into_inner();
            let bid: Decimal =
                book.bids.first().and_then(|l| l.first()).ok_or("No BTC bid")?.parse()?;
            if price >= bid {
                return Err("60,000 is no longer below the BTC bid; no order sent".into());
            }
            let client_id = ids.issue();
            let cid = ClientOrderId(client_id.parse()?);
            let mut order = NewOrderArgs::limit(
                PositiveDecimal::try_from(price)?,
                PositiveDecimal::try_from(quantity)?,
                Side::Bid,
            );
            order.client_order_id = Some(cid);
            issued.push(client_id.clone());
            let placed = submit(
                &client,
                &mut ws,
                CallMessage::User(UserAction::PlaceOrders {
                    market_id,
                    orders: vec![order],
                    replace: false,
                    sub_account_index: None,
                }),
                RequestId::new(cid.0 * 2),
            )
            .await;

            // Cancel even if the placement response was lost. Never cancel unrelated orders.
            let order_id = placed.as_ref().ok().and_then(|(_, id)| *id);
            if let Some(id) = order_id {
                venue_ids.push(id);
            }
            let cancel = CancelOrderArgs {
                order_id: order_id.map(OrderId),
                client_order_id: order_id.is_none().then_some(cid),
            };
            let cancelled = submit(
                &client,
                &mut ws,
                CallMessage::User(UserAction::CancelOrders {
                    market_id,
                    orders: vec![cancel],
                    sub_account_index: None,
                }),
                RequestId::new(cid.0 * 2 + 1),
            )
            .await;
            println!(
                "{}",
                json!({
                    "event": "sample", "sample": sample, "unix_us": unix_us(),
                    "client_id": client_id, "order_id": order_id, "bid": bid,
                    "place": placed.as_ref().map_err(ToString::to_string),
                    "cancel": cancelled.as_ref().map_err(ToString::to_string),
                })
            );
            let placed = placed?;
            let cancelled = cancelled?;
            if !placed.0 || order_id.is_none() || !cancelled.0 {
                return Err("Placement/cancellation not confirmed; stopping measurement".into());
            }
            let open = client.query_open_orders(&account, Some(SYMBOL)).await?.into_inner();
            if open.iter().any(|o| o.client_order_id.as_ref().is_some_and(|id| issued.contains(id)))
            {
                return Err("Measured order still open after cancellation".into());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Ok(())
    }
    .await;

    // Also runs on measurement failure. Only this run's orders are eligible for cleanup.
    let open = client.query_open_orders(&account, Some(SYMBOL)).await?.into_inner();
    let leftovers: Vec<_> = open
        .iter()
        .filter(|o| o.client_order_id.as_ref().is_some_and(|id| issued.contains(id)))
        .map(|o| CancelOrderArgs { order_id: Some(OrderId(o.order_id)), client_order_id: None })
        .collect();
    if !leftovers.is_empty() {
        let cleanup = client.cancel_orders(market_id, leftovers, None).await?;
        println!("{}", json!({"event": "cleanup", "response": cleanup}));
        if !successful(&cleanup) {
            return Err("Cleanup not confirmed; inspect recorded order IDs".into());
        }
    }
    let open = client.query_open_orders(&account, Some(SYMBOL)).await?.into_inner();
    let remaining = open
        .iter()
        .filter(|o| o.client_order_id.as_ref().is_some_and(|id| issued.contains(id)))
        .count();
    // Order history trails the transaction receipt; this wait is not part of either timing.
    let mut rows = Vec::new();
    for _ in 0..20 {
        let history = client
            .all_orders(Some(&account), None, Some(1000), None, None, Some(SYMBOL))
            .await?
            .into_inner();
        rows = history.data.into_iter().filter(|o| venue_ids.contains(&o.order_id)).collect();
        if rows.len() == venue_ids.len() && rows.iter().all(|o| o.status == "CANCELED") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let after = client.account_info(&account).await?.into_inner();
    let position_after =
        after.positions.iter().find(|p| p.symbol == SYMBOL).map(|p| p.position_amt);
    println!(
        "{}",
        json!({
            "event": "verification", "remaining_open": remaining, "orders": rows,
            "position_before": position_before, "position_after": position_after,
        })
    );
    clock_samples(&client, "after").await?;
    result?;
    if remaining != 0
        || rows.len() != SAMPLES
        || rows.iter().any(|o| o.status != "CANCELED" || !o.executed_qty.is_zero())
        || position_before != position_after
    {
        return Err("Final verification differs from 20 unfilled cancelled orders".into());
    }
    Ok(())
}
