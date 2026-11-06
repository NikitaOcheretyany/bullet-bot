//! Bounded mainnet comparison: resting `PostOnly` -> crossing Limit amend vs new IOC.
//! --check is read-only. --live runs 20 alternating pairs with minimum BTC size.
//! Each entry is closed by a reduce-only IOC before another entry is permitted.
//! Add --ioc-only to measure just 20 protected-market entries and their closes.
use std::collections::HashSet;
use std::error::Error;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bb_core::helpers::ClientIdIssuer;
use bb_exchange_bullet::{delegate, key};
use bullet_rust_sdk::codegen::types::Filter;
use bullet_rust_sdk::types::{ClientMessage, RequestId};
use bullet_rust_sdk::{
    AmendOrderArgs, CallMessage, CancelOrderArgs, Client, ClientOrderId, MarketId, Network,
    NewOrderArgs, OrderId, OrderType, PositiveDecimal, Side, Topic, Transaction, UserAction,
    UserActionDiscriminants, WebsocketHandle,
};
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::{Value, json};

type Result<T> = std::result::Result<T, Box<dyn Error>>;
const SYMBOL: &str = "BTC-USD";
const PAIRS: usize = 20;

#[derive(Clone, Copy, Serialize)]
struct Stamp {
    wall_ns: u128,
    mono_ns: u128,
}
fn stamp(origin: Instant) -> Stamp {
    Stamp {
        wall_ns: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos(),
        mono_ns: origin.elapsed().as_nanos(),
    }
}
#[allow(clippy::needless_pass_by_value)] // Convenient ownership of temporary json! values.
fn emit(value: Value) {
    println!("{value}");
}
fn number(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str().and_then(|s| s.parse().ok()))
}
fn decimal(value: &Value) -> Option<Decimal> {
    value.as_str().and_then(|s| s.parse().ok())
}
#[derive(Serialize)]
struct Frame {
    received: Stamp,
    message: Value,
}
#[derive(Default, Debug, Serialize)]
struct Outcome {
    ack: Option<usize>,
    ack_ok: bool,
    order_id: Option<u64>,
    first_new: Option<usize>,
    first_fill: Option<usize>,
    terminal: Option<usize>,
    filled: Decimal,
    maker_fill: bool,
    trade_ids: Vec<u64>,
}

// Amend replies can mention both old and new order IDs. Identify the new order
// from its unique client ID, never by blindly selecting the first ACK order ID.
fn outcome(frames: &[Frame], rid: u64, cid: u64, method: &str) -> Result<Outcome> {
    let mut out = Outcome {
        ack: frames.iter().position(|f| number(&f.message["id"]) == Some(rid)),
        ..Outcome::default()
    };
    if let Some(i) = out.ack {
        let msg = &frames[i].message;
        out.ack_ok = msg["e"] == method
            && matches!(
                msg["results"]["status"].as_str(),
                Some("submitted" | "published" | "processed" | "finalized")
            );
    }
    out.order_id = frames.iter().find_map(|f| {
        let o = &f.message["o"];
        (o["s"] == SYMBOL && number(&o["co"]) == Some(cid)).then(|| number(&o["i"])).flatten()
    });
    if out.order_id.is_none() && method == "order.place" && out.ack_ok {
        out.order_id = out.ack.and_then(|i| number(&frames[i].message["results"]["order_ids"][0]));
    }
    let mut seen = HashSet::new();
    for (i, f) in frames.iter().enumerate() {
        let o = &f.message["o"];
        if o["s"] != SYMBOL
            || !(number(&o["co"]) == Some(cid)
                || out.order_id.is_some_and(|oid| number(&o["i"]) == Some(oid)))
        {
            continue;
        }
        if o["x"] == "NEW" && out.first_new.is_none() {
            out.first_new = Some(i);
        }
        if o["x"] == "TRADE" {
            let tid = number(&o["t"]).ok_or("Fill lacks trade ID")?;
            if seen.insert(tid) {
                out.filled += decimal(&o["l"]).ok_or("Fill lacks quantity")?;
                out.maker_fill |= o["m"] == true;
                out.trade_ids.push(tid);
                if out.first_fill.is_none() {
                    out.first_fill = Some(i);
                }
            }
        }
        if matches!(o["X"].as_str(), Some("FILLED" | "CANCELED" | "CANCELLED" | "REJECTED")) {
            out.terminal = Some(i);
        }
    }
    Ok(out)
}

async fn receive(ws: &mut WebsocketHandle, origin: Instant) -> Result<Frame> {
    let message = ws.recv().await?;
    let received = stamp(origin);
    Ok(Frame { received, message: serde_json::to_value(message)? })
}

#[allow(clippy::too_many_arguments)]
async fn operation(
    client: &Client,
    ws: &mut WebsocketHandle,
    origin: Instant,
    call: CallMessage,
    pair: usize,
    arm: &str,
    role: &str,
    cid: u64,
    quantity: Decimal,
) -> Result<Outcome> {
    let method = if matches!(&call, CallMessage::User(UserAction::AmendOrders { .. })) {
        "order.amend"
    } else {
        "order.place"
    };
    let prepare_start = stamp(origin);
    // Call arguments contain order parameters only, never key material or signed bytes.
    emit(json!({"event":"intent","pair":pair,"arm":arm,"role":role,
        "client_id":cid,"method":method,"call":call,"at":prepare_start}));
    let signed = Transaction::builder().call_message(call).client(client).build()?;
    let encoded = Transaction::to_base64(&signed)?;
    let send_start = stamp(origin);
    let sent = if method == "order.amend" {
        ws.order_amend(encoded, Some(RequestId::new(cid))).await
    } else {
        ws.order_place(encoded, Some(RequestId::new(cid))).await
    };
    let send_done = stamp(origin);
    let mut error = sent.err().map(|e| e.to_string());
    let mut frames = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while error.is_none() {
        let state = outcome(&frames, cid, cid, method)?;
        if (state.ack.is_some() && !state.ack_ok)
            || (role == "prepare" && !state.filled.is_zero())
            || (state.ack_ok
                && if role == "prepare" {
                    state.first_new.is_some()
                } else {
                    state.terminal.is_some()
                })
        {
            break;
        }
        match tokio::time::timeout_at(deadline, receive(ws, origin)).await {
            Ok(Ok(f)) => frames.push(f),
            Ok(Err(e)) => error = Some(e.to_string()),
            Err(_) => error = Some("Timeout awaiting ACK and private state".into()),
        }
    }
    let state = outcome(&frames, cid, cid, method)?;
    emit(json!({"event":"operation","pair":pair,"arm":arm,"role":role,"method":method,
        "client_id":cid,"quantity":quantity,"prepare_start":prepare_start,
        "send_start":send_start,"send_done":send_done,"outcome":state,
        "error":error,"frames":frames}));
    if let Some(e) = error {
        return Err(e.into());
    }
    if !state.ack_ok {
        return Err(format!("{role} ACK rejected; see operation record").into());
    }
    if role == "prepare" {
        if state.first_new.is_none() || !state.filled.is_zero() {
            return Err("Distant PostOnly was not safely resting".into());
        }
    } else if state.filled != quantity || state.terminal.is_none() || state.maker_fill {
        return Err(format!("Expected full taker fill ({quantity}), observed {state:?}").into());
    }
    Ok(state)
}

fn order(
    cid: u64,
    price: Decimal,
    quantity: Decimal,
    side: Side,
    kind: OrderType,
    reduce: bool,
) -> Result<NewOrderArgs> {
    Ok(NewOrderArgs {
        price: PositiveDecimal::try_from(price)?,
        size: PositiveDecimal::try_from(quantity)?,
        side,
        order_type: kind,
        reduce_only: reduce,
        client_order_id: Some(ClientOrderId(cid)),
        pending_tpsl_pair: None,
    })
}
fn place(market_id: MarketId, order: NewOrderArgs) -> CallMessage {
    CallMessage::User(UserAction::PlaceOrders {
        market_id,
        orders: vec![order],
        replace: false,
        sub_account_index: None,
    })
}
async fn book(client: &Client) -> Result<(Decimal, Decimal)> {
    let b = client.order_book(Some(5), SYMBOL).await?.into_inner();
    let bid = b.bids.first().and_then(|r| r.first()).ok_or("Empty bid")?.parse()?;
    let ask = b.asks.first().and_then(|r| r.first()).ok_or("Empty ask")?.parse()?;
    if bid <= Decimal::ZERO || ask <= bid {
        return Err("Invalid BBO".into());
    }
    Ok((bid, ask))
}
fn crossing(touch: Decimal, tick: Decimal, buy: bool) -> Decimal {
    let raw = touch * if buy { Decimal::new(1005, 3) } else { Decimal::new(995, 3) };
    (if buy { (raw / tick).floor() } else { (raw / tick).ceil() }) * tick
}
async fn position(client: &Client, account: &str) -> Result<Decimal> {
    let a = client.account_info(account).await?.into_inner();
    Ok(a.positions.iter().find(|p| p.symbol == SYMBOL).map_or(Decimal::ZERO, |p| p.position_amt))
}
async fn clock_samples(client: &Client, origin: Instant, phase: &str) -> Result<()> {
    for _ in 0..7 {
        let begin = stamp(origin);
        let server_us = client.time().await?.into_inner().server_time;
        let received = stamp(origin);
        emit(
            json!({"event":"clock","phase":phase,"begin":begin,"received":received,"server_us":server_us}),
        );
    }
    Ok(())
}
async fn ping(ws: &mut WebsocketHandle, origin: Instant, phase: &str) -> Result<()> {
    for rid in 101..=103 {
        let begin = stamp(origin);
        ws.send(ClientMessage::Ping { id: Some(RequestId::new(rid)) }).await?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            let f = tokio::time::timeout_at(deadline, receive(ws, origin)).await??;
            if number(&f.message["id"]) == Some(rid) && f.message["e"] == "pong" {
                emit(json!({"event":"ping","phase":phase,"begin":begin,"reply":f}));
                break;
            }
            emit(json!({"event":"drain","frame":f}));
        }
    }
    Ok(())
}
fn next_id(ids: &mut ClientIdIssuer, issued: &mut Vec<u64>) -> Result<u64> {
    let cid = ids.issue().parse()?;
    issued.push(cid);
    Ok(cid)
}

// Exceptional recovery is outside the sample. Cancel only this invocation's
// orders before closing its exposure; no retry of an uncertain entry.
#[allow(clippy::too_many_arguments)]
async fn cleanup(
    client: &Client,
    account: &str,
    market: MarketId,
    tick: Decimal,
    quantity: Decimal,
    issued: &mut Vec<u64>,
    ids: &mut ClientIdIssuer,
) -> Result<()> {
    for pass in 0..12 {
        let open = client.query_open_orders(account, Some(SYMBOL)).await?.into_inner();
        let own: Vec<_> = open
            .iter()
            .filter(|o| {
                o.client_order_id
                    .as_ref()
                    .and_then(|s| s.parse::<u64>().ok())
                    .is_some_and(|id| issued.contains(&id))
            })
            .map(|o| CancelOrderArgs { order_id: Some(OrderId(o.order_id)), client_order_id: None })
            .collect();
        if !own.is_empty() {
            let reply = client.cancel_orders(market, own, None).await?;
            emit(json!({"event":"cleanup_cancel_rest","pass":pass,"reply":reply}));
        }
        let pos = position(client, account).await?;
        emit(json!({"event":"cleanup_check","pass":pass,"position":pos,"open":open}));
        if !pos.is_zero() && open.is_empty() {
            if pos.abs() > quantity {
                return Err("Exposure exceeds test quantity; needs manual inspection".into());
            }
            let (bid, ask) = book(client).await?;
            let buy = pos < Decimal::ZERO;
            let cid = next_id(ids, issued)?;
            let close = order(
                cid,
                crossing(if buy { ask } else { bid }, tick, buy),
                pos.abs(),
                if buy { Side::Bid } else { Side::Ask },
                OrderType::ImmediateOrCancel,
                true,
            )?;
            let reply = client.place_orders(market, vec![close], false, None).await?;
            emit(json!({"event":"cleanup_close_rest","pass":pass,"client_id":cid,"reply":reply}));
        }
        // Observe several snapshots even if initially flat: REST can lag the WS.
        if pos.is_zero() && open.is_empty() && pass >= 5 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err("Cleanup did not confirm a flat BTC account".into())
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let (mut live, mut check, mut ioc_only) = (false, false, false);
    let mut samples = PAIRS;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--live" => live = true,
            "--check" => check = true,
            "--ioc-only" => ioc_only = true,
            "--samples" => samples = args.next().ok_or("Missing sample count")?.parse()?,
            _ => {
                return Err(
                    "Use --check or --live, optionally --ioc-only and --samples 1..20".into()
                );
            }
        }
    }
    if (live && check) || !(1..=PAIRS).contains(&samples) {
        return Err("Conflicting modes or sample count outside 1..20".into());
    }
    let expected_orders = samples * if ioc_only { 2 } else { 5 };
    let expected_filled = samples * if ioc_only { 2 } else { 4 };
    let origin = Instant::now();
    let key_file = std::env::var("BB_BULLET_KEY_FILE")?;
    let client = Client::builder()
        .network(Network::Mainnet)
        .keypair(key::keypair_from_key_file(Path::new(&key_file))?)
        .user_actions(vec![
            UserActionDiscriminants::PlaceOrders,
            UserActionDiscriminants::AmendOrders,
            UserActionDiscriminants::CancelOrders,
        ])
        .build()
        .await?;
    let account = delegate::resolve_account_address(client.url(), &client.address()?).await?;
    let market = client.market_id(SYMBOL).ok_or("BTC market missing")?;
    let info = client.exchange_info().await?.into_inner();
    let info = info.symbols.iter().find(|s| s.symbol == SYMBOL).ok_or("BTC filters missing")?;
    let quantity = info
        .filters
        .iter()
        .find_map(|f| match f {
            Filter::LotSize { min_qty, step_size, .. } => Some((*min_qty).max(*step_size)),
            _ => None,
        })
        .ok_or("LOT_SIZE missing")?;
    let tick = info
        .filters
        .iter()
        .find_map(|f| match f {
            Filter::PriceFilter { tick_size, .. } => Some(*tick_size),
            _ => None,
        })
        .ok_or("PRICE_FILTER missing")?;
    let pos = position(&client, &account).await?;
    let open = client.query_open_orders(&account, Some(SYMBOL)).await?.into_inner();
    if !pos.is_zero() || !open.is_empty() {
        return Err("BTC must start flat with no open orders".into());
    }
    let (_, ask) = book(&client).await?;
    if quantity <= Decimal::ZERO || tick <= Decimal::ZERO || quantity * ask > Decimal::from(25) {
        return Err("Invalid filters or minimum quantity exceeds $25".into());
    }
    emit(json!({"event":"start","at":stamp(origin),"live":live,"symbol":SYMBOL,
        "quantity":quantity,"tick":tick,"pairs":samples,"entry_side":"BUY",
        "slippage_bps":50,"postonly_distance_pct":10,"max_entry_notional_usd":25,
        "arms":if ioc_only {vec!["IOC"]} else {vec!["AmendLimit", "IOC"]},
        "expected_orders":expected_orders,"expected_filled":expected_filled,
        "schedule":if ioc_only {"IOC entries only; flat between entries"} else {"alternating AmendLimit/IOC and IOC/AmendLimit; flat between entries"},
        "build":if cfg!(debug_assertions) {"debug"} else {"release"},
        "position_before":pos,"open_orders_before":open.len()}));
    clock_samples(&client, origin, "before").await?;
    let mut ws = client.connect_ws().call().await?;
    ws.subscribe([Topic::user_orders(&account), Topic::agg_trade(SYMBOL)], Some(RequestId::new(1)))
        .await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let f = tokio::time::timeout_at(deadline, receive(&mut ws, origin)).await??;
        let matched = number(&f.message["id"]) == Some(1);
        let good = f.message["e"] == "subscribe" && f.message["result"] == "success";
        emit(json!({"event":"subscription","frame":f}));
        if matched {
            if !good {
                return Err("Subscription failed".into());
            }
            break;
        }
    }
    ping(&mut ws, origin, "before").await?;
    if !live {
        emit(json!({"event":"check_complete","mutations":0}));
        return Ok(());
    }
    let mut ids = ClientIdIssuer::session_seeded();
    let mut issued = Vec::new();
    let mut resting_ids = Vec::new();
    let mut filled_ids = Vec::new();
    let stopped = Arc::new(AtomicBool::new(false));
    let stop_signal = stopped.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            stop_signal.store(true, Ordering::Relaxed);
        }
    });
    let mut completed_pairs = 0;
    let result: Result<()> = async {
        for pair in 0..samples {
            let arms: &[&str] = if ioc_only {
                &["IOC"]
            } else if pair % 2 == 0 {
                &["AmendLimit", "IOC"]
            } else {
                &["IOC", "AmendLimit"]
            };
            for &arm in arms {
                if stopped.load(Ordering::Relaxed) {
                    return Err("Interrupted".into());
                }
                // Account checks and BBO queries are outside the latency interval.
                if !position(&client, &account).await?.is_zero()
                    || !client.query_open_orders(&account, Some(SYMBOL)).await?.is_empty()
                {
                    return Err("BTC no longer flat before entry".into());
                }
                let mut resting = None;
                if arm == "AmendLimit" {
                    let (bid, _) = book(&client).await?;
                    let price = (bid * Decimal::new(9, 1) / tick).floor() * tick;
                    let cid = next_id(&mut ids, &mut issued)?;
                    let args = order(cid, price, quantity, Side::Bid, OrderType::PostOnly, false)?;
                    let state = operation(
                        &client,
                        &mut ws,
                        origin,
                        place(market, args),
                        pair,
                        arm,
                        "prepare",
                        cid,
                        quantity,
                    )
                    .await?;
                    let oid = state.order_id.ok_or("Resting order ID missing")?;
                    resting_ids.push(oid);
                    resting = Some(oid);
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
                let (bid, ask) = book(&client).await?;
                let price = crossing(ask, tick, true);
                if quantity * price > Decimal::from(25) {
                    return Err("Entry exceeds $25".into());
                }
                if stopped.load(Ordering::Relaxed) {
                    return Err("Interrupted before entry".into());
                }
                let cid = next_id(&mut ids, &mut issued)?;
                let args = order(
                    cid,
                    price,
                    quantity,
                    Side::Bid,
                    if resting.is_some() { OrderType::Limit } else { OrderType::ImmediateOrCancel },
                    false,
                )?;
                let call = if let Some(oid) = resting {
                    CallMessage::User(UserAction::AmendOrders {
                        market_id: market,
                        orders: vec![AmendOrderArgs {
                            cancel: CancelOrderArgs {
                                order_id: Some(OrderId(oid)),
                                client_order_id: None,
                            },
                            place: args,
                        }],
                        sub_account_index: None,
                    })
                } else {
                    place(market, args)
                };
                emit(json!({"event":"entry_book","pair":pair,"arm":arm,"bid":bid,"ask":ask,
                    "limit":price,"old_order_id":resting,"client_id":cid,"at":stamp(origin)}));
                let opened =
                    operation(&client, &mut ws, origin, call, pair, arm, "entry", cid, quantity)
                        .await?;
                filled_ids.push(opened.order_id.ok_or("Entry order ID missing")?);
                let (bid, _) = book(&client).await?;
                let close_id = next_id(&mut ids, &mut issued)?;
                let close = order(
                    close_id,
                    crossing(bid, tick, false),
                    opened.filled,
                    Side::Ask,
                    OrderType::ImmediateOrCancel,
                    true,
                )?;
                let closed = operation(
                    &client,
                    &mut ws,
                    origin,
                    place(market, close),
                    pair,
                    arm,
                    "close",
                    close_id,
                    opened.filled,
                )
                .await?;
                filled_ids.push(closed.order_id.ok_or("Close order ID missing")?);
                // Allow the REST state view to catch up before the next preflight.
                let mut flat = false;
                for _ in 0..10 {
                    if position(&client, &account).await?.is_zero()
                        && client.query_open_orders(&account, Some(SYMBOL)).await?.is_empty()
                    {
                        flat = true;
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                if !flat {
                    return Err("Not flat after reduce-only close".into());
                }
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
            completed_pairs += 1;
            eprintln!("Completed {completed_pairs}/{samples} rounds; BTC flat");
        }
        Ok(())
    }
    .await;
    let cleanup_error = if result.is_err() {
        eprintln!(
            "Series stopped: {:?}; reconciling test orders and exposure",
            result.as_ref().err()
        );
        cleanup(&client, &account, market, tick, quantity, &mut issued, &mut ids)
            .await
            .err()
            .map(|e| e.to_string())
    } else {
        None
    };
    let mut history = Vec::new();
    let mut trades = Vec::new();
    for _ in 0..15 {
        history = client
            .all_orders(Some(&account), None, Some(1000), None, None, Some(SYMBOL))
            .await?
            .into_inner()
            .data
            .into_iter()
            .filter(|o| resting_ids.contains(&o.order_id) || filled_ids.contains(&o.order_id))
            .collect::<Vec<_>>();
        trades = client
            .user_trades(Some(&account), None, Some(false), Some(1000), None, Some(SYMBOL))
            .await?
            .into_inner()
            .data
            .into_iter()
            .filter(|t| {
                filled_ids.contains(&t.order_id) || history.iter().any(|o| o.order_id == t.order_id)
            })
            .collect::<Vec<_>>();
        if history.len() == issued.len()
            && filled_ids.iter().all(|oid| trades.iter().any(|t| t.order_id == *oid))
            && resting_ids.iter().all(|id| {
                history.iter().any(|o| {
                    o.order_id == *id && o.status == "CANCELED" && o.executed_qty.is_zero()
                })
            })
            && filled_ids.iter().all(|id| {
                history.iter().any(|o| {
                    o.order_id == *id && o.status == "FILLED" && o.executed_qty == quantity
                })
            })
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let after_pos = position(&client, &account).await?;
    let after_open = client.query_open_orders(&account, Some(SYMBOL)).await?.into_inner();
    emit(json!({"event":"verification","at":stamp(origin),"issued":issued,
        "resting_ids":resting_ids,"filled_ids":filled_ids,"orders":history,"trades":trades,
        "btc_position_after":after_pos,"btc_open_orders_after":after_open.len(),
        "completed_pairs":completed_pairs,"cleanup_error":cleanup_error,
        "series_error":result.as_ref().err().map(ToString::to_string)}));
    clock_samples(&client, origin, "after").await?;
    ping(&mut ws, origin, "after").await?;
    result?;
    if cleanup_error.is_some()
        || !after_pos.is_zero()
        || !after_open.is_empty()
        || history.len() != expected_orders
        || filled_ids.len() != expected_filled
        || trades.iter().any(|t| t.maker)
        || resting_ids.iter().any(|id| {
            !history
                .iter()
                .any(|o| o.order_id == *id && o.status == "CANCELED" && o.executed_qty.is_zero())
        })
        || filled_ids.iter().any(|id| {
            !history
                .iter()
                .any(|o| o.order_id == *id && o.status == "FILLED" && o.executed_qty == quantity)
        })
    {
        return Err("Final trade/order/state validation incomplete".into());
    }
    emit(json!({"event":"complete","pairs":completed_pairs,"at":stamp(origin)}));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn f(message: Value) -> Frame {
        Frame { received: Stamp { wall_ns: 0, mono_ns: 0 }, message }
    }
    #[test]
    fn amend_matches_new_id_and_deduplicates_fills() {
        let fill = json!({"o":{"s":SYMBOL,"co":9,"i":102,"x":"TRADE","X":"FILLED",
            "t":50,"l":"0.0001","m":false}});
        let frames = vec![
            f(json!({"o":{"s":SYMBOL,"co":8,"i":101,"x":"CANCELED","X":"CANCELED"}})),
            f(fill.clone()),
            f(fill),
            f(
                json!({"id":9,"e":"order.amend","results":{"status":"processed","order_ids":[101,102]}}),
            ),
        ];
        let r = outcome(&frames, 9, 9, "order.amend").unwrap();
        assert!(r.ack_ok);
        assert_eq!(r.order_id, Some(102));
        assert_eq!(r.first_fill, Some(1));
        assert_eq!(r.filled, Decimal::new(1, 4));
        assert_eq!(r.trade_ids, vec![50]);
    }
    #[test]
    fn new_and_ack_are_not_fills() {
        let frames = vec![
            f(json!({"o":{"s":SYMBOL,"co":9,"i":102,"x":"NEW","X":"NEW"}})),
            f(json!({"id":9,"e":"order.amend","results":{"status":"processed","order_ids":[102]}})),
        ];
        let r = outcome(&frames, 9, 9, "order.amend").unwrap();
        assert!(r.ack_ok);
        assert!(r.first_fill.is_none());
        assert!(r.terminal.is_none());
        assert!(r.filled.is_zero());
    }
}
