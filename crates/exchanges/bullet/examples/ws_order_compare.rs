//! Bounded user-run comparison of PostOnly/Limit placement and targeted cancellation.
//! Defaults to a read-only preflight; --live is required for exchange mutations.
//! One persistent raw WS; one minimum-size BTC order at a time; no placement retries.
use std::error::Error;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bb_core::helpers::ClientIdIssuer;
use bb_exchange_bullet::{delegate, key};
use bullet_rust_sdk::codegen::types::Filter;
use bullet_rust_sdk::types::{ClientMessage, RequestId, UserAction};
use bullet_rust_sdk::{
    CallMessage, CancelOrderArgs, Client, ClientOrderId, Network, NewOrderArgs, NewOrderExt,
    OrderId, PositiveDecimal, Side, Topic, Transaction, UserActionDiscriminants, WebsocketHandle,
};
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::{Value, json};

type Result<T> = std::result::Result<T, Box<dyn Error>>;
const SYMBOL: &str = "BTC-USD";
const PAIRS: u64 = 30;

#[derive(Clone, Copy, Debug, Serialize)]
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
fn emit(value: &Value) {
    println!("{value}");
}
#[derive(Serialize)]
struct Frame {
    received: Stamp,
    message: Value,
}
#[derive(Default, Debug, Serialize)]
struct Receipts {
    ack: Option<usize>,
    private: Option<usize>,
    order_id: Option<u64>,
    ack_ok: bool,
    unexpected_fill: bool,
}
fn number(v: &Value) -> Option<u64> {
    v.as_u64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}
// Re-scan buffered messages: a private event without co may precede its ACK.
fn receipts(frames: &[Frame], rid: u64, cid: u64, oid: Option<u64>, cancel: bool) -> Receipts {
    let mut out = Receipts { order_id: oid, ..Receipts::default() };
    let ack_kind = if cancel { "order.cancel" } else { "order.place" };
    out.ack = frames.iter().position(|f| number(&f.message["id"]) == Some(rid));
    if let Some(i) = out.ack {
        let v = &frames[i].message;
        out.ack_ok = v["e"] == ack_kind
            && matches!(
                v["results"]["status"].as_str(),
                Some("submitted" | "published" | "processed" | "finalized")
            );
        out.order_id = out.order_id.or_else(|| number(&v["results"]["order_ids"][0]));
    }
    for (i, f) in frames.iter().enumerate() {
        let o = &f.message["o"];
        if o["s"] != SYMBOL {
            continue;
        }
        let matched = number(&o["co"]) == Some(cid)
            || out.order_id.is_some_and(|id| number(&o["i"]) == Some(id));
        if !matched {
            continue;
        }
        out.order_id = out.order_id.or_else(|| number(&o["i"]));
        if o["x"] == "TRADE" {
            out.unexpected_fill = true;
        }
        let wanted = if cancel {
            matches!(o["X"].as_str(), Some("CANCELED" | "CANCELLED"))
        } else {
            o["X"] == "NEW" && o["x"] == "NEW"
        };
        if wanted && out.private.is_none() {
            out.private = Some(i);
        }
    }
    out
}

async fn frame(ws: &mut WebsocketHandle, origin: Instant) -> Result<Frame> {
    let message = ws.recv().await?;
    let received = stamp(origin);
    // Signed transaction bytes and private keys are never written to this log.
    let message = serde_json::to_value(message)?;
    Ok(Frame { received, message })
}

#[allow(clippy::too_many_arguments)]
async fn operation(
    client: &Client,
    ws: &mut WebsocketHandle,
    origin: Instant,
    call: CallMessage,
    pair: u64,
    kind: &str,
    cid: u64,
    rid: u64,
    oid: Option<u64>,
    cancel: bool,
) -> Result<Receipts> {
    let prepare_start = stamp(origin);
    let signed = Transaction::builder().call_message(call).client(client).build()?;
    let encoded = Transaction::to_base64(&signed)?;
    // Signing and borsh/base64 preparation are outside send-to-event timing.
    let send_start = stamp(origin);
    let sent = if cancel {
        ws.order_cancel(encoded, Some(RequestId::new(rid))).await
    } else {
        ws.order_place(encoded, Some(RequestId::new(rid))).await
    };
    let send_done = stamp(origin);
    let mut error = sent.err().map(|e| e.to_string());
    let mut frames = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while error.is_none() {
        let state = receipts(&frames, rid, cid, oid, cancel);
        if state.unexpected_fill
            || (state.ack.is_some() && !state.ack_ok)
            || (state.ack_ok && state.private.is_some())
        {
            break;
        }
        match tokio::time::timeout_at(deadline, frame(ws, origin)).await {
            Ok(Ok(f)) => frames.push(f),
            Ok(Err(e)) => error = Some(e.to_string()),
            Err(_) => error = Some("Timeout waiting for ACK and private confirmation".into()),
        }
    }
    let state = receipts(&frames, rid, cid, oid, cancel);
    emit(&json!({"event":"operation", "pair":pair,"order_type":kind,
        "operation":if cancel {"cancel"} else {"place"},"client_id":cid,
        "request_id":rid,"prepare_start":prepare_start,"send_start":send_start,
        "send_done":send_done,"receipts":state,"error":error,"frames":frames}));
    if let Some(e) = error {
        return Err(e.into());
    }
    Ok(state)
}

async fn clocks(client: &Client, origin: Instant, phase: &str) -> Result<()> {
    for _ in 0..7 {
        let begin = stamp(origin);
        let server_us = client.time().await?.into_inner().server_time;
        let received = stamp(origin);
        emit(&json!({"event":"clock","phase":phase,"begin":begin,
            "received":received,"server_us":server_us}));
    }
    Ok(())
}

async fn ping(ws: &mut WebsocketHandle, origin: Instant, phase: &str) -> Result<()> {
    for i in 0..3 {
        let rid = 100 + i;
        let begin = stamp(origin);
        ws.send(ClientMessage::Ping { id: Some(RequestId::new(rid)) }).await?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            let f = tokio::time::timeout_at(deadline, frame(ws, origin)).await??;
            if number(&f.message["id"]) == Some(rid) {
                if f.message["e"] != "pong" {
                    return Err("Ping failed".into());
                }
                emit(&json!({"event":"ping","phase":phase,"begin":begin,"reply":f}));
                break;
            }
        }
    }
    Ok(())
}

fn pair_types(pair: u64) -> [&'static str; 2] {
    if pair.is_multiple_of(2) { ["PostOnly", "Limit"] } else { ["Limit", "PostOnly"] }
}
fn distant_price(bid: Decimal, tick: Decimal) -> Result<Decimal> {
    if bid <= Decimal::ZERO || tick <= Decimal::ZERO {
        return Err("Invalid book/tick".into());
    }
    let price = (bid * Decimal::new(9, 1) / tick).floor() * tick;
    if price <= Decimal::ZERO || price >= bid {
        return Err("Invalid distant price".into());
    }
    Ok(price)
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.iter().any(|s| !["--check", "--live"].contains(&s.as_str())) || args.len() > 1 {
        return Err("Use --check (read only, default) or --live".into());
    }
    let live = args.first().is_some_and(|s| s == "--live");
    let origin = Instant::now();
    let key_file = std::env::var("BB_BULLET_KEY_FILE")?;
    let client = Client::builder()
        .network(Network::Mainnet)
        .keypair(key::keypair_from_key_file(Path::new(&key_file))?)
        .user_actions(vec![
            UserActionDiscriminants::PlaceOrders,
            UserActionDiscriminants::CancelOrders,
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
    let tick = symbol
        .filters
        .iter()
        .find_map(|f| match f {
            Filter::PriceFilter { tick_size, .. } => Some(*tick_size),
            _ => None,
        })
        .ok_or("PRICE_FILTER missing")?;
    let before = client.account_info(&account).await?.into_inner();
    let position = before
        .positions
        .iter()
        .find(|p| p.symbol == SYMBOL)
        .map_or(Decimal::ZERO, |p| p.position_amt);
    let open = client.query_open_orders(&account, Some(SYMBOL)).await?.into_inner();
    if !position.is_zero() || !open.is_empty() {
        return Err("BTC already has a position or open orders; no test orders sent".into());
    }
    let book = client.order_book(Some(5), SYMBOL).await?.into_inner();
    let bid: Decimal = book.bids.first().and_then(|x| x.first()).ok_or("No BTC bid")?.parse()?;
    let price = distant_price(bid, tick)?;
    if quantity * bid > Decimal::from(25) {
        return Err("Minimum size exceeds $25 cap".into());
    }
    emit(&json!({"event":"start","live":live,"at":stamp(origin),"symbol":SYMBOL,
        "pairs":PAIRS,"placements":PAIRS*2,"cancels":PAIRS*2,"quantity":quantity,
        "initial_bid":bid,"initial_price":price,"distance_pct":10,"tick":tick,
        "position_before":position,"open_orders_before":open.len(),
        "transport":"single raw WS connection","schedule":"AB/BA alternating pairs",
        "timing":"signed and encoded before send_start; stamp before WS send call; receive after SDK parse",
        "build":if cfg!(debug_assertions) {"dev"} else {"release"}}));
    clocks(&client, origin, "before").await?;
    let mut ws = client.connect_ws().call().await?;
    ws.subscribe([Topic::user_orders(&account)], Some(RequestId::new(1))).await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let f = tokio::time::timeout_at(deadline, frame(&mut ws, origin)).await??;
        if number(&f.message["id"]) == Some(1) {
            if f.message["e"] != "subscribe" || f.message["result"] != "success" {
                return Err("User stream subscription not confirmed".into());
            }
            emit(&json!({"event":"subscription","reply":f}));
            break;
        }
    }
    ping(&mut ws, origin, "before").await?;
    if !live {
        emit(&json!({"event":"check_complete","mutations":0,"at":stamp(origin)}));
        return Ok(());
    }

    let mut ids = ClientIdIssuer::session_seeded();
    let mut issued = Vec::new();
    let mut venue_ids = Vec::new();
    let mut completed_pairs = 0;
    let stopped = Arc::new(AtomicBool::new(false));
    let stop_signal = stopped.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            stop_signal.store(true, Ordering::Relaxed);
        }
    });
    let result: Result<()> = async {
        for pair in 0..PAIRS {
            // Fresh price outside timed operations, identical within each AB/BA pair.
            let book = client.order_book(Some(5), SYMBOL).await?.into_inner();
            let bid: Decimal =
                book.bids.first().and_then(|x| x.first()).ok_or("No BTC bid")?.parse()?;
            let price = distant_price(bid, tick)?;
            if quantity * bid > Decimal::from(25) {
                return Err("Minimum size exceeds $25".into());
            }
            emit(&json!({"event":"pair","pair":pair,"types":pair_types(pair),"bid":bid,
                "price":price,"quantity":quantity,"at":stamp(origin)}));
            for kind in pair_types(pair) {
                if stopped.load(Ordering::Relaxed) {
                    return Err("Interrupted; no new orders".into());
                }
                let cid: u64 = ids.issue().parse()?;
                issued.push(cid);
                let mut order = if kind == "PostOnly" {
                    NewOrderArgs::post_only(
                        PositiveDecimal::try_from(price)?,
                        PositiveDecimal::try_from(quantity)?,
                        Side::Bid,
                    )
                } else {
                    NewOrderArgs::limit(
                        PositiveDecimal::try_from(price)?,
                        PositiveDecimal::try_from(quantity)?,
                        Side::Bid,
                    )
                };
                order.client_order_id = Some(ClientOrderId(cid));
                let place = operation(
                    &client,
                    &mut ws,
                    origin,
                    CallMessage::User(UserAction::PlaceOrders {
                        market_id,
                        orders: vec![order],
                        replace: false,
                        sub_account_index: None,
                    }),
                    pair,
                    kind,
                    cid,
                    cid * 2,
                    None,
                    false,
                )
                .await;
                let oid = place.as_ref().ok().and_then(|x| x.order_id);
                if let Some(id) = oid {
                    venue_ids.push(id);
                }
                // Cancellation is attempted even if placement acknowledgement is missing.
                let cancel = operation(
                    &client,
                    &mut ws,
                    origin,
                    CallMessage::User(UserAction::CancelOrders {
                        market_id,
                        orders: vec![CancelOrderArgs {
                            order_id: oid.map(OrderId),
                            client_order_id: oid.is_none().then_some(ClientOrderId(cid)),
                        }],
                        sub_account_index: None,
                    }),
                    pair,
                    kind,
                    cid,
                    cid * 2 + 1,
                    oid,
                    true,
                )
                .await;
                let placed = place?;
                let cancelled = cancel?;
                if !placed.ack_ok
                    || placed.private.is_none()
                    || placed.unexpected_fill
                    || !cancelled.ack_ok
                    || cancelled.private.is_none()
                    || cancelled.unexpected_fill
                {
                    return Err("Incomplete confirmation or unexpected fill; series stopped".into());
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            completed_pairs += 1;
            eprintln!("Completed {completed_pairs}/{PAIRS} pairs");
        }
        Ok(())
    }
    .await;

    // Clean up only IDs created by this invocation. Never cancel unrelated orders.
    let mut cleanup_error = None;
    let open = client.query_open_orders(&account, Some(SYMBOL)).await?.into_inner();
    let own: Vec<_> = open
        .iter()
        .filter(|o| {
            venue_ids.contains(&o.order_id)
                || o.client_order_id
                    .as_ref()
                    .and_then(|s| s.parse::<u64>().ok())
                    .is_some_and(|id| issued.contains(&id))
        })
        .map(|o| CancelOrderArgs { order_id: Some(OrderId(o.order_id)), client_order_id: None })
        .collect();
    if !own.is_empty() {
        // Exceptional cleanup outside the WS latency sample, explicitly labelled REST.
        match client.cancel_orders(market_id, own, None).await {
            Ok(reply) => emit(&json!({"event":"cleanup_rest","response":reply})),
            Err(e) => cleanup_error = Some(e.to_string()),
        }
    }
    let mut rows = Vec::new();
    for _ in 0..10 {
        rows = client
            .all_orders(Some(&account), None, Some(1000), None, None, Some(SYMBOL))
            .await?
            .into_inner()
            .data
            .into_iter()
            .filter(|o| venue_ids.contains(&o.order_id))
            .collect();
        if rows.len() == venue_ids.len() && rows.iter().all(|o| o.status == "CANCELED") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let after = client.account_info(&account).await?.into_inner();
    let after_position = after
        .positions
        .iter()
        .find(|p| p.symbol == SYMBOL)
        .map_or(Decimal::ZERO, |p| p.position_amt);
    let open_after = client.query_open_orders(&account, Some(SYMBOL)).await?.into_inner();
    emit(&json!({"event":"verification","issued":issued,"venue_ids":venue_ids,
        "orders":rows,"btc_position_after":after_position,"btc_open_orders_after":open_after.len(),
        "completed_pairs":completed_pairs,"cleanup_error":cleanup_error,
        "series_error":result.as_ref().err().map(ToString::to_string),"at":stamp(origin)}));
    clocks(&client, origin, "after").await?;
    let _ = ping(&mut ws, origin, "after").await;
    if !after_position.is_zero()
        || !open_after.is_empty()
        || cleanup_error.is_some()
        || rows.len() != usize::try_from(PAIRS * 2)?
        || rows.iter().any(|o| o.status != "CANCELED" || !o.executed_qty.is_zero())
    {
        return Err("Final state incomplete or unexpected fill; inspect verification record".into());
    }
    result?;
    emit(&json!({"event":"complete","at":stamp(origin),"pairs":completed_pairs}));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn f(message: Value) -> Frame {
        Frame { received: Stamp { wall_ns: 0, mono_ns: 0 }, message }
    }
    #[test]
    fn private_before_ack_matches_by_order_id_and_ignores_unrelated() {
        let frames = vec![
            f(json!({"o":{"s":SYMBOL,"i":77,"X":"NEW","x":"NEW"}})),
            f(json!({"e":"order.place","id":4,"results":{"status":"submitted","order_ids":[77]}})),
        ];
        let r = receipts(&frames, 4, 9, None, false);
        assert!(r.ack_ok);
        assert_eq!(r.private, Some(0));
        assert_eq!(r.order_id, Some(77));
        assert!(receipts(&frames, 5, 10, None, false).private.is_none());
    }
    #[test]
    fn cancel_does_not_accept_new_and_reports_fill() {
        let frames = vec![
            f(json!({"o":{"s":SYMBOL,"co":9,"i":77,"X":"NEW","x":"NEW"}})),
            f(json!({"o":{"s":SYMBOL,"co":9,"i":77,"X":"PARTIALLY_FILLED","x":"TRADE"}})),
            f(json!({"o":{"s":SYMBOL,"co":9,"i":77,"X":"CANCELED","x":"CANCELED"}})),
        ];
        let r = receipts(&frames, 4, 9, Some(77), true);
        assert!(r.unexpected_fill);
        assert_eq!(r.private, Some(2));
        assert!(!r.ack_ok);
    }
    #[test]
    fn experimental_price_and_order_are_balanced() {
        assert_eq!(
            distant_price(Decimal::from(80_001), Decimal::ONE).unwrap(),
            Decimal::from(72_000)
        );
        assert!(distant_price(Decimal::ZERO, Decimal::ONE).is_err());
        assert_eq!(pair_types(0), ["PostOnly", "Limit"]);
        assert_eq!(pair_types(1), ["Limit", "PostOnly"]);
    }
}
