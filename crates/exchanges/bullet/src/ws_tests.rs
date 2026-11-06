//! Offline tests of the real SDK, broker and shared WS muxer. No exchange credentials.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

use bb_core::broker::Broker;
use bb_core::types::{AmendOrder, CancelOrder, NewOrder, OrderType, Side};
use bullet_rust_sdk::types::bullet_exchange_interface::schema::Schema;
use bullet_rust_sdk::{Keypair, ManagedWsConfig, SignedTransaction};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::{Duration, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{WebSocketStream, accept_async};
use tracing::instrument::WithSubscriber;

use super::*;

struct Venue {
    broker: Arc<BulletBroker>,
    main: Option<WebSocketStream<TcpStream>>,
    backup: Option<WebSocketStream<TcpStream>>,
    sockets: mpsc::UnboundedReceiver<WebSocketStream<TcpStream>>,
    client: Arc<Client>,
    executions: mpsc::UnboundedReceiver<ExecutionUpdate>,
    tasks: Vec<JoinHandle<()>>,
    window: Arc<std::sync::atomic::AtomicU64>,
    requests: mpsc::UnboundedSender<WsRequest>,
    health: Arc<ConnectionHealth>,
}
impl Drop for Venue {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

async fn venue() -> Venue {
    venue_with_recording(None).await
}

async fn venue_with_recording(recording: Option<RecordingSender>) -> Venue {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (socket_tx, mut sockets) = mpsc::unbounded_channel();
    let window = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let server_window = window.clone();
    let server = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let socket_tx = socket_tx.clone();
            let window = server_window.clone();
            tokio::spawn(async move {
                let mut head = [0; 1024];
                let n = socket.peek(&mut head).await.unwrap();
                if String::from_utf8_lossy(&head[..n]).starts_with("GET /ws ") {
                    let mut ws = accept_async(socket).await.unwrap();
                    ws.send(Message::Text(
                        json!({"e":"status","E":1,"status":"connected","clientId":"test-client"})
                            .to_string()
                            .into(),
                    ))
                    .await
                    .unwrap();
                    let _ = socket_tx.send(ws);
                    return;
                }
                let mut request = Vec::new();
                while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                    let n = socket.read(&mut head).await.unwrap();
                    if n == 0 {
                        return;
                    }
                    request.extend_from_slice(&head[..n]);
                }
                let line = String::from_utf8_lossy(&request);
                // An accidental REST submission cannot pass these tests.
                assert!(line.starts_with("GET "), "Unexpected REST write: {line}");
                let body = if line.starts_with("GET /rollup/schema ") {
                    json!({"chain_hash":format!("0x{}", "07".repeat(32)),
                        "schema":Schema::of_single_type::<SignedTransaction>().unwrap()})
                } else if line.starts_with("GET /fapi/v1/userTrades?") {
                    json!({"data":[]})
                } else if line.starts_with("GET /fapi/v1/openOrders?") {
                    json!([])
                } else if line.starts_with("GET /api/v1/addressState?") {
                    let signer = Keypair::from_bytes([7; 32]).address();
                    assert!(line.contains(&format!("address={signer}")), "Use signer, not master");
                    json!({"address":signer,"window":window.load(std::sync::atomic::Ordering::SeqCst)})
                } else {
                    assert!(line.starts_with("GET /fapi/v1/exchangeInfo "));
                    json!({"assets":[],"rateLimits":[],"symbols":[{
                        "marketId":1,"symbol":"BTC-USD","pair":"BTC-USD","contractType":"CryptoPerp",
                        "deliveryDate":0,"onboardDate":0,"status":"TRADING","baseAsset":"BTC",
                        "baseAssetId":2,"quoteAsset":"USD","marginAsset":"USD","pricePrecision":8,
                        "quantityPrecision":4,"baseAssetPrecision":8,"quotePrecision":8,"underlyingType":"COIN",
                        "underlyingSubType":[],"settlePlan":0,"triggerProtect":"0.4","filters":[
                            {"filterType":"PRICE_FILTER","tickSize":"1","minPrice":"0","maxPrice":"0"},
                            {"filterType":"LOT_SIZE","stepSize":"0.0001","minQty":"0.0001","maxQty":"0"}],
                        "orderTypes":["LIMIT","MARKET"],"timeInForce":["GTC","IOC"],
                        "liquidationFee":"0.5","marketTakeBound":"0.4","makerFeeBps":[],"takerFeeBps":[]
                    }],"globalConfig":{"maxOrdersPerUser":100,"maxTriggerOrdersPerUser":100,
                        "maxTriggerOrdersToExecutePerMsg":100,"minNotionalTwapValue":"0",
                        "minNotionalTwapValuePerOrder":"0","twapExecutionIntervalSeconds":0},
                        "globalVaultConfig":{"leaderMinimumHoldingPercentage":0,"creationFeeUsdc":"0","minDepositValue":"0"}})
                }.to_string();
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
            });
        }
    });
    let client = Arc::new(
        Client::builder().network(url).keypair(Keypair::from_bytes([7; 32])).build().await.unwrap(),
    );
    let increments = load_increments(&client).await.unwrap();
    let ws = client
        .connect_ws_managed()
        .config(
            ManagedWsConfig::builder()
                .initial_backoff(Duration::from_millis(1))
                .max_retries(2)
                .build(),
        )
        .call()
        .await
        .unwrap();
    let health = Arc::new(ConnectionHealth::default());
    ws.subscribe_raw(vec![Topic::user_orders("test-account").to_string()], None).unwrap();
    let (request_tx, request_rx) = mpsc::unbounded_channel();
    let (execution_tx, executions) = mpsc::unbounded_channel();
    let tx = FeedSenders {
        execution: execution_tx,
        trade: mpsc::unbounded_channel().0,
        lifecycle: mpsc::unbounded_channel().0,
        mark: mpsc::channel(32).0,
    };
    let mux = tokio::spawn(
        muxer_loop(
            ws,
            tx,
            health.clone(),
            recording,
            request_rx,
            client.clone(),
            vec![Topic::user_orders("test-account").to_string()],
        )
        .with_current_subscriber(),
    );
    let broker = Arc::new(BulletBroker::new(
        client.clone(),
        "test-account".into(),
        increments,
        health.clone(),
        request_tx.clone(),
    ));
    let mut main = sockets.recv().await.unwrap();
    subscribe_ack(&mut main).await;
    let mut backup = timeout(Duration::from_secs(2), sockets.recv()).await.unwrap().unwrap();
    subscribe_ack(&mut backup).await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    Venue {
        broker,
        main: Some(main),
        backup: Some(backup),
        sockets,
        client,
        executions,
        tasks: vec![server, mux],
        window,
        requests: request_tx,
        health,
    }
}

fn order(id: &str, reduce_only: bool) -> NewOrder {
    NewOrder {
        symbol: "BTC-USD".into(),
        side: if reduce_only { Side::Sell } else { Side::Buy },
        order_type: OrderType::Market,
        price: Decimal::from(60_000),
        quantity: Decimal::new(1, 4),
        client_id: Some(id.into()),
        reduce_only,
    }
}
async fn command(ws: &mut WebSocketStream<TcpStream>) -> Value {
    loop {
        let msg = timeout(Duration::from_secs(2), ws.next()).await.unwrap().unwrap().unwrap();
        let command: Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
        if command["method"] == "PING" {
            pong(ws, &command).await;
        } else if command["method"] == "SUBSCRIBE" {
            acknowledge_subscription(ws, &command).await;
        } else {
            return command;
        }
    }
}
async fn pong(ws: &mut WebSocketStream<TcpStream>, request: &Value) {
    ws.send(Message::Text(json!({"e":"pong", "id":request["id"], "E":1}).to_string().into()))
        .await
        .unwrap();
}

async fn acknowledge_subscription(ws: &mut WebSocketStream<TcpStream>, request: &Value) {
    ws.send(Message::Text(
        json!({"e":"subscribe","id":request["id"],"E":1,"result":"success"}).to_string().into(),
    ))
    .await
    .unwrap();
}
async fn subscribe_ack(ws: &mut WebSocketStream<TcpStream>) {
    let request = raw_command(ws).await;
    assert_eq!(request["method"], "SUBSCRIBE");
    acknowledge_subscription(ws, &request).await;
}

async fn raw_command(ws: &mut WebSocketStream<TcpStream>) -> Value {
    let msg = timeout(Duration::from_secs(3), ws.next()).await.unwrap().unwrap().unwrap();
    serde_json::from_str(msg.to_text().unwrap()).unwrap()
}

async fn ping_round(ws: &mut WebSocketStream<TcpStream>, delays_ms: [u64; 4]) {
    for delay in delays_ms {
        let request = raw_command(ws).await;
        assert_eq!(request["method"], "PING");
        tokio::time::sleep(Duration::from_millis(delay)).await;
        pong(ws, &request).await;
    }
}

async fn wait_replacement(v: &Venue) {
    timeout(Duration::from_secs(2), async {
        while !v.health.replacement_ready() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn faster_peer_sends_protection_while_old_entry_rpc_remains_unknown() {
    let mut v = venue().await;
    let mut a = v.main.take().unwrap();
    let mut b = v.backup.take().unwrap();
    let broker = v.broker.clone();
    let entry = tokio::spawn(async move { broker.place_orders(&[order("10", false)]).await });
    let entry_rpc = command(&mut a).await;
    tokio::join!(ping_round(&mut a, [80; 4]), ping_round(&mut b, [0; 4]));
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(
        !v.broker.refresh_connection_if_idle().await,
        "cannot discard the unknown RPC's socket"
    );
    let broker = v.broker.clone();
    let close = tokio::spawn(async move { broker.place_orders(&[order("11", true)]).await });
    let close_rpc = command(&mut b).await;
    assert_ne!(entry_rpc["id"], close_rpc["id"]);
    // A reply on the wrong physical socket must not consume the original waiter.
    reply(&mut b, &entry_rpc, &[(10, 999)]).await;
    reply(&mut b, &close_rpc, &[(11, 101)]).await;
    assert!(close.await.unwrap().unwrap()[0].success);
    assert!(!entry.is_finished());
    reply(&mut a, &entry_rpc, &[(10, 100)]).await;
    assert_eq!(entry.await.unwrap().unwrap()[0].order_id.as_deref(), Some("100"));
    wait_replacement(&v).await;
    assert!(v.broker.refresh_connection_if_idle().await);
    let mut replacement = timeout(Duration::from_secs(2), v.sockets.recv()).await.unwrap().unwrap();
    subscribe_ack(&mut replacement).await;
    assert!(v.sockets.try_recv().is_err(), "only the retired slot is recreated");
    assert!(!v.health.take_reconcile_signal());
    assert!(v.health.last_reconnect_at().is_none());
}

#[tokio::test]
async fn first_ping_timeout_moves_protection_without_replaying_unknown_rpc() {
    let mut v = venue().await;
    let mut a = v.main.take().unwrap();
    let mut b = v.backup.take().unwrap();
    let broker = v.broker.clone();
    let entry = tokio::spawn(async move { broker.place_orders(&[order("10", false)]).await });
    let entry_rpc = command(&mut a).await;
    // A was the faster measured route before it silently stopped responding.
    tokio::join!(ping_round(&mut a, [0; 4]), ping_round(&mut b, [30; 4]));
    let (sent, mut received) = mpsc::unbounded_channel();
    v.tasks.push(tokio::spawn(async move {
        while let Some(Ok(frame)) = b.next().await {
            let request: Value = serde_json::from_str(frame.to_text().unwrap()).unwrap();
            if request["method"] == "PING" {
                tokio::time::sleep(Duration::from_millis(30)).await;
                pong(&mut b, &request).await;
            } else {
                sent.send(request.clone()).unwrap();
                reply(&mut b, &request, &[(11, 101)]).await;
            }
        }
    }));
    let first_ping =
        timeout(PROBE_INTERVAL + Duration::from_secs(2), a.next()).await.unwrap().unwrap().unwrap();
    let first_ping: Value = serde_json::from_str(first_ping.to_text().unwrap()).unwrap();
    assert_eq!(first_ping["method"], "PING");
    // Leave TCP open. One timeout must suffice, not all four failed probes.
    tokio::time::sleep(PING_TIMEOUT + Duration::from_millis(50)).await;
    let broker = v.broker.clone();
    let close = tokio::spawn(async move { broker.place_orders(&[order("11", true)]).await });
    let close_rpc = timeout(Duration::from_millis(500), received.recv()).await.unwrap().unwrap();
    assert_ne!(close_rpc["id"], entry_rpc["id"]);
    assert!(close.await.unwrap().unwrap()[0].success);
    assert!(!entry.is_finished(), "the unknown entry stays on its original socket");
    assert!(!v.health.replacement_ready(), "one missed probe is not a rotation median");
    assert!(!v.health.take_reconcile_signal());

    // A late pong does not revive the expired probe. Its late RPC still resolves.
    pong(&mut a, &first_ping).await;
    reply(&mut a, &entry_rpc, &[(10, 100)]).await;
    assert_eq!(entry.await.unwrap().unwrap()[0].order_id.as_deref(), Some("100"));
    // Only the next three real probes can complete a fresh speed measurement.
    // Any replayed entry or close on A would fail this PING assertion.
    for _ in 0..3 {
        let request = raw_command(&mut a).await;
        assert_eq!(request["method"], "PING");
        pong(&mut a, &request).await;
    }
    tokio::time::sleep(Duration::from_millis(10)).await;
    let broker = v.broker.clone();
    let next_close = tokio::spawn(async move { broker.place_orders(&[order("12", true)]).await });
    let request = command(&mut a).await;
    assert_ne!(request["id"], entry_rpc["id"]);
    reply(&mut a, &request, &[(12, 102)]).await;
    assert!(next_close.await.unwrap().unwrap()[0].success);
    assert!(received.try_recv().is_err(), "no duplicate commands on the peer");
    assert!(v.sockets.try_recv().is_err(), "send-route changes do not reconnect sockets");
}

#[tokio::test]
async fn recovered_pong_does_not_reuse_a_series_ending_in_timeout() {
    let mut v = venue().await;
    let mut a = v.main.take().unwrap();
    let mut b = v.backup.take().unwrap();
    let (sent, mut received) = mpsc::unbounded_channel();
    v.tasks.push(tokio::spawn(async move {
        let mut client_id = 11;
        while let Some(Ok(frame)) = b.next().await {
            let request: Value = serde_json::from_str(frame.to_text().unwrap()).unwrap();
            if request["method"] == "PING" {
                tokio::time::sleep(Duration::from_millis(30)).await;
                pong(&mut b, &request).await;
            } else {
                sent.send(request.clone()).unwrap();
                reply(&mut b, &request, &[(client_id, client_id + 90)]).await;
                client_id += 1;
            }
        }
    }));
    for _ in 0..3 {
        let request = raw_command(&mut a).await;
        assert_eq!(request["method"], "PING");
        pong(&mut a, &request).await;
    }
    let request = raw_command(&mut a).await;
    assert_eq!(request["method"], "PING");
    tokio::time::sleep(PING_TIMEOUT + Duration::from_millis(50)).await;
    // Median of fast, fast, timeout is still fast. Nevertheless A is unresponsive.
    let broker = v.broker.clone();
    let close = tokio::spawn(async move { broker.place_orders(&[order("11", true)]).await });
    timeout(Duration::from_millis(500), received.recv()).await.unwrap().unwrap();
    assert!(close.await.unwrap().unwrap()[0].success);

    let request =
        timeout(PROBE_INTERVAL + Duration::from_secs(2), a.next()).await.unwrap().unwrap().unwrap();
    let request: Value = serde_json::from_str(request.to_text().unwrap()).unwrap();
    assert_eq!(request["method"], "PING");
    pong(&mut a, &request).await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    let broker = v.broker.clone();
    let close = tokio::spawn(async move { broker.place_orders(&[order("12", true)]).await });
    timeout(Duration::from_millis(500), received.recv()).await.unwrap().unwrap();
    assert!(close.await.unwrap().unwrap()[0].success);
    assert!(v.sockets.try_recv().is_err());
}

#[tokio::test]
async fn faster_peer_below_rotation_threshold_gets_new_commands() {
    let mut v = venue().await;
    let mut a = v.main.take().unwrap();
    let mut b = v.backup.take().unwrap();
    tokio::join!(ping_round(&mut a, [35; 4]), ping_round(&mut b, [0; 4]));
    tokio::time::sleep(Duration::from_millis(10)).await;
    let broker = v.broker.clone();
    let close = tokio::spawn(async move { broker.place_orders(&[order("11", true)]).await });
    let request = command(&mut b).await;
    reply(&mut b, &request, &[(11, 101)]).await;
    assert!(close.await.unwrap().unwrap()[0].success);
    assert!(!v.broker.refresh_connection_if_idle().await);
    assert!(v.sockets.try_recv().is_err(), "a healthy slower socket remains connected");
    assert!(timeout(Duration::from_millis(30), a.next()).await.is_err());
}

#[tokio::test]
async fn both_ping_timeouts_do_not_forbid_protective_submission() {
    let mut v = venue().await;
    let mut a = v.main.take().unwrap();
    let mut b = v.backup.take().unwrap();
    let (first_a, first_b) = tokio::join!(raw_command(&mut a), raw_command(&mut b));
    assert_eq!(first_a["method"], "PING");
    assert_eq!(first_b["method"], "PING");
    tokio::time::sleep(PING_TIMEOUT + Duration::from_millis(50)).await;
    let broker = v.broker.clone();
    let close = tokio::spawn(async move { broker.place_orders(&[order("11", true)]).await });
    let (slot, request) = tokio::select! { r=command(&mut a)=>(0,r), r=command(&mut b)=>(1,r) };
    reply(if slot == 0 { &mut a } else { &mut b }, &request, &[(11, 101)]).await;
    assert!(close.await.unwrap().unwrap()[0].success);
    assert!(!v.health.take_reconcile_signal());
    assert!(v.sockets.try_recv().is_err());
}

#[tokio::test]
async fn warmup_and_one_ping_spike_do_not_replace_a_healthy_ws() {
    let mut v = venue().await;
    let mut a = v.main.take().unwrap();
    let mut b = v.backup.take().unwrap();
    tokio::join!(ping_round(&mut a, [80, 0, 80, 0]), ping_round(&mut b, [0; 4]));
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(!v.broker.refresh_connection_if_idle().await);
    assert!(v.sockets.try_recv().is_err());
    assert!(!v.health.take_reconcile_signal());
}

#[tokio::test]
async fn two_slow_routes_replace_one_while_the_other_keeps_serving_orders() {
    let mut v = venue().await;
    let mut a = v.main.take().unwrap();
    let mut b = v.backup.take().unwrap();
    tokio::join!(ping_round(&mut a, [80; 4]), ping_round(&mut b, [140; 4]));
    wait_replacement(&v).await;
    assert!(v.broker.refresh_connection_if_idle().await);
    let _unready = timeout(Duration::from_secs(2), v.sockets.recv()).await.unwrap().unwrap();
    let broker = v.broker.clone();
    let close = tokio::spawn(async move { broker.place_orders(&[order("11", true)]).await });
    let request = command(&mut a).await;
    reply(&mut a, &request, &[(11, 101)]).await;
    assert!(close.await.unwrap().unwrap()[0].success);
    assert!(!v.health.is_disconnected());
    assert!(v.sockets.try_recv().is_err(), "no third private probe socket");
}

#[tokio::test]
async fn peer_reconnect_keeps_continuous_source_and_deduplicates_partial_fills() {
    let mut v = venue().await;
    let mut a = v.main.take().unwrap();
    let mut b = v.backup.take().unwrap();
    let fill = json!({"e":"ORDER_TRADE_UPDATE","E":3,"o":{
        "s":"BTC-USD","i":100,"co":10,"X":"PARTIALLY_FILLED","x":"TRADE","T":2,"th":"fill-tx",
        "S":"BUY","l":"0.0003","L":"60000","z":"0.0003","rs":"0", "t":1,"m":false,"n":"0","N":"USD","rp":"0"}});
    a.send(Message::Text(fill.to_string().into())).await.unwrap();
    let first = v.executions.recv().await.unwrap();
    assert!(first.trade.is_some());
    b.send(Message::Text(fill.to_string().into())).await.unwrap();
    let second = v.executions.recv().await.unwrap();
    assert!(second.trade.is_none());
    assert_eq!(second.lifecycle.order.filled_quantity, Decimal::new(3, 4));
    assert_eq!(second.lifecycle.order.status, bb_core::types::OrderStatus::Cancelled);
    b.close(None).await.unwrap();
    let mut b = timeout(Duration::from_secs(3), v.sockets.recv()).await.unwrap().unwrap();
    subscribe_ack(&mut b).await;
    b.send(Message::Text(fill.to_string().into())).await.unwrap();
    let replay = v.executions.recv().await.unwrap();
    assert!(replay.trade.is_none());
    assert!(replay.stream_started_at > first.stream_started_at);
    assert!(!v.health.take_reconcile_signal(), "the other subscription covered the gap");
    assert!(!v.health.is_disconnected());
}

#[tokio::test]
async fn losing_both_private_sources_requests_recovery_but_allows_new_reduce_only() {
    let mut v = venue().await;
    let mut a = v.main.take().unwrap();
    let mut b = v.backup.take().unwrap();
    // Stop the SDK's fast-reconnecting first source by retaining its new socket
    // without acknowledging its replayed subscription, then lose the peer.
    a.close(None).await.unwrap();
    let mut a = timeout(Duration::from_secs(2), v.sockets.recv()).await.unwrap().unwrap();
    assert!(!v.health.take_reconcile_signal());
    b.close(None).await.unwrap();
    let mut b = timeout(Duration::from_secs(3), v.sockets.recv()).await.unwrap().unwrap();
    assert!(v.health.take_reconcile_signal());
    subscribe_ack(&mut a).await;
    subscribe_ack(&mut b).await;
    // Observe a frame behind the subscription ack before issuing the command.
    a.send(Message::Text(
        json!({"e":"ORDER_TRADE_UPDATE","E":1,"o":{
        "s":"BTC-USD","i":999,"co":999,"X":"CANCELED","x":"CANCELED","T":1,"th":"barrier"}})
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    v.executions.recv().await.unwrap();
    let broker = v.broker.clone();
    let close = tokio::spawn(async move { broker.place_orders(&[order("11", true)]).await });
    let (owner, request) = tokio::select! { r=command(&mut a)=>(0,r),r=command(&mut b)=>(1,r) };
    reply(if owner == 0 { &mut a } else { &mut b }, &request, &[(11, 101)]).await;
    assert!(close.await.unwrap().unwrap()[0].success);
    assert!(v.health.last_reconnect_at().is_none());
}
async fn reply(ws: &mut WebSocketStream<TcpStream>, request: &Value, ids: &[(u64, u64)]) {
    let event = match request["method"].as_str().unwrap() {
        "ORDER.PLACE" => "order.place",
        "ORDER.CANCEL" => "order.cancel",
        "ORDER.AMEND" => "order.amend",
        "ORDER.CANCEL_ALL" => "order.cancelAll",
        _ => unreachable!(),
    };
    ws.send(Message::Text(json!({"e":event,"id":request["id"],"E":1,
        "results":{"tx_id":"test-tx","status":"submitted","order_ids":ids.iter().map(|(_, id)| id).collect::<Vec<_>>(),
        "client_order_ids":ids.iter().map(|(id, _)| id).collect::<Vec<_>>()}}).to_string().into())).await.unwrap();
}

#[tokio::test]
async fn pending_entry_and_unavailable_recorder_do_not_block_fills_or_reduce_only_rpc() {
    // Stall the real nonblocking logger's writer and fill its queue. Neither
    // that disk stall nor an unavailable recorder may stall this socket.
    let (release, blocked) = std::sync::mpsc::channel();
    let (entered, started) = oneshot::channel();
    let output = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (logs, guard) = tracing_appender::non_blocking::NonBlockingBuilder::default()
        .buffered_lines_limit(1)
        .finish(LogOutput { output, blocked: Some(blocked), entered: Some(entered) });
    let drops = logs.error_counter();
    let subscriber =
        tracing_subscriber::fmt().with_ansi(false).without_time().with_writer(logs).finish();
    let _subscriber = tracing::subscriber::set_default(subscriber);
    tracing::info!("occupy log writer");
    timeout(Duration::from_secs(1), started).await.unwrap().unwrap();
    let (recording, mut records) = RecordingSender::channel(1);
    let mut v = venue_with_recording(Some(recording.clone())).await;
    let mut ws = v.main.take().unwrap();
    let broker = v.broker.clone();
    let entry = tokio::spawn(async move { broker.place_orders(&[order("10", false)]).await });
    let entry_request = command(&mut ws).await;
    ws.send(Message::Text(
        json!({"e":"error","E":2,"error":{"code":-2010,"msg":"test"}}).to_string().into(),
    ))
    .await
    .unwrap();
    ws.send(Message::Text(
        json!({"e":"ORDER_TRADE_UPDATE","E":3,"o":{
        "s":"BTC-USD","i":100,"co":10,"X":"PARTIALLY_FILLED","x":"TRADE","T":3,"th":"fill-tx",
        "S":"BUY","o":"LIMIT","f":"IOC","p":"60000","q":"0.0001","l":"0.0001",
        "L":"60000","z":"0.0001","t":1,"m":false,"n":"0","N":"USD","rp":"0"}})
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    let update = timeout(Duration::from_secs(1), v.executions.recv()).await.unwrap().unwrap();
    assert!(update.trade.is_some());
    assert!(!entry.is_finished());
    // The undrained recorder was full during both the error and the fill.
    assert_eq!(records.try_recv().unwrap().unwrap().kind, "rpc.request");
    recording.record("test", "probe", || Ok("{}".into()));
    assert_eq!(records.try_recv().unwrap().unwrap().dropped_total, 2);
    drop(records); // Losing the writer must not prevent the protective RPC either.
    let broker = v.broker.clone();
    let close = tokio::spawn(async move { broker.place_orders(&[order("11", true)]).await });
    let close_request = command(&mut ws).await;
    assert_ne!(entry_request["id"], close_request["id"]);
    reply(&mut ws, &close_request, &[(11, 101)]).await;
    assert_eq!(close.await.unwrap().unwrap()[0].order_id.as_deref(), Some("101"));
    assert!(!entry.is_finished());
    reply(&mut ws, &entry_request, &[(10, 100)]).await;
    assert_eq!(entry.await.unwrap().unwrap()[0].order_id.as_deref(), Some("100"));
    assert!(drops.dropped_lines() > 0);
    release.send(()).unwrap();
    drop(guard);
}

// Test-only writer: records ordinary logs and can simulate an indefinitely slow disk.
struct LogOutput {
    output: Arc<std::sync::Mutex<Vec<u8>>>,
    blocked: Option<std::sync::mpsc::Receiver<()>>,
    entered: Option<oneshot::Sender<()>>,
}
impl std::io::Write for LogOutput {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if let Some(entered) = self.entered.take() {
            let _ = entered.send(());
        }
        if let Some(blocked) = self.blocked.take() {
            let _ = blocked.recv();
        }
        self.output.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn recording_keeps_partial_ioc_replays_and_late_rpc_results() {
    let output = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (logs, guard) = tracing_appender::non_blocking(LogOutput {
        output: output.clone(),
        blocked: None,
        entered: None,
    });
    let subscriber =
        tracing_subscriber::fmt().with_ansi(false).without_time().with_writer(logs).finish();
    let _subscriber = tracing::subscriber::set_default(subscriber);
    let (recording, mut records) = RecordingSender::channel(16);
    let mut v = venue_with_recording(Some(recording)).await;
    let mut ws = v.main.take().unwrap();
    let broker = v.broker.clone();
    let entry = tokio::spawn(async move {
        let mut entry = order("10", false);
        entry.quantity = Decimal::new(8, 4);
        broker.place_orders(&[entry]).await
    });
    let entry_request = command(&mut ws).await;
    let fill = json!({"e":"ORDER_TRADE_UPDATE","E":3,"o":{
        "s":"BTC-USD","i":100,"co":10,"X":"PARTIALLY_FILLED","x":"TRADE","T":2,"th":"fill-tx",
        "S":"BUY","p":"60000","q":"0.0008","l":"0.0003",
        "L":"60000","z":"0.0003","Z":"18","rs":"0","t":1,"m":false,"n":"0","N":"USD","rp":"0"}});
    let frames = [
        json!({"e":"ORDER_TRADE_UPDATE","E":1,"o":{
            "s":"BTC-USD","i":100,"co":10,"X":"NEW","x":"NEW","T":1,"th":"entry-tx",
            "S":"BUY","o":"LIMIT","f":"IOC","p":"60000","q":"0.0008"}}),
        fill.clone(),
        fill,
        json!({"e":"ORDER_TRADE_UPDATE","E":4,"o":{
            "s":"BTC-USD","i":100,"co":10,"X":"EXPIRED","x":"EXPIRED","T":4,"th":"entry-tx"}}),
    ];
    for (index, frame) in frames.iter().enumerate() {
        ws.send(Message::Text(frame.to_string().into())).await.unwrap();
        let event = timeout(Duration::from_secs(1), v.executions.recv()).await.unwrap().unwrap();
        assert_eq!(event.trade.is_some(), index == 1, "only the first fill changes inventory");
        if matches!(index, 1 | 2) {
            assert_eq!(event.lifecycle.order.status, bb_core::types::OrderStatus::Cancelled);
            assert_eq!(event.lifecycle.order.quantity, Decimal::new(8, 4));
            assert_eq!(event.lifecycle.order.filled_quantity, Decimal::new(3, 4));
        }
    }
    entry.abort();
    let _ = entry.await; // As when the terminal private event finishes the order first.

    let broker = v.broker.clone();
    let close = tokio::spawn(async move { broker.place_orders(&[order("11", true)]).await });
    let close_request = command(&mut ws).await;
    let rejection = json!({"id":close_request["id"],"E":5,
        "error":{"code":-2010,"msg":"size_must_be_positive"}});
    ws.send(Message::Text(rejection.to_string().into())).await.unwrap();
    assert!(!close.await.unwrap().unwrap()[0].success);
    reply(&mut ws, &entry_request, &[(10, 100)]).await; // No live waiter, still recorded.

    let mut last_monotonic = 0;
    let mut saved = Vec::new();
    for _ in 0..8 {
        let record =
            timeout(Duration::from_secs(1), records.recv()).await.unwrap().unwrap().unwrap();
        assert_eq!(record.source, "bullet");
        assert_eq!(record.dropped_total, 0);
        assert!(record.recv_unix_ns > 0);
        assert!(record.recv_monotonic_ns >= last_monotonic);
        last_monotonic = record.recv_monotonic_ns;
        saved.push((record.kind, serde_json::from_str::<Value>(&record.payload).unwrap()));
    }
    for (index, request, reduce_only, client_id, size) in
        [(0, &entry_request, false, 10, "0.0008"), (5, &close_request, true, 11, "0.0001")]
    {
        let (kind, payload) = &saved[index];
        assert_eq!(*kind, "rpc.request");
        assert_eq!(payload["id"], request["id"]);
        assert_eq!(payload["queued"], true);
        assert_eq!(payload["error"], Value::Null);
        let order = &payload["call"]["user"]["place_orders"]["orders"][0];
        assert_eq!(order["client_order_id"], client_id);
        assert_eq!(order["reduce_only"], reduce_only);
        assert_eq!(order["size"], size);
        assert_eq!(order["price"], "60000");
        assert_eq!(order["order_type"], "immediate_or_cancel");
        assert!(!payload.to_string().contains(request["params"]["tx"].as_str().unwrap()));
    }
    for (index, original) in frames.iter().enumerate() {
        let (kind, payload) = &saved[index + 1];
        assert_eq!(*kind, "ORDER_TRADE_UPDATE");
        assert_eq!(payload["E"], original["E"]);
        for (field, value) in original["o"].as_object().unwrap() {
            assert_eq!(&payload["o"][field], value, "private field {field}");
        }
    }
    assert_eq!(saved[6].0, "error");
    assert_eq!(saved[6].1["error"], rejection["error"]);
    assert_eq!(saved[6].1["connection"], 0);
    assert_eq!(saved[7].1["connection"], 0);
    assert_eq!(saved[7].0, "order.place");
    assert_eq!(saved[7].1["id"], entry_request["id"]);
    assert_eq!(saved[7].1["results"]["client_order_ids"], json!([10]));
    assert_eq!(saved[7].1["results"]["order_ids"], json!([100]));

    records.close();
    // Late replies and private statuses still reach the ordinary log without a recorder.
    reply(&mut ws, &entry_request, &[(99, 999)]).await;
    ws.send(Message::Text(
        json!({"e":"ORDER_TRADE_UPDATE","E":6,"o":{
        "s":"BTC-USD","i":999,"co":99,"X":"CANCELED","x":"CANCELED","T":6,"th":"late-tx"}})
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    timeout(Duration::from_secs(1), v.executions.recv()).await.unwrap().unwrap();
    drop(guard);
    let logs = String::from_utf8(output.lock().unwrap().clone()).unwrap();
    for status in ["NEW", "PARTIALLY_FILLED", "EXPIRED", "CANCELED"] {
        assert!(
            logs.contains(&format!("status={status}")),
            "missing private status {status}: {logs}"
        );
    }
    assert!(logs.contains("Bullet RPC request"));
    assert!(logs.contains("client_id=10"));
    assert!(logs.contains("reduce_only=true"));
    assert!(logs.contains("Bullet RPC error reply"));
    assert!(logs.contains("size_must_be_positive"));
    assert!(logs.contains("Bullet RPC reply"));
    assert!(logs.contains("client_ids=[99]"));
    assert!(!logs.contains(entry_request["params"]["tx"].as_str().unwrap()));
}

#[tokio::test]
async fn trading_methods_use_same_ws_and_batch_ids_follow_client_ids() {
    let mut v = venue().await;
    let mut ws = v.main.take().unwrap();
    let broker = v.broker.clone();
    let placed = tokio::spawn(async move {
        broker.place_orders(&[order("10", false), order("11", false)]).await
    });
    let req = command(&mut ws).await;
    reply(&mut ws, &req, &[(11, 101), (10, 100)]).await;
    let results = placed.await.unwrap().unwrap();
    assert_eq!(results[0].order_id.as_deref(), Some("100"));
    assert_eq!(results[1].order_id.as_deref(), Some("101"));
    let cancel = CancelOrder {
        symbol: "BTC-USD".into(),
        order_id: "100".into(),
        client_id: Some("10".into()),
    };
    let broker = v.broker.clone();
    let c = cancel.clone();
    let cancelled = tokio::spawn(async move { broker.cancel_orders(&[c]).await });
    let req = command(&mut ws).await;
    assert_eq!(req["method"], "ORDER.CANCEL");
    reply(&mut ws, &req, &[(10, 100)]).await;
    assert!(cancelled.await.unwrap().unwrap()[0].success);
    let broker = v.broker.clone();
    let amended = tokio::spawn(async move {
        broker.amend_orders(&[AmendOrder { cancel, new_order: order("12", false) }]).await
    });
    let req = command(&mut ws).await;
    assert_eq!(req["method"], "ORDER.AMEND");
    reply(&mut ws, &req, &[(12, 102)]).await;
    assert!(amended.await.unwrap().unwrap()[0].success);
    let broker = v.broker.clone();
    let all = tokio::spawn(async move { broker.cancel_all_orders("BTC-USD").await });
    let req = command(&mut ws).await;
    assert_eq!(req["method"], "ORDER.CANCEL_ALL");
    reply(&mut ws, &req, &[]).await;
    all.await.unwrap().unwrap();
    assert!(v.sockets.try_recv().is_err());
}

#[tokio::test]
async fn private_reconnect_does_not_invalidate_independent_public_books() {
    let mut v = venue().await;
    let mut ws = v.main.take().unwrap();
    let cutoff = Instant::now();
    v.health.flag_market_reset(cutoff);
    ws.close(None).await.unwrap();
    let _ws = timeout(Duration::from_secs(2), v.sockets.recv()).await.unwrap().unwrap();
    assert!(!v.broker.take_reconcile_signal());
    assert_eq!(v.broker.last_reconnect_at(), Some(cutoff));
}

#[tokio::test]
async fn public_pool_merges_three_sdk_streams_and_recovers_without_blocking_reduce_only() {
    let (recorder, mut records) = RecordingSender::channel(32);
    let mut v = venue_with_recording(Some(recorder.clone())).await;
    let mut private = v.main.take().unwrap();
    let (bbo, mut quotes) = mpsc::channel(16);
    let (book, depth_rx) = mpsc::channel(16);
    drop(depth_rx); // reference-arb consumes BBO only, recording retains depth.
    v.tasks.push(tokio::spawn(crate::market_data::run(
        v.client.clone(),
        "BTC-USD".into(),
        bbo,
        book,
        v.health.clone(),
        Some(recorder),
    )));
    let mut public = Vec::new();
    for _ in 0..3 {
        let mut ws = timeout(Duration::from_secs(2), v.sockets.recv()).await.unwrap().unwrap();
        let sub = raw_command(&mut ws).await;
        assert_eq!(sub["method"], "SUBSCRIBE");
        assert_eq!(sub["params"], json!(["BTC-USD@depth20", "BTC-USD@aggTrade"]));
        public.push(ws);
    }
    let snapshot = |u, t, bid| {
        json!({"e":"depthUpdate","s":"BTC-USD","E":t+10,
        "T":t,"U":u,"u":u,"pu":0,"mt":"s","b":[[bid,"1"]],"a":[["61000","2"]]})
    };
    for (i, ws) in public.iter_mut().enumerate() {
        ws.send(Message::Text(snapshot(100 + i, 100, "60000").to_string().into())).await.unwrap();
    }
    assert_eq!(
        timeout(Duration::from_secs(1), quotes.recv())
            .await
            .unwrap()
            .unwrap()
            .orderbook
            .best_bid()
            .unwrap()
            .price,
        Decimal::from(60_000)
    );
    let record = timeout(Duration::from_secs(1), records.recv()).await.unwrap().unwrap().unwrap();
    assert_eq!(record.kind, "depthUpdate");
    assert_eq!(serde_json::from_str::<Value>(&record.payload).unwrap()["mt"], "s");
    assert!(timeout(Duration::from_millis(30), quotes.recv()).await.is_err());
    assert!(records.try_recv().is_err());
    let trade = json!({"e":"aggTrade","E":200,"T":190,"s":"BTC-USD","a":42,
        "p":"60000","q":"0.001","f":42,"l":42,"m":false,"th":"public-trade"});
    for ws in &mut public {
        ws.send(Message::Text(trade.to_string().into())).await.unwrap();
    }
    assert_eq!(
        timeout(Duration::from_secs(1), records.recv()).await.unwrap().unwrap().unwrap().kind,
        "aggTrade"
    );
    assert!(timeout(Duration::from_millis(30), records.recv()).await.is_err());
    assert!(v.executions.try_recv().is_err(), "public prints must not become private fills");

    let broker = v.broker.clone();
    let entry = tokio::spawn(async move { broker.place_orders(&[order("10", false)]).await });
    let entry_rpc = command(&mut private).await; // Deliberately unknown outcome.
    for (i, ws) in public.iter_mut().enumerate() {
        // The SDK can drop a frame when its own queue is full. pu catches that
        // real failure mode and replaces this public socket only.
        let gap = json!({"e":"depthUpdate","s":"BTC-USD","E":210,"T":200,
            "U":500+i,"u":500+i,"pu":499+i,"mt":"u","b":[],"a":[]});
        ws.send(Message::Text(gap.to_string().into())).await.unwrap();
    }
    let mut replacements = Vec::new();
    for _ in 0..3 {
        let mut ws = timeout(Duration::from_secs(2), v.sockets.recv()).await.unwrap().unwrap();
        assert_eq!(raw_command(&mut ws).await["method"], "SUBSCRIBE");
        replacements.push(ws); // Withhold every new market snapshot.
    }
    assert!(v.health.last_reconnect_at().is_some());
    assert!(!v.health.take_reconcile_signal(), "public loss must not start private REST recovery");
    assert!(!v.health.is_disconnected());
    let broker = v.broker.clone();
    let close = tokio::spawn(async move { broker.place_orders(&[order("11", true)]).await });
    let close_rpc = command(&mut private).await;
    reply(&mut private, &close_rpc, &[(11, 101)]).await;
    assert!(timeout(Duration::from_secs(1), close).await.unwrap().unwrap().is_ok());
    assert!(!entry.is_finished());
    let cutoff = v.health.last_reconnect_at().unwrap();
    replacements[0]
        .send(Message::Text(snapshot(999_000, 99, "59000").to_string().into()))
        .await
        .unwrap();
    assert!(timeout(Duration::from_millis(30), quotes.recv()).await.is_err());
    replacements[0]
        .send(Message::Text(snapshot(999_001, 100, "60000").to_string().into()))
        .await
        .unwrap();
    let quote = timeout(Duration::from_secs(1), quotes.recv()).await.unwrap().unwrap();
    assert!(quote.received_at >= cutoff);
    assert_eq!(quote.orderbook.best_bid().unwrap().price, Decimal::from(60_000));
    // Dedup remains intact after replacement.
    replacements[0].send(Message::Text(trade.to_string().into())).await.unwrap();
    reply(&mut private, &entry_rpc, &[(10, 100)]).await;
    entry.await.unwrap().unwrap();
    drop(quotes);
    timeout(Duration::from_secs(1), v.tasks.pop().unwrap()).await.unwrap().unwrap();
    while let Ok(Some(record)) = records.try_recv() {
        assert_ne!(record.kind, "aggTrade", "reconnect replay must not duplicate public trades");
    }
}

#[tokio::test]
async fn cancelled_waiter_and_reconnect_do_not_reissue_entry_or_mix_replies() {
    let mut v = venue().await;
    let mut ws = v.main.take().unwrap();
    let broker = v.broker.clone();
    let entry = tokio::spawn(async move { broker.place_orders(&[order("10", false)]).await });
    let old = command(&mut ws).await;
    entry.abort();
    let _ = entry.await; // a terminal private update made this RPC obsolete
    ws.close(None).await.unwrap();
    drop(ws);
    let mut ws = timeout(Duration::from_secs(2), v.sockets.recv()).await.unwrap().unwrap();
    subscribe_ack(&mut ws).await;
    let mut peer = v.backup.take().unwrap();
    assert!(!v.broker.take_reconcile_signal());
    let broker = v.broker.clone();
    let close = tokio::spawn(async move { broker.place_orders(&[order("11", true)]).await });
    let req = command(&mut peer).await;
    assert_ne!(req["id"], old["id"]);
    reply(&mut ws, &old, &[(10, 100)]).await;
    tokio::task::yield_now().await;
    assert!(!close.is_finished());
    reply(&mut peer, &req, &[(11, 101)]).await;
    assert_eq!(close.await.unwrap().unwrap()[0].client_id.as_deref(), Some("11"));
    assert!(timeout(Duration::from_millis(30), ws.next()).await.is_err());
}

#[tokio::test]
async fn reconnect_keeps_unknown_entry_correlated_while_close_completes() {
    let mut v = venue().await;
    let mut ws = v.main.take().unwrap();
    let broker = v.broker.clone();
    let entry = tokio::spawn(async move { broker.place_orders(&[order("10", false)]).await });
    let entry_request = command(&mut ws).await;
    ws.close(None).await.unwrap();
    drop(ws);
    let mut ws = timeout(Duration::from_secs(2), v.sockets.recv()).await.unwrap().unwrap();
    subscribe_ack(&mut ws).await;
    let mut peer = v.backup.take().unwrap();
    assert!(!v.broker.take_reconcile_signal());
    assert!(!entry.is_finished());
    let broker = v.broker.clone();
    let close = tokio::spawn(async move { broker.place_orders(&[order("11", true)]).await });
    let close_request = command(&mut peer).await;
    assert_ne!(close_request["id"], entry_request["id"]);
    reply(&mut peer, &close_request, &[(11, 101)]).await;
    assert!(close.await.unwrap().unwrap()[0].success);
    assert!(!entry.is_finished());
    reply(&mut ws, &entry_request, &[(10, 100)]).await;
    assert_eq!(entry.await.unwrap().unwrap()[0].order_id.as_deref(), Some("100"));
    assert!(timeout(Duration::from_millis(30), ws.next()).await.is_err());
}

#[tokio::test]
async fn rejects_are_definite_but_timeouts_and_dropped_transactions_stay_unknown() {
    let mut v = venue().await;
    let mut ws = v.main.take().unwrap();
    for (id, error, rejected) in [
        (10, json!({"code":-2010,"msg":"insufficient margin"}), true),
        (11, json!({"code":-1007,"msg":"timeout"}), false),
        (12, json!({"code":-2010,"msg":"duplicate transaction already processed"}), false),
    ] {
        let broker = v.broker.clone();
        let task =
            tokio::spawn(
                async move { broker.place_orders(&[order(&id.to_string(), false)]).await },
            );
        let req = command(&mut ws).await;
        ws.send(Message::Text(json!({"id":req["id"],"E":1,"error":error}).to_string().into()))
            .await
            .unwrap();
        let result = task.await.unwrap();
        if rejected {
            assert!(!result.unwrap()[0].success);
        } else {
            assert!(result.unwrap_err().is_retryable());
        }
    }
    let broker = v.broker.clone();
    let task = tokio::spawn(async move { broker.place_orders(&[order("13", false)]).await });
    let req = command(&mut ws).await;
    ws.send(Message::Text(
        json!({"e":"order.place","id":req["id"],"E":1,
        "results":{"tx_id":"tx","status":"dropped"}})
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    assert!(task.await.unwrap().unwrap_err().is_retryable());
    assert!(timeout(Duration::from_millis(30), ws.next()).await.is_err());
}

#[tokio::test]
async fn rejection_after_reconnect_does_not_resign_an_unknown_entry() {
    let mut v = venue().await;
    let mut ws = v.main.take().unwrap();
    let broker = v.broker.clone();
    let entry = tokio::spawn(async move { broker.place_orders(&[order("10", false)]).await });
    let req = command(&mut ws).await;
    ws.close(None).await.unwrap();
    drop(ws);
    let mut ws = timeout(Duration::from_secs(2), v.sockets.recv()).await.unwrap().unwrap();
    subscribe_ack(&mut ws).await;
    ws.send(Message::Text(
        json!({"id":req["id"],"E":1,"error":{
        "code":-1022,"msg":"invalid signature"}})
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    assert!(
        timeout(Duration::from_millis(100), ws.next()).await.is_err(),
        "An ambiguous pre-reconnect request must not be re-signed and sent again"
    );
    assert!(
        timeout(Duration::from_secs(1), entry).await.unwrap().unwrap().unwrap_err().is_retryable()
    );
}

#[tokio::test]
async fn signature_rejection_refreshes_once_and_retries_through_ws() {
    let mut v = venue().await;
    let mut ws = v.main.take().unwrap();
    let broker = v.broker.clone();
    let task = tokio::spawn(async move { broker.place_orders(&[order("10", false)]).await });
    let first = command(&mut ws).await;
    ws.send(Message::Text(
        json!({"id":first["id"],"E":1,"error":{"code":-1022,"msg":"invalid signature"}})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    let second = command(&mut ws).await;
    assert_ne!(first["id"], second["id"]);
    assert_ne!(first["params"]["tx"], second["params"]["tx"]);
    ws.send(Message::Text(
        json!({"id":second["id"],"E":1,"error":{"code":-1022,"msg":"invalid signature"}})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    assert!(!task.await.unwrap().unwrap()[0].success);
    assert!(timeout(Duration::from_millis(30), ws.next()).await.is_err());
}

#[tokio::test]
async fn mux_shutdown_wakes_pending_rpc_with_unknown_result() {
    let mut v = venue().await;
    let mut ws = v.main.take().unwrap();
    let broker = v.broker.clone();
    let task = tokio::spawn(async move { broker.place_orders(&[order("10", false)]).await });
    command(&mut ws).await;
    v.tasks[1].abort();
    assert!(
        timeout(Duration::from_secs(1), task).await.unwrap().unwrap().unwrap_err().is_retryable()
    );
}

#[tokio::test]
async fn lost_close_uses_sdk_replay_window_without_resubmission_or_blocking_new_close() {
    let mut v = venue().await;
    let mut ws = v.main.take().unwrap();
    let broker = v.broker.clone();
    let close = tokio::spawn(async move { broker.place_orders(&[order("10", true)]).await });
    let original = command(&mut ws).await;
    let (window_reply, rx) = oneshot::channel();
    v.requests
        .send(WsRequest::SubmissionWindow { client_id: "10".into(), reply: window_reply })
        .unwrap();
    let window = rx.await.unwrap().unwrap();
    // Lose the reply, as in the live incident; the SDK reconnects normally.
    ws.close(None).await.unwrap();
    drop(ws);
    let mut ws = timeout(Duration::from_secs(2), v.sockets.recv()).await.unwrap().unwrap();
    subscribe_ack(&mut ws).await;
    let mut peer = v.backup.take().unwrap();
    for advance in [0, 1000, 1001] {
        v.window.store(window + advance, std::sync::atomic::Ordering::SeqCst);
        let recovered = v.broker.recover_order("BTC-USD", "10", None, 0).await.unwrap();
        assert_eq!(recovered.submission_expired, advance > 1000);
        assert!(recovered.order.is_none()); // expiry never fabricates a cancellation or fill
        assert!(recovered.trades.is_empty());
        assert!(!close.is_finished()); // normal RPC/private evidence still wins if it arrives
    }
    let broker = v.broker.clone();
    let protective = tokio::spawn(async move { broker.place_orders(&[order("11", true)]).await });
    let next = command(&mut peer).await;
    assert_ne!(original["id"], next["id"]);
    reply(&mut peer, &next, &[(11, 101)]).await;
    assert!(protective.await.unwrap().unwrap()[0].success);
    assert!(!close.is_finished());
    assert!(timeout(Duration::from_millis(30), ws.next()).await.is_err());
    close.abort(); // strategy retired the old waiter; metadata must disappear with it
    let _ = close.await;
    let recovered = v.broker.recover_order("BTC-USD", "10", None, 0).await.unwrap();
    assert!(!recovered.submission_expired);
}

#[tokio::test]
async fn public_pending_bbo_wakes_on_capacity_without_delaying_protective_rpc() {
    let mut v = venue().await;
    let (bbo, mut quotes) = mpsc::channel(1);
    let (books, _books) = mpsc::channel(16);
    let (recorder, mut records) = RecordingSender::channel(64);
    v.tasks.push(tokio::spawn(crate::market_data::run(
        v.client.clone(),
        "BTC-USD".into(),
        bbo,
        books,
        v.health.clone(),
        Some(recorder),
    )));
    let mut public = Vec::new();
    for _ in 0..3 {
        let mut ws = timeout(Duration::from_secs(2), v.sockets.recv()).await.unwrap().unwrap();
        assert_eq!(raw_command(&mut ws).await["method"], "SUBSCRIBE");
        public.push(ws);
    }
    let snapshot = |u, t, bid| {
        json!({"e":"depthUpdate","s":"BTC-USD","E":t+10,
        "T":t,"U":u,"u":u,"pu":0,"mt":"s","b":[[bid,"1"]],"a":[["61000","2"]]})
    };
    let mut first_latest_processed = Instant::now();
    for (t, bid) in [(100, "60000"), (200, "59000"), (300, "58000")] {
        for (slot, ws) in public.iter_mut().enumerate() {
            ws.send(Message::Text(snapshot(t + slot as u64, t, bid).to_string().into()))
                .await
                .unwrap();
            let marker = t + slot as u64;
            ws.send(Message::Text(
                json!({"e":"aggTrade","E":t,"T":t,"s":"BTC-USD","a":marker,
                "p":bid,"q":"0.001","f":marker,"l":marker,"m":false,"th":"marker"})
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
            // Recording the trade proves this socket's preceding depth was
            // processed. Leave the first BBO in the full queue throughout.
            loop {
                let record = timeout(Duration::from_secs(1), records.recv())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                if record.kind == "aggTrade" {
                    if t == 300 && slot == 0 {
                        first_latest_processed = Instant::now();
                    }
                    break;
                }
            }
        }
    }

    let mut private = v.main.take().unwrap();
    let broker = v.broker.clone();
    let close = tokio::spawn(async move { broker.place_orders(&[order("77", true)]).await });
    let rpc = command(&mut private).await;
    reply(&mut private, &rpc, &[(77, 777)]).await;
    assert!(timeout(Duration::from_secs(1), close).await.unwrap().unwrap().is_ok());

    assert_eq!(
        quotes.recv().await.unwrap().orderbook.best_bid().unwrap().price,
        Decimal::from(60_000)
    );
    let retried = timeout(Duration::from_secs(1), quotes.recv()).await.unwrap().unwrap();
    assert_eq!(retried.orderbook.best_bid().unwrap().price, Decimal::from(58_000));
    assert!(
        retried.received_at <= first_latest_processed,
        "retain first receipt, not duplicate or retry time"
    );
    public[0].send(Message::Text(snapshot(999, 300, "58000").to_string().into())).await.unwrap();
    assert!(timeout(Duration::from_millis(30), quotes.recv()).await.is_err());
    assert!(records.try_recv().is_err(), "no repeat recording for delivery or duplicate");
}

#[tokio::test]
async fn private_decode_loss_flags_recovery_before_cancel_without_blocking_protection() {
    let (recorder, mut records) = RecordingSender::channel(32);
    let mut v = venue_with_recording(Some(recorder)).await;
    let mut a = v.main.take().unwrap();
    let mut b = v.backup.take().unwrap();
    a.send(Message::Text(json!({"e":"future_control_type","E":1}).to_string().into()))
        .await
        .unwrap();
    loop {
        let record =
            timeout(Duration::from_secs(1), records.recv()).await.unwrap().unwrap().unwrap();
        if record.kind == "unknown" {
            break;
        }
    }
    assert!(!v.health.take_reconcile_signal(), "unknown service types are not lost fills");
    let raw = json!({"e":"ORDER_TRADE_UPDATE","E":"invalid","o":{
        "s":"BTC-USD","i":100,"co":10,"X":"PARTIALLY_FILLED","x":"TRADE","T":2,"th":"fill-tx",
        "S":"BUY","l":"0.0003","L":"60000","z":"0.0003","rs":"0", "t":1,"m":false,"n":"0","N":"USD","rp":"0"}});
    assert!(serde_json::from_value::<ServerMessage>(raw.clone()).is_err());
    for ws in [&mut a, &mut b] {
        ws.send(Message::Text(raw.to_string().into())).await.unwrap();
        loop {
            let record =
                timeout(Duration::from_secs(1), records.recv()).await.unwrap().unwrap().unwrap();
            if record.kind == "unknown" {
                break;
            }
        }
    }
    assert!(v.executions.try_recv().is_err());
    a.send(Message::Text(
        json!({"e":"ORDER_TRADE_UPDATE","E":4,"o":{
        "s":"BTC-USD","i":100,"co":10,"X":"CANCELED","x":"CANCELED","T":4,"th":"entry-tx"}})
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    let cancel = timeout(Duration::from_secs(1), v.executions.recv()).await.unwrap().unwrap();
    assert!(cancel.trade.is_none());
    assert_eq!(cancel.lifecycle.order.status, bb_core::types::OrderStatus::Cancelled);
    assert_eq!(cancel.lifecycle.order.filled_quantity, Decimal::ZERO);
    assert!(cancel.stream_started_at.is_some());
    assert!(v.health.take_reconcile_signal(), "consume loss before applying cancel");
    assert!(!v.health.take_reconcile_signal(), "take and reset");
    let broker = v.broker.clone();
    let close = tokio::spawn(async move { broker.place_orders(&[order("77", true)]).await });
    let rpc = command(&mut a).await;
    reply(&mut a, &rpc, &[(77, 777)]).await;
    assert!(timeout(Duration::from_secs(1), close).await.unwrap().unwrap().is_ok());
}

#[tokio::test]
async fn sdk_overflow_counts_only_messages_and_preserves_reconnect_notifications() {
    let mut v = venue().await;
    let mut ws = v
        .client
        .connect_ws_managed()
        .config(ManagedWsConfig::builder().channel_capacity(1).build())
        .call()
        .await
        .unwrap();
    let mut server = timeout(Duration::from_secs(1), v.sockets.recv()).await.unwrap().unwrap();
    for n in 0..200 {
        server
            .send(Message::Text(
                json!({"e":"ORDER_TRADE_UPDATE","E":n+1,"o":{
            "s":"BTC-USD","i":100+n,"co":10+n,"X":"CANCELED","x":"CANCELED","T":n+1,"th":"tx"}})
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut seen = 0;
    let mut dropped = 0;
    while let Ok(Some(event)) = timeout(Duration::from_millis(30), ws.recv()).await {
        // Check after recv, before accepting a queued terminal status.
        dropped += ws.take_dropped_events();
        assert!(matches!(event, WsEvent::Message(_)));
        seen += 1;
    }
    assert!(dropped > 0);
    assert_eq!(seen + dropped, 200);
    assert_eq!(ws.take_dropped_events(), 0);

    for n in 0..2 {
        server
            .send(Message::Text(
                json!({"e":"status","E":n,"status":"connected","clientId":"full"})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
    }
    server.close(None).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!ws.is_connected(), "do not route commands to the reconnecting SDK");
    assert_eq!(ws.take_dropped_events(), 0, "control events do not count as data loss");
    for _ in 0..2 {
        assert!(matches!(ws.recv().await, Some(WsEvent::Message(_))));
        assert!(!ws.is_connected(), "old queued messages cannot revive the route");
    }
    assert!(matches!(ws.recv().await, Some(WsEvent::Reconnecting)));
    assert!(!ws.is_connected(), "consuming the boundary does not finish reconnect");
}

#[tokio::test]
async fn private_mux_consumes_sdk_loss_before_forwarding_queued_cancel() {
    let mut v = venue().await;
    let ws = v
        .client
        .connect_ws_managed()
        .config(ManagedWsConfig::builder().channel_capacity(1).build())
        .call()
        .await
        .unwrap();
    let mut server = timeout(Duration::from_secs(1), v.sockets.recv()).await.unwrap().unwrap();
    for n in 0..200 {
        server
            .send(Message::Text(
                json!({"e":"ORDER_TRADE_UPDATE","E":n+1,"o":{
            "s":"BTC-USD","i":100+n,"co":10+n,"X":"CANCELED","x":"CANCELED","T":n+1,"th":"tx"}})
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let health = Arc::new(ConnectionHealth::default());
    let (requests, request_rx) = mpsc::unbounded_channel();
    let (execution, mut events) = mpsc::unbounded_channel();
    v.tasks.push(tokio::spawn(muxer_loop(
        ws,
        FeedSenders {
            execution,
            trade: mpsc::unbounded_channel().0,
            lifecycle: mpsc::unbounded_channel().0,
            mark: mpsc::channel(1).0,
        },
        health.clone(),
        None,
        request_rx,
        v.client.clone(),
        vec![Topic::user_orders("test-account").to_string()],
    )));
    let event = timeout(Duration::from_secs(1), events.recv()).await.unwrap().unwrap();
    assert_eq!(event.lifecycle.order.status, bb_core::types::OrderStatus::Cancelled);
    assert!(health.take_reconcile_signal(), "loss is visible before applying the terminal event");
    let broker = Arc::new(BulletBroker::new(
        v.client.clone(),
        "test-account".into(),
        load_increments(&v.client).await.unwrap(),
        health,
        requests,
    ));
    let close = tokio::spawn(async move { broker.place_orders(&[order("77", true)]).await });
    let rpc = command(&mut server).await;
    reply(&mut server, &rpc, &[(77, 777)]).await;
    assert!(timeout(Duration::from_secs(1), close).await.unwrap().unwrap().is_ok());
}

#[tokio::test]
async fn maintenance_returns_without_polling_the_transport_and_claims_opportunity_once() {
    let v = venue().await;
    let health = Arc::new(ConnectionHealth::default());
    health.set_replacement_ready(true);
    let (requests, mut commands) = mpsc::unbounded_channel();
    let broker = BulletBroker::new(
        v.client.clone(),
        "test-account".into(),
        load_increments(&v.client).await.unwrap(),
        health,
        requests,
    );
    // Intentionally never poll a muxer: the broker must not await its reply.
    let mut refresh = Box::pin(broker.refresh_connection_if_idle());
    assert!(matches!(futures_util::poll!(&mut refresh), std::task::Poll::Ready(true)));
    assert!(matches!(commands.try_recv(), Ok(WsRequest::RefreshConnection)));
    assert!(!broker.refresh_connection_if_idle().await);
    assert!(commands.try_recv().is_err());
}

#[tokio::test]
async fn sdk_full_queue_keeps_multiple_boundaries_and_terminal_disconnect_in_order() {
    for terminal in [false, true] {
        let mut v = venue().await;
        let mut ws = v
            .client
            .connect_ws_managed()
            .config(
                ManagedWsConfig::builder()
                    .channel_capacity(1)
                    .initial_backoff(Duration::from_millis(10))
                    .max_retries(if terminal { 0 } else { 2 })
                    .build(),
            )
            .call()
            .await
            .unwrap();
        let mut server = v.sockets.recv().await.unwrap();
        assert!(ws.is_connected());
        // Exactly fill the bounded data channel (buffer + sender's own slot).
        for n in 0..2 {
            server
                .send(Message::Text(
                    json!({"e":"status","E":n,
                "status":"connected","clientId":"old"})
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
        }
        server.close(None).await.unwrap();
        if !terminal {
            // Complete TWO reconnects before the consumer sees either boundary.
            let mut next =
                timeout(Duration::from_secs(2), v.sockets.recv()).await.unwrap().unwrap();
            next.close(None).await.unwrap();
            server = timeout(Duration::from_secs(2), v.sockets.recv()).await.unwrap().unwrap();
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(ws.connected_epoch(), if terminal { None } else { Some(4) });
        assert_eq!(ws.received_epoch(), 0, "old messages still precede both boundaries");
        if !terminal {
            // A healthy replacement can send without draining the old queue.
            ws.ping(Some(RequestId::new(123))).unwrap();
            assert_eq!(raw_command(&mut server).await["method"], "PING");
        }
        for _ in 0..2 {
            assert!(matches!(ws.recv().await, Some(WsEvent::Message(_))));
            assert_eq!(ws.received_epoch(), 0);
            assert_eq!(ws.is_connected(), !terminal);
        }
        assert!(matches!(ws.recv().await, Some(WsEvent::Reconnecting)));
        assert_eq!(ws.received_epoch(), 2, "one boundary cannot acknowledge a later reconnect");
        if terminal {
            assert!(matches!(ws.recv().await, Some(WsEvent::Disconnected(_))));
            assert!(ws.recv().await.is_none());
            assert!(!ws.is_connected());
        } else {
            assert!(matches!(ws.recv().await, Some(WsEvent::Reconnecting)));
            assert_eq!(ws.received_epoch(), 4);
            assert_eq!(ws.connected_epoch(), Some(4));
        }
        assert_eq!(ws.take_dropped_events(), 0, "reconnects are not message loss");
        drop(server);
    }
}

// Poll the muxer explicitly to pause its consumer while the real SDK reader
// fills its queue. No production pause hooks or exchange connections.
#[tokio::test]
async fn private_data_loss_keeps_definite_rejections_and_reconnect_routes_protection() {
    for reconnect in [false, true] {
        let mut v = venue().await;
        let ws = v
            .client
            .connect_ws_managed()
            .config(
                ManagedWsConfig::builder()
                    .channel_capacity(1)
                    .initial_backoff(Duration::from_millis(400))
                    .build(),
            )
            .call()
            .await
            .unwrap();
        ws.subscribe_raw(vec![Topic::user_orders("test-account").to_string()], None).unwrap();
        let mut server = v.sockets.recv().await.unwrap();
        subscribe_ack(&mut server).await;
        let health = Arc::new(ConnectionHealth::default());
        let (requests, request_rx) = mpsc::unbounded_channel();
        let (execution, mut events) = mpsc::unbounded_channel();
        let mut mux = Box::pin(muxer_loop(
            ws,
            FeedSenders {
                execution,
                trade: mpsc::unbounded_channel().0,
                lifecycle: mpsc::unbounded_channel().0,
                mark: mpsc::channel(1).0,
            },
            health.clone(),
            None,
            request_rx,
            v.client.clone(),
            vec![Topic::user_orders("test-account").to_string()],
        ));
        let mut peer = tokio::select! {
            () = &mut mux => panic!("mux stopped"),
            peer = v.sockets.recv() => peer.unwrap(),
        };
        subscribe_ack(&mut peer).await;
        tokio::select! {
            () = &mut mux => panic!("mux stopped"),
            () = tokio::time::sleep(Duration::from_millis(10)) => {},
        }
        let broker = Arc::new(BulletBroker::new(
            v.client.clone(),
            "test-account".into(),
            load_increments(&v.client).await.unwrap(),
            health.clone(),
            requests,
        ));
        let entry_broker = broker.clone();
        let mut entry =
            tokio::spawn(async move { entry_broker.place_orders(&[order("42", false)]).await });
        let rpc = tokio::select! {
            () = &mut mux => panic!("mux stopped"),
            rpc = command(&mut server) => rpc,
        };
        // Consumer pauses, SDK reader continues; no reconnect in the first case.
        for n in 0..200 {
            server
                .send(Message::Text(
                    json!({"e":"ORDER_TRADE_UPDATE","E":n+1,"o":{
                "s":"BTC-USD","i":90000+n,"co":90000+n,"X":"CANCELED",
                "x":"CANCELED","T":n+1,"th":"tx"}})
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
        }
        if reconnect {
            server.close(None).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        let old_event = tokio::select! {
            () = &mut mux => panic!("mux stopped"),
            event = events.recv() => event.unwrap(),
        };
        assert!(health.take_reconcile_signal(), "real SDK queue loss detected");

        if reconnect {
            let close_broker = broker.clone();
            let mut close =
                tokio::spawn(async move { close_broker.place_orders(&[order("77", true)]).await });
            // The full queue still contains old messages before Reconnecting.
            // They must not cause a command to wait on that offline SDK route.
            let close_rpc = timeout(Duration::from_secs(2), async {
                tokio::select! {
                    () = &mut mux => panic!("mux stopped"),
                    rpc = command(&mut peer) => rpc,
                }
            })
            .await
            .expect("protective command must use the continuously connected peer");
            reply(&mut peer, &close_rpc, &[(77, 777)]).await;
            let result = tokio::select! {
                () = &mut mux => panic!("mux stopped"),
                result = &mut close => result.unwrap(),
            };
            assert!(result.unwrap()[0].success);
            assert!(!entry.is_finished(), "protective completion preserves the unresolved entry");
            let mut reconnected = tokio::select! {
                () = &mut mux => panic!("mux stopped"),
                socket = v.sockets.recv() => socket.unwrap(),
            };
            subscribe_ack(&mut reconnected).await;
            tokio::select! {
                () = &mut mux => panic!("mux stopped"),
                () = tokio::time::sleep(Duration::from_millis(10)) => {},
            }
            reconnected
                .send(Message::Text(
                    json!({"id":rpc["id"],"E":201,
                "error":{"code":-1022,"msg":"invalid signature"}})
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            let result = tokio::select! {
                () = &mut mux => panic!("mux stopped"),
                result = &mut entry => result.unwrap(),
            };
            assert!(result.unwrap_err().is_retryable(), "actual break keeps old RPC ambiguous");
            assert!(
                timeout(Duration::from_millis(30), command(&mut reconnected)).await.is_err(),
                "neither protective command nor ambiguous entry is re-sent"
            );
            reconnected
                .send(Message::Text(
                    json!({"e":"ORDER_TRADE_UPDATE","E":301,"o":{
                "s":"BTC-USD","i":888,"co":888,"X":"CANCELED","x":"CANCELED",
                "T":301,"th":"post-reconnect"}})
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            loop {
                let event = tokio::select! {
                    () = &mut mux => panic!("mux stopped"),
                    event = events.recv() => event.unwrap(),
                };
                if event.lifecycle.order.client_id.as_deref() == Some("888") {
                    assert!(
                        event.stream_started_at.unwrap() > old_event.stream_started_at.unwrap(),
                        "new subscription must not claim pre-reconnect coverage"
                    );
                    break;
                }
            }
        } else {
            server
                .send(Message::Text(
                    json!({"id":rpc["id"],"E":201,
                "error":{"code":-2010,"msg":"insufficient margin"}})
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            let result = tokio::select! {
                () = &mut mux => panic!("mux stopped"),
                result = &mut entry => result.unwrap(),
            };
            let result = result.unwrap();
            assert!(!result[0].success, "ordinary data loss must preserve a definite reject");
            assert!(result[0].error.as_deref().unwrap().contains("insufficient margin"));
            let close_broker = broker.clone();
            let mut close =
                tokio::spawn(async move { close_broker.place_orders(&[order("77", true)]).await });
            let rpc = tokio::select! {
                () = &mut mux => panic!("mux stopped"),
                rpc = command(&mut server) => rpc,
            };
            reply(&mut server, &rpc, &[(77, 777)]).await;
            let result = tokio::select! {
                () = &mut mux => panic!("mux stopped"),
                result = &mut close => result.unwrap(),
            };
            assert!(result.unwrap()[0].success);
        }
    }
}
