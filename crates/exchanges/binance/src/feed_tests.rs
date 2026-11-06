#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

use futures_util::SinkExt;
use serde_json::{Value, json};
use tokio::time::timeout;

use super::*;

fn frame(u: u64, bid: &str) -> String {
    json!({"e":"bookTicker", "E":999, "T":998, "u":u, "s":"BTCUSDT",
        "b":bid, "B":"9", "a":"102", "A":"1", "extra":"retained"})
    .to_string()
}

fn parse_ticker(text: &str, at: Instant) -> Result<ReferencePriceUpdate, String> {
    serde_json::from_str::<BookTicker<'_>>(text).map_err(|e| e.to_string())?.reference(at)
}

#[test]
fn merge_orders_by_u_not_timestamp_and_reconnect_cannot_refresh_replays() {
    let mut data = MarketData::default();
    let (tx, mut rx) = mpsc::channel(16);
    let (recording, mut records) = RecordingSender::channel(16);
    let at = Instant::now();
    for slot in 0..3 {
        data.connected(slot, at);
    }
    for (slot, id, bid) in [
        (0, 100, "100"),
        (1, 100, "100"),
        (2, 99, "99"),
        (2, 105, "101"),
        (0, 104, "100"),
        (1, 106, "100"),
    ] {
        data.ticker(slot, &frame(id, bid), at, "btcusdt", &tx, Some(&recording)).unwrap();
    }
    let events: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
    assert_eq!(events.len(), 3, "different u with equal T must all be accepted");
    assert_eq!(
        events.iter().map(|e| e.midpoint).collect::<Vec<_>>(),
        [Decimal::from(101), Decimal::new(1015, 1), Decimal::from(101)]
    );
    let saved: Vec<_> = std::iter::from_fn(|| records.try_recv().ok().flatten()).collect();
    assert_eq!(
        saved
            .iter()
            .map(|r| serde_json::from_str::<Value>(&r.payload).unwrap()["u"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        [100, 105, 106]
    );
    assert_eq!(saved[0].payload, frame(100, "100"), "keep the winning raw payload intact");
    for slot in 0..3 {
        data.reset(slot);
    }
    let later = at + Duration::from_secs(2);
    data.connected(0, later);
    data.ticker(0, &frame(105, "101"), later, "btcusdt", &tx, Some(&recording)).unwrap();
    data.ticker(0, &frame(106, "100"), later, "btcusdt", &tx, Some(&recording)).unwrap();
    assert!(rx.try_recv().is_err(), "old copies cannot refresh the strategy's freshness/impulse");
    assert!(records.try_recv().is_err());
    data.ticker(0, &frame(110, "101"), later, "btcusdt", &tx, Some(&recording)).unwrap();
    assert_eq!(rx.try_recv().unwrap().received_at, later);
}

#[test]
fn malformed_first_copy_and_wrong_symbol_cannot_advance_watermark() {
    let mut data = MarketData::default();
    let (tx, mut rx) = mpsc::channel(16);
    let at = Instant::now();
    let mut invalid: Value = serde_json::from_str(&frame(20, "100")).unwrap();
    invalid["B"] = "0".into();
    assert!(data.ticker(0, &invalid.to_string(), at, "BTCUSDT", &tx, None).is_err());
    assert!(data.last_id.is_none());
    let mut wrong = invalid.clone();
    wrong["s"] = "ETHUSDT".into();
    wrong["u"] = 9999.into();
    assert!(data.ticker(1, &wrong.to_string(), at, "BTCUSDT", &tx, None).is_err());
    data.ticker(2, &frame(20, "100"), at, "BTCUSDT", &tx, None).unwrap();
    assert_eq!(rx.try_recv().unwrap().mid, Decimal::new(1018, 1));
    assert_eq!(data.last_id, Some(20));
}

#[test]
fn full_channels_do_not_delay_quotes_or_renew_retry_timestamps() {
    let mut data = MarketData::default();
    let (tx, mut rx) = mpsc::channel(1);
    let (recording, mut records) = RecordingSender::channel(1);
    let at = Instant::now();
    data.ticker(0, &frame(1, "100"), at, "BTCUSDT", &tx, Some(&recording)).unwrap();
    let original = at + Duration::from_millis(1);
    data.ticker(1, &frame(2, "101"), original, "BTCUSDT", &tx, Some(&recording)).unwrap();
    assert!(data.pending.is_some());
    rx.try_recv().unwrap();
    data.ticker(2, &frame(2, "101"), at + Duration::from_secs(2), "BTCUSDT", &tx, Some(&recording))
        .unwrap();
    assert_eq!(rx.try_recv().unwrap().received_at, original);
    assert!(data.pending.is_none());
    let record = records.try_recv().unwrap().unwrap();
    assert_eq!(record.payload, frame(1, "100"));
    recording.record("test", "probe", || Ok("{}".into()));
    assert_eq!(
        records.try_recv().unwrap().unwrap().dropped_total,
        1,
        "one canonical overflow; duplicate was not recorded"
    );

    data.ticker(0, &frame(3, "100"), at, "BTCUSDT", &tx, None).unwrap();
    data.ticker(1, &frame(4, "101"), original, "BTCUSDT", &tx, None).unwrap();
    data.ticker(2, &frame(5, "100"), original, "BTCUSDT", &tx, None).unwrap();
    rx.try_recv().unwrap();
    data.ticker(0, &frame(4, "101"), original, "BTCUSDT", &tx, None).unwrap();
    assert!(rx.try_recv().is_err(), "an older copy cannot roll back a pending newer quote");
    data.ticker(0, &frame(5, "100"), original, "BTCUSDT", &tx, None).unwrap();
    assert_eq!(rx.try_recv().unwrap().midpoint, Decimal::from(101));
}

#[test]
fn rating_warmup_matching_ids_silent_peer_and_rotation_deadline() {
    let mut data = MarketData::default();
    let (tx, _rx) = mpsc::channel(1);
    let at = Instant::now();
    assert!(data.connected(0, at).is_none());
    assert!(data.connected(1, at + Duration::from_secs(1)).is_none());
    let ready = at + Duration::from_secs(2);
    let deadline = ready + ROTATION_INTERVAL;
    assert_eq!(data.connected(2, ready), Some(deadline));
    data.ticker(2, &frame(1, "100"), ready, "BTCUSDT", &tx, None).unwrap();
    assert!(data.arrivals.is_empty(), "warmup still forwards data, but does not affect ranking");
    for u in 2..7 {
        let first = ready + WARMUP + Duration::from_millis(u * 100);
        for (slot, delay) in [(0, 0), (1, 5), (2, 30)] {
            data.ticker(
                slot,
                &frame(u, "100"),
                first + Duration::from_millis(delay),
                "BTCUSDT",
                &tx,
                None,
            )
            .unwrap();
        }
    }
    let delays = data.delays();
    assert_eq!(delays.iter().map(Vec::len).collect::<Vec<_>>(), [5, 5, 5]);
    assert_eq!(delays[2][2], Duration::from_millis(30));
    assert_eq!(data.worst(&delays), 2);
    assert_eq!(data.rotation_deadline(), Some(deadline), "messages never postpone rotation");
    for arrivals in data.arrivals.values_mut() {
        arrivals[2] = None;
    }
    assert_eq!(
        data.worst(&data.delays()),
        2,
        "silent peer loses a zero-win tie to one delivering copies"
    );
    data.reset(2);
    assert!(data.rotation_deadline().is_none(), "do not rotate a second slot during reconnect");
    assert_eq!(data.last_id, Some(6));
    let reconnect = deadline + Duration::from_secs(1);
    assert_eq!(data.connected(2, reconnect), Some(reconnect + ROTATION_INTERVAL));
    assert_eq!(data.wins, [0; 3]);
    assert!(data.arrivals.is_empty());
    assert_eq!(data.generation, [0, 0, 1]);
}

#[test]
fn rating_memory_is_bounded() {
    let mut data = MarketData::default();
    let (tx, _rx) = mpsc::channel(1);
    let at = Instant::now();
    data.connected(0, at);
    for u in 0..HISTORY_CAPACITY as u64 + 10 {
        data.ticker(0, &frame(u, "100"), at + WARMUP, "BTCUSDT", &tx, None).unwrap();
    }
    assert_eq!(data.arrivals.len(), HISTORY_CAPACITY);
    assert_eq!(*data.arrivals.first_key_value().unwrap().0, 10);
}

#[test]
fn rating_replaces_previously_fast_peer_that_stopped_delivering() {
    let mut data = MarketData::default();
    let at = Instant::now();
    // All three deliver for a minute; the former winner then stops for almost
    // four minutes. Its old zero-lag samples must not protect it from rotation.
    for u in 1..=290 {
        let first = at + Duration::from_secs(u);
        data.arrivals.insert(
            u,
            [
                (u <= 60).then_some(first),
                Some(first + Duration::from_millis(1)),
                Some(first + Duration::from_millis(2)),
            ],
        );
    }
    data.wins = [60, 230, 0];
    assert_eq!(data.worst(&data.delays()), 0);

    // Once the channel resumes, recent evidence clears the silence penalty;
    // historical latency again chooses the slower live peer.
    let recovered = at + Duration::from_secs(291);
    data.arrivals.insert(
        291,
        [
            Some(recovered),
            Some(recovered + Duration::from_millis(1)),
            Some(recovered + Duration::from_millis(2)),
        ],
    );
    assert_eq!(data.worst(&data.delays()), 2);
}

#[test]
fn rating_does_not_confuse_market_silence_or_inflight_copy_with_failure() {
    let mut data = MarketData::default();
    let old = Instant::now().checked_sub(ROTATION_INTERVAL).unwrap();
    for u in 1..=10 {
        let first = old + Duration::from_millis(u * 100);
        data.arrivals.insert(
            u,
            [
                Some(first),
                Some(first + Duration::from_millis(2)),
                Some(first + Duration::from_millis(15)),
            ],
        );
    }
    // All streams stopped together with the market: use measured lag, not age.
    assert_eq!(data.worst(&data.delays()), 2);
    data.arrivals.insert(11, [None, Some(old + Duration::from_millis(1100)), None]);
    assert_eq!(data.worst(&data.delays()), 2, "one in-flight copy is not a stalled peer");
}

#[tokio::test]
async fn pending_latest_quote_is_delivered_when_capacity_returns_without_more_frames() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (tx, mut events) = mpsc::channel(1);
    let task = tokio::spawn(run_ws_loop(
        format!("ws://{}", listener.local_addr().unwrap()),
        "btcusdt".into(),
        tx,
        None,
    ));
    let mut sockets = Vec::new();
    for _ in 0..3 {
        let (socket, _) =
            timeout(Duration::from_secs(1), listener.accept()).await.unwrap().unwrap();
        sockets.push(tokio_tungstenite::accept_async(socket).await.unwrap());
    }
    // Each pong proves the receive loop consumed the preceding text. Keep the
    // queue full throughout: u=3 must replace pending u=2 without blocking reads.
    let first_received_after = Instant::now();
    for (u, bid) in [(1, "100"), (2, "101"), (3, "98")] {
        for socket in &mut sockets {
            socket.send(Message::Text(frame(u, bid).into())).await.unwrap();
            socket.send(Message::Ping(vec![1].into())).await.unwrap();
            assert_eq!(
                timeout(Duration::from_secs(1), socket.next()).await.unwrap().unwrap().unwrap(),
                Message::Pong(vec![1].into())
            );
        }
    }
    let received_before = Instant::now();
    assert_eq!(events.recv().await.unwrap().midpoint, Decimal::from(101));
    let latest = timeout(Duration::from_secs(1), events.recv()).await.unwrap().unwrap();
    assert_eq!(latest.midpoint, Decimal::from(100), "deliver u=3, not superseded u=2");
    assert!(
        latest.received_at >= first_received_after && latest.received_at <= received_before,
        "sending after backpressure must retain the original receive instant"
    );
    assert!(timeout(Duration::from_millis(30), events.recv()).await.is_err(), "no duplicates");
    drop(events);
    timeout(Duration::from_secs(1), task).await.unwrap().unwrap();
}

#[tokio::test]
async fn three_sockets_merge_record_reconnect_and_answer_server_ping() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let (tx, mut events) = mpsc::channel(16);
    let (recording, mut records) = RecordingSender::channel(32);
    let task = tokio::spawn(run_ws_loop(url, "btcusdt".into(), tx, Some(recording)));
    let mut sockets = Vec::new();
    for _ in 0..3 {
        let (socket, _) =
            timeout(Duration::from_secs(1), listener.accept()).await.unwrap().unwrap();
        sockets.push(tokio_tungstenite::accept_async(socket).await.unwrap());
    }
    for socket in &mut sockets {
        socket.send(Message::Ping(vec![1, 2, 3].into())).await.unwrap();
        let msg = timeout(Duration::from_secs(1), socket.next()).await.unwrap().unwrap().unwrap();
        assert_eq!(msg, Message::Pong(vec![1, 2, 3].into()));
    }
    sockets[0].send(Message::Text(frame(10, "100").into())).await.unwrap();
    assert_eq!(events.recv().await.unwrap().midpoint, Decimal::from(101));
    sockets[1].send(Message::Text(frame(10, "100").into())).await.unwrap();
    sockets[2].send(Message::Text(frame(9, "99").into())).await.unwrap();
    sockets[1].send(Message::Text(frame(11, "101").into())).await.unwrap();
    assert_eq!(events.recv().await.unwrap().midpoint, Decimal::new(1015, 1));
    sockets[0].close(None).await.unwrap();
    sockets[2].send(Message::Text(frame(12, "100").into())).await.unwrap();
    assert_eq!(
        timeout(Duration::from_millis(500), events.recv()).await.unwrap().unwrap().midpoint,
        Decimal::from(101)
    );
    let (socket, _) = timeout(Duration::from_secs(3), listener.accept()).await.unwrap().unwrap();
    let mut replacement = tokio_tungstenite::accept_async(socket).await.unwrap();
    replacement.send(Message::Text(frame(11, "101").into())).await.unwrap();
    replacement.send(Message::Text(frame(13, "101").into())).await.unwrap();
    assert_eq!(
        timeout(Duration::from_secs(1), events.recv()).await.unwrap().unwrap().midpoint,
        Decimal::new(1015, 1)
    );
    assert!(timeout(Duration::from_millis(30), events.recv()).await.is_err());
    assert!(
        timeout(Duration::from_millis(30), listener.accept()).await.is_err(),
        "never a fourth live connection"
    );
    let mut ids = Vec::new();
    while let Ok(Some(record)) = records.try_recv() {
        if record.kind == "bookTicker" {
            assert!(record.recv_unix_ns > 0);
            ids.push(
                serde_json::from_str::<Value>(&record.payload).unwrap()["u"].as_u64().unwrap(),
            );
        }
    }
    assert_eq!(ids, [10, 11, 12, 13]);
    drop(events);
    timeout(Duration::from_secs(1), task).await.unwrap().unwrap();
}

#[tokio::test]
async fn shutdown_cancels_unfinished_handshakes() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (tx, events) = mpsc::channel(1);
    let task = tokio::spawn(run_ws_loop(
        format!("ws://{}", listener.local_addr().unwrap()),
        "btcusdt".into(),
        tx,
        None,
    ));
    let (_socket, _) = timeout(Duration::from_secs(1), listener.accept()).await.unwrap().unwrap();
    // Keep the TCP peer open without completing the WS handshake.
    drop(events);
    timeout(Duration::from_millis(500), task).await.unwrap().unwrap();
}

#[test]
fn microprice_balanced_equals_mid() {
    let frame = r#"{"u":1,"s":"BTCUSDT","b":"100","B":"5","a":"102","A":"5"}"#;
    let received_at = Instant::now().checked_sub(Duration::from_millis(100)).unwrap();
    let event = parse_ticker(frame, received_at).expect("parse");
    assert_eq!(event.mid, Decimal::from(101));
    assert_eq!(event.received_at, received_at);
}

#[test]
fn microprice_imbalanced_tilts_toward_thin_side() {
    let event = parse_ticker(&frame(1, "100"), Instant::now()).expect("parse");
    assert_eq!(event.mid, Decimal::from_str("101.8").unwrap());
}

#[test]
fn rejects_empty_sizes() {
    let frame = r#"{"u":1,"s":"BTCUSDT","b":"100","B":"0","a":"102","A":"0"}"#;
    assert!(parse_ticker(frame, Instant::now()).is_err());
}

#[test]
fn rejects_non_json() {
    assert!(parse_ticker("not json", Instant::now()).is_err());
}

#[test]
fn rejects_missing_fields() {
    assert!(parse_ticker(r#"{"s":"X"}"#, Instant::now()).is_err());
}

#[test]
fn rejects_non_decimal_price() {
    let frame = r#"{"u":1,"s":"BTCUSDT","b":"notanumber","B":"1","a":"1","A":"1"}"#;
    assert!(parse_ticker(frame, Instant::now()).is_err());
}
