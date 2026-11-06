#![allow(clippy::unwrap_used, clippy::too_many_lines)]

use serde_json::json;

use super::*;

fn depth(u: u64, pu: u64, t: u64, snapshot: bool, bids: &[(&str, &str)]) -> DepthUpdate {
    serde_json::from_value(json!({"e":"depthUpdate", "s":"LIT-USD", "E":t+100,
        "T":t,"u":u,"U":u,"pu":pu,"mt":if snapshot { "s" } else { "u" },
        "b":bids,"a":if snapshot {vec![["4","10"]]}else{vec![]}}))
    .unwrap()
}

#[test]
fn reconstruct_before_merging_and_never_compare_local_update_ids() {
    let mut data = MarketData::default();
    let health = ConnectionHealth::default();
    let (bbo, mut rx) = mpsc::channel(16);
    let (book, mut depth_rx) = mpsc::channel(16);
    let at = Instant::now();
    data.depth(
        0,
        &depth(10, 0, 100, true, &[("3", "1"), ("2", "5")]),
        at,
        &bbo,
        &book,
        &health,
        None,
    )
    .unwrap();
    data.depth(
        1,
        &depth(120_000, 0, 100, true, &[("3", "1"), ("2", "5")]),
        at,
        &bbo,
        &book,
        &health,
        None,
    )
    .unwrap();
    assert_eq!(rx.try_recv().unwrap().orderbook.best_bid().unwrap().price, Decimal::from(3));
    assert!(rx.try_recv().is_err());
    assert!(depth_rx.try_recv().is_ok());
    assert!(depth_rx.try_recv().is_err(), "equal-T duplicate must not be published/recorded twice");
    data.depth(0, &depth(11, 10, 300, false, &[("3.5", "2")]), at, &bbo, &book, &health, None)
        .unwrap();
    rx.try_recv().unwrap();
    // Obsolete globally, but needed locally: remove 3 so a later deletion of
    // 3.5 must expose 2, not the obsolete 3 left in the other source's book.
    data.depth(
        1,
        &depth(120_001, 120_000, 200, false, &[("3", "0"), ("3.5", "2")]),
        at,
        &bbo,
        &book,
        &health,
        None,
    )
    .unwrap();
    assert!(rx.try_recv().is_err());
    data.depth(
        1,
        &depth(120_002, 120_001, 400, false, &[("3.5", "0")]),
        at,
        &bbo,
        &book,
        &health,
        None,
    )
    .unwrap();
    let event = rx.try_recv().unwrap();
    assert_eq!(event.orderbook.best_bid().unwrap().price, Decimal::from(2));
    assert_eq!(event.orderbook.best_ask().unwrap().price, Decimal::from(4));
    assert_eq!(data.current.as_ref().unwrap().0, 400);
}

#[test]
fn gap_and_reconnect_only_invalidate_entries_if_all_books_are_lost() {
    let mut data = MarketData::default();
    let health = ConnectionHealth::default();
    let (bbo, mut rx) = mpsc::channel(16);
    let (book, _rx) = mpsc::channel(16);
    let at = Instant::now();
    for slot in 0..3 {
        data.depth(
            slot,
            &depth(10 + slot as u64, 0, 100, true, &[("3", "1")]),
            at,
            &bbo,
            &book,
            &health,
            None,
        )
        .unwrap();
    }
    rx.try_recv().unwrap();
    let later = at + Duration::from_secs(1);
    assert_eq!(
        data.depth(
            0,
            &depth(12, 11, 200, false, &[("3.5", "1")]),
            later,
            &bbo,
            &book,
            &health,
            None
        ),
        Err("depth sequence gap")
    );
    data.reset(0, later, &health);
    assert!(health.last_reconnect_at().is_none());
    data.reset(1, later, &health);
    assert!(health.last_reconnect_at().is_none());
    data.reset(2, later, &health);
    assert_eq!(health.last_reconnect_at(), Some(later));
    assert!(!health.take_reconcile_signal());
    assert!(!health.is_disconnected());
    // Old snapshot doesn't advance the global quote; equal current state can
    // re-establish freshness after total public loss, even if prices unchanged.
    data.depth(0, &depth(50_000, 0, 99, true, &[("2", "1")]), later, &bbo, &book, &health, None)
        .unwrap();
    assert!(rx.try_recv().is_err());
    data.depth(0, &depth(50_001, 0, 100, true, &[("3", "1")]), later, &bbo, &book, &health, None)
        .unwrap();
    assert!(rx.try_recv().unwrap().received_at >= later);
    assert_eq!(data.current.as_ref().unwrap().0, 100);
}

#[test]
fn timestamp_conflicts_do_not_replace_quote_and_deep_updates_do_not_wake_bbo() {
    let mut data = MarketData::default();
    let health = ConnectionHealth::default();
    let (bbo, mut rx) = mpsc::channel(16);
    let (book, mut books) = mpsc::channel(16);
    let at = Instant::now();
    data.depth(0, &depth(1, 0, 100, true, &[("3", "1")]), at, &bbo, &book, &health, None).unwrap();
    rx.try_recv().unwrap();
    books.try_recv().unwrap();
    data.depth(1, &depth(300, 0, 100, true, &[("3.1", "1")]), at, &bbo, &book, &health, None)
        .unwrap();
    assert!(rx.try_recv().is_err());
    assert!(books.try_recv().is_err());
    data.depth(0, &depth(2, 1, 200, false, &[("2", "5")]), at, &bbo, &book, &health, None).unwrap();
    assert!(rx.try_recv().is_err());
    assert_eq!(books.try_recv().unwrap().orderbook.bids.len(), 2);
    assert_eq!(data.current.as_ref().unwrap().0, 200);
    // Its own top is unchanged, but differs from the accepted global top.
    // A higher-T update must compare against global state, not local old top.
    data.depth(1, &depth(301, 300, 300, false, &[("2", "7")]), at, &bbo, &book, &health, None)
        .unwrap();
    assert_eq!(rx.try_recv().unwrap().orderbook.best_bid().unwrap().price, Decimal::new(31, 1));
}

#[tokio::test]
async fn single_full_snapshot_stream_and_full_recorder_do_not_block_quotes() {
    let mut data = MarketData::default();
    let health = ConnectionHealth::default();
    let (bbo, mut rx) = mpsc::channel(16);
    let (book, _rx) = mpsc::channel(16);
    let (recorder, mut records) = RecordingSender::channel(1);
    let at = Instant::now();
    for slot in 0..3 {
        data.depth(
            slot,
            &depth(slot as u64 + 1, 0, 100, true, &[("3", "1")]),
            at,
            &bbo,
            &book,
            &health,
            Some(&recorder),
        )
        .unwrap();
    }
    rx.try_recv().unwrap();
    data.depth(
        1,
        &depth(3, 2, 200, false, &[("3", "0"), ("2", "8")]),
        at,
        &bbo,
        &book,
        &health,
        Some(&recorder),
    )
    .unwrap();
    assert_eq!(rx.try_recv().unwrap().orderbook.best_bid().unwrap().price, Decimal::from(2));
    let record = records.try_recv().unwrap().unwrap();
    let payload: serde_json::Value = serde_json::from_str(&record.payload).unwrap();
    assert_eq!(payload["mt"], "s");
    assert_eq!(payload["connection"], 0);
    assert!(records.try_recv().is_err());
    data.depth(
        1,
        &depth(4, 3, 300, false, &[("2", "9")]),
        at,
        &bbo,
        &book,
        &health,
        Some(&recorder),
    )
    .unwrap();
    let record = records.try_recv().unwrap().unwrap();
    assert_eq!(record.dropped_total, 1, "duplicates did not consume recorder capacity");
    let payload: serde_json::Value = serde_json::from_str(&record.payload).unwrap();
    assert_eq!(payload["b"], json!([["2", "9"]]));
    assert_eq!(payload["a"], json!([["4", "10"]]));
    assert_eq!(payload["connection"], 1);
    assert_eq!(payload["pu"], 0);
}

#[test]
fn rating_compares_matching_states_and_replacement_needs_new_measurements() {
    let mut data = MarketData::default();
    let at = Instant::now();
    data.ready_at = [Some(at); 3];
    for t in 1..=10 {
        data.arrivals.insert(
            t,
            [Some(at), Some(at + Duration::from_millis(2)), Some(at + Duration::from_millis(15))],
        );
    }
    data.arrivals.insert(11, [None, None, Some(at)]); // Not a zero-latency win.
    let delays = data.delays();
    assert_eq!(data.worst(&delays), Some(2));
    assert_eq!(delays[2].len(), 10);
    assert_eq!(delays[1][5], Duration::from_millis(2));
    assert_eq!(delays[2][5], Duration::from_millis(15));
    data.reset(2, at + ROTATION_INTERVAL, &ConnectionHealth::default());
    assert!(data.rotation_deadline().is_none());
    assert_eq!(data.delays()[2], [] as [Duration; 0]);
    data.ready_at[2] = Some(at);
    data.wins = [30, 20, 1];
    assert_eq!(data.worst(&data.delays()), Some(2));
}

#[test]
fn rotation_counts_once_from_last_initial_or_replacement_snapshot() {
    let mut data = MarketData::default();
    let health = ConnectionHealth::default();
    let (bbo, _quotes) = mpsc::channel(8);
    let (book, _books) = mpsc::channel(8);
    let start = Instant::now();
    for slot in 0..3 {
        assert!(data.rotation_deadline().is_none());
        data.depth(
            slot,
            &depth(1, 0, 100, true, &[("3", "1")]),
            start + Duration::from_millis(100 + slot as u64 * 10),
            &bbo,
            &book,
            &health,
            None,
        )
        .unwrap();
    }
    let deadline = start + Duration::from_millis(300_120);
    assert_eq!(data.rotation_deadline(), Some(deadline)); // Not the next 600s tick.
    data.depth(
        2,
        &depth(2, 1, 200, false, &[("3", "1")]),
        start + Duration::from_secs(299),
        &bbo,
        &book,
        &health,
        None,
    )
    .unwrap();
    assert_eq!(data.rotation_deadline(), Some(deadline), "ordinary data cannot extend the timer");
    data.reset(2, deadline, &health);
    assert!(data.rotation_deadline().is_none(), "do not rotate a second socket during recovery");
    let ready = deadline + Duration::from_millis(100);
    data.depth(2, &depth(500, 0, 200, true, &[("3", "1")]), ready, &bbo, &book, &health, None)
        .unwrap();
    assert_eq!(data.rotation_deadline(), Some(ready + ROTATION_INTERVAL));
}

#[test]
fn rating_replaces_previously_fast_peer_that_stopped_delivering() {
    let mut data = MarketData::default();
    let at = Instant::now();
    for t in 1..=290 {
        let first = at + Duration::from_secs(t);
        data.arrivals.insert(
            t,
            [
                (t <= 60).then_some(first),
                Some(first + Duration::from_millis(1)),
                Some(first + Duration::from_millis(2)),
            ],
        );
    }
    data.wins = [60, 230, 0];
    assert_eq!(data.worst(&data.delays()), Some(0));
    let recovered = at + Duration::from_secs(291);
    data.arrivals.insert(
        291,
        [
            Some(recovered),
            Some(recovered + Duration::from_millis(1)),
            Some(recovered + Duration::from_millis(2)),
        ],
    );
    assert_eq!(data.worst(&data.delays()), Some(2));
}

#[test]
fn rating_handles_disjoint_publishers_and_does_not_penalize_market_silence() {
    let mut data = MarketData::default();
    let old = Instant::now().checked_sub(ROTATION_INTERVAL).unwrap();
    // Different publishers need not share T. Missing *matching* timestamps do
    // not imply missing data: all three are still delivering at the same time.
    for slot in 0..3 {
        let mut arrivals = [None; 3];
        arrivals[slot] = Some(old + Duration::from_millis(slot as u64));
        data.arrivals.insert(slot as u64, arrivals);
    }
    data.wins = [30, 20, 1];
    assert_eq!(data.worst(&data.delays()), Some(2));
    data.arrivals.insert(3, [None, Some(old + Duration::from_millis(100)), None]);
    assert_eq!(data.worst(&data.delays()), Some(2), "an in-flight copy is not a failure");
    // Two streams then continue; the former fastest stream really goes silent.
    let later = old + Duration::from_secs(20);
    data.arrivals.insert(4, [None, Some(later), None]);
    data.arrivals.insert(5, [None, None, Some(later)]);
    assert_eq!(data.worst(&data.delays()), Some(0));
}

#[test]
fn median_selection_preserves_ranking_without_sorting_every_sample() {
    let mut data = MarketData::default();
    let at = Instant::now();
    let samples = [12, 3, 9, 0, 17, 1, 2, 6];
    for (t, &lag) in samples.iter().enumerate() {
        data.arrivals.insert(
            t as u64,
            [Some(at), Some(at + Duration::from_millis(lag)), Some(at + Duration::from_millis(5))],
        );
    }
    let delays = data.delays();
    assert_eq!(delays[1][4], Duration::from_millis(6)); // Upper middle, as before.
    assert_eq!(data.worst(&delays), Some(1));
}

#[tokio::test]
async fn bbo_retries_and_recovery_do_not_repeat_canonical_record_or_book() {
    let mut data = MarketData::default();
    let health = ConnectionHealth::default();
    let (bbo, mut quotes) = mpsc::channel(1);
    let (book, mut books) = mpsc::channel(8);
    let (recorder, mut records) = RecordingSender::channel(8);
    let at = Instant::now();
    data.depth(
        0,
        &depth(1, 0, 100, true, &[("3", "1")]),
        at,
        &bbo,
        &book,
        &health,
        Some(&recorder),
    )
    .unwrap();
    records.try_recv().unwrap();
    books.try_recv().unwrap(); // Leave the old quote in the full BBO channel.
    let later = at + Duration::from_millis(1);
    data.reset(0, later, &health);
    for slot in 0..3 {
        data.depth(
            slot,
            &depth(100 + slot as u64, 0, 200, true, &[("2", "1")]),
            later,
            &bbo,
            &book,
            &health,
            Some(&recorder),
        )
        .unwrap();
    }
    let record: serde_json::Value =
        serde_json::from_str(&records.try_recv().unwrap().unwrap().payload).unwrap();
    assert_eq!(record["T"], 200);
    assert_eq!(record["bbo_forwarded"], false);
    assert!(records.try_recv().is_err());
    assert_eq!(books.try_recv().unwrap().orderbook.best_bid().unwrap().price, Decimal::from(2));
    assert!(books.try_recv().is_err());
    quotes.try_recv().unwrap(); // Now delivery of the current quote can succeed.
    data.depth(0, &depth(101, 100, 200, false, &[]), later, &bbo, &book, &health, Some(&recorder))
        .unwrap();
    let quote = quotes.try_recv().unwrap();
    assert_eq!(quote.orderbook.best_bid().unwrap().price, Decimal::from(2));
    assert!(quote.received_at >= health.last_reconnect_at().unwrap());
    assert!(books.try_recv().is_err());
    assert!(records.try_recv().is_err(), "successful delivery retry is not a new book either");
    let recovered = later + Duration::from_secs(1);
    for slot in 0..3 {
        data.reset(slot, recovered, &health);
    }
    data.depth(
        2,
        &depth(999, 0, 200, true, &[("2", "1")]),
        recovered,
        &bbo,
        &book,
        &health,
        Some(&recorder),
    )
    .unwrap();
    assert_eq!(quotes.try_recv().unwrap().received_at, recovered);
    assert!(books.try_recv().is_err());
    assert!(records.try_recv().is_err(), "same-T recovery refreshes BBO without recording twice");
}

#[tokio::test]
async fn direct_snapshot_serialization_preserves_schema_prices_and_level_order() {
    let mut data = MarketData::default();
    let health = ConnectionHealth::default();
    let (bbo, _quotes) = mpsc::channel(8);
    let (book, _books) = mpsc::channel(8);
    let (recorder, mut records) = RecordingSender::channel(8);
    let mut snapshot = depth(999, 0, 100, true, &[("2.5000", "7.00"), ("3.01230", "1.2500")]);
    snapshot.asks.push(serde_json::from_value(json!(["4.1000", "2.300"])).unwrap());
    data.depth(2, &snapshot, Instant::now(), &bbo, &book, &health, Some(&recorder)).unwrap();
    let record = records.try_recv().unwrap().unwrap();
    let payload: serde_json::Value = serde_json::from_str(&record.payload).unwrap();
    assert_eq!(
        payload,
        json!({
            "e":"depthUpdate", "s":"LIT-USD", "E":200, "T":100,
            "U":999, "u":999, "pu":0, "mt":"s",
            "b":[["3.01230","1.2500"],["2.5000","7.00"]],
            "a":[["4","10"],["4.1000","2.300"]],
            "connection":2,"generation":0,
            "bbo_changed":true,"bbo_forwarded":true,"book_forwarded":true,
        })
    );
    let decoded: DepthUpdate = serde_json::from_str(&record.payload).unwrap();
    assert_eq!(decoded.bids.len(), 2);
    assert_eq!(decoded.asks.len(), 2);
}

#[tokio::test]
async fn book_only_consumer_dedups_and_full_bbo_channel_retries_latest_quote() {
    let mut data = MarketData::default();
    let health = ConnectionHealth::default();
    let (bbo, rx) = mpsc::channel(1);
    drop(rx);
    let (book, mut books) = mpsc::channel(8);
    let (recorder, mut records) = RecordingSender::channel(8);
    let at = Instant::now();
    for slot in 0..3 {
        data.depth(
            slot,
            &depth(1, 0, 100, true, &[("3", "1")]),
            at,
            &bbo,
            &book,
            &health,
            Some(&recorder),
        )
        .unwrap();
    }
    assert!(books.try_recv().is_ok());
    assert!(books.try_recv().is_err());
    assert!(records.try_recv().is_ok());
    assert!(records.try_recv().is_err());

    let mut data = MarketData::default();
    let (bbo, mut quotes) = mpsc::channel(1);
    data.depth(0, &depth(1, 0, 100, true, &[("3", "1")]), at, &bbo, &book, &health, None).unwrap();
    data.depth(
        0,
        &depth(2, 1, 200, false, &[("3", "0"), ("2", "1")]),
        at,
        &bbo,
        &book,
        &health,
        None,
    )
    .unwrap();
    assert_eq!(quotes.try_recv().unwrap().orderbook.best_bid().unwrap().price, Decimal::from(3));
    // Another copy of T=200 must retry the pending quote, not disappear as a
    // duplicate after the canonical book has already advanced.
    data.depth(1, &depth(500, 0, 200, true, &[("2", "1")]), at, &bbo, &book, &health, None)
        .unwrap();
    assert_eq!(quotes.try_recv().unwrap().orderbook.best_bid().unwrap().price, Decimal::from(2));
}

#[test]
fn unsynchronised_delta_and_bad_numbers_cannot_create_a_book() {
    let mut data = MarketData::default();
    let health = ConnectionHealth::default();
    let (bbo, mut rx) = mpsc::channel(8);
    let (book, _rx) = mpsc::channel(8);
    let at = Instant::now();
    data.depth(0, &depth(3, 2, 100, false, &[("3", "1")]), at, &bbo, &book, &health, None).unwrap();
    assert!(rx.try_recv().is_err());
    assert!(
        data.depth(0, &depth(3, 0, 100, true, &[("NaN", "1")]), at, &bbo, &book, &health, None)
            .is_err()
    );
    data.reset(0, at, &health);
    assert!(data.books[0].is_none());
    assert!(data.current.is_none());
}

#[test]
fn pending_bbo_is_discarded_on_total_loss_and_recovery_keeps_first_snapshot_time() {
    let mut data = MarketData::default();
    let health = ConnectionHealth::default();
    let (bbo, mut quotes) = mpsc::channel(1);
    let (books, _books) = mpsc::channel(8);
    let at = Instant::now();
    data.depth(0, &depth(1, 0, 100, true, &[("3", "1")]), at, &bbo, &books, &health, None).unwrap();
    data.depth(
        1,
        &depth(500, 0, 200, true, &[("2", "1")]),
        at + Duration::from_millis(1),
        &bbo,
        &books,
        &health,
        None,
    )
    .unwrap();
    let first = data.pending_bbo.as_ref().unwrap().received_at;
    data.reset(0, at + Duration::from_millis(2), &health);
    assert_eq!(
        data.pending_bbo.as_ref().unwrap().received_at,
        first,
        "a surviving book retains pending BBO"
    );
    data.reset(1, at + Duration::from_millis(3), &health);
    assert!(data.pending_bbo.is_none(), "old BBO cannot become a fresh post-reconnect snapshot");
    quotes.try_recv().unwrap();
    let recovered = at + Duration::from_millis(4);
    data.depth(2, &depth(999, 0, 199, true, &[("1", "1")]), recovered, &bbo, &books, &health, None)
        .unwrap();
    assert!(quotes.try_recv().is_err(), "older snapshot must not roll back T");
    data.depth(
        2,
        &depth(1000, 0, 200, true, &[("2", "1")]),
        recovered,
        &bbo,
        &books,
        &health,
        None,
    )
    .unwrap();
    assert_eq!(quotes.try_recv().unwrap().received_at, recovered);
    assert!(data.pending_bbo.is_none());
}
