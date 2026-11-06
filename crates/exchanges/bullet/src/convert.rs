//! Shared value-conversion helpers used by the Bullet broker + connection.

use std::collections::BTreeMap;
use std::str::FromStr;

use bb_core::events::{MarkPriceUpdate, OrderLifecycle, Trade};
use bb_core::helpers::parse_decimal_or_warn;
use bb_core::types::{Order, OrderStatus, OrderType, Side};
use bullet_rust_sdk::types::{MarkPriceMessage, OrderUpdateData, OrderUpdateMessage, PriceLevel};
use rust_decimal::Decimal;

const EXCHANGE: &str = "bullet";

/// Parse a `[price, qty]` price level into typed decimals. Returns `None` if
/// either string fails to parse.
pub fn parse_level_tuple(level: &PriceLevel) -> Option<(Decimal, Decimal)> {
    let price = Decimal::from_str(&level.0).ok()?;
    let qty = Decimal::from_str(&level.1).ok()?;
    Some((price, qty))
}

/// Parse a REST orderbook (`Vec<Vec<String>>`) into a `BTreeMap<Decimal, Decimal>`.
pub fn parse_orderbook_levels(raw: &[Vec<String>]) -> BTreeMap<Decimal, Decimal> {
    raw.iter()
        .filter_map(|lvl| {
            let price = Decimal::from_str(lvl.first()?).ok()?;
            let qty = Decimal::from_str(lvl.get(1)?).ok()?;
            Some((price, qty))
        })
        .collect()
}

/// Returns `None` if `mark_price` fails to parse (skip the update entirely).
/// A bad mark price would silently corrupt `PnL` accounting downstream.
pub fn mark_price_to_event(mp: &MarkPriceMessage) -> Option<MarkPriceUpdate> {
    let mark_price = parse_decimal_or_warn(&mp.mark_price, "mark_price")?;
    // funding_rate: None means parse failed / field absent — not the same as zero funding
    let funding_rate = parse_decimal_or_warn(&mp.funding_rate, "funding_rate");
    Some(MarkPriceUpdate {
        exchange: EXCHANGE.into(),
        symbol: mp.symbol.clone(),
        mark_price,
        funding_rate,
    })
}

/// Extract a `Trade` from `OrderUpdateData::TradeFill`. Returns `None` for
/// `PlaceOrder` / `Cancel` variants (no execution) or unknown sides (logged).
pub fn order_update_to_trade(msg: &OrderUpdateMessage) -> Option<Trade> {
    let OrderUpdateData::TradeFill(data) = &msg.order else { return None };
    let side = match data.side.as_str() {
        "BUY" => Side::Buy,
        "SELL" => Side::Sell,
        other => {
            tracing::warn!(side = other, "Bullet: unknown side in TradeFill — skipping trade");
            return None;
        }
    };
    let price = parse_decimal_or_warn(&data.last_filled_price, "last_filled_price")?;
    let quantity = parse_decimal_or_warn(&data.last_filled_qty, "last_filled_qty")?;
    Some(Trade {
        exchange: EXCHANGE.into(),
        symbol: data.common.symbol.clone(),
        order_id: data.common.order_id.to_string(),
        trade_id: Some(data.trade_id.to_string()),
        client_id: data.common.client_order_id.as_ref().map(ToString::to_string),
        side,
        price,
        quantity,
        fee: parse_decimal_or_warn(&data.commission, "commission")?,
        realized_pnl: Some(parse_decimal_or_warn(&data.realized_pnl, "realized_pnl")?),
        // Bullet stamps user-data frames in microseconds; Trade.timestamp is Unix ms.
        timestamp: Some(data.common.transaction_time / 1000),
    })
}

/// Extract an `OrderLifecycle` update from any `OrderUpdate` variant.
///
/// `price` / `quantity` on the resulting `Order` always carry the *original*
/// order's limit price and size — never per-fill execution values.
///
/// Note: Cancel events do not carry the original side in the current SDK.
/// We default to `Side::Buy` for Cancel; strategies should not rely on the
/// side field of a Cancel lifecycle event.
pub fn order_update_to_lifecycle(msg: &OrderUpdateMessage) -> OrderLifecycle {
    let (
        symbol,
        order_id,
        client_id,
        status_str,
        side_str,
        price,
        quantity,
        filled_quantity,
        order_type,
    ) = match &msg.order {
        OrderUpdateData::TradeFill(data) => (
            data.common.symbol.clone(),
            data.common.order_id.to_string(),
            data.common.client_order_id.as_ref().map(ToString::to_string),
            data.common.status.clone(),
            data.side.clone(),
            data.price.as_deref().and_then(|s| s.parse().ok()).unwrap_or(Decimal::ZERO),
            data.quantity.as_deref().and_then(|s| s.parse().ok()).unwrap_or(Decimal::ZERO),
            data.cumulative_filled_size.as_deref().and_then(|s| s.parse().ok()).unwrap_or_default(),
            OrderType::Limit, // TradeFill carries no order_type field
        ),
        OrderUpdateData::PlaceOrder(data) => (
            data.common.symbol.clone(),
            data.common.order_id.to_string(),
            data.common.client_order_id.as_ref().map(ToString::to_string),
            data.common.status.clone(),
            data.side.clone(),
            data.price.parse().unwrap_or_default(),
            data.quantity.parse().unwrap_or_default(),
            Decimal::ZERO,
            // Case-insensitive: the WS stream uses lowercase (`post_only`),
            // unlike the REST `BinanceOrder` form. Market/IoC orders aren't
            // distinguished on lifecycle (matching `get_open_orders`); the
            // maker/taker fee signal lives on `is_maker` of the fill instead.
            match data.order_type.to_ascii_uppercase().as_str() {
                "POST_ONLY" => OrderType::PostOnly,
                _ => OrderType::Limit,
            },
        ),
        OrderUpdateData::Cancel(data) => (
            data.common.symbol.clone(),
            data.common.order_id.to_string(),
            data.common.client_order_id.as_ref().map(ToString::to_string),
            data.common.status.clone(),
            String::new(), // SDK Cancel does not surface the original side
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
            OrderType::Limit, // Cancel carries no order_type field
        ),
    };
    let status = match status_str.as_str() {
        // Bullet ends a partial IOC with TRADE/PARTIALLY_FILLED + rs=0,
        // without a separate cancel. Only the remainder is cancelled: retain
        // the actual cumulative fill size and never credit the requested size.
        "PARTIALLY_FILLED"
            if matches!(&msg.order, OrderUpdateData::TradeFill(data)
                if data.remaining_size.as_deref().and_then(|s| s.parse::<Decimal>().ok())
                    == Some(Decimal::ZERO)) =>
        {
            OrderStatus::Cancelled
        }
        "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
        "FILLED" => OrderStatus::Filled,
        "CANCELED" | "CANCELLED" => OrderStatus::Cancelled,
        "EXPIRED" | "REJECTED" => OrderStatus::Rejected,
        _ => OrderStatus::Open,
    };
    let side = match side_str.as_str() {
        "SELL" => Side::Sell,
        "BUY" | "" => Side::Buy, // "" = Cancel events — no side in SDK response
        other => {
            tracing::warn!(
                side = other,
                "Bullet: unknown side in OrderLifecycle — defaulting to Buy"
            );
            Side::Buy
        }
    };
    // TODO(P1-15): Cancel / TradeFill updates carry no order_type field, so
    // those variants still default to Limit (which affects fee accounting).
    OrderLifecycle {
        exchange: EXCHANGE.into(),
        order: Order {
            id: order_id,
            client_id,
            symbol,
            side,
            order_type,
            price,
            quantity,
            filled_quantity,
            status,
        },
    }
}

/// Extract the `(client_order_id, order_id)` pair from a single submit-tx
/// `LedgerEvent` value. Bullet echoes each accepted order as a `place_order`
/// event carrying both the venue-assigned `order_id` and the
/// `client_order_id` we sent, so the broker can surface the oid synchronously
/// instead of waiting for the WS user-data stream. Returns `None` for any
/// other event type (or a malformed `place_order` payload).
pub fn place_order_ids(
    value: &serde_json::Map<String, serde_json::Value>,
) -> Option<(u64, String)> {
    let po = value.get("place_order")?.as_object()?;
    let order_id = po.get("order_id")?.as_u64()?;
    let client_order_id = po.get("client_order_id")?.as_u64()?;
    Some((client_order_id, order_id.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partial_fill_is_terminal_only_with_explicit_zero_remaining_size() {
        // Shape of the last HYPE fill in the recorded 1.96 -> 1.90 IOC:
        // original price/quantity are absent, and X remains PARTIALLY_FILLED.
        for (status, remaining, expected) in [
            ("PARTIALLY_FILLED", Some("0.48"), OrderStatus::PartiallyFilled),
            ("PARTIALLY_FILLED", Some("0.20"), OrderStatus::PartiallyFilled),
            ("PARTIALLY_FILLED", Some("0"), OrderStatus::Cancelled),
            ("PARTIALLY_FILLED", Some("0.000"), OrderStatus::Cancelled),
            ("PARTIALLY_FILLED", None, OrderStatus::PartiallyFilled),
            ("PARTIALLY_FILLED", Some("invalid"), OrderStatus::PartiallyFilled),
            ("FILLED", None, OrderStatus::Filled),
            ("FILLED", Some("0"), OrderStatus::Filled),
        ] {
            let mut raw = serde_json::json!({"e":"ORDER_TRADE_UPDATE","E":1_792_908_600_753_114_u64,"o":{
                "s":"HYPE-USD","i":5_376_913_729_u64,"co":17_928_924_140_617_u64,
                "X":status,"x":"TRADE","T":1_792_908_600_745_618_u64,"th":"test-tx",
                "S":"BUY","l":"0.14","L":"76.260","n":"0.001","N":"USDC",
                "m":false,"t":22_330_149,"rp":"0.015","ft":"o","z":"1.90","Z":"144.88780"}});
            if let Some(remaining) = remaining {
                raw["o"]["rs"] = remaining.into();
            }
            let msg: OrderUpdateMessage = serde_json::from_value(raw).unwrap();
            let order = order_update_to_lifecycle(&msg).order;
            assert_eq!(order.status, expected, "status={status}, rs={remaining:?}");
            assert_eq!(order.quantity, Decimal::ZERO, "do not invent the absent original size");
            assert_eq!(order.filled_quantity, Decimal::new(190, 2));
            assert_eq!(order_update_to_trade(&msg).unwrap().quantity, Decimal::new(14, 2));
            assert_eq!(order_update_to_trade(&msg).unwrap().fee, Decimal::new(1, 3));
            assert_eq!(
                order_update_to_trade(&msg).unwrap().realized_pnl,
                Some(Decimal::new(15, 3))
            );
        }
    }

    #[test]
    fn extracts_client_and_order_id_from_place_order_event() {
        let value = serde_json::json!({
            "place_order": {
                "user_address": "GM8pdc",
                "order_id": 223_265_793_u64,
                "client_order_id": 17_801_200_030_000_u64,
                "side": "bid"
            }
        });
        let (client_id, order_id) = place_order_ids(value.as_object().unwrap()).unwrap();
        assert_eq!(client_id, 17_801_200_030_000);
        assert_eq!(order_id, "223265793");
    }

    #[test]
    fn client_id_key_is_numeric_so_non_canonical_strings_still_match() {
        // The broker looks up by the numeric client_order_id, so a strategy that
        // sent a non-canonical string like "00123" (which the send path parses to
        // 123) still resolves its order_id.
        let value = serde_json::json!({
            "place_order": { "order_id": 9_u64, "client_order_id": 123_u64 }
        });
        let (client_id, _) = place_order_ids(value.as_object().unwrap()).unwrap();
        assert_eq!(client_id, "00123".parse::<u64>().unwrap());
    }

    #[test]
    fn returns_none_for_non_place_order_event() {
        let value = serde_json::json!({ "fill": { "order_id": 1_u64 } });
        assert!(place_order_ids(value.as_object().unwrap()).is_none());
    }

    #[test]
    fn returns_none_when_ids_missing() {
        let value = serde_json::json!({ "place_order": { "side": "bid" } });
        assert!(place_order_ids(value.as_object().unwrap()).is_none());
    }
}
