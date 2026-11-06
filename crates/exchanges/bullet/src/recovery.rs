//! Read-only recovery of an IOC from Bullet's existing account endpoints.
//! The SDK's generated history methods omit the cursor parameter, so these
//! two paginated reads borrow the SDK HTTP client (and its connection pool).
use std::collections::HashSet;

use bb_core::broker::OrderRecovery;
use bb_core::error::BotError;
use bb_core::events::Trade;
use bb_core::types::{Order, OrderStatus, OrderType, Side};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde::de::DeserializeOwned;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Page<T> {
    data: Vec<T>,
    next_cursor: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RestFill {
    symbol: String,
    id: u64,
    order_id: u64,
    client_order_id: Option<u64>,
    side: String,
    price: Decimal,
    qty: Decimal,
    commission: Decimal,
    realized_pnl: Decimal,
    time: i64,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RestOrder {
    symbol: String,
    order_id: u64,
    #[serde(default)]
    client_order_id: Option<serde_json::Value>,
    side: String,
    status: String,
    price: Decimal,
    orig_qty: Decimal,
    executed_qty: Decimal,
}
impl RestOrder {
    fn client_id(&self) -> Option<String> {
        self.client_order_id.as_ref().map(|v| match v {
            serde_json::Value::String(s) => s.clone(),
            v => v.to_string(),
        })
    }
    fn into_order(self, client_id: &str) -> Result<Order, BotError> {
        Ok(Order {
            id: self.order_id.to_string(),
            client_id: Some(client_id.to_owned()),
            symbol: self.symbol,
            side: side(&self.side)?,
            order_type: OrderType::Market,
            price: self.price,
            quantity: self.orig_qty,
            filled_quantity: self.executed_qty,
            status: status(&self.status)?,
        })
    }
}
fn side(value: &str) -> Result<Side, BotError> {
    match value {
        "BUY" => Ok(Side::Buy),
        "SELL" => Ok(Side::Sell),
        _ => Err(BotError::exchange(format!("Unknown REST side: {value}"), true)),
    }
}
fn status(value: &str) -> Result<OrderStatus, BotError> {
    match value {
        "NEW" => Ok(OrderStatus::Open),
        "PARTIALLY_FILLED" => Ok(OrderStatus::PartiallyFilled),
        "FILLED" => Ok(OrderStatus::Filled),
        "CANCELED" | "CANCELLED" | "EXPIRED" => Ok(OrderStatus::Cancelled),
        "REJECTED" | "FAILED" => Ok(OrderStatus::Rejected),
        _ => Err(BotError::exchange(format!("Unknown REST order status: {value}"), true)),
    }
}
async fn pages<T: DeserializeOwned>(
    http: &reqwest::Client,
    url: &str,
    params: &[(String, String)],
) -> Result<Vec<T>, BotError> {
    let mut result = Vec::new();
    let mut cursor: Option<String> = None;
    let mut seen = HashSet::new();
    loop {
        let mut query = params.to_vec();
        if let Some(cursor) = &cursor {
            query.push(("cursor".into(), cursor.clone()));
        }
        let page = http
            .get(url)
            .query(&query)
            .send()
            .await
            .map_err(|e| BotError::exchange(e, true))?
            .error_for_status()
            .map_err(|e| BotError::exchange(e, true))?
            .json::<Page<T>>()
            .await
            .map_err(|e| BotError::exchange(e, true))?;
        result.extend(page.data);
        let Some(next) = page.next_cursor.filter(|s| !s.is_empty()) else {
            break;
        };
        if !seen.insert(next.clone()) {
            return Err(BotError::exchange("History cursor repeated", true));
        }
        cursor = Some(next);
    }
    Ok(result)
}

pub async fn recover(
    http: &reqwest::Client,
    base: &str,
    account: &str,
    symbol: &str,
    client_id: &str,
    order_id: Option<&str>,
    submitted_ms: u64,
) -> Result<OrderRecovery, BotError> {
    let base = base.trim_end_matches('/');
    // Include a minute of overlap for local/venue clock skew; filter by identifiers below.
    let params = vec![
        ("address".into(), account.into()),
        ("symbol".into(), symbol.into()),
        ("startTime".into(), submitted_ms.saturating_sub(60_000).saturating_mul(1000).to_string()),
        ("limit".into(), "1000".into()),
    ];
    let fills: Vec<RestFill> = pages(http, &format!("{base}/fapi/v1/userTrades"), &params).await?;
    let known_id = order_id.map(str::to_owned).or_else(|| {
        fills
            .iter()
            .find(|f| f.client_order_id.map(|v| v.to_string()).as_deref() == Some(client_id))
            .map(|f| f.order_id.to_string())
    });
    // A known order ID already identifies the history row. Query open orders
    // only when both the submission response and fill-based lookup lack it.
    let open: Vec<RestOrder> = if known_id.is_none() {
        http.get(format!("{base}/fapi/v1/openOrders"))
            .query(&[("address", account), ("symbol", symbol)])
            .send()
            .await
            .map_err(|e| BotError::exchange(e, true))?
            .error_for_status()
            .map_err(|e| BotError::exchange(e, true))?
            .json()
            .await
            .map_err(|e| BotError::exchange(e, true))?
    } else {
        Vec::new()
    };
    let known_id = known_id.or_else(|| {
        open.iter()
            .find(|o| o.client_id().as_deref() == Some(client_id))
            .map(|o| o.order_id.to_string())
    });
    // Closed-order history has no guaranteed client ID. An unidentified empty
    // IOC cannot be resolved by scanning unrelated historical orders.
    let Some(known_id) = known_id else {
        return Ok(OrderRecovery::default());
    };
    let mut history_params = params.clone();
    history_params.push(("orderId".into(), known_id.clone()));
    let history: Vec<RestOrder> =
        pages(http, &format!("{base}/fapi/v1/allOrders"), &history_params).await?;
    let order = history
        .into_iter()
        .chain(open)
        .find(|o| {
            o.symbol == symbol
                && (known_id == o.order_id.to_string()
                    || o.client_id().as_deref() == Some(client_id))
        })
        .map(|o| o.into_order(client_id))
        .transpose()?;
    let mut trades = Vec::new();
    for f in fills {
        if f.symbol != symbol
            || !(f.client_order_id.map(|v| v.to_string()).as_deref() == Some(client_id)
                || known_id == f.order_id.to_string())
        {
            continue;
        }
        trades.push(Trade {
            exchange: "bullet".into(),
            symbol: f.symbol,
            order_id: f.order_id.to_string(),
            trade_id: Some(f.id.to_string()),
            client_id: Some(client_id.into()),
            side: side(&f.side)?,
            price: f.price,
            quantity: f.qty,
            fee: f.commission,
            realized_pnl: Some(f.realized_pnl),
            timestamp: u64::try_from(f.time).ok().map(|t| t / 1000),
        });
    }
    trades.sort_by_key(|t| (t.timestamp, t.trade_id.clone()));
    Ok(OrderRecovery { order, trades, ..OrderRecovery::default() })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use serde_json::{Value, json};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    async fn server(replies: Vec<(&'static str, Value)>) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            for (path, reply) in replies {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                loop {
                    let mut bytes = [0; 4096];
                    let n = socket.read(&mut bytes).await.unwrap();
                    assert!(n > 0);
                    request.extend_from_slice(&bytes[..n]);
                    if request.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                let request = String::from_utf8(request).unwrap();
                assert!(request.starts_with("GET "));
                assert!(request.lines().next().unwrap().contains(path), "{request}");
                let body = reply.to_string();
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        (url, task)
    }
    fn fill(id: u64, client: Option<u64>) -> Value {
        json!({"symbol":"LIT-USD","id":id,"orderId":456,"clientOrderId":client,"side":"SELL","price":"1.25","qty":"4","commission":"0.01","realizedPnl":"1.5","time":1_000_000_000})
    }
    fn order(status: &str, filled: &str) -> Value {
        // Historical orders have no clientOrderId; recover it through the fill.
        json!({"symbol":"LIT-USD","orderId":456,"side":"SELL","status":status,"price":"1.24","origQty":"10","executedQty":filled})
    }
    #[tokio::test]
    async fn follows_all_pages_resolves_order_and_converts_microsecond_timestamps() {
        let (url, task) = server(vec![
            ("/userTrades?", json!({"data":[fill(1,Some(123))],"nextCursor":"second"})),
            ("cursor=second", json!({"data":[fill(2,None)],"nextCursor":null})),
            ("orderId=456", json!({"data":[order("CANCELED","8")]})),
        ])
        .await;
        let recovered =
            recover(&reqwest::Client::new(), &url, "account", "LIT-USD", "123", None, 1_000_000)
                .await
                .unwrap();
        task.await.unwrap();
        assert_eq!(recovered.trades.len(), 2);
        assert_eq!(recovered.trades[0].timestamp, Some(1_000_000));
        assert_eq!(recovered.trades[0].side, Side::Sell);
        assert_eq!(recovered.trades[0].fee, Decimal::new(1, 2));
        assert_eq!(recovered.trades[0].realized_pnl, Some(Decimal::new(15, 1)));
        assert_eq!(recovered.trades[1].client_id.as_deref(), Some("123"));
        let order = recovered.order.unwrap();
        assert_eq!(order.id, "456");
        assert_eq!(order.status, OrderStatus::Cancelled);
        assert_eq!(order.filled_quantity, Decimal::from(8));
    }
    #[tokio::test]
    async fn known_empty_ioc_is_recovered_but_unidentified_history_is_not_guessed() {
        for known in [Some("456"), None] {
            let replies = if known.is_some() {
                vec![
                    ("/userTrades?", json!({"data":[]})),
                    ("/allOrders?", json!({"data":[order("EXPIRED","0")]})),
                ]
            } else {
                vec![("/userTrades?", json!({"data":[]})), ("/openOrders?", json!([]))]
            };
            let (url, task) = server(replies).await;
            let recovered = recover(
                &reqwest::Client::new(),
                &url,
                "account",
                "LIT-USD",
                "123",
                known,
                1_000_000,
            )
            .await
            .unwrap();
            task.await.unwrap();
            assert_eq!(recovered.order.is_some(), known.is_some());
            assert!(recovered.trades.is_empty());
        }
    }
    #[tokio::test]
    async fn resolves_missing_venue_id_from_an_open_order() {
        let mut open = order("NEW", "0");
        open["clientOrderId"] = json!("123");
        let (url, task) = server(vec![
            ("/userTrades?", json!({"data":[]})),
            ("/openOrders?", json!([open])),
            ("orderId=456", json!({"data":[order("EXPIRED", "0")]})),
        ])
        .await;
        let result =
            recover(&reqwest::Client::new(), &url, "account", "LIT-USD", "123", None, 1_000_000)
                .await
                .unwrap();
        task.await.unwrap();
        assert_eq!(result.order.unwrap().status, OrderStatus::Cancelled);
    }
    #[tokio::test]
    async fn repeated_cursor_is_an_error_not_silent_truncation() {
        let (url, task) = server(vec![
            ("/history?", json!({"data":[],"nextCursor":"same"})),
            ("cursor=same", json!({"data":[],"nextCursor":"same"})),
        ])
        .await;
        let result = pages::<Value>(
            &reqwest::Client::new(),
            &format!("{url}/history"),
            &[("limit".into(), "1000".into())],
        )
        .await;
        task.await.unwrap();
        assert!(result.is_err());
    }
}
