//! `BulletBroker` — WS trading and REST queries behind `bb_core::broker::Broker`.
//!
//! Holds an `Arc<Client>` shared with the streaming side (`BulletConnection`).
//! All order-placement / account-query methods go through this type; streaming
//! events are handled separately by the typed feeds.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use bb_core::broker::{Broker, OrderRecovery};
use bb_core::error::BotError;
use bb_core::health::ConnectionHealth;
use bb_core::types::{
    AmendOrder, Balance, CancelOrder, CancelResult, NewOrder, Order, OrderBook, OrderResult,
    OrderStatus, OrderType, Position, Side,
};
use bullet_rust_sdk::codegen::ClientInfo;
use bullet_rust_sdk::codegen::types::Filter;
use bullet_rust_sdk::types::{ClientMessage, ErrorCode, OrderParams, UserAction, WSError};
use bullet_rust_sdk::ws::models::{OrderResultPayload, ServerMessage, TaggedMessage};
use bullet_rust_sdk::{
    AmendOrderArgs, CallMessage, CancelOrderArgs, Client, ClientOrderId, MarketId, NewOrderArgs,
    OrderId, OrderType as BulletOrderType, PositiveDecimal, Side as BulletSide, SignedTransaction,
    Transaction, UniquenessData,
};
use rust_decimal::{Decimal, RoundingStrategy};
use tokio::sync::{mpsc, oneshot};

use crate::connection::WsRequest;
use crate::convert;

#[derive(Debug, Clone, Copy)]
pub(crate) struct Increments {
    pub tick_size: Decimal,
    pub step_size: Decimal,
}

pub struct BulletBroker {
    client: Arc<Client>,
    account_address: String,
    increments: HashMap<String, Increments>,
    health: Arc<ConnectionHealth>,
    requests: mpsc::UnboundedSender<WsRequest>,
}

impl BulletBroker {
    pub(crate) fn new(
        client: Arc<Client>,
        account_address: String,
        increments: HashMap<String, Increments>,
        health: Arc<ConnectionHealth>,
        requests: mpsc::UnboundedSender<WsRequest>,
    ) -> Self {
        Self { client, account_address, increments, health, requests }
    }

    fn market_id(&self, symbol: &str) -> Result<MarketId, BotError> {
        self.client
            .market_id(symbol)
            .ok_or_else(|| BotError::config(format!("Unknown symbol: {symbol}")))
    }

    /// Quantity increment already loaded from the venue's `LOT_SIZE` filter.
    pub fn quantity_step(&self, symbol: &str) -> Result<Decimal, BotError> {
        self.increments
            .get(symbol)
            .map(|increments| increments.step_size)
            .ok_or_else(|| BotError::config(format!("No quantity step cached for {symbol}")))
    }

    /// Snap `value` to the nearest multiple of `increment` using `strategy`.
    fn snap(value: Decimal, increment: Decimal, strategy: RoundingStrategy) -> Decimal {
        if increment.is_zero() {
            return value;
        }
        (value / increment).round_dp_with_strategy(0, strategy) * increment
    }

    async fn submit(&self, message: CallMessage) -> Result<OrderResultPayload, BotError> {
        let mut refreshed = false;
        loop {
            let signed = Transaction::builder()
                .call_message(message.clone())
                .client(&self.client)
                .build()
                .map_err(BotError::strategy)?;
            let params =
                OrderParams { tx: Transaction::to_base64(&signed).map_err(BotError::strategy)? };
            let window = match &signed {
                SignedTransaction::V0(tx) => match tx.uniqueness {
                    UniquenessData::Window(nonce) => Some(nonce),
                    _ => None,
                },
                _ => None,
            };
            let command = match &message {
                CallMessage::User(UserAction::PlaceOrders { .. }) => {
                    ClientMessage::OrderPlace { id: None, params }
                }
                CallMessage::User(UserAction::CancelOrders { .. }) => {
                    ClientMessage::OrderCancel { id: None, params }
                }
                CallMessage::User(UserAction::AmendOrders { .. }) => {
                    ClientMessage::OrderAmend { id: None, params }
                }
                CallMessage::User(UserAction::CancelMarketOrders { .. }) => {
                    ClientMessage::OrderCancelAll { id: None, params }
                }
                _ => return Err(BotError::strategy("Unsupported Bullet trading action")),
            };
            let (reply_tx, reply_rx) = oneshot::channel();
            self.requests
                .send(WsRequest::Submit { command, call: message.clone(), window, reply: reply_tx })
                .map_err(|_| BotError::strategy("Bullet WS stopped before request was queued"))?;
            let reply = reply_rx.await.map_err(|_| {
                BotError::exchange("Bullet WS ended before acknowledgement; outcome unknown", true)
            })??;
            match reply {
                ServerMessage::Tagged(
                    TaggedMessage::OrderPlace(r)
                    | TaggedMessage::OrderCancel(r)
                    | TaggedMessage::OrderAmend(r)
                    | TaggedMessage::OrderCancelAll(r),
                ) => {
                    if r.results.status.is_success() {
                        return Ok(r.results);
                    }
                    // A dropped transaction may be the SDK's identical replay after a reconnect.
                    // Its first delivery might have executed: do not release the pending entry.
                    return Err(BotError::exchange(
                        format!(
                            "Bullet WS transaction {}: {:?}; outcome unknown",
                            r.results.tx_id, r.results.status
                        ),
                        true,
                    ));
                }
                ServerMessage::Tagged(TaggedMessage::Error(e)) | ServerMessage::Error(e) => {
                    if e.error.error_code() == ErrorCode::InvalidSignature && !refreshed {
                        // Preserve the SDK REST helper's one refresh/re-sign after an explicit
                        // signature rejection. Never re-sign a timeout or an unknown outcome.
                        self.client.update_schema().await.map_err(|err| {
                            BotError::strategy(format!(
                                "Signature rejected; schema refresh failed: {err}"
                            ))
                        })?;
                        refreshed = true;
                        continue;
                    }
                    return Err(ws_error(&e.error));
                }
                _ => {
                    return Err(BotError::exchange(
                        "Unexpected Bullet WS reply; outcome unknown",
                        true,
                    ));
                }
            }
        }
    }
}

fn ws_error(error: &WSError) -> BotError {
    let message = error.message().to_ascii_lowercase();
    let rejected = matches!(
        error.error_code(),
        ErrorCode::Unauthorized
            | ErrorCode::ValidationError
            | ErrorCode::TooManyOrders
            | ErrorCode::UnsupportedOperation
            | ErrorCode::InvalidTimestamp
            | ErrorCode::InvalidSignature
            | ErrorCode::MandatoryParamMissing
            | ErrorCode::BadPrecision
            | ErrorCode::InvalidOrderType
            | ErrorCode::InvalidSide
            | ErrorCode::InvalidSymbol
            | ErrorCode::InvalidUserAddress
            | ErrorCode::NewOrderRejected
            | ErrorCode::CancelRejected
            | ErrorCode::NoSuchOrder
            | ErrorCode::ApiKeyFormatInvalid
            | ErrorCode::InvalidApiKeyIpPermissions
            | ErrorCode::OrderWouldTrigger
    ) && !message.contains("duplicate")
        && !message.contains("already");
    if rejected {
        BotError::strategy(format!("Bullet WS rejected ({}): {error}", error.code()))
    } else {
        BotError::exchange(format!("Bullet WS outcome unknown ({}): {error}", error.code()), true)
    }
}

/// Fetch symbol filters (tickSize / stepSize) from exchangeInfo and cache them
/// by symbol. Used at connect-time by `BulletConnection` before handing the
/// broker to the harness.
pub(crate) async fn load_increments(
    client: &Client,
) -> Result<HashMap<String, Increments>, BotError> {
    let info = client.exchange_info().await.map_err(|e| BotError::exchange(e, true))?;
    let mut out = HashMap::new();
    for sym in &info.into_inner().symbols {
        let mut tick = None;
        let mut step = None;
        for f in &sym.filters {
            match f {
                Filter::PriceFilter { tick_size, .. } => tick = Some(*tick_size),
                Filter::LotSize { step_size, .. } => step = Some(*step_size),
                _ => {}
            }
        }
        if let (Some(tick_size), Some(step_size)) = (tick, step) {
            out.insert(sym.symbol.clone(), Increments { tick_size, step_size });
        }
    }
    Ok(out)
}

#[async_trait]
impl Broker for BulletBroker {
    fn name(&self) -> &'static str {
        "bullet"
    }

    fn take_reconcile_signal(&self) -> bool {
        self.health.take_reconcile_signal()
    }

    fn is_disconnected(&self) -> bool {
        self.health.is_disconnected()
    }

    fn last_reconnect_at(&self) -> Option<Instant> {
        self.health.last_reconnect_at()
    }

    async fn refresh_connection_if_idle(&self) -> bool {
        if !self.health.take_replacement_ready() {
            return false;
        }
        self.requests.send(WsRequest::RefreshConnection).is_ok()
    }

    async fn get_orderbook(&self, symbol: &str, depth: usize) -> Result<OrderBook, BotError> {
        let resp = self
            .client
            .order_book(Some(i32::try_from(depth).unwrap_or(i32::MAX)), symbol)
            .await
            .map_err(|e| BotError::exchange(e, true))?
            .into_inner();
        Ok(OrderBook {
            bids: convert::parse_orderbook_levels(&resp.bids),
            asks: convert::parse_orderbook_levels(&resp.asks),
            last_update_id: resp.last_update_id,
        })
    }

    async fn get_balances(&self) -> Result<Vec<Balance>, BotError> {
        let resp = self
            .client
            .account_balance(&self.account_address)
            .await
            .map_err(|e| BotError::exchange(e, true))?
            .into_inner();
        Ok(resp
            .iter()
            .map(|b| Balance {
                asset: b.asset.clone(),
                available: b.available_balance,
                total: b.balance,
            })
            .collect())
    }

    async fn get_positions(&self) -> Result<Vec<Position>, BotError> {
        let resp = self
            .client
            .account_info(&self.account_address)
            .await
            .map_err(|e| BotError::exchange(e, true))?
            .into_inner();
        Ok(resp
            .positions
            .iter()
            .filter(|p| !p.position_amt.is_zero())
            .map(|p| {
                let side =
                    if p.position_amt > Decimal::ZERO { Some(Side::Buy) } else { Some(Side::Sell) };
                Position {
                    symbol: p.symbol.clone(),
                    side,
                    size: p.position_amt.abs(),
                    entry_price: p.entry_price,
                    unrealized_pnl: p.unrealized_profit,
                }
            })
            .collect())
    }

    async fn get_open_orders(&self, symbol: &str) -> Result<Vec<Order>, BotError> {
        let resp = self
            .client
            .query_open_orders(&self.account_address, Some(symbol))
            .await
            .map_err(|e| BotError::exchange(e, true))?
            .into_inner();
        Ok(resp
            .iter()
            .map(|o| {
                let side = if o.side == "BUY" { Side::Buy } else { Side::Sell };
                let order_type = match o.order_type.as_str() {
                    "POST_ONLY" => OrderType::PostOnly,
                    _ => OrderType::Limit,
                };
                Order {
                    id: o.order_id.to_string(),
                    client_id: o.client_order_id.clone(),
                    symbol: o.symbol.clone(),
                    side,
                    order_type,
                    price: o.price,
                    quantity: o.orig_qty,
                    filled_quantity: o.executed_qty,
                    status: OrderStatus::Open,
                }
            })
            .collect())
    }

    async fn recover_order(
        &self,
        symbol: &str,
        client_id: &str,
        order_id: Option<&str>,
        submitted_ms: u64,
    ) -> Result<OrderRecovery, BotError> {
        let mut recovered = crate::recovery::recover(
            ClientInfo::client(self.client.client()),
            self.client.url(),
            &self.account_address,
            symbol,
            client_id,
            order_id,
            submitted_ms,
        )
        .await?;
        if recovered.order.is_none() && recovered.trades.is_empty() {
            // A lost RPC can leave an empty IOC with no venue order ID forever.
            // Inspect the original SDK nonce; never re-sign or resend the order.
            let (reply, rx) = oneshot::channel();
            self.requests
                .send(WsRequest::SubmissionWindow { client_id: client_id.into(), reply })
                .map_err(|_| BotError::exchange("Bullet WS stopped during recovery", true))?;
            if let Some(window) = rx.await.map_err(|e| BotError::exchange(e, true))? {
                let signer = self
                    .client
                    .keypair()
                    .ok_or_else(|| BotError::config("Missing signer"))?
                    .address();
                let state = self
                    .client
                    .client()
                    .address_state(&signer)
                    .await
                    .map_err(|e| BotError::exchange(e, true))?
                    .into_inner();
                // Bullet's Window uniqueness spans 1000 nonces (protocol rule,
                // not 1000 ms of wall time). Query the signing delegate, not master.
                recovered.submission_expired = state.window.saturating_sub(window) > 1000;
                if recovered.submission_expired {
                    tracing::info!(
                        client_id,
                        window,
                        current_window = state.window,
                        "Original submission is outside the venue replay window"
                    );
                }
            }
        }
        Ok(recovered)
    }

    async fn place_orders(&self, orders: &[NewOrder]) -> Result<Vec<OrderResult>, BotError> {
        if orders.is_empty() {
            return Ok(vec![]);
        }
        let symbol = &orders[0].symbol;
        let market_id = self.market_id(symbol)?;
        let incr = *self
            .increments
            .get(symbol)
            .ok_or_else(|| BotError::config(format!("No tick/step cached for {symbol}")))?;

        let sdk_orders: Vec<NewOrderArgs> = orders
            .iter()
            .map(|o| -> Result<NewOrderArgs, BotError> {
                let price_strategy = match o.side {
                    Side::Buy => RoundingStrategy::ToZero,
                    Side::Sell => RoundingStrategy::AwayFromZero,
                };
                let snapped_price = Self::snap(o.price, incr.tick_size, price_strategy);
                let snapped_qty = Self::snap(o.quantity, incr.step_size, RoundingStrategy::ToZero);
                let price = PositiveDecimal::try_from(snapped_price)
                    .map_err(|e| BotError::strategy(format!("Invalid price: {e}")))?;
                let qty_pd = PositiveDecimal::try_from(snapped_qty)
                    .map_err(|e| BotError::strategy(format!("Invalid quantity: {e}")))?;
                let side = match o.side {
                    Side::Buy => BulletSide::Bid,
                    Side::Sell => BulletSide::Ask,
                };
                let order_type = match o.order_type {
                    OrderType::Limit => BulletOrderType::Limit,
                    OrderType::PostOnly => BulletOrderType::PostOnly,
                    OrderType::Market => BulletOrderType::ImmediateOrCancel,
                };
                let client_order_id = o
                    .client_id
                    .as_deref()
                    .map(|s| {
                        s.parse::<u64>().map(ClientOrderId).map_err(|e| {
                            BotError::strategy(format!(
                                "client_id '{s}' must be a u64 for Bullet: {e}"
                            ))
                        })
                    })
                    .transpose()?;

                Ok(NewOrderArgs {
                    price,
                    size: qty_pd,
                    side,
                    order_type,
                    reduce_only: o.reduce_only,
                    client_order_id,
                    pending_tpsl_pair: None,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let rejected = |error: String| {
            orders
                .iter()
                .map(|o| OrderResult {
                    order_id: None,
                    client_id: o.client_id.clone(),
                    success: false,
                    error: Some(error.clone()),
                })
                .collect()
        };
        let message = CallMessage::User(UserAction::PlaceOrders {
            market_id,
            orders: sdk_orders,
            replace: false,
            sub_account_index: None,
        });
        match self.submit(message).await {
            Ok(resp) => {
                // The SDK documents matching positions in these two response arrays.
                // Match by client ID, never by the order's position in our request.
                let oids: HashMap<u64, String> = resp
                    .client_order_ids
                    .iter()
                    .copied()
                    .zip(resp.order_ids.iter().map(ToString::to_string))
                    .collect();
                tracing::debug!(tx_id = %resp.tx_id, status = ?resp.status, oids = oids.len(), "Orders submitted via WS");
                Ok(orders
                    .iter()
                    .map(|o| {
                        let order_id = o
                            .client_id
                            .as_deref()
                            .and_then(|c| c.parse::<u64>().ok())
                            .and_then(|cid| oids.get(&cid).cloned());
                        OrderResult {
                            order_id,
                            client_id: o.client_id.clone(),
                            success: true,
                            error: None,
                        }
                    })
                    .collect())
            }
            Err(e @ (BotError::Config(_) | BotError::Strategy(_))) => Ok(rejected(e.to_string())),
            Err(e) => Err(e),
        }
    }

    async fn cancel_orders(&self, cancels: &[CancelOrder]) -> Result<Vec<CancelResult>, BotError> {
        if cancels.is_empty() {
            return Ok(vec![]);
        }
        let market_id = self.market_id(&cancels[0].symbol)?;

        let mut results: Vec<Option<CancelResult>> = vec![None; cancels.len()];
        let mut sdk_cancels: Vec<CancelOrderArgs> = Vec::new();
        let mut sent_indices: Vec<usize> = Vec::new();
        for (i, c) in cancels.iter().enumerate() {
            let order_id = c.order_id.parse::<u64>().ok().map(OrderId);
            let client_order_id =
                c.client_id.as_deref().and_then(|s| s.parse::<u64>().ok()).map(ClientOrderId);
            let cancel = match (order_id, client_order_id) {
                (Some(oid), _) => CancelOrderArgs { order_id: Some(oid), client_order_id: None },
                (None, Some(cid)) => CancelOrderArgs { order_id: None, client_order_id: Some(cid) },
                (None, None) => {
                    results[i] = Some(CancelResult {
                        order_id: c.order_id.clone(),
                        success: false,
                        error: Some(
                            "cancel not sent: Bullet requires numeric order_id or client_id"
                                .to_string(),
                        ),
                    });
                    continue;
                }
            };
            sent_indices.push(i);
            sdk_cancels.push(cancel);
        }

        if !sdk_cancels.is_empty() {
            let response = self
                .submit(CallMessage::User(UserAction::CancelOrders {
                    market_id,
                    orders: sdk_cancels,
                    sub_account_index: None,
                }))
                .await;
            let error = response.err().map(|e| {
                tracing::warn!(error = %e, "Bullet WS cancel request failed");
                e.to_string()
            });
            for i in sent_indices {
                results[i] = Some(CancelResult {
                    order_id: cancels[i].order_id.clone(),
                    success: error.is_none(),
                    error: error.clone(),
                });
            }
        }
        Ok(results
            .into_iter()
            .enumerate()
            .map(|(i, result)| {
                result.unwrap_or(CancelResult {
                    order_id: cancels[i].order_id.clone(),
                    success: false,
                    error: Some("cancel not sent".into()),
                })
            })
            .collect())
    }

    async fn amend_orders(&self, amends: &[AmendOrder]) -> Result<Vec<OrderResult>, BotError> {
        if amends.is_empty() {
            return Ok(vec![]);
        }
        let symbol = &amends[0].new_order.symbol;
        let market_id = self.market_id(symbol)?;
        let incr = *self
            .increments
            .get(symbol)
            .ok_or_else(|| BotError::config(format!("No tick/step cached for {symbol}")))?;

        let mut results: Vec<Option<OrderResult>> = vec![None; amends.len()];
        let mut sdk_amends: Vec<AmendOrderArgs> = Vec::new();
        let mut sent_indices: Vec<usize> = Vec::new();
        for (i, amend) in amends.iter().enumerate() {
            match build_amend_args(amend, incr) {
                Ok(args) => {
                    sent_indices.push(i);
                    sdk_amends.push(args);
                }
                Err(error) => {
                    results[i] = Some(OrderResult {
                        order_id: None,
                        client_id: amend.new_order.client_id.clone(),
                        success: false,
                        error: Some(error),
                    });
                }
            }
        }

        if !sdk_amends.is_empty() {
            let response = self
                .submit(CallMessage::User(UserAction::AmendOrders {
                    market_id,
                    orders: sdk_amends,
                    sub_account_index: None,
                }))
                .await;
            let error = response.err().map(|e| {
                tracing::warn!(error = %e, "Bullet WS amend request failed");
                e.to_string()
            });
            for i in sent_indices {
                results[i] = Some(OrderResult {
                    order_id: None,
                    client_id: amends[i].new_order.client_id.clone(),
                    success: error.is_none(),
                    error: error.clone(),
                });
            }
        }
        Ok(results
            .into_iter()
            .enumerate()
            .map(|(i, result)| {
                result.unwrap_or(OrderResult {
                    order_id: None,
                    client_id: amends[i].new_order.client_id.clone(),
                    success: false,
                    error: Some("amend not sent".into()),
                })
            })
            .collect())
    }

    async fn cancel_all_orders(&self, symbol: &str) -> Result<(), BotError> {
        let market_id = self.market_id(symbol)?;
        self.submit(CallMessage::User(UserAction::CancelMarketOrders {
            market_id,
            sub_account_index: None,
        }))
        .await?;
        Ok(())
    }
}

fn build_amend_args(amend: &AmendOrder, incr: Increments) -> Result<AmendOrderArgs, String> {
    let order_id = amend.cancel.order_id.parse::<u64>().ok().map(OrderId);
    let client_order_id =
        amend.cancel.client_id.as_deref().and_then(|s| s.parse::<u64>().ok()).map(ClientOrderId);
    let cancel = match (order_id, client_order_id) {
        (Some(oid), _) => CancelOrderArgs { order_id: Some(oid), client_order_id: None },
        (None, Some(cid)) => CancelOrderArgs { order_id: None, client_order_id: Some(cid) },
        (None, None) => {
            return Err("amend not sent: Bullet requires numeric order_id or client_id".to_string());
        }
    };

    let o = &amend.new_order;
    let price_strategy = match o.side {
        Side::Buy => RoundingStrategy::ToZero,
        Side::Sell => RoundingStrategy::AwayFromZero,
    };
    let price = BulletBroker::snap(o.price, incr.tick_size, price_strategy);
    let qty = BulletBroker::snap(o.quantity, incr.step_size, RoundingStrategy::ToZero);
    let price = PositiveDecimal::try_from(price).map_err(|e| format!("Invalid price: {e}"))?;
    let qty_pd = PositiveDecimal::try_from(qty).map_err(|e| format!("Invalid quantity: {e}"))?;
    let side = match o.side {
        Side::Buy => BulletSide::Bid,
        Side::Sell => BulletSide::Ask,
    };
    let order_type = match o.order_type {
        OrderType::Limit => BulletOrderType::Limit,
        OrderType::PostOnly => BulletOrderType::PostOnly,
        OrderType::Market => BulletOrderType::ImmediateOrCancel,
    };
    let new_client_order_id = o
        .client_id
        .as_deref()
        .map(|s| {
            s.parse::<u64>()
                .map(ClientOrderId)
                .map_err(|e| format!("client_id '{s}' must be a u64 for Bullet: {e}"))
        })
        .transpose()?;
    Ok(AmendOrderArgs {
        cancel,
        place: NewOrderArgs {
            price,
            size: qty_pd,
            side,
            order_type,
            reduce_only: o.reduce_only,
            client_order_id: new_client_order_id,
            pending_tpsl_pair: None,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn increments() -> Increments {
        Increments { tick_size: Decimal::new(1, 2), step_size: Decimal::new(1, 3) }
    }

    fn amend(
        cancel_order_id: &str,
        cancel_client_id: Option<&str>,
        new_client_id: Option<&str>,
    ) -> AmendOrder {
        AmendOrder {
            cancel: CancelOrder {
                symbol: "BTC-USD".into(),
                order_id: cancel_order_id.into(),
                client_id: cancel_client_id.map(str::to_string),
            },
            new_order: NewOrder {
                symbol: "BTC-USD".into(),
                side: Side::Buy,
                order_type: OrderType::PostOnly,
                price: Decimal::from(100),
                quantity: Decimal::new(1, 3),
                client_id: new_client_id.map(str::to_string),
                reduce_only: false,
            },
        }
    }

    #[test]
    fn build_amend_rejects_missing_cancel_identifier() {
        let err = build_amend_args(&amend("", None, Some("1")), increments()).unwrap_err();
        assert!(err.contains("numeric order_id or client_id"));
    }

    #[test]
    fn build_amend_rejects_non_numeric_new_client_id() {
        let err = build_amend_args(&amend("123", None, Some("abc")), increments()).unwrap_err();
        assert!(err.contains("must be a u64"));
    }
}
