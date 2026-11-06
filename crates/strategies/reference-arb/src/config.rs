//! Single-instrument Binance → Bullet lead–lag settings.
use bb_core::config::ValidateConfig;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceArbConfig {
    pub exchange: String,
    pub symbol: String,
    pub binance_symbol: String,
    #[serde(default = "default_binance_market")]
    pub binance_market: String,
    pub order_notional: Decimal,
    pub basis_half_life_secs: f64,
    /// A: minimum log edge from the executable ask/bid, in bp.
    pub entry_threshold_bps: f64,
    /// B: reserve relative to fair at the signal, in bp, used for the IOC limit.
    pub entry_reserve_bps: f64,
    pub exit: ExitConfig,
    pub stop_loss_bps: f64,
    /// Stop this run when net realized `PnL` falls this far from its high-water mark.
    /// Quote currency (USD on Bullet); omit to disable.
    pub max_drawdown_usd: Option<Decimal>,
    pub max_hold_secs: u64,
    pub reference_stale_ms: u64,
    /// Omit this table to disable the impulse filter.
    pub impulse: Option<ImpulseConfig>,
    /// Existing Bullet market-order protection, used only for exits.
    #[serde(default = "default_market_slippage_bps")]
    pub market_slippage_bps: Decimal,
    #[serde(default)]
    pub dry_run: bool,
}

/// Exactly one strategy exit, in addition to the common stop and hold deadline.
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExitConfig {
    /// R: exit when the signed distance from Bullet mid to frozen-basis fair is <= R.
    Fair { remaining_bps: f64 },
    /// D: exit on this log retreat from the best available closing quote since fill.
    Trailing { drawdown_bps: f64 },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ImpulseConfig {
    pub window_ms: u64,
    pub threshold_bps: f64,
}

fn default_binance_market() -> String {
    "perp".into()
}
fn default_market_slippage_bps() -> Decimal {
    Decimal::from(50)
}

impl ValidateConfig for ReferenceArbConfig {
    fn validate(&self) -> Result<(), String> {
        if self.order_notional <= Decimal::ZERO {
            return Err("order_notional must be positive".into());
        }
        if self.max_drawdown_usd.is_some_and(|limit| limit <= Decimal::ZERO) {
            return Err("max_drawdown_usd must be positive when set".into());
        }
        let exit_threshold = match self.exit {
            ExitConfig::Fair { remaining_bps } => ("exit.remaining_bps", remaining_bps, false),
            ExitConfig::Trailing { drawdown_bps } => ("exit.drawdown_bps", drawdown_bps, true),
        };
        for (name, value, positive) in [
            ("basis_half_life_secs", self.basis_half_life_secs, true),
            ("entry_threshold_bps", self.entry_threshold_bps, false),
            exit_threshold,
            ("stop_loss_bps", self.stop_loss_bps, true),
        ] {
            if !value.is_finite() || (positive && value <= 0.0) {
                return Err(format!(
                    "{name} must be finite{}",
                    if positive { " and positive" } else { "" }
                ));
            }
        }
        if self.max_hold_secs == 0 || self.reference_stale_ms == 0 {
            return Err("max_hold_secs and reference_stale_ms must be positive".into());
        }
        if !self.entry_reserve_bps.is_finite() || self.entry_reserve_bps < 0.0 {
            return Err("entry_reserve_bps must be finite and non-negative".into());
        }
        if self.market_slippage_bps < Decimal::ZERO
            || self.market_slippage_bps >= Decimal::from(10_000)
        {
            return Err("market_slippage_bps must be non-negative and less than 10000".into());
        }
        if let Some(impulse) = &self.impulse
            && (impulse.window_ms == 0 || !impulse.threshold_bps.is_finite())
        {
            return Err("impulse requires a positive window_ms and finite threshold_bps".into());
        }
        self.binance_market.parse::<bb_exchange_binance::BinanceMarket>()?;
        Ok(())
    }
}

impl ReferenceArbConfig {
    pub fn validate(&self) -> Result<(), String> {
        <Self as ValidateConfig>::validate(self)
    }
}
