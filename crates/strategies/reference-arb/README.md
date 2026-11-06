# Binance → Bullet lead–lag

This local version adapts the upstream `reference-arb` actor for one instrument:
Binance USDⓈ-M `LITUSDT` leads, Bullet `LIT-USD` executes. The existing harness,
Binance socket, Bullet SDK connection, broker, client IDs and inventory tracker
are reused. Settings live in `config/reference-arb-example.toml`.

## Signal

For the latest received quotes, `mid = (bid + ask) / 2`, and Binance microprice
is `(ask * bid_qty + bid * ask_qty) / (bid_qty + ask_qty)`. Microprice has full
weight (λ = 1); there is no extra blend parameter.

```
basis = time-weighted EMA(log(mid_X / mid_L))
fair_X = micro_L * exp(basis)
entry_quote = ask_X for long, bid_X for short
edge_s = s * 10000 * (log(micro_L / entry_quote) + basis)
limit_s = fair_signal * exp(-s * entry_reserve_bps / 10000)
impulse_s = s * 10000 * log(mid_L(now) / mid_L(now - window))
```

`s = +1` for buys, `-1` for sells. The EMA starts at the first valid, fresh
pair of quotes. On each market update its weight is
`1 - exp(-ln(2) * elapsed_seconds / basis_half_life_secs)`; timer ticks do not
resample it. Quotes and impulse history use local adapter receive times,
not exchange clocks. The impulse anchor is the last received quote at or
before the cutoff. No interpolation, future quotes or mandatory warmup period.

Entry requires Binance at most `reference_stale_ms` old (1000 ms in the example)
and a valid Bullet BBO from the public pool,
no position, no unconfirmed order, `edge > entry_threshold_bps` (A) and, when
enabled, `impulse > threshold_bps`. A formula without its inputs returns no
value; missing basis/history is never replaced with zero. There is no separate
warmup timer.

Bullet BBO has no age limit: an unchanged quote remains usable while the
public pool has a synchronised book. Replacing one public socket or reconnecting
a private socket does not invalidate it. After loss of all public books,
entries, EMA updates and signal-reset rearming wait for a new valid BBO;
queued pre-loss quotes cannot satisfy
this. EMA keeps its previous value and resumes with the usual elapsed-time
weight once both quotes are usable. Protective exits can use the last valid
prices while waiting. The 10-second reentry delay does not bypass this check.
Deleting the `impulse` table disables this filter. There is no persistence
filter or fee-floor veto from the original example.

Entry edge, fair-exit R and impulse thresholds may be any finite number,
including negative values. The IOC reserve B must be finite and nonnegative;
trailing retreat D must be finite and positive. The proposed research grid uses
`0 <= B <= A` and `R <= A`; these are search constraints, not extra runtime vetoes.
Zero exit slippage is allowed. Budget, EMA half-life, hold time, quote
age limit and the timer interval remain positive; an exit allowance of 100%
or more is invalid because it can make a sell price non-positive.

## Run drawdown stop

`max_drawdown_usd = "30"` stops the run when **net realized** trading PnL
falls at least 30 USD from its highest value in this run. Omit the parameter
to disable it; a configured limit must be positive. PnL and its peak start at
zero on each process start. For example, a peak of +70 stops at +40 or below;
before any profit, a loss of 30 also triggers the stop.

Only a new, deduplicated fill updates the peak and checks the limit. Live PnL
uses the venue-reported realized PnL minus the actual commission (negative
commission is a rebate), from the existing WS execution and REST recovery
messages. This avoids recomputing realized PnL from an incomplete local entry
when fills arrive late. Paper uses the existing inventory calculation with
zero simulated fees. Unrealized PnL and market quotes do not move this peak.

The stop latches for the rest of the run: no further entries, remaining
exposure is closed reduce-only, and the existing graceful shutdown confirms
closure. Unknown orders do not block protective closes. A queued signal or a
late profit cannot rearm the stopped run. `/status` exposes `peak_pnl`,
`drawdown`, `max_drawdown_usd`, `drawdown_triggered` and the `stopping` state;
`inventory.realized_pnl` is net of fees in this strategy.

There is no balance polling or extra connection. This tracks this bot's trade
results, not deposits, withdrawals, funding or other account activity. The
limit is a trigger, not a guaranteed maximum loss: a closing fill can realize
more than the remaining allowance. The per-position price/time stops remain
independent. Restarting explicitly begins a new run with a new peak.

## Orders and position lifecycle

Entry freezes `basis_entry` and sends a priced IOC through the existing
Bullet broker (`OrderType::Market` is mapped to SDK `ImmediateOrCancel`).
`entry_reserve_bps` (B) places the buy limit below signal-time fair or the sell
limit above it. The broker rounds buys down and sells up to its cached tick size.
No spread switch or absolute/tick offset is used: entry-side spread is already
included in the executable ask/bid edge. A does not have the same meaning as the
old midpoint threshold. The exit-side spread and fees still affect realized PnL.
The limit does not follow later fair updates. B protects the distance to the
**signal-time** fair, not a changing fair at execution. With `edge > A` and
`B <= A`, it crosses the observed quote, even after rounding that preserves
the tick-aligned touch. Delivery delay may leave the IOC empty or partial.
Quantity is rounded down to the venue's LOT_SIZE step already cached by the
broker; it is supplied to the actor at startup, not configured separately.
Sizing uses the larger of touch and limit. Thus the requested notional is at most
the configured budget at those prices; a sell executed at a better price may receive more.
The current example uses a 100 budget and `dry_run=false` on mainnet.
LIT Bullet filters checked on 2026-10-27 are tick 0.0001 and size step 1.
EMA half-life is 300 seconds, A=2 bp, B=1 bp, impulse 250 ms / 1 bp.
B=1 is an illustrative configuration value, not a fitted replacement for the
old three-tick allowance. Configuration migration does not start the bot.

`flat`, `entering`, `holding` and `exiting` are derived status labels, not a
separately stored state machine. A new entry requires zero inventory and no
pending orders. A latched exit remains `exiting` while its residual awaits a retry.
A partial entry stays `Entering` until the IOC is filled or its remainder is
cancelled, unless a protective exit is triggered first. It holds the actual
filled quantity; no top-up. Bullet's `PARTIALLY_FILLED` fill with `rs=0`
confirms cancellation of the unfilled remainder without waiting for a separate
cancel or REST response. The hold
clock starts at reception of the **first fill confirmation**, never at the
latest partial fill or a later signal. While an entry remains unresolved with
no visible fills, H uses its submission time as a conservative protective
deadline; missing all entry notifications must not disable time-based closing.

Entry and price-based exit checks run on each market update. Private execution
updates also check exits. `Tick` is an internal timer event, not a price tick
or a market message. The example uses `tick_interval_ms = 1000` to check H,
retries and connection health between messages. Completed REST reads wake the
actor through the existing event bus, apply recovered fills and check exits
without waiting for a timer or market update. With no other
events, the hold-deadline check may be up to one second late, plus processing
delay. It does not delay price-based exits until the next tick. The first
matching condition initiates a reduce-only close:

- the selected fair or trailing condition below;
- available closing PnL at bid for longs / ask for shorts ≤ -5 bp;
- 60 seconds since the first confirmed entry fill.

Exactly one exit mode is required, with no old-formula fallback:

```toml
# R uses its direct sign: +1 exits before fair; -1.5 waits 1.5 bp beyond fair.
exit = { mode = "fair", remaining_bps = -1.5 }
# Or, instead of the fair condition:
# exit = { mode = "trailing", drawdown_bps = 2.0 }
```

Fair mode closes when `s * 10000 * (log(micro_L / mid_X) + basis_entry) <= R`.
Binance keeps updating; only the entry basis is frozen. R replaces the old
negated delta, so a previous delta of +1.5 becomes R=-1.5.

Trailing mode tracks the highest bid for longs / lowest ask for shorts,
starting with the available quote at the first confirmed entry fill. It closes
when `10000 * log(best_bid / bid)` for a long, or
`10000 * log(ask / best_ask)` for a short, is at least D. Partial/duplicate fills
do not reset the extremum. Pre-confirmation extremes are not used, including
when a missing fill is recovered by REST: no historical peak is fabricated.
There is no fair-exit condition or activation threshold in this mode. The
common stop, H, drawdown stop and shutdown still send reduce-only exits;
before an unseen entry fill is confirmed, H retains its submission-time fallback.

The per-position bp stop uses actual entry VWAP and a simple price return,
without fees. Global
basis continues updating for future trades. Stale quotes block entries,
but do not block an existing position's exit; it uses the last valid prices.
Once exit is triggered, partial/empty closing IOCs retry the remainder after
terminal confirmation, without resetting the deadline or frozen basis.

Bullet's existing broker implements MARKET with a protected IOC as well:
`market_slippage_bps = 50` sets the worst permitted exit price from the
opposite touch. This is a limit, not a promised execution price or a simulated
cost. A cancelled residual remains a position until a later close fills.

For each side separately, another entry is allowed after the signal has
returned to `edge ≤ entry_threshold_bps` since the previous attempt, **or** 10 s
since confirmed closure. An empty IOC starts those 10 s at confirmed
cancellation/rejection. Opposite-side permission is independent.

Logs and `/status` report the executable-side `edge` (`long_edge_bps` /
`short_edge_bps`), the selected `exit`, signed `remaining_bps`, and the trailing
`best_close` / `trailing_drawdown_bps` when initialized. Entry logs include
`fair_signal`, `basis_entry` and the fixed IOC limit. Obsolete midpoint-signal
fields are not kept as aliases.

## Necessary differences in event handling

The adapter reconstructs depth20 independently on three public SDK sockets.
It merges complete books by depth transaction time `T`, then sends BBO changes
to this strategy. Local `u`/`pu` are used only to maintain each socket's book.
Private events and order RPCs use a separate managed socket. Public rotation
does not affect execution recovery or protective exits. The recorder saves one
merged stream of full depth snapshots and deduplicated public trades.
See `docs/LOCAL.md` for the comparison/rotation rules and timestamp assumption.

`BookUpdate.received_at` prevents queued old quotes from becoming fresh when
the actor finally handles them. Binance's existing microprice event now also
carries arithmetic `midpoint` for basis/impulse; other consumers keep `mid`'s
original microprice meaning.

Binance now races three bookTicker sockets for this symbol. The first valid new
update id `u` reaches the actor and recorder; duplicate/older ids only contribute
to connection evaluation and cannot refresh the reference timestamp. This keeps
distinct updates with the same `T` and preserves the dedup watermark on reconnect.
The slowest public channel is replaced on the same five-minute schedule as the
Bullet pool, while two peers keep serving. No strategy setting is added.

The original independent Trade and OrderLifecycle subscriptions can process
a cancel before the preceding partial fill. Bullet therefore also exposes
an ordered `ExecutionUpdate` feed containing the same canonical Trade (if
present) and lifecycle from each private message. This actor subscribes only
to that feed; inventory changes only from its Trade part. Other strategies
keep their original subscriptions. Both private SDK sockets share this feed
and one fill-ID cache. An event carries its source subscription's start time:
a newly joined source cannot alone prove an old IOC's empty cancellation.
Its fills still apply immediately. A terminal partial fill with all cumulative
volume accounted for can complete through either source without REST.

With recording enabled, the same writer also stores private order updates
before fill deduplication, trading RPC replies (including errors and late
replies), and SDK decode failures with their raw text. `rpc.request` links
the RPC id to unsigned SDK order parameters, including client order id and
`reduce_only`; it records the enqueue result, never the signed transaction.
Its timestamp is taken immediately before the SDK enqueue attempts, not at
socket write or execution. The record is built after enqueue. Replies and
fills reach consumers before serialization; records retain receive timestamps
and the source connection/generation, including duplicate private deliveries.
These records use the existing bounded recording queue and loss counter;
a full or closed recorder does not prevent order submission or event delivery.
The ordinary nonblocking log independently keeps compact RPC requests/replies,
private order statuses and tracking completion. Strategy fills and errors carry
client/order ids and `reduce_only`. Periodic `arb tick` snapshots are DEBUG;
only an explicit no-fill rejection of an unfilled entry IOC is INFO, while
unexpected rejections and rejected protective exits remain WARN.

The private subscription is account-wide (`account@user.orders`), not specific
to this process. It includes that account's manual/other-client orders and may
replay earlier orders. The handler matches one of our pending client IDs before
applying a fill; public `aggTrade` messages are only recorded. This does not
track account-wide inventory changes from concurrent manual trading.
Each pending order stores its client ID and is removed on completion;
`exit_requested` alone stores the latched exit reason. Binance quote validation
stays in the adapter, which emits local receive times in feed order.

Confirmed submission rejections release the pending order; a rejected close
retries no faster than once a second. REST recovery is limited to a pending IOC:
an ambiguous submission error, a cumulative fill count exceeding received fills,
an undecodable fill, loss of both private subscriptions, or no terminal confirmation
for one second. Reconnecting while Flat/Holding needs no order recovery.
An ordinary ordered fill → cancel sequence (including an empty IOC) completes
directly from WS, as does a partial fill with explicit zero remaining size.
The cumulative fill count still detects missing fills in that terminal message.
A cancel's absent cumulative count is not evidence of loss.

Unknown orders block **new entries**, not protective reduce-only exits. A stop,
fair/trailing exit or H submits a reducing close even while the entry or a prior close
remains unresolved. For an unresolved entry, requested close size is signed
local inventory plus the still-unconfirmed part of the original entry quantity.
Thus it can close exposure whose entry fill is invisible, without exceeding
our possible entry size after known closes. The original requested quantity is
stored in the existing tracked-order record; it is not recomputed from prices.
An unresolved close may be retried after one second, using
the existing retry interval. Every close keeps the original reducing direction;
reduce-only lets the venue reject or limit a redundant close without opening a
reverse position. These are distinct order requests, not exactly-once submission.
An empty close against an unknown entry is also retried no faster than once
a second. When the entry becomes terminal, its known residual can close
immediately. PnL-based stops still need an actual entry fill price; trailing
needs a confirmed entry and a closing quote. Fair, H and shutdown do not need
an entry fill price. Request size is not credited as an inventory fill.

The existing tracked-order record is kept for each unresolved IOC. REST and WS
fills update their matching order by client ID and deduplicate by trade ID.
A late entry fill after a close reuses the frozen basis, first-fill deadline and
latched exit; its remaining exposure is closed too. Temporary zero inventory
does not permit a new entry until all earlier orders are terminal. A closing
fill received before a missing entry fill must not reverse the close direction.

Order submission runs in the background, with its handle in the existing
tracked-order record. Recovery keeps one background read for pending orders in
turn. Neither network wait holds the actor mutex: quotes, fills and protective
exits continue while a submission or recovery is outstanding. Both completions
use `OrderTaskReady` over the same standard `MpscFeed`; no separate strategy
result channel or HTTP client is added. Results remain in their handles for shutdown, which
awaits them alongside its retry timer after event subscriptions stop.
Before evaluating an entry or exit, the actor also consumes already ready
submission/recovery results, even if their notifications are queued behind a
quote. It never waits for outstanding requests there. Late notifications
for an order already completed by WS/REST are ignored; they cannot reopen it
or change another order. Confirmed terminal status aborts that order's obsolete
submission/recovery task. Sending or completing a close does not discard another
order or its read. A full WS confirmation or normal cancel without observed
gaps cancels only that order's obsolete recovery. After an observed gap, recovery
requires terminal status plus all that order's fills; a separately indexed
account position is not an additional exit prerequisite. The first recovered
entry fill uses its execution timestamp to preserve H.

Bullet places, cancels, amends and cancels market orders through the existing
SDK `ManagedWebsocket`. The broker uses `Transaction::builder` for signing and
passes the SDK command to the two-socket private muxer. Each command is queued
on one socket; replies remain associated with that socket across route changes.
A request ID maps each RPC reply to its waiting broker call; acknowledgements
never update inventory. An outstanding entry RPC does not block a reduce-only
close or market/private messages. Disconnected/ambiguous results retain the
existing recovery behavior. The SDK may replay identical signed bytes after a
send failure; the adapter never re-signs an unknown transaction or retries it
through REST. Only an explicit `InvalidSignature` rejection without an intervening
disconnect refreshes the SDK schema and re-signs once. Errors on requests that
spanned a reconnect remain unknown: the first delivery may have executed.
A `dropped` status without a reason remains unknown,
because it may describe a duplicate of an already executed transaction.
No transport setting or new request timeout is added. The existing ten-second
shutdown deadline remains; REST is used for account queries and recovery.

Pagination uses the existing SDK HTTP client/connection pool through its public
`ClientInfo` accessor. There is no second HTTP client. The generated history
methods omit cursor support, so paginated reads retain a small adapter helper.
Open orders are queried only to resolve a missing venue order ID. REST and WS
fills share trade IDs to prevent double counting.
The entire recovery has a five-second deadline, with no additional three-second
timeout per HTTP request.

SDK 0.0.43 only logs internal event-queue overflow; it exposes no gap event for
that case. An otherwise silent loss of the final fill before cancel cannot be
detected here. No extra WebSocket or log interception is added for this case.

An empty open-order list never proves rejection. If the submission response
and all WS messages are lost and an IOC produces no fills, its venue ID may
remain unknown: closed-order history does not guarantee a client ID. An unknown
entry stays pending with recovery attempts; no duplicate entry.

For a protective exit whose RPC is still unanswered and whose history is empty,
recovery also reads the signing credential's `address_state` through the SDK.
The exact `Window` nonce comes from the already signed request retained by the
existing WS pending map. Bullet's replay window spans 1000 nonces; once its end
is more than 1000 beyond the original nonce, that submission cannot execute
in the future. This is a protocol bound, **not a wall-clock timeout**. Only when
all entry fills are known and accounted fills already net to zero can the actor
retire this obsolete protective exit and allow a subsequent entry. Expiry does
not fabricate a rejected order or prove there were no past fills. Unknown
entries and non-flat inventory keep recovering; protective closes remain allowed.
This read runs in the existing recovery task, not in quote/order dispatch, and
normal IOC completion adds no request. See Bullet's
[transaction fields](https://tradingapi.bullet.xyz/docs/print.html#transaction-fields).

Process-restart recovery is
not implemented. Live startup requires a flat account. Graceful shutdown
attempts recovery/reduce-only closing for up to 10 seconds and verifies the
account is flat. There is no cancel-all request before closing: this strategy
uses only IOC orders, which cannot leave a resting remainder. A lagging position snapshot or failed position read is retried
within that same deadline, without resubmitting a confirmed close. Failure to
confirm flat by the deadline is returned as an error through the harness and
CLI, after all actors have had a chance to clean up. No new timing config fields.

## Dry run and tests

`dry_run = true` uses the authenticated connection and records the same market
streams, but sends no order/cancel/account-mutating requests. Public book levels
simulate partial IOC execution at visible prices within the limit; unfilled
quantity is cancelled immediately. The simulator produces the existing
`ExecutionUpdate` (fill plus terminal status) and applies the same fill-ID
accounting, first-fill deadline and order completion logic as live executions.
It does not mutate inventory through a separate paper path.
Only the visible BBO level is simulated: deeper fills available within the
allowance are not assumed. A simulated fill consumes the cached BBO quantity
until a new quote arrives, so a partial exit cannot repeatedly reuse it.
Shutdown with insufficient BBO liquidity reports the unfilled paper position
as an error; it neither invents liquidity nor waits for market events after
subscriptions stop. Depth deltas remain in the recording. This checks signal/state logic, not venue latency,
queue position, fees or actual execution authorization; paper PnL is gross.

Deterministic tests cover time-weighted basis, causal impulse, strict executable
edge thresholds, fixed fair-based long/short limits, signed R, trailing extremes
from confirmation (including recovery), quote age including queue delay, partial/full/empty IOC,
re-entry, frozen basis, stop, elapsed-time exit, partial close retries,
ambiguous submission, concurrent protective exits, late entry/close fills,
WS/REST deduplication, recovery fairness, nonblocking submission with early WS
fills/protective closes, late acknowledgements, shared paper/live accounting,
shutdown error propagation and dry-run account isolation. The live smoke run checks
mainnet subscriptions and data recording without sending real orders.
