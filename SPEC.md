# Polymarket Trading Bot — End-to-End Pipeline (v2)
### Architecture: Oracle + Rule-Based Executor + LLM Circuit Breaker
### Scope: Binary BTC Markets Only (e.g. "Will BTC > $100k by X date?")

> **Why this architecture?**
> DRL requires millions of environment steps to converge. Our qualifying BTC binary market
> universe gives us a few thousand historical resolution events at best — not enough for
> a robust RL policy. The edge we're capturing (oracle probability diverging from market
> mid) is well-defined and human-articulable. You don't train a laser to cut an apple
> when a knife works and ships in a tenth of the time.
>
> The three-component architecture below maps cleanly to the actual problem:
> - **Oracle** — finds the edge (is the market mispriced?)
> - **Rule-Based Executor** — captures the edge (when and how much to trade)
> - **LLM Circuit Breaker** — protects the edge (pause when assumptions break down)

---

## System Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                    LIVE DATA FEEDS                       │
│   Polymarket WebSocket (L2)  │  Deribit API (BTC IV)    │
└──────────────────┬───────────────────────┬──────────────┘
                   │                       │
                   ▼                       ▼
┌──────────────────────────────────────────────────────────┐
│              MODULE A — THE ORACLE                        │
│   XGBoost → Sigmoid → Calibrated P(YES) probability      │
└──────────────────────────┬───────────────────────────────┘
                           │  oracle_prob
                           ▼
┌──────────────────────────────────────────────────────────┐
│           MODULE B — RULE-BASED EXECUTOR                  │
│   edge = oracle_prob − market_mid                         │
│   if edge > threshold AND spread < ceiling                │
│       AND gas_ratio < ceiling: → size via Kelly → order   │
└──────────┬───────────────────────────────────────────────┘
           │  PROCEED / PAUSE signal
           ▲
┌──────────┴───────────────────────────────────────────────┐
│           MODULE C — LLM CIRCUIT BREAKER                  │
│   Scans: news, dispute history, social sentiment          │
│   Output: PROCEED | PAUSE (+ reason string logged)        │
└──────────────────────────────────────────────────────────┘
```

The circuit breaker sits **between the oracle and the executor**. It does not make
trading decisions. It does one job: detect qualitative regime breaks the quantitative
pipeline cannot see, and pause execution when it finds one.

---

## Phase 0: Market Qualification Filter

Unchanged from the previous design. The `is_qualified_btc_market()` function with
`FilterConfig` and `FilterResult` dataclasses is already production-ready.

**Qualifying criteria:**

| Criterion | Rule |
|---|---|
| Market type | Binary YES/NO only |
| Underlying | BTC only |
| Strike type | Absolute price level ($100k, $110k) — no percentage-move markets |
| Time-to-expiry | 3–30 days at time of entry |
| Min daily volume | $10,000 USDC |

Applied as a filter function at every ingestion, training, and live execution step.
Pass 1 (whitelist builder) runs before any heavy data work to pre-qualify market IDs.

---

## Phase 1: Data Engineering & Infrastructure

### Step 1.1 — Historical Data Acquisition (Two-Pass Ingestion)

**Pass 1 — Whitelist Builder:**
Query Polymarket metadata (API or `pmxt` metadata files) and run each market through
the Phase 0 filter. Output: `qualified_markets_whitelist.csv`. This runs first, before
any tick data is touched.

**Pass 2 — Heavy Lift Ingestion:**
Stream the `pmxt` archive using Polars lazy evaluation. Filter against the whitelist
via predicate pushdown before any deeper parsing. Flatten the top-5 L2 book levels
into a strict, flat schema during ingestion — not at training time.

**Target Parquet schema:** flat, strongly typed, partitioned by `(market_id, date)`,
compressed with `zstd`. DuckDB sits as a query layer on top for cross-partition
metadata queries. PostgreSQL handles market metadata, resolution records, and OMS
state. Tick data never goes into PostgreSQL.

**Storage layers:**

| Layer | Tool | What lives here |
|---|---|---|
| Cold tick storage | Partitioned Parquet (Polars) | L2 snapshots, post-QA |
| Query coordination | DuckDB | Cross-partition queries, training window assembly |
| Metadata + state | PostgreSQL | Market metadata, resolutions, OMS state, PnL |

### Step 1.2 — External Data Pipeline (Deribit BTC IV)

Pull and store:
- BTC at-the-money IV for expiries matching market TTE windows
- IV term structure slope (front vs. back month)
- 25-delta skew

Store as time-series Parquet partitioned by date. Timestamp every record at the
**publication time**, not the request time — this distinction is critical for Step 1.3.

### Step 1.3 — Data Fusion (Polars, Strict Knowledge Cutoff)

Align Deribit IV (~1min resolution) with Polymarket L2 tick data (high-frequency).

**Interpolation strategy — decide once, apply everywhere:**
- Use **forward-fill only** (never linear interpolation near resolution events)
- Forward-fill means every feature value is the last *known* value at that timestamp
- Linear interpolation implies knowledge of future IV values — this is leakage

**Strict knowledge cutoff rule:** every feature value in a training row must have a
source timestamp strictly less than the prediction timestamp. This is enforced by the
leakage unit test in Step 1.4, not just by convention.

### Step 1.4 — Feature Leakage Prevention (CI Unit Test)

Automated test that runs on every training dataset build and breaks the build if
violated. Tests a random sample of rows and asserts every external feature timestamp
is strictly before the prediction timestamp. One silent leakage bug here ruins the
entire oracle — make this test impossible to skip.

### Step 1.5 — Resolution Metadata Pipeline

Separate ingestion for Polymarket resolution outcomes:
- Final YES/NO per market
- Actual resolution timestamp (not scheduled expiry)
- Disputed/early/walkover markets flagged and excluded from training

### Step 1.6 — Data Quality Validation

QA pass outputs a per-market rejection report:
- Crossed books (bid ≥ ask): drop
- Stale snapshots: detect and discard
- Tick gaps over N seconds: exclude from training windows

High rejection rate on a specific market → exclude that market from training entirely.

---

## Phase 2: Module A — The Oracle (Supervised Learning)

The oracle's job is one thing: **output a well-calibrated P(YES) for a qualifying
BTC binary market**. Single Sigmoid output. No Softmax, no multi-outcome logic.

### Step 2.1 — Feature Set

| Feature | Source | Notes |
|---|---|---|
| BTC ATM IV | Deribit | Nearest expiry matching market TTE |
| IV term structure slope | Deribit | Contango vs. backwardation regime signal |
| BTC spot momentum | Exchange API | Rolling returns at 1h, 4h, 24h |
| Market mid-price | Polymarket L2 | Crowd-wisdom prior — do not ignore this |
| Time-to-expiry (TTE) | Market metadata | Both raw days and log(TTE) |
| Distance-to-strike | Derived | (BTC spot − strike) / strike, normalized |

### Step 2.2 — Model

**XGBoost classifier** with Sigmoid output. This is the right tool — fast to iterate,
interpretable feature importances, handles tabular data well, and doesn't require
the volume of data that neural models need.

Do not upgrade to a neural model until XGBoost plateaus and feature importance
analysis points to non-linear interactions you cannot engineer manually.

### Step 2.3 — Evaluation & Go/No-Go Threshold

- **Brier Score** — primary metric (lower = better calibrated)
- **Log-Loss** — secondary metric
- Evaluate on held-out **resolved markets only**
- **Define the go/no-go threshold now:** oracle must hit Brier Score ≤ 0.10 on
  the held-out set before any live deployment. Write this down. Do not adjust it
  retroactively when you're eager to ship.

### Step 2.4 — Calibration & Weekly Recalibration

At training time: Platt Scaling or Isotonic Regression on held-out validation set
to map raw XGBoost outputs to true probabilities.

In production: **weekly recalibration cron job** that re-fits the calibration layer
on the most recently resolved BTC binary markets. Log Brier Score by week — a rising
trend is the early warning signal to retrain the full model, not just recalibrate.

---

## Phase 3: Module B — Rule-Based Execution Engine

This replaces the entire DRL stack. The edge we're capturing is explicitly
human-articulable — there is no reason to train an agent to rediscover it.

### Step 3.1 — The Core Execution Rule

```python
def should_execute(oracle_prob, market_mid, spread, gas_cost, trade_size, config):
    
    edge = oracle_prob - market_mid  # Positive = market underpricing YES
    
    # Gate 1: Minimum edge threshold (must exceed transaction costs + margin)
    if abs(edge) < config.min_edge_threshold:
        return False, "insufficient_edge"

    # Gate 2: Spread ceiling (wide spread eats the edge)
    if spread > config.max_spread:
        return False, "spread_too_wide"

    # Gate 3: Gas cost ceiling (dynamic — fetched live before each check)
    if (gas_cost / trade_size) > config.max_gas_ratio:
        return False, "gas_too_expensive"

    return True, "execute"
```

All thresholds live in a `ExecutionConfig` dataclass — never hardcoded. Every
gate decision is logged with its reason string for post-hoc analysis.

### Step 3.2 — Position Sizing (Fractional Kelly)

```python
def kelly_size(edge, oracle_prob, bankroll, config):
    # Full Kelly: f = edge / (1 - oracle_prob) for YES bets
    # Fractional Kelly: scale down to reduce variance
    p = oracle_prob
    q = 1 - p
    full_kelly = (p * (1 + edge) - 1) / edge  # simplified for binary
    fractional = full_kelly * config.kelly_fraction  # e.g. 0.25 = quarter-Kelly
    
    size = fractional * bankroll
    
    # Hard size caps regardless of Kelly output
    size = min(size, config.max_position_usdc)
    size = max(size, config.min_position_usdc)
    
    return size
```

Start at **quarter-Kelly** (0.25 fraction) and only increase after sustained
live profitability. Full Kelly is theoretically optimal but brutal in practice —
any oracle miscalibration translates directly into oversized losing positions.

### Step 3.3 — Order Management System (OMS)

Track every resting limit order with:

| Field | Purpose |
|---|---|
| `order_id` | Deduplication and tx hash checking |
| `submitted_at` | Age tracking |
| `intended_size` | Original target USDC |
| `filled_size` | Cumulative fills |
| `fill_ratio` | `filled_size / intended_size` |
| `status` | PENDING / PARTIAL / FILLED / CANCELLED |
| `mid_at_submission` | Adverse selection baseline |

**Ghost order cancellation logic — trigger on either condition:**
1. `fill_ratio < 0.2` AND order age > N seconds → not filling, cancel and recalculate
2. Market mid has moved > M% from limit price → thesis has changed, cancel regardless

Log every cancellation with reason. A high ghost order rate is a signal that your
limit price offset is too aggressive or the market is thinner than the volume
threshold implies.

### Step 3.4 — Portfolio-Level Risk Manager

Sits between execution logic and order submission:
- **Exposure cap:** max % of wallet in open positions simultaneously
- **Drawdown circuit breaker:** if PnL drops X% from peak in rolling 24h → pause Y hours
- **Correlation guard:** max % of wallet in BTC binary markets resolving within the
  same week (they are correlated — a single BTC move affects all of them)

---

## Phase 4: Module C — LLM Circuit Breaker

The circuit breaker's job is **not** to make trading decisions. It does one thing:
detect qualitative regime breaks that the quantitative oracle cannot see, and emit
a PAUSE signal when it finds one.

### Step 4.1 — What It Monitors

Three signal sources, polled on a configurable schedule (e.g. every 15 minutes):

**News sentiment scan:**
Query a news API (NewsAPI, Bing News, or similar) for BTC-relevant keywords.
Feed headlines to the LLM with a structured prompt.

**Polymarket dispute history:**
Check the Polymarket API for any active resolution disputes on BTC markets.
A disputed resolution in any BTC market should pause trading across all BTC markets
until resolved — the resolution mechanism itself is broken.

**Social sentiment spike:**
Monitor for abnormal volume spikes on crypto social channels. Not for trading signal —
for anomaly detection. A sudden spike often precedes an information event the oracle
hasn't priced.

### Step 4.2 — The Circuit Breaker Prompt

```python
CIRCUIT_BREAKER_PROMPT = """
You are a risk monitor for an automated trading bot that trades binary BTC 
prediction markets (e.g. "Will BTC exceed $100k by date X?").

Your ONLY job is to determine if current conditions represent an anomaly that 
would invalidate the bot's pricing model. You are NOT making trading decisions.

Flag PAUSE if you detect ANY of:
- Breaking news that would cause sudden, discontinuous BTC price movement
  (exchange hacks, ETF approvals/denials, major regulatory announcements,
   protocol failures, geopolitical escalation)
- Evidence that a Polymarket market may resolve incorrectly or be disputed
- Clear evidence of market manipulation or coordinated trading

If none of the above: output PROCEED.

OUTPUT FORMAT (strict JSON, nothing else):
{
  "signal": "PROCEED" | "PAUSE",
  "reason": "one sentence explanation",
  "confidence": "HIGH" | "MEDIUM" | "LOW"
}

Current headlines and context:
{context}
"""
```

### Step 4.3 — Signal Handling

```python
def check_circuit_breaker(news_context: str) -> CircuitBreakerResult:
    response = call_llm(CIRCUIT_BREAKER_PROMPT.format(context=news_context))
    result = parse_json(response)  # Strict parse — malformed output = PAUSE
    
    # Conservative default: if anything goes wrong, pause
    if result is None:
        return CircuitBreakerResult(signal="PAUSE", reason="LLM parse failure", 
                                    confidence="HIGH")
    
    log_circuit_breaker_event(result)  # Always log, regardless of signal
    return result
```

**Key design decisions:**
- Malformed LLM output defaults to PAUSE, never PROCEED — fail safe
- LOW confidence PAUSE still pauses — the cost of missing a trade is low,
  the cost of trading through an exchange hack is high
- Every signal is logged with the full context that generated it — this is your
  audit trail for tuning the prompt over time
- PAUSE duration: configurable, but start at 2 hours and require a manual review
  to resume if the LLM flags HIGH confidence

### Step 4.4 — What the LLM is NOT Doing

Be explicit about this boundary or it will creep:
- It does not adjust position sizes
- It does not predict market direction
- It does not override the oracle's probability estimate
- It does not decide which markets to enter
- It reads text and outputs one bit of information: stop or go

---

## Phase 5: Shadow Mode & Validation

### Step 5.1 — WebSocket Integration
Connect to the live Polymarket WebSocket for qualifying BTC binary markets.
Apply Phase 0 filter at subscription time — only subscribe to markets that pass.

### Step 5.2 — Shadow Execution
Full system runs — oracle, circuit breaker, execution rules, OMS logic — but instead
of signing transactions, every intended action is logged to a structured file:

```json
{
  "timestamp": "2024-01-15T12:00:00Z",
  "market_id": "abc123",
  "oracle_prob": 0.72,
  "market_mid": 0.61,
  "edge": 0.11,
  "circuit_breaker": "PROCEED",
  "execution_decision": "BUY_YES",
  "intended_size_usdc": 250.0,
  "intended_limit_price": 0.63,
  "rejection_reason": null
}
```

### Step 5.3 — Validation Criteria (Resolution-Based, Not Time-Based)

Do not exit shadow mode on a schedule. Exit when all of the following are met:

| Criterion | Threshold |
|---|---|
| Resolved BTC binary markets observed | ≥ 15 |
| Oracle Brier Score on live resolved markets | ≤ 0.10 |
| Circuit breaker false positive rate | Qualitatively reviewed — not just counted |
| Ghost order rate in shadow OMS | < 20% of submitted orders |
| Simulated PnL (mark-to-resolution) | Positive after fees |

### Step 5.4 — Slippage Audit
Compare intended fill prices to actual mid-prices at shadow execution time.
If real spread conditions are consistently worse than assumed, tune the
`max_spread` threshold in `ExecutionConfig` before going live.

---

## Phase 6: Production Deployment

### Step 6.1 — RPC Node Infrastructure
Do not use public Polygon RPC endpoints — they are rate-limited and will drop
connections under load. Since you are running on your own home server, you have
two viable options:

**Option A — Self-hosted Polygon node (preferred for long-term):**
Run a Polygon full node or light client (Erigon is the recommended client for
Polygon — lower disk footprint than the reference client). This gives you a
private, rate-limit-free local RPC endpoint at `http://localhost:8545`. The
tradeoff is initial sync time (days) and disk space (~2TB for a full Erigon node).

**Option B — Single dedicated RPC endpoint:**
If self-hosting a node is too much infra overhead right now, use a single
dedicated RPC endpoint and configure your own retry logic. The key point is
having one reliable, non-public endpoint rather than depending on shared
public infrastructure.

Regardless of which option you choose, fallback logic must check the original
tx hash before any retry — duplicate fills are the worst failure mode, not
missed trades.

### Step 6.2 — Web3 Wallet Integration
Connect funded Polygon wallet via `web3.py` against your local RPC endpoint
(localhost if self-hosting a node, or your dedicated endpoint if using Option B).

### Step 6.3 — Hard-Coded Guardrails (Non-Negotiable)
- Wallet below X USDC → kill the script
- Spread above Y% → do not execute (redundant with ExecutionConfig but hardcoded
  as a safety net independent of the execution logic)
- Ghost order count above Z simultaneously → pause new submissions
- Dynamic gas check: fetch live gas price before every tx; skip if
  `gas_cost / trade_size > ceiling`

### Step 6.4 — Home Server Setup
The rule-based executor is lightweight — this does not need a GPU or high-spec
machine. Any reasonably modern home server running Linux will handle the workload
comfortably.

**Recommended home server config:**
- OS: Ubuntu 22.04 LTS — stable, well-documented, good Python ecosystem support
- Run the bot process under `systemd` — gives you automatic restart on crash,
  structured journald logging, and clean process management without needing a
  separate process manager
- UPS (uninterruptible power supply): non-negotiable for a 24/7 trading process.
  A single power blip that kills the process mid-transaction leaves you with
  unknown order state. A basic UPS unit is cheap insurance.
- Static local IP + port forwarding if you need to reach the monitoring dashboard
  remotely, or use a lightweight reverse proxy (Caddy is the simplest option)

**The one real home server risk vs. cloud:**
Network uptime. Home ISPs go down. Configure the dead-man's switch in Step 6.5
to alert you within minutes if the bot stops heartbeating — you need to know
immediately if your connection dropped while orders are resting in the book.

### Step 6.5 — Monitoring & Observability

| Metric | Signal |
|---|---|
| Rolling PnL (hourly / daily) | Primary health |
| Edge distribution over time | Oracle drift early warning |
| Circuit breaker trigger rate | Prompt tuning signal |
| Fill rate vs. submission rate | Spread or connectivity issues |
| Ghost order rate | OMS health |
| Oracle Brier Score (weekly, live) | Recalibration trigger |
| Gas cost as % of trade size | Congestion bleed |
| RPC connection status | Node dropped or local network down |
| Dead-man's switch | Process silently died or ISP outage |

Minimum viable: structured JSON logs + a daily summary cron that emails you
the key metrics. Add Grafana when you have enough live data to make dashboards
meaningful (not before).

---

## Upgrade Path to DRL (If You Ever Need It)

After 6–12 months of live rule-based execution, you will have:
- Real fill data with adverse selection events tagged
- Real ghost order history
- Real oracle calibration drift curves
- A corpus of circuit breaker decisions and outcomes

**At that point, and only at that point**, the DRL question becomes answerable with
data rather than theory. The specific use case where RL might add value is
**execution timing optimization** — not the full trading decision, just refining
when within the market's lifecycle to enter a position given the oracle's signal.
The rule-based system remains the decision maker; RL becomes a parameter tuner.

This is not a consolation prize. This is the correct engineering sequence:
build the knife, use the knife, understand what the knife can't do, then consider
the laser.

---

## Full Component Summary

| Module | Tool | Job |
|---|---|---|
| Market filter | Python (FilterConfig) | Phase 0 gating |
| Data storage | Parquet + DuckDB + PostgreSQL | Layered by access pattern |
| Oracle | XGBoost + Platt Scaling | P(YES) probability |
| Executor | Rule-based Python | Edge capture |
| Sizer | Fractional Kelly | Position sizing |
| OMS | Python state tracker | Order lifecycle |
| Risk manager | Hard-coded rules | Portfolio-level protection |
| Circuit breaker | LLM (structured JSON output) | Qualitative anomaly detection |
| Monitoring | Structured logs + cron | Observability |
