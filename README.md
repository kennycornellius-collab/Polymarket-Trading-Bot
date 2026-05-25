# Polymarket Trading Bot

**Status: Abandoned**

---

## What this was

An automated trading bot for Polymarket's binary BTC prediction markets (e.g. "Will BTC > $100k by X date?"). The architecture had three components:

- **Oracle** — XGBoost model trained on historical 1-minute price bars to produce calibrated P(YES) probabilities
- **Rule-Based Executor** — enters positions when `oracle_prob − market_mid` exceeds a Kelly-sized edge threshold
- **LLM Circuit Breaker** — pauses execution on qualitative regime breaks (news, disputes, sentiment)

The pipeline was built phase-by-phase following a strict spec. What actually landed before abandonment:

- **Phase 0** — market qualification filter (binary BTC markets, TTE 3–30 days, $10k USDC daily volume)
- **Phase 1.1 Pass 1** — live market whitelist builder via Gamma REST API
- **Phase 1.5 / 1.5.1** — resolution metadata pipeline + incremental refresh (~10,400 labeled markets)
- **Phase 1.1 Pass 2** — 1-minute price bar ingestion from CLOB `/prices-history` (~7,000 markets, Hive-partitioned Parquet)
- **Phase 1.6** — data quality validation gate (~5,700 clean markets flagged training-eligible)

The oracle training (Phase 2) and everything after it were never started.

---

## Why I stopped

**The Polymarket API is a nightmare.** Every phase that touched a live endpoint followed the same loop: write code against the docs → run against real data at scale → discover an undocumented constraint or silent failure → patch → repeat. Some examples logged in `PROGRESS.md`:

- `volume24hr` documented as a string, actually a float (or absent entirely)
- `closedTimeMin` filter silently ignored — no error, no warning, just the full unfiltered corpus returned
- `interval=1m` on `/prices-history` returns HTTP 200 with an empty response instead of erroring
- `umaResolutionStatuses` field contains routine lifecycle states (`proposed`, `resolved`), not just disputes — the field name implies only disputes
- A hard 250,100-record Gamma pagination cap that terminates silently at the wrong time
- A 14-day CLOB window cap that only surfaces when you run against thousands of markets, not in single-market spikes

The pattern held consistently: one phase's worth of work would close one bug and open two or three more that only appear at production scale. For a solo junior developer this compounding cost was unsustainable.

**Polymarket is also now inaccessible in Indonesia** due to regulatory changes. Using it without a VPN isn't viable, which removes the ability to run live tests or actually use the system.

The project's overall scope — a full ML pipeline from raw API data through live on-chain execution — was also genuinely too large for one person to ship to a production standard.

---

## State of the code

The data pipeline (Phases 0–1.6) is complete and reasonably well-tested. The ML and execution layers (Phases 2–6) don't exist. The code here is not production-ready. Feel free to fork or learn from it.

Key files to orient yourself:

- `SPEC.md` — full architecture and phase breakdown
- `PROGRESS.md` — detailed log of every phase, including all the API bugs encountered
- `src/pmbot/` — source modules
- `tests/` — pytest test suite (run with `pytest`)
