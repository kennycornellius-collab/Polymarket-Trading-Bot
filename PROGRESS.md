# Progress Log

## 2026-04-19 — Phase 0: Market Qualification Filter

**SPEC step covered:** Phase 0 (Market Qualification Filter) — the qualifying-criteria
table and the note that the filter is applied at every ingestion, training, and live
execution step.

**Boundary convention:** All bounds are inclusive — tte_days ∈ [min_tte_days,
max_tte_days] and daily_volume_usdc ≥ min_daily_volume_usdc. The SPEC writes
"3–30 days" and "$10,000 USDC" without "strictly greater than" language; inclusive
is the natural reading and avoids silently dropping edge-case markets.

**Reason-code vocabulary (FilterResult.reasons):**
- `"wrong_market_type"` — market_type not in allowed_market_types
- `"wrong_underlying"` — underlying not in allowed_underlyings
- `"wrong_strike_type"` — strike_type not in allowed_strike_types
- `"tte_out_of_range"` — tte_days outside [min_tte_days, max_tte_days]
- `"volume_below_threshold"` — daily_volume_usdc < min_daily_volume_usdc

Codes are collected in the order above (no short-circuit); returned as a
`tuple[str, ...]`.

**Key decisions:**
- `MarketMetadata` implemented as a frozen dataclass (not TypedDict) for consistency
  with `FilterConfig` and the CLAUDE.md dataclass-for-configs convention.
- `tte_days` is `float` in `MarketMetadata` (fractional days at entry); config bounds
  remain `int` to match the SPEC table — Python compares them safely.
- `is_qualified_btc_market` accepts `config: FilterConfig = FilterConfig()` as a
  default argument; safe because `FilterConfig` is frozen with no mutable state.

**Deferred:** None. Phase 0 is self-contained. No stubs or scaffolding for later
phases were introduced.

## 2026-04-19 — Phase 1.1 Pass 1: Whitelist Builder

**SPEC step covered:** Phase 1, Step 1.1, Pass 1 — Whitelist Builder. Queries
Polymarket metadata and runs each market through the Phase 0 filter to produce
qualified_markets_whitelist.csv.

**Library change:** pmxt was not used. pmxt requires a Node.js ≥18 sidecar process
not available in this environment. The Polymarket Gamma Markets REST API
(https://gamma-api.polymarket.com) was called directly via urllib (stdlib). No new
Python dependencies added.

**Inference heuristics:**
- market_type: "binary" iff outcomes JSON decodes to exactly ["yes","no"]
  (case-insensitive). Up/Down markets → "non_binary".
- underlying: "BTC" if "btc" or "bitcoin" (case-insensitive) appears in question,
  slug, or any tag label; else "other".
- strike_type: "absolute" if title matches `\$\s*[\d,]+(?:\.\d+)?\s*[kKmM]?`;
  "percentage" if title matches `\b\d+(?:\.\d+)?\s*%` or directional verb + number;
  "unknown" otherwise. Unknown → rejected by Phase 0 filter.
- tte_days: (endDate - run_started_at).total_seconds() / 86400. Float, not int.

**Known-brittle points:**
- Gamma API field names (endDate, volume24hr, outcomes-as-JSON-string) assumed
  from documentation; verified by integration test.
- Regex patterns miss dollar-less absolute strikes ("100000 USD") and verbal
  percentage descriptions — both intentionally return "unknown" → excluded.
- volume24hr coerced from string; malformed values → "malformed_record".
- CRITICAL: Gamma API field names are verified ONLY by the opt-in integration
  test. If Polymarket renames volume24hr, endDate, or the outcomes-JSON-string
  convention, unit tests will still pass but the production script will write
  garbage. Run `pytest -m integration` manually before each weekly whitelist
  rebuild until a startup health check is added in a later phase.
- FIX: Cloudflare on gamma-api.polymarket.com returned HTTP 403 for the default
  Python-urllib/3.12 user agent. Fixed by sending an honest `User-Agent` header
  (`pmbot/0.1 (+https://github.com/<placeholder>)`) via `urllib.request.Request`.
  Added `user_agent` field to `WhitelistConfig` and a test asserting the header
  is present on every request.

### 2026-04-19 schema corrections (three bugs fixed via smoke test)

**Bug 1 — volume24hr is optional and numeric (not a string).** Some live records
lack `volume24hr` entirely; others that have it return a JSON number, not a string.
Fixed: marked as `NotRequired[float]`; adapter uses `record.get("volume24hr") or 0.0`
so missing volume → 0.0 → filter rejects as `volume_below_threshold` (correct) instead
of crashing as `malformed_record` (wrong).

**Bug 2 — tags can be null.** Live API returns `null` for `tags` on some records,
not just absent/list. Fixed: updated TypedDict to `NotRequired[list[dict[str, str]] | None]`;
`infer_underlying` already used `record.get("tags") or []` which handles None correctly.

**Bug 3 — integration test was a dead canary.** Rewrote to: (a) assert `volume24hr` is
numeric when present, (b) assert `tags` is None-or-list when present, (c) run the adapter
on all 50 records and assert ≥5 succeed, giving it a chance to catch type errors.

**Confirmed field names from live data** (48,600 markets, 198s run):
- Required: `id` (str), `question` (str), `slug` (str), `outcomes` (JSON str), `endDate` (str, but absent on ~350 records → currently `malformed_record`)
- Optional numeric: `volume24hr` (float), `volume`, `volume1wk`, `volume1mo`, `volumeNum`, `liquidity`, `bestBid`, `bestAsk`
- Optional other: `tags` (list | null), `clobTokenIds`, `active`, `closed`

**Additional fields available for future phases** (noted, not used here):
`volume`, `volume1wk`, `volume1mo`, `volumeNum` — useful for Pass 2 / Phase 2 training data;
`liquidity`, `bestBid`, `bestAsk` — useful for executor spread checks in Phase 3;
`clobTokenIds` — needed for CLOB order placement in Phase 6.

**Live smoke test result:** 15 qualified / 48,600 seen. CSV written to
`data/whitelist/qualified_markets_whitelist.csv`. Rejection leaders: `wrong_underlying`
(47,462), `volume_below_threshold` (47,120), `wrong_strike_type` (45,424),
`tte_out_of_range` (24,641), `wrong_market_type` (18,018).

**Deferred from this commit:** `endDate` is absent on ~350 records (currently
`malformed_record`). Likely these are perpetual or group markets without a fixed
resolution date. Should be marked `NotRequired` with a `None`/absent-safe TTE
computation in a follow-up fix.

**Deferred:**
- Loading WhitelistConfig from configs/whitelist.toml (Phase 1+).
- Resolved-market whitelist construction for historical training data (Phase 1.5).
- Volume/TTE proxies for historical/closed markets — out of scope for Pass 1.

## 2026-04-24 — Phase 1.5: Resolution Metadata Pipeline

**SPEC step covered:** Phase 1, Step 1.5 — Resolution Metadata Pipeline. Fetches all
closed Polymarket markets, filters to BTC binary absolute-strike shape, and produces
`data/resolutions/resolved_markets.csv` with outcome, resolution timestamp, and
data-quality flags. This CSV is the ground-truth label set for oracle training (Phase 2).

**New file:** `src/pmbot/phase1_data/resolutions.py`

**Empirical findings (1,000 closed records, 2026-04-24):**
- `outcomePrices` is NOT clean `["1","0"]`/`["0","1"]`. Live markets show high-precision
  floats (e.g. `["0.9999998374...", "0.0000001625..."]`) requiring a dominance threshold.
  Three cases: clean (one side ≥ 0.99), canceled (`["0","0"]`, sum ≈ 0 → INVALID), and
  ambiguous (sum ≈ 1 but neither side dominates → UNKNOWN).
- `closedTime` format: `'YYYY-MM-DD HH:MM:SS+00'` (space separator, `+00` suffix).
  Normalization: replace space→T, `+00`→`+00:00`. Do NOT use `updatedAt` — it reflects
  administrative write time, not resolution time.
- `umaResolutionStatuses` always `'[]'` in sample; `disputed` predicate is correct and
  future-safe.
- `closed=true` is honored server-side (no client-side filtering needed).
- BTC binary absolute-strike market density in closed records: ~0.8% (8 of first 1,000
  closed records). Early pages are dominated by 2020-era scalar/non-BTC markets.
- 20% of sample had `closedTime` more than 24h before `endDate` (resolved early).
- `volumeNum` present on all inspected records; `volume` is a string fallback.

**Flag vocabulary (alphabetical, pipe-delimited; "" = training-eligible):**

| Flag | Predicate |
|---|---|
| `ambiguous_resolution` | prices sum ≈ 1 but neither side ≥ dominance_threshold |
| `disputed` | `bool(json.loads(umaResolutionStatuses or "[]"))` |
| `invalid_resolution` | `abs(p0+p1 - 1.0) > sum_tolerance` (catches `["0","0"]`) |
| `malformed_prices` | JSON parse failure or list length ≠ 2 |
| `resolved_early` | `closedTime <= endDate - 24h` (inclusive at exactly tolerance) |
| `walkover` | `volume_lifetime_usdc < 1000.0` |

Price flags are mutually exclusive; all others are independent.

**Walkover threshold: $1,000 USDC.** Justified: p10=715 in live sample; excludes truly
anemic tail without cutting into median market ($24k). Tunable in `ResolutionConfig`.

**`resolved_early` boundary:** `<=` (inclusive). Markets closed exactly at the tolerance
boundary are flagged.

**Volume fallback:** `volumeNum` → `volume` (str→float coercion) → 0.0. WARNING logged
on fallback past `volumeNum`. No separate `missing_volume` flag — `walkover` already
excludes 0.0-volume records from training.

**`is_btc_binary_shape`:** Reuses Phase 0 inference (`infer_market_type`, `infer_underlying`,
`infer_strike_type`) via `cast(GammaMarketRecord, record)`. Drops `volume24hr` and TTE
gates (meaningless for closed markets). Named distinctly from `is_qualified_btc_market`.

**Integration test calibration:**
- Scans up to 1,000 closed records; target ≥ 6 BTC binary records (empirically found 8).
- Threshold for clean outcomes (YES/NO) set at 70% (not 80%) because the first-1,000
  cohort is early-era Polymarket with elevated canceled-market rate (~25% vs ~6% in
  broader sample).

**Deferred:**
- PostgreSQL persistence — SPEC Step 1.1 defers this to a dedicated infra phase.
- On-chain UMA authoritative timestamps — `closedTime` is a proxy (minutes-to-hours
  drift from true UMA finalization). Revisit in Phase 6 when web3.py is available.
- Shared HTTP pagination helper with `whitelist.py` — known DRY violation; deferred
  until a third caller justifies a shared `_fetch_page` abstraction.
- Non-BTC markets — out of scope per SPEC.

### 2026-04-25 — Phase 1.5 post-commit fix: 422 pagination cap + checkpoint writes

**Bug:** Live smoke run crashed at offset ~250,100 with HTTP 422 Unprocessable Entity.
The original "4xx raises immediately" policy treated 422 as a fatal error, losing ~25
minutes of in-memory records that had never been flushed to disk.

**Fix 1 — 422 is end-of-data, not an error.** `_fetch_closed_markets_page` now intercepts
HTTP 422 before the generic 4xx re-raise, logs INFO "Gamma pagination cap reached at
offset=N — stopping cleanly", and returns `[]`. `build_resolution_whitelist`'s existing
`if not page: break` naturally terminates. No retries on 422; all other 4xx still raise.

**Fix 2 — Periodic checkpoint writes.** `build_resolution_whitelist` writes accumulated
records to `<output>.partial.csv` every 50 pages. On clean completion the partial is
promoted to the final path via `Path.replace()` (atomic on Windows, overwrites if
exists). A crash now loses at most 50 pages (~5,000 markets) of work instead of the
entire run.

**Gamma corpus size:** The 422 at offset ~250,100 implies approximately 250,000 total
closed markets in the Gamma API corpus at the time of the smoke run (2026-04-25).

### 2026-04-25 — Phase 1.5 post-commit fix: disputed predicate over-firing

**Bug:** The `disputed` flag fired on ~96% of records (6,592 of 6,876) instead of the
expected ~1%. Root cause: the original 50-market planning sample showed
`umaResolutionStatuses` always `'[]'`, but a 2,500-market sweep across five offset bands
found the field is populated with UMA lifecycle states, not exclusively disputes:

  | Status | Count | Meaning |
  |---|---|---|
  | `'proposed'` | 1,705 | Routine: proposer submitted outcome |
  | `''` (empty list) | 810 | No UMA action |
  | `'resolved'` | 667 | Routine: finalized cleanly |
  | `'disputed'` | 16 | Actual dispute (~0.6%) |

The original predicate `bool(json.loads(uma_raw))` treated any non-empty list as a dispute.

**Fix:** Changed to membership check against `config.dispute_status_values`
(`frozenset({"disputed"})` by default, extensible). New predicate:
```python
set(json.loads(uma_raw)) & config.dispute_status_values
```
Malformed JSON now logs WARNING and defaults to `disputed=False` (previously silently
skipped).

**Dead-canary lesson (second occurrence in Phase 1 work):** The integration test only
asserted field presence (is `umaResolutionStatuses` parseable JSON?), not predicate
semantics (does a `'proposed'`-only record avoid the `disputed` flag?). Added a new
integration test (`test_uma_status_predicate_semantics`) that:
- Sweeps 600 records across 3 offset bands (0, 50k, 100k).
- Asserts no UMA status values outside `{'proposed', 'resolved', 'disputed'}`.
- Asserts `disputed`-flag rate < 5%.
- Asserts at least one `'proposed'`-only record does NOT carry the flag (negative case).

**Rule:** Integration tests must exercise predicate semantics with known-positive AND
known-negative cases — field-shape assertions alone cannot catch logic bugs.

### 2026-04-25 — Phase 1.5 post-commit fix: mid-response TimeoutError not retried

**Bug:** `resp.read()` inside the `with urlopen(...) as resp:` block can raise
`TimeoutError` directly (an `OSError` subclass) when the SSL/socket layer times out
mid-response. The retry handler caught `urllib.error.URLError` (connection-phase errors)
but not `TimeoutError` (read-phase error), so a single transient timeout crashed the
entire ~6-minute smoke run.

**Fix:** Changed `except urllib.error.URLError` to `except OSError` in
`_fetch_closed_markets_page`. Since `urllib.error.URLError` inherits from `OSError`,
this change is a strict superset — all previously caught errors are still caught, and
`TimeoutError` from `resp.read()` is now retried with the same exponential backoff.
`urllib.error.HTTPError` is caught first (more specific), so HTTP-level errors are
unaffected.

**Smoke run final results (2026-04-25, post-fix):**
- Total BTC binary shape records: **7,658**
- Outcomes: YES=2,327 (30%), NO=5,329 (70%), INVALID=2
- Flags: walkover=2,335 (30%), resolved_early=206 (3%), disputed=17 (0.2%), invalid_resolution=2
- **Training-eligible: 5,106** (66% of corpus)
- disputed rate 0.2% confirms predicate fix (was 96% pre-fix)
## 2026-04-25 — Design pivot: bar-level ingestion (was tick-level)

**SPEC steps affected:** Phase 1, Step 1.1 Pass 2 (heavy-lift ingestion); Step 1.3
(data fusion); Step 1.4 (leakage test framing); Phase 2, Step 2.1 (oracle feature
set). All updated in the SPEC.md edit pass dated today.

**Decision:** Replace tick-level orderbook capture with 1-minute price-bar ingestion
via Polymarket's CLOB `/prices-history` endpoint (`fidelity=1`). Schema is `{t, p}`
— Unix timestamp and price per bar. Not OHLCV: no high, no low, no open, no close,
no per-bar volume.

**Empirical confirmation (2026-04-25 spike):**
- `/prices-history?market=<token_id>&fidelity=1&startTs=...&endTs=...` returns
  1-minute bars cleanly on recent high-volume markets.
- Endpoint is auth-free; no API key required for historical backfill.
- A single response covers a 7-day window without pagination — 10,000+ records
  returned in one shot.
- Per-request latency ~500ms. Sequential backfill across 5,106 training-eligible
  markets ~45 minutes; 10–15 minutes with concurrency.

**Rationale (in order of strength):**

a) **Signal scale.** TTE is 3–30 days. The oracle predicts a multi-day binary
   outcome. Sub-minute price action is noise relative to that signal — there is
   no edge to capture at tick resolution that the oracle wouldn't smooth away.

b) **Storage.** ~1 GB total in compressed Parquet (5,106 markets × ~20,000 bars
   × ~10 bytes/row). Tick-level capture would have been hundreds of GB.
   Order-of-magnitude reduction.

c) **Iteration speed.** Prior DRL-bot project on tick data ran hours per training
   cycle, which made debugging and feature changes painful. XGBoost on bar data
   runs in minutes — feature changes can be tested same-day.

d) **Sufficiency.** Every feature in the SPEC's Phase 2.1 list is computable from
   a price-only series: price level, returns over windows, realized vol from
   squared returns, distance-to-strike, TTE interactions. Nothing in scope
   requires sub-minute or L2 data.

e) **No auth dependency for historical backfill.** `/prices-history` is open.
   `/trades` requires `CLOB_API_KEY` (free, but requires Polygon wallet setup).
   Pulling that into Phase 1 expands its dependency surface for no Phase 1
   benefit. CLOB_API_KEY stays scoped to Phase 6 where it is actually required.

**Foreclosed by the bar-data schema:** order book imbalance (no L2),
trade-by-trade flow, sweep depth, sub-minute momentum, OHLC analysis (true
range, wick patterns), per-bar volume features. None of these are listed as
SPEC features — the foreclosure is accepted, not a regression.

**Reversibility:** A future tick-level ingestion phase remains possible if the
rule-based executor demonstrates that microstructure features would meaningfully
improve the oracle. Two non-trivial blockers: Polymarket's tick-data retention
horizon is unverified, and `/trades` requires API key. Future option, not a
current dependency.

**Three dead-canary findings worth documenting (additions to the running Phase 1
list — total now five across this phase):**

1) **`interval=1m` on `/prices-history` silently returns empty.** The `interval`
   parameter accepts `1h`, `1d`, `1w`, `1M` only — `M` means **month** in their
   schema, not minute. Passing `1m` gets a 200 OK with empty `history`. Bad
   parameter, no error. Use `fidelity` (minutes) for sub-hour granularity,
   never `interval`.

2) **Sample bias (third occurrence in Phase 1).** Initial 5-record spike against
   pre-2021 markets returned all empty for `/prices-history`, leading to a false
   initial conclusion that the endpoint was broken. Recent high-volume markets
   returned 10,000+ records cleanly. Default-sort sampling is unreliable for any
   Polymarket schema discovery — the early-page records are 2020-era and behave
   differently from current ones. **Pattern:** any future endpoint discovery in
   this repo must sample across offset bands, not from offset 0.

3) **`/trades` returns HTTP 401 "Unauthorized/Invalid api key" when called
   without auth, even when other parameters are wrong.** Polymarket's auth
   middleware fires before input validation. Implication: when probing `/trades`
   semantics in the future, get the auth right first — otherwise every parameter
   error masquerades as an auth error.

**Files modified in this pivot:**
- `SPEC.md` — Phase 1.1 Pass 2 rewritten, Step 1.3 fusion language updated, Step
  1.6 QA criteria converted from L2-staleness to bar-gap rules, Step 2.1 feature
  table replaced with explicit in-scope / out-of-scope blocks, new Design
  Decisions section added before Full Component Summary.
- `CLAUDE.md` — Postgres prohibition reworded from "tick data" to
  "bar/time-series data" in the Stack section and the "What NOT to Do" list.

**Deferred:** Phase 1.1 Pass 2 implementation (the actual `/prices-history`
ingestion runner) — blocked on Phase 1.5.1 landing first; see next entry.

## 2026-04-25 — Coverage gap discovered; Phase 1.5.1 introduced

**SPEC step affected:** New Phase 1.5.1 sub-phase under Phase 1.5 — Incremental
Resolution Refresh.

**Discovery:** Pre-Pass-2 verification of `data/resolutions/resolved_markets.csv`
revealed the most recent ~3 months of resolutions (Feb–April 2026) are absent.
By-month histogram of `resolved_at` shows a sharp cliff after January 2026.
Phase 1.5's smoke-run total of 7,658 records (5,106 training-eligible) is correct
for what the run could see, but is not a complete picture of the closed-market
corpus.

**Root cause:** Gamma paginates `/markets?closed=true` oldest-to-newest by ID,
with a hard offset cap at 250,100 (the same cap that produced the HTTP 422
documented in the Phase 1.5 post-commit fix). Polymarket's archive exceeds the
cap. Phase 1.5's pagination terminated cleanly at the cap — the 422 fix worked
as intended — but never reached markets with offsets > 250,100, which are the
most recent ones. Phase 1.5 walked the available archive correctly given offset
pagination; it could not have discovered records that pagination structurally
excludes.

**Spike findings (2026-04-25):**
- **`end_date_min` works.** Filters records where `endDate >= since_date`.
  Filtered queries fit comfortably under the 250,100 offset cap.
- **`closedTimeMin` is silently ignored.** Returns the unfiltered set — no
  error, no warning. Yet another silent-fallback dead canary on the Gamma API.
- **No working sort/order parameter found.** `order=desc`, `order_by=...`, and
  `ascending=false` all either error or are silently ignored.
  Reverse-chronological pagination is not available; filtered forward
  pagination is the only path.

**Phase 1.5.1 design:**
- **New module:** `src/pmbot/phase1_data/resolutions_refresh.py`.
- **Reuses Phase 1.5 primitives** — `build_resolution_record`,
  `is_btc_binary_shape`, `ResolutionConfig`. No rewrites; the schema and
  predicate logic from Phase 1.5 are correct, only the traversal strategy
  changes.
- **Logic:** load existing CSV → derive `since_date` from `max(resolved_at) −
  7-day overlap buffer` (CLI override allowed) → paginate
  `/markets?closed=true&end_date_min=<since_date>` → apply
  `is_btc_binary_shape` filter → build records via `build_resolution_record`
  → merge: append new, dedupe by `market_id` (keep newest write) → atomic
  write-temp-then-rename.
- **Operational shape:** designed for both one-time gap-plugging now AND
  recurring weekly cron operation. New markets resolve daily; the corpus
  stales without periodic refresh.

**Why a separate sub-phase rather than a Phase 1.5 amendment:** Phase 1.5
walked the available archive correctly given offset pagination. The growth of
the archive past the cap is a different operational problem and requires a
different traversal (filtered, anchored on `end_date_min`). Conflating the two
muddies the audit trail and obscures the fact that Phase 1.5's design was
sound for what it could see. Phase 1.5.1 stacks on top — it does not replace.

**Deferred until Phase 1.5.1 lands:** Phase 1.1 Pass 2 (price-bar ingestion).
No point ingesting price history for an incomplete label set — the missing 3
months are the most recent and likely highest-quality training markets.

---

## 2026-04-26 — Phase 1.5.1: Incremental Resolution Refresh

**SPEC coverage:** Step 1.5.1 (new module, gap-fill, ongoing freshness design).

**What landed:**
- `src/pmbot/phase1_data/resolutions_refresh.py` (~170 LOC): importable `run_refresh()`
  function + `__main__` CLI entry point.
- `tests/phase1_data/test_resolutions_refresh.py`: 13 unit tests (all green, 0.63s);
  1 live integration test gated behind `@pytest.mark.integration`.
- `SPEC.md` Step 1.5.1 corrected: `max(resolved_at)` → `max(end_date)`, 7-day → 14-day
  buffer (field-mismatch fix; `end_date_min` filters on `endDate`, not `closedTime`).
- Surgical change to `resolutions.py`: `_fetch_closed_markets_page` promoted to public
  (`fetch_closed_markets_page`) with an `extra_params` dict kwarg; all existing tests
  updated to the new name. No other Phase 1.5 internals touched.

**Key decisions:**
- **`end_date` not `resolved_at` for since_date offset.** The filter parameter
  `end_date_min` compares against `endDate`; computing the offset from `closedTime`
  (which `resolved_at` derives from) introduces silent gaps or overlaps because those
  two fields skew by days on real markets.
- **14-day overlap buffer** (up from 7). Markets sometimes take days to finalize after
  `endDate`; 14 days is the empirically safe buffer for weekly-cadence refresh.
- **Refresh-wins conflict policy.** When a `market_id` exists in both the existing CSV
  and the refresh fetch, the refreshed row replaces the old one. Each conflict logs a
  structured INFO event (old/new flags and outcome) so flag-drift can be monitored.
- **`ColdStartRequired` domain exception, not `SystemExit`.** The importable function
  raises `ColdStartRequired(ValueError)` so library callers can handle it; the CLI
  wrapper catches and converts to `SystemExit` code 2.
- **`--cold-start` as explicit footgun guard.** Auto-defaulting to "all of time" hits
  the offset cap and produces an incomplete corpus. Cold start requires both
  `--cold-start` AND `--since` explicitly; neither flag alone is accepted.
- **Two hard-fail filter assertions** (per PROGRESS.md "silent fallback" pattern):
  (a) first-page `endDate` must be ≥ `since_date − 1h` (catches `end_date_min` silently
  ignored); (b) per-page BTC-binary count ceiling of 50,000 (catches unfiltered archive
  returned).
- **Merge preserves existing row order.** Phase 1.5 writes in API fetch order (no sort);
  the merge preserves that order, replaces conflicts in-place, and appends net-new at
  the end. Idempotent no-op produces byte-identical output.
- **Atomic write via `.tmp → os.replace`.** No `.bak`; the module is idempotent and
  re-runnable from cold start.

**Deferred (explicit out-of-scope):**
- Cron / systemd / scheduler wiring. The script is designed to be scheduled (weekly
  cadence, `end_date_min` keeps slices small), but scheduling is ops, not Phase 1.5.1.
- One-time gap fill (`--since 2026-01-01`) is an invocation of the new script after
  this phase lands, not part of the commit.

**Next:** Phase 1.1 Pass 2 (price-bar ingestion for the now-complete label set).

---

## 2026-04-27 — Phase 1.5.1 Addendum: Gap-Fill Execution and Findings

Addendum to the 2026-04-26 entry above. Records the live integration test result,
the two-pass gap-fill execution, anomalies discovered in the corpus, and follow-up
issues filed for future phases.

### Done checklist (complete)

5. Live integration test passed (`pytest -m integration`, 4:53s) — confirmed `end_date_min`
   is honored by Gamma at time of writing.

(Items 1–4 recorded in the 2026-04-26 entry.)

### One-time gap-fill execution

Required two passes — the first pass hit the Gamma offset cap mid-fetch.

**Pass 1:** `python -m pmbot.phase1_data.resolutions_refresh --since 2026-01-01`
- Walked 2,501 pages, hit the 250,100 offset cap mid-fetch (logged cleanly:
  `Gamma pagination cap reached at offset=250100 — stopping cleanly`).
- Wrote 1,565 new rows. CSV: 7,658 → 9,223. Zero conflicts.
- **The cap truncated the recent tail.** Post-pass-1, max `resolved_at` was
  2026-04-01; the histogram showed only 152 records for 2026-03 and 1 for
  2026-04 — far below expected volume. Gamma paginates oldest-to-newest within
  a filtered slice, so a wide slice hits the cap before it reaches recent markets.

**Pass 2:** `python -m pmbot.phase1_data.resolutions_refresh --since 2026-03-01`
- 56-day slice, no cap hit.
- Wrote 1,183 new rows. CSV: 9,223 → 10,406. Zero conflicts.
- The overlap window (~828 markets with `end_date >= 2026-02-15`) produced zero
  conflicts — all refetched rows were byte-identical.

**Final corpus state:**
- Total rows: 7,658 → **10,406**.
- Monthly histogram ramps cleanly through 2026-03 (1,111) and 2026-04 (225 — partial
  month).
- Top `resolved_at`: late April 2026 (within hours of the run).

### Positive finding

**Phase 1.5's flag-derivation predicates are stable on re-fetch.** Zero conflicts across
two overlap windows totalling ~1,674 markets. The disputed-predicate fix from Phase 1.5
is not flipping flags on re-fetch within the tested windows. (Caveat: re-fetching the
same markets produces the same Gamma responses if UMA status hasn't changed, so this isn't
a perfect predicate-quality probe — but it's not nothing.)

### Anomalies noted (not fixed here)

**2025-09 spike:** 3,733 resolutions in one month versus ~700-800/month before and after.
Confirmed cause: Polymarket received CFTC approval to return to the U.S. market in
September 2025 (alongside a $200M funding round at a $9B valuation), launched a U.S.
beta, and saw a mass activity spike. Worth flagging for training data balance — if
BTC binaries cluster heavily in 2025-09, that one month could dominate the loss surface.
Investigate if/when training data shape becomes a concern.

**Long-dated markets in CSV:** 7 rows with `end_date = 2027-01-01` — early-resolved
markets whose scheduled deadlines are in the future. This is a real Polymarket schema
property (`endDate` = scheduled deadline, `closedTime` = actual resolution), not data
corruption. It surfaces a design issue described in Follow-up #2 below.

### Follow-ups filed (deferred, not blocking Phase 1.6)

1. **Cap-detection silent failure.** When the Gamma offset cap trips mid-fetch, the
   current code logs the event and proceeds to merge whatever it fetched. This produces
   a "successful" run that is actually incomplete — the recent tail is missing. The
   Phase 1.5.1 hard-fail assertions catch Gamma silently ignoring parameters but do not
   cover this case. Fix: detect the cap signal and raise rather than merge, OR auto-chunk
   the slice into sub-windows. Matters most for ad-hoc gap fills with wide `--since`
   values; weekly cron will always have a small slice and should not trip this.

2. **`max(end_date)` auto-since default is broken when long-dated markets are present.**
   With 7 rows at `end_date = 2027-01-01`, `max(end_date) − 14d = 2026-12-18` — a
   future date. Passing that to Gamma as `end_date_min` returns zero records. Weekly
   cron would silently fetch nothing on its first auto-default run. Fix: use a robust
   percentile of `end_date` (e.g., 95th percentile or median) or revert to
   `max(resolved_at) − 14d` with a longer buffer. Requires a second SPEC.md Step 1.5.1
   correction. Must be resolved before the first weekly cron run.

3. **Cron / scheduler wiring** — explicitly deferred to ops as planned. Blocked on
   Follow-up #2 being resolved first.

### Lessons forward

- **Slice width that fits under the cap at current Polymarket volume: ~110 days.** A
  115-day slice (`--since 2026-01-01` on 2026-04-26) hit the cap; a 56-day slice
  (`--since 2026-03-01`) did not. For any ad-hoc gap fill wider than ~100 days, plan
  for multiple passes.
- **Page count is independent of post-filter record count.** Runtime is governed by raw
  pages walked, not BTC records found. A 25-day live integration test (996 BTC records)
  and Phase 1.5's full archive walk (~7,658 BTC records) took similar wall-clock time
  because both walked ~2,000-2,500 pages at `page_size=100`.
- **The hard-fail assertion suite caught its target failure modes but missed one** (cap
  mid-fetch). The pattern of "Polymarket silently accepts wrong input" now has a fourth
  recorded instance. Treat assertion design as a class of bug, not one-off cases.

### Confirmed out of scope (not done in this phase)

- Cron / systemd / scheduler wiring.
- Refactor of Phase 1.5 internals beyond the one-function promotion.
- The three follow-ups filed above.
- Phase 1.6 QA logic.

---

## 2026-04-29 — Phase 1.1 Pass 2: Bar Ingestion

**SPEC step covered:** Step 1.1 Pass 2 — Heavy Lift Bar Ingestion. Fetches 1-minute
price bars from CLOB `/prices-history` for all training-eligible markets, writes
Hive-partitioned Parquet to `data/bars/`, and emits a run-state manifest.

### New files
- `src/pmbot/phase1_data/bars_ingest.py` — full ingestion module (~500 LOC)
- `tests/phase1_data/test_bars_ingest.py` — 17 unit tests + 1 integration test
- `pyproject.toml` — added `polars==1.40.1`, `duckdb==1.5.2`, dev group pinned

### Key decisions

**Token source (pre-planning gap):** `resolved_markets.csv` (Phase 1.5 output) has no
`token_id` or `createdAt` columns. Resolution: a separate `_market_lookup.parquet` is
built from Gamma `/markets/{id}` before bar fetching. Three-path startup logic:
cold-build, delta-fetch (appends missing rows), or `--rebuild-lookup` full refresh.

**clobTokenIds shape confirmed by spike (2026-04-29):** JSON-encoded string, NOT a
native array. `json.loads()` required. YES token selected by index position in `outcomes`
array (Phase 1.5 pattern reused), not by assuming index 0. Hard-fail on unexpected shape
so the operator sees issues at lookup-build time, not 42 min later.

**createdAt fallback policy:** If Gamma response lacks `createdAt`, write
`status=fail, error_reason="missing_created_at"` to manifest and skip bar fetch.
No arbitrary day-count fallback (avoids silent-bad-data). If >1% of markets hit this in
the smoke run, switch to `start_ts=0` sentinel before the full run.

**YES token only:** `price_no = 1 − price_yes` by construction for binary markets. One
bar series per market. Halves disk footprint vs. fetching both tokens.

**Bar write — manual Hive partitioning:** Polars 1.40.1 includes partition columns (`market_id`,
`utc_date`) in the written Parquet file content when using `write_parquet(..., partition_by=[...])`.
Fixed by constructing Hive paths manually and writing `{t, p}` only per partition.
Confirmed by `test_partition_column_stripping`.

**CLOB response shape confirmed by spike (2026-04-29):** `{"history": [{t, p}, ...]}`.
Not a bare list. Market 1817348 (BTC $74k-$76k band, Apr 2026): 10,007 bars, 60s median Δt.

**Integration test marker — added `addopts`:** Existing pyproject.toml documented
integration tests as "skipped by default" but had no enforcement. Added
`addopts = "-m 'not integration'"` to match documented behavior. All 5 existing integration
tests (resolutions phase) unaffected.

**PyArrow:** Not needed. Polars 1.40.1 partitioned writes succeed without PyArrow.
Manual path construction eliminates any future dependency on Polars version behavior.

### Hard-fail predicates (locked)
- `bar_count < 10` → `bar_count_below_min`
- `median(Δt) > 300s` → `median_dt_exceeds_threshold`
- Any bar outside `[start_ts, end_ts + 60]` → `bar_outside_window`
- `max(Δt)` is NOT a predicate — real thin markets have real gaps.

### Done checklist
1. `pytest tests/phase1_data/test_bars_ingest.py` — 17/17 unit tests green (0.88s)
2. `mypy --strict src/pmbot/phase1_data/bars_ingest.py` — zero errors
3. `ruff check src/pmbot/phase1_data tests/phase1_data` — zero errors
4. PyArrow smoke check — passed without pin; no PyArrow in `dependencies`
5. Integration test — `pytest -m integration tests/phase1_data/test_bars_ingest.py` green (1.33s)
6. Full suite — 126/126 pass, 5 integration deselected (1.91s)

### Deferred follow-ups (out of scope here)
- Tune `max_workers` from empirical baseline after smoke run (current default: 4, ~22 min wall clock)
- Calibrate hard-fail predicates against post-run manifest distribution (Phase 1.6 prep)
- Verify if Gamma supports batch market lookup by multiple IDs (reduce ~42 min lookup build)
- Switch `createdAt` fallback to `start_ts=0` sentinel if missing rate >1% in smoke run

### Next steps (operator — before full run)
1. Run sample-bias smoke: `--limit 50` at offsets 0, 1000, 2500, 4500
2. Inspect manifest: success rate, median bar_count, median Δt, missing_created_at count
3. If smoke is clean, launch full run (add `--resume` if it crashes mid-way)
