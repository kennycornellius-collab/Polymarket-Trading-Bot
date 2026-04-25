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

**Checkpoint outcome from first smoke run:** The partial checkpoint at 7,166 records
confirmed the predicate fix: 15 disputed (0.2%) vs 96% before. Training-eligible count
projected to ~4,600 in the partial, consistent with the 3,000–4,500 target.