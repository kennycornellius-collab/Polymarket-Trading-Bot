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

**Deferred:**
- Loading WhitelistConfig from configs/whitelist.toml (Phase 1+).
- Resolved-market whitelist construction for historical training data (Phase 1.5).
- Volume/TTE proxies for historical/closed markets — out of scope for Pass 1.