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