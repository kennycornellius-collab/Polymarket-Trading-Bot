"""Integration tests — hit the live Polymarket Gamma API.

Run with: pytest -m integration tests/phase1/test_resolutions_integration.py

These tests validate GammaClosedMarketRecord field assumptions against the actual API
and verify build_resolution_record handles live data without malformed-price failures.
Run manually before each resolution rebuild.
"""

from __future__ import annotations

import json

import pytest

from pmbot.phase1_data.resolutions import (
    ResolutionConfig,
    _fetch_closed_markets_page,
    build_resolution_record,
    is_btc_binary_shape,
)

# Maximum closed markets to scan while accumulating the target BTC binary sample.
# BTC binary absolute-strike markets appear at ~0.8% of closed records (early pages
# skew to 2020-era scalar/non-BTC markets). 1000 records → ~8 qualifying empirically.
_MAX_SCAN = 1000
_TARGET_BTC_RECORDS = 6


@pytest.mark.integration
def test_closed_markets_schema_and_resolution_pipeline() -> None:
    """Accumulate ≥6 BTC binary records across pages, assert schema, run pipeline."""
    cfg = ResolutionConfig(page_size=100)

    raw_all: list = []
    btc_records: list = []
    offset = 0

    # Paginate until we have enough qualifying records or hit the scan cap
    while len(btc_records) < _TARGET_BTC_RECORDS and offset < _MAX_SCAN:
        page = _fetch_closed_markets_page(cfg, offset=offset)
        if not page:
            break

        # ── Schema assertions on records in this page ────────────────────────
        page_base = len(raw_all)
        for i, rec in enumerate(page):
            idx = page_base + i
            assert "outcomePrices" in rec, f"record[{idx}] missing 'outcomePrices'"
            assert isinstance(rec["outcomePrices"], str), (
                f"record[{idx}]['outcomePrices'] must be str, got {type(rec['outcomePrices'])}"
            )

            assert "closedTime" in rec, f"record[{idx}] missing 'closedTime'"
            assert isinstance(rec["closedTime"], str), (
                f"record[{idx}]['closedTime'] must be str, got {type(rec['closedTime'])}"
            )

            # endDate absent on some perpetual/group markets — graceful absence is fine
            if "endDate" in rec:
                assert isinstance(rec["endDate"], str), (
                    f"record[{idx}]['endDate'] must be str, got {type(rec['endDate'])}"
                )

            vol_present = rec.get("volumeNum") is not None or rec.get("volume") is not None
            assert vol_present, f"record[{idx}] missing both 'volumeNum' and 'volume'"

            uma = rec.get("umaResolutionStatuses")
            if uma is not None:
                try:
                    json.loads(uma)
                except Exception as exc:
                    pytest.fail(
                        f"record[{idx}]['umaResolutionStatuses'] not parseable as JSON: {exc}"
                    )

            assert rec.get("closed") is True, (
                f"record[{idx}] has closed={rec.get('closed')!r} — "
                f"closed=true server-side filter not working"
            )

        btc_records.extend(r for r in page if is_btc_binary_shape(r))
        raw_all.extend(page)
        offset += cfg.page_size

        if len(page) < cfg.page_size:
            break  # last page

    assert len(btc_records) >= _TARGET_BTC_RECORDS, (
        f"Expected ≥{_TARGET_BTC_RECORDS} BTC binary shape records after scanning "
        f"{len(raw_all)} closed markets, got {len(btc_records)}. "
        f"Shape filter may be over-rejecting or BTC binary universe is smaller than expected."
    )

    # ── Run build_resolution_record on all accumulated BTC binary records ─────
    built = [build_resolution_record(r, cfg) for r in btc_records]

    clean_outcomes = [r for r in built if r.outcome in {"YES", "NO"}]
    malformed = [r for r in built if "malformed_prices" in r.flags]
    flagged = [r for r in built if r.flags]

    clean_pct = len(clean_outcomes) / len(built) if built else 0.0

    assert clean_pct >= 0.70, (
        f"Expected ≥70% clean outcomes (YES/NO), got {clean_pct:.0%}.\n"
        f"Non-clean records:\n"
        + "\n".join(
            f"  id={r.market_id} outcome={r.outcome} flags={r.flags!r} "
            f"prices={r.outcome_prices_raw!r}"
            for r in built
            if r.outcome not in {"YES", "NO"}
        )
    )

    assert len(malformed) == 0, (
        f"Expected 0 malformed_prices flags on live data, got {len(malformed)}.\n"
        f"Malformed records:\n"
        + "\n".join(f"  id={r.market_id} prices={r.outcome_prices_raw!r}" for r in malformed)
    )

    yes_count = sum(1 for r in built if r.outcome == "YES")
    no_count = sum(1 for r in built if r.outcome == "NO")
    assert yes_count >= 1, (
        f"Expected ≥1 YES outcome in {len(built)} BTC binary records, got 0. "
        f"Outcome distribution: {_count_outcomes(built)}"
    )
    assert no_count >= 1, (
        f"Expected ≥1 NO outcome in {len(built)} BTC binary records, got 0. "
        f"Outcome distribution: {_count_outcomes(built)}"
    )

    assert len(flagged) >= 1, (
        f"Expected ≥1 flagged record (walkover likely at $1k threshold), "
        f"got 0 flags across {len(built)} records. "
        f"Min volume: {min(r.volume_lifetime_usdc for r in built):.2f} USDC"
    )


def _count_outcomes(records: list) -> dict[str, int]:  # type: ignore[type-arg]
    counts: dict[str, int] = {}
    for r in records:
        counts[r.outcome] = counts.get(r.outcome, 0) + 1
    return counts
