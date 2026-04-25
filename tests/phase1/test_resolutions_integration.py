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


# UMA status values observed in 2026-04-25 sweep across 2,500 records:
#   'proposed'  — routine: proposer submitted (~68% of non-empty)
#   'resolved'  — routine: finalized cleanly (~27%)
#   'disputed'  — actual dispute (~0.6%)
_KNOWN_UMA_STATUS_VALUES: frozenset[str] = frozenset({"proposed", "resolved", "disputed"})


@pytest.mark.integration
def test_uma_status_predicate_semantics() -> None:
    """Guard against the 2026-04-25 bug: routine 'proposed'/'resolved' must not fire disputed.

    Sweeps 3 offset bands (0, 50k, 100k) across Gamma's ~250k corpus for ≥500 records.
    Asserts:
      - No UMA status values outside the known set (catches new lifecycle states).
      - disputed-flag rate < 5% (catches wholesale mis-classification).
      - Records with 'proposed' (only) do NOT carry the disputed flag (direct regression).
    """
    cfg = ResolutionConfig(page_size=100)
    band_offsets = [0, 50_000, 100_000]
    pages_per_band = 2  # 200 records per band → 600 total

    raw_all: list = []
    for band_offset in band_offsets:
        for page_idx in range(pages_per_band):
            page = _fetch_closed_markets_page(cfg, offset=band_offset + page_idx * cfg.page_size)
            if not page:
                break
            raw_all.extend(page)

    assert len(raw_all) >= 500, (
        f"Expected ≥500 records across offset bands, got {len(raw_all)}. "
        "Gamma corpus may be smaller or API returned short pages early."
    )

    # ── Distinct UMA status values must be subset of known set ───────────────
    seen_statuses: set[str] = set()
    for rec in raw_all:
        uma_raw = rec.get("umaResolutionStatuses")
        if uma_raw:
            try:
                parsed = json.loads(uma_raw)
                if isinstance(parsed, list):
                    seen_statuses.update(str(v) for v in parsed)
            except Exception:
                pass

    unexpected = seen_statuses - _KNOWN_UMA_STATUS_VALUES
    assert not unexpected, (
        f"Polymarket added UMA status values not in known set {set(_KNOWN_UMA_STATUS_VALUES)!r}: "
        f"{unexpected!r}. Review dispute_status_values in ResolutionConfig."
    )

    # ── disputed-flag rate must be < 5% ──────────────────────────────────────
    built = [build_resolution_record(r, cfg) for r in raw_all]
    disputed_count = sum(1 for r in built if "disputed" in r.flags)
    disputed_rate = disputed_count / len(built) if built else 0.0
    assert disputed_rate < 0.05, (
        f"Expected <5% disputed-flag rate, got {disputed_rate:.1%} "
        f"({disputed_count}/{len(built)}). "
        "Predicate may be treating routine lifecycle states as disputes."
    )

    # ── 'proposed'-only records must NOT carry the disputed flag ─────────────
    proposed_only_built = []
    for raw, built_rec in zip(raw_all, built):
        uma_raw = raw.get("umaResolutionStatuses") or "[]"
        try:
            parsed_list: list[str] = json.loads(uma_raw)
        except Exception:
            parsed_list = []
        if "proposed" in parsed_list and "disputed" not in parsed_list:
            proposed_only_built.append(built_rec)

    assert len(proposed_only_built) >= 1, (
        "No records with 'proposed' (non-disputed) UMA status found in sample — "
        "cannot exercise the negative case. Widen the scan."
    )
    wrongly_flagged = [r for r in proposed_only_built if "disputed" in r.flags]
    assert not wrongly_flagged, (
        f"Found {len(wrongly_flagged)} record(s) with 'proposed'-only UMA status "
        f"wrongly flagged as disputed:\n"
        + "\n".join(f"  id={r.market_id} flags={r.flags!r}" for r in wrongly_flagged)
    )
