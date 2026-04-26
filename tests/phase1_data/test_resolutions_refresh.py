"""Tests for Phase 1.5.1 — Incremental Resolution Refresh."""

from __future__ import annotations

import csv
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from pmbot.phase1_data.resolutions import ResolutionConfig, ResolutionRecord
from pmbot.phase1_data.resolutions_refresh import (
    ColdStartRequired,
    _compute_since_date,
    _merge_records,
    run_refresh,
)

# ── Fixtures ───────────────────────────────────────────────────────────────────

_CSV_HEADER = (
    "market_id,question,slug,outcome,resolved_at,end_date,"
    "volume_lifetime_usdc,outcome_prices_raw,flags\n"
)


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    fieldnames = [
        "market_id",
        "question",
        "slug",
        "outcome",
        "resolved_at",
        "end_date",
        "volume_lifetime_usdc",
        "outcome_prices_raw",
        "flags",
    ]
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _make_row(
    market_id: str = "m1",
    end_date: str = "2026-03-01T00:00:00+00:00",
    flags: str = "",
    outcome: str = "YES",
) -> dict[str, str]:
    return {
        "market_id": market_id,
        "question": "Q?",
        "slug": market_id,
        "outcome": outcome,
        "resolved_at": "2026-03-02T00:00:00+00:00",
        "end_date": end_date,
        "volume_lifetime_usdc": "5000.00",
        "outcome_prices_raw": '["0.9999","0.0001"]',
        "flags": flags,
    }


def _make_record(
    market_id: str = "m1",
    end_date: str = "2026-03-01T00:00:00+00:00",
    flags: str = "",
    outcome: str = "YES",
) -> ResolutionRecord:
    return ResolutionRecord(
        market_id=market_id,
        question="Q?",
        slug=market_id,
        outcome=outcome,
        resolved_at="2026-03-02T00:00:00+00:00",
        end_date=end_date,
        volume_lifetime_usdc=5000.0,
        outcome_prices_raw='["0.9999","0.0001"]',
        flags=flags,
    )


# ── Unit tests ─────────────────────────────────────────────────────────────────


def test_since_date_from_csv_max_end_date(tmp_path: Path) -> None:
    csv_path = tmp_path / "resolved_markets.csv"
    _write_csv(
        csv_path,
        [
            _make_row("a", end_date="2026-04-10T00:00:00+00:00"),
            _make_row("b", end_date="2026-04-15T00:00:00+00:00"),  # max
            _make_row("c", end_date=""),  # null → skipped
        ],
    )
    since = _compute_since_date(csv_path, since_override=None, cold_start=False)
    assert since == date(2026, 4, 15) - timedelta(days=14)


def test_since_date_null_rows_trigger_warning(tmp_path: Path, caplog: Any) -> None:
    import logging

    csv_path = tmp_path / "resolved_markets.csv"
    _write_csv(
        csv_path,
        [
            _make_row("a", end_date="2026-04-10T00:00:00+00:00"),
            _make_row("b", end_date=""),
            _make_row("c", end_date="not-a-date"),
        ],
    )
    with caplog.at_level(logging.WARNING, logger="pmbot.phase1_data.resolutions_refresh"):
        _compute_since_date(csv_path, since_override=None, cold_start=False)
    assert any("Skipped 2 rows" in r.message for r in caplog.records)


def test_since_date_override_bypasses_csv(tmp_path: Path) -> None:
    # CSV absent — but --since provided → no error
    csv_path = tmp_path / "missing.csv"
    override = date(2026, 1, 1)
    since = _compute_since_date(csv_path, since_override=override, cold_start=False)
    assert since == override


def test_since_date_cold_start_errors_no_flags(tmp_path: Path) -> None:
    csv_path = tmp_path / "missing.csv"
    with pytest.raises(ColdStartRequired):
        _compute_since_date(csv_path, since_override=None, cold_start=False)


def test_since_date_cold_start_errors_missing_since(tmp_path: Path) -> None:
    # cold_start=True but no since_override
    csv_path = tmp_path / "missing.csv"
    with pytest.raises(ColdStartRequired):
        _compute_since_date(csv_path, since_override=None, cold_start=True)


def test_since_date_all_null_end_dates_is_cold_start(tmp_path: Path) -> None:
    csv_path = tmp_path / "resolved_markets.csv"
    _write_csv(csv_path, [_make_row("a", end_date="")])
    with pytest.raises(ColdStartRequired):
        _compute_since_date(csv_path, since_override=None, cold_start=False)


# ── Merge tests ────────────────────────────────────────────────────────────────


def test_merge_refresh_wins_conflict(tmp_path: Path, caplog: Any) -> None:
    import logging

    csv_path = tmp_path / "resolved_markets.csv"
    _write_csv(
        csv_path,
        [
            _make_row("m1", flags="walkover", outcome="NO"),
            _make_row("m2", flags=""),
        ],
    )
    # m1 refreshed with different flags/outcome
    refreshed = [
        _make_record("m1", flags="", outcome="YES"),
        _make_record("m3", flags=""),  # net-new
    ]
    with caplog.at_level(logging.INFO, logger="pmbot.phase1_data.resolutions_refresh"):
        merged, new_count, conflict_count = _merge_records(csv_path, refreshed)

    assert new_count == 1
    assert conflict_count == 1
    ids = [r.market_id for r in merged]
    assert ids == ["m1", "m2", "m3"]  # m1 in-place, m3 appended at end
    m1 = next(r for r in merged if r.market_id == "m1")
    assert m1.outcome == "YES"
    assert m1.flags == ""
    # Conflict log should fire
    assert any("conflict resolved" in r.message and "m1" in r.message for r in caplog.records)


def test_merge_non_conflicting_rows_preserved(tmp_path: Path) -> None:
    csv_path = tmp_path / "resolved_markets.csv"
    _write_csv(csv_path, [_make_row("m1"), _make_row("m2")])
    refreshed = [_make_record("m3")]
    merged, new_count, conflict_count = _merge_records(csv_path, refreshed)
    assert new_count == 1
    assert conflict_count == 0
    assert len(merged) == 3
    assert [r.market_id for r in merged] == ["m1", "m2", "m3"]


def test_merge_schema_preserved(tmp_path: Path) -> None:
    """write_resolution_csv produces the same column names and order as Phase 1.5."""
    expected_fields = [
        "market_id",
        "question",
        "slug",
        "outcome",
        "resolved_at",
        "end_date",
        "volume_lifetime_usdc",
        "outcome_prices_raw",
        "flags",
    ]
    csv_path = tmp_path / "resolved_markets.csv"
    _write_csv(csv_path, [_make_row("m1")])
    refreshed = [_make_record("m2")]
    merged, _, _ = _merge_records(csv_path, refreshed)

    out_path = tmp_path / "out.csv"
    from pmbot.phase1_data.resolutions import write_resolution_csv

    write_resolution_csv(merged, out_path)

    with out_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        assert reader.fieldnames == expected_fields


def test_idempotent_no_op(tmp_path: Path) -> None:
    csv_path = tmp_path / "resolved_markets.csv"
    _write_csv(csv_path, [_make_row("m1"), _make_row("m2")])
    original_bytes = csv_path.read_bytes()

    # Refresh with the exact same records
    refreshed = [_make_record("m1"), _make_record("m2")]
    merged, new_count, conflict_count = _merge_records(csv_path, refreshed)
    assert new_count == 0
    assert conflict_count == 0

    out_path = tmp_path / "out.csv"
    from pmbot.phase1_data.resolutions import write_resolution_csv

    write_resolution_csv(merged, out_path)
    assert out_path.read_bytes() == original_bytes


def test_atomic_write_crash_safety(tmp_path: Path) -> None:
    """If os.replace never runs (simulated crash), the original file must be intact."""
    csv_path = tmp_path / "resolved_markets.csv"
    _write_csv(csv_path, [_make_row("m1")])
    original_bytes = csv_path.read_bytes()

    # Simulate crash: write .tmp but don't replace
    tmp_path2 = Path(str(csv_path) + ".tmp")
    from pmbot.phase1_data.resolutions import write_resolution_csv

    write_resolution_csv([_make_record("m1"), _make_record("m2")], tmp_path2)
    # "crash" — never call os.replace
    assert tmp_path2.exists()
    assert csv_path.read_bytes() == original_bytes  # original intact


def test_hard_fail_filter_ignored(tmp_path: Path) -> None:
    """If first page contains records predating since_date, RuntimeError is raised."""
    cfg = ResolutionConfig(output_csv_path=tmp_path / "resolved_markets.csv")
    _write_csv(cfg.output_csv_path, [_make_row("m1", end_date="2026-04-10T00:00:00+00:00")])

    since = date(2026, 4, 20)
    # Page contains a record whose endDate is well before since_date
    stale_record: dict[str, Any] = {
        "id": "old",
        "question": "Q?",
        "slug": "old",
        "outcomes": '["Yes","No"]',
        "endDate": "2026-01-01T00:00:00Z",  # predates since by months
        "outcomePrices": '["0.9999","0.0001"]',
        "closedTime": "2026-01-02 00:00:00+00",
        "volumeNum": 9000.0,
    }

    with patch(
        "pmbot.phase1_data.resolutions_refresh.fetch_closed_markets_page",
        return_value=[stale_record],
    ):
        with pytest.raises(RuntimeError, match="end_date_min filter ignored"):
            run_refresh(since_override=since, out_path=cfg.output_csv_path, config=cfg)


def test_hard_fail_count_ceiling(tmp_path: Path) -> None:
    """If ≥50,000 BTC-binary records are fetched, RuntimeError is raised per-page."""
    page_size = 500
    cfg = ResolutionConfig(
        output_csv_path=tmp_path / "resolved_markets.csv",
        page_size=page_size,
        inter_request_delay_s=0.0,  # no sleep between pages
    )
    _write_csv(cfg.output_csv_path, [_make_row("m1", end_date="2026-04-10T00:00:00+00:00")])
    since = date(2026, 4, 9)

    # A record that passes the endDate assertion (after since) and is_btc_binary_shape (mocked)
    btc_record: dict[str, Any] = {
        "id": "btc1",
        "question": "Will BTC exceed $100000 on 2026-04-20?",
        "slug": "btc1",
        "outcomes": '["Yes","No"]',
        "endDate": "2026-04-20T00:00:00Z",
        "outcomePrices": '["0.9999","0.0001"]',
        "closedTime": "2026-04-21 00:00:00+00",
        "volumeNum": 9000.0,
    }

    # 100 full pages × 500 records = 50,000 BTC records → ceiling check fires after 100th page
    full_page = [btc_record] * page_size
    call_count = 0

    def mock_fetch(
        config: ResolutionConfig,
        offset: int,
        extra_params: dict[str, str] | None = None,
    ) -> list[Any]:
        nonlocal call_count
        call_count += 1
        return full_page  # always return a full page; ceiling check fires before we stop

    with (
        patch(
            "pmbot.phase1_data.resolutions_refresh.fetch_closed_markets_page",
            side_effect=mock_fetch,
        ),
        patch(
            "pmbot.phase1_data.resolutions_refresh.is_btc_binary_shape",
            return_value=True,
        ),
    ):
        with pytest.raises(RuntimeError, match="50,000 sanity ceiling"):
            run_refresh(since_override=since, out_path=cfg.output_csv_path, config=cfg)

    # No final write should have occurred
    assert not Path(str(cfg.output_csv_path) + ".tmp").exists()


# ── Integration test ───────────────────────────────────────────────────────────


@pytest.mark.integration
def test_recent_btc_binary_refresh(tmp_path: Path) -> None:
    """Fetch 2-day slice; assert filter honored and count is plausibly small."""
    from datetime import datetime, timezone

    cfg = ResolutionConfig(
        page_size=100,
        output_csv_path=tmp_path / "resolved_markets.csv",
    )
    since = date.today() - timedelta(days=2)
    from pmbot.phase1_data.resolutions_refresh import _fetch_refresh_records

    raw = _fetch_refresh_records(since, cfg, tmp_path / "resolved_markets.partial.csv")

    since_dt = datetime.combine(since, datetime.min.time()).replace(tzinfo=timezone.utc)
    tolerance = timedelta(hours=1)
    for r in raw:
        ed_raw = r.get("endDate", "")
        if ed_raw:
            normalized = ed_raw.replace("Z", "+00:00")
            ed_dt = datetime.fromisoformat(normalized)
            if ed_dt.tzinfo is None:
                ed_dt = ed_dt.replace(tzinfo=timezone.utc)
            assert ed_dt >= since_dt - tolerance, (
                f"Record endDate {ed_raw!r} predates since_date {since}"
            )

    assert len(raw) < 1_000, (
        f"2-day slice returned {len(raw)} records — suspiciously large, filter may be ignored"
    )
