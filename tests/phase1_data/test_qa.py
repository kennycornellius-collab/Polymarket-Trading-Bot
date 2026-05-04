"""Tests for Phase 1.6 — Data Quality Validation (pmbot.phase1_data.qa)."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from pmbot.phase1_data.qa import (
    QAConfig,
    _UnknownQACondition,
    qa_one_market,
    run_qa,
    validate_immutable_target_path,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────


def _make_config(tmp_path: Path, **overrides: Any) -> QAConfig:
    """Build a QAConfig rooted in tmp_path."""
    defaults: dict[str, Any] = dict(
        gap_threshold_seconds=300,
        min_bars_per_window=60,
        min_bars_per_market=60,
        max_rejected_window_fraction=0.5,
        bars_root=tmp_path / "bars",
        bars_clean_root=tmp_path / "bars_clean",
        qa_market_path=tmp_path / "bars" / "_qa_market.parquet",
        qa_windows_path=tmp_path / "bars" / "_qa_windows.parquet",
        qa_distributions_path=tmp_path / "bars" / "_qa_distributions.parquet",
        resolutions_csv_path=tmp_path / "resolutions" / "resolved_markets.csv",
    )
    defaults.update(overrides)
    return QAConfig(**defaults)


def _bars(ts: list[int], prices: list[float] | None = None) -> pl.DataFrame:
    """Build a minimal bars DataFrame."""
    if prices is None:
        prices = [0.5] * len(ts)
    return pl.DataFrame(
        {
            "t": pl.Series(ts, dtype=pl.Int64),
            "p": pl.Series(prices, dtype=pl.Float64),
        }
    )


def _contiguous(start_t: int, count: int, step: int = 60) -> list[int]:
    """Generate `count` timestamps starting at start_t with interval step."""
    return [start_t + i * step for i in range(count)]


def _write_bar_partition(
    bars_root: Path, market_id: int, date_str: str, ts: list[int]
) -> None:
    """Write a minimal Parquet partition file for test setup."""
    part_dir = bars_root / f"market_id={market_id}" / f"utc_date={date_str}"
    part_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "t": pl.Series(ts, dtype=pl.Int64),
            "p": pl.Series([0.5] * len(ts), dtype=pl.Float64),
        }
    ).write_parquet(part_dir / "00000000.parquet")


def _write_manifest(bars_root: Path, market_rows: list[dict[str, object]]) -> None:
    """Write a minimal _manifest.parquet."""
    bars_root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "market_id": pl.Series(
                [str(r["market_id"]) for r in market_rows], dtype=pl.String
            ),
            "status": pl.Series(
                [str(r["status"]) for r in market_rows], dtype=pl.String
            ),
            "bar_count": pl.Series(
                [int(r.get("bar_count", 0)) for r in market_rows], dtype=pl.Int64  # type: ignore[arg-type]
            ),
            "first_ts": pl.Series([0] * len(market_rows), dtype=pl.Int64),
            "last_ts": pl.Series([0] * len(market_rows), dtype=pl.Int64),
            "error_reason": pl.Series(
                [None] * len(market_rows), dtype=pl.String
            ),
            "run_id": pl.Series(
                ["test_run"] * len(market_rows), dtype=pl.String
            ),
            "completed_at": pl.Series(
                ["2026-05-01T00:00:00+00:00"] * len(market_rows), dtype=pl.String
            ),
            "attempt_count": pl.Series([1] * len(market_rows), dtype=pl.Int64),
        }
    ).write_parquet(bars_root / "_manifest.parquet")


def _write_resolutions_csv(
    resolutions_dir: Path,
    rows: list[dict[str, str]],
) -> None:
    """Write a minimal resolved_markets.csv."""
    resolutions_dir.mkdir(parents=True, exist_ok=True)
    header = "market_id,question,slug,outcome,resolved_at,end_date,volume_lifetime_usdc,outcome_prices_raw,flags\n"
    lines = [header]
    for r in rows:
        mid = r["market_id"]
        question = r.get("question", "q")
        slug = r.get("slug", "s")
        outcome = r.get("outcome", "YES")
        resolved_at = r.get("resolved_at", "2025-01-01T00:00:00+00:00")
        end_date = r.get("end_date", "")
        volume = r.get("volume", "1000.0")
        lines.append(
            f"{mid},{question},{slug},{outcome},{resolved_at},{end_date},{volume},"
            '"[""0.5""]",\n'
        )
    (resolutions_dir / "resolved_markets.csv").write_text("".join(lines))


# ── Test 1: no-bar-files market is never clean ────────────────────────────────


def test_no_bar_files_market_is_not_clean(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    empty_df = _bars([], [])
    result = qa_one_market(999, empty_df, config)
    assert result.qa_status == "rejected"
    assert result.qa_reason == "no_bar_files"
    assert result.clean_df.is_empty()


# ── Test 2: immutability check raises on bars_root partition path ─────────────


def test_immutability_check_blocks_writes_inside_bars_root(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    bad_path = (
        config.bars_root / "market_id=123" / "utc_date=2025-01-01" / "00000000.parquet"
    )
    with pytest.raises(RuntimeError, match="immutable"):
        validate_immutable_target_path(bad_path, config)


def test_immutability_check_allows_root_sidecar(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    ok_path = config.bars_root / "_qa_market.parquet"
    validate_immutable_target_path(ok_path, config)  # must not raise


def test_immutability_check_allows_bars_clean(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    ok_path = config.bars_clean_root / "market_id=1" / "utc_date=2025-01-01" / "00000000.parquet"
    validate_immutable_target_path(ok_path, config)  # must not raise


# ── Test 3: dedup keeps last for duplicate t ─────────────────────────────────


def test_dedupe_keeps_last_for_duplicate_t(tmp_path: Path) -> None:
    config = _make_config(tmp_path, min_bars_per_window=1, min_bars_per_market=1)
    # Two bars at t=100 (prices 0.5 and 0.7), one at t=200
    df = _bars([100, 100, 200], [0.5, 0.7, 0.8])
    result = qa_one_market(1, df, config)
    assert result.clean_df.height == 2
    row_100 = result.clean_df.filter(pl.col("t") == 100)
    assert row_100.height == 1
    assert float(row_100["p"][0]) == pytest.approx(0.7)
    # One bar was dropped (the duplicate)
    assert result.dropped_bar_count == 1


# ── Test 4: drops non-finite and non-positive prices ─────────────────────────


def test_drops_nonfinite_and_nonpositive_prices(tmp_path: Path) -> None:
    config = _make_config(tmp_path, min_bars_per_window=1, min_bars_per_market=1)
    df = _bars(
        [1, 2, 3, 4, 5, 6],
        [float("nan"), math.inf, -math.inf, 0.0, -0.1, 0.5],
    )
    result = qa_one_market(1, df, config)
    assert result.clean_df.height == 1
    assert float(result.clean_df["p"][0]) == pytest.approx(0.5)
    assert result.dropped_bar_count == 5


# ── Test 5: window split at gap threshold + property assertion ────────────────


def test_window_split_at_gap_threshold(tmp_path: Path) -> None:
    config = _make_config(tmp_path, gap_threshold_seconds=300, min_bars_per_window=3, min_bars_per_market=1)

    # 301s gap → two windows
    # Window 0: t=100, 160, 220
    # Gap: 220 → 521 = 301s
    # Window 1: t=521, 581, 641
    ts_two = [100, 160, 220, 521, 581, 641]
    result_two = qa_one_market(1, _bars(ts_two), config)
    assert result_two.window_count == 2
    assert result_two.usable_window_count == 2
    assert result_two.qa_status == "usable"
    assert result_two.qa_reason == "gap_split"

    # Boundary: exactly 300s gap → 1 window
    # Window 0: t=100, 160, 220
    # Gap: 220 → 520 = 300s (not > 300, so stays in same window)
    # Window continues: t=520, 580, 640
    ts_one = [100, 160, 220, 520, 580, 640]
    result_one = qa_one_market(1, _bars(ts_one), config)
    assert result_one.window_count == 1

    # Property: every bar in clean_df belongs to exactly one window (no duplicates, no orphans)
    all_clean_ts = set(result_two.clean_df["t"].to_list())
    ts_to_window: dict[int, int] = {}
    for wv in result_two.windows:
        if wv.usable:
            win_ts = set(
                t for t in all_clean_ts
                if wv.window_start_ts <= t <= wv.window_end_ts
            )
            for t_val in win_ts:
                assert t_val not in ts_to_window, f"t={t_val} assigned to multiple windows"
                ts_to_window[t_val] = wv.window_idx
    assert len(ts_to_window) == len(all_clean_ts), "Some clean bars are orphans (no owning window)"


# ── Test 6: window too short is marked unusable ───────────────────────────────


def test_window_too_short_marked_unusable(tmp_path: Path) -> None:
    # 59 bars → below min_bars_per_window=60
    config = _make_config(tmp_path, min_bars_per_window=60, min_bars_per_market=1)
    ts = _contiguous(1_000_000, 59)
    result = qa_one_market(1, _bars(ts), config)
    assert result.window_count == 1
    assert result.usable_window_count == 0
    assert result.windows[0].usable is False
    assert result.windows[0].reason == "window_too_short"


# ── Test 7: market with too few total bars is rejected ────────────────────────


def test_market_too_few_total_bars_rejected(tmp_path: Path) -> None:
    config = _make_config(tmp_path, min_bars_per_window=1, min_bars_per_market=60)
    # 59 bars → below min_bars_per_market=60
    ts_59 = _contiguous(1_000_000, 59)
    r59 = qa_one_market(1, _bars(ts_59), config)
    assert r59.qa_status == "rejected"
    assert r59.qa_reason == "too_few_total_bars"

    # 60 bars → exactly at threshold, not rejected
    ts_60 = _contiguous(1_000_000, 60)
    r60 = qa_one_market(1, _bars(ts_60), config)
    assert r60.qa_status != "rejected"


# ── Test 8: too many rejected windows → market rejected ───────────────────────


def test_market_too_many_rejected_windows_rejected(tmp_path: Path) -> None:
    # max_rejected_window_fraction=0.5; 3 of 4 windows rejected → 75% > 50% → reject
    config = _make_config(
        tmp_path,
        gap_threshold_seconds=300,
        min_bars_per_window=60,
        min_bars_per_market=1,
        max_rejected_window_fraction=0.5,
    )
    # Window 0 (usable): 80 bars
    # Windows 1, 2, 3 (rejected, each < 60 bars): 10 bars each, split by 301s gaps
    w0 = _contiguous(1_000_000, 80, 60)
    gap = 301
    base = w0[-1] + gap
    w1 = _contiguous(base, 10, 60)
    base = w1[-1] + gap
    w2 = _contiguous(base, 10, 60)
    base = w2[-1] + gap
    w3 = _contiguous(base, 10, 60)
    all_ts = w0 + w1 + w2 + w3
    result = qa_one_market(1, _bars(all_ts), config)
    assert result.window_count == 4
    assert result.usable_window_count == 1
    assert result.qa_status == "rejected"
    assert result.qa_reason == "too_many_rejected_windows"


# ── Test 9: clean status requires single full window, no drops ────────────────


def test_clean_status_requires_single_full_window_no_drops(tmp_path: Path) -> None:
    config = _make_config(tmp_path, min_bars_per_window=60, min_bars_per_market=60)
    ts = _contiguous(1_000_000, 80, 60)
    result = qa_one_market(1, _bars(ts), config)
    assert result.qa_status == "clean"
    assert result.qa_reason is None
    assert result.window_count == 1
    assert result.usable_window_count == 1
    assert result.dropped_bar_count == 0
    assert result.clean_df.height == 80


def test_usable_with_single_window_and_dropped_bars(tmp_path: Path) -> None:
    config = _make_config(tmp_path, min_bars_per_window=60, min_bars_per_market=60)
    ts = _contiguous(1_000_000, 80, 60)
    prices = [0.5] * 80
    prices[5] = float("nan")  # one drop
    result = qa_one_market(1, _bars(ts, prices), config)
    assert result.qa_status == "usable"
    assert result.qa_reason == "bars_dropped"
    assert result.dropped_bar_count == 1


# ── Test 10: lenient_upper_bound flag from CSV join ───────────────────────────


def test_lenient_upper_bound_flag_matches_csv_join(tmp_path: Path) -> None:
    resolutions_dir = tmp_path / "resolutions"
    _write_resolutions_csv(
        resolutions_dir,
        [
            # Market 1: end_date present → not lenient
            {"market_id": "1", "end_date": "2025-01-01T00:00:00+00:00", "resolved_at": "2024-12-31T00:00:00+00:00"},
            # Market 2: end_date empty, resolved_at present → lenient
            {"market_id": "2", "end_date": "", "resolved_at": "2024-12-31T00:00:00+00:00"},
            # Market 3: both empty → not lenient (no upper bound derivable)
            {"market_id": "3", "end_date": "", "resolved_at": ""},
        ],
    )
    config = _make_config(tmp_path)
    # Write manifest and bars for markets 1, 2, 3
    _write_manifest(
        config.bars_root,
        [
            {"market_id": "1", "status": "ok"},
            {"market_id": "2", "status": "ok"},
            {"market_id": "3", "status": "ok"},
        ],
    )
    for mid in [1, 2, 3]:
        ts = _contiguous(1_000_000, 80, 60)
        _write_bar_partition(config.bars_root, mid, "2001-09-09", ts)

    summary = run_qa(config, dry_run=False)
    assert summary["total_markets"] == 3

    mkt = pl.read_parquet(config.qa_market_path).sort("market_id")
    row1 = mkt.filter(pl.col("market_id") == 1)
    row2 = mkt.filter(pl.col("market_id") == 2)
    row3 = mkt.filter(pl.col("market_id") == 3)

    assert bool(row1["lenient_upper_bound"][0]) is False
    assert bool(row2["lenient_upper_bound"][0]) is True
    assert bool(row3["lenient_upper_bound"][0]) is False


# ── Test 11: unknown QA condition raises _UnknownQACondition ──────────────────


def test_unknown_condition_raises_not_writes_silent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Monkeypatch _split_into_windows so qa_one_market ends up in the usable
    branch with no matching reason (dropped_bar_count=0, window_count=1,
    usable_window_count=1, but we force usable not clean via an extra window).
    """
    config = _make_config(tmp_path, min_bars_per_window=1, min_bars_per_market=1)
    ts = _contiguous(1_000_000, 80, 60)
    df = _bars(ts)

    # Monkeypatch _split_into_windows to return two usable windows so that
    # qa_one_market enters the "usable" branch, then force all the reason
    # conditions to be False to trigger _UnknownQACondition.
    import pmbot.phase1_data.qa as qa_mod

    original_split = qa_mod._split_into_windows

    def patched_split(
        bars_df: pl.DataFrame, gap_threshold_seconds: int
    ) -> list[pl.DataFrame]:
        # Return two half-frames to simulate window_count > 1
        half = bars_df.height // 2
        return [bars_df[:half], bars_df[half:]]

    monkeypatch.setattr(qa_mod, "_split_into_windows", patched_split)

    # Now force the impossible condition: window_count>1 but also force
    # the "usable" path to have no reason by further patching. Instead of
    # that complexity, the simplest approach: the two windows each have 40 bars
    # (>= min_bars_per_window=1), so qa_status="usable", qa_reason="gap_split"
    # is assigned. That's fine — that test shows the normal usable path works.
    # For the unknown-condition path: directly monkeypatch the reason strings.
    monkeypatch.setattr(qa_mod, "_REASON_SOME_WINDOWS_REJECTED", "__sentinel__")
    monkeypatch.setattr(qa_mod, "_REASON_GAP_SPLIT", "__sentinel__")
    monkeypatch.setattr(qa_mod, "_REASON_BARS_DROPPED", "__sentinel__")

    with pytest.raises(_UnknownQACondition):
        qa_one_market(1, df, config)

    # Restore original
    monkeypatch.setattr(qa_mod, "_split_into_windows", original_split)


# ── Test 12: run_id and completed_at consistent across ledgers ────────────────


def test_run_id_and_completed_at_match_across_ledgers(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    _write_manifest(config.bars_root, [{"market_id": "42", "status": "ok"}])
    _write_bar_partition(config.bars_root, 42, "2001-09-09", _contiguous(1_000_000, 80, 60))

    run_qa(config, dry_run=False)

    market_df = pl.read_parquet(config.qa_market_path)
    windows_df = pl.read_parquet(config.qa_windows_path)

    assert market_df.height == 1
    mkt_run_id = market_df["run_id"][0]
    mkt_completed_at = market_df["completed_at"][0]
    assert mkt_run_id is not None and len(str(mkt_run_id)) == 32  # uuid hex
    assert windows_df.height >= 1
    assert all(str(r) == str(mkt_run_id) for r in windows_df["run_id"].to_list())
    assert all(str(c) == str(mkt_completed_at) for c in windows_df["completed_at"].to_list())


# ── Test 13: --calibrate writes distributions, not clean bars ─────────────────


def test_calibrate_only_writes_distributions_no_clean_bars(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    _write_manifest(config.bars_root, [{"market_id": "1", "status": "ok"}])
    _write_bar_partition(config.bars_root, 1, "2001-09-09", _contiguous(1_000_000, 80, 60))

    from pmbot.phase1_data.qa import compute_distributions

    compute_distributions(config)

    assert config.qa_distributions_path.exists()
    assert not config.qa_market_path.exists()
    assert not config.qa_windows_path.exists()
    assert not config.bars_clean_root.exists()


# ── Test 14: --dry-run writes nothing ─────────────────────────────────────────


def test_dry_run_writes_nothing(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    _write_manifest(config.bars_root, [{"market_id": "1", "status": "ok"}])
    _write_bar_partition(config.bars_root, 1, "2001-09-09", _contiguous(1_000_000, 80, 60))

    run_qa(config, dry_run=True)

    assert not config.qa_market_path.exists()
    assert not config.qa_windows_path.exists()
    assert not config.bars_clean_root.exists()


# ── Test 15: clean bars exclude rejected-window bars ─────────────────────────


def test_clean_bars_excludes_rejected_window_bars(tmp_path: Path) -> None:
    """Usable window (80 bars) + rejected window (10 bars) separated by >300s gap.

    bars_clean/ must contain only the usable window's bars.
    _qa_windows.parquet must record both windows with correct usable flags.
    _qa_market.parquet must show qa_status=usable and dropped_bar_count matching.
    """
    config = _make_config(
        tmp_path,
        gap_threshold_seconds=300,
        min_bars_per_window=60,
        min_bars_per_market=1,
        max_rejected_window_fraction=0.75,
    )

    # Usable window: 80 bars starting at t=1_000_000
    usable_ts = _contiguous(1_000_000, 80, 60)
    # Rejected window: 10 bars, 301s after the last usable bar
    rej_start = usable_ts[-1] + 301
    rejected_ts = _contiguous(rej_start, 10, 60)
    all_ts = usable_ts + rejected_ts

    # Write bars to a single partition (simplification — real data spans utc_dates)
    date_str = "2001-09-09"
    market_id = 7777
    _write_manifest(config.bars_root, [{"market_id": str(market_id), "status": "ok"}])
    _write_bar_partition(config.bars_root, market_id, date_str, all_ts)

    run_qa(config, dry_run=False)

    # (a) bars_clean/ contains only the usable window's bars
    clean_files = list(
        (config.bars_clean_root / f"market_id={market_id}").rglob("*.parquet")
    )
    assert clean_files, "bars_clean/ must have at least one partition file"
    clean_df = pl.concat([pl.read_parquet(f) for f in clean_files]).sort("t")
    assert clean_df.height == 80, (
        f"Expected 80 clean bars (usable window only), got {clean_df.height}"
    )
    clean_ts_set = set(clean_df["t"].to_list())
    assert clean_ts_set == set(usable_ts), "Clean bars must be exactly the usable window"
    assert not clean_ts_set.intersection(rejected_ts), "Rejected window bars leaked into clean"

    # (b) _qa_windows.parquet shows both windows with correct flags
    win_df = pl.read_parquet(config.qa_windows_path).filter(
        pl.col("market_id") == market_id
    ).sort("window_idx")
    assert win_df.height == 2
    assert bool(win_df["usable"][0]) is True
    assert bool(win_df["usable"][1]) is False
    assert str(win_df["reason"][1]) == "window_too_short"

    # (c) _qa_market.parquet shows qa_status=usable and correct reason
    mkt_df = pl.read_parquet(config.qa_market_path).filter(
        pl.col("market_id") == market_id
    )
    assert str(mkt_df["qa_status"][0]) == "usable"
    assert str(mkt_df["qa_reason"][0]) == "some_windows_rejected"
    # dropped_bar_count is the price-drop count only (0 here); the rejected-window
    # bars are NOT counted as "dropped" (they were clean price-wise but in bad window)
    assert int(mkt_df["dropped_bar_count"][0]) == 0
    # total_usable_bar_count = usable window's bar count
    assert int(mkt_df["total_usable_bar_count"][0]) == 80
