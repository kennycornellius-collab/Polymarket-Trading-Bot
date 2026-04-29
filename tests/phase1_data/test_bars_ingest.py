"""Tests for Phase 1.1 Pass 2 — bars_ingest.py.

All unit tests mock at the urllib boundary; no real network calls except the
integration test (gated with @pytest.mark.integration).
"""

from __future__ import annotations

import csv
import json
import statistics
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from pmbot.phase1_data.bars_ingest import (
    IngestConfig,
    _Bar,
    _GammaMarketDetail,
    _collect_manifest_frames,
    _extract_yes_token_id,
    _write_manifest_temp,
    derive_window,
    run_ingest,
    validate_bars,
    write_bars,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture()
def tmp_config(tmp_path: Path) -> IngestConfig:
    bars_dir = tmp_path / "bars"
    return IngestConfig(
        target_path_root=bars_dir,
        manifest_path=bars_dir / "_manifest.parquet",
        lookup_path=bars_dir / "_market_lookup.parquet",
    )


@pytest.fixture()
def minimal_csv(tmp_path: Path) -> Path:
    """Two training-eligible markets with distinct timestamps."""
    csv_path = tmp_path / "resolved_markets.csv"
    fields = [
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
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerow(
            {
                "market_id": "100",
                "question": "Will BTC break $100k?",
                "slug": "btc-100k",
                "outcome": "YES",
                "resolved_at": "2024-01-10T12:00:00+00:00",
                "end_date": "2024-01-15T00:00:00+00:00",
                "volume_lifetime_usdc": "50000",
                "outcome_prices_raw": '["1", "0"]',
                "flags": "",
            }
        )
        writer.writerow(
            {
                "market_id": "200",
                "question": "Will BTC break $90k?",
                "slug": "btc-90k",
                "outcome": "NO",
                "resolved_at": "2024-01-20T12:00:00+00:00",
                "end_date": "2024-01-25T00:00:00+00:00",
                "volume_lifetime_usdc": "30000",
                "outcome_prices_raw": '["0", "1"]',
                "flags": "",
            }
        )
    return csv_path


@pytest.fixture()
def minimal_lookup(tmp_config: IngestConfig) -> None:
    """Write a minimal lookup Parquet covering markets 100 and 200."""
    _write_lookup_parquet_fixture(
        [
            {"market_id": "100", "yes_token_id": "tok_yes_100", "created_at": "2024-01-01T00:00:00+00:00"},
            {"market_id": "200", "yes_token_id": "tok_yes_200", "created_at": "2024-01-10T00:00:00+00:00"},
        ],
        tmp_config.lookup_path,
    )


def _write_lookup_parquet_fixture(rows: list[dict[str, str]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "market_id": [r["market_id"] for r in rows],
            "yes_token_id": [r["yes_token_id"] for r in rows],
            "created_at": [r["created_at"] for r in rows],
        }
    ).write_parquet(path)


def _make_bars(count: int, start_t: int = 1_700_000_000, dt: int = 60) -> list[_Bar]:
    return [{"t": float(start_t + i * dt), "p": 0.5} for i in range(count)]


def _make_bars_with_dt(dts: list[int], start_t: int = 1_700_000_000) -> list[_Bar]:
    """Build a bar series with specific inter-bar deltas."""
    bars: list[_Bar] = [{"t": float(start_t), "p": 0.5}]
    for dt in dts:
        bars.append({"t": float(bars[-1]["t"]) + dt, "p": 0.5})
    return bars


# ── Window derivation ─────────────────────────────────────────────────────────


def test_window_derivation(tmp_config: IngestConfig) -> None:
    """start_ts = createdAt; end_ts = resolved_at + padding when resolved_at < end_date."""
    start_ts, end_ts = derive_window(
        "2024-01-01T00:00:00+00:00",
        "2024-01-15T00:00:00+00:00",
        "2024-01-10T12:00:00+00:00",
        tmp_config,
    )
    assert start_ts == 1_704_067_200  # 2024-01-01 00:00:00 UTC
    expected_end = 1_704_888_000 + tmp_config.upper_padding_seconds  # 2024-01-10 12:00:00 UTC + 60
    assert end_ts == expected_end


def test_window_uses_resolved_at_when_earlier(tmp_config: IngestConfig) -> None:
    _, end_ts = derive_window(
        "2024-01-01T00:00:00+00:00",
        "2024-01-20T00:00:00+00:00",
        "2024-01-10T00:00:00+00:00",
        tmp_config,
    )
    resolved_unix = 1_704_844_800  # 2024-01-10 00:00:00 UTC
    assert end_ts == resolved_unix + tmp_config.upper_padding_seconds


def test_window_uses_end_date_when_earlier(tmp_config: IngestConfig) -> None:
    _, end_ts = derive_window(
        "2024-01-01T00:00:00+00:00",
        "2024-01-10T00:00:00+00:00",
        "2024-01-20T00:00:00+00:00",
        tmp_config,
    )
    end_date_unix = 1_704_844_800  # 2024-01-10 00:00:00 UTC
    assert end_ts == end_date_unix + tmp_config.upper_padding_seconds


# ── Manifest schema ───────────────────────────────────────────────────────────


def test_manifest_schema_success(tmp_config: IngestConfig) -> None:
    """_write_manifest_temp writes all 9 columns correctly on the ok path."""
    _write_manifest_temp(
        tmp_config,
        run_id="abc123",
        market_id="42",
        status="ok",
        bar_count=500,
        first_ts=1_000_000,
        last_ts=2_000_000,
        error_reason=None,
        attempt_count=1,
    )
    path = tmp_config.target_path_root / "_manifest_abc123_42.parquet"
    assert path.exists()
    df = pl.read_parquet(path)
    assert set(df.columns) == {
        "market_id", "status", "bar_count", "first_ts", "last_ts",
        "error_reason", "run_id", "completed_at", "attempt_count",
    }
    row = df.row(0, named=True)
    assert row["market_id"] == "42"
    assert row["status"] == "ok"
    assert row["bar_count"] == 500
    assert row["first_ts"] == 1_000_000
    assert row["last_ts"] == 2_000_000
    assert row["error_reason"] is None
    assert row["run_id"] == "abc123"
    assert row["attempt_count"] == 1


def test_manifest_schema_fail(tmp_config: IngestConfig) -> None:
    """Fail path: error_reason non-null, bar_count=0."""
    _write_manifest_temp(
        tmp_config,
        run_id="def456",
        market_id="99",
        status="fail",
        bar_count=0,
        first_ts=0,
        last_ts=0,
        error_reason="bar_count_below_min",
        attempt_count=1,
    )
    path = tmp_config.target_path_root / "_manifest_def456_99.parquet"
    df = pl.read_parquet(path)
    row = df.row(0, named=True)
    assert row["status"] == "fail"
    assert row["bar_count"] == 0
    assert row["error_reason"] == "bar_count_below_min"


# ── Resume behavior ───────────────────────────────────────────────────────────


def test_manifest_resume_increments_attempt(
    tmp_config: IngestConfig, minimal_csv: Path, minimal_lookup: None
) -> None:
    """status=fail on prior run → retried with attempt_count incremented by 1."""
    # Simulate a prior run that failed for market 100
    _write_manifest_temp(
        tmp_config,
        run_id="prior_run",
        market_id="100",
        status="fail",
        bar_count=5,
        first_ts=0,
        last_ts=0,
        error_reason="bar_count_below_min",
        attempt_count=1,
    )
    # Consolidate so _load_resume_state can read it
    from pmbot.phase1_data.bars_ingest import _consolidate_manifest
    _consolidate_manifest(tmp_config)

    bars = _make_bars(count=200)
    clob_resp = {"history": bars}

    def fake_urlopen(req: Any, **kwargs: Any) -> Any:
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(clob_resp).encode()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        return mock_resp

    with patch("pmbot.phase1_data.bars_ingest.ensure_lookup", return_value=[]):
        with patch("pmbot.phase1_data.bars_ingest.urllib.request.urlopen", side_effect=fake_urlopen):
            run_ingest(minimal_csv, tmp_config, resume=True)

    df = pl.read_parquet(tmp_config.manifest_path)
    row_100 = df.filter(pl.col("market_id") == "100").row(0, named=True)
    assert row_100["attempt_count"] == 2


def test_manifest_resume_skips_ok(
    tmp_config: IngestConfig, minimal_csv: Path, minimal_lookup: None
) -> None:
    """status=ok on prior run → market is not re-fetched."""
    # Simulate a prior successful run for market 100
    _write_manifest_temp(
        tmp_config,
        run_id="prior_run",
        market_id="100",
        status="ok",
        bar_count=500,
        first_ts=1_700_000_000,
        last_ts=1_700_030_000,
        error_reason=None,
        attempt_count=1,
    )
    from pmbot.phase1_data.bars_ingest import _consolidate_manifest
    _consolidate_manifest(tmp_config)

    call_log: list[str] = []

    def fake_urlopen(req: Any, **kwargs: Any) -> Any:
        call_log.append(str(req.full_url))
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps({"history": _make_bars(200)}).encode()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        return mock_resp

    with patch("pmbot.phase1_data.bars_ingest.ensure_lookup", return_value=[]):
        with patch("pmbot.phase1_data.bars_ingest.urllib.request.urlopen", side_effect=fake_urlopen):
            run_ingest(minimal_csv, tmp_config, resume=True)

    # No CLOB call should have been made for tok_yes_100
    clob_calls = [u for u in call_log if "tok_yes_100" in u]
    assert not clob_calls, f"Expected no CLOB call for market 100, got: {clob_calls}"


# ── Hard-fail predicates ──────────────────────────────────────────────────────


def test_hardfail_low_bar_count(tmp_config: IngestConfig) -> None:
    """8 bars is below min_bars=10 → rejected."""
    bars = _make_bars(count=8)
    valid, reason = validate_bars(bars, 1_700_000_000, 1_700_500_000, tmp_config)
    assert not valid
    assert reason == "bar_count_below_min"


def test_hardfail_high_median_dt(tmp_config: IngestConfig) -> None:
    """Median inter-bar gap of 3600s exceeds 300s threshold → rejected."""
    # 20 bars at 3600s intervals
    bars = _make_bars(count=20, dt=3600)
    valid, reason = validate_bars(bars, 1_700_000_000, 1_700_500_000, tmp_config)
    assert not valid
    assert reason == "median_dt_exceeds_threshold"


def test_hardfail_bar_outside_window(tmp_config: IngestConfig) -> None:
    """Bar at end_ts + 300s (padding=60) → outside window → rejected."""
    start_ts = 1_700_000_000
    end_ts = 1_700_030_000
    bars = _make_bars(count=20, start_t=start_ts, dt=60)
    # Inject an out-of-window bar
    bars.append({"t": float(end_ts + 300), "p": 0.5})
    valid, reason = validate_bars(bars, start_ts, end_ts, tmp_config)
    assert not valid
    assert reason == "bar_outside_window"


def test_accept_real_thin_market(tmp_config: IngestConfig) -> None:
    """5000 bars, median=60s, max gap=1800s → accepted (max gap not a predicate)."""
    start_t = 1_700_000_000
    # Build a series where most gaps are 60s but one is 1800s
    dts = [60] * 4998 + [1800]
    bars = _make_bars_with_dt(dts, start_t=start_t)
    assert len(bars) == 5000

    med = statistics.median([bars[i + 1]["t"] - bars[i]["t"] for i in range(len(bars) - 1)])
    assert med == 60.0

    end_ts = int(bars[-1]["t"]) + 60
    valid, reason = validate_bars(bars, start_t, end_ts, tmp_config)
    assert valid, f"Expected valid, got reason={reason!r}"


# ── Atomic write crash safety ─────────────────────────────────────────────────


def test_atomic_write_crash_safety(tmp_config: IngestConfig) -> None:
    """.tmp file remains and original is untouched if os.replace raises."""
    target = tmp_config.target_path_root / "_manifest_crash_test_99.parquet"
    assert not target.exists()

    with patch("pmbot.phase1_data.bars_ingest.os.replace", side_effect=OSError("disk full")):
        with pytest.raises(OSError, match="disk full"):
            _write_manifest_temp(
                tmp_config,
                run_id="crash_test",
                market_id="99",
                status="ok",
                bar_count=1,
                first_ts=0,
                last_ts=0,
                error_reason=None,
                attempt_count=1,
            )

    tmp_file = Path(str(target) + ".tmp")
    assert tmp_file.exists(), ".tmp file should remain after crash"
    assert not target.exists(), "Final file must not exist after aborted rename"


# ── YES token selection ───────────────────────────────────────────────────────


def test_yes_token_selection() -> None:
    """YES token is selected by index position in outcomes, not assumed to be index 0."""
    # outcomes = ["No", "Yes"] → YES is index 1
    record: _GammaMarketDetail = {
        "id": "42",
        "outcomes": '["No", "Yes"]',
        "clobTokenIds": '["no_token_id", "yes_token_id"]',
        "createdAt": "2024-01-01T00:00:00Z",
    }
    yes_token = _extract_yes_token_id(record, "42")
    assert yes_token == "yes_token_id"


def test_yes_token_selection_standard_order() -> None:
    """Works when YES is at index 0 too."""
    record: _GammaMarketDetail = {
        "id": "43",
        "outcomes": '["Yes", "No"]',
        "clobTokenIds": '["yes_token_id", "no_token_id"]',
        "createdAt": "2024-01-01T00:00:00Z",
    }
    assert _extract_yes_token_id(record, "43") == "yes_token_id"


# ── Missing createdAt ─────────────────────────────────────────────────────────


def test_missing_created_at_hard_fails(
    tmp_config: IngestConfig, minimal_csv: Path
) -> None:
    """createdAt absent from Gamma response → manifest status=fail, error_reason='missing_created_at'."""
    gamma_record_no_created_at = {
        "id": "100",
        "outcomes": '["Yes", "No"]',
        "clobTokenIds": '["tok_yes", "tok_no"]',
        # createdAt intentionally absent
    }

    def fake_urlopen(req: Any, **kwargs: Any) -> Any:
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(gamma_record_no_created_at).encode()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        return mock_resp

    with patch("pmbot.phase1_data.bars_ingest.urllib.request.urlopen", side_effect=fake_urlopen):
        run_ingest(minimal_csv, tmp_config, rebuild_lookup=True)

    # Manifest must exist and market 100 must have status=fail
    manifest_frames = _collect_manifest_frames(tmp_config)
    assert manifest_frames, "Expected at least one manifest frame"
    df = pl.concat(manifest_frames)
    row_100 = df.filter(pl.col("market_id") == "100")
    assert row_100.height == 1, "Expected exactly one manifest row for market 100"
    row = row_100.row(0, named=True)
    assert row["status"] == "fail"
    assert row["error_reason"] == "missing_created_at"


# ── Lookup join missing market ────────────────────────────────────────────────


def test_lookup_join_missing_market(
    tmp_config: IngestConfig, minimal_csv: Path
) -> None:
    """Market absent from lookup after ensure_lookup (with no unresolved explanation) → hard-fail."""
    # Lookup covers market 200 only — market 100 is absent
    _write_lookup_parquet_fixture(
        [{"market_id": "200", "yes_token_id": "tok_yes_200", "created_at": "2024-01-10T00:00:00+00:00"}],
        tmp_config.lookup_path,
    )

    # ensure_lookup returns [] (no unresolved) — means it thinks everything is covered
    with patch("pmbot.phase1_data.bars_ingest.ensure_lookup", return_value=[]):
        with pytest.raises(RuntimeError, match="absent from lookup"):
            run_ingest(minimal_csv, tmp_config)


# ── Partition column stripping ────────────────────────────────────────────────


def test_partition_column_stripping(tmp_config: IngestConfig) -> None:
    """Written Parquet files contain exactly {t, p} — no market_id, no utc_date."""
    bars = _make_bars(count=50, start_t=1_704_067_200, dt=60)  # 2024-01-01
    write_bars(bars, "100", tmp_config)

    parquet_files = list(tmp_config.target_path_root.rglob("*.parquet"))
    assert parquet_files, "Expected at least one Parquet file to be written"
    for pf in parquet_files:
        if "_manifest" in pf.name:
            continue
        df = pl.read_parquet(pf)
        assert set(df.columns) == {"t", "p"}, (
            f"File {pf} has columns {df.columns}, expected exactly {{t, p}}"
        )


# ── Integration test ──────────────────────────────────────────────────────────


@pytest.mark.integration
def test_integration_real_clob(tmp_config: IngestConfig, tmp_path: Path) -> None:
    """Fetch bars for one known-good market end-to-end.

    Market 1817348: BTC $74k-$76k band, resolved 2026-04-08.
    Verified by spike 2026-04-29: 10,007 bars, 60s median dt.
    """
    import json as _json
    import urllib.request as ureq
    from datetime import datetime, timezone

    from pmbot.phase1_data.bars_ingest import fetch_bars as _fetch

    # Fetch live market metadata from Gamma to get token ID and timestamps
    gamma_req = ureq.Request(
        "https://gamma-api.polymarket.com/markets/1817348",
        headers={"User-Agent": "pmbot/0.1"},
    )
    with ureq.urlopen(gamma_req, timeout=30) as resp:
        record = _json.loads(resp.read())

    outcomes = _json.loads(record["outcomes"])
    token_ids: list[str] = _json.loads(record["clobTokenIds"])
    yes_index = next(i for i, o in enumerate(outcomes) if o.strip().lower() == "yes")
    live_yes_token = token_ids[yes_index]

    # Derive window from live Gamma timestamps (same logic as derive_window)
    created_at = record["createdAt"].replace("Z", "+00:00")
    start_ts = int(datetime.fromisoformat(created_at).timestamp())
    end_date_str = record["endDate"].replace("Z", "+00:00")
    end_dt = datetime.fromisoformat(end_date_str).replace(tzinfo=timezone.utc)
    end_ts = int(end_dt.timestamp()) + tmp_config.upper_padding_seconds

    bars = _fetch(live_yes_token, start_ts, end_ts, tmp_config)

    assert len(bars) > 100, f"Expected >100 bars, got {len(bars)}"

    deltas = [bars[i + 1]["t"] - bars[i]["t"] for i in range(len(bars) - 1)]
    med = statistics.median(deltas)
    assert 55 <= med <= 65, f"Expected median dt in [55,65], got {med}"

    upper_bound = end_ts + tmp_config.upper_padding_seconds
    for bar in bars:
        assert bar["t"] >= start_ts, f"Bar {bar['t']} < start_ts {start_ts}"
        assert bar["t"] <= upper_bound, f"Bar {bar['t']} > upper_bound {upper_bound}"

    # Write manifest and verify
    _write_manifest_temp(
        tmp_config,
        run_id="integration",
        market_id="1817348",
        status="ok",
        bar_count=len(bars),
        first_ts=int(bars[0]["t"]),
        last_ts=int(bars[-1]["t"]),
        error_reason=None,
        attempt_count=1,
    )
    df = pl.read_parquet(
        tmp_config.target_path_root / "_manifest_integration_1817348.parquet"
    )
    row = df.row(0, named=True)
    assert row["status"] == "ok"
    assert row["bar_count"] == len(bars)
    assert row["first_ts"] == int(bars[0]["t"])
    assert row["last_ts"] == int(bars[-1]["t"])
