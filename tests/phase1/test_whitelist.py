from __future__ import annotations

import csv
import json
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest

from pmbot.phase0_filter import FilterConfig
from pmbot.phase1_data.whitelist import (
    GammaMarketRecord,
    WhitelistConfig,
    build_whitelist,
    compute_tte_days,
    gamma_record_to_market_metadata,
    infer_market_type,
    infer_strike_type,
    infer_underlying,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _future(days: float) -> str:
    return (_now_utc() + timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _record(**overrides: Any) -> GammaMarketRecord:
    base: dict[str, Any] = {
        "id": "market-001",
        "question": "Will BTC hit $100k by March?",
        "slug": "btc-100k-march",
        "outcomes": '["Yes","No"]',
        "endDate": _future(15),
        "volume24hr": 50000.0,
        "tags": [{"id": "1", "label": "BTC", "slug": "btc"}],
    }
    base.update(overrides)
    return base  # type: ignore[return-value]


def _make_urlopen_cm(records: list[dict[str, Any]]) -> MagicMock:
    """Return a mock context manager whose .read() yields JSON-encoded records."""
    cm = MagicMock()
    cm.__enter__.return_value.read.return_value = json.dumps(records).encode("utf-8")
    cm.__exit__.return_value = False
    return cm


def _http_error(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError("http://test", code, f"Error {code}", MagicMock(), None)


# ── infer_market_type ─────────────────────────────────────────────────────────


def test_infer_market_type_yes_no() -> None:
    assert infer_market_type(_record(outcomes='["Yes","No"]')) == "binary"


def test_infer_market_type_no_yes_order() -> None:
    assert infer_market_type(_record(outcomes='["No","Yes"]')) == "binary"


def test_infer_market_type_case_insensitive() -> None:
    assert infer_market_type(_record(outcomes='["YES","NO"]')) == "binary"


def test_infer_market_type_up_down() -> None:
    assert infer_market_type(_record(outcomes='["Up","Down"]')) == "non_binary"


def test_infer_market_type_three_way() -> None:
    assert infer_market_type(_record(outcomes='["A","B","C"]')) == "non_binary"


def test_infer_market_type_non_yes_no_labels() -> None:
    assert infer_market_type(_record(outcomes='["Higher","Lower"]')) == "non_binary"


def test_infer_market_type_malformed_json() -> None:
    assert infer_market_type(_record(outcomes="not json")) == "non_binary"


# ── infer_underlying ──────────────────────────────────────────────────────────


def test_infer_underlying_btc_in_question() -> None:
    assert infer_underlying(_record(question="Will BTC hit $100k", slug="other")) == "BTC"


def test_infer_underlying_bitcoin_in_question() -> None:
    assert infer_underlying(_record(question="Bitcoin price by EOY", slug="other")) == "BTC"


def test_infer_underlying_btc_in_slug() -> None:
    assert infer_underlying(_record(question="crypto market", slug="btc-100k-march")) == "BTC"


def test_infer_underlying_btc_in_tags() -> None:
    tags: list[dict[str, str]] = [{"id": "1", "label": "BTC", "slug": "btc"}]
    assert infer_underlying(_record(question="crypto", slug="other", tags=tags)) == "BTC"


def test_infer_underlying_eth() -> None:
    assert infer_underlying(_record(question="ETH to $5k", slug="eth-5k", tags=[])) == "other"


def test_infer_underlying_empty() -> None:
    assert infer_underlying(_record(question="", slug="", tags=[])) == "other"


def test_infer_underlying_case_insensitive() -> None:
    assert infer_underlying(_record(question="will bitcoin reach 100k", slug="x")) == "BTC"


# ── infer_strike_type ─────────────────────────────────────────────────────────


def test_infer_strike_dollar_k() -> None:
    assert infer_strike_type(_record(question="Will BTC hit $100k")) == "absolute"


def test_infer_strike_dollar_comma() -> None:
    assert infer_strike_type(_record(question="Will BTC reach $100,000")) == "absolute"


def test_infer_strike_dollar_m() -> None:
    assert infer_strike_type(_record(question="Will BTC hit $1M")) == "absolute"


def test_infer_strike_dollar_decimal() -> None:
    assert infer_strike_type(_record(question="Will BTC exceed $95,000.00")) == "absolute"


def test_infer_strike_rise_percent() -> None:
    assert infer_strike_type(_record(question="Will BTC rise 5% this month")) == "percentage"


def test_infer_strike_fall_percent() -> None:
    assert infer_strike_type(_record(question="Will BTC fall 10% by EOY")) == "percentage"


def test_infer_strike_up_percent() -> None:
    assert infer_strike_type(_record(question="Will BTC go up 20%")) == "percentage"


def test_infer_strike_ambiguous() -> None:
    assert infer_strike_type(_record(question="Will BTC recover by March")) == "unknown"


# ── compute_tte_days ──────────────────────────────────────────────────────────


def test_compute_tte_future() -> None:
    now = datetime(2026, 4, 19, 12, 0, 0, tzinfo=timezone.utc)
    record = _record(endDate="2026-04-24T12:00:00Z")
    assert abs(compute_tte_days(record, now) - 5.0) < 0.01


def test_compute_tte_past() -> None:
    now = datetime(2026, 4, 19, 12, 0, 0, tzinfo=timezone.utc)
    record = _record(endDate="2026-04-18T12:00:00Z")
    assert compute_tte_days(record, now) < 0


def test_compute_tte_utc_aware() -> None:
    now = datetime(2026, 4, 19, 0, 0, 0, tzinfo=timezone.utc)
    record = _record(endDate="2026-04-29T00:00:00+00:00")
    result = compute_tte_days(record, now)
    assert abs(result - 10.0) < 0.01


# ── Adapter ───────────────────────────────────────────────────────────────────


def test_adapter_full_record() -> None:
    now = datetime(2026, 4, 19, 12, 0, 0, tzinfo=timezone.utc)
    record = _record(
        id="mkt-42",
        question="Will BTC hit $100k by March?",
        slug="btc-100k-march",
        outcomes='["Yes","No"]',
        endDate="2026-05-04T12:00:00Z",  # 15 days from now
        volume24hr=75000.5,
        tags=[{"id": "1", "label": "BTC", "slug": "btc"}],
    )
    meta = gamma_record_to_market_metadata(record, now)
    assert meta.market_id == "mkt-42"
    assert meta.market_type == "binary"
    assert meta.underlying == "BTC"
    assert meta.strike_type == "absolute"
    assert abs(meta.tte_days - 15.0) < 0.01
    assert meta.daily_volume_usdc == 75000.5


def test_adapter_explicit_zero_volume() -> None:
    now = _now_utc()
    meta = gamma_record_to_market_metadata(_record(volume24hr=0.0), now)
    assert meta.daily_volume_usdc == 0.0


def test_adapter_missing_volume_and_null_tags() -> None:
    """Missing volume24hr → 0.0; tags=None → no crash. Filter rejects via volume_below_threshold."""
    from pmbot.phase0_filter import FilterConfig, is_qualified_btc_market

    now = _now_utc()
    record: GammaMarketRecord = {  # type: ignore[typeddict-item]
        "id": "no-vol",
        "question": "Will BTC hit $100k by May?",
        "slug": "btc-100k-may",
        "outcomes": '["Yes","No"]',
        "endDate": _future(15),
        "tags": None,
    }
    meta = gamma_record_to_market_metadata(record, now)
    assert meta.daily_volume_usdc == 0.0

    result = is_qualified_btc_market(meta, FilterConfig())
    assert not result.qualified
    assert "volume_below_threshold" in result.reasons
    assert "malformed_record" not in result.reasons


# ── build_whitelist (mocked HTTP) ─────────────────────────────────────────────


def _make_fixture_records() -> list[dict[str, Any]]:
    """8 records: 3 qualifying, 4 rejected (one per reason), 1 malformed."""
    return [
        # qualifying
        _record(id="q1", question="Will BTC hit $100k by May?", volume24hr=50000.0),
        _record(id="q2", question="Will BTC exceed $110k by June?", volume24hr=20000.0),
        _record(id="q3", question="Will BTC reach $90k by April?", volume24hr=15000.0),
        # wrong market type (up/down)
        _record(
            id="r1", question="BTC up or down $100k?", outcomes='["Up","Down"]', volume24hr=30000.0
        ),
        # wrong underlying
        _record(id="r2", question="Will ETH hit $5k?", slug="eth-5k", tags=[], volume24hr=30000.0),
        # wrong strike type (percentage move — but no dollar sign)
        _record(id="r3", question="Will BTC rise 10%?", volume24hr=30000.0),
        # tte out of range (45 days)
        _record(id="r4", question="Will BTC hit $100k?", endDate=_future(45), volume24hr=30000.0),
        # malformed (missing required field)
        {"id": "m1"},  # no question/outcomes/endDate/volume24hr
    ]


def _cfg(tmp_path: Path, fetch_limit: int = 10) -> WhitelistConfig:
    return WhitelistConfig(
        output_csv_path=tmp_path / "out" / "whitelist.csv",
        fetch_limit=fetch_limit,
        inter_request_delay_s=0.0,
        max_retries=2,
        retry_base_delay_s=0.0,
    )


def test_build_whitelist_e2e_mocked(tmp_path: Path) -> None:
    records = _make_fixture_records()
    cfg = _cfg(tmp_path)

    with patch("pmbot.phase1_data.whitelist.urllib.request.urlopen") as mock_url:
        with patch("pmbot.phase1_data.whitelist.time.sleep"):
            mock_url.side_effect = [
                _make_urlopen_cm(records),  # page 0: 8 records < fetch_limit=10 → break
            ]
            result = build_whitelist(cfg, FilterConfig())

    assert result.qualified_count == 3
    assert result.rejected_count == 5  # 4 filtered + 1 malformed
    assert result.total_markets_seen == 8
    assert result.rejection_reasons.get("malformed_record", 0) == 1
    assert result.rejection_reasons.get("wrong_market_type", 0) == 1
    assert result.rejection_reasons.get("wrong_underlying", 0) == 1


def test_build_whitelist_csv_columns(tmp_path: Path) -> None:
    records = [_record(id="q1")]
    cfg = _cfg(tmp_path)

    with patch("pmbot.phase1_data.whitelist.urllib.request.urlopen") as mock_url:
        with patch("pmbot.phase1_data.whitelist.time.sleep"):
            mock_url.side_effect = [_make_urlopen_cm(records)]
            result = build_whitelist(cfg, FilterConfig())

    assert result.qualified_count == 1
    with result.output_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)

    assert len(rows) == 1
    row = rows[0]
    expected_cols = {
        "market_id",
        "slug",
        "question",
        "resolution_date_utc",
        "tte_days",
        "daily_volume_usdc",
        "qualified_at_utc",
    }
    assert expected_cols <= set(row.keys())
    # qualified_at_utc must be a parseable ISO 8601 datetime
    dt = datetime.fromisoformat(row["qualified_at_utc"])
    assert dt.tzinfo is not None


def test_build_whitelist_creates_parent_dirs(tmp_path: Path) -> None:
    cfg = WhitelistConfig(
        output_csv_path=tmp_path / "deep" / "nested" / "whitelist.csv",
        fetch_limit=10,
        inter_request_delay_s=0.0,
        max_retries=0,
        retry_base_delay_s=0.0,
    )
    with patch("pmbot.phase1_data.whitelist.urllib.request.urlopen") as mock_url:
        with patch("pmbot.phase1_data.whitelist.time.sleep"):
            mock_url.side_effect = [_make_urlopen_cm([])]
            build_whitelist(cfg, FilterConfig())

    assert (tmp_path / "deep" / "nested").is_dir()
    assert cfg.output_csv_path.exists()


def test_build_whitelist_malformed_record_continues(tmp_path: Path) -> None:
    records: list[dict[str, Any]] = [
        {"id": "bad"},  # malformed — missing required fields
        _record(id="q1"),  # valid and qualifying
    ]
    cfg = _cfg(tmp_path)

    with patch("pmbot.phase1_data.whitelist.urllib.request.urlopen") as mock_url:
        with patch("pmbot.phase1_data.whitelist.time.sleep"):
            mock_url.side_effect = [_make_urlopen_cm(records)]
            result = build_whitelist(cfg, FilterConfig())

    assert result.qualified_count == 1
    assert result.rejection_reasons.get("malformed_record", 0) == 1
    assert result.total_markets_seen == 2


def test_fetch_markets_page_sends_user_agent(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    captured: list[urllib.request.Request] = []

    def fake_urlopen(req: urllib.request.Request, **_: object) -> MagicMock:
        captured.append(req)
        return _make_urlopen_cm([])

    with patch("pmbot.phase1_data.whitelist.urllib.request.urlopen", side_effect=fake_urlopen):
        with patch("pmbot.phase1_data.whitelist.time.sleep"):
            build_whitelist(cfg, FilterConfig())

    assert len(captured) >= 1
    sent_ua = captured[0].get_header("User-agent")
    assert sent_ua == cfg.user_agent


def test_build_whitelist_4xx_error(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)

    with patch("pmbot.phase1_data.whitelist.urllib.request.urlopen") as mock_url:
        with patch("pmbot.phase1_data.whitelist.time.sleep"):
            mock_url.side_effect = _http_error(404)
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                build_whitelist(cfg, FilterConfig())

    assert exc_info.value.code == 404
    assert mock_url.call_count == 1  # no retries


def test_build_whitelist_5xx_retries_then_raises(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)  # max_retries=2 → 3 total attempts

    with patch("pmbot.phase1_data.whitelist.urllib.request.urlopen") as mock_url:
        with patch("pmbot.phase1_data.whitelist.time.sleep") as mock_sleep:
            mock_url.side_effect = _http_error(500)
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                build_whitelist(cfg, FilterConfig())

    assert exc_info.value.code == 500
    assert mock_url.call_count == cfg.max_retries + 1  # 3 total attempts
    assert mock_sleep.call_count == cfg.max_retries  # sleep between each retry


def test_build_whitelist_5xx_recovers_on_retry(tmp_path: Path) -> None:
    records = [_record(id="q1")]
    cfg = _cfg(tmp_path)

    with patch("pmbot.phase1_data.whitelist.urllib.request.urlopen") as mock_url:
        with patch("pmbot.phase1_data.whitelist.time.sleep") as mock_sleep:
            mock_url.side_effect = [
                _http_error(500),  # first attempt fails
                _make_urlopen_cm(records),  # retry succeeds (1 qualifying record < limit → done)
            ]
            result = build_whitelist(cfg, FilterConfig())

    assert result.qualified_count == 1
    assert mock_url.call_count == 2
    # one sleep for the retry (inter_request_delay_s=0 so no page sleep)
    assert mock_sleep.call_count == 1
    assert mock_sleep.call_args == call(0.0)  # retry_base_delay_s * 2^0 = 0.0
