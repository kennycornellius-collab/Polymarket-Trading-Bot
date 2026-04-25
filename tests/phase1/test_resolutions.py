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

from pmbot.phase1_data.resolutions import (
    GammaClosedMarketRecord,
    ResolutionConfig,
    _derive_outcome_and_flags,
    build_resolution_record,
    build_resolution_whitelist,
    is_btc_binary_shape,
    write_resolution_csv,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

_END_DT = datetime(2023, 6, 17, 0, 0, 0, tzinfo=timezone.utc)


def _closed_time(dt: datetime) -> str:
    """Format as Gamma API closedTime: 'YYYY-MM-DD HH:MM:SS+00'."""
    return dt.strftime("%Y-%m-%d %H:%M:%S+00")


def _end_date(dt: datetime) -> str:
    """Format as Gamma API endDate: 'YYYY-MM-DDTHH:MM:SSZ'."""
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _record(**overrides: Any) -> GammaClosedMarketRecord:
    base: dict[str, Any] = {
        "id": "mkt-001",
        "question": "Will BTC hit $100k by March?",
        "slug": "btc-100k-march",
        "outcomes": '["Yes","No"]',
        "endDate": _end_date(_END_DT),
        "outcomePrices": '["0","1"]',
        "closedTime": _closed_time(_END_DT - timedelta(hours=1)),
        "volumeNum": 24190.0,
        "umaResolutionStatuses": "[]",
        "tags": [{"id": "1", "label": "BTC", "slug": "btc"}],
    }
    base.update(overrides)
    return base  # type: ignore[return-value]


def _cfg(tmp_path: Path) -> ResolutionConfig:
    return ResolutionConfig(
        output_csv_path=tmp_path / "out" / "resolutions.csv",
        inter_request_delay_s=0.0,
        max_retries=2,
        retry_base_delay_s=0.0,
    )


def _make_urlopen_cm(records: list[dict[str, Any]]) -> MagicMock:
    cm = MagicMock()
    cm.__enter__.return_value.read.return_value = json.dumps(records).encode("utf-8")
    cm.__exit__.return_value = False
    return cm


def _http_error(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError("http://test", code, f"Error {code}", MagicMock(), None)


_CFG = ResolutionConfig()


# ── _derive_outcome_and_flags ─────────────────────────────────────────────────


def test_derive_yes_exact() -> None:
    outcome, flags = _derive_outcome_and_flags('["1","0"]', 0.99, 0.01)
    assert outcome == "YES"
    assert flags == []


def test_derive_no_exact() -> None:
    outcome, flags = _derive_outcome_and_flags('["0","1"]', 0.99, 0.01)
    assert outcome == "NO"
    assert flags == []


def test_derive_yes_float() -> None:
    outcome, flags = _derive_outcome_and_flags(
        '["0.9999998374530032", "0.0000001625469967"]', 0.99, 0.01
    )
    assert outcome == "YES"
    assert flags == []


def test_derive_no_float() -> None:
    outcome, flags = _derive_outcome_and_flags(
        '["0.0000001170843180", "0.9999998829156819"]', 0.99, 0.01
    )
    assert outcome == "NO"
    assert flags == []


def test_derive_canceled_zero_zero() -> None:
    outcome, flags = _derive_outcome_and_flags('["0","0"]', 0.99, 0.01)
    assert outcome == "INVALID"
    assert flags == ["invalid_resolution"]


def test_derive_ambiguous_5050() -> None:
    outcome, flags = _derive_outcome_and_flags(
        '["0.5768324844193577", "0.4231675155806422"]', 0.99, 0.01
    )
    assert outcome == "UNKNOWN"
    assert flags == ["ambiguous_resolution"]


def test_derive_malformed_json() -> None:
    outcome, flags = _derive_outcome_and_flags("not-json", 0.99, 0.01)
    assert outcome == "UNKNOWN"
    assert flags == ["malformed_prices"]


def test_derive_wrong_length() -> None:
    outcome, flags = _derive_outcome_and_flags('["1","0","0"]', 0.99, 0.01)
    assert outcome == "UNKNOWN"
    assert flags == ["malformed_prices"]


def test_derive_non_numeric_values() -> None:
    outcome, flags = _derive_outcome_and_flags('["yes","no"]', 0.99, 0.01)
    assert outcome == "UNKNOWN"
    assert flags == ["malformed_prices"]


def test_derive_price_error_flags_are_mutually_exclusive() -> None:
    """Only one of malformed_prices / invalid_resolution / ambiguous_resolution fires."""
    cases = [
        '["0","0"]',  # invalid_resolution
        "bad-json",  # malformed_prices
        '["0.5","0.5"]',  # ambiguous_resolution
    ]
    for raw in cases:
        _, flags = _derive_outcome_and_flags(raw, 0.99, 0.01)
        price_error_flags = [
            f
            for f in flags
            if f in {"malformed_prices", "invalid_resolution", "ambiguous_resolution"}
        ]
        assert len(price_error_flags) <= 1, f"Multiple price error flags for {raw!r}: {flags}"


# ── is_btc_binary_shape ───────────────────────────────────────────────────────


def test_shape_qualifies() -> None:
    assert is_btc_binary_shape(_record()) is True


def test_shape_non_binary_outcomes() -> None:
    assert is_btc_binary_shape(_record(outcomes='["Up","Down"]')) is False


def test_shape_non_btc_underlying() -> None:
    r = _record(question="Will ETH hit $5k?", slug="eth-5k", tags=[])
    assert is_btc_binary_shape(r) is False


def test_shape_no_absolute_strike() -> None:
    assert is_btc_binary_shape(_record(question="Will BTC rise 10%?")) is False


def test_shape_20k_30k_non_yes_no() -> None:
    r = _record(outcomes='["20k","30k"]', question="Will BTC hit $20k or $30k first?")
    assert is_btc_binary_shape(r) is False


# ── build_resolution_record ───────────────────────────────────────────────────


def test_record_clean_yes_float() -> None:
    r = _record(outcomePrices='["0.9999998374530032","0.0000001625469967"]')
    rec = build_resolution_record(r, _CFG)
    assert rec.outcome == "YES"
    assert rec.flags == ""
    assert rec.volume_lifetime_usdc == 24190.0


def test_record_clean_no_float() -> None:
    r = _record(outcomePrices='["0.0000001170843180","0.9999998829156819"]', volumeNum=379670.0)
    rec = build_resolution_record(r, _CFG)
    assert rec.outcome == "NO"
    assert rec.flags == ""


def test_record_clean_yes_exact() -> None:
    r = _record(outcomePrices='["1","0"]', volumeNum=50000.0)
    rec = build_resolution_record(r, _CFG)
    assert rec.outcome == "YES"
    assert rec.flags == ""


def test_record_clean_no_exact() -> None:
    r = _record(outcomePrices='["0","1"]', volumeNum=50000.0)
    rec = build_resolution_record(r, _CFG)
    assert rec.outcome == "NO"
    assert rec.flags == ""


def test_record_canceled_zero_zero() -> None:
    r = _record(outcomePrices='["0","0"]', volumeNum=5000.0)
    rec = build_resolution_record(r, _CFG)
    assert rec.outcome == "INVALID"
    assert rec.flags == "invalid_resolution"


def test_record_ambiguous_5050() -> None:
    r = _record(outcomePrices='["0.5768324844193577","0.4231675155806422"]', volumeNum=10000.0)
    rec = build_resolution_record(r, _CFG)
    assert rec.outcome == "UNKNOWN"
    assert rec.flags == "ambiguous_resolution"


def test_record_malformed_json_prices() -> None:
    r = _record(outcomePrices="not-json", volumeNum=10000.0)
    rec = build_resolution_record(r, _CFG)
    assert rec.outcome == "UNKNOWN"
    assert rec.flags == "malformed_prices"


def test_record_wrong_length_prices() -> None:
    r = _record(outcomePrices='["1","0","0"]', volumeNum=10000.0)
    rec = build_resolution_record(r, _CFG)
    assert rec.outcome == "UNKNOWN"
    assert rec.flags == "malformed_prices"


def test_record_resolved_early_over_tolerance() -> None:
    r = _record(
        outcomePrices='["0","1"]',
        endDate=_end_date(_END_DT),
        closedTime=_closed_time(_END_DT - timedelta(hours=65 * 24)),
        volumeNum=24190.0,
    )
    rec = build_resolution_record(r, _CFG)
    assert rec.outcome == "NO"
    assert rec.flags == "resolved_early"


def test_record_resolved_exactly_at_tolerance() -> None:
    """Boundary: closedTime exactly 24h before endDate → resolved_early fires (inclusive ≤)."""
    r = _record(
        outcomePrices='["1","0"]',
        endDate=_end_date(_END_DT),
        closedTime=_closed_time(_END_DT - timedelta(hours=24)),
        volumeNum=24190.0,
    )
    rec = build_resolution_record(r, _CFG)
    assert rec.outcome == "YES"
    assert rec.flags == "resolved_early"


def test_record_resolved_just_under_tolerance() -> None:
    """23h before endDate is within tolerance — no resolved_early flag."""
    r = _record(
        outcomePrices='["1","0"]',
        endDate=_end_date(_END_DT),
        closedTime=_closed_time(_END_DT - timedelta(hours=23)),
        volumeNum=24190.0,
    )
    rec = build_resolution_record(r, _CFG)
    assert rec.outcome == "YES"
    assert rec.flags == ""


def test_record_walkover() -> None:
    r = _record(outcomePrices='["0","1"]', volumeNum=200.0)
    rec = build_resolution_record(r, _CFG)
    assert rec.outcome == "NO"
    assert rec.flags == "walkover"


def test_record_disputed() -> None:
    """Flat string list containing 'disputed' fires the flag."""
    r = _record(
        outcomePrices='["0","1"]',
        volumeNum=24190.0,
        umaResolutionStatuses='["disputed"]',
    )
    rec = build_resolution_record(r, _CFG)
    assert rec.outcome == "NO"
    assert rec.flags == "disputed"


def test_record_uma_proposed_not_disputed() -> None:
    """'proposed' is a routine UMA lifecycle state and must NOT fire the disputed flag."""
    r = _record(umaResolutionStatuses='["proposed"]', volumeNum=24190.0)
    rec = build_resolution_record(r, _CFG)
    assert "disputed" not in rec.flags


def test_record_uma_resolved_not_disputed() -> None:
    """'resolved' is a routine UMA finalization state and must NOT fire the disputed flag."""
    r = _record(umaResolutionStatuses='["resolved"]', volumeNum=24190.0)
    rec = build_resolution_record(r, _CFG)
    assert "disputed" not in rec.flags


def test_record_uma_proposed_then_disputed() -> None:
    """A list containing both 'proposed' and 'disputed' fires the flag (membership, not exclusivity)."""
    r = _record(
        umaResolutionStatuses='["proposed","disputed"]',
        volumeNum=24190.0,
    )
    rec = build_resolution_record(r, _CFG)
    assert "disputed" in rec.flags


def test_record_multi_flag_alphabetical() -> None:
    """invalid_resolution + resolved_early + walkover; flags must be alphabetical."""
    r = _record(
        outcomePrices='["0","0"]',
        endDate=_end_date(_END_DT),
        closedTime=_closed_time(_END_DT - timedelta(days=30)),
        volumeNum=200.0,
    )
    rec = build_resolution_record(r, _CFG)
    assert rec.outcome == "INVALID"
    assert rec.flags == "invalid_resolution|resolved_early|walkover"


def test_record_volume_fallback_to_volume(caplog: pytest.LogCaptureFixture) -> None:
    """When volumeNum absent, falls back to 'volume' field and logs a WARNING."""
    r: GammaClosedMarketRecord = {
        "id": "mkt-vol-fallback",
        "question": "Will BTC hit $100k?",
        "slug": "btc-100k",
        "outcomes": '["Yes","No"]',
        "endDate": _end_date(_END_DT),
        "outcomePrices": '["0","1"]',
        "closedTime": _closed_time(_END_DT - timedelta(hours=1)),
        "volume": 5000.0,
        "umaResolutionStatuses": "[]",
        "tags": [{"id": "1", "label": "BTC", "slug": "btc"}],
    }
    import logging

    with caplog.at_level(logging.WARNING, logger="pmbot.phase1_data.resolutions"):
        rec = build_resolution_record(r, _CFG)
    assert rec.volume_lifetime_usdc == 5000.0
    assert rec.flags == ""
    assert any("falling back to 'volume'" in msg for msg in caplog.messages)


def test_record_outcome_prices_raw_preserved() -> None:
    """outcomePrices stored verbatim regardless of outcome."""
    raw = '["0.9999998374530032","0.0000001625469967"]'
    r = _record(outcomePrices=raw)
    rec = build_resolution_record(r, _CFG)
    assert rec.outcome_prices_raw == raw


def test_record_resolved_at_is_iso8601_utc() -> None:
    """resolved_at comes from closedTime, normalized to ISO 8601 UTC."""
    r = _record(closedTime="2023-06-18 06:31:33+00")
    rec = build_resolution_record(r, _CFG)
    dt = datetime.fromisoformat(rec.resolved_at)
    assert dt.tzinfo is not None
    assert "2023-06-18" in rec.resolved_at


# ── write_resolution_csv ──────────────────────────────────────────────────────


def test_write_csv_columns(tmp_path: Path) -> None:
    r = _record(outcomePrices='["1","0"]', volumeNum=5000.0)
    rec = build_resolution_record(r, _CFG)
    path = tmp_path / "out.csv"
    write_resolution_csv([rec], path)
    with path.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert len(rows) == 1
    expected = {
        "market_id",
        "question",
        "slug",
        "outcome",
        "resolved_at",
        "end_date",
        "volume_lifetime_usdc",
        "outcome_prices_raw",
        "flags",
    }
    assert expected <= set(rows[0].keys())
    assert rows[0]["outcome"] == "YES"
    assert rows[0]["flags"] == ""


def test_write_csv_creates_parent_dirs(tmp_path: Path) -> None:
    r = _record(outcomePrices='["0","1"]', volumeNum=5000.0)
    rec = build_resolution_record(r, _CFG)
    path = tmp_path / "deep" / "nested" / "out.csv"
    write_resolution_csv([rec], path)
    assert path.exists()


def test_write_csv_empty(tmp_path: Path) -> None:
    path = tmp_path / "empty.csv"
    write_resolution_csv([], path)
    with path.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert rows == []


# ── build_resolution_whitelist (mocked HTTP) ──────────────────────────────────


def _qualifying_raw() -> dict[str, Any]:
    """A raw record that passes is_btc_binary_shape and produces a clean result."""
    return {
        "id": "q1",
        "question": "Will BTC hit $100k by March?",
        "slug": "btc-100k-march",
        "outcomes": '["Yes","No"]',
        "endDate": _end_date(_END_DT),
        "outcomePrices": '["0","1"]',
        "closedTime": _closed_time(_END_DT - timedelta(hours=1)),
        "volumeNum": 24190.0,
        "umaResolutionStatuses": "[]",
        "tags": [{"id": "1", "label": "BTC", "slug": "btc"}],
    }


def _non_btc_raw() -> dict[str, Any]:
    return {
        "id": "nb1",
        "question": "Will ETH hit $5k?",
        "slug": "eth-5k",
        "outcomes": '["Yes","No"]',
        "endDate": _end_date(_END_DT),
        "outcomePrices": '["1","0"]',
        "closedTime": _closed_time(_END_DT - timedelta(hours=1)),
        "volumeNum": 50000.0,
        "umaResolutionStatuses": "[]",
        "tags": [],
    }


def test_build_whitelist_e2e_mocked(tmp_path: Path) -> None:
    page = [_qualifying_raw(), _non_btc_raw()]
    cfg = _cfg(tmp_path)

    with patch("pmbot.phase1_data.resolutions.urllib.request.urlopen") as mock_url:
        with patch("pmbot.phase1_data.resolutions.time.sleep"):
            mock_url.side_effect = [_make_urlopen_cm(page)]
            records = build_resolution_whitelist(cfg)

    assert len(records) == 1  # only the BTC one qualifies
    assert records[0].market_id == "q1"
    assert records[0].outcome == "NO"


def test_build_whitelist_pagination_terminates(tmp_path: Path) -> None:
    """Stops when page length < page_size (partial last page)."""
    full_page = [_qualifying_raw()] * 10
    partial_page = [_qualifying_raw()] * 3
    cfg = ResolutionConfig(
        output_csv_path=tmp_path / "out.csv",
        page_size=10,
        inter_request_delay_s=0.0,
        max_retries=0,
        retry_base_delay_s=0.0,
    )

    with patch("pmbot.phase1_data.resolutions.urllib.request.urlopen") as mock_url:
        with patch("pmbot.phase1_data.resolutions.time.sleep"):
            mock_url.side_effect = [
                _make_urlopen_cm(full_page),  # page 1: full → continue
                _make_urlopen_cm(partial_page),  # page 2: partial → stop
            ]
            records = build_resolution_whitelist(cfg)

    assert mock_url.call_count == 2
    assert len(records) == 13


def test_build_whitelist_empty_response_terminates(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)

    with patch("pmbot.phase1_data.resolutions.urllib.request.urlopen") as mock_url:
        with patch("pmbot.phase1_data.resolutions.time.sleep"):
            mock_url.side_effect = [_make_urlopen_cm([])]
            records = build_resolution_whitelist(cfg)

    assert records == []
    assert mock_url.call_count == 1


def test_fetch_sends_user_agent(tmp_path: Path) -> None:
    """Every HTTP request must carry config.user_agent (guard against Cloudflare 403)."""
    cfg = _cfg(tmp_path)
    captured: list[urllib.request.Request] = []

    def fake_urlopen(req: urllib.request.Request, **_: object) -> MagicMock:
        captured.append(req)
        return _make_urlopen_cm([])

    with patch("pmbot.phase1_data.resolutions.urllib.request.urlopen", side_effect=fake_urlopen):
        with patch("pmbot.phase1_data.resolutions.time.sleep"):
            build_resolution_whitelist(cfg)

    assert len(captured) >= 1
    assert captured[0].get_header("User-agent") == cfg.user_agent


def test_build_whitelist_4xx_raises(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)

    with patch("pmbot.phase1_data.resolutions.urllib.request.urlopen") as mock_url:
        with patch("pmbot.phase1_data.resolutions.time.sleep"):
            mock_url.side_effect = _http_error(403)
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                build_resolution_whitelist(cfg)

    assert exc_info.value.code == 403
    assert mock_url.call_count == 1


def test_build_whitelist_5xx_retries_then_raises(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)  # max_retries=2 → 3 total attempts

    with patch("pmbot.phase1_data.resolutions.urllib.request.urlopen") as mock_url:
        with patch("pmbot.phase1_data.resolutions.time.sleep") as mock_sleep:
            mock_url.side_effect = _http_error(500)
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                build_resolution_whitelist(cfg)

    assert exc_info.value.code == 500
    assert mock_url.call_count == cfg.max_retries + 1
    assert mock_sleep.call_count == cfg.max_retries


def test_build_whitelist_5xx_recovers_on_retry(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    page = [_qualifying_raw()]

    with patch("pmbot.phase1_data.resolutions.urllib.request.urlopen") as mock_url:
        with patch("pmbot.phase1_data.resolutions.time.sleep") as mock_sleep:
            mock_url.side_effect = [
                _http_error(503),
                _make_urlopen_cm(page),
            ]
            records = build_resolution_whitelist(cfg)

    assert len(records) == 1
    assert mock_url.call_count == 2
    assert mock_sleep.call_count == 1
    assert mock_sleep.call_args == call(0.0)  # retry_base_delay_s=0.0 * 2^0


# ── 422 pagination cap ────────────────────────────────────────────────────────


def test_fetch_page_422_returns_empty_and_logs_info(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """HTTP 422 from Gamma signals the hard pagination cap; must return [] and log INFO."""
    import logging

    from pmbot.phase1_data.resolutions import _fetch_closed_markets_page

    cfg = _cfg(tmp_path)
    with patch("pmbot.phase1_data.resolutions.urllib.request.urlopen") as mock_url:
        mock_url.side_effect = _http_error(422)
        with caplog.at_level(logging.INFO, logger="pmbot.phase1_data.resolutions"):
            result = _fetch_closed_markets_page(cfg, offset=250100)

    assert result == []
    assert mock_url.call_count == 1  # no retries on 422
    assert any("pagination cap" in msg for msg in caplog.messages)


def test_build_whitelist_422_terminates_cleanly_and_returns_prior_records(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """422 mid-pagination returns all records accumulated so far without raising."""
    import logging

    page_a = [_qualifying_raw()]
    page_b = [_qualifying_raw()]

    fetch_returns = [page_a, page_b, []]  # [] = what 422 handler returns
    cfg = ResolutionConfig(
        output_csv_path=tmp_path / "out.csv",
        page_size=1,  # match page length so partial-page stop doesn't fire early
        inter_request_delay_s=0.0,
        max_retries=0,
        retry_base_delay_s=0.0,
    )
    with patch(
        "pmbot.phase1_data.resolutions._fetch_closed_markets_page",
        side_effect=fetch_returns,
    ):
        with caplog.at_level(logging.INFO, logger="pmbot.phase1_data.resolutions"):
            records = build_resolution_whitelist(cfg)

    assert len(records) == 2
    assert records[0].market_id == "q1"
    assert records[1].market_id == "q1"


# ── Checkpoint / partial CSV ──────────────────────────────────────────────────


def test_build_whitelist_checkpoint_fires_at_50_pages(tmp_path: Path) -> None:
    """Every 50 pages, checkpoint is written; on completion partial is promoted to final."""
    single = [_qualifying_raw()]
    # 50 full pages of 1 record each (page_size=1 so each is "full"), then empty → stop
    fetch_returns = [single] * 50 + [[]]
    cfg = ResolutionConfig(
        output_csv_path=tmp_path / "out.csv",
        page_size=1,
        inter_request_delay_s=0.0,
        max_retries=0,
        retry_base_delay_s=0.0,
    )

    with patch(
        "pmbot.phase1_data.resolutions._fetch_closed_markets_page",
        side_effect=fetch_returns,
    ):
        records = build_resolution_whitelist(cfg)

    assert len(records) == 50
    # Partial was promoted to final path
    assert (tmp_path / "out.csv").exists()
    # Partial file is gone after promotion
    partial = tmp_path / "out.partial.csv"
    assert not partial.exists()
