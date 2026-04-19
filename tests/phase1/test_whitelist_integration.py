"""Integration tests — hit the live Polymarket Gamma API.

Run with: pytest -m integration tests/phase1/test_whitelist_integration.py

These tests are skipped by default. Their purpose is to validate the field-name
and field-type assumptions in GammaMarketRecord against the actual API response.
Run manually before each weekly whitelist rebuild until a startup health check
is added.

If any assertion in test_fetch_markets_live_returns_expected_fields fails, the
Gamma API schema has changed and GammaMarketRecord must be updated before the
production whitelist script will work correctly.
"""

from __future__ import annotations

import json

import pytest

from pmbot.phase1_data.whitelist import (
    WhitelistConfig,
    _fetch_markets_page,
    gamma_record_to_market_metadata,
)
from pmbot.phase0_filter import MarketMetadata
from datetime import datetime, timezone


@pytest.mark.integration
def test_fetch_markets_live_returns_expected_fields() -> None:
    """Fetch one page, validate schema assumptions, and run records through the adapter."""
    config = WhitelistConfig(fetch_limit=50)
    page = _fetch_markets_page(config, offset=0)

    assert isinstance(page, list), "Expected a list of market records"
    assert len(page) > 0, "Expected at least one active market"

    # ── Required field presence and types ───────────────────────────────────
    for i, record in enumerate(page):
        assert "id" in record, f"record[{i}] missing 'id'"
        assert isinstance(record["id"], str), f"record[{i}]['id'] must be str"

        assert "question" in record, f"record[{i}] missing 'question'"
        assert isinstance(record["question"], str), f"record[{i}]['question'] must be str"

        assert "slug" in record, f"record[{i}] missing 'slug'"
        assert isinstance(record["slug"], str), f"record[{i}]['slug'] must be str"

        assert "endDate" in record, f"record[{i}] missing 'endDate'"
        assert isinstance(record["endDate"], str), f"record[{i}]['endDate'] must be str"

        assert "outcomes" in record, f"record[{i}] missing 'outcomes'"
        assert isinstance(record["outcomes"], str), (
            f"record[{i}]['outcomes'] must be a JSON string — if this fails, the API schema changed"
        )
        # outcomes must be valid JSON
        json.loads(record["outcomes"])

        # volume24hr: either absent or a number (never a string)
        if "volume24hr" in record:
            assert isinstance(record["volume24hr"], (int, float)), (
                f"record[{i}]['volume24hr'] must be numeric, got {type(record['volume24hr'])}"
            )

        # tags: either absent, None, or a list
        if "tags" in record:
            assert record["tags"] is None or isinstance(record["tags"], list), (
                f"record[{i}]['tags'] must be None or list, got {type(record['tags'])}"
            )

    # ── Adapter round-trip: at least 5 records must convert without raising ─
    now = datetime.now(timezone.utc)
    successful_conversions = 0
    errors: list[str] = []
    for record in page:
        try:
            meta = gamma_record_to_market_metadata(record, now)
            assert isinstance(meta, MarketMetadata)
            successful_conversions += 1
        except Exception as exc:
            errors.append(f"id={record.get('id', '?')}: {exc}")

    assert successful_conversions >= 5, (
        f"Expected ≥5 successful adapter conversions, got {successful_conversions}. "
        f"Errors: {errors[:3]}"
    )
