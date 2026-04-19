"""Integration tests — hit the live Polymarket Gamma API.

Run with: pytest -m integration tests/phase1/test_whitelist_integration.py

These tests are skipped by default. Their purpose is to validate the field-name
assumptions in GammaMarketRecord against the actual API response. Run manually
before each weekly whitelist rebuild until a startup health check is added.
"""

from __future__ import annotations

import pytest

from pmbot.phase1_data.whitelist import WhitelistConfig, _fetch_markets_page


@pytest.mark.integration
def test_fetch_markets_live_returns_expected_fields() -> None:
    """Fetch one page from the live API and assert the expected fields are present."""
    config = WhitelistConfig(fetch_limit=10)
    page = _fetch_markets_page(config, offset=0)

    assert isinstance(page, list), "Expected a list of market records"
    assert len(page) > 0, "Expected at least one active market"

    first = page[0]
    for field in ("id", "question", "slug", "outcomes", "endDate", "volume24hr"):
        assert field in first, f"Expected field '{field}' missing from Gamma API response"

    assert isinstance(first["id"], str)
    assert isinstance(first["question"], str)
    assert isinstance(first["outcomes"], str), (
        "outcomes should be a JSON string — if this fails, the API schema changed"
    )
