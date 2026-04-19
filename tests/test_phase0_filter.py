from pmbot.phase0_filter import (
    FilterConfig,
    MarketMetadata,
    is_qualified_btc_market,
)


def _market(**overrides: object) -> MarketMetadata:
    defaults: dict[str, object] = {
        "market_id": "test-001",
        "market_type": "binary",
        "underlying": "BTC",
        "strike_type": "absolute",
        "tte_days": 15.0,
        "daily_volume_usdc": 50_000.0,
    }
    defaults.update(overrides)
    return MarketMetadata(**defaults)  # type: ignore[arg-type]


def test_happy_path() -> None:
    result = is_qualified_btc_market(_market())
    assert result.qualified is True
    assert result.reasons == ()


def test_wrong_market_type() -> None:
    result = is_qualified_btc_market(_market(market_type="options"))
    assert result.qualified is False
    assert "wrong_market_type" in result.reasons


def test_wrong_underlying() -> None:
    result = is_qualified_btc_market(_market(underlying="ETH"))
    assert result.qualified is False
    assert "wrong_underlying" in result.reasons


def test_wrong_strike_type() -> None:
    result = is_qualified_btc_market(_market(strike_type="percentage_move"))
    assert result.qualified is False
    assert "wrong_strike_type" in result.reasons


def test_tte_below_min() -> None:
    result = is_qualified_btc_market(_market(tte_days=2.9))
    assert result.qualified is False
    assert "tte_out_of_range" in result.reasons


def test_tte_above_max() -> None:
    result = is_qualified_btc_market(_market(tte_days=30.1))
    assert result.qualified is False
    assert "tte_out_of_range" in result.reasons


def test_volume_below_threshold() -> None:
    result = is_qualified_btc_market(_market(daily_volume_usdc=9_999.99))
    assert result.qualified is False
    assert "volume_below_threshold" in result.reasons


def test_multi_reason_rejection() -> None:
    result = is_qualified_btc_market(
        _market(underlying="ETH", tte_days=1.0, daily_volume_usdc=100.0)
    )
    assert result.qualified is False
    assert set(result.reasons) == {
        "wrong_underlying",
        "tte_out_of_range",
        "volume_below_threshold",
    }


def test_boundary_tte_min_inclusive() -> None:
    result = is_qualified_btc_market(_market(tte_days=3.0))
    assert result.qualified is True


def test_boundary_tte_max_inclusive() -> None:
    result = is_qualified_btc_market(_market(tte_days=30.0))
    assert result.qualified is True


def test_boundary_volume_inclusive() -> None:
    result = is_qualified_btc_market(_market(daily_volume_usdc=10_000.0))
    assert result.qualified is True


def test_default_config_matches_spec() -> None:
    cfg = FilterConfig()
    assert cfg.min_tte_days == 3
    assert cfg.max_tte_days == 30
    assert cfg.min_daily_volume_usdc == 10_000.0
    assert cfg.allowed_market_types == frozenset({"binary"})
    assert cfg.allowed_underlyings == frozenset({"BTC"})
    assert cfg.allowed_strike_types == frozenset({"absolute"})
