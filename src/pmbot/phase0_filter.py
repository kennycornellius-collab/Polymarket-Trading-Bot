from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class FilterConfig:
    min_tte_days: int = 3
    max_tte_days: int = 30
    min_daily_volume_usdc: float = 10_000.0
    allowed_market_types: frozenset[str] = field(default_factory=lambda: frozenset({"binary"}))
    allowed_underlyings: frozenset[str] = field(default_factory=lambda: frozenset({"BTC"}))
    allowed_strike_types: frozenset[str] = field(default_factory=lambda: frozenset({"absolute"}))


@dataclass(frozen=True)
class MarketMetadata:
    market_id: str
    market_type: str
    underlying: str
    strike_type: str
    tte_days: float
    daily_volume_usdc: float


@dataclass(frozen=True)
class FilterResult:
    market_id: str
    qualified: bool
    reasons: tuple[str, ...]


def is_qualified_btc_market(
    market: MarketMetadata,
    config: FilterConfig = FilterConfig(),
) -> FilterResult:
    """Return a FilterResult indicating whether *market* passes all qualifying
    criteria in *config*.

    Boundary convention: tte_days in [min_tte_days, max_tte_days] (inclusive);
    daily_volume_usdc >= min_daily_volume_usdc (inclusive).

    All failing criteria are collected before returning — the function never
    short-circuits. Callers are responsible for logging the result.
    """
    reasons: list[str] = []

    if market.market_type not in config.allowed_market_types:
        reasons.append("wrong_market_type")

    if market.underlying not in config.allowed_underlyings:
        reasons.append("wrong_underlying")

    if market.strike_type not in config.allowed_strike_types:
        reasons.append("wrong_strike_type")

    if market.tte_days < config.min_tte_days or market.tte_days > config.max_tte_days:
        reasons.append("tte_out_of_range")

    if market.daily_volume_usdc < config.min_daily_volume_usdc:
        reasons.append("volume_below_threshold")

    return FilterResult(
        market_id=market.market_id,
        qualified=len(reasons) == 0,
        reasons=tuple(reasons),
    )
