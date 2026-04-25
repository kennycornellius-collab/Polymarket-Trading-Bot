from __future__ import annotations

import csv
import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import NotRequired, TypedDict, cast

from pmbot.phase1_data.whitelist import (
    GammaMarketRecord,
    infer_market_type,
    infer_strike_type,
    infer_underlying,
)

logger = logging.getLogger(__name__)


# ── External data contract ────────────────────────────────────────────────────


class GammaClosedMarketRecord(TypedDict):
    """Fields read from the Polymarket Gamma API for closed markets.

    Field names and types verified against live API 2026-04-24 (50-record sample).
    Run pytest -m integration to re-verify against the live API.
    """

    id: str
    question: str
    slug: str
    outcomes: str  # JSON-encoded e.g. '["Yes","No"]'
    endDate: str  # ISO 8601 UTC e.g. "2023-06-17T00:00:00Z"
    outcomePrices: str  # JSON-encoded e.g. '["0.9999...", "0.0000..."]'
    closedTime: str  # format: "YYYY-MM-DD HH:MM:SS+00" (space separator, +00 suffix)
    volumeNum: NotRequired[float]  # lifetime volume as JSON number
    volume: NotRequired[float | str]  # alternate volume field; API returns as JSON string
    umaResolutionStatuses: NotRequired[str]  # JSON-encoded list string; always "[]" in sample
    resolvedBy: NotRequired[str | None]  # Ethereum address or None
    tags: NotRequired[list[dict[str, str]] | None]  # null on some records
    updatedAt: NotRequired[str]  # administrative timestamp — NOT used for resolved_at


# ── Config and result dataclasses ─────────────────────────────────────────────


@dataclass(frozen=True)
class ResolutionConfig:
    gamma_api_base_url: str = "https://gamma-api.polymarket.com"
    user_agent: str = "pmbot/0.1 (+https://github.com/<placeholder>)"
    request_timeout_seconds: float = 30.0
    page_size: int = 100
    inter_request_delay_s: float = 0.15
    max_retries: int = 3
    retry_base_delay_s: float = 0.5
    # Outcome derivation thresholds — empirically calibrated from live data 2026-04-24
    outcome_price_dominance_threshold: float = 0.99
    outcome_price_sum_tolerance: float = 0.01
    # Resolution quality thresholds
    early_resolution_tolerance_hours: float = 24.0
    walkover_volume_threshold_usdc: float = 1000.0  # justified: p10=715 in 50-market sample
    # UMA lifecycle states that indicate a genuine dispute.
    # Observed values across 2,500-market sweep (2026-04-25):
    #   'proposed'  — routine: proposer submitted outcome (~68% of non-empty)
    #   'resolved'  — routine: finalized cleanly (~27%)
    #   'disputed'  — actual dispute (~0.6%)
    # Only 'disputed' should set the flag; the others are normal lifecycle noise.
    dispute_status_values: frozenset[str] = frozenset({"disputed"})
    output_csv_path: Path = Path("data/resolutions/resolved_markets.csv")


@dataclass(frozen=True)
class ResolutionRecord:
    market_id: str
    question: str
    slug: str
    outcome: str  # YES | NO | INVALID | UNKNOWN
    resolved_at: str  # ISO 8601 UTC from closedTime; empty string if parse fails
    end_date: str  # ISO 8601 UTC from endDate; empty string if parse fails
    volume_lifetime_usdc: float
    outcome_prices_raw: str  # verbatim outcomePrices for audit
    flags: str  # alphabetical pipe-delimited; "" = training-eligible


# ── CSV schema ────────────────────────────────────────────────────────────────

_RESOLUTION_CSV_FIELDS = [
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


# ── Pure helper functions (no I/O) ────────────────────────────────────────────


def _parse_closed_time(raw: str) -> datetime:
    """Parse Gamma closedTime 'YYYY-MM-DD HH:MM:SS+00' to a timezone-aware datetime."""
    normalized = raw.replace(" ", "T")
    if normalized.endswith("+00"):
        normalized += ":00"
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _parse_end_date(raw: str) -> datetime:
    """Parse Gamma endDate ISO 8601 string to a timezone-aware datetime."""
    normalized = raw.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _derive_outcome_and_flags(
    prices_raw: str,
    dominance_threshold: float,
    sum_tolerance: float,
) -> tuple[str, list[str]]:
    """Return (outcome, flags) from the raw outcomePrices string.

    Outcome is one of YES / NO / INVALID / UNKNOWN.
    The returned flags list is one of: [], [malformed_prices], [invalid_resolution],
    or [ambiguous_resolution]. These three price-error flags are mutually exclusive.
    """
    try:
        prices = json.loads(prices_raw)
    except (json.JSONDecodeError, ValueError, TypeError):
        return ("UNKNOWN", ["malformed_prices"])

    if not isinstance(prices, list) or len(prices) != 2:
        return ("UNKNOWN", ["malformed_prices"])

    try:
        p0, p1 = float(prices[0]), float(prices[1])
    except (ValueError, TypeError):
        return ("UNKNOWN", ["malformed_prices"])

    if abs(p0 + p1 - 1.0) > sum_tolerance:
        # Catches ["0","0"] canceled markets and any non-unit-sum case
        return ("INVALID", ["invalid_resolution"])

    if p0 >= dominance_threshold:
        return ("YES", [])
    if p1 >= dominance_threshold:
        return ("NO", [])

    # Sum ≈ 1 but neither side dominates — market closed without clean resolution
    return ("UNKNOWN", ["ambiguous_resolution"])


def _extract_volume(record: GammaClosedMarketRecord, market_id: str) -> float:
    """Extract lifetime volume USDC, falling back volumeNum → volume → 0.0."""
    vol_num = record.get("volumeNum")
    if vol_num is not None:
        return float(vol_num)
    vol = record.get("volume")
    if vol is not None:
        logger.warning("market id=%s: volumeNum absent, falling back to 'volume' field", market_id)
        try:
            return float(str(vol))
        except (ValueError, TypeError):
            logger.warning(
                "market id=%s: 'volume' field not convertible to float: %r", market_id, vol
            )
    else:
        logger.warning("market id=%s: no volume field found, defaulting to 0.0", market_id)
    return 0.0


def is_btc_binary_shape(record: GammaClosedMarketRecord) -> bool:
    """True iff the market is a binary YES/NO BTC market with an absolute price strike.

    Reuses Phase 0 inference without volume24hr or TTE gates (meaningless for closed markets).
    Named distinctly from is_qualified_btc_market to avoid confusion with the live filter.
    """
    r = cast(GammaMarketRecord, record)
    return (
        infer_market_type(r) == "binary"
        and infer_underlying(r) == "BTC"
        and infer_strike_type(r) == "absolute"
    )


def build_resolution_record(
    record: GammaClosedMarketRecord,
    config: ResolutionConfig,
) -> ResolutionRecord:
    """Build a ResolutionRecord from a raw Gamma closed market record.

    Never raises. Malformed inputs produce UNKNOWN/INVALID outcomes with appropriate flags.
    """
    market_id = record.get("id", "unknown")

    # Parse closedTime → resolved_at
    resolved_dt: datetime | None = None
    ct_raw = record.get("closedTime", "")
    if ct_raw:
        try:
            resolved_dt = _parse_closed_time(ct_raw)
            resolved_at = resolved_dt.isoformat()
        except ValueError as exc:
            logger.warning(
                "market id=%s: failed to parse closedTime %r: %s", market_id, ct_raw, exc
            )
            resolved_at = ""
    else:
        logger.warning("market id=%s: closedTime absent", market_id)
        resolved_at = ""

    # Parse endDate → end_date
    end_dt: datetime | None = None
    ed_raw = record.get("endDate", "")
    if ed_raw:
        try:
            end_dt = _parse_end_date(ed_raw)
            end_date = end_dt.isoformat()
        except ValueError as exc:
            logger.warning("market id=%s: failed to parse endDate %r: %s", market_id, ed_raw, exc)
            end_date = ""
    else:
        logger.warning("market id=%s: endDate absent", market_id)
        end_date = ""

    # Extract lifetime volume
    volume = _extract_volume(record, market_id)

    # Derive outcome and price-based flags
    prices_raw = record.get("outcomePrices", "")
    outcome, flags = _derive_outcome_and_flags(
        prices_raw,
        config.outcome_price_dominance_threshold,
        config.outcome_price_sum_tolerance,
    )

    # Disputed: umaResolutionStatuses contains a value in dispute_status_values.
    # The field is a JSON-encoded flat list of strings, e.g. '["proposed"]' or
    # '["proposed","disputed"]'. Routine lifecycle values ('proposed', 'resolved')
    # must NOT set the flag — only values in config.dispute_status_values do.
    uma_raw = record.get("umaResolutionStatuses") or "[]"
    try:
        uma_parsed: list[str] = json.loads(uma_raw)
        if set(uma_parsed) & config.dispute_status_values:
            flags.append("disputed")
    except (json.JSONDecodeError, ValueError, TypeError):
        logger.warning("market id=%s: umaResolutionStatuses not parseable: %r", market_id, uma_raw)

    # Resolved early: closedTime ≤ endDate − tolerance (inclusive at exactly tolerance)
    if resolved_dt is not None and end_dt is not None:
        tolerance = timedelta(hours=config.early_resolution_tolerance_hours)
        if resolved_dt <= end_dt - tolerance:
            flags.append("resolved_early")

    # Walkover: lifetime volume below threshold
    if volume < config.walkover_volume_threshold_usdc:
        flags.append("walkover")

    return ResolutionRecord(
        market_id=market_id,
        question=record.get("question", ""),
        slug=record.get("slug", ""),
        outcome=outcome,
        resolved_at=resolved_at,
        end_date=end_date,
        volume_lifetime_usdc=volume,
        outcome_prices_raw=prices_raw,
        flags="|".join(sorted(flags)) if flags else "",
    )


# ── HTTP with retry ───────────────────────────────────────────────────────────


def _fetch_closed_markets_page(
    config: ResolutionConfig, offset: int
) -> list[GammaClosedMarketRecord]:
    """Fetch one page of closed markets from the Gamma API with retry on 5xx/network errors.

    closed=true is honored server-side (verified empirically 2026-04-24).
    4xx errors re-raise immediately. 5xx and URLError retry with exponential backoff.
    """
    params = urllib.parse.urlencode(
        {
            "closed": "true",
            "limit": config.page_size,
            "offset": offset,
        }
    )
    url = f"{config.gamma_api_base_url}/markets?{params}"
    req = urllib.request.Request(url, headers={"User-Agent": config.user_agent})

    for attempt in range(config.max_retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=config.request_timeout_seconds) as resp:
                return cast(list[GammaClosedMarketRecord], json.loads(resp.read()))
        except urllib.error.HTTPError as exc:
            if exc.code == 422:
                # 422 signals the API's hard pagination cap — treat as end-of-data.
                logger.info(
                    "Gamma pagination cap reached at offset=%d — stopping cleanly", offset
                )
                return []
            if 400 <= exc.code < 500:
                logger.error("Gamma API client error code=%d url=%s — not retrying", exc.code, url)
                raise
            if attempt >= config.max_retries:
                logger.error(
                    "Gamma API server error code=%d — exhausted %d retries",
                    exc.code,
                    config.max_retries,
                )
                raise
            delay = config.retry_base_delay_s * (2.0**attempt)
            logger.warning(
                "Gamma API server error code=%d attempt=%d/%d — retrying in %.1fs",
                exc.code,
                attempt + 1,
                config.max_retries,
                delay,
            )
            time.sleep(delay)
        except OSError as exc:
            # Covers urllib.error.URLError (connection errors) and TimeoutError
            # (raised directly from resp.read() on mid-response SSL timeouts).
            if attempt >= config.max_retries:
                logger.error("Network error — exhausted %d retries", config.max_retries)
                raise
            delay = config.retry_base_delay_s * (2.0**attempt)
            logger.warning(
                "Network error attempt=%d/%d — retrying in %.1fs: %s",
                attempt + 1,
                config.max_retries,
                delay,
                exc,
            )
            time.sleep(delay)
    raise RuntimeError(  # pragma: no cover
        "unreachable: retry loop terminated without return or raise"
    )


# ── Main entry points ─────────────────────────────────────────────────────────


_CHECKPOINT_INTERVAL_PAGES = 50


def build_resolution_whitelist(config: ResolutionConfig) -> list[ResolutionRecord]:
    """Fetch all closed Polymarket markets, filter to BTC binary shape, return ResolutionRecords.

    Pagination terminates when a page returns fewer records than page_size (last page),
    or when the API returns HTTP 422 (hard pagination cap, ~250k offset empirically).
    Writes a checkpoint to <output>.partial.csv every 50 pages; promotes it to the final
    path on clean completion so a crash never loses more than 50 pages of work.
    Logs progress every 10 pages and emits a final summary.
    """
    partial_path = config.output_csv_path.with_stem(
        config.output_csv_path.stem + ".partial"
    )
    records: list[ResolutionRecord] = []
    total_closed_seen = 0
    btc_binary_seen = 0
    offset = 0
    page_num = 0

    while True:
        page = _fetch_closed_markets_page(config, offset)
        if not page:
            break

        page_num += 1
        for raw in page:
            total_closed_seen += 1
            if not is_btc_binary_shape(raw):
                continue
            btc_binary_seen += 1
            records.append(build_resolution_record(raw, config))

        if page_num % _CHECKPOINT_INTERVAL_PAGES == 0:
            write_resolution_csv(records, partial_path)
            logger.info(
                "checkpoint written: page=%d records=%d path=%s",
                page_num,
                len(records),
                partial_path,
            )

        if page_num % 10 == 0:
            logger.info(
                "pagination progress: page=%d total_closed_seen=%d btc_binary_seen=%d",
                page_num,
                total_closed_seen,
                btc_binary_seen,
            )

        if len(page) < config.page_size:
            break

        offset += config.page_size
        time.sleep(config.inter_request_delay_s)

    if partial_path.exists():
        config.output_csv_path.parent.mkdir(parents=True, exist_ok=True)
        partial_path.replace(config.output_csv_path)
        logger.info("checkpoint promoted to final path: %s", config.output_csv_path)

    flag_counts: dict[str, int] = {}
    training_eligible = 0
    for r in records:
        if not r.flags:
            training_eligible += 1
        else:
            for flag in r.flags.split("|"):
                flag_counts[flag] = flag_counts.get(flag, 0) + 1

    logger.info(
        "Resolution summary: total_closed=%d btc_binary_shape=%d training_eligible=%d flags=%s",
        total_closed_seen,
        btc_binary_seen,
        training_eligible,
        flag_counts,
    )

    return records


def write_resolution_csv(records: list[ResolutionRecord], path: Path) -> None:
    """Write ResolutionRecord list to a CSV file. Creates parent directories as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=_RESOLUTION_CSV_FIELDS)
        writer.writeheader()
        for r in records:
            writer.writerow(
                {
                    "market_id": r.market_id,
                    "question": r.question,
                    "slug": r.slug,
                    "outcome": r.outcome,
                    "resolved_at": r.resolved_at,
                    "end_date": r.end_date,
                    "volume_lifetime_usdc": f"{r.volume_lifetime_usdc:.2f}",
                    "outcome_prices_raw": r.outcome_prices_raw,
                    "flags": r.flags,
                }
            )
