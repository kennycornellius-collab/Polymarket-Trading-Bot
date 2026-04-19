from __future__ import annotations

import csv
import json
import logging
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import NotRequired, TypedDict, cast

from pmbot.phase0_filter import FilterConfig, MarketMetadata, is_qualified_btc_market

logger = logging.getLogger(__name__)


# ── External data contract ────────────────────────────────────────────────────


class GammaMarketRecord(TypedDict):
    """Fields we read from the Polymarket Gamma Markets API response.

    Field names match the Gamma API JSON keys as of 2026-04.
    Run pytest -m integration to verify these assumptions against the live API.
    """

    id: str
    question: str
    slug: str
    outcomes: str  # JSON-encoded string: '["Yes","No"]' — must json.loads()
    endDate: str  # ISO 8601 e.g. "2025-03-01T00:00:00Z"
    volume24hr: str  # API returns as string; coerced to float in adapter
    tags: NotRequired[list[dict[str, str]]]  # [{id, label, slug}, ...]
    active: NotRequired[bool]
    closed: NotRequired[bool]


# ── Configs and results ───────────────────────────────────────────────────────


@dataclass(frozen=True)
class WhitelistConfig:
    output_csv_path: Path = Path("data/whitelist/qualified_markets_whitelist.csv")
    gamma_base_url: str = "https://gamma-api.polymarket.com"
    fetch_limit: int = 100
    request_timeout_s: float = 30.0
    log_every_n_rejections: int = 100
    inter_request_delay_s: float = 0.15  # sleep between pagination requests
    max_retries: int = 3  # retries for 5xx / transient network errors
    retry_base_delay_s: float = 0.5  # first retry delay; doubles each attempt
    user_agent: str = "pmbot/0.1 (+https://github.com/<placeholder>)"


@dataclass(frozen=True)
class WhitelistResult:
    total_markets_seen: int
    qualified_count: int
    rejected_count: int
    rejection_reasons: dict[str, int]
    output_path: Path
    run_started_at: datetime  # timezone-aware UTC
    run_completed_at: datetime  # timezone-aware UTC


# ── Regex patterns ────────────────────────────────────────────────────────────

# Matches: $100k, $100K, $100,000, $1.5M, $95,000.00
# Misses intentionally: "100000 USD" (no $ sign) → returns "unknown" → excluded
_ABSOLUTE_RE = re.compile(r"\$\s*[\d,]+(?:\.\d+)?\s*[kKmM]?")

# Matches: "5%", "rise 5%", "rise by 5%", "fall 10%", "up 20%", "down 3%"
# Misses intentionally: "double", "triple" → returns "unknown" → excluded
_PERCENTAGE_RE = re.compile(
    r"\b\d+(?:\.\d+)?\s*%"
    r"|\b(?:rise|fall|increase|decrease|surge|drop|up|down)\s+(?:by\s+)?\d",
    re.IGNORECASE,
)


# ── Pure inference functions (no I/O) ────────────────────────────────────────


def infer_market_type(record: GammaMarketRecord) -> str:
    """Return "binary" iff outcomes decodes to exactly {"yes","no"} (case-insensitive)."""
    try:
        labels = sorted(s.strip().lower() for s in json.loads(record["outcomes"]))
    except (json.JSONDecodeError, ValueError, TypeError):
        return "non_binary"
    return "binary" if labels == ["no", "yes"] else "non_binary"


def infer_underlying(record: GammaMarketRecord) -> str:
    """Return "BTC" if "btc" or "bitcoin" appears in question, slug, or any tag label."""
    tag_labels = " ".join(t.get("label", "") for t in (record.get("tags") or []))
    haystack = f"{record['question']} {record['slug']} {tag_labels}".lower()
    if "btc" in haystack or "bitcoin" in haystack:
        return "BTC"
    return "other"


def infer_strike_type(record: GammaMarketRecord) -> str:
    """Return "absolute", "percentage", or "unknown". Absolute wins if both match."""
    title = record["question"]
    if _ABSOLUTE_RE.search(title):
        return "absolute"
    if _PERCENTAGE_RE.search(title):
        return "percentage"
    return "unknown"


def compute_tte_days(record: GammaMarketRecord, now: datetime) -> float:
    """Return time-to-expiry in fractional days. `now` must be timezone-aware UTC."""
    end_str = record["endDate"].replace("Z", "+00:00")
    resolution_dt = datetime.fromisoformat(end_str)
    if resolution_dt.tzinfo is None:
        resolution_dt = resolution_dt.replace(tzinfo=timezone.utc)
    return (resolution_dt - now).total_seconds() / 86400.0


# ── Adapter ───────────────────────────────────────────────────────────────────


def gamma_record_to_market_metadata(record: GammaMarketRecord, now: datetime) -> MarketMetadata:
    """Map a Gamma API market record to a MarketMetadata ready for Phase 0 filtering."""
    return MarketMetadata(
        market_id=record["id"],
        market_type=infer_market_type(record),
        underlying=infer_underlying(record),
        strike_type=infer_strike_type(record),
        tte_days=compute_tte_days(record, now),
        daily_volume_usdc=float(record["volume24hr"]),
    )


# ── HTTP with retry ───────────────────────────────────────────────────────────


def _fetch_markets_page(config: WhitelistConfig, offset: int) -> list[GammaMarketRecord]:
    """Fetch one page of active markets from the Gamma API with retry on 5xx/network errors.

    4xx errors re-raise immediately (caller bug, not transient).
    5xx and URLError retry up to config.max_retries times with exponential backoff.
    """
    params = urllib.parse.urlencode(
        {
            "active": "true",
            "closed": "false",
            "limit": config.fetch_limit,
            "offset": offset,
        }
    )
    url = f"{config.gamma_base_url}/markets?{params}"
    req = urllib.request.Request(url, headers={"User-Agent": config.user_agent})

    for attempt in range(config.max_retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=config.request_timeout_s) as resp:
                return cast(list[GammaMarketRecord], json.loads(resp.read()))
        except urllib.error.HTTPError as exc:
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
        except urllib.error.URLError as exc:
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


# ── Main entry point ──────────────────────────────────────────────────────────

_CSV_FIELDS = [
    "market_id",
    "slug",
    "question",
    "resolution_date_utc",
    "tte_days",
    "daily_volume_usdc",
    "qualified_at_utc",
]


def build_whitelist(config: WhitelistConfig, filter_config: FilterConfig) -> WhitelistResult:
    """Fetch all active Polymarket markets, apply the Phase 0 filter, and write
    qualifying markets to a CSV at config.output_csv_path.

    Malformed records are logged at WARNING and counted as "malformed_record" in
    rejection_reasons. They never crash the run.
    """
    run_started_at = datetime.now(timezone.utc)
    rejection_reasons: dict[str, int] = {}
    qualified_rows: list[dict[str, str]] = []
    total_seen = 0
    rejected_count = 0

    offset = 0
    while True:
        page = _fetch_markets_page(config, offset)
        if not page:
            break

        for record in page:
            total_seen += 1
            try:
                metadata = gamma_record_to_market_metadata(record, run_started_at)
            except (KeyError, ValueError, json.JSONDecodeError) as exc:
                logger.warning(
                    "Malformed market record id=%s: %s", record.get("id", "<unknown>"), exc
                )
                rejection_reasons["malformed_record"] = (
                    rejection_reasons.get("malformed_record", 0) + 1
                )
                rejected_count += 1
                continue

            filter_result = is_qualified_btc_market(metadata, filter_config)

            if filter_result.qualified:
                qualified_rows.append(
                    {
                        "market_id": metadata.market_id,
                        "slug": record["slug"],
                        "question": record["question"],
                        "resolution_date_utc": record["endDate"],
                        "tte_days": f"{metadata.tte_days:.6f}",
                        "daily_volume_usdc": f"{metadata.daily_volume_usdc:.2f}",
                        "qualified_at_utc": run_started_at.isoformat(),
                    }
                )
            else:
                for reason in filter_result.reasons:
                    rejection_reasons[reason] = rejection_reasons.get(reason, 0) + 1
                rejected_count += 1
                if rejected_count % config.log_every_n_rejections == 0:
                    logger.debug(
                        "Rejected market id=%s reasons=%s",
                        record.get("id", "<unknown>"),
                        filter_result.reasons,
                    )

        if len(page) < config.fetch_limit:
            break

        offset += config.fetch_limit
        time.sleep(config.inter_request_delay_s)

    config.output_csv_path.parent.mkdir(parents=True, exist_ok=True)
    with config.output_csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=_CSV_FIELDS)
        writer.writeheader()
        writer.writerows(qualified_rows)

    run_completed_at = datetime.now(timezone.utc)
    qualified_count = len(qualified_rows)

    logger.info(
        "Whitelist complete: %d qualified / %d seen. Top rejections: %s",
        qualified_count,
        total_seen,
        sorted(rejection_reasons.items(), key=lambda kv: -kv[1])[:5],
    )

    return WhitelistResult(
        total_markets_seen=total_seen,
        qualified_count=qualified_count,
        rejected_count=rejected_count,
        rejection_reasons=rejection_reasons,
        output_path=config.output_csv_path,
        run_started_at=run_started_at,
        run_completed_at=run_completed_at,
    )
