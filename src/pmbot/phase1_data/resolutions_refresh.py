"""Phase 1.5.1 — Incremental Resolution Refresh.

Queries Gamma /markets?closed=true&end_date_min=<since_date>, filters to BTC binary
shape, and merges the resulting records into the existing resolved_markets.csv.

NOTE: For the one-time gap fill, always pass --since explicitly (e.g. --since 2026-01-01).
Do NOT rely on the auto-default for the first invocation — the auto-default depends on the
existing CSV being correct, which is exactly what the gap fill is fixing.

Cron scheduling lives outside this module (deferred to ops; see SPEC.md Step 1.5.1 cadence).
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import time
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from pmbot.phase1_data.resolutions import (
    GammaClosedMarketRecord,
    ResolutionConfig,
    ResolutionRecord,
    build_resolution_record,
    fetch_closed_markets_page,
    is_btc_binary_shape,
    write_resolution_csv,
)

logger = logging.getLogger(__name__)

_CHECKPOINT_INTERVAL_PAGES = 50


class ColdStartRequired(ValueError):
    """Raised when the existing CSV is absent/empty and --cold-start + --since are not both set."""


# ── since_date derivation ─────────────────────────────────────────────────────


def _compute_since_date(
    csv_path: Path,
    since_override: date | None,
    cold_start: bool,
) -> date:
    """Return the since_date to use for end_date_min filtering.

    Raises ColdStartRequired if the existing CSV is absent/empty and both
    cold_start=True and since_override are not provided.
    """
    if since_override is not None:
        return since_override

    if csv_path.exists():
        end_dates: list[date] = []
        skipped = 0
        with csv_path.open(newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                raw = row.get("end_date", "")
                if not raw:
                    skipped += 1
                    continue
                try:
                    normalized = raw.replace("Z", "+00:00")
                    dt = datetime.fromisoformat(normalized)
                    end_dates.append(dt.date())
                except ValueError:
                    skipped += 1

        if skipped:
            logger.warning(
                "Skipped %d rows with null/unparseable end_date when computing since_date",
                skipped,
            )

        if end_dates:
            since = max(end_dates) - timedelta(days=14)
            logger.info("Auto since_date = max(end_date) − 14 days = %s", since)
            return since

    # Cold start path
    if not cold_start or since_override is None:
        raise ColdStartRequired(
            "No existing CSV (or all end_date values are null/unparseable). "
            "Pass --cold-start AND --since YYYY-MM-DD to proceed. "
            "Do NOT omit --since — auto-default to 'all of time' hits the pagination cap."
        )
    # unreachable: since_override is None checked at top and cold_start path requires it
    raise ColdStartRequired("--cold-start requires --since YYYY-MM-DD")  # pragma: no cover


# ── Pagination + filter ───────────────────────────────────────────────────────


def _fetch_refresh_records(
    since: date,
    config: ResolutionConfig,
    checkpoint_path: Path,
) -> list[GammaClosedMarketRecord]:
    """Paginate Gamma with end_date_min=since and return all BTC-binary-shape records.

    Writes a checkpoint every 50 pages. Hard-fails if the first page contains records
    that predate since (Gamma silently ignored end_date_min) or if total records ≥ 50,000
    (filter clearly returned the whole archive).
    """
    extra = {"end_date_min": since.isoformat()}
    all_raw: list[GammaClosedMarketRecord] = []
    offset = 0
    page_num = 0
    first_page_checked = False

    while True:
        page = fetch_closed_markets_page(config, offset, extra_params=extra)
        if not page:
            break

        page_num += 1

        # Hard-fail: assert end_date_min was honored (check only the first page)
        if not first_page_checked:
            since_dt = datetime.combine(since, datetime.min.time()).replace(tzinfo=timezone.utc)
            tolerance = timedelta(hours=1)
            for raw in page:
                ed_raw = raw.get("endDate", "")
                if ed_raw:
                    try:
                        normalized = ed_raw.replace("Z", "+00:00")
                        ed_dt = datetime.fromisoformat(normalized)
                        if ed_dt.tzinfo is None:
                            ed_dt = ed_dt.replace(tzinfo=timezone.utc)
                        if ed_dt < since_dt - tolerance:
                            raise RuntimeError(
                                f"end_date_min filter ignored by Gamma API — "
                                f"record endDate {ed_raw!r} predates since_date {since}. "
                                "Aborting to prevent silent gap. "
                                "See PROGRESS.md for Polymarket silent-fallback pattern."
                            )
                    except (ValueError, AttributeError):
                        pass  # unparseable endDate: skip this assertion for this record
            first_page_checked = True

        for raw in page:
            if is_btc_binary_shape(raw):
                all_raw.append(raw)

        # Hard-fail: sanity ceiling — checked per-page so we error as soon as threshold is hit
        if len(all_raw) >= 50_000:
            raise RuntimeError(
                f"Fetched {len(all_raw)} BTC-binary records ≥ 50,000 sanity ceiling. "
                "end_date_min filter likely ignored — aborting to prevent corrupt merge."
            )

        if page_num % _CHECKPOINT_INTERVAL_PAGES == 0:
            checkpoint_records = [build_resolution_record(r, config) for r in all_raw]
            write_resolution_csv(checkpoint_records, checkpoint_path)
            logger.info(
                "checkpoint written: page=%d btc_records=%d path=%s",
                page_num,
                len(all_raw),
                checkpoint_path,
            )

        if page_num % 10 == 0:
            logger.info(
                "pagination progress: page=%d btc_records_so_far=%d", page_num, len(all_raw)
            )

        if len(page) < config.page_size:
            break

        offset += config.page_size
        time.sleep(config.inter_request_delay_s)

    logger.info("Fetch complete: %d BTC-binary records across %d pages", len(all_raw), page_num)
    return all_raw


# ── Merge ────────────────────────────────────────────────────────────────────


def _resolution_record_to_dict(r: ResolutionRecord) -> dict[str, str]:
    d = asdict(r)
    d["volume_lifetime_usdc"] = f"{r.volume_lifetime_usdc:.2f}"
    return d


def _merge_records(
    csv_path: Path,
    refreshed: list[ResolutionRecord],
) -> tuple[list[ResolutionRecord], int, int]:
    """Merge refreshed records into existing CSV rows.

    Existing rows are preserved in their original order.  Conflicts (same market_id,
    different values) are replaced in-place with the refresh row.  Net-new records are
    appended at the end.  Identical rows (market_id matches AND all fields match) are
    skipped silently.

    Returns (merged_records, new_count, conflict_count).
    """
    existing_rows: list[dict[str, str]] = []
    market_id_to_index: dict[str, int] = {}

    if csv_path.exists():
        with csv_path.open(newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                idx = len(existing_rows)
                existing_rows.append(dict(row))
                mid = row.get("market_id", "")
                if mid:
                    market_id_to_index[mid] = idx

    new_count = 0
    conflict_count = 0

    for rec in refreshed:
        rec_dict = _resolution_record_to_dict(rec)
        mid = rec.market_id

        if mid not in market_id_to_index:
            market_id_to_index[mid] = len(existing_rows)
            existing_rows.append(rec_dict)
            new_count += 1
        else:
            idx = market_id_to_index[mid]
            old_row = existing_rows[idx]
            if old_row != rec_dict:
                logger.info(
                    "conflict resolved: market_id=%s old_flags=%r new_flags=%r "
                    "old_outcome=%r new_outcome=%r",
                    mid,
                    old_row.get("flags"),
                    rec_dict.get("flags"),
                    old_row.get("outcome"),
                    rec_dict.get("outcome"),
                )
                existing_rows[idx] = rec_dict
                conflict_count += 1
            # else: identical — skip silently

    # Reconstruct ResolutionRecord list from final ordered dicts
    merged: list[ResolutionRecord] = []
    for row in existing_rows:
        try:
            vol = float(row.get("volume_lifetime_usdc", "0") or "0")
        except ValueError:
            vol = 0.0
        merged.append(
            ResolutionRecord(
                market_id=row.get("market_id", ""),
                question=row.get("question", ""),
                slug=row.get("slug", ""),
                outcome=row.get("outcome", "UNKNOWN"),
                resolved_at=row.get("resolved_at", ""),
                end_date=row.get("end_date", ""),
                volume_lifetime_usdc=vol,
                outcome_prices_raw=row.get("outcome_prices_raw", ""),
                flags=row.get("flags", ""),
            )
        )
    return merged, new_count, conflict_count


# ── Main entry point ──────────────────────────────────────────────────────────


def run_refresh(
    *,
    since_override: date | None = None,
    out_path: Path | None = None,
    dry_run: bool = False,
    cold_start: bool = False,
    config: ResolutionConfig | None = None,
) -> None:
    """Run the incremental resolution refresh.

    Importable function — raises ColdStartRequired instead of SystemExit so callers can
    handle the exception cleanly.  The CLI wrapper converts ColdStartRequired to SystemExit.
    """
    if config is None:
        config = ResolutionConfig()
    if out_path is None:
        out_path = config.output_csv_path

    since = _compute_since_date(out_path, since_override, cold_start)
    logger.info("Starting refresh: since=%s out=%s dry_run=%s", since, out_path, dry_run)

    checkpoint_path = out_path.with_stem(out_path.stem + ".partial")
    raw_records = _fetch_refresh_records(since, config, checkpoint_path)
    refreshed = [build_resolution_record(r, config) for r in raw_records]

    merged, new_count, conflict_count = _merge_records(out_path, refreshed)

    if dry_run:
        if checkpoint_path.exists():
            checkpoint_path.unlink()
        logger.info(
            "--dry-run: would write %d total rows (%d new, %d conflicts, %d existing unchanged)",
            len(merged),
            new_count,
            conflict_count,
            len(merged) - new_count - conflict_count,
        )
        return

    tmp_path = Path(str(out_path) + ".tmp")
    write_resolution_csv(merged, tmp_path)
    os.replace(tmp_path, out_path)

    if checkpoint_path.exists():
        checkpoint_path.unlink()

    logger.info(
        "Refresh complete: wrote %d total rows (%d new, %d conflicts) to %s",
        len(merged),
        new_count,
        conflict_count,
        out_path,
    )


# ── CLI ───────────────────────────────────────────────────────────────────────


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Phase 1.5.1 — Incremental Resolution Refresh",
        epilog=(
            "For the one-time gap fill, always pass --since explicitly (e.g. --since 2026-01-01). "
            "Do NOT rely on the auto-default for the first invocation."
        ),
    )
    parser.add_argument("--since", metavar="YYYY-MM-DD", help="Override since_date")
    parser.add_argument(
        "--out",
        metavar="PATH",
        help=f"Output CSV path (default: {ResolutionConfig().output_csv_path})",
    )
    parser.add_argument("--dry-run", action="store_true", help="Skip final write")
    parser.add_argument(
        "--cold-start",
        action="store_true",
        help="Allow running with no existing CSV; requires --since",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    parser = _build_parser()
    args = parser.parse_args(argv)

    since_override: date | None = None
    if args.since:
        try:
            since_override = date.fromisoformat(args.since)
        except ValueError:
            parser.error(f"--since must be YYYY-MM-DD, got: {args.since!r}")

    out_path = Path(args.out) if args.out else None

    try:
        run_refresh(
            since_override=since_override,
            out_path=out_path,
            dry_run=args.dry_run,
            cold_start=args.cold_start,
        )
    except ColdStartRequired as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
