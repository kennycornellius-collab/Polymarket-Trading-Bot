from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import statistics
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import NotRequired, TypedDict, cast
from uuid import uuid4

import polars as pl

logger = logging.getLogger(__name__)


# ── External data contracts ───────────────────────────────────────────────────


class _GammaMarketDetail(TypedDict):
    """Fields from GET /markets/{id} needed by the lookup build.

    clobTokenIds is a JSON-encoded string, NOT a native JSON array.
    Confirmed by live API spike 2026-04-29.
    """

    id: str
    outcomes: str
    clobTokenIds: str
    createdAt: NotRequired[str]


class _Bar(TypedDict):
    t: float  # Unix seconds; json.loads returns int, mypy accepts int for float
    p: float


class _LookupRow(TypedDict):
    market_id: str
    yes_token_id: str
    created_at: str


class _ManifestRow(TypedDict):
    market_id: str
    status: str
    bar_count: int
    first_ts: int
    last_ts: int
    error_reason: str | None
    run_id: str
    completed_at: str
    attempt_count: int


class _ResumeEntry(TypedDict):
    status: str
    attempt_count: int


# ── Config ────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class IngestConfig:
    max_workers: int = 4
    request_timeout: float = 30.0
    retry_max: int = 5
    retry_backoff_base: float = 0.5
    target_path_root: Path = Path("data/bars")
    manifest_path: Path = Path("data/bars/_manifest.parquet")
    lookup_path: Path = Path("data/bars/_market_lookup.parquet")
    upper_padding_seconds: int = 60
    bar_density_max_median_dt_s: float = 300.0
    min_bars: int = 10
    gamma_base_url: str = "https://gamma-api.polymarket.com"
    clob_base_url: str = "https://clob.polymarket.com"
    lookup_inter_request_delay_s: float = 0.15


# ── Lookup build ──────────────────────────────────────────────────────────────


def _fetch_gamma_market(market_id: str, config: IngestConfig) -> _GammaMarketDetail:
    """Fetch individual market detail from Gamma /markets/{id} with retry."""
    url = f"{config.gamma_base_url}/markets/{market_id}"
    req = urllib.request.Request(url, headers={"User-Agent": "pmbot/0.1"})
    for attempt in range(config.retry_max + 1):
        try:
            with urllib.request.urlopen(req, timeout=config.request_timeout) as resp:
                return cast(_GammaMarketDetail, json.loads(resp.read()))
        except urllib.error.HTTPError as exc:
            if 400 <= exc.code < 500:
                raise
            if attempt >= config.retry_max:
                raise
            delay = config.retry_backoff_base * (2.0**attempt)
            logger.warning(
                "Gamma 5xx market_id=%s attempt=%d/%d retry in %.1fs",
                market_id,
                attempt + 1,
                config.retry_max,
                delay,
            )
            time.sleep(delay)
        except OSError as exc:
            if attempt >= config.retry_max:
                raise
            delay = config.retry_backoff_base * (2.0**attempt)
            logger.warning(
                "Gamma network error market_id=%s attempt=%d/%d retry in %.1fs: %s",
                market_id,
                attempt + 1,
                config.retry_max,
                delay,
                exc,
            )
            time.sleep(delay)
    raise RuntimeError("unreachable")  # pragma: no cover


def _extract_yes_token_id(record: _GammaMarketDetail, market_id: str) -> str:
    """Extract the YES CLOB token ID.

    clobTokenIds is a JSON-encoded string (NOT a native array) — confirmed by spike.
    YES position is derived from outcomes array, not assumed to be index 0.
    Raises RuntimeError on any unexpected shape so the operator sees it immediately.
    """
    raw_token_ids = record.get("clobTokenIds")
    if not isinstance(raw_token_ids, str):
        raise RuntimeError(
            f"market_id={market_id}: clobTokenIds missing or not a string: {raw_token_ids!r}"
        )
    try:
        token_ids: list[str] = json.loads(raw_token_ids)
    except (json.JSONDecodeError, ValueError) as exc:
        raise RuntimeError(
            f"market_id={market_id}: clobTokenIds is not valid JSON: {raw_token_ids!r}"
        ) from exc
    if not isinstance(token_ids, list) or not token_ids:
        raise RuntimeError(
            f"market_id={market_id}: clobTokenIds parsed to unexpected shape: {token_ids!r}"
        )

    raw_outcomes = record.get("outcomes")
    if not isinstance(raw_outcomes, str):
        raise RuntimeError(
            f"market_id={market_id}: outcomes missing or not a string: {raw_outcomes!r}"
        )
    try:
        outcomes: list[str] = json.loads(raw_outcomes)
    except (json.JSONDecodeError, ValueError) as exc:
        raise RuntimeError(
            f"market_id={market_id}: outcomes is not valid JSON: {raw_outcomes!r}"
        ) from exc

    try:
        yes_index = next(i for i, o in enumerate(outcomes) if o.strip().lower() == "yes")
    except StopIteration:
        raise RuntimeError(
            f"market_id={market_id}: no 'Yes' entry in outcomes: {outcomes!r}"
        )
    if yes_index >= len(token_ids):
        raise RuntimeError(
            f"market_id={market_id}: yes_index={yes_index} out of range "
            f"for token_ids (len={len(token_ids)})"
        )
    return token_ids[yes_index]


def _fetch_lookup_row(
    market_id: str, config: IngestConfig
) -> _LookupRow | None:
    """Fetch and extract one lookup row. Returns None if createdAt is absent."""
    record = _fetch_gamma_market(market_id, config)
    yes_token_id = _extract_yes_token_id(record, market_id)

    created_at_raw = record.get("createdAt")
    if not created_at_raw or not isinstance(created_at_raw, str):
        logger.error("market_id=%s: createdAt absent from Gamma response", market_id)
        return None

    created_at = created_at_raw.replace("Z", "+00:00")
    return _LookupRow(
        market_id=market_id, yes_token_id=yes_token_id, created_at=created_at
    )


def _fetch_lookup_rows(
    market_ids: list[str], config: IngestConfig
) -> tuple[list[_LookupRow], list[str]]:
    """Fetch lookup rows for a list of market_ids.

    Returns (rows_fetched, unresolved_ids). Serial execution at
    lookup_inter_request_delay_s cadence to keep clean logs.
    """
    rows: list[_LookupRow] = []
    unresolved: list[str] = []
    total = len(market_ids)
    for i, market_id in enumerate(market_ids):
        if i > 0:
            time.sleep(config.lookup_inter_request_delay_s)
        if i % 100 == 0:
            logger.info("lookup fetch: %d/%d", i, total)
        try:
            row = _fetch_lookup_row(market_id, config)
        except Exception as exc:
            logger.error("lookup fetch failed market_id=%s: %s", market_id, exc)
            unresolved.append(market_id)
            continue
        if row is None:
            unresolved.append(market_id)
        else:
            rows.append(row)
    logger.info(
        "lookup fetch complete: %d rows, %d unresolved",
        len(rows),
        len(unresolved),
    )
    return rows, unresolved


def _write_lookup_parquet(rows: list[_LookupRow], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame(
        {
            "market_id": pl.Series([r["market_id"] for r in rows], dtype=pl.String),
            "yes_token_id": pl.Series([r["yes_token_id"] for r in rows], dtype=pl.String),
            "created_at": pl.Series([r["created_at"] for r in rows], dtype=pl.String),
        }
    )
    tmp = Path(str(path) + ".tmp")
    df.write_parquet(tmp)
    os.replace(tmp, path)


def ensure_lookup(
    market_ids: list[str], config: IngestConfig, *, rebuild: bool = False
) -> list[str]:
    """Ensure _market_lookup.parquet covers all market_ids.

    Three-path logic:
    1. File absent or rebuild=True: full cold build.
    2. File present, no rebuild: fetch only missing market_ids, append atomically.
    3. rebuild=True: delete existing file, then full cold build.

    Returns list of market_ids that could not be resolved (missing createdAt
    or Gamma fetch error). Callers must pre-populate manifest fail rows for these.
    """
    if rebuild and config.lookup_path.exists():
        config.lookup_path.unlink()

    existing_rows: list[_LookupRow] = []
    if config.lookup_path.exists():
        existing_df = pl.read_parquet(config.lookup_path)
        existing_ids = set(existing_df["market_id"].to_list())
        missing_ids = [mid for mid in market_ids if mid not in existing_ids]
        if not missing_ids:
            logger.info("lookup already covers all %d markets", len(market_ids))
            return []
        logger.info("lookup delta-fetch: %d new market_ids", len(missing_ids))
        existing_rows = [
            _LookupRow(
                market_id=str(row["market_id"]),
                yes_token_id=str(row["yes_token_id"]),
                created_at=str(row["created_at"]),
            )
            for row in existing_df.to_dicts()
        ]
    else:
        missing_ids = market_ids
        logger.info("lookup cold build: %d market_ids", len(missing_ids))

    new_rows, unresolved = _fetch_lookup_rows(missing_ids, config)
    _write_lookup_parquet(existing_rows + new_rows, config.lookup_path)
    return unresolved


# ── Window derivation ─────────────────────────────────────────────────────────


def derive_window(
    created_at: str,
    end_date: str,
    resolved_at: str,
    config: IngestConfig,
) -> tuple[int, int]:
    """Derive (start_ts, end_ts) from market metadata.

    start_ts = createdAt unix seconds.
    end_ts   = min(end_date, resolved_at) unix seconds + upper_padding_seconds.
    Pure function — no I/O.
    """
    start_ts = int(datetime.fromisoformat(created_at).timestamp())
    end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
    if end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=timezone.utc)
    resolved_dt = datetime.fromisoformat(resolved_at.replace("Z", "+00:00"))
    if resolved_dt.tzinfo is None:
        resolved_dt = resolved_dt.replace(tzinfo=timezone.utc)
    upper = min(end_dt, resolved_dt)
    end_ts = int(upper.timestamp()) + config.upper_padding_seconds
    return start_ts, end_ts


# ── CLOB bar fetch ────────────────────────────────────────────────────────────


def fetch_bars(
    token_id: str, start_ts: int, end_ts: int, config: IngestConfig
) -> list[_Bar]:
    """Fetch 1-minute bars from CLOB /prices-history with retry.

    Response shape: {"history": [{t, p}, ...]} — confirmed by spike 2026-04-29.
    Retry on OSError (covers URLError, TimeoutError, RemoteDisconnected,
    ConnectionResetError, ConnectionAbortedError) and 5xx errors.
    4xx errors re-raise immediately.
    """
    params = urllib.parse.urlencode(
        {
            "market": token_id,
            "fidelity": "1",
            "startTs": str(start_ts),
            "endTs": str(end_ts),
        }
    )
    url = f"{config.clob_base_url}/prices-history?{params}"
    req = urllib.request.Request(url, headers={"User-Agent": "pmbot/0.1"})

    for attempt in range(config.retry_max + 1):
        try:
            with urllib.request.urlopen(req, timeout=config.request_timeout) as resp:
                data: dict[str, list[_Bar]] = json.loads(resp.read())
                return data.get("history", [])
        except urllib.error.HTTPError as exc:
            if 400 <= exc.code < 500:
                raise
            if attempt >= config.retry_max:
                raise
            delay = config.retry_backoff_base * (2.0**attempt)
            logger.warning(
                "CLOB 5xx attempt=%d/%d retry in %.1fs", attempt + 1, config.retry_max, delay
            )
            time.sleep(delay)
        except OSError as exc:
            if attempt >= config.retry_max:
                raise
            delay = config.retry_backoff_base * (2.0**attempt)
            logger.warning(
                "CLOB network error attempt=%d/%d retry in %.1fs: %s",
                attempt + 1,
                config.retry_max,
                delay,
                exc,
            )
            time.sleep(delay)
    raise RuntimeError("unreachable")  # pragma: no cover


# ── Hard-fail predicates ──────────────────────────────────────────────────────


def validate_bars(
    bars: list[_Bar],
    start_ts: int,
    end_ts: int,
    config: IngestConfig,
) -> tuple[bool, str | None]:
    """Return (valid, error_reason). All predicates are independent; first failure wins.

    Rejects: bar_count < min_bars, median(delta_t) > max_median_dt_s,
             any bar outside [start_ts, end_ts + upper_padding_seconds].
    Does NOT reject on max(delta_t) — real thin markets have real gaps.
    """
    if len(bars) < config.min_bars:
        return False, "bar_count_below_min"

    deltas = [bars[i + 1]["t"] - bars[i]["t"] for i in range(len(bars) - 1)]
    if statistics.median(deltas) > config.bar_density_max_median_dt_s:
        return False, "median_dt_exceeds_threshold"

    upper_bound = end_ts + config.upper_padding_seconds
    for bar in bars:
        if bar["t"] < start_ts or bar["t"] > upper_bound:
            return False, "bar_outside_window"

    return True, None


# ── Bar write (manual Hive partitioning) ──────────────────────────────────────


def write_bars(bars: list[_Bar], market_id: str, config: IngestConfig) -> int:
    """Write bars to Hive-partitioned Parquet. Files contain only {t, p}.

    Polars 1.40 includes partition columns in written files when using
    write_parquet(..., partition_by=[...]). We construct Hive paths manually
    and write {t, p} only to guarantee the stored schema is exactly {t, p}.
    """
    if not bars:
        return 0
    ts = [int(b["t"]) for b in bars]
    ps = [float(b["p"]) for b in bars]
    df = pl.DataFrame(
        {
            "t": pl.Series(ts, dtype=pl.Int64),
            "p": pl.Series(ps, dtype=pl.Float64),
        }
    )
    df = df.with_columns(
        pl.from_epoch(pl.col("t"), time_unit="s").dt.date().alias("utc_date")
    )
    for date_val in df["utc_date"].unique().sort().to_list():
        date_str = str(date_val)
        partition_dir = (
            config.target_path_root / f"market_id={market_id}" / f"utc_date={date_str}"
        )
        partition_dir.mkdir(parents=True, exist_ok=True)
        date_df = df.filter(pl.col("utc_date") == date_val).select(["t", "p"])
        date_df.write_parquet(partition_dir / "00000000.parquet", compression="zstd")
    return len(bars)


# ── Manifest handling ─────────────────────────────────────────────────────────


def _write_manifest_temp(
    config: IngestConfig,
    run_id: str,
    market_id: str,
    *,
    status: str,
    bar_count: int,
    first_ts: int,
    last_ts: int,
    error_reason: str | None,
    attempt_count: int,
) -> None:
    """Write a single-row manifest Parquet atomically via .tmp + os.replace."""
    completed_at = datetime.now(timezone.utc).isoformat()
    df = pl.DataFrame(
        {
            "market_id": pl.Series([market_id], dtype=pl.String),
            "status": pl.Series([status], dtype=pl.String),
            "bar_count": pl.Series([bar_count], dtype=pl.Int64),
            "first_ts": pl.Series([first_ts], dtype=pl.Int64),
            "last_ts": pl.Series([last_ts], dtype=pl.Int64),
            "error_reason": pl.Series([error_reason], dtype=pl.String),
            "run_id": pl.Series([run_id], dtype=pl.String),
            "completed_at": pl.Series([completed_at], dtype=pl.String),
            "attempt_count": pl.Series([attempt_count], dtype=pl.Int64),
        }
    )
    path = config.target_path_root / f"_manifest_{run_id}_{market_id}.parquet"
    tmp = Path(str(path) + ".tmp")
    config.target_path_root.mkdir(parents=True, exist_ok=True)
    df.write_parquet(tmp)
    os.replace(tmp, path)


def _collect_manifest_frames(config: IngestConfig) -> list[pl.DataFrame]:
    """Read consolidated manifest + all temp files. Skips unreadable temps with a warning."""
    frames: list[pl.DataFrame] = []
    if config.manifest_path.exists():
        frames.append(pl.read_parquet(config.manifest_path))
    for tf in sorted(config.target_path_root.glob("_manifest_*_*.parquet")):
        try:
            frames.append(pl.read_parquet(tf))
        except Exception as exc:
            logger.warning("Skipping unreadable manifest temp %s: %s", tf.name, exc)
    return frames


def _load_resume_state(config: IngestConfig) -> dict[str, _ResumeEntry]:
    """Build resume state dict from consolidated + temp manifests.

    Returns {market_id: {status, attempt_count}} using latest-attempt-wins logic.
    """
    frames = _collect_manifest_frames(config)
    if not frames:
        return {}
    df = pl.concat(frames)
    df = df.sort(["attempt_count", "completed_at"])
    df = df.unique(subset=["market_id"], keep="last")
    return {
        str(row["market_id"]): _ResumeEntry(
            status=str(row["status"]),
            attempt_count=int(row["attempt_count"]),
        )
        for row in df.to_dicts()
    }


def _consolidate_manifest(config: IngestConfig) -> None:
    """Merge per-market temp files + consolidated manifest. Atomic overwrite."""
    frames = _collect_manifest_frames(config)
    if not frames:
        return
    temp_files = list(config.target_path_root.glob("_manifest_*_*.parquet"))
    df = pl.concat(frames)
    df = df.sort(["attempt_count", "completed_at"])
    df = df.unique(subset=["market_id"], keep="last")
    tmp = Path(str(config.manifest_path) + ".tmp")
    config.manifest_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(tmp)
    os.replace(tmp, config.manifest_path)
    for tf in temp_files:
        tf.unlink(missing_ok=True)
    logger.info(
        "manifest consolidated: %d rows (%d ok, %d fail)",
        len(df),
        df.filter(pl.col("status") == "ok").height,
        df.filter(pl.col("status") == "fail").height,
    )


# ── Worker ────────────────────────────────────────────────────────────────────


def _worker(
    market_id: str,
    csv_row: dict[str, str],
    lookup_row: _LookupRow,
    attempt_count: int,
    run_id: str,
    config: IngestConfig,
) -> None:
    """Fetch, validate, and write bars for one market. Writes manifest temp on success or failure."""
    try:
        start_ts, end_ts = derive_window(
            lookup_row["created_at"],
            csv_row["end_date"],
            csv_row["resolved_at"],
            config,
        )
        bars = fetch_bars(lookup_row["yes_token_id"], start_ts, end_ts, config)
        valid, reason = validate_bars(bars, start_ts, end_ts, config)
        if not valid:
            _write_manifest_temp(
                config,
                run_id,
                market_id,
                status="fail",
                bar_count=len(bars),
                first_ts=0,
                last_ts=0,
                error_reason=reason,
                attempt_count=attempt_count,
            )
            logger.warning(
                "market_id=%s validation failed: %s (bar_count=%d)",
                market_id,
                reason,
                len(bars),
            )
            return
        write_bars(bars, market_id, config)
        first_ts = int(bars[0]["t"]) if bars else 0
        last_ts = int(bars[-1]["t"]) if bars else 0
        _write_manifest_temp(
            config,
            run_id,
            market_id,
            status="ok",
            bar_count=len(bars),
            first_ts=first_ts,
            last_ts=last_ts,
            error_reason=None,
            attempt_count=attempt_count,
        )
        logger.debug("market_id=%s done: bar_count=%d", market_id, len(bars))
    except Exception as exc:
        logger.exception("worker error market_id=%s: %s", market_id, exc)
        _write_manifest_temp(
            config,
            run_id,
            market_id,
            status="fail",
            bar_count=0,
            first_ts=0,
            last_ts=0,
            error_reason=str(exc)[:200],
            attempt_count=attempt_count,
        )


# ── CSV loading ───────────────────────────────────────────────────────────────


def _load_training_markets(resolved_csv: Path) -> list[dict[str, str]]:
    """Read resolved_markets.csv, return rows where flags is empty string."""
    rows: list[dict[str, str]] = []
    with resolved_csv.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            if row.get("flags", "X") == "":
                rows.append(dict(row))
    return rows


# ── Main ingestion entry point ────────────────────────────────────────────────


def run_ingest(
    resolved_csv: Path,
    config: IngestConfig,
    *,
    dry_run: bool = False,
    resume: bool = False,
    limit: int | None = None,
    offset: int = 0,
    rebuild_lookup: bool = False,
    lookup_only: bool = False,
) -> None:
    """Ingest price bars for all training-eligible markets.

    Importable entry point — raises on hard failures instead of SystemExit.
    """
    all_rows = _load_training_markets(resolved_csv)
    all_rows.sort(key=lambda r: r["market_id"])
    sliced = all_rows[offset : offset + limit] if limit is not None else all_rows[offset:]
    market_ids = [r["market_id"] for r in sliced]
    market_map = {r["market_id"]: r for r in sliced}
    logger.info("training-eligible slice: %d markets (offset=%d limit=%s)", len(sliced), offset, limit)

    # ── Lookup pre-pass ───────────────────────────────────────────────────────
    if dry_run:
        logger.info("dry-run: would ensure lookup for %d markets", len(market_ids))
        unresolved_ids: list[str] = []
    else:
        unresolved_ids = ensure_lookup(market_ids, config, rebuild=rebuild_lookup)

    if lookup_only:
        logger.info("--lookup-only: done")
        return

    # ── Load lookup ───────────────────────────────────────────────────────────
    if not config.lookup_path.exists():
        raise RuntimeError(f"Lookup Parquet not found: {config.lookup_path}")
    lookup_df = pl.read_parquet(config.lookup_path)
    lookup_dict: dict[str, _LookupRow] = {
        str(row["market_id"]): _LookupRow(
            market_id=str(row["market_id"]),
            yes_token_id=str(row["yes_token_id"]),
            created_at=str(row["created_at"]),
        )
        for row in lookup_df.to_dicts()
    }

    # ── Resume state ──────────────────────────────────────────────────────────
    run_id = uuid4().hex[:8]
    resume_state: dict[str, _ResumeEntry] = {}
    if resume:
        resume_state = _load_resume_state(config)
        skipped = sum(1 for e in resume_state.values() if e["status"] == "ok")
        logger.info("resume: %d previously ok markets will be skipped", skipped)

    # ── Pre-write fail rows for unresolved (missing createdAt / fetch error) ──
    unresolved_set = set(unresolved_ids)
    if not dry_run:
        for uid in unresolved_ids:
            prior = resume_state.get(uid)
            attempt_count = (prior["attempt_count"] + 1) if prior else 1
            _write_manifest_temp(
                config,
                run_id,
                uid,
                status="fail",
                bar_count=0,
                first_ts=0,
                last_ts=0,
                error_reason="missing_created_at",
                attempt_count=attempt_count,
            )

    # ── Hard-fail: markets missing from lookup with no explanation ────────────
    bar_market_ids = [mid for mid in market_ids if mid not in unresolved_set]
    missing_from_lookup = [mid for mid in bar_market_ids if mid not in lookup_dict]
    if missing_from_lookup:
        raise RuntimeError(
            f"{len(missing_from_lookup)} market(s) absent from lookup after ensure_lookup "
            f"(first 5: {missing_from_lookup[:5]}). Run with --rebuild-lookup to force refresh."
        )

    # ── Build work items ──────────────────────────────────────────────────────
    work_items: list[tuple[str, dict[str, str], _LookupRow, int]] = []
    for mid in bar_market_ids:
        prior = resume_state.get(mid)
        if prior and prior["status"] == "ok":
            continue
        attempt_count = (prior["attempt_count"] + 1) if prior else 1
        work_items.append((mid, market_map[mid], lookup_dict[mid], attempt_count))

    if dry_run:
        for mid, _, lrow, _ in work_items:
            logger.info(
                "dry-run: would fetch market_id=%s yes_token_id=%.20s...",
                mid,
                lrow["yes_token_id"],
            )
        return

    logger.info("starting bar fetch: %d markets (max_workers=%d)", len(work_items), config.max_workers)

    # ── Concurrent bar fetch ──────────────────────────────────────────────────
    with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
        futures = {
            executor.submit(_worker, mid, csv_row, lrow, attempt, run_id, config): mid
            for mid, csv_row, lrow, attempt in work_items
        }
        done_count = 0
        for future in as_completed(futures):
            mid = futures[future]
            done_count += 1
            try:
                future.result()
            except Exception as exc:
                logger.error(
                    "market_id=%s raised unexpectedly from worker: %s", mid, exc
                )
            if done_count % 100 == 0:
                logger.info("progress: %d/%d markets complete", done_count, len(work_items))

    # ── Manifest consolidation ────────────────────────────────────────────────
    _consolidate_manifest(config)


# ── CLI ───────────────────────────────────────────────────────────────────────


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="Phase 1.1 Pass 2: ingest 1-minute price bars for training-eligible markets."
    )
    parser.add_argument(
        "--resolved-csv",
        type=Path,
        default=Path("data/resolutions/resolved_markets.csv"),
        help="Path to resolved_markets.csv (default: data/resolutions/resolved_markets.csv)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log planned fetches without writing anything",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip markets with status=ok; retry status=fail with attempt_count++",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Process first N training-eligible markets (deterministic sort by market_id)",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        metavar="N",
        help="Start at row N of the sorted training-eligible markets",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=None,
        metavar="N",
        help="Override default max_workers (default: 4)",
    )
    parser.add_argument(
        "--rebuild-lookup",
        action="store_true",
        help="Delete and fully rebuild _market_lookup.parquet",
    )
    parser.add_argument(
        "--lookup-only",
        action="store_true",
        help="Run only the lookup pre-pass, then exit",
    )
    args = parser.parse_args(argv)

    config_kwargs: dict[str, object] = {}
    if args.max_workers is not None:
        config_kwargs["max_workers"] = args.max_workers
    config = IngestConfig(**config_kwargs)  # type: ignore[arg-type]

    run_ingest(
        args.resolved_csv,
        config,
        dry_run=args.dry_run,
        resume=args.resume,
        limit=args.limit,
        offset=args.offset,
        rebuild_lookup=args.rebuild_lookup,
        lookup_only=args.lookup_only,
    )


if __name__ == "__main__":
    main()
