"""Phase 1.6 — Data Quality Validation for the pmbot bar corpus.

Produces four outputs:
  data/bars/_qa_market.parquet        per-market verdict
  data/bars/_qa_windows.parquet       per-window detail
  data/bars/_qa_distributions.parquet Step 0 empirical pre-pass
  data/bars_clean/                    cleaned bars (usable-window bars only)

Design notes
------------
Chunk-seam phantom: PROGRESS.md 2026-05-01 noted potential 1s gaps at 14-day
chunk boundaries from Pass 2's window splitting. Step 0 empirical pre-pass
finds ZERO bars with dt=1s across all 7,021 markets; only 254 bars with
dt in [1,5]s across 160 markets. CLOB fidelity=1 returns naturally-occurring
bars, not synthetic boundary bars. No special handling; noted to prevent
future investigation.

Lenient-upper-bound (35 markets): Pass 2 fell back to resolved_at as the
upper bound when end_date was empty. These 35 status=ok markets are identified
by joining data/resolutions/resolved_markets.csv (end_date empty, resolved_at
present). Their bars may extend slightly past strict end_date. Phase 2 decides
whether to exclude; Phase 1.6 flags them via lenient_upper_bound=True.

Phase 1.6 vs Pass 2 predicates: Pass 2's min_bars=10 and median(dt)>300s are
endpoint-fidelity checks ("did the API return 1-min bars at all?"). Phase 1.6's
predicates are training-quality checks ("is this market usable for training?").
Downstream consumers read _qa_market.parquet for usability decisions, never
Pass 2's predicates.

min_bars_per_market=60 rationale: the original draft used 360 (6h) but Step 0.5
showed that threshold would drop 1,021 markets, 98.7% of which are short-dated
BTC strike markets (btc-multistrike-4h and will-the-price-of-bitcoin-be) --
exactly the Phase 2 Oracle's training target per SPEC §2.1. Lowered to 60 to
match min_bars_per_window; the per-window predicate already enforces the 1h
floor, making the per-market predicate a hard-fail safety net only.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TypedDict, cast

import duckdb
import polars as pl

logger = logging.getLogger(__name__)


# ── Categorical reason vocabulary ─────────────────────────────────────────────

# Per-window rejection reasons
_REASON_WINDOW_TOO_SHORT = "window_too_short"

# Per-market rejection reasons
_REASON_NO_BAR_FILES = "no_bar_files"
_REASON_TOO_FEW_TOTAL_BARS = "too_few_total_bars"
_REASON_TOO_MANY_REJECTED_WINDOWS = "too_many_rejected_windows"

# Per-market usable reasons (qa_status == "usable")
_REASON_SOME_WINDOWS_REJECTED = "some_windows_rejected"
_REASON_GAP_SPLIT = "gap_split"
_REASON_BARS_DROPPED = "bars_dropped"

# Frozenset captures literal values at load time — immune to monkeypatching of
# the individual constants, enabling the unknown-condition guard to detect
# when a patched constant injects a sentinel not in the real vocabulary.
_USABLE_REASON_VOCAB: frozenset[str] = frozenset(
    {_REASON_SOME_WINDOWS_REJECTED, _REASON_GAP_SPLIT, _REASON_BARS_DROPPED}
)


class _UnknownQACondition(RuntimeError):
    """Raised when a market reaches a state not covered by the reason vocabulary."""


# ── Config ────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class QAConfig:
    gap_threshold_seconds: int = 300
    min_bars_per_window: int = 60
    min_bars_per_market: int = 60
    max_rejected_window_fraction: float = 0.5
    bars_root: Path = field(default_factory=lambda: Path("data/bars"))
    bars_clean_root: Path = field(default_factory=lambda: Path("data/bars_clean"))
    qa_market_path: Path = field(default_factory=lambda: Path("data/bars/_qa_market.parquet"))
    qa_windows_path: Path = field(default_factory=lambda: Path("data/bars/_qa_windows.parquet"))
    qa_distributions_path: Path = field(
        default_factory=lambda: Path("data/bars/_qa_distributions.parquet")
    )
    resolutions_csv_path: Path = field(
        default_factory=lambda: Path("data/resolutions/resolved_markets.csv")
    )


# ── Internal result types ─────────────────────────────────────────────────────


@dataclass
class _WindowResult:
    window_idx: int
    window_start_ts: int
    window_end_ts: int
    bar_count: int
    median_dt: float
    max_dt: float
    usable: bool
    reason: str | None


@dataclass
class _MarketQAResult:
    """Result of qa_one_market. clean_df contains only usable-window bars."""

    qa_status: str
    qa_reason: str | None
    # clean_bar_count = bars after dedup+nonfinite drop, all windows combined
    clean_bar_count: int
    # dropped_bar_count = raw bars minus clean_bar_count (price-quality drops only)
    dropped_bar_count: int
    window_count: int
    usable_window_count: int
    # total_usable_bar_count = sum of bars in usable windows = what goes to bars_clean/
    total_usable_bar_count: int
    windows: list[_WindowResult]
    # Bars from usable windows only — Phase 2 reads this directly without
    # consulting _qa_windows.parquet to identify gap boundaries.
    clean_df: pl.DataFrame


# ── Ledger row TypedDicts (one row per parquet output) ────────────────────────


class _MarketRow(TypedDict):
    market_id: int
    qa_status: str
    qa_reason: str | None
    clean_bar_count: int
    dropped_bar_count: int
    window_count: int
    usable_window_count: int
    total_usable_bar_count: int
    lenient_upper_bound: bool
    run_id: str
    completed_at: str


class _WindowRow(TypedDict):
    market_id: int
    window_idx: int
    window_start_ts: int
    window_end_ts: int
    bar_count: int
    median_dt: float
    max_dt: float
    usable: bool
    reason: str | None
    run_id: str
    completed_at: str


class RunSummary(TypedDict):
    run_id: str
    total_markets: int
    clean: int
    usable: int
    rejected: int
    completed_at: str


# ── JSON structured logger ────────────────────────────────────────────────────


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        extra = getattr(record, "extra_fields", None)
        if isinstance(extra, dict):
            for k, v in extra.items():
                payload[k] = v
        return json.dumps(payload, separators=(",", ":"), default=str)


# ── Path safety ───────────────────────────────────────────────────────────────


def validate_immutable_target_path(path: Path, config: QAConfig) -> None:
    """Raise RuntimeError if path would write inside a bars_root partition dir.

    data/bars/market_id=*/ is immutable after Pass 2. This guard enforces
    that QA writes only to bars_root/_qa_*.parquet or to bars_clean_root/.
    """
    try:
        rel = path.relative_to(config.bars_root)
    except ValueError:
        return  # path not under bars_root — fine
    parts = rel.parts
    if parts and parts[0].startswith("market_id="):
        raise RuntimeError(
            f"Attempted write inside immutable bars_root partition: {path}. "
            "QA must not modify data/bars/market_id=*/ directories."
        )


# ── Manifest / resolutions readers ────────────────────────────────────────────


def load_ok_market_ids(config: QAConfig) -> list[int]:
    """Return sorted list of market_ids with status=ok in the Pass 2 manifest."""
    manifest = pl.read_parquet(config.bars_root / "_manifest.parquet")
    ids = (
        manifest.filter(pl.col("status") == "ok")
        .select(pl.col("market_id").cast(pl.Int64))["market_id"]
        .to_list()
    )
    return sorted(int(x) for x in ids)


def load_lenient_market_ids(config: QAConfig) -> set[int]:
    """Return market_ids where Pass 2 used resolved_at as upper bound (end_date empty).

    These 35 markets are identified by joining resolved_markets.csv where
    end_date is empty/null AND resolved_at is non-empty.
    """
    if not config.resolutions_csv_path.exists():
        logger.warning(
            "resolutions CSV not found; lenient_upper_bound will be False for all markets",
            extra={"extra_fields": {"path": str(config.resolutions_csv_path)}},
        )
        return set()
    csv = pl.read_csv(config.resolutions_csv_path, infer_schema_length=0)
    lenient = csv.filter(
        (pl.col("end_date").is_null() | (pl.col("end_date").str.strip_chars() == ""))
        & pl.col("resolved_at").is_not_null()
        & (pl.col("resolved_at").str.strip_chars() != "")
    ).select(pl.col("market_id").cast(pl.Int64))["market_id"]
    return {int(x) for x in lenient.to_list()}


# ── Core QA logic (pure) ──────────────────────────────────────────────────────


def _split_into_windows(
    bars_df: pl.DataFrame,
    gap_threshold_seconds: int,
) -> list[pl.DataFrame]:
    """Split sorted bars into contiguous windows at gaps > gap_threshold_seconds.

    Returns list of DataFrames (columns: t, p) in chronological order,
    each representing a maximal run with no internal gap > threshold.
    """
    if bars_df.is_empty():
        return []
    t_series = bars_df["t"]
    dt = t_series.diff()  # Int64, first element is null
    # New window starts where dt is null (first bar) or dt > threshold
    is_new = dt.is_null() | (dt > gap_threshold_seconds)
    seg_idx = is_new.cast(pl.Int32).cum_sum() - 1  # 0-indexed
    with_seg = bars_df.with_columns(seg_idx.alias("_seg"))
    return [p.drop("_seg") for p in with_seg.partition_by("_seg", maintain_order=True)]


def qa_one_market(
    market_id: int,
    bars_df: pl.DataFrame,
    config: QAConfig,
) -> _MarketQAResult:
    """QA a single market. Pure function — no I/O.

    bars_df must have columns t (Int64) and p (Float64). Bars need not be
    pre-sorted or pre-cleaned; this function handles ordering.

    The returned clean_df contains ONLY bars from windows whose bar_count
    >= min_bars_per_window (usable windows). Phase 2 reads clean_df directly
    without consulting _qa_windows.parquet to filter gap-spanning sequences.

    Raises _UnknownQACondition if a market reaches a state not in the
    categorical reason vocabulary (hard-fail over silent-pass).
    """
    # ── Empty input ────────────────────────────────────────────────────────────
    if bars_df.is_empty():
        empty = pl.DataFrame(
            {"t": pl.Series([], dtype=pl.Int64), "p": pl.Series([], dtype=pl.Float64)}
        )
        return _MarketQAResult(
            qa_status="rejected",
            qa_reason=_REASON_NO_BAR_FILES,
            clean_bar_count=0,
            dropped_bar_count=0,
            window_count=0,
            usable_window_count=0,
            total_usable_bar_count=0,
            windows=[],
            clean_df=empty,
        )

    raw_bar_count = bars_df.height

    # ── Price cleaning: drop non-finite and non-positive p ────────────────────
    price_cleaned = bars_df.filter(pl.col("p").is_finite() & (pl.col("p") > 0.0))

    # ── Dedup duplicate t (keep last occurrence in sort order) ────────────────
    deduped = (
        price_cleaned.sort("t").unique(subset=["t"], keep="last", maintain_order=True).sort("t")
    )

    clean_bar_count = deduped.height
    dropped_bar_count = raw_bar_count - clean_bar_count

    # ── Window split ──────────────────────────────────────────────────────────
    windows_dfs = _split_into_windows(deduped, config.gap_threshold_seconds)

    # ── Per-window verdict ────────────────────────────────────────────────────
    window_results: list[_WindowResult] = []
    usable_dfs: list[pl.DataFrame] = []

    for idx, win_df in enumerate(windows_dfs):
        bar_count = win_df.height
        t_w = win_df["t"]
        if bar_count > 1:
            dts = t_w.diff().drop_nulls()
            med_val = cast(float | None, dts.cast(pl.Float64).median())
            max_raw = cast(int | None, dts.max())
            med_dt = float(med_val) if med_val is not None else 0.0
            max_dt = float(max_raw) if max_raw is not None else 0.0
        else:
            med_dt = 0.0
            max_dt = 0.0

        usable = bar_count >= config.min_bars_per_window
        win_reason: str | None = _REASON_WINDOW_TOO_SHORT if not usable else None

        window_results.append(
            _WindowResult(
                window_idx=idx,
                window_start_ts=int(t_w[0]),
                window_end_ts=int(t_w[-1]),
                bar_count=bar_count,
                median_dt=med_dt,
                max_dt=max_dt,
                usable=usable,
                reason=win_reason,
            )
        )
        if usable:
            usable_dfs.append(win_df)

    window_count = len(window_results)
    usable_window_count = sum(1 for w in window_results if w.usable)
    rejected_window_count = window_count - usable_window_count

    if usable_dfs:
        clean_final = pl.concat(usable_dfs)
        total_usable_bar_count = clean_final.height
    else:
        clean_final = pl.DataFrame(
            {"t": pl.Series([], dtype=pl.Int64), "p": pl.Series([], dtype=pl.Float64)}
        )
        total_usable_bar_count = 0

    # ── Per-market predicates ─────────────────────────────────────────────────
    if total_usable_bar_count < config.min_bars_per_market:
        return _MarketQAResult(
            qa_status="rejected",
            qa_reason=_REASON_TOO_FEW_TOTAL_BARS,
            clean_bar_count=clean_bar_count,
            dropped_bar_count=dropped_bar_count,
            window_count=window_count,
            usable_window_count=usable_window_count,
            total_usable_bar_count=total_usable_bar_count,
            windows=window_results,
            clean_df=clean_final,
        )

    rej_frac = rejected_window_count / window_count if window_count > 0 else 0.0
    if rej_frac > config.max_rejected_window_fraction:
        return _MarketQAResult(
            qa_status="rejected",
            qa_reason=_REASON_TOO_MANY_REJECTED_WINDOWS,
            clean_bar_count=clean_bar_count,
            dropped_bar_count=dropped_bar_count,
            window_count=window_count,
            usable_window_count=usable_window_count,
            total_usable_bar_count=total_usable_bar_count,
            windows=window_results,
            clean_df=clean_final,
        )

    # ── qa_status: clean vs usable ────────────────────────────────────────────
    is_clean = window_count == 1 and usable_window_count == 1 and dropped_bar_count == 0
    if is_clean:
        qa_status = "clean"
        mkt_reason: str | None = None
    else:
        qa_status = "usable"
        mkt_reason = None
        if rejected_window_count > 0:
            mkt_reason = _REASON_SOME_WINDOWS_REJECTED
        elif window_count > 1:
            mkt_reason = _REASON_GAP_SPLIT
        elif dropped_bar_count > 0:
            mkt_reason = _REASON_BARS_DROPPED
        if mkt_reason not in _USABLE_REASON_VOCAB:
            raise _UnknownQACondition(
                f"market_id={market_id}: qa_status=usable but reason {mkt_reason!r} "
                f"not in vocabulary {_USABLE_REASON_VOCAB!r}"
            )

    return _MarketQAResult(
        qa_status=qa_status,
        qa_reason=mkt_reason,
        clean_bar_count=clean_bar_count,
        dropped_bar_count=dropped_bar_count,
        window_count=window_count,
        usable_window_count=usable_window_count,
        total_usable_bar_count=total_usable_bar_count,
        windows=window_results,
        clean_df=clean_final,
    )


# ── I/O helpers ───────────────────────────────────────────────────────────────


def write_clean_bars(
    market_id: int,
    clean_df: pl.DataFrame,
    config: QAConfig,
) -> None:
    """Write clean bars to bars_clean/. Only called for clean/usable markets.

    clean_df must already contain only usable-window bars (output of
    qa_one_market). Partitions by utc_date, writes one file per partition,
    atomic .tmp + os.replace. Schema: {t: Int64, p: Float64}.
    """
    if clean_df.is_empty():
        return
    df_dated = clean_df.with_columns(
        pl.from_epoch(pl.col("t"), time_unit="s").dt.date().alias("utc_date")
    )
    for date_val in df_dated["utc_date"].unique().sort().to_list():
        date_str = str(date_val)
        partition_dir = config.bars_clean_root / f"market_id={market_id}" / f"utc_date={date_str}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        out_path = partition_dir / "00000000.parquet"
        validate_immutable_target_path(out_path, config)
        date_df = df_dated.filter(pl.col("utc_date") == date_val).select(["t", "p"])
        tmp = Path(str(out_path) + ".tmp")
        date_df.write_parquet(tmp, compression="zstd")
        os.replace(tmp, out_path)


def _write_qa_market_parquet(rows: list[_MarketRow], config: QAConfig) -> None:
    validate_immutable_target_path(config.qa_market_path, config)
    df = pl.DataFrame(
        {
            "market_id": pl.Series([r["market_id"] for r in rows], dtype=pl.Int64),
            "qa_status": pl.Series([r["qa_status"] for r in rows], dtype=pl.String),
            "qa_reason": pl.Series([r["qa_reason"] for r in rows], dtype=pl.String),
            "clean_bar_count": pl.Series([r["clean_bar_count"] for r in rows], dtype=pl.Int64),
            "dropped_bar_count": pl.Series([r["dropped_bar_count"] for r in rows], dtype=pl.Int64),
            "window_count": pl.Series([r["window_count"] for r in rows], dtype=pl.Int64),
            "usable_window_count": pl.Series(
                [r["usable_window_count"] for r in rows], dtype=pl.Int64
            ),
            "total_usable_bar_count": pl.Series(
                [r["total_usable_bar_count"] for r in rows], dtype=pl.Int64
            ),
            "lenient_upper_bound": pl.Series(
                [r["lenient_upper_bound"] for r in rows], dtype=pl.Boolean
            ),
            "run_id": pl.Series([r["run_id"] for r in rows], dtype=pl.String),
            "completed_at": pl.Series([r["completed_at"] for r in rows], dtype=pl.String),
        }
    )
    config.qa_market_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(str(config.qa_market_path) + ".tmp")
    df.write_parquet(tmp)
    os.replace(tmp, config.qa_market_path)


def _write_qa_windows_parquet(rows: list[_WindowRow], config: QAConfig) -> None:
    validate_immutable_target_path(config.qa_windows_path, config)
    df = pl.DataFrame(
        {
            "market_id": pl.Series([r["market_id"] for r in rows], dtype=pl.Int64),
            "window_idx": pl.Series([r["window_idx"] for r in rows], dtype=pl.Int32),
            "window_start_ts": pl.Series([r["window_start_ts"] for r in rows], dtype=pl.Int64),
            "window_end_ts": pl.Series([r["window_end_ts"] for r in rows], dtype=pl.Int64),
            "bar_count": pl.Series([r["bar_count"] for r in rows], dtype=pl.Int64),
            "median_dt": pl.Series([r["median_dt"] for r in rows], dtype=pl.Float64),
            "max_dt": pl.Series([r["max_dt"] for r in rows], dtype=pl.Float64),
            "usable": pl.Series([r["usable"] for r in rows], dtype=pl.Boolean),
            "reason": pl.Series([r["reason"] for r in rows], dtype=pl.String),
            "run_id": pl.Series([r["run_id"] for r in rows], dtype=pl.String),
            "completed_at": pl.Series([r["completed_at"] for r in rows], dtype=pl.String),
        }
    )
    config.qa_windows_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(str(config.qa_windows_path) + ".tmp")
    df.write_parquet(tmp)
    os.replace(tmp, config.qa_windows_path)


# ── Step 0: empirical distributions via DuckDB ────────────────────────────────


def compute_distributions(config: QAConfig) -> pl.DataFrame:
    """Step 0 empirical pre-pass. Writes _qa_distributions.parquet.

    Runs DuckDB queries over the full partitioned bar corpus to compute
    inter-bar dt histogram, per-market max-gap distribution, per-market
    bar-count percentiles, window-count at candidate N values, and gap-rate
    percentiles. Returns a long-format DataFrame with (metric, bucket, value).
    """
    con = duckdb.connect()
    con.execute("PRAGMA disable_progress_bar;")

    glob_pattern = config.bars_root.as_posix() + "/market_id=*/utc_date=*/*.parquet"
    manifest_path = (config.bars_root / "_manifest.parquet").as_posix()

    con.execute(
        f"""
        CREATE OR REPLACE TABLE _bars AS
        SELECT CAST(market_id AS BIGINT) AS market_id, t, p
        FROM read_parquet('{glob_pattern}', hive_partitioning=true)
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE _ok AS
        SELECT CAST(market_id AS BIGINT) AS market_id
        FROM read_parquet('{manifest_path}')
        WHERE status = 'ok'
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE _deltas AS
        SELECT b.market_id, b.t,
          b.t - LAG(b.t) OVER (PARTITION BY b.market_id ORDER BY b.t) AS dt
        FROM _bars b JOIN _ok USING (market_id)
        """
    )

    rows: list[dict[str, object]] = []

    # Histogram 1: inter-bar dt log-buckets
    for bucket, n in con.execute(
        """
        SELECT
          CASE
            WHEN dt IS NULL THEN 'first_bar'
            WHEN dt <= 0 THEN 'lte_0'
            WHEN dt < 60 THEN 'lt_60s'
            WHEN dt = 60 THEN 'eq_60s'
            WHEN dt <= 120 THEN 'lt_120s'
            WHEN dt <= 300 THEN 'lt_300s'
            WHEN dt <= 600 THEN 'lt_600s'
            WHEN dt <= 1800 THEN 'lt_1800s'
            WHEN dt <= 3600 THEN 'lt_3600s'
            WHEN dt <= 86400 THEN 'lt_1d'
            ELSE 'gte_1d'
          END AS bucket,
          COUNT(*) AS n
        FROM _deltas GROUP BY 1
        """
    ).fetchall():
        rows.append({"metric": "dt_histogram", "bucket": bucket, "value": float(n)})

    # Histogram 2: per-market max-gap distribution
    for bucket, n in con.execute(
        """
        WITH pm AS (SELECT market_id, MAX(dt) AS g FROM _deltas WHERE dt IS NOT NULL GROUP BY 1)
        SELECT
          CASE
            WHEN g <= 60 THEN 'lte_60s'
            WHEN g <= 120 THEN 'lt_120s'
            WHEN g <= 300 THEN 'lt_300s'
            WHEN g <= 600 THEN 'lt_600s'
            WHEN g <= 1800 THEN 'lt_1800s'
            WHEN g <= 3600 THEN 'lt_3600s'
            WHEN g <= 86400 THEN 'lt_1d'
            WHEN g <= 7*86400 THEN 'lt_7d'
            WHEN g <= 30*86400 THEN 'lt_30d'
            ELSE 'gte_30d'
          END AS bucket, COUNT(*) AS n
        FROM pm GROUP BY 1
        """
    ).fetchall():
        rows.append({"metric": "max_gap_histogram", "bucket": bucket, "value": float(n)})

    # Histogram 3: per-market bar-count percentiles
    bar_pct_row = con.execute(
        """
        WITH pm AS (SELECT market_id, COUNT(*) AS bc FROM _bars b JOIN _ok USING (market_id) GROUP BY 1)
        SELECT quantile_cont(bc, [0.0,0.01,0.05,0.10,0.25,0.50,0.75,0.90,0.95,0.99,1.0]) FROM pm
        """
    ).fetchone()
    if bar_pct_row is not None:
        pct_labels = ["p0", "p1", "p5", "p10", "p25", "p50", "p75", "p90", "p95", "p99", "p100"]
        for label, val in zip(pct_labels, bar_pct_row[0]):
            rows.append({"metric": "bar_count_percentile", "bucket": label, "value": float(val)})

    # Histogram 4: window-count at candidate N values (key calibration table)
    for n_val in (120, 300, 600, 1800):
        for wc, cnt in con.execute(
            f"""
            WITH pm AS (
              SELECT market_id, COUNT(*) FILTER (WHERE dt > {n_val}) + 1 AS wc
              FROM _deltas GROUP BY 1)
            SELECT wc, COUNT(*) FROM pm GROUP BY 1 ORDER BY 1
            """
        ).fetchall():
            rows.append(
                {
                    "metric": f"window_count_at_N{n_val}",
                    "bucket": str(wc),
                    "value": float(cnt),
                }
            )

    # Histogram 5: per-market gap-rate percentiles
    gap_pct_row = con.execute(
        """
        WITH pm AS (
          SELECT market_id,
            COUNT(*) FILTER (WHERE dt > 60) AS big,
            COUNT(*) FILTER (WHERE dt IS NOT NULL) AS itvl
          FROM _deltas GROUP BY 1)
        SELECT quantile_cont(big::DOUBLE/NULLIF(itvl,0), [0.50,0.75,0.90,0.95,0.99,1.0])
        FROM pm WHERE itvl > 0
        """
    ).fetchone()
    if gap_pct_row is not None:
        for label, val in zip(["p50", "p75", "p90", "p95", "p99", "p100"], gap_pct_row[0]):
            rows.append({"metric": "gap_rate_percentile", "bucket": label, "value": float(val)})

    dist_df = pl.DataFrame(
        {
            "metric": pl.Series([r["metric"] for r in rows], dtype=pl.String),
            "bucket": pl.Series([r["bucket"] for r in rows], dtype=pl.String),
            "value": pl.Series([r["value"] for r in rows], dtype=pl.Float64),
        }
    )

    validate_immutable_target_path(config.qa_distributions_path, config)
    config.qa_distributions_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(str(config.qa_distributions_path) + ".tmp")
    dist_df.write_parquet(tmp)
    os.replace(tmp, config.qa_distributions_path)
    logger.info(
        "distributions written",
        extra={
            "extra_fields": {
                "rows": dist_df.height,
                "path": str(config.qa_distributions_path),
            }
        },
    )
    return dist_df


# ── Orchestration ─────────────────────────────────────────────────────────────


def run_qa(
    config: QAConfig,
    *,
    dry_run: bool = False,
    limit: int = 0,
    offset: int = 0,
) -> RunSummary:
    """Full QA recompute over all status=ok markets.

    Reads bars per-market from bars_root/market_id={id}/utc_date=*/*.parquet
    using Polars (one targeted read per market, ~8 files). This avoids a
    global DuckDB corpus scan which is slow on Windows due to sequential
    partition-predicate evaluation of the WHERE IN clause. DuckDB is kept
    for compute_distributions (cross-partition aggregation) where it excels.

    Writes clean bars (if not dry_run), then atomically writes both ledger
    parquets. run_id and completed_at are consistent across both ledgers.

    Note: bars_clean/ is NOT purged before writing. Rejected markets from
    a previous run that later become non-rejected (or vice versa) may leave
    stale partition files. This is acceptable for full-recompute semantics;
    add --clean flag if purge-before-write is needed in future.
    """
    run_id = uuid.uuid4().hex
    completed_at = datetime.now(timezone.utc).isoformat()

    ok_ids = load_ok_market_ids(config)
    if offset:
        ok_ids = ok_ids[offset:]
    if limit:
        ok_ids = ok_ids[:limit]

    lenient_ids = load_lenient_market_ids(config)
    ok_id_set = set(ok_ids)

    logger.info(
        "run_qa start",
        extra={
            "extra_fields": {
                "run_id": run_id,
                "markets": len(ok_ids),
                "dry_run": dry_run,
            }
        },
    )

    t0 = datetime.now(timezone.utc).timestamp()
    market_rows: list[_MarketRow] = []
    window_rows: list[_WindowRow] = []
    counts = {"clean": 0, "usable": 0, "rejected": 0}

    for i, mid in enumerate(sorted(ok_id_set), start=1):
        if i % 100 == 1:
            logger.info(
                "market_progress",
                extra={
                    "extra_fields": {
                        "i": i,
                        "market_id": mid,
                        "elapsed_s": round(datetime.now(timezone.utc).timestamp() - t0, 2),
                    }
                },
            )

        market_dir = config.bars_root / f"market_id={mid}"
        parquet_files = sorted(market_dir.glob("utc_date=*/*.parquet"))

        if not parquet_files:
            market_rows.append(
                _MarketRow(
                    market_id=mid,
                    qa_status="rejected",
                    qa_reason=_REASON_NO_BAR_FILES,
                    clean_bar_count=0,
                    dropped_bar_count=0,
                    window_count=0,
                    usable_window_count=0,
                    total_usable_bar_count=0,
                    lenient_upper_bound=mid in lenient_ids,
                    run_id=run_id,
                    completed_at=completed_at,
                )
            )
            counts["rejected"] += 1
            logger.info(
                "market_qa",
                extra={
                    "extra_fields": {
                        "market_id": mid,
                        "qa_status": "rejected",
                        "qa_reason": _REASON_NO_BAR_FILES,
                        "run_id": run_id,
                    }
                },
            )
            continue

        bars_df = pl.read_parquet(parquet_files)
        result = qa_one_market(mid, bars_df, config)
        lenient = mid in lenient_ids

        market_rows.append(
            _MarketRow(
                market_id=mid,
                qa_status=result.qa_status,
                qa_reason=result.qa_reason,
                clean_bar_count=result.clean_bar_count,
                dropped_bar_count=result.dropped_bar_count,
                window_count=result.window_count,
                usable_window_count=result.usable_window_count,
                total_usable_bar_count=result.total_usable_bar_count,
                lenient_upper_bound=lenient,
                run_id=run_id,
                completed_at=completed_at,
            )
        )
        for wres in result.windows:
            window_rows.append(
                _WindowRow(
                    market_id=mid,
                    window_idx=wres.window_idx,
                    window_start_ts=wres.window_start_ts,
                    window_end_ts=wres.window_end_ts,
                    bar_count=wres.bar_count,
                    median_dt=wres.median_dt,
                    max_dt=wres.max_dt,
                    usable=wres.usable,
                    reason=wres.reason,
                    run_id=run_id,
                    completed_at=completed_at,
                )
            )
        counts[result.qa_status] += 1
        logger.info(
            "market_qa",
            extra={
                "extra_fields": {
                    "market_id": mid,
                    "qa_status": result.qa_status,
                    "qa_reason": result.qa_reason,
                    "clean_bar_count": result.clean_bar_count,
                    "dropped_bar_count": result.dropped_bar_count,
                    "window_count": result.window_count,
                    "usable_window_count": result.usable_window_count,
                    "total_usable_bar_count": result.total_usable_bar_count,
                    "lenient_upper_bound": lenient,
                    "run_id": run_id,
                }
            },
        )
        if not dry_run and result.qa_status != "rejected":
            write_clean_bars(mid, result.clean_df, config)

    if not dry_run:
        _write_qa_market_parquet(market_rows, config)
        _write_qa_windows_parquet(window_rows, config)
        logger.info(
            "ledgers written",
            extra={
                "extra_fields": {
                    "qa_market_path": str(config.qa_market_path),
                    "qa_windows_path": str(config.qa_windows_path),
                    "run_id": run_id,
                }
            },
        )

    summary = RunSummary(
        run_id=run_id,
        total_markets=len(ok_ids),
        clean=counts["clean"],
        usable=counts["usable"],
        rejected=counts["rejected"],
        completed_at=completed_at,
    )
    logger.info("run_qa done", extra={"extra_fields": dict(summary)})
    return summary


# ── CLI ───────────────────────────────────────────────────────────────────────


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Phase 1.6: data quality validation for pmbot bar corpus"
    )
    parser.add_argument(
        "--calibrate",
        action="store_true",
        help="Step 0 only: compute distributions and write _qa_distributions.parquet",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Full plan with no writes; structured JSON decisions logged to stderr",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        metavar="N",
        help="Process only the first N markets after offset (0 = all)",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        metavar="N",
        help="Skip the first N markets in manifest order",
    )
    args = parser.parse_args(argv)

    handler = logging.StreamHandler()
    handler.setFormatter(_JsonFormatter())
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)

    config = QAConfig()

    if args.calibrate:
        logger.info("calibrate step 0 start")
        compute_distributions(config)
        return

    dry_run: bool = args.dry_run
    summary = run_qa(config, dry_run=dry_run, limit=args.limit, offset=args.offset)
    logger.info("complete", extra={"extra_fields": dict(summary)})


if __name__ == "__main__":
    main()
