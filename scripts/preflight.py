#!/usr/bin/env python3
"""Preflight verification for the pmbot repo.

Usage: python scripts/preflight.py [--quick] [--with-integration] [--verbose]

Exit 0 if all required checks pass; non-zero otherwise.
"""

from __future__ import annotations

import argparse
import csv
import importlib.metadata
import os
import subprocess
import sys
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

# ---------------------------------------------------------------------------
# DATA FILE MANIFEST
# Add entries here as new phases land.  Each DataFileSpec describes one file
# that must be present (or is optional) for the repo to be work-ready.
#
# Fields:
#   path            – relative to repo root
#   min_rows        – row count must be >= this (0 = no minimum)
#   required_columns – every column in this list must exist in the file
#   optional        – if True, absence is a warning rather than a failure
# ---------------------------------------------------------------------------

@dataclass
class DataFileSpec:
    path: str
    min_rows: int
    required_columns: list[str]
    optional: bool = False


DATA_FILES: list[DataFileSpec] = [
    # Phase 0 / 1 — resolution CSV
    DataFileSpec(
        path="data/resolutions/resolved_markets.csv",
        min_rows=10_000,
        required_columns=["market_id", "end_date", "resolved_at", "flags"],
        optional=False,
    ),
    # Phase 1.1 — market lookup (bar ingestion anchor)
    DataFileSpec(
        path="data/bars/_market_lookup.parquet",
        min_rows=5_000,
        required_columns=["market_id", "yes_token_id", "created_at"],
        optional=False,
    ),
    # Phase 1.1 — bar manifest (absent until first bar run completes)
    DataFileSpec(
        path="data/bars/_manifest.parquet",
        min_rows=0,
        required_columns=[
            "market_id", "status", "bar_count", "first_ts", "last_ts",
            "error_reason", "run_id", "completed_at", "attempt_count",
        ],
        optional=True,
    ),
    # -----------------------------------------------------------------------
    # Add Phase 1.2, 1.3, ... entries below as they land:
    # DataFileSpec(
    #     path="data/iv/_iv_surface.parquet",
    #     min_rows=1_000,
    #     required_columns=["market_id", "ts", "iv"],
    # ),
    # -----------------------------------------------------------------------
]

# ---------------------------------------------------------------------------
# ANSI colour helpers — no external deps, graceful fallback
# ---------------------------------------------------------------------------

def _enable_ansi_windows() -> bool:
    if os.name != "nt":
        return True
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
        ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x0004
        handle = kernel32.GetStdHandle(-11)  # STD_OUTPUT_HANDLE
        mode = ctypes.c_ulong()
        if kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            kernel32.SetConsoleMode(handle, mode.value | ENABLE_VIRTUAL_TERMINAL_PROCESSING)
        return True
    except Exception:
        return False


_USE_COLOR: bool = sys.stdout.isatty() and _enable_ansi_windows()

# Detect whether the terminal encoding can render Unicode box characters
_encoding = getattr(sys.stdout, "encoding", None) or "ascii"
try:
    "✓✗⚠".encode(_encoding)
    _UNICODE_OK = True
except (UnicodeEncodeError, LookupError):
    _UNICODE_OK = False


def _c(code: str, s: str) -> str:
    return f"\033[{code}m{s}\033[0m" if _USE_COLOR else s


def _green(s: str) -> str:
    return _c("32", s)


def _red(s: str) -> str:
    return _c("31", s)


def _yellow(s: str) -> str:
    return _c("33", s)


if _UNICODE_OK:
    PASS = _green("✓")
    FAIL = _red("✗")
    WARN = _yellow("⚠")
else:
    PASS = _green("[OK]")
    FAIL = _red("[FAIL]")
    WARN = _yellow("[WARN]")

# ---------------------------------------------------------------------------
# Result tracking
# ---------------------------------------------------------------------------

@dataclass
class CheckResult:
    label: str
    passed: bool
    warning: bool = False
    remediation: str = ""


_results: list[CheckResult] = []


def _ok(label: str) -> None:
    print(f"  {PASS} {label}")
    _results.append(CheckResult(label=label, passed=True))


def _fail(label: str, remediation: str = "") -> None:
    print(f"  {FAIL} {label}")
    _results.append(CheckResult(label=label, passed=False, warning=False, remediation=remediation))


def _warn(label: str, remediation: str = "") -> None:
    print(f"  {WARN} {label}")
    _results.append(CheckResult(label=label, passed=True, warning=True, remediation=remediation))


def _section(title: str) -> None:
    print(f"\n[{title}]")


# ---------------------------------------------------------------------------
# Repo root (one level up from scripts/)
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------------------
# 1. Python environment
# ---------------------------------------------------------------------------

def check_python() -> None:
    _section("Python")
    vi = sys.version_info
    label = f"Python {vi.major}.{vi.minor}.{vi.micro}"
    if vi.major == 3 and vi.minor == 12:
        _ok(label)
    else:
        _fail(
            f"{label} - need 3.12.x",
            remediation="Install Python 3.12.x and recreate the venv",
        )

    in_venv = sys.prefix != sys.base_prefix
    if in_venv:
        _ok(f"Running in venv: {sys.prefix}")
    else:
        _warn(
            "Not running inside a venv",
            remediation="Activate: venv\\Scripts\\activate (Windows) or source venv/bin/activate",
        )

# ---------------------------------------------------------------------------
# 2. Pinned dependencies
# ---------------------------------------------------------------------------

def _parse_pinned_deps() -> dict[str, str]:
    """Return {normalised_pkg_name: pinned_version} from pyproject.toml."""
    toml_path = REPO_ROOT / "pyproject.toml"
    with open(toml_path, "rb") as fh:
        data = tomllib.load(fh)

    deps: dict[str, str] = {}

    def _add(spec: str) -> None:
        for sep in ("==", "~=", ">=", "<=", "!=", ">", "<"):
            if sep in spec:
                name, ver = spec.split(sep, 1)
                normalised = name.strip().lower().replace("-", "_")
                deps[normalised] = ver.strip()
                return
        deps[spec.strip().lower().replace("-", "_")] = ""

    for s in data.get("project", {}).get("dependencies", []):
        _add(s)
    for group in data.get("project", {}).get("optional-dependencies", {}).values():
        for s in group:
            _add(s)

    return deps


def check_dependencies() -> None:
    _section("Dependencies")
    try:
        pinned = _parse_pinned_deps()
    except Exception as exc:
        _fail(f"Could not parse pyproject.toml: {exc}", remediation="Check pyproject.toml syntax")
        return

    for pkg in sorted(pinned):
        expected = pinned[pkg]
        dist_name = pkg.replace("_", "-")
        try:
            actual = importlib.metadata.version(dist_name)
        except importlib.metadata.PackageNotFoundError:
            try:
                actual = importlib.metadata.version(pkg)
            except importlib.metadata.PackageNotFoundError:
                _fail(f"{pkg}: not installed", remediation="pip install -e '.[dev]'")
                continue

        if expected:
            if actual == expected:
                _ok(f"{pkg}=={actual}")
            else:
                _fail(
                    f"{pkg}: expected {expected}, found {actual}",
                    remediation="pip install -e '.[dev]'",
                )
        else:
            _ok(f"{pkg}=={actual}")

# ---------------------------------------------------------------------------
# 3. Data files
# ---------------------------------------------------------------------------

def _check_csv(spec: DataFileSpec) -> None:
    path = REPO_ROOT / spec.path
    if not path.exists():
        if spec.optional:
            _warn(f"{spec.path} (absent - expected if no bar runs yet)")
        else:
            _fail(f"{spec.path}: file missing", remediation="Sync from Google Drive or regenerate")
        return

    try:
        with open(path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            headers = list(reader.fieldnames or [])
            row_count = sum(1 for _ in reader)

        missing = [c for c in spec.required_columns if c not in headers]
        if missing:
            _fail(
                f"{spec.path} ({row_count} rows, missing columns: {missing})",
                remediation="Re-download or regenerate the file",
            )
        elif spec.min_rows > 0 and row_count < spec.min_rows:
            _warn(
                f"{spec.path} ({row_count:,} rows - expected >={spec.min_rows:,}, sync may be incomplete)",
                remediation="Wait for Google Drive sync to finish",
            )
        else:
            _ok(f"{spec.path} ({row_count:,} rows, schema OK)")
    except Exception as exc:
        _fail(f"{spec.path}: read error ({exc})", remediation="Check file integrity")


def _check_parquet(spec: DataFileSpec) -> None:
    path = REPO_ROOT / spec.path
    if not path.exists():
        if spec.optional:
            _warn(f"{spec.path} (absent - expected if no bar runs yet)")
        else:
            _fail(f"{spec.path}: file missing", remediation="Sync from Google Drive or regenerate")
        return

    try:
        import polars as pl  # already a pinned dep

        df = pl.read_parquet(path)
        row_count = len(df)
        missing = [c for c in spec.required_columns if c not in df.columns]
        if missing:
            _fail(
                f"{spec.path} ({row_count:,} rows, missing columns: {missing})",
                remediation="Re-download or regenerate the file",
            )
        elif spec.min_rows > 0 and row_count < spec.min_rows:
            _warn(
                f"{spec.path} ({row_count:,} rows - expected >={spec.min_rows:,}, sync may be incomplete)",
                remediation="Wait for Google Drive sync to finish",
            )
        else:
            _ok(f"{spec.path} ({row_count:,} rows, schema OK)")
    except Exception as exc:
        _fail(f"{spec.path}: read error ({exc})", remediation="Check file integrity")


def check_data_files() -> None:
    _section("Data files")
    for spec in DATA_FILES:
        try:
            if spec.path.endswith(".csv"):
                _check_csv(spec)
            elif spec.path.endswith(".parquet"):
                _check_parquet(spec)
            else:
                _warn(f"{spec.path}: unknown extension, skipping")
        except Exception as exc:
            _fail(f"{spec.path}: unexpected error ({exc})", remediation="Check file integrity")

# ---------------------------------------------------------------------------
# 4. Environment variables
# ---------------------------------------------------------------------------

def check_env() -> None:
    _section("Environment")
    example_path = REPO_ROOT / ".env.example"
    if not example_path.exists():
        _fail(
            ".env.example missing - this is a repo bug",
            remediation="Restore .env.example from git history",
        )
        return

    required_keys: list[str] = []
    with open(example_path) as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key = line.split("=", 1)[0].strip()
            if key:
                required_keys.append(key)

    # Load .env file values (without polluting os.environ)
    env_file_values: dict[str, str] = {}
    env_path = REPO_ROOT / ".env"
    if env_path.exists():
        with open(env_path) as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    k, v = line.split("=", 1)
                    env_file_values[k.strip()] = v.strip()

    for key in required_keys:
        value = env_file_values.get(key) or os.environ.get(key, "")
        if value:
            _ok(f"{key} set")
        else:
            _fail(f"{key} missing/empty", remediation=f"Set {key} in .env")

# ---------------------------------------------------------------------------
# 5. Test suite
# ---------------------------------------------------------------------------

def check_tests(*, with_integration: bool, verbose: bool) -> None:
    _section("Tests")
    cmd = [sys.executable, "-m", "pytest", "tests/", "-q", "--no-header", "-x"]
    if with_integration:
        # Clear the default addopts (-m 'not integration') to include all tests
        cmd += ["--override-ini=addopts="]

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, cwd=REPO_ROOT)
        output = (proc.stdout + proc.stderr).strip()

        if proc.returncode == 0:
            summary = next(
                (ln.strip() for ln in reversed(output.splitlines()) if "passed" in ln),
                "Tests passed",
            )
            integration_note = "" if with_integration else " (integration skipped)"
            _ok(f"{summary}{integration_note}")
            if verbose:
                print(output)
        else:
            _fail("Test suite failed", remediation="Run: pytest tests/ -v")
            print(output)
    except Exception as exc:
        _fail(f"Could not run pytest: {exc}", remediation="Ensure pytest is installed: pip install -e '.[dev]'")

# ---------------------------------------------------------------------------
# 6. Lint and type check
# ---------------------------------------------------------------------------

def check_lint_types(verbose: bool) -> None:
    _section("Lint & types")

    for tool, args, label, fix in [
        (
            "ruff",
            ["check", "src/pmbot", "tests"],
            "ruff: no lint errors",
            "ruff check --fix src/pmbot tests",
        ),
        (
            "mypy",
            ["--strict", "src/pmbot"],
            "mypy: no type errors",
            "mypy --strict src/pmbot",
        ),
    ]:
        try:
            proc = subprocess.run(
                [sys.executable, "-m", tool, *args],
                capture_output=True,
                text=True,
                cwd=REPO_ROOT,
            )
            if proc.returncode == 0:
                _ok(label)
                if verbose:
                    print((proc.stdout + proc.stderr).strip())
            else:
                _fail(f"{tool}: errors found", remediation=f"Run: {fix}")
                print((proc.stdout + proc.stderr).strip())
        except Exception as exc:
            _fail(f"Could not run {tool}: {exc}", remediation=f"pip install -e '.[dev]'")

# ---------------------------------------------------------------------------
# 7. Git state
# ---------------------------------------------------------------------------

def check_git() -> None:
    _section("Git")

    try:
        probe = subprocess.run(
            ["git", "rev-parse", "--git-dir"],
            capture_output=True, text=True, cwd=REPO_ROOT,
        )
        if probe.returncode != 0:
            _fail("Not inside a git repo", remediation="Run from within the cloned repo directory")
            return
    except FileNotFoundError:
        _fail("git not found on PATH", remediation="Install git")
        return

    try:
        branch = subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"], text=True, cwd=REPO_ROOT,
        ).strip()
        commit = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], text=True, cwd=REPO_ROOT,
        ).strip()
        _ok(f"branch: {branch}, commit: {commit}")
    except Exception as exc:
        _warn(f"Could not read branch/commit: {exc}")

    try:
        dirty = subprocess.check_output(
            ["git", "status", "--porcelain"], text=True, cwd=REPO_ROOT,
        ).strip()
        if dirty:
            _warn("Working tree has uncommitted changes")
        else:
            _ok("Working tree clean")
    except Exception as exc:
        _warn(f"Could not check working tree: {exc}")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Preflight check - answers 'is this machine ready to work?'",
    )
    parser.add_argument(
        "--with-integration",
        action="store_true",
        help="Also run integration-marked tests (default: skip, they hit the network)",
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Skip tests and lint/mypy - only check deps, data, env, git",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print all subprocess output, not just failures",
    )
    args = parser.parse_args()

    print("Preflight check - pmbot repo")
    print("===========================")

    check_python()
    check_dependencies()
    check_data_files()
    check_env()

    if not args.quick:
        check_tests(with_integration=args.with_integration, verbose=args.verbose)
        check_lint_types(verbose=args.verbose)

    check_git()

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    failed  = [r for r in _results if not r.passed]
    warns   = [r for r in _results if r.passed and r.warning]
    clean   = [r for r in _results if r.passed and not r.warning]
    total   = len(_results)

    parts = [f"{len(clean)}/{total} passed"]
    if failed:
        parts.append(f"{len(failed)} failed")
    if warns:
        parts.append(f"{len(warns)} warning{'s' if len(warns) != 1 else ''}")

    print(f"\nSummary: {', '.join(parts)}")

    if failed:
        print("Failed:")
        for r in failed:
            line = f"  - {r.label}"
            if r.remediation:
                line += f"  ->{r.remediation}"
            print(_red(line) if _USE_COLOR else line)

    if warns and args.verbose:
        print("Warnings:")
        for r in warns:
            line = f"  - {r.label}"
            if r.remediation:
                line += f"  ->{r.remediation}"
            print(_yellow(line) if _USE_COLOR else line)

    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
