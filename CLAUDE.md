# CLAUDE.md — Operating Manual for Claude Code

## Purpose
This file tells Claude Code how to work in this repo. The authoritative design
document is `SPEC.md` — everything built here must trace back to a phase and
step in that spec. When in doubt, read the spec; do not invent.

## Authoritative References
- `SPEC.md` — architecture, phases, criteria. Source of truth for all design
  decisions. Treat as read-only unless I explicitly ask for a spec change.
- `PROGRESS.md` — running log of completed phases/steps. Append only; never
  delete or rewrite entries.
- `CLAUDE.md` (this file) — conventions and workflow.

If `SPEC.md` and anything else disagree, `SPEC.md` wins.

## Stack (pinned)
- **Python 3.12** — no newer, no older
- **Polars** — primary DataFrame library; lazy evaluation by default for ingestion
- **DuckDB** — cross-partition queries over Parquet
- **PostgreSQL** — market metadata, resolutions, OMS state, PnL. Never tick data.
- **XGBoost** — oracle model (Phase 2)
- **web3.py** — Polygon RPC interaction (Phase 6)
- **pytest** — test runner
- **ruff** — lint + format (single tool; do not add black/isort/flake8)
- **mypy** — static type checking, strict on the public surface of `src/pmbot`

Keep deps pinned in `pyproject.toml`. Do not add a new top-level dep without
asking me first.

## Repo Conventions
- **One module per phase.** Each phase in the spec maps to one top-level module
  (or subpackage) under `src/pmbot/`. Phase 1 is a subpackage because it has
  six substeps; the others are single files unless they outgrow that.
- **Dataclasses for configs.** Every tunable threshold lives in a *frozen*
  dataclass (e.g. `FilterConfig`, `ExecutionConfig`, `OracleConfig`). No magic
  numbers in function bodies. Defaults live on the dataclass; runtime overrides
  load from `configs/*.toml`.
- **Type hints on every public function.** Private helpers (leading underscore)
  may skip. Anything importable from another module is fully annotated.
  `mypy --strict` passes on `src/pmbot`.
- **Absolute imports only**, rooted at `pmbot`. No relative imports across
  modules.
- **No global state.** Pass configs in, return results out. This is what makes
  shadow mode (Phase 5) a drop-in wrapper instead of a rewrite.
- **Structured logging, not `print`.** Stdlib `logging` with a JSON formatter
  everywhere the spec calls for audit trails (executor gate decisions, OMS
  cancellations, circuit breaker signals, shadow-mode intended actions).
- **Paths come from `pmbot.config`**, never hardcoded string literals.
- **Pure functions where possible.** I/O at the edges, logic in the middle.

## Definition of "Done" for a Phase
A phase is done when *all* of the following hold:
1. **Tests pass.** `pytest` green for that phase's test file(s). New code has
   tests; untested code is not done.
2. **Type-check clean.** `mypy --strict src/pmbot/<phase>` returns zero errors.
3. **Lint clean.** `ruff check src/pmbot/<phase> tests/<phase>` returns zero
   errors.
4. **PROGRESS.md updated.** Append a dated entry naming the phase, the SPEC
   steps covered, the key decisions taken (especially boundary/convention
   choices), and any deferred work.
5. **Committed.** One logical commit per phase or per substep. Commit-message
   format: `phase N: <short summary>` or `phase N.M: <short summary>`.

Do not declare a phase done if any of the above fails. Do not skip PROGRESS.md.

## Workflow with Claude Code
- **Start every phase in Plan Mode** (`Shift+Tab`). Produce a plan, wait for my
  approval, then write code.
- **Stay scoped.** If working on Phase N, do not touch files from Phase N+1. If
  the spec genuinely requires a cross-phase change, stop and flag it.
- **Read before write.** Before editing any existing file, open it. Do not
  regenerate from memory.
- **Ask, don't guess.** If `SPEC.md` is ambiguous for a step, stop and ask.
  A paused conversation beats silent drift from the spec.
- **Small diffs.** Prefer several focused commits over one sprawling one.
- **Every gate decision logs its reason.** The SPEC mandates reason strings for
  executor gates and circuit breaker signals — honor this from the first phase.

## Secrets and Environment
- Real secrets (Polymarket keys, Polygon private key, Deribit keys, LLM API
  key, news API key) live in `.env`, which is gitignored.
- `.env.example` lists every required key with an empty value. Keep it in sync
  when a new secret is introduced.
- Tests never read `.env`. They use fixtures or mocks.
- Never log a secret value. Not once, not in debug mode.

## What NOT to Do
- Do not add a phase or step that isn't in `SPEC.md`.
- Do not "upgrade" Phase 2 to a neural model before XGBoost plateaus (explicit
  spec rule — SPEC.md Step 2.2).
- Do not put tick data in PostgreSQL (SPEC.md Step 1.1).
- Do not use linear interpolation in feature fusion — forward-fill only
  (SPEC.md Step 1.3, leakage risk).
- Do not skip or weaken the leakage test in Step 1.4.
- Do not use public Polygon RPC endpoints in Phase 6 (SPEC.md Step 6.1).
- Do not hardcode thresholds; they go in dataclass configs.
- Do not commit `.env`, `data/`, model artifacts, or logs.
- Do not delete or rewrite entries in `PROGRESS.md`.
- Do not declare a phase done without running the full "done" checklist above.

## Current Phase
See the most recent dated entry in `PROGRESS.md`. On a fresh clone, start at
Phase 0.
