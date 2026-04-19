#!/usr/bin/env python3
"""Build the qualified BTC binary market whitelist from the Polymarket Gamma API."""

from __future__ import annotations

import logging

from pmbot.phase0_filter import FilterConfig
from pmbot.phase1_data.whitelist import WhitelistConfig, build_whitelist

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

result = build_whitelist(WhitelistConfig(), FilterConfig())
print(f"Qualified: {result.qualified_count}/{result.total_markets_seen}")
top = sorted(result.rejection_reasons.items(), key=lambda kv: -kv[1])[:5]
print(f"Top rejection reasons: {top}")
print(f"Output: {result.output_path}")
print(f"Duration: {(result.run_completed_at - result.run_started_at).total_seconds():.1f}s")
