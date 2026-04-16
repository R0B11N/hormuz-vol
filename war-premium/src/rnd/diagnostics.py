"""Diagnostics and failure logs for SVI/RND extraction."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


def append_failure_log(log_path: Path, date: str, underlying: str, reason: str) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    row = pd.DataFrame(
        [{"date": date, "underlying": underlying, "reason": reason, "logged_at_utc": pd.Timestamp.utcnow()}]
    )
    if log_path.exists():
        existing = pd.read_parquet(log_path)
        row = pd.concat([existing, row], ignore_index=True)
    row.to_parquet(log_path, index=False)
