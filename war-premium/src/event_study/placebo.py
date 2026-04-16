"""Placebo event generator and validation helpers."""

from __future__ import annotations

import numpy as np
import pandas as pd


def generate_placebo_events(
    start: str,
    end: str,
    freq: str = "1h",
    n_events: int = 50,
    seed: int = 42,
) -> pd.DataFrame:
    np.random.seed(seed)
    grid = pd.date_range(start, end, freq=freq, tz="UTC")
    if len(grid) < n_events:
        raise ValueError("Requested placebo sample is larger than available grid.")
    picks = np.random.choice(grid, size=n_events, replace=False)
    frame = pd.DataFrame(
        {
            "timestamp_utc": pd.to_datetime(sorted(picks), utc=True),
            "event_type": "PLACEBO",
            "event_id": [f"placebo_{i:03d}" for i in range(n_events)],
        }
    )
    return frame
