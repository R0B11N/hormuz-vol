"""Event window extraction utilities."""

from __future__ import annotations

import numpy as np
import pandas as pd


def extract_event_windows(
    series: pd.Series,
    events: pd.DataFrame,
    windows: list[int] | None = None,
) -> pd.DataFrame:
    if windows is None:
        windows = [-60, -30, -15, -5, 0, 5, 15, 30, 60]
    series = series.sort_index()
    rows = []
    for _, ev in events.iterrows():
        t0 = pd.to_datetime(ev["timestamp_utc"], utc=True)
        baseline = series.asof(t0)
        for w in windows:
            t_w = t0 + pd.Timedelta(minutes=int(w))
            value = series.asof(t_w)
            delta = value - baseline if pd.notna(value) and pd.notna(baseline) else np.nan
            rows.append(
                {
                    "event_id": ev.get("event_id"),
                    "event_type": ev.get("event_type"),
                    "window_min": w,
                    "timestamp_utc": t0,
                    "value": value,
                    "delta": delta,
                    "pct_delta": (delta / abs(baseline)) if pd.notna(delta) and baseline not in [0, None] else np.nan,
                }
            )
    return pd.DataFrame(rows)
