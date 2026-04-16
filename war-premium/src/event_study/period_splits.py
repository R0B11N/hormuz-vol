"""Run period-split robustness summaries."""

from __future__ import annotations

import pandas as pd

PERIODS = {
    "pre_war": ("2026-02-20", "2026-02-26"),
    "war_hot": ("2026-02-27", "2026-03-31"),
    "ceasefire": ("2026-04-01", "2026-04-14"),
    "post": ("2026-04-15", None),
}


def period_label(ts: pd.Timestamp) -> str:
    t = pd.Timestamp(ts)
    for name, (start, end) in PERIODS.items():
        start_ts = pd.Timestamp(start)
        end_ts = pd.Timestamp(end) if end else pd.Timestamp.max
        if start_ts <= t <= end_ts:
            return name
    return "outside_sample"


def summarize_by_period(panel: pd.DataFrame) -> pd.DataFrame:
    out = panel.copy()
    out["date"] = pd.to_datetime(out["date"])
    out["period"] = out["date"].map(period_label)
    metrics = [c for c in ["poly_prob", "poly_implied_spike", "p_spike_130", "vol", "ovx"] if c in out.columns]
    if not metrics:
        return pd.DataFrame(columns=["period"])
    summary = out.groupby(["period", "underlying"], dropna=False)[metrics].mean().reset_index()
    return summary.sort_values(["period", "underlying"])
