"""Execute event-study figures and robustness artifacts."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import pandas_market_calendars as mcal

from .events_table import load_master
from .period_splits import summarize_by_period
from .placebo import generate_placebo_events
from .plots import plot_car, save_figure
from .windows import extract_event_windows


def label_options_open(index: pd.DatetimeIndex) -> pd.Series:
    nyse = mcal.get_calendar("NYSE")
    sched = nyse.schedule(start_date=index.min().date(), end_date=index.max().date())
    flag = pd.Series(False, index=index)
    for _, row in sched.iterrows():
        mask = (index >= row["market_open"]) & (index <= row["market_close"])
        flag.loc[mask] = True
    return flag


def _load_poly_series(project_root: Path) -> pd.Series:
    pieces = []
    root = project_root / "data" / "raw" / "polymarket"
    for file in sorted(root.glob("*.parquet")):
        df = pd.read_parquet(file)
        if df.empty:
            continue
        if not isinstance(df.index, pd.DatetimeIndex):
            if "ts" in df.columns:
                df["ts"] = pd.to_datetime(df["ts"], utc=True)
                df = df.set_index("ts")
            else:
                continue
        pieces.append(df["poly_prob"].dropna())
    if not pieces:
        return pd.Series(dtype=float)
    return pd.concat(pieces).sort_index()


def run(project_root: Path) -> None:
    processed = project_root / "data" / "processed"
    panel_path = processed / "analysis_panel.parquet"
    panel = pd.read_parquet(panel_path) if panel_path.exists() else pd.DataFrame()
    events = load_master(project_root / "data" / "events" / "events_master.parquet")
    events = events[events["verified"] == True].copy()  # noqa: E712

    poly = _load_poly_series(project_root)
    if poly.empty or events.empty or panel.empty:
        print("Insufficient data for event study; skipping execution.")
        return

    panel_idx = panel.copy()
    panel_idx["timestamp_utc"] = pd.to_datetime(panel_idx["date"], utc=True) + pd.Timedelta(hours=20)
    panel_idx = panel_idx.set_index("timestamp_utc").sort_index()
    options_proxy = panel_idx["vol"].dropna() if "vol" in panel_idx.columns else pd.Series(dtype=float)

    poly_w = extract_event_windows(poly, events)
    opt_w = extract_event_windows(options_proxy, events)
    fig = plot_car(poly_w, opt_w, title="Pooled CAR: Polymarket vs Options")
    save_figure(fig, project_root / "paper" / "figures" / "car_pooled.pdf")
    poly_w.to_parquet(processed / "event_windows_poly.parquet", index=False)
    opt_w.to_parquet(processed / "event_windows_options.parquet", index=False)

    # Event-type heterogeneity (Task 25)
    for etype in ["MILITARY", "DIPLOMATIC", "ECONOMIC", "POLITICAL"]:
        subset = events[events["event_type"] == etype]
        if len(subset) < 3:
            continue
        pw = extract_event_windows(poly, subset)
        ow = extract_event_windows(options_proxy, subset)
        fig = plot_car(pw, ow, title=f"CAR by event type: {etype}")
        save_figure(fig, project_root / "paper" / "figures" / f"car_{etype.lower()}.pdf")

    # After-hours split (Task 26)
    events["options_open"] = label_options_open(pd.to_datetime(events["timestamp_utc"], utc=True).dt.tz_convert("UTC"))
    for lbl, sub in [("after_hours", events[~events["options_open"]]), ("market_hours", events[events["options_open"]])]:
        if len(sub) < 3:
            continue
        pw = extract_event_windows(poly, sub)
        ow = extract_event_windows(options_proxy, sub)
        fig = plot_car(pw, ow, title=f"CAR {lbl.replace('_', ' ')}")
        save_figure(fig, project_root / "paper" / "figures" / f"car_{lbl}.pdf")

    # High/low OVX regime split (Task 27)
    if "ovx" in panel_idx.columns:
        ovx_median = float(panel_idx["ovx"].median())
        events["vol_regime"] = events["timestamp_utc"].apply(
            lambda t: "HIGH" if panel_idx["ovx"].asof(pd.Timestamp(t, tz="UTC")) > ovx_median else "LOW"
        )
        for regime in ["HIGH", "LOW"]:
            sub = events[events["vol_regime"] == regime]
            if len(sub) < 3:
                continue
            pw = extract_event_windows(poly, sub)
            ow = extract_event_windows(options_proxy, sub)
            fig = plot_car(pw, ow, title=f"CAR vol regime: {regime}")
            save_figure(fig, project_root / "paper" / "figures" / f"car_regime_{regime.lower()}.pdf")

    # Placebo test artifact (Task 28)
    placebo = generate_placebo_events("2026-02-27", "2026-04-15", freq="1h", n_events=50)
    placebo_w = extract_event_windows(poly, placebo)
    placebo_w.to_parquet(processed / "placebo_windows.parquet", index=False)

    # Period split summary (Task 30)
    summary = summarize_by_period(panel)
    summary.to_parquet(processed / "period_split_summary.parquet", index=False)


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run event-study outputs.")
    parser.add_argument("--project-root", type=Path, default=Path(__file__).resolve().parents[2])
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    run(args.project_root)
    print("Event-study run complete.")


if __name__ == "__main__":
    main()
