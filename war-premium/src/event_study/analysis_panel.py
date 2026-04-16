"""Build merged analysis panel for event study and comparative tests."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import pandas as pd

from .events_table import load_master
from .poly_bridge import apply_bridge, calibrate_conditionals


def _load_daily_polymarket(raw_poly_root: Path) -> pd.DataFrame:
    rows = []
    for file in sorted(raw_poly_root.glob("*.parquet")):
        df = pd.read_parquet(file)
        if df.empty:
            continue
        if not isinstance(df.index, pd.DatetimeIndex):
            if "ts" in df.columns:
                df["ts"] = pd.to_datetime(df["ts"], utc=True)
                df = df.set_index("ts")
            else:
                continue
        day = pd.Timestamp(file.stem)
        rows.append(
            {
                "date": day,
                "poly_prob": float(df["poly_prob"].dropna().iloc[-1]) if "poly_prob" in df.columns and df["poly_prob"].notna().any() else float("nan"),
            }
        )
    if not rows:
        return pd.DataFrame(columns=["date", "poly_prob"])
    return pd.DataFrame(rows)


def _ceasefire_regime(date: pd.Timestamp) -> str:
    if date < pd.Timestamp("2026-04-01"):
        return "active_conflict"
    return "ceasefire"


def build_analysis_panel(project_root: Path) -> pd.DataFrame:
    processed_root = project_root / "data" / "processed"
    rnd_path = processed_root / "rnd_panel.parquet"
    spot_root = project_root / "data" / "raw" / "spot_series"
    raw_poly_root = project_root / "data" / "raw" / "polymarket"

    rnd = pd.read_parquet(rnd_path) if rnd_path.exists() else pd.DataFrame()
    if "date" not in rnd.columns:
        rnd["date"] = pd.Series(dtype="datetime64[ns]")
    if "underlying" not in rnd.columns:
        rnd["underlying"] = pd.Series(dtype="string")
    poly_daily = _load_daily_polymarket(raw_poly_root) if raw_poly_root.exists() else pd.DataFrame(columns=["date", "poly_prob"])

    spot_rows = []
    for file in sorted(spot_root.glob("*.parquet")):
        df = pd.read_parquet(file)
        if df.empty:
            continue
        spot_rows.append(
            {
                "date": pd.Timestamp(file.stem),
                "cl_spot": float(df["cl_spot"].dropna().iloc[-1]) if "cl_spot" in df.columns and df["cl_spot"].notna().any() else float("nan"),
                "uso": float(df["uso"].dropna().iloc[-1]) if "uso" in df.columns and df["uso"].notna().any() else float("nan"),
                "ovx": float(df["ovx"].dropna().iloc[-1]) if "ovx" in df.columns and df["ovx"].notna().any() else float("nan"),
            }
        )
    spot = pd.DataFrame(spot_rows)
    if "date" not in spot.columns:
        spot["date"] = pd.Series(dtype="datetime64[ns]")

    rnd["date"] = pd.to_datetime(rnd["date"], errors="coerce")
    poly_daily["date"] = pd.to_datetime(poly_daily["date"], errors="coerce")
    spot["date"] = pd.to_datetime(spot["date"], errors="coerce")
    panel = rnd.merge(poly_daily, on="date", how="outer").merge(spot, on="date", how="outer")
    if panel.empty:
        return panel

    panel["date"] = pd.to_datetime(panel["date"])
    panel["ceasefire_regime"] = panel["date"].map(_ceasefire_regime)
    cond = calibrate_conditionals(panel)
    panel["poly_implied_spike"] = apply_bridge(panel, p_spike_given_cf=cond["p_spike_given_cf"], p_spike_given_no_cf=cond["p_spike_given_no_cf"])

    events = load_master(project_root / "data" / "events" / "events_master.parquet")
    if not events.empty:
        events["event_date"] = pd.to_datetime(events["timestamp_utc"]).dt.tz_convert("UTC").dt.floor("D").dt.tz_localize(None)
        events_by_day = events.groupby("event_date")["event_id"].count().rename("event_count").reset_index()
        events_by_day = events_by_day.rename(columns={"event_date": "date"})
        panel = panel.merge(events_by_day, on="date", how="left")
    if "event_count" in panel.columns:
        panel["event_count"] = panel["event_count"].fillna(0).astype(int)
    else:
        panel["event_count"] = 0
    return panel.sort_values(["date", "underlying"]).reset_index(drop=True)


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build merged analysis panel.")
    parser.add_argument("--project-root", type=Path, default=Path(__file__).resolve().parents[2])
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    panel = build_analysis_panel(args.project_root)
    out = args.project_root / "data" / "processed" / "analysis_panel.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    panel.to_parquet(out, index=False)
    print(f"Saved analysis panel: {out} ({len(panel)} rows)")


if __name__ == "__main__":
    main()
