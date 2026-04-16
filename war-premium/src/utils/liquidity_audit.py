"""Polymarket liquidity diagnostics."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt
import pandas as pd


def audit_polymarket_liquidity(
    poly: pd.DataFrame,
    min_ticks_per_day: int = 50,
    min_volume_usd: float = 50_000.0,
) -> pd.DataFrame:
    """Create daily liquidity flags from Polymarket tick history."""
    if poly.empty:
        return pd.DataFrame(
            columns=["tick_count", "sum_volume", "unreliable_by_ticks", "unreliable_by_volume", "unreliable"]
        )
    daily = pd.DataFrame(index=poly.resample("1D").size().index)
    daily["tick_count"] = poly.resample("1D").size()
    if "volume" in poly.columns:
        daily["sum_volume"] = poly["volume"].resample("1D").sum(min_count=1)
    else:
        daily["sum_volume"] = pd.NA
    daily["unreliable_by_ticks"] = daily["tick_count"] < min_ticks_per_day
    daily["unreliable_by_volume"] = daily["sum_volume"].fillna(0.0) < min_volume_usd
    daily["unreliable"] = daily["unreliable_by_ticks"] | daily["unreliable_by_volume"]
    return daily


def plot_daily_liquidity(daily: pd.DataFrame, output: Path) -> None:
    fig, ax1 = plt.subplots(figsize=(10, 4))
    ax1.plot(daily.index, daily["tick_count"], color="#1D9E75", label="daily ticks")
    ax1.set_ylabel("tick count")
    ax2 = ax1.twinx()
    if "sum_volume" in daily.columns:
        ax2.plot(daily.index, daily["sum_volume"], color="#534AB7", alpha=0.7, label="daily volume")
        ax2.set_ylabel("volume")
    ax1.set_title("Polymarket liquidity audit")
    ax1.set_xlabel("date")
    fig.tight_layout()
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output)
    plt.close(fig)


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit Polymarket daily liquidity.")
    parser.add_argument("--input", type=Path, required=True, help="Polymarket parquet file")
    parser.add_argument("--output", type=Path, required=True, help="Output parquet path for flags")
    parser.add_argument("--plot", type=Path, help="Optional output PNG path")
    parser.add_argument("--min-ticks", type=int, default=50)
    parser.add_argument("--min-volume", type=float, default=50_000.0)
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    poly = pd.read_parquet(args.input)
    if not isinstance(poly.index, pd.DatetimeIndex):
        if "ts" in poly.columns:
            poly["ts"] = pd.to_datetime(poly["ts"], utc=True)
            poly = poly.set_index("ts")
        else:
            raise ValueError("Polymarket frame requires DatetimeIndex or a 'ts' column.")

    daily = audit_polymarket_liquidity(
        poly,
        min_ticks_per_day=args.min_ticks,
        min_volume_usd=args.min_volume,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    daily.to_parquet(args.output)
    if args.plot:
        plot_daily_liquidity(daily, args.plot)
    print(f"Saved daily liquidity audit: {args.output}")


if __name__ == "__main__":
    main()
