"""Daily integrity checks for ingestion outputs."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd


@dataclass
class IntegrityThresholds:
    min_poly_ticks: int = 100
    min_option_strikes: int = 10


def daily_integrity_check(project_root: Path, date: str, thresholds: IntegrityThresholds | None = None) -> tuple[bool, list[str]]:
    t = thresholds or IntegrityThresholds()
    issues: list[str] = []

    poly_path = project_root / "data" / "raw" / "polymarket" / f"{date}.parquet"
    if not poly_path.exists():
        issues.append("MISSING polymarket data")
    else:
        poly = pd.read_parquet(poly_path)
        n_ticks = len(poly)
        if n_ticks < t.min_poly_ticks:
            issues.append(f"SPARSE polymarket: only {n_ticks} ticks")

    for symbol in ["CL", "USO"]:
        opt_path = project_root / "data" / "raw" / "options" / symbol / f"{date}.parquet"
        if not opt_path.exists():
            issues.append(f"MISSING options chain: {symbol}")
            continue
        opts = pd.read_parquet(opt_path)
        if len(opts) < t.min_option_strikes:
            issues.append(f"SPARSE chain {symbol}: only {len(opts)} strikes")

    return (len(issues) == 0), issues


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run integrity checks for one day.")
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--project-root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--min-poly-ticks", type=int, default=100)
    parser.add_argument("--min-option-strikes", type=int, default=10)
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    ok, issues = daily_integrity_check(
        args.project_root,
        args.date,
        IntegrityThresholds(
            min_poly_ticks=args.min_poly_ticks,
            min_option_strikes=args.min_option_strikes,
        ),
    )
    if ok:
        print(f"Integrity OK for {args.date}")
    else:
        print(f"DATA ISSUES {args.date}: {issues}")


if __name__ == "__main__":
    main()
