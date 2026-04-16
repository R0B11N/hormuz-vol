"""Run RND extraction over all available daily option snapshots."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from .bl import breeden_litzenberger
from .cleaning import clean_chain, compute_forward_and_features
from .diagnostics import append_failure_log
from .moments import rnd_moments
from .svi import fit_svi, interpolated_iv_grid


def _iter_chain_files(options_root: Path) -> list[tuple[str, Path]]:
    files = []
    for underlying_dir in sorted(options_root.glob("*")):
        if not underlying_dir.is_dir():
            continue
        for file in sorted(underlying_dir.glob("*.parquet")):
            files.append((underlying_dir.name.upper(), file))
    return files


def _load_spot(spot_root: Path, trade_date: str, underlying: str) -> float:
    spot_candidates = sorted(spot_root.glob(f"{trade_date}.parquet"))
    if not spot_candidates:
        raise FileNotFoundError(f"Missing spot snapshot for {trade_date}")
    frame = pd.read_parquet(spot_candidates[0])
    col = "cl_spot" if underlying == "CL" else "uso"
    if col not in frame.columns:
        raise KeyError(f"Spot column {col} missing in {spot_candidates[0]}")
    val = float(frame[col].dropna().iloc[-1])
    return val


def run(project_root: Path, rate: float = 0.05) -> pd.DataFrame:
    options_root = project_root / "data" / "raw" / "options"
    spot_root = project_root / "data" / "raw" / "spot_series"
    processed = project_root / "data" / "processed"
    diagnostics_path = processed / "rnd_failures.parquet"
    processed.mkdir(parents=True, exist_ok=True)

    results = []
    for underlying, chain_file in _iter_chain_files(options_root):
        trade_date = chain_file.stem
        try:
            chain = pd.read_parquet(chain_file)
            spot = _load_spot(spot_root, trade_date, underlying)
            trade_ts = pd.Timestamp(trade_date, tz="UTC")
            prepared = compute_forward_and_features(chain=chain, spot=spot, rate=rate, trade_date=trade_ts)
            prepared = clean_chain(prepared)
            if prepared.empty or len(prepared) < 6:
                raise ValueError("Insufficient cleaned chain points for SVI.")

            # Use nearest expiry bucket (median T across filtered chain).
            median_t = float(prepared["T"].median())
            sub = prepared[np.abs(prepared["T"] - median_t) < 0.05].copy()
            if len(sub) < 6:
                sub = prepared

            fit = fit_svi(sub["log_moneyness"].to_numpy(), sub["total_var"].to_numpy())
            grid = interpolated_iv_grid(fit.as_dict(), F=float(sub["F"].median()), T=median_t, n_strikes=200)
            K, rnd, _cdf, mass = breeden_litzenberger(
                strikes=grid["strike"].to_numpy(),
                iv_interp=grid["iv"].to_numpy(),
                F=float(sub["F"].median()),
                T=median_t,
                r=rate,
            )
            moments = rnd_moments(K, rnd, F=float(sub["F"].median()))
            moments.update(
                {
                    "date": trade_date,
                    "underlying": underlying,
                    "rnd_mass_coverage": mass,
                    "svi_rmse": fit.rmse,
                    "svi_success": fit.success,
                }
            )
            results.append(moments)
        except Exception as exc:  # noqa: BLE001
            append_failure_log(diagnostics_path, trade_date=trade_date, underlying=underlying, reason=str(exc))

    panel = pd.DataFrame(results)
    if not panel.empty:
        panel["date"] = pd.to_datetime(panel["date"])
        panel = panel.sort_values(["underlying", "date"]).reset_index(drop=True)
    output = processed / "rnd_panel.parquet"
    panel.to_parquet(output, index=False)
    return panel


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build daily RND panel from stored options snapshots.")
    parser.add_argument("--project-root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--rate", type=float, default=0.05)
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    panel = run(args.project_root, rate=args.rate)
    print(f"RND panel rows: {len(panel)}")
    print(f"Saved to: {args.project_root / 'data' / 'processed' / 'rnd_panel.parquet'}")


if __name__ == "__main__":
    main()
