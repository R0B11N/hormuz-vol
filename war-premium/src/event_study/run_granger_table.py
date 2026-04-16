"""Generate clean Granger results matrix with bootstrap p-values."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from statsmodels.tsa.stattools import grangercausalitytests

from .bootstrap_granger import bootstrap_granger_pval


def _collect_stats(df: pd.DataFrame, lag: int) -> tuple[float, float]:
    gc = grangercausalitytests(df, maxlag=lag, verbose=False)
    stat = float(gc[lag][0]["ssr_ftest"][0])
    pval = float(gc[lag][0]["ssr_ftest"][1])
    return stat, pval


def build_granger_table(panel: pd.DataFrame, lags: list[int] | None = None) -> pd.DataFrame:
    if lags is None:
        lags = [1, 5, 10, 15]
    if panel.empty or "poly_prob" not in panel.columns or "vol" not in panel.columns:
        return pd.DataFrame()

    ts = panel.sort_values("date").copy()
    ts["d_poly"] = ts["poly_prob"].diff()
    ts["d_iv"] = ts["vol"].diff()
    ts = ts.dropna(subset=["d_poly", "d_iv"])
    if len(ts) < max(lags) + 5:
        return pd.DataFrame()

    rows = []
    for lag in lags:
        df_poly_to_iv = ts[["d_iv", "d_poly"]]
        df_iv_to_poly = ts[["d_poly", "d_iv"]]
        stat1, p1 = _collect_stats(df_poly_to_iv, lag=lag)
        stat2, p2 = _collect_stats(df_iv_to_poly, lag=lag)
        boot1 = bootstrap_granger_pval(ts["d_iv"].to_numpy(), ts["d_poly"].to_numpy(), lag=lag, n_boot=200)
        boot2 = bootstrap_granger_pval(ts["d_poly"].to_numpy(), ts["d_iv"].to_numpy(), lag=lag, n_boot=200)
        rows.append(
            {
                "lag_min": lag,
                "poly_to_iv_fstat": stat1,
                "poly_to_iv_p_asymptotic": p1,
                "poly_to_iv_p_bootstrap": boot1,
                "iv_to_poly_fstat": stat2,
                "iv_to_poly_p_asymptotic": p2,
                "iv_to_poly_p_bootstrap": boot2,
            }
        )
    return pd.DataFrame(rows)


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Granger results matrix table.")
    parser.add_argument("--project-root", type=Path, default=Path(__file__).resolve().parents[2])
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    panel_path = args.project_root / "data" / "processed" / "analysis_panel.parquet"
    panel = pd.read_parquet(panel_path) if panel_path.exists() else pd.DataFrame()
    table = build_granger_table(panel)
    out = args.project_root / "paper" / "tables" / "granger_matrix.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    table.to_parquet(out, index=False)
    print(f"Saved Granger matrix: {out} ({len(table)} rows)")


if __name__ == "__main__":
    main()
