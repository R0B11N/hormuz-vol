"""Bootstrap Granger causality helper for fat-tailed returns."""

from __future__ import annotations

import numpy as np
from arch.bootstrap import StationaryBootstrap
from statsmodels.tsa.stattools import grangercausalitytests


def bootstrap_granger_pval(
    y: np.ndarray,
    x: np.ndarray,
    lag: int,
    n_boot: int = 1000,
    block_length: int = 12,
) -> float:
    y = np.asarray(y, dtype=float)
    x = np.asarray(x, dtype=float)
    valid = np.isfinite(y) & np.isfinite(x)
    y = y[valid]
    x = x[valid]
    if len(y) <= (lag + 2):
        return float("nan")

    obs = grangercausalitytests(np.column_stack([y, x]), maxlag=lag, verbose=False)[lag][0]["ssr_ftest"][0]

    stats = []
    bs = StationaryBootstrap(block_length, y, x)
    for sample in bs.bootstrap(n_boot):
        y_b, x_b = sample[0][0], sample[0][1]
        gc = grangercausalitytests(np.column_stack([y_b, x_b]), maxlag=lag, verbose=False)
        stats.append(gc[lag][0]["ssr_ftest"][0])
    stats = np.asarray(stats, dtype=float)
    return float(np.mean(stats >= obs))
