"""Bidirectional Granger causality utilities."""

from __future__ import annotations

import pandas as pd
from statsmodels.tsa.stattools import grangercausalitytests


def run_granger_both_ways(poly: pd.Series, iv: pd.Series, max_lag: int = 15) -> dict:
    frame = pd.DataFrame({"d_poly": poly.diff(), "d_iv": iv.diff()}).dropna()
    if frame.empty:
        return {"poly_to_iv": {}, "iv_to_poly": {}}
    return {
        "poly_to_iv": grangercausalitytests(frame[["d_iv", "d_poly"]], maxlag=max_lag, verbose=False),
        "iv_to_poly": grangercausalitytests(frame[["d_poly", "d_iv"]], maxlag=max_lag, verbose=False),
    }
