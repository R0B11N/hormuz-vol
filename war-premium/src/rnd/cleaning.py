"""Options chain cleaning and feature engineering for RND extraction."""

from __future__ import annotations

import numpy as np
import pandas as pd


def compute_forward_and_features(
    chain: pd.DataFrame,
    spot: float,
    rate: float,
    trade_date: pd.Timestamp,
) -> pd.DataFrame:
    """Compute forward proxy, maturity, total variance, and log-moneyness."""
    out = chain.copy()
    out["trade_date"] = pd.to_datetime(trade_date, utc=True)
    out["expiry"] = pd.to_datetime(out["expiry"], utc=True, errors="coerce")
    out = out.dropna(subset=["expiry", "strike"])
    out["T"] = ((out["expiry"] - out["trade_date"]).dt.total_seconds() / (365.0 * 24.0 * 3600.0)).clip(lower=1e-6)
    out["iv_mid"] = np.nanmean(np.vstack([out["call_iv"].to_numpy(), out["put_iv"].to_numpy()]), axis=0)
    out["oi"] = out[["call_oi", "put_oi"]].sum(axis=1, min_count=1).fillna(0.0)
    out["mid_call"] = (out["call_bid"] + out["call_ask"]) / 2.0
    out["mid_put"] = (out["put_bid"] + out["put_ask"]) / 2.0
    out["r"] = float(rate)
    out["F"] = float(spot) * np.exp(out["r"] * out["T"])
    out["log_moneyness"] = np.log(out["strike"] / out["F"].clip(lower=1e-6))
    out["total_var"] = (out["iv_mid"] ** 2) * out["T"]
    return out


def clean_chain(df: pd.DataFrame) -> pd.DataFrame:
    """Apply strict filters for B-L validity."""
    out = df.copy()
    out = out.dropna(subset=["strike", "call_bid", "call_ask", "put_bid", "put_ask", "oi", "F", "T", "total_var"])
    out = out[(out["call_bid"] > 0) | (out["put_bid"] > 0)]

    mid_call = ((out["call_bid"] + out["call_ask"]) / 2.0).clip(lower=1e-6)
    spread_ratio = (out["call_ask"] - out["call_bid"]) / mid_call
    out = out[spread_ratio < 0.30]

    out = out[out["oi"] > 50]
    out = out[(out["strike"] / out["F"] > 0.70) & (out["strike"] / out["F"] < 1.40)]
    out = out[(out["total_var"] > 0) & np.isfinite(out["total_var"])]
    return out.sort_values("strike").reset_index(drop=True)
