"""Bridge Polymarket ceasefire probabilities to oil spike probabilities."""

from __future__ import annotations

import numpy as np
import pandas as pd


def poly_implied_spike(
    p_ceasefire: float,
    p_spike_given_cf: float,
    p_spike_given_no_cf: float,
) -> float:
    return float(
        p_ceasefire * p_spike_given_cf + (1.0 - p_ceasefire) * p_spike_given_no_cf
    )


def calibrate_conditionals(
    panel: pd.DataFrame,
    ceasefire_col: str = "poly_prob",
    spike_col: str = "p_spike_130",
    label_col: str = "ceasefire_regime",
) -> dict:
    """Calibrate P(spike|cf) and P(spike|no_cf) from labeled periods."""
    out = {"p_spike_given_cf": 0.03, "p_spike_given_no_cf": 0.25}
    if panel.empty or label_col not in panel.columns or spike_col not in panel.columns:
        return out

    cf_mask = panel[label_col].astype(str).str.lower().eq("ceasefire")
    non_cf_mask = panel[label_col].astype(str).str.lower().ne("ceasefire")
    if cf_mask.any():
        out["p_spike_given_cf"] = float(np.nanmean(panel.loc[cf_mask, spike_col]))
    if non_cf_mask.any():
        out["p_spike_given_no_cf"] = float(np.nanmean(panel.loc[non_cf_mask, spike_col]))
    out["p_spike_given_cf"] = float(np.clip(out["p_spike_given_cf"], 0.0, 1.0))
    out["p_spike_given_no_cf"] = float(np.clip(out["p_spike_given_no_cf"], 0.0, 1.0))
    return out


def apply_bridge(
    panel: pd.DataFrame,
    ceasefire_col: str = "poly_prob",
    p_spike_given_cf: float = 0.03,
    p_spike_given_no_cf: float = 0.25,
) -> pd.Series:
    probs = panel[ceasefire_col].astype(float).clip(0.0, 1.0)
    return probs * p_spike_given_cf + (1.0 - probs) * p_spike_given_no_cf
