"""Risk-neutral moments from extracted RND."""

from __future__ import annotations

import numpy as np
from scipy.integrate import trapezoid


def rnd_moments(strikes: np.ndarray, density: np.ndarray, F: float) -> dict:
    strikes = np.asarray(strikes, dtype=float)
    density = np.asarray(density, dtype=float)
    mu = float(trapezoid(strikes * density, strikes))
    var = float(trapezoid(((strikes - mu) ** 2) * density, strikes))
    var = max(var, 1e-12)
    vol = float(np.sqrt(var))
    skew = float(trapezoid((((strikes - mu) / vol) ** 3) * density, strikes))
    kurt = float(trapezoid((((strikes - mu) / vol) ** 4) * density, strikes) - 3.0)
    mask_130 = strikes > 130.0
    mask_60 = strikes < 60.0
    p_spike_130 = float(trapezoid(density[mask_130], strikes[mask_130])) if mask_130.any() else 0.0
    p_crash_60 = float(trapezoid(density[mask_60], strikes[mask_60])) if mask_60.any() else 0.0
    return {
        "mean": mu,
        "variance": var,
        "vol": vol,
        "skew": skew,
        "kurt": kurt,
        "p_spike_130": p_spike_130,
        "p_crash_60": p_crash_60,
        "forward": float(F),
    }
