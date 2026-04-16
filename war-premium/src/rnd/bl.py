"""Breeden-Litzenberger RND extraction."""

from __future__ import annotations

import numpy as np
from scipy.integrate import trapezoid
from scipy.stats import norm


def bs_call(F: float, K: float, T: float, r: float, sigma: float) -> float:
    sigma = max(float(sigma), 1e-8)
    T = max(float(T), 1e-8)
    d1 = (np.log(max(F, 1e-8) / max(K, 1e-8)) + 0.5 * sigma * sigma * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    return float(np.exp(-r * T) * (F * norm.cdf(d1) - K * norm.cdf(d2)))


def breeden_litzenberger(
    strikes: np.ndarray,
    iv_interp: np.ndarray,
    F: float,
    T: float,
    r: float = 0.05,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, float]:
    strikes = np.asarray(strikes, dtype=float)
    iv_interp = np.asarray(iv_interp, dtype=float)
    calls = np.array([bs_call(F, K, T, r, iv) for K, iv in zip(strikes, iv_interp)], dtype=float)

    dK = np.diff(strikes)
    dC = np.diff(calls)
    dCdK = dC / dK
    k_mid = (strikes[1:] + strikes[:-1]) / 2.0
    k_mid2 = (k_mid[1:] + k_mid[:-1]) / 2.0
    d2CdK2 = np.diff(dCdK) / dK[1:]
    rnd = np.exp(r * T) * d2CdK2
    rnd = np.maximum(rnd, 0.0)

    mass = float(trapezoid(rnd, k_mid2))
    if mass <= 0 or not np.isfinite(mass):
        raise ValueError("Invalid density mass; cannot normalize RND.")
    rnd_norm = rnd / mass

    dk = np.diff(k_mid2, prepend=k_mid2[0])
    cdf = np.cumsum(rnd_norm * dk)
    cdf = np.clip(cdf, 0.0, 1.0)
    return k_mid2, rnd_norm, cdf, mass
