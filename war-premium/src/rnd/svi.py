"""SVI fitting and interpolation."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy.optimize import minimize


def svi_raw(k: np.ndarray, a: float, b: float, rho: float, m: float, sigma: float) -> np.ndarray:
    return a + b * (rho * (k - m) + np.sqrt((k - m) ** 2 + sigma**2))


@dataclass
class SVIFitResult:
    a: float
    b: float
    rho: float
    m: float
    sigma: float
    success: bool
    message: str
    rmse: float

    def as_dict(self) -> dict:
        return {
            "a": self.a,
            "b": self.b,
            "rho": self.rho,
            "m": self.m,
            "sigma": self.sigma,
            "success": self.success,
            "message": self.message,
            "rmse": self.rmse,
        }


def fit_svi(log_moneyness: np.ndarray, total_var: np.ndarray) -> SVIFitResult:
    x = np.asarray(log_moneyness, dtype=float)
    y = np.asarray(total_var, dtype=float)
    mask = np.isfinite(x) & np.isfinite(y) & (y > 0)
    x = x[mask]
    y = y[mask]
    if len(x) < 5:
        raise ValueError("Need at least 5 valid points for SVI fit.")

    def loss(params: np.ndarray) -> float:
        a, b, rho, m, sigma = params
        fit = svi_raw(x, a, b, rho, m, sigma)
        penalty = 0.0
        if b <= 0:
            penalty += 1e3
        if sigma <= 0:
            penalty += 1e3
        return float(np.mean((fit - y) ** 2) + penalty)

    bounds = [(-1.0, 1.0), (1e-5, 5.0), (-0.999, 0.999), (-3.0, 3.0), (1e-5, 3.0)]
    x0 = np.array([max(float(np.nanmin(y)), 1e-4), 0.2, -0.2, 0.0, 0.2], dtype=float)
    result = minimize(loss, x0=x0, bounds=bounds, method="L-BFGS-B")
    params = result.x
    fitted = svi_raw(x, *params)
    rmse = float(np.sqrt(np.mean((fitted - y) ** 2)))
    return SVIFitResult(
        a=float(params[0]),
        b=float(params[1]),
        rho=float(params[2]),
        m=float(params[3]),
        sigma=float(params[4]),
        success=bool(result.success),
        message=str(result.message),
        rmse=rmse,
    )


def interpolated_iv_grid(params: dict, F: float, T: float, n_strikes: int = 200) -> pd.DataFrame:
    k_grid = np.linspace(-0.6, 0.6, n_strikes)
    w_grid = svi_raw(k_grid, params["a"], params["b"], params["rho"], params["m"], params["sigma"])
    iv_grid = np.sqrt(np.maximum(w_grid / max(T, 1e-8), 0.0))
    strikes = F * np.exp(k_grid)
    return pd.DataFrame(
        {
            "strike": strikes,
            "log_moneyness": k_grid,
            "iv": iv_grid,
            "total_var": w_grid,
        }
    )
