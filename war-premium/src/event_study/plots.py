"""Plot helpers for event-study analysis."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def plot_car(poly_windows: pd.DataFrame, options_windows: pd.DataFrame, title: str = "") -> plt.Figure:
    poly_car = poly_windows.groupby("window_min")["pct_delta"].agg(["mean", "sem"])
    opts_car = options_windows.groupby("window_min")["pct_delta"].agg(["mean", "sem"])

    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(poly_car.index, poly_car["mean"], label="Polymarket", color="#1D9E75", lw=2)
    ax.fill_between(
        poly_car.index,
        poly_car["mean"] - poly_car["sem"].fillna(0),
        poly_car["mean"] + poly_car["sem"].fillna(0),
        alpha=0.15,
        color="#1D9E75",
    )
    ax.plot(opts_car.index, opts_car["mean"], label="Options", color="#534AB7", lw=2)
    ax.fill_between(
        opts_car.index,
        opts_car["mean"] - opts_car["sem"].fillna(0),
        opts_car["mean"] + opts_car["sem"].fillna(0),
        alpha=0.15,
        color="#534AB7",
    )
    ax.axvline(0, color="black", lw=0.7, ls="--")
    ax.set_xlabel("minutes relative to event")
    ax.set_ylabel("avg pct change from T=0")
    ax.set_title(title)
    ax.legend()
    fig.tight_layout()
    return fig


def save_figure(fig: plt.Figure, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path)
    plt.close(fig)
