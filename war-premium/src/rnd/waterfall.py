"""RND waterfall figure builder."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def build_placeholder_waterfall(rnd_panel: pd.DataFrame, output: Path) -> None:
    fig = plt.figure(figsize=(10, 6))
    ax = fig.add_subplot(111, projection="3d")
    if rnd_panel.empty:
        ax.text(0, 0, 0, "No RND data yet")
    else:
        sample = rnd_panel.dropna(subset=["date", "p_spike_130"]).copy()
        sample = sample.sort_values("date").head(10)
        xs = np.arange(len(sample))
        ys = np.zeros_like(xs)
        zs = np.zeros_like(xs, dtype=float)
        dx = np.ones_like(xs) * 0.6
        dy = np.ones_like(xs) * 0.6
        dz = sample["p_spike_130"].to_numpy()
        ax.bar3d(xs, ys, zs, dx, dy, dz, shade=True, alpha=0.7)
        ax.set_xticks(xs + 0.3)
        ax.set_xticklabels(sample["date"].astype(str).tolist(), rotation=45, ha="right")
    ax.set_title("Figure 1 Draft: RND Evolution Waterfall")
    ax.set_zlabel("P(oil > 130)")
    fig.tight_layout()
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output)
    plt.close(fig)


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate RND waterfall figure.")
    parser.add_argument("--project-root", type=Path, default=Path(__file__).resolve().parents[2])
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    rnd_path = args.project_root / "data" / "processed" / "rnd_panel.parquet"
    rnd = pd.read_parquet(rnd_path) if rnd_path.exists() else pd.DataFrame()
    out = args.project_root / "paper" / "figures" / "rnd_waterfall.pdf"
    build_placeholder_waterfall(rnd, out)
    print(f"Saved waterfall figure: {out}")


if __name__ == "__main__":
    main()
