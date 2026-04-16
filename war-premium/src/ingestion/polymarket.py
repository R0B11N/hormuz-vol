"""Polymarket ingestion for war-premium project.

The CLOB ``/prices-history`` endpoint expects a CLOB *token id* (long numeric string),
not a URL slug. Resolve via Gamma API (``/markets/slug/{slug}``) or set
``POLYMARKET_YES_TOKEN_ID`` in ``.env``. See ``env.sample`` in the repo root.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

BASE_URL = "https://clob.polymarket.com"
GAMMA_SLUG_URL = "https://gamma-api.polymarket.com/markets/slug"
DEFAULT_SLUG = "iran-israel-ceasefire-2026"
DEFAULT_INTERVAL = "1m"


def _load_env(project_root: Path) -> None:
    if load_dotenv is None:
        return
    env_path = project_root / ".env"
    if env_path.is_file():
        load_dotenv(env_path)


def looks_like_clob_token(value: str) -> bool:
    s = value.strip()
    return s.isdigit() and len(s) >= 10


def resolve_clob_token_id(market_arg: str, project_root: Path) -> str:
    """Return CLOB asset id for ``prices-history`` (YES outcome by default)."""
    _load_env(project_root)
    env_tok = os.environ.get("POLYMARKET_YES_TOKEN_ID") or os.environ.get("POLYMARKET_TOKEN_ID")
    if env_tok and env_tok.strip():
        return env_tok.strip()
    if looks_like_clob_token(market_arg):
        return market_arg.strip()
    slug = (os.environ.get("POLYMARKET_SLUG") or market_arg or DEFAULT_SLUG).strip()
    r = requests.get(f"{GAMMA_SLUG_URL}/{slug}", timeout=30)
    if r.status_code == 404:
        raise ValueError(
            f"No Gamma market for slug={slug!r}. Set POLYMARKET_YES_TOKEN_ID in .env "
            f"(from Polymarket UI → market → inspect ``clobTokenIds``) or fix POLYMARKET_SLUG."
        )
    r.raise_for_status()
    data = r.json()
    raw_ids = data.get("clobTokenIds")
    if not raw_ids:
        raise ValueError(f"Market {slug!r} has no clobTokenIds in Gamma response.")
    ids = json.loads(raw_ids) if isinstance(raw_ids, str) else list(raw_ids)
    idx = int(os.environ.get("POLYMARKET_OUTCOME_INDEX", "0"))
    if idx < 0 or idx >= len(ids):
        raise ValueError(f"POLYMARKET_OUTCOME_INDEX={idx} out of range (len={len(ids)})")
    return str(ids[idx])


@dataclass
class PolymarketConfig:
    clob_token_id: str
    interval: str = DEFAULT_INTERVAL
    fidelity: int = 60
    timeout_sec: int = 30


def fetch_polymarket_history(config: PolymarketConfig) -> pd.DataFrame:
    """Fetch historical tick series for one Polymarket outcome token."""
    response = requests.get(
        f"{BASE_URL}/prices-history",
        params={
            "market": config.clob_token_id,
            "interval": config.interval,
            "fidelity": config.fidelity,
        },
        timeout=config.timeout_sec,
    )
    response.raise_for_status()
    payload = response.json()
    history = payload.get("history", [])
    if not history:
        return pd.DataFrame(
            columns=["poly_prob", "volume", "trade_count"],
            index=pd.DatetimeIndex([], tz="UTC"),
        )

    frame = pd.DataFrame(history)
    if "t" not in frame.columns:
        raise ValueError("Unexpected Polymarket payload: missing 't' field.")
    frame["ts"] = pd.to_datetime(frame["t"], unit="s", utc=True)
    rename_map = {"p": "poly_prob", "v": "volume", "n": "trade_count"}
    frame = frame.rename(columns=rename_map)
    keep = [c for c in ["poly_prob", "volume", "trade_count"] if c in frame.columns]
    out = frame.set_index("ts")[keep].sort_index()
    if not isinstance(out.index, pd.DatetimeIndex):
        out.index = pd.to_datetime(out.index, utc=True)
    return out


def raw_output_path(project_root: Path, trade_date: str) -> Path:
    return project_root / "data" / "raw" / "polymarket" / f"{trade_date}.parquet"


def save_immutable(df: pd.DataFrame, output_path: Path, overwrite: bool = False) -> None:
    """Write raw file once; refuse rewrites unless explicitly requested."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists() and not overwrite:
        raise FileExistsError(
            f"Raw Polymarket file already exists: {output_path}. "
            "Pass --overwrite if you truly need to replace it."
        )
    df.to_parquet(output_path)


def run_for_day(
    project_root: Path,
    trade_date: str,
    market: str = DEFAULT_SLUG,
    interval: str = DEFAULT_INTERVAL,
    overwrite: bool = False,
) -> Path:
    project_root = project_root.resolve()
    token_id = resolve_clob_token_id(market, project_root)
    config = PolymarketConfig(clob_token_id=token_id, interval=interval)
    df = fetch_polymarket_history(config)
    day_start = pd.Timestamp(trade_date, tz="UTC")
    day_end = day_start + pd.Timedelta(days=1)
    if df.empty:
        daily = df
    else:
        daily = df[(df.index >= day_start) & (df.index < day_end)].copy()
    output = raw_output_path(project_root, trade_date)
    save_immutable(daily, output, overwrite=overwrite)
    return output


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pull Polymarket raw history.")
    parser.add_argument("--date", help="YYYY-MM-DD (UTC); required unless --print-clob-token")
    parser.add_argument(
        "--market",
        default=DEFAULT_SLUG,
        help="Gamma slug (default) or numeric CLOB token id; override with POLYMARKET_* env vars",
    )
    parser.add_argument(
        "--interval",
        default=DEFAULT_INTERVAL,
        choices=["1m", "5m", "1h", "6h", "1d", "max", "all"],
    )
    parser.add_argument("--project-root", default=Path(__file__).resolve().parents[2], type=Path)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument(
        "--print-clob-token",
        action="store_true",
        help="Resolve slug/env to YES CLOB token id and print it (no download)",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    project_root = args.project_root.resolve()
    if args.print_clob_token:
        tok = resolve_clob_token_id(args.market, project_root)
        print(tok)
        return
    if not args.date:
        raise SystemExit("--date is required unless --print-clob-token")
    output = run_for_day(
        project_root=project_root,
        trade_date=args.date,
        market=args.market,
        interval=args.interval,
        overwrite=args.overwrite,
    )
    print(f"Saved Polymarket raw snapshot: {output}")


if __name__ == "__main__":
    main()
