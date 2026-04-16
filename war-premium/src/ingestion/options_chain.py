"""Daily EOD options chain ingestion for CL and USO."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import pandas as pd

REQUIRED_COLUMNS = {
    "strike": ["strike", "Strike"],
    "expiry": ["expiry", "expiration", "Expiry", "Expiration"],
    "call_bid": ["call_bid", "Call Bid", "cBid", "bid_call"],
    "call_ask": ["call_ask", "Call Ask", "cAsk", "ask_call"],
    "put_bid": ["put_bid", "Put Bid", "pBid", "bid_put"],
    "put_ask": ["put_ask", "Put Ask", "pAsk", "ask_put"],
    "call_iv": ["call_iv", "Call IV", "cIV", "iv_call"],
    "put_iv": ["put_iv", "Put IV", "pIV", "iv_put"],
    "call_oi": ["call_oi", "Call OI", "cOI", "oi_call"],
    "put_oi": ["put_oi", "Put OI", "pOI", "oi_put"],
    "call_volume": ["call_volume", "Call Volume", "cVol", "vol_call"],
    "put_volume": ["put_volume", "Put Volume", "pVol", "vol_put"],
}


def _pick_column(frame: pd.DataFrame, aliases: list[str]) -> pd.Series:
    for alias in aliases:
        if alias in frame.columns:
            return frame[alias]
    return pd.Series([pd.NA] * len(frame), index=frame.index)


def normalize_chain(frame: pd.DataFrame, symbol: str, trade_date: str) -> pd.DataFrame:
    normalized = pd.DataFrame(index=frame.index)
    for target_col, aliases in REQUIRED_COLUMNS.items():
        normalized[target_col] = _pick_column(frame, aliases)

    numeric_cols = [
        "strike",
        "call_bid",
        "call_ask",
        "put_bid",
        "put_ask",
        "call_iv",
        "put_iv",
        "call_oi",
        "put_oi",
        "call_volume",
        "put_volume",
    ]
    for col in numeric_cols:
        normalized[col] = pd.to_numeric(normalized[col], errors="coerce")

    normalized["expiry"] = pd.to_datetime(normalized["expiry"], errors="coerce", utc=True)
    normalized["date"] = pd.Timestamp(trade_date, tz="UTC")
    normalized["underlying"] = symbol.upper()
    normalized = normalized.dropna(subset=["strike"]).sort_values("strike")
    return normalized.reset_index(drop=True)


def raw_output_path(project_root: Path, symbol: str, trade_date: str) -> Path:
    return project_root / "data" / "raw" / "options" / symbol.upper() / f"{trade_date}.parquet"


def read_chain_source(source: str) -> pd.DataFrame:
    src = source.strip()
    if src.lower().startswith("http"):
        return pd.read_csv(src)
    src_path = Path(src).expanduser().resolve()
    if not src_path.exists():
        raise FileNotFoundError(f"Options source not found: {src_path}")
    return pd.read_csv(src_path)


def save_immutable(df: pd.DataFrame, output: Path, overwrite: bool = False) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists() and not overwrite:
        raise FileExistsError(
            f"Raw options snapshot already exists: {output}. "
            "Use --overwrite if replacement is intentional."
        )
    df.to_parquet(output)


def run_for_day(
    project_root: Path,
    symbol: str,
    trade_date: str,
    source: str,
    overwrite: bool = False,
) -> Path:
    raw_frame = read_chain_source(source)
    normalized = normalize_chain(raw_frame, symbol=symbol, trade_date=trade_date)
    output = raw_output_path(project_root, symbol=symbol, trade_date=trade_date)
    save_immutable(normalized, output, overwrite=overwrite)
    return output


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest a daily options chain snapshot.")
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--symbol", required=True, choices=["CL", "USO"])
    parser.add_argument(
        "--source",
        required=True,
        help="CSV path or HTTP URL from Barchart export/API response",
    )
    parser.add_argument("--project-root", default=Path(__file__).resolve().parents[2], type=Path)
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    out = run_for_day(
        project_root=args.project_root,
        symbol=args.symbol,
        trade_date=args.date,
        source=args.source,
        overwrite=args.overwrite,
    )
    print(f"Saved options snapshot: {out}")


if __name__ == "__main__":
    main()
