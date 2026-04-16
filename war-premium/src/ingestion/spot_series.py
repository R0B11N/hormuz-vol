"""Spot and volatility baseline ingestion."""

from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Optional

import pandas as pd
import yfinance as yf

HOURLY_TICKERS = {
    "CL=F": "cl_spot",
    "USO": "uso",
}
OVX_TICKER = "OVX"


def _yf_download_with_retry(
    tickers: list[str],
    *,
    start: str,
    end: Optional[str],
    interval: str,
    max_attempts: int = 3,
) -> pd.DataFrame:
    last: Optional[Exception] = None
    for attempt in range(max_attempts):
        try:
            return yf.download(
                tickers,
                start=start,
                end=end,
                interval=interval,
                auto_adjust=False,
                group_by="ticker",
                progress=False,
            )
        except Exception as exc:
            last = exc
            time.sleep(2.0 * (attempt + 1))
    if last:
        raise last
    return pd.DataFrame()


def _empty_spot_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=list(HOURLY_TICKERS.values()) + ["ovx"],
        index=pd.DatetimeIndex([], tz="UTC"),
    )


def fetch_spot_series(
    start_date: str,
    end_date: Optional[str] = None,
    interval: str = "1h",
    ovx_interval: str = "1d",
) -> pd.DataFrame:
    """Fetch CL, USO at ``interval``; OVX at ``ovx_interval`` (daily avoids yfinance hourly gaps)."""
    try:
        hourly = _yf_download_with_retry(
            list(HOURLY_TICKERS.keys()),
            start=start_date,
            end=end_date,
            interval=interval,
        )
    except Exception:
        hourly = pd.DataFrame()

    close_hourly: dict[str, pd.Series] = {}
    if not hourly.empty and hourly.columns.nlevels >= 2:
        for ticker, alias in HOURLY_TICKERS.items():
            if ticker in hourly.columns.get_level_values(0):
                close_hourly[alias] = hourly[ticker]["Close"]
            else:
                close_hourly[alias] = pd.Series(dtype="float64")
        frame = pd.DataFrame(close_hourly).sort_index()
        frame.index = pd.to_datetime(frame.index, utc=True)
    else:
        return _empty_spot_frame()

    if frame.empty:
        return _empty_spot_frame()

    ovx_close = pd.Series(dtype="float64")
    time.sleep(1.0)
    try:
        ovx_raw = _yf_download_with_retry(
            [OVX_TICKER],
            start=start_date,
            end=end_date,
            interval=ovx_interval,
        )
        if not ovx_raw.empty and OVX_TICKER in ovx_raw.columns.get_level_values(0):
            ovx_close = ovx_raw[OVX_TICKER]["Close"].copy()
            ovx_close.index = pd.to_datetime(ovx_close.index, utc=True)
            ovx_close = ovx_close.sort_index()
    except Exception:
        pass

    if ovx_close.empty:
        frame["ovx"] = pd.NA
        return frame

    bdf = frame.reset_index()
    ts_col = bdf.columns[0]
    bdf = bdf.rename(columns={ts_col: "ts"})
    odf = ovx_close.reset_index()
    odf.columns = ["ts", "ovx"]
    merged = pd.merge_asof(
        bdf.sort_values("ts"),
        odf.sort_values("ts"),
        on="ts",
        direction="backward",
    )
    out = merged.set_index("ts").sort_index()
    return out


def output_path(project_root: Path, trade_date: str) -> Path:
    return project_root / "data" / "raw" / "spot_series" / f"{trade_date}.parquet"


def run_for_day(
    project_root: Path,
    trade_date: str,
    start_date: str,
    interval: str = "1h",
    ovx_interval: str = "1d",
    overwrite: bool = False,
) -> Path:
    frame = fetch_spot_series(
        start_date=start_date,
        end_date=None,
        interval=interval,
        ovx_interval=ovx_interval,
    )
    day_start = pd.Timestamp(trade_date, tz="UTC")
    day_end = day_start + pd.Timedelta(days=1)
    if frame.empty:
        daily = frame
    else:
        daily = frame[(frame.index >= day_start) & (frame.index < day_end)]
    out = output_path(project_root, trade_date)
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.exists() and not overwrite:
        raise FileExistsError(f"Raw spot snapshot already exists: {out}")
    daily.to_parquet(out)
    return out


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch CL/USO/OVX baseline series.")
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--start-date", default="2026-02-20")
    parser.add_argument("--interval", default="1h", help="yfinance interval for CL=F and USO")
    parser.add_argument(
        "--ovx-interval",
        default="1d",
        help="yfinance interval for OVX (1d recommended; hourly often missing)",
    )
    parser.add_argument("--project-root", default=Path(__file__).resolve().parents[2], type=Path)
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    out = run_for_day(
        project_root=args.project_root,
        trade_date=args.date,
        start_date=args.start_date,
        interval=args.interval,
        ovx_interval=args.ovx_interval,
        overwrite=args.overwrite,
    )
    print(f"Saved spot snapshot: {out}")


if __name__ == "__main__":
    main()
