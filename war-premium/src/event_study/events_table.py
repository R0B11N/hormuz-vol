"""Canonical event table schema and verification workflow."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

EVENT_COLUMNS = [
    "event_id",
    "timestamp_utc",
    "event_type",
    "headline",
    "source",
    "gdelt_tone",
    "verified",
    "verification_source",
    "notes",
]

VALID_EVENT_TYPES = {"MILITARY", "DIPLOMATIC", "ECONOMIC", "POLITICAL", "PLACEBO"}


def empty_event_table() -> pd.DataFrame:
    frame = pd.DataFrame(columns=EVENT_COLUMNS)
    frame["timestamp_utc"] = pd.to_datetime(frame["timestamp_utc"], utc=True)
    frame["verified"] = frame["verified"].astype("boolean")
    return frame


def standardize_events(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    for col in EVENT_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA
    out = out[EVENT_COLUMNS]
    out["timestamp_utc"] = pd.to_datetime(out["timestamp_utc"], utc=True, errors="coerce")
    out["event_type"] = out["event_type"].astype("string").str.upper()
    out.loc[~out["event_type"].isin(VALID_EVENT_TYPES), "event_type"] = pd.NA
    out["verified"] = out["verified"].astype("boolean")
    return out.sort_values(["timestamp_utc", "event_id"]).reset_index(drop=True)


def append_events(master: pd.DataFrame, new_events: Iterable[dict]) -> pd.DataFrame:
    incoming = pd.DataFrame(list(new_events))
    combined = pd.concat([master, incoming], ignore_index=True)
    combined = standardize_events(combined)
    dedup_key = ["event_id"] if combined["event_id"].notna().all() else ["timestamp_utc", "headline", "source"]
    return combined.drop_duplicates(subset=dedup_key, keep="last").reset_index(drop=True)


def verify_events(frame: pd.DataFrame) -> pd.DataFrame:
    """Mark events as verified only when Reuters/AP timestamp evidence exists."""
    out = frame.copy()
    source_text = out["verification_source"].fillna("").astype(str).str.lower()
    out["verified"] = source_text.str.contains("reuters|associated press|\\bap\\b", regex=True)
    return out


def save_master(frame: pd.DataFrame, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    standardize_events(frame).to_parquet(output, index=False)


def load_master(path: Path) -> pd.DataFrame:
    if not path.exists():
        return empty_event_table()
    return standardize_events(pd.read_parquet(path))


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create/update canonical event table.")
    parser.add_argument(
        "--master-path",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "data" / "events" / "events_master.parquet",
    )
    parser.add_argument("--input-csv", type=Path, help="Optional CSV of curated events.")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    master = load_master(args.master_path)
    if args.input_csv:
        incoming = pd.read_csv(args.input_csv)
        merged = append_events(master, incoming.to_dict(orient="records"))
        merged = verify_events(merged)
    else:
        merged = master
    save_master(merged, args.master_path)
    print(f"Saved canonical events table: {args.master_path} ({len(merged)} rows)")


if __name__ == "__main__":
    main()
