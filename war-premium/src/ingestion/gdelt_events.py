"""GDELT events extraction wrapper."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Optional

import pandas as pd

try:
    from google.cloud import bigquery
except ImportError:  # pragma: no cover - optional dependency
    bigquery = None


GDELT_QUERY_TEMPLATE = """
SELECT
  SQLDATE,
  Actor1Name,
  Actor2Name,
  Actor1CountryCode,
  Actor2CountryCode,
  EventCode,
  AvgTone,
  NumArticles,
  SOURCEURL
FROM `gdelt-bq.gdeltv2.events`
WHERE SQLDATE >= @start_sql_date
  AND (Actor1CountryCode = 'IRN' OR Actor2CountryCode = 'IRN')
  AND Actor1CountryCode != Actor2CountryCode
ORDER BY SQLDATE, NumArticles DESC
"""


def fetch_gdelt_events(start_sql_date: int = 20260227) -> pd.DataFrame:
    if bigquery is None:
        raise ImportError("google-cloud-bigquery is required for GDELT ingestion.")
    client = bigquery.Client()
    cfg = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_sql_date", "INT64", start_sql_date),
        ]
    )
    query_job = client.query(GDELT_QUERY_TEMPLATE, job_config=cfg)
    rows = query_job.result()
    frame = rows.to_dataframe(create_bqstorage_client=False)
    if frame.empty:
        return frame
    frame["timestamp_utc"] = pd.to_datetime(frame["SQLDATE"].astype(str), format="%Y%m%d", utc=True)
    return frame


def output_path(project_root: Path, trade_date: str) -> Path:
    return project_root / "data" / "raw" / "gdelt" / f"{trade_date}.parquet"


def run_for_day(project_root: Path, trade_date: str, start_sql_date: int, overwrite: bool = False) -> Path:
    frame = fetch_gdelt_events(start_sql_date=start_sql_date)
    out = output_path(project_root, trade_date)
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.exists() and not overwrite:
        raise FileExistsError(f"Raw GDELT file already exists: {out}")
    frame.to_parquet(out)
    return out


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pull GDELT event candidates.")
    parser.add_argument("--date", required=True, help="Snapshot date (YYYY-MM-DD)")
    parser.add_argument("--start-sql-date", type=int, default=20260227)
    parser.add_argument("--project-root", default=Path(__file__).resolve().parents[2], type=Path)
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
        print("WARNING: GOOGLE_APPLICATION_CREDENTIALS not set; BigQuery may fail.")
    out = run_for_day(
        project_root=args.project_root,
        trade_date=args.date,
        start_sql_date=args.start_sql_date,
        overwrite=args.overwrite,
    )
    print(f"Saved GDELT candidate events: {out}")


if __name__ == "__main__":
    main()
