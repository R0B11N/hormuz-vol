"""Run full daily pipeline: ingest raw data, integrity, then processed outputs.

Designed for Task Scheduler / cron. Continues after non-fatal step failures; exits
non-zero if any step fails (ingest + post are both counted).
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def _log(log_path: Optional[Path], msg: str) -> None:
    line = msg if msg.endswith("\n") else msg + "\n"
    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as f:
            f.write(line)
    print(line, end="")


def _run(
    project_root: Path,
    module: str,
    args: list[str],
    log_path: Optional[Path],
    label: str,
) -> int:
    cmd = [sys.executable, "-m", module, *args]
    _log(log_path, f"[{label}] {' '.join(cmd)}\n")
    proc = subprocess.run(
        cmd,
        cwd=str(project_root),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if proc.stdout:
        _log(log_path, proc.stdout)
    if proc.stderr:
        _log(log_path, proc.stderr)
    _log(log_path, f"[{label}] exit_code={proc.returncode}\n")
    return proc.returncode


def default_trade_date() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def run_post_steps(project_root: Path, log_path: Optional[Path]) -> int:
    """Run RND + analysis + figures. Returns count of failed steps."""
    failed = 0
    root_s = str(project_root)
    post_steps = [
        ("src.rnd.run_daily_rnd", ["--project-root", root_s]),
        ("src.event_study.analysis_panel", ["--project-root", root_s]),
        ("src.event_study.run_event_study", ["--project-root", root_s]),
        ("src.event_study.run_granger_table", ["--project-root", root_s]),
        ("src.rnd.waterfall", ["--project-root", root_s]),
    ]
    for module, margs in post_steps:
        rc = _run(project_root, module, margs, log_path, module)
        if rc != 0:
            failed += 1
    return failed


def run_post_only(project_root: Path, log_path: Optional[Path]) -> int:
    failed = run_post_steps(project_root, log_path)
    _log(log_path, f"[pipeline] post-only failed_steps={failed}\n")
    return 0 if failed == 0 else 1


def run_daily(
    project_root: Path,
    trade_date: str,
    start_date: str,
    spot_interval: str,
    log_path: Optional[Path],
    skip_gdelt: bool,
    skip_post: bool,
) -> int:
    """Run pipeline. Returns 0 if all steps succeeded."""
    errors = 0
    root_s = str(project_root)

    (project_root / "data" / "raw" / "imports").mkdir(parents=True, exist_ok=True)

    rc = _run(
        project_root,
        "src.ingestion.polymarket",
        ["--date", trade_date, "--overwrite", "--project-root", root_s],
        log_path,
        "polymarket",
    )
    if rc != 0:
        errors += 1

    cl_csv = project_root / "data" / "raw" / "imports" / f"CL_{trade_date}.csv"
    if cl_csv.is_file():
        rc = _run(
            project_root,
            "src.ingestion.options_chain",
            [
                "--date",
                trade_date,
                "--symbol",
                "CL",
                "--source",
                str(cl_csv),
                "--overwrite",
                "--project-root",
                root_s,
            ],
            log_path,
            "options_CL",
        )
        if rc != 0:
            errors += 1

    uso_csv = project_root / "data" / "raw" / "imports" / f"USO_{trade_date}.csv"
    if uso_csv.is_file():
        rc = _run(
            project_root,
            "src.ingestion.options_chain",
            [
                "--date",
                trade_date,
                "--symbol",
                "USO",
                "--source",
                str(uso_csv),
                "--overwrite",
                "--project-root",
                root_s,
            ],
            log_path,
            "options_USO",
        )
        if rc != 0:
            errors += 1

    rc = _run(
        project_root,
        "src.ingestion.spot_series",
        [
            "--date",
            trade_date,
            "--start-date",
            start_date,
            "--interval",
            spot_interval,
            "--ovx-interval",
            "1d",
            "--overwrite",
            "--project-root",
            root_s,
        ],
        log_path,
        "spot_series",
    )
    if rc != 0:
        errors += 1

    if not skip_gdelt:
        rc = _run(
            project_root,
            "src.ingestion.gdelt_events",
            [
                "--date",
                trade_date,
                "--start-sql-date",
                "20260227",
                "--overwrite",
                "--project-root",
                root_s,
            ],
            log_path,
            "gdelt_events",
        )
        if rc != 0:
            _log(log_path, "[gdelt_events] non-fatal: continuing without GDELT snapshot\n")

    rc = _run(
        project_root,
        "src.utils.integrity",
        ["--date", trade_date, "--project-root", root_s],
        log_path,
        "integrity",
    )
    if rc != 0:
        _log(log_path, "[integrity] reported issues (see output above)\n")

    poly_path = project_root / "data" / "raw" / "polymarket" / f"{trade_date}.parquet"
    liq_out = project_root / "data" / "processed" / f"polymarket_liquidity_{trade_date}.parquet"
    if poly_path.is_file() and poly_path.stat().st_size > 0:
        rc = _run(
            project_root,
            "src.utils.liquidity_audit",
            [
                "--input",
                str(poly_path),
                "--output",
                str(liq_out),
            ],
            log_path,
            "liquidity_audit",
        )
        if rc != 0:
            _log(log_path, "[liquidity_audit] non-fatal\n")

    if skip_post:
        _log(log_path, f"[pipeline] post-processing skipped. errors_so_far={errors}\n")
        return 0 if errors == 0 else 1

    errors += run_post_steps(project_root, log_path)

    _log(log_path, f"[pipeline] done trade_date={trade_date} total_failed_steps={errors}\n")
    return 0 if errors == 0 else 1


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Daily ingest + process pipeline.")
    p.add_argument("--date", default=None, help="Trade date UTC YYYY-MM-DD (default: today UTC)")
    p.add_argument("--start-date", default="2026-02-20", help="Spot series download start")
    p.add_argument("--spot-interval", default="1h", help="yfinance interval for spot batch")
    p.add_argument("--project-root", type=Path, default=Path(__file__).resolve().parents[2])
    p.add_argument(
        "--log",
        type=Path,
        default=None,
        help="Append log file (default: logs/pipeline_YYYY-MM-DD.log)",
    )
    p.add_argument("--skip-gdelt", action="store_true", help="Skip BigQuery GDELT pull")
    p.add_argument("--post-only", action="store_true", help="Only run RND/analysis/post steps")
    p.add_argument("--ingest-only", action="store_true", help="Only run ingest + integrity + liquidity audit")
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    project_root = args.project_root.resolve()
    trade_date = args.date or default_trade_date()
    log_path = args.log
    if log_path is None:
        log_path = project_root / "logs" / f"pipeline_{trade_date}.log"

    _log(log_path, f"=== pipeline start {trade_date} ===\n")

    if args.post_only:
        sys.exit(run_post_only(project_root, log_path))

    rc = run_daily(
        project_root=project_root,
        trade_date=trade_date,
        start_date=args.start_date,
        spot_interval=args.spot_interval,
        log_path=log_path,
        skip_gdelt=args.skip_gdelt,
        skip_post=bool(args.ingest_only),
    )
    sys.exit(rc)


if __name__ == "__main__":
    main()
