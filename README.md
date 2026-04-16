# hormuz-vol

Primary content: **war-premium** — empirical stack for comparing **Polymarket** (binary geopolitical risk) with **options-implied oil distributions**: daily CL/USO chains, SVI-stabilized surfaces, Breeden–Litzenberger extraction, hand-verified event windows, and Granger/bootstrap scaffolding. The pipeline is built to survive a short, non-repeatable sample period—raw pulls stay append-only; processed layers are reproducible from `raw/`.

```
Gamma slug / token ──► CLOB prices-history ──► raw/polymarket/*.parquet
Barchart CSV       ──► normalized chain     ──► raw/options/{CL,USO}/*.parquet
yfinance           ──► CL=F, USO (1h) + OVX (1d, asof-merge) ──► raw/spot_series/*.parquet
verified events    ──► data/events/events_master.parquet
        │                      │
        └──────────► SVI → B-L → processed/rnd_panel.parquet
                              → processed/analysis_panel.parquet
                              → paper/figures | paper/tables
```

## Dependencies

See [`war-premium/requirements.txt`](war-premium/requirements.txt). Python 3.9+; `pandas_market_calendars` is pinned to 4.4.x because 5.x uses `|` union types that break on 3.9.

## Repository map

All executable code and data layout live under **`war-premium/`**:

| Path | Role |
|------|------|
| `war-premium/src/pipeline/daily.py` | Single daily driver (ingest + post-processing) |
| `war-premium/src/ingestion/` | Polymarket, options CSV, spot, GDELT |
| `war-premium/src/rnd/` | Cleaning, SVI, B-L, moments, waterfall plot |
| `war-premium/src/event_study/` | Events schema, CAR, placebo, Granger, bridge |
| `war-premium/data/raw/imports/` | Drop `CL_YYYY-MM-DD.csv`, `USO_YYYY-MM-DD.csv` |
| `war-premium/env.sample` | Template for `.env` (Polymarket token, optional BigQuery) |

## Setup

```bash
cd war-premium
py -m pip install -r requirements.txt
```

Copy `env.sample` to `.env`. Polymarket requires a **CLOB outcome token id** for `prices-history` (not a slug). Resolve the Yes token without downloading:

```bash
py -m src.ingestion.polymarket --print-clob-token --market <gamma-market-slug>
```

Or set `POLYMARKET_YES_TOKEN_ID` from Gamma’s `clobTokenIds` JSON.

## Run

```bash
cd war-premium
py -m src.pipeline.daily --date YYYY-MM-DD --skip-gdelt
```

`--skip-gdelt` avoids BigQuery when `GOOGLE_APPLICATION_CREDENTIALS` is unset. Logs: `war-premium/logs/pipeline_YYYY-MM-DD.log`.

Scheduled runs: `war-premium/scripts/daily_pull.ps1` (Windows) or `daily_pull.sh` (Unix). Task registration: `register_scheduled_task.ps1` (elevated PowerShell once).

## Methods (code-level)

- **SVI** on total variance \(w(k)=\sigma^2 T\) before finite-difference density; **B-L** on interpolated Black–Scholes call prices on a dense strike grid.
- **Polymarket bridge** in `poly_bridge.py`: maps \(P(\text{ceasefire})\) to a comparable tail probability via calibrated conditionals—not a claim that the contracts measure the same object.
- **Events**: schema in `events_table.py`; Reuters/AP verification path before `verified=True`.

## License

Not set. Choose one before publishing.
