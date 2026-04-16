# Results Workflow

## Core Outputs
- Figure 1 target: RND evolution waterfall (key dates with event annotations).
- Figure set: pooled CAR and stratified CAR plots.
- Table 1 target: lag-wise Granger matrix with bootstrap p-values.
- Robustness tables: period split summary and placebo diagnostics.

## Current Artifact Paths
- Processed panel: `data/processed/analysis_panel.parquet`
- RND panel: `data/processed/rnd_panel.parquet`
- Event windows: `data/processed/event_windows_poly.parquet`, `data/processed/event_windows_options.parquet`
- Placebo windows: `data/processed/placebo_windows.parquet`
- Granger matrix: `paper/tables/granger_matrix.parquet`

## Interpretation Checklist
1. Confirm RND mass coverage is stable by date and underlying.
2. Compare `poly_implied_spike` vs options `p_spike_130`.
3. Evaluate CAR sign/magnitude around event time across strata.
4. Verify placebo CAR remains approximately flat.
5. Report period-dependent changes without overclaiming causality.
