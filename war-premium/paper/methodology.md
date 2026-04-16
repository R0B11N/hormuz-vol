# Methodology

## Data
- Prediction market series: Polymarket ceasefire contract probability (`poly_prob`).
- Options market series: daily CL and USO chains with strike-level quotes and implied volatilities.
- Baseline controls: `CL=F`, `USO`, `OVX`.
- Event identification: GDELT candidate events with Reuters/AP timestamp verification.

## Risk-Neutral Density Extraction
1. Clean daily chains with quote quality, spread, OI, and moneyness filters.
2. Compute per-contract log-moneyness and total variance.
3. Fit raw SVI to \\\\(k, w(k)\\\\) pairs each day.
4. Interpolate an evenly spaced strike grid from fitted SVI.
5. Apply Breeden-Litzenberger second-derivative extraction on interpolated call prices.
6. Normalize density and report mass coverage over observed strike support.

## Comparative Mapping
Polymarket prices a binary ceasefire probability. To compare with options-implied tail risk, map
\\\\(P(ceasefire)\\\\) into an implied oil-spike probability:

\\\\[
P(oil > 130) = P(cf)P(spike|cf) + (1-P(cf))P(spike|no\_cf)
\\\\]

Conditionals are calibrated from event-labeled regimes in the merged panel.

## Event Study
- Event windows sampled at \\\\(-60,-30,-15,-5,0,5,15,30,60\\\\) minutes around verified events.
- Main output: pooled CAR comparison between Polymarket and options proxy.
- Cross-sections:
  - event type (`MILITARY`, `DIPLOMATIC`, `ECONOMIC`, `POLITICAL`),
  - market-hours vs after-hours,
  - high-OVX vs low-OVX regime.

## Robustness
- Placebo event windows on random timestamps.
- Period-split summaries (`pre_war`, `war_hot`, `ceasefire`, `post`).
- Bidirectional Granger matrix with asymptotic and stationary-bootstrap p-values.

## Limitations
- Daily chain granularity reduces power versus intraday options data.
- Sparse dates can weaken SVI stability and event-study precision.
- Verified-event counts determine inferential strength; low counts require narrative framing.
