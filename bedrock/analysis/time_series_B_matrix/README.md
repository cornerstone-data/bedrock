# B matrix time series analysis

Tools and notes for checking whether the 2017-2024 emission factors `B` move for
real reasons or because of how the model is built.

## Start here

1. **Pick the sectors:** [`IV_score.py`](IV_score.py) ranks every commodity. Read
   [`About_IV_score.md`](About_IV_score.md) for what the score means.
2. **See what moved:** [`sector_deep_dive_triage.py`](sector_deep_dive_triage.py) shows
   `E`, `q`, and whether `B` or `L` drives each sector.
   [`About_sector_deep_dive.md`](About_sector_deep_dive.md) is the how-to.
3. **Record the explanation:** in
   [`About_sector_trend_explanations.md`](About_sector_trend_explanations.md).
4. **Update the status and links:** in
   [`B_driver_investigation.md`](B_driver_investigation.md), the tracker.

```
uv run python -m bedrock.analysis.time_series_B_matrix.IV_score --weights-csv my_weights.csv
uv run python -m bedrock.analysis.time_series_B_matrix.sector_deep_dive_triage --above-iv-score 0.2
```

Each `IV_score` run overwrites `output/IV_score/`, and the triage reads that folder
unless you pass `--iv-csv`. Results go under `output/`, which is not committed. The
triage also overwrites its sectors' curves in `images/sector_trends/`, so commit only
the ones the log uses.

## Terms

| Term | Meaning |
|---|---|
| `E` | Sector emissions |
| `x`, `q` | Industry output and commodity output |
| `B` | Direct emission factor by commodity, `(E / x) @ Vnorm` |
| `A`, `L` | Direct requirements and the Leontief inverse |
| `L_dom`, `N_d` | `L` from domestic `A` only, and the total factor `B L_dom` |
| IV score | Importance times volatility; high means check this factor first |

## Files

**Current workflow**

| File | Use it to |
|---|---|
| [`IV_score.py`](IV_score.py), [`About_IV_score.md`](About_IV_score.md) | Rank commodities for review |
| [`sector_deep_dive_triage.py`](sector_deep_dive_triage.py), [`About_sector_deep_dive.md`](About_sector_deep_dive.md) | See what moved for each sector, and how to follow it up |
| [`About_sector_trend_explanations.md`](About_sector_trend_explanations.md) | Read and record why a sector's trend looks the way it does |
| [`B_driver_investigation.md`](B_driver_investigation.md) | See each sector's status, issues and PRs |
| [`time_series_matrix_assessment.py`](time_series_matrix_assessment.py) | Chart `E`, `B` and `N` over the span for a sector |

**Deeper diagnostics**

| File | Use it to |
|---|---|
| [`B_change_diagnostics.py`](B_change_diagnostics.py) | Split the gap between `E` and `x` by source and route. Writes the `D#` tables, on the full `L` and `N` |

**Past findings**

These are dated, and their figures predate the current vintages.

| File | What it covers |
|---|---|
| [`About_B_change_drivers.md`](About_B_change_drivers.md) | What moved `B` on the 2026-09-21 run |
| [`About_B_smoothing_phase1_outcome.md`](About_B_smoothing_phase1_outcome.md) | The result of B Smoothing Phase 1 |
