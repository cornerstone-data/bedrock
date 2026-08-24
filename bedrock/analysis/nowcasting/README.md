# nowcasting

Diagnostics for the nowcasted US IOT build: how far each block of the Supply and
Use tables has got, measured cell by cell against the published 2017 detail SUT
([#587](https://github.com/cornerstone-data/bedrock/issues/587)).

## Running

```bash
# every runnable block: writes output/<section>_<year>.png and prints its report
uv run python -m bedrock.analysis.nowcasting.plots

# one block, screen resolution
uv run python -m bedrock.analysis.nowcasting.plots --section use_fd_detail_sut

# the Step 4c margin anchor against the published Supply columns
uv run python -m bedrock.analysis.nowcasting.margins_2017_baseline --check

# how fast a frozen 2017 input structure goes stale, and whether inflation fixes it (Step 3)
uv run python -m bedrock.analysis.nowcasting.intermediate_structure_drift --all

# the copies embedded in the progress report
uv run python -m bedrock.analysis.nowcasting.plots \
    --dpi 110 --out-dir <report images dir> --no-report

# the colour-vision separation check behind the palette
uv run python -m bedrock.analysis.nowcasting.plots --check-palette

# toy SUT balancer walkthrough (writes sut_balancing/output/)
uv run python bedrock/analysis/nowcasting/sut_balancing/plot_full_nowcasting_sut_balance.py
```

Blocks with no candidate yet are skipped with a message rather than failing.

## Layout

- `table_match.py` — the comparison engine. `compare_tables(candidate,
  reference, tolerance=Tolerance(...))` returns a `TableMatch`: a per-cell
  status matrix (`MATCH` / `PARTIAL` / `MISS` / `EXTRA` / `ABSENT`), relative
  errors, a `severity` ramp, `Margin` objects for the row and column totals
  classified by the same rules, `counts()` / `summary()` for machine-readable
  output, and `assert_ok(...)` for use as an assertion. One `Tolerance` governs
  a whole comparison — it never varies by row or column. No plotting.
- `sections.py` — the comparable blocks. Each `Section` fixes a row axis, a
  column axis, a reference loader, a candidate loader and a tolerance;
  `SECTIONS` is the registry and `Section.run(year)` returns a `TableMatch`. A
  section with `candidate=None` is declared but not yet runnable
  (`Section.runnable`).

  | section | step | shape | candidate |
  |---|---|---|---|
  | `use_fd_detail_sut` | 1 — Use final-demand columns | 402 × 19 | exported CSV |
  | `use_va_detail_sut` | 2 — Use value-added rows | 3 × 402 | none yet |
  | `supply_bridge_detail_sut` | 4 — Supply imports, margins, taxes and the basic→purchaser subtotals | 402 × 12 | `derive_initial_supply_bridge` (MCIF) |

  Those three are the whole of what a published 2017 detail reference supports
  outside the two 402 × 402 interiors.
- `plots.py` — the renderer and its CLI. Draws the interior as a single raster
  `imshow`, with the row totals as a strip down the right edge and the column
  totals along the bottom, on the same colour scale. `palette_separation()`
  re-runs the colour-vision check behind the palette.
- `initial_Y_pur_baseline.py` — the Step 1 final-demand comparison against the
  published 2017 detail Use table and against the PCE/PEQ bridges. Writes the
  cell-wise CSV exports in `output/` that `sections.py` reads as the Step 1
  candidate.
- `margins_2017_baseline.py` — the Step 4c phase-1 check ([#610](https://github.com/cornerstone-data/bedrock/issues/610)):
  aggregates [`transform/iot/nowcast_margins.py`](../../transform/iot/nowcast_margins.py)'s
  transaction-level rates back to the published Supply table's `TRADE`/`TRANS`
  columns, commodity by commodity, and reports what each residual is. `--check`
  exits non-zero if a count regresses. Writes the per-commodity comparison and
  the rate table to `output/`.
- `intermediate_structure_drift.py` — the Step 3 measurements
  ([#497](https://github.com/cornerstone-data/bedrock/issues/497)): the index of
  dissimilarity between a carried input structure and a published one, computed
  on column shares with the column total given, because Step 5 holds both
  margins of that block and only the structure survives. `--drift` scores frozen
  2017 against the summary Use SUT 2018-2024, `--inflation` scores the price-index
  carry against it, `--holdout` runs the out-of-sample 2012 → 2017 detail version,
  `--where` locates the drift by column. Prints only; writes nothing.
- [`trade_data/`](trade_data/README.md) — Step 1d/4b source evaluation for the
  trade columns (#527): three 2017 probes scoring a Census goods + BEA services
  extract against the SUT targets — Use `F04000` for exports, Supply `MCIF` /
  `MADJ` / `MDTY` for imports — plus the options writeup behind the source
  decision.
- [`compare_NIPA_to_IOT/`](compare_NIPA_to_IOT/README.md) — the NIPA side. Loads
  any NIPA table as a flat frame with its hierarchy intact
  (`nipa_flat_table`), loads BEA IOT matrices (`bea_matrix_row` /
  `bea_matrix_column`), and aligns the two while keeping **matched-cell
  disagreement separate from unmatched mass** (`compare`) — the distinction
  every reconciliation here depends on. `value_added_control_totals.py` is built
  on it.

Candidate mass on labels outside a section's frame is not drawn and not dropped:
`TableMatch.residual` totals it and `report()` prints it.

`output/` is untracked — CSV exports and figures are working artifacts of a
local run.

## Documents

- [`sut_balancing/`](sut_balancing/) — Step 5 balancer plans and the toy
  walkthrough (`plot_full_nowcasting_sut_balance.py`).
- [`plan.md`](plan.md) — the nowcast build plan: the seven steps, their data
  sources, and the open decisions.
- [`compensation_disaggregation_plan.md`](compensation_disaggregation_plan.md) —
  Step 2, splitting `V00100` from NIPA's ~74 industries to BEA 2017 detail:
  wages and supplements separately, QCEW payroll as the movement series, and
  the sectors where QCEW does not work.
- [`intermediate_estimation_plan.md`](intermediate_estimation_plan.md) — Step 3,
  the Use table's commodity × industry interior: why the estimand is the
  cross-structure rather than any level, how fast a frozen 2017 structure decays,
  why the inflation carry turns harmful from 2022, and a re-sort of #577 / #578 /
  #564 by whether a source delivers a mix or only a total.
- [`margins_estimation_plan.md`](margins_estimation_plan.md) — Step 4c, the
  transaction-level Margins table and the Supply `TRADE`/`TRANS` columns: BEA's
  own method from the 2009 IO manual chapter 8 checked against the 2017 tables,
  what of it is reproducible, and where the sources already exist (flowsa
  `margins` branch, stateior FAF).
- [`inventories_estimation_plan.md`](inventories_estimation_plan.md) — Step 1e,
  the `F03000` change-in-inventories column: BEA's four allocation rules from
  the Hill correspondence, why the previous "deferred pending ASM and Economic
  Census" scoping was wrong, and the one crosswalk that remains. Rescopes
  [#530](https://github.com/cornerstone-data/bedrock/issues/530).
- [`progress_report.md`](progress_report.md) — where the build stands, with the
  figures embedded. Regenerated per milestone; its images are the one tracked
  thing under `images/`.
- [`About_table_match.md`](About_table_match.md) — what the first Step 1 run
  showed.
- [`annual_survey_expense_sources.md`](annual_survey_expense_sources.md) — Step 3
  probe of annual survey data as a source of input structure for the Use
  intermediate block: Census business surveys, Census state and local government
  finances, USDA ERS farm income. Verdict is mixed — negative for the business
  surveys, positive for agriculture and government. Existing extractors it
  builds on: [`Census_SAS.yaml`](../../extract/census/Census_SAS.yaml),
  [`Census_ASM.yaml`](../../extract/census/Census_ASM.yaml),
  [`Census_EC.yaml`](../../extract/census/Census_EC.yaml),
  [`USDA_ERS_FIWS.yaml`](../../extract/usda/USDA_ERS_FIWS.yaml).
