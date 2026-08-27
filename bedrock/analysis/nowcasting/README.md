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

# how T00OTOP should be allocated to detail industries (Step 2)
uv run python -m bedrock.analysis.nowcasting.other_taxes_allocation --check

# what industry axis V00100 and V00300 have in NIPA (Step 2)
uv run python -m bedrock.analysis.nowcasting.compensation_allocation --check

# how fast a frozen 2017 input structure goes stale, and whether inflation fixes it (Step 3)
uv run python -m bedrock.analysis.nowcasting.intermediate_structure_drift --all

# what the imports and exports estimates cost the Use interior (Steps 3/4b/1d)
uv run python -m bedrock.analysis.nowcasting.row_control_exposure

# what Step 2 estimates for 2018-2024, now that VAPRO and its three components
# are both published annually (Step 2)
uv run python -m bedrock.analysis.nowcasting.value_added_timeseries --check

# does QCEW wage growth predict detail compensation? graded 2012->2017 (Step 2)
uv run python -m bedrock.analysis.nowcasting.compensation_movement_holdout --check

# the shipped V00100 weight vector: vintage bridge, carve-out, coverage and
# suppression guards. Not analysis -- this one feeds NIPA_VA_compensation_<year>
uv run python -m bedrock.transform.nipa.compensation_movement --check

# the shipped T00OTOP weight vector: the housing and farm blocks rescaled to
# their published NIPA lines. Feeds NIPA_VA_othertax_<year>
uv run python -m bedrock.transform.nipa.othertax_lookups --check

# can the product-tax rows be converted from commodity to industry? (Step 2)
uv run python -m bedrock.analysis.nowcasting.tax_axis_conversion --check

# the shipped T00TOP/T00SUB rows: the operator above, made annual. Not analysis --
# derive_initial_value_added stacks these two onto the three NIPA rows
uv run python -m bedrock.transform.iot.nowcast_va_taxes --check
# what can be sourced for manufacturing's input column (needs the
# Census_EC_MatFuel, Census_EC_Expenses, Census_ASM_Expenses and
# Census_AIES_Expenses FBAs)
uv run python -m bedrock.analysis.nowcasting.inputs_structure --all

# the non-materials cells, and the seed they carry
uv run python -m bedrock.analysis.nowcasting.inputs_structure --services

# how good the MATFUEL suppression fill is, against masked truth
uv run python -m bedrock.analysis.nowcasting.inputs_structure --holdout

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
  | `use_va_detail_sut` | 2 — Use value-added rows | 3 × 402 | `derive_initial_value_added` (all three rows, 2017) |
  | `use_intermediate_detail_sut` | 3 — Use intermediate interior | 402 × 402 | `derive_initial_U_intermediate` |
  | `supply_bridge_detail_sut` | 4 — Supply imports, margins, taxes and the basic→purchaser subtotals | 402 × 12 | `derive_initial_supply_bridge` (MCIF) |

  The first three are the whole of what a published 2017 detail reference
  supports *outside* the two 402 × 402 interiors.
  `use_intermediate_detail_sut` is one of those interiors: 161,604 cells is too
  many to read as a picture but not too many to score, and the section machinery
  reports the totals, the margins and a status count without anyone looking at
  the grid. The Supply interior — the Make table, Step 4a — is still undeclared.
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
  `--where` locates the drift by column, `--revision` measures BEA's own restatement
  of a year it had already published, `--theta` fits the carry exponent,
  `--control` scores the built column control against the published summary
  `T005`, and `--seed` reports the built block year by year. Prints only; writes
  nothing.
- `row_control_exposure.py` — what `MCIF` and `F04000` error costs the
  intermediate block. Step 3's commodity row is the residual
  `T001 = T016 − Σ_FD Y`, so trade error lands in the interior; this scores it per
  commodity, weighted by how much of the commodity goes to industry rather than to
  final demand. Prints only; writes nothing.
- `inputs_structure.py` — what can be sourced for manufacturing's intermediate
  input column. Was `materials_structure.py`; renamed because the sources it
  reads now reach **86% of that column**, 79 points of which are materials.
  `--coverage` classifies every `MATFUEL` code into `direct` (one BEA detail
  commodity), `group` (a BEA group needing a within-group split) and `residual`
  (Census could not place it); `--groups` splits the group tier onto commodities
  on 2017 Use shares and scores the prior against Census's own placements (72%
  land on the right commodity, against 47% for an economy-wide prior);
  `--vintage` reconciles the 2017/2022 NAICS revision onto one 365-industry
  basis carrying 100% of both years; `--annual` scores linear interpolation
  against the observed ASM/AIES path and rejects it — off by 28.8% in 2020 and
  wrong in sign for 2023; `--movement` scores the 2017 → 2022
  materials mix on the same index of dissimilarity as
  `intermediate_structure_drift.py`, both on the full frame and on the
  unsuppressed subsample, which is the one to quote; `--where` ranks industries
  by dollars reallocated; `--recovery` shows what filling the withheld cells
  changed; `--holdout` masks published cells and recovers them, which is the
  measurement behind the suppression prior and the error bar on every recovered
  cell. `--services` measures the named non-materials cells against BEA's own
  2017 Use rows — they disagree by factors of **0.40 to 8.01**, which is why
  `nonmaterial_seed()` carries an *index* rather than a level — and reports what
  that index does to the block, which is +23.8% by 2023 against a frozen 2017.
  Reads the `Census_EC_MatFuel`, `Census_EC_Expenses`, `Census_ASM_Expenses` and
  `Census_AIES_Expenses` FBAs; prints only.
- [`trade_data/`](trade_data/README.md) — Step 1d/4b source evaluation for the
  trade columns (#527): three 2017 probes scoring a Census goods + BEA services
  extract against the SUT targets — Use `F04000` for exports, Supply `MCIF` /
  `MADJ` / `MDTY` for imports — plus the options writeup behind the source
  decision.
- `tax_axis_conversion.py` — whether the Use table's `T00TOP`/`T00SUB` rows can
  be converted from the Supply table's commodity-side `TOP`/`SUB`/`MDTY` by the
  benchmark market-share matrix. They cannot: correlation 0.202 on `T00TOP`,
  because 55.7% of that row sits in trade industries and market shares place a
  product tax with the producer rather than the seller. Also scores the operator
  that does work — Step 4c's producer-level/trade-level split plus one named
  routing for motor fuel, plus the exclusion of the ten government industry codes
  BEA books no taxes on production to — corr 0.948, error 27.9%. Also answers how
  far trade industries have to be differentiated within wholesale and within
  retail. Construction is read separately and converts exactly (corr 1.000, error
  1.7%) because its Make block is 100% diagonal, and `error_concentration` shows
  the remaining error is 20 industries rather than 402, which is the case for
  building against the seed and repairing by name later. `--check` asserts the
  findings; Step 2 and Step 5 both lean on them.

- `other_taxes_allocation.py` — how `T00OTOP` should be allocated to the 402
  detail industries (#538). NIPA `T30500` puts **88.1%** of the row in recurrent
  property tax, so the intuitive allocator is the wrong one: industry output
  scores corr 0.590 with an absolute error of **92.3% of the row**, missing
  `531HSO` alone by 150,567. What works instead is that the row is
  concentrated — three real-estate codes carry 46.3% — and BEA publishes the big
  cells: `T70405` `B1031C` is the `531HSO`+`531HST` pair to the dollar and
  `T70305` `B1017C` the ten farm codes within 3, both holding across 2017-2024
  on the summary tables. The remainder rides frozen 2017 shares, graded out of
  sample on the held-out summary SUT at **1.9% composition drift against a 40.5%
  level move**. `--check` asserts all of it.

- `compensation_allocation.py` — the industry axis behind the other two Step 2
  rows (#538). For `V00100`: NIPA `T60200D`'s **69 leaves partition the 71 BEA
  summary industries exactly**, 63 of them equal a summary industry's published
  compensation to the dollar, so each is its own control. ⚠️ It also prices the
  plan's headline decision and finds against it — splitting wages (69 groups)
  from supplements (**16**) misplaces **0.95% of the row**, because NIPA
  publishes the two halves at coarser grain than the total. For `V00300`: eight
  controls across five tables, whose industry axes are mutually incompatible, so
  the build uses one distribution and says so. ⚠️ The obvious fix — a **value
  added by industry** series — now exists as
  [`BEA_GDPbyIndustry`](../../extract/bea/BEA_GDPbyIndustry.yaml) and is
  deliberately *not* used: all 71 summary industries' `V003` match its `TVA113`
  surplus rows to the dollar, so it is the summary Use SUT by another door and
  Decision 3 holds that in the test set. `--check` asserts all of it.

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
