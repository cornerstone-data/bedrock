# Is the nowcast `A` matrix tied to primary data?

Findings from `a_influence.py` and `a_evidence_2024.py`, run 2026-09-04 against
nowcast MUT vintage `v0.3.0_c71ce5f` and snapshot `v0_3_1`
(`00524c3c8ba122a7a5b7f2139ff7ea6de08947bb`).

Both models are stamped 2024 and reach `A` two different ways:

| | `2025_usa_cornerstone_v0_3` | `..._v0_4_nowcast_2024` |
|---|---|---|
| detail IO source | published BEA 2017 benchmark | `nowcast` |
| base IO year | 2017 | 2024 |
| `apply_io_year_adjustments` | `True` — summary-block scaling + commodity price index | `False` |

**`B` is held fixed at the v0.3 snapshot throughout.** The two models also differ
in GHG attribution and in `x`; letting `B` move would mix the emissions side into
a measurement of the economics. Every number below is the `A` method alone, and
no dollar-year rebase is needed because one `B` serves both.

---

## 1. v0.3's detail cells carry no 2024 observation — measured, not asserted

Fit `A_2024 = A_2017 × (summary block factor) × (commodity price factor)` against
the **published** BEA 2017 detail `A`, weighted by 2017 cell size. That model is
what summary-ratio scaling followed by a price index produces; whatever it cannot
explain is cell-level information the 2017 benchmark did not already contain.

| | rms of log ratio | residual after the fit | explained |
|---|---:|---:|---:|
| v0.3 2024 | 0.258 | **0.086** | 88.9% |
| nowcast 2024 | 0.731 | **0.523** | 48.7% |
| *nowcast 2017 (control)* | 0.141 | *0.080* | 67.5% |

⚠️ **The control is what makes this readable.** The pipeline does not reproduce
the 2017 benchmark exactly — its own 2017 leaves a residual of 0.080, which is
the noise floor. v0.3's 2024 residual is **1.1× that floor**: statistically it
holds no cell-level 2024 information. The nowcast's is **6.5× the floor**.

The largest nowcast-2017-vs-published gaps are the motor-vehicle rows (`336111`,
`33641A`, `336991`, `336412`, `336500`) — the known support-infeasible block the
interior fit relaxes every year, tracked on #767.

## 2. What that is worth in `N`

Across the 168 significant sectors on the model axis (`562000` is absent — waste
disaggregation splits it), with `B` held fixed:

- median |% difference| in `N`: **13.2%**
- p95: **56.9%**
- share of sectors moving more than 10%: **60.1%**

## 3. Which `A` cells those `N` values rest on

`a_influence.py` carries three readings, two exact and one a ranking:

- `compute_output_contribution(L, D)` — `D_i L_ic`, sums to `N_c` exactly.
- `compute_input_contribution(A, N)` — `N_i A_ic`, sums to `N_c − D_c` exactly.
- `a_cell_leverage` — `A_ij N_i L_jc`, from `∂N_c/∂A_ij = N_i L_jc`. Reaches
  every cell in the table. ⚠️ **A ranking, not a partition**: it sums to
  `Σ_j (N_j − D_j) L_jc`, because a cell on a long path is counted at every step
  it participates in.

Leverage is not concentrated: the top 50 cells carry 21.2% of it and the top 200
carry 38.4%. The ranking is economically legible — the top cells are oil and gas
into refineries, electricity into real estate, petrochemicals into plastics
resin, grain into organic chemicals (corn ethanol), cattle into meat processing.

## 4. Where those cells come from

Joining the leverage to `seed_coverage.pedigree_cells(2024)`:

| | share of leverage |
|---|---:|
| primary — the datum *is* the cell (`n = 1`) | **23.0%** |
| allocated — the datum is shared across cells | **21.9%** |
| carried — 2017 structure, no annual source | 55.2% |

**44.8% of the leverage carries an observation made since 2017**, against 29.4%
of the Use table's dollars unweighted — the cells that matter most for these
sectors' footprints are better observed than the table average. By source:
Economic Census 2022 (held) 32.1%, AIES 9.6%, USDA ERS 1.7%, Economic Census
recovered 0.9%, EIA-923 0.6%.

The corresponding figure for v0.3 is **0%**: there is no annual detail source in
that path at all, which §1 measures rather than assumes.

## 5. The balancing does not overwrite the primary data

For the top 40 cells by leverage, tracing seed → interior fit → RAS → producer
prices → after redefinitions:

| step | share of the total seed→model move | median cell moves |
|---|---:|---:|
| seed → interior fit (observed output control) | 53% | 12.6% |
| interior fit → RAS (balancing) | **4%** | **0.6%** |
| RAS → producer prices (valuation restatement) | 40% | 12.9% |
| producer prices → after redefinitions | 2% | 0.0% |

Balancing and redefinitions together move these cells by about 6%. The rest is
an observed control (`GO − VAPRO`) and a purchaser→producer price restatement,
which revises nothing about the transaction.

⚠️ The stage artifacts must be chained through each file's `_metadata.json`
sidecar: the balanced SUT for 2024 is vintage `31f4712` while the MUT built on it
is `c71ce5f`, so a filename guess silently reads a different run.

## 6. Imports — a real but modest gain, and partly circular

Comparing each model's import **composition** (each commodity's share of all
imports in `A`) against `Trade_Imports_2024`:

| | Spearman | share of imports on the wrong commodity |
|---|---:|---:|
| 2024 nowcast | 0.749 | **34.1%** |
| Cornerstone v0.3 | 0.654 | 38.7% |

Leverage-weighted import share of `A`: nowcast 12.3%, v0.3 13.8%.

⚠️ **Two things to say out loud.** First, the nowcast's import matrix is
conditioned on this same Census/IEA series, so its fit is partly built in; the
asymmetry is the point, since v0.3 carries the 2017 import matrix forward and is
free to disagree with 2024 trade. Second, both misplacement figures are inflated
by a scope difference — the trade series includes imports going straight to final
demand while `Aimp` is intermediate only — so only the **gap between the two
models** is meaningful, not the level.

---

## Reproducing

```
uv run python -m bedrock.analysis.nowcasting.results.a_influence --check
uv run python -m bedrock.analysis.nowcasting.results.a_evidence_2024 --ladder
```

`--ladder` rebuilds the Step 3 seed and the interior fit (~5 minutes); everything
else runs in under a minute from local artifacts plus the v0.3 snapshot. The
`--check` flag asserts four identities, all of which hold at float64 noise.

## Known limits

- `a_cell_leverage` is a leverage ranking, not a partition of `N_c`.
- The Use-axis restatement of the leverage carries 0.872 of the commodity-axis
  mass; the gap is what the Cornerstone correspondence drops on entry (scrap,
  noncomparable imports, `S00900`) and the waste disaggregation.
- The 2024 Step 3 expense panel is carried from 2023 and marked `held` — 2024's
  AIES dropped four variables and combined the two rent lines. Those cells are
  graded `carried`, not as 2024 observations.
- One span only. No claim here is a check against an out-of-sample answer key;
  §1 is a decomposition of what each method *can* know, not a scoring of which
  is closer to truth.
