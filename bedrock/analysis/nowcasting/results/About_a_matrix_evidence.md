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

## 2. What that is worth in `N`, and why it is negative

Across the 168 significant sectors on the model axis (`562000` is absent — waste
disaggregation splits it), with `B` held fixed:

- median |% difference| in `N`: **13.2%**
- p95: **56.9%**
- share of sectors moving more than 10%: **60.1%**

⚠️ **Two numbers circulate and they are different cuts.** `nowcast_vs_v03_hists.py`
reports a signed median of **−10.6%** over all 405 commodities with the *full*
model. The 13.2% above is the median *size* of the move on the significant
sectors with only `A` changing. Both are right; quote the population and the
`B` treatment with either.

### Where the −10.6% comes from

Swapping one side at a time. `N = 1ᵀ B L`, so holding `B` at v0.3 and swapping
only `L` isolates the input structure, and the reverse isolates emissions.

| what is allowed to move | median % change in `N` | excluding #850 columns |
|---|---:|---:|
| both `B` and `A` — the reported figure | **−10.64** | −9.03 |
| `A` only, `B` held at v0.3 | **−8.30** | −6.50 |
| `B` only, `A` held at v0.3 | −1.73 | |
| direct intensity `D` alone | −1.59 | |

**Direct emissions per dollar barely move.** So this is not a story about the
emissions inventory; it is about the multiplier — how much of the rest of the
economy a dollar of output drags behind it. Splitting the `A` effect further,
reading `N_c = (Σᵢ L_ic) × (N_c / Σᵢ L_ic)` as *how much output a dollar pulls*
times *how emission-intensive that output is*:

| step | v0.3 | nowcast | median change |
|---|---:|---:|---:|
| intermediate input per $ of output (`A` column sum) | 0.5033 | 0.4895 | −2.9% |
| total output pulled per $ (`L` column sum) | 2.006 | 1.934 | **−4.0%** |
| emission intensity of what is pulled, kg CO₂e/$ | 0.0930 | 0.0877 | **−4.1%** |

The last two compose: `0.960 × 0.959 − 1 = −7.9%`, the −8.3% `A` effect to
rounding. Add the −1.7% from the emissions side and you have −10.6%.

**In one sentence:** the nowcast says the supply chain behind a dollar of output
is both shorter and slightly cleaner than the 2017-scaled matrix assumed, and
because direct emissions are unchanged that shows up almost entirely in `N`.

⚠️ **Roughly 1.6 points of the −10.6% is issue #850, not economics.** Excluding
the 40 columns whose intermediate share has collapsed, the medians move to −9.0%
full and −6.5% for `A` alone. The figure will move when #850 is fixed.

Reproduce with `--decompose`. ⚠️ That flag is the one place the module derives the
nowcast's own `B`; everywhere else `B` is held fixed on purpose, and here the
point is to measure how much the emissions side contributes.

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

## 7. Named sectors: which inputs, from which survey, and where v0.3 disagrees

Sections 3 and 4 are aggregates. This section is the same claim at the level of a
single coefficient, for sectors chosen by two gates rather than by eye:

1. the sector's intermediate share of output must be plausible — this excludes the
   17 targets held back by issue **#850** (below);
2. at least **60%** of its input structure, weighted by contribution to `N`, must
   be observed rather than carried.

47 of 168 targets clear both. They are then ranked by how far `N` moved.

⚠️ **Ranking on `observed × |N move|` alone is wrong and was corrected.** It puts
`334111` electronic computers first at +180%, but only 24% of its inputs are
observed and its three largest contributors are *carried* cells whose 2017
coefficients were near zero — a +5,064% relative move on a rounding error, not a
measurement. `MIN_OBSERVED_INPUT_SHARE` gates on evidence first.

### The clearest cases

**What the columns are.** `A` is the direct-requirements coefficient — dollars of
that input per dollar of the sector's output. It is the number the two methods
disagree about, and it is **not** a footprint. The partial `N` is `N_i x A_ic`,
in kg CO2e per dollar, and `share` is that divided by `N_c − D_c`. Those products
sum across all 402 inputs to the sector's whole indirect footprint exactly — for
`313100`, 0.6707 kg CO2e per dollar, against `N` 0.7005 and direct `D` 0.0299.

⚠️ The two `A` columns are a like-for-like comparison of the input recipe. `N_i`
and the product are the **nowcast's**, because `B` is held fixed and only `A` is
under comparison; v0.3's own `N_i` vector differs and is not mixed in here.

`fan-out 1` means the survey datum *is* that cell.

**`313100` Fiber, yarn, and thread mills — `N` +30.0%, 94% of inputs observed,
94% primary.** The most completely observed column in the significant set. Two
inputs carry 84% of the entire indirect footprint, and both are single-cell
Economic Census observations:

| input | | `A` nowcast | `A` v0.3 | `N` of input | carried into `N` | share | source |
|---|---|---:|---:|---:|---:|---:|---|
| `3252A0` | synthetic fibers and filaments | 0.372 | 0.230 | 0.782 | **0.291** | 43.4% | Econ Census 2022, fan-out 1 |
| `111900` | other crop farming (cotton) | 0.146 | 0.089 | 1.894 | **0.276** | 41.1% | Econ Census 2022, fan-out 1 |
| `221100` | electric power | 0.013 | 0.025 | 2.565 | **0.032** | 4.8% | AIES, fan-out 1 |

Electric power is why the coefficient alone is not the story: at 0.013 it is a
thirtieth the size of the fiber coefficient, but a dollar of electricity carries
2.565 kg CO2e against synthetic fiber's 0.782, so it still contributes 4.8%.

**`332310` Plate work and fabricated structural product manufacturing — `N` +23.6%,
90% observed, 85% primary.** One cell carries 74% of the indirect footprint:
iron and steel at 0.342 against v0.3's 0.245, a single Economic Census
observation of how much steel a structural-metal plant buys per dollar of output.

**`315000` Apparel manufacturing — `N` +28.2%, 75% observed, 74% primary.** Fabric
mills carry 48% of the indirect footprint, at 0.130 against v0.3's 0.099.

**`335311` Power, distribution, and specialty transformer manufacturing — `N`
+24.7%, 86% observed.** Copper rolling and drawing carries 26% at 0.130 against
v0.3's 0.093 — the input whose intensity matters most for grid equipment.

### Where v0.3 disagrees, and how

v0.3 is not frozen — section 1 shows it moves cells, just not by more than the
pipeline's own noise. The sharper question is whether it moves them the *right
way*. Taking the same seven high-contribution cells and comparing both models to
the published 2017 benchmark they both start from:

| input → sector | 2017 | v0.3 vs 2017 | Economic Census vs 2017 | agree? |
|---|---:|---:|---:|:--:|
| steel → plate work `332310` | 0.275 | **−11.0%** | **+24.3%** | no |
| steel → hand tools `332200` | 0.123 | **+18.7%** | **−9.4%** | no |
| steel → boilers `332410` | 0.100 | −7.7% | −33.4% | yes |
| synthetic fibers → yarn mills `313100` | 0.297 | **−22.7%** | **+25.1%** | no |
| cotton → yarn mills `313100` | 0.213 | −58.0% | −31.5% | yes |
| fabric → apparel `315000` | 0.113 | **−12.8%** | **+14.7%** | no |
| copper → transformers `335311` | 0.106 | **−12.3%** | **+22.3%** | no |

**In five of seven, the two methods move the coefficient in opposite directions.**

The steel rows are the clearest illustration, because `332310`, `332200` and
`332410` all sit in the same BEA summary block. v0.3 sends the first down and the
second up; the 2022 Economic Census says the opposite in both cases. That is not a
claim that v0.3 cannot differentiate within a block — it demonstrably does, its
factors for these two cells differ by 0.30 — but its differentiation comes from
prices and aggregate scaling, not from anyone measuring what a structural-steel
fabricator or a hand-tool plant actually bought. Where the two methods disagree,
only one of them has an observation behind it.

---

## ⚠️ Correction to sections 2 and 3: the negative tail is contaminated

Filing **#850** came out of this sector work and changes how section 2's
distribution should be read.

The interior fit sets each industry's intermediate-input total to
**gross output less value added**. That control drifts toward zero across the span
for a growing set of industries and passes through it by 2024: six industries have
a *negative* intermediate-input target, meaning value added exceeds gross output,
which cannot happen. Economy-wide at 2024, 25 industries sit below a 15%
intermediate share and they carry **8.2% of all output**. The count of negative
targets runs 0, 0, 1, 5, 4, 2, 5, 6 across 2017 to 2024 — it starts clean at the
benchmark anchor and worsens every year.

**Several of the largest apparent wins in section 2 are this defect, not better
data**: `334413` semiconductors (−54%), `334210` (−72%), `339112` (−61%),
`511110` (−73%), `334515` (−53%). `334413`'s intermediate share is 0.002 against
0.242 in the 2017 benchmark — its input column is effectively empty.

The 13.2% median in section 2 is not materially affected, since 17 of 168 targets
are involved and they sit in the tail. But no individual sector from that list
should be quoted as evidence of anything until #850 is resolved, and
`sector_ranking` marks them `suspect` so they cannot be picked up by accident.

---

## Reproducing

```
uv run python -m bedrock.analysis.nowcasting.results.a_influence --check
uv run python -m bedrock.analysis.nowcasting.results.a_evidence_2024 \
    --ladder --sectors --decompose
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
- Sector narratives exclude the 17 targets held back by #850; see the correction above.
- One span only. No claim here is a check against an out-of-sample answer key;
  §1 is a decomposition of what each method *can* know, not a scoring of which
  is closer to truth.
