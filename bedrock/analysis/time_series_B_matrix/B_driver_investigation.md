# B driver investigation tracker

A running list of the departures the diagnostics surface, what each one is
worth, and whether it turns out to be justified. This is the working document
for the plan's "determine why the drivers change and how to fix them" step.

Measurements come from `B_change_diagnostics.py`; the standing results are in
[`About_B_change_drivers.md`](About_B_change_drivers.md). Figures quoted here
are the run of 2026-09-16 on FBS `v0.3.0_796a6ca` and MUT `v0.3.0_4276083`.

## How to use it

**Justified?** is the field that takes real work. It cannot be answered from
the diagnostics — they say *that* something departed and by how much, never
whether it should have. Answering it means going back to the source and the
assumptions behind it. Until that is done the verdict is `unresolved`, and an
`unresolved` driver is not a candidate for smoothing.

| verdict | meaning |
|---|---|
| **justified** | a real movement in the world. Do not smooth; a smoothing change here would destroy signal. |
| **not justified** | movement with nothing underneath it. A defect, in scope, fix it. |
| **out of scope** | real driver, but this project has no lever on it. Hand over with the evidence. |
| **unresolved** | not yet investigated. Needs the source-level work before it can be ranked. |
| **split** | only for an umbrella row whose parts landed differently. State the share each way, or the row says nothing. |

**Diagnostic** is the output that pointed us at the row, and the one to open to
confirm it or to re-check it after a fix. The key beneath the tracker says what
to open for each `D#`; the full register — the question each asks, the method,
every file it writes — is in
[`B_matrix_smoothing_plan.md`](B_matrix_smoothing_plan.md#methods-which-diagnostic-answers-which-approach).
Cite the diagnostic that *surfaced* the driver, not every table the row could
be checked against. A row with no diagnostic behind it came from somewhere else
— a code or source review — and says so; that is allowed, but it means the
diagnostics cannot yet see the driver, which is itself worth recording.

**Issue** is empty only where nothing is owed. Rows 6 and 7 were resolved in
the change that created this tracker; rows 8 and 9 are `justified` verdicts,
and a justified driver needs no issue because the correct action is to leave
it alone. ⚠️ An `unresolved` row with no issue is a gap — every one of them
now has an issue, and a new row should get one before the verdict is written.

**Potency** is whichever measure fits the driver. Top-down drivers are in Mt
CO2e of gross divergence over the span; bottom-up ones in `delta_B_pct_of_N`.
They are not comparable across rows and should not be summed — read potency
within a class of driver, not across.

⚠️ **Oscillation is the single most useful screen.** A large driver that
oscillates near 1 moves a great deal and arrives where it started, which is the
signature of movement with nothing underneath it. A large driver that
oscillates near 0 is on a trend and is usually real. Neither is proof, but it
is the right place to start an investigation.

---

## Tracker

| # | Driver | Issue | Diagnostic | Effect | Potency | Justified? | Status |
|---|---|---|---|---|---|---|---|
| 1 | Oil and gas extraction `211000` industrial fuel — **both cells run on the Use table** | [#923](https://github.com/cornerstone-data/bedrock/issues/923) · cause [#933](https://github.com/cornerstone-data/bedrock/issues/933) | D9 · D8 | Two cells move 228 Mt across the span and arrive 13 Mt from where they started. Largest pair of rocky cells in the panel. The cell labelled MECS is not a MECS vector: `petroleum_industrial` is BEA Use purchases of `324110` rescaled by a MECS fuel share that is constant within a survey vintage, so both cells move with the Use table and that is why they move together. | 121.8 + 106.6 Mt gross; oscillation 0.97 and 0.91 | **not justified** — three external checks agree: GHGRP subpart C for these facilities holds a 1.08x span while ours spans 12.8x, the allocation drops **below** that measured floor in 2017 and 2020, and the driving coefficient moves at the 98th percentile of its own Use row | **Reassess after [#929](https://github.com/cornerstone-data/bedrock/issues/929) lands.** The facility basis replaces this vector outright — `211000` is in scope and the facility evidence marks it **down 4.6pp** of share. ⚠️ #918 was never the lever and is being retired; the cause is row 12 ([#933](https://github.com/cornerstone-data/bedrock/issues/933)) and the fix is [#923](https://github.com/cornerstone-data/bedrock/issues/923) |
| 2 | Petroleum refineries `324110` industrial fuel — Use table, MECS label | [#923](https://github.com/cornerstone-data/bedrock/issues/923) · cause [#933](https://github.com/cornerstone-data/bedrock/issues/933) | D9 · D8 | Third-largest rocky cell, and the same route as #1 now that the route is identified: `petroleum_industrial` weights are BEA Use money, not MECS quantities. Same underlying cause confirmed rather than suspected. | 79.4 Mt gross, net −4.6; oscillation 0.94 | **not justified** — GHGRP alone, one source that cannot double count itself, reports **176.4 Mt against the 86.9 Mt** this method assigns, a 2.03x disagreement on the same facilities | **Reassess after [#929](https://github.com/cornerstone-data/bedrock/issues/929) lands**, with #1 — one Use-table cause, not two. ⚠️ The verdict here rests on a **level** disagreement, not on a movement study like #1's; the 79.4 Mt of churn is attributed to the shared mechanism rather than separately measured |
| 3 | Grain farming `1111B0` soils via `EPA_GHGI_soils` | [#934](https://github.com/cornerstone-data/bedrock/issues/934) | D9 · D8 | The largest non-fuel rocky cell. Soils attribution moved 71.8 Mt and settled 10.6 Mt lower. | 71.8 Mt gross; oscillation 0.85 | **unresolved** | Filed as [#934](https://github.com/cornerstone-data/bedrock/issues/934) |
| 4 | Air transportation `481000` jet fuel, `Direct` | [#935](https://github.com/cornerstone-data/bedrock/issues/935) | D9 · D8 | 99.5 Mt of movement, a quarter of it net. Mixed trend and churn; the only large `Direct` cell that is not electric power. | 99.5 Mt gross, net −24.7; oscillation 0.75 | **unresolved** | Filed as [#935](https://github.com/cornerstone-data/bedrock/issues/935). Check the denominator too — `481000` output collapsed in 2020 as well |
| 5 | Negative emission factors on `114000` and `327910` | [#912](https://github.com/cornerstone-data/bedrock/issues/912) | D6a · D7 | Fossil-combustion rows go below zero. Makes both commodities unrankable on any percentage metric; `114000` otherwise takes 6 of the top 30 slots including first. | `114000` swings +379% on a base that crossed zero | **not justified** | Filed, Priority High |
| 6 | EPA attribution table renumbering | — | D2b vs D2a | Soils `T_5_17`→`T_5_18`→`T_5_19` and non-energy use `T_3_25b`→`T_3_25` mid-span, so `T_5_18` means *direct* in some years and *indirect* in others. 2019 booked −297 Mt against +255 Mt on flat emissions. | ±300 Mt of pure artefact in 2019, ±90 Mt in 2023 | **not justified** | **Fixed** in this PR via `ATTRIBUTION_ROLE_ALIAS` |
| 7 | Prices in a nominal denominator | — | D5 · D6a | `x` is nominal on the nowcast path. 74% and 73% of the 2021 and 2022 E-versus-x gap is the deflator, and 56% of the span's nominal output growth. | 24% price wedge over the span | **justified** — inflation is real, but must be controlled for | **Handled**: every divergence table has a `*_real` companion |
| 8 | Electric power `221100` own direct emissions | — | D9 · D8 | The largest single cell, 609 Mt. Direct intensity falls 3.82 → 3.06 kg CO2e per constant-2017 dollar. Two of seven years reverse: 2021 emissions +7.0% on flat real output, 2024 output −1.8% on flat emissions. | 609 Mt gross, net −355; oscillation 0.42 | **justified** — real decarbonisation | **Do not smooth.** Closed except for the 2021 reversal, which [#936](https://github.com/cornerstone-data/bedrock/issues/936) checks against water transportation — if both are one fuel-price event they close together |
| 9 | COVID shock and rebound, 2020-21 | — | D6a · D3 | Every attribution class dips in 2020 and rebounds in 2021. Puts 14 of the bottom-up top 30 in 2021 alone. | median `abs_delta_B_pct_of_N` 1.44% in 2021 against 0.33% in 2018 | **justified** — a real event | **Do not smooth.** Argues for ranking within year as well as pooled |
| 10 | `L` / the `A` matrix | [#937](https://github.com/cornerstone-data/bedrock/issues/937) | D6b | Moves `N` two to four times as much as the emission factors do — 14.9 of the 18.2 percentage points of median `N` movement in 2021. | median `pct_change_N_L_effect` 3.1% to 14.9% a year | **out of scope** | Handed over as [#937](https://github.com/cornerstone-data/bedrock/issues/937), on Nowcasting Phase 2 |
| 11 | `Vnorm` / the nowcast Make | [#938](https://github.com/cornerstone-data/bedrock/issues/938) | D3 · D13 | 3% to 6% of gross `B` movement through 2022, then 10.7% and 13.0%. Cumulative displacement flattens after 2022 at ~25 Mt while annual churn rises. | output-weighted share of gross `B` movement, to 13.0% | **out of scope** for remediation | Handed over as [#938](https://github.com/cornerstone-data/bedrock/issues/938), on Nowcasting Phase 2. The 2023 step-up is unexplained |
| 12 | Use-table attribution over-tracks output | [#933](https://github.com/cornerstone-data/bedrock/issues/933) | D4 · D9 · D10 | Emissions spread by a row of `U` have an output elasticity of **1.51** — they amplify output rather than follow it, because they ride a sector's fuel *purchases* while `x` is a row sum of `V`. Affects 30% of `E`. | slope 1.51; 348 Mt of the rocky mass routes this way by label, ~716 Mt of table 3-11 in fact | **unresolved** | Structural, and **larger than the route labels show** — see row 15. Promoted from possible to likely common cause behind rows 1 and 2, and the reason [#933](https://github.com/cornerstone-data/bedrock/issues/933) outranks #918 for them |
| 13 | Water transportation `483000` | [#936](https://github.com/cornerstone-data/bedrock/issues/936) | D6a | The largest single bottom-up movement in the panel: +87.6% on the `N`-weighted metric in 2021, reversing −25.8% in 2022. Own-share 0.79 to 0.89, so it passes almost undamped to buyers. | `delta_B_pct_of_N` +87.6% (2021) | **unresolved** | Filed as [#936](https://github.com/cornerstone-data/bedrock/issues/936). Check against the 2021 reversal in row 8 — may be the same fuel-price event |
| 14 | Electricity source switch, UMD → eGRID (#911) | [#914](https://github.com/cornerstone-data/bedrock/issues/914), [#913](https://github.com/cornerstone-data/bedrock/issues/913) | — ([#911](https://github.com/cornerstone-data/bedrock/pull/911) review) | #911 moves electricity to plant-level eGRID for 2018-2024 only. No eGRID 2017 exists, so the base year keeps a different source and the switch lands on the 2017/2018 boundary that every indexed series is anchored on. eGRID also includes Puerto Rico, ~12 Mt/yr, where the GHG inventory does not. | boundary error 0.8% of electricity emissions; discontinuity sits on the largest cell in the panel, 609 Mt gross | **not justified** — a source switch is not a movement | Pending #911. Row 8 must be re-derived on the eGRID series once it lands |
| 15 | EPA table 3-11 fossil fuel combustion, as a family | [#923](https://github.com/cornerstone-data/bedrock/issues/923) · [#918](https://github.com/cornerstone-data/bedrock/issues/918) | D9 · D8 · D2a | The largest driver family in the panel and the umbrella over rows 1 and 2, which name its three biggest cells. 1,009 Mt of gross movement across 567 cells, netting −45 Mt, ⚠️ **The route labels mislead.** By label it is 706 Mt MECS and 303 Mt Use; by provenance 412 Mt of that MECS mass is `petroleum_industrial`, whose weights are BEA Use money rescaled by a MECS fuel share, so **~716 Mt of the 1,009 Mt — 71% — runs on the Use table** and only ~293 Mt on MECS quantities. The MECS vintage steps from the 2018 survey to the 2022 survey between nowcast 2020 and 2021, and 2021 carries 2.5x the sector reallocation of a within-vintage year against 1.6x on the non-MECS control. | 1,009 Mt gross, 30.5% of all gross movement in the panel; ~10pp of 2021 reallocation above the control | **split** — the Use-table 71% is **not justified** via rows 1 and 2; the MECS 29% stays **unresolved**, its cutoff measured but its emissions not | ⚠️ Umbrella: do not sum its potency with rows 1 and 2. Both halves resolve the same way — [#929](https://github.com/cornerstone-data/bedrock/issues/929) replaces the Use-table part outright, and manufacturing combustion is **100%** inside the facility scope, so [#918](https://github.com/cornerstone-data/bedrock/issues/918), [#920](https://github.com/cornerstone-data/bedrock/issues/920) and [#921](https://github.com/cornerstone-data/bedrock/issues/921) all retire with it. Evidence in [`About_B_change_drivers.md`](About_B_change_drivers.md) §4 mechanism 5 |
| 16 | Oil and gas extraction's petroleum input coefficient | [#922](https://github.com/cornerstone-data/bedrock/issues/922) | D9 · D8 · D14 | The mechanism under row 1. `211`'s purchases of refined petroleum per $100 of its own gross output go 0.85 → 2.64 → 1.69 over the span — **3.11x to 2022, then −29%** — against a median buyer move of 0.91x and a 90th percentile of 1.53x on the same row. The largest single jump, +71%, is 2017→2018, the first year off the published benchmark. Both sides are nominal and every buyer purchases the same commodity, so a petroleum price move cannot produce it. | 98th percentile of coefficient movement; drives the 106.6 Mt cell in row 1 | **not justified** — GHGRP subpart C for these facilities holds 44.9-48.4 Mt across 2017-2023, a 1.08x span, while our allocation spans 12.8x and drops **below the physical floor** in 2017 and 2020 | On D14 it is the worst intermittent sector in the panel: ratio spread 13.0x against 2.3x for the next worst. Two separable defects — a **level** bias (purchases miss self-supplied lease fuel) and the **volatility**. `32551` moves 17.69x on the coefficient and likely shares a cause |
| 17 | Byproduct fuel has no purchase to be allocated by | [#923](https://github.com/cornerstone-data/bedrock/issues/923) | D15 | Refinery still gas, coke oven gas and blast furnace gas are burned by the facility that made them, so they never appear as a purchase and **no row of the Use table can carry them**. Today they ride the same purchased-fuel vector as everything else, which is a category error rather than an inaccuracy. Measured at 37.0 Mt in 2022, 22.4 Mt of it petroleum refineries. | 37.0 Mt, 5.5% of facility-reported combustion; the direct cause of the 19 sectors D14 puts in `boundary_offset` | **not justified** — the vector cannot represent the flow | Needs its own category before a facility basis lands. The figure is a **floor**: GHGRP does not name the fuel, so it is imputed from matched NEI records |

### Key to the Diagnostic column

What to open to confirm a row, and what to re-run after a fix. One line each;
the full definitions — the question, the method and every file — are in the
register in
[`B_matrix_smoothing_plan.md`](B_matrix_smoothing_plan.md#methods-which-diagnostic-answers-which-approach).

| ID | Open | To see |
|---|---|---|
| **D2a** | `divergence_by_attribution_real.csv`, `divergence_by_metasource_real.csv` | which source moved CO2e away from output, keyed on the attribution *role* |
| **D2b** | `divergence_by_attribution_raw.csv` | the same keyed on EPA's raw table numbers. A split here that D2a does not show is a relabelling, not a movement |
| **D3** | `E_vs_x_indexed.png` | E by attribution class against x, indexed, with the Make's cumulative reallocation on the right axis |
| **D4** | `output_elasticity_real.csv` | whether a source tracked output (slope 1), ignored it (0) or amplified it (>1) |
| **D5** | `price_effect.csv` | how much of the year's E-versus-x gap is the deflator rather than emissions |
| **D6a** | `B_change_real.csv`, sorted on `abs_delta_B_pct_of_N` | which commodity factors moved most, weighted by the share of their own `N` they drive. **Also the acceptance measure** |
| **D6b** | `B_change_real.csv`, `pct_change_N_L_held` / `pct_change_N_L_effect` | how much of a commodity's `N` move is its factors and how much is `L` |
| **D7** | `B_by_attribution.csv` | which attribution route builds one commodity's factor, and what each contributes |
| **D8** | `divergence_by_sector_stratum.csv` | one (sector × source) cell year by year — the level behind a span figure |
| **D9** | `sector_stratum_span.csv`, `sector_stratum_divergence.png` | gross movement and `oscillation` per cell: the rocky-cell ranking rows 1-4 and 8 come from |
| **D10** | `io_derived_share.csv` | how much of `E` has its sector split derived from the IO tables at all |
| **D13** | ⚠️ nothing yet | the Make's share of gross `B` movement, year on year. Row 11's potency is quoted from an ad-hoc calculation; it cannot be re-run until the function exists |

⚠️ **After a fix, run the module whole and read D6a as well as the row's own
diagnostic.** It writes every table in one pass, so there is no cost to
re-reading the cited ID; what a single-row re-check cannot see is movement
displaced onto other commodities, which only the acceptance measures in
[`B_matrix_smoothing_plan.md`](B_matrix_smoothing_plan.md) will show. D1 runs on
every invocation and raises if the decomposition stops closing — a fix that
breaks it invalidates every other number in this file.

---

## What the rocky mass routes through

From D9, `sector_stratum_span.csv`. Of the 63 cells carrying more than 10 Mt
of gross movement, 33 oscillate above 0.75. Grouped by the vector that spread
them:

| attribution route | rocky mass | cells |
|---|---:|---:|
| `Nowcast_Detail_Use_AfterRedef` | 348 Mt | 8 |
| `Direct` | 302 Mt | 8 |
| `Energy_manufacturing_national_nowcast` | 292 Mt | 9 |
| `EPA_GHGI_soils` | 141 Mt | 5 |
| `EPA_GHGI_NEU` | 74 Mt | 3 |

Two of the three largest routes are attribution vectors rather than inventory
rows, which is why row 12 is on the list as a candidate common cause rather
than as one more individual cell.

⚠️ `Direct` appearing here is not a contradiction of row 8. Electric power is
not in this group — its oscillation is 0.42. The 302 Mt is eight *other*
`Direct` cells, led by air transportation and coal mining.

---

## Closing a row

A row leaves `unresolved` only with a stated reason and a source-level check
behind it, not on plausibility. Record the check in
[`About_B_change_drivers.md`](About_B_change_drivers.md) and link it here, and
add the diagnostic that carried the check to the row's **Diagnostic** cell —
the ID that *closed* a row is usually not the one that opened it. When
a driver is fixed, re-run the diagnostics and record the before-and-after on
the two acceptance measures in the plan — median `abs_delta_B_pct_of_N` and the
share over the gate — so an improvement bought by displacing movement onto
other commodities is visible rather than netted away.
