# B driver investigation tracker

A running list of the departures the diagnostics surface, what each one is
worth, and whether it turns out to be justified. This is the working document
for the plan's "determine why the drivers change and how to fix them" step.

Measurements come from `B_change_diagnostics.py`; the standing results are in
[`About_B_change_drivers.md`](About_B_change_drivers.md). Figures quoted here
are the run of 2026-09-21 on FBS `v0.3.0_99655e9` and MUT `v0.3.0_4276083`.
⚠️ Every `N`-derived figure was restated on 2026-09-21 when `N_total` stopped
mixing a real `B` with a nominal `L` ([#957](https://github.com/cornerstone-data/bedrock/issues/957));
`B`-only potencies and every Mt figure are untouched.
⚠️ Every figure was restated on 2026-09-18: the prior run resolved to a
local-only FBS build that no ref reaches. See the vintage section of
[`About_B_change_drivers.md`](About_B_change_drivers.md).

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
| 1 | Oil and gas extraction `211000` industrial fuel — **both cells run on the Use table** | [#923](https://github.com/cornerstone-data/bedrock/issues/923) · cause [#933](https://github.com/cornerstone-data/bedrock/issues/933) | D9 · D8 | Two cells move 115 Mt across the span and arrive 6 Mt from where they started. The largest pair of rocky cells in the panel, and the contrast with this sector's own `Direct` cells — 79.0 and 62.9 Mt gross at oscillation 0.11 and 0.17 — is the cleanest statement in the panel of what attribution churn costs. The cell labelled MECS is not a MECS vector: `petroleum_industrial` is BEA Use purchases of `324110` rescaled by a MECS fuel share that is constant within a survey vintage, so both cells move with the Use table and that is why they move together. | 64.0 + 51.3 Mt gross; oscillation 0.98 and 0.87 | **not justified** — three external checks agree: GHGRP subpart C for these facilities holds a 1.08x span while ours spans 3.05x, the allocation drops **below** that measured floor in 2017 and 2020, and the driving coefficient moves at the 98th percentile of its own Use row | **Reassess after [#929](https://github.com/cornerstone-data/bedrock/issues/929) lands.** The facility basis replaces this vector outright — `211000` is in scope. ⚠️ The claim that the facility evidence marks it **down 2.6pp** of share is **withdrawn**: on the published build `share_shift_pp` is **+1.81pp**, so the basis would give the sector more share, not less. The three checks in this row stand — they compare against subpart C, which is combustion only, while the share shift compares against the full facility union including process units. ⚠️ #918 was never the lever and is being retired; the cause is row 12 ([#933](https://github.com/cornerstone-data/bedrock/issues/933)) and the fix is [#923](https://github.com/cornerstone-data/bedrock/issues/923) |
| 2 | Petroleum refineries `324110` industrial fuel — Use table, MECS label | [#923](https://github.com/cornerstone-data/bedrock/issues/923) · cause [#933](https://github.com/cornerstone-data/bedrock/issues/933) | D9 · D8 | Same route as #1 now that the route is identified: `petroleum_industrial` weights are BEA Use money, not MECS quantities. Same underlying cause confirmed rather than suspected. | 35.3 Mt gross, net −8.8; oscillation 0.75 | **not justified** — GHGRP alone, one source that cannot double count itself, reports **176.4 Mt against the 76.1 Mt** this method assigns, a 2.32x disagreement on the same facilities | **Reassess after [#929](https://github.com/cornerstone-data/bedrock/issues/929) lands**, with #1 — one Use-table cause, not two. ⚠️ The verdict here rests on a **level** disagreement, not on a movement study like #1's; the 35.3 Mt of churn is attributed to the shared mechanism rather than separately measured |
| 3 | Grain farming `1111B0` soils via `EPA_GHGI_soils` | [#934](https://github.com/cornerstone-data/bedrock/issues/934) | D9 · D8 | The largest non-fuel rocky cell. Soils attribution moved 88.1 Mt and settled 9.5 Mt lower. | 88.1 Mt gross; oscillation 0.89 | **unresolved** | Filed as [#934](https://github.com/cornerstone-data/bedrock/issues/934) |
| 4 | Air transportation `481000` jet fuel, `Direct` | [#935](https://github.com/cornerstone-data/bedrock/issues/935) | D9 · D8 | 99.5 Mt of movement, a quarter of it net. Mixed trend and churn; the only large `Direct` cell that is not electric power. | 99.5 Mt gross, net −24.7; oscillation 0.75 | **unresolved** | Filed as [#935](https://github.com/cornerstone-data/bedrock/issues/935). Check the denominator too — `481000` output collapsed in 2020 as well |
| 5 | ~~Negative emission factors on `114000` and `327910`~~ — **a build-vintage artefact** | [#912](https://github.com/cornerstone-data/bedrock/issues/912) | D6a · D7 | Does not reproduce on the published FBS `v0.3.0_99655e9`, where **no commodity is negative in any year**. The negatives came from `v0.3.0_796a6ca`, a local-only build off a branch no ref reaches, which the resolver picked because it ranked span-covering vintages on **file mtime**. The real defect was the resolver, now fixed to rank on provenance and to refuse to guess. | none — the finding is withdrawn. `114000` is a genuine mover on the published build, second in the panel; `327910` is unremarkable at rank 655 of 2,835 | **n/a — withdrawn** | **Resolved.** Close #912. Lesson recorded in [`About_B_change_drivers.md`](About_B_change_drivers.md) |
| 6 | EPA attribution table renumbering | — | D2b vs D2a | Soils `T_5_17`→`T_5_18`→`T_5_19` and non-energy use `T_3_25b`→`T_3_25` mid-span, so `T_5_18` means *direct* in some years and *indirect* in others. 2019 booked −301 Mt against +255 Mt on flat emissions. | ±300 Mt of pure artefact in 2019, ±85 Mt in 2023 | **not justified** | **Fixed** in this PR via `ATTRIBUTION_ROLE_ALIAS` |
| 7 | Prices in a nominal denominator | — | D5 · D6a | `x` is nominal on the nowcast path. 74% of both the 2021 and 2022 E-versus-x gap is the deflator, and 56% of the span's nominal output growth. | 24% price wedge over the span | **justified** — inflation is real, but must be controlled for | **Handled**: every divergence table has a `*_real` companion |
| 8 | Electric power `221100` own direct emissions | — | D9 · D8 | The largest single cell, 609 Mt. Direct intensity falls 3.75 → 3.01 kg CO2e per constant-2017 dollar. Two of seven years reverse: 2021 emissions +7.0% on flat real output, 2024 output −1.8% on flat emissions. | 609 Mt gross, net −355; oscillation 0.42 | **justified** — real decarbonisation | **Do not smooth.** Closed except for the 2021 reversal, which [#936](https://github.com/cornerstone-data/bedrock/issues/936) checks against water transportation — if both are one fuel-price event they close together |
| 9 | COVID shock and rebound, 2020-21 | — | D6a · D3 | Every attribution class dips in 2020 and rebounds in 2021. Puts 13 of the bottom-up top 30 in 2021 alone. | median `abs_delta_B_pct_of_N` 1.17% in 2021 against 0.32% in 2018 | **justified** — a real event | **Do not smooth.** Argues for ranking within year as well as pooled |
| 10 | `L` / the `A` matrix | [#937](https://github.com/cornerstone-data/bedrock/issues/937) · basis [#957](https://github.com/cornerstone-data/bedrock/issues/957) | D6b | Moves `N` **1.1 to 3.0 times** as much as the emission factors do — 5.9 of the 8.1 percentage points of median `N` movement in 2021. ⚠️ **Restated 2026-09-21.** The original "two to four times", and the 14.8 of 18.4 points it rested on, were measured against a hybrid `N` — real `B`, nominal `L`. On one basis the 2021 `L` effect is 5.9 points, not 14.8, and **the worst year is 2020, not 2021**. | median `pct_change_N_L_effect` 2.7% to 6.1% a year | **out of scope** | Handed over as [#937](https://github.com/cornerstone-data/bedrock/issues/937), on Nowcasting Phase 2. ➡️ **Now ranked per commodity — see row 18**, which turns this bound into a work list. ⚠️ **The price question is closed, not open**: a consistent deflation cancels out of `N` (`N_real = N_nominal / ρ`), so none of the residual 2.7-6.1% is a price term, and deflating `A` buys no stability — real gross \|d`L`\| is within 9% of nominal every year but 2024, where it is 64% *larger*. Evidence in [`About_the_L_dollar_basis.md`](../nowcasting/About_the_L_dollar_basis.md) |
| 11 | `Vnorm` / the nowcast Make | [#938](https://github.com/cornerstone-data/bedrock/issues/938) | D3 · D13 | 3% to 6% of gross `B` movement through 2022, then 12.2% and 11.9%. Cumulative displacement flattens after 2022 at ~25 Mt while annual churn rises. | output-weighted share of gross `B` movement, to 12.2% | **out of scope** for remediation | Handed over as [#938](https://github.com/cornerstone-data/bedrock/issues/938), on Nowcasting Phase 2. The 2023 step-up is unexplained |
| 12 | Use-table attribution tracks output, and the inventory-named half does not | [#933](https://github.com/cornerstone-data/bedrock/issues/933) | D4 · D9 · D10 | Emissions spread by a row of `U` have an output elasticity of **1.02** against **0.54** for emissions the inventory names directly — so a Use-derived split moves one-for-one with the economy whether or not anything was emitted, while measured emissions move at half that rate. It rides a sector's fuel *purchases* while `x` is a row sum of `V`. Affects 30% of `E`. ⚠️ The pre-vintage-fix run put the slope at 1.51 and read it as *amplification*; at 1.02 the finding is the **gap to `Direct`**, not an overshoot. | slope 1.02 against 0.54; 194 Mt of the rocky mass routes this way by label, ~409 Mt of table 3-11 in fact | **unresolved** | Structural, and **larger than the route labels show** — see row 15. Promoted from possible to likely common cause behind rows 1 and 2, and the reason [#933](https://github.com/cornerstone-data/bedrock/issues/933) outranks #918 for them |
| 13 | Water transportation `483000` | [#936](https://github.com/cornerstone-data/bedrock/issues/936) | D6a | The largest single bottom-up movement in the panel: +87.0% on the `N`-weighted metric in 2021, reversing −25.8% in 2022. Own-share 0.78 to 0.89, so it passes almost undamped to buyers. | `delta_B_pct_of_N` +87.0% (2021) | **unresolved** | Filed as [#936](https://github.com/cornerstone-data/bedrock/issues/936). Check against the 2021 reversal in row 8 — may be the same fuel-price event |
| 14 | Electricity source switch, UMD → eGRID (#911) | [#914](https://github.com/cornerstone-data/bedrock/issues/914), [#913](https://github.com/cornerstone-data/bedrock/issues/913) | — ([#911](https://github.com/cornerstone-data/bedrock/pull/911) review) | #911 moves electricity to plant-level eGRID for 2018-2024 only. No eGRID 2017 exists, so the base year keeps a different source and the switch lands on the 2017/2018 boundary that every indexed series is anchored on. eGRID also includes Puerto Rico, ~12 Mt/yr, where the GHG inventory does not. | boundary error 0.8% of electricity emissions; discontinuity sits on the largest cell in the panel, 609 Mt gross | **not justified** — a source switch is not a movement | Pending #911. Row 8 must be re-derived on the eGRID series once it lands |
| 15 | EPA table 3-11 fossil fuel combustion, as a family | [#923](https://github.com/cornerstone-data/bedrock/issues/923) · [#918](https://github.com/cornerstone-data/bedrock/issues/918) | D9 · D8 · D2a | The largest driver family in the panel and the umbrella over rows 1 and 2, which name its three biggest cells. 698 Mt of gross movement across 567 cells, netting −50 Mt, ⚠️ **The route labels mislead.** By label it is 504 Mt MECS and 194 Mt Use; by provenance 216 Mt of that MECS mass is `petroleum_industrial`, whose weights are BEA Use money rescaled by a MECS fuel share, so **~409 Mt of the 698 Mt — 59% — runs on the Use table** and only ~288 Mt on MECS quantities. The MECS vintage steps from the 2018 survey to the 2022 survey between nowcast 2020 and 2021, and 2021 carries 2.9x the sector reallocation of a within-vintage year against 1.6x on the non-MECS control. | 698 Mt gross, 23.3% of all gross movement in the panel; ~9pp of 2021 reallocation above the control | **split** — the Use-table 59% is **not justified** via rows 1 and 2; the MECS 41% stays **unresolved**, its cutoff measured but its emissions not | ⚠️ Umbrella: do not sum its potency with rows 1 and 2. Both halves resolve the same way — [#929](https://github.com/cornerstone-data/bedrock/issues/929) replaces the Use-table part outright, and manufacturing combustion is **100%** inside the facility scope, so [#918](https://github.com/cornerstone-data/bedrock/issues/918), [#920](https://github.com/cornerstone-data/bedrock/issues/920) and [#921](https://github.com/cornerstone-data/bedrock/issues/921) all retire with it. Evidence in [`About_B_change_drivers.md`](About_B_change_drivers.md) §4 mechanism 5 |
| 16 | Oil and gas extraction's petroleum input coefficient | [#922](https://github.com/cornerstone-data/bedrock/issues/922) | D9 · D8 · D14 | The mechanism under row 1. `211`'s purchases of refined petroleum per $100 of its own gross output go 0.85 → 2.64 → 1.69 over the span — **3.11x to 2022, then −29%** — against a median buyer move of 0.91x and a 90th percentile of 1.53x on the same row. The largest single jump, +71%, is 2017→2018, the first year off the published benchmark. Both sides are nominal and every buyer purchases the same commodity, so a petroleum price move cannot produce it. | 98th percentile of coefficient movement; drives the 51.3 Mt cell in row 1 | **not justified** — the complete facility floor for these facilities, GHGRP subpart C **plus the subpart W combustion tables** ([#927](https://github.com/cornerstone-data/bedrock/issues/927), PR #961), holds 113.3-140.2 Mt across 2017-2024 against an allocation of 29.6-90.1 Mt. The sector is allocated **26-67% of the fuel its own facilities reported burning, in every year of the span** | ⚠️ **Restated on the complete floor.** Read against subpart C alone this was the worst *intermittent* sector, breaching in 2017 and 2020 only; subpart C holds no lease fuel at all, because onshore production reports its combustion under subpart W. On the whole floor it breaches in **all eight years**, with the largest cumulative shortfall in D14 — 589 Mt against 321 Mt for petroleum refineries — and its verdict reads `boundary_offset`, which here means a persistent level defect and not a definition difference. The two defects now separate cleanly: the **level** bias is measured and attributed (#927), the **volatility** is still open. `32551` moves 17.69x on the coefficient and likely shares a cause |
| 17 | Self-supplied fuel has no purchase to be allocated by | [#923](https://github.com/cornerstone-data/bedrock/issues/923) · [#927](https://github.com/cornerstone-data/bedrock/issues/927) | D15 | Fuel that never changed hands — refinery still gas, coke oven gas and blast furnace gas, **and the lease and plant fuel an oil and gas operation burns out of its own stream** — never appears as a purchase, so **no row of the Use table can carry it**. Today it rides the same purchased-fuel vector as everything else, which is a category error rather than an inaccuracy. The byproduct-gas half was measured at 37.0 Mt in 2022, 22.4 Mt of it petroleum refineries; widening the class to every self-supplied fuel takes it to **144.9 Mt**, of which 83.8 Mt is oil and gas extraction (PR #961, a later run vintage). | 144.9 Mt, 9.7% of facility-reported emissions; the direct cause of the sectors D14 puts in `boundary_offset` | **not justified** — the vector cannot represent the flow | The category now exists: `fuel_class` is `purchased` or `self_supplied`, with `fuel_class_basis` naming the evidence. ⚠️ Still a **floor** for byproduct gas, which GHGRP does not label and which is imputed from matched NEI records — 46.9 Mt of the 144.9. The lease and plant fuel half is not imputed: subpart W has the reporter name the fuel |
| 18 | **Which commodities' `L` actually destabilises `N`** — the ranked work list behind row 10 | [#896](https://github.com/cornerstone-data/bedrock/issues/896) · [#922](https://github.com/cornerstone-data/bedrock/issues/922) | D6b, ranked per commodity rather than at the median | Row 10 gives a bound; this gives the list. Ranking every commodity on output-weighted gross `L` effect and splitting on oscillation separates churn from trend. ⚠️ **The electricity row `221100` is a driving `A` cell for ALL 20 top-ranked commodities and carries 30.9% of their top-cell contribution mass** — reaching [#896](https://github.com/cornerstone-data/bedrock/issues/896) from `N` stability rather than from the electricity method. Worst case `441000` motor vehicle dealers: oscillation 0.97, `L` moves its `N` **4.8x** its own factors, own electricity coefficient +60% from its 2019 trough to 2022 then −43% by 2024. Second convergence: `324110` refineries' churn is the oil and gas → refineries coefficient, i.e. rows 1, 2 and 16. | gross 109.1 at oscillation 0.97 (`441000`), down to 38.9 at 0.98 (`324110`); trend group oscillates at 0.00-0.21 | **split** — the churn group is **unresolved but pointed**, the trend group (`531HSO`, `550000`, `541511`, `541100`, `511200`, `541512`, oscillation 0.00-0.21, all monotone) looks **justified** and must not be smoothed | ⚠️ **Order: settle [#896](https://github.com/cornerstone-data/bedrock/issues/896) first, then [#922](https://github.com/cornerstone-data/bedrock/issues/922)/[#923](https://github.com/cornerstone-data/bedrock/issues/923), then re-run.** Six of the eight churners carry an electricity share, so their movement may go with #896; and several of the *trend* commodities do too (`541511` is 36.3% electricity at oscillation 0.06), so even the leave-alone verdicts need re-reading after. Evidence and full tables in [`About_the_L_flux_priority.md`](../nowcasting/About_the_L_flux_priority.md) |

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

From D9, `sector_stratum_span.csv`. Of the 59 cells carrying more than 10 Mt
of gross movement, 28 oscillate above 0.75. Grouped by the vector that spread
them:

| attribution route | rocky mass | cells |
|---|---:|---:|
| `Direct` | 302 Mt | 8 |
| `Nowcast_Detail_Use_AfterRedef` | 192 Mt | 7 |
| `EPA_GHGI_soils` | 164 Mt | 6 |
| `Energy_manufacturing_national_nowcast` | 141 Mt | 5 |
| `EPA_GHGI_NEU` | 60 Mt | 2 |

Three of the five routes are attribution vectors rather than inventory rows,
and together they carry 497 Mt against `Direct`'s 302 Mt, which is why row 12
is on the list as a candidate common cause rather than as one more individual
cell.

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
