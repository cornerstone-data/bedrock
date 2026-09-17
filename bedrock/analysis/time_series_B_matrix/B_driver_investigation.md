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

| # | Driver | Issue | Effect | Potency | Justified? | Status |
|---|---|---|---|---|---|---|
| 1 | Oil and gas extraction `211000` industrial fuel, split by the Use table and by MECS | — | Two cells move 228 Mt across the span and arrive 13 Mt from where they started. Largest pair of rocky cells in the panel. | 121.8 + 106.6 Mt gross; oscillation 0.97 and 0.91 | **unresolved** | Needs an issue. Top candidate. |
| 2 | Petroleum refineries `324110` industrial fuel via MECS | — | Third-largest rocky cell; same attribution route as #1, so probably the same underlying cause. | 79.4 Mt gross, net −4.6; oscillation 0.94 | **unresolved** | Investigate with #1 |
| 3 | Grain farming `1111B0` soils via `EPA_GHGI_soils` | — | The largest non-fuel rocky cell. Soils attribution moved 71.8 Mt and settled 10.6 Mt lower. | 71.8 Mt gross; oscillation 0.85 | **unresolved** | Needs an issue |
| 4 | Air transportation `481000` jet fuel, `Direct` | — | 99.5 Mt of movement, a quarter of it net. Mixed trend and churn; the only large `Direct` cell that is not electric power. | 99.5 Mt gross, net −24.7; oscillation 0.75 | **unresolved** | Investigate; may be COVID |
| 5 | Negative emission factors on `114000` and `327910` | [#912](https://github.com/cornerstone-data/bedrock/issues/912) | Fossil-combustion rows go below zero. Makes both commodities unrankable on any percentage metric; `114000` otherwise takes 6 of the top 30 slots including first. | `114000` swings +379% on a base that crossed zero | **not justified** | Filed, Priority High |
| 6 | EPA attribution table renumbering | — | Soils `T_5_17`→`T_5_18`→`T_5_19` and non-energy use `T_3_25b`→`T_3_25` mid-span, so `T_5_18` means *direct* in some years and *indirect* in others. 2019 booked −297 Mt against +255 Mt on flat emissions. | ±300 Mt of pure artefact in 2019, ±90 Mt in 2023 | **not justified** | **Fixed** in this PR via `ATTRIBUTION_ROLE_ALIAS` |
| 7 | Prices in a nominal denominator | — | `x` is nominal on the nowcast path. 74% and 73% of the 2021 and 2022 E-versus-x gap is the deflator, and 56% of the span's nominal output growth. | 24% price wedge over the span | **justified** — inflation is real, but must be controlled for | **Handled**: every divergence table has a `*_real` companion |
| 8 | Electric power `221100` own direct emissions | — | The largest single cell, 609 Mt. Direct intensity falls 3.82 → 3.06 kg CO2e per constant-2017 dollar. Two of seven years reverse: 2021 emissions +7.0% on flat real output, 2024 output −1.8% on flat emissions. | 609 Mt gross, net −355; oscillation 0.42 | **justified** — real decarbonisation | **Do not smooth.** Closed unless the 2021 reversal proves to be a data artefact |
| 9 | COVID shock and rebound, 2020-21 | — | Every attribution class dips in 2020 and rebounds in 2021. Puts 14 of the bottom-up top 30 in 2021 alone. | median `abs_delta_B_pct_of_N` 1.44% in 2021 against 0.33% in 2018 | **justified** — a real event | **Do not smooth.** Argues for ranking within year as well as pooled |
| 10 | `L` / the `A` matrix | — | Moves `N` two to four times as much as the emission factors do — 14.9 of the 18.2 percentage points of median `N` movement in 2021. | median `pct_change_N_L_effect` 3.1% to 14.9% a year | **out of scope** | Hand to Nowcast Phase 2 |
| 11 | `Vnorm` / the nowcast Make | — | 3% to 6% of gross `B` movement through 2022, then 10.7% and 13.0%. Cumulative displacement flattens after 2022 at ~25 Mt while annual churn rises. | output-weighted share of gross `B` movement, to 13.0% | **out of scope** for remediation | Hand to Nowcast Phase 2. The 2023 step-up is unexplained |
| 12 | Use-table attribution over-tracks output | — | Emissions spread by a row of `U` have an output elasticity of **1.51** — they amplify output rather than follow it, because they ride a sector's fuel *purchases* while `x` is a row sum of `V`. Affects 30% of `E`. | slope 1.51; 348 Mt of the rocky mass routes this way | **unresolved** | Structural. May be the common cause behind #1, #3 and #4 |
| 14 | Electricity source switch, UMD → eGRID (#911) | [#914](https://github.com/cornerstone-data/bedrock/issues/914), [#913](https://github.com/cornerstone-data/bedrock/issues/913) | #911 moves electricity to plant-level eGRID for 2018-2024 only. No eGRID 2017 exists, so the base year keeps a different source and the switch lands on the 2017/2018 boundary that every indexed series is anchored on. eGRID also includes Puerto Rico, ~12 Mt/yr, where the GHG inventory does not. | boundary error 0.8% of electricity emissions; discontinuity sits on the largest cell in the panel, 609 Mt gross | **not justified** — a source switch is not a movement | Pending #911. Row 8 must be re-derived on the eGRID series once it lands |
| 13 | Water transportation `483000` | — | The largest single bottom-up movement in the panel: +87.6% on the `N`-weighted metric in 2021, reversing −25.8% in 2022. Own-share 0.79 to 0.89, so it passes almost undamped to buyers. | `delta_B_pct_of_N` +87.6% (2021) | **unresolved** | Check against the 2021 reversal in #8 — may be the same fuel-price event |

---

## What the rocky mass routes through

Of the 63 cells carrying more than 10 Mt of gross movement, 33 oscillate above
0.75. Grouped by the vector that spread them:

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
[`About_B_change_drivers.md`](About_B_change_drivers.md) and link it here. When
a driver is fixed, re-run the diagnostics and record the before-and-after on
the two acceptance measures in the plan — median `abs_delta_B_pct_of_N` and the
share over the gate — so an improvement bought by displacing movement onto
other commodities is visible rather than netted away.
