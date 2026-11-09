# Electricity modeling record

This file is the starting point for electricity work in the nowcast. It records what we have established, what has been corrected, and what is still open, with a pointer to the document or issue that holds the evidence. Read it before opening a new line of analysis on electric power output, own use or the electricity row.

Unless noted, tables are before redefinitions, and our figures are the balanced supply and use tables of build `v1.0.0_5514c3b`. BEA's figures are from the 2026 annual update.

Codes used throughout:

| code | meaning |
|---|---|
| `221100` | electric power generation, transmission and distribution (private) |
| `S00101` | federal electric utilities (mainly TVA, Bonneville and the other power marketing administrations) |
| `S00202` | state and local government electric utilities |

## 1. What the v1.0 build does

| setting | value | effect | evidence |
|---|---|---|---|
| `rebase_utility_gross_output_on_eia` | on | moves `221100` gross output on two EIA series (section 4) | #1009, #1010, #1030 |
| electricity price carry | on, with the rebase | prices unseeded electricity cells on EIA's average retail price | #1030 |
| `constrain_electricity_pce_cell`, mode `eia_band` | on | keeps household electricity near EIA residential revenue during balancing | #1008, #1011 |
| `chain_services_seed_across_aies` | on | keeps services electricity cells from jumping at the 2023 survey change | #1030 |
| `move_data_processing_electricity_on_lbnl` | on | moves data processing's electricity on LBNL's data center series | #1035 |
| `implement_electricity_reallocation`, `_disaggregation`, `_mixed_units`, `_reaggregation` | off by default | generation, transmission and distribution split; not part of the nowcast tables | methods Discussions #88 and #90 |

## 2. How BEA builds electric gross output

BEA's table of gross output by detailed industry (UGO305-A) has ten electric lines: eight generation technologies, bulk transmission and distribution. Their sum is `221100` gross output. In 2017, $133.1bn + $15.2bn + $241.2bn = $389.4bn. BEA's source table names the 2017 Economic Census for the benchmark year and EIA forms 861 and 861M for other years. It does not say which series on those forms.

The eight generation lines share one price index and move together: in 2022 fossil is 1.62x its 2017 value, nuclear 1.62x, wind 1.63x and hydro 1.61x. They carry no change in technology mix (`About_1009_electricity_output_basis.md`, section 6).

The generation lines follow EIA 861 sales for resale, and distribution follows EIA retail revenue:

| $bn | 2017 | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| BEA generation, eight lines | 133.1 | 151.3 | 154.8 | 139.9 | 193.7 | 215.9 | 171.8 | 154.4 |
| EIA 861 sales for resale revenue | 142.2 | 158.2 | 154.5 | 139.6 | 198.8 | 216.0 | 167.6 | 148.2 |
| generation / sales for resale | 0.94 | 0.96 | 1.00 | 1.00 | 0.97 | 1.00 | 1.02 | 1.04 |
| BEA distribution | 241.2 | 254.6 | 252.1 | 244.1 | 270.8 | 320.8 | 313.8 | 326.9 |
| EIA retail revenue, all sellers | 390.3 | 406.4 | 401.7 | 393.6 | 422.3 | 485.2 | 491.4 | 514.4 |

| correlation of year-to-year changes, 2017-2024 | sales for resale | retail revenue |
|---|---:|---:|
| BEA generation | 0.995 | 0.49 |
| BEA distribution | 0.65 | 0.97 |

This is inferred from the fit, on seven changes. It has not been confirmed from BEA or EIA documentation.

## 3. BEA's series counts the wholesale price cycle twice

When wholesale prices rise, generation rises with sales for resale, and distribution rises with the retail prices that pass the same cost through. Gross output therefore moves more than sales to customers:

| change 2017 to 2022 | $bn |
|---|---:|
| BEA `221100` gross output | +166.8 |
| generation | +82.8 |
| distribution | +79.6 |
| transmission | +4.4 |
| EIA retail revenue | +94.9 |

This is the likely source of the excursion recorded in #1009: BEA's implied price per kWh runs 14.7% above EIA's published price in 2022 and returns to 2.2% below it in 2024.

## 4. Correction: the record's "netting" claim is not established

`About_1009_electricity_output_basis.md`, section 6, reasons that distribution buys about $130bn of power from generation, and that consolidating the ten lines into `221100` nets this sale out. The same claim was repeated on #890. It does not hold as stated: `221100` gross output is the plain sum of the ten lines, so no netting of the kind described takes place when they are added.

The evidence on the 2017 level is mixed:

- `221100` alone is $389.4bn against $390.3bn of retail revenue, which looks like sales to outsiders. But EIA's retail figure includes public power, which BEA books in `S00202` and `S00101`.
- The `221100 x 221100` cell in the 2017 benchmark is $9.5bn, so internal power sales are not recorded in the Use table.
- Distribution at $241.2bn is well below retail revenue. Distribution could therefore already be net of purchased power, which would make the ten-line sum a consolidated total in 2017. Distribution plus transmission, $256.4bn, is close to private retail revenue less investor-owned utilities' purchased power, about $260bn, which fits this. It is not confirmed.

The ownership split in section 6 answers the level question. BEA nets part of the resale, but `221100` is still at least $36bn above what its own customers pay in 2017. The movement after 2017 is not net (section 3).

## 5. Our own use is not on the same basis as our gross output

Step 3 sets each column's intermediate inputs to gross output minus value added. If the own-use cell does not take a change in output, the other inputs take it.

The #1009 rebase splits 2017 `221100` gross output into two legs:

- a retail leg of $324.3bn, moved on EIA Table 2.3 retail revenue;
- a wholesale leg of $65.1bn, moved on EIA Table 8.3 purchased power (investor-owned utilities).

The two legs remove about $47bn of BEA's 2022 output. The wholesale leg still outruns retail in 2021-22, and own use takes little of it:

| | 2017 | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Table 8.3 index, moves the wholesale leg | 1.000 | 1.082 | 1.027 | 1.028 | 1.257 | 1.632 | 1.385 | 1.300 |
| EIA 861 resale index, moves the own-use cell in the utilities seed | 1.000 | 1.112 | 1.086 | 0.982 | 1.398 | 1.519 | 1.178 | 1.042 |
| retail revenue index | 1.000 | 1.041 | 1.029 | 1.008 | 1.082 | 1.243 | 1.259 | 1.318 |
| wholesale leg above a retail path, $bn | 0 | 2.7 | -0.1 | 1.3 | 11.4 | 25.3 | 8.2 | -1.1 |
| our own use `221100 x 221100`, $bn | 9.4 | 10.3 | 10.0 | 8.6 | 10.6 | 13.6 | 8.2 | 8.8 |
| own use / our gross output | 2.6% | 2.7% | 2.7% | 2.3% | 2.6% | 2.8% | 1.8% | 1.8% |

In 2022 the wholesale leg adds $25.3bn above a retail path, and own use rises $4.2bn from 2017. The remainder goes into the industry's other inputs and into the electricity row, which balancing spreads across buyers. How much reaches each has not been measured.

The two resale indices also differ: in 2024 the gross output leg is at 1.30 and the own-use seed at 1.04.

BEA's annual tables behave differently. Published summary utilities own use (`22 x 22`) is:

| $bn | 2017 | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| BEA `22 x 22` | 11.3 | 23.5 | 31.3 | 29.3 | 42.6 | 54.3 | 32.8 | 31.8 |
| ours, same cell | 11.2 | 12.3 | 11.9 | 10.3 | 12.3 | 15.7 | 9.8 | 10.4 |

BEA's cell has a step between the 2017 benchmark and 2018, and then tracks generation output (correlation 0.93 in levels, 0.90 in changes). Electricity is $9.5bn of the $11.3bn in 2017, and gas sold to power plants is $47M. This cell includes gas and water, and BEA does not publish the annual detail, so attributing its movement to electricity resale is an inference.

A pure self-loop is close to neutral for total emission factors, because direct intensity uses the same gross output: the loop raises output and the diagonal together. A #890 comment said the opposite, and that was wrong. The concern is the mismatch above, where the difference lands in other cells.

Other industries show the same question:

- Oil and gas extraction (`211000`), same mechanism. The census self-use cell is mostly gas bought by processing plants. It falls from 54.8% to 26.4% of materials while output stays gross, which lifted other inputs. #1187 addresses this; #922 has the detail. It is not electricity, and it is listed for comparison.
- Refineries (`324`), no seam. Own use stays between 2.8% and 4.5% of output in BEA's tables and between 3.2% and 4.2% in ours. Resale goes through the wholesale margin. Our margin falls short of BEA's in 2022 by $40.9bn.

## 6. Open questions

### What is the $78bn of electric output above retail revenue?

All electric gross output in 2017 is $468.7bn against $390.3bn of EIA retail revenue, a difference of $78.4bn. The rebase calls this difference the wholesale wedge, and allocates it across `221100`, `S00101` and `S00202` by output share. Numerically it is close to government electric output itself ($79.3bn), because `221100` alone is within $0.9bn of retail revenue. The ownership split below shows that closeness is a coincidence.

In the 2017 Use table utilities buy only $11.5bn of electricity from each other ($9.5bn by `221100`, $2.0bn by `S00101` and `S00202`). Of the rest, $266.3bn goes to other industries and $180.4bn to final uses. So the benchmark books this output as sales to ordinary buyers, and the label "sales for resale between utilities" does not describe where the table puts it. As a share of EIA 861 resale revenue the difference is 55% in 2017 and 84% in 2022.

**Split by ownership (measured 2026-11-07).** EIA 861 retail and resale revenue by the seller's entity type, from the PUDL tables `core_eia861__yearly_sales` and `core_eia861__yearly_operational_data_revenue`. Federal sellers are federal. Municipal, political subdivision, state and municipal marketing authority sellers are state and local. Every other type is private, including cooperatives and power marketers. The 861 retail total is within $0.2bn of EIA Table 2.3 in every year.

| 2017, $bn | BEA output | EIA retail | above retail | EIA resale |
|---|---:|---:|---:|---:|
| private sellers, `221100` | 389.4 | 332.9 | +56.5 | 117.2 |
| state and local, `S00202` | 63.4 | 56.1 | +7.3 | 11.2 |
| federal, `S00101` | 15.9 | 1.1 | +14.8 | 13.8 |
| all electric | 468.7 | 390.1 | +78.6 | 142.2 |

- **`221100` is above what its own customers pay.** It looked equal to retail revenue only because that total includes public power. Against private retail it is 17% high.
- **Sales to public power don't close the gap.** State, local and federal utilities bought 502 TWh wholesale in 2017. At the private resale price of $39.8 per MWh that is at most $20.0bn, which leaves `221100` at least $36bn above its customers' payments.
- **BEA nets part of the resale.** Private retail plus private resale is $450.1bn, $60.7bn above `221100`, so the ten lines are not a plain gross total either.
- **Federal output is wholesale.** 87% of it is resale, yet the 2017 Use table sells it to ordinary buyers, whose bills are already in retail revenue.
- **The ratio moves with the double count.** `221100` over private retail is 1.17, 1.22, 1.24, 1.18, 1.32, 1.32, 1.18 and 1.12 in 2017-2024.

Limits: $5.2bn of 2017 revenue has no entity type and is counted as private. Other operating revenue, $15.1bn across all sellers, is not counted. Whether BEA books cooperatives and power marketers in `221100` is assumed. The split shows the level is too high against customers; it does not show how BEA builds the lines, which only BEA can confirm.

An alternative model that takes the $54.4bn of 2017 commercial and industrial bills above EIA revenue out of electric output, in proportion to output, puts `221100` at $344bn. That is between private retail ($333bn) and private retail plus every public purchase ($353bn). Because the cut follows output, federal output stays near $14bn although most of it is wholesale. A cut by owner would follow the split more closely.

### Which Form 861 series does BEA use?

Open since #1009 and not filed separately. Section 2 gives a fit-based answer. A statement from BEA or the form's documentation would settle it.

### Does the input floor still rule out a single retail index?

#1010 rejected moving all output on retail revenue because intermediate inputs fell to $126.6bn in 2022, below the $129.5bn investor-owned utilities report for fuel and purchased power. `About_1009`, section 6, then calls purchased power internal to `221100`, but the floor was not revisited. Purchases from other private generators are internal under consolidation, and purchases from federal or public power are not. The right floor depends on the 2017 answer above, and lies between fuel alone and fuel plus purchased power.

### Does electric power's value-added share hold up?

#1012, open. It rests on the same basis question: BEA has value added at 66.4% of output in 2017, which leaves intermediate inputs at $131.0bn.

### Items with no issue

- `S00101` federal power is largely wholesale, but only $2.0bn of electricity is bought by government utilities in the 2017 Use table, so its output reaches final purchasers directly. It is held out of the rebase for this reason.
- The $10.4bn electricity supply-use gap in 2022 was measured on build `v0.3.0_4276083` and has not been re-measured.

## 7. Treatments considered

| treatment | status | what the record says |
|---|---:|---|
| Leave BEA's series as published | rejected in #1009 | the balancing moved $17.6bn into household electricity with no evidence for it (`About_1009`, section 4) |
| Set own use to the size of purchased power, about $72.5bn | rejected in #1009 | at the 2017 level, fuel plus purchased power of $81.2bn nearly fills investor-owned utilities' $88.5bn share of intermediate inputs; BEA's source has no line for power bought for resale |
| Two legs: retail and Table 8.3 (current) | shipped in #1010, on since #1030 | worst implied price deviation falls from 14.7% to about 5%; leaves the mismatch in section 5 |
| All output on retail revenue | rejected in #1010 on the input floor | the floor needs re-testing (section 6); removes the wholesale cycle from output; has to come with a matching deflator (section 11) |
| Keep the wholesale leg and put its movement in own use | proposed on #890, not graded | follows BEA's annual tables but not the benchmark. Moves only the change, not the $72.5bn level, but the no-source objection to the row above still applies |
| Keep BEA's ten lines and replace the generation price index | proposed in `About_1009`, section 6, not built | preferred there as the exact decomposition. Needs checking against section 3, because a better price index alone may not remove the double count |

## 8. Proposed next steps (not decided)

1. Done: about half of the IV score for electric power is the deflator mismatch (section 11).
2. Done: EIA 861 revenue split by ownership (section 6). `221100` is at least $36bn above its customers' payments in 2017.
3. Ask BEA which Form 861 series it uses for generation and distribution, and whether 2017 distribution is net of purchased power.
4. Re-run the input floor on the ownership split.
5. Grade the current two legs against all output on retail revenue with EIA's retail price as the deflator, behind flags, on an evaluation build, on implied price, the electricity row and the cells in section 5. Decide after that. A change would need a rebuild and reopens a #1009 decision.
6. Correct the statements in section 9. The correction on #890 was posted on 2026-11-07.

## 9. Statements elsewhere that are out of date

| where | what it says | current state |
|---|---|---|
| `current/eia_gtd/About_1009_electricity_output_basis.md:204` | rebase off by default | on since #1030 |
| `current/eia_gtd/About_1009_electricity_output_basis.md:295-309` | consolidation nets about $130bn | BEA nets part of the resale, but `221100` is still at least $36bn above its customers' payments (sections 4 and 6) |
| `current/eia_gtd/About_990_target_attribution.md:177-181` | residential constraint off, "production flag stays off" | on, mode `eia_band` |
| `current/eia_gtd/About_1008_pce_electricity_pin.md:313-318` | constraint defaults to false; mode default `'none'`; do not co-enable with the rebase unattributed | both on; both arms graded |
| `current/eia_gtd/About_price_proposal.md:147` | MECS is the tie-breaker | on a dollar basis Census is closest to the Use table in 2018 and 2022 (#898) |
| `current/eia_gtd/About_why_the_flat_price_fails.md` | capped purchaser counts, 6 in 2017 to 44 in 2024 | measured before #1030 changed the row; not re-measured |
| `bedrock/transform/iot/eia_utility_go_adjustment.py`, module docstring | wholesale leg is "sales for resale between utilities" | the benchmark sells it to ordinary buyers (section 6) |
| `time_series_B_matrix/About_sector_trend_explanations.md`, earlier tracker row 8 | 2021 emissions rose on flat real output; "justified, do not smooth"; re-derive on eGRID | on the current build, real output falls 7.0% in 2021 on a mismatched price (section 11). The verdict needs re-deriving |
| `time_series_B_matrix/About_sector_trend_explanations.md`, earlier tracker row 14 | pending #911; no eGRID 2017; Puerto Rico included | #911 and #947 merged; #913 and #914 closed |
| `time_series_B_matrix/About_sector_trend_explanations.md`, earlier tracker row 18 | settle #896 first, then #922 and #923, then re-run | #896 and #923 closed; #922 open with #1187 in review; the ranking has not been re-run on the current build |
| #890 comments of 2026-11-03 | benchmark nets about $130bn; recommends the "net basis"; describes the wedge as resale | corrected on #890, 2026-11-07 (sections 4 to 6) |

## 10. Where the evidence is

| topic | document or issue |
|---|---|
| Why our output differs from BEA's, and the treatments | `current/eia_gtd/About_1009_electricity_output_basis.md`, #1009, #1010 |
| The electricity row's 2023-24 share fall | `current/eia_gtd/About_896_row_gate.md`, #896 |
| The electricity intermediate target for 2022-24 | `current/eia_gtd/About_990_target_attribution.md`, #990 |
| Household electricity during balancing | `current/eia_gtd/About_1008_pce_electricity_pin.md`, #1008 |
| Output, price carry and survey seam for v0.5 | #1030 |
| Purchaser shares: Use table, MECS, Census | `current/eia_gtd/About_electricity_shares.md`, #894, #898 |
| Generation, transmission and distribution revision | `current/eia_gtd/About_price_proposal.md`, `current/eia_gtd/About_why_the_flat_price_fails.md`, board 29, #901, #902, #903 |
| Settled generation, transmission and distribution method | methods repository Discussions #88 and #90 |
| Commercial buyers' electricity | #899 |
| Fuel seed for the electric columns | `bedrock/analysis/nowcasting/utilities_expense_seed.py`, #719, #1187 |
| Fuel purchases against eGRID emissions | #1002 |
| Own use cells and the 2022 spike | #890 |
| Sector list, status and links | `time_series_B_matrix/B_driver_investigation.md` |
| Sector explanations and the earlier tracker rows | `time_series_B_matrix/About_sector_trend_explanations.md` |
| IV score | `time_series_B_matrix/About_IV_score.md` |
| How much each A cell moves `N`, and the electricity row across commodities | `nowcasting/About_the_L_flux_priority.md` |
| Electricity emissions source: eGRID, Puerto Rico, 2017 | #911, #913, #914, #947 |

## 11. Electric power in the B tracker

The B tracker (`time_series_B_matrix/B_driver_investigation.md`) restarted from the IV score on GHG FBS `v1.0.0_79273ed` and MUT `v1.0.0_5514c3b`. Electric power ranks first under all four weightings in `About_IV_score.md`. With spend weights its score is 3.23, against 0.40 for the next sector, and 93% of the movement in its domestic emission factor comes from `B`. Its status is unresolved and its explanation fields are not yet assessed. The IV note attributes most of the 2021 share of movement across all commodities to this one factor: 2021 is 64.9% of the movement with electric power and 22.4% without it.

### Most of the 2021 and 2023 swing in `E/q` comes from the output price

`B` is roughly `E / q`. The tracker's `q` is our nominal commodity output in constant 2017 dollars. The nominal output of the `221100` industry has been on EIA since #1030. The deflator is still BEA's price index for `221100`, which includes the single wholesale price on all eight generation lines. #1030 moved the price carry for unseeded cells onto EIA's retail price, but `derive_industry_price_index` and the `rho` built from it have no such override.

| index, 2017 = 1 | 2017 | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| nominal commodity output, $bn | 454.9 | 478.3 | 472.0 | 460.7 | 512.1 | 597.9 | 577.6 | 588.2 |
| BEA price index, `221100` | 1.000 | 1.046 | 1.063 | 1.030 | 1.230 | 1.367 | 1.247 | 1.216 |
| EIA average retail price | 1.000 | 1.005 | 1.006 | 1.010 | 1.059 | 1.179 | 1.210 | 1.234 |
| `q`, output on BEA's price (reproduces the tracker) | 1.000 | 1.005 | 0.976 | 0.984 | 0.915 | 0.962 | 1.018 | 1.063 |
| `q`, output on EIA's price | 1.000 | 1.047 | 1.032 | 1.003 | 1.063 | 1.115 | 1.050 | 1.047 |
| EIA retail kWh | 1.000 | 1.036 | 1.024 | 0.998 | 1.022 | 1.055 | 1.041 | 1.068 |

| annual change, % | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 | sum of absolute changes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `E` | 1.0 | -7.9 | -9.7 | 6.0 | 0.0 | -7.9 | 0.0 | 32.4 |
| `E / q`, tracker | 0.5 | -5.2 | -10.4 | 13.9 | -4.9 | -12.9 | -4.2 | 52.1 |
| `E /` output on EIA's price | -3.5 | -6.6 | -7.1 | 0.0 | -4.7 | -2.2 | 0.3 | 24.3 |
| `E /` EIA retail kWh | -2.5 | -6.8 | -7.3 | 3.5 | -3.1 | -6.6 | -2.5 | 32.4 |

The tracker's `q` falls 7.0% in 2021 while kWh sold rise 2.4%, then rises 5.8% in 2023 while kWh fall 1.3%. On a consistent basis the 2021 rise of 13.9% becomes about 0% or +3.5%, and the 2023 fall of 12.9% becomes -2.2% or -6.6%. The overall decline in `E/q` from 2017 to 2024 is 21.7% to 23.2% on all three bases, so the long-run decline does not depend on this.

Limits of this check:

- `E` is rounded to two decimals from the tracker, so the annual changes are approximate. `q` is the unrounded series in the table above.
- The EIA retail price is not the exact price of our output, which includes a wholesale leg and the government utilities that are not rebased. That is why output on EIA's price still rises 11.5% in 2022 against 5.5% for kWh.
- kWh are retail sales only.
- Where else this deflator reaches, including the released factors, has not been traced.

### IV score with electric power on a consistent real output

Measured on the cached tables for GHG FBS `v1.0.0_79273ed` and MUT `v1.0.0_5514c3b`, with no rebuild and spend weights. Only electric power's real output changes. `I` is built on nominal values, so it does not move, and `V` is recomputed.

| real output of `221100` | IV score | V | annual change in real `B`, 2021 | annual change in real `B`, 2023 | 2021 share of all movement |
|---|---:|---:|---:|---:|---:|
| nominal output over BEA's price index (as built) | 3.23 | 0.100 | +13.8% | -13.8% | 66.4% |
| nominal output over EIA's average retail price | 1.48 | 0.046 | +0.7% | -2.1% | 24.4% |
| 2017 output moved on EIA retail kWh | 1.76 | 0.055 | +4.2% | -6.8% | 47.9% |

Electric power stays first; the next sector scores 0.40. About half of its score comes from the mismatch. What remains on the kWh basis includes a real 2021 rise in emissions and the long-run decline, and is not a reason to smooth.

The change is almost entirely in electric power's own factor. Its gross `N_d` movement falls from 56.1 to 35.1 on the kWh basis, and gas distribution (`221200`) falls by 4.0. Other commodities move by 0.2 or less, because the deflator cancels between `B` and `L` for a buyer. What changes for buyers is the split between `B` and `L`, for example truck transportation goes from 65% to 60% `B`. So the deflator does not address electricity's effect on buyers' factors. That runs through the nominal output and the electricity row (section 5).

Nominal output and the deflator have to change together. The table shows real output, as an index with 2017 = 1:

| | 2021 | 2022 | 2023 | 2024 |
|---|---:|---:|---:|---:|
| current output over BEA's price | 0.915 | 0.962 | 1.018 | 1.063 |
| all output on retail revenue, over BEA's price | 0.880 | 0.909 | 1.010 | 1.084 |
| all output on retail revenue, over EIA's retail price, equal to kWh | 1.022 | 1.055 | 1.041 | 1.068 |

Removing the wholesale leg alone makes the 2021-22 dip in real output deeper, because BEA's price index keeps the wholesale spike. The government utilities, which are not rebased, sit in Cornerstone `221100` as well: federal electric output goes from $15.9bn to $26.6bn by 2022. A real output that tracks kWh closely has to account for them.

### What the tracker's fields point to

| field | current reading | where it goes |
|---|---|---|
| `E`, 1.00 to 0.82 | eGRID via `Direct` is 99%. Largest fall comes in 2019-20 and 2023 | Real decarbonisation is the earlier verdict; re-derive it on this basis. 2021 is the one rise, +6.0% |
| `q`, 1.00 to 1.06 | not physical in 2021-23; see above | this section |
| largest `A` cell, refined petroleum, 1.8% of `N_d` | the EIA oil bill moved a $17.4bn cell | #922, fixed behind a flag in #1187, not yet in the tables |
| second `A` cell, own use, 1.6% | section 5 | #890 |
| third `A` cell, coal mining, 1.5% | EIA 923 coal moves this row, and is 2.1x BEA's row in 2017 (`utilities_expense_seed.scope`) | not yet examined on this build |

### Electricity as an input to other tracker sectors

The electricity row is one of the three largest `A` cells in seven of the other eight sectors on the list, in gross percent of `N_d`:

| buyer | electricity cell | rank among its `A` cells |
|---|---:|---:|
| `531ORE` other real estate | 20.0 | 1 |
| `484000` truck transportation | 7.9 | 1 |
| `211000` oil and gas extraction | 7.4 | 1 |
| `324110` petroleum refineries | 6.3 | 2 |
| `325190` other basic organic chemicals | 5.7 | 3 |
| `325211` plastics and resin | 5.7 | 3 |
| `325110` petrochemicals | 4.9 | 3 |

These are the same movements reached from the buyer side, so their explanations should wait for the row's. The earlier work reached the same place: on 2026-09-21, the electricity row was a driving `A` cell for all 20 top-ranked commodities and held 30.9% of their top-cell mass (earlier tracker row 18). #896 closed on 2026-09-30, when #1030 reduced the 2023-24 share fall: electricity as a share of intermediate use for 2021-24 went from 2.02, 2.02, 1.69, 1.53% to 1.73, 1.79, 1.65, 1.57%. The ranking has not been re-run since. What #899 still has open is the same price question for buyers: BEA's price index runs about 17% above EIA's commercial price in 2021-22, for every unseeded buyer.

### Status of the earlier tracker items

| item | status | what remains |
|---|---|---|
| Row 8, direct emissions, "justified, do not smooth" | written on the UMD series and `v0.3.0_99655e9` | re-derive on eGRID and a consistent `q` |
| Row 8 and #936, shared 2021 event | #936 open | not supported so far. Water transportation's 2021 is two fuel rows in the inventory, with emissions up 98%; electric power's is up 6% on eGRID. No shared cause has been shown |
| Row 14, switch to eGRID | #911 and #947 merged; #913 and #914 closed | 2017 is indexed to the inventory, not observed. We did not find a check of the 2017/2018 boundary on the current build |
| Row 18, ranked `L` movers | see above | re-run on the current build |
| #1002, EIA 923 against eGRID | closed; no comparison was run | nothing checks that the fuel we book agrees with the emissions we attribute. Expect a gap, because receipts are purchases and eGRID is combustion |
