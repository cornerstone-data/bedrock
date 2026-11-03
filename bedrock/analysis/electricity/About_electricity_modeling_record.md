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
- Distribution at $241.2bn is well below retail revenue. Distribution could therefore already be net of purchased power, which would make the ten-line sum a consolidated total in 2017. We have not tested this.

So the 2017 level may or may not be net. The movement after 2017 is not net (section 3). Section 6 lists the test.

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

All electric gross output in 2017 is $468.7bn against $390.3bn of EIA retail revenue, a difference of $78.4bn. The rebase calls this difference the wholesale wedge, and allocates it across `221100`, `S00101` and `S00202` by output share. Numerically it is close to government electric output itself ($79.3bn), because `221100` alone is within $0.9bn of retail revenue.

In the 2017 Use table utilities buy only $11.5bn of electricity from each other ($9.5bn by `221100`, $2.0bn by `S00101` and `S00202`). Of the rest, $266.3bn goes to other industries and $180.4bn to final uses. So the benchmark books this output as sales to ordinary buyers, and the label "sales for resale between utilities" does not describe where the table puts it. As a share of EIA 861 resale revenue the difference is 55% in 2017 and 84% in 2022.

Test: split EIA 861 retail revenue by ownership. Compare private retail revenue with `221100` and its distribution line, and public power retail revenue with `S00101` and `S00202`. This also tests the 2017 question in section 4.

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
| All output on retail revenue | rejected in #1010 on the input floor | the floor needs re-testing (section 6); removes the wholesale cycle from output |
| Keep the wholesale leg and put its movement in own use | proposed on #890, not graded | follows BEA's annual tables but not the benchmark. Moves only the change, not the $72.5bn level, but the no-source objection to the row above still applies |
| Keep BEA's ten lines and replace the generation price index | proposed in `About_1009`, section 6, not built | preferred there as the exact decomposition. Needs checking against section 3, because a better price index alone may not remove the double count |

## 8. Proposed next steps (not decided)

1. Split EIA 861 retail revenue by ownership (section 6). This answers the 2017 level question, the $78bn and the right input floor. It needs no rebuild.
2. Ask BEA which Form 861 series it uses for generation and distribution, and whether 2017 distribution is net of purchased power.
3. Re-run the input floor on the answer from step 1.
4. Grade the current two legs against the all-retail treatment on an evaluation build, on implied price, the electricity row and the cells in section 5. Decide after that. A change would need a rebuild and reopens a #1009 decision.
5. Correct the statements in section 9, and post a correction on #890 pointing here.

## 9. Statements elsewhere that are out of date

| where | what it says | current state |
|---|---|---|
| `current/eia_gtd/About_1009_electricity_output_basis.md:204` | rebase off by default | on since #1030 |
| `current/eia_gtd/About_1009_electricity_output_basis.md:295-309` | consolidation nets about $130bn | not established (section 4) |
| `current/eia_gtd/About_990_target_attribution.md:177-181` | residential constraint off, "production flag stays off" | on, mode `eia_band` |
| `current/eia_gtd/About_1008_pce_electricity_pin.md:313-318` | constraint defaults to false; mode default `'none'`; do not co-enable with the rebase unattributed | both on; both arms graded |
| `current/eia_gtd/About_price_proposal.md:147` | MECS is the tie-breaker | on a dollar basis Census is closest to the Use table in 2018 and 2022 (#898) |
| `current/eia_gtd/About_why_the_flat_price_fails.md` | capped purchaser counts, 6 in 2017 to 44 in 2024 | measured before #1030 changed the row; not re-measured |
| `bedrock/transform/iot/eia_utility_go_adjustment.py`, module docstring | wholesale leg is "sales for resale between utilities" | the benchmark sells it to ordinary buyers (section 6) |
| #890 comments of 2026-11-03 | benchmark nets about $130bn; recommends the "net basis"; describes the wedge as resale | see sections 4 to 6 |

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
