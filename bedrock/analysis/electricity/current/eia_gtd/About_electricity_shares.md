# Who buys manufacturing's electricity: three sources, 2017-2024

The G/T/D purchaser allocation distributes the Industrial end-use class across
manufacturing by **MECS Table 7.7 purchased-kWh shares**
(`_industrial_mecs_class_mwh` in `transform/eeio/electricity_gtd_allocation.py`).
The Use table's own electricity row decides the manufacturing-vs-residual pool
split but is never consulted for the split *inside* manufacturing. MECS and the
IO model are therefore two separate claims about the same allocation, and
nothing in the pipeline reconciles them.

Census publishes a third claim — cost of purchased electricity by NAICS,
annually, in dollars. It is the only source that is both annual and in the same
units as the Use table, which makes it the natural referee.

Reproduce with:

```bash
python -m bedrock.analysis.electricity.current.eia_gtd.annual_electricity_shares --csv
python -m bedrock.analysis.electricity.current.eia_gtd.annual_electricity_shares --check
```

## Everything here is a share, never a level

`Census_EC_Expenses` warns that Census and BEA disagree about what these expense
cells contain, by large factors, at the 2017 base itself, and that an expense
cell should be indexed rather than substituted. Shares respect that: they are
comparable where the levels are not. A share is also what makes the kWh source
commensurate with the two dollar sources without asserting a price per sector.

Shares are taken over the sectors all three cover in a given year (215-232 of
232), so each column sums to 100.

## MECS is the outlier of the three

Half the summed absolute gap — the share of the manufacturing total one source
would have to move to agree with the other:

| year | sectors | IO vs MECS | IO vs Census | Census vs MECS |
|---|---:|---:|---:|---:|
| 2017 | 232 | 15.62 | 12.96 | 16.55 |
| 2018 | 231 | 17.70 | 11.06 | 14.88 |
| 2019 | 229 | 17.69 | 12.43 | 15.32 |
| 2020 | 227 | 17.95 | 12.38 | 17.22 |
| 2021 | 228 | 18.68 | 12.91 | 16.65 |
| 2022 | 215 | 18.78 | 14.76 | 14.21 |
| 2023 | 224 | **20.22** | 14.09 | 18.13 |
| 2024 | 224 | 16.68 | 14.59 | 16.70 |
| **mean** | | **17.91** | **13.15** | **16.21** |

✅ **The IO model agrees with Census more closely than with MECS in all 8 of 8
years.** The two dollar-denominated sources are closer to each other than either
is to the kWh source. That ordering is the main result: the allocation the model
*rejects* is the one better corroborated by an independent annual survey.

⚠️ **MECS contributes no annual variation.** Table 7.7 has two vintages across
this span — the 2018 survey for 2017, the 2022 survey for 2018-2024
(`mecs_year_for_eia_year`). Its shares are flat to three digits within each
regime; the small wobble that remains comes from the IO weights used to split
7.7 rows that straddle several IO codes. So a growing IO-vs-MECS gap is the IO
side moving, and 2018-2024 is anchored on a single survey year.

## The commodities that depart most

Share of the manufacturing total, %:

**331110 Primary iron, steel and ferroalloy products** — MECS is high against
both dollar sources, every year:

| | 2017 | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| IO | 3.75 | 4.40 | 3.47 | 2.34 | 3.38 | 3.68 | 4.42 | 5.12 |
| Census | 5.66 | 5.41 | 4.90 | 4.41 | 4.95 | 5.30 | 4.58 | 4.40 |
| MECS | **7.24** | **6.80** | **6.80** | **6.80** | **6.80** | **7.08** | **6.80** | **6.98** |

Census sits between the two but nearer IO. This is also the sector that gains
the most generation dollars from the MECS weighting.

**324110 Petroleum refineries** — here **the IO side is the unstable one**:

| | 2017 | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| IO | 6.56 | 8.35 | 9.94 | 5.90 | 7.99 | **12.14** | 9.49 | 5.72 |
| Census | 5.83 | 5.57 | 5.32 | 4.81 | 6.27 | 7.00 | 6.26 | 5.91 |
| MECS | 6.44 | 5.81 | 5.81 | 5.81 | 5.81 | 6.05 | 5.81 | 5.97 |

The IO share more than doubles between 2020 and 2022 and halves again by 2024,
while both surveys stay inside 4.8-7.0. Not a MECS problem.

**334413 Semiconductors** — and here **Census is the outlier**:

| | 2017 | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| IO | 2.15 | 1.76 | 1.90 | 2.28 | 2.28 | 2.23 | 1.61 | 1.42 |
| Census | **3.66** | **3.35** | **3.58** | **3.95** | **3.84** | **3.95** | **3.93** | **3.74** |
| MECS | 1.50 | 1.69 | 1.69 | 1.69 | 1.69 | 1.76 | 1.69 | 1.74 |

IO and MECS agree; Census is roughly double both. So the verdict is per
commodity, not a blanket ranking of the sources.

**322130 Cardboard** — IO runs about 1.6x Census throughout (3.07-3.82 against
1.77-2.19), with MECS in between.

## Two data defects found on the way

⚠️ **322120 Paper mills has $0 electricity in the nowcast Use table in 2021 and
2023.** Read straight off the pinned MUT `v0.3.0_4276083`:

```
2020  322120 = $0.722bn
2021  322120 = $0.000bn
2022  322120 = $0.908bn
2023  322120 = $0.000bn
2024  322120 = $0.744bn
```

Paper mills are among the most electricity-intensive manufacturers, and both
neighbouring years carry the expected magnitude. This is independent of MECS and
sits in the vintage the electricity integration builds on.

⚠️ **2017 Economic Census `CSTELEC` publishes suppressed cells as `0`, not
null** — `Census_EC_Expenses` documents this for food industries, and 322130
Cardboard shows the same shape (0.00 in 2017 against 1.77-2.19 every later
year). Treat a Census zero as missing, not as evidence of no purchase.

⚠️ **The local `EIA_MECS_Energy_2018` FBA can predate Table 7.7.** Older
parquets carry 7.2 and 7.10 instead, and `mecs_purchased_kwh(2018)` raises on
them. FBA parquets are keyed on the git hash, so regenerate before reading the
2017 regime. The unit tests patch the MECS frame and will not catch it.

## What this does not settle

The comparison is of allocations, not of correctness. Census is annual and in
the right units but measures a *cost* under a Census definition of the
establishment; MECS measures *kWh* and is the only physical-unit source; the IO
row is a modelled result. `334413` shows they do not fail in one consistent
direction.

What is established is that the source the model uses is the one least
corroborated by the other two, that its contribution is frozen at one survey
year across 2018-2024, and that the gap has widened over the nowcast span.
