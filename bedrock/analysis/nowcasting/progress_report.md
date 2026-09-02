# Nowcasting Phase 1 Report

*Updated 2 September 2026*

The nowcast builds annual US input-output tables — Supply and Use tables,
then Make-Use tables before and after redefinitions — for 2017 through 2023
at BEA detail level (402 commodities × 402 industries), from primary annual
sources rather than by carrying the 2017 benchmark structure forward. This
report is the standing record of how close those tables are to every
published answer that exists: the 2017 detail benchmark, the annual summary
tables, and the published industry output series.

## Contents

1. [Comparison to BEA 2017 Benchmark SUTs](#1-comparison-to-bea-2017-benchmark-suts)
   — [Initial Seed Tables](#11-initial-seed-tables) ·
   [Balanced RAS Tables](#12-balanced-ras-tables)
2. [Annual Comparison to Published Summary Tables](#2-annual-comparison-to-published-summary-tables)
3. [Industry Output Against the Published Series](#3-industry-output-against-the-published-series)
4. [Conversion to Make-Use Tables](#4-conversion-to-make-use-tables)
5. [Redefinitions: Before to After](#5-redefinitions-before-to-after)
6. [Seed Data Provenance and Quality](#6-seed-data-provenance-and-quality)
7. [Conclusions and Next Steps](#7-conclusions-and-next-steps)

**How to read the comparison figures.** Every comparison figure in sections
1 and 2 uses one visual language. Each cell of the table under comparison is
coloured by its match status against the published reference: **green**
lands within tolerance (1% of the cell, or 1.3% for final demand, with an
absolute floor of $0.5M — half BEA's publication grain); **amber** is
outside tolerance, darkening with severity; **purple** cells are populated
only in the published table; **blue** cells only in ours; white cells are
empty on both sides. The strip on the right is the row totals, the strip
along the bottom the column totals, and the corner square the grand total —
drawn smallest because it is the check that passes on broken data.
**Coverage** is the share of reference-populated cells we also populate;
**accuracy** is the share of populated cells landing within tolerance.

## 1. Comparison to BEA 2017 Benchmark SUTs

2017 is the benchmark year: the one year BEA publishes the full detail
Supply and Use tables, and therefore the one year with a complete answer
key. Two states of our 2017 tables are compared against it. The **initial
seed tables** are the direct output of the estimation methods — final
demand, value added, the intermediate interior, the domestic output block,
and the supply bridge, each built from its own primary sources. The
**balanced RAS tables** are the final product: the same blocks after the
balancing step imposed the accounting identities and observed aggregates.
Table 1 summarises both states.

*Table 1. The five SUT blocks against the published 2017 detail benchmark,
in the seed state and the balanced state. Coverage / accuracy over populated
cells; total difference is the grand total against published.*

| block | seed cov. / acc. | balanced cov. / acc. | balanced total diff |
|---|---:|---:|---:|
| Final demand (402 × 19) | 90.0% / 66.0% | 89.9% / 50.9% | 0.071% |
| Value added (6 × 402) | 99.9% / 79.4% | 99.9% / 67.7% | 0.000% |
| Intermediate interior (402 × 402) | 100.0% / 100.0% | 100.0% / 57.2% | 0.063% |
| Domestic output interior (402 × 402) | 100.0% / 99.6% | 100.0% / 74.5% | 0.009% |
| Supply bridge (402 × 12) | 99.5% / 76.1% | 99.4% / 49.5% | 0.018% |

⚠️ **The two columns answer different questions, and the drop between them
is not degradation.** The seed interiors reproduce published 2017 because at
the benchmark year they are seeded from it — their near-perfect scores test
plumbing, not estimation. The balance then moved interior cells to close
the supply-equals-use identity exactly and absorb the seed's real gaps, so
the balanced tables trade cell-level agreement with the published benchmark
for exact identities. Where the balance put that reconciliation is visible
in the difference between the two figure sets below.

### 1.1 Initial Seed Tables

Figures 1-5 show the seed state. The blocks scored against an answer they
never saw — final demand, value added, the supply bridge — carry the real
information; the two interiors are near-circular at 2017 and their scores
certify only that the build is unbroken.

![Seed final demand](images/use_fd_detail_sut_seed_2017.png)

*Figure 1. Seed final-demand columns (commodity × final-demand code,
purchaser price) against the published 2017 detail Use table. Coverage
90.0%, accuracy 66.0%. The inventory-change and export columns carry nearly
all misses; the twelve government columns land cell for cell.*

![Seed value added](images/use_va_detail_sut_seed_2017.png)

*Figure 2. Seed value-added rows (six rows × 402 industries) against the
published 2017 detail Use table. Coverage 99.9%, accuracy 79.4%. All row
totals match; the shortfall is one row — taxes on products, whose industry
split places tax with producers where the published row places it with
sellers (wholesale and retail).*

![Seed intermediate interior](images/use_intermediate_detail_sut_seed_2017.png)

*Figure 3. Seed intermediate interior (402 commodities × 402 industries)
against the published 2017 detail Use table. Coverage and accuracy 100.0% —
a plumbing certification, since the 2017 seed carries the published interior
by construction. The estimation content of this block is scored in section
6, where 35.7% of its dollars are observed by an annual source.*

![Seed domestic output](images/supply_output_detail_sut_seed_2017.png)

*Figure 4. Seed domestic output block (402 × 402, basic price) against the
published 2017 detail Supply table. Coverage 100.0%, accuracy 99.6%. Only
~5,000 of 161,604 cells are populated on either side — an industry makes a
handful of commodities — and both-empty cells count as absent, not matches.
Near-circular at 2017: the same detail mix appears on both sides.*

![Seed supply bridge](images/supply_bridge_detail_sut_seed_2017.png)

*Figure 5. Seed supply bridge (402 commodities × 12 bridge codes: imports,
margins, taxes and their subtotals) against the published 2017 detail Supply
table. Coverage 99.5%, accuracy 76.1%. The two margin columns net to
exactly zero by construction and cannot be scored by their column totals;
the import column's remaining error is concentrated in ~50 crosswalk
decisions on goods commodities.*

### 1.2 Balanced RAS Tables

Figures 6-10 show the same five blocks in the final balanced state — the
tables the rest of the pipeline consumes. Totals and identities are exact
everywhere; the amber that appears relative to the seed figures marks where
the balance placed the reconciliation.

![Balanced final demand](images/use_fd_detail_sut_2017.png)

*Figure 6. Balanced final-demand columns against the published 2017 detail
Use table. Coverage 89.9%, accuracy 50.9%. Compare Figure 1: the balance
spread part of the inventory and export reconciliation across otherwise
matching columns while closing every imposed column total exactly.*

![Balanced value added](images/use_va_detail_sut_2017.png)

*Figure 7. Balanced value-added rows against the published 2017 detail Use
table. Coverage 99.9%, accuracy 67.7%; the grand total agrees to 0.000%.
Compare Figure 2.*

![Balanced intermediate interior](images/use_intermediate_detail_sut_2017.png)

*Figure 8. Balanced intermediate interior against the published 2017 detail
Use table. Coverage 100.0%, accuracy 57.2%. The drop from Figure 3's 100%
is the balance absorbing the seed's supply-use gap — a fitted 2.3% before
balancing — into the interior while closing the identity exactly for every
commodity.*

![Balanced domestic output](images/supply_output_detail_sut_2017.png)

*Figure 9. Balanced domestic output block against the published 2017 detail
Supply table. Coverage 100.0%, accuracy 74.5%; the grand total agrees to
0.009%. Compare Figure 4.*

![Balanced supply bridge](images/supply_bridge_detail_sut_2017.png)

*Figure 10. Balanced supply bridge against the published 2017 detail Supply
table. Coverage 99.4%, accuracy 49.5%; the grand total agrees to 0.018%.
The trade-margin rows dominate the amber: the balance moved margin mass
between commodities to close the purchaser-value identities.*

## 2. Annual Comparison to Published Summary Tables

The published summary Supply and Use tables (71 industries) are the only
tables available for every year of the span, so they are the standing annual
diagnostic: the balanced detail tables are aggregated to summary on the
sector correspondences and compared to the published summary tables for the
same year. Close-not-exact is the design — the balance held its own
observed aggregates, not every published summary cell, and from 2022 the
detail mix deliberately follows the 2022 Economic Census where it disagrees
with the published tables. Table 2 summarises the span; Figures 11-24 show
each year.

*Table 2. The balanced tables aggregated to summary against the published
summary tables, per year. Accuracy is the share of populated cells within
1%; total difference is the grand total against published.*

| year | Supply cov. | Supply acc. | Use cov. | Use acc. | Supply total diff | Use total diff |
|---:|---:|---:|---:|---:|---:|---:|
| 2017 | 99.2% | 55.5% | 99.6% | 41.5% | 0.012% | 0.012% |
| 2018 | 99.0% | 50.0% | 99.5% | 15.6% | 0.011% | 0.009% |
| 2019 | 98.7% | 42.2% | 99.5% | 12.6% | 0.058% | 0.008% |
| 2020 | 98.4% | 25.0% | 99.5% | 7.8% | 0.067% | 0.185% |
| 2021 | 98.1% | 17.5% | 99.1% | 6.9% | 0.340% | 0.586% |
| 2022 | 98.4% | 12.0% | 98.7% | 6.3% | **1.033%** | **1.038%** |
| 2023 | 98.4% | 35.9% | 98.6% | 6.3% | 0.017% | 0.016% |

Three things the span says. **Coverage holds at ~99% every year** — the
nowcast populates what the published tables populate. **Cell-level
agreement decays with distance from the anchor**, which is two real
estimates diverging rather than one degrading: our interior moves on census
and survey evidence, the published tables move on BEA's own methods.
**Grand totals stay within 0.07% through 2019, spread to ~1% in 2022** —
the census-conditioning year — **and close back to 0.02% at 2023.**

![Supply summary 2017](images/supply_summary_sut_2017.png)

*Figure 11. Balanced Supply aggregated to summary vs published, 2017.
Coverage 99.2%, accuracy 55.5%.*

![Use summary 2017](images/use_summary_sut_2017.png)

*Figure 12. Balanced Use aggregated to summary vs published, 2017.
Coverage 99.6%, accuracy 41.5%.*

![Supply summary 2018](images/supply_summary_sut_2018.png)

*Figure 13. Supply at summary vs published, 2018. Coverage 99.0%, accuracy
50.0%.*

![Use summary 2018](images/use_summary_sut_2018.png)

*Figure 14. Use at summary vs published, 2018. Coverage 99.5%, accuracy
15.6%.*

![Supply summary 2019](images/supply_summary_sut_2019.png)

*Figure 15. Supply at summary vs published, 2019. Coverage 98.7%, accuracy
42.2%.*

![Use summary 2019](images/use_summary_sut_2019.png)

*Figure 16. Use at summary vs published, 2019. Coverage 99.5%, accuracy
12.6%.*

![Supply summary 2020](images/supply_summary_sut_2020.png)

*Figure 17. Supply at summary vs published, 2020. Coverage 98.4%, accuracy
25.0%.*

![Use summary 2020](images/use_summary_sut_2020.png)

*Figure 18. Use at summary vs published, 2020. Coverage 99.5%, accuracy
7.8%.*

![Supply summary 2021](images/supply_summary_sut_2021.png)

*Figure 19. Supply at summary vs published, 2021. Coverage 98.1%, accuracy
17.5%.*

![Use summary 2021](images/use_summary_sut_2021.png)

*Figure 20. Use at summary vs published, 2021. Coverage 99.1%, accuracy
6.9%.*

![Supply summary 2022](images/supply_summary_sut_2022.png)

*Figure 21. Supply at summary vs published, 2022 — the census-conditioning
year, and the widest divergence of the span. Coverage 98.4%, accuracy
12.0%.*

![Use summary 2022](images/use_summary_sut_2022.png)

*Figure 22. Use at summary vs published, 2022. Coverage 98.7%, accuracy
6.3%.*

![Supply summary 2023](images/supply_summary_sut_2023.png)

*Figure 23. Supply at summary vs published, 2023. Coverage 98.4%, accuracy
35.9%.*

![Use summary 2023](images/use_summary_sut_2023.png)

*Figure 24. Use at summary vs published, 2023. Coverage 98.6%, accuracy
6.3%.*

## 3. Industry Output Against the Published Series

Industry output computed from the nowcast tables (Make-table row sums,
producer prices) against the published BEA detail gross-output series, and
against the census-conditioned output panel the balance was actually asked
to hit. The two comparisons separate deliberate census conditioning from
drift: a year can sit 2% off the published series while sitting 0.2% from
its own target panel. Distribution statistics are over industries the
published series carries at $100M or more. Table 3 shows the span; Figure
25 shows the industry-level structure.

*Table 3. Nowcast industry output vs the published BEA detail gross-output
series, 2017-2023. Weighted difference is the sum of absolute per-industry
gaps over total output.*

| year | nowcast $B | published $B | total diff | weighted diff | median \|diff\| | max \|diff\| (industry) | industries >5% off | vs target panel |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2017 | 34,468 | 34,468 | 0.00% | 0.00% | 0.00% | 0.1% (`335912`) | 0 | 0.00% |
| 2018 | 36,509 | 36,505 | +0.01% | 0.06% | 0.00% | 0.9% (`452000`) | 0 | 0.06% |
| 2019 | 37,664 | 37,658 | +0.01% | 0.06% | 0.00% | 0.9% (`452000`) | 0 | 0.06% |
| 2020 | 36,744 | 36,715 | +0.08% | 0.10% | 0.01% | 4.7% (`S00201`) | 0 | 0.10% |
| 2021 | 41,902 | 41,833 | +0.17% | 0.19% | 0.01% | 10.9% (`S00201`) | 2 | 0.19% |
| 2022 | 46,706 | 46,611 | +0.20% | **2.20%** | **2.26%** | **38.1%** (`54151A`) | **118** | 0.23% |
| 2023 | 48,558 | 48,540 | +0.04% | **2.11%** | **2.24%** | **38.0%** (`54151A`) | **112** | 0.06% |

**The story splits at 2022, and both halves are the intended behaviour.**
Through 2021 the nowcast tracks the published series to 0.06-0.19% weighted
— the median industry is within 0.01% — and the only notable outlier is
state-and-local transit, a pandemic-recovery disagreement. In 2022-2023 the
weighted difference jumps to ~2.2% with over a hundred industries more than
5% apart: the 2022 Economic Census conditioning, under which the detail mix
follows the census rather than the carried-forward 2017 split. The final
column proves the distinction — against the census-conditioned panel, every
year stays at or under 0.23%. The largest 2023 divergences are the census
corrections by name: scientific R&D services +13.7% (+$65B), advertising
−18.3%, architectural and engineering −7.6%, pharmaceutical preparations
−13.4%; the largest relative move is other computer services including
facilities management at +38%.

![Industry output vs the published series](images/go_vs_nowcast_mut.png)

*Figure 25. Nowcast industry output vs the published BEA gross-output
series. Left: every scored industry's percent difference, one jittered
column per year — blue above the published series, red below, dashed guides
at ±5%. The 2017-2021 columns sit flat at zero; the 2022 fan-out is the
census conditioning arriving, and 2023 keeps its shape. Right: the largest
2023 divergences in dollars, with the percent difference in each label —
professional services, wholesale, and pharmaceuticals, the industries where
the census disagrees most with the carried-forward detail mix.*

## 4. Conversion to Make-Use Tables

The balanced Supply-Use tables convert each year to the four Make-Use
products — the Make table, the producer-price Use table, the import matrix,
and a margins table detailed by buyer, commodity, and supplying margin
commodity. Every conversion rule was developed against the published 2017
tables and validated a second time on the published 2012 benchmark, which
shares the 2017 commodity basis; a rule that fits one benchmark is a fit,
one that holds on two is treated as an identity. Table 4 summarises the
conversion's validation.

*Table 4. Make-Use conversion validation. Benchmark replays reconstruct the
published tables from the published Supply-Use inputs; production gates run
on every year's build and refuse to save on breach.*

| check | result |
|---|---|
| Producer-price Use replay, 2017 | interior and final demand reproduce published to $0M per cell |
| Imports column rule (goods, adjustment, duties with the customs credit) | every commodity within tolerance at 2017 and 2012 |
| Import matrix row control | exact at both published benchmarks |
| Margins layered identities (basic → producer → purchaser) | close at $0.00 per cell at 2017, 2012, and 2012 on the 2017 anchor |
| Cost of the frozen 2017 margin rates, measured at 2012 | 2.06% of goods mass; 25.0% of margin-row placement |
| Production gates, all six years 2018-2023 | buyer totals preserved, margin mass conserved to $0.000M, value-added collapse exact, import allocation exact — all at a $1M bar |

All four tables are stored per year, before and after redefinitions, in the
shared artifact store, and the model build reads them through a configurable
loader that can also serve the published 2017 tables unchanged.

## 5. Redefinitions: Before to After

Redefinitions move secondary production — an activity like a manufacturer's
retail outlet — from the industry that happens to house it to the industry
that primarily produces it, leaving every commodity total unchanged. The
method was chosen by measurement against the published 2017
before-and-after pair and an out-of-sample test on the published summary
tables for 2018-2024. Its anatomy: 1,850 of the 1,880 Make cells that move
at 2017 transfer at ~100% of the cell, so the Make side applies that
whole-cell pattern to each year's own cells; the Use side carries each
cell's 2017 after-to-before ratio and closes every commodity row back to
its before-redefinitions total; final demand crosses unchanged (measured
exactly invariant); imports re-allocate along the moved purchases; each
margins record scales with its own transaction. Table 5 summarises the
validation.

*Table 5. Redefinitions validation: the 2017 replay against the published
after-redefinitions tables, and the out-of-sample span on published summary
tables.*

| check | result |
|---|---|
| 2017 replay, worst cell | Make $19M · Use interior $5.3M · value added $15.8M — publication-rounding scale |
| Final demand across redefinitions | $0 on a $25.8T block (exactly invariant) |
| Out-of-sample 2018-2024, Make | residual 0.05-0.13% of table mass (doing nothing: 7.5-7.7%) |
| Out-of-sample 2018-2024, Use | residual 0.26-0.95% (doing nothing: 2.5-3.4%) |
| Margins commodity totals across redefinitions | conserved to 0.06-0.28% per value column |
| Identity gates on every year's build | commodity output invariant, commodity rows invariant, columns closed on the moved output |

The one measured limitation: the published margins table re-books about
2.8% of its mass through transaction records a proportional carry cannot
create; our margins remain exactly coherent with our own Use table, which
is what the downstream build requires.

## 6. Seed Data Provenance and Quality

Match scores against 2017 say whether a block reproduces the benchmark;
they do not say where its **annual movement** comes from. Every populated
cell is classified by provenance: **primary** (a source observes this
cell), **allocated** (a source observes an aggregate containing it, spread
by a weight), or **carried** (no annual source; the cell holds its 2017
structure). Table 6 summarises the blocks.

*Table 6. Provenance of the seed blocks. "Observed $" counts primary plus
allocated dollars; "primary $" is the share a source observes cell-level;
median k is how many cells share one source datum.*

| block | populated cells | observed $ | primary $ | median k |
|---|---:|---:|---:|---:|
| Final demand | 1,258 | 100% | 48.1% | 4 |
| Value added | 1,586 | 100% | 7.9% | 29 |
| Domestic output mix | 5,080 | 100% | 20.4% | 14 |
| Supply bridge | 1,804 | 85.4% | 0.0% | 4 |
| Intermediate interior | 44,281 | 35.7% | — | 7 |

The intermediate interior is the informative row: 35.7% of its dollars are
observed by an annual survey or census, concentrated where the evidence is
strongest — manufacturing at 71.4% observed with half of that cell-specific
(the Economic Census materials detail) — while construction, trade, and
government (24% of the block) carry no annual observation of their input
mix, each a measured verdict with a recorded reopening condition rather
than unattempted work. A match score of 100% can coexist with one source
datum spread over 400 cells; reliability and specificity are scored
separately for exactly that reason.

## 7. Conclusions and Next Steps

**The pipeline is complete and its products exist.** Annual detail Supply
and Use tables for 2017-2023 balance exactly under the accounting
identities; each year converts to the four Make-Use products before and
after redefinitions; all artifacts are stored and loadable by the model
build. Every conversion rule that could be graded on a published benchmark
was graded on two, and every production run is gated by identity checks
that refuse to save on breach.

**What the comparisons establish.** At the 2017 anchor, totals and
identities are exact and the balanced interior sits measurably away from
the published benchmark precisely where the balance absorbed the seeds'
real gaps. Across the span, the tables track the published summary
aggregates within about 1% at worst — and the largest divergences are
deliberate: from 2022, the detail industry mix follows the 2022 Economic
Census, an observation the published series has not yet incorporated, with
over a hundred industries corrected by more than 5%.

**Next steps.**

1. **Diagnose what the balance moved.** The seed-versus-balanced figure
   pairs in section 1 localise the reconciliation; the standing task is to
   attribute it — including a cell-level comparison of the soft and hard
   balancing protocols before any downstream emission-factor difference is
   interpreted.
2. **Route the model build onto the nowcast tables** and regenerate the
   model diagnostics against the current release, completing the path from
   annual tables to annual emission factors.
3. **Refine the two measured conversion limitations**: the import valuation
   basis for nowcast years, and the year-specific split of sales taxes out
   of trade margins.
4. **Phase 2**: the 2025 tables after BEA's annual update, a retrospective
   refresh of 2018-2024 on the revised vintage, and the reallocation of the
   remaining government enterprises into the private industries they
   produce in.
