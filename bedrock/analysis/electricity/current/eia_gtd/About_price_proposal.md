# Proposal: price the kWh before allocating the dollars

**Status: draft for discussion.** Pairs a revision of the electricity G/T/D
disaggregation with a revision of the nowcast electricity Use column, because
neither can be fixed alone — the allocator's weights and the Use row it allocates
over disagree, and each is currently used to justify the other.

Companion measurement: `About_electricity_shares.md` (annual three-source
shares), issue #894, PR #892 (the integration this would revise).

## The defect

`allocate_purchaser_gtd` converts MWh to dollars with **one price for every
purchaser in the economy**:

```python
p = (p_share_2017 * electricity_purchases_total) / egrid_mwh   # a scalar
...
proportional = mwh.loc[members] * p
```

Inside manufacturing the MWh are distributed by MECS Table 7.7 kWh shares, so a
sector's generation dollars are `its kWh share × the average price`. Where a
sector actually buys cheap power, that product exceeds what it spends on
electricity in total, water-fill caps it, and the excess is pushed onto other
purchasers.

This is measured, not hypothesised. On the 2024 nowcast purchases, 44
manufacturing sectors clip under MECS weights and **zero** clip under dollar
weights, and the clipped sectors are the cheap-power ones — median 5.58 ¢/kWh
against 8.68 ¢/kWh for the rest.

⚠️ Those counts used the published `p_share`/`td_share`, not #892's re-anchored
ones, and the prices behind them were Census-derived. Both need redoing on the
re-anchored shares and on MECS Table 7.2 before the magnitudes are quotable. The
sign and the mechanism are not in doubt.

## What the data says a price should look like

**Manufacturing prices vary, and MECS publishes them.** Table 7.2 gives
$/MMBtu by NAICS and Table 7.10 gives expenditure in dollars; 7.2 and 7.10÷7.7
agree to within 0.124 ¢/kWh on all 82 published rows, and the 31-33 aggregate
lands at 6.86 ¢/kWh against EIA's published industrial average of 6.92. The
published detail runs from 4.21 ¢/kWh (cyclic crudes) to 11.83 (asphalt paving),
with the heavy continuous-process industries cheap — newsprint 4.43, nonferrous
smelting 4.68, inorganic chemicals 4.71, iron and steel 4.95 — and light
discrete assembly dear: apparel 9.97, furniture 10.02, machinery 10.23.

**Commercial prices barely vary.** CBECS 2018 Table C13 gives consumption and
expenditure by principal building activity; across the thirteen activities the
implied price spans only **9.19 to 11.22 ¢/kWh, a 1.22× spread**, against an
all-buildings 9.95. Manufacturing's spread is 1.8× at the deciles and wider at
the extremes.

That asymmetry is the design input:

| | within-class price spread | so a single class price is |
|---|---|---|
| Commercial | 1.22× | **defensible** |
| Manufacturing | 1.8× at deciles, 4.21-11.83 full | **not defensible** |

And the between-class gap is larger than either: commercial 9.95 against
manufacturing 6.86, a 45% premium that one economy-wide `p` cannot represent.

## Proposal

**A. Make `p` vary by end-use class.** EIA Table 2.4 publishes price by end-use
class and the repo already knows this is missing — `decompose_d_n_step` states
"there is **no** Table 2.4 class-varying `c_j = λ / p_j`". This alone should fix
most of the commercial and residential side, where CBECS says one price per
class is close to right.

**B. Inside manufacturing, price the kWh from MECS Table 7.2, then allocate
dollars.** Replace `gen = mwh × p` with `gen = mwh × p_naics`, using the deepest
published MECS row for each IO sector and falling back to its parent. Cost of the
fallback is measured: kWh-weighted mean deviation of a six-digit child from its
three-digit parent is **12.0%**, and 92.5% of manufacturing kWh sits in children
within 25% of their parent — small against the 1.8× spread it captures. Sectors
on a fallback should be flagged, not silently inherited.

**C. Reconcile the Use column against the priced kWh, rather than allocating
over it unexamined.** With a price, MECS kWh becomes MECS dollars, directly
comparable to the Use electricity row. Today the two disagree by 15.6-20.2pp of
the manufacturing total with no reconciliation step and no report. Priced, the
disagreement becomes a residual that can be attributed — to the survey, to the
nowcast, or to a genuine scope difference — instead of being silently resolved in
the allocator by whichever source happens to be used.

**D. Make water-fill an exception, not a mechanism.** With the right price,
clipping should be rare. If 44 sectors still clip after A-C, that is a finding
about the Use column, and should be surfaced rather than absorbed. In particular
**47 sectors currently end with zero T&D dollars** — modelled as buying
generation and no transmission or distribution, which no real purchaser does.

## Why the nowcast column has to move in tandem

Fixing only the allocator hides the other half. Three defects sit in the Use
column itself and none is caused by MECS:

- **322120 Paper mills carries $0 electricity in the pinned MUT
  `v0.3.0_4276083`, in 2021 and 2023** ($0.722bn / **$0** / $0.908bn / **$0** /
  $0.744bn over 2020-2024). Among the most electricity-intensive manufacturers.
- **324110 refineries swing 5.72% to 12.14% of manufacturing electricity and
  back** across 2017-2024, while both independent surveys stay inside 4.8-7.0%.
- The IO-vs-MECS gap **widens across the nowcast span**, 15.62pp to 20.22pp, on a
  MECS side that is frozen — all of 2018-2024 rides the 2022 survey.

A price-based allocator sitting on that column would attribute its instability to
the electricity method.

## Open questions

1. Does a class-varying `p` break the eGRID class-MWh targets, or do they
   already accommodate it?
2. Which is right where MECS and Census disagree most? At fine detail they
   diverge badly — semiconductors 6.36 ¢/kWh on MECS against 14.70 from Census,
   on an identical kWh denominator, so the two surveys differ 2.3× on the dollars
   alone. Three-digit rows agree far better (paper −3.9%, chemicals −3.0%).
3. Non-manufacturing, non-commercial industrial — agriculture, mining,
   construction — has neither a MECS nor a CBECS price. Class price, or
   something better?
4. MECS is quadrennial. Interpolate prices between surveys, or hold?

## Work required before this is actionable

- ⚠️ **Add Table 7.2 and 7.10 to the MECS extractor.** Both are commented out of
  the `tables:` list in `EIA_MECS_Energy.yaml`; 7.10 has a 2018 layout but
  **neither has a 2022 layout**, and the 2022 row offsets differ from 7.7's by
  one row, so the ranges must be verified against the workbook rather than
  derived. Until then the only 7.10 data on hand is in the superseded 2018
  parquet `EIA_MECS_Energy_2018_v2.0.0_c31283d`.
- Add CBECS C13 as a source for the commercial price. ⚠️ **CBECS has no NAICS at
  any level** — it classifies by principal building activity and the 2018
  microdata codebook contains zero NAICS references — so it can supply a
  commercial *price*, which is a tariff-class property, but it cannot supply a
  commercial *allocation* by industry. A building is not an establishment, and a
  multi-tenant office cannot be resolved to NAICS by any mapping.
- Re-run the clip and reassignment counts under #892's re-anchored shares.
