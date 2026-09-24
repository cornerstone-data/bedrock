# What the census materials seed can and cannot observe

Findings behind the guard in `materials_seed` and the `--unobserved` check in
`inputs_structure.py`. Everything here was measured off the stored MUT vintage
`v0.3.0_4276083` and the Step 3 modules directly; no model rebuild was needed.

Filed as [#987](https://github.com/cornerstone-data/bedrock/issues/987),
[#988](https://github.com/cornerstone-data/bedrock/issues/988),
[#989](https://github.com/cornerstone-data/bedrock/issues/989) and
[#991](https://github.com/cornerstone-data/bedrock/issues/991); the trigger was
[#922](https://github.com/cornerstone-data/bedrock/issues/922).

## The seed multiplies a slow base by a fast index

```
seed[c, i] = Use2017[c, i] * census_mix[c, i, year] / census_mix[c, i, 2017]
```

BEA supplies the level and the census supplies the movement. Measured on the
census-covered commodity set (213 commodities x 227 industries), median column
mix churn:

| series | median | p90 |
|---|---:|---:|
| BEA benchmark 2007 -> 2012 | 6.9pp | 13.0 |
| BEA benchmark 2012 -> 2017 | 8.5pp | 16.8 |
| census 2012 -> 2017 | 29.8pp | 56.0 |
| census 2017 -> 2022 | 11.2pp | 27.9 |

**BEA's own benchmark interior moves about 3.5x less than the census it
documents as its source.** BEA 2017 sits 8.5pp from BEA 2012 and 39.5pp from
census 2017, and **97% of columns are closer to the prior benchmark than to the
contemporaneous census**. Table C2 of `bea_2017_benchmark_sources.md` says why:
manufacturing inputs come "primarily from 2017 Economic Census manufacturing
sector reports", but "inputs were also **interpolated using the 2007 benchmark
I-O estimates and ASM data**".

⚠️ So a legacy-anchored level is being multiplied by a much more volatile index,
and nothing reconciles the two rates. That mismatch is the likely root cause of
the extreme outcomes below, and whether census movement should be damped toward
BEA's own observed rate of structural change is open on #987.

⚠️ **Compare like with like when repeating this.** A first pass put the
BEA-to-census distance at 52.2pp by scoring BEA's full 402-commodity column
against the census's 213, which diluted BEA's shares with services MATFUEL never
covers. 39.5pp is the number.

## Flat median, exploding tail — three times over

Every axis tested behaves the same way: the typical cell is unaffected and the
tail is where the damage is. None of these is a signal about the central case.

| axis | median index | tail |
|---|---|---|
| base cell size (2017 census share) | ~0.95 in **every** band | p99 2.05x at >10% base, **56.8x** below 0.01% |
| base agreement (BEA vs census 2017) | — | p99 **73.9** where BEA > 3x census, **5.2** where they agree, *within* the same size band |
| column thinness (material rows) | 8.8pp at 1-3 rows vs 12.9pp at >20 | p90 **68.6** at 1-3 rows vs 28.6 at >20 |

**A gate here should be shaped to catch a tail, never to shift a middle.** Note
the second row: a *small* cell both sources agree is small behaves fine. It is
disagreement, not smallness, that predicts the tail.

## What causes the extreme columns

Sixteen columns reallocate more than 20pp with a single donor supplying over 70%
of the outflow. Testing four candidate causes:

- ❌ **Our BEA concordance is not one.** Of the 276 material codes present in
  both vintages, **0** change their BEA commodity assignment.
- ❌ **Census code-list churn explains exactly one.** 291 of 291 codes shared,
  one new in 2022 (`31122020`), which is `311221` wet corn milling.
- ⚠️ **Suppression recovery fabricates two outright.** `336414` and `33641A`
  show 94.1pp and 73.4pp of churn with recovery on and **exactly 0.0pp** with it
  off. This is what the guard addresses.
- ✅ **The rest is observed.** Median recovery share across the other fourteen is
  0%, several negative (recovery *reduces* their churn, which is it working).

## The guard

`columns_without_observed_mix` holds any column with fewer than
`MIN_OBSERVED_MATERIAL_ROWS` unsuppressed material rows in either vintage. With
one observed row a column's mix is 100% on that row by construction, so its
2017 -> 2022 "movement" compares two numbers that were never measured.

Five columns qualify — `311221`, `331313`, `336414`, `33641A`, `336991` —
carrying **0.85%** of the seeded columns' BEA intermediate mass ($30bn of
$3,538bn). Four of the five carry 25-94pp of churn that has no observed basis;
`336991` moves 2.6pp and costs nothing either way.

⚠️ **Count on the reconciled `_unit` basis, not bare NAICS.** Counting on bare
NAICS wrongly adds the NAICS-2022 merge pairs (`336111`/`336112` and others),
whose 2017-basis codes have no 2022 rows *because they merged*.
`_common_industry_basis` reconciles that and they are not a gap. That mistake
inflates the held set from 5 columns to 15.

This is deliberately the *only* change made to the seed. It removes movement
that provably has no observed basis, which is the one remedy that does not need
an out-of-sample score — and per #989 there is not one.

## Why there is no holdout

`Census_EC_MatFuel` **does** have a 2012 vintage (7,488 rows, 393 industries,
$6,260bn) and it places onto 252 BEA commodities against 2017's 213. The
`mining_seed` docstring previously said it did not; that has been corrected.

But the span cannot grade a mix. Only **132 of 1,274** material codes are shared
with 2017 (against 291 of 291 across 2017 -> 2022), a leading-digit rollup
recovers just **26.7%** of 2012 mass with 700 codes matching nothing, and median
column churn is **30.4pp** against 11.2pp on the operational span. Scored there,
the census seed comes out **133% worse** than a frozen mix — which grades the
taxonomy break, not the seed.

⚠️ **Reach is not distribution.** The 2012 holdout initially looked viable
because 2017's BEA commodity reach is a strict subset of 2012's, with the
only-2012 remainder at 0.4% of mass. That shows the axes match and says nothing
about how mass spreads across them, which is the entire content of a mix
comparison. Do not clear a holdout on reach.

## Three censuses: 2017 is the anomalous vintage

The 2012 vintage cannot grade a mix, but it can still answer a narrower
question — for one cell, which of three points is the odd one out. Magnitudes
from 2012 are unreliable; 2012 and 2022 agreeing *against* 2017 is not.

Eleven cells across nine columns show the same shape: **2017 inflates the donor
and starves the recipient while 2012 and 2022 agree with each other.** The
clearest is `336411`, which books **69.61%** of its materials as aircraft
engines in 2017 against **5.23%** in 2012 and **4.11%** in 2022.

Since the seed interpolates 2017 -> 2022 and holds 2022, this means the movement
it encodes is largely 2017 reverting to the 2012 norm — **spurious movement with
a probably-correct endpoint**. The defect sits in the 2017 base, not the 2022
target.

⚠️ Our recovery causes only two of the nine (`334517` and `33451A`, where it
over-allocates 6-7x). For the other seven the *published* 2017 value is the
outlier and we can only decline to read movement off it.

## Open, and deliberately not addressed here

- `331410` scrap (14.5 -> 10.9 -> 31.8%) and `325910` ink resins
  (13.2 -> 14.3 -> 37.7%) are accepted as real pending external corroboration.
- `333111` farm machinery buying iron and steel (20.1 -> 24.9 -> **7.8%**) moves
  the *wrong way against prices* — a nominal share should not collapse in the
  year steel spiked. Treated as a defect candidate; the general instrument is a
  price-consistency test rather than a one-off exclusion.
- ⚠️ 2017-outlier cells fail #987's base-agreement gate **less** often (29.1% vs
  47.2%, lift 0.62x) and the cause is unexplained. BEA's inertia predicts the
  opposite sign. The likely confound is that the three-point classifier requires
  the 2012 and 2022 censuses to agree within 50%, which preferentially selects
  large, stably-measured cells.
