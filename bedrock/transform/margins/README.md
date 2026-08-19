# margins

FBS methods for the Supply table's margin columns (nowcast step 4c, #571).

| method | years | output |
|---|---|---|
| `Margins_Transport_<year>` | 2017–2022 | the `TRANS` column, split by the transport commodity that gives the margin up |

`Margins_Trade_<year>` belongs here too and is not built yet (phase 3).

## What `Margins_Transport` produces

One row per (receiving commodity, mode):

| column | value |
|---|---|
| `SectorProducedBy` | the BEA 2017 commodity that **receives** the margin |
| `SectorConsumedBy` | the transport commodity that **gives it up** — `481000` air, `482000` rail, `483000` water, `484000` truck, `486000` pipeline |
| `Flowable` | that commodity's name, e.g. `Rail transportation` |
| `FlowAmount` | USD |

Summed over `SectorConsumedBy` this is the Supply table's `TRANS` column.
Kept apart it is the expanded margins file — the transport column broken out
by mode, which is what the FBS exists for.

Only the positive side is here. The negative give-up on each of the five
transport commodities is the column's other half, and is added by
`transport_margin_column()` in
[`nowcast_transport_margins.py`](../iot/nowcast_transport_margins.py), which is
also what enforces `sum(TRANS) = 0` (target T16).

## Where the numbers come from

| mode | share of column | allocator | source |
|---|---:|---|---|
| truck | 67.8% | revenue by commodity group, ten used | `Census_SAS` Table 8 |
| rail | 16.5% | revenue by product, 498 STCC5 codes | `STB_CRSR` |
| pipeline | 11.9% | four margin items to named commodity sets | `Census_SAS` Table 2 |
| water | 2.3% | ton-miles × 1/2/3 difficulty multiplier | `BTS_FAF` |
| air | 1.5% | ton-miles × 1/2/3 difficulty multiplier | `BTS_FAF` |

Each activity set is one mode, keyed by a single `clean_parameter` — the mode
name. That one value selects the allocator before mapping and the control total
after aggregation, so a set cannot be half-configured for one mode and half for
another.

## The division of labour with `nowcast_transport_margins`

The FBS and the module reach the same numbers by different routes, on purpose.

**The module owns each mode's allocator series and its control total.** That is
where the source-reading judgement lives: the suppressed SAS truck group
recovered by subtraction, the "other goods" group BEA discards, the STCC codes
that name a service class rather than a commodity, the difficulty multiplier,
and the 2017-anchored coverage ratio that turns observed freight revenue into a
margin level. None of it is expressible in a method yaml.

**The FBS owns the split from the source's own key to BEA commodities** — ten
SAS groups, 498 STCC codes, four pipeline items and 42 SCTG groups onto 258
receiving commodities — via the crosswalks and proportional attribution against
the published 2017 `TRANS` column.

`test_the_fbs_reproduces_the_module_per_commodity` asserts the two agree, which
is what keeps the division honest.

⚠️ **They agree to about 0.3%, not exactly.** The within-group weight is the
published 2017 transport margin, and the FBS reads it off `BEA_Detail_Supply`'s
`TRANS` column — published in whole millions — while the module sums it out of
the Margins transaction table, which is finer. Same quantity, different
publication rounding. Drift larger than that is a real divergence.

## Crosswalks

The FBS reads three `Sector_Crosswalk_*` files in
[`activitytosectormapping/`](../../utils/mapping/activitytosectormapping/).
They hold **no judgement of their own** — they are re-emissions of the
`Crosswalk_*_to_BEA_2017.csv` files (and, for SCTG, of the ported FAF
crosswalk), written by
[`write_margins_sector_crosswalks.py`](../../utils/mapping/write_margins_sector_crosswalks.py).

Edit a source crosswalk, then re-run:

```
uv run python bedrock/utils/mapping/write_margins_sector_crosswalks.py
```

`test_sector_crosswalks_are_a_faithful_re_emission` fails if you forget.

## Per-year files

Everything except the year lives in `Margins_Transport_common.yaml`; each
source inherits `year` from the method config, so a per-year file is two lines.

⚠️ **`Margins_Transport_common.yaml` must stay ASCII.** The `!include:`
constructor opens files without an explicit encoding, so a non-ASCII character
in an included yaml raises `UnicodeDecodeError` on Windows. That is why the
warnings in it read `CAUTION:` rather than carrying the ⚠️ this repo uses
elsewhere.

## Known limits

⚠️ **The five modes collide per commodity.** Summed, they exceed the published
2017 column on 97 of 258 commodities by 10.9%, essentially all truck. The cause
is that truck's commodity detail comes from a weight blind to the modes already
occupying a commodity. A feasible allocation provably exists (Hall's condition
holds over all 32 subsets), so this is an artifact of independent construction
rather than of the data. Put to BEA on 2026-08-19 — see
[`bea_correspondence.md`](../../analysis/nowcasting/bea_correspondence.md).

In a nowcast year there is no published column to violate, so this does not
block the build: whatever resolves it changes how the positive side is
distributed, not its total, its signs, or the identity.

⚠️ **2023–24 are unsourced.** SAS stops at 2022, which ends truck, pipeline,
water and air; AIES `miscsector` carries 2023 and is not wired; 2024 is
unpublished. Rail alone reaches 2024.

⚠️ **Water and air coverage ratios are 0.478 and 0.584**, not near 1 like the
other three, because their freight revenue includes international legs while the
margin is the domestic leg only. Bounded by their 3.8% share of the column.

The method is documented end to end in
[`margins_estimation_plan.md`](../../analysis/nowcasting/margins_estimation_plan.md).
