# What the 2017 final-demand match picture shows

Findings from `bedrock/analysis/nowcasting/table_match.py` and
its sections. Reference docs live in [README.md](README.md); this file is what
the first run actually said.


First run of `use_fd_detail_sut`, candidate read from the last CSV export of
`derive_initial_Y_pur`, reference the published `Use_SUT_Framework_2017_DET.xlsx`,
tolerance `rtol=0.013, atol=5e5, ramp=0.25`.

```
            absent  match  partial  miss  extra
cells         6385    181      375   697      0
row_totals      22      7      290    83      0
col_totals       0      7        4     8      0

coverage  44.4%   accuracy  32.6%
grand total  ours 1.374e13  vs  reference 2.224e13   (off by 38.2%)
residual outside the frame  6,227,917,314,385  on 14 rows, not drawn
```

## The grand total is not the story, and neither is any single scalar

The build reaches 44% of the cells the reference populates, and lands a third of
those within 1.3%. The grand total is off by 38%. But the 38% and the 44% are
not two views of one problem — they decompose into four separate ones, and the
picture separates them where a total cannot.

## 1. $6.23 trillion is parked on NAICS codes, outside the BEA code space

Fourteen candidate rows are not BEA 2017 detail commodities at all:

| code | ours (USD) | code | ours (USD) |
|---|---|---|---|
| `531110` | 2,020,853,000,000 | `238110` | 376,761,200,966 |
| `923110` | 1,737,213,000,000 | `922110` | 86,185,314,385 |
| `928110` | 978,501,000,000 | `562*` (8 codes) | 37,342,000,000 |
| `236115` | 600,475,537,255 | | |
| `237110` | 399,594,261,779 | | |

These are NAICS codes standing where BEA detail commodities belong — the
`bea_code_space` defect the plan describes, showing up as raw dollars. They are
reported as a **residual** rather than drawn, because the picture is pinned to
the reference's code space; `TableMatch.residual` carries the total so it cannot
go unnoticed. Note that `923110` at 1,737,213,000,000 is *exactly* the reference's
`F10C00` column total, and `928110` sits against `F06C00`/`F07C00` — the
government columns landed intact, on the wrong codes.

$6.23T on a $13.3T reference: this residual alone is larger than the grand-total
gap it is partly causing.

## 2. 697 cells are misses, and they are structural, not scattered

Whole columns are empty on our side — eight of nineteen column totals are
`miss`, not `partial`:

`F03000`, `F04000`, `F06C00`, `F06S00`, `F07C00`, `F07S00`, `F10C00`, `F10S00`.

Change in private inventories and exports are known-unsourced (#529, #526) and
render as solid purple bands. The other six are government columns, and they are
item 1 wearing a different hat: their mass exists, on NAICS codes. The two
structures columns that *are* populated, `F02R00` and `F02S00`, are short by
1.07e12 between them while the three construction NAICS rows in the residual
(`236115`, `237110`, `238110`) hold 1.38e12 — the same story one step less
cleanly.

**373 of 402** commodity row totals are outside tolerance — 290 partial and 83
outright misses, with only 7 landing inside. On the other axis, **12 of 19**
column totals are outside, of which 8 are misses. Reading the two strips
together is the point: the interior's 697 misses are not spread evenly, they
pile into whole columns and then into whole rows within the columns that do
have data.

## 3. `S00900` has the right magnitude and the wrong sign

`S00900 / F01000`: ours +200,997,000,000 against a reference of
−200,997,000,000, a relative error of exactly 2.0. `7a04a71` added `negate_flows`
for this and the fix is in the config; this export predates it taking effect.

## 4. `F01000` is amber almost everywhere, not green

Personal consumption is the column with the most cells populated on both sides,
and nearly all of them sit at the far end of the yellow ramp rather than near
the tolerance boundary. Column total −17.3%. This is the per-commodity split
problem, not a coverage problem — and it is the one the totals check is least
able to see, because the column total being 17% off understates cells that are
individually much further off in both directions.

## The live candidate does not currently run

`derive_initial_Y_pur(2017)` raises `KeyError: ['ActivityProducedBy'] not in
index`. `NIPA_FD_2017.yaml` sets `attribute_on: ['PrimarySector',
'ActivityProducedBy']` on the PCE activity sets, but the `retain_activity_columns`
plumbing in `flowby.py` that keeps that column alive into the attribution source
landed in `9668ce7`, was reverted from main in `42f7e59`, and was **not** part of
the `7a04a71` salvage. So the config asks for a column the framework no longer
retains.

That is a Step 1 defect, not a defect in this diagnostic. Until it is fixed the
section reads the CSV export; switching back is one line —
`Section.candidate = initial_Y_pur_candidate`.

Because the export predates the current config, the numbers above are a snapshot
of that run, not of `nowcast` HEAD. Re-run once the FBS runs again.
