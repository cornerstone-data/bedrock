# Which trade rows are out of balance, and which issue closes each one

The standing triage list for the trade columns, kept here rather than in a pull request so it survives
the merge that produced it.

⚠️ **Regenerate before quoting.** Every number below is a measurement of the artifacts on disk at the
time it was taken, and the artifacts change:

```bash
uv run python -m bedrock.analysis.nowcasting.trade_data.row_exposure --decompose
uv run python -m bedrock.utils.validation.stale_artifacts --name Trade_   # are they current?
```

**State reflected:** 2017, after re-export removal (#762), `S00300` sourcing (#766), and `990000` removal (#764).
`row_exposure` reports **43 of 359** commodities above 25% of their own intermediate use.

## How to read it

Trade error matters in proportion to **the row it lands on**, not to the import pool, because the balance
imposes `T001[c] = T016[c] − Σ_FD Y[c]` hard. A trade error moves that one commodity's intermediate total
one-for-one, and the RAS then converges by inflating or draining the row.

`--decompose` splits each family's error into two parts, and the split decides which instrument can help:

| part | meaning | what can fix it |
|---|---|---|
| **level** | our family total ÷ published family total | a source or a concept correction |
| **mix** | share of family mass on the wrong sibling | a concordance or a re-split |

A family whose level is 21 is not a mapping problem however bad its mix looks.

## Closed

| row | was | now | closed by |
|---|---|---|---|
| `S00300` noncomparable imports | 127.5% exposure, −181,645 $M | **0.6%**, +840 $M | [#766](https://github.com/cornerstone-data/bedrock/pull/766) — nine IEA leaves routed to `S00300` |
| `S00402` used and secondhand goods | 61.0% exposure, −20,613 $M | **1.6%**, +540 $M | [#764](https://github.com/cornerstone-data/bedrock/pull/764) — Census `990000` catch-all removed |
| aircraft `336411` / `336412` / `336413` | 162% / 184% / 52% exposure | **35% / 88% / 5%** | #720 + #748 applied by rebuild ([#758](https://github.com/cornerstone-data/bedrock/pull/758)) |
| goods export level (economy-wide) | **+18.1%** vs published goods `F04000` | **−0.5%** | [#762](https://github.com/cornerstone-data/bedrock/issues/762) — Census `DF` domestic exports (`ALL_VAL_YR_DOM`) |
| `492000` couriers | `MISS` both sides, 8,008 $M exposure | **exact at 2017; 1.68% MAPE 2018-24** | #771 anchor-and-move on `TransportAirFreight` — graded in [`courier_air_freight.py`](courier_air_freight.py) |
| `52A000` financial service imports | 5.7× published | **exact at 2017** | #771 — `FinFisim` anchored on published `MCIF` |
| aerospace `3364`, all five leaves | gross 35,446 $M, `336412` at 0.60× and `336414` at 4.94× | **6,434 $M, every leaf at 1.06×** | #701 — export 1:m weight moved off same-year `T007` onto published `F04000`, family consolidated onto `33641X` |
| vehicle exports `336111` / `336112` | 0.31× and 1.52× published | **1.01× both, `MATCH`** | #701 — same weight change |

## ⚠️ `row_control_exposure` reads a pinned export CSV by default

The diagnostic #701 was raised from takes its `F04000` candidate from
`output/nowcast_initial_Y_pur_vs_use_sut_framework_2017.csv`, last written
**2026-08-14** — before #762, #764, #766 and #771. Run without `--live` it
reproduces #701's export column *to the dollar* and reports that nothing has
changed. Pass `--live` (roughly ten minutes) or the reading is three weeks stale.

| quantity | #701, $M | live now, $M |
|---|---:|---:|
| gross error, Σ\|dT001\| | 1,164,084 | 575,013 |
| **landing in the intermediate block** | **488,408** (3.3%) | **170,835** (1.1%) |
| from `MCIF` | 454,227 | 122,807 |
| from `F04000` | 313,910 | 160,699 |

Concentration also flattened, because the single large offenders are the ones
that went: top 1 29% → **6.3%**, top 10 57% → **35.7%**, top 20 71% → **52.0%**.

## Open

### ✅ #763 · the import within-family split — WIRED, and the mix is gone

#763 validated the construction on the 2012 holdout and closed without wiring it: *each family's
within-split anchored on the published 2017 mix, moved by the Census family level*. The import method
already split **1:m** activities by frozen 2017 `MCIF`, which is that construction exactly — but 62 of
the families are mapped **1:1 or m:1**, so no weight was ever applied and Census's own split stood.
Giving each family a four-digit parent turns the family into one 1:m row, the device #702 used for
vehicles and #865 for aerospace exports.

| | gross import commodity error | family-level | mix | import `MATCH` |
|---|---:|---:|---:|---:|
| before | 445,999 $M | 66% | 151,978 $M (34%) | 109 |
| **after** | **309,342 $M** | 95% | 15,203 $M (5%) | **123** |

❌ **A family is only re-split when Census's total for it is within 25% of BEA's.** The construction is
"anchored on the published mix, *moved by the Census family level*", and that premise fails when the
family total is contaminated — the re-split then takes one leaf's level error and smears it over
siblings that were fine.

`3399` forced the guard: **88% of its excess sat on `339910` jewelry at 2.93×**, and an unguarded
re-split dragged five leaves that were within 9% of published to 1.50×, taking `339930` dolls and toys
from 161% to **1049%** of its own intermediate use. Family *gross* barely moved (28,880 → 28,912), so an
aggregate check cannot see it: one bad row becomes six mediocre ones.

| band on \|level − 1\| | families | gross saved | mass pushed *away* from published |
|---|---:|---:|---:|
| 0.20 | 48 | 107,684 $M | 11,737 $M |
| **0.25 (shipped)** | **52** | **136,228 $M** | **15,148 $M** |
| 0.35 | 58 | 149,818 $M | 24,228 $M |
| no guard | 65 | 150,726 $M | **76,499 $M** |

0.25 keeps 90% of the gain and removes 80% of the damage. It also scores better: unguarded, import
`MATCH` reaches only 115 against the guard's **123**, because a bad family's good leaves all get pushed
to one wrong ratio.

The 13 families the guard excludes are **#670's by definition** — their level is wrong, and re-splitting
inside them would disguise one level error as six mix errors.

✅ **Both survive the 2012 benchmark.** With weight and selection frozen at 2017, the construction beats
no-consolidation by **93,672 $M (20%)** against published 2012 `MCIF`, and the guard halves the damage the
unguarded version does (56,611 against 107,788). [`import_resplit_holdout.py`](import_resplit_holdout.py) `--check`.

### → #670 · import level gaps

No within-family re-split reaches these.

| commodity | exposure | family | **level** | mix |
|---|---:|---|---:|---:|
| `334610` magnetic and optical media | 280.7% | 3346 | **21.08** | 0.00 |
| `333242` semiconductor machinery | 320.6% | 3332 | 1.30 | 0.30 |
| `325910` printing ink | 176.8% | 3259 | **2.53** | 0.58 |
| `334418` printed circuit assembly | 114.9% | 3344 | **1.77** | 0.23 |

`334610` carries 21× its family's published imports with a mix error of **zero**. Check the wrong-vintage
join class first (the route #675 verified), HS product concordance only if that comes back clean.

### → #767 · distributing `S00300` across its users

#766 sourced the commodity total. The **use side** — 142,497 $M across 342 industries, 110,221 $M `F01000`
PCE, 7,711 $M `F02N00` — is unbuilt. Concentrated enough to be tractable: 10 industries carry 58.7%.

### → #703 · the `930000` residue

The `990000` catch-all is removed (#764, which closes #703's main finding). What remains under it is
`930000 → S00402`, leaving imports at **1.61×** published — small, and still a trade question.

⚠️ The **price** treatment of scrap and used goods is **not** a trade question and is now
[#768](https://github.com/cornerstone-data/bedrock/issues/768): `S00401` and `S00402` have no price index,
`S00402` is given a neutral 1.0 in the carry, and both are *recovered* rather than produced, so an output
index is the wrong instrument in principle.

### → #747 · IEA publisher leaves

`311810` and `1121A0` resolved by the rebuild. What remains is the 2021–22 IEA suppression, an
interpolation gap rather than a mapping problem.

## The import goods level gap, unowned

Not yet an issue, and it is the largest remaining error in the block.

Against the gross (NIPA) concept, using the measured c.i.f./customs wedge of +2.84%, **BEA's published
goods rows sit −13.0%** while ours sit −4.1%; goods rows run **+190,623 $M (+9.0%)** against published.

Ruled out so far: **re-imports** (already excluded — Census `980000`, 71,503 $M, is deliberately unmapped),
**general vs consumption imports** (12,570 $M), and **margins** (margin-bearing rows absorb 1.6% of the
excess).

⚠️ National `MCIF` reads **+7.3%** since #766. That is not a regression from it: the `S00300` shortfall was
cancelling this goods excess, and the old +1.2% was two independent errors of opposite sign.
