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

## Open

### → #763 · the import within-family split

Families where level is right and mix is wrong.

| commodity | exposure | family | level | mix |
|---|---:|---|---:|---:|
| `334118` computer terminals | 139.2% | 3341 | 0.98 | 0.14 |
| `336211` motor vehicle bodies | 146.1% | 3362 | 1.04 | 0.18 |
| `337121` upholstered household furniture | 105.4% | 3371 | 1.03 | 0.21 |
| `541511` custom programming | 53.8% | 5415 | 0.99 | **0.38** |

⚠️ **Ceiling: 32%.** Of 466,604 $M gross import commodity error, **68% is family-level** and only
**151,431 $M (32%)** is mix. `family_resplit` shows 14 of 58 families improve under PxI or the PCE bridge,
but choosing by 2017 performance fits the answer key — it needs the 2012 holdout (#700's panels).

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
