# Nowcast progress report — blocks against the published 2017 detail SUT

Generated from `bedrock.analysis.nowcasting.sections`
([#587](https://github.com/cornerstone-data/bedrock/issues/587)). Reference is
the published 2017 detail SUT throughout — the benchmark year's answer in the
framework we are building in.

Regenerate with:

```
uv run python -m bedrock.analysis.nowcasting.plots
uv run python -m bedrock.analysis.nowcasting.plots \
    --dpi 110 --out-dir bedrock/analysis/nowcasting/images --no-report   # the copies below
```

**Snapshot date:** 2026-08-14. Step 1 is a live `derive_initial_Y_pur` (`NIPA_final_dom_uses_2017` plus Trade `F04000`). Step 4 is a live `derive_initial_supply_bridge` (`MCIF` from `Trade_Imports_2017`; other bridge columns unsourced).

---

## Where the build stands

| block | step | shape | reference populates | reference total | candidate | coverage | accuracy |
|---|---|---|---:|---:|---|---:|---:|
| `use_fd_detail_sut` | 1 — final demand | 402 × 19 | 1,253 cells | $22.24T | live | **75.3%** | **69.8%** |
| `use_va_detail_sut` | 2 — value added | 3 × 402 | 1,189 cells | $18.92T | *none yet* | — | — |
| `supply_bridge_detail_sut` | 4 — supply bridge | 402 × 12 | 3,202 cells | $111.28T | live (MCIF) | **8.5%** | **8.9%** |

**coverage** = of the cells the reference populates, how many we populate.
**accuracy** = of the cells we populate, how many land within tolerance.

Those three blocks are the whole of what a published 2017 detail reference
supports outside the two 402 × 402 interiors. The reference columns are the
denominator: they are what "done" looks like, and they are known before the
corresponding step is built.

---

## Step 1 — Use table, final-demand columns

`use_fd_detail_sut` · 402 commodities × 19 final-demand codes · tolerance
`rtol=0.013, atol=5e5, ramp=0.25`

![Step 1 final demand match](images/use_fd_detail_sut_2017.png)

| scope | absent | match | partial | miss | extra |
|---|---:|---:|---:|---:|---:|
| cells | 6,385 | 658 | 285 | 310 | 0 |
| row totals | 22 | 125 | 248 | 7 | 0 |
| column totals | 0 | 17 | 1 | 1 | 0 |

| | |
|---|---:|
| coverage | 75.3% |
| accuracy | 69.8% |
| candidate grand total | $22.33T |
| reference grand total | $22.24T |
| grand total error | 0.42% |
| residual outside the frame | none |

### What the picture says that the totals do not

**Only two column totals are outside tolerance, and both are known.** `F03000` (change in private inventories, #529) is a whole-column `miss` — unsourced. `F04000` is populated from `Trade_Exports_2017` and is `partial` (+6.15% vs published, #528). **Every other final-demand column now reconciles at the column level**, including the twelve government columns, which land cell for cell.

**The two axes disagree, which is the point.** 255 of 402 row totals are outside tolerance against 2 of 19 column totals — a column total can be right in aggregate while its commodity split is wrong, and only the row strip shows it. That gap is not a curiosity: all three fixes below were column totals that looked right while the value sat on the wrong commodity, and the row strip is what exposed them.

**What moved since 2026-08-06** — coverage 74.4% → 75.3%, accuracy 67.4% → 69.8%, column totals outside tolerance 3 → 2. `F02R00` came inside tolerance on three fixes sharing that one failure mode:

- **`S00402` was unreachable in the government columns** ([#633](https://github.com/cornerstone-data/bedrock/issues/633)) — missing from two crosswalks, one supplying the attribution weight and one the receiving set, so fixing either alone changed nothing. `F10E00` had been misallocating 32.8% of its column.
- **Ten structures lines mapped to a catch-all** rather than the single commodity each names ([#576](https://github.com/cornerstone-data/bedrock/issues/576)) — $142B on the wrong commodity inside `F02R00`, netting to exactly zero.
- **Residential equipment was routed to `F02E00`** ([#547](https://github.com/cornerstone-data/bedrock/issues/547)) — U50505 line 46 sits under the residential branch, so its 15,025 belongs in `F02R00`. `F02E00` is *nonresidential* equipment.

**Row totals are now dominated by `F04000`.** The ten worst cells are all trade: `336411`, `S00402`, `336412`, `339910`. With the NIPA-sourced columns reconciled, the remaining Step 1 error is concentrated in the trade column and the unbuilt inventories column.

**`F01000` is mostly `match`.** Personal consumption is the densest live column. Remaining amber is per-commodity split, not a missing column — and it is genuine bridge-vs-Use disagreement rather than attribution error: `F01000` reproduces `BEA_PCEBridge` cell for cell, with the totals agreeing to $2M.

**`F04000` is mixed `partial` / `miss`.** National mass is close enough to turn the column total amber rather than purple; commodity holes (`492000`, `550000`, and the rest of the ≥1 B USD list) are in [`transform/trade/README.md`](../../transform/trade/README.md).

Longer form: [`About_table_match.md`](About_table_match.md).

---

## Step 2 — Use table, value-added rows

`use_va_detail_sut` · 3 rows × 402 industries · tolerance
`rtol=0.01, atol=5e5, ramp=0.25` · **no candidate yet**

The reference, the frame and the bar are settled; Step 2 supplies the candidate
and the picture appears. The target:

| row | description | reference total |
|---|---|---:|
| `V00100` | Compensation of employees | $10.435T |
| `T00OTOP` | Other taxes on production, less subsidies | $0.609T |
| `V00300` | Gross operating surplus | $7.873T |
| | **total (`VABAS`)** | **$18.917T** |

1,189 of 1,206 cells are populated in the reference, so this block is nearly
dense — unlike final demand, there is almost nowhere for a miss to hide as a
structural zero.

**The valuation is the thing to get right.** `T00OTOP` is *other* taxes on
production less subsidies at basic prices. It is **not** the MUT's `V00200`,
which is taxes on production *and imports* at producer prices. A candidate built
on `V00200` will not match this reference, and the section deliberately does not
alias the two — the gap shows as a `MISS`/`EXTRA` pair rather than a bad match.

Turning it on: point `Section.candidate` at the Step 2 output.

---

## Step 4 — Supply table, bridge to purchaser value

`supply_bridge_detail_sut` · 402 commodities × 12 bridge codes · tolerance
`rtol=0.01, atol=5e5, ramp=0.25`

![Step 4 supply bridge match](images/supply_bridge_detail_sut_2017.png)

| scope | absent | match | partial | miss | extra |
|---|---:|---:|---:|---:|---:|
| cells | 1,599 | 77 | 382 | 2,743 | 23 |
| row totals | 9 | 0 | 276 | 117 | 0 |
| column totals | 0 | 2 | 1 | 9 | 0 |

| | |
|---|---:|
| coverage | 14.3% |
| accuracy | 16.8% |
| candidate grand total | $2.78T |
| reference grand total | $111.28T |
| grand total error | 97.5% |

`MCIF`, `MADJ`, and `MDTY` are sourced for 2017 (`Trade_Imports_2017` with ITA scale; Census `GEN_CHA_YR` reassigned onto Supply `MADJ` destinations and leveled to Supply `MADJ`; Census duty rates leveled to NIPA `B235RC`). Column totals for `MADJ` and `MDTY` match the published national (within the section tolerance). `T007`, `TRADE`/`TRANS`/`T014`, `TOP`/`SUB`/`T015`, and the `T013`/`T016` identities are unsourced.

The right-hand block of the Supply table: imports, margins, taxes and the
subtotals carrying a commodity from domestic output at basic value to total
supply at purchaser value. Reference totals, in millions:

| code | description | 2017 total |
|---|---|---:|
| `T007` | Total commodity output (domestic, basic) | 33,772,566 |
| `MCIF` | Imports of goods and services, CIF | 2,649,430 |
| `MADJ` | CIF/FOB adjustment on imports | −23,116 |
| `T013` | **Total supply, basic** | **36,398,867** |
| `TRADE` | Trade margins | 1 |
| `TRANS` | Transportation costs | 10 |
| `T014` | **Total margins** | **1** |
| `MDTY` | Import duties | 38,507 |
| `TOP` | Taxes on products | 716,926 |
| `SUB` | Subsidies (stored negative) | −59,876 |
| `T015` | **Taxes less subsidies** | **695,565** |
| `T016` | **Total supply, purchaser** | **37,094,434** |

**`T014` sums to 1 across the whole economy.** Margins net to nothing in
aggregate while being large and offsetting per commodity. No scalar check on
this block can work; a per-commodity picture is the only thing that can. This
block is the strongest case in the project for the diagnostic existing.

Subtotals are kept in the frame rather than stripped, because they are the
Supply identities and a subtotal disagreeing with its own components is exactly
what is worth seeing:

```
T013 = T007 + MCIF + MADJ
T014 = TRADE + TRANS
T015 = MDTY + TOP + SUB
T016 = T013 + T014 + T015
```

---

## Caveats

**`F04000` / `MCIF` do not clear the #557 bars.** National F040 is +6.15%; import Pearson on non-specials is 0.84 vs ≳ 0.85. Hole rules and ITA G+S scale sit on #528.

**`F03000` is unsourced.** Inventories are a whole-column miss (#529).

**Nothing here is a rollup.** Every number is BEA 2017 detail. Margins and redefinitions net out at summary and above, so an aggregate view of these blocks would pass on data these pictures show to be broken.
