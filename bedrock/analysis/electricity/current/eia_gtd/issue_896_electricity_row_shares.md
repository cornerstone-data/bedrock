# Issue #896 — electricity Use row share collapse (plain language)

Diagnostics and measured results for
[issue #896](https://github.com/cornerstone-data/bedrock/issues/896), as shipped
in this PR. **Schemas, gate thresholds, and how to read the CSVs** are in
[`About_896_row_gate.md`](About_896_row_gate.md). Method background:
`About_why_the_flat_price_fails.md` (point 6) and `About_price_proposal.md`.

Reproduce the original row-control measurement:

```bash
python -m bedrock.analysis.electricity.current.eia_gtd.electricity_row_control \
    --mut-vintage v0.3.0_4276083 --csv
```

Run the diagnostic gate in this PR:

```bash
python -m bedrock.analysis.electricity.current.eia_gtd.electricity_row_896 \
    --mode all --mut-vintage v0.3.0_4276083 --csv --check \
    --anchor-span 2022-2023
```

`--years` (default `2017-2024`) drives bands and gate. `--anchor-span` drives
stages / seed_status ranking (and which industries aies_seam lists). AIES
**index** years stay pinned at 2022/2023 (SAS→AIES survey break).

## Why this work exists

The model’s electricity **bills** look wrong in 2023–24: electricity’s share of
all intermediate purchases fell sharply. Later G/T/D v3 work cannot be judged
until those bills are fixed and measured on their own: **find where the dollars
went wrong → fix the bills → only then change the splitter**.

## Analysis on this PR

This PR adds measurement only. It does **not** change the production MUT or the
G/T/D allocator.

| Check | Plain question |
|---|---|
| **Bands** | Of the share-effect dollars, how much came from manufacturing vs services vs trade vs other — **every** YoY span 2017–24? |
| **Target gate** | Did the *allowed* intermediate total for electricity fall, or only the *realized* Use row? |
| **IPF vs GRAS** | Did the shrink happen when fitting margins, or later in free-cell balance? |
| **Seed/mask status** | For the industries that lost the most: surveyed, held at 2017, or free for the balancer? |
| **AIES seam** | Did the 2023 SAS→AIES survey switch jolt purchased-electricity indexes? |

## Results (MUT `v0.3.0_4276083`, run 2026-09-22)

**Bottom line for the 2023–24 share drop:** primarily a **target collapse** — the
*allowed* intermediate-electricity budget (`T016 − ΣY`) fell in step with the
realized Use row on both crisis spans. Unseeded trade and services absorbed most
of the column-level share-effect dollars when the row was fitted down. The
SAS→AIES electricity seam is **not** the driver.

**Dual window (important for follow-ups):** target scheduling stays on
**2022→23 / 2023→24**. Allocation diagnostics must **not** inherit that window —
band churn and trade/paper defects show up across **2017–2024** (see full-span
tables below, especially **2020→21**).

Artifacts: `electricity_row_896_*.csv` in this folder.

### Target gate — full span

| Year | Target $bn | Realized Use $bn | Realized share of all intermediate | Supply−use gap $bn |
|---:|---:|---:|---:|---:|
| 2017 | 280.4 | 279.3 | 1.88% | 1.1 |
| 2018 | 304.8 | 303.3 | 1.91% | 1.5 |
| 2019 | 308.7 | 306.9 | 1.90% | 1.9 |
| 2020 | 282.4 | 279.7 | 1.82% | 2.6 |
| 2021 | 368.2 | 367.8 | 2.03% | 0.4 |
| 2022 | 419.6 | 418.9 | **2.02%** | 0.7 |
| 2023 | 358.3 | 352.0 | **1.70%** | 6.3 |
| 2024 | 337.6 | 330.3 | **1.54%** | 7.3 |

| Span | Classification | Target fraction of level | Shares parallel? | Role |
|---|---|---:|---|---|
| 2017→18 | `allocation` | 1.02 | no | context |
| 2018→19 | `stable` | — | no | context |
| 2019→20 | `target_collapse` | 0.97 | yes | pandemic / BEA swing |
| 2020→21 | `target_collapse` | 0.97 | yes | mostly **column** effect at row level (+$88bn); still large band share effects |
| 2021→22 | `allocation` | 1.01 | no | BEA GO already attributed |
| **2022→23** | **`target_collapse`** | **0.92** | yes | **crisis — target scheduling** |
| **2023→24** | **`target_collapse`** | **0.95** | yes | **crisis — target scheduling** |

For 2023–24: almost all of the dollar decline in the Use row is mirrored by a
decline in the **row target**. That is **not** “the total was fine and RAS stole
from commercial columns” as the first-order story — though those columns still
show large share effects once the smaller total is imposed.

### Bands — full span (share effect $bn)

Share effect = electricity changing its bite of each purchaser’s input column.

| Band | 17→18 | 18→19 | 19→20 | **20→21** | 21→22 | **22→23** | **23→24** |
|---|---:|---:|---:|---:|---:|---:|---:|
| manufacturing | +1.3 | +1.8 | −0.6 | **+2.3** | +6.3 | **−9.3** | −2.9 |
| services_transport | +1.9 | −4.0 | −9.3 | **+10.9** | −3.6 | **−13.8** | **−19.4** |
| trade | +1.9 | −2.2 | +0.7 | **+8.9** | −3.6 | **−17.3** | −3.8 |
| trade_unseeded | ~0 | ~0 | ~0 | +0.1 | ~0 | −0.3 | ~0 |
| held_2017 | +0.3 | −0.4 | +3.0 | +1.9 | −1.4 | −8.0 | −1.4 |
| utilities | +0.1 | +2.1 | ~0 | +3.9 | −3.9 | −3.8 | −1.2 |
| mining | +0.4 | +0.4 | +0.2 | +0.2 | ~0 | −2.1 | −0.8 |
| agriculture | −0.1 | ~0 | +0.8 | −0.6 | −0.8 | −0.4 | ~0 |

**2020→21 (highest-value “missing” row in earlier write-ups):** services
(+$10.9bn) and trade (+$8.9bn) both **rose** hard on share effect while the gate
also labels `target_collapse` on the row total — the up-leg of a round trip that
crisis-only tables hide. That is why allocation follow-ups grade **2017–24**,
not only 2022–24.

**Crisis spans:** 2022→23 largest bite is **trade (unseeded)**; 2023→24 largest
is **services**. Same “commercial on the carry” picture for *where* the rescale
shows up, under a **smaller national intermediate budget**.

### Stages (IPF vs GRAS)

On top losers for the default anchor 2022→23, Step-3 → IPF cuts 2023 cells by
on the order of **$1–2bn each**; GRAS vs that IPF is usually small (hotels
excepted). The shrink happens when the interior is **fitted to the lower row
target**.

### Seed / mask status — top losers 2022→23

| Industry | Band | Elec in seed? | Cell free? | Share effect $bn |
|---|---|---|---|---:|
| 221100 utilities | utilities | no | yes | −3.6 |
| 445000 food/beverage stores | **trade** | **no** | yes | −3.1 |
| 722211 limited-service restaurants | services | yes | yes | −3.0 |
| 622000 hospitals | services | yes | yes | −2.4 |
| 452000 general merchandise | **trade** | **no** | yes | −2.3 |
| 325190 organic chemicals | manufacturing | yes | yes | −2.1 |
| 550000 management of companies | **held_2017** | **no** | yes | −2.1 |

Several of the biggest losers have **no electricity overlay** and free Use
cells — columns that can give up dollars when the row target falls.

### AIES seam — not the cause of the decline

For large services losers with a measurable SAS-2022 vs AIES-2023 purchased-
electricity index, the index **rose** (restaurants ~+7%, hospitals ~+4%). Holding
the 2022 index would have made those cells *smaller*. Trucking’s index jumped
and its share effect went *up*. Services-band share effect for 2022→23 is
**−$13.8bn**; the seam counterfactual does not explain that loss.

## Follow-ups (out of scope for this PR)

Both crisis spans classify as **`target_collapse`**, so the next work is
**required**. Do **not** change the G/T/D allocator until a new MUT clears the
row gate. Change one thing, measure one thing.

### 1. Trace or fix the intermediate electricity target (2022–24 only)

Explain and/or fix the drop in Supply `T016` and/or final demand `Y` for
`221100`. Do **not** expand this track to 2020–21 for target scheduling — that
span was mostly column effect at the row level.

### 2. Residual bill / allocation fixes (grade on 2017–24)

After the target is settled (or attributed), fix who gets how much electricity
inside the table. **#900** (paper $0) and **#899** (trade electricity seed)
must be graded on the **full 2017–24** span — including the 2021 paper zero and
2021 wholesale peaks visible in the band table. Prefer commercial electricity
that moves **uniformly** across trade (or stays physically consistent), not
idiosyncratic sector churn.

| Follow-up | Plain meaning |
|---|---|
| **Paper mills $0 (#900)** | Fix $0 electricity in **2021 and 2023**; time-series gap guard. |
| **Services electricity seam** | Only if a **post-target** re-measure still shows material services-band loss on **crisis spans**; this PR’s AIES check did not make that the first lever. |
| **Trade electricity seed (#899)** | Grade a uniform EIA commercial index (primary) vs QCEW payroll (fallback) on **2017–24**; wire only if Slack bars pass. |
| **Re-measure** | New MUT; share of intermediate + capped-purchaser counts with allocator frozen. |

**#896 can close when:** share is stable again (or explained by a named observed
series), no bogus $0 bills, trade meets the commercial uniformity bars (or
documented carry), and any remaining services seam defect is fixed or shown
immaterial. Construction/government may stay on carry. Manufacturing shape vs
Census/MECS is a **separate** follow-up (#898), not required for #896 exit.

### 3. Later, separate from #896

| Follow-up | Plain meaning |
|---|---|
| **Manufacturing shape (#898)** | MECS anchors + Census `CSTELEC` index. |
| **G/T/D allocator (#902)** | Class `p_gen`, T&D residual — after bills are pinned. |

```text
This PR: measure (full-span bands/gate)  →  fix/attribute the target (2022-24)
                                              ↓
                                    residual bills (#900/#899 on 2017-24)
                                              ↓
                                          #896 closed
                                              ↓
                                 later: #898 → #902
```
