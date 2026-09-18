# #755 RAS movement: seed→balanced Use Δ

Standing report for the Step-5 RAS movement diagnosis. Detail CSVs live under
`bedrock/analysis/nowcasting/output/ras_movement/` (gitignored). Reproduce with
the command in §3.

Does **not** call the production one-shot `balance_year` wrapper. For each year
it assembles the seed tables once, runs the balance engine with soft and/or
hard targets, and measures movement on the restored Use table **before** any
save-time cleanup.

Runs: soft+hard **2017–2023** (~73 min, 2026-09-17); **2024** added (~18 min,
same day). Year-over-year (YoY) series rebuilt from all year CSVs so 2023→2024
is included.

---

## 1. What

This analysis measures how much **Step 5** (the RAS / GRAS balance) moves the
**Use** table between the **seed** (the assembled starting tables for that
year) and the **balanced** tables (after the engine restores fixed blocks).

For each commodity it splits that movement into two column blocks:

- **Intermediate use** — purchases of that commodity by industries
(`balance_industries()`): the I×I middle of the Use table.
- **Final demand (FD)** — households, investment, government, exports, etc.
(`SUT_FINAL_DEMAND_CODES`): the right-hand side of the Use table.

Two protocols are compared on the **same** seed:

- **Soft** — production settings: hard accounting identities plus soft
(weighted) targets such as detailed industry output mixes.
- **Hard** — hard identities only (`impose_soft=False`). Differences between
soft and hard show how much the soft layer changes Use commodity movement.

Years covered: **2017–2024**. The original #755 window was 2017–2023; 2024 was
added to reach the end of the interior-fit year list (`FIT_YEARS`). 2017 is
mostly a plumbing check (seed is partly circular with the published detail);
the main evidence years are 2018–2024.

---

## 2. Why

The goal is a **standalone diagnosis**: how large is RAS movement, where does
it land (intermediate vs FD), and does the soft layer matter? That evidence
feeds later Step-5 work without fixing those issues in this report.

**Referenced work:**


| ID                     | What it is                                                                                                                                                                                                                                   |
| ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **#755** (this report) | “Diagnosis Impacts of RAS 2017–2023” — measure seed→balanced SUT movement by year and flag problems.                                                                                                                                         |
| **#839**               | Bundle of small, specific RAS/balance improvements. Hygiene items (clean tiny illicit negatives at save; standing zero-pattern / sign checks) shipped in **PR #915**. Soft-vs-hard and other findings stay as Later additions on that issue. |
| **PR #915**            | Implements the #839 hygiene hooks in production assembly (residue sweep + post-balance audits).                                                                                                                                              |
| **#809**               | Trade-margin column (T15) stops netting to zero under the balance — about **$34B** in 2022. Next fix after this diagnosis.                                                                                                                   |
| **#808**               | Value-added–dominated Use columns hit a plateau under hard targets (T1 / T17 / T18). After #809.                                                                                                                                             |


This report does **not** implement #808 or #809; it only notes where movement
patterns may co-locate with those defects.

---

## 3. Metrics

All dollar figures are BEA **millions of dollars ($M)** — the same units as
the Use table in the balance pipeline.

**Building blocks**

- **Seed** — Use table after assembly / interior fit, before the balance
engine runs.
- **Balanced** — Use table after the engine, with fixed cells restored.
- **RAS movement (Δ)** — balanced minus seed: how far the balance moved each
cell (or each commodity’s block total).

**Per-commodity level and movement columns** (file
`commodity_ras_delta_{year}_{protocol}.csv`):


| Metric                     | Meaning                                                                                                                                                                                                                   |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `seed_inter`               | For one commodity: sum of its seed Use cells across **industry** columns (intermediate demand for that commodity).                                                                                                        |
| `seed_fd`                  | Same commodity: sum across **final-demand** columns at seed.                                                                                                                                                              |
| `bal_inter` / `bal_fd`     | Same two block totals after the balance.                                                                                                                                                                                  |
| `delta_inter` / `delta_fd` | How much those block totals moved: `bal − seed`. Positive = balance raised that block; negative = lowered it.                                                                                                             |
| `absorption_share`         | Of the absolute movement on that commodity, what share landed in intermediates: `\|δ_inter\| / (\|δ_inter\| + \|δ_fd\|)`. **1.0** = all intermediate; **0.0** = all FD; **0.5** = split evenly. Zero when both deltas are zero. |
| `l1_inter` / `l1_fd`       | **L1** = sum of absolute cell-by-cell changes inside the block (not just the change in the row sum). If two cells move +10 and −10, the row-sum Δ is 0 but L1 is 20 — L1 catches reshuffling inside the block.            |
| `hygiene_ok`               | Whether the post-balance hygiene checks passed for that run.                                                                                                                                                              |


**Year-over-year (YoY) columns** (file `commodity_ras_yoy.csv`; first year in
the span has no YoY row):

Balanced change from year *t−1* to *t* always decomposes as:

`balanced YoY = seed YoY + (RAS Δ at t − RAS Δ at t−1)`.


| Metric                             | Meaning                                                                              |
| ---------------------------------- | ------------------------------------------------------------------------------------ |
| `seed_yoy_inter` / `seed_yoy_fd`   | How much the **seed** block totals changed from last year to this year (before RAS). |
| `ras_delta_inter` / `ras_delta_fd` | This year’s RAS movement (same as `delta_*` for year *t* — not a YoY).               |
| `ras_delta_yoy_*`                  | How much the RAS movement **itself** changed vs last year.                           |
| `balanced_yoy_*`                   | How much the **balanced** block totals changed YoY.                                  |


**Soft vs hard** (file `protocol_soft_vs_hard_{year}.csv`):


| Metric                          | Plain meaning                                                                                                             |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `soft_minus_hard_inter` / `_fd` | Soft-protocol Δ minus hard-protocol Δ on the same seed. Near zero ⇒ soft targets barely change that commodity’s movement. |


Reproduce (full span)::

```
uv run python -m bedrock.analysis.nowcasting.ras_improvements.ras_movement \
    --years 2017-2024 --protocols soft,hard
```

---

## 4. Results

### (a) Year × protocol totals / top movers

Table columns are **sums across all 400 balance commodities** for the soft
protocol ($M):

- **Σ|δ_inter| / Σ|δ_fd|** — total absolute movement of commodity **block
totals** (row sums) in intermediate vs FD.
- **Σ L1_inter / Σ L1_fd** — total absolute **cell-level** reshuffling inside
those blocks (always ≥ the row-sum movement).
- **Median absorption** — median across commodities of
`absorption_share` (see §3). Near 0.5 ⇒ a typical commodity’s movement is
split between intermediate and FD; near 0 ⇒ FD-heavy; near 1 ⇒
intermediate-only.


| Year | Protocol | Σ\|δ_inter\| | Σ\|δ_fd\| | Σ L1_inter | Σ L1_fd | Median absorption |
| ---- | -------- | ----------- | --------- | ---------- | ------- | ----------------- |
| 2017 | soft     | 50,301      | 71,782    | 68,970     | 80,010  | 0.4               |
| 2018 | soft     | 67,208      | 193,499   | 93,941     | 214,555 | 0.3               |
| 2019 | soft     | 72,078      | 234,432   | 102,249    | 260,234 | 0.3               |
| 2020 | soft     | 128,137     | 426,156   | 170,131    | 485,093 | 0.4               |
| 2021 | soft     | 250,334     | 531,887   | 296,808    | 614,489 | 0.4               |
| 2022 | soft     | **936,795** | **1,148,250** | **1,067,937** | **1,286,784** | 0.5        |
| 2023 | soft     | 207,358     | 751,160   | 261,516    | 817,842 | 0.2               |
| 2024 | soft     | 215,666     | 841,113   | 256,523    | 906,920 | 0.2               |


![Soft Σ|δ| by year (intermediate vs FD)](../images/ras_755_soft_abs_delta_by_year.png)

*Figure: Soft Σ|δ_inter| and Σ|δ_fd| by year ($B). Source:
`commodity_ras_delta_{year}_soft.csv`.*

**Reading one line (2017 soft).** The balance moved commodity intermediate
block totals by **50,301 $M** in absolute value and FD block totals by
**71,782 $M**. Against the seed’s own absolute mass (~14.9T $M intermediate,
~22.2T $M FD), that is only about **0.3%** of each block’s seed mass — a
small correction on 2017. Cell-level L1 is a bit larger (69k / 80k $M ≈
0.4–0.5% of seed mass), so some reshuffling inside blocks is not visible in
the row sums alone. Median absorption **0.4** means that for a typical
commodity with any movement, a bit under half of that movement’s absolute
size is in intermediates and a bit over half is in FD.

**Across years.** FD absolute movement exceeds intermediate every year.
Movement grows through **2022** (outlier: Σ|δ_inter| ≈ **4.6%** of seed
intermediate mass, vs ~0.3–1% in quiet years), falls sharply in 2023, and
**2024 stays near 2023** (both ~1.0% of seed intermediate mass) — no second
spike. Median absorption drifts toward **0.2** in 2023–2024: more of a
typical commodity’s movement sits in FD than in intermediates.

![Soft |δ_inter| as percent of seed intermediate mass](../images/ras_755_soft_delta_inter_pct_seed.png)

*Figure: Soft |δ_inter| / sum(seed_inter) by year (%). Confirms the 2022
outlier; 2024 does not re-spike. Source: same soft delta CSVs.*

**High absorption_share** on a commodity means its movement is mostly
intermediate, not FD. The recurring **top soft movers by |δ_inter|** are
wholesale-trade commodities (`423*`, `424*`) with absorption ≈ **1.0**
(almost pure intermediate — for all nine wholesale balance commodities,
`seed_fd` is identically **0**, so there is no FD block mass to move), truck
transportation `484000`, and large moves in other real estate `531ORE`,
management of companies `550000`, and household appliances `335220`. Full
ranked lists: `top_movers_{year}_soft.csv`.

**Net vs absolute.** Net signed Σδ is much smaller than Σ|δ| every year (e.g.
2022 net intermediate ≈ **+174k $M** vs abs ≈ **937k $M**). Cross-commodity
cancel is large; the tables above use absolute sums deliberately, so “total
movement” should not be read as one-way pressure on the Use table.

### (b) Seed-driven vs RAS-driven YoY

YoY rows start at **2018** (no prior year for 2017). Soft protocol. Each cell
sums absolute commodity YoY across all commodities ($M).

**“Mostly seed”** means: the year-to-year change in the **balanced** tables is
driven mainly by the year-to-year change in the **seed**, not by RAS changing
how far it moves the table. In the decomposition
`balanced YoY = seed YoY + ras_delta_yoy`, if Σ|ras_delta_yoy| is much smaller
than Σ|seed_yoy|, RAS is a small add-on to an already-moving seed. The
**ras/seed** column is Σ|ras_delta_yoy_inter| ÷ Σ|seed_yoy_inter|.


| Year | Σ\|seed_yoy_inter\| | Σ\|ras_delta_yoy_inter\| | ras/seed | Σ\|seed_yoy_fd\| | Σ\|ras_delta_yoy_fd\| |
| ---- | ------------------- | ------------------------ | -------- | ---------------- | --------------------- |
| 2018 | 1,243,535           | 26,571                   | 0.02     | 1,351,460        | 152,957               |
| 2019 | 1,082,646           | 25,581                   | 0.02     | 1,186,651        | 107,078               |
| 2020 | 1,677,929           | 95,172                   | 0.06     | 2,338,898        | 268,543               |
| 2021 | 3,220,448           | 160,383                  | 0.05     | 3,041,314        | 251,581               |
| 2022 | 2,839,166           | **816,636**              | **0.29** | 2,794,776        | **787,384**           |
| 2023 | 2,399,969           | **999,575**              | **0.42** | 2,904,826        | **1,066,241**         |
| 2024 | 1,300,765           | 61,609                   | **0.05** | 2,092,096        | 138,535               |


![YoY ras/seed ratio (soft, intermediate)](../images/ras_755_yoy_ras_seed_ratio.png)

*Figure: Soft intermediate ras/seed =
Σ|ras_delta_yoy_inter| ÷ Σ|seed_yoy_inter|. Source: `commodity_ras_yoy.csv`.*

**Reading one line (2018).** Seed intermediate block totals shifted by about
**1.24T $M** in absolute commodity YoY from 2017→2018, while the *change in
RAS movement* was only **27B $M** (ras/seed ≈ **0.02**). So almost all of the
balanced YoY is already in the seed; RAS is a small adjustment. FD shows the
same pattern (seed YoY ~1.35T vs RAS YoY ~153B).

Through **2021**, balanced YoY stays **mostly seed** (ras/seed ≈ 0.02–0.06).
In **2022–2023**, RAS Δ YoY becomes a large share of seed YoY (0.29–0.42 on
intermediates) — the balance is no longer a small correction relative to seed
change. **2024 returns to seed-dominated** YoY (ras/seed ≈ 0.05); the
2022–2023 RAS YoY surge does not continue.

**2023 ras/seed is mostly unwind, not a second spike.** Soft |δ_inter| levels
fall from ~937k $M in 2022 to ~207k $M in 2023, but ras/seed rises to **0.42**
because |Δ_2023 − Δ_2022| is large when Δ shrinks from a huge base. About
**95%** of that absolute YoY in Δ moves **toward zero** (281 commodities have
smaller |δ_inter| in 2023 than in 2022; of the 360 with a defined sign in both
years, **253** reverse sign and **107** keep direction). So 2023 is quiet in
*levels*; the loud YoY ratio is the hangover from unwinding 2022.

### (c) Soft vs hard divergence

Σ|soft − hard| is the sum over commodities of absolute differences between
soft and hard Δ ($M). **Near zero** means soft targets barely change Use
commodity movement vs hard-only.


| Year | Σ\|Δ_inter soft−hard\| | Σ\|Δ_fd soft−hard\| |
| ---- | ---------------------- | ------------------- |
| 2017 | 10                     | 8                   |
| 2018 | 15                     | 11                  |
| 2019 | 16                     | 13                  |
| 2020 | 42                     | 29                  |
| 2021 | 86                     | 48                  |
| 2022 | **963**                | **465**             |
| 2023 | 47                     | 38                  |
| 2024 | 40                     | 38                  |


![Soft vs hard Σ|soft − hard| intermediate](../images/ras_755_soft_minus_hard_inter.png)

*Figure: Σ|soft − hard|_inter by year ($M). Soft layer is noise relative to
total Δ except a small 2022 bump. Source: `protocol_soft_vs_hard_{year}.csv`.*

**High |soft − hard|** on a commodity would mean soft targets matter for that
row. Here soft ≈ hard almost everywhere: the soft layer is **not** the main
driver of Use commodity Δ (soft/hard Σ|δ_inter| ratio ≈ **1.000** every year).
The exception is **2022** (~963 $M intermediate), still small next to that
year’s ~937k $M of soft Σ|δ_inter|. Largest mean |soft−hard| commodities
across years: wholesale `423400`, `424700`, `423800`, `423A00` — tens of $M
at most.

### (d) Hygiene flags

All **16** year×protocol runs (2017–2024 × soft/hard): **`hygiene_ok=True`**
(zero-pattern mass and illicit-sign checks from the #839 / PR #915 hygiene
passed after metrics were recorded).

---

## 5. Takeaways

### 1. Does this analysis surface RAS issues that need to be resolved? (#755)

**No new RAS *defect* is proven by these commodity Use Δ tables alone.** Hygiene
passed on every year×protocol run, and soft ≈ hard means the soft-target layer
is not what is driving Use commodity movement. Large Δ is not automatically a
bug: RAS is supposed to move the seed when sources disagree.

What **is** surfaced — and should stay on the workstream as investigation, not
as a fresh #839 hygiene bullet — is a **2022 anomaly**:

| Signal | Why it matters |
|--------|----------------|
| Σ\|δ\| spike (intermediate ~4.6% of seed mass vs ~0.3–1% in quiet years) | Balance correction is an order of magnitude larger than in other years. |
| YoY ras/seed 0.29–0.42 in 2022–2023 | Balanced year-to-year change is no longer “mostly seed”; RAS itself is a first-order driver. |
| Soft−hard peak in 2022 (~963 $M inter) | Soft layer still small vs total Δ, but 2022 is the only year where soft vs hard is noticeable. |

**2024 does not continue the spike** (levels near 2023, soft≈hard, YoY
seed-dominated again). So the actionable reading for #755 is: **treat 2022
(and the 2022→2023 RAS YoY hangover) as the year to explain**, not as evidence
that every year’s RAS is broken. Recurring wholesale (`423*` / `424*`)
intermediate movers with absorption ≈ 1 are a *pattern* to check against known
margin issues (#809), not a standalone proof of error.

### Reading caveats (adequacy gaps)

These do not overturn the tables; they sharpen how to read them:

1. **2023 ras/seed 0.42 ≠ a second pathology year in levels.** Soft |δ_inter|
   drops sharply in 2023; ~**95%** of |Δ_2023 − Δ_2022| is movement **toward
   zero** (unwind of the 2022 spike; 281 commodities shrink in |δ_inter|). The
   YoY ratio looks loud because Δ shrinks from a huge base — same “hangover”
   point as above, quantified.
2. **Wholesale absorption = 1 because `seed_fd ≡ 0`** for all nine `423*` /
   `424*` balance commodities — not merely “little” FD mass. High absorption
   there is structural (no FD block), not evidence of intermediate-only
   targeting preference.
3. **Net signed Δ ≪ Σ|δ| every year** (cross-commodity cancel). Absolute sums
   are the right scan metric, but readers should not treat Σ|δ| as one-way
   Use pressure.
4. **Soft−hard table integers are rounded** (e.g. 9.7 → 10); immaterial next
   to total Δ.

### 2. Implications for #809 and #808

**#809 (trade margin column T15 fails to net to zero — ~$34B in 2022) should
stay next, and this diagnosis strengthens that ordering.** The movement spike,
the soft−hard bump, and the documented T15 failure all peak in **2022**. This
report does not measure T15 netting directly, but the coincidence is strong
enough that fixing #809 first, then **re-running this diagnosis**, is the
right test: if 2022 Σ\|δ\| and ras/seed fall materially after the margin fix,
the anomaly was largely margin/seed inconsistency; if they stay large, dig
deeper (seed quality, other targets, outer iterations).

**#808 (VA-dominated Use columns plateau under hard targets) is only weakly
informed here.** Commodity intermediate/FD Δ deliberately excludes `VA_ROWS`,
and soft ≈ hard on those commodity blocks does **not** speak to whether VA
columns are stuck. Keep #808 after #809; do not expect this Use-commodity
census to predict the VA fix. Optionally, after #809, add a small VA-row
movement companion (same assemble-once path) if #808 still needs evidence.

### 3. Additional workstream items

Worth adding or keeping explicit (beyond “fix #809 then #808”):

1. **Re-run #755 after #809** (and again after #808 if VA work changes Use
   seeds/targets) — same CLI, compare 2022 and ras/seed YoY to this baseline.
2. **2022 deep-dive note (not a production change)** — link top movers /
   `protocol_soft_vs_hard_2022.csv` to margin commodities and T15; ask whether
   wholesale `423*`/`424*` Δ is the Use-side footprint of the margin netting
   failure.
3. **Outer-iteration-cap check** — only if post-#809 2022 still looks
   pathological *and* engine logs show years hitting `max_outer=20` without
   T11 close; this diagnosis did not record outer-iteration counts.
4. **Do not** add soft-vs-hard as a hygiene/#839 production change based on
   these results — soft is not the main Use Δ driver.
5. **Optional later:** Supply-side seed→balanced Δ (out of scope here); FD
   column-level (not just commodity×FD block) movers for 2022 if #809 does not
   explain the FD Σ\|δ\| spike.

