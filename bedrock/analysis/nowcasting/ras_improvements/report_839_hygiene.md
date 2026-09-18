# #839 hygiene: balance census and illicit-vintage root cause

Standing report for the [#839](https://github.com/cornerstone-data/bedrock/issues/839)
hygiene workstream (residue sweep + zero-pattern / sign audits). Reproduce with
the modules in this package; see [`README.md`](README.md).

Production hooks: `bedrock.transform.iot.nowcast_sut_assembly`
(`RESIDUE_EPS_USD_M = 0.05`, `ZERO_PATTERN_MASS_USD_M = 1.0`,
`assert_post_balance_hygiene` after restore, Use sweep in `save_balance`).

---

## 1. Pipeline status (balance check 2018–2024)

Item 1 / 2b spot-check on fresh `balance_year` for **2018, 2021, 2023**
(~15–17 min each; `b25b8ac` artifact hash). Seed-vs-RAS attribution below uses
a later soft `hygiene_census` span **2018–2024** (same machine; `--force`).

Issues along the way (resolved during the first local campaign):

1. Missing `BEA_IEA_2017_Exports.csv` — downloaded from GCS.
2. First long run died mid-2021 with no traceback (buffered stdout); rerun with
   `-u` / `PYTHONUNBUFFERED` succeeded.

### Fresh balance (this branch) — item 1 / 2b

| Year | **2b** illicit negatives (below / at / above 0.05 $M) | Item 1: illicit below-eps swept | Illicit below-eps after sweep |
|------|------------------------------------------------------|-----------------------------------|-------------------------------|
| 2018 | **0** (0/0/0) | **0** | **0** |
| 2021 | **0** (0/0/0) | **0** | **0** |
| 2023 | **0** (0/0/0) | **0** | **0** |

**A / item 1.** On today’s balance there are **no illicit offset-residue
negatives** to remove. Item 1 sweeps only that class (`0 < |x| < 0.05` on the
illicit mask); the sidecar records `residue_swept_cells` /
`residue_eps_usd_m=0.05` / `residue_sweep=illicit_below_eps`. An earlier
revision of the sweep used `|x| < eps` on **all** Use cells and reported
~124k “swept” per year — almost entirely exact zeros already at 0, not
economic edits (see §3).

**2b.** **0** illicit Use negatives outside the whitelist
(`sign_lock != -1`, excl. `F03000` / `V00300` / `pattern2017 < 0`).

### 2a — published-pattern fills

#### Walkthrough (what the check counts)

After balance, item 2(a) loads the official 2017 Use/Supply detail as the
**pattern** (`published_2017_panel` in `nowcast_mask.py`). Most 2017 zeros are
true structure and stay empty. The leak set is only the cells that were **zero
in that pattern and nonzero after balance**:

```python
leak = (pattern2017 == 0.0) & (balanced != 0.0)  # zero_pattern_leak
```

The engine is **not** allowed to fill most of those empties: they are frozen as
`mask.structural_zero` (Tier 0). A small set of 2017 zeros is **deliberately
freed** because a 2017 zero there means “no flow that year,” not “never.” When
later years put real dollars there, 2(a) counts them. Those exemptions are built
in `structural_zero_mask` (`nowcast_mask.py` ~459–502):

| Kind | Plain language | Code |
|------|----------------|------|
| Trade / inventory (Use) | Exports and inventory-change columns | `TRADE_FLOW_USE_COLUMNS` (`F04000`, `F03000`) |
| Trade / duties (Supply) | Import / adjustment / duty bridges (with carve-outs) | `TRADE_FLOW_SUPPLY_COLUMNS`, `NEVER_IMPORTED_COMMODITIES` |
| Subsidies (Supply) | Subsidy bridge | `SUBSIDY_FLOW_SUPPLY_COLUMNS` (`SUB`) |
| Fiscal rows (Use) | Tax / subsidy VA rows over industries | `FISCAL_FLOW_USE_ROWS` (`T00TOP`, `T00SUB`, `T00OSUB`) |

So #839’s original **~21–40 cells / ~$0M** audit was mostly **expected exemption
fills** (plus dust), not a Tier-0 breach. An early PR implementation audited
`structural_zero` instead of the published pattern and therefore always reported
**0 / $0M** — that was the wrong population (Wes’s review on #915), not proof
the balance had cleaned up.

#### Measured series (GCS `d2e2112`, 2018–2024)

Re-ran `hygiene_census_gcs` after the published-pattern definition landed:

| Year | Use cells | Use mass ($M) | Supply cells | Supply mass ($M) |
|------|----------:|--------------:|-------------:|-----------------:|
| 2018 | 35 | 171 | 52 | 1,855 |
| 2019 | 35 | 272 | 52 | 5,913 |
| 2020 | **423** | **561,486** | 53 | 27,963 |
| 2021 | **423** | **401,145** | 52 | 27,165 |
| 2022 | 355 | 49,259 | 53 | 38,129 |
| 2023 | 54 | 20,155 | 53 | 14,023 |
| 2024 | 53 | 11,246 | 53 | 11,752 |

Same metric on this branch’s fresh local saves (`b25b8ac`, 2018/2021/2023)
matches GCS to rounding (e.g. 2018 Use 34–35 cells / ~$171M).

**How to read the series.** Use cell counts in **2018–2019** sit in #839’s
historical **~21–40** band; later years are higher. Decomposition on 2018 Use:
all 35 leak cells are **outside** `structural_zero` (exemptions); **34** are
on fiscal rows `T00TOP`/`T00SUB` (~$170M) and **1** on trade/inventory
(`F03000`, ~$0.4M). **2020–2021** jump in Use cells/mass is dominated by large
fiscal-row fills (e.g. pandemic-era subsidy incidence on 2017-zero industry
cells) — still exempt, not an engine breach of Tier 0. Structural-zero leak
remains **0 cells / $0M** every year (engine invariant).

**Mass vs #839’s “~$0M”.** On current vintages, published-pattern **dollar mass
is routinely ≫ $1M**, even in quiet years (~$171M Use in 2018). The issue’s
~$0M figure does not hold for this metric on today’s tables; the useful
comparison is cell counts in quiet years and the fiscal-driven spikes later.

#### Census vs fail gate (why measure fills we do not zero)

Two different jobs:

| | Published-pattern 2(a) | Structural Tier 0 |
|--|------------------------|-------------------|
| Helper | `zero_pattern_leak` | `structural_zero_leak` |
| Role | **Census / YoY telemetry** — how far this year departed from 2017 sparsity on *allowed* empties | **Production fail gate** — engine must not fill frozen zeros |
| `$1M` mass gate | **No** (exemption mass routinely exceeds it) | **Yes** (`ZERO_PATTERN_MASS_USD_M` in `assert_post_balance_hygiene`) |

If nothing is “done” about large exemption fills, that is intentional: those
cells are free by design. The census still earns its keep by confirming fills
land on expected trade/fiscal rows, flagging year shocks (2020–2021), and
keeping #839’s quiet-year cell band grounded. The check that protects later
years is the structural gate (silent when healthy). Seed vs RAS attribution of
those fills is in the next subsection.

#### Seed vs RAS on exemption fills

Same eligible set as non-structural 2(a):
`(pattern2017 == 0) & ~structural_zero`. Question: is the 2020–22 Use mass
mostly already in the **seed** when it enters GRAS, or does RAS **open** those
cells?

**Definitions** (seed = post-`conform_seeds` frame on `YearBalance.seeds`):

| Metric | Meaning |
|--------|---------|
| `seed_fill` | Eligible + nonzero in seed (count + abs seed mass) |
| `bal_fill` | Eligible + nonzero after balance (exemption-restricted 2a) |
| `ras_introduced` | Eligible + seed 0 + bal ≠ 0 — cells RAS opened |
| `ras_cleared` | Eligible + seed ≠ 0 + bal 0 — count + abs seed mass RAS zeroed |
| `ras_moved_l1` | `Σ\|bal − seed\|` on eligible cells that changed |
| `ras_introduced_share` | `ras_introduced_mass / bal_fill_mass` — **primary** |
| `seed_share_of_bal_fill_mass` | Balanced mass on cells that were already nonzero in the seed / `bal_fill_mass` — **location** share only (not seed-dollar attribution) |

**Vintage lock.** Tables below are from one local soft `hygiene_census`
2018–2024 run (matched seed + balanced). Do **not** attach these shares to the
GCS `d2e2112` 2(a) mass table above. Local `bal_fill` cells/mass match that GCS
series to rounding (same spike years and order of magnitude).

**Use results**

| Year | bal_fill cells / $M | seed_fill cells / $M | ras_introduced cells / $M | ras_cleared cells / $M | ras_introduced_share | seed_share (location) | ras_moved_l1 ($M) |
|------|--------------------:|---------------------:|--------------------------:|-----------------------:|---------------------:|----------------------:|------------------:|
| 2018 | 35 / 171 | 35 / 172 | **0 / 0** | 0 / 0 | **0.00** | 1.00 | 2 |
| 2019 | 35 / 272 | 35 / 270 | **0 / 0** | 0 / 0 | **0.00** | 1.00 | 6 |
| 2020 | 423 / 561,436 | 423 / 554,325 | **0 / 0** | 0 / 0 | **0.00** | 1.00 | 7,130 |
| 2021 | 423 / 401,144 | 423 / 394,179 | **0 / 0** | 0 / 0 | **0.00** | 1.00 | 6,982 |
| 2022 | 355 / 49,249 | 355 / 46,566 | **0 / 0** | 0 / 0 | **0.00** | 1.00 | 2,722 |
| 2023 | 54 / 20,155 | 54 / 19,960 | **0 / 0** | 0 / 0 | **0.00** | 1.00 | 208 |
| 2024 | 53 / 11,244 | 53 / 11,150 | **0 / 0** | 0 / 0 | **0.00** | 1.00 | 107 |

**Supply** (same story): `ras_introduced_share = 0` every year; ~52–53
`bal_fill` cells; `ras_moved_l1` peaks ~$5–9B in 2020–22 on cells already open
in the seed.

**Interpretation**

1. **Quiet years (2018–19):** `ras_introduced_share = 0` — every exemption fill
   that survives balance was already nonzero entering the engine. RAS only
   nudges dollars (`ras_moved_l1` ~$2–6M on Use).
2. **2020–21 Use spike:** Still `ras_introduced_share = 0` and
   `seed_fill_cells == bal_fill_cells` (423). The hundreds-of-billions of
   fiscal mass are **seed incidence** (post-conform seed already carries them),
   not RAS inventing new 2017-zero cells. RAS does **reshape** those cells
   (`ras_moved_l1` ~$7B) but does not open the set.
3. **2022–24:** Same zero introduced share; cell counts and mass unwind toward
   quiet-year levels while L1 falls.
4. **Standing hygiene story unchanged.** Exemption fills remain free by design;
   Tier 0 stays the fail gate. Nothing here argues for a production assert on
   published-pattern mass or RAS-introduced share — RAS is not the opener.

Reproduce (matched seed + balanced; ~15–17 min/year)::

    uv run python -m bedrock.analysis.nowcasting.ras_improvements.hygiene_census \
        --years 2018,2019,2020,2021,2022,2023,2024 --force

Balanced-only YoY (no seed split)::

    uv run python -m bedrock.analysis.nowcasting.ras_improvements.hygiene_census_gcs \
        --years 2018,2019,2020,2021,2022,2023,2024
    uv run python -m bedrock.analysis.nowcasting.ras_improvements.hygiene_summary

### Contrast: older GCS vintage `163db0e` (2026-09-01)

| Year | 2a Use cells / mass (published pattern) | 2b illicit (below 0.05 $M) | After sweep: illicit below-eps |
|------|-----------------------------------------|----------------------------|--------------------------------|
| 2018 | 35 / $171M | 0 (0) | 0 |
| 2021 | 423 / $415,278M | **258** (**62**) | **0** (those 62 zeroed) |
| 2023 | 54 / $20,189M | **328** (**3**) | **0** (those 3 zeroed) |

On that vintage, item 1 **does** clear the sub-eps illicit set. Many illicit
cells there are **above** 0.05 $M (up to hundreds of $M) — that is **not** the
“all below $50k / −$0.0M” census in #839, and today’s hygiene audit would
**fail** those tables. Latest GCS `d2e2112` and today’s fresh run are already
illicit-clean, like 2018 in the issue.

**Reproduce without rebalancing** (uses saved / GCS artifacts)::

    uv run python -m bedrock.analysis.nowcasting.ras_improvements.hygiene_summary

**Reproduce with a full Step 5 balance** (includes seed-vs-RAS split)::

    uv run python -m bedrock.analysis.nowcasting.ras_improvements.hygiene_census \
        --years 2018,2019,2020,2021,2022,2023,2024 --force

---

## 2. Root cause: why `163db0e` had large illicit negatives current balances lack

**Root cause: the Sep 7 rebuild’s value-added reconciliation (#856), not the
#839 residue sweep.**

### What `163db0e` was

- Built **2026-09-01** on branch `soft_targets_step5` (soft RAS).
- `d2e2112` was built **2026-09-07** on `ec-conditioning-corroboration-862`
  after the [7 September rebuild](../progress_report.md#6a-the-7-september-2026-rebuild)
  (methods changed in seven places since Sep 3).

### What the “large illicit” actually was

Not offset dust. Concentrated **negative intermediate cells** in a few
industry columns:

| Year | Hot column | Illicit mass | Share of year’s illicit |
|------|------------|--------------|-------------------------|
| 2021 | `5191A0` (other info services) | **−$3,750M** | 36 cells, ~93% of 258 |
| 2023 | `33451A` (+ `114000`, `5191A0`, …) | **−$1,858M** in `33451A` alone | 101 cells |

Every one of those cells **became non-negative** in `d2e2112` (258/258 and
328/328 cleared). Fresh `b25b8ac` matches that: **0 illicit**.

### Mechanism (shown on `5191A0` / 2021)

| | `163db0e` | `d2e2112` |
|--|-----------|-----------|
| Column total | $11,133M | $11,098M (≈ unchanged) |
| VA total | $14,883M | $9,527M (**−$5,356M**) |
| Intermediate (non-VA) | **−$3,750M** (all illicit) | **+$1,571M** |
| `V00300` | +$5,886M | **−$3,313M** (allowed) |

Before [#856](https://github.com/cornerstone-data/bedrock/pull/856), VA used
NIPA totals on **frozen 2017 industry shares**, so industries like internet
publishing / `5191A0` got too much surplus. The balance still enforced T1/T18
from that block, so GRAS had to **drive free intermediate cells negative** to
close the column. After reconciliation, surplus absorbs the gap on unlocked
`V00300`, and intermediates stay positive.

PR #856 explicitly called out this class of misallocation (e.g. internet
publishing **−$75B** vs published VA) and that built SUTs were stale until
rebuilt — which is exactly the Sep 1 → Sep 7 artifact jump.

### Relation to #839

#839’s Sep 3 audit (sub-$50k dust totaling −$0.0M) is a **different, smaller**
defect. The hundreds-of-$M illicit on `163db0e` is the pre-#856 VA-split bug.
Current balances don’t show it because the seed/targets no longer force that
squeeze.

**Reproduce**::

    uv run python -m bedrock.analysis.nowcasting.ras_improvements.illicit_vintage_diff

---

## 3. Implementation follow-up: why item 1 is an *illicit* below-eps sweep

#839 asked to zero cells with `|x|` below ε at save time so the Use table is
bit-clean of offset residue. The first implementation took that literally and
swept **every** Use cell with `|x| < 0.05 $M`. Review against saved vintages
showed that was the wrong cut:

1. **The ~202 sub-\$50k illicit census does not reappear on current seeds.**
   Fresh balances are already at **0 illicit** of any size. The large illicit
   counts on `163db0e` were mostly **above** ε (the pre-#856 VA squeeze in §2),
   not the publication-dust class #839 described. So item 1 is a standing
   cleanup for when that dust class returns, not a fix that must rewrite today’s
   tables.

2. **Among cells that actually change under a broad `|x| < ε` sweep
   (`0 < |x| < ε`), almost none are zero in the 2017 detail pattern** — on
   `d2e2112` / `163db0e`, ~93–100% sit on **2017-nonzero** positions (tiny
   remnants of allowed structure). The engine still preserves
   `mask.structural_zero` (Tier 0) exactly; published-pattern 2(a) is a
   separate census (see §1 walkthrough) — not a reason to broaden the item-1
   sweep onto 2017-nonzero cells.

3. **The ~124k sidecar count was misleading.** `|x| < ε` includes exact zeros.
   On a 2023 Use table (~171k cells), ~124k are already 0; only ~15–20 are true
   dust. Broad sweep “re-zeroed” the sparse pattern and counted it as residue.

4. **A/L and EF impact of narrowing is negligible on clean tables.** Leaving
   those ~15–20 non-illicit dust cells in place moves ~\$0.4M L1 on a
   \$60–80T Use table (~5×10⁻⁹). That will not show up in A, L, D, or N at
   any precision used in diagnostics. Narrowing is a correctness choice, not an
   EF tradeoff.

**Decision.** Item 1 sweeps only **illicit** Use negatives with
`0 < |x| < RESIDUE_EPS_USD_M` (same whitelist as 2b). Positive near-zeros and
whitelisted negatives stay. Sidecar: `residue_sweep=illicit_below_eps`, and
`residue_swept_cells` counts cells that actually change (expect **0** on
current builds; non-zero if offset dust returns). Above-ε illicit remains a
hard fail in `assert_post_balance_hygiene`, not a save-time sweep.
