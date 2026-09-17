# #839 hygiene: balance census and illicit-vintage root cause

Standing report for the [#839](https://github.com/cornerstone-data/bedrock/issues/839)
hygiene workstream (residue sweep + zero-pattern / sign audits). Reproduce with
the modules in this package; see [`README.md`](README.md).

Production hooks: `bedrock.transform.iot.nowcast_sut_assembly`
(`RESIDUE_EPS_USD_M = 0.05`, `ZERO_PATTERN_MASS_USD_M = 1.0`,
`assert_post_balance_hygiene` after restore, Use sweep in `save_balance`).

---

## 1. Pipeline status (balance check 2018 / 2021 / 2023)

Fresh `balance_year` for **2018, 2021, 2023** completed (~15–17 min each) and
saved under the analysis output dir (`b25b8ac` artifact hash).

Issues along the way (resolved during the first local campaign):

1. Missing `BEA_IEA_2017_Exports.csv` — downloaded from GCS.
2. First long run died mid-2021 with no traceback (buffered stdout); rerun with
   `-u` / `PYTHONUNBUFFERED` succeeded.

### Fresh balance (this branch) — pre-sweep → post-sweep

| Year | **2a** Use leak cells / mass | **2a** Supply | **2b** illicit negatives (below / at / above 0.05 $M) | Item 1: cells swept (`|x|<0.05`) | Illicit below-eps after sweep |
|------|------------------------------|---------------|------------------------------------------------------|-----------------------------------|-------------------------------|
| 2018 | 0 / **$0M** | 0 / $0M | **0** (0/0/0) | 123,960 | **0** |
| 2021 | 0 / **$0M** | 0 / $0M | **0** (0/0/0) | 123,771 | **0** |
| 2023 | 0 / **$0M** | 0 / $0M | **0** (0/0/0) | 124,362 | **0** |

**A / item 1.** On today’s balance there are **no illicit offset-residue
negatives** to remove. The sweep still zeros ~124k sub-0.05 $M cells (mostly
positive dust); the sidecar records `residue_swept_cells` /
`residue_eps_usd_m=0.05`. Post-save Use has **0** illicit below-eps cells.

**2a.** Structural-zero leak mass is **$0** on both blocks (0 cells) — passes
the mass gate.

**2b.** **0** illicit Use negatives outside the whitelist
(`sign_lock != -1`, excl. `F03000` / `V00300` / `pattern2017 < 0`).

### Contrast: older GCS vintage `163db0e` (2026-09-01)

| Year | 2a mass | 2b illicit (below 0.05 $M) | After sweep: illicit below-eps |
|------|---------|----------------------------|--------------------------------|
| 2018 | $0 | 0 (0) | 0 |
| 2021 | $0 | **258** (**62**) | **0** (those 62 zeroed) |
| 2023 | $0 | **328** (**3**) | **0** (those 3 zeroed) |

On that vintage, item 1 **does** clear the sub-eps illicit set. Many illicit
cells there are **above** 0.05 $M (up to hundreds of $M) — that is **not** the
“all below $50k / −$0.0M” census in #839, and today’s hygiene audit would
**fail** those tables. Latest GCS `d2e2112` and today’s fresh run are already
illicit-clean, like 2018 in the issue.

**Reproduce without rebalancing** (uses saved / GCS artifacts)::

    uv run python -m bedrock.analysis.nowcasting.ras_improvements.hygiene_summary

**Reproduce with a full Step 5 balance**::

    uv run python -m bedrock.analysis.nowcasting.ras_improvements.hygiene_census \
        --years 2018,2021,2023

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
