# PR #1220 review — observed generation price from EIA-861 sales for resale

**PR:** https://github.com/cornerstone-data/bedrock/pull/1220
**Closes:** #1215 · **Plan:** #902 · **Stacked on:** #1219 · **Project:** [Electricity v3](https://github.com/orgs/cornerstone-data/projects/29)
**Branch:** `electricity_generation_price` → `electricity_price_by_buyer`
**Scope:** +62 / −0 across `electricity_price.py`, its tests, and `utilities_expense_seed.py` (`resale_volume`). CI green. No model output change.

## Verdict

**Approve as the next foundation brick.** This PR supplies an **observed** generation price per kWh so later stack work does not have to invent generation dollars from a share of the electricity row. Nothing in the live model reads it yet. The open design question it surfaces (buyers priced below generation in 2021–22) is already decided upstack in #1221 via a three-year trailing average; this PR correctly leaves that choice to #902 rather than baking in a premature rule.

## What it does

Adds two public helpers on top of #1219’s buyer prices:

1. **`generation_price(year)`** — one national price per kWh **generated**, ¢/kWh, from EIA Form **861 sales for resale** (revenue ÷ MWh via the Public Utility Data Liberation (PUDL) project).  
   - **Sales for resale** = wholesale-style power sales between utilities/marketers, not the retail bill a factory or household pays.
2. **`buyers_below(prices, generation)`** — lists manufacturing (or other) buyers whose delivered ¢/kWh from #1219 sits **below** that generation price (would imply a negative delivery margin).

Also adds `resale_volume()` next to the existing `resale_revenue()` loader so the price is revenue over volume, not a share of IO dollars.

Reported annual series (from the PR body):

| | 2017 | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Generation price, ¢/kWh | 4.11 | 4.36 | 4.22 | 3.83 | 5.68 | 6.29 | 5.27 | 5.00 |
| EIA retail (all sectors), ¢/kWh | 10.48 | 10.53 | 10.54 | 10.59 | 11.10 | 12.36 | 12.68 | 12.94 |
| Manufacturing industries below it | 1 | 1 | 0 | 0 | 6 | 4 | 1 | 0 |
| Their share of manufacturing kWh | 2.3% | 2.3% | 0 | 0 | 16.0% | 11.7% | 1.0% | 0 |

**Basis note:** this price is per kWh **generated**. Buyer prices in #1219 are per kWh **delivered**. Delivery margin per delivered kWh is roughly buyer price − `p_gen`, with line losses charged through delivery (#1217).

## Relation to earlier electricity disaggregation decisions

### Discussion #85 (first G/T/D)

- Generation vs T/D dollars came from BEA gross-output shares, not an observed wholesale price.
- Equal-price buyer split; later retail-price experiments warped class MWh vs EIA.
- #1220 does not touch buyer allocation; it only quotes generation.

### Discussion #88 (EIA-anchored G/T/D)

- Used **one national generation price `p`**, but constructed it so generation dollars matched BEA’s generation **share of the electricity row** (~34%). That ties `p` to the same bills it divides.
- Leftover bill → T&D; water-fill when `p × MWh` would exceed a buyer’s bill.
- #1220 **replaces that construction**: `p_gen` is an external EIA-861 observation. That is a core Electricity v3 requirement (#1215 / #902 acceptance: generation price must not be a function of the electricity row’s dollar total).

### Discussion #90 (MECS inside Industrial)

- Kept #88’s single national `p`; used MECS **kWh shares**, not prices, inside manufacturing.
- #1220 does not change factory kWh ranking; together with #1219 it feeds the later bill÷price path.

### Electricity v3 (#902) and this PR

v3 charges each buyer `gen_i = kWh_i × p_gen` (floored at the bill) and puts the rest in `42ELEC`. #1220 is the annual observed `p_gen` input. It explicitly lists three options for buyers below `p_gen` in the 2021–22 wholesale spike; **#1221 chooses option 2** (three-year trailing average of sales for resale), so single-year negatives in 2021–22 largely go away before the model split. Reviewers should read #1220 as the observed annual series + helper, not as the final production smoother.

## What looks good

- Small, scoped PR; reuses existing PUDL resale revenue path and adds the matching volume.
- Correct methods break with #88’s bill-tied `p`.
- Documents delivered vs generated basis, double-counting in wholesale volume, and possible transmission bundling in sales for resale.
- Surfaces `buyers_below` instead of papering over negative margins.
- Synthetic unit tests for `resale_price` and `buyers_below`; format / lint / typecheck / tests pass in CI.

## Watch items (non-blocking for merge)

1. **Decision handoff:** this PR still presents three options for buyers below `p_gen`. #902/#1221 already pick the trailing average + floor. Fine to merge #1220 as-is; do not treat the annual series alone as the production `p_gen` without that follow-on.
2. **#1215 “check against FERC / Table 8.3”** is named in the issue and not executed in this PR. Worth a later note or check; not required to land the helper.
3. **2021 marketer revenue spike** (~$67.5bn vs ~$30–40bn nearby years) is flagged but unexplained; it lifts annual `p_gen` and is part of why trailing averages appear upstack.
4. **Sales for resale volume** falls over the span and counts power traded more than once — good as a **price** measure, weak as a generation quantity. The PR states this clearly; consumers must multiply by a separate kWh basis (eGRID / delivered / losses), not by resale MWh.
5. Tests are synthetic only (same pattern as #1219); real series in the PR body are from a local run.

## Bottom line

#1220 cleanly adds the observed generation-price half of Electricity v3’s pricing pair (buyer delivered prices in #1219; wholesale-style `p_gen` here). It correctly refuses to keep #88’s bill-tied generation price. Merge as foundation; expect #1221 to smooth and apply it, and leave FERC cross-checks / marketer-spike forensics as follow-ups rather than merge blockers.
