# Should the EC conditioner extend beyond manufacturing? The sector verdicts

The manufacturing entry in `ec_go_adjustment` was licensed by two measured facts: a **tight, stable
shipments-to-GO wedge** (coverage 0.988 median, 0.031 IQR at 2017) and a **detail-mix disagreement BEA
demonstrably had not absorbed** (5.1% value-weighted). Wes's question, 2026-08-30: do the other
EC-covered sectors — and the annually-observed sectors (ag, electricity, gas, mining) — pass the same
two tests, **or does BEA already incorporate their sources** (no comprehensive-update hold applies to
them)? Run with `ec_manufacturing_output_check.implied_bea_growth(prefixes)`, now prefix-parametric.

⚠️ **Regenerate before quoting** — numbers measured 2026-08-30 on the #779 stack.

## The full-scope table (EC 2017→2022 growth vs BEA, within-group, value-weighted)

| family | BEA inds | GO 2022 $B | wtd \|diff\| | % value >5% off | coverage med ± IQR |
|---|---:|---:|---:|---:|---|
| **manufacturing 31-33** (calibration) | 232 | 7,132 | **5.1%** | 29.5% | 0.988 ± 0.031 |
| health 62 | 13 | 3,030 | 5.0% | 61.7% | 0.962 ± 0.053 |
| admin/waste 56 | 9 | 1,467 | 6.4% | 40.7% | 0.905 ± 0.075 |
| accommodation/food 72 | 4 | 1,509 | 3.9% | 34.9% | 0.861 ± 0.204 |
| professional 54 | 14 | 3,112 | 10.0% | 62.9% | 0.838 ± 0.185 |
| arts 71 | 8 | 481 | 4.5% | 37.5% | 0.781 ± 0.230 |
| other services 81 | 10 | 792 | 9.0% | 66.2% | 0.816 ± 0.338 |
| transport 48-49 | 8 | 1,704 | 7.4% | 78.1% | 0.838 ± 0.206 |
| mining 21 | 8 | 891 | 6.3% | 23.9% | 0.847 ± 0.090 |
| utilities 22 | 3 | 699 | 6.5% | 20.4% | **1.186** ± 0.241 |
| construction 23 | 12 | 2,204 | 9.1% | 55.6% | **1.478** ± 0.590 |
| wholesale 42 | 10 | 2,781 | 5.3% | 36.5% | **3.920** ± 3.072 |
| finance 52 | 7 | 3,298 | 7.9% | 85.0% | 1.286 ± **1.166** |
| real estate 53 | 7 | 5,128 | 5.1% | 9.7% | **0.300** ± 0.545 |
| retail 44-45 | — | — | — | — | *bridge fails: the NAICS-2022 retail redesign crosses 3-digit boundaries* |
| information 51 | — | — | — | — | *bridge fails: 5111 publishing moved to 513* |

## Four classes

**1. Extend — same shape as manufacturing.** `62` health (wedge nearly as tight as manufacturing's,
5.0% signal on $3.0T), `56` admin/waste, `72` accommodation/food, and — with a wider but coherent wedge —
`54` professional (10.0% signal on $3.1T; the 0.84 level is partnerships/own-account, plausibly stable)
and `71` arts. Together roughly **$9.6T of GO** with manufacturing-grade or near-grade instruments.

**2. Repairable — needs member exclusions or an explicit concordance, then re-verdict.** `48-49`
(drop EC-out-of-scope rail `482000` and postal `491000` before believing the 78%), `81` (drop religious
`813*`/households `814000`), `21` mining (`212` only — `211000` is a single-industry group, and the
quick-look divergences were NAICS-2022 gold/silver recode artifacts; the bridged machinery handles it),
`44-45` and `51` (the two bridge failures need explicit NAICS-2022 concordance entries — the retail
redesign and the 5111→513 move cross 3-digit boundaries, which the published-parent walk cannot span).

**3. Wrong instrument — RCPTOT is not the concept; do not extend with this data.** `42` wholesale
(coverage 3.9: receipts are sales including COGS, BEA GO is margin — the margin-side conditioning is
#778's published-cells fix, already shipped), `23` construction (1.48: gross billings vs BEA's netted
output; the VIP route from the registry notes remains the right instrument), `52` finance (funds
excluded, insurance measured differently — IQR 1.17), `53` real estate (0.30: owner-occupied housing is
imputed, not an establishment), `22` utilities (1.19: BEA includes public power the EC excludes; and
BEA's gas growth already matches the EC to 3% — EIA-sourced annually).

**4. Already incorporated — Wes's hold hypothesis confirmed; no conditioner.**
- **Agriculture**: not EC-covered; the candidate was ERS FIWS cash receipts — and BEA's detail GO
  already tracks it. Crops and animal aggregates agree within ±2% in most years 2018-2023; member gaps
  are single-digit and mean-reverting (`1121A0` −6→−1%, `112300` +3-4%), nothing like a structural
  census gap. BEA's annual ag source *is* USDA farm income; conditioning would re-derive BEA with noise.
- **State & local government**: not EC-covered at all — the census excludes government-owned
  establishments. No census instrument exists for it, full stop.
- **Oil and gas** `211`: single-industry summary group — a within-group conditioner has nothing to move.

## What this changes in the plan

The registry's next entries are **not** ag/electricity/mining (all held: incorporated, wrong-instrument,
or structural no-op) but the **class-1 services families**, which carry ~2x manufacturing's GO under
comparable instruments — plus the class-2 repairs if their re-verdicts hold. Each entry is the same
construction `ec_go_adjustment` already implements; the work per family is the member-exclusion list or
concordance entries plus a wedge screen, not new machinery.
