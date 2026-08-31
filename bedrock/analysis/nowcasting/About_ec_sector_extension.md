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
- **State & local government**: not EC-covered — the *Economic* Census excludes government-owned
  establishments. C1 names its real census: the Census of Governments — see §C1 below and Wave 3.
- **Oil and gas** `211`: single-industry summary group — a within-group conditioner has nothing to move.

## C1 grounds the whole program (Wes, reading `bea_2017_benchmark_sources.md`)

Three rows of C1 settle questions the wedge table alone could not:

- **The annual column for nearly every service family reads "Census Bureau SAS data; for 2022 only
  Census Bureau QSS data"** — BEA's 2022 detail for these sectors is quarterly-survey-carried, not
  census-based. That *is* the absorption gap, in BEA's own words; the class-1 signals measure it.
- **Where RCPTOT failed, C1 shows BEA used a different EC variable, not no census**: trade margins for
  42/44-45; for `81`, *"2017 Economic Census taxable revenue and tax-exempt expenses"* — the nonprofit
  side is measured by expenses, which is exactly the wedge instability the receipts-only check saw.
  The repairs are written in C1's own sources column.
- **State and local government's benchmark source is the 2017 Census of Governments** (+ Government
  Finances + Public Employment) — not the Economic Census, so a COG-2022 conditioner is the analogue,
  on a different instrument. (Federal is budget data; no census applies.)

## The waves

**Wave 1 — IMPLEMENTED** (`SECTOR_CONDITIONERS`): manufacturing + health `62` + admin/waste `56` +
accommodation/food `72` + professional `54` + arts `71`, ~379bn/yr of within-group conditioning at 2022.
Screens per family caught exactly the nonemployer-heavy industries the EC cannot see (`711500`
independent artists, `5419A0`/`541920`) plus manufacturing's `334111` and the `334118` hold.

**Wave 2 — member exclusions / concordance, then re-verdict**: `48-49` minus rail+postal (C1: EC except
rail), `52` minus funds/trusts `525000`, `81` via the taxable/tax-exempt two-part construction, `212`
on the bridged machinery, `44-45` and `51` with explicit NAICS-2022 concordance entries, `53` restricted
to the establishment subfamily (dwellings are NIPA-imputed per C1).

**Wave 3 — different variables or instruments**: trade via EC-2022 margins (interacts with #778 — trade
output *is* margin), construction via EC value of construction work (or VIP), utilities' private
universe, and **S&L government via the 2022 Census of Governments** (the repo's #578 gov-finance
machinery is the starting point; note the gov *commodity-mix* bridge was a measured no-go — this is the
industry-output axis, a different question).

**Wave 2 — SAMPLED AND SETTLED** (Wes's directive: sample first, wire only what clearly works):

| candidate | measure sampled | verdict |
|---|---|---|
| wholesale `42` | `GRMARG`, `ecnmargin`/`ecngrmargprof` | **WIRED** — merchant wedge 0.74–0.95, moves 71bn; `424700` held (0.42, MSBOs), `425000` on kind average |
| other services `81` | taxable `RCPTOT` + exempt `OPEX` (`TAXSTAT`) | **WIRED** — core wedge 0.80–0.91, moves 22bn; screens catch grantmaking pass-through (2.16) and nonemployer personal services |
| transport `48-49` | RCPTOT (rail/postal absent from EC naturally) | **WIRED but structurally near-neutral** (2.4bn): the surviving members are singleton summary groups, so within-group conditioning cannot express the EC's 7% disagreement — that lives between groups, i.e. levels we hold on BEA; `493000` (0.31) and `485000` (0.56, gov-transit boundary) held |
| construction `23` | `RCPNCW`; hunted type-of-work tables | **NOT wired** — net-value by *contractor* NAICS is the wrong axis for BEA's by-*structure-type* detail (coverage 0.18–3.64); `ecnvalcon` is by state, `ecnconact` is other industries' capex. The type-of-construction table is not in the API's obvious datasets |
| finance `52` ex-funds | RCPTOT re-verdict | **NOT wired** — member-level wedges say the C1 supplements are load-bearing: `524113` at 5.46 (premiums vs premiums-less-benefits), `522A00` at 1.79 (FISIM) |
| real estate `532/533` | RCPTOT re-verdict | **NOT wired** — `533000` at 0.30 (IP imputations), diffs to 46% |
| mining `212` / support `213` | bridged RCPTOT | **HELD, ag-class** — C1's annual column is EIA/USGS/MSHA/trade-source, observation-grade independents, and the census *conflicts* with them (coal receipts flat in EC 2022 against the price surge EIA carries; drilling flat against rig counts). Overwriting BEA here needs arbitration, not a growth carry |

Confirmed holds: agriculture (BEA already tracks FIWS), oil-gas `211` (single-industry group), federal
government (no census instrument), retail `44-45` + information `51` (the NAICS-2022 redesigns cross
3-digit boundaries — nonstore dissolved into the store lines — so per-industry bridging to the BEA-2017
axis is not possible from 2022-vintage tables at any variable).

Registry after wave 2: **nine entries, ~475bn/yr of conditioning at 2022.** The committed
alternative-measure data lives in `census_alt/ec_alt_measures.csv`, written by
`bedrock/utils/mapping/write_ec_alt_measures.py`.
