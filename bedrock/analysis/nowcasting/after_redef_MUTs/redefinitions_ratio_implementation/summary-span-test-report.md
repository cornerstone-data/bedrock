# Summary redefinition span test report

This report asks a follow-on question after [`ratio-reconstruction-report.md`](ratio-reconstruction-report.md): the ratio method in [`ratio-plan.md`](ratio-plan.md) can rebuild the **2017** after-redefinitions tables cell by cell — but does freezing those 2017 movements still look reasonable in **later years**?

We answer that at the **summary** (coarse industry) level using BEA’s published before- and after-redefinitions Make / Use / value added / import tables for 2017–2024. Margins are not scored here (no matching annual summary margins series). Detail-level year-`t` checks still wait on Step 6 before-redefinitions inputs.

## 1. Summary

### Overall conclusion

**Verdict: mixed — do not treat 2017 reconstruction alone as enough for year-`t`.**

- The summary ratio path is **wired correctly** (2017 round-trip **PASS**).
- Freezing 2017 movements tracks BEA’s later published after tables **on average** (Use L1 about **0.3%** – **1.2%** over scored span years). See section 4 for industry-level fit.
- The detail→summary rollup gate is **False** (section 2). That is a concordance diagnostic only; learning and scoring use published summary tables, so it does **not** by itself reject the ratio method.

Relative to [`ratio-reconstruction-report.md`](ratio-reconstruction-report.md) and [`ratio-plan.md`](ratio-plan.md): keep the 2017 operator story when round-trip passes; judge year-`t` carry from sections 4–5 using the bands in [`summary-span-test-plan.md`](summary-span-test-plan.md).

### Scorecard

| Check | Result | What it means |
| --- | --- | --- |
| Rollup gate (section 2) | **False** | Detail added up to summary vs published summary before within $0.5M |
| 2017 summary round-trip (section 3) | **True** | Learned 2017 summary ratios rebuild published 2017 after |
| Use L1 relative error (section 4) | **0.3%** – **1.2%** | Min–max of Use table L1 across scored span years |

**Definition — L1 relative error.** For one year and one table:

```text
sum(|built − published|) / sum(|published|)
```

Only cells with published after greater than $0.5M are included. That ratio is the total dollar gap as a share of scored published after dollars. The **range** above is the minimum and maximum of that Use number across scored span years — not a confidence interval.

### How to read the rest of this report

- **Support keeping the ratio approach** if round-trip passes and later-year Use gaps stay small without sector meltdowns.
- **Treat as mixed / discuss before merge** if L1 stays modest but many industries are still >1% off, or drift worsens.
- **Reject / pivot** if L1 hits double digits in several years, or crisis / watch sectors explode.

**This run falls in the mixed band.** Details: sections 2–6.

## 2. Rollup gate (2017 before detail → summary)

**What this test is doing.** It takes the detailed 2017 before-redefinitions tables, adds child industries/commodities into their first summary parent, and compares that rolled-up result to BEA’s published summary before-redefinitions tables.

**Why we do it.** The span test learns and scores on **published summary** tables, not on rolled detail. This gate only checks whether our detail→summary concordance is close enough that a reader can trust comparisons that mix those two worlds. It is a setup / taxonomy diagnostic, not a test of the ratio formula in [`ratio-plan.md`](ratio-plan.md).

**Inputs.** Detailed 2017 before Make, Use, value added, and import; published summary 2017 before for the same four blocks; BEA detail→summary parent maps (first parent only).

**What the outputs mean.**

- `ok` — every compared cell matches within $0.5M, with no missing or extra cells.
- `max abs diff` — largest single-cell dollar gap.
- `partial` / `miss` / `extra` — cells that disagree, exist only on one side, or exist only on the other.

| Block | ok | max abs diff | partial | miss | extra |
| --- | --- | ---: | ---: | ---: | ---: |
| Make | False | 7,000,000 | 267 | 5 | 0 |
| Use | False | 11,000,000 | 1741 | 10 | 0 |
| VA | False | 5,000,000 | 63 | 0 | 0 |
| Import | False | 10,000,000 | 921 | 83 | 0 |

### Conclusion

**Fail — concordance is imperfect; this does not reject the ratio method.** Learning and scoring in sections 3–4 always use **published** summary before/after pairs. Treat this as a warning against trusting any story that *depends* on rolling detail up to summary for acceptance.

### Why the rollup gate failed

The gate fails because **BEA’s published summary before-redef tables are not the mechanical sum of the published detail before-redef tables**. The rollup code itself is doing the right arithmetic.

`run_rollup_gate` loads 2017 **detail** before-redef Make / Use / VA / Import, rolls each to summary via `parents[0]` groupby-sum, and compares to published **summary** before-redef at `ATOL = $0.5M`.

| Step | What the code does | Pass/fail | Why |
| --- | --- | --- | --- |
| 1. Load detail before-redef | `load_2017_*_before_redef_usa()` — detail workbooks, ×$1M → USD | OK | Tables load; 402×402 industry/commodity axes |
| 2. Load published summary before-redef | `load_summary_*_before_redef_usa(2017)` — summary workbooks, same unit scale | OK | Same summary code sets as rollup output |
| 3. Build detail→summary maps | `first_parent_map(industry/commodity concordance)` | OK | **0 multi-parent codes**; all 402 detail industries & commodities map; every summary code is targeted; nothing unmapped |
| 4. Groupby-sum axes | `_groupby_sum_axis` then reindex to summary codes | OK | Rolled cell **equals** sum of detail children exactly (verified on miss cells and on Make `332×332`) |
| 5. VA row remap | `V00100→V001` etc., then industry rollup | OK | Same pattern as Make/Use; no structural miss/extra |
| 6. Compare at $0.5M | `compare_tables(..., atol=ATOL)` | **FAIL** | Candidate (rolled detail) ≠ reference (published summary) |

Failure is entirely in **step 6**: two independently published BEA tables disagree. Not a mapping bug, not a coding bug in the sum.

**Evidence (re-checked locally).**

- **Mapping is clean.** Multi-parent count = 0. Coverage = 402/402. No “wrong parent” path to blame.
- **Arithmetic matches detail.** Example Make miss `334×337`: sum of detail children = **0**, rolled = **0**, published summary = **$1M**. Example largest Make partial `332×332`: sum of 20×20 detail children = **325,385** ($M) = rolled; published summary = **325,392** → gap **−7** ($M) = the reported max abs diff.
- **Gaps are discrete million-USD integers**, not float dust:

| Block | Partial pattern | Misses | Economy gap (rolled − pub, $M) |
| --- | --- | ---: | ---: |
| Make | mostly ±1 or ±2; max −7 | 5 cells, all pub=$1M & detail sum=0 | −71 |
| Use | same; max −11 | 10 cells, same $1M dust pattern | −489 |
| VA | mostly ±1; max −5 | 0 | −8 |
| Import | mostly ±1–2; max −10 | 83 cells, pub mostly $1–3M, detail sum=0 | −851 |

So:

- **PARTIAL** = summary cell exists on both sides but published summary ≠ sum of detail (typical ±1–2 million USD).
- **MISS** = published summary has a small nonzero ($1–3M) where every detail child is zero — summary-only dust, not a missing map entry.
- **EXTRA** = 0 everywhere — the rollup never invents codes the summary doesn’t have.

Relative to cell size these gaps are tiny (e.g. $7M on a $325B Make diagonal ≈ 2×10⁻⁵). They still fail a hard `$0.5M` / `max_partial=0` gate.

| Hypothesis | Verdict |
| --- | --- |
| Bug in groupby / axis orientation | No — rolled ≡ child sum |
| Wrong / incomplete concordance | No — full coverage, no multi-parent ambiguity |
| `parents[0]` cutting multi-parent mass | No — there are no multi-parent rows |
| Unit / ×$1M mistake | No — both sides scaled the same way |
| **Published summary ≠ aggregation of published detail** | **Yes — this is the failure** |

BEA’s summary MUT is its own estimate (and/or separately rounded/balanced), not a forced sum of the detail MUT. This gate is a taxonomy/concordance diagnostic, and it correctly reports that those two published worlds don’t match cell-by-cell at $0.5M — which is why the span test learns and scores on **published** summary pairs, not on rolled detail.

## 3. 2017 summary round-trip

**What this test is doing.** Same idea as the detail acceptance in [`ratio-reconstruction-report.md`](ratio-reconstruction-report.md), but on summary tables: learn movement ratios from the 2017 published summary before/after pair, apply them back to the 2017 before tables, and ask whether published 2017 after is recovered.

**Why we do it.** If this fails, later-year span scores are untrustworthy — the operator, loaders, or industry filter would be broken before we even leave 2017. If it passes, we know the summary path implements the same ratio story as [`ratio-plan.md`](ratio-plan.md) on the tables we will freeze.

**Inputs.** Published summary 2017 before Make / Use / value added / import; published summary 2017 after from the matched 1997–2024 BEA vintage files; empty margins stub (API only).

**What the output means.**

Published before + learned ratios vs 2024-vintage after: **PASS**

PASS means all four blocks match within $0.5M cell by cell. FAIL means at least one block does not.

### Conclusion

**PASS — the summary ratio path is accepted for 2017.** This is the summary analogue of the detail full-grid accept in [`ratio-reconstruction-report.md`](ratio-reconstruction-report.md). Span scores in section 4 are interpretable: failures there are about **generalization**, not a broken learn/apply implementation.

## 4. Span scores (2018–2024)

**What this test is doing.** Freeze the 2017 summary ratios from section 3. For each later year, take that year’s published summary **before** tables, apply the frozen ratios scaled by that year’s industry gross output, and compare the result to that year’s published summary **after** tables (same BEA vintage as learning).

**Why we do it.** This is the first out-of-sample check of the production Step 7 story in [`ratio-plan.md`](ratio-plan.md): learn movements once in 2017, carry them forward. The 2017 reconstruction report only shows in-sample fit; reviewers asked whether that freeze still tracks BEA’s later after tables at summary level (without waiting on detail Step 6).

**Inputs.** Frozen 2017 summary ratios; published summary before for each score year; published summary after for each score year (2024-vintage workbooks); industry gross output from that year’s before Make.

**What the columns mean.**

- **L1 rel err** — sum of absolute dollar gaps ÷ sum of absolute published after dollars, counting only cells whose published after amount exceeds $0.5M. A single number for “how far off is the whole table?” `n/a` means no cells cleared that floor.
- **cells** — how many published cells entered that L1 calculation.
- **>1% / >25% / >50% inds** — how many industries have at least one scored cell whose relative error exceeds that threshold (Make: industry rows; Use / value added / import: industry columns). High counts mean the gap is not just a few big cells. The three columns are **nested**, not exclusive bins: an industry with max cell rel err >50% is counted in all three.
- **worst cell** — the single scored cell with the largest absolute dollar mismatch `|built − published|` (same `$0.5M` published floor as L1). Shown as gap in millions of USD, then `row×col`, then that cell’s relative error `|built − published| / |published|`. This is one cell’s dollar miss, not the industry L1 contribution in **worst inds**.
- **worst inds** — industries contributing the most absolute dollar error across all of their scored cells (top five shown).

| Year | Block | L1 rel err | cells | >1% inds | >25% | >50% | worst cell | worst inds |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| 2018 | Make | 0.19% | 425 | 30 | 23 | 20 | $9,321M (5412OP×5412OP, 0.5%) | 514, 5412OP, 5415, 525, 42 |
| 2018 | Use | 0.35% | 3824 | 60 | 24 | 17 | $4,733M (81×GFGD, 123.4%) | 5412OP, GFGD, GSLG, 722, 525 |
| 2018 | VA | 0.65% | 210 | 30 | 2 | 0 | $23,162M (V001×5412OP, 2.6%) | 5412OP, 514, GFGD, 721, 334 |
| 2018 | Import | 0.25% | 2107 | 54 | 9 | 5 | $1,036M (5412OP×5412OP, 11.5%) | 5412OP, GSLG, 61, 81, GFGN |
| 2019 | Make | 0.25% | 419 | 32 | 24 | 23 | $15,853M (514×514, 4.6%) | 514, 42, 525, 5412OP, 5415 |
| 2019 | Use | 0.50% | 3829 | 62 | 27 | 19 | $4,999M (81×GFGD, 120.8%) | 5412OP, 722, GFGD, 81, GSLG |
| 2019 | VA | 0.83% | 210 | 41 | 2 | 0 | $25,939M (V001×5412OP, 2.7%) | 5412OP, 514, GFGD, 42, 325 |
| 2019 | Import | 0.46% | 2125 | 56 | 17 | 13 | $1,294M (5412OP×5412OP, 13.9%) | 5412OP, 81, GSLG, 441, 4A0 |
| 2020 | Make | 0.54% | 412 | 33 | 24 | 23 | $34,719M (5412OP×5412OP, 1.7%) | 514, 5412OP, 722, 42, 334 |
| 2020 | Use | 0.88% | 3798 | 61 | 28 | 21 | $7,701M (55×722, 12.5%) | 722, 5412OP, 61, GSLG, 81 |
| 2020 | VA | 1.43% | 210 | 49 | 5 | 2 | $59,090M (V001×5412OP, 5.8%) | 5412OP, 514, 81, 42, 334 |
| 2020 | Import | 0.84% | 2069 | 58 | 27 | 19 | $1,703M (3361MV×81, 88.0%) | 81, 441, GSLG, 5412OP, 4A0 |
| 2021 | Make | 0.58% | 420 | 34 | 24 | 21 | $39,024M (514×514, 8.6%) | 514, 722, 42, 532RL, 23 |
| 2021 | Use | 1.11% | 3824 | 62 | 31 | 26 | $19,511M (523×GSLG, 1914.8%) | 722, GSLG, 5412OP, 525, 61 |
| 2021 | VA | 1.62% | 210 | 54 | 6 | 3 | $36,157M (V001×5412OP, 3.3%) | 5412OP, GSLG, 514, 525, 81 |
| 2021 | Import | 0.67% | 2095 | 58 | 25 | 20 | $1,527M (3361MV×81, 40.5%) | 81, 441, 5412OP, GSLG, 4A0 |
| 2022 | Make | 0.66% | 419 | 36 | 26 | 25 | $47,352M (514×514, 9.4%) | 514, 5412OP, 722, 42, 213 |
| 2022 | Use | 0.94% | 3849 | 63 | 29 | 24 | $17,906M (523×GSLG, 1796.0%) | GSLG, 5412OP, 722, 514, 525 |
| 2022 | VA | 1.98% | 210 | 50 | 7 | 3 | $50,221M (V001×5412OP, 4.1%) | 5412OP, 514, GSLG, 525, GFGN |
| 2022 | Import | 0.53% | 2070 | 60 | 29 | 18 | $910M (3361MV×441, 59.9%) | 81, 5412OP, GSLG, 441, 4A0 |
| 2023 | Make | 0.53% | 419 | 32 | 25 | 23 | $52,622M (514×514, 9.8%) | 514, 722, 42, 532RL, 334 |
| 2023 | Use | 1.09% | 3833 | 62 | 34 | 26 | $18,329M (523×525, 12.1%) | 722, GSLG, 5412OP, 514, 525 |
| 2023 | VA | 1.59% | 210 | 51 | 6 | 1 | $31,927M (V001×514, 20.6%) | 514, GSLG, 5412OP, 525, 42 |
| 2023 | Import | 0.72% | 2048 | 56 | 26 | 19 | $1,281M (3361MV×81, 17.8%) | 81, GSLG, 441, 4A0, 5412OP |
| 2024 | Make | 0.60% | 417 | 34 | 25 | 23 | $64,746M (514×514, 10.7%) | 514, 722, 42, 532RL, 334 |
| 2024 | Use | 1.24% | 3836 | 64 | 33 | 26 | $23,586M (523×525, 13.2%) | 5412OP, GSLG, 722, 525, 514 |
| 2024 | VA | 1.61% | 210 | 54 | 4 | 1 | $37,019M (V003×GSLG, 17.0%) | 514, GSLG, 525, 5412OP, 42 |
| 2024 | Import | 0.64% | 2043 | 57 | 28 | 19 | $1,284M (3361MV×81, 15.7%) | 81, 4A0, 441, GSLG, 514 |

### Conclusion

**Mixed on out-of-sample carry — average error stays small; industry-level fit does not.** Use L1 remains low single digits (**0.3%** – **1.2%**) but many industries still have cells >1% off. Not “mostly supportive”; not “reject/pivot” on L1 alone. **Discuss before treating frozen 2017 ratios as settled for year-`t`.** See section 5 for watch-sector concentration.

## 5. Worst sectors notes

**What this section is doing.** It flags when pre-chosen “watch” industries appear in each block/year’s top-10 dollar error contributors. Watch codes: `22` (utilities), `23` (construction), `42` (wholesale), `721` (accommodation), `722` (food services / restaurants), `HS` (housing).

**Why we do it.** Economy-wide L1 (section 4) can look fine while a few hard sectors absorb most of the miss — the same totals trap the reconstruction docs warn about. These sectors are also ones where redefinitions and mix shifts often matter.

**Inputs.** The per-block/year worst-industry lists from section 4, filtered to the watch set above.

**What the lines mean.** A bullet means that watch code was among the ten industries with the largest absolute dollar gap for that year and table. Absence of a code does not mean that sector matched perfectly — only that it was not in the top ten.

- 2018 Make: highlighted among worst — 42, 23
- 2018 Use: highlighted among worst — 722, 721
- 2018 VA: highlighted among worst — 721, HS
- 2018 Import: highlighted among worst — 42
- 2019 Make: highlighted among worst — 42, 722
- 2019 Use: highlighted among worst — 722, 721
- 2019 VA: highlighted among worst — 42, 721
- 2019 Import: highlighted among worst — 42
- 2020 Make: highlighted among worst — 722, 42
- 2020 Use: highlighted among worst — 722, 721
- 2020 VA: highlighted among worst — 42, 721
- 2020 Import: highlighted among worst — 722, 42, 23
- 2021 Make: highlighted among worst — 722, 42, 23
- 2021 Use: highlighted among worst — 722, 721
- 2021 VA: highlighted among worst — 42, 23
- 2021 Import: highlighted among worst — 722, 42
- 2022 Make: highlighted among worst — 722, 42, 23
- 2022 Use: highlighted among worst — 722, 721
- 2022 VA: highlighted among worst — 42, 721
- 2022 Import: highlighted among worst — 23
- 2023 Make: highlighted among worst — 722, 42, 721
- 2023 Use: highlighted among worst — 722, 721
- 2023 VA: highlighted among worst — 42, 721, 722
- 2023 Import: highlighted among worst — 722, 23
- 2024 Make: highlighted among worst — 722, 42
- 2024 Use: highlighted among worst — 722, 721
- 2024 VA: highlighted among worst — 42, 721, 722
- 2024 Import: highlighted among worst — 722, 23

### Conclusion

**Systematic watch-sector misses — reinforces a mixed (or worse) verdict, not a clean pass.** Recurring codes among top-10 dollar errors: 23, 42, 721, 722, HS. Errors are not random dust: the freeze is repeatedly wrong in these sectors relative to BEA’s later after tables.

## 6. Caveat

**What this section is doing.** States the limit of what “match published after” can prove.

**Why we include it.** Without this, section 4 reads like a ground-truth exam. It is not. BEA’s published summary after tables are annual estimates, not an independent lab measurement of the “true” redefinition operator for year `t` (same spirit as `frozen_mix_diagnostic.py`).

**Inputs / outputs.** Narrative only — no extra numbers.

Published summary after-redefinitions tables are BEA’s annual estimate, not independent ground truth. Using the matched 1997–2024 after files for both learning and scoring removes release-revision noise from the frozen-ratio question; it does **not** make those after tables the true redefinitions for year `t`.

### Conclusion

**Span results measure agreement with BEA’s later after tables — not absolute truth of redefinitions.** Weigh sections 3–5 with the in-sample 2017 success in [`ratio-reconstruction-report.md`](ratio-reconstruction-report.md). Do **not** read a span “miss” as proof the 2017 operator in [`ratio-plan.md`](ratio-plan.md) is wrong for 2017, or a span “pass” as proof the method is BEA’s production rule for year `t`.
