# Redefinition-ratio reconstruction report

This report records how the production Step 7 path — per-cell 2017 GO-ratio carry — reconstructs the published 2017 after-redefinitions Make / Use / value-added / import / margins tables. Motivation and method are pinned in [`ratio-plan.md`](ratio-plan.md). The earlier BEA-style attempt and why production left it are in [`../BEA_style_redefinitions_implementation/BEA-style_redefinitions_reconstruction_report.md`](../BEA_style_redefinitions_implementation/BEA-style_redefinitions_reconstruction_report.md).

---

## Methods summary

BEA publishes detailed Make, Use, import, and margins tables for 2017 both **before** and **after** redefinitions. Issue #572 / `plan.md` Step 7 ask for a transform that learns movement from that 2017 pair and reapplies it so the published after tables match cell by cell.

The production operator does one thing everywhere (including Make):

1. **Industry control** — industry gross output `x = V.sum(axis=1)` from the before Make (USD).
2. **Learn sparse ratios** — for every cell with `|before − after| > ATOL` ($0.5M), store `ratio = delta / x[control]` (or `0` if `|x| ≤ ATOL`). Make control is the **row** industry; Use / VA / Import / industry-buyer margins use the **column** (buyer) industry.
3. **Apply cellwise** — `cell_after = cell_before − ratio * x_t`. No Make destination credit. Non-industry margin buyers (e.g. final demand) store and replay an **absolute** USD residual instead of a GO ratio.
4. **Artifacts** — five tracked sparse parquets under `bedrock/analysis/nowcasting/redefinition_ratios_2017_*.parquet`, written by `python -m bedrock.analysis.nowcasting.redefinition_ratios_2017`.

On 2017, with `x_t = x_2017`, every stored cell round-trips by construction (up to float dust scrubbed below $0.001). Year-`t` apply uses that year’s Make-derived `x_t` for all `go_ratio` rows; absolute margin residuals do not scale.

| Piece | Location |
| --- | --- |
| Transform | `bedrock/transform/iot/nowcast_redefinition_ratios.py` |
| CLI | `bedrock/analysis/nowcasting/redefinition_ratios_2017.py` |
| Step 7 sections | `bedrock/analysis/nowcasting/sections.py` (`_mut_after_redef_2017` → `apply_redefinition_ratios`) |
| Archived BEA-style stack | [`../BEA_style_redefinitions_implementation/code/`](../BEA_style_redefinitions_implementation/code/) (not imported by production) |

---

## Redefinition-ratio reconstruction findings

Numbers below are from applying disk-loaded 2017 ratios to the published before-redef tables and scoring against published after (half-million USD bar; `assert_ok(max_partial=0, max_miss=0, max_extra=0, max_margin_partial=0)`).

### 1. Did we recreate the 2017 after-redefinitions tables?

**Yes — all five frames, including Make, at the full-grid gate.**

| Table | Populated cells matched | Accuracy |
| --- | ---: | ---: |
| Make | 3,233 | 100% |
| Use intermediate | 48,754 | 100% |
| Value added | 1,191 | 100% |
| Import | 18,505 | 100% |
| Margins (five columns together) | 128,911 | 100% |

That is the acceptance bar issue #572 asks for. Make no longer uses the softer classified-source / $11M column-leftover gate from the BEA-style plan; cellwise ratios learn published diagonal leftovers as well as off-diagonals, so full-grid equality holds.

Stored ratio rows (above `ATOL`):

| Artifact | Rows |
| --- | ---: |
| Make (`V`) | 2,024 |
| Use (`U`) | 5,740 |
| Value added (`VA`) | 1,101 |
| Import (`Uimp`) | 1,290 |
| Margins | 14,819 (14,794 `go_ratio`, 25 `absolute`) |

Published before→after Use movement still matches the About-doc figures exactly: **5,740** cells change, **553,635** million dollars of gross movement, largest single cell **42,893**, net **−7**.

### 2. How does this compare to the BEA-style stack?

| Question | BEA-style (archived) | Ratio carry (production) |
| --- | --- | --- |
| Use / VA / Import / Margins 2017 match? | Yes, **only** with residual layer C6 (~696B Use residual gross alone) | Yes, by construction from the same published deltas |
| Make full-grid match? | No — classified-source gate; ~$10M column leftover | Yes — cellwise includes diagonal leftovers |
| Operator story | Census → shares → dest mix → specials → residual | One GO-ratio idiom (+ absolute FD margins) |
| Production imports | Removed from `bedrock.transform` / sections | `nowcast_redefinition_ratios` only |

The BEA-style reconstruction already showed that Chapter 9 default mix plus named specials do not describe published Use-side after tables without a learned residual. The ratio path makes that residual the explicit operator and drops the pretence that destination-industry recipes carry the match.

### Bottom line

| Question | Answer |
| --- | --- |
| **Recreate 2017 after tables?** | **Yes** — Make, Use, value added, imports, and margins to half a million with full-grid `assert_ok`. |
| **Use magnitude sanity?** | **Yes** — 5,740 / 553,635 / 42,893 / −7 unchanged. |
| **Honest method?** | **Yes** — production is “learned 2017 cell movement,” not Chapter 9 plus a silent residual. |

### 3. What remains open for later years?

A 2017 ratio carry **describes** 2017 and freezes that year’s movement structure. It is not BEA’s production-function rule for year `t`.

**Summary-level out-of-sample gate (first):** published before-redef summary MUT + matched 2024-vintage after for 2018–2024 — see [`summary-span-test-report.md`](summary-span-test-report.md) (plan: [`summary-span-test-plan.md`](summary-span-test-plan.md)). Headline: 2017 summary round-trip **PASS**; Use L1 relative error about **0.3%–1.2%** across 2018–2024 with many industries still >1% off on max cell relative error (review judgment for #775). Detail year-`t` evaluation still needs Step 6 before-redef MUTs; margins remain untested on the summary span (no annual summary margins series).

Practical summary:

| Piece | Status |
| --- | --- |
| **2017 acceptance** | Passed on all five frames |
| **Production path** | Ratio module + sections rewired; BEA stack archived under `after_redef_MUTs/.../code/` |
| **Summary span 2018–2024** | Report artifact posted; Use L1 ~0.3%–1.2%; see linked report |
| **Detail year `t`** | Apply API ready; needs Step 6 before-redef inputs |

So: **yes**, the ratio implementation achieves the after-redefinitions tables at the #572 cell-by-cell bar, including Make, with a single documented operator.
