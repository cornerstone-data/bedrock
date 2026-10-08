# Nowcasting redefinitions — review notes

Review date: 2026-08-29. Planning docs for issue #572 (Step 7). Implementation lives on `jv_nowcast_after_redefinitions_MUTs`.

This folder is the write-up of a review of (1) the bedrock repo and nowcast plan, (2) BEA's *Concepts and Methods of the U.S. Input-Output Accounts* (September 2006), especially Chapters 4 and 9, (3) [GitHub project 26](https://github.com/orgs/cornerstone-data/projects/26/views/1) and [issue #572](https://github.com/cornerstone-data/bedrock/issues/572).


| File                                                   | Contents                                                                                                           |
| ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------ |
| [summary.md](summary.md)                               | The three requested sections: nowcast status, BEA redefinitions method, next steps for #572                        |
| [bea-style-plan.md](bea-style-plan.md)       | Phased implementation plan for #572, including how the BEA algorithm will fail on 2017 and where residual rules go |
| [BEA-style_redefinitions_reconstruction_report.md](BEA-style_redefinitions_reconstruction_report.md) | 2017 BEA-style reconstruction results, residual by layer, and whether to realign with #572 / `plan.md` |


**Sources**

- Plan: `bedrock/analysis/nowcasting/plan.md` (on the `nowcast` branch; not on `main`)
- Progress: `bedrock/analysis/nowcasting/progress_report.md` (snapshot 2026-08-28)
- BEA IO manual: [https://www.bea.gov/sites/default/files/methodologies/IOmanual_092906.pdf](https://www.bea.gov/sites/default/files/methodologies/IOmanual_092906.pdf)
- Issue: [https://github.com/cornerstone-data/bedrock/issues/572](https://github.com/cornerstone-data/bedrock/issues/572)
- Project: [https://github.com/orgs/cornerstone-data/projects/26/views/1](https://github.com/orgs/cornerstone-data/projects/26/views/1)

**Scope note.** Issue #572 as filed and the work in these docs share the same 2017 **test** (the published after-redefinitions detail tables must match cell by cell) but they are different **operators**.

- **#572 as filed** learns a number per cell from the 2017 before/after pair (the `compute_coproduction_ratios` template: movement as a share of source-industry GO) and reapplies it. On 2017 that round-trips by construction. On later years it moves “whatever fraction of industry output moved in 2017.” It does not implement BEA’s production-function rule, and a per-cell `after/before` multiplier cannot create Use cells that were zero before and nonzero after.
- **These docs** treat 2017 as a classification and calibration year. Learn *which* `(source, commodity)` pairs BEA redefines; on year `t` move that year’s actual secondary output of those pairs; reallocate Use/VA with Chapter 9 (destination-industry recipe × amount moved) plus the named special-case rules. Whatever still misses the published 2017 after tables is a residual overlay — required, because exact 2017 reconstruction is a hard #572 gate — stored as its own artifact, not as the whole method.

The summary treats that second operator as the implementation of #572. Longer contrast: `summary.md` §4.2.
