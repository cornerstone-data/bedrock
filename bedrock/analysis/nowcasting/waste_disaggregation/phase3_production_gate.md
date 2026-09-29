# Phase 3.4 — Production inclusion gate

**Status:** **HOLD locked for v0.5** — multi-year evidence is analysis-only;
canonical nowcast stays on **2017** weights. Flip is **deferred** past v0.5
(not an equal open option for this release).

## Locked context

| Item | Value |
|------|--------|
| Production nowcast weights | Bundled **2017** CSVs (unchanged) |
| Analysis treatment | `waste_weights_year: match_io` on Phase 3 YAMLs only |
| Evidence scope | Year-aligned weights vs 2017 production on **GCS** MUT only (Track C / local MUT retired from evidence) |
| MUT pin (GCS) | **`v0.3.0_92b7a8a`** (Phase 3.1) |
| RCRA Use intersection | **`rcra_path=br_bypass`** until Phase 4 |
| Electricity | Off for all weight A/B runs |

## Decision options

1. **Hold (locked for v0.5):** keep canonical `v0_4_nowcast_*` / `v0_5` on
   `waste_weights_year: 2017`; leave `match_io` analysis-only. **Do not** set
   `match_io` on `2025_usa_cornerstone_v0_5.yaml`.
2. **Flip (deferred):** set canonical nowcast YAMLs to `match_io` (or explicit years);
   refresh EF snapshot / waterfall; release note must state:
   - national EFs may shift from waste weights alone
   - Use intersection still uses BR bypass until Phase 4
   - which **GCS** MUT vintage underpinned the flip evidence

**Production flip does not require Phase 4.** Flip is not pending for #1029 / #1028.

## Do not flip on Sep-22 pilot alone

The original local paired run used an **unpinned** control MUT. The evidence base
is both arms pinned on a verified **GCS** MUT, year-aligned weights vs 2017.

## Checklist before Flip (future gate)

- [ ] GCS by-year report reviewed on the target MUT pin (`impact_nowcast_updated_weights_by_year.md`)
- [ ] Release note drafted (BR bypass + GCS MUT vintage)
- [ ] Snapshot / waterfall refresh planned
- [ ] Prefer §11 hardening + optional who-buys SAS-scale before/with flip
