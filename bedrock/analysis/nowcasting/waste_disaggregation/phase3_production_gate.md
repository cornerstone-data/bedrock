# Phase 3.4 — Production inclusion gate

**Status:** **HOLD** pending Track A multi-year results + Track C local-MUT
comparison (and stakeholder review).

## Locked context

| Item | Value |
|------|--------|
| Production nowcast weights | Bundled **2017** CSVs (unchanged) |
| Analysis treatment | `waste_weights_year: match_io` on Phase 3 YAMLs only |
| MUT pin (Track A) | Verified GCS `v0.3.0_f709829` (see `cache/phase3_gcs_mut_vintage.json`) |
| RCRA Use intersection | **`rcra_path=br_bypass`** until Phase 4 |
| Electricity | Off for all weight A/B runs |

## Decision options

1. **Hold (default until review):** keep canonical `v0_4_nowcast_*` on
   `waste_weights_year: 2017`; leave `match_io` analysis-only.
2. **Flip:** set canonical nowcast YAMLs to `match_io` (or explicit years);
   refresh EF snapshot / waterfall; release note must state:
   - national EFs may shift from waste weights alone
   - Use intersection still uses BR bypass until Phase 4
   - which MUT vintage underpinned the flip evidence (**prefer Track C** if
     GCS vs local deltas differ materially)

**Production flip does not require Phase 4.**

## Do not flip on Sep-22 pilot alone

The original local paired run used an **unpinned** control MUT. Phase 3.1
re-baseline (both arms pinned) is the Track A evidence base; Track C is preferred
when available.

## Checklist before Flip

- [ ] Track A by-year report reviewed (`impact_nowcast_updated_weights_by_year.md`)
- [ ] Track C GCS vs local section reviewed (or explicitly waived)
- [ ] Release note drafted (BR bypass + MUT vintage)
- [ ] Snapshot / waterfall refresh planned
- [ ] Prefer §11 hardening + optional who-buys SAS-scale before/with flip
