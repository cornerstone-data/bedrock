# Phase 3 — Production inclusion gate (historical Phase 3.4 label)

**Status:** **FLIP** (2026-09-29) — Phase 3.2 evidence package accepted; canonical nowcast YAMLs use `waste_weights_year: match_io`. Release note: [`phase32_flip_release_note.md`](phase32_flip_release_note.md). Snapshot / waterfall refresh = remaining Phase 3.2.D ops after merge (see release note).

Flip is **not** blocked by BR bypass, RCRA ≥2017, §11 hardening, or who-buys SAS-scale.

## Locked context

| Item | Value |
|------|--------|
| Production nowcast weights | **`waste_weights_year: match_io`** on canonical v0.4 / v0.5 nowcast YAMLs |
| Analysis control (A/B) | `waste_weights_year: 2017` on Phase 3 control YAMLs under `configs/` |
| Evidence scope | Year-aligned weights vs 2017 production on **GCS** MUT only (Track C / local MUT retired from evidence) |
| MUT pin (GCS) | **`v0.3.0_92b7a8a`** (Phase 3.1; impact caches regenerated after SAS Table 2/3 suppression recovery) |
| RCRA Use intersection | **`rcra_path=br_bypass`** until Phase 4 — **settled / OK for Flip** (Phase 4 is post-v0.5 separate PR) |
| Who-buys 2018–2021 | **`freeze_confirmed`** (Phase 3.2.A) — bare EC 2017; see [`phase32_who_buys_search.md`](phase32_who_buys_search.md) |
| Electricity | Off for all weight A/B evidence runs |

## Settled (no longer Flip blockers) — Phase 3.2

1. **`rcra_path=br_bypass`** instead of full stewiFBS modification — OK for production Flip. Phase 4 retires bypass separately.
2. **RCRA ≥2017 (BR)** on treatment vs workbook 2012 RCRA in production 2017 CSVs — OK.
3. **Who-buys path for 2018–2021** — bare EC 2017 freeze (`outcome=freeze_confirmed`). SAS-scale of 2017 EC and §11 hardening are **optional hardening only** and are **never** Flip prerequisites.

## Decision (recorded)

1. ~~Hold~~ — superseded.
2. **Flip (accepted):** canonical nowcast YAMLs set to `match_io`; national EFs may shift from waste weights alone; Use intersection still uses BR bypass until Phase 4; Flip evidence MUT = `v0.3.0_92b7a8a`; EC path for 2018–2021 = bare 2017 freeze (Phase 3.2.A).

**Production flip does not require Phase 4, §11 hardening, or who-buys SAS-scale.**

## Do not flip on Sep-22 pilot alone

The original local paired run used an **unpinned** control MUT. The evidence base
is both arms pinned on a verified **GCS** MUT, year-aligned weights vs 2017.

## Checklist (Phase 3.2)

- [x] Phase 3.2.A who-buys result recorded (`freeze_confirmed`) — [`phase32_who_buys_search.md`](phase32_who_buys_search.md)
- [x] Phase 3.2.B waste-N variance note — [`phase32_waste_n_variance.md`](phase32_waste_n_variance.md)
- [x] Phase 3.2.C control vs treatment Y2Y — [`phase32_y2y_comparison.md`](phase32_y2y_comparison.md)
- [x] Stakeholder accept “explained” evidence package — **FLIP**
- [x] GCS by-year report on target MUT pin — [`impact_nowcast_updated_weights_by_year.md`](impact_nowcast_updated_weights_by_year.md)
- [x] Release note drafted — [`phase32_flip_release_note.md`](phase32_flip_release_note.md)
- [x] Canonical YAMLs set to `waste_weights_year: match_io`
- [x] Snapshot regenerate on Flip branch (`generate_snapshots` run [36665300634](https://github.com/cornerstone-data/bedrock/actions/runs/36665300634); GCS `gs://cornerstone-default/snapshots/60c8a6b8568b3002a73cdf8569114b2250571e94/`)
- [x] `.SNAPSHOT_KEY` / `releases.py` / Literal bumped to that SHA on this branch
- [x] Waterfall / Flip diagnostics dispatched (see [`phase32_flip_release_note.md`](phase32_flip_release_note.md) run index)
- [ ] ~~Prefer §11 hardening + optional who-buys SAS-scale before/with flip~~ → **optional only; not a gate**
