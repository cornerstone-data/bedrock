# Production inclusion gate

**Status:** **FLIP** (2026-09-29) — evidence package accepted; canonical nowcast YAMLs use `waste_weights_year: match_io`. Release note: [`flip_release_note.md`](flip_release_note.md). Branch snapshot / waterfall done; **re-snapshot after prior-weighted fill + industry-mix chain** (provisional `v0_4_1` = `60c8a6b…`; shipped `v0_4_0` = `2fcbd68…`).

Flip is **not** blocked by BR bypass, RCRA ≥2017, §11 hardening, or who-buys SAS-scale.

## Locked context

| Item | Value |
|------|--------|
| Production nowcast weights | **`waste_weights_year: match_io`** on canonical v0.4 / v0.5 nowcast YAMLs (**v0.4.1** story) |
| Published v0.4.0 baseline | **`releases.v0_4_0` = `2fcbd68…`** (not Flip) |
| Analysis control (A/B) | `waste_weights_year: 2017` on control YAMLs under `configs/` |
| Evidence scope | Year-aligned weights vs 2017 production on **GCS** MUT only (local MUT retired from evidence) |
| MUT pin (GCS) | **`v0.3.0_92b7a8a`** (impact caches regenerated 2026-09-30 under prior-weighted fill + chain) |
| SAS→AIES share-seam grade | **max \|Δ\| = 4.46 pp → chain on** (`chain_waste_industry_mix_across_aies`) |
| RCRA Use intersection | **`rcra_path=br_bypass`** until follow-on BR→FBS work — **settled / OK for Flip** (follow-on BR→FBS is post-v0.5 separate PR) |
| Who-buys 2018–2021 | **`freeze_confirmed`** — bare EC 2017; see [`who_buys_search.md`](who_buys_search.md) |
| Electricity | Off for all weight A/B evidence runs |

## Settled (no longer Flip blockers)

1. **`rcra_path=br_bypass`** instead of full stewiFBS modification — OK for production Flip. Follow-on BR→FBS retires bypass separately.
2. **RCRA ≥2017 (BR)** on treatment vs workbook 2012 RCRA in production 2017 CSVs — OK.
3. **Who-buys path for 2018–2021** — bare EC 2017 freeze (`outcome=freeze_confirmed`). SAS-scale of 2017 EC and §11 hardening are **optional hardening only** and are **never** Flip prerequisites.

## Decision (recorded)

1. ~~Hold~~ — superseded.
2. **Flip (accepted):** canonical nowcast YAMLs set to `match_io`; national EFs may shift from waste weights alone; Use intersection still uses BR bypass until follow-on BR→FBS work; Flip evidence MUT = `v0.3.0_92b7a8a`; EC path for 2018–2021 = bare 2017 freeze.

**Production flip does not require follow-on BR→FBS work, §11 hardening, or who-buys SAS-scale.**

## Do not flip on Sep-22 pilot alone

The original local paired run used an **unpinned** control MUT. The evidence base
is both arms pinned on a verified **GCS** MUT, year-aligned weights vs 2017.

## Checklist

- [x] Who-buys result recorded (`freeze_confirmed`) — [`who_buys_search.md`](who_buys_search.md)
- [x] Waste-N variance note — [`waste_n_variance.md`](waste_n_variance.md)
- [x] Control vs treatment Y2Y — [`waste_y2y_comparison.md`](waste_y2y_comparison.md)
- [x] Stakeholder accept “explained” evidence package — **FLIP**
- [x] GCS by-year report on target MUT pin — [`impact_nowcast_updated_weights_by_year.md`](impact_nowcast_updated_weights_by_year.md)
- [x] Release note drafted — [`flip_release_note.md`](flip_release_note.md)
- [x] Canonical YAMLs set to `waste_weights_year: match_io`
- [x] Snapshot regenerate on Flip branch (`generate_snapshots` run [36665300634](https://github.com/cornerstone-data/bedrock/actions/runs/36665300634); GCS `gs://cornerstone-default/snapshots/60c8a6b8568b3002a73cdf8569114b2250571e94/`)
- [x] `.SNAPSHOT_KEY` / `releases.py` / Literal bumped to that SHA on this branch
- [x] Waterfall / Flip diagnostics dispatched (see [`flip_release_note.md`](flip_release_note.md) run index)
- [ ] ~~Prefer §11 hardening + optional who-buys SAS-scale before/with flip~~ → **optional only; not a gate**
