# RAS improvements

Analysis for Step-5 balance hygiene ([#839](https://github.com/cornerstone-data/bedrock/issues/839))
and seed→balanced RAS movement ([#755](https://github.com/cornerstone-data/bedrock/issues/755)).

**Production hygiene** (every balance) stays in
`bedrock/transform/iot/nowcast_sut_assembly.py`:
`illicit_negative_mask`, `sweep_offset_residue` (illicit below-eps only),
`assert_post_balance_hygiene`, save-sidecar fields
(`residue_sweep=illicit_below_eps`).
Unit tests: `bedrock/transform/iot/__tests__/test_nowcast_sut_assembly_hygiene.py`
and the save sweep cases in `test_nowcast_sut_assembly_save.py`.

## #755 RAS movement

Seed→balanced Use Δ (intermediate vs FD), YoY, soft vs hard. Does **not** call
`balance_year`. Soft+hard over 2017–2023 is ~half a workday; not CI::

    uv run python -m bedrock.analysis.nowcasting.ras_improvements.ras_movement
    uv run python -m bedrock.analysis.nowcasting.ras_improvements.ras_movement \
        --years 2017-2023 --protocols soft,hard

CSVs: `bedrock/analysis/nowcasting/output/ras_movement/` (gitignored).
Report: [`report_755_ras_movement.md`](report_755_ras_movement.md).

## #839 hygiene

Fresh balance census (slow, ~15–17 min/year; ~2 h for 2018–2024). Includes
published-pattern 2(a) and the **seed vs RAS** exemption-fill split (JSON keys
`{use,supply}_seed_fill_*`, `_ras_introduced_*`, …). Use `--force` to drop
stale year rows and rewrite::

    uv run python -m bedrock.analysis.nowcasting.ras_improvements.hygiene_census \
        --years 2018,2019,2020,2021,2022,2023,2024 --force

GCS BalancedSUT census (no GRAS; balanced-only 2(a)/2b — **no** seed-vs-RAS)::

    uv run python -m bedrock.analysis.nowcasting.ras_improvements.hygiene_census_gcs \
        --years 2018,2021,2023

Fresh vs older-vintage summary::

    uv run python -m bedrock.analysis.nowcasting.ras_improvements.hygiene_summary

Root-cause vintage diff (`163db0e` → `d2e2112`)::

    uv run python -m bedrock.analysis.nowcasting.ras_improvements.illicit_vintage_diff

Artifacts write under `bedrock/analysis/nowcasting/output/ras_improvements/`
(gitignored). Prior local runs under `output/ras_hygiene_839/` are still found
as a fallback.

Report: [`report_839_hygiene.md`](report_839_hygiene.md).

## #808 VA support

Soft-only census of named VA-dominated Use columns (T1/T17/T18 residuals +
closer support + `b1_gate` for `814000`). Does not call `balance_year`::

    uv run python -m bedrock.analysis.nowcasting.ras_improvements.va_support_census \
        --years 2018,2021,2022,2023 --force

CSVs: `bedrock/analysis/nowcasting/output/va_support/` (gitignored).
Report: [`report_808_va_support.md`](report_808_va_support.md).
