# RAS improvements (#839 hygiene)

Analysis and documentation for [issue #839](https://github.com/cornerstone-data/bedrock/issues/839)
hygiene items (residue sweep + standing zero-pattern / sign audits).

**Production code** (called on every balance) stays in
`bedrock/transform/iot/nowcast_sut_assembly.py`:
`sweep_offset_residue`, `assert_post_balance_hygiene`, save-sidecar fields.
Unit tests: `bedrock/transform/iot/__tests__/test_nowcast_sut_assembly_hygiene.py`
and the save sweep cases in `test_nowcast_sut_assembly_save.py`.

## Reproduce

Fresh balance census (slow, ~15–17 min/year)::

    uv run python -m bedrock.analysis.nowcasting.ras_improvements.hygiene_census \
        --years 2018,2021,2023

GCS BalancedSUT census (no GRAS)::

    uv run python -m bedrock.analysis.nowcasting.ras_improvements.hygiene_census_gcs \
        --years 2018,2021,2023

Fresh vs older-vintage summary::

    uv run python -m bedrock.analysis.nowcasting.ras_improvements.hygiene_summary

Root-cause vintage diff (`163db0e` → `d2e2112`)::

    uv run python -m bedrock.analysis.nowcasting.ras_improvements.illicit_vintage_diff

Artifacts write under `bedrock/analysis/nowcasting/output/ras_improvements/`
(gitignored). Prior local runs under `output/ras_hygiene_839/` are still found
as a fallback.

## Report

See [`report_839_hygiene.md`](report_839_hygiene.md).
