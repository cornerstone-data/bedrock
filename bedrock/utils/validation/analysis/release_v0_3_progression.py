"""v0.3 release-deck diagnostics progression — shared sheet registry.

Sheet IDs and ``config_name`` values for the v0.3 release-deck progression.
Used by ``bedrock.analysis.v0_3.plot_ef_release_v0_3`` and the
``v0.2_to_v0.3_*`` combine combos in ``combinations.py``.
"""

from __future__ import annotations

from dataclasses import dataclass

PINNED_USEEIO_BASELINE = 'pinned_useeio_baseline'
CEDA_V0_BASELINE = 'v8_ceda_2025_usa'
# ``config_summary`` on the v0.2 FINAL diagnostics sheets records this stem.
# The yaml was renamed to ``2025_usa_cornerstone_v0_2`` after those runs;
# keep this literal for merge keys and net-diff column lookup.
V02_FULL_MODEL = '2025_usa_cornerstone_full_model'

# One-flag sensitivities on the v0.2 full-model footing: net-diff vs v0.2 FINAL,
# not the prior progression step.
ATOMIC_V02_FOOTING_CONFIGS: frozenset[str] = frozenset(
    {
        '2025_usa_cornerstone_full_model_v0_3_update_inflation_factors',
        '2025_usa_cornerstone_full_model_v0_3_A_ceda_fallback_approach',
        '2025_usa_cornerstone_full_model_v0_3_ghgi_mecs',
    }
)


@dataclass(frozen=True)
class ProgressionSheet:
    step_label: str
    sheet_id: str
    config_name: str
    sheet_title: str


V0_BASELINE_CEDA = ProgressionSheet(
    step_label='v0 baseline',
    sheet_id='1lCh_LsNbylyLSfwQMqiSh2_oh5qtihXF800om5T02Lc',
    config_name='v8_ceda_2025_usa',
    sheet_title='[2026-03-26 v0 baseline] EF diagnostics',
)

V02_FINAL_CEDA = ProgressionSheet(
    step_label='FINAL v0.2',
    sheet_id='1h_Ra-jPkcivuqiQahAIex4PoZXTaYijRFKFzpmKFiXU',
    config_name='2025_usa_cornerstone_full_model',
    sheet_title=(
        '[2026-06-18, bedrock repo, 2023, CEDA based, v0.2 / FINAL v0.2] EFs diagnostics'
    ),
)

V02_FINAL_USEEIO = ProgressionSheet(
    step_label='FINAL v0.2',
    sheet_id='153b904T8qq9qBt4qELvDLNE7K6T12kOyXchM9maABmE',
    config_name='2025_usa_cornerstone_full_model',
    sheet_title=(
        '[2026-06-18, bedrock repo, 2023, USEEIO based, v0.2 / FINAL v0.2] EFs diagnostics'
    ),
)

V03_CEDA_STEPS: tuple[ProgressionSheet, ...] = (
    ProgressionSheet(
        step_label='Update inflation factors',
        sheet_id='1Wi6q8dr6u1sP0ZqaeuW1SYgfKgVsOlatJQDFxduQ6HU',
        config_name='2025_usa_cornerstone_full_model_v0_3_update_inflation_factors',
        sheet_title=(
            '[2026-06-18, bedrock repo, 2023, CEDA based, '
            'v0.3 / Update inflation factors] EFs diagnostics'
        ),
    ),
    ProgressionSheet(
        step_label='CEDA A approach w/ price adj',
        sheet_id='1Ywx6eyMnCTvHPG92uCsmH159A6BZA50GYWaZzrJn6f8',
        config_name='2025_usa_cornerstone_full_model_v0_3_A_ceda_fallback_approach',
        sheet_title=(
            '[2026-06-18, bedrock repo, 2023, CEDA based, '
            'v0.3 / CEDA A approach w/ price adj] EFs diagnostics'
        ),
    ),
    ProgressionSheet(
        step_label='MECS adjustment',
        sheet_id='1hbS5sVk3oHTkVDp-z5v2j-pFt81uRKFFzWEKONIBG5E',
        config_name='2025_usa_cornerstone_full_model_v0_3_ghgi_mecs',
        sheet_title=(
            '[2026-06-29, bedrock, v0.3 MECS on v0.2 footing — CEDA based] '
            'EFs diagnostics'
        ),
    ),
    ProgressionSheet(
        step_label='Switch to 2023 UMD data',
        sheet_id='14OxuVk_BXKUesQ6snw_s3oiQANl-uNBS7dFQxzBXrLc',
        config_name='2025_usa_cornerstone_full_model_v0_3_umd_2023_ghgia',
        sheet_title=(
            '[2026-06-23, bedrock repo, 2023, CEDA based, '
            'v0.3 / Switch to 2023 UMD data] EFs diagnostics'
        ),
    ),
    ProgressionSheet(
        step_label='Switch to 2024 UMD data',
        sheet_id='1BjOcH1Oo3oB7hfKL87VqM93_QEKaKSeOEoN0zj0XjXU',
        config_name='2025_usa_cornerstone_full_model_v0_3_umd_2024_ghgia',
        sheet_title=(
            '[2026-06-24, bedrock repo, 2024, CEDA based, '
            'v0.3 / Update to 2024 UMD data] EFs diagnostics'
        ),
    ),
    ProgressionSheet(
        step_label='2024 US IO+GHG data',
        sheet_id='1pfneC_RzW4mmVkGyitVnDnahAV0rQ9mAhhIsBTQb1EE',
        config_name='2025_usa_cornerstone_full_model_v0_3_2024_io_ghg',
        sheet_title=(
            '[2026-06-24, bedrock repo, 2024, CEDA based, '
            'v0.3 / 2024 US IO+GHG data] EFs diagnostics'
        ),
    ),
    ProgressionSheet(
        step_label='FINAL v0.3',
        sheet_id='1mibFzzRyZShMS8MgDqtdmMSqvoos9JT7yWNo-wH6PTo',
        config_name='2025_usa_cornerstone_v0_3',
        sheet_title=(
            '[2026-06-24, bedrock repo, 2024, CEDA based, '
            'v0.3 / FINAL v0.3] EFs diagnostics'
        ),
    ),
)

V03_USEEIO_STEPS: tuple[ProgressionSheet, ...] = (
    ProgressionSheet(
        step_label='Update inflation factors',
        sheet_id='1VG5WsspWLpsbMEvoDY8-7qEi8MYQ-y1Xu7M4x9-abMw',
        config_name='2025_usa_cornerstone_full_model_v0_3_update_inflation_factors',
        sheet_title=(
            '[2026-06-18, bedrock repo, 2023, USEEIO based, '
            'v0.3 / Update inflation factors] EFs diagnostics'
        ),
    ),
    ProgressionSheet(
        step_label='CEDA A approach w/ price adj',
        sheet_id='1QZBKKHqaZm84-Kn1Fz0gpEdRTNCqnw0YJ8TQAsDn87o',
        config_name='2025_usa_cornerstone_full_model_v0_3_A_ceda_fallback_approach',
        sheet_title=(
            '[2026-06-18, bedrock repo, 2023, USEEIO based, '
            'v0.3 / CEDA A approach w/ price adj] EFs diagnostics'
        ),
    ),
    ProgressionSheet(
        step_label='MECS adjustment',
        sheet_id='1bzOlu1hmhPXa5D-uUvBe6fT8ompHIhtC9D-BcKQwmb4',
        config_name='2025_usa_cornerstone_full_model_v0_3_ghgi_mecs',
        sheet_title=(
            '[2026-06-29, bedrock, v0.3 MECS on v0.2 footing — USEEIO based] '
            'EFs diagnostics'
        ),
    ),
    ProgressionSheet(
        step_label='Switch to 2023 UMD data',
        sheet_id='1rzJEZuDe-GK_C5juKtIlrkYyQpnU0S9KUCBzMl9FT4M',
        config_name='2025_usa_cornerstone_full_model_v0_3_umd_2023_ghgia',
        sheet_title=(
            '[2026-06-23, bedrock repo, 2023, USEEIO based, '
            'v0.3 / Switch to 2023 UMD data] EFs diagnostics'
        ),
    ),
    ProgressionSheet(
        step_label='Switch to 2024 UMD data',
        sheet_id='1YcctdXvoDPQLBnYyNZqSdgxqvLL47fv5dDCTjLkE-JY',
        config_name='2025_usa_cornerstone_full_model_v0_3_umd_2024_ghgia',
        sheet_title=(
            '[2026-06-24, bedrock repo, 2024, USEEIO based, '
            'v0.3 / Update to 2024 UMD data] EFs diagnostics'
        ),
    ),
    ProgressionSheet(
        step_label='2024 US IO+GHG data',
        sheet_id='1lqwoSVuDcMkSp56p1U8uQBkNuIVgDYJiy8Pog8IU-Zg',
        config_name='2025_usa_cornerstone_full_model_v0_3_2024_io_ghg',
        sheet_title=(
            '[2026-06-24, bedrock repo, 2024, USEEIO based, '
            'v0.3 / 2024 US IO+GHG data] EFs diagnostics'
        ),
    ),
    ProgressionSheet(
        step_label='FINAL v0.3',
        sheet_id='1FJYZHmQVN9D0JcUq7_hkqrxBU_h_K_xUPFQGDvf7gII',
        config_name='2025_usa_cornerstone_v0_3',
        sheet_title=(
            '[2026-06-24, bedrock repo, 2024, USEEIO based, '
            'v0.3 / FINAL v0.3] EFs diagnostics'
        ),
    ),
)

CEDA_V02_TO_V03_SHEETS: tuple[ProgressionSheet, ...] = (
    V0_BASELINE_CEDA,
    V02_FINAL_CEDA,
) + V03_CEDA_STEPS

USEEIO_V02_TO_V03_SHEETS: tuple[ProgressionSheet, ...] = (
    V02_FINAL_USEEIO,
) + V03_USEEIO_STEPS


def sheets_in_order(
    sheets: tuple[ProgressionSheet, ...],
) -> tuple[tuple[str, str], ...]:
    """``(sheet_id, sheet_title)`` pairs for ``ComboSpec.sheets_in_order``."""
    return tuple((s.sheet_id, s.sheet_title) for s in sheets)


def _apply_atomic_v02_footing_overrides(mapping: dict[str, str]) -> dict[str, str]:
    """Point atomic v0.2-footing configs at ``V02_FULL_MODEL`` for net-diff."""
    out = dict(mapping)
    for cfg in ATOMIC_V02_FOOTING_CONFIGS:
        if cfg in out:
            out[cfg] = V02_FULL_MODEL
    return out


def ceda_stepwise_target_mapping(
    sheets: tuple[ProgressionSheet, ...],
) -> dict[str, str]:
    """Each step's net-diff subtracts the prior step (first step vs itself).

    Atomic v0.2-footing configs in ``ATOMIC_V02_FOOTING_CONFIGS`` net-diff
    against ``2025_usa_cornerstone_full_model`` instead.
    """
    config_names = [s.config_name for s in sheets]
    if not config_names:
        return {}
    mapping = {config_names[0]: config_names[0]}
    for idx in range(1, len(config_names)):
        mapping[config_names[idx]] = config_names[idx - 1]
    return _apply_atomic_v02_footing_overrides(mapping)


def useeio_stepwise_target_mapping(
    sheets: tuple[ProgressionSheet, ...],
) -> dict[str, str]:
    """Chain net-diffs; anchor v0.2 FINAL vs ``pinned_useeio_baseline``.

    Atomic v0.2-footing configs in ``ATOMIC_V02_FOOTING_CONFIGS`` net-diff
    against ``2025_usa_cornerstone_full_model`` instead of the prior step.
    """
    config_names = [s.config_name for s in sheets]
    if not config_names:
        return {}
    mapping: dict[str, str] = {config_names[0]: PINNED_USEEIO_BASELINE}
    for idx in range(1, len(config_names)):
        mapping[config_names[idx]] = config_names[idx - 1]
    return _apply_atomic_v02_footing_overrides(mapping)
