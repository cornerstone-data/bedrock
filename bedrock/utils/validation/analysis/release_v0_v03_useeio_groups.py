"""Wholesale v0→v0.3 USEEIO diagnostics — v03_waterfall cumulative group endpoints.

Four ``v03_waterfall_*`` configs at IO@2024 producer footing. Combine net-diffs
chain G1 → G2 → G3 (marginal strips); FINAL verifies the shipped feature mix.

Group definitions (cumulative):
  G1 — ``v03_waterfall_useeio_g1_schema_ghg`` — USEEIO-like A/margins + Cornerstone schema/GHG
  G2 — ``v03_waterfall_g2_methods`` — G1 + CEDA A/price, cornerstone margins, inflation
  G3 — ``v03_waterfall_g3_data`` — G2 + 2024 UMD GHG / IO data
  FINAL — ``v03_waterfall_final`` — full v0.3 methodology (verification column)

Dispatch: ``bedrock.analysis.v0_3.dispatch_ef_v03_waterfall``. After dispatch,
paste sheet IDs from ``output/release_v0_v03_groups/ef_run_index_v03_waterfall.csv``
into the ``ProgressionSheet`` entries below.
"""

from __future__ import annotations

from bedrock.utils.validation.analysis.release_v0_3_progression import (
    PINNED_USEEIO_BASELINE,
    ProgressionSheet,
    sheets_in_order,
)

G1_SCHEMA_GHG = ProgressionSheet(
    step_label="G1: USEEIO-like A/margins + Cornerstone schema/GHG",
    sheet_id="18Nz4dfrv0kvSFo3G1dGrOgM_1HLC9843o4-yrO6m-uM",
    config_name="v03_waterfall_useeio_g1_schema_ghg",
    sheet_title=(
        "[2026-07-09, bedrock repo, 2024, USEEIO based, "
        "v0.3 / waterfall USEEIO G1 schema/GHG] EFs diagnostics"
    ),
)

G2_METHODS = ProgressionSheet(
    step_label="G2: Bedrock methods (CEDA A/price, margins, inflation)",
    sheet_id="1RPd44RBxUTtThz33EGH2ILt8o0WGWsLtJU4763run8U",
    config_name="v03_waterfall_g2_methods",
    sheet_title=(
        "[2026-07-09, bedrock repo, 2024, USEEIO based, "
        "v0.3 / waterfall G2 methods] EFs diagnostics"
    ),
)

G3_DATA = ProgressionSheet(
    step_label="G3: Data update (MECS, UMD, 2024 IO/GHG)",
    sheet_id="1uFL4hfOofBUeV05fQOFIophabRFrVizo_N5wSqd_2wA",
    config_name="v03_waterfall_g3_data",
    sheet_title=(
        "[2026-07-09, bedrock repo, 2024, USEEIO based, "
        "v0.3 / waterfall G3 data] EFs diagnostics"
    ),
)

FINAL_V03_USEEIO = ProgressionSheet(
    step_label="FINAL v0.3 (waterfall)",
    sheet_id="1SHSf6IINj6a79qjmhwJGHZR0Oo8XOaxCpv0dggsfkSU",
    config_name="v03_waterfall_final",
    sheet_title=(
        "[2026-07-09, bedrock repo, 2024, USEEIO based, "
        "v0.3 / waterfall FINAL v0.3] EFs diagnostics"
    ),
)

V0_V03_USEEIO_GROUP_SHEETS: tuple[ProgressionSheet, ...] = (
    G1_SCHEMA_GHG,
    G2_METHODS,
    G3_DATA,
    FINAL_V03_USEEIO,
)

V0_V03_USEEIO_STACK_SHEETS: tuple[ProgressionSheet, ...] = (
    G1_SCHEMA_GHG,
    G2_METHODS,
    G3_DATA,
)

V03_WATERFALL_CONFIGS: tuple[str, ...] = tuple(
    s.config_name for s in V0_V03_USEEIO_GROUP_SHEETS
)

# Default local merge workbook for combo ``v0_to_v03_useeio_groups`` (USEEIO baseline).
V0_TO_V03_USEEIO_GROUPS_MERGED_XLSX = (
    "ef_diagnostics_merged_v0_to_v03_useeio_groups_useeio.xlsx"
)


def useeio_group_stack_target_mapping(
    sheets: tuple[ProgressionSheet, ...],
) -> dict[str, str]:
    """Stacked net-diff targets: G1 vs pinned USEEIO, then each step vs prior."""
    config_names = [s.config_name for s in sheets]
    if not config_names:
        return {}
    mapping: dict[str, str] = {config_names[0]: PINNED_USEEIO_BASELINE}
    for idx in range(1, len(config_names)):
        mapping[config_names[idx]] = config_names[idx - 1]
    return mapping


__all__ = [
    "FINAL_V03_USEEIO",
    "G1_SCHEMA_GHG",
    "G2_METHODS",
    "G3_DATA",
    "V03_WATERFALL_CONFIGS",
    "V0_TO_V03_USEEIO_GROUPS_MERGED_XLSX",
    "V0_V03_USEEIO_GROUP_SHEETS",
    "V0_V03_USEEIO_STACK_SHEETS",
    "sheets_in_order",
    "useeio_group_stack_target_mapping",
]
