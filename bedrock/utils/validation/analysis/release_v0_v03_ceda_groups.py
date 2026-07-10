"""Wholesale v0→v0.3 CEDA diagnostics — v03_waterfall cumulative group endpoints.

Five ``v03_waterfall_*`` configs at IO@2024 producer footing (CEDA-specific
G1a/G1b split plus shared G2/G3/FINAL). Combine net-diffs chain G1a → G1b →
G2 → G3 (marginal strips); FINAL verifies the shipped feature mix.

Group definitions (cumulative):
  G1a — ``v03_waterfall_ceda_g1a_schema_ghg`` — Cornerstone schema/GHG (no waste)
  G1b — ``v03_waterfall_ceda_g1b_waste_disagg`` — G1a + waste disaggregation
  G2 — ``v03_waterfall_g2_methods`` — G1b + CEDA A/price, margins, inflation
  G3 — ``v03_waterfall_g3_data`` — G2 + 2024 UMD GHG / IO data
  FINAL — ``v03_waterfall_final`` — full v0.3 methodology (verification column)

Dispatch: ``bedrock.analysis.v0_3.dispatch_ef_v03_waterfall --baseline ceda``.
After dispatch, paste sheet IDs from
``output/release_v0_v03_groups/ef_run_index_v03_waterfall_ceda.csv`` into the
``ProgressionSheet`` entries below.
"""

from __future__ import annotations

from bedrock.utils.validation.analysis.release_v0_3_progression import (
    CEDA_V0_BASELINE,
    ProgressionSheet,
    sheets_in_order,
)

G1A_SCHEMA_GHG = ProgressionSheet(
    step_label="G1a: Cornerstone schema/GHG (no waste)",
    sheet_id="1rFe4PmoEqDZ2NNUf5gx9jyvy3IQJEE7QGxXkygvVjhU",
    config_name="v03_waterfall_ceda_g1a_schema_ghg",
    sheet_title=(
        "[2026-07-10, bedrock repo, 2024, CEDA based, "
        "v0.3 / waterfall CEDA G1a schema/GHG] EFs diagnostics"
    ),
)

G1B_WASTE_DISAGG = ProgressionSheet(
    step_label="G1b: Waste disaggregation",
    sheet_id="1cTWFxTt2ux51Lk6x6pesJiOMKlNMgZABquV3GCJhJXE",
    config_name="v03_waterfall_ceda_g1b_waste_disagg",
    sheet_title=(
        "[2026-07-10, bedrock repo, 2024, CEDA based, "
        "v0.3 / waterfall CEDA G1b waste disagg] EFs diagnostics"
    ),
)

G2_METHODS = ProgressionSheet(
    step_label="G2: Bedrock methods (CEDA A/price, margins, inflation)",
    sheet_id="1SrBs3qrWdPGExxCCMoblDslmzGxps4BUq4EHMGSiNeA",
    config_name="v03_waterfall_g2_methods",
    sheet_title=(
        "[2026-07-10, bedrock repo, 2024, CEDA based, "
        "v0.3 / waterfall G2 methods] EFs diagnostics"
    ),
)

G3_DATA = ProgressionSheet(
    step_label="G3: Data update (MECS, UMD, 2024 IO/GHG)",
    sheet_id="1TgyffESrSghyfwEez_nE3vPpuLusus4uoa10bSSO1BA",
    config_name="v03_waterfall_g3_data",
    sheet_title=(
        "[2026-07-10, bedrock repo, 2024, CEDA based, "
        "v0.3 / waterfall G3 data] EFs diagnostics"
    ),
)

FINAL_V03_CEDA = ProgressionSheet(
    step_label="FINAL v0.3 (waterfall)",
    sheet_id="19b0Nmuz4Ymj-jKlCrGWVymgpcBereD738DNcTN3kZgA",
    config_name="v03_waterfall_final",
    sheet_title=(
        "[2026-07-10, bedrock repo, 2024, CEDA based, "
        "v0.3 / waterfall FINAL v0.3] EFs diagnostics"
    ),
)

V0_V03_CEDA_GROUP_SHEETS: tuple[ProgressionSheet, ...] = (
    G1A_SCHEMA_GHG,
    G1B_WASTE_DISAGG,
    G2_METHODS,
    G3_DATA,
    FINAL_V03_CEDA,
)

V0_V03_CEDA_STACK_SHEETS: tuple[ProgressionSheet, ...] = (
    G1A_SCHEMA_GHG,
    G1B_WASTE_DISAGG,
    G2_METHODS,
    G3_DATA,
)

V03_WATERFALL_CEDA_CONFIGS: tuple[str, ...] = tuple(
    s.config_name for s in V0_V03_CEDA_GROUP_SHEETS
)

V0_TO_V03_CEDA_GROUPS_MERGED_XLSX = (
    "ef_diagnostics_merged_v0_to_v03_ceda_groups_ceda.xlsx"
)


def ceda_group_stack_target_mapping(
    sheets: tuple[ProgressionSheet, ...],
) -> dict[str, str]:
    """Stacked net-diff targets: G1a vs CEDA v0, then each step vs prior."""
    config_names = [s.config_name for s in sheets]
    if not config_names:
        return {}
    mapping: dict[str, str] = {config_names[0]: CEDA_V0_BASELINE}
    for idx in range(1, len(config_names)):
        mapping[config_names[idx]] = config_names[idx - 1]
    return mapping


__all__ = [
    "CEDA_V0_BASELINE",
    "FINAL_V03_CEDA",
    "G1A_SCHEMA_GHG",
    "G1B_WASTE_DISAGG",
    "G2_METHODS",
    "G3_DATA",
    "V03_WATERFALL_CEDA_CONFIGS",
    "V0_TO_V03_CEDA_GROUPS_MERGED_XLSX",
    "V0_V03_CEDA_GROUP_SHEETS",
    "V0_V03_CEDA_STACK_SHEETS",
    "ceda_group_stack_target_mapping",
    "sheets_in_order",
]
