"""Wholesale v0→v0.3 USEEIO diagnostics — three stacked group endpoints + FINAL.

Each group endpoint is a dispatched diagnostics Sheet. Combine net-diffs chain
G1 → G2 → G3 (marginal strips); FINAL is the shipped release (verification
column, not a fourth deck marginal).

Group definitions:
  G1 — Cornerstone 2023 GHG + 2026 schema (Phoebe restore schema+GHG)
  G2 — Methods: Phoebe B/A/IoT bridge + inflation + CEDA A/price (A_ceda endpoint)
  G3 — Data: MECS + UMD + 2024 IO/GHG (2024_io_ghg endpoint)
  FINAL — ``2025_usa_cornerstone_v0_3``
"""

from __future__ import annotations

from bedrock.utils.validation.analysis.release_v0_3_progression import (
    PINNED_USEEIO_BASELINE,
    ProgressionSheet,
    sheets_in_order,
)

G1_SCHEMA_GHG = ProgressionSheet(
    step_label="G1: Cornerstone 2023 GHG + schema",
    sheet_id="1l6Yzys1yNNvmZX2Fno30ftWjuMBCtRPFTESTbinYKz0",
    config_name="useeio_phoebe_23_restore_schema_and_ghg",
    sheet_title="[05-06-2026] USEEIO - restore GHG model and schema",
)

G2_METHODS = ProgressionSheet(
    step_label="G2: Methods (A/B, IoT, inflation, CEDA A/price)",
    sheet_id="1QZBKKHqaZm84-Kn1Fz0gpEdRTNCqnw0YJ8TQAsDn87o",
    config_name="2025_usa_cornerstone_full_model_v0_3_A_ceda_fallback_approach",
    sheet_title=(
        "[2026-06-18, bedrock repo, 2023, USEEIO based, "
        "v0.3 / CEDA A approach w/ price adj] EFs diagnostics"
    ),
)

G3_DATA = ProgressionSheet(
    step_label="G3: Data update (MECS, UMD, 2024 IO/GHG)",
    sheet_id="1lqwoSVuDcMkSp56p1U8uQBkNuIVgDYJiy8Pog8IU-Zg",
    config_name="2025_usa_cornerstone_full_model_v0_3_2024_io_ghg",
    sheet_title=(
        "[2026-06-24, bedrock repo, 2024, USEEIO based, "
        "v0.3 / 2024 US IO+GHG data] EFs diagnostics"
    ),
)

FINAL_V03_USEEIO = ProgressionSheet(
    step_label="FINAL v0.3 (shipped)",
    sheet_id="1FJYZHmQVN9D0JcUq7_hkqrxBU_h_K_xUPFQGDvf7gII",
    config_name="2025_usa_cornerstone_v0_3",
    sheet_title=(
        "[2026-06-24, bedrock repo, 2024, USEEIO based, "
        "v0.3 / FINAL v0.3] EFs diagnostics"
    ),
)

# IO@2023 reference for 2023→2024 inflation on the G3 marginal (G3 vs G2).
REF_2023_FOR_INFLATION = "1rzJEZuDe-GK_C5juKtIlrkYyQpnU0S9KUCBzMl9FT4M"

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
    "REF_2023_FOR_INFLATION",
    "V0_TO_V03_USEEIO_GROUPS_MERGED_XLSX",
    "V0_V03_USEEIO_GROUP_SHEETS",
    "V0_V03_USEEIO_STACK_SHEETS",
    "sheets_in_order",
    "useeio_group_stack_target_mapping",
]
