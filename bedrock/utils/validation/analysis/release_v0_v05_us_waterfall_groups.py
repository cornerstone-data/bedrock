"""v0.4→v0.5 US waterfall diagnostics — USEEIO-track group endpoints.

Four configs at IO@2024 producer footing. Combine net-diffs chain G2 → G3 →
G4 → FINAL (marginal strips on the US release ladder).

Group definitions (cumulative):
  G2 — ``v05_waterfall_g2_methods`` — bedrock methods on published BEA detail,
       2023 UMD GHG
  G3 — ``v05_waterfall_g3_data`` — G2 + 2024 UMD GHG / IO data
  G4 — ``v05_waterfall_g4_nowcast`` — G3 + v0.5 nowcast MUT (facility GHG off)
  FINAL — ``2025_usa_cornerstone_v0_5`` — G4 + facility GHG attribution

Sheet IDs point at diagnostics spreadsheets in the v0.5 Diagnostics Drive
folder (G2–G4) and v0.4 Diagnostics (FINAL release sheet cut with the
``3a1dddc`` facilities FBS). Each ``sheet_title`` records the date and
release the sheet was generated for.

When a snapshot bump moves a config's ``N_new``, mint a fresh sheet and repoint
the ``sheet_id`` here rather than re-running ``generate_diagnostics`` against
the existing ID. That call clears and rewrites every tab in place, which
destroys the prior release's columns and leaves the title describing data the
sheet no longer holds.
"""

from __future__ import annotations

from bedrock.utils.validation.analysis.release_v0_3_progression import (
    PINNED_USEEIO_BASELINE,
    ProgressionSheet,
    sheets_in_order,
)

G2_METHODS = ProgressionSheet(
    step_label="G2: Bedrock methods (published BEA, 2023 UMD GHG)",
    sheet_id="15f1ORDsM6bxvihEH-Ae0smRLQV9NRj5Xe6utTF6rHHM",
    config_name="v05_waterfall_g2_methods",
    sheet_title=(
        "[2026-10-01, bedrock repo, 2024, USEEIO, "
        "v05_waterfall_g2_methods @ 3a1dddc] EFs diagnostics"
    ),
)

G3_DATA = ProgressionSheet(
    step_label="G3: US data update (2024 UMD GHG / IO)",
    sheet_id="1Qu2dxuS5wwSOyBOpys6_q9CHVkrbW6sBWbW6OJh0ceE",
    config_name="v05_waterfall_g3_data",
    sheet_title=(
        "[2026-10-01, bedrock repo, 2024, USEEIO, "
        "v05_waterfall_g3_data @ 3a1dddc] EFs diagnostics"
    ),
)

G4_NOWCAST = ProgressionSheet(
    step_label="G4: Nowcasting (v0.5 MUT, facility GHG off)",
    sheet_id="1KBn9lFI-_AcT_6nL-dMcv9sB3dcBdyw3Q81Xzbgs8Bw",
    config_name="v05_waterfall_g4_nowcast",
    sheet_title=(
        "[2026-10-01, bedrock repo, 2024, USEEIO, "
        "v05_waterfall_g4_nowcast @ 3a1dddc] EFs diagnostics"
    ),
)

FINAL_V05_USEEIO = ProgressionSheet(
    step_label="FINAL v0.5 (facility GHG)",
    sheet_id="1m1bC80uaomBu8VAL9TabvLisjqHuqqBtxBdgH32hlMA",
    config_name="2025_usa_cornerstone_v0_5",
    sheet_title=(
        "[2026-10-01, bedrock repo, 2024, USEEIO, "
        "2025_usa_cornerstone_v0_5 release FBS 3a1dddc] EFs diagnostics"
    ),
)

V0_V05_US_WATERFALL_GROUP_SHEETS: tuple[ProgressionSheet, ...] = (
    G2_METHODS,
    G3_DATA,
    G4_NOWCAST,
    FINAL_V05_USEEIO,
)

V0_V05_US_WATERFALL_STACK_SHEETS: tuple[ProgressionSheet, ...] = (
    G2_METHODS,
    G3_DATA,
    G4_NOWCAST,
)

V05_WATERFALL_CONFIGS: tuple[str, ...] = tuple(
    s.config_name for s in V0_V05_US_WATERFALL_GROUP_SHEETS
)


def useeio_group_stack_target_mapping(
    sheets: tuple[ProgressionSheet, ...],
) -> dict[str, str]:
    """Stacked net-diff targets: G2 vs pinned USEEIO, then each step vs prior."""
    config_names = [s.config_name for s in sheets]
    if not config_names:
        return {}
    mapping: dict[str, str] = {config_names[0]: PINNED_USEEIO_BASELINE}
    for idx in range(1, len(config_names)):
        mapping[config_names[idx]] = config_names[idx - 1]
    return mapping


__all__ = [
    "FINAL_V05_USEEIO",
    "G2_METHODS",
    "G3_DATA",
    "G4_NOWCAST",
    "V05_WATERFALL_CONFIGS",
    "V0_V05_US_WATERFALL_GROUP_SHEETS",
    "V0_V05_US_WATERFALL_STACK_SHEETS",
    "sheets_in_order",
    "useeio_group_stack_target_mapping",
]
