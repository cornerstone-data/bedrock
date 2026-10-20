"""v0.5→v0.6 US waterfall diagnostics — USEEIO-track group endpoints.

Five configs at IO@2024 producer footing. Combine net-diffs chain G2 → G3 →
G4 → G5 → FINAL (marginal strips on the US release ladder).

Group definitions (cumulative):
  G2 — ``v06_waterfall_g2_methods`` — bedrock methods on published BEA detail,
       2023 UMD GHG (same as v0.5 G2)
  G3 — ``v06_waterfall_g3_data`` — G2 + 2024 UMD GHG / IO data (same as v0.5 G3)
  G4 — ``v06_waterfall_g4_nowcast`` — G3 + nowcast MUT
       (same as v0.5 G4)
  G5 — ``v06_waterfall_g5_facilities`` — G4 + the v0.5.9 Use and price index
       (≡ ``2025_usa_cornerstone_v0_5_9``)
  FINAL — ``2025_usa_cornerstone_v0_6`` — G5 + the v0.6 GHG FBS pin

Sheet IDs point at diagnostics spreadsheets in the v0.6 Diagnostics Drive
folder. G2-G5 are the 2026-10-12 USEEIO cut on ``dfe743bc``. FINAL is the
2026-10-13 USEEIO cut on ``e10c75f5``. Each ``sheet_title`` records the date
and release the sheet was generated for.

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
    sheet_id="1R4YwO3iJlpgTS3v3tzff9dK0fS0Jh50hnLCfFXOWIdA",
    config_name="v06_waterfall_g2_methods",
    sheet_title=(
        "[2026-10-12, bedrock repo, 2024, useeio, "
        "v06_waterfall_g2_methods] EFs diagnostics"
    ),
)

G3_DATA = ProgressionSheet(
    step_label="G3: US data update (2024 UMD GHG / IO)",
    sheet_id="1h3XSik89fgBGRVRIFlbEZlgx7twfqwRJZbkGJ_pjLW0",
    config_name="v06_waterfall_g3_data",
    sheet_title=(
        "[2026-10-12, bedrock repo, 2024, useeio, "
        "v06_waterfall_g3_data] EFs diagnostics"
    ),
)

G4_NOWCAST = ProgressionSheet(
    step_label="G4: Nowcasting (MUT)",
    sheet_id="1k5lbEmatm3tTxAJnezPomHGi910JoD7lvBdRFUbtIf8",
    config_name="v06_waterfall_g4_nowcast",
    sheet_title=(
        "[2026-10-12, bedrock repo, 2024, useeio, "
        "v06_waterfall_g4_nowcast] EFs diagnostics"
    ),
)

G5_FACILITIES = ProgressionSheet(
    step_label="G5: v0.5.9 Use and price index",
    sheet_id="1eFDud33FtKVrwcmTDAhWR-6ikPYZT8GMblEaMkilsnU",
    config_name="v06_waterfall_g5_facilities",
    sheet_title=(
        "[2026-10-12, bedrock repo, 2024, useeio, "
        "v06_waterfall_g5_facilities] EFs diagnostics"
    ),
)

FINAL_V06_USEEIO = ProgressionSheet(
    step_label="FINAL v0.6",
    sheet_id="1NTeXoNNcSwwroee6F884mYPlMvqniMHRIcW1f5GVKuY",
    config_name="2025_usa_cornerstone_v0_6",
    sheet_title=(
        "[2026-10-13, bedrock repo, 2024, useeio, "
        "2025_usa_cornerstone_v0_6] EFs diagnostics"
    ),
)

V0_V06_US_WATERFALL_GROUP_SHEETS: tuple[ProgressionSheet, ...] = (
    G2_METHODS,
    G3_DATA,
    G4_NOWCAST,
    G5_FACILITIES,
    FINAL_V06_USEEIO,
)

V0_V06_US_WATERFALL_STACK_SHEETS: tuple[ProgressionSheet, ...] = (
    G2_METHODS,
    G3_DATA,
    G4_NOWCAST,
    G5_FACILITIES,
)

V06_WATERFALL_CONFIGS: tuple[str, ...] = tuple(
    s.config_name for s in V0_V06_US_WATERFALL_GROUP_SHEETS
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
    "FINAL_V06_USEEIO",
    "G2_METHODS",
    "G3_DATA",
    "G4_NOWCAST",
    "G5_FACILITIES",
    "V06_WATERFALL_CONFIGS",
    "V0_V06_US_WATERFALL_GROUP_SHEETS",
    "V0_V06_US_WATERFALL_STACK_SHEETS",
    "sheets_in_order",
    "useeio_group_stack_target_mapping",
]
