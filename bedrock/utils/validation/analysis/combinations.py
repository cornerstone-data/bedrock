"""Registered diagnostics combinations: named multi-run comparisons.

Each entry describes one ``combine_ef_diagnostics`` run: an ordered list of
diagnostics Sheet titles (matching titles inside ``drive_folder_id``), the
target column to subtract from for net-difference tabs, and the output Sheet
where merged tabs are pushed. Names mirror the original
``INPUT_FILE_STEMS``/``TARGET_COLUMN_BY_CONFIG_NAME`` constants so the v0.2
run is reproducible by name.

To run a registered combination::

    python -m bedrock.utils.validation.analysis.combine_ef_diagnostics \
        --combo v0.2

For ad-hoc comparisons (Sheet IDs supplied on the command line) skip this
file and pass ``--sheet-id`` repeatedly instead.
"""

from __future__ import annotations

from dataclasses import dataclass

from bedrock.utils.validation.analysis.release_v0_3_progression import (
    CEDA_V02_TO_V03_SHEETS,
    USEEIO_V02_TO_V03_SHEETS,
    ceda_stepwise_target_mapping,
    sheets_in_order,
    useeio_stepwise_target_mapping,
)


@dataclass(frozen=True)
class ComboSpec:
    """One registered diagnostics-comparison preset.

    Attributes:
        drive_folder_id: Drive folder holding the per-run diagnostics Sheets.
            Used to resolve ``names_in_order`` to Sheet IDs when
            ``sheets_in_order`` is empty.
        names_in_order: Ordered Sheet titles (one per diagnostics run). Order
            controls merge order in the output tabs.
        target_mapping: Per-``config_name`` target column for the net-diff
            tabs (subtract ``target`` from ``config``). Keys must match the
            ``config_name`` value stored in each run's ``config_summary`` tab.
        sheets_in_order: Optional explicit ``(sheet_id, title)`` pairs. When
            non-empty, inputs are taken directly and ``drive_folder_id`` /
            ``names_in_order`` are ignored (for Sheets that span folders).

    The destination Sheet for merged output is always supplied on the
    command line via ``--output-sheet-id``; it is intentionally not part of
    the combo spec so a single combo can be reused across destinations.
    """

    drive_folder_id: str
    names_in_order: list[str]
    target_mapping: dict[str, str]
    sheets_in_order: tuple[tuple[str, str], ...] = ()


# Historical destination Sheets (pass via --output-sheet-id when reproducing):
#   v0.2 -> 1TOLpjg80GBeb3C8sVKGvYRL9U5HfUgKSz_IHoWHainY  ('v0.2 Diagnostics')
COMBINATIONS: dict[str, ComboSpec] = {
    'v0.3_useeio_phoebe': ComboSpec(
        drive_folder_id='1M2-Vopqfrx1vGcwoNi6wq55FmoELNV1s',
        names_in_order=[
            '[05-06-2026] USEEIO Baseline build',
            '[05-06-2026] USEEIO - restore GHG only',
            '[05-06-2026] USEEIO - restore GHG model and schema',
            '[05-06-2026] USEEIO - restore Cornerstone B approach',
            '[05-06-2026] USEEIO - restore Cornerstone A approach',
            '[05-06-2026] USEEIO - restore after redefinitions',
            '[05-06-2026] USEEIO - compare to full Cornerstone',
        ],
        target_mapping={
            # ``useeio_phoebe_23`` is the Bedrock attempt to rebuild the pinned
            # USEEIO model and is itself imperfect, so its net-diff column
            # subtracts the synthetic ``pinned_useeio_baseline`` (sourced from
            # this Sheet's ``D_old_inflated`` and ``N_old_purchaser``, i.e.
            # the pinned USEEIO Excel baseline at purchaser price). Every
            # restoration step then
            # compares against the rebuild so the chain of divergence reads
            # naturally.
            'useeio_phoebe_23': 'pinned_useeio_baseline',
            'useeio_phoebe_23_restore_ghg': 'useeio_phoebe_23',
            'useeio_phoebe_23_restore_schema_and_ghg': 'useeio_phoebe_23',
            'useeio_phoebe_23_restore_cornerstone_B': 'useeio_phoebe_23',
            'useeio_phoebe_23_restore_cornerstone_A': 'useeio_phoebe_23',
            'useeio_phoebe_23_restore_iot_redefinition': 'useeio_phoebe_23',
            'useeio_phoebe_23_cornerstone_margins': 'useeio_phoebe_23',
            '2025_usa_cornerstone_v0_2': 'useeio_phoebe_23',
        },
    ),
    'v0.2': ComboSpec(
        drive_folder_id='1eJ648O86tPqQnQwetYsXo7FtZLeHAsqG',
        names_in_order=[
            '[2026-03-26 v0 baseline] EF diagnostics',
            '[2026-03-26 row 3, Cornerstone schema] EF diagnostics',
            '[2026-03-26 row 4, CEDA FBS] EF diagnostics',
            '[2026-03-26 row 5, Cornerstone schema and CEDA FBS] EF diagnostics',
            '[2026-03-26 row 6, refrigerants compared to GHG baseline] EF diagnostics',
            '[2026-03-26 row 7, ng and petrol systems compared to GHG baseline] EF diagnostics',
            '[2026-03-26 row 8, mobile transport compared to GHG baseline] EF diagnostics',
            '[2026-03-26 row 9, electricity compared to GHG baseline] EF diagnostics',
            '[2026-03-26 row 10, new activities compared to GHG baseline] EF diagnostics',
            '[2026-03-26 row 11, other gases update compared to GHG baseline] EF diagnostics',
            '[2026-03-26 row 12, ag and field emissions compared to GHG baseline] EF diagnostics',
            '[2026-03-26 row 13, CoA update compared to GHG baseline] EF diagnostics',
            '[2026-03-26 row 18, Waste disagg compared to GHG baseline] EF diagnostics',
            '[2026-03-26 row 20, full GHG model] EF diagnostics',
            '[2026-04-14 row 14, B transformation] EF diagnostics',
            '[2026-04-15 row 21, B transformation AND waste] EF diagnostics',
            '[2026-04-08 row 23, all changes] EF diagnostics',
        ],
        target_mapping={
            'v8_ceda_2025_usa': 'v8_ceda_2025_usa',
            '2025_usa_ceda_ghg_from_flowsa': 'v8_ceda_2025_usa',
            '2025_usa_cornerstone_taxonomy': 'v8_ceda_2025_usa',
            '2025_usa_cornerstone_fbs_schema': 'v8_ceda_2025_usa',
            # GHG runs -> cornerstone_fbs_schema
            '2025_usa_cornerstone_ghg_refrigerants_foams': '2025_usa_cornerstone_fbs_schema',
            '2025_usa_cornerstone_ghg_petroleum_natgas': '2025_usa_cornerstone_fbs_schema',
            '2025_usa_cornerstone_ghg_mobile_combustion': '2025_usa_cornerstone_fbs_schema',
            '2025_usa_cornerstone_ghg_electricity': '2025_usa_cornerstone_fbs_schema',
            '2025_usa_cornerstone_ghg_new_activities': '2025_usa_cornerstone_fbs_schema',
            '2025_usa_cornerstone_ghg_other_gases': '2025_usa_cornerstone_fbs_schema',
            '2025_usa_cornerstone_ghg_ag_soils': '2025_usa_cornerstone_fbs_schema',
            '2025_usa_cornerstone_ghg_updated_coa': '2025_usa_cornerstone_fbs_schema',
            '2025_usa_cornerstone_ghg': '2025_usa_cornerstone_fbs_schema',
            # Non-GHG configs (edit if your comparison target differs)
            '2025_usa_cornerstone_taxonomy_and_waste_disagg': '2025_usa_cornerstone_fbs_schema',
            '2025_usa_cornerstone_taxonomy_and_B_transformation': 'v8_ceda_2025_usa',
            '2025_usa_cornerstone_B_transformation_and_waste_disaggregation': '2025_usa_cornerstone_taxonomy_and_waste_disagg',
            '2025_usa_cornerstone_v0_2': 'v8_ceda_2025_usa',
        },
    ),
    # v0 baseline → FINAL v0.2 → v0.3 release steps. Net-diff chains stepwise;
    # atomic v0.2-footing configs (inflation, A/price, MECS) diff vs full_model.
    'v0.2_to_v0.3_ceda': ComboSpec(
        drive_folder_id='',
        names_in_order=[],
        target_mapping=ceda_stepwise_target_mapping(CEDA_V02_TO_V03_SHEETS),
        sheets_in_order=sheets_in_order(CEDA_V02_TO_V03_SHEETS),
    ),
    # FINAL v0.2 → v0.3 release steps. Net-diff chains stepwise; atomic
    # v0.2-footing configs (inflation, A/price, MECS) diff vs full_model.
    'v0.2_to_v0.3_useeio': ComboSpec(
        drive_folder_id='',
        names_in_order=[],
        target_mapping=useeio_stepwise_target_mapping(USEEIO_V02_TO_V03_SHEETS),
        sheets_in_order=sheets_in_order(USEEIO_V02_TO_V03_SHEETS),
    ),
}
