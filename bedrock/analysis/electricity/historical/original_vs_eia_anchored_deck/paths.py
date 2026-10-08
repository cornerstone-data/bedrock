"""Paths for the published original vs EIA-anchored (pre-MECS) comparison pack."""

from __future__ import annotations

from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent
FIGURES_DIR = PACKAGE_DIR / 'figures'
TABLES_YAML = PACKAGE_DIR / 'tables.yaml'

PANEL_PNG: dict[tuple[str, str], str] = {
    ('original', 'D'): 'v0.2_original_electricity_disagg_D.png',
    ('original', 'N'): 'v0.2_original_electricity_disagg_N.png',
    ('eia_gtd', 'D'): 'v0.3_eia_gtd_pre_mecs_D.png',
    ('eia_gtd', 'N'): 'v0.3_eia_gtd_pre_mecs_N.png',
}


def panel_png(impl_id: str, kind: str) -> Path | None:
    name = PANEL_PNG.get((impl_id, kind))
    if name is None:
        return None
    path = FIGURES_DIR / name
    return path if path.is_file() else None
