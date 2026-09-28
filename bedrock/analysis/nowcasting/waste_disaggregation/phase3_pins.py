"""Phase 3 locked GCS NowcastMUT vintage (Track A gate).

Verified by ``scripts/probe_gcs_mut_vintage.py`` — full after-redef Make
coverage for 2018–2024. Commit ``f709829a`` is not proof of MUT upload;
this string comes from the GCS probe only.
"""

from __future__ import annotations

GCS_MUT_VINTAGE = "v0.3.0_f709829"
PHASE3_YEARS = tuple(range(2018, 2025))

CONTROL_CONFIG_FMT = "2025_usa_cornerstone_v0_4_nowcast_{year}_waste_weights_control"
TREATMENT_CONFIG_FMT = "2025_usa_cornerstone_v0_4_nowcast_{year}_waste_weights_match_io"


def control_config_name(year: int) -> str:
    return CONTROL_CONFIG_FMT.format(year=year)


def treatment_config_name(year: int) -> str:
    return TREATMENT_CONFIG_FMT.format(year=year)
