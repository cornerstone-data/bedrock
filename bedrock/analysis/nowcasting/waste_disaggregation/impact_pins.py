"""Locked GCS NowcastMUT vintage for waste-weight impact analysis.

Verified by ``scripts/probe_gcs_mut_vintage.py`` — full after-redef Make
coverage for 2018–2024 under ``v0.3.0_92b7a8a`` only (fail-closed). Commit
SHAs are not proof of MUT upload; this string comes from the GCS probe only.

Analysis YAMLs live under ``configs/`` next to this module (not production
``utils/config/configs``). Load them via ``install_analysis_usa_config``.
"""

from __future__ import annotations

from pathlib import Path

import yaml

from bedrock.utils.config.usa_config import USAConfig, set_global_usa_config_object

GCS_MUT_VINTAGE = "v0.3.0_92b7a8a"
IMPACT_YEARS = tuple(range(2018, 2025))

# Analysis-only YAMLs (GCS year-aligned vs 2017); not under utils/config/configs.
ANALYSIS_CONFIG_DIR = Path(__file__).resolve().parent / "configs"

CONTROL_CONFIG_FMT = "2025_usa_cornerstone_v0_4_nowcast_{year}_waste_weights_control"
TREATMENT_CONFIG_FMT = "2025_usa_cornerstone_v0_4_nowcast_{year}_waste_weights_match_io"


def control_config_name(year: int) -> str:
    return CONTROL_CONFIG_FMT.format(year=year)


def treatment_config_name(year: int) -> str:
    return TREATMENT_CONFIG_FMT.format(year=year)


def install_analysis_usa_config(path: Path) -> USAConfig:
    """Load an analysis-only USAConfig from *path* and install it process-wide.

    Production ``set_global_usa_config`` only resolves stems under
    ``utils/config/configs``. Impact workers must use this helper.
    """
    path = Path(path)
    with path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    config = USAConfig.model_validate(data, strict=True)
    set_global_usa_config_object(config, source_label=str(path))
    return config
