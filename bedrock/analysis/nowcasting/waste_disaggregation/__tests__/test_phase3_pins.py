"""Phase 3 pin-equality and impact-cache helpers."""

from __future__ import annotations

from pathlib import Path

import pytest

from bedrock.analysis.nowcasting.waste_disaggregation.phase3_pins import (
    GCS_MUT_VINTAGE,
    PHASE3_YEARS,
    control_config_name,
    treatment_config_name,
)
from bedrock.analysis.nowcasting.waste_disaggregation.scripts.run_waste_weight_impact_efs import (  # noqa: E501
    impact_cache_dir,
)
from bedrock.utils.config.usa_config import _load_usa_config_from_file_name

CONFIG_DIR = Path(__file__).resolve().parents[4] / "utils" / "config" / "configs"


@pytest.mark.parametrize("year", list(PHASE3_YEARS))
def test_phase3_control_treatment_share_gcs_mut_pin(year: int) -> None:
    control = _load_usa_config_from_file_name(f"{control_config_name(year)}.yaml")
    treatment = _load_usa_config_from_file_name(f"{treatment_config_name(year)}.yaml")
    assert control.nowcast_mut_vintage == GCS_MUT_VINTAGE
    assert treatment.nowcast_mut_vintage == GCS_MUT_VINTAGE
    assert control.nowcast_mut_vintage == treatment.nowcast_mut_vintage
    assert control.waste_weights_year == 2017
    assert treatment.waste_weights_year == "match_io"
    assert control.usa_base_io_data_year == year
    assert treatment.usa_base_io_data_year == year
    assert control.implement_waste_disaggregation is True
    assert treatment.implement_waste_disaggregation is True
    # Electricity off (Decision 6)
    assert control.implement_electricity_reallocation is False
    assert treatment.implement_electricity_reallocation is False


@pytest.mark.parametrize("year", list(PHASE3_YEARS))
def test_phase3_yaml_files_exist(year: int) -> None:
    c = CONFIG_DIR / f"{control_config_name(year)}.yaml"
    t = CONFIG_DIR / f"{treatment_config_name(year)}.yaml"
    assert c.is_file(), c
    assert t.is_file(), t
    assert GCS_MUT_VINTAGE in c.read_text(encoding="utf-8")
    assert GCS_MUT_VINTAGE in t.read_text(encoding="utf-8")


def test_impact_cache_dirs_isolate_gcs_vs_local_vintages() -> None:
    gcs = impact_cache_dir(2024, GCS_MUT_VINTAGE)
    local = impact_cache_dir(2024, "v0.3.0_deadbeef")
    assert gcs != local
    assert gcs.name.startswith("impact_2024_")
    assert local.name.startswith("impact_2024_")
    assert GCS_MUT_VINTAGE.replace(".", ".") in gcs.name or "v0" in gcs.name
