"""Phase 3 pin-equality and impact-cache helpers."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from bedrock.analysis.nowcasting.waste_disaggregation.phase3_pins import (
    ANALYSIS_CONFIG_DIR,
    GCS_MUT_VINTAGE,
    PHASE3_YEARS,
    control_config_name,
    install_analysis_usa_config,
    treatment_config_name,
)
from bedrock.analysis.nowcasting.waste_disaggregation.scripts import (
    probe_gcs_mut_vintage as probe_mod,
)
from bedrock.analysis.nowcasting.waste_disaggregation.scripts.run_waste_weight_impact_efs import (  # noqa: E501
    impact_cache_dir,
)
from bedrock.utils.config.usa_config import (
    USAConfig,
    _load_usa_config_from_file_name,
    get_usa_config,
    reset_usa_config,
)

CONFIG_DIR = ANALYSIS_CONFIG_DIR


def _load_analysis_config(stem: str) -> USAConfig:
    path = CONFIG_DIR / f"{stem}.yaml"
    with path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return USAConfig.model_validate(data, strict=True)


@pytest.mark.parametrize("year", list(PHASE3_YEARS))
def test_phase3_control_treatment_share_gcs_mut_pin(year: int) -> None:
    control = _load_analysis_config(control_config_name(year))
    treatment = _load_analysis_config(treatment_config_name(year))
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


def test_no_local_yaml_configs_ship() -> None:
    locals_ = list(CONFIG_DIR.glob("*_local.yaml"))
    assert locals_ == [], f"Track C local YAMLs must be deleted: {locals_}"


def test_impact_cache_dirs_isolate_gcs_vs_other_vintages() -> None:
    gcs = impact_cache_dir(2024, GCS_MUT_VINTAGE)
    other = impact_cache_dir(2024, "v0.3.0_deadbeef")
    assert gcs != other
    assert gcs.name.startswith("impact_2024_")
    assert other.name.startswith("impact_2024_")
    assert "92b7a8a" in gcs.name


def test_analysis_stems_do_not_resolve_via_production_loader() -> None:
    stem = f"{control_config_name(2024)}.yaml"
    with pytest.raises(FileNotFoundError):
        _load_usa_config_from_file_name(stem)


def test_install_analysis_usa_config_sets_singleton() -> None:
    reset_usa_config()
    path = CONFIG_DIR / f"{control_config_name(2024)}.yaml"
    cfg = install_analysis_usa_config(path)
    assert cfg.nowcast_mut_vintage == GCS_MUT_VINTAGE
    assert get_usa_config() is cfg
    reset_usa_config()


def test_probe_fail_closed_on_misses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(probe_mod, "CACHE", tmp_path / "phase3_gcs_mut_vintage.json")
    monkeypatch.setattr(
        probe_mod,
        "_coverage",
        lambda vintage: ([2018, 2019], [2020, 2021, 2022, 2023, 2024]),
    )
    monkeypatch.setattr(
        probe_mod,
        "latest_nowcast_mut_vintage",
        lambda **kwargs: GCS_MUT_VINTAGE,
    )
    with pytest.raises(SystemExit) as exc:
        probe_mod.main()
    assert "FAILED" in str(exc.value)
    report = json.loads(
        (tmp_path / "phase3_gcs_mut_vintage.json").read_text(encoding="utf-8")
    )
    assert report["status"] == "failed"
    assert report["holes"]
