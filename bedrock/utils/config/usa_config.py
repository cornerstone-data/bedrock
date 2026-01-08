import os
import typing as ta

import pandas as pd
import yaml
from pydantic import BaseModel

CONFIG_DIR = os.path.join(os.path.dirname(__file__), "configs")
CEDA_USA_CONFIG_ENV_VAR = "CEDA_USA_CONFIG_FILE"


class USAConfig(BaseModel):
    #####
    # CEDA base year
    #####
    ceda_base_year: ta.Literal[2022, 2023] = 2022

    #####
    # Data selection
    #####
    usa_io_data_year: ta.Literal[2022, 2023] = 2022
    usa_ghg_data_year: ta.Literal[2022, 2023] = 2022

    ipcc_ar_version: ta.Literal["AR5", "AR6"] = "AR5"

    #####
    # Bugfix
    #####

    @property
    def usa_detail_original_year(self) -> ta.Literal[2012, 2017]:
        return 2017

    def to_dict(self) -> dict[str, bool]:
        return {
            field_name: getattr(self, field_name) for field_name in self.model_fields
        }

    def to_dataframe(self, config_name: str) -> pd.DataFrame:
        config_dict = self.to_dict()
        config_dict_df = pd.DataFrame(
            [
                {"config_field": key, "value": value}
                for key, value in config_dict.items()
            ]
        )
        summaries = pd.concat(
            [
                pd.DataFrame(
                    {"config_field": "config_name", "value": config_name}, index=[0]
                ),
                config_dict_df,
            ],
        )
        return summaries


_usa_config: ta.Optional[USAConfig] = None


def _load_usa_config_from_file_name(config_file_name: str) -> USAConfig:
    assert config_file_name.endswith(".yaml"), "config file name must end with .yaml"
    with open(os.path.join(CONFIG_DIR, config_file_name)) as f:
        data = yaml.safe_load(f)
    config = USAConfig.model_validate(data, strict=True)
    return config


def set_global_usa_config(config_file: str) -> None:
    global _usa_config
    config_file_env = os.environ.get(CEDA_USA_CONFIG_ENV_VAR)

    if (_usa_config is not None) or (config_file_env is not None):
        raise ValueError("Global USA config already set")

    if not config_file.endswith(".yaml"):
        config_file += ".yaml"

    _usa_config = _load_usa_config_from_file_name(config_file)
    os.environ[CEDA_USA_CONFIG_ENV_VAR] = config_file


def get_usa_config() -> USAConfig:
    global _usa_config
    if _usa_config is None:
        env_usa_config_file = os.environ.get(CEDA_USA_CONFIG_ENV_VAR)
        if env_usa_config_file:
            _usa_config = _load_usa_config_from_file_name(env_usa_config_file)
        else:
            set_global_usa_config("v8_ceda_2025_usa.yaml")
    assert _usa_config is not None
    return _usa_config


def reset_usa_config(should_reset_env_var: bool = False) -> None:
    """For testing purposes"""
    global _usa_config
    _usa_config = None
    if should_reset_env_var and CEDA_USA_CONFIG_ENV_VAR in os.environ:
        del os.environ[CEDA_USA_CONFIG_ENV_VAR]
