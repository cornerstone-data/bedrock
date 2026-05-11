"""
Depict the time series of changes to d, L and n in the given models
Use initial cfg settings in __init__ and constants
Build d, L and n components of model1, model2 and model3
Avoid generating model diagnostics instead compute components right here in this script

The script inherits the cache management of the model config from the A matrix time series
analysis script to handle multiple years of multiple models

Model1 is only one model then the final d and n get adjusted to the time series years

"""

import logging
import os
from pathlib import Path

import yaml

import bedrock.utils.config.usa_config as usa_config
from bedrock.analysis.a_matrix_time_series.derive_A_time_series import (
    _clear_config_dependent_caches,
    _reset_config,
)
from bedrock.analysis.reconciling_data_years.constants import (
    LATEST_TARGET_YEAR,
    MODEL_YAMLS,
    MODELS,
    ORIGINAL_YEAR,
    PLOTS_DIR,
    RESULTS_DIR,
)
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_Aq,
    derive_cornerstone_B_via_vnorm,
)
from bedrock.utils.config.usa_config import USAConfig
from bedrock.utils.math.formulas import (
    compute_d,
    compute_L_matrix,
    compute_M_matrix,
    compute_n,
)

TARGET_YEARS: list[int] = list(range(ORIGINAL_YEAR, LATEST_TARGET_YEAR))

logger = logging.getLogger(__name__)


def _set_config(model: str, year: int) -> None:
    """Install a config for the given (model, year), bypassing pydantic
    Literal validation on ``model_base_year`` since we run for target years but
    the schema only allows 2022–2024.

    Uses ``USAConfig.model_construct`` to skip validation. Also clears the
    ``USA_CONFIG_FILE`` env var and ``_usa_config`` module global so each call
    is fresh — ``set_global_usa_config`` raises if invoked twice.
    """
    yaml_path = Path(usa_config.CONFIG_DIR) / MODEL_YAMLS[model]
    with open(yaml_path) as f:
        data = yaml.safe_load(f) or {}
    data["model_base_year"] = year
    if model != "model1":
        data["usa_ghg_data_year"] = year
    # The package `__init__.py` flips these on the initial config; replacing
    # `_usa_config` here would otherwise drop them back to field defaults.

    usa_config._usa_config = USAConfig.model_construct(**data)
    os.environ[usa_config.USA_CONFIG_ENV_VAR] = MODEL_YAMLS[model]


def main() -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    for model in MODELS:
        for year in TARGET_YEARS:
            logger.info("Building matrices: model=%s year=%d", model, year)
            try:
                # Clear cache and reset
                _reset_config()
                _clear_config_dependent_caches()
                _set_config(model, year)
                # Get model config vars to understand settings
                config_vars = usa_config._usa_config.model_dump()
                logger.info(config_vars)
                B = derive_cornerstone_B_via_vnorm()
                d = compute_d(B)

                Aqset = derive_cornerstone_Aq()
                A = Aqset.Adom + Aqset.Aimp

                L = compute_L_matrix(A=A)

                n = compute_n(compute_M_matrix(B=B, L=L))  # noqa: F841

            except Exception as e:
                logger.warning("Model,year (%s, %d) failed: %s", model, year, e)
                continue

    print("Done.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
