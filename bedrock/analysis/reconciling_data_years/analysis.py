"""
Depict the time series of changes to d, L and n in the given models
Use initial cfg settings in __init__ and constants
Build d, L and n components of model1, model2 and model3
Avoid generating model diagnostics instead compute components right here in this script

The script inherits the cache management of the model config from the A matrix time series
analysis script to handle multiple years of multiple models

The script uses sectors from SIGNIFICANT_SECTORS modified in constants.
The script identifies those among this list with the max range of efs over those years
to display in the plots

For d and n there are plots for each model with lines for each sectors over the time period.

Resulting efs are put in a common dollar year of LATEST_TARGET_YEAR

Model1 is only one model then the final d and n get adjusted to the time series years

"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
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
    OUTPUT_DIR,
    PLOTS_DIR,
    RESULTS_DIR,
    sector_names,
    sectors,
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
    data["usa_io_data_year"] = 2017
    data["use_cornerstone_2026_model_schema"] = (
        True  # if not it will default to CEDAv7 schema which could cause issues with ghg_method?
    )
    data["implement_waste_disaggregation"] = True
    data["apply_inflation_to_V"] = True
    if model != "model1":
        data["usa_ghg_data_year"] = year
    # The package `__init__.py` flips these on the initial config; replacing
    # `_usa_config` here would otherwise drop them back to field defaults.
    usa_config._usa_config = USAConfig.model_construct(**data)
    os.environ[usa_config.USA_CONFIG_ENV_VAR] = MODEL_YAMLS[model]


def deflate_ef(
    ef: pd.Series[float], original_year: int, target_year: int
) -> pd.Series[float]:
    from bedrock.utils.economic.inflation_helpers_cornerstone import (  # noqa: PLC0415
        get_vnorm_adjusted_commodity_price_ratio,
    )

    rho = 1 / get_vnorm_adjusted_commodity_price_ratio(original_year, target_year)
    return ef * rho


def _top_fluctuating_sectors(
    all_results: dict[str, dict[int, dict[str, pd.Series]]],
    top_n: int = 5,
) -> list[str]:
    """Rank sectors by their maximum value range across all models and ef keys."""
    scores: dict[str, float] = {}
    for sector in sectors:
        max_range = 0.0
        for year_results in all_results.values():
            for ef_key in ("d", "n"):
                vals = [year_results[yr][ef_key][sector] for yr in sorted(year_results)]
                max_range = max(max_range, max(vals) - min(vals))
        scores[sector] = max_range

    top = sorted(scores, key=lambda s: scores[s], reverse=True)[:top_n]
    logger.info("Top %d fluctuating sectors:", top_n)
    for s in top:
        logger.info("  %s  %s  range=%.4f", s, sector_names.get(s, s), scores[s])
    return top


def _plot_ef_trends(
    ef_key: str,
    label: str,
    all_results: dict[str, dict[int, dict[str, pd.Series]]],
    plot_sectors: list[str],
) -> None:
    models = list(all_results)
    fig, axes = plt.subplots(1, len(models), figsize=(7 * len(models), 6), sharey=True)
    if len(models) == 1:
        axes = [axes]
    fig.suptitle(f"{label} — emission intensity trends")

    for ax, model in zip(axes, models):
        year_results = all_results[model]
        years = sorted(year_results)
        for sector in plot_sectors:
            vals = [year_results[yr][ef_key][sector] for yr in years]
            indexed = [v / vals[0] * 100 for v in vals]
            name = sector_names.get(sector, sector)[:15]
            ax.plot(years, indexed, marker="o", label=name)
        ax.axhline(100, color="black", linewidth=0.7, linestyle="--")
        ax.set_title(model)
        ax.set_xlabel("Year")
        ax.set_ylabel("Index (first year = 100)")
        if ax is axes[0]:
            ax.legend(fontsize=7, loc="best")

    fig.tight_layout()
    plot_path = PLOTS_DIR / f"trends_{ef_key}.png"
    fig.savefig(plot_path, dpi=150)
    plt.close(fig)
    logger.info("Saved plot: %s", plot_path)


def main() -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    all_results: dict[str, dict[int, dict[str, pd.Series]]] = {}

    for model in MODELS:
        year_results: dict[int, dict[str, pd.Series]] = {}
        for year in TARGET_YEARS:
            logger.info("Building matrices: model=%s year=%d", model, year)
            try:
                # Clear cache and reset
                _reset_config()
                _clear_config_dependent_caches()
                _set_config(model, year)
                # Get model config vars to understand settings
                config_vars = usa_config._usa_config.model_dump()
                config_file = OUTPUT_DIR / f"config_vars_{model}_{year}.yaml"
                with open(config_file, "w") as f:
                    yaml.dump(config_vars, f, default_flow_style=False)

                B = derive_cornerstone_B_via_vnorm()
                d = compute_d(B=B)
                Aqset = derive_cornerstone_Aq()
                A = Aqset.Adom + Aqset.Aimp
                L = compute_L_matrix(A=A)
                n = compute_n(M=compute_M_matrix(B=B, L=L))
                # Put efs in common dollar year
                d = deflate_ef(d, original_year=year, target_year=LATEST_TARGET_YEAR)
                n = deflate_ef(n, original_year=year, target_year=LATEST_TARGET_YEAR)
                year_results[year] = {"d": d, "n": n}

            except Exception as e:
                logger.warning("Model,year (%s, %d) failed: %s", model, year, e)
                continue

        if year_results:
            all_results[model] = year_results

    if all_results:
        top = _top_fluctuating_sectors(all_results)
        _plot_ef_trends("d", "d (direct intensity)", all_results, top)
        _plot_ef_trends("n", "n (total intensity)", all_results, top)

    print("Done.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
