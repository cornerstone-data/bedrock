"""
Depict the time series of changes to d, L and n in the given models
Use initial cfg settings in __init__ and constants
Build d, L and n components of model1, model2 and model3
Avoid generating model diagnostics instead compute components right here in this script

The script inherits the cache management of the model config from the A matrix time series
analysis script to handle multiple years of multiple models.
And it uses the E time series generation and storage from the B time series analysis
so a single set of E's is used for the models. Not this means
derive_cornerstone_B_via_vnorm is not used to get B because we want to be able to
derive it with our own E

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
from bedrock.analysis.time_series_B_matrix.derive_B_time_series import (
    derive_E_time_series,
)
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_Vnorm_scrap_corrected,
    derive_cornerstone_x_after_redefinition,
)
from bedrock.utils.config.usa_config import USAConfig
from bedrock.utils.economic.inflation_helpers_cornerstone import (
    get_cornerstone_industry_price_ratio,
    inflate_cornerstone_q_or_y_with_commodity_pi,
)
from bedrock.utils.math.formulas import (
    backcompute_E_matrix_via_commodity_shortcut,
    compute_B_ind_matrix,
    compute_B_matrix,
    compute_d,
    compute_L_matrix,
    compute_M_matrix,
    compute_n,
    derive_q_from_x_and_Vnorm,
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
    data["use_cornerstone_2026_model_schema"] = (
        True  # if not it will default to CEDAv7 schema which could cause issues with ghg_method?
    )
    data["implement_waste_disaggregation"] = True
    # For model 1 the values should not change year to year
    if model != "model1":
        data["usa_ghg_data_year"] = year

    # Model base year for models 4 should change but not for 1, 2 or 3
    if model == "model4":
        data["model_base_year"] = year
    else:
        data["model_base_year"] = 2017

    # The package `__init__.py` flips these on the initial config; replacing
    # `_usa_config` here would otherwise drop them back to field defaults.
    usa_config._usa_config = USAConfig.model_construct(**data)
    os.environ[usa_config.USA_CONFIG_ENV_VAR] = MODEL_YAMLS[model]


def store_E_matrices() -> dict[int, pd.DataFrame]:
    """Load FBS parquets from fbs_cache, derive E matrices, save to e_cache."""
    fbs_cache = OUTPUT_DIR / "fbs_cache"
    e_cache = OUTPUT_DIR / "e_cache"
    e_cache.mkdir(parents=True, exist_ok=True)

    fbs_by_year: dict[int, pd.DataFrame] = {}
    for year in TARGET_YEARS:
        matches = list(fbs_cache.glob(f"GHG_national_Cornerstone_{year}_*.parquet"))
        if not matches:
            logger.warning("No FBS parquet found for %d in %s", year, fbs_cache)
            continue
        logger.info("Loading FBS for %d: %s", year, matches[0].name)
        fbs_by_year[year] = pd.read_parquet(matches[0])

    E_by_year = derive_E_time_series(fbs_by_year)

    for year, E in E_by_year.items():
        out_path = e_cache / f"E_{year}.parquet"
        E.to_parquet(out_path)
        logger.info("Saved E matrix for %d to %s", year, out_path)

    return E_by_year


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
    fig, axes = plt.subplots(2, 2, figsize=(14, 10), sharey=True)
    axes_flat = axes.flatten()
    fig.suptitle(f"{label} — emission intensity trends")

    for ax, model in zip(axes_flat, models):
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
        if ax is axes_flat[0]:
            ax.legend(fontsize=7, loc="best")

    for ax in axes_flat[len(models) :]:
        ax.set_visible(False)

    fig.tight_layout()
    plot_path = PLOTS_DIR / f"trends_{ef_key}.png"
    fig.savefig(plot_path, dpi=150)
    plt.close(fig)
    logger.info("Saved plot: %s", plot_path)


def _plot_d_with_q_ec(
    all_results: dict[str, dict[int, dict]],
    plot_sectors: list[str],
) -> None:
    models = list(all_results)
    fig, axes = plt.subplots(2, 2, figsize=(14, 10), sharey=True)
    axes_flat = axes.flatten()
    fig.suptitle("d, q, and e_c — indexed to first year = 100")

    var_styles = {
        "d": {"linestyle": "-", "linewidth": 1.5},
        "q": {"linestyle": "--", "linewidth": 1.2},
        "e_c": {"linestyle": ":", "linewidth": 1.2},
    }
    colors = plt.rcParams["axes.prop_cycle"].by_key()["color"]

    for ax, model in zip(axes_flat, models):
        year_results = all_results[model]
        years = sorted(year_results)
        for i, sector in enumerate(plot_sectors):
            color = colors[i % len(colors)]
            name = sector_names.get(sector, sector)[:15]
            for key, style in var_styles.items():
                vals = []
                for yr in years:
                    item = year_results[yr][key]
                    # e_c is a DataFrame (stressors × sectors); sum across stressors
                    if isinstance(item, pd.DataFrame):
                        val = (
                            float(item[sector].sum())
                            if sector in item.columns
                            else float("nan")
                        )
                    else:
                        val = (
                            float(item[sector])
                            if sector in item.index
                            else float("nan")
                        )
                    vals.append(val)
                base = vals[0] if vals[0] else float("nan")
                indexed = [v / base * 100 for v in vals]
                ax.plot(
                    years,
                    indexed,
                    color=color,
                    marker="o",
                    markersize=4,
                    label=f"{name} ({key})",
                    **style,
                )
        ax.axhline(100, color="black", linewidth=0.7, linestyle="--")
        ax.set_title(model)
        ax.set_xlabel("Year")
        ax.set_ylabel("Index (first year = 100)")

    for ax in axes_flat[len(models) :]:
        ax.set_visible(False)

    # Two-part legend: sector colors + variable line styles
    from matplotlib.lines import Line2D  # noqa: PLC0415

    sector_handles = [
        Line2D(
            [0],
            [0],
            color=colors[i % len(colors)],
            linewidth=2,
            label=sector_names.get(s, s)[:15],
        )
        for i, s in enumerate(plot_sectors)
    ]
    var_handles = [
        Line2D([0], [0], color="black", label=key, **style)
        for key, style in var_styles.items()
    ]
    axes_flat[0].legend(
        handles=sector_handles + var_handles,
        fontsize=7,
        loc="best",
        title="sector / variable",
        title_fontsize=7,
    )

    fig.tight_layout()
    plot_path = PLOTS_DIR / "trends_d_q_ec.png"
    fig.savefig(plot_path, dpi=150)
    plt.close(fig)
    logger.info("Saved plot: %s", plot_path)


def main() -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    E_by_year = store_E_matrices()
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
                cfg = usa_config._usa_config
                config_vars = cfg.model_dump()
                config_file = OUTPUT_DIR / f"config_vars_{model}_{year}.yaml"
                with open(config_file, "w") as f:
                    yaml.dump(config_vars, f, default_flow_style=False)

                x = derive_cornerstone_x_after_redefinition()
                ## Mimick derive_cornerstone_B_via_vnorm
                if cfg.use_useeio_B:
                    ratio = get_cornerstone_industry_price_ratio(
                        original_year=cfg.usa_ghg_data_year,
                        target_year=cfg.usa_detail_original_year,
                    )
                    # ratio is PI_target / PI_original; divide nominal target-year dollars
                    # to convert x into original-year dollars for USEEIO-style B.
                    ratio_aligned = ratio.reindex(x.index)
                    ratio_aligned = ratio_aligned.where(ratio_aligned.notna(), 1.0)
                    x = x * ratio_aligned
                Vnorm = derive_cornerstone_Vnorm_scrap_corrected(
                    apply_inflation=cfg.apply_inflation_to_V,
                    target_year=cfg.model_base_year,
                )
                B_ind = compute_B_ind_matrix(E=E_by_year[cfg.usa_ghg_data_year], x=x)

                B = compute_B_matrix(B_ind=B_ind, V_norm=Vnorm)
                d_current_USD = compute_d(B=B)
                Aqset = derive_cornerstone_Aq_scaled()
                A = Aqset.Adom + Aqset.Aimp

                q_current_USD = derive_q_from_x_and_Vnorm(x=x, Vnorm=Vnorm)
                e_c = backcompute_E_matrix_via_commodity_shortcut(B=B, q=q_current_USD)
                L = compute_L_matrix(A=A)
                n_current_USD = compute_n(M=compute_M_matrix(B=B, L=L))
                # Put efs in constant dollar year using most recent dollar year
                d = deflate_ef(
                    d_current_USD,
                    original_year=cfg.model_base_year,
                    target_year=LATEST_TARGET_YEAR,
                )
                n = deflate_ef(
                    n_current_USD,
                    original_year=cfg.model_base_year,
                    target_year=LATEST_TARGET_YEAR,
                )
                # Deflate q as well for presentation
                q = inflate_cornerstone_q_or_y_with_commodity_pi(
                    q_current_USD,
                    original_year=cfg.model_base_year,
                    target_year=LATEST_TARGET_YEAR,
                )
                year_results[year] = {
                    "d": d,
                    "n": n,
                    "d_current_USD": d_current_USD,
                    "n_current_USD": n_current_USD,
                    "q": q,
                    "e_c": e_c,
                }

            except Exception as e:
                logger.warning("Model,year (%s, %d) failed: %s", model, year, e)
                continue

        if year_results:
            all_results[model] = year_results

    if all_results:
        top = _top_fluctuating_sectors(all_results)
        _plot_d_with_q_ec(all_results, top)
        _plot_ef_trends("n", "n (total intensity)", all_results, top)

        records = []
        for model, year_results in all_results.items():
            for year, ef_dict in year_results.items():
                for variable, series in ef_dict.items():
                    for sector in sectors:
                        if sector in series.index:
                            records.append(
                                {
                                    "year": year,
                                    "model": model,
                                    "sector": sector,
                                    "variable": variable,
                                    "ef": series[sector],
                                }
                            )
        results_df = pd.DataFrame(
            records, columns=["year", "model", "sector", "variable", "ef"]
        )
        csv_path = RESULTS_DIR / "efs.csv"
        results_df.to_csv(csv_path, index=False)
        logger.info("Saved results to %s", csv_path)

    print("Done.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
