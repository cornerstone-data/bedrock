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

import matplotlib.pyplot as plt
import pandas as pd
import yaml

import bedrock.analysis.a_matrix_time_series.compare_method_stability as cms
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
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_B_non_finetuned,
)
from bedrock.utils.config.config_controllers import clear_caches, force_set_usa_config
from bedrock.utils.config.usa_config import get_usa_config, reset_usa_config
from bedrock.utils.economic.inflation_helpers_cornerstone import (
    inflate_cornerstone_q_or_y_with_commodity_pi,
)
from bedrock.utils.math.formulas import (
    backcompute_E_matrix_via_commodity_shortcut,
    compute_d,
    compute_L_matrix,
    compute_M_matrix,
    compute_n,
)

TARGET_YEARS: list[int] = list(range(ORIGINAL_YEAR, LATEST_TARGET_YEAR))

logger = logging.getLogger(__name__)


_CACHE_BEARING_MODULE_PATHS = (
    "bedrock.transform.eeio.derived_cornerstone",
    "bedrock.transform.eeio.cornerstone_bea_intermediates",
    "bedrock.transform.eeio.derived_useeio_nowcast",
    "bedrock.utils.economic.inflation_helpers_cornerstone",
)


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
    all_results: dict[str, dict[int, dict]],
    plot_sectors: list[str],
) -> None:
    from matplotlib.lines import Line2D  # noqa: PLC0415

    row_models = [m for m in all_results if m != "model1"]
    col_vars = ["e_c", "q", "d", "n"]
    col_titles = {
        "e_c": "e_c (emissions)",
        "q": "q (output)",
        "d": "d (intensity)",
        "n": "n (total intensity)",
    }

    colors = plt.rcParams["axes.prop_cycle"].by_key()["color"]
    fig, axes = plt.subplots(
        len(row_models),
        4,
        figsize=(20, 4 * len(row_models)),
        sharey="row",
        squeeze=False,
    )
    fig.suptitle("e_c, q, d, and n — indexed to first year = 100", fontsize=12)

    for row_idx, model in enumerate(row_models):
        year_results = all_results[model]
        years = sorted(year_results)

        for col_idx, var_key in enumerate(col_vars):
            ax = axes[row_idx, col_idx]
            for i, sector in enumerate(plot_sectors):
                color = colors[i % len(colors)]
                vals = []
                for yr in years:
                    item = year_results[yr][var_key]
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
                    years, indexed, color=color, marker="o", markersize=3, linewidth=1.2
                )

            ax.axhline(100, color="black", linewidth=0.7, linestyle="--")
            ax.set_xlabel("Year")
            if col_idx == 0:
                ax.set_ylabel(f"{model}\nIndex (first year = 100)", fontsize=8)
            if row_idx == 0:
                ax.set_title(col_titles[var_key])

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
    fig.legend(
        handles=sector_handles,
        fontsize=7,
        loc="lower center",
        ncol=len(plot_sectors),
        title="sector",
        title_fontsize=7,
        bbox_to_anchor=(0.5, 0),
    )

    fig.tight_layout(rect=[0, 0.05, 1, 1])
    plot_path = PLOTS_DIR / "trends_d_q_ec.png"
    fig.savefig(plot_path, dpi=150)
    plt.close(fig)
    logger.info("Saved plot: %s", plot_path)


def _build_n_yoy_per_sector(
    all_results: dict[str, dict[int, dict[str, pd.Series]]],
) -> pd.DataFrame:
    """Build a per_sector DataFrame compatible with _yoy_distribution_plot.

    Columns: approach, sector, mean_N, mean_abs_yoy_pct, yoy_{y0}_{y1} per transition.
    """
    transitions = [
        (TARGET_YEARS[i], TARGET_YEARS[i + 1]) for i in range(len(TARGET_YEARS) - 1)
    ]
    rows = []
    for model, year_results in all_results.items():
        for sector in sectors:
            n_by_year = {
                yr: float(year_results[yr]["n"].get(sector, float("nan")))
                for yr in TARGET_YEARS
                if yr in year_results
            }
            row: dict = {"approach": model, "sector": sector}
            row["mean_N"] = pd.Series(list(n_by_year.values())).mean()
            yoy_abs: list[float] = []
            for y0, y1 in transitions:
                v0 = n_by_year.get(y0, float("nan"))
                v1 = n_by_year.get(y1, float("nan"))
                if v0 and v0 != 0:
                    pct = (v1 - v0) / abs(v0)
                else:
                    pct = float("nan")
                row[f"yoy_{y0}_{y1}"] = pct
                if not pd.isna(pct):
                    yoy_abs.append(abs(pct))
            row["mean_abs_yoy_pct"] = (
                pd.Series(yoy_abs).mean() if yoy_abs else float("nan")
            )
            rows.append(row)
    return pd.DataFrame(rows)


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
                reset_usa_config()
                clear_caches(*_CACHE_BEARING_MODULE_PATHS)
                force_set_usa_config(
                    MODEL_YAMLS[model],
                    **({} if model == "model1" else {"usa_ghg_data_year": year}),
                    **(
                        {"model_base_year": year}
                        if model not in {"model1", "model3a"}
                        else {}
                    ),
                    **({"usa_io_data_year": year} if model == "model4" else {}),
                )

                # Get model config vars to understand settings
                cfg = get_usa_config()
                config_vars = cfg.model_dump()
                config_file = OUTPUT_DIR / f"config_vars_{model}_{year}.yaml"
                with open(config_file, "w") as f:
                    yaml.dump(config_vars, f, default_flow_style=False)

                B = derive_cornerstone_B_non_finetuned()
                d_current_USD = compute_d(B=B)
                Aqset = derive_cornerstone_Aq_scaled()
                A = Aqset.Adom + Aqset.Aimp
                q_current_USD = Aqset.scaled_q
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
        _plot_ef_trends(all_results, top)

        cms.YOY_TRANSITIONS = tuple(
            (TARGET_YEARS[i], TARGET_YEARS[i + 1]) for i in range(len(TARGET_YEARS) - 1)
        )
        per_sector = _build_n_yoy_per_sector(all_results)
        cms._yoy_signed_violin_plot(per_sector, PLOTS_DIR / "n_yoy_distribution.png")
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
