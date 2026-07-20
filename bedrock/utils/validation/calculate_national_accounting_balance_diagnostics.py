# ruff: noqa: PLC0415
from __future__ import annotations

import logging
import typing as ta

import numpy as np
import pandas as pd

from bedrock.utils.config.usa_config import USAConfig, get_usa_config
from bedrock.utils.io.gcp import update_sheet_tab
from bedrock.utils.snapshots.loader import (
    load_configured_snapshot,
    load_snapshot,
    resolve_snapshot_key,
)

logger = logging.getLogger(__name__)
_USEEIO_ONLY_NON_CORNERSTONE_SECTORS = frozenset({"S00300", "S00401", "S00900"})


def _series_from_1d_frame_or_series(obj: pd.DataFrame | pd.Series) -> pd.Series[float]:
    """Parquet ``y_nab_USA`` is a one-column frame or a Series; avoid ``squeeze()`` for mypy."""
    if isinstance(obj, pd.Series):
        return obj.astype(float)
    if obj.shape[1] == 1:
        return obj.iloc[:, 0].astype(float)
    raise ValueError(f"y_nab snapshot expected 1 column, got shape {obj.shape}")


def _compute_bly_series(
    *,
    B: pd.DataFrame,
    Adom: pd.DataFrame,
    y: pd.Series[float],
) -> pd.Series[float]:
    from bedrock.utils.math.formulas import compute_d, compute_L_matrix

    L = compute_L_matrix(A=Adom)
    d = compute_d(B=B)
    raw = pd.DataFrame(np.diag(d), index=L.index, columns=L.columns) @ L @ y
    if isinstance(raw, pd.Series):
        return raw.astype(float)
    if raw.shape[1] == 1:
        return raw.iloc[:, 0].astype(float)
    if raw.shape[0] == 1:
        return raw.iloc[0, :].astype(float)
    raise TypeError(f"unexpected BLy shape {raw.shape}")


def _cornerstone_y_nab_dollar_year(cfg: USAConfig) -> int:
    """Dollar year of ``derive_cornerstone_y_nab()`` under the active A/q path.

    ``y_nab`` is backcomputed from ``derive_cornerstone_Aq_scaled()``. When
    ``scale_a_matrix_with_useeio_method`` is on, that path returns the 2017
    detail base unchanged, so ``y`` stays in ``usa_detail_original_year``
    dollars even if ``model_base_year`` is later. Other scale/inflate paths
    put ``q`` (and thus ``y``) in ``model_base_year`` dollars.
    """
    if cfg.scale_a_matrix_with_useeio_method:
        return int(cfg.usa_detail_original_year)
    return int(cfg.model_base_year)


def _rebase_b_intensity_dollar_year(
    B: pd.DataFrame,
    *,
    from_year: int,
    to_year: int,
) -> pd.DataFrame:
    """Rebase ``B`` intensity denominators from ``from_year`` $ to ``to_year`` $.

    No-op when the years match. Uses the same sector price-index ratio as
    ``inflation_adjust_ef_denom_to_new_base_year`` (EF ``D_old_inflated`` path).
    """
    if from_year == to_year:
        return B
    from bedrock.utils.validation.diagnostics_helpers import (
        inflation_adjust_ef_denom_to_new_base_year,
    )

    scale = inflation_adjust_ef_denom_to_new_base_year(
        pd.Series(1.0, index=B.columns, dtype=float),
        new_base_year=to_year,
        old_base_year=from_year,
    )
    return B * scale.to_numpy(dtype=float)


def _percent_diff_vs_denominator(
    diff_kg: np.ndarray,
    denom_kg: np.ndarray,
) -> np.ndarray:
    with np.errstate(divide="ignore", invalid="ignore"):
        ratio = np.where(
            np.isfinite(denom_kg) & (denom_kg != 0),
            diff_kg / denom_kg,
            np.nan,
        )
    return np.where(
        np.isfinite(ratio),
        ratio,
        np.where(np.isfinite(diff_kg) & (diff_kg == 0), 0.0, np.nan),
    )


def calculate_national_accounting_balance_diagnostics(
    sheet_id: str,
) -> None:
    """Validate that the US portion of Cornerstone's MRIO model conserves total emissions.

    Checks whether sum(BLy) (total production-induced emissions from the
    IO framework) matches sum(E_orig) (total original emissions data)
    at the national level.

    BLy = diag(d) @ L @ y_nab
    Where:
    - d = compute_d(B) = column sums of B (direct emissions per $ output)
    - L = Leontief inverse (total requirements matrix)
    - y_nab = final demand vector for national accounting balance

    If the model is balanced, sum(BLy) should equal sum(E_orig).

    Writes:

    - ``BLy_and_E_orig_diffs``: USA totals only (live BLy vs snapshot ``E``).
      **Skipped** when ``diagnostics_baseline_source == 'gcs_useeio_xlsx'`` (no
      ``E_old`` for the Excel baseline path).
    - ``BLy_new_vs_BLy_old``: per-sector live BLy vs BLy recomputed from the
      baseline (parquet ``B_USA_non_finetuned`` / ``Adom_USA`` / ``y_nab_USA`` at
      the configured snapshot key, or USEEIO Excel synthetic ``B`` / ``A_d``
      when in Excel baseline mode). For USEEIO, ``y_old`` is Cornerstone
      ``derive_cornerstone_y_nab()`` reindexed to the USEEIO sector axis
      (USEEIO-only sectors such as ``S00300`` / ``S00401`` / ``S00900`` zeroed).
      Workbook ``B`` intensities are rebased from workbook ``dollar_year`` to
      the dollar year of that ``y_old`` (see ``_cornerstone_y_nab_dollar_year``),
      not blindly to ``model_base_year``. Snapshot baselines leave ``B_old``
      unchanged: snapshot ``B`` and ``y_nab`` already share one dollar year.
    """
    # Late-binding imports - depend on global config
    from bedrock.transform.eeio.cornerstone_disagg_pipeline import (  # noqa: PLC0415
        electricity_mixed_units_enabled,
    )
    from bedrock.transform.eeio.derived import (
        derive_Aq_usa,
        derive_B_usa_non_finetuned,
        derive_y_for_national_accounting_balance_usa,
    )
    from bedrock.transform.eeio.derived_cornerstone import (
        derive_cornerstone_Aq_mixed_units,
        derive_cornerstone_B_mixed_units,
        derive_cornerstone_y_nab_mixed_units,
    )
    from bedrock.utils.validation.diagnostics_helpers import (
        apply_mixed_units_bly_diff_exemptions,
    )

    logger.info("------ Calculating national accounting balance diagnostics ------")

    if electricity_mixed_units_enabled():
        B_new = derive_cornerstone_B_mixed_units()
        Aq_set = derive_cornerstone_Aq_mixed_units()
        y_new = derive_cornerstone_y_nab_mixed_units()
    else:
        B_new = derive_B_usa_non_finetuned()
        Aq_set = derive_Aq_usa()
        y_new = derive_y_for_national_accounting_balance_usa()

    logger.info("1. Calculating BLy (live)...")
    BLy_new = _compute_bly_series(B=B_new, Adom=Aq_set.Adom, y=y_new)

    cfg = get_usa_config()
    if cfg.diagnostics_baseline_source != "gcs_useeio_xlsx":
        logger.info("2. Loading E_orig from snapshot...")
        E_orig = load_configured_snapshot("E_USA_ES")
        E_orig_by_sector = ta.cast("pd.Series[float]", E_orig.sum(axis=0))

        BLy_total = float(BLy_new.sum())
        E_orig_total = float(E_orig_by_sector.sum())
        diff = BLy_total - E_orig_total
        perc_diff = diff / E_orig_total if E_orig_total != 0 else 0.0

        comparison_data: dict[str, list[float | str]] = {
            "BLy (MtCO2e)": [BLy_total / 1e9],
            "E_orig (MtCO2e)": [E_orig_total / 1e9],
            "BLy - E_orig (MtCO2e)": [diff / 1e9],
            "(BLy - E_orig) / E_orig (%)": [perc_diff],
        }
        if electricity_mixed_units_enabled():
            mixed_note = "mixed BLy_new vs monetary E_orig; national drift expected"
            logger.info(mixed_note)
            if abs(perc_diff) > 0.10:
                logger.warning(
                    "BLy vs E_orig national drift |perc_diff|=%.4f exceeds 0.10: %s",
                    abs(perc_diff),
                    mixed_note,
                )
            comparison_data["note"] = [mixed_note]

        logger.info("3. Writing BLy vs E (national totals)...")
        comparison = pd.DataFrame(
            comparison_data,
            index=pd.Index(["USA"]),
        )
        update_sheet_tab(
            sheet_id,
            "BLy_and_E_orig_diffs",
            comparison.reset_index(),
            clean_nans=True,
        )
    else:
        logger.info(
            "2–3. Skipping BLy_and_E_orig_diffs (USEEIO Excel baseline; no E_old)"
        )

    logger.info("4. Loading baseline B, Adom, y_nab; computing BLy_old...")
    if cfg.diagnostics_baseline_source == "gcs_useeio_xlsx":
        from bedrock.transform.eeio.derived_cornerstone import derive_cornerstone_y_nab
        from bedrock.utils.validation.useeio_excel_baseline import (
            load_useeio_baseline_bundle,
        )

        ub = load_useeio_baseline_bundle(cfg)
        y_dollar_year = _cornerstone_y_nab_dollar_year(cfg)
        B_old = _rebase_b_intensity_dollar_year(
            ub.b_old_synthetic,
            from_year=ub.dollar_year,
            to_year=y_dollar_year,
        )
        Adom_old = ub.adom_old
        y_cornerstone = derive_cornerstone_y_nab()
        baseline_axis = Adom_old.index
        missing_in_cornerstone = sorted(
            set(baseline_axis.astype(str)) - set(y_cornerstone.index.astype(str))
        )
        y_old = y_cornerstone.reindex(baseline_axis, fill_value=0.0)
        if missing_in_cornerstone:
            logger.warning(
                "USEEIO baseline sectors not in Cornerstone y_nab were zeroed: "
                "count=%d sectors=%s",
                len(missing_in_cornerstone),
                missing_in_cornerstone,
            )
            unexpected = sorted(
                set(missing_in_cornerstone) - _USEEIO_ONLY_NON_CORNERSTONE_SECTORS
            )
            if unexpected:
                logger.warning(
                    "Unexpected non-overlap beyond expected USEEIO-only sectors: %s",
                    unexpected,
                )
        logger.info(
            "   USEEIO BLy: y_old dollar year=%s (model_base_year=%s, "
            "useeio_A=%s); B_old intensities %s->%s $ when those years differ",
            y_dollar_year,
            cfg.model_base_year,
            cfg.scale_a_matrix_with_useeio_method,
            ub.dollar_year,
            y_dollar_year,
        )
    else:
        snap_key = resolve_snapshot_key()
        B_old = load_snapshot("B_USA_non_finetuned", snap_key)
        Adom_old = load_snapshot("Adom_USA", snap_key)
        y_old = _series_from_1d_frame_or_series(load_snapshot("y_nab_USA", snap_key))
    BLy_old = _compute_bly_series(B=B_old, Adom=Adom_old, y=y_old)

    sector_index = BLy_new.index.union(BLy_old.index).sort_values()
    bly_new_by_sec = BLy_new.reindex(sector_index)
    bly_old_by_sec = BLy_old.reindex(sector_index)
    diff_kg = bly_new_by_sec.fillna(0) - bly_old_by_sec.fillna(0)
    old_arr = bly_old_by_sec.to_numpy(dtype=float, copy=True)
    d_kg = diff_kg.to_numpy(dtype=float, copy=True)
    perc_arr = _percent_diff_vs_denominator(d_kg, old_arr)

    bly_diff_out = pd.DataFrame(
        {
            "index": sector_index,
            "BLy_new (MtCO2e)": bly_new_by_sec / 1e9,
            "BLy_old (MtCO2e)": bly_old_by_sec / 1e9,
            "BLy_new - BLy_old (MtCO2e)": diff_kg / 1e9,
            "(BLy_new - BLy_old) / BLy_old (%)": perc_arr,
        }
    )
    bly_diff_out = apply_mixed_units_bly_diff_exemptions(bly_diff_out)
    logger.info("5. Writing BLy_new vs BLy_old (by sector)...")
    update_sheet_tab(
        sheet_id,
        "BLy_new_vs_BLy_old",
        bly_diff_out,
        clean_nans=True,
    )
