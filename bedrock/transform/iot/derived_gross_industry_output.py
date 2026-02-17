from __future__ import annotations

import logging

import numpy as np
import pandas as pd

SECTOR_CODE_COL = "sector_code"

logger = logging.getLogger(__name__)


def derive_gross_output_after_redefinition(target_year: int) -> pd.Series:
    """
    Derive after-redefinition gross industry output for a target year.

    Loads the 2017 benchmark Make table (before redefinitions), computes
    co-production ratios, then applies them to the BEA gross-output time
    series for the requested *target_year*.

    Parameters
    ----------
    target_year : int
        Calendar year (e.g. 2023) for which to derive the adjusted gross
        output.  Must be present in the BEA detail gross-output workbook.

    Returns
    -------
    pd.Series
        After-redefinition gross output indexed by BEA detail industry code.
    """
    from bedrock.extract.iot.gdp import load_go_detail
    from bedrock.extract.iot.io_2017 import (
        load_2017_V_before_redef_usa,
        load_2017_V_usa,
    )
    from bedrock.transform.iot.helpers import map_detail_table

    V_before_redef = load_2017_V_before_redef_usa()
    V_after_redef = load_2017_V_usa()
    ratios = compute_coproduction_ratios(V_before_redef, V_after_redef)

    go_detail = map_detail_table(load_go_detail())
    go_detail = go_detail.dropna(subset=[SECTOR_CODE_COL])

    year_col: int | str
    if target_year in go_detail.columns:
        year_col = target_year
    elif str(target_year) in go_detail.columns:
        year_col = str(target_year)
    else:
        available = sorted(
            c for c in go_detail.columns if c not in ("sector_name", SECTOR_CODE_COL)
        )
        raise ValueError(
            f"Target year {target_year} not found in gross output time series. "
            f"Available columns: {available}"
        )

    go_before = go_detail.set_index(SECTOR_CODE_COL)[year_col]
    assert isinstance(go_before, pd.Series)

    if not go_before.index.is_unique:
        logger.warning("Duplicate sector codes in gross output; aggregating by sum.")
        go_before = go_before.groupby(level=0).sum()

    return adjust_gross_output(go_before, ratios)


def extract_coproduction_entries(V_before_redef: pd.DataFrame) -> pd.DataFrame:
    """
    Extract off-diagonal (co-production) entries from the Make table.

    In the Make table (industry x commodity), diagonal entries represent an
    industry producing its primary commodity. Off-diagonal entries represent
    co-production — an industry producing a commodity that is not its primary
    product.

    Parameters
    ----------
    V_before_redef : pd.DataFrame
        Make table (industry x commodity), before redefinitions.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns ``source_industry``, ``commodity``, ``value``.
        Only includes entries where industry != commodity and value != 0.
    """
    stacked = V_before_redef.stack()

    source_industry = stacked.index.get_level_values(0)
    commodity = stacked.index.get_level_values(1)
    is_off_diagonal = source_industry != commodity
    is_nonzero = stacked.values != 0

    coproduction = stacked[is_off_diagonal & is_nonzero].reset_index()
    coproduction.columns = pd.Index(["source_industry", "commodity", "value"])
    return coproduction


def compute_coproduction_ratios(
    V_before_redef: pd.DataFrame,
    V_after_redef: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Compute co-production movement ratios from the benchmark Make tables.

    When only *V_before_redef* is supplied the ratio uses the full
    off-diagonal value (original behaviour)::

        ratio(i, c) = V_before_redef[i, c] / g_before[i]

    When *V_after_redef* is also supplied the ratio is **constrained** to
    the amount actually moved during BEA's redefinition process::

        ratio(i, c) = (V_before_redef[i, c] - V_after_redef[i, c]) / g_before[i]

    This ensures an exact round-trip for the benchmark year and a more
    accurate adjustment for other years.

    The **destination industry** is the industry whose primary commodity is
    ``c``.  At BEA's detail level the industry and commodity code namespaces
    are the same, so the destination industry code equals the commodity code.

    Parameters
    ----------
    V_before_redef : pd.DataFrame
        Make table (industry x commodity), before redefinitions.
    V_after_redef : pd.DataFrame or None
        Make table (industry x commodity), after redefinitions.  When
        provided the movement delta ``V_before_redef - V_after_redef``
        is used as the numerator.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns ``source_industry``, ``destination_industry``,
        ``ratio``.
    """
    V_movement = (
        V_before_redef - V_after_redef if V_after_redef is not None else V_before_redef
    )
    coproduction = extract_coproduction_entries(V_movement)

    g = V_before_redef.sum(axis=1)
    source_g: np.ndarray[tuple[int], np.dtype[np.floating]] = np.asarray(
        g.reindex(coproduction["source_industry"].values).values, dtype=float
    )
    coprod_values: np.ndarray[tuple[int], np.dtype[np.floating]] = np.asarray(
        coproduction["value"].values, dtype=float
    )
    ratio = np.where(source_g != 0, coprod_values / source_g, 0.0)

    return pd.DataFrame(
        {
            "source_industry": coproduction["source_industry"].values,
            "destination_industry": coproduction["commodity"].values,
            "ratio": ratio,
        }
    )


def adjust_gross_output(
    go_before: pd.Series,
    coproduction_ratios: pd.DataFrame,
) -> pd.Series:
    """
    Apply redefinition adjustments to a gross-output vector.

    For every co-production ratio entry the function computes::

        value_to_move = ratio * go_before[source_industry]

    then subtracts that amount from the source industry and adds it to the
    destination industry.  The total across all industries is preserved
    (zero-sum redistribution).

    Parameters
    ----------
    go_before : pd.Series
        Gross output by industry (before redefinition) for a single year,
        indexed by BEA industry code.
    coproduction_ratios : pd.DataFrame
        Output of :func:`compute_coproduction_ratios`.

    Returns
    -------
    pd.Series
        Adjusted gross output vector (after redefinition).
    """
    go_adjusted = go_before.copy().astype(float)

    valid_mask = coproduction_ratios["source_industry"].isin(
        go_before.index
    ) & coproduction_ratios["destination_industry"].isin(go_before.index)

    skipped = coproduction_ratios[~valid_mask]
    if len(skipped) > 0:
        missing_sources = set(skipped["source_industry"]) - set(go_before.index)
        missing_dests = set(skipped["destination_industry"]) - set(go_before.index)
        if missing_sources:
            logger.warning(
                "Skipping co-production entries for source industries "
                "not in gross output: %s",
                missing_sources,
            )
        if missing_dests:
            logger.warning(
                "Skipping co-production entries for destination industries "
                "not in gross output: %s",
                missing_dests,
            )

    valid_ratios = coproduction_ratios[valid_mask]
    if valid_ratios.empty:
        return go_adjusted

    ratio_arr = np.asarray(valid_ratios["ratio"].values, dtype=float)
    source_keys = list(valid_ratios["source_industry"])
    source_go_arr = np.asarray(go_before.loc[source_keys].values, dtype=float)
    movements = ratio_arr * source_go_arr

    subtractions = (
        pd.Series(movements, index=valid_ratios["source_industry"].values)
        .groupby(level=0)
        .sum()
    )
    additions = (
        pd.Series(movements, index=valid_ratios["destination_industry"].values)
        .groupby(level=0)
        .sum()
    )

    go_adjusted -= subtractions.reindex(go_adjusted.index, fill_value=0.0)
    go_adjusted += additions.reindex(go_adjusted.index, fill_value=0.0)

    return go_adjusted
