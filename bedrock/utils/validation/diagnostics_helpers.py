"""Helper functions for diagnostics.

This module provides:
- Pydantic models for structuring EF comparison data
- Core comparison functions (diff, percent diff)
- Summary statistics calculations
- Inflation adjustment for EF denominators
- Data loading for diagnostics
"""

from __future__ import annotations

import typing as ta

import numpy as np
import pandas as pd
from pydantic import BaseModel

from bedrock.utils.economic.inflation import (
    obtain_inflation_factors_from_reference_data,
)
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTOR_DESC


class OldEfSet(BaseModel):
    """Container for old emission factor data with raw and inflation-adjusted versions.

    The raw values are in the original base year's dollars.
    The inflated values are adjusted to the current base year for fair comparison.
    """

    raw: pd.DataFrame
    inflated: pd.DataFrame

    class Config:
        arbitrary_types_allowed = True


class EfsForDiagnostics(BaseModel):
    """Container for all emission factor data needed for diagnostics.

    Contains new (derived) EFs and old (snapshot) EFs for both D and N:
    - D = direct emission factors (from B matrix)
    - N = total emission factors (from M matrix)
    """

    D_new: pd.DataFrame
    N_new: pd.DataFrame
    D_old: OldEfSet
    N_old: OldEfSet

    class Config:
        arbitrary_types_allowed = True


def diff_and_perc_diff_two_vectors(
    vector_new: pd.DataFrame,
    vector_old: pd.DataFrame,
    old_val_name: str,
    new_val_name: ta.Optional[str] = None,
) -> pd.DataFrame:
    """Compute absolute and percentage differences between two vectors.

    Args:
        vector_new: New vector values (as single-column DataFrame)
        vector_old: Old vector values (as single-column DataFrame)
        old_val_name: Name for the old value in output columns
        new_val_name: Name for new value columns (defaults to old_val_name)

    Returns:
        DataFrame with columns:
        - {new_val_name}_new: New values
        - {old_val_name}_old: Old values
        - {old_val_name}_diff: Absolute difference (new - old)
        - {old_val_name}_perc_diff: Percentage difference ((new - old) / old)
    """
    if new_val_name is None:
        new_val_name = old_val_name

    val_name_new = f"{new_val_name}_new"
    val_name_old = f"{old_val_name}_old"

    comparison = pd.concat([vector_new, vector_old], axis=1)
    comparison.columns = pd.Index([val_name_new, val_name_old])

    comparison[f"{old_val_name}_diff"] = (
        comparison[val_name_new] - comparison[val_name_old]
    )

    # Handle division by zero: replace 0 with NaN, compute ratio, replace inf with NaN, fill with 0
    comparison[f"{old_val_name}_perc_diff"] = (
        (
            comparison[f"{old_val_name}_diff"]
            / comparison[val_name_old].replace(0, np.nan)
        )
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0.0)
    )

    return comparison[
        [
            val_name_new,
            val_name_old,
            f"{old_val_name}_diff",
            f"{old_val_name}_perc_diff",
        ]
    ]


def diff_and_perc_diff_two_sector_vectors(
    vector_old: pd.DataFrame,
    vector_new: pd.DataFrame,
    old_val_name: str,
    new_val_name: str,
) -> pd.DataFrame:
    """Compute diff/perc_diff and add human-readable sector names.

    Args:
        vector_old: Old vector values
        vector_new: New vector values
        old_val_name: Name for old value columns
        new_val_name: Name for new value columns

    Returns:
        DataFrame with sector_name column prepended to diff results
    """
    comparison = diff_and_perc_diff_two_vectors(
        vector_new,
        vector_old,
        new_val_name=new_val_name,
        old_val_name=old_val_name,
    )

    existing_cols = comparison.columns.tolist()
    comparison["sector_name"] = comparison.index.map(CEDA_V7_SECTOR_DESC)

    return comparison[["sector_name"] + existing_cols]


def calculate_summary_stats_for_ef_diff_dataframe(
    ef_name: str,
    ef_comparison: pd.DataFrame,
    cols_to_summarize: ta.List[str],
) -> pd.DataFrame:
    """Calculate summary statistics (median, std) for EF differences.

    Args:
        ef_name: Name of the emission factor (e.g., "D" or "N")
        ef_comparison: DataFrame from construct_ef_diff_dataframe
        cols_to_summarize: List of column names to compute stats for

    Returns:
        DataFrame with columns: ef_name, statistic, median, std
    """
    summary_rows = []

    for col in cols_to_summarize:
        stats: dict[str, ta.Union[str, float]] = {
            "ef_name": ef_name,
            "statistic": col,
        }
        stats["median"] = ef_comparison[col].median()
        stats["std"] = ef_comparison[col].std()
        summary_rows.append(stats)

    return pd.DataFrame(summary_rows)


def construct_ef_diff_dataframe(
    ef_name: str,
    ef_new: pd.DataFrame,
    ef_old: OldEfSet,
) -> pd.DataFrame:
    """Build full comparison DataFrame for emission factors.

    Compares new EFs against old versions,
    including both raw and inflation-adjusted old values.

    Args:
        ef_name: Name of the emission factor ("D" or "N")
        ef_new: New emission factor DataFrame
        ef_old: Old EF (raw + inflated)

    Returns:
        DataFrame with columns for new values, old values (raw & inflated),
        and percentage differences
    """
    ef_comparison = (
        diff_and_perc_diff_two_sector_vectors(
            vector_old=ef_old.inflated,
            vector_new=ef_new,
            new_val_name=ef_name,
            old_val_name=ef_name,
        )
        .rename(columns={f"{ef_name}_old": f"{ef_name}_old_inflated"})
        .drop(columns=[f"{ef_name}_diff"])
    )

    raw_values = ta.cast("pd.Series[float]", ef_old.raw.squeeze())
    ef_comparison.insert(
        3,
        f"{ef_name}_old",
        raw_values,
    )

    return ef_comparison


def inflation_adjust_ef_denom_to_new_base_year(
    old_ef_vector: pd.Series[float],
    new_base_year: int,
    old_base_year: int,
) -> pd.Series[float]:
    """Adjust emission factor denominators for inflation between base years.

    Emission factors have units like "kg CO2 / $" where $ is in a specific base year.
    This function adjusts old EFs to a new base year so comparisons are fair.

    The adjustment multiplies by (old_base_year_price / new_base_year_price),
    effectively converting the denominator from old $ to new $.

    Args:
        old_ef_vector: Old emission factor values (indexed by sector)
        new_base_year: Target base year for the denominator
        old_base_year: Original base year of the old EF values

    Returns:
        Inflation-adjusted emission factor values
    """
    price_index = obtain_inflation_factors_from_reference_data()

    # Calculate price ratio between base years for each sector
    price_ratio = (price_index[old_base_year] / price_index[new_base_year]).fillna(1.0)

    # Align to EF vector's index, filling missing sectors with 1.0 (no adjustment)
    price_ratio_aligned = price_ratio.reindex(old_ef_vector.index).fillna(1.0)

    return old_ef_vector * price_ratio_aligned


def pull_efs_for_diagnostics() -> EfsForDiagnostics:
    """Load and prepare all emission factor data for diagnostics."""
    raise NotImplementedError("pull_efs_for_diagnostics is not yet implemented.")
