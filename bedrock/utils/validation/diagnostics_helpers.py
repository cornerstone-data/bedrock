"""Helper functions for diagnostics.

This module provides:
- Pydantic models for structuring EF comparison data
- Core comparison functions (diff, percent diff)
- Summary statistics calculations
- Inflation adjustment for EF denominators
- Data loading for diagnostics
- Schema alignment for CEDA v7 ↔ cornerstone diagnostics
"""

from __future__ import annotations

import logging
import time
import typing as ta

import numpy as np
import pandas as pd
from pydantic import BaseModel

from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.economic.inflation import (
    obtain_inflation_factors_from_reference_data,
)
from bedrock.utils.snapshots.loader import load_current_snapshot
from bedrock.utils.snapshots.names import SnapshotName
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTOR_DESC

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Sector alignment constants for CEDA v7 (old) ↔ cornerstone (new)
#
# Sources of truth:
#   Waste:     taxonomy/cornerstone/commodities.py  → WASTE_DISAGG_COMMODITIES
#   Appliance: taxonomy/mappings/bea_v2017_commodity__bea_ceda_v7.py  (335220 → 4 codes)
#   Aluminum:  taxonomy/mappings/bea_v2017_commodity__bea_ceda_v7.py  (33131B → 331313)
# ---------------------------------------------------------------------------
# Appliances: old has 4 codes (see bea_v2017_commodity__bea_ceda_v7); new aggregates to 335220
_APPLIANCE_OLD_CODES: ta.List[str] = ['335221', '335222', '335224', '335228']
_APPLIANCE_NEW_CODE = '335220'
# Aluminum: old has 331313 only; new may split into 331313 + 33131B
_ALUMINUM_OLD_CODE = '331313'
_ALUMINUM_NEW_EXTRA_CODE = '33131B'


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


# ---------------------------------------------------------------------------
# Schema alignment helpers
# ---------------------------------------------------------------------------


def _waste_disagg() -> ta.Tuple[str, ta.List[str]]:
    """Return (old_code, new_subsector_codes) from the cornerstone taxonomy."""
    from bedrock.utils.taxonomy.cornerstone.commodities import (  # noqa: PLC0415
        WASTE_DISAGG_COMMODITIES,
    )

    ((old_code, new_codes),) = WASTE_DISAGG_COMMODITIES.items()
    return old_code, list(new_codes)


def get_aligned_sector_desc() -> ta.Dict[str, str]:
    """Build a sector description dict that covers the aligned diagnostic index.

    Combines CEDA v7 descriptions with cornerstone-only codes so that every
    sector in the aligned index has a human-readable name.
    """
    from bedrock.utils.taxonomy.cornerstone.commodities import (  # noqa: PLC0415
        COMMODITY_DESC,
    )

    desc: ta.Dict[str, str] = dict(CEDA_V7_SECTOR_DESC)  # type: ignore[arg-type]
    for code, name in COMMODITY_DESC.items():
        if code not in desc:
            desc[code] = name
    return desc


def compute_active_mapped_sectors(
    old_ef: pd.DataFrame,
    new_ef: pd.DataFrame,
) -> ta.Dict[str, str]:
    """Determine which sector mappings are active based on the actual indices.

    Only returns mappings where the relevant codes actually exist in the old
    and new EF vectors.  This makes alignment robust to partially-enabled
    config flags (e.g. cornerstone schema on but waste disaggregation off).
    """
    waste_old, waste_new = _waste_disagg()
    old_idx = set(old_ef.index)
    new_idx = set(new_ef.index)
    active: ta.Dict[str, str] = {}

    # Waste: mapping needed only when old has the aggregate but new has subsectors
    if waste_old in old_idx and waste_old not in new_idx:
        if all(c in new_idx for c in waste_new):
            active[waste_old] = 'disaggregated (summed new)'

    # Appliances: mapping needed only when new has 335220 but old has the 4 codes
    if _APPLIANCE_NEW_CODE in new_idx and _APPLIANCE_NEW_CODE not in old_idx:
        if all(c in old_idx for c in _APPLIANCE_OLD_CODES):
            active[_APPLIANCE_NEW_CODE] = 'aggregated (summed old)'

    # Aluminum: mapping needed only when new has extra 33131B that old lacks
    if _ALUMINUM_NEW_EXTRA_CODE in new_idx and _ALUMINUM_NEW_EXTRA_CODE not in old_idx:
        active[_ALUMINUM_OLD_CODE] = 'disaggregated (summed new)'

    return active


def _compute_aligned_index(
    old_ef: pd.DataFrame,
    new_ef: pd.DataFrame,
    active_mappings: ta.Dict[str, str],
) -> ta.List[str]:
    """Compute the common aligned index for old and new EF vectors.

    Starts with codes present in both indices, then appends mapped codes
    that only exist on one side (e.g. ``562000`` from old, ``335220`` from new).
    Codes that are new-only with no baseline counterpart are excluded.
    """
    waste_old, _ = _waste_disagg()
    old_idx = set(old_ef.index)
    new_idx = set(new_ef.index)
    direct_shared = sorted(old_idx & new_idx)

    extra: ta.List[str] = []
    if waste_old in active_mappings:
        extra.append(waste_old)
    if _APPLIANCE_NEW_CODE in active_mappings:
        extra.append(_APPLIANCE_NEW_CODE)

    return direct_shared + extra


def _align_old_ef(
    old_ef: pd.DataFrame,
    aligned_index: ta.List[str],
    active_mappings: ta.Dict[str, str],
) -> pd.DataFrame:
    """Reindex an old (CEDA v7) EF vector onto the aligned index.

    For the appliance group (if active), the four old codes are summed into
    335220.  All other codes are looked up directly.
    """
    appliance_active = _APPLIANCE_NEW_CODE in active_mappings
    rows: ta.Dict[str, float] = {}
    for code in aligned_index:
        if code == _APPLIANCE_NEW_CODE and appliance_active:
            rows[code] = float(old_ef.loc[_APPLIANCE_OLD_CODES].values.sum())
        else:
            rows[code] = float(old_ef.loc[code].values[0])
    return pd.DataFrame.from_dict(rows, orient='index', columns=old_ef.columns)


def _align_new_ef(
    new_ef: pd.DataFrame,
    aligned_index: ta.List[str],
    active_mappings: ta.Dict[str, str],
) -> pd.DataFrame:
    """Reindex a new (cornerstone) EF vector onto the aligned index.

    For waste (if active), subsectors are summed into the aggregate code.
    For aluminum (if active), 331313 + 33131B are summed into 331313.
    All other codes are looked up directly.
    """
    waste_old, waste_new = _waste_disagg()
    waste_active = waste_old in active_mappings
    aluminum_active = _ALUMINUM_OLD_CODE in active_mappings
    rows: ta.Dict[str, float] = {}
    for code in aligned_index:
        if code == waste_old and waste_active:
            rows[code] = float(new_ef.loc[waste_new].values.sum())
        elif code == _ALUMINUM_OLD_CODE and aluminum_active:
            rows[code] = float(
                new_ef.loc[[_ALUMINUM_OLD_CODE, _ALUMINUM_NEW_EXTRA_CODE]].values.sum()
            )
        else:
            rows[code] = float(new_ef.loc[code].values[0])
    return pd.DataFrame.from_dict(rows, orient='index', columns=new_ef.columns)


def align_efs_across_schemas(
    efs: EfsForDiagnostics,
) -> ta.Tuple[EfsForDiagnostics, ta.Dict[str, str]]:
    """Align all EF vectors when old and new schemas differ.

    Inspects the actual indices to determine which mappings are needed, making
    this robust to partially-enabled config flags.

    Returns:
        A tuple of (aligned EfsForDiagnostics, active mapped-sectors dict).
    """
    active_mappings = compute_active_mapped_sectors(efs.D_old.raw, efs.D_new)
    aligned_index = _compute_aligned_index(efs.D_old.raw, efs.D_new, active_mappings)

    logger.info(
        f'Schema alignment: {len(aligned_index)} sectors in common index, '
        f'{len(active_mappings)} active mappings: {active_mappings}'
    )

    aligned_efs = EfsForDiagnostics(
        D_new=_align_new_ef(efs.D_new, aligned_index, active_mappings),
        N_new=_align_new_ef(efs.N_new, aligned_index, active_mappings),
        D_old=OldEfSet(
            raw=_align_old_ef(efs.D_old.raw, aligned_index, active_mappings),
            inflated=_align_old_ef(efs.D_old.inflated, aligned_index, active_mappings),
        ),
        N_old=OldEfSet(
            raw=_align_old_ef(efs.N_old.raw, aligned_index, active_mappings),
            inflated=_align_old_ef(efs.N_old.inflated, aligned_index, active_mappings),
        ),
    )
    return aligned_efs, active_mappings


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

    val_name_new = f'{new_val_name}_new'
    val_name_old = f'{old_val_name}_old'

    comparison = pd.concat([vector_new, vector_old], axis=1)
    comparison.columns = pd.Index([val_name_new, val_name_old])

    comparison[f'{old_val_name}_diff'] = (
        comparison[val_name_new] - comparison[val_name_old]
    )

    # Handle division by zero: replace 0 with NaN, compute ratio, replace inf with NaN, fill with 0
    comparison[f'{old_val_name}_perc_diff'] = (
        (
            comparison[f'{old_val_name}_diff']
            / comparison[val_name_old].replace(0, np.nan)
        )
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0.0)
    )

    return comparison[
        [
            val_name_new,
            val_name_old,
            f'{old_val_name}_diff',
            f'{old_val_name}_perc_diff',
        ]
    ]


def diff_and_perc_diff_two_sector_vectors(
    vector_old: pd.DataFrame,
    vector_new: pd.DataFrame,
    old_val_name: str,
    new_val_name: str,
    sector_desc: ta.Optional[ta.Dict[str, str]] = None,
) -> pd.DataFrame:
    """Compute diff/perc_diff and add human-readable sector names.

    Args:
        vector_old: Old vector values
        vector_new: New vector values
        old_val_name: Name for old value columns
        new_val_name: Name for new value columns
        sector_desc: Mapping of sector codes to descriptions.
            Defaults to ``CEDA_V7_SECTOR_DESC``.

    Returns:
        DataFrame with sector_name column prepended to diff results
    """
    if sector_desc is None:
        sector_desc = CEDA_V7_SECTOR_DESC  # type: ignore[assignment]

    comparison = diff_and_perc_diff_two_vectors(
        vector_new,
        vector_old,
        new_val_name=new_val_name,
        old_val_name=old_val_name,
    )

    existing_cols = comparison.columns.tolist()
    comparison['sector_name'] = comparison.index.map(sector_desc)

    return comparison[['sector_name'] + existing_cols]


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
            'ef_name': ef_name,
            'statistic': col,
        }
        stats['median'] = ef_comparison[col].median()
        stats['std'] = ef_comparison[col].std()
        summary_rows.append(stats)

    return pd.DataFrame(summary_rows)


def construct_ef_diff_dataframe(
    ef_name: str,
    ef_new: pd.DataFrame,
    ef_old: OldEfSet,
    sector_desc: ta.Optional[ta.Dict[str, str]] = None,
) -> pd.DataFrame:
    """Build full comparison DataFrame for emission factors.

    Compares new EFs against old versions,
    including both raw and inflation-adjusted old values.

    Args:
        ef_name: Name of the emission factor ("D" or "N")
        ef_new: New emission factor DataFrame
        ef_old: Old EF (raw + inflated)
        sector_desc: Mapping of sector codes to descriptions.
            Defaults to ``CEDA_V7_SECTOR_DESC``.

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
            sector_desc=sector_desc,
        )
        .rename(columns={f'{ef_name}_old': f'{ef_name}_old_inflated'})
        .drop(columns=[f'{ef_name}_diff'])
    )

    raw_values = ta.cast('pd.Series[float]', ef_old.raw.squeeze())
    ef_comparison.insert(
        3,
        f'{ef_name}_old',
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
    """Load and prepare all emission factor data for diagnostics.

    This function:
    1. Derives new D and N from current matrices (B and M)
    2. Loads old D and N from GCS snapshots
    3. Applies inflation adjustment to old values
    4. Packages everything into a EfsForDiagnostics object

    Returns:
        EfsForDiagnostics with new and old EF data for comparison
    """
    # Late-binding imports - these depend on global config
    from bedrock.transform.eeio.derived import (  # noqa: PLC0415
        derive_Aq_usa,
        derive_B_usa_non_finetuned,
    )
    from bedrock.utils.math.formulas import (  # noqa: PLC0415
        compute_d,
        compute_L_matrix,
        compute_M_matrix,
        compute_n,
    )

    config = get_usa_config()
    new_base_year = config.model_base_year
    B_snapshot_name: SnapshotName = 'B_USA_non_finetuned'
    Adom_snapshot_name: SnapshotName = 'Adom_USA'
    Aimp_snapshot_name: SnapshotName = 'Aimp_USA'

    t0 = time.time()
    B_new = derive_B_usa_non_finetuned()
    logger.info(
        f'[TIMING] derive_B_usa_non_finetuned completed in {time.time() - t0:.1f}s'
    )

    t0 = time.time()
    Aq_set = derive_Aq_usa()
    logger.info(f'[TIMING] derive_Aq_usa completed in {time.time() - t0:.1f}s')

    t0 = time.time()
    L_new = compute_L_matrix(A=Aq_set.Adom + Aq_set.Aimp)
    M_new = compute_M_matrix(B=B_new, L=L_new)
    D_new = compute_d(B=B_new)
    N_new = compute_n(M=M_new)
    logger.info(f'[TIMING] New L, M, D, N matrices computed in {time.time() - t0:.1f}s')

    # Uses the snapshot version specified in bedrock/utils/snapshots/.SNAPSHOT_KEY
    t0 = time.time()
    B_old = load_current_snapshot(B_snapshot_name)
    Adom_old = load_current_snapshot(Adom_snapshot_name)
    Aimp_old = load_current_snapshot(Aimp_snapshot_name)
    logger.info(f'[TIMING] Old snapshots loaded in {time.time() - t0:.1f}s')

    t0 = time.time()
    L_old = compute_L_matrix(A=Adom_old + Aimp_old)
    M_old = compute_M_matrix(B=B_old, L=L_old)
    D_old_raw = compute_d(B=B_old)
    N_old_raw = compute_n(M=M_old)
    logger.info(f'[TIMING] Old L, M, D, N matrices computed in {time.time() - t0:.1f}s')

    t0 = time.time()
    D_old_inflated = inflation_adjust_ef_denom_to_new_base_year(
        old_ef_vector=D_old_raw,
        new_base_year=new_base_year,
        old_base_year=2023,
    )
    N_old_inflated = inflation_adjust_ef_denom_to_new_base_year(
        old_ef_vector=N_old_raw,
        new_base_year=new_base_year,
        old_base_year=2023,
    )
    logger.info(f'[TIMING] Inflation adjustment completed in {time.time() - t0:.1f}s')

    return EfsForDiagnostics(
        D_new=D_new.to_frame(),
        N_new=N_new.to_frame(),
        D_old=OldEfSet(
            raw=D_old_raw.to_frame(),
            inflated=D_old_inflated.to_frame(),
        ),
        N_old=OldEfSet(
            raw=N_old_raw.to_frame(),
            inflated=N_old_inflated.to_frame(),
        ),
    )
