"""Classify CH4 as fossil vs non-fossil for AR6 GWP selection.

Inventory sources publish undifferentiated Methane. Before applying
``GWP100_AR6_CEDA``, biogenic rows (agriculture, landfills, wastewater, ...)
must use ``CH4_non_fossil`` (27.0) rather than ``CH4_fossil`` (29.8).

Classification keys on inventory **table family** and model **sector**, not
on FlowSA ``activity_sets`` names (the MetaSources suffix after the first
``.``), which are unstable method-config labels. Expected activities and
typical activity_set names are listed next to each rule for human tracking
only -- they are not match keys.

Longer-term, tag biogenic CH4 at FBS creation so consumers need not
reclassify from MetaSources.
"""

from __future__ import annotations

import re

import pandas as pd

# National totals (EPA Table 2-1 / UMD Table 2-S1): biogenic CH4 only on
# agriculture (1*), solid waste (562*), and wastewater utilities (2213*).
# End-anchored so T_2_10 does not match T_2_1.
#
# Expected PrimaryActivity labels on the ``direct`` activity_set (Cornerstone
# GHG methods) that this rule is meant to cover -- not matched by name:
#   562*: Landfills, Composting, Anaerobic Digestion at Biogas Facilities,
#         Incineration of Waste
#   2213*: Wastewater Treatment
#   1*: Rice Cultivation (and Field Burning when attributed via T_2_S1)
# Other CH4 on these tables (e.g. Coal Mining, Abandoned Oil and Gas Wells)
# lands on non-matching sectors and stays fossil.
# Typical MetaSources suffixes seen today: ``.direct`` (ignored by the stem
# parser). Do not treat ``.electric_power`` / other activity_set names as keys.
_NATIONAL_TOTALS_STEM = re.compile(r'(?:^|_)T_2_(?:1|S1)$')

# Agriculture chapter tables (EPA/UMD T_5_*): all CH4 is treated as biogenic.
# Expected coverage (activity_set / PrimaryActivity vary by method year):
#   T_5_3  enteric fermentation (often animal species as PrimaryActivity;
#          activity_set commonly ``animals``)
#   T_5_6  manure management (same animal list / ``animals`` set)
#   T_5_10 / T_5_15 / nested EPA T_5_18 / T_5_19 -- soils / field-burning
#          paths; any CH4 there inherits non-fossil GWP
# Typical MetaSources suffixes: ``.animals``, ``.direct`` (ignored).
_AG_CHAPTER_STEM = re.compile(r'_T_5_')
_BIOGENIC_SECTOR = re.compile(r'^(1|562|2213)')


def inventory_table_stem(meta_sources: str) -> str:
    """Drop the FlowSA activity_set suffix from a MetaSources value."""
    return str(meta_sources).split('.', 1)[0]


def ch4_non_fossil_mask(
    meta_sources: pd.Series,
    sector: pd.Series,
) -> pd.Series:
    """True where CH4 should use the non-fossil (biogenic) AR6 GWP."""
    stem = meta_sources.astype(str).map(inventory_table_stem)
    is_ag = stem.str.contains(_AG_CHAPTER_STEM.pattern, regex=True, na=False)
    is_national = stem.str.contains(_NATIONAL_TOTALS_STEM.pattern, regex=True, na=False)
    sector_ok = sector.astype(str).str.match(_BIOGENIC_SECTOR.pattern, na=False)
    return is_ag | (is_national & sector_ok)


def apply_ch4_non_fossil_flowable(
    df: pd.DataFrame,
    *,
    flowable_col: str = 'Flowable',
    meta_col: str = 'MetaSources',
    sector_col: str = 'SectorProducedBy',
) -> None:
    """In-place: remap ``CH4_fossil`` → ``CH4_non_fossil`` where biogenic."""
    mask = ch4_non_fossil_mask(df[meta_col], df[sector_col]) & (
        df[flowable_col] == 'CH4_fossil'
    )
    df.loc[mask, flowable_col] = 'CH4_non_fossil'
