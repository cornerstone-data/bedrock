"""Production home for EPA end-use mapping and EIA Table 2.4 prices (PR4).

Promoted from ``bedrock/analysis/electricity/d_85/``; analysis modules re-import from here.
"""

from __future__ import annotations

import typing as ta
from pathlib import Path

import pandas as pd

from bedrock.utils.schemas.cornerstone_schemas import (
    CORNERSTONE_COMMODITIES_ELEC,
    ELECTRICITY_DISAGG_SECTORS,
)
from bedrock.utils.taxonomy.cornerstone.final_demand import FINAL_DEMANDS

_DATA_DIR = Path(__file__).resolve().parent / 'data'
_OVERRIDES_PATH = _DATA_DIR / 'cornerstone_to_epa_end_use.csv'

END_USE_MAPPING_REVIEW_STATUS = (
    'DRAFT — pending PR-4 mapping review; not release-approved'
)

EPA_END_USES = ('Residential', 'Commercial', 'Industrial', 'Transportation')

TABLE_2_4_DESCRIPTION = 'Table 2.4 Average price of electricity to ultimate customers'
TABLE_2_4_PROVIDER = 'Total Electric Industry'

EPAEndUse = ta.Literal[
    'Residential', 'Commercial', 'Industrial', 'Transportation', 'Total'
]

# FD codes → end-use (initial draft mapping)
_FD_DEFAULTS: dict[str, str] = {
    'F01000': 'Residential',
    'F02R00': 'Residential',
    'F02E00': 'Industrial',
    'F02N00': 'Commercial',
    'F02S00': 'Commercial',
    'F03000': 'Commercial',
    'F04000': 'Commercial',
    'F05000': 'Commercial',
    'F06C00': 'Commercial',
    'F06E00': 'Commercial',
    'F06N00': 'Commercial',
    'F06S00': 'Commercial',
    'F07C00': 'Commercial',
    'F07N00': 'Commercial',
    'F07S00': 'Commercial',
    'F10C00': 'Commercial',
    'F10E00': 'Commercial',
    'F10N00': 'Commercial',
    'F10S00': 'Commercial',
}


def _naics_chapter(code: str) -> int | None:
    if len(code) < 2 or not code[:2].isdigit():
        return None
    return int(code[:2])


def classify_industry_end_use(industry_code: str) -> tuple[str, str]:
    """Rule-based EPA end-use for a Cornerstone industry code."""
    if industry_code in ELECTRICITY_DISAGG_SECTORS:
        return 'Industrial', 'electricity_child'
    chapter = _naics_chapter(industry_code)
    if chapter is None:
        return 'Commercial', 'naics_catchall'
    if chapter == 21:
        return 'Industrial', 'naics_21'
    if chapter == 23:
        return 'Industrial', 'naics_23'
    if chapter in (31, 32, 33):
        return 'Industrial', 'naics_31-33'
    if chapter == 11:
        return 'Industrial', 'naics_11'
    if chapter in (48, 49):
        return 'Transportation', 'naics_48-49'
    if 52 <= chapter <= 81:
        return 'Commercial', 'naics_52-81'
    if chapter == 22:
        return 'Industrial', 'naics_22'
    return 'Commercial', 'naics_catchall'


def build_end_use_map() -> dict[str, str]:
    """Map every Use/A column key (commodity + FD) to an EPA end-use sector."""
    mapping: dict[str, str] = {}
    for code in CORNERSTONE_COMMODITIES_ELEC:
        end_use, _rule = classify_industry_end_use(code)
        mapping[code] = end_use
    for fd in FINAL_DEMANDS:
        mapping[fd] = _FD_DEFAULTS.get(fd, 'Commercial')
    if _OVERRIDES_PATH.exists():
        overrides = pd.read_csv(_OVERRIDES_PATH)
        if len(overrides) > 0 and 'cornerstone_code' in overrides.columns:
            for _, row in overrides.iterrows():
                code = str(row['cornerstone_code']).strip()
                if code and code.lower() != 'nan':
                    mapping[code] = str(row['epa_end_use'])
    return mapping


def build_end_use_map_resolved(
    prices_by_class: dict[str, float] | None = None,
    *,
    c_col: float | None = None,
    c_row: pd.Series[float] | None = None,
) -> pd.DataFrame:
    """Resolved mapping review table (one row per industry + FD column)."""
    rows: list[dict[str, ta.Any]] = []
    for code in CORNERSTONE_COMMODITIES_ELEC:
        end_use, rule = classify_industry_end_use(code)
        price = (
            float(prices_by_class[end_use])
            if prices_by_class and end_use in prices_by_class
            else float('nan')
        )
        rel_factor = (
            float(c_row[code] / c_row.mean())
            if c_row is not None and code in c_row.index and c_row.mean() > 0
            else float('nan')
        )
        rows.append(
            {
                'cornerstone_code': code,
                'code_type': 'industry',
                'assigned_end_use': end_use,
                'assignment_rule': rule,
                'table_2_4_price': price,
                'mwh_per_usd_relative_to_mean': rel_factor,
            }
        )
    for fd in FINAL_DEMANDS:
        end_use = _FD_DEFAULTS.get(fd, 'Commercial')
        price = (
            float(prices_by_class[end_use])
            if prices_by_class and end_use in prices_by_class
            else float('nan')
        )
        rel_factor = (
            float(c_row[fd] / c_row.mean())
            if c_row is not None and fd in c_row.index and c_row.mean() > 0
            else float('nan')
        )
        rows.append(
            {
                'cornerstone_code': fd,
                'code_type': 'final_demand',
                'assigned_end_use': end_use,
                'assignment_rule': (
                    'fd_default' if fd not in _FD_DEFAULTS else 'fd_mapped'
                ),
                'table_2_4_price': price,
                'mwh_per_usd_relative_to_mean': rel_factor,
            }
        )
    out = pd.DataFrame(rows)
    out.attrs['review_status'] = END_USE_MAPPING_REVIEW_STATUS
    out.attrs['c_col'] = c_col
    return out


def _load_eia_fba(year: int) -> pd.DataFrame:
    from bedrock.extract.flowbyactivity import getFlowByActivity  # noqa: PLC0415

    return getFlowByActivity('EIA_ElectricPowerAnnual', year)


def table_2_4_prices_cents_kwh(
    year: int,
    provider: str = TABLE_2_4_PROVIDER,
    *,
    fba: pd.DataFrame | None = None,
) -> dict[EPAEndUse, float]:
    """Return end-use retail prices (cents/kWh) from Table 2.4."""
    df = fba if fba is not None else _load_eia_fba(year)
    mask = (
        (df['Year'] == year)
        & (df['Description'].str.startswith(TABLE_2_4_DESCRIPTION, na=False))
        & (df['ActivityProducedBy'] == provider)
    )
    subset = df.loc[mask]
    sectors: ta.List[EPAEndUse] = [
        'Residential',
        'Commercial',
        'Industrial',
        'Transportation',
        'Total',
    ]
    out: dict[EPAEndUse, float] = {}
    for sector in sectors:
        rows = subset.loc[subset['ActivityConsumedBy'] == sector, 'FlowAmount']
        if rows.empty:
            raise ValueError(
                f'Table 2.4 missing sector {sector!r} for year {year}, provider {provider!r}'
            )
        val = float(rows.iloc[0])
        if val <= 0:
            raise ValueError(
                f'Table 2.4 non-positive price for sector {sector!r} year {year}'
            )
        out[sector] = val
    return out
