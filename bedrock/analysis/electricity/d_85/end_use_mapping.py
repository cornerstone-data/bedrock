"""Cornerstone industry / FD → EPA Table 2.4 end-use sector mapping."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from bedrock.utils.schemas.cornerstone_schemas import (
    CORNERSTONE_INDUSTRIES_ELEC,
    ELECTRICITY_DISAGG_SECTORS,
)
from bedrock.utils.taxonomy.cornerstone.final_demand import FINAL_DEMANDS

_DATA_DIR = Path(__file__).resolve().parent / 'data'
_OVERRIDES_PATH = _DATA_DIR / 'cornerstone_to_epa_end_use.csv'

EPA_END_USES = ('Residential', 'Commercial', 'Industrial', 'Transportation')

# FD codes → end-use (initial draft per plan §5.1)
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
    'F07E00': 'Commercial',
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


def classify_industry_end_use(industry_code: str) -> str:
    """Rule-based EPA end-use for a Cornerstone industry code."""
    if industry_code in ELECTRICITY_DISAGG_SECTORS:
        return 'Industrial'
    chapter = _naics_chapter(industry_code)
    if chapter is None:
        return 'Commercial'
    if chapter == 21:
        return 'Industrial'
    if chapter == 23:
        return 'Industrial'
    if chapter in (31, 32, 33):
        return 'Industrial'
    if chapter == 11:
        return 'Industrial'
    if chapter in (48, 49):
        return 'Transportation'
    if 52 <= chapter <= 81:
        return 'Commercial'
    if chapter == 22:
        return 'Industrial'
    return 'Commercial'


def build_end_use_map() -> dict[str, str]:
    """Map every Use column key (industry + FD) to an EPA end-use sector."""
    mapping: dict[str, str] = {}
    for code in CORNERSTONE_INDUSTRIES_ELEC:
        if code in ELECTRICITY_DISAGG_SECTORS:
            continue
        mapping[code] = classify_industry_end_use(code)
    for fd in FINAL_DEMANDS:
        mapping[fd] = _FD_DEFAULTS.get(fd, 'Commercial')
    if _OVERRIDES_PATH.exists():
        overrides = pd.read_csv(_OVERRIDES_PATH)
        for _, row in overrides.iterrows():
            mapping[str(row['cornerstone_code'])] = str(row['epa_end_use'])
    return mapping


def build_price_tilt_weights_by_column(
    w_base: pd.Series[float],
    prices: dict[str, float],
    end_use_map: dict[str, str],
    columns: list[str],
) -> pd.DataFrame:
    """Build per-column 221110/121/122 weights from Table 2.4 price tilt."""
    p_ref = prices['Total']
    tilt = {'221110': -1.0, '221121': 0.5, '221122': 0.5}
    w = w_base.reindex(list(ELECTRICITY_DISAGG_SECTORS)).astype(float)
    out = pd.DataFrame(
        index=list(ELECTRICITY_DISAGG_SECTORS), columns=columns, dtype=float
    )
    for col in columns:
        eu = end_use_map.get(str(col), 'Commercial')
        p_e = prices.get(eu, p_ref)
        price_factor = p_e / p_ref - 1.0
        raw = w * pd.Series(
            {k: 1.0 + price_factor * tilt[k] for k in w.index},
            dtype=float,
        )
        total = float(raw.sum())
        out[col] = raw / total if total else w
    return out


def write_default_overrides_csv() -> Path:
    """Write initial override CSV (electricity children + sample FD)."""
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            'cornerstone_code': c,
            'code_type': 'industry',
            'epa_end_use': 'Industrial',
            'mapping_rule': 'electricity_child',
            'notes': '',
        }
        for c in ELECTRICITY_DISAGG_SECTORS
    ]
    pd.DataFrame(rows).to_csv(_OVERRIDES_PATH, index=False)
    return _OVERRIDES_PATH
