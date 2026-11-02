"""Census trade product codes onto one NAICS assignment for every year (#1174).

Census assigns each 10-digit product code (Schedule B for exports, HTS for
imports) to one NAICS code and publishes the assignment yearly
(``expconcord{yy}``, ``impconcord{yy}``). For both directions the 2017-2023
files carry one assignment and the **2024** file a revised one, and Census
compiled its ``exports/naics`` and ``imports/naics`` series for 2022 onward on
the revised one. Read directly, those series change classification between
2021 and 2022: ``332996`` fabricated pipe and fittings has no trade before 2022
and 2.2bn USD of exports and 3.8bn of imports after, taken from ``332919``;
``335929`` communication wire exports fall from 6.3bn to 1.6bn and imports
from 10.4bn to 2.5bn.

The extract instead reads trade by product code and assigns every year on the
2024 file, so 2017-2021 are back-cast onto the classification of the later
years. A code the 2024 file does not list takes, in order:

1. the 2024 NAICS mix of its **successor codes**: codes in the same HS6 that
   trade in 2024 but not in the year being assigned. This is what a code that
   Census split or renumbered became;
2. the 2024 NAICS mix of **its HS6**;
3. the 2024 NAICS mix of the **HS 2022 headings that replaced its HS6**, from
   the WCO HS 2017 to HS 2022 correlation, for an HS6 the 2022 revision removed;
4. its **own year's** concordance;
5. ``990000`` for the ``988`` low-value and estimated export shipments, which
   no concordance lists and which Census places there in ``exports/naics``.

Rules 1 and 3 matter. Without them about 55bn USD a year of 2017-2021 exports
stays on the old assignment, because in 2022 Census replaced several large
"not elsewhere specified" codes and assigned the replacements differently from
the codes that survived; ``335210`` small appliances exports then still fall
from 6.4bn to 2.1bn between 2021 and 2022.
"""

from __future__ import annotations

import io
import logging
import os
import typing as ta
from collections.abc import Mapping, Sequence

import pandas as pd
import requests

log = logging.getLogger(__name__)

Flow = ta.Literal['exports', 'imports']

#: The concordance every year is assigned on.
CURRENT_CONCORDANCE_YEAR = 2024
CONCORDANCE_YEARS = tuple(range(2017, CURRENT_CONCORDANCE_YEAR + 1))

LOW_VALUE_PREFIX = '988'
LOW_VALUE_NAICS = '990000'

HS_CORRELATION_CSV = 'HS2022_HS2017_correlation.csv'

_CONCORDANCE_PREFIX: dict[Flow, str] = {'exports': 'exp', 'imports': 'imp'}
_CONCORDANCE_URL = (
    'https://www.census.gov/foreign-trade/reference/codes/concordance/'
    '{prefix}concord{yy:02d}.{ext}'
)
#: census.gov and unstats.un.org refuse the default python user agent (403).
_HEADERS = {'User-Agent': 'Mozilla/5.0'}
_HS_CORRELATION_URL = (
    'https://unstats.un.org/unsd/classifications/Econ/tables/'
    'HS2022toHS2017ConversionAndCorrelationTables.xlsx'
)

RULES = (
    'current concordance',
    'successor codes',
    'HS6 mix',
    'successor HS6',
    'own-year concordance',
    'low-value shipments',
    'unassigned',
)


def concordance_csv(flow: str) -> str:
    """The cached concordance file name for *flow*."""
    return f'Census_USATrade_{flow[:-1]}_concordance.csv'


def assign_trade_to_naics(
    values: pd.Series,
    *,
    own: pd.Series,
    current: pd.Series,
    reference: pd.Series,
    hs6_successors: Mapping[str, Sequence[str]],
) -> tuple[pd.Series, pd.Series]:
    """One year's trade by product code, summed onto the current NAICS codes.

    *values* is the year's trade by 10-digit product code. *own* and *current*
    map product code to NAICS: the year's own concordance and the current one.
    *reference* is trade by product code in the current concordance's year; its
    mix decides where unlisted codes go. *hs6_successors* maps an HS 2017
    heading to the HS 2022 headings that replaced it.

    Returns the NAICS totals and the value each rule assigned (see the module
    docstring). Value no rule can place is reported as ``unassigned`` and left
    out of the totals.
    """
    values = values[values != 0]
    codes = values.index.astype(str)
    listed = codes.isin(current.index)
    parts = [values[listed].groupby(current.reindex(codes[listed]).to_numpy()).sum()]
    by_rule = dict.fromkeys(RULES, 0.0)
    by_rule['current concordance'] = float(values[listed].sum())

    ref = reference[reference.index.isin(current.index) & (reference > 0)]
    ref_naics = current.reindex(ref.index).to_numpy()
    ref_hs6 = ref.index.astype(str).str[:6]
    traded = ref.index.isin(codes)

    def mix(mask: ta.Any) -> pd.Series | None:
        if not mask.any():
            return None
        shares = ref[mask].groupby(ref_naics[mask]).sum()
        return shares / shares.sum()

    for code in codes[~listed]:
        value = float(values[code])
        hs6 = code[:6]
        same = ref_hs6 == hs6
        steps = (
            ('successor codes', same & ~traded),
            ('HS6 mix', same),
            ('successor HS6', ref_hs6.isin(list(hs6_successors.get(hs6, ())))),
        )
        for rule, mask in steps:
            shares = mix(mask)
            if shares is not None:
                break
        else:
            if code in own.index:
                rule, shares = 'own-year concordance', pd.Series({own[code]: 1.0})
            elif code.startswith(LOW_VALUE_PREFIX):
                rule, shares = 'low-value shipments', pd.Series({LOW_VALUE_NAICS: 1.0})
            else:
                by_rule['unassigned'] += value
                continue
        by_rule[rule] += value
        parts.append(shares * value)

    totals = pd.concat(parts).groupby(level=0).sum()
    return totals, pd.Series(by_rule)


def _download(url: str) -> bytes:
    resp = requests.get(url, headers=_HEADERS, timeout=120)
    resp.raise_for_status()
    return resp.content


def _read_concordance_workbook(content: bytes) -> pd.DataFrame:
    """One concordance workbook as product code and NAICS, both strings.

    The 2023 export file does not open with openpyxl (invalid XML), so the
    calamine reader is the fallback.
    """
    try:
        frame = pd.read_excel(io.BytesIO(content), dtype=str)
    except Exception:  # noqa: BLE001 - openpyxl rejects the 2023 export workbook
        frame = pd.read_excel(io.BytesIO(content), dtype=str, engine='calamine')
    frame.columns = [str(c).strip().lower() for c in frame.columns]
    return frame[['commodity', 'naics']].dropna()


def refresh_concordances(out_dir: str, flow: Flow) -> str:
    """Download the 2017-2024 concordances for *flow* into one CSV in *out_dir*."""
    frames = []
    for year in CONCORDANCE_YEARS:
        url = _CONCORDANCE_URL.format(
            prefix=_CONCORDANCE_PREFIX[flow],
            yy=year % 100,
            ext='xls' if year <= 2019 else 'xlsx',
        )
        frames.append(_read_concordance_workbook(_download(url)).assign(year=year))
    out = os.path.join(out_dir, concordance_csv(flow))
    pd.concat(frames)[['year', 'commodity', 'naics']].to_csv(out, index=False)
    return out


def refresh_hs_correlation(out_dir: str) -> str:
    """Download the WCO HS 2022 to HS 2017 correlation into a CSV in *out_dir*."""
    frame = pd.read_excel(
        io.BytesIO(_download(_HS_CORRELATION_URL)),
        sheet_name='HS2022-HS2017 Correlations',
        dtype=str,
        skiprows=1,
    )
    frame = frame.iloc[:, :2]
    frame.columns = ['hs2022', 'hs2017']
    out = os.path.join(out_dir, HS_CORRELATION_CSV)
    frame.dropna().to_csv(out, index=False)
    return out


def concordance_by_year(frame: pd.DataFrame) -> dict[int, pd.Series]:
    """The stacked concordance CSV as product code -> NAICS, per year."""
    frame = frame.astype({'year': int, 'commodity': str, 'naics': str})
    return {
        year: frame.loc[frame['year'] == year].set_index('commodity')['naics']
        for year in sorted(int(y) for y in frame['year'].unique())
    }


def hs6_successors(frame: pd.DataFrame) -> dict[str, list[str]]:
    """The correlation CSV as HS 2017 heading -> the HS 2022 headings that replaced it."""
    frame = frame.astype(str)
    return {
        str(hs2017): sorted(set(group['hs2022']))
        for hs2017, group in frame.groupby('hs2017')
    }
