"""Trade electricity seed for #899 — annual index on WHOLESALE ∪ RETAIL.

Production overlay for commodity ``221100`` × trade seed-set industries only.
Does **not** wire BES ``trade_seed`` (holdout no-go). Candidate A (default) is a
uniform EPA Table 2.3 commercial revenue index; Candidate B is QCEW payroll
(grade / fallback only).

::

    from bedrock.analysis.nowcasting.trade_electricity_seed import (
        trade_electricity_seed,
        trade_seed_set,
    )

Shape matches :func:`~bedrock.analysis.nowcasting.utilities_expense_seed.utilities_seed`:
``commodity x industry`` in **$M**, only row ``221100`` filled on the seed-set
columns. Cell-wise index only — :func:`~bedrock.transform.iot.nowcast_intermediate.apply_column_control`
owns column totals.
"""

from __future__ import annotations

import math
import typing as ta

import pandas as pd

ELECTRICITY_ROW = '221100'
Candidate = ta.Literal['uniform_eia_commercial', 'qcew_payroll']


def trade_seed_set() -> frozenset[str]:
    """BEA detail industries in the production trade electricity seed set."""
    from bedrock.analysis.nowcasting.trade_expense_supplement import (  # noqa: PLC0415
        RETAIL,
        WHOLESALE,
    )

    return (frozenset(WHOLESALE) | frozenset(RETAIL)) - frozenset({'4200ID'})


def _use_2017_detail() -> pd.DataFrame:
    """2017 benchmark detail Use intermediate block, commodity x industry, $M."""
    from bedrock.analysis.nowcasting.inputs_structure import (  # noqa: PLC0415
        _use_2017_detail as use,
    )

    return use()


def _commercial_revenue_bn(year: int) -> float:
    from bedrock.analysis.electricity.current.eia_gtd.electricity_row_control import (  # noqa: PLC0415
        eia_epa_table_2_3_revenue_bn,
    )

    _, commercial, _, _ = eia_epa_table_2_3_revenue_bn(year)
    if math.isnan(commercial):
        raise ValueError(f'EPA Table 2.3 commercial revenue is NaN for {year}')
    return float(commercial)


def _uniform_commercial_growth(year: int, base_year: int) -> float:
    commercial_t = _commercial_revenue_bn(year)
    commercial_0 = _commercial_revenue_bn(base_year)
    if commercial_0 == 0.0:
        raise ValueError(
            f'EPA Table 2.3 commercial revenue is 0 for base_year={base_year}'
        )
    return commercial_t / commercial_0


def qcew_detail_payroll(year: int, *, peer_year: int = 2017) -> pd.Series:
    """QCEW payroll by BEA detail ($M), vintage-consistent vs ``peer_year``.

    Uses the production :func:`~bedrock.transform.nipa.compensation_movement.qcew_national_payroll`
    loader (GCS / generate), not the holdout's local-parquet glob. Shared NAICS
    are the intersection of ``year`` and ``peer_year`` with the unambiguous
    crosswalk — same discipline as the holdout, without requiring QCEW 2012.
    """
    from bedrock.transform.nipa.compensation_movement import (  # noqa: PLC0415
        naics_to_detail,
        qcew_national_payroll,
    )
    from bedrock.utils.taxonomy.bea.v2017_industry import (  # noqa: PLC0415
        USA_2017_INDUSTRY_CODES,
    )

    mapping = naics_to_detail()
    pay_y = qcew_national_payroll(year)
    pay_peer = qcew_national_payroll(peer_year)
    shared = sorted(set(pay_y.index) & set(pay_peer.index) & set(mapping))
    payroll = pay_y.reindex(shared).fillna(0.0)
    rolled = payroll.groupby(pd.Series({n: mapping[n] for n in shared})).sum()
    return rolled.reindex(list(USA_2017_INDUSTRY_CODES)).fillna(0.0)


def _payroll_growth(
    year: int, base_year: int, industries: frozenset[str]
) -> tuple[pd.Series, int]:
    """Per-industry payroll index; missing/zero base → hold 1.0 and count."""
    pay_t = qcew_detail_payroll(year, peer_year=base_year)
    pay_0 = qcew_detail_payroll(base_year, peer_year=base_year)
    growth = pd.Series(1.0, index=sorted(industries), dtype=float)
    held = 0
    for industry in industries:
        base = float(pay_0.get(industry, 0.0) or 0.0)
        target = float(pay_t.get(industry, 0.0) or 0.0)
        if base <= 0.0 or math.isnan(base) or math.isnan(target):
            held += 1
            growth[industry] = 1.0
        else:
            growth[industry] = target / base
    return growth, held


def trade_electricity_seed(
    year: int,
    base_year: int = 2017,
    *,
    candidate: Candidate = 'uniform_eia_commercial',
) -> pd.DataFrame:
    """BEA 2017 trade×electricity cells moved on an annual observed index ($M).

    Returns a frame with **only** row ``221100`` and columns in the trade
    seed-set (other cells absent). Default ``candidate`` is A (uniform EPA
    commercial). Pass ``qcew_payroll`` for Candidate B.

    Raises on missing/NaN EPA commercial, zero commercial base, empty seed-set,
    or zero/NaN ``Use2017[221100, i]`` for a claimed industry.
    """
    industries = trade_seed_set()
    if not industries:
        raise ValueError('trade electricity seed-set is empty after 4200ID filter')

    use = _use_2017_detail()
    if ELECTRICITY_ROW not in use.index:
        raise ValueError(f'{ELECTRICITY_ROW} missing from 2017 Use detail')

    columns = [c for c in sorted(industries) if c in use.columns]
    missing_cols = sorted(industries - set(columns))
    if missing_cols:
        raise ValueError(
            f'trade seed-set industries missing from Use2017 columns: {missing_cols}'
        )

    base_row = use.loc[ELECTRICITY_ROW].reindex(columns).astype(float)
    bad = [
        c
        for c in columns
        if math.isnan(float(base_row[c])) or float(base_row[c]) == 0.0
    ]
    if bad:
        raise ValueError(
            f'Use2017[{ELECTRICITY_ROW}, i] zero or NaN for seed-set industries: {bad}'
        )

    seed = pd.DataFrame(0.0, index=[ELECTRICITY_ROW], columns=columns, dtype=float)

    if candidate == 'uniform_eia_commercial':
        g = _uniform_commercial_growth(year, base_year)
        seed.loc[ELECTRICITY_ROW] = base_row.to_numpy() * g
    elif candidate == 'qcew_payroll':
        growth, _held = _payroll_growth(year, base_year, frozenset(columns))
        seed.loc[ELECTRICITY_ROW] = base_row.mul(growth.reindex(columns)).to_numpy()
    else:
        raise ValueError(
            f'unknown candidate {candidate!r}; '
            "expected 'uniform_eia_commercial' or 'qcew_payroll'"
        )

    seed.index.name = 'commodity'
    seed.columns.name = 'industry'
    return seed


def held_missing_qcew_count(year: int, base_year: int = 2017) -> int:
    """Count of seed-set industries held at 2017 for Candidate B (missing QCEW)."""
    columns = frozenset(trade_seed_set())
    _growth, held = _payroll_growth(year, base_year, columns)
    return held
