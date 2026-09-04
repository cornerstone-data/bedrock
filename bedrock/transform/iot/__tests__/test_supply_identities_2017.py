"""Step 4e: the four Supply identities, per commodity, on the published 2017 SUT.

The gate on declaring the Supply table done (#581). ``test_nowcast_subtotals``
checks that :func:`fill_supply_bridge_subtotals` does the arithmetic; this
checks that the arithmetic is **BEA's**, cell by cell on the published table,
so a wrong component decomposition or a wrong sign convention fails here rather
than inside the balance.

⚠️ **Per commodity, never in aggregate.** ``T014`` nets to about 1 economy-wide
against 7.4 trillion of gross mass, because a trade margin is added to the good
and subtracted from the trade commodity that earned it. An aggregate check does
not merely risk passing on broken data - it passes on anything.

What is checked elsewhere
-------------------------

#581 also asks for Supply/Use and industry-output consistency. Both are already
verified, as hard targets rather than as tests here, and
``mask_layer_feasibility --check`` reproduces them on the published 2017 tables:

* ``T11`` commodity identity, Supply row against Use row - 400 margins, max 21
* ``T17`` basic-to-producer wedge - 402 margins, max 12
* ``T1`` industry gross output - 402 margins, max 13
* ``GO(producer) - T007(basic) == T00TOP - T00SUB`` per industry - max 4

Tolerances
----------

BEA publishes no cell below 1 million, so every identity here carries
publication rounding and the bar is stated in those units, not in zero. The
measured worst case is **1.00** on all four identities, with **no** commodity
off by more than 1. ``T007`` against the make block sums up to 402 industries
and reaches **10**, which is the same rounding accumulated rather than a
different kind of error.
"""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.extract.iot.io_2017 import _load_2017_detail_supply_use_usa
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES

#: One published cell's worth of rounding.
ROUNDING = 1.0

#: The same rounding accumulated across up to 402 industries.
MAKE_ROW_ROUNDING = 10.0

COMMODITIES = list(USA_2017_COMMODITY_CODES)
INDUSTRIES = list(USA_2017_INDUSTRY_CODES)


@pytest.fixture(scope='module')
def supply() -> pd.DataFrame:
    """Published 2017 detail Supply.

    ⚠️ Columns are stripped, which turns BEA's ``'TRADE '`` into ``'TRADE'``.
    The trailing space is load-bearing elsewhere in the codebase - the balance
    panel keeps it - so the name is spelled without it *only* here.
    """
    frame = _load_2017_detail_supply_use_usa('Supply_detail')
    frame.columns = frame.columns.str.strip()
    return frame


@pytest.fixture(scope='module')
def use() -> pd.DataFrame:
    frame = _load_2017_detail_supply_use_usa('Use_SUT_detail')
    frame.columns = frame.columns.str.strip()
    return frame


def _column(frame: pd.DataFrame, name: str) -> pd.Series:
    return pd.to_numeric(frame[name], errors='coerce').reindex(COMMODITIES).fillna(0.0)


def _assert_identity(residual: pd.Series, label: str, tolerance: float) -> None:
    """Fail naming the commodity and the identity, as #581 requires."""
    off = residual[residual.abs() > tolerance]
    assert off.empty, (
        f'{label} fails for {len(off)} of {len(residual)} commodities '
        f'(tolerance {tolerance}); worst: '
        + ', '.join(
            f'{code}={value:,.2f}'
            for code, value in off.reindex(off.abs().sort_values(ascending=False).index)
            .head(5)
            .items()
        )
    )


def test_t013_is_output_plus_imports(supply: pd.DataFrame) -> None:
    """``T013 = T007 + MCIF + MADJ`` — total supply at basic value."""
    residual = _column(supply, 'T013') - (
        _column(supply, 'T007') + _column(supply, 'MCIF') + _column(supply, 'MADJ')
    )
    _assert_identity(residual, 'T013 = T007 + MCIF + MADJ', ROUNDING)


def test_t014_is_the_two_margins(supply: pd.DataFrame) -> None:
    """``T014 = TRADE + TRANS`` — and it nets to ~1 economy-wide, hence per cell."""
    residual = _column(supply, 'T014') - (
        _column(supply, 'TRADE') + _column(supply, 'TRANS')
    )
    _assert_identity(residual, 'T014 = TRADE + TRANS', ROUNDING)


def test_t015_is_taxes_less_subsidies(supply: pd.DataFrame) -> None:
    """``T015 = MDTY + TOP + SUB``.

    ``SUB`` is published **negative** in the Supply table, so the identity adds
    it. Getting that sign wrong is worth ``2 x SUB`` and this is where it shows.
    """
    residual = _column(supply, 'T015') - (
        _column(supply, 'MDTY') + _column(supply, 'TOP') + _column(supply, 'SUB')
    )
    _assert_identity(residual, 'T015 = MDTY + TOP + SUB', ROUNDING)


def test_t016_is_purchaser_value(supply: pd.DataFrame) -> None:
    """``T016 = T013 + T014 + T015`` — total supply at purchaser value."""
    residual = _column(supply, 'T016') - (
        _column(supply, 'T013') + _column(supply, 'T014') + _column(supply, 'T015')
    )
    _assert_identity(residual, 'T016 = T013 + T014 + T015', ROUNDING)


def test_t007_is_the_make_block_row_margin(supply: pd.DataFrame) -> None:
    """``T007[c]`` is what every industry makes of ``c``, not a separate series."""
    made = (
        supply.loc[COMMODITIES, INDUSTRIES]
        .apply(pd.to_numeric, errors='coerce')
        .fillna(0.0)
        .sum(axis=1)
    )
    residual = made - _column(supply, 'T007')
    _assert_identity(residual, 'T007 = make block row sum', MAKE_ROW_ROUNDING)


def test_supply_purchaser_equals_total_use_per_commodity(
    supply: pd.DataFrame, use: pd.DataFrame
) -> None:
    """``T016[c] == T019[c]``, the identity Step 5 balances on.

    ⚠️ **Exact in the published tables** — the measured gap is 0 for all 402,
    not merely within rounding. So the whole of any gap a nowcast shows on this
    identity is the nowcast's own, which is what makes it a usable measure of
    how far Steps 1-4 are from consistent before the balance runs.
    """
    residual = _column(supply, 'T016') - _column(use, 'T019')
    _assert_identity(residual, 'T016 = T019 per commodity', 0.0)


def test_the_derived_bridge_reproduces_the_identities(supply: pd.DataFrame) -> None:
    """Our own bridge satisfies them too, so a refactor cannot quietly break it.

    Holds by construction today — ``fill_supply_bridge_subtotals`` computes the
    subtotals from the components — which is exactly why it is worth pinning:
    the construction is what a change would alter.
    """
    from bedrock.transform.iot.nowcast import (  # noqa: PLC0415
        derive_initial_supply_bridge,
    )

    bridge = derive_initial_supply_bridge(2017)
    t013 = bridge['T013'] - (bridge['T007'] + bridge['MCIF'] + bridge['MADJ'])
    t014 = bridge['T014'] - (bridge['TRADE'] + bridge['TRANS'])
    t015 = bridge['T015'] - (bridge['MDTY'] + bridge['TOP'] + bridge['SUB'])
    t016 = bridge['T016'] - (bridge['T013'] + bridge['T014'] + bridge['T015'])
    for residual, label in (
        (t013, 'T013'),
        (t014, 'T014'),
        (t015, 'T015'),
        (t016, 'T016'),
    ):
        finite = residual.dropna()
        assert not finite.empty, f'{label} is entirely NaN; components unsourced'
        assert finite.abs().max() < 1.0, (
            f'derived {label} does not reproduce its components: '
            f'max {finite.abs().max():,.4f}'
        )
