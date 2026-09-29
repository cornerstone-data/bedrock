"""The electricity physical-share pin (T40): the cell formula, the buyer set, and
that the interior fit holds pinned cells while still meeting both margins.

Synthetic throughout; no extract.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from bedrock.transform.iot import eia_utility_go_adjustment as eia
from bedrock.transform.iot import nowcast_interior_fit as fi
from bedrock.transform.iot import nowcast_intermediate as ni
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES

ELEC = ni.ELECTRICITY_COMMODITY
BUYERS = list(ni.PHYSICAL_SHARE_BUYERS)


def _flag(monkeypatch: pytest.MonkeyPatch, on: bool) -> None:
    config = get_usa_config().model_copy(update={'pin_electricity_physical_share': on})
    monkeypatch.setattr(ni, 'get_usa_config', lambda: config)


def test_the_flag_is_off_by_default() -> None:
    assert get_usa_config().pin_electricity_physical_share is False


def test_no_buyer_is_pinned_with_the_flag_off(monkeypatch: pytest.MonkeyPatch) -> None:
    _flag(monkeypatch, False)
    assert ni.pinned_electricity_buyers(2023) == []


def test_a_survey_observed_cell_is_never_pinned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _flag(monkeypatch, True)
    observed = pd.DataFrame(False, index=[ELEC], columns=BUYERS)
    observed.loc[ELEC, '441000'] = True
    monkeypatch.setattr(ni, 'observed_cells', lambda year: observed)
    pinned = ni.pinned_electricity_buyers(2023)
    assert '441000' not in pinned
    assert set(pinned) == set(BUYERS) - {'441000'}


def test_cells_hold_kwh_per_real_dollar_of_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Output up 50% nominal on a 25% output price rise is 20% real growth;
    with electricity 10% dearer, the cell is 1.2 x 1.1 = 1.32 x its 2017 value.
    """
    go = {
        2017: pd.Series(1000.0, index=BUYERS),
        2023: pd.Series(1500.0, index=BUYERS),
    }
    bench = pd.DataFrame(0.0, index=[ELEC], columns=BUYERS)
    bench.loc[ELEC] = 20.0
    prices = pd.DataFrame({2017: 1.0, 2023: 1.25}, index=pd.Index(BUYERS))
    monkeypatch.setattr(ni, 'gross_output', lambda year: go[year])
    monkeypatch.setattr(ni, 'benchmark_intermediate', lambda: bench)
    monkeypatch.setattr(ni, 'derive_industry_price_index', lambda: prices.copy())
    monkeypatch.setattr(
        eia, 'commercial_price_index', lambda year: {2017: 1.0, 2023: 1.1}[year]
    )
    cells = ni.electricity_physical_share_cells(2023)
    assert cells.to_numpy() == pytest.approx(np.full(len(BUYERS), 20.0 * 1.32))


def test_the_fit_holds_pinned_cells_and_meets_both_margins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rng = np.random.default_rng(5)
    shape = (len(USA_2017_COMMODITY_CODES), len(USA_2017_INDUSTRY_CODES))
    seed = pd.DataFrame(
        np.where(rng.uniform(size=shape) < 0.4, rng.uniform(1, 9, size=shape), 0.0)
        * 1e7,
        index=pd.Index(USA_2017_COMMODITY_CODES, name='commodity'),
        columns=pd.Index(USA_2017_INDUSTRY_CODES, name='industry'),
    )
    pinned = ['441000', '423A00']
    seed.loc[[ELEC], pinned] = 5e7
    row_t = seed.sum(axis=1) * 1.2
    col_t = seed.sum(axis=0) * 1.2
    monkeypatch.setattr(fi, 'interior_row_targets', lambda year: row_t)
    monkeypatch.setattr(fi, 'interior_column_targets', lambda year: col_t)
    monkeypatch.setattr(ni, 'pinned_electricity_buyers', lambda year: pinned)

    result = fi.fit_interior(2023, seed=seed)

    assert result.interior.loc[[ELEC], pinned].to_numpy() == pytest.approx(
        seed.loc[[ELEC], pinned].to_numpy(), rel=1e-12
    )
    active_cols = result.column_targets.index.difference(result.held_columns.index)
    col_miss = (result.interior.sum(axis=0) - result.column_targets)[active_cols]
    assert float(col_miss.abs().max()) < fi.TOLERANCE_USD
    # Unpinned cells in a pinned column absorb the whole column move.
    free = [c for c in USA_2017_COMMODITY_CODES if c != ELEC]
    grew = result.interior.loc[free, '441000'].sum() / seed.loc[free, '441000'].sum()
    assert float(grew) > 1.2
