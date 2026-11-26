"""US final demand comes from the nowcast's own year, split by its Import table."""

from __future__ import annotations

from collections.abc import Iterator

import pandas as pd
import pytest

from bedrock.extract.iot import nowcast_mut_storage
from bedrock.transform.eeio import usa_matrices as m
from bedrock.utils.config.usa_config import (
    USAConfig,
    reset_usa_config,
    set_global_usa_config_object,
)
from bedrock.utils.schemas.single_region_types import (
    SingleRegionYtotAndTradeVectorSet,
)
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_final_demand import (
    USA_2017_FINAL_DEMAND_CODES,
)

BEA = ['a', 'b', 'c']
SECTORS = ['a1', 'a2', 'b', 'new']


@pytest.fixture(autouse=True)
def _nowcast() -> Iterator[None]:
    reset_usa_config(should_reset_env_var=True)
    config = USAConfig(
        usa_detail_io_source='nowcast',
        usa_base_io_data_year=2019,
        usa_ghg_data_year=2019,
        model_base_year=2019,
    )
    set_global_usa_config_object(config, source_label='test')
    yield
    reset_usa_config(should_reset_env_var=True)


@pytest.fixture
def _tables(monkeypatch: pytest.MonkeyPatch) -> None:
    used = pd.DataFrame(
        {'F01000': [80.0, 50.0, 5.0], 'F04000': [7.0] * 3, 'F05000': [-9.0] * 3},
        index=BEA,
    )
    imported = pd.DataFrame({'F01000': [20.0, 60.0, 0.0]}, index=BEA)
    corresp = pd.DataFrame(
        [[0.5, 0.0, 0.0], [0.5, 0.0, 0.0], [0.0, 1.0, 1.0]],
        index=['a1', 'a2', 'b'],
        columns=BEA,
    )
    ytot = pd.Series([60.0, 20.0, 55.0, 3.0], index=SECTORS)
    yset = SingleRegionYtotAndTradeVectorSet(
        ytot=ytot, exports=ytot * 0.1, imports=ytot * 0.2
    )
    monkeypatch.setattr(m, 'load_detail_Ytot_usa', lambda: used)
    monkeypatch.setattr(m, 'load_nowcast_detail_Yimp_usa', lambda: imported)
    monkeypatch.setattr(m, 'commodity_corresp', lambda: corresp)
    monkeypatch.setattr(m, 'derive_Ytot_matrix_set', lambda: yset)
    monkeypatch.setattr(
        m, 'inflate_q_or_y_with_industry_pi', lambda v, original_year, target_year: v
    )


@pytest.mark.usefixtures('_tables')
def test_ydom_and_yimp_split_the_nowcast_years_final_demand() -> None:
    y = m.derive_ydom_and_yimp()
    # 'a' imports 20 of 80 and passes its share to both of its sectors.
    assert y.yimp.loc[['a1', 'a2']].tolist() == [15.0, 5.0]
    # 'b' pools b and c, whose imports (60) exceed their use (55): the share is capped.
    assert y.yimp['b'] == 55.0
    # A sector no BEA commodity maps to is not imported.
    assert y.yimp['new'] == 0.0
    assert (y.ydom + y.yimp).tolist() == [60.0, 20.0, 55.0, 3.0]


@pytest.mark.usefixtures('_tables')
def test_ytot_and_trade_are_the_nowcasts_own() -> None:
    y = m.derive_Y_and_trade_scaled()
    assert y.ytot.sum() == 138.0
    assert y.exports.sum() == pytest.approx(13.8)
    assert y.imports.sum() == pytest.approx(27.6)


def test_the_summary_tables_are_not_read(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*args: object) -> None:
        raise AssertionError('nowcast final demand must not read summary tables')

    monkeypatch.setattr(m, 'derive_summary_Ytot_usa_matrix_set', refuse)
    monkeypatch.setattr(m, 'derive_summary_Yimp_usa', refuse)
    monkeypatch.setattr(m, '_derive_nowcast_ydom_and_yimp', lambda: 'nowcast')
    assert m.derive_ydom_and_yimp() == 'nowcast'


def test_yimp_view_drops_trade_and_fills_commodities_not_imported() -> None:
    commodity = list(USA_2017_COMMODITY_CODES)[0]
    stored = pd.DataFrame(1.0, index=[commodity], columns=USA_2017_FINAL_DEMAND_CODES)
    view = nowcast_mut_storage._as_yimp_view(stored)
    assert not {'F04000', 'F05000'} & set(view.columns)
    assert view.sum().sum() == len(USA_2017_FINAL_DEMAND_CODES) - 2
    assert len(view) == len(USA_2017_COMMODITY_CODES)
