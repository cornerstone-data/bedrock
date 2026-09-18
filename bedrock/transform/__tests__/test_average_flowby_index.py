"""Contract for ``average_flowby``'s ``index_source`` (#914).

eGRID has no 2017, so 2017 is built from the 2016 and 2018 inventories. A plain
average of the two assumes 2017 sits on the line between them; the GHG inventory
says it does not -- electric power CO2 fell 4.3% into 2017 and rose 1.2% out of
it, so 2017 is *below both* neighbours and no average of them can reach it.
``index_source`` moves each source year onto the target with the inventory's own
year-over-year ratio first.

These pin the two things that would go wrong silently: the scaling being dropped
(leaving the old straight average, 2.8% high on 2017) and a source year with no
``year`` to index from, which would otherwise be averaged in unscaled.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from bedrock.transform import flowbyclean
from bedrock.transform.flowbysector import FlowBySector

# Electric power CO2 in Mt: eGRID for the two published years, UMD GHGIA
# table 2-S1 for the index. 2017 is a trough in the index, not a midpoint.
_EGRID = {2016: 1845.658, 2018: 1790.875}
_GHGI = {2016: 1808.871516, 2017: 1732.032401, 2018: 1753.432368}


def _fbs(year: int, amount: float, *, record_year: bool = True) -> FlowBySector:
    return FlowBySector(
        pd.DataFrame(
            [
                {
                    'Flowable': 'Carbon dioxide',
                    'Class': 'Chemicals',
                    'SectorProducedBy': '221112',
                    'SectorConsumedBy': pd.NA,
                    'SectorSourceName': 'NAICS_2017_Code',
                    'Context': 'emission/air',
                    'Location': '00000',
                    'LocationSystem': 'FIPS_2015',
                    'FlowAmount': amount,
                    'Unit': 'Mt',
                    'FlowType': 'ELEMENTARY_FLOW',
                    'Year': year,
                    'MetaSources': 'EPA_eGRID_electric',
                    'DataReliability': 5.0,
                    'TemporalCorrelation': 5.0,
                    'GeographicalCorrelation': 5.0,
                    'TechnologicalCorrelation': 5.0,
                    'DataCollection': 5.0,
                }
            ]
        ),
        config={'year': year} if record_year else {},
        full_name='EPA_eGRID_electric',
    )


class _Loaded:
    """Stands in for the object ``get_flowby_from_config`` returns."""

    def __init__(self, fbs: FlowBySector) -> None:
        self._fbs = fbs

    def prepare_fbs(self, **_kwargs: Any) -> FlowBySector:
        return self._fbs


@pytest.fixture
def stub_sources(monkeypatch: pytest.MonkeyPatch) -> None:
    def _get_flowby_from_config(
        name: str, config: dict[str, Any], **_kwargs: Any
    ) -> _Loaded:
        if config.get('year') is None:  # datasource that lost its year
            return _Loaded(_fbs(2018, _EGRID[2018], record_year=False))
        year = int(config['year'])
        return _Loaded(_fbs(year, _EGRID[year]))

    def _index_series_total(
        name: str, overrides: dict[str, Any] | None, year: int, *_args: Any
    ) -> float:
        return _GHGI[year]

    monkeypatch.setattr(flowbyclean, 'get_flowby_from_config', _get_flowby_from_config)
    monkeypatch.setattr(flowbyclean, '_index_series_total', _index_series_total)


def _config(*, indexed: bool, drop_year_on_second: bool = False) -> dict[str, Any]:
    second: dict[str, Any] = {'year': 2018}
    if drop_year_on_second:
        second = {}
    config: dict[str, Any] = {
        'year': 2017,
        'datasource_1': {'EPA_eGRID_electric': {'year': 2016}},
        'datasource_2': {'EPA_eGRID_electric': second},
    }
    if indexed:
        config['index_source'] = {'UMD_GHGIA_T_2_S1': {}}
    return config


def test_index_moves_each_source_year_onto_the_target(stub_sources: None) -> None:
    """Each source is scaled by the index's own ratio, then averaged.

    Failure mode this catches: the scaling silently not applying, which returns
    the straight average of 1818.3 Mt -- 2.8% above what the inventory implies
    for 2017, and enough to turn the observed 2017->2018 rise into a fall.
    """
    out = flowbyclean.average_flowby(
        config=_config(indexed=True), full_name='EPA_eGRID_electric'
    )
    from_2016 = _EGRID[2016] * _GHGI[2017] / _GHGI[2016]
    from_2018 = _EGRID[2018] * _GHGI[2017] / _GHGI[2018]
    assert out['FlowAmount'].sum() == pytest.approx((from_2016 + from_2018) / 2)
    assert out['FlowAmount'].sum() == pytest.approx(1768.14, abs=0.01)
    assert out['Year'].unique().tolist() == [2017]


def test_indexed_target_can_sit_below_every_source(stub_sources: None) -> None:
    """The whole point: 2017 is a trough, so it is outside the source range.

    A plain average is bounded by its inputs and lands between them; only the
    indexed one can put 2017 below both 2016 and 2018, which is where the
    inventory has it.
    """
    indexed = flowbyclean.average_flowby(
        config=_config(indexed=True), full_name='EPA_eGRID_electric'
    )['FlowAmount'].sum()
    plain = flowbyclean.average_flowby(
        config=_config(indexed=False), full_name='EPA_eGRID_electric'
    )['FlowAmount'].sum()
    assert indexed < min(_EGRID.values())
    assert min(_EGRID.values()) < plain < max(_EGRID.values())
    assert plain == pytest.approx(sum(_EGRID.values()) / 2)


def test_source_without_a_year_cannot_be_indexed(stub_sources: None) -> None:
    """Refuse rather than average an un-indexed source in with the scaled ones.

    Failure mode this catches: a datasource that lost its ``year`` quietly
    entering the mean at its own year's level.
    """
    with pytest.raises(ValueError, match='needs a `year` on every datasource'):
        flowbyclean.average_flowby(
            config=_config(indexed=True, drop_year_on_second=True),
            full_name='EPA_eGRID_electric',
        )
