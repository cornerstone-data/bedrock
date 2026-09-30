"""interpolate_census_years blends two census years for a method year between them (#934)."""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from bedrock.extract.flowbyactivity import FlowByActivity
from bedrock.transform import flowbyclean


def _fba(year: int, amounts: dict[str, float], **config: Any) -> FlowByActivity:
    df = pd.DataFrame(
        {
            'Class': 'Land',
            'SourceName': 'USDA_CoA_Cropland_NAICS',
            'Flowable': 'AG LAND, CROPLAND, HARVESTED',
            'Unit': 'ACRES',
            'ActivityConsumedBy': list(amounts),
            'Location': '00000',
            'Year': year,
            'FlowAmount': list(amounts.values()),
        }
    )
    return FlowByActivity(
        df, full_name='USDA_CoA_Cropland_NAICS', config={'year': year, **config}
    )


class _Prepared(FlowByActivity):
    """The other census year, with the preparation chain as a no-op."""

    def function_socket(self, *_: Any, **__: Any) -> _Prepared:
        return self

    def select_by_fields(self, *_: Any, **__: Any) -> _Prepared:
        return self

    def convert_units_and_flows(self) -> _Prepared:
        return self


def _patch_other(
    monkeypatch: pytest.MonkeyPatch, year: int, amounts: dict[str, float]
) -> None:
    other = _Prepared(pd.DataFrame(_fba(year, amounts)))

    def fake_return_fba(**kwargs: Any) -> _Prepared:
        assert kwargs['year'] == year
        assert 'clean_fba' not in kwargs['config']
        return other

    monkeypatch.setattr(FlowByActivity, 'return_FBA', staticmethod(fake_return_fba))


def _settings(target: int) -> dict[str, Any]:
    return {'census_interpolation': {'years': [2017, 2022], 'target_year': target}}


def test_between_censuses_blends_linearly(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_other(monkeypatch, 2022, {'111150': 200.0, '111199': 50.0})
    fba = _fba(2017, {'111150': 100.0, '111160': 40.0}, **_settings(2020))
    out = pd.DataFrame(flowbyclean.interpolate_census_years(fba))
    by_code = out.groupby('ActivityConsumedBy')['FlowAmount'].sum()
    # 2020 is 3/5 of the way from 2017 to 2022.
    assert by_code['111150'] == pytest.approx(0.4 * 100 + 0.6 * 200)
    assert by_code['111160'] == pytest.approx(0.4 * 40)  # absent in 2022
    assert by_code['111199'] == pytest.approx(0.6 * 50)  # absent in 2017
    assert set(out['Year']) == {2017}


def test_loaded_high_census_takes_its_own_weight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_other(monkeypatch, 2017, {'111150': 100.0})
    fba = _fba(2022, {'111150': 200.0}, **_settings(2021))
    out = pd.DataFrame(flowbyclean.interpolate_census_years(fba))
    assert out['FlowAmount'].sum() == pytest.approx(0.2 * 100 + 0.8 * 200)


@pytest.mark.parametrize('target', [2017, 2022, 2024])
def test_census_and_later_years_are_unchanged(target: int) -> None:
    fba = _fba(2022 if target >= 2022 else 2017, {'111150': 5.0}, **_settings(target))
    out = flowbyclean.interpolate_census_years(fba)
    assert out is fba
