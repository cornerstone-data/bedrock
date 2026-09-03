"""x and q under ``usa_detail_io_source == 'nowcast'`` (Step 9a).

Hermetic: the Make is a 3x3 frame and every loader is patched out. Under test
is the routing - the nowcast path reads industry output off the Make and never
off the BEA gross-output series, and B skips the legacy year scaling and
inflation. The real-data counterpart is ``test_nowcast_x_q_realdata``.
"""

from __future__ import annotations

from collections.abc import Iterator
from types import SimpleNamespace

import pandas as pd
import pytest

from bedrock.transform.eeio import derived_cornerstone as dc

INDUSTRIES = ['I1', 'I2', 'I3']
COMMODITIES = ['C1', 'C2', 'C3']
GASES = ['CO2', 'CH4']


def _make() -> pd.DataFrame:
    return pd.DataFrame(
        [[10.0, 2.0, 0.0], [0.0, 20.0, 1.0], [3.0, 0.0, 30.0]],
        index=pd.Index(INDUSTRIES, name='sector'),
        columns=pd.Index(COMMODITIES, name='sector'),
    )


def _cfg(source: str = 'nowcast', **overrides: object) -> SimpleNamespace:
    """The USAConfig fields ``derived_cornerstone`` reads, for one source."""
    nowcast = source == 'nowcast'
    fields: dict[str, object] = dict(
        usa_detail_io_source=source,
        usa_base_io_data_year=2023 if nowcast else 2017,
        usa_ghg_data_year=2023,
        model_base_year=2023,
        usa_io_data_year=2022,
        usa_detail_original_year=2023 if nowcast else 2017,
        iot_before_or_after_redefinition='after',
        apply_io_year_adjustments=False,
        use_E_data_year_for_x_in_B=False,
        deflate_x_to_detail_io_year_for_B=False,
        use_ghg_year_x_in_B=False,
        implement_waste_disaggregation=False,
        implement_electricity_reallocation=False,
        implement_electricity_disaggregation=False,
        implement_electricity_mixed_units=False,
    )
    fields.update(overrides)
    return SimpleNamespace(**fields)


def _refuse(*_args: object, **_kwargs: object) -> None:
    raise AssertionError('this path must not be reached under the configured source')


_CACHED = (
    dc.derive_cornerstone_V,
    dc.derive_cornerstone_x,
    dc.derive_cornerstone_q,
    dc.derive_cornerstone_x_after_redefinition,
    dc.derive_cornerstone_B_non_finetuned,
)


@pytest.fixture(autouse=True)
def _clear_caches() -> Iterator[None]:
    for fn in _CACHED:
        fn.cache_clear()
    yield
    for fn in _CACHED:
        fn.cache_clear()


@pytest.fixture
def nowcast_make(monkeypatch: pytest.MonkeyPatch) -> pd.DataFrame:
    """A nowcast config whose router Make is the 3x3 frame; GO loaders refuse."""
    V = _make()
    monkeypatch.setattr(dc, 'get_usa_config', lambda: _cfg())
    monkeypatch.setattr(dc, 'derive_cornerstone_V', lambda *a, **k: V.copy())
    monkeypatch.setattr(dc, 'validate_cornerstone', lambda df, kind: None)
    monkeypatch.setattr(dc, 'derive_gross_output', _refuse)
    monkeypatch.setattr(dc, 'expand_industry_output_vector', _refuse)
    return V


def test_x_after_redefinition_is_the_make_row_sum(nowcast_make: pd.DataFrame) -> None:
    x = dc.derive_cornerstone_x_after_redefinition()
    pd.testing.assert_series_equal(x, nowcast_make.sum(axis=1))
    pd.testing.assert_series_equal(x, dc.derive_cornerstone_x())
    pd.testing.assert_series_equal(dc.derive_cornerstone_x_after_redefinition(2023), x)


def test_x_after_redefinition_rejects_another_year(nowcast_make: pd.DataFrame) -> None:
    with pytest.raises(ValueError, match='one calendar year'):
        dc.derive_cornerstone_x_after_redefinition(2022)


def test_q_is_the_make_column_sum(nowcast_make: pd.DataFrame) -> None:
    pd.testing.assert_series_equal(dc.derive_cornerstone_q(), nowcast_make.sum(axis=0))


def test_published_path_still_reads_gross_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression guard: ``bea_published`` keeps the GO-series x."""
    go = pd.Series([5.0, 6.0, 7.0], index=INDUSTRIES)
    calls: list[tuple[object, object]] = []

    def fake_go(
        *, target_year: object, iot_before_or_after_redefinition: object
    ) -> pd.Series:
        calls.append((target_year, iot_before_or_after_redefinition))
        return go

    monkeypatch.setattr(dc, 'get_usa_config', lambda: _cfg('bea_published'))
    monkeypatch.setattr(dc, 'derive_gross_output', fake_go)
    monkeypatch.setattr(dc, 'expand_industry_output_vector', lambda s: s.copy())
    monkeypatch.setattr(
        dc, '_distribute_waste_parent_x_using_v_row_shares', lambda s: s
    )
    monkeypatch.setattr(dc, 'validate_cornerstone', lambda df, kind: None)
    monkeypatch.setattr(dc, 'derive_cornerstone_x', _refuse)

    x = dc.derive_cornerstone_x_after_redefinition()

    assert calls == [(2023, 'after')]
    pd.testing.assert_series_equal(x, go, check_names=False)


def test_b_via_vnorm_divides_e_by_make_x_even_with_legacy_flags(
    nowcast_make: pd.DataFrame, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The legacy x flags cannot pull B back onto the GO series under nowcast."""
    monkeypatch.setattr(
        dc,
        'get_usa_config',
        lambda: _cfg(
            use_E_data_year_for_x_in_B=True,
            deflate_x_to_detail_io_year_for_B=True,
            use_ghg_year_x_in_B=True,
        ),
    )
    E = pd.DataFrame(
        [[10.0, 20.0, 30.0], [1.0, 2.0, 3.0]],
        index=pd.Index(GASES, name='ghg'),
        columns=pd.Index(INDUSTRIES, name='sector'),
    )
    identity = pd.DataFrame(
        [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]],
        index=pd.Index(INDUSTRIES, name='sector'),
        columns=pd.Index(COMMODITIES, name='sector'),
    )
    monkeypatch.setattr(dc, 'derive_E_usa', lambda: E)
    monkeypatch.setattr(
        dc, 'derive_cornerstone_Vnorm_scrap_corrected', lambda *a, **k: identity
    )
    monkeypatch.setattr(dc, 'get_cornerstone_industry_price_ratio', _refuse)

    B = dc.derive_cornerstone_B_via_vnorm()

    expected = E.divide(nowcast_make.sum(axis=1), axis=1) @ identity
    pd.testing.assert_frame_equal(B, expected)


def test_b_non_finetuned_is_the_vnorm_b(
    nowcast_make: pd.DataFrame, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No summary-ratio scaling to usa_io_data_year, no PI inflation, under nowcast."""
    marker = pd.DataFrame([[1.0]], index=['CO2'], columns=['C1'])
    monkeypatch.setattr(dc, 'derive_cornerstone_B_via_vnorm', lambda: marker)
    monkeypatch.setattr(dc, 'scale_cornerstone_B', _refuse)
    monkeypatch.setattr(dc, 'inflate_cornerstone_B_matrix_with_industry_pi', _refuse)

    assert dc.derive_cornerstone_B_non_finetuned() is marker


def test_b_non_finetuned_keeps_the_legacy_footing_when_published(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression guard: ``bea_published`` without GHG-year x still scales and inflates."""
    monkeypatch.setattr(dc, 'get_usa_config', lambda: _cfg('bea_published'))
    marker = pd.DataFrame([[1.0]], index=['CO2'], columns=['C1'])
    seen: list[str] = []

    def scale(*, B: pd.DataFrame, original_year: int, target_year: int) -> pd.DataFrame:
        seen.append(f'scale {original_year}->{target_year}')
        return B

    def inflate(
        B: pd.DataFrame, *, original_year: int, target_year: int
    ) -> pd.DataFrame:
        seen.append(f'inflate {original_year}->{target_year}')
        return B

    monkeypatch.setattr(dc, 'derive_cornerstone_B_via_vnorm', lambda: marker)
    monkeypatch.setattr(dc, 'scale_cornerstone_B', scale)
    monkeypatch.setattr(dc, 'inflate_cornerstone_B_matrix_with_industry_pi', inflate)

    assert dc.derive_cornerstone_B_non_finetuned() is marker
    assert seen == ['scale 2017->2022', 'inflate 2022->2023']
