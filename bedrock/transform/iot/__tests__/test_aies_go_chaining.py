"""The AIES chain for manufacturing 2023-24 (#1013).

Synthetic throughout -- no extract.  The contract is three things at once: the
manufacturing **total** is held (deliberately not each summary group, see the
module note), the mix moves onto census, and nothing outside manufacturing or
outside the chained years is touched.
"""

from __future__ import annotations

from collections.abc import Iterator

import numpy as np
import pandas as pd
import pytest

import bedrock.transform.iot.aies_go_chaining as ch

YEARS = [2017, 2022, 2023, 2024]
MFG = ['311111', '324110', '336411']
OTHER = ['221100', '541100']


def _panel() -> pd.DataFrame:
    """Manufacturing plus two rows that must never move, $M."""
    return pd.DataFrame(
        {
            2017: [100.0, 500.0, 150.0, 390.0, 200.0],
            2022: [120.0, 820.0, 95.0, 550.0, 240.0],
            2023: [130.0, 670.0, 125.0, 500.0, 250.0],
            2024: [135.0, 590.0, 135.0, 505.0, 260.0],
        },
        index=pd.Index(MFG + OTHER, name='industry'),
    )


def _f(value: object) -> float:
    """``.at`` and Series indexing are a wide union; go through numpy."""
    return float(np.asarray(value).item())


def _receipts(
    monkeypatch: pytest.MonkeyPatch, table: dict[int, dict[str, float]]
) -> None:
    def fake(year: int) -> pd.Series:
        return pd.Series(table[year], dtype=float)

    def fake_common(year: int, against: int) -> pd.Series:
        """Both sides restricted to the industries present in both years."""
        shared = set(table[ch.ANCHOR_YEAR]) & set(table[against])
        return pd.Series(
            {k: v for k, v in table[year].items() if k in shared}, dtype=float
        )

    monkeypatch.setattr(ch, 'receipts_by_bea', fake)
    monkeypatch.setattr(ch, 'receipts_on_common_basis', fake_common)


#: Census says refineries fell and aircraft fell; BEA's panel has aircraft
#: rising.  Deliberately mirrors the real 2024 disagreement.
CENSUS = {
    2022: {'311111': 120.0, '324110': 825.0, '336411': 93.0},
    2023: {'311111': 130.0, '324110': 710.0, '336411': 106.0},
    2024: {'311111': 134.0, '324110': 656.0, '336411': 97.0},
}


def test_the_manufacturing_total_is_held_in_every_chained_year(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The invariant this module chose over preserving each summary group."""
    _receipts(monkeypatch, CENSUS)
    raw = _panel()
    out = ch.apply_aies_chaining(raw, raw)
    for year in ch.CHAINED_YEARS:
        assert float(out.loc[MFG, year].sum()) == pytest.approx(
            float(raw.loc[MFG, year].sum()), rel=1e-12
        )


def test_the_mix_moves_onto_census(monkeypatch: pytest.MonkeyPatch) -> None:
    """Shares within manufacturing match census shares, which is the whole point."""
    _receipts(monkeypatch, CENSUS)
    raw = _panel()
    out = ch.apply_aies_chaining(raw, raw)
    for year in ch.CHAINED_YEARS:
        # anchor ratio x census growth == census level x (anchor ratio at 2022)
        anchor_ratio = raw.loc[MFG, ch.ANCHOR_YEAR] / pd.Series(
            CENSUS[ch.ANCHOR_YEAR]
        ).reindex(MFG)
        want = pd.Series(CENSUS[year]).reindex(MFG) * anchor_ratio
        want = want / want.sum()
        got = out.loc[MFG, year] / out.loc[MFG, year].sum()
        pd.testing.assert_series_equal(
            got.astype(float), want.astype(float), check_names=False, rtol=1e-10
        )


def test_aircraft_stops_rising_when_census_says_it_fell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The 2024 case that motivated the module, in miniature."""
    _receipts(monkeypatch, CENSUS)
    raw = _panel()
    assert _f(raw.at['336411', 2024]) > _f(raw.at['336411', 2023])  # BEA has it rising
    out = ch.apply_aies_chaining(raw, raw)
    assert _f(out.at['336411', 2024]) < _f(out.at['336411', 2023])


def test_nothing_outside_manufacturing_moves(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _receipts(monkeypatch, CENSUS)
    raw = _panel()
    out = ch.apply_aies_chaining(raw, raw)
    pd.testing.assert_frame_equal(out.loc[OTHER], raw.loc[OTHER])


def test_unchained_years_are_returned_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _receipts(monkeypatch, CENSUS)
    raw = _panel()
    out = ch.apply_aies_chaining(raw, raw)
    for year in (2017, ch.ANCHOR_YEAR):
        pd.testing.assert_series_equal(out[year], raw[year])


def test_an_industry_census_does_not_observe_keeps_bea_movement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No census in both years -> BEA's own growth, not an invented one.

    Checked on the *relative* movement against the two observed industries,
    because the final rescale to BEA's sector total moves every level.
    """
    partial = {year: dict(values) for year, values in CENSUS.items()}
    for year in partial:
        partial[year].pop('336411')
    _receipts(monkeypatch, partial)
    raw = _panel()
    out = ch.apply_aies_chaining(raw, raw)
    bea_growth = _f(raw.at['336411', 2024]) / _f(raw.at['336411', ch.ANCHOR_YEAR])
    got_growth = _f(out.at['336411', 2024]) / _f(out.at['336411', ch.ANCHOR_YEAR])
    other_growth = _f(out.at['324110', 2024]) / _f(out.at['324110', ch.ANCHOR_YEAR])
    census_growth = partial[2024]['324110'] / partial[ch.ANCHOR_YEAR]['324110']
    # the unobserved row carries BEA's shape and the observed row census's,
    # up to the one common rescale factor
    assert got_growth / bea_growth == pytest.approx(
        other_growth / census_growth, rel=1e-10
    )


def test_a_zero_census_level_is_not_treated_as_observed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A suppressed cell published as 0 must not become an infinite growth."""
    zeroed = {year: dict(values) for year, values in CENSUS.items()}
    zeroed[ch.ANCHOR_YEAR]['311111'] = 0.0
    _receipts(monkeypatch, zeroed)
    raw = _panel()
    out = ch.apply_aies_chaining(raw, raw)
    assert np.isfinite(out.loc[MFG, 2024].to_numpy()).all()
    assert (out.loc[MFG, 2024] > 0).all()


def test_chain_factors_are_nan_where_census_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    partial = {year: dict(values) for year, values in CENSUS.items()}
    partial[2024].pop('324110')
    _receipts(monkeypatch, partial)
    factors = ch.chain_factors(2024, MFG)
    assert np.isnan(_f(factors.loc['324110']))
    assert _f(factors.loc['336411']) == pytest.approx(97.0 / 93.0)


def test_a_panel_without_the_anchor_year_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _receipts(monkeypatch, CENSUS)
    raw = _panel().drop(columns=[ch.ANCHOR_YEAR])
    with pytest.raises(KeyError, match='anchor year'):
        ch.apply_aies_chaining(raw, raw)


def test_a_panel_with_no_manufacturing_is_returned_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _receipts(monkeypatch, CENSUS)
    raw = _panel().loc[OTHER]
    pd.testing.assert_frame_equal(ch.apply_aies_chaining(raw, raw), raw)


def test_the_flag_is_on_by_default() -> None:
    """⚠️ This one ships ENABLED, unlike its neighbours.

    The convention for the flags around it is default-off and production sets
    none, because they are options being trialled.  This is not one of those --
    BEA states it could not incorporate AIES (SCB 2026-06 preview), the measured
    mix error is $387bn in the release year, and leaving it off would ship the
    error.  The test exists so the default cannot be changed silently in either
    direction.
    """
    from bedrock.utils.config.usa_config import get_usa_config  # noqa: PLC0415

    assert get_usa_config().chain_manufacturing_on_aies is True


def test_a_naics_missing_from_one_year_is_dropped_from_both_sides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression: coverage shifts year to year and the ratio must not.

    ⚠️ Census publishes a different set of six-digit codes each year (909 in
    2022, 883 in 2023, 868 in 2024).  Dividing each year's full population by
    the other's compares two different universes -- on the first build that put
    ``33399A`` at +398% and ``315000`` at +197%, which were mapping artefacts.
    """
    base = (
        ch._receipts_by_naics.__wrapped__
        if hasattr(ch._receipts_by_naics, '__wrapped__')
        else ch._receipts_by_naics
    )

    table = {
        ch.ANCHOR_YEAR: pd.Series({'311111': 100.0, '324110': 800.0}),
        2024: pd.Series({'324110': 700.0, '336411': 90.0}),
    }
    monkeypatch.setattr(ch, '_receipts_by_naics', lambda year: table[year])
    ch._common_naics.cache_clear()
    try:
        common = ch._common_naics(2024)
    finally:
        ch._common_naics.cache_clear()

    # the intersection is by CODE, not by value -- frozenset(series) would
    # iterate the values and silently return an empty set
    assert common == frozenset({'324110'})
    assert base is not None


def test_pooled_industries_share_the_pool_growth(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cement and ready-mix chain on their combined receipts (#1032)."""
    _receipts(
        monkeypatch,
        {
            2022: {'327310': 11.7, '327320': 43.0},
            2024: {'327310': 10.6, '327320': 51.3},
        },
    )
    factors = ch.chain_factors(2024, ['327310', '327320'])
    pooled = (10.6 + 51.3) / (11.7 + 43.0)
    assert _f(factors['327310']) == pytest.approx(pooled)
    assert _f(factors['327320']) == pytest.approx(pooled)


def test_a_pool_census_only_partly_observes_chains_each_member_alone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _receipts(
        monkeypatch,
        {2022: {'327310': 11.7, '327320': 43.0}, 2024: {'327310': 10.6}},
    )
    factors = ch.chain_factors(2024, ['327310', '327320'])
    assert _f(factors['327310']) == pytest.approx(10.6 / 11.7)
    assert np.isnan(_f(factors['327320']))


#: All published levels per year, $M. AIES withholds ``325110`` and ``325199``
#: in 2023 but publishes ``3251``; ``336110`` is Economic Census only (AIES
#: uses NAICS 2017 auto codes), so it is a classification gap, not a withheld
#: cell.
LEVELS = {
    2022: {
        '3251': 300.0,
        '325110': 78.0,
        '325193': 50.0,
        '325199': 100.0,
        '325120': 72.0,
        '3361': 390.0,
        '336110': 357.0,
        '336120': 33.0,
    },
    2023: {
        '3251': 285.0,
        '325193': 48.0,
        '325120': 75.0,
        '3361': 481.0,
        '336120': 40.0,
    },
    2024: {
        '3251': 304.0,
        '325110': 68.0,
        '325193': 41.0,
        '325199': 122.0,
        '325120': 73.0,
        '3361': 425.0,
        '336120': 33.0,
    },
}


@pytest.fixture
def levels(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    def fake(year: int) -> pd.Series:
        return pd.Series(LEVELS[year], dtype=float).sort_index()

    monkeypatch.setattr(ch, '_receipts_all_levels', fake)
    ch.withheld_fill.cache_clear()
    yield
    ch.withheld_fill.cache_clear()


def test_withheld_codes_share_the_parent_residual(levels: None) -> None:
    """``3251`` less its published codes is split by the nearest published years."""
    fill = ch.withheld_fill(2023)
    residual = 285.0 - 48.0 - 75.0
    assert float(fill['filled'].sum()) == pytest.approx(residual)
    w110 = np.sqrt(78.0 * 68.0)
    w199 = np.sqrt(100.0 * 122.0)
    assert _f(fill.at['325110', 'filled']) == pytest.approx(
        residual * w110 / (w110 + w199)
    )
    assert set(fill['parent']) == {'3251'}


def test_a_census_only_code_is_not_filled(levels: None) -> None:
    """``336110`` is in neither AIES year, so ``3361``'s residual is not it."""
    assert '336110' not in ch.withheld_fill(2023).index


def test_years_with_nothing_withheld_fill_nothing(
    levels: None,
) -> None:
    fill = ch.withheld_fill(2024)
    assert fill.empty  # 2024 publishes every code 2023 does
    assert ch.withheld_fill(ch.ANCHOR_YEAR).empty


def test_a_parent_with_nothing_left_over_fills_nothing(
    monkeypatch: pytest.MonkeyPatch, levels: None
) -> None:
    exact = {year: dict(values) for year, values in LEVELS.items()}
    exact[2023]['3251'] = 48.0 + 75.0
    monkeypatch.setattr(
        ch, '_receipts_all_levels', lambda year: pd.Series(exact[year], dtype=float)
    )
    ch.withheld_fill.cache_clear()
    assert ch.withheld_fill(2023).empty


def test_filled_codes_join_the_common_basis(levels: None) -> None:
    """With the fill, 2023 compares the full industry with 2022."""
    ch._common_naics.cache_clear()
    try:
        assert '325199' not in ch._common_naics(2023, False)
        assert {'325110', '325199'} <= ch._common_naics(2023, True)
    finally:
        ch._common_naics.cache_clear()


def test_the_fill_flag_is_off_by_default() -> None:
    from bedrock.utils.config.usa_config import get_usa_config  # noqa: PLC0415

    assert get_usa_config().fill_withheld_aies_codes is False


def test_a_held_industry_keeps_bea_movement_though_census_observes_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``BEA_HOLDS`` overrides an observed census growth (#1211, ``325190``)."""
    _receipts(monkeypatch, CENSUS)
    monkeypatch.setattr(ch, '_held_on_bea', lambda: frozenset({'336411'}))
    raw = _panel()
    out = ch.apply_aies_chaining(raw, raw)
    bea_growth = _f(raw.at['336411', 2024]) / _f(raw.at['336411', ch.ANCHOR_YEAR])
    got_growth = _f(out.at['336411', 2024]) / _f(out.at['336411', ch.ANCHOR_YEAR])
    other_growth = _f(out.at['324110', 2024]) / _f(out.at['324110', ch.ANCHOR_YEAR])
    census_growth = CENSUS[2024]['324110'] / CENSUS[ch.ANCHOR_YEAR]['324110']
    assert got_growth / bea_growth == pytest.approx(
        other_growth / census_growth, rel=1e-10
    )
    assert float(out.loc[MFG, 2024].sum()) == pytest.approx(
        float(raw.loc[MFG, 2024].sum()), rel=1e-12
    )


def test_the_hold_is_off_by_default_and_names_325190() -> None:
    from bedrock.utils.config.usa_config import get_usa_config  # noqa: PLC0415

    assert get_usa_config().hold_aies_outliers_on_bea is False
    assert ch._held_on_bea() == frozenset()
    assert frozenset({'325190'}) == ch.BEA_HOLDS


#: Gas distribution receipts, $M, as published for NAICS 221210 (#1004).
GAS = {2022: 182_670.0, 2023: 165_320.0, 2024: 156_180.0}


def _gas_levels(monkeypatch: pytest.MonkeyPatch, table: dict[int, float]) -> None:
    monkeypatch.setattr(
        ch,
        '_receipts_all_levels',
        lambda year: pd.Series({'221210': table[year]} if year in table else {}),
    )


def _gas_panel() -> pd.DataFrame:
    return pd.DataFrame(
        {
            2017: [69_550.0, 400_000.0, 15_000.0],
            2022: [124_260.0, 480_000.0, 20_000.0],
            2023: [117_710.0, 470_000.0, 20_500.0],
            2024: [117_510.0, 490_000.0, 21_000.0],
        },
        index=pd.Index(['221200', '221100', '221300'], name='industry'),
    )


def test_gas_distribution_takes_census_growth_and_moves_its_level(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _gas_levels(monkeypatch, GAS)
    raw = _gas_panel()
    out = ch.apply_utility_chaining(raw)
    for year in ch.CHAINED_YEARS:
        want = 124_260.0 * GAS[year] / GAS[ch.ANCHOR_YEAR]
        assert _f(out.at['221200', year]) == pytest.approx(want)
    # the level moves: no group total is held
    assert float(out.loc[:, 2024].sum()) < float(raw.loc[:, 2024].sum())


def test_the_gas_chain_touches_nothing_else(monkeypatch: pytest.MonkeyPatch) -> None:
    _gas_levels(monkeypatch, GAS)
    raw = _gas_panel()
    out = ch.apply_utility_chaining(raw)
    pd.testing.assert_frame_equal(
        out.loc[['221100', '221300']], raw.loc[['221100', '221300']]
    )
    for year in (2017, ch.ANCHOR_YEAR):
        pd.testing.assert_series_equal(out[year], raw[year])


def test_gas_distribution_keeps_bea_where_census_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _gas_levels(monkeypatch, {2022: GAS[2022], 2024: GAS[2024]})
    raw = _gas_panel()
    out = ch.apply_utility_chaining(raw)
    assert _f(out.at['221200', 2023]) == _f(raw.at['221200', 2023])
    assert _f(out.at['221200', 2024]) != _f(raw.at['221200', 2024])


def test_the_gas_chain_flag_is_off_by_default() -> None:
    from bedrock.utils.config.usa_config import get_usa_config  # noqa: PLC0415

    assert get_usa_config().chain_gas_distribution_on_aies is False
