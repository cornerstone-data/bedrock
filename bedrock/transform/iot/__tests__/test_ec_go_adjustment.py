"""The EC-2022 conditioning of manufacturing detail GO (#724).

The contract is four invariants: group totals stay BEA in every adjusted year,
nothing outside manufacturing or before 2022 moves, bridged families keep
BEA's within-family split, and the screen holds its named industries on BEA.
"""

from __future__ import annotations

import pandas as pd
import pytest

import bedrock.transform.iot.ec_go_adjustment as adj
from bedrock.transform.iot.derived_intermediate_and_value_added import (
    detail_gross_output_panel,
)


@pytest.fixture(scope='module')
def raw() -> pd.DataFrame:
    return detail_gross_output_panel(ec_adjusted=False)


@pytest.fixture(scope='module')
def adjusted() -> pd.DataFrame:
    return detail_gross_output_panel()


def _parents(index: pd.Index) -> pd.Series:
    return pd.Series({code: adj._industry_parent()[code] for code in index})


def test_group_totals_stay_bea_in_every_adjusted_year(
    raw: pd.DataFrame, adjusted: pd.DataFrame
) -> None:
    parents = _parents(raw.index)
    for year in adj.EC_ADJUSTED_YEARS:
        got = adjusted[year].groupby(parents).sum()
        want = raw[year].groupby(parents).sum()
        pd.testing.assert_series_equal(got, want, rtol=1e-9)


def test_nothing_before_2022_and_nothing_outside_manufacturing_moves(
    raw: pd.DataFrame, adjusted: pd.DataFrame
) -> None:
    early = [c for c in raw.columns if int(c) < adj.CENSUS_YEAR]
    pd.testing.assert_frame_equal(adjusted[early], raw[early])

    factors = adj.ec_growth_factors()
    outside = [i for i in raw.index if i not in factors.index]
    pd.testing.assert_frame_equal(adjusted.loc[outside], raw.loc[outside])


def test_the_adjustment_actually_moves_the_mix(
    raw: pd.DataFrame, adjusted: pd.DataFrame
) -> None:
    """A regression stop: a silent no-op adjustment would pass everything else."""
    delta = (adjusted[adj.CENSUS_YEAR] - raw[adj.CENSUS_YEAR]).abs().sum() / 2

    assert delta > 100_000  # $M; measured ~144,571


def test_screened_industries_keep_bea(raw: pd.DataFrame) -> None:
    factors = adj.ec_growth_factors()
    screened = factors[factors['screened']]

    assert set(adj.PENDING_REVIEW) <= set(screened.index)
    pd.testing.assert_series_equal(
        screened['g_ec'], screened['g_bea'], check_names=False
    )


def test_bridged_families_keep_beas_within_family_split(raw: pd.DataFrame) -> None:
    """Inside a bridged family the adjusted relative movement is BEA's own."""
    from bedrock.analysis.nowcasting.ec_manufacturing_output_check import (  # noqa: PLC0415
        units,
    )

    table = units()
    bridged = table[table['bridged']]
    allocation = adj._bridged_members_to_bea(bridged)
    factors = adj.ec_growth_factors()
    checked = 0
    for members in allocation.values():
        inside = [
            m for m in members if m in factors.index and not factors.loc[m, 'screened']
        ]
        if len(inside) < 2:
            continue
        # g_ec ratios between two family members equal their g_bea ratios
        first, second = inside[0], inside[1]
        got = float(str(factors.loc[first, 'g_ec'])) / float(
            str(factors.loc[second, 'g_ec'])
        )
        want = float(str(factors.loc[first, 'g_bea'])) / float(
            str(factors.loc[second, 'g_bea'])
        )
        assert got == pytest.approx(want, rel=1e-9), (first, second)
        checked += 1
    assert checked > 0


def test_chained_years_carry_beas_annual_movement(
    raw: pd.DataFrame, adjusted: pd.DataFrame
) -> None:
    """2023's adjusted-to-raw ratio equals 2022's, up to the group rescale."""
    factors = adj.ec_growth_factors()
    parents = _parents(raw.index).reindex(factors.index)
    ratio_22 = adjusted[2022] / raw[2022]
    ratio_23 = adjusted[2023] / raw[2023]
    # within each group the two ratio vectors differ only by a scalar
    for group, members in parents.groupby(parents):
        codes = [c for c in members.index if raw.loc[c, 2022] > 0]
        if len(codes) < 2:
            continue
        rel = (ratio_23[codes] / ratio_22[codes]).dropna()
        assert float(rel.max() - rel.min()) < 1e-9, group
