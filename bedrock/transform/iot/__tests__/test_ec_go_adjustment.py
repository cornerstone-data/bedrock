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


def test_group_totals_stay_bea_in_the_census_year(
    raw: pd.DataFrame, adjusted: pd.DataFrame
) -> None:
    """The EC conditioning holds every summary group -- at 2022.

    ⚠️ **Scoped to the census year since #1013.** The AIES chain deliberately
    reallocates *between* summary groups in 2023-24 and holds only the
    manufacturing sector total, because $250bn of the $387bn of measured mix
    error sits between groups rather than inside them. Asserting group totals for
    the chained years asserts the thing the chain exists to change.
    """
    parents = _parents(raw.index)
    got = adjusted[adj.CENSUS_YEAR].groupby(parents).sum()
    want = raw[adj.CENSUS_YEAR].groupby(parents).sum()
    pd.testing.assert_series_equal(got, want, rtol=1e-9)


def test_the_chained_years_hold_the_manufacturing_total_not_the_groups(
    raw: pd.DataFrame, adjusted: pd.DataFrame
) -> None:
    """The contract the chain replaced group totals with (#1013).

    Both halves matter: the sector total is held exactly, and the groups inside
    it genuinely move -- otherwise a no-op chain would pass.
    """
    from bedrock.transform.iot.aies_go_chaining import (  # noqa: PLC0415
        CHAINED_YEARS,
        PREFIXES,
    )

    members = [c for c in raw.index.astype(str) if str(c).startswith(PREFIXES)]
    parents = _parents(raw.index)
    for year in CHAINED_YEARS:
        assert float(adjusted.loc[members, year].sum()) == pytest.approx(
            float(raw.loc[members, year].sum()), rel=1e-9
        ), year
        got = adjusted.loc[members, year].groupby(parents[members]).sum()
        want = raw.loc[members, year].groupby(parents[members]).sum()
        assert (
            float((got - want).abs().sum()) > 10_000.0
        ), f'{year}: no group moved; the chain is a no-op'


def test_nothing_before_2022_and_nothing_outside_manufacturing_moves(
    raw: pd.DataFrame, adjusted: pd.DataFrame
) -> None:
    early = [c for c in raw.columns if int(c) < adj.CENSUS_YEAR]
    pd.testing.assert_frame_equal(adjusted[early], raw[early])

    conditioned: set[str] = set()
    for factors_fn, _years in adj.SECTOR_CONDITIONERS.values():
        conditioned |= set(factors_fn().index)
    outside = [i for i in raw.index if i not in conditioned]
    pd.testing.assert_frame_equal(adjusted.loc[outside], raw.loc[outside])


def test_the_adjustment_actually_moves_the_mix(
    raw: pd.DataFrame, adjusted: pd.DataFrame
) -> None:
    """A regression stop: a silent no-op adjustment would pass everything else."""
    delta = (adjusted[adj.CENSUS_YEAR] - raw[adj.CENSUS_YEAR]).abs().sum() / 2

    assert delta > 100_000  # $M; measured ~144,571


def test_screened_industries_keep_bea(raw: pd.DataFrame) -> None:
    for factors_fn, _years in adj.SECTOR_CONDITIONERS.values():
        factors = factors_fn()
        screened = factors[factors['screened']]
        pd.testing.assert_series_equal(
            screened['g_ec'], screened['g_bea'], check_names=False
        )
    assert set(adj.PENDING_REVIEW) <= set(
        adj.ec_growth_factors()[adj.ec_growth_factors()['screened']].index
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


def test_chained_years_no_longer_carry_beas_annual_movement(
    raw: pd.DataFrame, adjusted: pd.DataFrame
) -> None:
    """⚠️ **Inverted by #1013**, and kept because the inversion IS the change.

    Before the AIES chain, 2023's adjusted-to-raw ratio equalled 2022's up to a
    per-group scalar -- that is what "carries BEA's own annual movement" means,
    and it was the defect: BEA's 2023-24 manufacturing detail has no AIES in it
    at all. The chain replaces that movement with census receipts, so the ratio
    vectors must now differ **within** a group by more than rounding.

    The old invariant is asserted here against the unchained panel, so this test
    still pins the pre-chain behaviour rather than merely dropping it.
    """
    from bedrock.transform.iot.ec_go_adjustment import (  # noqa: PLC0415
        apply_ec_adjustment,
    )

    factors = adj.ec_growth_factors()
    parents = _parents(raw.index).reindex(factors.index)

    def worst_spread(panel: pd.DataFrame) -> float:
        ratio_22 = panel[2022] / raw[2022]
        ratio_23 = panel[2023] / raw[2023]
        spreads = []
        for _group, members in parents.groupby(parents):
            codes = [c for c in members.index if raw.loc[c, 2022] > 0]
            if len(codes) < 2:
                continue
            rel = (ratio_23[codes] / ratio_22[codes]).dropna()
            if not rel.empty:
                spreads.append(float(rel.max() - rel.min()))
        return max(spreads) if spreads else 0.0

    # unchained: BEA's own movement, so the ratios differ only by a scalar
    assert worst_spread(apply_ec_adjustment(raw)) < 1e-9

    # as shipped: census movement, so they do not
    assert worst_spread(adjusted) > 1e-6
