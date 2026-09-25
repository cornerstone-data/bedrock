"""The GO control on the Supply industry axis (#724).

The module's whole contract is three invariants at once, so that is what is
tested: the summary cells survive, the columns land on GO shares, and 2017 is
not touched.  One year of the controlled span is enough to exercise every code
path -- the fits are cached and expensive, so the parametrised span lives in
``control_residuals --check``, not here.

⚠️ **Two of those invariants are conditional on no group being released** (#1009,
:func:`~bedrock.transform.iot.nowcast_supply_go_control.released_groups`).  A
released group gives up its published summary Supply cell on purpose, so
:func:`test_every_summary_cell_is_preserved` and
:func:`test_the_block_total_is_unchanged` hold because
``rebase_utility_gross_output_on_eia`` is off by default rather than
unconditionally.  That precondition is asserted rather than assumed, and the
released behaviour is tested directly on a synthetic sub-block.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import bedrock.transform.iot.nowcast_supply_go_control as gc
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY

MILLION = MILLION_CURRENCY_TO_CURRENCY
BILLION = 1_000.0 * MILLION_CURRENCY_TO_CURRENCY

#: One controlled year exercises fit, skip, revert and the wedge fixed point.
YEAR = 2023


@pytest.fixture(scope='module')
def seed() -> pd.DataFrame:
    return gc.raw_supply_block(YEAR, download_sources_ok=True)


@pytest.fixture(scope='module')
def controlled(seed: pd.DataFrame) -> pd.DataFrame:
    return gc.go_controlled_supply_block(YEAR, download_sources_ok=True)


def _summary_cells(block: pd.DataFrame) -> pd.DataFrame:
    rows = pd.Series(block.index, index=block.index).map(gc._commodity_parent())
    cols = pd.Series(block.columns, index=block.columns).map(gc._industry_parent())
    grouped = block.groupby(rows).sum()
    return grouped.T.groupby(cols).sum().T


def test_2017_passes_through_untouched() -> None:
    """The benchmark year's detail split is observed and outranks GO's."""
    raw = gc.raw_supply_block(2017, download_sources_ok=True)
    out = gc.go_controlled_supply_block(2017, download_sources_ok=True)

    pd.testing.assert_frame_equal(out, raw)


def test_every_summary_cell_is_preserved(
    seed: pd.DataFrame, controlled: pd.DataFrame
) -> None:
    """Constraint 1: the published summary control must not be given up."""
    gap = (_summary_cells(controlled) - _summary_cells(seed)).abs()

    assert float(gap.to_numpy().max()) < 1.0 * MILLION


def test_the_block_total_is_unchanged(
    seed: pd.DataFrame, controlled: pd.DataFrame
) -> None:
    """The control redistributes; it must not reprice the economy."""
    assert float(controlled.to_numpy().sum()) == pytest.approx(
        float(seed.to_numpy().sum()), rel=1e-12
    )


def test_converged_columns_land_on_their_go_share(controlled: pd.DataFrame) -> None:
    """Constraint 2, on every group the fit did not skip or revert."""
    diagnostics = gc.group_diagnostics(YEAR, download_sources_ok=True)
    fitted_groups = set(diagnostics.index[~diagnostics['note'].astype(bool)])
    wedge = gc._wedge(YEAR, controlled)
    target = gc.gross_output_at_basic(YEAR, wedge)

    parents = pd.Series(
        {code: gc._industry_parent()[code] for code in controlled.columns}
    )
    columns = controlled.sum(axis=0)
    for group in fitted_groups:
        members = list(parents.index[parents == group])
        want = target[members] * (
            float(columns[members].sum()) / float(target[members].sum())
        )
        worst = float((columns[members] - want).abs().max())
        assert worst <= gc.TOLERANCE_USD, (group, worst)


def test_reverted_groups_keep_the_seed_exactly(
    seed: pd.DataFrame, controlled: pd.DataFrame
) -> None:
    """All or nothing per group: a failed fit must not ship a half-fit."""
    diagnostics = gc.group_diagnostics(YEAR, download_sources_ok=True)
    reverted = diagnostics.index[diagnostics['note'].astype(bool)]
    parents = pd.Series({code: gc._industry_parent()[code] for code in seed.columns})
    for group in reverted:
        members = list(parents.index[parents == group])
        pd.testing.assert_frame_equal(controlled[members], seed[members])


def test_no_cell_changes_sign(seed: pd.DataFrame, controlled: pd.DataFrame) -> None:
    """Multiplicative scaling cannot invent mass where the seed has none."""
    seeded = seed.to_numpy()
    fitted = controlled.to_numpy()

    assert not np.any((seeded == 0) & (fitted != 0))
    assert not np.any(np.sign(seeded) * np.sign(fitted) < 0)


def test_uncontrolled_years_are_refused_nothing(seed: pd.DataFrame) -> None:
    """The seed accessor is the cycle-breaker: same frame, no fit."""
    assert gc.seed_commodity_output(YEAR, download_sources_ok=True).equals(
        seed.sum(axis=1)
    )


# --------------------------------------------------------------------------
# Released groups (#1009). The synthetic sub-block keeps these independent of
# the config and of the cached real fits.
# --------------------------------------------------------------------------


def _sub_block() -> pd.DataFrame:
    """Three industries, three commodity groups, every cell positive.

    ⚠️ **The pattern has to be dense for the held path to be feasible at all.**
    A commodity produced by a single industry pins that industry's column, and
    with three such commodities the biproportional fit is over-determined -- it
    leaves a residual and the test fails for a reason that has nothing to do
    with what is being tested.

    ⚠️ **And the magnitudes have to be realistic**, because
    :data:`~bedrock.transform.iot.nowcast_supply_go_control.TOLERANCE_USD` is an
    absolute 1 million USD.  On toy numbers the fit clears it on the first sweep
    and returns whatever residual that leaves, so the test would be measuring
    one sweep of biproportional fitting rather than convergence.
    """
    return (
        pd.DataFrame(
            {
                'i_elec': [80.0, 10.0, 2.0],
                'i_gas': [4.0, 40.0, 5.0],
                'i_water': [1.0, 2.0, 15.0],
            },
            index=pd.Index(['c_elec', 'c_gas', 'c_water'], name='commodity'),
        )
        * BILLION
    )


def _row_groups() -> 'pd.Series[str]':
    return pd.Series({'c_elec': 'A', 'c_gas': 'B', 'c_water': 'C'}, name='group')


#: Cut the electricity column by 10 and leave the siblings on the totals they
#: already have, so any movement in them is the release misbehaving.
_TARGETS = pd.Series({'i_elec': 82.0, 'i_gas': 49.0, 'i_water': 18.0}) * BILLION


def test_no_group_is_released_by_default() -> None:
    """The precondition the two invariant tests above depend on."""
    assert gc.released_groups() == frozenset()


def test_a_released_group_lands_on_the_absolute_column_target() -> None:
    """The level survives, which is the whole point of releasing the row."""
    fitted, sweeps, worst = gc.fit_group(
        _sub_block(), _TARGETS, _row_groups(), hold_summary_rows=False
    )

    assert sweeps == 1
    assert worst < 1.0
    for industry, want in _TARGETS.items():
        assert float(fitted[industry].sum()) == pytest.approx(float(want))


def test_a_released_group_does_not_hand_the_cut_to_its_siblings() -> None:
    """The defect this exists to fix.

    Holding the summary row forced the reduction to reappear on the other
    industries in the group. Measured on 2022 before the release, the electric
    power industry's gross output fell 47.08bn USD while the electricity
    commodity row fell only 9.67bn and gas distribution *gained* 8.45bn.
    """
    sub = _sub_block()
    fitted, _sweeps, _worst = gc.fit_group(
        sub, _TARGETS, _row_groups(), hold_summary_rows=False
    )

    # The cut comes out of what i_elec produces...
    assert float(fitted.at['c_elec', 'i_elec']) < float(sub.at['c_elec', 'i_elec'])
    # ...and the siblings are untouched, cell by cell, not merely in total.
    for industry in ('i_gas', 'i_water'):
        pd.testing.assert_series_equal(
            fitted[industry], sub[industry], check_names=False
        )
    # The group total moves, which a held group cannot do.
    assert float(fitted.to_numpy().sum()) < float(sub.to_numpy().sum())


def test_a_held_group_divides_the_level_change_out() -> None:
    """The contrast, and why releasing was necessary rather than cosmetic."""
    sub = _sub_block()
    total = float(sub.to_numpy().sum())
    normalised = _TARGETS * (total / float(_TARGETS.sum()))

    fitted, _sweeps, worst = gc.fit_group(
        sub, normalised, _row_groups(), hold_summary_rows=True
    )

    # The module's own bar, not a tighter one: a held group is converged when it
    # is inside TOLERANCE_USD, and it ends on the row sweep so the rows are exact.
    assert worst < gc.TOLERANCE_USD
    # The group total survives, so the 10-unit cut did not leave the group.
    assert float(fitted.to_numpy().sum()) == pytest.approx(total)
    for commodity in sub.index:
        assert float(fitted.loc[commodity].sum()) == pytest.approx(
            float(sub.loc[commodity].sum())
        )
    # And the siblings moved, which is the reallocation that has no source.
    assert float(fitted['i_gas'].sum()) != pytest.approx(float(sub['i_gas'].sum()))
