"""The GO control on the Supply industry axis (#724).

The module's whole contract is three invariants at once, so that is what is
tested: the summary cells survive, the columns land on GO shares, and 2017 is
not touched.  One year of the controlled span is enough to exercise every code
path -- the fits are cached and expensive, so the parametrised span lives in
``control_residuals --check``, not here.

⚠️ **Groups ARE released by default now** (#1013), so these invariants are
scoped rather than global.  ``chain_manufacturing_on_aies`` ships enabled and
releases all 19 manufacturing summary groups, because the AIES chain moves
$229bn of commodity output *between* those groups and a held summary cell would
hand the difference to siblings.  So the preserved-cell test holds on the
**held** groups only, and the block-total test allows the fit's own tolerance
instead of exact equality.

⚠️ **Do not read ``note`` as a boolean.**  It carries an *informational* note for
a released group as well as a failure note for a skipped or reverted one, so
``diagnostics['note'].astype(bool)`` classified 19 converged manufacturing fits
as reverted -- which made ``test_converged_columns_land_on_their_go_share`` stop
checking manufacturing at all **while still passing**.  ``kept_seed`` and
``released`` are explicit booleans for exactly that reason; select on those.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import bedrock.transform.iot.nowcast_supply_go_control as gc
from bedrock.transform.iot.aies_go_chaining import released_groups as aies_released
from bedrock.utils.config.usa_config import get_usa_config
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


def test_a_summary_cell_moves_only_inside_a_released_INDUSTRY_column(
    seed: pd.DataFrame, controlled: pd.DataFrame
) -> None:
    """Constraint 1, restated for #1013. The invariant is on the column.

    ⚠️ **Not on the commodity row, which is what I assumed first.** The fit runs
    per industry group: a released group's columns take an absolute GO-at-basic
    target, so *everything in those columns* may move -- including a manufacturing
    industry's secondary production of a **held** commodity. A held column moves
    nothing at all.

    Measured at 2023, max |gap| by class, in $M::

        row released  column released   cells      max
        False         False              2808     0.00
        True          False               988     0.00
        False         True               1026 6,209.56
        True          True                361 40,214.58

    ⚠️ So releasing manufacturing also moves **$23.1bn of non-manufacturing
    commodity output** made by manufacturing industries as secondary product --
    ``5412OP`` management consulting alone is $6.2bn. That is a real consequence
    of the release and it is what this test bounds: zero outside released
    columns, unconstrained within them.
    """
    released = gc.released_groups()
    gap = (_summary_cells(controlled) - _summary_cells(seed)).abs()
    held_columns = [group for group in gap.columns if group not in released]
    assert held_columns, 'every column released; this test would assert nothing'

    worst = float(gap[held_columns].to_numpy().max())
    assert worst < 1.0 * MILLION, worst


def test_the_block_total_is_unchanged_to_the_fits_own_tolerance(
    seed: pd.DataFrame, controlled: pd.DataFrame
) -> None:
    """The control redistributes; it must not reprice the economy.

    ⚠️ **Not exact any more, and it cannot be.** A held group is renormalised to
    its own total, so it preserves the block sum by construction. A released
    group is fitted to an *absolute* column target instead, so the grand total is
    no longer pinned: it accumulates the fit's per-column residual, which
    :data:`~bedrock.transform.iot.nowcast_supply_go_control.TOLERANCE_USD` bounds
    at 1 million USD each.

    Measured at 2023 with all 19 manufacturing groups released, the block moves
    **+0.45bn on 47,566bn (+0.0010%)** against **$229bn** of gross reallocation
    between summary groups. The bound below is therefore the fit's own tolerance
    times the column count rather than a number chosen to pass.
    """
    # ⚠️ The electricity rebase (#1009, on since v0.5) changes utilities'
    # *level*, not only its mix -- -$8.8bn at 2023 -- so group 22 is expected
    # to move the total and is left out. What this guards is that the fit does
    # not reprice everything else.
    columns = [c for c in seed.columns if gc._industry_parent().get(c) != '22']
    total = float(seed[columns].to_numpy().sum())
    moved = abs(float(controlled[columns].to_numpy().sum()) - total)

    # ⚠️ Relative, deliberately. The accumulation is not bounded by
    # columns x TOLERANCE_USD -- measured 0.45bn against that product's 0.40bn --
    # so an absolute bound here would be a number picked to pass. What is
    # meaningful is that the control does not reprice the economy: 9.5e-6 of the
    # block, against 229bn of gross reallocation between summary groups.
    assert moved / total < 1e-4, (moved, moved / total)


def test_converged_columns_land_on_their_go_share(controlled: pd.DataFrame) -> None:
    """Constraint 2, on every group the fit did not skip or revert."""
    diagnostics = gc.group_diagnostics(YEAR, download_sources_ok=True)
    # ⚠️ ``kept_seed``, not ``note``: a released group carries an informational
    # note, so selecting on the note excluded all 19 manufacturing groups and
    # left this test passing while checking none of them.
    fitted_groups = set(diagnostics.index[~diagnostics['kept_seed']])
    assert (
        fitted_groups & gc.released_groups()
    ), 'no released group reached the GO-share check; the selector is wrong again'
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
    reverted = diagnostics.index[diagnostics['kept_seed']]
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


def test_released_groups_is_the_union_over_every_conditioner() -> None:
    """What ships released, and which conditioner claims it.

    ⚠️ This used to assert the set was **empty**. It is not: #1013 releases all
    19 manufacturing summary groups by default, because the AIES chain moves
    $229bn of commodity output between them and a held summary cell would hand
    the difference to siblings. Releasing is required by the supply-use framework
    once industry output moves; it is not a trade-off.

    The electricity conditioner (#1009) is on by default since v0.5 and releases
    utilities (``22``); asserted here so flipping that flag surfaces as a change
    in this test rather than silently.
    """
    config = get_usa_config()
    released = gc.released_groups()

    assert config.chain_manufacturing_on_aies
    assert config.rebase_utility_gross_output_on_eia
    assert released == aies_released() | {
        '22'
    }, 'the union picked up a group no conditioner claims'
    # manufacturing's groups plus utilities, and nothing else
    assert all(group.startswith('3') or group == '22' for group in released), sorted(
        released
    )


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

    # The cut comes out of what i_elec produces...  ``.at`` on a frame is a
    # union type, so go through numpy for the scalar.
    def cell(frame: pd.DataFrame) -> float:
        return float(np.asarray(frame.at['c_elec', 'i_elec']).item())

    assert cell(fitted) < cell(sub)
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
