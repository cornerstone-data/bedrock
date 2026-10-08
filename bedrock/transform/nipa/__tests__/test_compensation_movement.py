"""Structural tests for the QCEW movement series.

Following ``test_nowcast_targets.py``: these check that the weight vector is
*shaped* the way the method needs, because that is what the yaml is built
against. Whether the movement is any good is a real-data question and lives in
``compensation_movement_holdout.py`` and behind ``--check``, per the convention
that real-data assertions do not become unit tests.

The highest-value test here is
:func:`test_the_carve_out_leaves_group_shares_untouched` - QCEW applied to
construction and government makes the block **worse**, and nothing else in the
pipeline would say so.
"""

from __future__ import annotations

import inspect
import re

import numpy as np
import pandas as pd
import pytest

from bedrock.transform.nipa import compensation_movement as cm
from bedrock.transform.nipa.compensation_movement import (
    BENCHMARK_YEAR,
    COVERAGE_FLOOR,
    apply_qcew_movement,
)


class _Frame(pd.DataFrame):
    """A DataFrame carrying a ``config``, which is all the socket reads."""

    _metadata = ['config']

    @property
    def _constructor(self) -> type[_Frame]:
        return _Frame


def _fba(config: dict[str, object]) -> _Frame:
    frame = _Frame(
        {
            'ActivityProducedBy': ['V00100'] * 3,
            'ActivityConsumedBy': ['1111A0', '1111B0', 'T001'],
            'FlowAmount': [100.0, 200.0, 999.0],
        }
    )
    frame.config = config
    return frame


def test_the_socket_needs_movement_year_not_year() -> None:
    """``year`` on the attribution source is the 2017 benchmark.

    Reading ``year`` instead would look for a detail Use SUT in a year BEA does
    not publish one, so the socket refuses rather than guessing.
    """
    with pytest.raises(ValueError, match='movement_year'):
        apply_qcew_movement(_fba({'year': 2018}))


def test_the_benchmark_year_is_the_identity() -> None:
    """Growth from 2017 to 2017 is 1, so the 2017 file must be untouched.

    This is what keeps ``NIPA_VA_compensation_2017`` reproducing the published
    benchmark exactly after the movement was wired in.
    """
    fba = _fba({'movement_year': BENCHMARK_YEAR})
    moved = apply_qcew_movement(fba)
    pd.testing.assert_series_equal(moved['FlowAmount'], fba['FlowAmount'])


def test_industries_the_weights_do_not_name_are_left_alone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``T001`` and the other Use-table totals ride along in the FBA.

    They are dropped later by ``exclusion_fields``; until then they must not be
    scaled by a growth factor that does not exist, and above all must not
    become ``NaN``.
    """
    factors = pd.DataFrame({'applied': [2.0, 0.5]}, index=['1111A0', '1111B0'])
    monkeypatch.setattr(cm, 'compensation_weights', lambda year: factors)

    moved = apply_qcew_movement(_fba({'movement_year': 2018}))

    assert moved['FlowAmount'].tolist()[:2] == [200.0, 100.0]
    totals_row = float(np.asarray(moved['FlowAmount'].iloc[2]).item())
    assert totals_row == pytest.approx(999.0)
    assert moved['FlowAmount'].notna().all()


@pytest.fixture
def synthetic_weights(monkeypatch: pytest.MonkeyPatch) -> pd.DataFrame:
    industries = [
        'construction',
        'government',
        'measured',
        'missing',
        'unobserved',
        'unbridged',
    ]
    benchmark = pd.Series(100.0, index=industries, name='benchmark')
    growth = pd.Series([2.0, 3.0, 4.0, np.nan, 5.0, 6.0], index=industries)
    parents = {
        'construction': '23',
        'government': 'GSLG',
        'measured': '44',
        'missing': '44',
        'unobserved': '44',
        'unbridged': '44',
    }
    monkeypatch.setattr(cm, 'benchmark_compensation', lambda: benchmark)
    monkeypatch.setattr(cm, 'qcew_growth', lambda year: growth)
    monkeypatch.setattr(cm, 'detail_to_summary', lambda: parents)
    monkeypatch.setattr(cm, 'unobserved_industries', lambda: ('unobserved',))
    monkeypatch.setattr(cm, 'unbridged_industries', lambda year: ('unbridged',))
    return cm.compensation_weights(2024)


def test_the_carve_out_leaves_group_shares_untouched(
    synthetic_weights: pd.DataFrame,
) -> None:
    """Construction and government must come through frozen, exactly.

    Applied to them QCEW makes the block *worse* - ``GSLG`` alone goes from
    4,748 misplaced to 71,694 - because the concordance puts 47 NAICS codes
    under more than one BEA detail industry and cannot express the split.
    """
    carved = ['construction', 'government']
    assert np.allclose(synthetic_weights.loc[carved, 'applied'], 1.0)
    assert (
        synthetic_weights.loc[carved, 'reason']
        == 'concordance cannot resolve the group'
    ).all()
    assert synthetic_weights.loc['measured', 'applied'] == 4.0


def test_no_industry_is_weighted_to_zero(synthetic_weights: pd.DataFrame) -> None:
    """A zero weight deletes the industry from its group.

    QCEW reaches 379 of 402; the rest keep their group's movement rather than a
    zero, which is the difference between "no opinion" and "no compensation".
    """
    assert (synthetic_weights['weight'] > 0).all()
    assert synthetic_weights['applied'].notna().all()
    assert synthetic_weights.loc['missing', 'applied'] == 1.0
    assert synthetic_weights.loc['unobserved', 'applied'] == 1.0


def test_an_industry_whose_year_bridge_collapses_is_frozen(
    synthetic_weights: pd.DataFrame,
) -> None:
    """#857: the floor has to be checked on the year, not only the benchmark.

    ``4B0000`` all other retail is covered 83% on 2017 and 0.71% on its own
    2022 bridge - every constituent NAICS was renumbered in the 2022 revision
    and one pair survived, manufactured home dealers at 0.86% of its payroll,
    whose 79% housing-boom growth was applied to the whole retail residual.
    """
    assert synthetic_weights.loc['unbridged', 'applied'] == 1.0
    assert synthetic_weights.loc['unbridged', 'reason'] == '2024 bridge covers below 1%'
    # the guard must not spread: a covered industry keeps QCEW's number
    assert synthetic_weights.loc['measured', 'applied'] == 4.0


def test_nearly_losing_coverage_is_distinguished_from_losing_it(
    synthetic_weights: pd.DataFrame,
) -> None:
    """Both fall back, and the reason has to say which happened.

    An industry with *no* paired codes already fell back correctly; the one
    that kept a single pair did not, and that is the whole defect. They land in
    the same place, so only ``reason`` can tell them apart afterwards.
    """
    assert synthetic_weights.loc['missing', 'reason'] == 'no paired qcew payroll'
    assert synthetic_weights.loc['unbridged', 'reason'] != 'no paired qcew payroll'
    assert (synthetic_weights.loc[['missing', 'unbridged'], 'applied'] == 1.0).all()


def test_the_coverage_floor_is_a_guard_not_a_selector() -> None:
    """One percent, and it is deliberately not tuned.

    Graded on the 2012 -> 2017 holdout a 1% floor scores -10.016%, *identical*
    to no floor - the span does not contain the failure it exists to prevent -
    and tuning it upward makes the block worse (-8.2% at 0.10). It is kept
    because an industry QCEW observes at one part in a thousand is not being
    measured, which is an argument about the source rather than the score.
    """
    assert COVERAGE_FLOOR == 0.01


def test_both_coverage_guards_share_one_constant() -> None:
    """#857's guard is the same floor moved, not a second tunable.

    Two thresholds would invite tuning one against the other; the fix is that
    the existing constant is evaluated on the year's own bridge as well as on
    the benchmark cross-section.
    """
    source = inspect.getsource(cm.unbridged_industries)
    assert 'COVERAGE_FLOOR' in source
    assert not re.search(r'0\.\d+', source)
