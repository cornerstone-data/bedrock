"""The SUT and MUT final-demand code lists differ by exactly ``F05000`` (#575).

``F05000`` (imports) is a MUT-only column. It does not exist in the SUT Use
table: imports enter on the *Supply* side as ``MCIF`` + ``MADJ``, and ``F05000``
is derived during the SUT to MUT conversion at Step 6b rather than sourced.

This matters beyond tidiness. The identity Step 5 balances against is *total
supply at purchaser = total use at purchaser, per commodity*. A phantom imports
column in the Use table breaks that identity in a way that reads as a data
problem somewhere else entirely.
"""

import inspect

from bedrock.transform.eeio import nowcast
from bedrock.utils.taxonomy.bea.v2017_final_demand import (
    SUT_FINAL_DEMAND_CODES,
    USA_2017_FINAL_DEMAND_CODES,
)


def test_sut_and_mut_lists_differ_by_exactly_f05000() -> None:
    mut, sut = set(USA_2017_FINAL_DEMAND_CODES), set(SUT_FINAL_DEMAND_CODES)
    assert mut - sut == {'F05000'}
    assert sut - mut == set()


def test_f05000_is_absent_from_the_sut_list() -> None:
    assert 'F05000' not in SUT_FINAL_DEMAND_CODES
    assert 'F05000' in USA_2017_FINAL_DEMAND_CODES


def test_sut_list_preserves_mut_order() -> None:
    """Column order is load-bearing for the golden-file comparisons."""
    assert list(SUT_FINAL_DEMAND_CODES) == [
        c for c in USA_2017_FINAL_DEMAND_CODES if c != 'F05000'
    ]


def test_derive_initial_y_pur_targets_the_sut_list() -> None:
    """``derive_initial_Y_pur`` reindexes to the SUT list, not the MUT one.

    Asserted by source inspection rather than by building the frame: a real
    call generates the full FBS plus both Trade methods, which is far too slow
    for a unit test. The behavioural check that the output carries 19 columns
    and no ``F05000`` belongs with the nowcast validation runs (#576).
    """

    src = inspect.getsource(nowcast.derive_initial_Y_pur)
    assert 'SUT_FINAL_DEMAND_CODES' in src
    assert 'USA_2017_FINAL_DEMAND_CODES' not in src
