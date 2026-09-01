from __future__ import annotations

import pandas as pd
import pytest

from bedrock.transform.iot.supply_to_make import make_from_sut


def _tax_rows(top: list[float], sub: list[float]) -> pd.DataFrame:
    return pd.DataFrame(
        [top, sub, [-999.0] * len(top)],
        index=['T00TOP', 'T00SUB', 'T00OSUB'],
        columns=['i1', 'i2'][: len(top)],
    )


def test_transposes_and_allocates_net_wedge_over_each_industry_mix() -> None:
    supply = pd.DataFrame(
        [[60.0, 0.0], [40.0, 50.0]],
        index=['c1', 'c2'],
        columns=['i1', 'i2'],
    )
    original = supply.copy()

    make = make_from_sut(supply, _tax_rows([12.0, 5.0], [-2.0, 0.0]), 2020)

    expected = pd.DataFrame(
        [[66.0, 44.0], [0.0, 55.0]],
        index=pd.Index(['i1', 'i2'], name='industry'),
        columns=pd.Index(['c1', 'c2'], name='commodity'),
    )
    pd.testing.assert_frame_equal(make, expected)
    pd.testing.assert_frame_equal(supply, original)


def test_customs_wedge_goes_to_named_zero_output_cell() -> None:
    supply = pd.DataFrame(
        [[10.0, 0.0], [0.0, 0.0]],
        index=['i1', '4200ID'],
        columns=['i1', '4200ID'],
    )
    taxes = pd.DataFrame(
        [[0.0, 38.0], [0.0, 0.0]],
        index=['T00TOP', 'T00SUB'],
        columns=['i1', '4200ID'],
    )

    make = make_from_sut(supply, taxes, 2017)

    assert make.loc['4200ID', '4200ID'] == 38.0
    assert make.loc['4200ID'].sum() == 38.0


def test_production_subsidies_are_not_part_of_the_make_wedge() -> None:
    supply = pd.DataFrame([[10.0]], index=['i1'], columns=['i1'])
    taxes = pd.DataFrame(
        [[2.0], [-1.0], [-100.0]],
        index=['T00TOP', 'T00SUB', 'T00OSUB'],
        columns=['i1'],
    )

    make = make_from_sut(supply, taxes, 2020)

    assert make.loc['i1', 'i1'] == 11.0


def test_rejects_positive_subsidy_sign() -> None:
    supply = pd.DataFrame([[10.0]], index=['i1'], columns=['i1'])
    taxes = pd.DataFrame([[2.0], [1.0]], index=['T00TOP', 'T00SUB'], columns=['i1'])

    with pytest.raises(ValueError, match='must be non-positive'):
        make_from_sut(supply, taxes, 2020)


def test_rejects_noncustoms_wedge_with_no_output() -> None:
    supply = pd.DataFrame([[0.0]], index=['c1'], columns=['i1'])
    taxes = pd.DataFrame([[2.0], [0.0]], index=['T00TOP', 'T00SUB'], columns=['i1'])

    with pytest.raises(ValueError, match='no domestic output'):
        make_from_sut(supply, taxes, 2020)
