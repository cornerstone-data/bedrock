"""CLI: compute and write 2017 redefinition ratio artifacts.

Usage:
    python -m bedrock.analysis.nowcasting.redefinition_ratios_2017
    python -m bedrock.analysis.nowcasting.redefinition_ratios_2017 --check
"""

from __future__ import annotations

import argparse
import sys

import pandas as pd

from bedrock.analysis.nowcasting.table_match import Tolerance, compare_tables
from bedrock.extract.iot.io_2017 import (
    load_2017_margins_after_redef_usa,
    load_2017_margins_before_redef_usa,
    load_2017_Uimp_after_redef_usa,
    load_2017_Uimp_before_redef_usa,
    load_2017_Utot_after_redef_usa,
    load_2017_Utot_before_redef_usa,
    load_2017_V_after_redef_usa,
    load_2017_V_before_redef_usa,
    load_2017_value_added_before_redef_usa,
    load_2017_value_added_usa,
)
from bedrock.transform.iot.nowcast_redefinition_ratios import (
    ATOL,
    apply_redefinition_ratios,
    compute_redefinition_ratios,
    load_redefinition_ratios,
    write_redefinition_ratios,
)
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY

EXPECTED_CELLS_THAT_DIFFER = 5740
EXPECTED_GROSS_MOVEMENT_MILLION = 553_635
EXPECTED_LARGEST_CELL_MILLION = 42_893
EXPECTED_NET_MILLION = -7


def use_intermediate_magnitudes(
    U_before: pd.DataFrame, U_after: pd.DataFrame
) -> dict[str, float]:
    """Published before→after Use-intermediate movement, in million USD."""
    delta = U_after - U_before
    scale = MILLION_CURRENCY_TO_CURRENCY
    return {
        'cells_that_differ': float((delta.abs() > ATOL).sum().sum()),
        'gross_movement_million': float(delta.abs().sum().sum() / scale),
        'largest_cell_million': float(delta.abs().max().max() / scale),
        'net_million': float(delta.sum().sum() / scale),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description='Compute 2017 redefinition ratio artifacts.'
    )
    parser.add_argument(
        '--check',
        action='store_true',
        help='After writing, reload ratios and assert 2017 round-trip.',
    )
    args = parser.parse_args(argv)

    V_b = load_2017_V_before_redef_usa()
    U_b = load_2017_Utot_before_redef_usa()
    VA_b = load_2017_value_added_before_redef_usa()
    Uimp_b = load_2017_Uimp_before_redef_usa()
    M_b = load_2017_margins_before_redef_usa()

    V_a = load_2017_V_after_redef_usa()
    U_a = load_2017_Utot_after_redef_usa()
    VA_a = load_2017_value_added_usa()
    Uimp_a = load_2017_Uimp_after_redef_usa()
    M_a = load_2017_margins_after_redef_usa()

    ratios = compute_redefinition_ratios(
        V_b, U_b, VA_b, Uimp_b, M_b, V_a, U_a, VA_a, Uimp_a, M_a
    )
    write_redefinition_ratios(ratios)
    print(
        f'Wrote ratios: V={len(ratios.V)} U={len(ratios.U)} '
        f'VA={len(ratios.VA)} Uimp={len(ratios.Uimp)} '
        f'margins={len(ratios.margins)}'
    )

    if not args.check:
        return 0

    mags = use_intermediate_magnitudes(U_b, U_a)
    checks = (
        (
            'cells_that_differ',
            mags['cells_that_differ'],
            float(EXPECTED_CELLS_THAT_DIFFER),
            0.0,
        ),
        (
            'gross_movement_million',
            mags['gross_movement_million'],
            float(EXPECTED_GROSS_MOVEMENT_MILLION),
            0.5,
        ),
        (
            'largest_cell_million',
            mags['largest_cell_million'],
            float(EXPECTED_LARGEST_CELL_MILLION),
            0.5,
        ),
        (
            'net_million',
            mags['net_million'],
            float(EXPECTED_NET_MILLION),
            0.5,
        ),
    )
    for name, got, expected, tol in checks:
        if abs(got - expected) > tol:
            print(f'Use magnitude mismatch on {name}: got {got}, expected {expected}')
            return 1

    loaded = load_redefinition_ratios()
    V_hat, U_hat, VA_hat, Uimp_hat, M_hat = apply_redefinition_ratios(
        V_b, U_b, VA_b, Uimp_b, M_b, ratios=loaded
    )
    table_tol = Tolerance(atol=ATOL, rtol=0.0)
    for table_name, candidate, reference in (
        ('V', V_hat, V_a),
        ('U', U_hat, U_a),
        ('VA', VA_hat, VA_a),
        ('Uimp', Uimp_hat, Uimp_a),
        ('margins', M_hat, M_a),
    ):
        try:
            compare_tables(candidate, reference, tolerance=table_tol).assert_ok(
                max_partial=0,
                max_miss=0,
                max_extra=0,
                max_margin_partial=0,
            )
        except AssertionError as exc:
            print(f'{table_name} round-trip failed:\n{exc}')
            return 1
    print('2017 round-trip OK for V, U, VA, Uimp, margins.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
