"""2017 before/after redefinitions census and overlay writer.

Classifies Make off-diagonals, recovers named recipes, and reports the Use
intermediate magnitudes. ``--overlay`` writes the residual overlay after the
named reallocation rules. ``--check`` exits non-zero if a magnitude or Make
leftover bound is missed.

Run from the repo root::

    uv run python -m bedrock.analysis.nowcasting.redefinitions_2017
    uv run python -m bedrock.analysis.nowcasting.redefinitions_2017 --check
    uv run python -m bedrock.analysis.nowcasting.redefinitions_2017 --overlay
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd

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
from bedrock.transform.iot.nowcast_redefinitions import (
    ATOL,
    DEFAULT_ONLY,
    RecipeKey,
    RedefinitionOverlay,
    RedefinitionPair,
    apply_redefinitions,
    classify_redefinitions,
    compute_redefinition_overlay,
    log_recipe_integrity,
    write_classification,
    write_overlay,
    write_recipes,
)
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY

logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).parent / 'output'
LEFTOVER_CSV_PATH = OUTPUT_DIR / 'redefinitions_2017_leftover_cells.csv'
DEFAULT_RESIDUAL_CSV_PATH = OUTPUT_DIR / 'redefinitions_2017_default_residual.csv'
MAKE_LEFTOVER_CAP = 11 * MILLION_CURRENCY_TO_CURRENCY

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


def make_column_leftover(V_before: pd.DataFrame, V_after: pd.DataFrame) -> pd.Series:
    """Per-commodity ``|q_before - q_after|`` in USD."""
    return (V_before.sum(axis=0) - V_after.sum(axis=0)).abs()


def write_default_residual(
    U_before: pd.DataFrame,
    U_after_pub: pd.DataFrame,
    VA_before: pd.DataFrame,
    VA_after_pub: pd.DataFrame,
    Uimp_before: pd.DataFrame,
    Uimp_after_pub: pd.DataFrame,
    margins_before: pd.DataFrame,
    margins_after_pub: pd.DataFrame,
    V_before: pd.DataFrame,
    classification: list[RedefinitionPair],
    recipes: dict[RecipeKey, pd.Series],
    path: Path = DEFAULT_RESIDUAL_CSV_PATH,
) -> Path:
    """Write the DEFAULT_ONLY residual CSV (not git-tracked)."""
    _, U_alg, VA_alg, Uimp_alg, margins_alg = apply_redefinitions(
        V_before,
        U_before,
        VA_before,
        Uimp_before,
        margins_before,
        classification=classification,
        recipes=recipes,
        overlay=None,
        rules=DEFAULT_ONLY,
    )
    rows: list[dict[str, str | float]] = []

    def add_frame(name: str, published: pd.DataFrame, algorithm: pd.DataFrame) -> None:
        left, right = published.align(algorithm, fill_value=0.0)
        residual = left.astype(float) - right.astype(float)
        stacked = residual.stack(future_stack=True)
        for key, value in stacked.items():
            if abs(float(value)) <= ATOL:
                continue
            if isinstance(key, tuple) and len(key) == 2:
                row, column = str(key[0]), str(key[1])
            else:
                row, column = str(key), ''
            rows.append(
                {
                    'table': name,
                    'row': row,
                    'column': column,
                    'published_after': (
                        float(
                            np.asarray(left.loc[row, column], dtype=float).reshape(-1)[
                                0
                            ]
                        )
                        if column
                        else float(
                            np.asarray(left.loc[row], dtype=float).reshape(-1)[0]
                        )
                    ),
                    'algorithm': (
                        float(
                            np.asarray(right.loc[row, column], dtype=float).reshape(-1)[
                                0
                            ]
                        )
                        if column
                        else float(
                            np.asarray(right.loc[row], dtype=float).reshape(-1)[0]
                        )
                    ),
                    'residual': float(np.asarray(value, dtype=float).reshape(-1)[0]),
                }
            )

    add_frame('U', U_after_pub, U_alg)
    add_frame('VA', VA_after_pub, VA_alg)
    add_frame('Uimp', Uimp_after_pub, Uimp_alg)
    left_m, right_m = margins_after_pub.align(margins_alg, fill_value=0.0)
    residual_m = left_m.astype(float) - right_m.astype(float)
    for idx in residual_m.index:
        for col in residual_m.columns:
            value = float(residual_m.loc[idx, col])
            if abs(value) <= ATOL:
                continue
            industry, commodity = idx
            rows.append(
                {
                    'table': 'margins',
                    'row': f'{industry}|{commodity}',
                    'column': str(col),
                    'published_after': float(left_m.loc[idx, col]),
                    'algorithm': float(right_m.loc[idx, col]),
                    'residual': value,
                }
            )
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def write_leftover_cells(
    classification: list[RedefinitionPair],
    overlay: RedefinitionOverlay,
    U_before: pd.DataFrame,
    U_after_pub: pd.DataFrame,
    VA_before: pd.DataFrame,
    VA_after_pub: pd.DataFrame,
    path: Path = LEFTOVER_CSV_PATH,
) -> Path:
    """Rows where published Use/VA delta is not explained by default/C1–C4."""

    def _cell(frame: pd.DataFrame, row: str, column: str) -> float:
        if row in frame.index and column in frame.columns:
            return float(np.asarray(frame.loc[row, column], dtype=float).reshape(-1)[0])
        return 0.0

    rows: list[dict[str, str | float]] = []
    for frame, before, published, label in (
        (overlay.U, U_before, U_after_pub, 'U'),
        (overlay.VA, VA_before, VA_after_pub, 'VA'),
    ):
        stacked = frame.stack(future_stack=True)
        for key, value in stacked.items():
            if not isinstance(key, tuple) or len(key) != 2:
                continue
            row_code, industry = str(key[0]), str(key[1])
            residual = float(np.asarray(value, dtype=float).reshape(-1)[0])
            if abs(residual) <= ATOL:
                continue
            published_delta = _cell(published, row_code, industry) - _cell(
                before, row_code, industry
            )
            match = next(
                (
                    p
                    for p in classification
                    if p.source_industry == industry
                    or p.destination_industry == industry
                ),
                None,
            )
            source = match.source_industry if match else ''
            dest = match.destination_industry if match else ''
            commodity = match.commodity if match else ''
            rows.append(
                {
                    'source_industry': source,
                    'destination_industry': dest,
                    'commodity': commodity,
                    'row_code': str(row_code),
                    'published_delta': published_delta,
                    'explained_by_C1_C4': published_delta - residual,
                    'residual': residual,
                    'table': label,
                }
            )
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def run_census() -> tuple[list[RedefinitionPair], dict[RecipeKey, pd.Series]]:
    """Classify 2017 pairs and write classification / recipes CSVs."""
    V_before = load_2017_V_before_redef_usa()
    V_after = load_2017_V_after_redef_usa()
    U_before = load_2017_Utot_before_redef_usa()
    U_after = load_2017_Utot_after_redef_usa()
    VA_before = load_2017_value_added_before_redef_usa()
    VA_after = load_2017_value_added_usa()
    pairs, recipes = classify_redefinitions(
        V_before, V_after, U_before, U_after, VA_before, VA_after
    )
    write_classification(pairs)
    write_recipes(recipes)
    log_recipe_integrity(pairs, recipes, V_before)
    return pairs, recipes


def _print_magnitudes(magnitudes: dict[str, float]) -> None:
    print(
        f'intermediate cells that differ: '
        f'{int(magnitudes["cells_that_differ"])} of 161604'
    )
    print(f'gross movement: {magnitudes["gross_movement_million"]:,.0f} million')
    print(f'largest single cell shift: {magnitudes["largest_cell_million"]:,.0f}')
    print(f'net: {magnitudes["net_million"]:,.0f}')


def main(check: bool = False, overlay: bool = False) -> int:
    """Print the 2017 census; with *check*, fail if a magnitude bound is missed."""
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    V_before = load_2017_V_before_redef_usa()
    V_after = load_2017_V_after_redef_usa()
    U_before = load_2017_Utot_before_redef_usa()
    U_after = load_2017_Utot_after_redef_usa()
    VA_before = load_2017_value_added_before_redef_usa()
    VA_after = load_2017_value_added_usa()
    Uimp_before = load_2017_Uimp_before_redef_usa()
    Uimp_after = load_2017_Uimp_after_redef_usa()
    margins_before = load_2017_margins_before_redef_usa()
    margins_after = load_2017_margins_after_redef_usa()

    pairs, recipes = classify_redefinitions(
        V_before, V_after, U_before, U_after, VA_before, VA_after
    )
    write_classification(pairs)
    write_recipes(recipes)
    log_recipe_integrity(pairs, recipes, V_before)
    print(f'classified {len(pairs)} Make redefinition pairs; wrote recipes')

    magnitudes = use_intermediate_magnitudes(U_before, U_after)
    _print_magnitudes(magnitudes)
    leftover = make_column_leftover(V_before, V_after)
    print(
        f'Make column-sum leftover max: '
        f'{float(leftover.max()) / MILLION_CURRENCY_TO_CURRENCY:,.1f} million'
    )

    if overlay:
        computed = compute_redefinition_overlay(
            V_before,
            U_before,
            VA_before,
            Uimp_before,
            margins_before,
            classification=pairs,
            recipes=recipes,
            U_published_after=U_after,
            VA_published_after=VA_after,
            Uimp_published_after=Uimp_after,
            margins_published_after=margins_after,
        )
        write_overlay(computed)
        print('wrote overlay parquets')
        write_leftover_cells(
            pairs,
            computed,
            U_before,
            U_after,
            VA_before,
            VA_after,
        )
        write_default_residual(
            U_before,
            U_after,
            VA_before,
            VA_after,
            Uimp_before,
            Uimp_after,
            margins_before,
            margins_after,
            V_before,
            pairs,
            recipes,
        )

    if not check:
        return 0

    failures: list[str] = []
    if int(magnitudes['cells_that_differ']) != EXPECTED_CELLS_THAT_DIFFER:
        failures.append(
            f'cells that differ {int(magnitudes["cells_that_differ"])}, '
            f'expected {EXPECTED_CELLS_THAT_DIFFER}'
        )
    if round(magnitudes['gross_movement_million']) != EXPECTED_GROSS_MOVEMENT_MILLION:
        failures.append(
            f'gross {magnitudes["gross_movement_million"]:.0f}, '
            f'expected {EXPECTED_GROSS_MOVEMENT_MILLION}'
        )
    if round(magnitudes['largest_cell_million']) != EXPECTED_LARGEST_CELL_MILLION:
        failures.append(
            f'largest {magnitudes["largest_cell_million"]:.0f}, '
            f'expected {EXPECTED_LARGEST_CELL_MILLION}'
        )
    if round(magnitudes['net_million']) != EXPECTED_NET_MILLION:
        failures.append(
            f'net {magnitudes["net_million"]:.0f}, expected {EXPECTED_NET_MILLION}'
        )
    if float(leftover.max()) > MAKE_LEFTOVER_CAP:
        failures.append(
            f'Make leftover {float(leftover.max())} exceeds {MAKE_LEFTOVER_CAP}'
        )
    if failures:
        for failure in failures:
            print(f'FAIL: {failure}')
        return 1
    print('OK: magnitude and Make leftover bounds hold.')
    return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--check',
        action='store_true',
        help='exit non-zero if a 2017 magnitude or Make leftover bound is missed',
    )
    parser.add_argument(
        '--overlay',
        action='store_true',
        help='write the residual overlay parquets from the named-rule algorithm',
    )
    raise SystemExit(main(**vars(parser.parse_args())))
