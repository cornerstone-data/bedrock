"""Unit tests for before→after redefinitions (issue #572 / Step 7).

Synthetic tables unless marked ``eeio_integration`` (those load the published
2017 MUT from GCS). Amounts are in USD and sit above ``ATOL`` ($0.5M).
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from bedrock.analysis.nowcasting.sections import SECTIONS
from bedrock.transform.iot.nowcast_redefinitions import (
    ATOL,
    CLASSIFICATION_PATH,
    DEFAULT_ONLY,
    FULL,
    OVERLAY_U_PATH,
    RECIPES_PATH,
    RecipeKey,
    RedefinitionPair,
    apply_redefinitions,
    assign_named_reallocation_rules,
    classify_make_pairs,
    classify_redefinitions,
    compute_redefinition_overlay,
    empty_recipe,
    load_classification,
    load_overlay,
    load_recipes,
    moved_amount,
    recover_named_reallocation_recipes,
    recover_own_account_software_recipe,
    write_classification,
    write_recipes,
)
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY as M

MARGINS_COLS = [
    "Producers' Value",
    'Transportation',
    'Wholesale',
    'Retail',
    "Purchasers' Value",
]


def _margins(*keys: tuple[str, str]) -> pd.DataFrame:
    index = pd.MultiIndex.from_tuples(keys, names=['Industry Code', 'Commodity Code'])
    return pd.DataFrame(0.0, index=index, columns=MARGINS_COLS)


def _va(
    industries: list[str], values: dict[str, dict[str, float]] | None = None
) -> pd.DataFrame:
    frame = pd.DataFrame(0.0, index=['V00100', 'V00200', 'V00300'], columns=industries)
    if values:
        for industry, rows in values.items():
            for code, amount in rows.items():
                frame.loc[code, industry] = amount
    return frame


def test_chapter_9_toy_tables() -> None:
    """Manual Tables 9.1–9.4: $10M of B produced by A is redefined to B."""
    V_before = pd.DataFrame(
        {'A': [90.0 * M, 0.0], 'B': [10.0 * M, 100.0 * M]}, index=['A', 'B']
    )
    U_before = pd.DataFrame(
        {'A': [52.0 * M, 3.0 * M], 'B': [20.0 * M, 30.0 * M]}, index=['A', 'B']
    )
    VA_before = _va(['A', 'B'], {'A': {'V00100': 45.0 * M}, 'B': {'V00100': 50.0 * M}})
    Uimp_before = U_before * 0.0
    margins_before = _margins(('A', 'A'), ('A', 'B'), ('B', 'A'), ('B', 'B'))
    pair = RedefinitionPair(
        source_industry='A',
        commodity='B',
        destination_industry='B',
        share=1.0,
        delta=10.0 * M,
        rule_id='default',
    )
    V_after, U_after, VA_after, _, _ = apply_redefinitions(
        V_before,
        U_before,
        VA_before,
        Uimp_before,
        margins_before,
        classification=[pair],
        recipes={},
        overlay=None,
        rules=DEFAULT_ONLY,
    )
    pd.testing.assert_frame_equal(
        V_after,
        pd.DataFrame({'A': [90.0 * M, 0.0], 'B': [0.0, 110.0 * M]}, index=['A', 'B']),
    )
    pd.testing.assert_frame_equal(
        U_after,
        pd.DataFrame(
            {'A': [50.0 * M, 0.0], 'B': [22.0 * M, 33.0 * M]}, index=['A', 'B']
        ),
    )
    assert VA_after.loc['V00100', 'A'] == pytest.approx(40.0 * M)
    assert VA_after.loc['V00100', 'B'] == pytest.approx(55.0 * M)


def test_classification_csv_round_trip(tmp_path: Path) -> None:
    pairs = [
        RedefinitionPair('1111A0', '511200', '511200', 0.4, 12.0 * M, 'C2', None),
        RedefinitionPair('331110', '423100', '423100', 1.0, 5.0 * M, 'C1', 'dest'),
    ]
    path = tmp_path / 'classification.csv'
    write_classification(pairs, path)
    loaded = load_classification(path)
    assert loaded[0].rule_id == 'C2'
    assert loaded[0].va_mix is None
    assert loaded[1].va_mix == 'dest'
    assert loaded[1].source_industry == '331110'


def test_recipes_csv_round_trip(tmp_path: Path) -> None:
    c2 = empty_recipe()
    c2.loc['V00100'] = 0.4
    c2.loc['V00300'] = 0.2
    c2.loc['221100'] = 0.4
    c3 = empty_recipe()
    c3.loc['1111A0'] = 1.0
    recipes: dict[RecipeKey, pd.Series] = {'C2': c2, ('721000', '713200'): c3}
    path = tmp_path / 'recipes.csv'
    write_recipes(recipes, path)
    loaded = load_recipes(path)
    assert loaded['C2'].loc['V00100'] == pytest.approx(0.4)
    assert loaded[('721000', '713200')].loc['1111A0'] == pytest.approx(1.0)
    assert loaded['C2'].loc['511200'] == pytest.approx(0.0)


def test_wholesale_margin_reallocation() -> None:
    industries = ['331110', '423100']
    commodities = ['331110', '423100']
    V_before = pd.DataFrame(0.0, index=industries, columns=commodities)
    V_before.loc['331110', '331110'] = 100.0 * M
    V_before.loc['331110', '423100'] = 10.0 * M
    V_before.loc['423100', '423100'] = 50.0 * M
    U_before = pd.DataFrame(0.0, index=commodities, columns=industries)
    U_before.loc['331110', '331110'] = 40.0 * M
    VA_before = _va(
        industries,
        {
            '331110': {'V00100': 30.0 * M, 'V00300': 30.0 * M},
            '423100': {'V00100': 20.0 * M, 'V00300': 30.0 * M},
        },
    )
    pair = RedefinitionPair('331110', '423100', '423100', 1.0, 10.0 * M, 'C1', 'dest')
    _, U_after, VA_after, _, _ = apply_redefinitions(
        V_before,
        U_before,
        VA_before,
        U_before * 0.0,
        _margins(('331110', '331110'), ('423100', '331110')),
        classification=[pair],
        recipes={},
        overlay=None,
        rules=FULL - {'C6'},
    )
    assert U_after.loc['331110', '331110'] == pytest.approx(40.0 * M)
    w1 = 20.0 / 50.0
    assert VA_after.loc['V00100', '331110'] == pytest.approx(30.0 * M - 10.0 * M * w1)
    assert VA_after.loc['V00300', '331110'] == pytest.approx(
        30.0 * M - 10.0 * M * (1 - w1)
    )
    assert VA_after.loc['V00100', '423100'] == pytest.approx(20.0 * M + 10.0 * M * w1)


def test_own_account_software_recipe_and_apply() -> None:
    industries = ['1111A0', '511200']
    commodities = ['1111A0', '511200', '221100']
    V_before = pd.DataFrame(0.0, index=industries, columns=commodities)
    V_before.loc['1111A0', '1111A0'] = 100.0 * M
    V_before.loc['1111A0', '511200'] = 10.0 * M
    V_before.loc['511200', '511200'] = 80.0 * M
    U_before = pd.DataFrame(0.0, index=commodities, columns=industries)
    U_before.loc['221100', '1111A0'] = 5.0 * M
    U_before.loc['1111A0', '1111A0'] = 4.0 * M
    U_before.loc['511200', '1111A0'] = 3.0 * M
    VA_before = _va(
        industries,
        {
            '1111A0': {'V00100': 40.0 * M, 'V00300': 45.0 * M},
            '511200': {'V00100': 50.0 * M, 'V00300': 30.0 * M},
        },
    )
    U_after = U_before.copy()
    VA_after = VA_before.copy()
    VA_after.loc['V00100', '511200'] = 54.0 * M
    VA_after.loc['V00300', '511200'] = 32.0 * M
    U_after.loc['221100', '511200'] = 1.5 * M
    U_after.loc['1111A0', '511200'] = 1.5 * M
    U_after.loc['511200', '511200'] = 1.0 * M
    VA_after.loc['V00100', '1111A0'] = 36.0 * M
    VA_after.loc['V00300', '1111A0'] = 43.0 * M
    U_after.loc['221100', '1111A0'] = 3.5 * M
    U_after.loc['1111A0', '1111A0'] = 2.5 * M
    U_after.loc['511200', '1111A0'] = 2.0 * M
    pair = RedefinitionPair('1111A0', '511200', '511200', 1.0, 10.0 * M, 'default')
    recipe = recover_own_account_software_recipe(
        V_before, U_before, U_after, VA_before, VA_after, [pair]
    )
    assert recipe is not None
    assert recipe.loc['V00100'] == pytest.approx(0.4)
    assert recipe.loc['V00300'] == pytest.approx(0.2)
    assert recipe.loc['221100'] == pytest.approx(0.15)
    labeled = RedefinitionPair('1111A0', '511200', '511200', 1.0, 10.0 * M, 'C2')
    _, U2, VA2, _, _ = apply_redefinitions(
        V_before,
        U_before,
        VA_before,
        U_before * 0.0,
        _margins(('1111A0', '221100'), ('511200', '221100')),
        classification=[labeled],
        recipes={'C2': recipe},
        overlay=None,
        rules=FULL - {'C6'},
    )
    assert U2.loc['221100', '511200'] == pytest.approx(1.5 * M)
    assert VA2.loc['V00100', '511200'] == pytest.approx(54.0 * M)


def test_missing_c2_recipe_raises() -> None:
    V = pd.DataFrame({'511200': [10.0 * M, 80.0 * M]}, index=['1111A0', '511200'])
    U = pd.DataFrame({'1111A0': [0.0], '511200': [0.0]}, index=['511200'])
    VA = _va(['1111A0', '511200'])
    pair = RedefinitionPair('1111A0', '511200', '511200', 1.0, 10.0 * M, 'C2')
    with pytest.raises(ValueError, match='C2 recipe missing'):
        apply_redefinitions(
            V,
            U,
            VA,
            U * 0.0,
            _margins(('1111A0', '511200')),
            classification=[pair],
            recipes={},
            overlay=None,
            rules=FULL - {'C6'},
        )


def test_named_reallocation_shared_dest_unique_source() -> None:
    """Two own-account construction sources into dest 233240; recover from each source."""
    sources = ['221100', '517110', '233240']
    commodities = ['221100', '517110', '233240']
    V_before = pd.DataFrame(0.0, index=sources, columns=commodities)
    V_before.loc['221100', '221100'] = 200.0 * M
    V_before.loc['221100', '233240'] = 20.0 * M
    V_before.loc['517110', '517110'] = 150.0 * M
    V_before.loc['517110', '233240'] = 10.0 * M
    V_before.loc['233240', '233240'] = 400.0 * M
    U_before = pd.DataFrame(0.0, index=commodities, columns=sources)
    U_before.loc['221100', '221100'] = 50.0 * M
    U_before.loc['517110', '517110'] = 40.0 * M
    U_before.loc['233240', '233240'] = 80.0 * M
    VA_before = _va(
        sources,
        {
            '221100': {'V00100': 150.0 * M},
            '517110': {'V00100': 110.0 * M},
            '233240': {'V00100': 320.0 * M},
        },
    )
    U_after = U_before.copy()
    VA_after = VA_before.copy()
    VA_after.loc['V00100', '221100'] = 130.0 * M
    VA_after.loc['V00100', '517110'] = 100.0 * M
    VA_after.loc['V00100', '233240'] = 350.0 * M
    pairs = [
        RedefinitionPair('221100', '233240', '233240', 1.0, 20.0 * M, 'C3'),
        RedefinitionPair('517110', '233240', '233240', 1.0, 10.0 * M, 'C3'),
    ]
    recovered = recover_named_reallocation_recipes(
        V_before, U_before, U_after, VA_before, VA_after, pairs, {}
    )
    rec_elec = recovered[('221100', '233240')]
    rec_tel = recovered[('517110', '233240')]
    assert rec_elec.loc['V00100'] == pytest.approx(1.0)
    assert rec_tel.loc['V00100'] == pytest.approx(1.0)
    _, _, VA2, _, _ = apply_redefinitions(
        V_before,
        U_before,
        VA_before,
        U_before * 0.0,
        _margins(('221100', '233240'), ('517110', '233240'), ('233240', '233240')),
        classification=pairs,
        recipes=recovered,
        overlay=None,
        rules=FULL - {'C6'},
    )
    assert VA2.loc['V00100', '221100'] == pytest.approx(130.0 * M)
    assert VA2.loc['V00100', '517110'] == pytest.approx(100.0 * M)


def test_named_reallocation_unnormalized_shares_are_applied() -> None:
    """Identity fallback stores raw shares; apply must not renormalize them."""
    V = pd.DataFrame({'713200': [10.0 * M, 20.0 * M]}, index=['721000', '713200'])
    U = pd.DataFrame({'721000': [5.0 * M], '713200': [0.0]}, index=['713200'])
    VA = _va(
        ['721000', '713200'],
        {'713200': {'V00100': 20.0 * M}, '721000': {'V00100': 15.0 * M}},
    )
    pair = RedefinitionPair('721000', '713200', '713200', 1.0, 10.0 * M, 'C3')
    recipe = empty_recipe()
    recipe.loc['V00100'] = 0.5
    _, _, VA_after, _, _ = apply_redefinitions(
        V,
        U,
        VA,
        U * 0.0,
        _margins(('721000', '713200')),
        classification=[pair],
        recipes={('721000', '713200'): recipe},
        overlay=None,
        rules=FULL - {'C6'},
    )
    assert VA_after.loc['V00100', '721000'] == pytest.approx(10.0 * M)
    assert VA_after.loc['V00100', '713200'] == pytest.approx(25.0 * M)


def test_named_reallocation_missing_recipe_raises() -> None:
    V = pd.DataFrame({'713200': [5.0 * M, 20.0 * M]}, index=['721000', '713200'])
    U = pd.DataFrame({'721000': [0.0], '713200': [0.0]}, index=['713200'])
    VA = _va(
        ['721000', '713200'],
        {'713200': {'V00100': 20.0 * M}, '721000': {'V00100': 5.0 * M}},
    )
    pair = RedefinitionPair('721000', '713200', '713200', 1.0, 5.0 * M, 'C3')
    with pytest.raises(ValueError, match='C3 recipe missing'):
        apply_redefinitions(
            V,
            U,
            VA,
            U * 0.0,
            _margins(('721000', '713200')),
            classification=[pair],
            recipes={},
            overlay=None,
            rules=FULL - {'C6'},
        )


def test_negative_input_repair_replaces_dest_b() -> None:
    industries = ['A', 'B']
    commodities = ['X', 'Y']
    V_before = pd.DataFrame(
        {'X': [90.0 * M, 0.0], 'Y': [10.0 * M, 100.0 * M]}, index=industries
    )
    U_before = pd.DataFrame(
        {'A': [1.0 * M, 0.0], 'B': [50.0 * M, 0.0]}, index=commodities
    )
    VA_before = _va(industries, {'A': {'V00100': 99.0 * M}, 'B': {'V00100': 50.0 * M}})
    pair = RedefinitionPair('A', 'Y', 'B', 1.0, 10.0 * M, 'default')
    _, U_after, VA_after, _, _ = apply_redefinitions(
        V_before,
        U_before,
        VA_before,
        U_before * 0.0,
        _margins(('A', 'X'), ('B', 'X')),
        classification=[pair],
        recipes={},
        overlay=None,
        rules=frozenset({'default', 'C4'}),
    )
    take_x = 1.0 * M + ATOL
    assert float(np.asarray(U_after.loc['X', 'A'])) >= -ATOL
    assert U_after.loc['X', 'A'] == pytest.approx(1.0 * M - take_x)
    assert U_after.loc['X', 'B'] == pytest.approx(50.0 * M + take_x)
    assert float(np.asarray(VA_after.loc['V00100', 'B'])) > 50.0 * M


def test_pair_order_is_abs_R_then_codes() -> None:
    """Larger |R| is applied first, so the second pair sees the drained source."""
    industries = ['A', 'B', 'C']
    V_before = pd.DataFrame(0.0, index=industries, columns=['B', 'C'])
    V_before.loc['A', 'B'] = 3.0 * M
    V_before.loc['A', 'C'] = 10.0 * M
    V_before.loc['B', 'B'] = 50.0 * M
    V_before.loc['C', 'C'] = 50.0 * M
    U_before = pd.DataFrame(0.0, index=['X'], columns=industries)
    U_before.loc['X', 'A'] = 12.0 * M
    U_before.loc['X', 'B'] = 100.0 * M
    U_before.loc['X', 'C'] = 100.0 * M
    VA_before = _va(industries)
    pairs = [
        RedefinitionPair('A', 'B', 'B', 1.0, 3.0 * M, 'default'),
        RedefinitionPair('A', 'C', 'C', 1.0, 10.0 * M, 'default'),
    ]
    _, U_after, _, _, _ = apply_redefinitions(
        V_before,
        U_before,
        VA_before,
        U_before * 0.0,
        _margins(('A', 'X'), ('B', 'X'), ('C', 'X')),
        classification=pairs,
        recipes={},
        overlay=None,
        rules=DEFAULT_ONLY,
    )
    # C (|R|=10M) first: source X 12→2. B (|R|=3M) would go negative and is skipped.
    assert U_after.loc['X', 'A'] == pytest.approx(2.0 * M)
    assert U_after.loc['X', 'C'] == pytest.approx(110.0 * M)
    assert U_after.loc['X', 'B'] == pytest.approx(100.0 * M)


def test_default_only_falls_through_to_dest_b() -> None:
    industries = ['331110', '423100']
    commodities = ['331110', '423100']
    V_before = pd.DataFrame(0.0, index=industries, columns=commodities)
    V_before.loc['331110', '331110'] = 100.0 * M
    V_before.loc['331110', '423100'] = 10.0 * M
    V_before.loc['423100', '423100'] = 50.0 * M
    U_before = pd.DataFrame(0.0, index=commodities, columns=industries)
    U_before.loc['331110', '331110'] = 90.0 * M
    U_before.loc['331110', '423100'] = 20.0 * M
    VA_before = _va(
        industries,
        {
            '331110': {'V00100': 90.0 * M},
            '423100': {'V00100': 30.0 * M},
        },
    )
    pair = RedefinitionPair('331110', '423100', '423100', 1.0, 10.0 * M, 'C1', 'dest')
    _, U_after, _, _, _ = apply_redefinitions(
        V_before,
        U_before,
        VA_before,
        U_before * 0.0,
        _margins(('331110', '331110'), ('423100', '331110')),
        classification=[pair],
        recipes={},
        overlay=None,
        rules=DEFAULT_ONLY,
    )
    assert float(np.asarray(U_after.loc['331110', '423100'])) > 20.0 * M


def test_overlay_none_with_c6_raises() -> None:
    V = pd.DataFrame({'A': [1.0 * M]}, index=['A'])
    U = pd.DataFrame({'A': [1.0 * M]}, index=['A'])
    VA = _va(['A'], {'A': {'V00100': 0.0}})
    with pytest.raises(ValueError, match='overlay is required'):
        apply_redefinitions(
            V,
            U,
            VA,
            U * 0.0,
            _margins(('A', 'A')),
            classification=[],
            recipes={},
            overlay=None,
            rules=FULL,
        )


def test_overlay_union_align_and_identity() -> None:
    industries = ['A', 'B']
    commodities = ['A', 'B']
    V_before = pd.DataFrame(
        {'A': [90.0 * M, 0.0], 'B': [10.0 * M, 100.0 * M]}, index=industries
    )
    U_before = pd.DataFrame(
        {'A': [50.0 * M, 0.0], 'B': [20.0 * M, 30.0 * M]}, index=commodities
    )
    VA_before = _va(industries, {'A': {'V00100': 50.0 * M}, 'B': {'V00100': 50.0 * M}})
    U_pub = U_before.copy()
    U_pub.loc['A', 'B'] = 25.0 * M
    VA_pub = VA_before.copy()
    pair = RedefinitionPair('A', 'B', 'B', 1.0, 10.0 * M, 'default')
    margins = _margins(('A', 'A'), ('B', 'A'))
    overlay = compute_redefinition_overlay(
        V_before,
        U_before,
        VA_before,
        U_before * 0.0,
        margins,
        classification=[pair],
        recipes={},
        U_published_after=U_pub,
        VA_published_after=VA_pub,
        Uimp_published_after=U_before * 0.0,
        margins_published_after=margins,
    )
    _, U_full, _, _, _ = apply_redefinitions(
        V_before,
        U_before,
        VA_before,
        U_before * 0.0,
        margins,
        classification=[pair],
        recipes={},
        overlay=overlay,
        rules=FULL,
    )
    assert U_full.loc['A', 'B'] == pytest.approx(25.0 * M)


def test_make_pairs_ignore_other_secondary() -> None:
    V_before = pd.DataFrame(
        {'A': [90.0 * M, 0.0], 'B': [10.0 * M, 5.0 * M]}, index=['A', 'B']
    )
    V_after = pd.DataFrame(
        {'A': [90.0 * M, 0.0], 'B': [0.0, 15.0 * M]}, index=['A', 'B']
    )
    pairs = classify_make_pairs(V_before, V_after)
    assert len(pairs) == 1
    assert pairs[0].source_industry == 'A'
    assert pairs[0].commodity == 'B'
    assert pairs[0].destination_industry == 'B'


def test_named_table_labels_c3() -> None:
    pair = RedefinitionPair('721000', '713200', '713200', 1.0, 8.0 * M, 'default')
    V = pd.DataFrame({'713200': [8.0 * M, 20.0 * M]}, index=['721000', '713200'])
    labeled = assign_named_reallocation_rules([pair], V)
    assert labeled[0].rule_id == 'C3'
    assert moved_amount(labeled[0], V) == pytest.approx(8.0 * M)


def test_step7_sections_registered() -> None:
    for name in (
        'make_after_redef_detail_mut',
        'use_after_redef_detail_mut',
        'va_after_redef_detail_mut',
        'uimp_after_redef_detail_mut',
    ):
        assert name in SECTIONS
        assert SECTIONS[name].step == 'Step 7 - after-redef MUT'


def _assert_within_atol(left: pd.DataFrame, right: pd.DataFrame) -> None:
    aligned_left, aligned_right = left.align(right, fill_value=0.0)
    delta = (aligned_left.astype(float) - aligned_right.astype(float)).abs()
    assert float(delta.max().max()) <= ATOL


def _load_2017_mut() -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
]:
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

    return (
        load_2017_V_before_redef_usa(),
        load_2017_V_after_redef_usa(),
        load_2017_Utot_before_redef_usa(),
        load_2017_Utot_after_redef_usa(),
        load_2017_value_added_before_redef_usa(),
        load_2017_value_added_usa(),
        load_2017_Uimp_before_redef_usa(),
        load_2017_Uimp_after_redef_usa(),
        load_2017_margins_before_redef_usa(),
        load_2017_margins_after_redef_usa(),
    )


@pytest.mark.eeio_integration
def test_2017_classified_make_source_cells() -> None:
    (
        V_before,
        V_after,
        U_before,
        U_after,
        VA_before,
        VA_after,
        Uimp_before,
        _,
        margins_before,
        _,
    ) = _load_2017_mut()
    pairs, recipes = classify_redefinitions(
        V_before, V_after, U_before, U_after, VA_before, VA_after
    )
    V_computed, *_ = apply_redefinitions(
        V_before,
        U_before,
        VA_before,
        Uimp_before,
        margins_before,
        classification=pairs,
        recipes=recipes,
        overlay=None,
        rules=DEFAULT_ONLY,
    )
    for pair in pairs:
        R = moved_amount(pair, V_before)
        if abs(R) <= ATOL:
            continue
        i, c = pair.source_industry, pair.commodity
        assert V_computed.loc[i, c] == pytest.approx(V_after.loc[i, c], abs=ATOL)
    leftover = (V_computed.sum(axis=0) - V_after.sum(axis=0)).abs()
    assert float(leftover.max()) <= 11 * M


@pytest.mark.eeio_integration
def test_2017_overlay_matches_fresh_compute() -> None:
    assert CLASSIFICATION_PATH.exists(), 'run redefinitions_2017 --overlay'
    assert OVERLAY_U_PATH.exists(), 'run redefinitions_2017 --overlay'
    (
        V_before,
        _,
        U_before,
        U_after,
        VA_before,
        VA_after,
        Uimp_before,
        Uimp_after,
        margins_before,
        margins_after,
    ) = _load_2017_mut()
    pairs = load_classification()
    recipes = load_recipes()
    fresh = compute_redefinition_overlay(
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
    loaded = load_overlay()
    _assert_within_atol(loaded.U, fresh.U)
    _assert_within_atol(loaded.VA, fresh.VA)
    _assert_within_atol(loaded.Uimp, fresh.Uimp)
    _assert_within_atol(loaded.margins, fresh.margins)


@pytest.mark.eeio_integration
def test_2017_full_matches_published_after() -> None:
    from bedrock.analysis.nowcasting.sections import (
        UIMP_AFTER_REDEF_DETAIL_MUT,
        USE_AFTER_REDEF_DETAIL_MUT,
        VA_AFTER_REDEF_DETAIL_MUT,
        compare_redef_margins_2017,
    )

    assert CLASSIFICATION_PATH.exists()
    assert RECIPES_PATH.exists()
    assert OVERLAY_U_PATH.exists()
    USE_AFTER_REDEF_DETAIL_MUT.run(2017).assert_ok(
        max_partial=0, max_miss=0, max_extra=0, max_margin_partial=0
    )
    VA_AFTER_REDEF_DETAIL_MUT.run(2017).assert_ok(
        max_partial=0, max_miss=0, max_extra=0, max_margin_partial=0
    )
    UIMP_AFTER_REDEF_DETAIL_MUT.run(2017).assert_ok(
        max_partial=0, max_miss=0, max_extra=0, max_margin_partial=0
    )
    compare_redef_margins_2017(2017).assert_ok(
        max_partial=0, max_miss=0, max_extra=0, max_margin_partial=0
    )
