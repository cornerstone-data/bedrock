"""Hand-checkable tests for the ndarray GRAS kernel."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from bedrock.utils.economic.balance import (
    SutMask,
    gras_balance,
    restore_fixed,
    split_fixed,
)
from bedrock.utils.economic.balance.gras import (
    STALL_MIN_RELATIVE_GAP_FLOOR,
    _max_relative_error,
    _normalized_residual,
    _stall_min_relative_gap,
)

TOY = np.array(
    [
        [10.0, 20.0, 5.0, 5.0],
        [4.0, 16.0, 8.0, 12.0],
        [2.0, 6.0, 12.0, 10.0],
        [8.0, 8.0, 4.0, 20.0],
    ]
)


def _locally_infeasible_system() -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    seed = np.array(
        [
            [1.0, 0.0, 0.0],
            [1.0, 0.0, 0.0],
            [0.0, 1.0, 1.0],
            [0.0, 1.0, 1.0],
        ]
    )
    row_targets = np.array([4.0, 4.0, 5.0, 5.0])
    col_targets = np.array([6.0, 6.0, 6.0])
    return seed, row_targets, col_targets


def test_gras_balance_toy_dense_matches_identity_margins() -> None:
    row_targets = TOY.sum(axis=1)
    col_targets = TOY.sum(axis=0)
    result = gras_balance(
        matrix=TOY,
        row_targets=row_targets,
        col_targets=col_targets,
        max_iter=500,
        rtol=1e-6,
    )
    assert result.converged
    np.testing.assert_allclose(result.matrix.sum(axis=1), row_targets, rtol=1e-5)
    np.testing.assert_allclose(result.matrix.sum(axis=0), col_targets, rtol=1e-5)


def test_normalized_residual_is_elementwise_not_anchored_to_largest_target() -> None:
    targets = np.array([1e13, 1e8])
    sums = np.array([1e13, 1.5e8])
    assert 5e7 <= 1e-3 * float(np.max(np.abs(targets)))
    assert _normalized_residual(sums, targets, rtol=1e-3, atol=0.0) > 1.0
    assert _max_relative_error(sums, targets, atol=0.0) == pytest.approx(0.5)


def test_normalized_residual_atol_is_a_dollar_floor() -> None:
    targets = np.array([1e9, 0.0])
    sums = np.array([1e9 + 5e5, 5e5])
    assert _normalized_residual(sums, targets, rtol=1e-3, atol=1e6) <= 1.0
    assert not np.isfinite(_normalized_residual(sums, targets, rtol=1e-3, atol=0.0))


def test_normalized_residual_zero_target_met_exactly_scores_zero() -> None:
    residual = _normalized_residual(
        sums=np.array([0.0, 5.0]),
        targets=np.array([0.0, 5.0]),
        rtol=1e-6,
        atol=0.0,
    )
    assert residual == 0.0


def test_gras_balance_relative_tolerance_converges_at_dollar_scale() -> None:
    seed = np.array([[2.0, 1.0, 0.5], [0.5, 3.0, 1.0], [1.0, 0.5, 2.5]]) * 1e9
    row_targets = np.array([4.0, 5.0, 6.0]) * 1e9
    col_targets = np.array([5.0, 5.0, 5.0]) * 1e9
    result = gras_balance(
        matrix=seed,
        row_targets=row_targets,
        col_targets=col_targets,
        max_iter=200,
        rtol=1e-6,
    )
    assert result.converged
    np.testing.assert_allclose(result.matrix.sum(axis=1), row_targets, rtol=1e-5)
    np.testing.assert_allclose(result.matrix.sum(axis=0), col_targets, rtol=1e-5)


def test_gras_balance_inconsistent_margins_do_not_converge() -> None:
    seed = np.array([[1.0, 2.0], [3.0, 4.0]])
    result = gras_balance(
        matrix=seed,
        row_targets=np.array([5.0, 5.0]),
        col_targets=np.array([3.0, 3.0]),
        max_iter=100,
        rtol=1e-6,
    )
    assert not result.converged
    assert result.max_row_err > 0.0 or result.max_col_err > 0.0


def test_negative_cell_survives_a_scale_step() -> None:
    seed = np.array([[1.0, -2.0], [3.0, 4.0]])
    result = gras_balance(
        matrix=seed,
        row_targets=np.array([-1.0, 7.0]),
        col_targets=np.array([4.0, 2.0]),
        sign_flex=np.array([[True, False], [True, True]]),
        max_iter=200,
        rtol=1e-6,
    )
    assert result.matrix[0, 1] <= 0.0
    assert not np.isclose(result.matrix[0, 1], 0.0) or seed[0, 1] == 0.0


def test_sign_lock_negative_cell_does_not_take_opposite_sign() -> None:
    seed = np.array([[2.0, -3.0], [1.0, 4.0]])
    flex = np.array([[True, False], [True, True]])
    result = gras_balance(
        matrix=seed,
        row_targets=np.array([1.0, 3.0]),
        col_targets=np.array([2.0, 2.0]),
        sign_flex=flex,
        max_iter=200,
        rtol=1e-6,
    )
    assert result.matrix[0, 1] <= 0.0


def test_temurshoev_flex_may_cross_sign() -> None:
    seed = np.array([[-2.0, -1.0]])
    result = gras_balance(
        matrix=seed,
        row_targets=np.array([3.0]),
        col_targets=np.array([2.0, 1.0]),
        sign_flex=np.ones((1, 2), dtype=bool),
        max_iter=50,
        rtol=1e-6,
    )
    assert (result.matrix > 0.0).any()


def test_sign_flex_none_does_not_cross_on_temurshoev_opposite_target() -> None:
    seed = np.array([[-2.0, -1.0]])
    result = gras_balance(
        matrix=seed,
        row_targets=np.array([3.0]),
        col_targets=np.array([2.0, 1.0]),
        max_iter=50,
        rtol=1e-6,
    )
    assert (result.matrix >= 0.0).all()
    assert not (result.matrix > 0.0).any() or np.allclose(result.matrix, 0.0)
    assert (np.sign(result.matrix) * np.sign(seed) >= 0.0).all()


def test_free_mask_none_participates_every_cell() -> None:
    seed = np.zeros((2, 2))
    result = gras_balance(
        matrix=seed,
        row_targets=np.array([1.0, 1.0]),
        col_targets=np.array([1.0, 1.0]),
        max_iter=20,
        rtol=1e-6,
    )
    assert not result.converged


def test_nonzero_target_on_empty_free_margin_raises() -> None:
    seed = np.array([[1.0, 2.0], [3.0, 4.0]])
    free = np.array([[True, True], [False, False]])
    with pytest.raises(ValueError, match='empty free margin'):
        gras_balance(
            matrix=seed,
            row_targets=np.array([3.0, 5.0]),
            col_targets=np.array([4.0, 4.0]),
            free_mask=free,
        )


def test_zero_target_on_empty_or_live_margin_does_not_raise() -> None:
    seed = np.array([[1.0, 2.0], [0.0, 0.0]])
    gras_balance(
        matrix=seed,
        row_targets=np.array([3.0, 0.0]),
        col_targets=np.array([1.0, 2.0]),
        free_mask=np.array([[True, True], [False, False]]),
        max_iter=50,
    )
    gras_balance(
        matrix=seed,
        row_targets=np.array([3.0, 0.0]),
        col_targets=np.array([1.0, 2.0]),
        max_iter=50,
    )


def test_free_all_zero_row_does_not_raise_or_mutate_target() -> None:
    seed = np.array([[1.0, 2.0], [0.0, 0.0]])
    row_targets = np.array([3.0, 5.0])
    col_targets = np.array([1.0, 2.0])
    row_copy = row_targets.copy()
    result = gras_balance(
        matrix=seed,
        row_targets=row_targets,
        col_targets=col_targets,
        free_mask=np.ones((2, 2), dtype=bool),
        max_iter=20,
        rtol=1e-6,
    )
    assert not result.converged
    np.testing.assert_array_equal(row_targets, row_copy)


def test_empty_free_mask_and_nonzero_target_raises_not_sum_abs() -> None:
    """A live all-zero row is not the same as no free cell."""
    seed = np.array([[1.0, 2.0], [0.0, 0.0]])
    result = gras_balance(
        matrix=seed,
        row_targets=np.array([3.0, 5.0]),
        col_targets=np.array([1.0, 2.0]),
        free_mask=np.array([[True, True], [True, True]]),
        max_iter=10,
    )
    assert not result.converged
    with pytest.raises(ValueError, match='empty free margin'):
        gras_balance(
            matrix=seed,
            row_targets=np.array([3.0, 5.0]),
            col_targets=np.array([1.0, 2.0]),
            free_mask=np.array([[True, True], [False, False]]),
        )


def test_signed_residual_after_split_fixed_is_accepted() -> None:
    seed = pd.DataFrame(
        [[10.0, 5.0, 5.0], [4.0, 6.0, 0.0], [0.0, 2.0, 8.0]],
        index=['r1', 'r2', 'r3'],
        columns=['c1', 'c2', 'c3'],
    )
    fixed = pd.DataFrame(False, index=seed.index, columns=seed.columns)
    fixed.loc['r2', 'c2'] = True
    mask = SutMask.from_pattern(seed, fixed_value=fixed)
    frozen, free = split_fixed(seed, mask)
    # Published r2 total 3 against frozen 6 → residual -3 (sign change).
    row_targets = np.array([20.0, -3.0, 10.0])
    col_targets = free.sum(axis=0).to_numpy(dtype=np.float64)
    assert row_targets[1] < 0.0
    result = gras_balance(
        matrix=free.to_numpy(),
        row_targets=row_targets,
        col_targets=col_targets,
        free_mask=mask.free.to_numpy(),
        sign_flex=(mask.sign_lock.to_numpy() == 0),
        max_iter=50,
    )
    np.testing.assert_array_equal(result.matrix[~mask.free.to_numpy()], 0.0)


def test_all_negative_row_converges_on_negative_target() -> None:
    seed = np.array([[-2.0, -1.0]])
    result = gras_balance(
        matrix=seed,
        row_targets=np.array([-6.0]),
        col_targets=np.array([-4.0, -2.0]),
        sign_flex=np.zeros((1, 2), dtype=bool),
        max_iter=50,
        rtol=1e-8,
    )
    assert result.converged
    np.testing.assert_allclose(result.matrix.sum(axis=1), [-6.0], rtol=1e-6)
    assert (result.matrix < 0.0).all()


def test_project_infeasible_raises_on_negative_seed_or_target() -> None:
    seed = np.array([[1.0, -2.0], [3.0, 4.0]])
    with pytest.raises(ValueError, match='signed problems'):
        gras_balance(
            matrix=seed,
            row_targets=np.array([1.0, 7.0]),
            col_targets=np.array([4.0, 4.0]),
            project_infeasible=True,
        )
    with pytest.raises(ValueError, match='signed problems'):
        gras_balance(
            matrix=np.array([[1.0, 2.0], [3.0, 4.0]]),
            row_targets=np.array([-1.0, 11.0]),
            col_targets=np.array([4.0, 6.0]),
            project_infeasible=True,
        )


def test_mixed_sign_close_rows_exactly_hits_signed_row_targets() -> None:
    seed = np.array([[2.0, -3.0], [1.0, 4.0]])
    row_targets = np.array([-2.0, 6.0])
    col_targets = np.array([3.0, 1.0])
    result = gras_balance(
        matrix=seed,
        row_targets=row_targets,
        col_targets=col_targets,
        sign_flex=np.ones((2, 2), dtype=bool),
        max_iter=1,
        rtol=1e-2,
        close_rows_exactly=True,
    )
    np.testing.assert_allclose(result.matrix.sum(axis=1), row_targets, rtol=1e-10)


def test_locally_infeasible_cut_stalls_without_projection() -> None:
    seed, row_targets, col_targets = _locally_infeasible_system()
    result = gras_balance(
        matrix=seed,
        row_targets=row_targets,
        col_targets=col_targets,
        max_iter=200,
        rtol=1e-6,
    )
    assert not result.converged
    assert result.max_row_err > 0.5
    assert not result.projected_rows.any()
    assert not result.projected_cols.any()


def test_projects_locally_infeasible_cut_and_converges() -> None:
    seed, row_targets, col_targets = _locally_infeasible_system()
    result = gras_balance(
        matrix=seed,
        row_targets=row_targets,
        col_targets=col_targets,
        max_iter=200,
        rtol=1e-6,
        project_infeasible=True,
    )
    assert result.converged
    np.testing.assert_array_equal(result.projected_rows, [True, True, False, False])
    np.testing.assert_array_equal(result.projected_cols, [False, True, True])
    assert 3.5 <= result.projected_target_mass <= 4.5
    x = result.matrix
    assert np.isclose(x[:, 0].sum(), 6.0, rtol=1e-5)
    np.testing.assert_allclose(x[2:].sum(axis=1), [5.0, 5.0], rtol=1e-5)
    assert np.isclose(x[:2].sum(), 6.0, rtol=1e-5)


def test_projection_leaves_feasible_problems_untouched() -> None:
    rng = np.random.default_rng(11)
    seed = rng.uniform(0.1, 1.0, size=(20, 20))
    row_targets = rng.uniform(5.0, 15.0, size=20)
    col_targets = rng.uniform(1.0, 2.0, size=20)
    col_targets *= row_targets.sum() / col_targets.sum()
    result = gras_balance(
        matrix=seed,
        row_targets=row_targets,
        col_targets=col_targets,
        max_iter=500,
        rtol=1e-6,
        project_infeasible=True,
    )
    assert result.converged
    assert result.projected_target_mass == 0.0
    np.testing.assert_allclose(result.matrix.sum(axis=1), row_targets, rtol=1e-5)
    np.testing.assert_allclose(result.matrix.sum(axis=0), col_targets, rtol=1e-5)


def test_close_rows_exactly_zeroes_row_residual() -> None:
    row_targets = np.array([50.0, 30.0, 40.0, 30.0])
    col_targets = TOY.sum(axis=0) * (row_targets.sum() / TOY.sum())
    loose = gras_balance(
        matrix=TOY,
        row_targets=row_targets,
        col_targets=col_targets,
        max_iter=500,
        rtol=1e-2,
    )
    assert loose.converged
    loose_row_err = float(np.max(np.abs(loose.matrix.sum(axis=1) - row_targets)))
    assert loose_row_err > 1e-6
    polished = gras_balance(
        matrix=TOY,
        row_targets=row_targets,
        col_targets=col_targets,
        max_iter=500,
        rtol=1e-2,
        close_rows_exactly=True,
    )
    assert polished.converged
    np.testing.assert_allclose(polished.matrix.sum(axis=1), row_targets, rtol=1e-12)
    assert polished.max_col_err <= loose.max_col_err + loose_row_err


def test_closes_rows_exactly_even_when_not_converged() -> None:
    seed, row_targets, col_targets = _locally_infeasible_system()
    result = gras_balance(
        matrix=seed,
        row_targets=row_targets,
        col_targets=col_targets,
        max_iter=60,
        rtol=1e-6,
        close_rows_exactly=True,
    )
    assert not result.converged
    np.testing.assert_allclose(result.matrix.sum(axis=1), row_targets, rtol=1e-12)
    assert result.max_row_rel_err < 1e-12
    assert result.max_col_rel_err > 0.0


def test_relaxes_the_gap_threshold_rather_than_spinning() -> None:
    seed = np.array(
        [
            [1.0, 0.0, 0.0],
            [1.0, 0.0, 0.0],
            [0.0, 1.0, 1.0],
            [0.0, 1.0, 1.0],
        ]
    )
    row_targets = np.array([3.006, 3.006, 5.0, 5.0])
    col_targets = np.array([6.0, 5.006, 5.006])
    col_targets *= row_targets.sum() / col_targets.sum()
    result = gras_balance(
        matrix=seed,
        row_targets=row_targets,
        col_targets=col_targets,
        max_iter=400,
        rtol=1e-4,
        atol=0.0,
        project_infeasible=True,
    )
    assert result.converged
    assert result.projection_rounds >= 1
    assert result.projected_rows[:2].any()
    assert not result.projected_rows[2:].any()


def test_stall_min_relative_gap_stays_clear_of_the_convergence_tolerance() -> None:
    assert _stall_min_relative_gap(1e-3) == pytest.approx(1e-2)
    assert _stall_min_relative_gap(1e-6) == pytest.approx(STALL_MIN_RELATIVE_GAP_FLOOR)


def test_output_is_zero_on_non_free_cells() -> None:
    seed = np.array([[10.0, 20.0, 0.0], [4.0, 16.0, 8.0]])
    free = np.array([[True, True, False], [True, True, True]])
    result = gras_balance(
        matrix=seed,
        row_targets=np.array([30.0, 28.0]),
        col_targets=np.array([14.0, 36.0, 8.0]),
        free_mask=free,
        max_iter=100,
        rtol=1e-6,
    )
    assert np.all(result.matrix[~free] == 0.0)


def test_close_rows_and_project_keep_structural_zero_and_fixed_at_zero() -> None:
    """Both flags must actually run, not no-op on identity margins.

    The locally infeasible cut makes ``project_infeasible`` fire and
    ``close_rows_exactly`` rescale rows. An extra structural-zero column and a
    fixed-value cell (0 in Z, F nonzero) would be filled by an additive dump
    onto the row; they must stay 0 in the kernel output.
    """
    seed = pd.DataFrame(
        [
            [1.0, 0.0, 0.0, 0.0],
            [1.0, 0.0, 0.0, 0.0],
            [0.0, 1.0, 1.0, 0.0],
            [5.0, 1.0, 1.0, 0.0],
        ],
        index=['r1', 'r2', 'r3', 'r4'],
        columns=['c1', 'c2', 'c3', 'c4'],
    )
    structural = pd.DataFrame(False, index=seed.index, columns=seed.columns)
    structural['c4'] = True
    fixed = pd.DataFrame(False, index=seed.index, columns=seed.columns)
    fixed.loc['r4', 'c1'] = True
    sign_lock = pd.DataFrame(0, index=seed.index, columns=seed.columns, dtype=int)
    mask = SutMask(structural_zero=structural, fixed_value=fixed, sign_lock=sign_lock)
    frozen, free = split_fixed(seed, mask)
    assert frozen.loc['r4', 'c1'] == 5.0
    assert free.loc['r4', 'c1'] == 0.0
    z = free.to_numpy(dtype=np.float64)
    z_copy = z.copy()
    free_mask = mask.free.to_numpy()
    row_targets = np.array([4.0, 4.0, 5.0, 5.0])
    col_targets = np.array([6.0, 6.0, 6.0, 0.0])
    result = gras_balance(
        matrix=z,
        row_targets=row_targets,
        col_targets=col_targets,
        free_mask=free_mask,
        sign_flex=(mask.sign_lock.to_numpy() == 0),
        close_rows_exactly=True,
        project_infeasible=True,
        max_iter=200,
        rtol=1e-6,
    )
    assert result.projection_rounds >= 1
    assert result.max_row_rel_err < 1e-12
    assert np.all(result.matrix[~free_mask] == 0.0)
    balanced = pd.DataFrame(result.matrix, index=seed.index, columns=seed.columns)
    restored = restore_fixed(balanced, frozen)
    assert restored.loc['r4', 'c1'] == 5.0
    assert (restored['c4'] == 0.0).all()
    np.testing.assert_array_equal(z, z_copy)


def test_does_not_mutate_caller_arrays() -> None:
    seed = np.array([[1.0, 2.0], [3.0, 4.0]])
    row_targets = np.array([4.0, 6.0])
    col_targets = np.array([4.0, 6.0])
    free = np.ones((2, 2), dtype=bool)
    flex = np.zeros((2, 2), dtype=bool)
    seed_c, row_c, col_c = seed.copy(), row_targets.copy(), col_targets.copy()
    free_c, flex_c = free.copy(), flex.copy()
    gras_balance(
        matrix=seed,
        row_targets=row_targets,
        col_targets=col_targets,
        free_mask=free,
        sign_flex=flex,
        max_iter=20,
    )
    np.testing.assert_array_equal(seed, seed_c)
    np.testing.assert_array_equal(row_targets, row_c)
    np.testing.assert_array_equal(col_targets, col_c)
    np.testing.assert_array_equal(free, free_c)
    np.testing.assert_array_equal(flex, flex_c)


def test_nonfinite_after_copy_raises() -> None:
    with pytest.raises(ValueError, match='non-finite'):
        gras_balance(
            matrix=np.array([[1.0, np.nan], [3.0, 4.0]]),
            row_targets=np.array([1.0, 7.0]),
            col_targets=np.array([4.0, 4.0]),
        )
