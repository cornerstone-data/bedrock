"""Tests for persisting balanced SUT products. Hermetic - no 2017 / GCS."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from bedrock.transform.iot.nowcast_sut_assembly import (
    BALANCED_ARTIFACT_NAMES,
    RESIDUE_EPS_USD_M,
    YearBalance,
    save_balance,
)
from bedrock.transform.iot.nowcast_sut_gras import SutBalanceResult
from bedrock.utils.config.settings import GIT_HASH, PKG_VERSION_NUMBER
from bedrock.utils.economic.balance.mask import SutMask
from bedrock.utils.economic.balance.targets import TargetSet


def _toy_mask(index: list[str], columns: list[str]) -> SutMask:
    return SutMask(
        structural_zero=pd.DataFrame(False, index=index, columns=columns),
        fixed_value=pd.DataFrame(False, index=index, columns=columns),
        sign_lock=pd.DataFrame(0, index=index, columns=columns, dtype=int),
    )


def _toy_balance(
    *,
    use: pd.DataFrame | None = None,
    supply: pd.DataFrame | None = None,
) -> YearBalance:
    use_frame = (
        use
        if use is not None
        else pd.DataFrame([[1.0, 2.0]], index=['c1'], columns=['i1', 'F01000'])
    )
    supply_frame = (
        supply
        if supply is not None
        else pd.DataFrame([[3.0, 4.0]], index=['c1'], columns=['i1', 'MDTY'])
    )
    result = SutBalanceResult(
        blocks={'use': use_frame, 'supply': supply_frame},
        outer_iterations=20,
        t11_max_abs_residual=0.0,
        skipped=(),
        last={},
        soft_deferred=('T6', 'T8', 'T9'),
    )
    return YearBalance(
        year=2022,
        seeds={'use': use_frame, 'supply': supply_frame},
        masks={
            'use': _toy_mask(
                list(use_frame.index.astype(str)),
                list(use_frame.columns.astype(str)),
            ),
            'supply': _toy_mask(
                list(supply_frame.index.astype(str)),
                list(supply_frame.columns.astype(str)),
            ),
        },
        targets=TargetSet.of(),
        sweep=pd.DataFrame(),
        result=result,
        balanced={'use': use_frame, 'supply': supply_frame},
    )


def test_save_balance_writes_parquet_and_sidecar_per_block(tmp_path: Path) -> None:
    balance = _toy_balance()
    assert balance.balanced is not None
    pattern = pd.DataFrame(
        0.0,
        index=balance.balanced['use'].index,
        columns=balance.balanced['use'].columns,
    )
    with patch(
        'bedrock.transform.iot.nowcast_sut_assembly.published_2017_panel',
        return_value=pattern,
    ):
        written = save_balance(
            balance, tmp_path, protocol='soft (impose_soft=True), max_outer=20'
        )

    assert len(written) == 4  # two blocks x (parquet + sidecar)
    hash_suffix = f'_{GIT_HASH}' if GIT_HASH is not None else ''
    for block, name in BALANCED_ARTIFACT_NAMES.items():
        stem = f'{name}_2022_v{PKG_VERSION_NUMBER}{hash_suffix}'
        frame = pd.read_parquet(tmp_path / f'{stem}.parquet')
        pd.testing.assert_frame_equal(frame, balance.balanced[block])
        meta = json.loads((tmp_path / f'{stem}_metadata.json').read_text())
        assert meta['name_data'] == f'{name}_2022'
        assert meta['category'] == 'BalancedSUT'
        assert meta['tool_meta']['protocol'].startswith('soft')
        assert meta['tool_meta']['units'] == 'BEA million USD'
        assert 'T11 max |residual| 0.0 $M' in meta['tool_meta']['engine_result']
        assert meta['tool_meta']['residue_eps_usd_m'] == RESIDUE_EPS_USD_M
        assert meta['tool_meta']['residue_swept_cells'] == 0
        assert meta['tool_meta']['residue_sweep'] == 'illicit_below_eps'


def test_save_balance_sweeps_illicit_below_eps_only(tmp_path: Path) -> None:
    use = pd.DataFrame(
        [[1e-4, -0.01, RESIDUE_EPS_USD_M, -0.06]],
        index=['c1'],
        columns=['a', 'b', 'at_eps', 'c'],
    )
    supply = pd.DataFrame([[3.0]], index=['c1'], columns=['i1'])
    balance = _toy_balance(use=use, supply=supply)
    assert balance.balanced is not None
    pattern = pd.DataFrame(0.0, index=use.index, columns=use.columns)
    with patch(
        'bedrock.transform.iot.nowcast_sut_assembly.published_2017_panel',
        return_value=pattern,
    ):
        written = save_balance(balance, tmp_path, protocol='soft')
    assert written

    hash_suffix = f'_{GIT_HASH}' if GIT_HASH is not None else ''
    use_stem = (
        f'{BALANCED_ARTIFACT_NAMES["use"]}_2022_v{PKG_VERSION_NUMBER}{hash_suffix}'
    )
    supply_stem = (
        f'{BALANCED_ARTIFACT_NAMES["supply"]}_2022_v{PKG_VERSION_NUMBER}{hash_suffix}'
    )
    saved_use = pd.read_parquet(tmp_path / f'{use_stem}.parquet')
    # Positive near-zero kept; illicit below-eps negative swept; at-eps and
    # above-eps negatives kept (above-eps is a 2b gate concern, not item 1).
    assert saved_use.loc['c1', 'a'] == pytest.approx(1e-4)
    assert saved_use.loc['c1', 'b'] == 0.0
    assert saved_use.loc['c1', 'at_eps'] == pytest.approx(RESIDUE_EPS_USD_M)
    assert saved_use.loc['c1', 'c'] == pytest.approx(-0.06)
    # In-memory balanced Use is untouched.
    assert balance.balanced['use'].loc['c1', 'a'] == pytest.approx(1e-4)
    assert balance.balanced['use'].loc['c1', 'b'] == pytest.approx(-0.01)

    use_meta = json.loads((tmp_path / f'{use_stem}_metadata.json').read_text())
    assert use_meta['tool_meta']['residue_swept_cells'] == 1
    assert use_meta['tool_meta']['residue_eps_usd_m'] == RESIDUE_EPS_USD_M
    assert use_meta['tool_meta']['residue_sweep'] == 'illicit_below_eps'
    supply_meta = json.loads((tmp_path / f'{supply_stem}_metadata.json').read_text())
    assert supply_meta['tool_meta']['residue_swept_cells'] == 0
    assert supply_meta['tool_meta']['residue_eps_usd_m'] == RESIDUE_EPS_USD_M


def test_save_balance_refuses_an_unbalanced_year(tmp_path: Path) -> None:
    balance = _toy_balance()
    unrun = YearBalance(
        year=balance.year,
        seeds=balance.seeds,
        masks=balance.masks,
        targets=balance.targets,
        sweep=balance.sweep,
        result=None,
        balanced=None,
    )
    with pytest.raises(ValueError, match='balance_year first'):
        save_balance(unrun, tmp_path, protocol='soft')
