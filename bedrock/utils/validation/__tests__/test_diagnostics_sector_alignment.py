"""Unit tests for CEDA v7 ↔ cornerstone schema alignment in diagnostics."""

from __future__ import annotations

from typing import Generator

import pandas as pd
import pytest

from bedrock.utils.config.usa_config import reset_usa_config, set_global_usa_config
from bedrock.utils.schemas.cornerstone_schemas import (
    ELECTRICITY_AGGREGATE_SECTOR,
    ELECTRICITY_DISAGG_SECTORS,
)
from bedrock.utils.validation.diagnostics_helpers import (
    EfsForDiagnostics,
    OldEfSet,
    align_efs_across_schemas,
    get_aligned_sector_desc,
)

# ---------------------------------------------------------------------------
# Fixtures — minimal CEDA v7 (old) and cornerstone (new) sector indices
# ---------------------------------------------------------------------------

_SHARED = ['1111A0', '1111B0', '221100']
_OLD_INDEX = _SHARED + ['331313', '335221', '335222', '335224', '335228', '562000']
_NEW_INDEX = (
    _SHARED
    + ['331313', '33131B', '335220']
    + ['562111', '562HAZ', '562212', '562213', '562910', '562920', '562OTH']
    + ['S00402']
)

_ELECTRICITY_DISAGG_CONFIG = (
    'test_usa_config_waste_disagg_electricity_disaggregation.yaml'
)


@pytest.fixture(autouse=True)
def _reset_usa_config() -> Generator[None, None, None]:
    reset_usa_config(should_reset_env_var=True)
    yield
    reset_usa_config(should_reset_env_var=True)


def _build_efs(
    old_values: list[float],
    new_values: list[float],
) -> EfsForDiagnostics:
    old_df = pd.DataFrame({'v': old_values}, index=pd.Index(_OLD_INDEX))
    new_df = pd.DataFrame({'v': new_values}, index=pd.Index(_NEW_INDEX))
    return EfsForDiagnostics(
        D_new=new_df,
        N_new=new_df.copy(),
        D_old=OldEfSet(raw=old_df, inflated=old_df.copy()),
        N_old=OldEfSet(raw=old_df.copy(), inflated=old_df.copy()),
    )


def _build_electricity_disagg_efs(
    old_index: list[str],
    new_index: list[str],
    old_values: list[float],
    new_values: list[float],
) -> EfsForDiagnostics:
    old_df = pd.DataFrame({'v': old_values}, index=pd.Index(old_index))
    new_df = pd.DataFrame({'v': new_values}, index=pd.Index(new_index))
    return EfsForDiagnostics(
        D_new=new_df,
        N_new=new_df.copy(),
        D_old=OldEfSet(raw=old_df, inflated=old_df.copy()),
        N_old=OldEfSet(raw=old_df.copy(), inflated=old_df.copy()),
    )


class TestAlignEfsAcrossSchemas:
    def test_full_schema_diff(self) -> None:
        """When all structural differences are present, alignment works end-to-end."""
        # old: shared(3) + aluminum(1) + appliances(4) + waste(1)
        old_vals = [1.0, 2.0, 3.0, 12.0, 5.0, 5.0, 5.0, 5.0, 50.0]
        # new: shared(3) + aluminum(2) + appliance(1) + waste(7) + S00402(1)
        new_vals = [
            1.1,
            2.2,
            3.3,
            8.0,
            4.0,
            18.0,
            10.0,
            10.0,
            10.0,
            10.0,
            10.0,
            10.0,
            10.0,
            99.0,
        ]

        result, active = align_efs_across_schemas(_build_efs(old_vals, new_vals))

        # Indices match
        assert list(result.D_new.index) == list(result.D_old.raw.index)
        # Shared sectors pass through
        assert result.D_old.raw.loc['1111A0', 'v'] == pytest.approx(1.0)
        assert result.D_new.loc['1111A0', 'v'] == pytest.approx(1.1)
        # Waste: new subsectors averaged into 562000 (mean, not sum, for intensities)
        assert result.D_old.raw.loc['562000', 'v'] == pytest.approx(50.0)
        assert result.D_new.loc['562000', 'v'] == pytest.approx(10.0)
        # Appliances: old 4 codes averaged into 335220 (mean for intensities)
        assert result.D_old.raw.loc['335220', 'v'] == pytest.approx(5.0)
        assert result.D_new.loc['335220', 'v'] == pytest.approx(18.0)
        # Aluminum: 331313 exists in both schemas; 33131B filled on old side only
        assert result.D_old.raw.loc['331313', 'v'] == pytest.approx(12.0)
        assert result.D_new.loc['331313', 'v'] == pytest.approx(8.0)
        # S00402 is new-only, present in the full union index (no exclusion logic)
        assert 'S00402' in result.D_new.index
        # Active mappings: 8 waste + 5 appliance + 1 aluminum = 14 individual codes
        assert len(active) == 14

    def test_partial_schema_no_aluminum_split(self) -> None:
        """When 33131B is absent from new, aluminum mapping is skipped (no KeyError)."""
        partial_new_index = (
            _SHARED
            + ['331313']  # no 33131B
            + ['335220']
            + ['562111', '562HAZ', '562212', '562213', '562910', '562920', '562OTH']
            + ['S00402']
        )
        old_df = pd.DataFrame(
            {'v': [0.0] * len(_OLD_INDEX)}, index=pd.Index(_OLD_INDEX)
        )
        new_df = pd.DataFrame(
            {'v': [0.0] * len(partial_new_index)}, index=pd.Index(partial_new_index)
        )
        efs = EfsForDiagnostics(
            D_new=new_df,
            N_new=new_df.copy(),
            D_old=OldEfSet(raw=old_df, inflated=old_df.copy()),
            N_old=OldEfSet(raw=old_df.copy(), inflated=old_df.copy()),
        )

        result, active = align_efs_across_schemas(efs)

        assert '331313' not in active  # aluminum mapping not active
        assert '331313' in result.D_new.index  # but code still present as shared
        assert list(result.D_new.index) == list(result.D_old.raw.index)

    def test_identical_indices_is_noop(self) -> None:
        """When old and new share the exact same index, no mappings are active."""
        idx = ['A', 'B', 'C']
        old_df = pd.DataFrame({'v': [1.0, 2.0, 3.0]}, index=pd.Index(idx))
        new_df = pd.DataFrame({'v': [4.0, 5.0, 6.0]}, index=pd.Index(idx))
        efs = EfsForDiagnostics(
            D_new=new_df,
            N_new=new_df.copy(),
            D_old=OldEfSet(raw=old_df, inflated=old_df.copy()),
            N_old=OldEfSet(raw=old_df.copy(), inflated=old_df.copy()),
        )

        result, active = align_efs_across_schemas(efs)

        assert active == {}
        assert list(result.D_new.index) == idx
        assert result.D_new.loc['A', 'v'] == pytest.approx(4.0)
        assert result.D_old.raw.loc['A', 'v'] == pytest.approx(1.0)


class TestElectricityDisaggAlignment:
    _OLD_INDEX = ['1111A0', ELECTRICITY_AGGREGATE_SECTOR]
    _NEW_INDEX = ['1111A0', *ELECTRICITY_DISAGG_SECTORS]

    def test_electricity_disagg_alignment(self) -> None:
        set_global_usa_config(_ELECTRICITY_DISAGG_CONFIG)

        baseline_elec = 30.0
        child_new_vals = [10.0, 20.0, 40.0]
        efs = _build_electricity_disagg_efs(
            old_index=self._OLD_INDEX,
            new_index=self._NEW_INDEX,
            old_values=[1.0, baseline_elec],
            new_values=[1.1, *child_new_vals],
        )

        result, active = align_efs_across_schemas(efs)

        assert (
            active[ELECTRICITY_AGGREGATE_SECTOR] == 'old-only (electricity aggregate)'
        )
        for child in ELECTRICITY_DISAGG_SECTORS:
            assert active[child] == 'new-only (electricity subsector)'
        assert len(active) == 4

        for child in ELECTRICITY_DISAGG_SECTORS:
            assert result.D_old.raw.loc[child, 'v'] == pytest.approx(baseline_elec)
        assert result.D_new.loc[ELECTRICITY_AGGREGATE_SECTOR, 'v'] == pytest.approx(
            sum(child_new_vals) / len(child_new_vals)
        )

        assert result.N_old.raw.loc['221121', 'v'] == pytest.approx(baseline_elec)
        assert result.N_new.loc[ELECTRICITY_AGGREGATE_SECTOR, 'v'] == pytest.approx(
            sum(child_new_vals) / len(child_new_vals)
        )

        assert list(result.D_new.index) == list(result.D_old.raw.index)
        assert list(result.N_new.index) == list(result.N_old.raw.index)

    def test_flag_off_no_electricity_mapping(self) -> None:
        efs = _build_electricity_disagg_efs(
            old_index=self._OLD_INDEX,
            new_index=self._NEW_INDEX,
            old_values=[1.0, 30.0],
            new_values=[1.1, 10.0, 20.0, 40.0],
        )

        result, active = align_efs_across_schemas(efs)

        assert '221110' not in active
        assert pd.isna(result.D_old.raw.loc['221110', 'v'])

    def test_partial_electricity_disagg_skipped_when_incomplete(self) -> None:
        set_global_usa_config(_ELECTRICITY_DISAGG_CONFIG)

        partial_new_index = ['1111A0', '221110', '221121']
        efs = _build_electricity_disagg_efs(
            old_index=self._OLD_INDEX,
            new_index=partial_new_index,
            old_values=[1.0, 30.0],
            new_values=[1.1, 10.0, 20.0],
        )

        result, active = align_efs_across_schemas(efs)

        assert ELECTRICITY_AGGREGATE_SECTOR not in active
        for child in ('221110', '221121'):
            assert child not in active
            assert pd.isna(result.D_old.raw.loc[child, 'v'])


def test_get_aligned_sector_desc_electricity() -> None:
    set_global_usa_config(_ELECTRICITY_DISAGG_CONFIG)
    desc_on = get_aligned_sector_desc()

    assert desc_on['221110'] == 'Electric power generation'
    assert desc_on['221121'] == 'Electric Bulk Power Transmission and Control'
    assert desc_on['221122'] == 'Electric Power Distribution'
    assert (
        desc_on[ELECTRICITY_AGGREGATE_SECTOR]
        == 'Electric power generation, transmission, and distribution'
    )

    reset_usa_config(should_reset_env_var=True)
    desc_off = get_aligned_sector_desc()

    assert '221121' not in desc_off
    assert '221122' not in desc_off
