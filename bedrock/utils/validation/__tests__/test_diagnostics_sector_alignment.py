"""Unit tests for CEDA v7 ↔ cornerstone schema alignment in diagnostics."""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.utils.validation.diagnostics_helpers import (
    EfsForDiagnostics,
    OldEfSet,
    align_efs_across_schemas,
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
        # Waste: new subsectors summed into 562000
        assert result.D_old.raw.loc['562000', 'v'] == pytest.approx(50.0)
        assert result.D_new.loc['562000', 'v'] == pytest.approx(70.0)
        # Appliances: old 4 codes summed into 335220
        assert result.D_old.raw.loc['335220', 'v'] == pytest.approx(20.0)
        assert result.D_new.loc['335220', 'v'] == pytest.approx(18.0)
        # Aluminum: new 331313 + 33131B summed into 331313
        assert result.D_old.raw.loc['331313', 'v'] == pytest.approx(12.0)
        assert result.D_new.loc['331313', 'v'] == pytest.approx(12.0)
        # S00402 excluded
        assert 'S00402' not in result.D_new.index
        # All 3 mappings active
        assert len(active) == 3

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
