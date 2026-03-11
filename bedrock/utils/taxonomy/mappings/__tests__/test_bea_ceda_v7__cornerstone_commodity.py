"""Tests for the CEDA v7 → Cornerstone commodity mapping and correspondence."""

import typing as ta

import numpy as np
import pandas as pd
import pytest

from bedrock.transform.eeio.cornerstone_expansion import (
    CS_COMMODITY_LIST,
    expand_ghg_matrix_from_ceda_to_cornerstone,
)
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTOR, CEDA_V7_SECTORS
from bedrock.utils.taxonomy.cornerstone.commodities import (
    COMMODITIES,
    COMMODITY,
    WASTE_DISAGG_COMMODITIES,
)
from bedrock.utils.taxonomy.mappings.bea_ceda_v7__cornerstone_commodity import (
    load_ceda_v7_commodity_to_cornerstone_commodity,
)
from bedrock.utils.taxonomy.usa_taxonomy_correspondence_helpers import (
    load_ceda_v7_commodity__cornerstone_commodity_correspondence,
)

Mapping = ta.Dict[CEDA_V7_SECTOR, ta.List[COMMODITY]]

# ---------------------------------------------------------------------------
# Mapping tests
# ---------------------------------------------------------------------------


@pytest.fixture()
def mapping() -> Mapping:
    return load_ceda_v7_commodity_to_cornerstone_commodity()


def test_mapping_covers_all_ceda_sectors(mapping: Mapping) -> None:
    assert set(mapping.keys()) == set(CEDA_V7_SECTORS)


def test_all_mapped_codes_are_valid_cornerstone(mapping: Mapping) -> None:
    all_targets = {code for targets in mapping.values() for code in targets}
    assert all_targets <= set(COMMODITIES)


def test_aluminum_rename(mapping: Mapping) -> None:
    assert mapping['331313'] == ['33131B']


def test_appliances_aggregation(mapping: Mapping) -> None:
    for code in ('335221', '335222', '335224', '335228'):
        assert mapping[code] == ['335220']


def test_waste_disaggregation(mapping: Mapping) -> None:
    assert mapping['562000'] == WASTE_DISAGG_COMMODITIES['562000']
    assert len(mapping['562000']) == 7


def test_identity_codes(mapping: Mapping) -> None:
    """Most codes map 1:1 to themselves."""
    identity_count = sum(1 for ceda, targets in mapping.items() if targets == [ceda])
    # 400 total - 1 aluminum - 4 appliances - 1 waste = 394 identity codes
    assert identity_count == 394


def test_no_empty_mappings_except_none(mapping: Mapping) -> None:
    """Every CEDA code maps to at least one Cornerstone code."""
    for ceda, targets in mapping.items():
        assert len(targets) >= 1, f"CEDA code {ceda} has no mapping"


# ---------------------------------------------------------------------------
# Correspondence matrix tests
# ---------------------------------------------------------------------------


@pytest.fixture()
def corresp() -> pd.DataFrame:
    return load_ceda_v7_commodity__cornerstone_commodity_correspondence()


def test_corresp_shape(corresp: pd.DataFrame) -> None:
    assert corresp.shape == (len(COMMODITIES), len(CEDA_V7_SECTORS))
    assert corresp.shape == (405, 400)


def test_corresp_is_binary(corresp: pd.DataFrame) -> None:
    unique_vals = set(np.unique(corresp.values))
    assert unique_vals <= {0.0, 1.0}


def test_corresp_aluminum_column(corresp: pd.DataFrame) -> None:
    col = corresp['331313']
    assert col['33131B'] == 1.0
    assert col.sum() == 1.0


def test_corresp_appliances_rows(corresp: pd.DataFrame) -> None:
    """Each appliance CEDA code maps to the same Cornerstone code 335220."""
    for code in ('335221', '335222', '335224', '335228'):
        assert corresp.loc['335220', code] == 1.0
    # Row sum for 335220 should be 4 (one hit per appliance code)
    assert corresp.loc['335220'].sum() == 4.0


def test_corresp_waste_column(corresp: pd.DataFrame) -> None:
    """562000 should map to all 7 waste subsectors."""
    col = corresp['562000']
    assert col.sum() == 7.0
    for waste_code in WASTE_DISAGG_COMMODITIES['562000']:
        assert col[waste_code] == 1.0


def test_corresp_identity_codes_have_unit_col_sum(corresp: pd.DataFrame) -> None:
    """Identity-mapped CEDA codes should have column sum of 1."""
    special_codes = {'331313', '335221', '335222', '335224', '335228', '562000'}
    for code in CEDA_V7_SECTORS:
        if code not in special_codes:
            assert corresp[code].sum() == 1.0, f"Column sum for {code} != 1"


# ---------------------------------------------------------------------------
# Expansion function tests
# ---------------------------------------------------------------------------


class TestExpandGhgMatrixFromCedaToCornerstone:
    @pytest.fixture()
    def synthetic_E(self) -> pd.DataFrame:
        """Synthetic (ghg × CEDA_sector) matrix with known values."""
        ghgs = ['CO2', 'CH4']
        sectors = ['211000', '331313', '335221', '335222', '335224', '335228', '562000']
        data = [
            [100.0, 20.0, 1.0, 2.0, 3.0, 4.0, 70.0],
            [50.0, 10.0, 0.5, 1.0, 1.5, 2.0, 35.0],
        ]
        return pd.DataFrame(data, index=ghgs, columns=sectors)

    def test_appliances_are_summed(self, synthetic_E: pd.DataFrame) -> None:
        result = expand_ghg_matrix_from_ceda_to_cornerstone(
            synthetic_E, CS_COMMODITY_LIST
        )
        # 1 + 2 + 3 + 4 = 10 for CO2
        assert result.loc['CO2', '335220'] == pytest.approx(10.0)
        # 0.5 + 1.0 + 1.5 + 2.0 = 5 for CH4
        assert result.loc['CH4', '335220'] == pytest.approx(5.0)

    def test_aluminum_renamed(self, synthetic_E: pd.DataFrame) -> None:
        result = expand_ghg_matrix_from_ceda_to_cornerstone(
            synthetic_E, CS_COMMODITY_LIST
        )
        assert result.loc['CO2', '33131B'] == pytest.approx(20.0)

    def test_waste_distributed_equally(self, synthetic_E: pd.DataFrame) -> None:
        result = expand_ghg_matrix_from_ceda_to_cornerstone(
            synthetic_E, CS_COMMODITY_LIST
        )
        waste_codes = WASTE_DISAGG_COMMODITIES['562000']
        for code in waste_codes:
            assert result.loc['CO2', code] == pytest.approx(70.0 / 7)

    def test_identity_preserved(self, synthetic_E: pd.DataFrame) -> None:
        result = expand_ghg_matrix_from_ceda_to_cornerstone(
            synthetic_E, CS_COMMODITY_LIST
        )
        assert result.loc['CO2', '211000'] == pytest.approx(100.0)

    def test_output_shape(self, synthetic_E: pd.DataFrame) -> None:
        result = expand_ghg_matrix_from_ceda_to_cornerstone(
            synthetic_E, CS_COMMODITY_LIST
        )
        assert result.shape == (2, 405)

    def test_total_emissions_conserved_for_identity_and_rename(
        self, synthetic_E: pd.DataFrame
    ) -> None:
        """Total emissions for 1:1 and rename codes should be preserved exactly."""
        result = expand_ghg_matrix_from_ceda_to_cornerstone(
            synthetic_E, CS_COMMODITY_LIST
        )
        # Total across all columns should equal input total
        assert result.loc['CO2'].sum() == pytest.approx(synthetic_E.loc['CO2'].sum())
        assert result.loc['CH4'].sum() == pytest.approx(synthetic_E.loc['CH4'].sum())

    def test_index_names(self, synthetic_E: pd.DataFrame) -> None:
        result = expand_ghg_matrix_from_ceda_to_cornerstone(
            synthetic_E, CS_COMMODITY_LIST
        )
        assert result.index.name == 'ghg'
        assert result.columns.name == 'sector'
