"""Unit tests for locked waste NAICS map and year resolvers."""

from __future__ import annotations

import pytest

from bedrock.extract.disaggregation.rcra_waste_flows import (
    allocate_rcra_mass,
    map_rcra_naics_to_cornerstone,
)
from bedrock.extract.disaggregation.sas_waste_metrics import (
    map_sas_naics_to_cornerstone,
)
from bedrock.extract.disaggregation.waste_static_rules import SAS_NAICS_TO_CORNERSTONE
from bedrock.extract.disaggregation.waste_year_resolvers import (
    resolve_ec_year,
    resolve_industry_mix_source,
    resolve_rcra_year,
    resolve_sas_year,
    resolve_weights_year,
)


def test_562119_maps_to_oth_not_haz() -> None:
    assert SAS_NAICS_TO_CORNERSTONE["562119"] == "562OTH"
    assert map_sas_naics_to_cornerstone("562119") == "562OTH"
    assert map_rcra_naics_to_cornerstone("562119") == "562OTH"


def test_haz_codes() -> None:
    assert map_sas_naics_to_cornerstone("562112") == "562HAZ"
    assert map_sas_naics_to_cornerstone("562211") == "562HAZ"


def test_rcra_five_digit_residuals() -> None:
    assert map_rcra_naics_to_cornerstone("56211") == "562111"
    assert map_rcra_naics_to_cornerstone("56221") == "562212"
    assert map_rcra_naics_to_cornerstone("56299") == "562OTH"


def test_unmapped_rcra_fails_closed() -> None:
    with pytest.raises(ValueError, match="Unmapped"):
        map_rcra_naics_to_cornerstone("111111")


def test_allocate_rcra_mass_single() -> None:
    assert allocate_rcra_mass("562111", 10.0) == {"562111": 10.0}


@pytest.mark.parametrize(
    ("year", "expected"),
    [(2018, 2017), (2019, 2019), (2020, 2019), (2021, 2021), (2024, 2021)],
)
def test_resolve_rcra_year(year: int, expected: int) -> None:
    assert resolve_rcra_year(year) == expected


def test_resolve_industry_mix_2024_basic() -> None:
    assert resolve_industry_mix_source(2024) == ("aies_basic", 2024)
    assert resolve_industry_mix_source(2023) == ("aies_exp01", 2023)
    assert resolve_industry_mix_source(2022) == ("sas_table3", 2022)


def test_resolve_ec_pilot_freeze() -> None:
    assert resolve_ec_year(2024, ec_2022_wired=False) == 2017
    assert resolve_ec_year(2024, ec_2022_wired=True) == 2022
    # Default after EC 2022 wiring (2024 pilot): 2022
    assert resolve_ec_year(2024) == 2022
    assert resolve_ec_year(2022) == 2022


def test_resolve_weights_year_match_io() -> None:
    assert resolve_weights_year("match_io", usa_base_io_data_year=2024) == 2024
    assert resolve_weights_year(2017, usa_base_io_data_year=2024) == 2017
    assert resolve_weights_year(None, usa_base_io_data_year=2024) == 2017


def test_resolve_sas_carry_2022() -> None:
    assert resolve_sas_year(2024) == 2022
