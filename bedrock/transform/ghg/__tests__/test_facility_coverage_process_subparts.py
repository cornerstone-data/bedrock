"""Tests for #1060: subpart C labeled as fuel; no vector mode with process subparts."""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.extract.stewifbs.facility_combustion import split_subpart_c_fuel
from bedrock.transform.ghg.facility_coverage import coverage_from_components


def _components() -> tuple[pd.Series, pd.Series, pd.Series, pd.Series]:
    # cement-like: little subpart C, kiln fuel and calcination under subpart H;
    # boiler-like: all combustion under subpart C, no process subparts.
    ghgrp = pd.Series({'327310': 2.0, '311221': 15.0})
    nei_below = pd.Series({'327310': 0.0, '311221': 0.5})
    nei_above = pd.Series({'327310': 0.0, '311221': 0.0})
    process = pd.Series({'327310': 62.0, '999999': 4.0})
    return ghgrp, nei_below, nei_above, process


def test_process_subparts_do_not_enter_coverage() -> None:
    out = coverage_from_components(*_components())
    assert out.loc['327310', 'coverage'] == pytest.approx(1.0)
    assert out.loc['327310', 'process_subparts_Mt'] == pytest.approx(62.0)
    assert out.loc['311221', 'coverage'] == pytest.approx(15.0 / 15.5)
    assert out.loc['311221', 'process_subparts_Mt'] == 0.0


def test_process_only_sector_without_combustion_is_dropped() -> None:
    out = coverage_from_components(*_components())
    assert '999999' not in out.index


def _ghgrp(
    fid: str, frs: str | None, naics: str, sector: str, co2e: float
) -> dict[str, object]:
    return {
        'FacilityID': fid,
        'FRS_ID': frs,
        'NAICS': naics,
        'sector': sector,
        'CO2e': co2e,
        'source': 'GHGRP',
        'fuel_class': 'unclassified',
        'Flowable': 'Other',
    }


def _nei(
    frs: str, naics: str, sector: str, cls: str, flow: str, co2e: float
) -> dict[str, object]:
    return {
        'FacilityID': f'n{frs}',
        'FRS_ID': frs,
        'NAICS': naics,
        'sector': sector,
        'fuel_class': cls,
        'Flowable': flow,
        'CO2e': co2e,
    }


def test_subpart_c_is_fuel_even_when_nei_files_the_kiln_as_process() -> None:
    # Lime plant: 100 total, 60 under subpart C. NEI files the kiln (90) under a
    # process SCC and a small boiler (10) as purchased natural gas.
    remainder = pd.DataFrame([_ghgrp('g1', 'F1', '327410', '327400', 100.0)])
    nei = pd.DataFrame(
        [
            _nei('F1', '327410', '327400', 'process', 'Other', 90.0),
            _nei('F1', '327410', '327400', 'purchased', 'Natural Gas', 10.0),
        ]
    )
    out = split_subpart_c_fuel(remainder, pd.Series({'g1': 60.0}), nei)
    by = out.groupby(['fuel_class', 'Flowable'])['CO2e'].sum()
    assert by[('purchased', 'Natural Gas')] == pytest.approx(60.0)
    assert by[('process', 'Other')] == pytest.approx(40.0)
    assert out['CO2e'].sum() == pytest.approx(100.0)


def test_fuel_type_falls_back_to_naics6_then_sector() -> None:
    remainder = pd.DataFrame(
        [
            _ghgrp('g1', 'F1', '327310', '327310', 50.0),  # no NEI fuel rows
            _ghgrp('g2', None, '327999', '327999', 10.0),  # no FRS match at all
        ]
    )
    nei = pd.DataFrame(
        [
            _nei('F1', '327310', '327310', 'process', 'Other', 40.0),
            _nei('F9', '327310', '327310', 'purchased', 'Coal', 30.0),
            _nei('F9', '327310', '327310', 'purchased', 'Natural Gas', 10.0),
        ]
    )
    out = split_subpart_c_fuel(remainder, pd.Series({'g1': 20.0, 'g2': 10.0}), nei)
    g1 = out[out['FacilityID'] == 'g1'].groupby('Flowable')['CO2e'].sum()
    assert g1['Coal'] == pytest.approx(15.0)
    assert g1['Natural Gas'] == pytest.approx(5.0)
    assert g1['Other'] == pytest.approx(30.0)  # the process part
    g2 = out[out['FacilityID'] == 'g2']
    assert g2['CO2e'].sum() == pytest.approx(10.0)
    assert set(g2['Flowable']) == {'Other'}  # no fuel information anywhere


def test_subpart_c_is_capped_at_the_facility_total() -> None:
    remainder = pd.DataFrame([_ghgrp('g1', 'F1', '331110', '331110', 10.0)])
    nei = pd.DataFrame(
        [_nei('F1', '331110', '331110', 'purchased', 'Natural Gas', 5.0)]
    )
    out = split_subpart_c_fuel(remainder, pd.Series({'g1': 12.0}), nei)
    assert out['CO2e'].sum() == pytest.approx(10.0)
    assert (out['fuel_class'] != 'process').all() or out.loc[
        out['fuel_class'] == 'process', 'CO2e'
    ].sum() == pytest.approx(0.0)
