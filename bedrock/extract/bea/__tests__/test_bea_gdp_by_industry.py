"""Value added by industry from BEA's GDP-by-Industry accounts (#538).

The acceptance test is not "the archive parses" but "the numbers come back
correct through the FBA", so every expectation is a published 2017 figure and a
silent BEA vintage change fails here rather than inside a method.

Two kinds of check, deliberately separated. The config checks read only
``BEA_GDPbyIndustry.yaml`` and always run. The reconciliation checks need the
release archive and skip without it.
"""

from __future__ import annotations

import os
from typing import Any

import pandas as pd
import pytest

from bedrock.extract.bea.BEA_GDPbyIndustry import (
    extract_table_info,
    gdp_by_industry_local_path,
)
from bedrock.extract.flowbyactivity import getFlowByActivity
from bedrock.extract.generateflowbyactivity import load_fba_config
from bedrock.extract.iot.io_2017 import _load_usa_summary_sut
from bedrock.utils.taxonomy.bea.matrix_mappings import USA_SUMMARY_SUT_YEARS
from bedrock.utils.taxonomy.bea.v2017_industry_summary import (
    USA_2017_SUMMARY_INDUSTRY_CODES,
)

YEAR: USA_SUMMARY_SUT_YEARS = 2017

#: Millions of dollars, the unit ``TVA113`` publishes in.  The FBA carries
#: dollars, so every reading here is divided by this.
MILLION = 1e6

#: BEA publishes these to the million, so a reconciliation closer than this is
#: exact as far as the source can say.
ROUNDING = 25.0

#: Line 1 of ``TVA113``, the all-industry root, in millions.  Its three
#: components sum to it exactly, which is the identity the whole table rests on.
ROOT_2017 = {
    'VAPRO': 19_612_102.0,
    'V00100': 10_434_978.0,
    'V00200': 1_304_097.0,
    'V00300': 7_873_027.0,
}

#: General government pays no taxes on production, so these four industry lines
#: carry no ``V00200`` at all -- the same accounting rule ``tax_axis_conversion``
#: documents for the SUT's government columns.  Absent rows, not zero rows.
NO_TAXES_LINES = (361, 365, 369, 381)


def _config() -> dict[str, Any]:
    _, _, config = load_fba_config('BEA_GDPbyIndustry', YEAR)
    return config


def _summary_row(row: str, codes: list[str]) -> 'pd.Series[float]':
    """One summary Use SUT row over the given industry codes, in millions."""
    series = _load_usa_summary_sut('Use_SUT_summary', YEAR).loc[row]
    assert isinstance(series, pd.Series)
    return pd.to_numeric(series.reindex(codes), errors='coerce').fillna(0.0)


def _summary_industry_codes() -> list[str]:
    table = _load_usa_summary_sut('Use_SUT_summary', YEAR)
    return [c for c in USA_2017_SUMMARY_INDUSTRY_CODES if c in table.columns]


def _archive_missing() -> bool:
    return not os.path.exists(gdp_by_industry_local_path())


requires_archive = pytest.mark.skipif(
    _archive_missing(),
    reason='GDPbyInd.zip is not cached under extract/input_data',
)


@pytest.fixture(scope='module')
def fba() -> pd.DataFrame:
    """The 2017 FBA with ``Table``/``Code``/``Line`` split out."""
    return extract_table_info(getFlowByActivity('BEA_GDPbyIndustry', YEAR))


def test_config_declares_the_components_table() -> None:
    """``TVA113-A`` is the sheet, and the annual one.

    ``TVA113-Q`` is the quarterly restatement of the same concept; reading both
    would double every industry.
    """
    config = _config()
    assert config['files'] == ['ValueAdded.xlsx']
    assert config['sheets'] == ['TVA113-A']
    assert not any(sheet.endswith('-Q') for sheet in config['sheets'])


def test_config_does_not_redownload_by_default() -> None:
    """The archive is cached, not re-fetched on every run."""
    assert _config()['extract_data_from_raw_sources'] is False


@requires_archive
def test_root_components_sum_to_value_added(fba: pd.DataFrame) -> None:
    """Line 1: compensation + taxes + surplus = value added, exactly.

    This is the identity that makes the table usable as a ``V00300`` source at
    all. If it stops holding, the component labels have moved.
    """
    root = fba[fba['Line'] == 1].set_index('Code')['FlowAmount'] / MILLION
    for code, expected in ROOT_2017.items():
        assert root[code] == pytest.approx(expected, abs=ROUNDING), code
    components = float(root[['V00100', 'V00200', 'V00300']].sum())
    assert components == pytest.approx(root['VAPRO'], abs=ROUNDING)


@requires_archive
def test_gross_operating_surplus_matches_the_summary_sut(fba: pd.DataFrame) -> None:
    """Every one of the 71 BEA summary industries' ``V003``, to the dollar.

    ⚠️ This is the test that says what the source *is*: BEA's GDP-by-Industry
    surplus is the summary Use SUT's ``V003`` row by another door, not an
    independent estimate of it. Step 5's Decision 3 holds the summary SUT in the
    test set, so anything consuming this as an input is spending that table --
    see NIPA_VA_surplus_2017.yaml.
    """
    codes = _summary_industry_codes()
    published = _summary_row('V003', codes)
    surplus = set((fba.loc[fba['Code'] == 'V00300', 'FlowAmount'] / MILLION).round(0))
    unmatched = [c for c in codes if round(float(published[c])) not in surplus]
    assert not unmatched, f'{len(unmatched)} summary industries unmatched: {unmatched}'


@requires_archive
def test_compensation_matches_the_summary_sut(fba: pd.DataFrame) -> None:
    """The same for ``V001``, which also ties the source to ``T60200D``."""
    codes = _summary_industry_codes()
    published = _summary_row('V001', codes)
    compensation = set(
        (fba.loc[fba['Code'] == 'V00100', 'FlowAmount'] / MILLION).round(0)
    )
    unmatched = [c for c in codes if round(float(published[c])) not in compensation]
    assert not unmatched, f'{len(unmatched)} summary industries unmatched: {unmatched}'


@requires_archive
def test_general_government_carries_no_taxes_on_production(
    fba: pd.DataFrame,
) -> None:
    """Four industry lines have no ``V00200`` row, and that is the rule.

    A tax levied by government and remitted by a government producer nets out.
    Asserted as *absent* rather than zero, because a future parser that filled
    them with zeros would look identical in a total and wrong in a share.
    """
    taxed = set(fba.loc[fba['Code'] == 'V00200', 'Line'])
    assert not (set(NO_TAXES_LINES) & taxed)
    # and they do carry the other three, so the lines themselves are real
    for code in ('VAPRO', 'V00100', 'V00300'):
        present = set(fba.loc[fba['Code'] == code, 'Line'])
        assert set(NO_TAXES_LINES) <= present, code


@requires_archive
def test_addenda_header_produces_no_rows(fba: pd.DataFrame) -> None:
    """``Addenda:`` is a bare header with no data and must not become an industry.

    The three industries beneath it -- private goods-producing,
    private services-producing, ICT-producing -- *are* in the FBA and *are*
    restatements of industries already counted, so a method summing the table
    without an explicit line list double-counts them.
    """
    assert 'Addenda:' not in set(fba['ActivityProducedBy'])
    restatements = {
        'Private goods-producing industries',
        'Private services-producing industries',
        'Information-communications-technology-producing industries',
    }
    assert restatements <= set(fba['ActivityProducedBy'])


@requires_archive
def test_footnote_markers_are_stripped(fba: pd.DataFrame) -> None:
    """No industry name keeps BEA's trailing ``\\1\\`` reference."""
    assert not fba['ActivityProducedBy'].str.contains(r'\\\d+\\').any()


@requires_archive
def test_line_is_the_industry_line_not_the_component_row(
    fba: pd.DataFrame,
) -> None:
    """All four of an industry's rows share one ``Line``.

    Selections are made on the industry axis, so ``Line`` has to identify the
    industry rather than the value's own row in the sheet. ``Farms`` is line 13
    and its three components are sheet lines 14-16; all four carry 13.
    """
    per_line = fba.groupby('Line')['Code'].nunique()
    assert per_line.max() == 4
    farms = fba[fba['ActivityProducedBy'] == 'Farms']
    assert set(farms['Line']) == {13}
    assert set(farms['Code']) == {'VAPRO', 'V00100', 'V00200', 'V00300'}
