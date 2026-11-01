import pandas as pd
import pytest

from bedrock.extract.census.census_export_concordance import (
    assign_exports_to_naics,
    concordance_by_year,
    hs6_successors,
)
from bedrock.extract.census.Census_USATrade import (
    _census_usatrade_filename,
    census_usatrade_url_helper,
)

CURRENT = pd.Series(
    {
        '7307110000': '332996',  # moved off 332919 in the current file
        '7307990000': '332919',
        '8479890001': '333998',
        '8479890002': '335210',
        '3402500000': '325611',
    }
)
REFERENCE = pd.Series(
    {
        '7307110000': 100.0,
        '7307990000': 50.0,
        '8479890001': 300.0,
        '8479890002': 100.0,
        '3402500000': 40.0,
    }
)
SUCCESSORS = {'340220': ['340250']}


def _assign(
    values: dict[str, float], own: dict[str, str]
) -> tuple[pd.Series, pd.Series]:
    return assign_exports_to_naics(
        pd.Series(values),
        own=pd.Series(own, dtype=str),
        current=CURRENT,
        reference=REFERENCE,
        hs6_successors=SUCCESSORS,
    )


def test_listed_code_takes_the_current_assignment_not_its_own_year() -> None:
    totals, by_rule = _assign({'7307110000': 10.0}, {'7307110000': '332919'})
    assert totals.to_dict() == {'332996': 10.0}
    assert by_rule['current concordance'] == 10.0


def test_retired_code_splits_by_its_successor_codes() -> None:
    # 8479890099 was retired; both 8479890001 and -002 trade in the reference
    # year but not in this one, so they are its successors (300:100).
    totals, by_rule = _assign({'8479890099': 40.0}, {'8479890099': '335210'})
    assert totals.to_dict() == pytest.approx({'333998': 30.0, '335210': 10.0})
    assert by_rule['successor codes'] == 40.0


def test_codes_still_traded_this_year_are_not_successors() -> None:
    # 8479890001 trades this year, so only 8479890002 is a successor.
    totals, by_rule = _assign(
        {'8479890001': 5.0, '8479890099': 8.0},
        {'8479890001': '333998', '8479890099': '335210'},
    )
    assert totals.to_dict() == pytest.approx({'333998': 5.0, '335210': 8.0})
    assert by_rule['successor codes'] == 8.0


def test_without_successor_codes_the_hs6_mix_applies() -> None:
    totals, by_rule = _assign(
        {'8479890001': 1.0, '8479890002': 1.0, '8479890099': 8.0},
        {'8479890099': '335210'},
    )
    assert totals.to_dict() == pytest.approx({'333998': 7.0, '335210': 3.0})
    assert by_rule['HS6 mix'] == 8.0


def test_removed_hs6_follows_its_hs2022_successor_heading() -> None:
    totals, by_rule = _assign({'3402205100': 12.0}, {'3402205100': '325613'})
    assert totals.to_dict() == {'325611': 12.0}
    assert by_rule['successor HS6'] == 12.0


def test_unmatched_codes_fall_back_to_own_year_then_low_value() -> None:
    totals, by_rule = _assign(
        {'9701100000': 7.0, '9880004000': 3.0, '9999999999': 1.0},
        {'9701100000': '930000'},
    )
    assert totals.to_dict() == {'930000': 7.0, '990000': 3.0}
    assert by_rule['own-year concordance'] == 7.0
    assert by_rule['low-value shipments'] == 3.0
    assert by_rule['unassigned'] == 1.0


def test_total_is_conserved_apart_from_unassigned() -> None:
    values = {'7307110000': 10.0, '8479890099': 40.0, '3402205100': 12.0}
    totals, by_rule = _assign(values, {})
    assert totals.sum() == pytest.approx(sum(values.values()))
    assert by_rule.drop('unassigned').sum() == pytest.approx(sum(values.values()))


def test_csv_loaders() -> None:
    concord = concordance_by_year(
        pd.DataFrame(
            {'year': ['2023', '2024'], 'commodity': ['a', 'a'], 'naics': ['1', '2']}
        )
    )
    assert concord[2023]['a'] == '1' and concord[2024]['a'] == '2'
    succ = hs6_successors(
        pd.DataFrame({'hs2022': ['940191', '940199'], 'hs2017': ['940190', '940190']})
    )
    assert succ == {'940190': ['940191', '940199']}


def test_export_url_reads_hs10_and_imports_stay_on_naics() -> None:
    build_url = (
        'https://api.census.gov/data/timeseries/intltrade/__flow__/naics?'
        'get=__get_fields__&COMM_LVL=NA6&YEAR=2021&MONTH=12'
    )
    imports, exports = census_usatrade_url_helper(
        build_url=build_url,
        config={
            'import_get_fields': 'NAICS,GEN_CIF_YR',
            'export_get_fields': 'E_COMMODITY,ALL_VAL_YR,DF',
        },
    )
    assert '/imports/naics?' in imports and 'COMM_LVL=NA6' in imports
    assert '/exports/hs?' in exports and 'COMM_LVL=HS10' in exports
    assert (
        _census_usatrade_filename(exports, 2021)
        == 'Census_USATrade_2021_exports_hs10.csv'
    )
    assert (
        _census_usatrade_filename(imports, 2021) == 'Census_USATrade_2021_imports.csv'
    )
