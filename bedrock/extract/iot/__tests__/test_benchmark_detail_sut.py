"""Unit tests for the benchmark detail Supply-Use panel loader.

The panel is one zip holding one workbook per table, each with a sheet per
benchmark year, so what the loader has to get right is member selection, sheet
selection and the header layout. All three are exercised here against a
synthetic archive built in ``tmp_path`` -- no GCS, no 5MB download.

The claim that the panel's 2017 sheets equal the single-year workbooks is
checked against the real data by
``io_2017.assert_benchmark_panel_matches_2017``, which needs both files and so
is a callable check rather than a test.
"""

import typing as ta
import zipfile
from pathlib import Path

import pandas as pd
import pytest

from bedrock.extract.iot import io_2017
from bedrock.utils.taxonomy.bea.matrix_mappings import (
    USA_BENCHMARK_DETAIL_SUT_MEMBER_MAPPING,
    USA_BENCHMARK_DETAIL_SUT_YEARS,
)

#: BEA puts five title rows above the header on every one of these sheets.
SKIPPED_HEADER_ROWS = 5

YEARS = (2007, 2012, 2017)


def _sheet(
    code_column: list[str], value_columns: ta.Mapping[str, list[float | None]]
) -> pd.DataFrame:
    """One sheet laid out as BEA lays them out: five title rows, then the header.

    Written with ``header=False`` so the column names land on the sixth row,
    which is where ``skiprows=5`` expects to find them.
    """
    names = ['Code', 'Commodity Description', *value_columns]
    rows: list[list[object]] = [[None] * len(names) for _ in range(SKIPPED_HEADER_ROWS)]
    rows.append(list(names))
    for position, code in enumerate(code_column):
        rows.append(
            [code, 'x', *(values[position] for values in value_columns.values())]
        )
    return pd.DataFrame(rows)


def _archive(path: Path) -> Path:
    """A stand-in for the published zip: both members, three years each."""
    workbooks: dict[str, Path] = {}
    for matrix_name, member in USA_BENCHMARK_DETAIL_SUT_MEMBER_MAPPING.items():
        book = path / member
        with pd.ExcelWriter(book) as writer:
            for year in YEARS:
                # One value per year so a sheet mix-up is visible in the value.
                if matrix_name == 'Use_SUT_detail':
                    codes = ['1111A0', 'T00SUB']
                    columns: dict[str, list[float | None]] = {
                        '1111A0': [float(year), 7.0],
                        '111CA': [1.0, None],
                    }
                else:
                    codes = ['1111A0', 'other']
                    columns = {
                        '1111A0': [float(year), 7.0],
                        'SUB': [-1.0, None],
                    }
                _sheet(codes, columns).to_excel(
                    writer, sheet_name=str(year), index=False, header=False
                )
        workbooks[member] = book

    bundle = path / 'panel.zip'
    with zipfile.ZipFile(bundle, 'w') as zf:
        for member, book in workbooks.items():
            zf.write(book, arcname=member)
    return bundle


@pytest.fixture
def local_panel(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> ta.Iterator[Path]:
    """Point the loader's GCS read at a local synthetic archive."""
    bundle = _archive(tmp_path)
    io_2017._load_benchmark_detail_supply_use_usa.cache_clear()

    def fake_load_from_gcs(
        name: str,
        sub_bucket: str,
        local_dir: str,
        loader: ta.Callable[[str], pd.DataFrame],
        **_: object,
    ) -> pd.DataFrame:
        assert name.endswith('.zip'), f'expected the panel zip, got {name}'
        return loader(str(bundle))

    monkeypatch.setattr(io_2017, 'load_from_gcs', fake_load_from_gcs)
    yield bundle
    io_2017._load_benchmark_detail_supply_use_usa.cache_clear()


@pytest.mark.parametrize('year', YEARS)
def test_each_year_comes_off_its_own_sheet(local_panel: Path, year: int) -> None:
    frame = io_2017._load_benchmark_detail_supply_use_usa('Use_SUT_detail', year)
    assert frame.loc['1111A0', '1111A0'] == float(year)


@pytest.mark.parametrize('matrix_name', sorted(USA_BENCHMARK_DETAIL_SUT_MEMBER_MAPPING))
def test_both_matrices_read_their_own_member(
    local_panel: Path, matrix_name: str
) -> None:
    frame = io_2017._load_benchmark_detail_supply_use_usa(matrix_name, 2012)
    # The Supply member has no T00SUB row and the Use member has no SUB column,
    # so either is proof the right workbook was opened.
    if matrix_name == 'Use_SUT_detail':
        assert 'T00SUB' in frame.index
        assert 'SUB' not in frame.columns
    else:
        assert 'SUB' in frame.columns
        assert 'T00SUB' not in frame.index


def test_header_rows_are_skipped_and_blanks_are_zero(local_panel: Path) -> None:
    frame = io_2017._load_benchmark_detail_supply_use_usa('Use_SUT_detail', 2017)
    assert frame.index.name == 'Code'
    assert list(frame.index) == ['1111A0', 'T00SUB']
    assert frame.loc['T00SUB', '111CA'] == 0


def test_positive_use_subsidies_are_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The sign convention check runs on the panel too, not only on 2017."""
    book = tmp_path / 'Use_SUT_Detail.xlsx'
    with pd.ExcelWriter(book) as writer:
        _sheet(['1111A0', 'T00SUB'], {'1111A0': [1.0, -5.0]}).to_excel(
            writer, sheet_name='2012', index=False, header=False
        )
    bundle = tmp_path / 'panel.zip'
    with zipfile.ZipFile(bundle, 'w') as zf:
        zf.write(book, arcname='Use_SUT_Detail.xlsx')

    io_2017._load_benchmark_detail_supply_use_usa.cache_clear()
    monkeypatch.setattr(
        io_2017,
        'load_from_gcs',
        lambda name, sub_bucket, local_dir, loader, **_: loader(str(bundle)),
    )
    with pytest.raises(AssertionError, match='T00SUB'):
        io_2017._load_benchmark_detail_supply_use_usa('Use_SUT_detail', 2012)
    io_2017._load_benchmark_detail_supply_use_usa.cache_clear()


def test_the_year_literal_covers_the_published_benchmarks() -> None:
    assert set(ta.get_args(USA_BENCHMARK_DETAIL_SUT_YEARS)) == set(YEARS)
