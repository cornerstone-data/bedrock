import pathlib
from collections import defaultdict
from typing import cast

import pandas as pd
import pytest

from bedrock.extract.disaggregation.waste_weights import (
    WasteDisaggCorrespondenceError,
    WasteDisaggWeightError,
    WasteDisaggWeights,
    WasteWeightTable,
    _apply_correspondence_to_series,
    _empty_weight_table,
    load_waste_disagg_weights,
)
from bedrock.utils.config.usa_config import EEIOWasteDisaggConfig
from bedrock.utils.taxonomy.cornerstone.commodities import WASTE_DISAGG_COMMODITIES


def _make_table(
    index: list[str], columns: list[str], values: list[list[float]]
) -> WasteWeightTable:
    return pd.DataFrame(values, index=index, columns=columns, dtype=float)


def test_waste_disagg_weights_construction() -> None:
    idx = ['562111', '562212']
    # 2x2 table (industry x commodity), e.g. intersection
    tbl = _make_table(idx, idx, [[0.5, 0.0], [0.0, 0.5]])
    empty_tbl = _empty_weight_table()
    w = WasteDisaggWeights(
        use_intersection=tbl.copy(),
        use_waste_industry_columns_all_rows=tbl.copy(),
        use_waste_commodity_rows_all_columns=tbl.copy(),
        use_waste_rows_specific_columns=empty_tbl,
        use_va_rows_for_waste_industry_columns=tbl.copy(),
        use_fd_columns_for_waste_commodity_rows=empty_tbl,
        make_intersection=tbl.copy(),
        make_waste_commodity_columns_all_rows=tbl.copy(),
        make_waste_commodity_columns_specific_rows=empty_tbl,
        make_waste_industry_rows_specific_columns=empty_tbl,
        year=2017,
        source_name='Test',
    )
    assert w.year == 2017
    assert w.source_name == 'Test'
    assert list(w.use_intersection.index) == idx
    assert list(w.use_intersection.columns) == idx
    assert w.use_waste_rows_specific_columns.empty
    assert w.make_waste_industry_rows_specific_columns.empty


def test_waste_disagg_weights_required_fields() -> None:
    idx = ['562111']
    tbl = _make_table(idx, idx, [[1.0]])
    empty_tbl = _empty_weight_table()
    w = WasteDisaggWeights(
        use_intersection=tbl.copy(),
        use_waste_industry_columns_all_rows=tbl.copy(),
        use_waste_commodity_rows_all_columns=tbl.copy(),
        use_waste_rows_specific_columns=empty_tbl,
        use_va_rows_for_waste_industry_columns=tbl.copy(),
        use_fd_columns_for_waste_commodity_rows=empty_tbl,
        make_intersection=tbl.copy(),
        make_waste_commodity_columns_all_rows=tbl.copy(),
        make_waste_commodity_columns_specific_rows=empty_tbl,
        make_waste_industry_rows_specific_columns=empty_tbl,
        year=2017,
        source_name='WasteDisaggregationDetail2017',
    )
    assert hasattr(w, 'use_intersection')
    assert hasattr(w, 'make_intersection')
    assert hasattr(w, 'year')
    assert hasattr(w, 'source_name')


@pytest.mark.eeio_integration
def _write_csv(path: pathlib.Path, rows: list[dict[str, str]]) -> None:
    df = pd.DataFrame(rows)
    df.to_csv(path, index=False)


@pytest.mark.eeio_integration
def test_load_waste_disagg_weights_normalizes_slices(tmp_path: pathlib.Path) -> None:
    use_path = tmp_path / 'use.csv'
    make_path = tmp_path / 'make.csv'
    _write_csv(
        use_path,
        [
            {
                'IndustryCode': '562111/US',
                'CommodityCode': '562000/US',
                'PercentUsed': '0.6',
            },
            {
                'IndustryCode': '562212/US',
                'CommodityCode': '562000/US',
                'PercentUsed': '0.4',
            },
            {
                'IndustryCode': '562111/US',
                'CommodityCode': '562111/US',
                'PercentUsed': '0.3',
            },
            {
                'IndustryCode': '562212/US',
                'CommodityCode': '562212/US',
                'PercentUsed': '0.7',
            },
        ],
    )
    _write_csv(
        make_path,
        [
            {
                'IndustryCode': '562000/US',
                'CommodityCode': '562111/US',
                'PercentMake': '0.25',
            },
            {
                'IndustryCode': '562000/US',
                'CommodityCode': '562212/US',
                'PercentMake': '0.75',
            },
            {
                'IndustryCode': '562111/US',
                'CommodityCode': '562111/US',
                'PercentMake': '0.5',
            },
            {
                'IndustryCode': '562212/US',
                'CommodityCode': '562212/US',
                'PercentMake': '0.5',
            },
        ],
    )

    cfg = EEIOWasteDisaggConfig(
        use_weights_file=str(use_path),
        make_weights_file=str(make_path),
        year=2017,
        source_name='test',
    )
    waste_sectors = ['562111', '562212']

    weights = load_waste_disagg_weights(
        cfg,
        disagg_original_code='562000',
        disagg_new_codes=waste_sectors,
        waste_sectors=waste_sectors,
        naics_to_cornerstone=None,
    )

    assert pytest.approx(float(weights.use_intersection.values.sum()), rel=1e-6) == 1.0
    assert pytest.approx(float(weights.make_intersection.values.sum()), rel=1e-6) == 1.0
    assert all(code in weights.use_intersection.index for code in waste_sectors)
    assert all(code in weights.use_intersection.columns for code in waste_sectors)
    assert all(code in weights.make_intersection.index for code in waste_sectors)
    assert all(code in weights.make_intersection.columns for code in waste_sectors)


@pytest.mark.eeio_integration
def test_load_waste_disagg_weights_missing_sectors_get_zero_weight(
    tmp_path: pathlib.Path,
) -> None:
    use_path = tmp_path / 'use_missing.csv'
    make_path = tmp_path / 'make_missing.csv'
    _write_csv(
        use_path,
        [
            {
                'IndustryCode': '562111',
                'CommodityCode': '562111',
                'PercentUsed': '1.0',
            },
        ],
    )
    _write_csv(
        make_path,
        [
            {
                'IndustryCode': '562000',
                'CommodityCode': '562111',
                'PercentMake': '1.0',
            },
        ],
    )

    cfg = EEIOWasteDisaggConfig(
        use_weights_file=str(use_path),
        make_weights_file=str(make_path),
        year=2017,
        source_name='test',
    )
    waste_sectors = ['562111', '562212']

    weights = load_waste_disagg_weights(
        cfg,
        disagg_original_code='562000',
        disagg_new_codes=waste_sectors,
        waste_sectors=waste_sectors,
        naics_to_cornerstone=None,
    )

    assert weights.use_intersection.loc['562212', :].sum() == pytest.approx(0.0)
    assert weights.use_intersection.loc[:, '562212'].sum() == pytest.approx(0.0)
    assert pytest.approx(float(weights.use_intersection.values.sum()), rel=1e-6) == 1.0


@pytest.mark.eeio_integration
def test_load_waste_disagg_weights_all_zero_raises(tmp_path: pathlib.Path) -> None:
    use_path = tmp_path / 'use_zero.csv'
    make_path = tmp_path / 'make_zero.csv'
    _write_csv(
        use_path,
        [
            {
                'IndustryCode': '562111',
                'CommodityCode': '562000',
                'PercentUsed': '0.0',
            },
        ],
    )
    _write_csv(
        make_path,
        [
            {
                'IndustryCode': '562000',
                'CommodityCode': '562111',
                'PercentMake': '0.0',
            },
        ],
    )

    cfg = EEIOWasteDisaggConfig(
        use_weights_file=str(use_path),
        make_weights_file=str(make_path),
        year=2017,
        source_name='test',
    )

    with pytest.raises(WasteDisaggWeightError):
        load_waste_disagg_weights(
            cfg,
            disagg_original_code='562000',
            disagg_new_codes=['562111'],
            waste_sectors=['562111'],
            naics_to_cornerstone=None,
        )


@pytest.mark.eeio_integration
def test_load_waste_disagg_weights_missing_required_column(
    tmp_path: pathlib.Path,
) -> None:
    use_path = tmp_path / 'use_missing_column.csv'
    make_path = tmp_path / 'make_missing_column.csv'
    _write_csv(
        use_path,
        [
            {
                'IndustryCode': '562111',
                'CommodityCode': '562000',
            },
        ],
    )
    _write_csv(
        make_path,
        [
            {
                'IndustryCode': '562000',
                'CommodityCode': '562111',
                'PercentMake': '1.0',
            },
        ],
    )

    cfg = EEIOWasteDisaggConfig(
        use_weights_file=str(use_path),
        make_weights_file=str(make_path),
        year=2017,
        source_name='test',
    )

    with pytest.raises(WasteDisaggWeightError):
        load_waste_disagg_weights(
            cfg,
            disagg_original_code='562000',
            disagg_new_codes=['562111'],
            waste_sectors=['562111'],
            naics_to_cornerstone=None,
        )


@pytest.mark.eeio_integration
def test_load_waste_disagg_weights_nan_values_raise(tmp_path: pathlib.Path) -> None:
    use_path = tmp_path / 'use_nan.csv'
    make_path = tmp_path / 'make_nan.csv'
    _write_csv(
        use_path,
        [
            {
                'IndustryCode': '562111',
                'CommodityCode': '562000',
                'PercentUsed': 'not-a-number',
            },
        ],
    )
    _write_csv(
        make_path,
        [
            {
                'IndustryCode': '562000',
                'CommodityCode': '562111',
                'PercentMake': '1.0',
            },
        ],
    )

    cfg = EEIOWasteDisaggConfig(
        use_weights_file=str(use_path),
        make_weights_file=str(make_path),
        year=2017,
        source_name='test',
    )

    with pytest.raises(WasteDisaggWeightError):
        load_waste_disagg_weights(
            cfg,
            disagg_original_code='562000',
            disagg_new_codes=['562111'],
            waste_sectors=['562111'],
            naics_to_cornerstone=None,
        )


@pytest.mark.eeio_integration
def test_apply_correspondence_to_series_success() -> None:
    series = pd.Series([0.5, 0.5], index=['N1', 'N2'], dtype=float)
    mapping = {'N1': ['C1'], 'N2': ['C2']}
    target_codes = ['C1', 'C2']

    result = _apply_correspondence_to_series(series, mapping, target_codes)

    assert list(result.index) == target_codes
    assert pytest.approx(float(result.sum()), rel=1e-6) == 1.0


@pytest.mark.eeio_integration
def test_apply_correspondence_to_series_incomplete_mapping_raises() -> None:
    series = pd.Series([1.0], index=['N1'], dtype=float)
    mapping = {'N1': ['C1']}
    target_codes = ['C1', 'C2']

    with pytest.raises(WasteDisaggCorrespondenceError):
        _apply_correspondence_to_series(series, mapping, target_codes)


# ---------------------------------------------------------------------------
# Fixtures shared by the 2017-CSV value-verification tests
# ---------------------------------------------------------------------------

DATA_DIR = pathlib.Path(__file__).resolve().parents[1]
USE_PATH = DATA_DIR / 'WasteDisaggregationDetail2017_Use.csv'
MAKE_PATH = DATA_DIR / 'WasteDisaggregationDetail2017_Make.csv'

WASTE_CODES = cast(list[str], list(WASTE_DISAGG_COMMODITIES['562000']))

# VA row codes present in the Use CSV
VA_ROWS = ['V00100', 'V00200', 'V00300']

# FD/industry column codes that produce Use row data for commodity disaggregation
# (all IndustryCodes in Use CSV that are NOT new waste codes, NOT FD, NOT VA)
# Includes 562000 (Use row sum), S00101, S00102 … etc.

_RAW_MAKE_INTERSECTION = {
    # (industry, commodity): raw CSV PercentMake
    ('562111', '562111'): 4.23e-01,
    ('562HAZ', '562HAZ'): 1.50e-01,
    ('562212', '562212'): 7.87e-02,
    ('562213', '562213'): 1.31e-02,
    ('562910', '562910'): 1.68e-01,
    ('562920', '562920'): 5.23e-02,
    ('562OTH', '562OTH'): 1.15e-01,
}

_RAW_MAKE_COL_SUM = {
    # (industry, commodity): raw CSV PercentMake  — "Make column sum" rows + "commodity disaggregation"
    ('562000', '562111'): 4.79e-01,
    ('562000', '562HAZ'): 1.28e-01,
    ('562000', '562212'): 8.12e-02,
    ('562000', '562213'): 1.36e-02,
    ('562000', '562910'): 1.47e-01,
    ('562000', '562920'): 5.40e-02,
    ('562000', '562OTH'): 9.78e-02,
    ('221300', '562111'): 7.63e-01,
    ('221300', '562212'): 1.29e-01,
    ('221300', '562213'): 2.16e-02,
    ('221300', '562920'): 8.61e-02,
    ('484000', '562111'): 1.00e00,
    ('511110', '562111'): 1.00e00,
    ('561700', '562910'): 1.00e00,
    ('GSLGO', '562111'): 7.63e-01,
    ('GSLGO', '562212'): 1.29e-01,
    ('GSLGO', '562213'): 2.16e-02,
    ('GSLGO', '562920'): 8.61e-02,
}

_RAW_MAKE_INDUSTRY_ROWS = {
    # (industry, commodity): raw CSV PercentMake — "industry disaggregation" rows (non-waste commodities)
    ('562212', '211000'): 1.00e00,
    ('562111', '2332D0'): 4.31e-01,
    ('562HAZ', '2332D0'): 1.94e-01,
    ('562212', '2332D0'): 8.19e-02,
    ('562213', '2332D0'): 1.36e-02,
    ('562910', '2332D0'): 1.51e-01,
    ('562920', '2332D0'): 4.06e-02,
    ('562OTH', '2332D0'): 8.85e-02,
    ('562111', '444000'): 4.31e-01,
    ('562HAZ', '444000'): 1.94e-01,
    ('562212', '444000'): 8.19e-02,
    ('562213', '444000'): 1.36e-02,
    ('562910', '444000'): 1.51e-01,
    ('562920', '444000'): 4.06e-02,
    ('562OTH', '444000'): 8.85e-02,
    ('562111', '48A000'): 4.31e-01,
    ('562HAZ', '48A000'): 1.94e-01,
    ('562212', '48A000'): 8.19e-02,
    ('562213', '48A000'): 1.36e-02,
    ('562910', '48A000'): 1.51e-01,
    ('562920', '48A000'): 4.06e-02,
    ('562OTH', '48A000'): 8.85e-02,
    ('562111', '532400'): 4.31e-01,
    ('562HAZ', '532400'): 1.94e-01,
    ('562212', '532400'): 8.19e-02,
    ('562213', '532400'): 1.36e-02,
    ('562910', '532400'): 1.51e-01,
    ('562920', '532400'): 4.06e-02,
    ('562OTH', '532400'): 8.85e-02,
    ('562111', '541511'): 4.31e-01,
    ('562HAZ', '541511'): 1.94e-01,
    ('562212', '541511'): 8.19e-02,
    ('562213', '541511'): 1.36e-02,
    ('562910', '541511'): 1.51e-01,
    ('562920', '541511'): 4.06e-02,
    ('562OTH', '541511'): 8.85e-02,
    ('562111', '5416A0'): 4.31e-01,
    ('562HAZ', '5416A0'): 1.94e-01,
    ('562212', '5416A0'): 8.19e-02,
    ('562213', '5416A0'): 1.36e-02,
    ('562910', '5416A0'): 1.51e-01,
    ('562920', '5416A0'): 4.06e-02,
    ('562OTH', '5416A0'): 8.85e-02,
    ('562111', '541700'): 4.31e-01,
    ('562HAZ', '541700'): 1.94e-01,
    ('562212', '541700'): 8.19e-02,
    ('562213', '541700'): 1.36e-02,
    ('562910', '541700'): 1.51e-01,
    ('562920', '541700'): 4.06e-02,
    ('562OTH', '541700'): 8.85e-02,
    ('562920', 'S00401'): 1.00e00,
}

_RAW_USE_INTERSECTION = {
    ('562111', '562111'): 6.16e-06,
    ('562111', '562HAZ'): 1.11e-02,
    ('562111', '562212'): 9.32e-04,
    ('562111', '562213'): 6.16e-06,
    ('562111', '562910'): 0.00,
    ('562111', '562920'): 5.89e-06,
    ('562111', '562OTH'): 2.69e-04,
    ('562HAZ', '562111'): 3.24e-05,
    ('562HAZ', '562HAZ'): 5.80e-01,
    ('562HAZ', '562212'): 7.17e-02,
    ('562HAZ', '562213'): 3.24e-05,
    ('562HAZ', '562910'): 0.00,
    ('562HAZ', '562920'): 6.63e-04,
    ('562HAZ', '562OTH'): 1.41e-02,
    ('562212', '562111'): 6.22e-06,
    ('562212', '562HAZ'): 1.43e-01,
    ('562212', '562212'): 5.98e-04,
    ('562212', '562213'): 6.22e-06,
    ('562212', '562910'): 0.00,
    ('562212', '562920'): 2.97e-05,
    ('562212', '562OTH'): 5.54e-03,
    ('562213', '562111'): 0.00,
    ('562213', '562HAZ'): 1.84e-03,
    ('562213', '562212'): 0.00,
    ('562213', '562213'): 0.00,
    ('562213', '562910'): 0.00,
    ('562213', '562920'): 0.00,
    ('562213', '562OTH'): 0.00,
    ('562910', '562111'): 1.11e-05,
    ('562910', '562HAZ'): 5.54e-02,
    ('562910', '562212'): 1.04e-03,
    ('562910', '562213'): 1.11e-05,
    ('562910', '562910'): 1.28e-03,
    ('562910', '562920'): 0.00,
    ('562910', '562OTH'): 2.12e-04,
    ('562920', '562111'): 0.00,
    ('562920', '562HAZ'): 4.06e-03,
    ('562920', '562212'): 1.87e-08,
    ('562920', '562213'): 0.00,
    ('562920', '562910'): 0.00,
    ('562920', '562920'): 4.22e-05,
    ('562920', '562OTH'): 2.46e-06,
    ('562OTH', '562111'): 4.90e-05,
    ('562OTH', '562HAZ'): 9.49e-02,
    ('562OTH', '562212'): 3.89e-05,
    ('562OTH', '562213'): 4.90e-05,
    ('562OTH', '562910'): 0.00,
    ('562OTH', '562920'): 1.03e-05,
    ('562OTH', '562OTH'): 1.33e-02,
}

_RAW_USE_COL_SUM = {
    # Use column sum rows: IndustryCode=waste subsector, CommodityCode=562000
    ('562111', '562000'): 4.76e-01,
    ('562HAZ', '562000'): 1.21e-01,
    ('562212', '562000'): 8.27e-02,
    ('562213', '562000'): 1.51e-02,
    ('562910', '562000'): 1.66e-01,
    ('562920', '562000'): 4.48e-02,
    ('562OTH', '562000'): 9.43e-02,
}

_RAW_USE_ROW_SUM = {
    # Use row sum rows: IndustryCode=562000, CommodityCode=waste subsector
    ('562000', '562111'): 4.63e-01,
    ('562000', '562HAZ'): 1.03e-01,
    ('562000', '562212'): 9.80e-02,
    ('562000', '562213'): 1.50e-02,
    ('562000', '562910'): 1.66e-01,
    ('562000', '562920'): 7.53e-02,
    ('562000', '562OTH'): 7.97e-02,
}

_RAW_USE_VA = {
    # VA disaggregation: IndustryCode=waste subsector, CommodityCode=V00100/V00200/V00300
    ('562111', 'V00100'): 4.50e-01,
    ('562HAZ', 'V00100'): 2.30e-02,
    ('562212', 'V00100'): 7.86e-02,
    ('562213', 'V00100'): 1.37e-02,
    ('562910', 'V00100'): 2.02e-01,
    ('562920', 'V00100'): 8.47e-02,
    ('562OTH', 'V00100'): 1.48e-01,
    ('562111', 'V00200'): 4.50e-01,
    ('562HAZ', 'V00200'): 2.30e-02,
    ('562212', 'V00200'): 7.86e-02,
    ('562213', 'V00200'): 1.37e-02,
    ('562910', 'V00200'): 2.02e-01,
    ('562920', 'V00200'): 8.47e-02,
    ('562OTH', 'V00200'): 1.48e-01,
    ('562111', 'V00300'): 4.50e-01,
    ('562HAZ', 'V00300'): 2.30e-02,
    ('562212', 'V00300'): 7.86e-02,
    ('562213', 'V00300'): 1.37e-02,
    ('562910', 'V00300'): 2.02e-01,
    ('562920', 'V00300'): 8.47e-02,
    ('562OTH', 'V00300'): 1.48e-01,
}

_RAW_USE_FD_DISAGG: dict[str, dict[str, float]] = {
    # Only Cornerstone FD codes (from final_demand.py) that appear in the Use weights CSV.
    # BEA aggregate codes (S00101, S00201, GSLGE, 813100, etc.) are excluded because
    # they are not Cornerstone final-demand columns.
    'F01000': {
        '562111': 7.22e-01,
        '562HAZ': 1.68e-02,
        '562212': 4.00e-02,
        '562213': 3.35e-03,
        '562910': 6.91e-02,
        '562920': 3.41e-02,
        '562OTH': 1.15e-01,
    },
}


@pytest.fixture(scope='module')
def weights_2017() -> WasteDisaggWeights:
    cfg = EEIOWasteDisaggConfig(
        use_weights_file=str(USE_PATH),
        make_weights_file=str(MAKE_PATH),
        year=2017,
        source_name='WasteDisaggregationDetail2017',
    )
    return load_waste_disagg_weights(
        cfg,
        disagg_original_code='562000',
        disagg_new_codes=WASTE_CODES,
        waste_sectors=WASTE_CODES,
        va_row_codes=VA_ROWS,
        naics_to_cornerstone=None,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _check_ratios(
    tbl: 'WasteWeightTable',
    raw: dict[tuple[str, str], float],
    label: str,
) -> None:
    """Assert that every pair in raw appears in tbl and that the ratios between
    pairs (i.e. relative weights) exactly match the raw CSV values.

    We compare ratios rather than absolute values because the loader normalises
    each slice.  For any two entries (r1,c1) and (r2,c2) in the same
    normalisation unit the ratio raw[r1,c1] / raw[r2,c2] must equal
    tbl[r1,c1] / tbl[r2,c2].
    """
    pairs = [(r, c, v) for (r, c), v in raw.items()]
    # Use the first non-zero pair as the reference
    ref_r, ref_c, ref_raw = next(((r, c, v) for r, c, v in pairs if v > 0), pairs[0])
    ref_tbl = cast(float, tbl.loc[ref_r, ref_c])
    assert ref_tbl > 0, f'{label}: reference cell ({ref_r},{ref_c}) is zero in output'
    scale = ref_tbl / ref_raw
    for row, col, expected_raw in pairs:
        if row not in tbl.index or col not in tbl.columns:
            continue
        got = cast(float, tbl.loc[row, col])
        expected_scaled = expected_raw * scale
        assert got == pytest.approx(expected_scaled, rel=1e-4, abs=1e-12), (
            f'{label}: ({row},{col}) expected {expected_scaled:.6e} got {got:.6e}'
        )


def _check_row_ratios(
    tbl: 'WasteWeightTable',
    raw: dict[tuple[str, str], float],
    label: str,
) -> None:
    """For row-normalised slices, verify ratios within each row."""
    by_row: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for (r, c), v in raw.items():
        by_row[r].append((c, v))

    for row, entries in by_row.items():
        if row not in tbl.index:
            continue
        ref_col, ref_raw = next(((c, v) for c, v in entries if v > 0), entries[0])
        if ref_col not in tbl.columns:
            continue
        ref_tbl = cast(float, tbl.loc[row, ref_col])
        assert ref_tbl > 0, f'{label} row={row}: reference col {ref_col} is zero'
        scale = ref_tbl / ref_raw
        for col, raw_val in entries:
            if col not in tbl.columns:
                continue
            got = cast(float, tbl.loc[row, col])
            expected = raw_val * scale
            assert got == pytest.approx(expected, rel=1e-4, abs=1e-12), (
                f'{label} row={row} col={col}: expected {expected:.6e} got {got:.6e}'
            )


# ---------------------------------------------------------------------------
# Value-verification tests (one per WasteDisaggWeights field)
# ---------------------------------------------------------------------------


class TestMakeIntersection:
    """make_intersection — diagonal entries from 'Make table intersection' rows."""

    def test_all_diagonal_pairs_present(self, weights_2017: WasteDisaggWeights) -> None:
        tbl = weights_2017.make_intersection
        for r, c in _RAW_MAKE_INTERSECTION:
            assert r in tbl.index, f'row {r} missing'
            assert c in tbl.columns, f'col {c} missing'

    def test_ratios_preserved(self, weights_2017: WasteDisaggWeights) -> None:
        _check_ratios(
            weights_2017.make_intersection,
            _RAW_MAKE_INTERSECTION,
            'make_intersection',
        )

    def test_off_diagonal_are_zero(self, weights_2017: WasteDisaggWeights) -> None:
        tbl = weights_2017.make_intersection
        for r in tbl.index:
            for c in tbl.columns:
                if r != c:
                    assert float(tbl.loc[r, c]) == pytest.approx(0.0, abs=1e-12), (
                        f'make_intersection off-diagonal ({r},{c}) should be 0'
                    )


class TestMakeWasteCommodityColumnsAllRows:
    """make_waste_commodity_columns_all_rows = default (e.g. 562000); specific rows in make_waste_commodity_columns_specific_rows."""

    def _combined_make_commodity_columns(
        self, weights_2017: WasteDisaggWeights
    ) -> WasteWeightTable:
        """Default row(s) + row-specific overrides = full logical table."""
        return pd.concat(
            [
                weights_2017.make_waste_commodity_columns_all_rows,
                weights_2017.make_waste_commodity_columns_specific_rows,
            ],
            axis=0,
        )

    def test_all_pairs_present(self, weights_2017: WasteDisaggWeights) -> None:
        tbl = self._combined_make_commodity_columns(weights_2017)
        for r, c in _RAW_MAKE_COL_SUM:
            assert r in tbl.index, f'row {r} missing'
            assert c in tbl.columns, f'col {c} missing'

    def test_row_ratios_preserved(self, weights_2017: WasteDisaggWeights) -> None:
        _check_row_ratios(
            self._combined_make_commodity_columns(weights_2017),
            _RAW_MAKE_COL_SUM,
            'make_waste_commodity_columns (all_rows + specific_rows)',
        )


class TestMakeWasteIndustryRowsSpecificColumns:
    """make_waste_industry_rows_specific_columns — from 'industry disaggregation' rows in Make.
    Index = non-waste commodity codes; columns = waste industry subsectors.
    Each row sums to 1.
    """

    def test_commodity_contexts_present(self, weights_2017: WasteDisaggWeights) -> None:
        tbl = weights_2017.make_waste_industry_rows_specific_columns
        expected_contexts = {c for (_, c) in _RAW_MAKE_INDUSTRY_ROWS}
        for ctx in expected_contexts:
            assert ctx in tbl.index, f'context row {ctx} missing'

    def test_row_ratios_preserved(self, weights_2017: WasteDisaggWeights) -> None:
        # raw is (industry, commodity) but the table is (commodity → industry);
        # transpose raw to (commodity, industry)
        raw_transposed = {(c, r): v for (r, c), v in _RAW_MAKE_INDUSTRY_ROWS.items()}
        _check_row_ratios(
            weights_2017.make_waste_industry_rows_specific_columns,
            raw_transposed,
            'make_waste_industry_rows_specific_columns',
        )


class TestUseIntersection:
    """use_intersection — from 'Use table intersection' rows."""

    def test_all_pairs_present(self, weights_2017: WasteDisaggWeights) -> None:
        tbl = weights_2017.use_intersection
        for r, c in _RAW_USE_INTERSECTION:
            assert r in tbl.index, f'row {r} missing'
            assert c in tbl.columns, f'col {c} missing'

    def test_ratios_preserved(self, weights_2017: WasteDisaggWeights) -> None:
        _check_ratios(
            weights_2017.use_intersection,
            _RAW_USE_INTERSECTION,
            'use_intersection',
        )


class TestUseWasteIndustryColumnsAllRows:
    """use_waste_industry_columns_all_rows — from Use column-sum rows (IndustryCode=waste, CommodityCode=562000).
    Index = CommodityCode (562000); columns = waste industry subsectors; each row sums to 1.
    """

    def test_waste_industry_columns_present(
        self, weights_2017: WasteDisaggWeights
    ) -> None:
        tbl = weights_2017.use_waste_industry_columns_all_rows
        for ind in {r for (r, _) in _RAW_USE_COL_SUM}:
            assert ind in tbl.columns, f'industry col {ind} missing'

    def test_ratios_preserved(self, weights_2017: WasteDisaggWeights) -> None:
        # raw has (industry, commodity) — column sum slice: commodity=562000, industry=subsector
        # the table has index=commodity (562000), columns=industry subsectors
        raw_as_row = {('562000', ind): v for (ind, _), v in _RAW_USE_COL_SUM.items()}
        _check_row_ratios(
            weights_2017.use_waste_industry_columns_all_rows,
            raw_as_row,
            'use_waste_industry_columns_all_rows',
        )


class TestUseWasteCommodityRowsAllColumns:
    """use_waste_commodity_rows_all_columns — from Use row-sum rows (IndustryCode=562000, CommodityCode=waste).
    Index = IndustryCode (562000); columns = waste commodity subsectors; each row sums to 1.
    """

    def test_waste_commodity_columns_present(
        self, weights_2017: WasteDisaggWeights
    ) -> None:
        tbl = weights_2017.use_waste_commodity_rows_all_columns
        for com in {c for (_, c) in _RAW_USE_ROW_SUM}:
            assert com in tbl.columns, f'commodity col {com} missing'

    def test_ratios_preserved(self, weights_2017: WasteDisaggWeights) -> None:
        # raw has (IndustryCode=562000, CommodityCode=waste)
        raw_as_row = {('562000', com): v for (_, com), v in _RAW_USE_ROW_SUM.items()}
        _check_row_ratios(
            weights_2017.use_waste_commodity_rows_all_columns,
            raw_as_row,
            'use_waste_commodity_rows_all_columns',
        )


class TestUseVARowsForWasteIndustryColumns:
    """use_va_rows_for_waste_industry_columns — from VA disaggregation rows in Use CSV.
    Index = VA row code (V00100 / V00200 / V00300); columns = waste industry subsectors.
    Each row sums to 1.
    """

    def test_va_rows_present(self, weights_2017: WasteDisaggWeights) -> None:
        tbl = weights_2017.use_va_rows_for_waste_industry_columns
        for va_row in VA_ROWS:
            assert va_row in tbl.index, f'VA row {va_row} missing'

    def test_industry_columns_present(self, weights_2017: WasteDisaggWeights) -> None:
        tbl = weights_2017.use_va_rows_for_waste_industry_columns
        for ind in {r for (r, _) in _RAW_USE_VA}:
            assert ind in tbl.columns, f'industry col {ind} missing'

    def test_row_ratios_preserved(self, weights_2017: WasteDisaggWeights) -> None:
        # raw has (industry, VA_row); table has index=VA_row, columns=industry
        raw_transposed = {(va, ind): v for (ind, va), v in _RAW_USE_VA.items()}
        _check_row_ratios(
            weights_2017.use_va_rows_for_waste_industry_columns,
            raw_transposed,
            'use_va_rows_for_waste_industry_columns',
        )

    def test_all_three_va_rows_have_identical_ratios(
        self, weights_2017: WasteDisaggWeights
    ) -> None:
        """V00100, V00200, V00300 have identical raw values so their output rows must match."""
        tbl = weights_2017.use_va_rows_for_waste_industry_columns
        row_v1 = tbl.loc['V00100'].to_numpy()
        for va_row in ('V00200', 'V00300'):
            row_vn = tbl.loc[va_row].to_numpy()
            assert row_vn == pytest.approx(row_v1, rel=1e-6), (
                f'VA row {va_row} differs from V00100'
            )


class TestUseFDColumnsForWasteCommodityRows:
    """use_fd_columns_for_waste_commodity_rows — rows from Use CSV whose IndustryCode is a
    Cornerstone FD code (from final_demand.py).  BEA aggregate codes (S00101, S00201,
    GSLGE, 813100, etc.) are intentionally excluded.
    Index = Cornerstone FD codes; columns = waste commodity subsectors.
    Each row sums to 1.
    """

    def test_fd_context_rows_present(self, weights_2017: WasteDisaggWeights) -> None:
        tbl = weights_2017.use_fd_columns_for_waste_commodity_rows
        for fd_col in _RAW_USE_FD_DISAGG:
            assert fd_col in tbl.index, f'FD row {fd_col} missing'

    def test_non_cornerstone_fd_codes_excluded(
        self, weights_2017: WasteDisaggWeights
    ) -> None:
        """BEA aggregate codes must not appear — only Cornerstone FD codes are kept."""
        tbl = weights_2017.use_fd_columns_for_waste_commodity_rows
        bea_codes = (
            'S00101',
            'S00102',
            'S00500',
            'S00600',
            'S00201',
            'S00202',
            'S00203',
            'GSLGE',
            'GSLGH',
            'GSLGO',
            '813100',
            '813A00',
            '813B00',
        )
        for code in bea_codes:
            assert code not in tbl.index, f'BEA code {code} should not be in FD table'

    def test_commodity_columns_present(self, weights_2017: WasteDisaggWeights) -> None:
        tbl = weights_2017.use_fd_columns_for_waste_commodity_rows
        for col in WASTE_CODES:
            assert col in tbl.columns, f'commodity col {col} missing'

    def test_row_ratios_preserved_for_each_fd_context(
        self, weights_2017: WasteDisaggWeights
    ) -> None:
        tbl = weights_2017.use_fd_columns_for_waste_commodity_rows
        for fd_col, raw_row in _RAW_USE_FD_DISAGG.items():
            raw_as_row = {(fd_col, com): v for com, v in raw_row.items()}
            _check_row_ratios(tbl, raw_as_row, f'use_fd_columns[{fd_col}]')


# ---------------------------------------------------------------------------
# Pre-existing integration test (kept intact)
# ---------------------------------------------------------------------------


@pytest.mark.eeio_integration
def test_load_waste_disagg_weights_integration_with_2017_files() -> None:
    data_dir = pathlib.Path(__file__).resolve().parents[1]
    use_path = data_dir / 'WasteDisaggregationDetail2017_Use.csv'
    make_path = data_dir / 'WasteDisaggregationDetail2017_Make.csv'

    cfg = EEIOWasteDisaggConfig(
        use_weights_file=str(use_path),
        make_weights_file=str(make_path),
        year=2017,
        source_name='WasteDisaggregationDetail2017',
    )
    waste_codes = cast(list[str], list(WASTE_DISAGG_COMMODITIES['562000']))

    weights = load_waste_disagg_weights(
        cfg,
        disagg_original_code='562000',
        disagg_new_codes=waste_codes,
        waste_sectors=waste_codes,
        naics_to_cornerstone=None,
    )

    assert weights.year == 2017
    assert weights.source_name == 'WasteDisaggregationDetail2017'
    assert pytest.approx(float(weights.use_intersection.values.sum()), rel=1e-6) == 1.0
    assert pytest.approx(float(weights.make_intersection.values.sum()), rel=1e-6) == 1.0
    assert all(code in weights.use_intersection.index for code in waste_codes)
    assert all(code in weights.use_intersection.columns for code in waste_codes)
    assert all(code in weights.make_intersection.index for code in waste_codes)
    assert all(code in weights.make_intersection.columns for code in waste_codes)
    assert weights.make_intersection.loc['562111', '562111'] == pytest.approx(
        4.23e-01, rel=1e-3
    )
