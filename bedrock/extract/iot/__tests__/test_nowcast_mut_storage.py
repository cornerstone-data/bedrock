"""The storage layer's naming, views and local-first round trip.

The heavy end-to-end path (balanced SUT in, quartet out) is exercised by the
driver's own identity gates on every build; here only the pure pieces are
under test, on synthetic frames carrying the real taxonomy axes.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from bedrock.extract.iot.nowcast_mut_storage import (
    MARGINS_VALUE_COLUMNS,
    STORED_MUT_TABLES,
    _as_make_view,
    _as_margins_view,
    _as_uimp_view,
    _as_utot_view,
    _as_value_added_view,
    _as_ytot_view,
    _load_stored_table,
    latest_nowcast_mut_vintage,
    nowcast_mut_artifact_name,
    resolve_nowcast_mut_uri,
)
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_final_demand import (
    USA_2017_FINAL_DEMAND_CODES,
)
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES
from bedrock.utils.taxonomy.bea.v2017_value_added import USA_2017_VALUE_ADDED_CODES


def test_artifact_names_carry_every_key() -> None:
    name = nowcast_mut_artifact_name(
        'Make', year=2023, stage='before', vintage='v0.3.0_abc1234'
    )
    assert name == 'Nowcast_Detail_Make_before_redef_2023_v0.3.0_abc1234.parquet'
    assert len(
        {
            nowcast_mut_artifact_name(t, year=2023, stage='before', vintage='v1')
            for t in STORED_MUT_TABLES
        }
    ) == len(STORED_MUT_TABLES)


def test_artifact_name_rejects_unknown_table_and_stage() -> None:
    with pytest.raises(ValueError, match='unknown MUT table'):
        nowcast_mut_artifact_name(
            'Ytot',  # type: ignore[arg-type]  # a view, not a stored table
            year=2023,
            stage='before',
            vintage='v1',
        )
    with pytest.raises(ValueError, match='stage'):
        nowcast_mut_artifact_name(
            'Make',
            year=2023,
            stage='published',  # type: ignore[arg-type]
            vintage='v1',
        )


def test_uri_is_flat_under_the_nowcast_mut_dir() -> None:
    uri = resolve_nowcast_mut_uri(
        vintage='v0.3.0_abc1234', year=2022, stage='after', table='Margins'
    )
    assert uri.startswith('gs://')
    assert uri.endswith(
        'flowsa/NowcastMUT/'
        'Nowcast_Detail_Margins_after_redef_2022_v0.3.0_abc1234.parquet'
    )


def test_latest_vintage_parses_most_recent_make_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    latest_nowcast_mut_vintage.cache_clear()

    def fake_most_recent(name: str, sub_bucket: str) -> list[str]:
        assert name == 'Nowcast_Detail_Make_after_redef_2022.parquet'
        assert sub_bucket == 'flowsa/NowcastMUT'
        return [
            'Nowcast_Detail_Make_after_redef_2022_v0.3.0_16f96b1.parquet',
            'Nowcast_Detail_Make_after_redef_2022_v0.3.0_16f96b1_metadata.json',
        ]

    monkeypatch.setattr(
        'bedrock.utils.io.gcp.get_most_recent_from_bucket',
        fake_most_recent,
    )
    assert latest_nowcast_mut_vintage(year=2022, stage='after') == 'v0.3.0_16f96b1'


def test_latest_vintage_errors_when_bucket_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    latest_nowcast_mut_vintage.cache_clear()
    monkeypatch.setattr(
        'bedrock.utils.io.gcp.get_most_recent_from_bucket',
        lambda name, sub_bucket: [],
    )
    with pytest.raises(ValueError, match='no NowcastMUT Make parquet'):
        latest_nowcast_mut_vintage(year=2017, stage='after')


def _synthetic_use() -> pd.DataFrame:
    rows = list(USA_2017_COMMODITY_CODES) + list(USA_2017_VALUE_ADDED_CODES)
    columns = list(USA_2017_INDUSTRY_CODES) + list(USA_2017_FINAL_DEMAND_CODES)
    rng = np.random.default_rng(7)
    return pd.DataFrame(
        rng.normal(size=(len(rows), len(columns))), index=rows, columns=columns
    )


def test_use_views_slice_the_three_blocks() -> None:
    use = _synthetic_use()
    utot = _as_utot_view(use)
    ytot = _as_ytot_view(use)
    value_added = _as_value_added_view(use)

    assert utot.shape == (len(USA_2017_COMMODITY_CODES), len(USA_2017_INDUSTRY_CODES))
    assert list(ytot.columns) == list(USA_2017_FINAL_DEMAND_CODES)
    assert list(value_added.index) == list(USA_2017_VALUE_ADDED_CODES)
    first_commodity = USA_2017_COMMODITY_CODES[0]
    first_industry = USA_2017_INDUSTRY_CODES[0]
    assert utot.iloc[0, 0] == use.loc[first_commodity, first_industry]
    # The three views tile the stored table without overlap or invention.
    assert utot.index.name == 'commodity'
    assert ytot.index.name == 'commodity'
    assert value_added.columns.name == 'industry'


def test_make_and_import_views_take_taxonomy_axes() -> None:
    make = pd.DataFrame(
        1.0,
        index=list(USA_2017_INDUSTRY_CODES),
        columns=list(USA_2017_COMMODITY_CODES),
    )
    imports = pd.DataFrame(
        2.0,
        index=list(USA_2017_COMMODITY_CODES),
        columns=list(USA_2017_INDUSTRY_CODES) + list(USA_2017_FINAL_DEMAND_CODES),
    )
    v = _as_make_view(make)
    uimp = _as_uimp_view(imports)
    assert v.index.name == 'industry' and v.columns.name == 'commodity'
    assert uimp.shape == (
        len(USA_2017_COMMODITY_CODES),
        len(USA_2017_INDUSTRY_CODES),
    )
    assert 'F05000' not in uimp.columns


def test_margins_view_is_the_published_five_columns() -> None:
    index = pd.MultiIndex.from_tuples(
        [('1111A0', '111200'), ('F01000', '111200')],
        names=['Industry Code', 'Commodity Code'],
    )
    stored = pd.DataFrame(
        1.0, index=index, columns=[*MARGINS_VALUE_COLUMNS, '420000', '4A0000']
    )
    view = _as_margins_view(stored)
    assert list(view.columns) == MARGINS_VALUE_COLUMNS

    with pytest.raises(ValueError, match='missing'):
        _as_margins_view(stored[MARGINS_VALUE_COLUMNS[:-1] + ['420000']])


def test_stored_table_round_trips_local_first(tmp_path: Path) -> None:
    frame = _synthetic_use()
    name = nowcast_mut_artifact_name('Use', year=2021, stage='before', vintage='v-test')
    frame.to_parquet(tmp_path / name)

    loaded = _load_stored_table(
        'Use', vintage='v-test', year=2021, stage='before', local_dir=str(tmp_path)
    )
    pd.testing.assert_frame_equal(loaded, frame)
