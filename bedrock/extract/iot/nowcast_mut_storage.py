"""Nowcast BEA-detail MUT artifact storage.

Four physical tables per (vintage, year, stage): ``Make``, ``Use``, ``Import``
and ``Margins`` - the Step 6 quartet, written by
:mod:`bedrock.transform.iot.nowcast_mut` and uploaded flat under
:data:`GCS_NOWCAST_MUT_DIR` with every key in the filename, the same
one-artifact-one-file convention as the rest of ``transform/output_data``.

The detail IO router (:mod:`bedrock.extract.iot.detail_io`) needs six loaders,
not four: ``Ytot`` and ``ValueAdded`` are *views* of the stored Use table
(its final-demand and value-added blocks), so they slice at read time rather
than being stored twice. Likewise the published five-column Margins layout is
a view of the stored hyper-detailed table, which carries one column per margin
commodity (the per-buyer seller placement) alongside the five published
columns.

Stored axes and units (all USD, matching the ``load_2017_*`` loaders the
router swaps these with):

* ``Make`` - industry x commodity, producer price.
* ``Use`` - (402 commodities + V00100/V00200/V00300) x (402 industries + 20
  final-demand codes, ``F05000`` included), producer price.
* ``Import`` - commodity x (industries + final demand), ``F05000`` negative.
* ``Margins`` - (Industry Code, Commodity Code) x (the five published value
  columns + one column per margin commodity). Margin-commodity rows follow
  the published convention: their ``Producers' Value`` carries the margin
  routed onto them, their ``Purchasers' Value`` only the direct purchase.

Loading is local-first: a table just built by the Step 6 driver is read from
``transform/output_data`` without touching GCS; otherwise it is downloaded
there once and cached.
"""

from __future__ import annotations

import functools
import posixpath
import typing as ta

import pandas as pd

from bedrock.utils.config.settings import FBS_DIR, GIT_HASH, PKG_VERSION_NUMBER
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_final_demand import (
    USA_2017_FINAL_DEMAND_CODES,
)
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES
from bedrock.utils.taxonomy.bea.v2017_value_added import USA_2017_VALUE_ADDED_CODES
from bedrock.utils.taxonomy.usa_taxonomy_correspondence_helpers import (
    USA_2017_COMMODITY_INDEX,
    USA_2017_FINAL_DEMAND_INDEX,
    USA_2017_INDUSTRY_INDEX,
    USA_2017_VALUE_ADDED_INDEX,
)

#: Where the nowcast MUT artifacts live on GCS, under ``GCS_CORNERSTONE``.
GCS_NOWCAST_MUT_DIR = 'flowsa/NowcastMUT'

#: The four tables Step 6 stores per (vintage, year, stage).
MutTable = ta.Literal['Make', 'Use', 'Import', 'Margins']
STORED_MUT_TABLES: tuple[MutTable, ...] = ('Make', 'Use', 'Import', 'Margins')

Stage = ta.Literal['before', 'after']

#: The published Margins layout, the first five columns of the stored table.
MARGINS_VALUE_COLUMNS = [
    "Producers' Value",
    'Transportation',
    'Wholesale',
    'Retail',
    "Purchasers' Value",
]


def default_nowcast_mut_vintage() -> str:
    """The vintage label a build stamped from this working tree would carry.

    ``v<package version>_<short git hash>`` - the same suffix every other
    ``transform/output_data`` artifact wears, so the sidecar, the filename and
    the ``nowcast_mut_vintage`` config value are one string.
    """
    if GIT_HASH is None:
        return f'v{PKG_VERSION_NUMBER}'
    return f'v{PKG_VERSION_NUMBER}_{GIT_HASH}'


def nowcast_mut_artifact_name(
    table: MutTable,
    *,
    year: int,
    stage: Stage,
    vintage: str,
) -> str:
    """The artifact's flat filename, identical locally and on GCS."""
    if table not in STORED_MUT_TABLES:
        raise ValueError(f'unknown MUT table {table!r}; expected {STORED_MUT_TABLES}')
    if stage not in ('before', 'after'):
        raise ValueError(f"stage must be 'before' or 'after', got {stage!r}")
    return f'Nowcast_Detail_{table}_{stage}_redef_{year}_{vintage}.parquet'


def resolve_nowcast_mut_uri(
    *,
    vintage: str,
    year: int,
    stage: Stage,
    table: MutTable,
) -> str:
    """Return the GCS URI for a nowcast MUT artifact.

    Logical layout ``{vintage}/{year}/{stage}/{table}``; physically the four
    keys live in one flat filename under :data:`GCS_NOWCAST_MUT_DIR`.
    """
    # Deferred import: resolving a name should not require GCS credentials.
    from bedrock.utils.io.gcp import GCS_CORNERSTONE  # noqa: PLC0415

    name = nowcast_mut_artifact_name(table, year=year, stage=stage, vintage=vintage)
    return posixpath.join(GCS_CORNERSTONE, GCS_NOWCAST_MUT_DIR, name)


@functools.cache
def _load_stored_table(
    table: MutTable,
    *,
    vintage: str,
    year: int,
    stage: Stage,
    local_dir: str | None = None,
) -> pd.DataFrame:
    """One stored table, local-first, downloaded from GCS when absent."""
    from bedrock.utils.io.gcp import load_from_gcs  # noqa: PLC0415

    name = nowcast_mut_artifact_name(table, year=year, stage=stage, vintage=vintage)
    return load_from_gcs(
        name=name,
        sub_bucket=GCS_NOWCAST_MUT_DIR,
        local_dir=local_dir if local_dir is not None else str(FBS_DIR),
        loader=pd.read_parquet,
    )


def _configured() -> tuple[str, int, Stage]:
    cfg = get_usa_config()
    if not cfg.nowcast_mut_vintage or not cfg.nowcast_mut_vintage.strip():
        raise ValueError(
            'nowcast_mut_vintage is not set; the nowcast detail loaders need '
            'it to pick an artifact build (e.g. '
            f'{default_nowcast_mut_vintage()!r})'
        )
    return (
        cfg.nowcast_mut_vintage,
        cfg.usa_base_io_data_year,
        cfg.iot_before_or_after_redefinition,
    )


def _stored(table: MutTable) -> pd.DataFrame:
    vintage, year, stage = _configured()
    return _load_stored_table(table, vintage=vintage, year=year, stage=stage)


# --- the six router views over the four stored tables ----------------------


def _as_make_view(make: pd.DataFrame) -> pd.DataFrame:
    """Make, industry x commodity, USD, on the taxonomy's named indexes."""
    df = make.reindex(
        index=list(USA_2017_INDUSTRY_CODES), columns=list(USA_2017_COMMODITY_CODES)
    ).astype(float)
    df.index = USA_2017_INDUSTRY_INDEX.copy()
    df.columns = USA_2017_COMMODITY_INDEX.copy()
    return df


def _as_utot_view(use: pd.DataFrame) -> pd.DataFrame:
    """Use interior, commodity x industry, USD."""
    df = use.loc[list(USA_2017_COMMODITY_CODES), list(USA_2017_INDUSTRY_CODES)].astype(
        float
    )
    df.index = USA_2017_COMMODITY_INDEX.copy()
    df.columns = USA_2017_INDUSTRY_INDEX.copy()
    return df


def _as_ytot_view(use: pd.DataFrame) -> pd.DataFrame:
    """Final demand, commodity x final-demand category, ``F05000`` negative."""
    df = use.loc[
        list(USA_2017_COMMODITY_CODES), list(USA_2017_FINAL_DEMAND_CODES)
    ].astype(float)
    df.index = USA_2017_COMMODITY_INDEX.copy()
    df.columns = USA_2017_FINAL_DEMAND_INDEX.copy()
    return df


def _as_value_added_view(use: pd.DataFrame) -> pd.DataFrame:
    """Value added, VA category x industry (three MUT rows)."""
    df = use.loc[
        list(USA_2017_VALUE_ADDED_CODES), list(USA_2017_INDUSTRY_CODES)
    ].astype(float)
    df.index = USA_2017_VALUE_ADDED_INDEX.copy()
    df.columns = USA_2017_INDUSTRY_INDEX.copy()
    return df


def _as_uimp_view(imports: pd.DataFrame) -> pd.DataFrame:
    """Import matrix, commodity x industry (final-demand block dropped)."""
    df = imports.loc[
        list(USA_2017_COMMODITY_CODES), list(USA_2017_INDUSTRY_CODES)
    ].astype(float)
    df.index = USA_2017_COMMODITY_INDEX.copy()
    df.columns = USA_2017_INDUSTRY_INDEX.copy()
    return df


def _as_margins_view(margins: pd.DataFrame) -> pd.DataFrame:
    """The published five-column Margins layout, sliced from the stored table.

    The stored per-margin-commodity columns stay behind; the five published
    columns are stored alongside them precisely so this view is a slice, not
    a recomputation needing the margin-family lists from the transform layer.
    """
    missing = [c for c in MARGINS_VALUE_COLUMNS if c not in margins.columns]
    if missing:
        raise ValueError(f'stored Margins table is missing {missing}')
    return margins[MARGINS_VALUE_COLUMNS].astype(float)


def load_nowcast_detail_V_usa() -> pd.DataFrame:
    return _as_make_view(_stored('Make'))


def load_nowcast_detail_Utot_usa() -> pd.DataFrame:
    return _as_utot_view(_stored('Use'))


def load_nowcast_detail_Uimp_usa() -> pd.DataFrame:
    return _as_uimp_view(_stored('Import'))


def load_nowcast_detail_margins_usa() -> pd.DataFrame:
    return _as_margins_view(_stored('Margins'))


def load_nowcast_detail_Ytot_usa() -> pd.DataFrame:
    return _as_ytot_view(_stored('Use'))


def load_nowcast_detail_value_added_usa() -> pd.DataFrame:
    return _as_value_added_view(_stored('Use'))
