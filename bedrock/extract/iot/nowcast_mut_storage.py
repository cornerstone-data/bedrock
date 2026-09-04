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
there once and cached. When ``nowcast_mut_vintage`` is omitted, loaders resolve
the most recently uploaded Make parquet for the configured year and stage via
GCS, then load that vintage for all four stored tables.
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


def nowcast_mut_artifact_stem(
    table: MutTable,
    *,
    year: int,
    stage: Stage,
) -> str:
    """Filename stem without the vintage suffix (GCS most-recent probe key)."""
    if table not in STORED_MUT_TABLES:
        raise ValueError(f'unknown MUT table {table!r}; expected {STORED_MUT_TABLES}')
    if stage not in ('before', 'after'):
        raise ValueError(f"stage must be 'before' or 'after', got {stage!r}")
    return f'Nowcast_Detail_{table}_{stage}_redef_{year}'


def nowcast_mut_artifact_name(
    table: MutTable,
    *,
    year: int,
    stage: Stage,
    vintage: str,
) -> str:
    """The artifact's flat filename, identical locally and on GCS."""
    stem = nowcast_mut_artifact_stem(table, year=year, stage=stage)
    return f'{stem}_{vintage}.parquet'


@functools.cache
def latest_nowcast_mut_vintage(*, year: int, stage: Stage) -> str:
    """Most recently uploaded Make parquet vintage for ``year`` / ``stage`` on GCS.

    Uses :func:`bedrock.utils.io.gcp.get_most_recent_from_bucket` against the
    unversioned Make stem so package version + git hash sort by upload time.
    """
    from bedrock.utils.io.gcp import (  # noqa: PLC0415
        get_most_recent_from_bucket,
        parse_methodname,
    )

    probe = f'{nowcast_mut_artifact_stem("Make", year=year, stage=stage)}.parquet'
    candidates = get_most_recent_from_bucket(probe, GCS_NOWCAST_MUT_DIR)
    parquets = [c for c in candidates if c.endswith('.parquet')]
    if not parquets:
        raise ValueError(
            'no NowcastMUT Make parquet on GCS for '
            f'year={year}, stage={stage!r} under {GCS_NOWCAST_MUT_DIR!r}; '
            'set nowcast_mut_vintage explicitly or upload artifacts first'
        )
    _base, _ext, version, git_hash = parse_methodname(parquets[0])
    if version is None or git_hash is None:
        raise ValueError(
            f'could not parse version/hash from NowcastMUT filename {parquets[0]!r}'
        )
    return f'{version}_{git_hash}'


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
    """One stored table, local-first, downloaded from GCS when absent.

    Same pattern as FlowBy: ``get_most_recent_from_bucket`` returns the parquet
    plus sidecars (metadata, logs), and each candidate is written under its own
    filename in ``local_dir``.
    """
    import os  # noqa: PLC0415

    from bedrock.utils.io.gcp import (  # noqa: PLC0415
        download_gcs_file,
        get_most_recent_from_bucket,
    )

    directory = local_dir if local_dir is not None else str(FBS_DIR)
    name = nowcast_mut_artifact_name(table, year=year, stage=stage, vintage=vintage)
    parquet_path = os.path.join(directory, name)

    if not os.path.exists(parquet_path):
        candidates = get_most_recent_from_bucket(name, GCS_NOWCAST_MUT_DIR)
        if not candidates:
            raise FileNotFoundError(
                f'NowcastMUT artifact not found locally or on GCS: {name} '
                f'under {GCS_NOWCAST_MUT_DIR}'
            )
        os.makedirs(directory, exist_ok=True)
        for n in candidates:
            dest = os.path.join(directory, os.path.basename(n))
            if os.path.exists(dest):
                continue
            download_gcs_file(n, GCS_NOWCAST_MUT_DIR, dest)

    return pd.read_parquet(parquet_path)


def _configured() -> tuple[str, int, Stage]:
    cfg = get_usa_config()
    year = cfg.usa_base_io_data_year
    stage = cfg.iot_before_or_after_redefinition
    configured = (cfg.nowcast_mut_vintage or '').strip()
    vintage = configured or latest_nowcast_mut_vintage(year=year, stage=stage)
    return vintage, year, stage


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
