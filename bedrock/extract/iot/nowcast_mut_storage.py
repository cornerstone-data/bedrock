"""Nowcast BEA-detail MUT artifact storage (GCS layout TBD).

Published artifacts are the Make, Use, Import, and Margins tables in BEA 2017
Detail space before and after redefinition. The detail IO router loads those
artifacts and applies existing Cornerstone correspondence at read time.

Physical GCS URIs are not finalized; ``resolve_nowcast_mut_uri`` and the
``load_nowcast_detail_*`` entry points raise ``NotImplementedError`` until
wired.
"""

from __future__ import annotations

import typing as ta

import pandas as pd

from bedrock.utils.config.usa_config import get_usa_config


def resolve_nowcast_mut_uri(
    *,
    vintage: str,
    year: int,
    stage: ta.Literal['before', 'after'],
    table: ta.Literal['Make', 'Use', 'Import', 'Margins', 'Ytot', 'ValueAdded'],
) -> str:
    """Return the GCS URI for a nowcast MUT artifact.

    Logical layout: ``{vintage}/{year}/{stage}/{table}`` — physical bucket and
    prefix are not finalized.
    """
    raise NotImplementedError(
        'GCS layout for nowcast MUT artifacts is not finalized; '
        f'vintage={vintage!r}, year={year}, stage={stage!r}, table={table!r}'
    )


def _resolve_configured_nowcast_uri(
    table: ta.Literal['Make', 'Use', 'Import', 'Margins', 'Ytot', 'ValueAdded'],
) -> str:
    cfg = get_usa_config()
    return resolve_nowcast_mut_uri(
        vintage=cfg.nowcast_mut_vintage or '',
        year=cfg.usa_base_io_data_year,
        stage=cfg.iot_before_or_after_redefinition,
        table=table,
    )


def load_nowcast_detail_V_usa() -> pd.DataFrame:
    _resolve_configured_nowcast_uri('Make')
    raise AssertionError('resolve_nowcast_mut_uri must raise NotImplementedError')


def load_nowcast_detail_Utot_usa() -> pd.DataFrame:
    _resolve_configured_nowcast_uri('Use')
    raise AssertionError('resolve_nowcast_mut_uri must raise NotImplementedError')


def load_nowcast_detail_Uimp_usa() -> pd.DataFrame:
    _resolve_configured_nowcast_uri('Import')
    raise AssertionError('resolve_nowcast_mut_uri must raise NotImplementedError')


def load_nowcast_detail_margins_usa() -> pd.DataFrame:
    _resolve_configured_nowcast_uri('Margins')
    raise AssertionError('resolve_nowcast_mut_uri must raise NotImplementedError')


def load_nowcast_detail_Ytot_usa() -> pd.DataFrame:
    _resolve_configured_nowcast_uri('Ytot')
    raise AssertionError('resolve_nowcast_mut_uri must raise NotImplementedError')


def load_nowcast_detail_value_added_usa() -> pd.DataFrame:
    _resolve_configured_nowcast_uri('ValueAdded')
    raise AssertionError('resolve_nowcast_mut_uri must raise NotImplementedError')
