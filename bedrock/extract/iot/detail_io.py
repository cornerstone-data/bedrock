"""Router for BEA 2017 Detail IO tables used by the Cornerstone pipeline.

``usa_detail_io_source`` selects published BEA tables (``bea_published``) or
nowcast replay artifacts (``nowcast``). Cornerstone correspondence and
expansion run unchanged downstream; only the loader inputs differ.
"""

from __future__ import annotations

import pandas as pd

from bedrock.extract.iot.io_2017 import (
    load_2017_margins_usa,
    load_2017_Uimp_usa,
    load_2017_Utot_usa,
    load_2017_V_usa,
    load_2017_value_added_usa,
    load_2017_Ytot_usa,
)
from bedrock.extract.iot.nowcast_mut_storage import (
    load_nowcast_detail_margins_usa,
    load_nowcast_detail_Uimp_usa,
    load_nowcast_detail_Utot_usa,
    load_nowcast_detail_V_usa,
    load_nowcast_detail_value_added_usa,
    load_nowcast_detail_Ytot_usa,
)
from bedrock.utils.config.usa_config import get_usa_config


def _detail_io_source() -> str:
    return get_usa_config().usa_detail_io_source


def load_detail_V_usa() -> pd.DataFrame:
    if _detail_io_source() == 'bea_published':
        return load_2017_V_usa()
    return load_nowcast_detail_V_usa()


def load_detail_Utot_usa() -> pd.DataFrame:
    if _detail_io_source() == 'bea_published':
        return load_2017_Utot_usa()
    return load_nowcast_detail_Utot_usa()


def load_detail_Uimp_usa() -> pd.DataFrame:
    if _detail_io_source() == 'bea_published':
        return load_2017_Uimp_usa()
    return load_nowcast_detail_Uimp_usa()


def load_detail_margins_usa() -> pd.DataFrame:
    if _detail_io_source() == 'bea_published':
        return load_2017_margins_usa()
    return load_nowcast_detail_margins_usa()


def load_detail_Ytot_usa() -> pd.DataFrame:
    if _detail_io_source() == 'bea_published':
        return load_2017_Ytot_usa()
    return load_nowcast_detail_Ytot_usa()


def load_detail_value_added_usa() -> pd.DataFrame:
    if _detail_io_source() == 'bea_published':
        return load_2017_value_added_usa()
    return load_nowcast_detail_value_added_usa()
