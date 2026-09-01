# ruff: noqa: PLC0415

from collections.abc import Callable

import pandas as pd
import pytest

from bedrock.extract.iot.detail_io import (
    load_detail_margins_usa,
    load_detail_Uimp_usa,
    load_detail_Utot_usa,
    load_detail_V_usa,
    load_detail_value_added_usa,
    load_detail_Ytot_usa,
)
from bedrock.extract.iot.io_2017 import (
    load_2017_margins_usa,
    load_2017_Uimp_usa,
    load_2017_Utot_usa,
    load_2017_V_usa,
    load_2017_value_added_usa,
    load_2017_Ytot_usa,
)
from bedrock.utils.config.usa_config import reset_usa_config, set_global_usa_config


@pytest.fixture(autouse=True)
def _reset_config() -> None:
    reset_usa_config(should_reset_env_var=True)


@pytest.mark.parametrize(
    ('detail_loader', 'published_loader'),
    [
        (load_detail_V_usa, load_2017_V_usa),
        (load_detail_Utot_usa, load_2017_Utot_usa),
        (load_detail_Uimp_usa, load_2017_Uimp_usa),
        (load_detail_margins_usa, load_2017_margins_usa),
        (load_detail_Ytot_usa, load_2017_Ytot_usa),
        (load_detail_value_added_usa, load_2017_value_added_usa),
    ],
)
def test_bea_published_detail_loaders_match_2017(
    detail_loader: Callable[[], pd.DataFrame],
    published_loader: Callable[[], pd.DataFrame],
) -> None:
    set_global_usa_config('test_usa_config.yaml')
    pd.testing.assert_frame_equal(detail_loader(), published_loader())
