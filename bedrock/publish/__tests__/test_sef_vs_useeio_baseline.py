"""eeio_integration: published Phi vs pinned phoebe USEEIO workbook."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import bedrock.utils.config.common as common
from bedrock.publish.__tests__._helpers import clear_all_caches, teardown
from bedrock.publish.model_objects import PUBLISH_LOCATION, get_Phi
from bedrock.utils.config.usa_config import reset_usa_config, set_global_usa_config
from bedrock.utils.validation.useeio_excel_baseline import (
    _local_cache_path,
    ensure_useeio_xlsx_local,
    load_useeio_baseline_pin_overrides,
)

_PIN_JSON = (
    Path(__file__).resolve().parents[2]
    / 'utils'
    / 'snapshots'
    / 'useeio_baseline_pin.json'
)

_PHI_RTOL = 0.01


def _setup_phoebe_with_useeio_pin() -> dict[str, str]:
    pin = load_useeio_baseline_pin_overrides(str(_PIN_JSON))
    overrides: dict[str, object] = {
        **pin,
        'diagnostics_baseline_source': 'gcs_useeio_xlsx',
    }
    clear_all_caches()
    reset_usa_config(should_reset_env_var=True)
    set_global_usa_config(
        'useeio_phoebe_23',
        diagnostics_cli_overrides=overrides,
    )
    common.download_fba_on_api_error = True
    return pin


def _strip_loc_suffix(values: pd.Index) -> pd.Index:
    suffix = f'/{PUBLISH_LOCATION}'
    return pd.Index(
        [str(v)[: -len(suffix)] if str(v).endswith(suffix) else str(v) for v in values],
        name=values.name,
    )


def _load_workbook_phi(xlsx_path: str, year: int) -> pd.Series:
    raw = pd.read_excel(xlsx_path, sheet_name='Phi', header=None, engine='openpyxl')
    headers = (
        raw.iloc[0, 1:].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
    )
    sectors = raw.iloc[1:, 0].astype(str).str.strip()
    values = raw.iloc[1:, 1:].copy()
    values.columns = pd.Index(headers)
    values.index = pd.Index(sectors)
    year_str = str(year)
    phi = values[year_str].astype(float)
    phi.index = pd.Index(
        [s[:-3] if s.endswith('/US') else s for s in phi.index], name='sector'
    )
    return phi.dropna()


@pytest.mark.eeio_integration
@pytest.mark.parametrize('year', [2017])
def test_published_phi_matches_useeio_workbook(year: int) -> None:
    try:
        pin = _setup_phoebe_with_useeio_pin()
        xlsx = _local_cache_path(pin['useeio_baseline_xlsx_gs_uri'])
        ensure_useeio_xlsx_local(
            pin['useeio_baseline_xlsx_gs_uri'],
            pin['useeio_baseline_xlsx_sha256'],
            xlsx,
        )
        bedrock_phi_df = get_Phi()
        assert bedrock_phi_df is not None
        year_str = str(year)
        assert (
            year_str in bedrock_phi_df.columns
        ), f'bedrock Phi panel missing column {year_str!r}'
        bedrock_phi = bedrock_phi_df[year_str].astype(float)
        bedrock_phi.index = _strip_loc_suffix(bedrock_phi.index)

        ref_phi = _load_workbook_phi(xlsx, year)
        common_sectors = bedrock_phi.index.intersection(ref_phi.index)
        assert len(common_sectors) > 100
        np.testing.assert_allclose(
            bedrock_phi.reindex(common_sectors).astype(float),
            ref_phi.reindex(common_sectors).astype(float),
            rtol=_PHI_RTOL,
            atol=1e-12,
        )
    finally:
        teardown()
