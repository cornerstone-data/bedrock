"""Unit tests for year-keyed electricity-disagg eGRID FBS selection."""

from __future__ import annotations

from collections.abc import Generator
from unittest.mock import MagicMock

import pandas as pd
import pytest

from bedrock.transform.allocation import derived as allocation_derived
from bedrock.transform.allocation.derived import egrid_fbs_method_for_year
from bedrock.utils.config.usa_config import USAConfig, reset_usa_config


@pytest.fixture(autouse=True)
def _reset_usa_config() -> Generator[None, None, None]:
    reset_usa_config(should_reset_env_var=True)
    yield
    reset_usa_config(should_reset_env_var=True)


@pytest.mark.parametrize(
    ('year', 'expected'),
    [
        (2023, 'GHG_national_Cornerstone_2023_egrid'),
        (2024, 'GHG_national_Cornerstone_2024_egrid'),
    ],
)
def test_egrid_fbs_method_for_year(year: int, expected: str) -> None:
    assert egrid_fbs_method_for_year(year) == expected


def test_egrid_fbs_method_for_year_unsupported() -> None:
    with pytest.raises(ValueError, match='unsupported'):
        egrid_fbs_method_for_year(2022)


@pytest.mark.parametrize(
    ('year', 'expected_method'),
    [
        (2023, 'GHG_national_Cornerstone_2023_egrid'),
        (2024, 'GHG_national_Cornerstone_2024_egrid'),
    ],
)
def test_load_egrid_fbs_selects_method_by_ghg_year(
    year: int,
    expected_method: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = USAConfig(
        usa_ghg_data_year=year,  # type: ignore[arg-type]
        use_cornerstone_ghg_model=True,
        implement_waste_disaggregation=True,
        implement_electricity_reallocation=True,
        implement_electricity_disaggregation=True,
    )
    monkeypatch.setattr(allocation_derived, 'get_usa_config', lambda: cfg)

    sentinel = pd.DataFrame({'ok': [1]})
    gcs_calls: list[str] = []

    def fake_gcs(
        *, base_name: str | None = None, year: int | None = None
    ) -> pd.DataFrame:
        assert base_name is not None
        gcs_calls.append(base_name)
        return sentinel

    monkeypatch.setattr(
        allocation_derived,
        '_load_cornerstone_ghg_fbs_from_gcs',
        fake_gcs,
    )
    flowsa = MagicMock()
    monkeypatch.setattr(allocation_derived, 'getFlowBySector', flowsa)

    out = allocation_derived._load_egrid_fbs_for_electricity_disagg()
    assert out is sentinel
    assert gcs_calls == [expected_method]
    flowsa.assert_not_called()


def test_load_egrid_fbs_falls_back_to_flowsa(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = USAConfig(
        usa_ghg_data_year=2024,
        use_cornerstone_ghg_model=True,
        implement_waste_disaggregation=True,
        implement_electricity_reallocation=True,
        implement_electricity_disaggregation=True,
    )
    monkeypatch.setattr(allocation_derived, 'get_usa_config', lambda: cfg)

    def raise_missing(**_kwargs: object) -> pd.DataFrame:
        raise FileNotFoundError('missing')

    sentinel = pd.DataFrame({'ok': [1]})
    flowsa = MagicMock(return_value=sentinel)
    monkeypatch.setattr(
        allocation_derived,
        '_load_cornerstone_ghg_fbs_from_gcs',
        raise_missing,
    )
    monkeypatch.setattr(allocation_derived, 'getFlowBySector', flowsa)

    out = allocation_derived._load_egrid_fbs_for_electricity_disagg()
    assert out is sentinel
    flowsa.assert_called_once_with(
        methodname='GHG_national_Cornerstone_2024_egrid',
        download_FBS_if_missing=True,
    )
