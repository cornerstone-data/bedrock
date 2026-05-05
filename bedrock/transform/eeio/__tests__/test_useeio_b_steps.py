from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pandas as pd
import pytest

from bedrock.transform.eeio import derived_cornerstone as dc


def test_useeio_b_adjust_divide_transform_steps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    e = pd.DataFrame(
        [
            [10.0, 20.0],  # CO2
            [0.0, 0.0],  # CH4
            [0.0, 0.0],  # N2O
            [0.0, 0.0],  # HFCs
            [0.0, 0.0],  # PFCs
            [0.0, 0.0],  # SF6
            [0.0, 0.0],  # NF3
        ],
        index=["CO2", "CH4", "N2O", "HFCs", "PFCs", "SF6", "NF3"],
        columns=["I1", "I2"],
    )
    x_nominal = pd.Series([100.0, 200.0], index=["I1", "I2"])
    # Price ratio orientation is PI_target / PI_original.
    # Here: I1=0.8 (deflationary), I2=1.25 (inflationary), so adjusted x is
    # x_nominal / ratio = [125, 160].
    ratio = pd.Series([0.8, 1.25], index=["I1", "I2"])
    # Identity market-share transform for easy expected value check.
    vnorm = pd.DataFrame(
        [[1.0, 0.0], [0.0, 1.0]], index=["I1", "I2"], columns=["I1", "I2"]
    )

    monkeypatch.setattr(
        dc,
        "get_usa_config",
        lambda: SimpleNamespace(
            use_useeio_B=True,
            use_E_data_year_for_x_in_B=False,
            usa_ghg_data_year=2023,
            usa_detail_original_year=2017,
        ),
    )
    monkeypatch.setattr(dc, "derive_E_usa", lambda: e)
    monkeypatch.setattr(
        dc, "derive_cornerstone_x_after_redefinition", lambda: x_nominal
    )
    monkeypatch.setattr(
        dc,
        "get_cornerstone_industry_price_ratio",
        lambda original_year, target_year: ratio,
    )
    monkeypatch.setattr(dc, "derive_cornerstone_Vnorm_scrap_corrected", lambda: vnorm)

    out = cast(Any, dc.derive_cornerstone_B_via_vnorm).__wrapped__()

    # Step 1: adjusted x = x_nominal / ratio = [125, 160]
    # Step 2: Bi = E / adjusted x = [0.08, 0.125]
    # Step 3: B = Bi @ I = Bi
    expected = pd.DataFrame(
        [
            [0.08, 0.125],  # CO2
            [0.0, 0.0],  # CH4
            [0.0, 0.0],  # N2O
            [0.0, 0.0],  # HFCs
            [0.0, 0.0],  # PFCs
            [0.0, 0.0],  # SF6
            [0.0, 0.0],  # NF3
        ],
        index=["CO2", "CH4", "N2O", "HFCs", "PFCs", "SF6", "NF3"],
        columns=["I1", "I2"],
    )
    pd.testing.assert_frame_equal(out, expected)
