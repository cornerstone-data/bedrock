from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from bedrock.transform.eeio import derived_cornerstone as dc


def test_useeio_b_adjust_divide_transform_steps(monkeypatch) -> None:
    e = pd.DataFrame([[10.0, 20.0]], index=["CO2"], columns=["I1", "I2"])
    x_nominal = pd.Series([100.0, 200.0], index=["I1", "I2"])
    # Convert target-year nominal output to 2017 USD.
    ratio = pd.Series([0.5, 0.5], index=["I1", "I2"])
    # Identity market-share transform for easy expected value check.
    vnorm = pd.DataFrame([[1.0, 0.0], [0.0, 1.0]], index=["I1", "I2"], columns=["I1", "I2"])

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
    monkeypatch.setattr(dc, "derive_cornerstone_x_after_redefinition", lambda: x_nominal)
    monkeypatch.setattr(
        dc,
        "get_cornerstone_industry_price_ratio",
        lambda original_year, target_year: ratio,
    )
    monkeypatch.setattr(dc, "derive_cornerstone_Vnorm_scrap_corrected", lambda: vnorm)

    out = dc.derive_cornerstone_B_via_vnorm.__wrapped__()

    # Step 1: adjusted x = [50, 100]
    # Step 2: Bi = E / adjusted x = [0.2, 0.2]
    # Step 3: B = Bi @ I = Bi
    expected = pd.DataFrame([[0.2, 0.2]], index=["CO2"], columns=["I1", "I2"])
    pd.testing.assert_frame_equal(out, expected)
