"""Tests that emission-factor publish applies Phi and dollar-year rebasing."""

from __future__ import annotations

import numpy as np
import pytest

from bedrock.publish.__tests__._helpers import setup_config, teardown
from bedrock.publish.emission_factors.table import (
    COL_WITHOUT,
    build_emission_factor_table,
    finalize_cornerstone_ef_table,
)
from bedrock.publish.model_objects import get_N
from bedrock.transform.iot.derive_PRO_to_PUR_ratio import phi_for_sectors
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.economic.inflation_helpers_cornerstone import (
    get_vnorm_adjusted_commodity_price_ratio,
)
from bedrock.utils.emissions.characterization import GREENHOUSE_GASES_INDICATOR


@pytest.mark.eeio_integration
def test_sef_applies_phi_and_dollar_year() -> None:
    """without_margins / (N_producer / cpi) equals phi per sector."""
    setup_config('useeio_phoebe_23')
    try:
        dollar_year = 2024
        cfg = get_usa_config()
        table = finalize_cornerstone_ef_table(
            build_emission_factor_table(
                dollar_year=dollar_year,
                purchaser_price=True,
            )
        )
        n_producer = get_N().loc[GREENHOUSE_GASES_INDICATOR].astype(float)
        pi = get_vnorm_adjusted_commodity_price_ratio(cfg.model_base_year, dollar_year)
        n_producer_cpi = n_producer / pi.reindex(n_producer.index, fill_value=1.0)
        phi = phi_for_sectors(n_producer.index)

        for _, row in table.iterrows():
            code = str(row['Cornerstone Commodity Code']).removesuffix('/US')
            without = float(row[COL_WITHOUT])
            if code not in n_producer_cpi.index:
                continue
            denom = float(n_producer_cpi[code])
            if denom == 0.0:
                continue
            np.testing.assert_allclose(
                without / denom,
                float(phi[code]),
                rtol=1e-9,
                err_msg=f'sector {code}',
            )
    finally:
        teardown()
