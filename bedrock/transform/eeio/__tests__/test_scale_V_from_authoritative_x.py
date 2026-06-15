from __future__ import annotations

import numpy as np

from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_x_after_redefinition,
    scale_cornerstone_V_with_authoritative_x,
)
from bedrock.utils.config.usa_config import get_usa_config


def test_row_sums_equal_authoritative_x() -> None:
    """V_new row sums must match the authoritative model-year x for all non-zero industries."""
    scale_cornerstone_V_with_authoritative_x.cache_clear()
    derive_cornerstone_x_after_redefinition.cache_clear()

    cfg = get_usa_config()
    V_new = scale_cornerstone_V_with_authoritative_x()
    x_new = derive_cornerstone_x_after_redefinition(year=cfg.model_base_year)

    row_sums = V_new.sum(axis=1)
    x_aligned = x_new.reindex(row_sums.index).fillna(0.0)

    mask = x_aligned.values != 0
    np.testing.assert_allclose(
        row_sums.values[mask],
        x_aligned.values[mask],
        rtol=1e-6,
        err_msg="V_new row sums do not match authoritative x_new",
    )
