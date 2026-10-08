"""The imports summary conditioning (#785).

The invariants: conditioned groups aggregate to the published summary MCIF
wherever a group is reachable, zeros stay zero (the never-imported structure
survives), the factors are nonnegative, and every consumer of the imports
vector reads the same conditioned copy — MDTY's goods base above all, which
used to derive its own from Census.
"""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.analysis.nowcasting.trade_data.family_resplit import benchmark_mcif
from bedrock.extract.iot.io_2017 import _load_usa_summary_sut
from bedrock.transform.iot.nowcast_import_conditioning import (
    ANCHORED_FAMILIES,
    EMPTY_GROUP_USD,
    _commodity_to_summary,
    anchored_mcif,
    conditioned_mcif,
    mcif_condition_factors,
    raw_import_vector,
)
from bedrock.transform.iot.nowcast_mask import NEVER_IMPORTED_COMMODITIES
from bedrock.transform.trade.duties import mdty_detail_usd

YEAR = 2017


def _published_mcif(year: int) -> pd.Series:
    supply = _load_usa_summary_sut('Supply_summary', year)  # type: ignore[arg-type]
    return pd.to_numeric(pd.Series(supply['MCIF']), errors='coerce').fillna(0.0) * 1e6


def test_reachable_groups_land_on_the_published_allocation() -> None:
    out = conditioned_mcif(YEAR)
    groups = _commodity_to_summary().reindex(out.index)
    published = _published_mcif(YEAR)
    ours = out.groupby(groups).sum()
    raw = raw_import_vector(YEAR).groupby(groups).sum()
    reachable = raw.abs() >= EMPTY_GROUP_USD
    pub = published.reindex(ours.index).fillna(0.0)
    assert reachable.sum() > 40
    pd.testing.assert_series_equal(
        pd.Series(ours[reachable]),
        pd.Series(pub[reachable]),
        check_names=False,
        rtol=1e-9,
    )


def test_zeros_stay_zero_and_outside_families_pass_raw() -> None:
    """Raw zeros survive both layers (an anchor-only hold inside an anchored
    family is the one designed exception, small by construction), the
    never-imported structure stays exactly zero, and every commodity outside
    :data:`ANCHORED_FAMILIES` keeps its raw FBS value through the anchor
    layer — Wes's scope decision, full primary signal elsewhere."""
    raw = raw_import_vector(YEAR)
    out = conditioned_mcif(YEAR)
    factors = mcif_condition_factors(YEAR)
    anchored = anchored_mcif(YEAR)

    anchor_only = (raw < 1e6) & (anchored > 0.0)
    assert float(anchored[anchor_only].sum()) < 3e9
    violating = out[(raw == 0.0) & ~anchor_only]
    assert (violating == 0.0).all()
    never = [c for c in NEVER_IMPORTED_COMMODITIES if c in out.index]
    assert (out.reindex(never).fillna(0.0) == 0.0).all()
    assert (factors >= 0.0).all()

    outside = pd.Series(
        [str(c)[:4] not in ANCHORED_FAMILIES for c in raw.index], index=raw.index
    )
    pd.testing.assert_series_equal(
        pd.Series(anchored[outside]),
        pd.Series(raw[outside]),
        check_names=False,
        rtol=1e-12,
    )


def test_2017_anchoring_reproduces_the_published_cells_in_the_families() -> None:
    """At the anchor year growth is 1 by construction, so every anchored-family
    commodity with both bases must sit exactly on its published 2017 cell."""
    anchored = anchored_mcif(2017)
    raw = raw_import_vector(2017)
    pub = benchmark_mcif(2017).reindex(anchored.index).fillna(0.0)
    in_family = pd.Series(
        [str(c)[:4] in ANCHORED_FAMILIES for c in anchored.index],
        index=anchored.index,
    )
    both = in_family & (pub >= 1e6) & (raw >= 1e6)
    assert both.sum() > 20
    pd.testing.assert_series_equal(
        pd.Series(anchored[both]), pd.Series(pub[both]), check_names=False, rtol=1e-9
    )


def test_the_2017_between_group_gap_closes() -> None:
    """The between-group composition error at the anchor (~8.8% once the
    S00300 pass-through restored that row; #785's original 20.6% predated it)
    is exactly what the conditioning removes; the raw vector must show it and
    the conditioned one must not."""
    published = _published_mcif(YEAR)
    groups = _commodity_to_summary()

    def comp_error(vector: pd.Series) -> float:
        ours = vector.groupby(groups.reindex(vector.index)).sum()
        pub = published.reindex(ours.index).fillna(0.0)
        scaled = pub * (float(ours.sum()) / float(pub.sum()))
        return float((ours - scaled).abs().sum() / ours.sum())

    assert comp_error(raw_import_vector(YEAR)) > 0.05
    assert comp_error(conditioned_mcif(YEAR)) < 0.01


def test_duties_ride_the_conditioned_goods_base() -> None:
    """MDTY's split must move with the conditioning; its total must not
    (B235RC is observed)."""
    duties = mdty_detail_usd(YEAR, False)
    assert float(duties.sum()) > 0
    # services and never-imported structure carry no duty
    assert float(duties.get('531ORE', 0.0)) == 0.0


def test_years_outside_the_workbook_are_refused() -> None:
    with pytest.raises(ValueError, match='summary Supply workbook'):
        mcif_condition_factors(2025)
