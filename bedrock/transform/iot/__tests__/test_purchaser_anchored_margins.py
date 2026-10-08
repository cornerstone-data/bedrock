"""The purchaser-value anchor for final-demand margins (#872).

BEA: *"We treat the initial purchaser valuation as fixed and so we subtract off
the distributed margins and transportation to calculate a residual basic
value."* These pin that the construction is exact at the anchor year, that the
identity holds by build, and that the rows which must not be purchaser-anchored
are not.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from bedrock.transform.iot.nowcast_margins import (
    BUYER_LEVEL,
    INVENTORY_BUYER_CODE,
    LEVEL_BASIS,
    MARGIN_TYPE_LEVEL,
    MARGIN_TYPES,
    PRODUCER_ANCHOR,
    PURCHASER_ANCHOR,
    PURCHASER_COLUMN,
    final_demand_buyers,
    load_margins_transactions,
    negative_basic_value,
    purchaser_anchored_final_demand,
    purchaser_anchored_shares,
)


def test_final_demand_buyers_are_the_twenty_f_codes() -> None:
    buyers = final_demand_buyers()
    assert len(buyers) == 20
    assert all(code.startswith('F') for code in buyers)
    assert INVENTORY_BUYER_CODE in buyers


def test_anchor_takes_exactly_two_values() -> None:
    shares = purchaser_anchored_shares()
    assert set(shares['anchor']) == {PURCHASER_ANCHOR, PRODUCER_ANCHOR}


def test_industry_buyers_are_never_purchaser_anchored() -> None:
    """The intermediate block is built at producer prices - no PUR to anchor on."""
    shares = purchaser_anchored_shares()
    anchored = shares[shares['anchor'] == PURCHASER_ANCHOR]
    buyers = anchored.index.get_level_values(BUYER_LEVEL).astype(str)
    assert buyers.isin(final_demand_buyers()).all()


def test_change_in_inventories_is_not_purchaser_anchored() -> None:
    """⚠️ F03000 is a final-demand buyer, and a share of a *change* means nothing.

    It is carried as a level for the same reason it is not carried as a rate:
    margin is booked when inventory builds and a drawdown carries the offsetting
    negative, so the denominator is a timing artefact.
    """
    shares = purchaser_anchored_shares()
    buyers = shares.index.get_level_values(BUYER_LEVEL).astype(str)
    inventory = shares[buyers == INVENTORY_BUYER_CODE]
    assert not inventory.empty
    assert (inventory['anchor'] == PRODUCER_ANCHOR).all()
    assert (inventory['basis'] == LEVEL_BASIS).all()


def test_producer_anchored_rows_carry_no_purchaser_share() -> None:
    shares = purchaser_anchored_shares()
    producer = shares[shares['anchor'] == PRODUCER_ANCHOR]
    assert producer['purchaser_share'].isna().all()
    anchored = shares[shares['anchor'] == PURCHASER_ANCHOR]
    assert anchored['purchaser_share'].notna().all()
    assert (anchored['purchaser_share'] > 0).all()


def _anchored_purchaser_block(year: int) -> pd.DataFrame:
    shares = purchaser_anchored_shares()
    anchored = shares[shares['anchor'] == PURCHASER_ANCHOR]
    pairs = anchored.index.droplevel(MARGIN_TYPE_LEVEL).unique()
    published = load_margins_transactions(year)
    values = published[PURCHASER_COLUMN].reindex(pairs).fillna(0.0)
    return values.unstack(BUYER_LEVEL).fillna(0.0)


def test_the_anchor_year_is_reproduced_exactly() -> None:
    """The shares come from 2017, so 2017 must come back to the dollar.

    ⚠️ Scored against the published margin **restricted to the anchored rows**.
    A (buyer, commodity) pair can be purchaser-anchored for wholesale and
    carried as a level for retail; comparing against the pair's published total
    would charge this construction for margin it never claimed.
    """
    purchaser = _anchored_purchaser_block(2017)
    basic, booked = purchaser_anchored_final_demand(purchaser)
    shares = purchaser_anchored_shares()
    anchored = shares[shares['anchor'] == PURCHASER_ANCHOR]
    published = load_margins_transactions(2017)

    for margin_type in MARGIN_TYPES:
        rows = anchored.xs(margin_type, level=MARGIN_TYPE_LEVEL).index
        reference = (
            published[margin_type]
            .reindex(rows)
            .fillna(0.0)
            .unstack(BUYER_LEVEL)
            .reindex(index=purchaser.index, columns=purchaser.columns)
            .fillna(0.0)
        )
        assert np.allclose(booked[margin_type].to_numpy(), reference.to_numpy())
    assert basic.to_numpy().min() > -1.0


def test_the_identity_holds_by_construction() -> None:
    """PUR - (Margin+TC) = BAS, on any base, because BAS is the residual."""
    purchaser = _anchored_purchaser_block(2017)
    basic, booked = purchaser_anchored_final_demand(purchaser)
    total = sum(booked[margin_type] for margin_type in MARGIN_TYPES)
    assert np.allclose((basic + total).to_numpy(), purchaser.to_numpy())


def test_a_moved_base_carries_the_shares_not_the_levels() -> None:
    """Doubling the purchaser value doubles the margin, which a level would not."""
    purchaser = _anchored_purchaser_block(2017)
    _, once = purchaser_anchored_final_demand(purchaser)
    _, twice = purchaser_anchored_final_demand(purchaser * 2.0)
    for margin_type in MARGIN_TYPES:
        assert np.allclose(
            twice[margin_type].to_numpy(), once[margin_type].to_numpy() * 2.0
        )


def test_negative_basic_value_is_reported_not_clipped() -> None:
    """⚠️ Not hypothetical - one cell does go negative on the 2012 base.

    This test constructs the condition directly rather than depending on that
    measurement, so it keeps pinning the guard if the published tables are
    revised. The guard reports rather than clips: a cell whose 2017 shares
    overshoot its later purchaser value is telling you about the share.
    """
    purchaser = _anchored_purchaser_block(2017)
    basic, _ = purchaser_anchored_final_demand(purchaser)
    assert negative_basic_value(basic).empty

    squeezed = purchaser.copy()
    squeezed.iloc[:, 0] = -squeezed.iloc[:, 0].abs()
    flagged = negative_basic_value(purchaser_anchored_final_demand(squeezed)[0])
    assert not flagged.empty
    assert (flagged['basic_value'] < 0).all()


def test_cells_with_no_published_margin_pass_through_untouched() -> None:
    """BEA's zeros are a selection, not missing data - no margin is invented."""
    purchaser = _anchored_purchaser_block(2017)
    empty = purchaser.copy()
    empty.loc[:, :] = 0.0
    basic, booked = purchaser_anchored_final_demand(empty)
    assert np.allclose(basic.to_numpy(), 0.0)
    for margin_type in MARGIN_TYPES:
        assert np.allclose(booked[margin_type].to_numpy(), 0.0)
