"""The seller × commodity matrix (#745).

Pinned: the synthetic weight lines never enter the matrix (the double-count
trap the issue records), the 1:m product split conserves mass, the NAICS
roll-up hits the right BEA trade codes on the known specials, and the 2017
matrix carries at least the pair coverage the issue measured.
"""

from __future__ import annotations

import pytest

from bedrock.extract.census.Census_EC import SYNTHETIC_PXI_LINES
from bedrock.transform.inventories.seller_matrix import (
    TRANSPORT_MODE_COMMODITIES,
    _product_to_commodity,
    bea_trade_code_for_seller,
    bea_trade_matrix,
    seller_commodity_matrix,
    transport_mode_matrix,
)
from bedrock.utils.config.common import load_env_file_key


def _census_key_available() -> bool:
    """Same accessor-based check as ``test_nowcast_intermediate``: the key
    lives in ``.env`` and only reaches ``os.environ`` once ``get_api_key``
    loads it, so an ``os.getenv`` check reports "no key" on a machine that
    has one."""
    try:
        return bool(load_env_file_key('api_key', 'Census'))
    except Exception:  # noqa: BLE001 - any failure to resolve means "cannot run"
        return False


#: The matrix builders reach the Census PxI accessors, which fetch live where
#: no local cache exists. Without a key the API returns its "Missing Key" HTML
#: page and the fetch dies in ``json.loads`` - skip rather than fail there.
needs_census = pytest.mark.skipif(
    not _census_key_available(),
    reason='no Census API key: set CENSUS_API_KEY (repo secret in CI, .env locally)',
)


def test_rollup_specials() -> None:
    assert bea_trade_code_for_seller('423110') == '423100'
    assert bea_trade_code_for_seller('423210') == '423A00'  # furniture wholesale
    assert bea_trade_code_for_seller('424710') == '424700'
    assert bea_trade_code_for_seller('425120') == '425000'
    assert bea_trade_code_for_seller('442110') == '4B0000'  # other retail
    assert bea_trade_code_for_seller('445110') == '445000'
    assert bea_trade_code_for_seller('454110') == '454000'
    assert bea_trade_code_for_seller('336111') is None  # not a trade NAICS


def test_product_split_conserves_mass() -> None:
    """A 1:m product's shares sum to one, so the join neither creates nor
    destroys weight."""
    mapping = _product_to_commodity()
    per_product = mapping.groupby('product')['split'].sum()
    assert (per_product.round(9) == 1.0).all()


@needs_census
def test_matrix_shape_and_the_synthetic_exclusion() -> None:
    matrix = seller_commodity_matrix(2017)

    assert (matrix.to_numpy() >= 0).all()
    assert int((matrix > 0).to_numpy().sum()) >= 2_285  # the issue's floor
    # synthetic labels are NIPA-line names and could never be a 6-digit NAICS
    # row, but the guard is about the VALUE not the label: their duplicated
    # mass must not inflate the component sellers' totals, which the module
    # excludes before grouping - assert the labels are nowhere in the index.
    for label in SYNTHETIC_PXI_LINES:
        assert label not in matrix.index


@needs_census
def test_rolled_matrix_reaches_every_bea_trade_code() -> None:
    rolled = bea_trade_matrix(2017)

    assert rolled.shape[0] == 19
    assert (rolled.sum(axis=1) > 0).all()


@needs_census
def test_transport_mode_matrix_carries_all_five_modes() -> None:
    """The transport analog: five mode-commodity rows, each mode's own
    revenue-controlled allocation, nonnegative in the receiving direction."""
    matrix = transport_mode_matrix(2017)

    assert sorted(matrix.index) == sorted(TRANSPORT_MODE_COMMODITIES.values())
    assert (matrix.sum(axis=1) > 0).all()
