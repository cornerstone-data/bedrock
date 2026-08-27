"""Tests for post–3-way reaggregation of G/T/D back to monetary 221100."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Callable, Iterator
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from bedrock.publish.cache_reset import clear_all_publish_caches
from bedrock.publish.model_objects import (
    get_D,
    get_N,
    get_Phi,
    get_q,
    get_Rho,
    get_U,
    get_x,
)
from bedrock.transform.eeio.cornerstone_disagg_pipeline import (
    collapse_electricity_children_square,
    cornerstone_sector_disagg_active,
    derive_disagg_io_bundle,
    derive_disagg_Ytot_with_trade,
    electricity_disaggregation_enabled,
    electricity_reaggregation_enabled,
    electricity_reallocation_enabled,
    get_waste_disagg_weights,
    reaggregate_electricity_children_aq,
    reaggregate_electricity_children_b,
)
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_Aq,
    derive_cornerstone_Aq_reaggregated,
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_B_non_finetuned,
    derive_cornerstone_B_reaggregated,
    derive_cornerstone_U_set,
    derive_cornerstone_U_with_negatives,
    derive_cornerstone_V,
    derive_cornerstone_VA,
    derive_cornerstone_Vnorm_scrap_corrected,
    derive_cornerstone_x,
    derive_cornerstone_x_after_redefinition,
    derive_cornerstone_y_nab_reaggregated,
)
from bedrock.transform.eeio.electricity_disaggregation import (
    ELECTRICITY_AGGREGATE,
    ELECTRICITY_DISAGG_SECTORS,
    _derive_post_reallocation_checkpoint_for_disagg,
    build_electricity_disagg_go_weights,
    build_electricity_disagg_use_intersection_weights,
    get_2017_eia_purchaser_allocation,
)
from bedrock.transform.eeio.electricity_gtd_allocation import mecs_purchased_kwh
from bedrock.utils.config.usa_config import reset_usa_config, set_global_usa_config
from bedrock.utils.economic.inflation_helpers_cornerstone import (
    clear_cornerstone_inflation_caches,
)
from bedrock.utils.math.formulas import backcompute_y_from_A_and_q, compute_L_matrix
from bedrock.utils.schemas.cornerstone_schemas import (
    CORNERSTONE_COMMODITIES,
    CORNERSTONE_COMMODITIES_ELEC,
    CORNERSTONE_INDUSTRIES,
    CORNERSTONE_INDUSTRIES_ELEC,
)
from bedrock.utils.schemas.single_region_types import SingleRegionAqMatrixSet

_CACHED_FUNCTIONS: list[Callable[..., object]] = [
    get_waste_disagg_weights,
    electricity_reallocation_enabled,
    electricity_disaggregation_enabled,
    electricity_reaggregation_enabled,
    derive_disagg_io_bundle,
    cornerstone_sector_disagg_active,
    derive_disagg_Ytot_with_trade,
    build_electricity_disagg_go_weights,
    build_electricity_disagg_use_intersection_weights,
    get_2017_eia_purchaser_allocation,
    mecs_purchased_kwh,
    _derive_post_reallocation_checkpoint_for_disagg,
    derive_cornerstone_V,
    derive_cornerstone_Vnorm_scrap_corrected,
    derive_cornerstone_U_with_negatives,
    derive_cornerstone_U_set,
    derive_cornerstone_VA,
    derive_cornerstone_x,
    derive_cornerstone_x_after_redefinition,
    derive_cornerstone_Aq,
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_Aq_reaggregated,
    derive_cornerstone_B_non_finetuned,
    derive_cornerstone_B_reaggregated,
]


def _clear_caches() -> None:
    for fn in _CACHED_FUNCTIONS:
        if hasattr(fn, 'cache_clear'):
            fn.cache_clear()
    clear_cornerstone_inflation_caches()
    clear_all_publish_caches()
    from bedrock.transform.eeio.cornerstone_year_scaling import (  # noqa: PLC0415
        clear_summary_year_scaled_aq,
    )
    from bedrock.transform.eeio.electricity_gtd_allocation import (  # noqa: PLC0415
        clear_reanchored_electricity_q,
    )

    clear_summary_year_scaled_aq()
    clear_reanchored_electricity_q()


@contextmanager
def _dollar_industrial_weights() -> Iterator[None]:
    import bedrock.transform.eeio.electricity_gtd_allocation as gtd  # noqa: PLC0415

    orig = gtd.allocate_purchaser_gtd

    def _wrapped(*args: Any, **kwargs: Any) -> Any:
        kwargs = dict(kwargs)
        kwargs['industrial_weights'] = 'dollars'
        return orig(*args, **kwargs)

    with patch.object(gtd, 'allocate_purchaser_gtd', _wrapped):
        yield


def _setup(config_name: str) -> None:
    _clear_caches()
    reset_usa_config(should_reset_env_var=True)
    set_global_usa_config(config_name)


def _teardown() -> None:
    _clear_caches()
    reset_usa_config(should_reset_env_var=True)


def _as_float(value: object) -> float:
    arr = np.asarray(value, dtype=float).reshape(-1)
    if arr.size != 1:
        raise TypeError(f'expected a scalar, got size {arr.size}')
    return float(arr[0])


def _as_series(value: object) -> pd.Series[Any]:
    if not isinstance(value, pd.Series):
        raise TypeError(f'expected a Series, got {type(value)!r}')
    return value


def test_collapse_3x3_block_identity() -> None:
    children = list(ELECTRICITY_DISAGG_SECTORS)
    other = '1111A0'
    codes = children + [other]
    df = pd.DataFrame(0.0, index=codes, columns=codes)
    df.loc[children, children] = np.arange(9.0).reshape(3, 3)
    df.loc[other, children] = [1.0, 2.0, 3.0]
    df.loc[children, other] = [4.0, 5.0, 6.0]
    df.loc[other, other] = 7.0
    out = collapse_electricity_children_square(
        df, row_codes=['221100', other], col_codes=['221100', other]
    )
    assert list(out.index) == ['221100', other]
    assert list(out.columns) == ['221100', other]
    assert _as_float(out.loc['221100', '221100']) == pytest.approx(36.0)
    assert _as_float(out.loc[other, '221100']) == pytest.approx(6.0)
    assert _as_float(out.loc['221100', other]) == pytest.approx(15.0)
    assert _as_float(out.loc[other, other]) == pytest.approx(7.0)


def test_reaggregate_aq_is_noop_when_flag_off() -> None:
    _setup('test_usa_config_waste_disagg_electricity_disaggregation.yaml')
    try:
        cols = list(ELECTRICITY_DISAGG_SECTORS) + ['1111A0']
        adom = pd.DataFrame(0.01, index=cols, columns=cols)
        aimp = pd.DataFrame(0.0, index=cols, columns=cols)
        q = pd.Series({c: 10.0 for c in cols}, dtype=float)
        aq = SingleRegionAqMatrixSet(Adom=adom, Aimp=aimp, scaled_q=q)  # type: ignore[arg-type]
        out = reaggregate_electricity_children_aq(aq)
        pd.testing.assert_frame_equal(out.Adom, adom)
        pd.testing.assert_series_equal(out.scaled_q, q)
    finally:
        _teardown()


def test_reaggregate_raises_when_children_missing() -> None:
    _setup('test_usa_config_waste_disagg_electricity_reaggregation.yaml')
    try:
        codes = ['1111A0', '221100']
        adom = pd.DataFrame(0.01, index=codes, columns=codes)
        aimp = pd.DataFrame(0.0, index=codes, columns=codes)
        q = pd.Series({c: 10.0 for c in codes}, dtype=float)
        aq = SingleRegionAqMatrixSet(Adom=adom, Aimp=aimp, scaled_q=q)  # type: ignore[arg-type]
        with pytest.raises(ValueError, match='missing child sectors'):
            reaggregate_electricity_children_aq(aq)
    finally:
        _teardown()


def test_reaggregate_aq_reconstructs_u_from_collapsed_block() -> None:
    _setup('test_usa_config_waste_disagg_electricity_reaggregation.yaml')
    try:
        children = list(ELECTRICITY_DISAGG_SECTORS)
        codes = children + ['1111A0']
        adom = pd.DataFrame(0.02, index=codes, columns=codes)
        aimp = pd.DataFrame(0.01, index=codes, columns=codes)
        q = pd.Series({'221110': 10.0, '221121': 20.0, '221122': 30.0, '1111A0': 5.0})
        aq = SingleRegionAqMatrixSet(Adom=adom, Aimp=aimp, scaled_q=q)  # type: ignore[arg-type]
        with patch(
            'bedrock.transform.eeio.cornerstone_disagg_pipeline.'
            'CORNERSTONE_COMMODITIES',
            ['221100', '1111A0'],
        ):
            out = reaggregate_electricity_children_aq(aq)
        assert float(out.scaled_q.loc['221100']) == pytest.approx(60.0)
        for a_collapsed, a_src in ((out.Adom, adom), (out.Aimp, aimp)):
            u_src = a_src.multiply(q, axis=1)
            u_expected = collapse_electricity_children_square(
                u_src, row_codes=['221100', '1111A0'], col_codes=['221100', '1111A0']
            )
            u_got = a_collapsed.multiply(out.scaled_q, axis=1)
            pd.testing.assert_frame_equal(u_got, u_expected, check_names=False)
    finally:
        _teardown()


def test_reaggregate_b_is_q_weighted() -> None:
    _setup('test_usa_config_waste_disagg_electricity_reaggregation.yaml')
    try:
        children = list(ELECTRICITY_DISAGG_SECTORS)
        b = pd.DataFrame(
            [[1.0, 2.0, 3.0, 4.0]],
            index=['CO2'],
            columns=children + ['1111A0'],
        )
        q = pd.Series({'221110': 1.0, '221121': 1.0, '221122': 2.0, '1111A0': 5.0})
        with patch(
            'bedrock.transform.eeio.cornerstone_disagg_pipeline.'
            'CORNERSTONE_COMMODITIES',
            ['221100', '1111A0'],
        ):
            out = reaggregate_electricity_children_b(b, q)
        expected = (1.0 * 1.0 + 2.0 * 1.0 + 3.0 * 2.0) / 4.0
        assert _as_float(out.loc['CO2', '221100']) == pytest.approx(expected)
        assert _as_float(out.loc['CO2', '1111A0']) == pytest.approx(4.0)
        assert list(out.columns) == ['221100', '1111A0']
    finally:
        _teardown()


@pytest.mark.eeio_integration
def test_reagg_internals_stay_407_published_are_405() -> None:
    _setup('test_usa_config_waste_disagg_electricity_reaggregation.yaml')
    try:
        with _dollar_industrial_weights():
            V = derive_cornerstone_V()
            aq_scaled = derive_cornerstone_Aq_scaled()
            aq = derive_cornerstone_Aq_reaggregated()
            b = derive_cornerstone_B_reaggregated()
            n = get_N()
            u = get_U()
            q = get_q()
            x = get_x()
        assert list(V.index) == CORNERSTONE_INDUSTRIES_ELEC
        assert list(aq_scaled.Adom.columns) == CORNERSTONE_COMMODITIES_ELEC
        assert ELECTRICITY_AGGREGATE not in V.index
        assert list(aq.Adom.columns) == CORNERSTONE_COMMODITIES
        assert ELECTRICITY_AGGREGATE in aq.Adom.columns
        for code in ELECTRICITY_DISAGG_SECTORS:
            assert code not in aq.Adom.columns
            assert code not in n.columns
            assert code not in q.index
            assert code not in x.index
        assert ELECTRICITY_AGGREGATE in n.columns
        assert ELECTRICITY_AGGREGATE in q.index
        assert ELECTRICITY_AGGREGATE in x.index
        assert list(b.columns) == CORNERSTONE_COMMODITIES
        assert u.shape[1] >= len(CORNERSTONE_INDUSTRIES)
    finally:
        _teardown()


@pytest.mark.eeio_integration
def test_reagg_identities_on_scaled_q() -> None:
    _setup('test_usa_config_waste_disagg_electricity_reaggregation.yaml')
    try:
        with _dollar_industrial_weights():
            aq_407 = derive_cornerstone_Aq_scaled()
            aq = derive_cornerstone_Aq_reaggregated()
            b_407 = derive_cornerstone_B_non_finetuned()
            b = derive_cornerstone_B_reaggregated()
            y_nab = derive_cornerstone_y_nab_reaggregated()
        q407 = aq_407.scaled_q
        assert float(aq.scaled_q.loc[ELECTRICITY_AGGREGATE]) == pytest.approx(
            float(q407.loc[list(ELECTRICITY_DISAGG_SECTORS)].sum())
        )
        for a_coll, a_src in (
            (aq.Adom, aq_407.Adom),
            (aq.Aimp, aq_407.Aimp),
        ):
            u_src = a_src.multiply(q407, axis=1)
            u_coll = collapse_electricity_children_square(
                u_src,
                row_codes=CORNERSTONE_COMMODITIES,
                col_codes=CORNERSTONE_COMMODITIES,
            )
            u_got = a_coll.multiply(aq.scaled_q, axis=1)
            pd.testing.assert_frame_equal(u_got, u_coll, check_names=False)
        bq_407 = b_407.multiply(q407.reindex(b_407.columns), axis=1).sum(axis=1)
        bq_405 = b.multiply(aq.scaled_q.reindex(b.columns), axis=1).sum(axis=1)
        pd.testing.assert_series_equal(
            bq_407, bq_405, check_names=False, atol=1e-6, rtol=1e-8
        )
        l_dom = compute_L_matrix(A=aq.Adom)
        q_from_y = l_dom @ y_nab.reindex(l_dom.columns, fill_value=0.0)
        np.testing.assert_allclose(
            aq.scaled_q.to_numpy(),
            np.asarray(q_from_y.reindex(aq.scaled_q.index).to_numpy(), dtype=float),
            rtol=1e-6,
            atol=1e-4,
        )
        _ = backcompute_y_from_A_and_q(A=aq.Adom, q=aq.scaled_q)
    finally:
        _teardown()


@pytest.mark.eeio_integration
def test_reagg_phi_rho_on_product_yaml() -> None:
    _setup('2025_usa_cornerstone_v0_3_electricity_reaggregation.yaml')
    try:
        with _dollar_industrial_weights():
            phi = get_Phi()
            rho = get_Rho()
        assert phi is not None
        assert rho is not None
        from bedrock.utils.config.usa_config import get_usa_config  # noqa: PLC0415
        from bedrock.utils.economic.inflation_helpers_cornerstone import (  # noqa: PLC0415
            default_price_index_panel_years,
            derive_price_index_panel,
        )

        model_year = str(get_usa_config().model_base_year)
        assert _as_float(phi.loc[ELECTRICITY_AGGREGATE, model_year]) == pytest.approx(
            1.0
        )
        assert np.isfinite(_as_series(rho.loc[ELECTRICITY_AGGREGATE])).all()
        panel_407 = derive_price_index_panel(default_price_index_panel_years())
        pd.testing.assert_series_equal(
            _as_series(rho.loc[ELECTRICITY_AGGREGATE]),
            _as_series(panel_407.loc['221110']),
            check_names=False,
        )
    finally:
        _teardown()


@pytest.mark.eeio_integration
def test_reagg_dn_differs_from_production() -> None:
    from bedrock.utils.math.formulas import (  # noqa: PLC0415
        compute_d,
        compute_L_matrix,
        compute_M_matrix,
        compute_n,
    )

    _setup('2025_usa_cornerstone_v0_3.yaml')
    try:
        from bedrock.transform.eeio.derived import (  # noqa: PLC0415
            derive_Aq_usa,
            derive_B_usa_non_finetuned,
        )

        aq_prod = derive_Aq_usa()
        b_prod = derive_B_usa_non_finetuned()
        d_prod = compute_d(B=b_prod)
        n_prod = compute_n(
            M=compute_M_matrix(
                B=b_prod, L=compute_L_matrix(A=aq_prod.Adom + aq_prod.Aimp)
            )
        )
        d_prod_s = d_prod.squeeze()
        n_prod_s = n_prod.squeeze()
        d_prod_221100 = _as_float(_as_series(d_prod_s).loc[ELECTRICITY_AGGREGATE])
        n_prod_221100 = _as_float(_as_series(n_prod_s).loc[ELECTRICITY_AGGREGATE])
    finally:
        _teardown()

    _setup('2025_usa_cornerstone_v0_3_electricity_reaggregation.yaml')
    try:
        with _dollar_industrial_weights():
            d_reagg = _as_float(
                _as_series(get_D().squeeze()).loc[ELECTRICITY_AGGREGATE]
            )
            n_reagg = _as_float(
                _as_series(get_N().squeeze()).loc[ELECTRICITY_AGGREGATE]
            )
        assert d_reagg != pytest.approx(d_prod_221100, rel=1e-9, abs=1e-12)
        assert n_reagg != pytest.approx(n_prod_221100, rel=1e-9, abs=1e-12)
    finally:
        _teardown()
