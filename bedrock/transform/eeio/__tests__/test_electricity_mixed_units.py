"""Tests for 221110 mixed-unit (MWh) conversion."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Callable, Iterator, cast
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest

from bedrock.extract.disaggregation.egrid_generation import (
    egrid_mwh_for_io_year,
    eia_table_2_2_end_use_mwh,
    eia_table_2_14_export_mwh,
    eia_table_3_1_total_mwh,
)
from bedrock.transform.eeio.cornerstone_disagg_pipeline import (
    build_end_use_map,
    compute_mixed_unit_ef_vectors,
    cornerstone_sector_disagg_active,
    derive_disagg_io_bundle,
    derive_disagg_Ytot_with_trade,
    electricity_conversion_factors,
    electricity_disaggregation_enabled,
    electricity_mixed_units_enabled,
    electricity_reallocation_enabled,
    get_waste_disagg_weights,
)
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_Aq,
    derive_cornerstone_Aq_mixed_units,
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_B_non_finetuned,
    derive_cornerstone_U_set,
    derive_cornerstone_U_with_negatives,
    derive_cornerstone_V,
    derive_cornerstone_VA,
    derive_cornerstone_Vnorm_scrap_corrected,
    derive_cornerstone_x,
    derive_cornerstone_x_after_redefinition,
    derive_cornerstone_y_nab,
    derive_cornerstone_y_nab_mixed_units,
)
from bedrock.transform.eeio.electricity_disaggregation import (
    GENERATION_SECTOR,
    _derive_post_reallocation_checkpoint_for_disagg,
    apply_electricity_unit_conversion_to_A,
    apply_electricity_unit_conversion_to_B,
    build_electricity_disagg_go_weights,
    build_electricity_disagg_use_intersection_weights,
    electricity_output_factor,
    get_2017_eia_purchaser_allocation,
)
from bedrock.transform.eeio.electricity_gtd_allocation import mecs_purchased_kwh
from bedrock.utils.config.usa_config import (
    get_usa_config,
    reset_usa_config,
    set_global_usa_config,
)
from bedrock.utils.economic.inflation_helpers_cornerstone import (
    clear_cornerstone_inflation_caches,
)
from bedrock.utils.math.formulas import backcompute_y_from_A_and_q, compute_d
from bedrock.utils.schemas.cornerstone_schemas import ELECTRICITY_DISAGG_SECTORS
from bedrock.utils.schemas.single_region_types import SingleRegionAqMatrixSet
from bedrock.utils.validation.diagnostics_helpers import (
    apply_mixed_units_bly_diff_exemptions,
    apply_mixed_units_ef_diff_exemptions,
    pull_efs_for_diagnostics,
)

# Include disagg-gate caches so mixed-units setup/teardown cannot leave
# electricity_disaggregation_enabled() sticky True for later 405-sector tests.
_CACHED_FUNCTIONS: list[Callable[..., object]] = [
    get_waste_disagg_weights,
    electricity_reallocation_enabled,
    electricity_disaggregation_enabled,
    electricity_mixed_units_enabled,
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
    derive_cornerstone_Aq_mixed_units,
    derive_cornerstone_B_non_finetuned,
    derive_cornerstone_y_nab,
    derive_cornerstone_y_nab_mixed_units,
]


def _clear_caches() -> None:
    for fn in _CACHED_FUNCTIONS:
        if hasattr(fn, 'cache_clear'):
            fn.cache_clear()
    clear_cornerstone_inflation_caches()
    from bedrock.transform.eeio.cornerstone_year_scaling import (  # noqa: PLC0415
        clear_summary_year_scaled_aq,
    )
    from bedrock.transform.eeio.electricity_gtd_allocation import (  # noqa: PLC0415
        clear_reanchored_electricity_q,
    )

    clear_summary_year_scaled_aq()
    clear_reanchored_electricity_q()
    egrid_mwh_for_io_year.cache_clear()
    eia_table_2_2_end_use_mwh.cache_clear()
    eia_table_2_14_export_mwh.cache_clear()
    eia_table_3_1_total_mwh.cache_clear()


@contextmanager
def _dollar_industrial_weights() -> Iterator[None]:
    import bedrock.transform.eeio.electricity_gtd_allocation as gtd  # noqa: PLC0415

    orig = gtd.allocate_purchaser_gtd

    def _wrapped(*args: object, **kwargs: object) -> object:
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


@pytest.fixture
def mixed_units_config() -> str:
    return 'test_usa_config_waste_disagg_electricity_mixed_units.yaml'


def test_electricity_output_factor_raises_on_bad_inputs() -> None:
    with pytest.raises(ValueError, match='q\\$_221110'):
        electricity_output_factor(0.0, 1e9)
    with pytest.raises(ValueError, match='mwh_221110'):
        electricity_output_factor(1e11, 0.0)


def test_c_row_from_conversion_factors_is_flat_one_over_p(
    mixed_units_config: str,
) -> None:
    _setup(mixed_units_config)
    try:
        cols = list(ELECTRICITY_DISAGG_SECTORS) + ['1111A0']
        adom = pd.DataFrame(0.01, index=cols, columns=cols)
        aimp = pd.DataFrame(0.0, index=cols, columns=cols)
        q = pd.Series({c: 1.0e9 for c in cols}, dtype=float)
        q[GENERATION_SECTOR] = 1.0e11
        aq = SingleRegionAqMatrixSet(Adom=adom, Aimp=aimp, scaled_q=q)  # type: ignore[arg-type]
        mwh = 4.0e9
        p_share = 0.4
        with (
            patch(
                'bedrock.extract.disaggregation.egrid_generation.egrid_mwh_for_io_year',
                return_value=mwh,
            ),
            patch(
                'bedrock.transform.eeio.electricity_gtd_allocation._go_p_and_td_shares',
                return_value=(p_share, 0.3),
            ),
        ):
            _c_col, c_row = electricity_conversion_factors(aq)
        q_elec = float(q.reindex(ELECTRICITY_DISAGG_SECTORS).sum())
        p = p_share * q_elec / mwh
        assert int(c_row.nunique()) == 1
        assert float(c_row.iloc[0]) == pytest.approx(1.0 / p)
        assert list(c_row.index) == cols
    finally:
        _teardown()


def test_uniform_c_row_similarity_transform() -> None:
    cols = [GENERATION_SECTOR, 'c0', 'c1', 'c2']
    A = pd.DataFrame(0.0, index=cols, columns=cols)
    for c in cols:
        A.loc[GENERATION_SECTOR, c] = 0.02
    q = pd.Series({c: 100.0 for c in cols}, dtype=float)
    q[GENERATION_SECTOR] = 500.0
    mwh = float(q[GENERATION_SECTOR]) * 0.01
    c_col = electricity_output_factor(float(q[GENERATION_SECTOR]), mwh)
    c_row = pd.Series(c_col, index=cols, dtype=float)
    A_m = apply_electricity_unit_conversion_to_A(A, c_col=c_col, c_row=c_row)
    for c in cols:
        if c == GENERATION_SECTOR:
            continue
        expected = cast(float, A.loc[GENERATION_SECTOR, c]) * float(c_row[c])
        assert A_m.loc[GENERATION_SECTOR, c] == pytest.approx(expected)


def test_build_end_use_map_includes_electricity_children() -> None:
    mapping = build_end_use_map()
    for code in ELECTRICITY_DISAGG_SECTORS:
        assert mapping[code] == 'Industrial'
    assert mapping['F04000'] == 'Exports'


def test_mixed_units_flag_off_is_noop() -> None:
    _setup('test_usa_config_waste_disagg_electricity_disaggregation.yaml')
    try:
        with _dollar_industrial_weights():
            aq_mon = derive_cornerstone_Aq_scaled()
            aq_mixed = derive_cornerstone_Aq_mixed_units()
        pd.testing.assert_frame_equal(aq_mon.Adom, aq_mixed.Adom)
        pd.testing.assert_series_equal(aq_mon.scaled_q, aq_mixed.scaled_q)
    finally:
        _teardown()


@patch(
    'bedrock.transform.eeio.electricity_gtd_allocation.egrid_mwh_for_io_year',
    return_value=4_000_000_000.0,
)
@patch(
    'bedrock.extract.disaggregation.egrid_generation.egrid_mwh_for_io_year',
    return_value=4_000_000_000.0,
)
def test_output_mwh_anchor(
    _mock_egrid: Mock,
    _mock_egrid_gtd: Mock,
    mixed_units_config: str,
) -> None:
    _setup(mixed_units_config)
    try:
        with _dollar_industrial_weights():
            aq = derive_cornerstone_Aq_mixed_units()
        assert aq.scaled_q[GENERATION_SECTOR] == pytest.approx(4_000_000_000.0)
    finally:
        _teardown()


def test_apply_mixed_units_ef_diff_exemptions() -> None:
    _setup('test_usa_config_waste_disagg_electricity_mixed_units.yaml')
    try:
        idx = pd.Index(['221110', '1111A0'], name='sector')
        comp = pd.DataFrame(
            {
                'D_new': [0.5, 1.0],
                'D_old_inflated': [50.0, 1.0],
                'D_old': [50.0, 1.0],
                'D_perc_diff': [0.99, 0.1],
            },
            index=idx,
        )
        out = apply_mixed_units_ef_diff_exemptions(comp, 'D')
        assert bool(np.isnan(cast(float, out.loc['221110', 'D_perc_diff'])))
        assert (
            out.loc['221110', 'exemption_reason'] == 'unit_incommensurate_mixed_units'
        )
        assert out.loc['1111A0', 'D_perc_diff'] == pytest.approx(0.1)
    finally:
        _teardown()


def test_apply_mixed_units_bly_diff_exemptions() -> None:
    _setup('test_usa_config_waste_disagg_electricity_mixed_units.yaml')
    try:
        df = pd.DataFrame(
            {
                'index': ['221110', '1111A0'],
                'BLy_new (MtCO2e)': [1.0, 2.0],
                'BLy_old (MtCO2e)': [0.5, 2.0],
                '(BLy_new - BLy_old) / BLy_old (%)': [1.0, 0.0],
            }
        )
        out = apply_mixed_units_bly_diff_exemptions(df)
        row = out.loc[out['index'] == '221110'].iloc[0]
        assert bool(np.isnan(cast(float, row['(BLy_new - BLy_old) / BLy_old (%)'])))
        assert row['exemption_reason'] == 'baseline_monetary_vs_live_mixed'
        other = out.loc[out['index'] == '1111A0'].iloc[0]
        assert other['(BLy_new - BLy_old) / BLy_old (%)'] == pytest.approx(0.0)
        assert other['exemption_reason'] == ''
    finally:
        _teardown()


def test_y_nab_stays_monetary_under_mixed_gate(mixed_units_config: str) -> None:
    _setup('test_usa_config_waste_disagg_electricity_disaggregation.yaml')
    try:
        with _dollar_industrial_weights():
            y_off = derive_cornerstone_y_nab().copy()
    finally:
        _teardown()
    _setup(mixed_units_config)
    try:
        with _dollar_industrial_weights():
            y_on = derive_cornerstone_y_nab()
        pd.testing.assert_series_equal(y_off, y_on)
    finally:
        _teardown()


@patch(
    'bedrock.transform.eeio.electricity_gtd_allocation.egrid_mwh_for_io_year',
    return_value=4_000_000_000.0,
)
@patch(
    'bedrock.extract.disaggregation.egrid_generation.egrid_mwh_for_io_year',
    return_value=4_000_000_000.0,
)
def test_y_nab_mixed_differs_from_monetary_under_gate(
    _mock_egrid: Mock,
    _mock_egrid_gtd: Mock,
    mixed_units_config: str,
) -> None:
    _setup(mixed_units_config)
    try:
        with _dollar_industrial_weights():
            y_mon = derive_cornerstone_y_nab()
            y_mix = derive_cornerstone_y_nab_mixed_units()
            aq = derive_cornerstone_Aq_mixed_units()
        assert y_mix[GENERATION_SECTOR] != pytest.approx(y_mon[GENERATION_SECTOR])
        y_back = backcompute_y_from_A_and_q(A=aq.Adom, q=aq.scaled_q)
        pd.testing.assert_series_equal(y_mix, y_back)
        u = aq.Adom.multiply(aq.scaled_q, axis=1).sum(axis=1) + y_mix
        pd.testing.assert_series_equal(aq.scaled_q, u, rtol=1e-6, check_names=False)
    finally:
        _teardown()


@patch(
    'bedrock.transform.eeio.electricity_gtd_allocation.egrid_mwh_for_io_year',
    return_value=4_000_000_000.0,
)
@patch(
    'bedrock.extract.disaggregation.egrid_generation.egrid_mwh_for_io_year',
    return_value=4_000_000_000.0,
)
def test_d_scalar_bridge_under_gate(
    _mock_egrid: Mock,
    _mock_egrid_gtd: Mock,
    mixed_units_config: str,
) -> None:
    _setup(mixed_units_config)
    try:
        gen = GENERATION_SECTOR
        cols = [gen, '1111A0', '1111B0']
        b_mon = pd.DataFrame(np.eye(len(cols)) * 10.0, index=cols, columns=cols)
        c_col = 0.02
        b_mix = apply_electricity_unit_conversion_to_B(b_mon, c_col)
        d_mon = compute_d(B=b_mon)
        d_mix = compute_d(B=b_mix)
        assert float(d_mix[gen]) == pytest.approx(float(d_mon[gen]) / c_col)
    finally:
        _teardown()


def test_compute_mixed_unit_ef_vectors_not_cached() -> None:
    gen = GENERATION_SECTOR
    cols = [gen, '1111A0']
    adom = pd.DataFrame([[0.0, 0.01], [0.0, 0.0]], index=cols, columns=cols)
    aimp = pd.DataFrame(0.0, index=cols, columns=cols)
    q = pd.Series({gen: 100.0, '1111A0': 50.0}, dtype=float)
    aq = SingleRegionAqMatrixSet(Adom=adom, Aimp=aimp, scaled_q=q)  # type: ignore[arg-type]
    b = pd.DataFrame(10.0, index=cols, columns=cols)
    c_row_a = pd.Series({gen: 0.5, '1111A0': 0.5})
    c_row_b = pd.Series({gen: 0.8, '1111A0': 0.8})
    with patch(
        'bedrock.transform.eeio.cornerstone_disagg_pipeline.electricity_conversion_factors',
        side_effect=[(0.5, c_row_a), (0.5, c_row_b)],
    ):
        r_class = compute_mixed_unit_ef_vectors(aq, b, prices_by_class=None)
        r_uniform = compute_mixed_unit_ef_vectors(
            aq, b, prices_by_class={'Industrial': 10.0, 'Total': 10.0}
        )
    assert not r_class.N.equals(r_uniform.N)


@pytest.mark.eeio_integration
@patch(
    'bedrock.transform.eeio.electricity_gtd_allocation.egrid_mwh_for_io_year',
    return_value=4_000_000_000.0,
)
@patch(
    'bedrock.extract.disaggregation.egrid_generation.egrid_mwh_for_io_year',
    return_value=4_000_000_000.0,
)
def test_pull_efs_mixed_units_config(_mock_egrid: Mock, _mock_egrid_gtd: Mock) -> None:
    _setup('2025_usa_cornerstone_v0_3_electricity_mixed_units.yaml')
    try:
        result = pull_efs_for_diagnostics()
        assert result.D_new is not None
        assert result.N_new is not None
    finally:
        _teardown()


def test_electricity_class_row_factors_missing_column_raises() -> None:
    from bedrock.transform.eeio.electricity_disaggregation import (  # noqa: PLC0415
        electricity_class_row_factors,
    )

    prices = {
        'Industrial': 7.0,
        'Commercial': 10.0,
        'Residential': 12.0,
        'Transportation': 9.0,
        'Total': 10.0,
    }
    with pytest.raises(ValueError, match='absent from end_use_map'):
        electricity_class_row_factors(
            pd.Series({'unknown_col': 0.1}),
            pd.Series({'unknown_col': 1.0}),
            pd.Series(dtype=float),
            prices,
            {},
            1e9,
        )


def test_export_mwh_is_twh_not_gwh() -> None:
    from bedrock.extract.disaggregation.egrid_generation import (  # noqa: PLC0415
        eia_table_2_14_export_mwh,
    )

    mwh = eia_table_2_14_export_mwh(2017)
    # ~10 TWh, not ~10 GWh (loader units vs Table 3.1 scale 1000).
    assert 1e6 < mwh < 5e7


@pytest.mark.eeio_integration
def test_live_conversion_factors_and_generation_mwh(
    mixed_units_config: str,
) -> None:
    from bedrock.transform.eeio.electricity_gtd_allocation import (  # noqa: PLC0415
        _go_p_and_td_shares,
        get_reanchored_eia_purchaser_allocation,
    )

    _setup(mixed_units_config)
    try:
        aq = derive_cornerstone_Aq_scaled()
        _c_col, c_row = electricity_conversion_factors(aq)
        cfg = get_usa_config()
        mwh = float(egrid_mwh_for_io_year(int(cfg.model_base_year)))
        p_share, _td = _go_p_and_td_shares()
        q_elec = float(aq.scaled_q.reindex(ELECTRICITY_DISAGG_SECTORS).sum())
        p = p_share * q_elec / mwh
        assert int(c_row.nunique()) == 1
        assert float(c_row.iloc[0]) == pytest.approx(1.0 / p, rel=1e-9)
        assert list(c_row.index) == list(aq.Adom.columns)

        alloc = get_reanchored_eia_purchaser_allocation()
        assert alloc is not None
        mwh_from_dollars = float(alloc.gen_dollars.sum()) / float(alloc.p)
        assert mwh_from_dollars == pytest.approx(
            float(alloc.mwh.sum()), rel=1e-6, abs=1.0
        )
        if not bool(alloc.clipped.any()):
            assert float(alloc.mwh.sum()) == pytest.approx(
                float(alloc.egrid_mwh), rel=1e-4, abs=1.0
            )
        else:
            assert mwh_from_dollars <= float(alloc.egrid_mwh) + 1.0

        aq_mix = derive_cornerstone_Aq_mixed_units()
        assert float(aq_mix.scaled_q[GENERATION_SECTOR]) == pytest.approx(
            mwh, rel=1e-6, abs=1.0
        )
    finally:
        _teardown()
