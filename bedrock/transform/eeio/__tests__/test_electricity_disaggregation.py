"""Tests for 221100 electricity sector disaggregation."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Callable, Iterator
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from bedrock.transform.allocation.derived import derive_E_usa
from bedrock.transform.eeio.cornerstone_disagg_pipeline import (
    cornerstone_sector_disagg_active,
    derive_disagg_io_bundle,
    derive_disagg_Ytot_with_trade,
    electricity_disaggregation_enabled,
    electricity_reallocation_enabled,
    get_waste_disagg_weights,
)
from bedrock.transform.eeio.cornerstone_year_scaling import scale_cornerstone_q
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_Aq,
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_B_non_finetuned,
    derive_cornerstone_U_set,
    derive_cornerstone_U_with_negatives,
    derive_cornerstone_V,
    derive_cornerstone_VA,
    derive_cornerstone_Vnorm_scrap_corrected,
    derive_cornerstone_x,
    derive_cornerstone_x_after_redefinition,
)
from bedrock.transform.eeio.electricity_disaggregation import (
    ELECTRICITY_AGGREGATE,
    ELECTRICITY_DISAGG_SECTORS,
    _derive_post_reallocation_checkpoint_for_disagg,
    _float_ndarray,
    _frame_cell_float,
    _loc_cols_sum,
    applied_utilities_summary_q_growth_ratio,
    build_electricity_detail_GO_growth_ratios,
    build_electricity_disagg_go_weights,
    build_electricity_disagg_use_intersection_weights,
    disaggregate_use_industry_columns,
    get_2017_eia_purchaser_allocation,
)
from bedrock.transform.eeio.electricity_gtd_allocation import mecs_purchased_kwh
from bedrock.utils.config.usa_config import reset_usa_config, set_global_usa_config
from bedrock.utils.economic.inflation_helpers_cornerstone import (
    clear_cornerstone_inflation_caches,
)
from bedrock.utils.schemas.cornerstone_schemas import (
    CORNERSTONE_COMMODITIES_ELEC,
    CORNERSTONE_INDUSTRIES_ELEC,
)
from bedrock.utils.taxonomy.cornerstone.value_added import VALUE_ADDEDS
from bedrock.utils.validation.diagnostics_helpers import pull_efs_for_diagnostics

_CACHED_FUNCTIONS: list[Callable[..., object]] = [
    get_waste_disagg_weights,
    electricity_reallocation_enabled,
    electricity_disaggregation_enabled,
    derive_disagg_io_bundle,
    cornerstone_sector_disagg_active,
    derive_disagg_Ytot_with_trade,
    build_electricity_disagg_go_weights,
    build_electricity_disagg_use_intersection_weights,
    build_electricity_detail_GO_growth_ratios,
    applied_utilities_summary_q_growth_ratio,
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
    derive_cornerstone_B_non_finetuned,
]


def _clear_all_caches() -> None:
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


@contextmanager
def dollar_industrial_weights() -> Iterator[None]:
    """Force dollar Industrial weights so unmarked tests do not load Table 7.7."""
    import bedrock.transform.eeio.electricity_gtd_allocation as gtd  # noqa: PLC0415

    orig = gtd.allocate_purchaser_gtd

    def _wrapped(*args: object, **kwargs: object) -> object:
        kwargs = dict(kwargs)
        kwargs['industrial_weights'] = 'dollars'
        return orig(*args, **kwargs)

    with patch.object(gtd, 'allocate_purchaser_gtd', _wrapped):
        yield


def _setup_config(config_name: str) -> None:
    _clear_all_caches()
    reset_usa_config(should_reset_env_var=True)
    set_global_usa_config(config_name)


def _teardown() -> None:
    _clear_all_caches()
    reset_usa_config(should_reset_env_var=True)


@pytest.fixture
def electricity_disagg_config() -> str:
    return 'test_usa_config_waste_disagg_electricity_disaggregation.yaml'


class TestGoWeights:
    def test_go_weights_sum_to_one(self) -> None:
        _setup_config('test_usa_config_waste_disagg_electricity_disaggregation.yaml')
        try:
            w = build_electricity_disagg_go_weights()
            assert set(w.index) == set(ELECTRICITY_DISAGG_SECTORS)
            np.testing.assert_allclose(float(w.sum()), 1.0, rtol=1e-9, atol=1e-12)
        finally:
            _teardown()


class TestStep3WorkedExample:
    def test_column_balance_and_non_electricity_use_rows(self) -> None:
        """Per-column inputs+VA = x_s; non-electricity Use-row totals preserved.

        VA row totals across G/T/D need not match the old aggregate VA rows.
        """
        w = pd.Series({'221110': 0.34, '221121': 0.04, '221122': 0.62})
        codes = list(ELECTRICITY_DISAGG_SECTORS)
        agg = ELECTRICITY_AGGREGATE
        extra_rows = ['212100', '541000']
        rows = codes + [agg, *extra_rows, *VALUE_ADDEDS]
        cols = codes + [agg, *extra_rows]
        Udom = pd.DataFrame(0.0, index=rows, columns=cols)
        Uimp = pd.DataFrame(0.0, index=rows, columns=cols)
        Udom.at[agg, agg] = 100.0
        Udom.at['212100', agg] = 50.0
        Udom.at['541000', agg] = 40.0
        for code in codes:
            Udom.at[code, code] = 100.0 * float(w[code])
        Udom.at[agg, agg] = 0.0
        VA = pd.DataFrame(0.0, index=list(VALUE_ADDEDS), columns=[agg])
        VA.at['V00100', agg] = 70.0
        VA.at['V00200', agg] = 30.0
        VA.at['V00300', agg] = 60.0
        x_agg = 350.0

        Udom, Uimp, VA = disaggregate_use_industry_columns(x_agg, Udom, Uimp, VA, w)

        use_sub = pd.Series(
            {code: float(Udom[code].sum()) + float(Uimp[code].sum()) for code in codes}
        )
        np.testing.assert_allclose(
            _float_ndarray(use_sub.to_numpy()),
            np.array([97.6, 5.6, 86.8]),
            rtol=1e-9,
            atol=1e-6,
        )
        va_col_totals = VA[codes].sum(axis=0)
        np.testing.assert_allclose(
            _float_ndarray(va_col_totals.to_numpy()),
            np.array([21.4, 8.4, 130.2]),
            rtol=1e-9,
            atol=1e-6,
        )
        col_totals = use_sub + va_col_totals
        np.testing.assert_allclose(
            _float_ndarray(col_totals.to_numpy()),
            np.array([119.0, 14.0, 217.0]),
            rtol=1e-9,
            atol=1e-6,
        )
        for row, expected in (('212100', 50.0), ('541000', 40.0)):
            got = _loc_cols_sum(Udom, row, codes) + _loc_cols_sum(Uimp, row, codes)
            assert got == pytest.approx(expected, abs=1e-6)


@pytest.mark.eeio_integration
class TestElectricityDisaggregationPipeline:
    def test_schema_is_407_sectors(self, electricity_disagg_config: str) -> None:
        _setup_config(electricity_disagg_config)
        try:
            V = derive_cornerstone_V()
            assert list(V.index) == CORNERSTONE_INDUSTRIES_ELEC
            assert list(V.columns) == CORNERSTONE_COMMODITIES_ELEC
            assert ELECTRICITY_AGGREGATE not in V.index
        finally:
            _teardown()

    def test_make_and_use_balance(self, electricity_disagg_config: str) -> None:
        _setup_config(electricity_disagg_config)
        try:
            bundle = derive_disagg_io_bundle()
            V, Udom, Uimp, VA = bundle.V, bundle.Udom, bundle.Uimp, bundle.VA
            for frame in (V, Udom, Uimp, VA):
                assert ELECTRICITY_AGGREGATE not in frame.index
                assert ELECTRICITY_AGGREGATE not in frame.columns
        finally:
            _teardown()

    def test_pipeline_aq_and_diagnostics(self, electricity_disagg_config: str) -> None:
        _setup_config(electricity_disagg_config)
        try:
            aq = derive_cornerstone_Aq_scaled()
            assert aq.Adom.shape[0] == len(CORNERSTONE_COMMODITIES_ELEC)
            elec_col_sums = aq.Adom[ELECTRICITY_DISAGG_SECTORS].sum(axis=0)
            assert (elec_col_sums <= 1.0 + 1e-6).all()
            pull_efs_for_diagnostics()
        finally:
            _teardown()

    def test_e_attribution(self) -> None:
        _setup_config('2025_usa_cornerstone_v0_3_electricity_disaggregation.yaml')
        try:
            E = derive_E_usa()
            elec_cols = [c for c in ELECTRICITY_DISAGG_SECTORS if c in E.columns]
            assert len(elec_cols) == 3
            assert float(E[elec_cols].sum().sum()) > 0
        finally:
            _teardown()


@pytest.mark.eeio_integration
class TestGtdUseIntersection:
    def test_udom_diagonals_and_uimp_generation_only(
        self, electricity_disagg_config: str
    ) -> None:
        _setup_config(electricity_disagg_config)
        try:
            bundle = derive_disagg_io_bundle()
            udom, uimp = bundle.Udom, bundle.Uimp
            codes = list(ELECTRICITY_DISAGG_SECTORS)
            for i in codes:
                for j in codes:
                    if i != j:
                        assert _frame_cell_float(udom, i, j) == pytest.approx(
                            0.0, abs=1e-8
                        )
                        assert _frame_cell_float(uimp, i, j) == pytest.approx(
                            0.0, abs=1e-8
                        )
            leftover_udom = _frame_cell_float(udom, '221121', '221121') + (
                _frame_cell_float(udom, '221122', '221122')
            )
            assert leftover_udom >= -1e-6
            assert _frame_cell_float(uimp, '221121', '221121') == pytest.approx(
                0.0, abs=1e-8
            )
            assert _frame_cell_float(uimp, '221122', '221122') == pytest.approx(
                0.0, abs=1e-8
            )
            for col in uimp.columns:
                if col in codes:
                    continue
                assert _frame_cell_float(uimp, '221121', str(col)) == pytest.approx(
                    0.0, abs=1e-6
                )
                assert _frame_cell_float(uimp, '221122', str(col)) == pytest.approx(
                    0.0, abs=1e-6
                )
        finally:
            _teardown()


class Test2017EIAPurchaserAllocation:
    def test_getter_does_not_call_io_bundle(
        self, electricity_disagg_config: str
    ) -> None:
        from unittest import mock  # noqa: PLC0415

        _setup_config(electricity_disagg_config)
        try:
            with mock.patch(
                'bedrock.transform.eeio.cornerstone_disagg_pipeline.derive_disagg_io_bundle'
            ) as bundle_mock:
                with mock.patch(
                    'bedrock.transform.eeio.cornerstone_disagg_pipeline.derive_disagg_Ytot_with_trade'
                ) as y_mock:
                    get_2017_eia_purchaser_allocation.cache_clear()
                    _derive_post_reallocation_checkpoint_for_disagg.cache_clear()
                    with dollar_industrial_weights():
                        alloc = get_2017_eia_purchaser_allocation()
                    assert ELECTRICITY_AGGREGATE in alloc.bill.index
                    bundle_mock.assert_not_called()
                    y_mock.assert_not_called()
        finally:
            _teardown()


@pytest.mark.eeio_integration
class TestDetailGoGrowthScaling:
    def test_differentiated_child_q_scaling(
        self, electricity_disagg_config: str
    ) -> None:
        _setup_config(electricity_disagg_config)
        try:
            from bedrock.utils.config.usa_config import get_usa_config  # noqa: PLC0415

            cfg = get_usa_config()
            q_pre = derive_cornerstone_Aq().scaled_q.astype(float)
            q_scaled = scale_cornerstone_q(
                q_pre,
                target_year=int(cfg.usa_io_data_year),  # type: ignore[arg-type]
                original_year=int(cfg.usa_detail_original_year),  # type: ignore[arg-type]
            )
            ratios = build_electricity_detail_GO_growth_ratios(
                int(cfg.usa_detail_original_year), int(cfg.usa_io_data_year)
            )
            q_vals = [float(q_scaled[c]) for c in ELECTRICITY_DISAGG_SECTORS]
            assert len(set(round(v, 6) for v in q_vals)) == 3
            assert ratios['221110'] != pytest.approx(ratios['221121'])
        finally:
            _teardown()

    def test_apply_io_plus_elec_child_q_matches_detail_GO_growth(self) -> None:
        """Summary Utilities-22 growth must use the ITA-rebased ratio so net child growth is GO_i."""
        _setup_config('2025_usa_cornerstone_v0_3_electricity_disaggregation.yaml')
        try:
            from bedrock.utils.config.usa_config import get_usa_config  # noqa: PLC0415

            cfg = get_usa_config()
            assert cfg.apply_io_year_adjustments is True
            detail_year = int(cfg.usa_detail_original_year)
            io_year = int(cfg.usa_io_data_year)
            q_pre = derive_cornerstone_Aq().scaled_q.astype(float)
            q_scaled = scale_cornerstone_q(
                q_pre,
                target_year=io_year,  # type: ignore[arg-type]
                original_year=detail_year,  # type: ignore[arg-type]
            )
            go = build_electricity_detail_GO_growth_ratios(detail_year, io_year)
            for code in ELECTRICITY_DISAGG_SECTORS:
                implied = float(q_scaled.loc[code]) / float(q_pre.loc[code])
                assert implied == pytest.approx(float(go.loc[code]), rel=1e-9, abs=1e-9)
        finally:
            _teardown()

    def test_published_q_is_not_detail_go_growth_q(self) -> None:
        """Published q is rewritten after year-scale + PI, not left as GO-growth q."""
        _setup_config('2025_usa_cornerstone_v0_3_electricity_disaggregation.yaml')
        try:
            from bedrock.utils.config.usa_config import get_usa_config  # noqa: PLC0415
            from bedrock.utils.economic.inflation_helpers_cornerstone import (  # noqa: PLC0415
                inflate_cornerstone_q_or_y_with_commodity_pi,
            )

            cfg = get_usa_config()
            q_pre = derive_cornerstone_Aq().scaled_q.astype(float)
            q_year_scaled = scale_cornerstone_q(
                q_pre,
                target_year=int(cfg.usa_io_data_year),  # type: ignore[arg-type]
                original_year=int(cfg.usa_detail_original_year),  # type: ignore[arg-type]
            )
            q_year_scaled_pi = inflate_cornerstone_q_or_y_with_commodity_pi(
                q_year_scaled,
                original_year=int(cfg.usa_detail_original_year),
                target_year=int(cfg.model_base_year),
            )
            published = derive_cornerstone_Aq_scaled().scaled_q.astype(float)
            for code in ELECTRICITY_DISAGG_SECTORS:
                assert float(published.loc[code]) != pytest.approx(
                    float(q_year_scaled_pi.loc[code]), rel=1e-4, abs=1.0
                )
        finally:
            _teardown()


@pytest.mark.eeio_integration
class TestReanchoredAqIdentities:
    def test_adom_times_q_matches_allocated_udom_and_written_y(self) -> None:
        _setup_config('2025_usa_cornerstone_v0_3_electricity_disaggregation.yaml')
        try:
            from bedrock.transform.eeio.electricity_gtd_allocation import (  # noqa: PLC0415
                DISTRIBUTION_SECTOR,
                ELECTRICITY_AGGREGATE,
                GENERATION_SECTOR,
                IMPORT_FD_CODE,
                TRANSMISSION_SECTOR,
                get_reanchored_eia_purchaser_allocation,
            )
            from bedrock.utils.math.formulas import (  # noqa: PLC0415
                backcompute_y_from_A_and_q,
            )
            from bedrock.utils.taxonomy.cornerstone.final_demand import (  # noqa: PLC0415
                FINAL_DEMANDS,
            )

            aq = derive_cornerstone_Aq_scaled()
            alloc = get_reanchored_eia_purchaser_allocation()
            assert alloc is not None
            udom = aq.Adom.multiply(aq.scaled_q, axis=1)
            y = backcompute_y_from_A_and_q(A=aq.Adom, q=aq.scaled_q)
            g, t, d = GENERATION_SECTOR, TRANSMISSION_SECTOR, DISTRIBUTION_SECTOR
            for col in udom.columns:
                col_s = str(col)
                if col_s in ELECTRICITY_DISAGG_SECTORS:
                    continue
                if col_s not in alloc.bill.index:
                    continue
                assert _frame_cell_float(udom, g, col_s) == pytest.approx(
                    float(alloc.gen_dollars[col_s]), rel=1e-6, abs=1.0
                )
                assert _frame_cell_float(udom, t, col_s) == pytest.approx(
                    float(alloc.t_dollars[col_s]), rel=1e-6, abs=1.0
                )
                assert _frame_cell_float(udom, d, col_s) == pytest.approx(
                    float(alloc.d_dollars[col_s]), rel=1e-6, abs=1.0
                )
            gen_self = float(alloc.gen_dollars[ELECTRICITY_AGGREGATE])
            leftover = float(alloc.bill[ELECTRICITY_AGGREGATE]) - gen_self
            assert _frame_cell_float(udom, g, g) == pytest.approx(
                gen_self, rel=1e-6, abs=1.0
            )
            assert _frame_cell_float(udom, t, t) == pytest.approx(
                leftover * float(alloc.td_share), rel=1e-6, abs=1.0
            )
            assert _frame_cell_float(udom, d, d) == pytest.approx(
                leftover * (1.0 - float(alloc.td_share)), rel=1e-6, abs=1.0
            )
            for i in ELECTRICITY_DISAGG_SECTORS:
                for j in ELECTRICITY_DISAGG_SECTORS:
                    if i == j:
                        continue
                    assert _frame_cell_float(udom, i, j) == pytest.approx(0.0, abs=1e-6)
            fd_keys = [
                k
                for k in alloc.bill.index
                if k in set(FINAL_DEMANDS) and k != IMPORT_FD_CODE
            ]
            assert float(y.loc[g]) == pytest.approx(
                float(alloc.gen_dollars.reindex(fd_keys).fillna(0.0).sum()),
                rel=1e-6,
                abs=1.0,
            )
            assert float(y.loc[t]) == pytest.approx(
                float(alloc.t_dollars.reindex(fd_keys).fillna(0.0).sum()),
                rel=1e-6,
                abs=1.0,
            )
            assert float(y.loc[d]) == pytest.approx(
                float(alloc.d_dollars.reindex(fd_keys).fillna(0.0).sum()),
                rel=1e-6,
                abs=1.0,
            )
        finally:
            _teardown()


def test_f04000_mapped_to_exports() -> None:
    from bedrock.transform.eeio.electricity_end_use_mapping import (  # noqa: PLC0415
        build_end_use_map,
    )

    assert build_end_use_map()['F04000'] == 'Exports'


def test_egrid_mwh_for_io_year_2017() -> None:
    from bedrock.extract.disaggregation.egrid_generation import (  # noqa: PLC0415
        egrid_mwh_for_io_year,
    )

    got = egrid_mwh_for_io_year(2017)
    assert got == pytest.approx(4.038559e9, rel=1e-4)


@pytest.mark.eeio_integration
class Test2017PurchaserEiaIdentities:
    def test_generation_mwh_class_mix_exports_and_clip(
        self, electricity_disagg_config: str
    ) -> None:
        from bedrock.extract.disaggregation.egrid_generation import (  # noqa: PLC0415
            egrid_mwh_for_io_year,
            eia_table_2_2_end_use_mwh,
            eia_table_2_14_export_mwh,
        )
        from bedrock.transform.eeio.electricity_gtd_allocation import (  # noqa: PLC0415
            EXPORT_FD_CODE,
        )

        _setup_config(electricity_disagg_config)
        try:
            alloc = get_2017_eia_purchaser_allocation()
            egrid = egrid_mwh_for_io_year(2017)
            export = eia_table_2_14_export_mwh(2017)
            t22 = eia_table_2_2_end_use_mwh(2017)
            assert float(alloc.egrid_mwh) == pytest.approx(egrid, rel=1e-9)
            assert float(alloc.mwh.sum()) == pytest.approx(egrid, rel=1e-4, abs=1.0)
            assert float(alloc.mwh[EXPORT_FD_CODE]) == pytest.approx(
                export, rel=1e-4, abs=1.0
            )
            remaining = egrid - export
            teu = float(t22['Total End Use'])
            pools = {
                'Residential': float(t22['Residential']),
                'Commercial': float(t22['Commercial']),
                'Industrial': float(t22['Industrial']) + float(t22['Direct Use']),
                'Transportation': float(t22['Transportation']),
            }
            for cls, pool in pools.items():
                got = float(alloc.mwh[alloc.end_use_class == cls].sum())
                expected = remaining * (pool / teu)
                assert got == pytest.approx(expected, rel=1e-4, abs=1.0)
            assert float(alloc.gen_dollars.sum()) / float(alloc.p) == pytest.approx(
                float(alloc.mwh.sum()), rel=1e-6, abs=1.0
            )
        finally:
            _teardown()


def test_collapse_electricity_imports_onto_generation() -> None:
    from bedrock.transform.eeio.electricity_gtd_allocation import (  # noqa: PLC0415
        DISTRIBUTION_SECTOR,
        GENERATION_SECTOR,
        TRANSMISSION_SECTOR,
        collapse_electricity_imports_onto_generation,
    )

    imports = pd.Series(
        {
            GENERATION_SECTOR: 10.0,
            TRANSMISSION_SECTOR: 20.0,
            DISTRIBUTION_SECTOR: 30.0,
            '1111A0': 5.0,
        }
    )
    out = collapse_electricity_imports_onto_generation(imports)
    assert float(out[GENERATION_SECTOR]) == pytest.approx(60.0)
    assert float(out[TRANSMISSION_SECTOR]) == pytest.approx(0.0)
    assert float(out[DISTRIBUTION_SECTOR]) == pytest.approx(0.0)
    assert float(out['1111A0']) == pytest.approx(5.0)


def test_import_fd_column_uses_child_sum_when_aggregate_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from bedrock.transform.eeio.electricity_gtd_allocation import (  # noqa: PLC0415
        DISTRIBUTION_SECTOR,
        GENERATION_SECTOR,
        IMPORT_FD_CODE,
        TRANSMISSION_SECTOR,
        EIAPurchaserAllocation,
        apply_purchaser_allocation_to_y,
    )

    empty = pd.Series(dtype=float)
    alloc = EIAPurchaserAllocation(
        bill=empty,
        end_use_class=empty,
        mwh=empty,
        gen_dollars=empty,
        t_dollars=empty,
        d_dollars=empty,
        clipped=pd.Series(dtype=bool),
        p=1.0,
        egrid_mwh=1.0,
        td_share=0.06,
    )
    monkeypatch.setattr(
        'bedrock.transform.eeio.electricity_gtd_allocation.get_2017_eia_purchaser_allocation',
        lambda: alloc,
    )
    Y = pd.DataFrame(
        0.0,
        index=[GENERATION_SECTOR, TRANSMISSION_SECTOR, DISTRIBUTION_SECTOR, '1111A0'],
        columns=[IMPORT_FD_CODE, 'F01000'],
    )
    Y.at[GENERATION_SECTOR, IMPORT_FD_CODE] = 10.0
    Y.at[TRANSMISSION_SECTOR, IMPORT_FD_CODE] = 20.0
    Y.at[DISTRIBUTION_SECTOR, IMPORT_FD_CODE] = 30.0
    out = apply_purchaser_allocation_to_y(Y)
    assert _frame_cell_float(out, GENERATION_SECTOR, IMPORT_FD_CODE) == pytest.approx(
        60.0
    )
    assert _frame_cell_float(out, TRANSMISSION_SECTOR, IMPORT_FD_CODE) == pytest.approx(
        0.0
    )
    assert _frame_cell_float(out, DISTRIBUTION_SECTOR, IMPORT_FD_CODE) == pytest.approx(
        0.0
    )
