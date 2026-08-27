"""Unit tests for MECS Table 7.7 Industrial manufacturing MWh shares."""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest

from bedrock.publish.cache_reset import _clear_electricity_caches_if_loaded
from bedrock.transform.allocation.mappings.v7.ceda_mecs import NON_MECS_INDUSTRIES
from bedrock.transform.eeio.electricity_end_use_mapping import build_end_use_map
from bedrock.transform.eeio.electricity_gtd_allocation import (
    ELECTRICITY_AGGREGATE,
    EXPORT_FD_CODE,
    MECS_7_7_NAICS_OVERLAY,
    TABLE_7_7_DESCRIPTION,
    TABLE_7_7_ELECTRICITY_TOTAL,
    EIAPurchaserAllocation,
    _dedupe_overlay,
    _fill_three_digit_suppressed,
    _mecs_purchased_kwh_cached,
    _overlaid_3_1_mapping,
    _overlaid_3_1_subtraction,
    _overlay_naics_tuple,
    _required_7_7_naics,
    allocate_purchaser_gtd,
    industrial_manufacturing_pool,
    io_manufacturing_purchased_kwh,
    mecs_purchased_kwh,
    mecs_year_for_eia_year,
)
from bedrock.utils.mapping.location import US_FIPS
from bedrock.utils.schemas.cornerstone_schemas import ELECTRICITY_DISAGG_SECTORS

_MFG_A = '331110'
_MFG_B = '327310'
_RES_AG = '1111A0'
_COMMERCIAL = '452000'
_RESIDENTIAL_FD = 'F01000'


def _fake_7_7_frame(
    *,
    amounts_mkwh: dict[str, float] | None = None,
    suppressed: dict[str, str | float | None] | None = None,
    description: str = TABLE_7_7_DESCRIPTION,
    flow_name: str = TABLE_7_7_ELECTRICITY_TOTAL,
) -> pd.DataFrame:
    needed = sorted(_required_7_7_naics() | {'31-33'})
    three = [c for c in needed if len(c) == 3 and c.isdigit()]
    amt = {c: 1.0 for c in needed}
    amt['31-33'] = float(sum(amt[c] for c in three))
    if amounts_mkwh:
        amt.update(amounts_mkwh)
    supp: dict[str, str | float | None] = {c: None for c in needed}
    if suppressed:
        supp.update(suppressed)
    return pd.DataFrame(
        {
            'Description': description,
            'Location': US_FIPS,
            'FlowName': flow_name,
            'ActivityConsumedBy': needed,
            'FlowAmount': [amt[c] for c in needed],
            'Suppressed': [supp[c] for c in needed],
        }
    )


def _allocate(
    bills: pd.Series,
    *,
    industrial_weights: str,
    targets: dict[str, float] | None = None,
    egrid: float = 1_000.0,
    p_share: float = 0.1,
    td_share: float = 0.3,
    eia_year: int = 2017,
) -> EIAPurchaserAllocation:
    if targets is None:
        targets = {
            'Residential': 200.0,
            'Commercial': 200.0,
            'Industrial': 400.0,
            'Transportation': 0.0,
            'Exports': 200.0,
        }
    with (
        patch(
            'bedrock.transform.eeio.electricity_gtd_allocation.egrid_mwh_for_io_year',
            return_value=egrid,
        ),
        patch(
            'bedrock.transform.eeio.electricity_gtd_allocation._class_mwh_targets',
            return_value=targets,
        ),
    ):
        return allocate_purchaser_gtd(
            bills,
            self_use_key=ELECTRICITY_AGGREGATE,
            eia_year=eia_year,
            p_share_2017=p_share,
            td_share_2017=td_share,
            industrial_weights=industrial_weights,  # type: ignore[arg-type]
        )


def test_mecs_year_for_eia_year() -> None:
    assert mecs_year_for_eia_year(2017) == 2018
    assert mecs_year_for_eia_year(2016) == 2018
    assert mecs_year_for_eia_year(2018) == 2022
    assert mecs_year_for_eia_year(2024) == 2022


def test_overlay_dedupes_paper_and_auto_naics() -> None:
    assert _dedupe_overlay(('322121', '322122')) == ('322120',)
    assert _dedupe_overlay(('336111', '336112')) == ('336110',)
    for src, dst in MECS_7_7_NAICS_OVERLAY.items():
        assert _dedupe_overlay((src,)) == (dst,)
    assert _overlay_naics_tuple(('322121', '322122'), {'322120', '322'}) == ('322120',)
    assert _overlay_naics_tuple(('322121', '322122'), {'322121', '322122', '322'}) == (
        '322121',
        '322122',
    )


def test_mapping_and_subtraction_io_keys_are_disjoint() -> None:
    available = set(_required_7_7_naics())
    mapped = {str(c) for k in _overlaid_3_1_mapping(available) for c in k}
    subtracted = {str(c) for k in _overlaid_3_1_subtraction(available) for c in k}
    assert not (mapped & subtracted)


def test_manufacturing_pool_is_io_keys_not_naics_values() -> None:
    pool = industrial_manufacturing_pool()
    assert _MFG_A in pool
    assert _MFG_B in pool
    assert '31-33' not in pool
    assert '322121' not in pool
    assert '322120' in pool


def test_non_mecs_industrial_codes_land_in_residual() -> None:
    mfg = industrial_manufacturing_pool()
    end_use = build_end_use_map()
    industrial = {c for c, cls in end_use.items() if cls == 'Industrial'}
    industrial.add(ELECTRICITY_AGGREGATE)
    residual = industrial - mfg
    for code in NON_MECS_INDUSTRIES:
        if code in industrial:
            assert code in residual
            assert code not in mfg


def test_manufacturing_pool_matches_ghg_combustion_mecs_io_keys() -> None:
    """Same Cornerstone 3.1 IO keys as GHG industrial coal/gas combustion."""
    from bedrock.transform.allocation.co2.industrial_coal import (  # noqa: PLC0415
        _get_mecs_3_1_naics_mappings as coal_maps,
    )
    from bedrock.transform.allocation.co2.industrial_natural_gas import (  # noqa: PLC0415
        _get_mecs_3_1_naics_mappings as gas_maps,
    )
    from bedrock.transform.allocation.utils import flatten_items  # noqa: PLC0415

    coal_map, coal_sub = coal_maps()
    gas_map, gas_sub = gas_maps()
    assert coal_map is gas_map
    assert coal_sub is gas_sub
    ghg_mfg = {str(c) for c in flatten_items(coal_map.keys())}
    ghg_mfg.update(str(c) for c in flatten_items(coal_sub.keys()))
    assert industrial_manufacturing_pool() == frozenset(ghg_mfg)
    assert not (industrial_manufacturing_pool() & set(NON_MECS_INDUSTRIES))


def test_self_use_industrial_fd_and_gtd_children_are_residual() -> None:
    mfg = industrial_manufacturing_pool()
    for code in (ELECTRICITY_AGGREGATE, 'F02E00', *ELECTRICITY_DISAGG_SECTORS):
        assert code not in mfg


def test_fill_three_digit_assigns_single_qd_residual() -> None:
    kwh = pd.Series(
        {
            '31-33': 100.0e6,
            '311': 40.0e6,
            '331': 50.0e6,
            '337': float('nan'),
        }
    )
    suppressed = pd.Series({'31-33': None, '311': None, '331': None, '337': 'Q'})
    out = _fill_three_digit_suppressed(kwh, suppressed)
    assert float(out['337']) == pytest.approx(10.0e6)


def test_fill_three_digit_treats_zero_flowamount_qd_as_suppressed() -> None:
    kwh = pd.Series(
        {
            '31-33': 100.0e6,
            '311': 40.0e6,
            '331': 50.0e6,
            '337': 0.0,
        }
    )
    suppressed = pd.Series({'31-33': None, '311': None, '331': None, '337': 'Q'})
    out = _fill_three_digit_suppressed(kwh, suppressed)
    assert float(out['337']) == pytest.approx(10.0e6)


def test_fill_three_digit_ok_when_residual_near_zero() -> None:
    kwh = pd.Series({'31-33': 100.0e6, '311': 60.0e6, '331': 40.0e6})
    suppressed = pd.Series({'31-33': None, '311': None, '331': None})
    out = _fill_three_digit_suppressed(kwh, suppressed)
    assert float(out['311']) == pytest.approx(60.0e6)
    assert float(out.sum()) == pytest.approx(200.0e6)


def test_fill_three_digit_hard_error_if_leftover_and_not_one_qd() -> None:
    kwh = pd.Series(
        {
            '31-33': 100.0e6,
            '311': 10.0e6,
            '331': float('nan'),
            '337': float('nan'),
        }
    )
    suppressed = pd.Series({'31-33': None, '311': None, '331': 'Q', '337': 'D'})
    with pytest.raises(ValueError, match='3-digit residual'):
        _fill_three_digit_suppressed(kwh, suppressed)
    kwh_none = pd.Series({'31-33': 100.0e6, '311': 10.0e6})
    suppressed_none = pd.Series({'31-33': None, '311': None})
    with pytest.raises(ValueError, match='3-digit residual'):
        _fill_three_digit_suppressed(kwh_none, suppressed_none)


def test_fill_three_digit_zeros_remaining_non_three_digit_qd() -> None:
    kwh = pd.Series(
        {
            '31-33': 100.0e6,
            '311': 100.0e6,
            '331110': float('nan'),
        }
    )
    suppressed = pd.Series({'31-33': None, '311': None, '331110': 'Q'})
    out = _fill_three_digit_suppressed(kwh, suppressed)
    assert float(out['331110']) == pytest.approx(0.0)


def test_empty_us_electricity_total_hard_errors() -> None:
    _mecs_purchased_kwh_cached.cache_clear()
    empty = pd.DataFrame(
        {
            'Description': ['Table 3.1'],
            'Location': [US_FIPS],
            'FlowName': ['Net Electricity'],
            'ActivityConsumedBy': ['311'],
            'FlowAmount': [1.0],
            'Suppressed': [None],
        }
    )
    with patch(
        'bedrock.extract.flowbyactivity.getFlowByActivity',
        return_value=empty,
    ):
        with pytest.raises(ValueError, match='no US Table 7.7'):
            mecs_purchased_kwh(2018)
    _mecs_purchased_kwh_cached.cache_clear()


def test_star_is_zero_and_missing_mapped_naics_hard_errors() -> None:
    _mecs_purchased_kwh_cached.cache_clear()
    frame = _fake_7_7_frame()
    frame = frame.loc[frame['ActivityConsumedBy'] != '331110'].reset_index(drop=True)
    with patch(
        'bedrock.extract.flowbyactivity.getFlowByActivity',
        return_value=frame,
    ):
        with pytest.raises(ValueError, match='missing NAICS'):
            mecs_purchased_kwh(2018)
    _mecs_purchased_kwh_cached.cache_clear()


def test_io_kwh_sum_excludes_manufacturing_total_row() -> None:
    _mecs_purchased_kwh_cached.cache_clear()
    frame = _fake_7_7_frame()
    bills = pd.Series(1.0, index=list(industrial_manufacturing_pool()), dtype=float)
    with patch(
        'bedrock.extract.flowbyactivity.getFlowByActivity',
        return_value=frame,
    ):
        naics = mecs_purchased_kwh(2018)
        io_kwh = io_manufacturing_purchased_kwh(bills, 2018)
    assert '31-33' not in io_kwh.index
    assert float(io_kwh.sum()) != pytest.approx(float(naics.sum()))
    _mecs_purchased_kwh_cached.cache_clear()


def test_one_to_one_zero_bill_still_gets_kwh() -> None:
    _mecs_purchased_kwh_cached.cache_clear()
    frame = _fake_7_7_frame()
    pool = list(industrial_manufacturing_pool())
    bills = pd.Series(1.0, index=pool, dtype=float)
    bills[_MFG_A] = 0.0
    with patch(
        'bedrock.extract.flowbyactivity.getFlowByActivity',
        return_value=frame,
    ):
        io_kwh = io_manufacturing_purchased_kwh(bills, 2018)
    assert _MFG_A in io_kwh.index
    assert float(io_kwh[_MFG_A]) > 0.0
    _mecs_purchased_kwh_cached.cache_clear()


def test_many_to_one_zero_bill_member_gets_zero_kwh() -> None:
    _mecs_purchased_kwh_cached.cache_clear()
    naics_to_io: dict[str, list[str]] = {}
    for io_key, naics_vals in _overlaid_3_1_mapping(set(_required_7_7_naics())).items():
        for n in naics_vals:
            bucket = naics_to_io.setdefault(n, [])
            for io in io_key:
                if str(io) not in bucket:
                    bucket.append(str(io))
    shared = next((ios for ios in naics_to_io.values() if len(ios) >= 2), None)
    if shared is None:
        pytest.skip('no many:1 MECS NAICS in the overlay mapping')
    frame = _fake_7_7_frame()
    pool = list(industrial_manufacturing_pool())
    bills = pd.Series(1.0, index=pool, dtype=float)
    bills[shared[0]] = 0.0
    with patch(
        'bedrock.extract.flowbyactivity.getFlowByActivity',
        return_value=frame,
    ):
        io_kwh = io_manufacturing_purchased_kwh(bills, 2018)
    assert float(io_kwh.get(shared[0], 0.0)) == pytest.approx(0.0)
    assert float(sum(float(io_kwh.get(c, 0.0)) for c in shared[1:])) > 0.0
    _mecs_purchased_kwh_cached.cache_clear()


def test_two_pool_identities_and_class_total() -> None:
    bills = pd.Series(
        {
            _MFG_A: 600.0,
            _MFG_B: 200.0,
            _RES_AG: 200.0,
            ELECTRICITY_AGGREGATE: 100.0,
            _RESIDENTIAL_FD: 2_000.0,
            _COMMERCIAL: 2_000.0,
            EXPORT_FD_CODE: 2_000.0,
        },
        dtype=float,
    )
    io_kwh = pd.Series({_MFG_A: 3.0, _MFG_B: 1.0}, dtype=float)
    industrial_t = 400.0
    with patch(
        'bedrock.transform.eeio.electricity_gtd_allocation.io_manufacturing_purchased_kwh',
        return_value=io_kwh,
    ):
        mecs = _allocate(bills, industrial_weights='mecs')
        dollars = _allocate(bills, industrial_weights='dollars')
    industrial = mecs.end_use_class == 'Industrial'
    assert float(mecs.mwh[industrial].sum()) == pytest.approx(
        float(dollars.mwh[industrial].sum())
    )
    assert float(mecs.mwh[industrial].sum()) == pytest.approx(industrial_t)
    mfg_bills = 600.0 + 200.0
    ind_bills = mfg_bills + 200.0 + 100.0
    pool_mfg = industrial_t * mfg_bills / ind_bills
    pool_res = industrial_t - pool_mfg
    assert float(mecs.mwh[_MFG_A]) == pytest.approx(pool_mfg * 3.0 / 4.0)
    assert float(mecs.mwh[_MFG_B]) == pytest.approx(pool_mfg * 1.0 / 4.0)
    assert float(mecs.mwh[_RES_AG]) == pytest.approx(pool_res * 200.0 / 300.0)
    assert float(mecs.mwh[ELECTRICITY_AGGREGATE]) == pytest.approx(
        pool_res * 100.0 / 300.0
    )
    assert float(mecs.mwh[_RESIDENTIAL_FD]) == pytest.approx(
        float(dollars.mwh[_RESIDENTIAL_FD])
    )
    assert float(mecs.mwh[_COMMERCIAL]) == pytest.approx(
        float(dollars.mwh[_COMMERCIAL])
    )
    assert float(mecs.mwh[EXPORT_FD_CODE]) == pytest.approx(
        float(dollars.mwh[EXPORT_FD_CODE])
    )


def test_dollars_path_does_not_load_mecs() -> None:
    bills = pd.Series(
        {
            _MFG_A: 100.0,
            _RES_AG: 50.0,
            _RESIDENTIAL_FD: 200.0,
            _COMMERCIAL: 200.0,
            EXPORT_FD_CODE: 200.0,
        },
        dtype=float,
    )
    with patch(
        'bedrock.transform.eeio.electricity_gtd_allocation.mecs_purchased_kwh'
    ) as mecs_mock:
        _allocate(bills, industrial_weights='dollars')
        mecs_mock.assert_not_called()


def test_manufacturing_kwh_sum_zero_falls_back_to_dollars_inside_mfg() -> None:
    bills = pd.Series(
        {
            _MFG_A: 80.0,
            _MFG_B: 20.0,
            _RES_AG: 100.0,
            _RESIDENTIAL_FD: 1_000.0,
            _COMMERCIAL: 1_000.0,
            EXPORT_FD_CODE: 1_000.0,
        },
        dtype=float,
    )
    with patch(
        'bedrock.transform.eeio.electricity_gtd_allocation.io_manufacturing_purchased_kwh',
        return_value=pd.Series({_MFG_A: 0.0, _MFG_B: 0.0}, dtype=float),
    ):
        mecs = _allocate(bills, industrial_weights='mecs')
        dollars = _allocate(bills, industrial_weights='dollars')
    assert float(mecs.mwh[_MFG_A]) == pytest.approx(float(dollars.mwh[_MFG_A]))
    assert float(mecs.mwh[_MFG_B]) == pytest.approx(float(dollars.mwh[_MFG_B]))
    assert float(mecs.mwh[_RES_AG]) == pytest.approx(float(dollars.mwh[_RES_AG]))


def test_zero_residual_bills_assign_industrial_mwh_to_manufacturing() -> None:
    bills = pd.Series(
        {
            _MFG_A: 200.0,
            _RES_AG: -10.0,
            _RESIDENTIAL_FD: 1_000.0,
            _COMMERCIAL: 1_000.0,
            EXPORT_FD_CODE: 1_000.0,
        },
        dtype=float,
    )
    with patch(
        'bedrock.transform.eeio.electricity_gtd_allocation.io_manufacturing_purchased_kwh',
        return_value=pd.Series({_MFG_A: 5.0}, dtype=float),
    ):
        mecs = _allocate(bills, industrial_weights='mecs')
    industrial = mecs.end_use_class == 'Industrial'
    assert float(mecs.mwh[industrial].sum()) == pytest.approx(400.0)
    assert float(mecs.mwh[_MFG_A]) == pytest.approx(400.0)
    assert float(mecs.mwh[_RES_AG]) == pytest.approx(0.0)


def test_industrial_clip0_sum_zero_hard_errors() -> None:
    bills = pd.Series(
        {
            _MFG_A: -5.0,
            _RES_AG: 0.0,
            _RESIDENTIAL_FD: 1_000.0,
            _COMMERCIAL: 1_000.0,
            EXPORT_FD_CODE: 1_000.0,
        },
        dtype=float,
    )
    with pytest.raises(ValueError, match='cannot split'):
        _allocate(bills, industrial_weights='mecs')


def test_mecs_purchased_kwh_returns_copy() -> None:
    _mecs_purchased_kwh_cached.cache_clear()
    frame = _fake_7_7_frame()
    with patch(
        'bedrock.extract.flowbyactivity.getFlowByActivity',
        return_value=frame,
    ):
        first = mecs_purchased_kwh(2018)
        first.iloc[0] = -1.0
        second = mecs_purchased_kwh(2018)
    assert float(second.iloc[0]) != pytest.approx(-1.0)
    _mecs_purchased_kwh_cached.cache_clear()


def test_cache_reset_clears_mecs_purchased_kwh() -> None:
    _mecs_purchased_kwh_cached.cache_clear()
    frame = _fake_7_7_frame()
    with patch(
        'bedrock.extract.flowbyactivity.getFlowByActivity',
        return_value=frame,
    ):
        mecs_purchased_kwh(2018)
    assert _mecs_purchased_kwh_cached.cache_info().currsize == 1
    _clear_electricity_caches_if_loaded()
    assert _mecs_purchased_kwh_cached.cache_info().currsize == 0


@pytest.mark.eeio_integration
@pytest.mark.parametrize('year', [2018, 2022])
def test_post_overlay_naics_exist_in_table_7_7(year: int) -> None:
    _mecs_purchased_kwh_cached.cache_clear()
    kwh = mecs_purchased_kwh(year)
    _required_7_7_naics(set(kwh.index.astype(str)))
    leftover = float(kwh.loc['31-33']) - float(
        kwh.loc[[c for c in kwh.index if len(str(c)) == 3 and str(c).isdigit()]].sum()
    )
    if year == 2018:
        assert abs(leftover) <= 1.0e6
    if year == 2022:
        assert float(kwh.loc['337']) > 0.0
    _mecs_purchased_kwh_cached.cache_clear()
