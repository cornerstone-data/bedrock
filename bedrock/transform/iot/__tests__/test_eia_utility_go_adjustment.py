"""Unit tests for the EIA two-leg utility gross-output control (#1009).

The leg arithmetic takes a frame, so these are synthetic and need no extract:
the two EIA loaders and the two index helpers are monkeypatched. The live
reconciliation against EIA's published average price is ``--check`` on the
module rather than a test, matching how the other diagnostics here are graded.
"""

import pandas as pd
import pytest

from bedrock.extract.disaggregation import egrid_generation
from bedrock.transform.iot import eia_utility_go_adjustment as ug
from bedrock.utils.config.usa_config import get_usa_config

#: 2017 electric gross output totals 500 in the synthetic panel, so a retail
#: revenue of 400 leaves a wholesale wedge of 100 that splits 80/10/10.
RETAIL_USD_2017 = 400.0 * 1e6

RETAIL_INDEX = {2017: 1.0, 2018: 1.1, 2022: 2.0}
WHOLESALE_INDEX = {2017: 1.0, 2018: 1.2, 2022: 0.5}


def _panel() -> pd.DataFrame:
    return pd.DataFrame(
        {
            2017: [400.0, 50.0, 50.0, 70.0, 900.0],
            2018: [440.0, 55.0, 52.0, 77.0, 950.0],
            2022: [600.0, 80.0, 70.0, 124.0, 1100.0],
        },
        index=pd.Index(
            ['221100', 'S00101', 'S00202', '221200', '111200'], name='industry'
        ),
    )


@pytest.fixture(autouse=True)
def _stub_eia(monkeypatch: pytest.MonkeyPatch) -> None:
    """Retail doubles by 2022, wholesale halves; both are 1.0 at the base year."""
    monkeypatch.setattr(
        egrid_generation, 'eia_retail_revenue_usd', lambda year: RETAIL_USD_2017
    )
    monkeypatch.setattr(ug, 'retail_index', lambda year: RETAIL_INDEX[int(year)])
    monkeypatch.setattr(ug, 'wholesale_index', lambda year: WHOLESALE_INDEX[int(year)])


def test_the_legs_sum_back_to_published_gross_output_at_the_base_year() -> None:
    """The split is a partition, so nothing is created or lost at 2017."""
    panel = _panel()
    legs = ug.base_year_legs(panel)
    total = legs['retail'] + legs['wholesale']
    for code in legs.index:
        assert total[code] == pytest.approx(panel.at[code, ug.BASE_YEAR])


def test_the_wedge_splits_on_published_gross_output_share() -> None:
    """221100 is 80% of electric output, so it carries 80% of the 100 wedge."""
    legs = ug.base_year_legs(_panel())
    assert legs.at['221100', 'wholesale'] == pytest.approx(80.0)
    assert legs.at['S00101', 'wholesale'] == pytest.approx(10.0)
    assert legs.at['S00202', 'wholesale'] == pytest.approx(10.0)
    assert legs.at['221100', 'retail'] == pytest.approx(320.0)


def test_retail_revenue_above_gross_output_raises() -> None:
    """A non-positive wedge means the scope mapping is wrong, not that it is zero."""
    panel = _panel()
    panel.loc[['221100', 'S00101', 'S00202'], ug.BASE_YEAR] = [100.0, 10.0, 10.0]
    with pytest.raises(ValueError, match='wholesale wedge is non-positive'):
        ug.base_year_legs(panel)


def test_a_panel_without_any_electric_industry_raises() -> None:
    panel = _panel().drop(index=['221100', 'S00101', 'S00202'])
    with pytest.raises(KeyError, match='gross-output panel'):
        ug.base_year_legs(panel)


def test_the_base_year_is_returned_unchanged() -> None:
    """Both indices are 1.0 at 2017; the observed benchmark outranks the index."""
    panel = _panel()
    adjusted = ug.apply_eia_utility_adjustment(panel)
    pd.testing.assert_series_equal(
        adjusted[ug.BASE_YEAR], panel[ug.BASE_YEAR], check_names=False
    )


def test_only_controlled_industries_move() -> None:
    """Gas, government electric and agriculture come back bit-identical."""
    panel = _panel()
    adjusted = ug.apply_eia_utility_adjustment(panel)
    for code in ('221200', 'S00101', 'S00202', '111200'):
        pd.testing.assert_series_equal(
            adjusted.T[code], panel.T[code], check_names=False
        )
    assert adjusted.at['221100', 2022] != panel.at['221100', 2022]


def test_the_two_legs_move_on_their_own_index() -> None:
    """2022: retail 320 x 2.0 plus wholesale 80 x 0.5 = 680."""
    panel = _panel()
    adjusted = ug.apply_eia_utility_adjustment(panel)
    assert adjusted.at['221100', 2022] == pytest.approx(320.0 * 2.0 + 80.0 * 0.5)
    assert adjusted.at['221100', 2018] == pytest.approx(320.0 * 1.1 + 80.0 * 1.2)


def test_a_single_retail_index_would_give_a_different_answer() -> None:
    """The second leg is load-bearing, not decoration.

    A one-index control would put 2022 at ``400 x 2.0 = 800``. Measured on the
    real panel, that difference is what drives ``T005`` below what
    investor-owned utilities alone report spending on fuel and purchased power.
    """
    adjusted = ug.apply_eia_utility_adjustment(_panel())
    assert adjusted.at['221100', 2022] != pytest.approx(400.0 * 2.0)


def test_uncontrolled_electric_industries_are_still_reported() -> None:
    """``--check`` has to be able to grade the rows the control holds out."""
    rebased = ug.two_leg_gross_output(_panel(), 2022)
    for code in ug.ELECTRIC_INDUSTRIES:
        assert code in rebased.index


def test_the_argument_is_not_mutated() -> None:
    panel = _panel()
    before = panel.copy()
    ug.apply_eia_utility_adjustment(panel)
    pd.testing.assert_frame_equal(panel, before)


def test_a_year_outside_the_controlled_span_raises() -> None:
    with pytest.raises(ValueError, match='outside the controlled span'):
        ug.two_leg_gross_output(_panel(), max(ug.CONTROLLED_YEARS) + 1)


def test_an_industry_awaiting_a_source_is_never_controlled() -> None:
    """Gas is specified but unpopulated; it must not be silently rebased."""
    assert '221200' in ug.HELD_OUT
    assert '221200' not in ug.CONTROLLED


def test_government_electric_is_held_out_of_the_control() -> None:
    """The wedge is split on output share, which is wrong for federal power.

    ``S00101`` is largely Bonneville and TVA, which sell mostly at wholesale,
    so its true share of the resale wedge is far above its 3.4% share of
    output. It is reported so ``--check`` can show what the control *would* do,
    and excluded until resale by ownership class is available.
    """
    for code in ug.GOVERNMENT_ELECTRIC:
        assert code not in ug.CONTROLLED
        assert code in ug.ELECTRIC_INDUSTRIES


def test_direct_use_is_excluded_from_the_sales_denominator() -> None:
    """Self-generated electricity is never sold, so it has no revenue behind it."""
    assert 'Direct Use' not in ug.RETAIL_SALES_CLASSES
    assert 'Total End Use' not in ug.RETAIL_SALES_CLASSES


# --- all electric output on retail revenue ---------------------------------


@pytest.fixture
def _retail_basis(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        get_usa_config(), 'electric_output_on_retail_revenue', True, raising=True
    )


def test_two_legs_and_one_industry_stay_the_default() -> None:
    assert get_usa_config().electric_output_on_retail_revenue is False
    assert ug.controlled_industries() == ug.CONTROLLED


@pytest.mark.usefixtures('_retail_basis')
def test_all_three_electric_industries_follow_the_retail_index() -> None:
    """Identical trends: each is its 2017 value times retail, 2.0 by 2022."""
    panel = _panel()
    adjusted = ug.apply_eia_utility_adjustment(panel)
    assert set(ug.controlled_industries()) == set(ug.ELECTRIC_INDUSTRIES)
    for code, base in {'221100': 400.0, 'S00101': 50.0, 'S00202': 50.0}.items():
        assert adjusted.at[code, 2017] == pytest.approx(base)
        assert adjusted.at[code, 2018] == pytest.approx(base * 1.1)
        assert adjusted.at[code, 2022] == pytest.approx(base * 2.0)
    for code in ('221200', '111200'):
        pd.testing.assert_series_equal(adjusted.loc[code], panel.loc[code])


@pytest.mark.usefixtures('_retail_basis')
def test_real_output_follows_kwh_on_the_retail_price(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Revenue over revenue per kWh is kWh, so the total tracks physical sales."""
    kwh = {2017: 100.0, 2018: 103.0, 2022: 106.0}
    monkeypatch.setattr(ug, 'retail_sales_mwh', lambda year: kwh[int(year)])
    adjusted = ug.apply_eia_utility_adjustment(_panel())
    total = adjusted.loc[list(ug.ELECTRIC_INDUSTRIES)].sum()
    for year in (2018, 2022):
        price = ug.retail_price_index(year) / ug.retail_price_index(2017)
        assert total[year] / price / total[2017] == pytest.approx(kwh[year] / 100.0)


def test_retail_output_outside_the_span_raises() -> None:
    with pytest.raises(ValueError, match='outside the controlled span'):
        ug.retail_gross_output(_panel(), max(ug.CONTROLLED_YEARS) + 1)


def test_the_retail_basis_needs_the_rebase_on() -> None:
    with pytest.raises(ValueError, match='requires'):
        get_usa_config().model_validate(
            {
                **get_usa_config().model_dump(),
                'rebase_utility_gross_output_on_eia': False,
                'electric_output_on_retail_revenue': True,
            }
        )


@pytest.mark.usefixtures('_retail_basis')
def test_price_lines_follow_eia_and_keep_their_seams_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """On EIA 2017-2024; BEA's change after; other sectors and the input untouched."""
    from bedrock.utils.economic import inflation_helpers_usa as helpers  # noqa: PLC0415

    span = {year: 1.0 + 0.1 * (year - 2017) for year in ug.CONTROLLED_YEARS}
    monkeypatch.setattr(ug, 'retail_price_index', lambda year: span[int(year)])
    years = [2016, *ug.CONTROLLED_YEARS, 2025]
    levels = pd.DataFrame(
        {year: [100.0, 80.0, 50.0] for year in years},
        index=['221100', 'S00101', '111200'],
        dtype=float,
    )
    levels.loc['221100', 2024] = 200.0
    levels.loc['221100', 2025] = 220.0
    levels.loc['221100', 2016] = 90.0
    original = levels.copy()

    out = helpers._electric_lines_on_eia_retail_price(levels)

    pd.testing.assert_frame_equal(levels, original)
    assert out.at['221100', 2017] == pytest.approx(100.0)
    assert out.at['221100', 2022] == pytest.approx(150.0)
    assert out.at['S00101', 2022] == pytest.approx(120.0)
    assert out.at['221100', 2024] == pytest.approx(170.0)
    # BEA's +10% from 2024 to 2025 on the new 2024 level; 2016 as published.
    assert out.at['221100', 2025] == pytest.approx(187.0)
    assert out.at['221100', 2016] == pytest.approx(90.0)
    pd.testing.assert_series_equal(out.loc['111200'], original.loc['111200'])
