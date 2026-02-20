"""Unit and integration tests for gross output redefinition adjustment logic."""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.transform.iot.derived_gross_industry_output import (
    adjust_gross_output,
    compute_coproduction_ratios,
    extract_coproduction_entries,
)


def _make_V(data: list[list[float]], codes: list[str]) -> pd.DataFrame:
    """Helper to build a small Make table (industry x commodity)."""
    return pd.DataFrame(data, index=pd.Index(codes), columns=pd.Index(codes))


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------


class TestExtractCoproductionEntries:
    def test_basic_3x3(self) -> None:
        """Off-diagonal nonzero entries are returned; diagonal and zeros are excluded."""
        V = _make_V(
            [[100, 10, 0], [5, 200, 0], [0, 0, 300]],
            ['A', 'B', 'C'],
        )
        result = extract_coproduction_entries(V)

        assert set(result.columns) == {'source_industry', 'commodity', 'value'}
        assert len(result) == 2
        entries = set(zip(result['source_industry'], result['commodity']))
        assert entries == {('A', 'B'), ('B', 'A')}
        assert (
            result.loc[
                (result['source_industry'] == 'A') & (result['commodity'] == 'B'),
                'value',
            ].iloc[0]
            == 10
        )


class TestComputeCoproductionRatios:
    def test_known_ratios(self) -> None:
        """Ratios match hand computation: value / row total."""
        V = _make_V([[800, 200], [50, 950]], ['A', 'B'])
        V_after = _make_V([[1000, 0], [0, 1000]], ['A', 'B'])
        ratios = compute_coproduction_ratios(V, V_after)

        a_to_b = ratios[ratios['source_industry'] == 'A']
        assert a_to_b['ratio'].iloc[0] == pytest.approx(200 / 1000)

        b_to_a = ratios[ratios['source_industry'] == 'B']
        assert b_to_a['ratio'].iloc[0] == pytest.approx(50 / 1000)


class TestAdjustGrossOutput:
    def test_bidirectional_coproduction(self) -> None:
        """
        Both industries co-produce each other's commodity.

        A: 900 of A, 100 of B -> g=1000, ratio A->B = 0.10
        B: 50 of A,  450 of B -> g=500,  ratio B->A = 0.10

        Target year: A=2000, B=1000
        After:  A = 2000 - 200 + 100 = 1900
                B = 1000 + 200 - 100 = 1100
        """
        V = _make_V([[900, 100], [50, 450]], ['A', 'B'])
        V_after = _make_V([[1000, 0], [0, 500]], ['A', 'B'])
        ratios = compute_coproduction_ratios(V, V_after)

        go_before = pd.Series([2000.0, 1000.0], index=pd.Index(['A', 'B']))
        go_after = adjust_gross_output(go_before, ratios)

        assert go_after['A'] == pytest.approx(1900.0)
        assert go_after['B'] == pytest.approx(1100.0)
        assert go_after.sum() == pytest.approx(go_before.sum())

    def test_missing_destination_industry_skipped(self) -> None:
        """
        Co-production entries whose destination is not in the gross output
        vector are skipped (guards real-world S00401/S00900 codes).
        """
        ratios = pd.DataFrame(
            {
                'source_industry': ['A'],
                'destination_industry': ['Z'],
                'ratio': [0.1],
            }
        )
        go_before = pd.Series([1000.0, 500.0], index=pd.Index(['A', 'B']))
        go_after = adjust_gross_output(go_before, ratios)

        pd.testing.assert_series_equal(go_after, go_before)


class TestConstrainedCoproductionRatios:
    def test_constrained_roundtrip_exact(self) -> None:
        """
        Using constrained ratios with g_before reproduces g_after exactly
        when column sums (commodity output) are preserved, as in BEA's
        redefinition.

        V_before:                V_after (half off-diag moved to diagonal):
          A: [700, 100, 200]       A: [740,  50, 100]
          B: [ 50, 800, 150]       B: [ 25, 860,  75]
          C: [ 30,  20, 950]       C: [ 15,  10, 1125]

        Column sums preserved: [780, 920, 1300] in both.
        """
        V_before_redef = _make_V(
            [[700, 100, 200], [50, 800, 150], [30, 20, 950]],
            ['A', 'B', 'C'],
        )
        V_after_redef = _make_V(
            [[740, 50, 100], [25, 860, 75], [15, 10, 1125]],
            ['A', 'B', 'C'],
        )
        pd.testing.assert_series_equal(
            V_before_redef.sum(axis=0), V_after_redef.sum(axis=0), check_names=False
        )

        g_before = V_before_redef.sum(axis=1)
        g_after_expected = V_after_redef.sum(axis=1)

        ratios = compute_coproduction_ratios(V_before_redef, V_after_redef)
        g_after_computed = adjust_gross_output(g_before, ratios)

        pd.testing.assert_series_equal(
            g_after_computed,
            g_after_expected.astype(float),
            check_names=False,
            rtol=1e-12,
        )


# ---------------------------------------------------------------------------
# Integration test: 2017 benchmark round-trip
# ---------------------------------------------------------------------------


@pytest.mark.eeio_integration
def test_2017_redefinition_roundtrip() -> None:
    """
    Applying **constrained** co-production ratios (using both V_before_redef
    and V_after_redef) to g_before must reproduce g_after for the 2017
    benchmark year.

    Algebraically the per-industry gap between computed and expected equals
    the column-sum discrepancy (commodity output rounding in the BEA source
    data which is published in millions)::

        gap[i] = g_computed[i] - g_expected[i] = q_before[i] - q_after[i]

    The test verifies this identity exactly (floating-point tolerance) and
    also checks that the absolute per-industry error is negligible.
    """
    from bedrock.extract.iot.io_2017 import (
        load_2017_V_before_redef_usa,
        load_2017_V_usa,
    )
    from bedrock.utils.math.formulas import compute_g

    V_before_redef = load_2017_V_before_redef_usa()
    V_after_redef = load_2017_V_usa()

    g_before = compute_g(V=V_before_redef)
    g_after_expected = compute_g(V=V_after_redef)

    ratios = compute_coproduction_ratios(V_before_redef, V_after_redef)
    g_after_computed = adjust_gross_output(g_before, ratios)

    common = g_after_expected.index.intersection(g_after_computed.index)
    assert len(common) > 0, 'No overlapping industry codes'

    # 1. Total output is preserved (zero-sum redistribution)
    assert g_after_computed.loc[common].sum() == pytest.approx(
        g_before.loc[common].sum()
    )

    # 2. Per-industry gap equals column-sum rounding discrepancy — proves
    #    the math is exact; any residual is BEA rounding (data in millions).
    q_before = V_before_redef.sum(axis=0).reindex(common, fill_value=0.0)
    q_after = V_after_redef.sum(axis=0).reindex(common, fill_value=0.0)
    column_sum_gap = q_before - q_after

    gap = g_after_computed.loc[common] - g_after_expected.loc[common]
    pd.testing.assert_series_equal(gap, column_sum_gap, check_names=False, rtol=1e-10)

    # 3. Absolute per-industry error < $11M (≤ 10 in BEA's million-dollar units)
    assert (
        gap.abs().max() <= 11_000_000
    ), f'Max abs gap {gap.abs().max():.0f} exceeds BEA rounding tolerance'
