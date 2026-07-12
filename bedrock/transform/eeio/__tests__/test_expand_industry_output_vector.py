"""Tests for industry gross-output expand (many-to-one sum vs reverse-map)."""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.transform.eeio.cornerstone_expansion import (
    CS_INDUSTRY_LIST,
    cs_industry_to_bea_map,
    expand_industry_output_vector,
    expand_vector,
    industry_corresp_raw,
)


def _synthetic_bea_x() -> pd.Series[float]:
    """Distinct positive values on every BEA industry column in the correspondence."""
    corresp = industry_corresp_raw()
    values = {bea: float(i + 1) * 1_000.0 for i, bea in enumerate(corresp.columns)}
    return pd.Series(values, dtype=float)


def _multi_parent_industry_rows() -> list[str]:
    corresp = industry_corresp_raw()
    return [c for c in corresp.index if (corresp.loc[c] > 0).sum() > 1]


def _first_parent_industry_expand(x_bea: pd.Series[float]) -> pd.Series[float]:
    """Historical reverse-map expand: each CS row gets only its first BEA parent."""
    corresp = industry_corresp_raw()
    out = pd.Series(0.0, index=CS_INDUSTRY_LIST, dtype=float)
    for cs in CS_INDUSTRY_LIST:
        parents = corresp.columns[corresp.loc[cs] > 0].tolist()
        if parents and parents[0] in x_bea.index:
            out[cs] = float(x_bea[parents[0]])
    return out


def test_expand_industry_output_sums_all_multi_parent_rows() -> None:
    """Property: every many-to-one CS industry gets the sum of its BEA parents."""
    corresp = industry_corresp_raw()
    x_bea = _synthetic_bea_x()
    x_cs = expand_industry_output_vector(x_bea)
    multi = _multi_parent_industry_rows()
    assert multi, "expected at least one industry many-to-one row"
    for cs in multi:
        parents = corresp.columns[corresp.loc[cs] > 0].tolist()
        assert x_cs[cs] == sum(float(x_bea[p]) for p in parents)


def test_expand_industry_output_vs_first_parent_only_multi_parent_rows_differ() -> None:
    """Blast radius: vs first-parent expand, only multi-parent CS rows change."""
    corresp = industry_corresp_raw()
    x_bea = _synthetic_bea_x()
    x_new = expand_industry_output_vector(x_bea)
    x_old = _first_parent_industry_expand(x_bea)
    multi = set(_multi_parent_industry_rows())

    delta = x_new - x_old
    assert set(delta.index[delta.abs() > 1e-9]) == multi
    for cs in multi:
        parents = corresp.columns[corresp.loc[cs] > 0].tolist()
        assert delta[cs] == sum(float(x_bea[p]) for p in parents[1:])


def test_expand_vector_rejects_industry_reverse_map() -> None:
    """Hard guard: industry GO must not use expand_vector + cs_industry_to_bea_map."""
    with pytest.raises(ValueError, match="expand_industry_output_vector"):
        expand_vector(_synthetic_bea_x(), CS_INDUSTRY_LIST, cs_industry_to_bea_map())
