"""Unit tests for minimal FBS -> ceda emissions-source tagging."""

from __future__ import annotations

import pandas as pd

from bedrock.transform.allocation.emissions_source_crosswalk import (
    load_crosswalk,
    load_emissions_sources,
    tag_fbs_rows,
)


def test_ceda_emissions_source_catalog_has_123_codes() -> None:
    sources = load_emissions_sources()
    assert len(sources) == 123
    assert sources['code'].is_unique


def test_crosswalk_loads_against_catalog() -> None:
    cw = load_crosswalk()
    assert {'meta_source', 'activity', 'flowable', 'ceda_source', 'status'} <= set(
        cw.columns
    )


def test_tag_fbs_rows_prefers_more_specific_crosswalk() -> None:
    fbs = pd.DataFrame(
        {
            'MetaSources': ['M.a', 'M.a'],
            'ActivityProducedBy': ['Cement Production', 'Other'],
            'Flowable': ['Carbon dioxide', 'Carbon dioxide'],
            'SectorProducedBy': ['327310', '111'],
            'Unit': ['kg', 'kg'],
            'FlowAmount': [1.0, 1.0],
        }
    )
    cw = pd.DataFrame(
        {
            'meta_source': ['M.a', 'M.a'],
            'activity': ['*', 'Cement Production'],
            'flowable': ['Carbon dioxide', 'Carbon dioxide'],
            'ceda_source': ['co2_generic', 'co2_cement_production'],
            'status': ['mapped', 'mapped'],
            'note': ['', ''],
            'specificity': [5, 7],
        }
    )
    tagged = tag_fbs_rows(fbs, cw)
    by_act = tagged.set_index('ActivityProducedBy')['ceda_source']
    assert by_act['Cement Production'] == 'co2_cement_production'
    assert by_act['Other'] == 'co2_generic'
