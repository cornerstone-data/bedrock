"""The model geography boundary: the 50 states, DC and offshore.

The regression these guard is a quiet one - offshore reads like a data defect,
so every tidy-up that filters on "is this a state?" or "did the FIPS lookup
succeed?" deletes it without saying so.
"""

import pandas as pd

from bedrock.utils.mapping.location import (
    BEA_ECONOMIC_TERRITORY,
    apply_county_FIPS,
    filter_to_model_geography,
    in_model_geography,
)


def _facilities() -> pd.DataFrame:
    """One facility per jurisdiction class, shaped like a stewi facility list."""
    return pd.DataFrame(
        {
            'FacilityID': ['onshore', 'offshore', 'territory', 'nostate'],
            'State': ['LA', 'DM', 'PR', None],
            'County': ['Terrebonne', 'Federal Waters', None, None],
            'FlowAmount': [1e9, 2e9, 3e9, 4e9],
        }
    )


def test_boundary_is_the_50_states_dc_and_offshore() -> None:
    assert len(BEA_ECONOMIC_TERRITORY) == 52
    assert 'DC' in BEA_ECONOMIC_TERRITORY
    assert 'DM' in BEA_ECONOMIC_TERRITORY
    for territory in ('PR', 'VI', 'GU', 'AS', 'MP'):
        assert territory not in BEA_ECONOMIC_TERRITORY


def test_offshore_is_kept_and_territories_go() -> None:
    kept = filter_to_model_geography(_facilities(), label='test')
    assert list(kept['FacilityID']) == ['onshore', 'offshore']


def test_missing_state_is_dropped_not_treated_as_a_jurisdiction() -> None:
    # eGRID 2024 carries one plant whose State is the literal string 'None'
    frame = _facilities().assign(State=['LA', 'DM', 'PR', 'None'])
    kept = filter_to_model_geography(frame, label='test')
    assert list(kept['FacilityID']) == ['onshore', 'offshore']


def test_fips_success_is_not_the_boundary() -> None:
    """Offshore has no county FIPS, so a null ``Location`` cannot stand in.

    This is the substitution that silently deletes the Gulf: ``DM`` and ``PR``
    both come out of :func:`apply_county_FIPS` with a null ``Location``, but one
    is inside the boundary and the other is outside it.
    """
    located = apply_county_FIPS(_facilities())
    by_id = located.set_index('FacilityID')['Location']
    assert pd.isna(by_id['offshore'])
    assert pd.isna(by_id['territory'])

    survives_fips_filter = set(located.loc[located['Location'].notna(), 'FacilityID'])
    survives_boundary = set(filter_to_model_geography(_facilities())['FacilityID'])
    assert 'offshore' in survives_boundary
    assert 'offshore' not in survives_fips_filter


def test_in_model_geography_is_elementwise() -> None:
    state = pd.Series(['LA', 'DM', 'PR'])
    assert list(in_model_geography(state)) == [True, True, False]
