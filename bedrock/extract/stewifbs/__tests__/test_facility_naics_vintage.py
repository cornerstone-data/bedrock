"""Facility NAICS from other vintages are recoded to NAICS 2017 (#1042)."""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.extract.stewifbs.facility_combustion import (
    naics_to_2017,
    recode_naics_to_2017,
)


@pytest.mark.parametrize(
    ('reported', 'expected'),
    [
        # NAICS 2022, from GHGRP/NEI 2022 on
        ('322120', '322121'),  # paper mills
        ('212115', '212112'),  # surface coal mining
        ('212390', '212391'),  # other nonmetallic mineral mining
        # NAICS 2012 and 2007, in GHGRP 2017
        ('211111', '211120'),  # oil and gas extraction
        ('331111', '331110'),  # iron and steel mills
        ('325181', '325180'),  # alkalies and chlorine
    ],
)
def test_other_vintages_recode_to_2017(reported: str, expected: str) -> None:
    assert naics_to_2017()[reported] == expected


def test_2017_codes_and_short_codes_are_unchanged() -> None:
    codes = pd.Series(['322121', '211130', '2111', '32731', 'nan', 331110.0])
    out = recode_naics_to_2017(codes)
    assert out.tolist() == ['322121', '211130', '2111', '32731', 'nan', '331110']


def test_no_recode_target_outside_2017() -> None:
    concordance = pd.read_csv(
        'bedrock/utils/mapping/naics/NAICS_Year_Concordance.csv', dtype=str
    )
    current = set(concordance['NAICS_2017_Code'].dropna())
    recode = naics_to_2017()
    assert not set(recode) & current
    assert set(recode.values()) <= current
