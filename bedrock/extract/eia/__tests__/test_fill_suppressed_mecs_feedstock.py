"""Suppressed MECS feedstock cells filled from the other survey x GHGRP (#1200)."""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.extract.eia import EIA_MECS
from bedrock.extract.eia.EIA_MECS import (
    GHGRP_FILL_MARKER,
    estimate_suppressed_mecs_energy,
    fill_suppressed_feedstock,
)

NG = 'Natural Gas'
HGL = 'Hydrocarbon Gas Liquids, excluding natural gasoline'


def _table(
    rows: list[tuple[str, str, float, str | None]], **extra: str
) -> pd.DataFrame:
    frame = pd.DataFrame(
        rows, columns=['ActivityConsumedBy', 'FlowName', 'FlowAmount', 'Suppressed']
    )
    frame['Unit'] = frame['FlowName'].map({NG: 'billion cu ft', HGL: 'million bbl'})
    frame['Description'] = extra.get('Description', 'Table 2.1')
    frame['Location'] = extra.get('Location', '00000')
    return frame


def _ghgrp(**by_year: dict[str, float]) -> dict[int, pd.Series]:
    return {int(y[1:]): pd.Series(v, dtype=float) for y, v in by_year.items()}


def test_fills_from_other_survey_scaled_by_ghgrp() -> None:
    survey_2018 = _table(
        [
            ('325', NG, 900.0, None),
            ('325311', NG, 0.0, 'D'),
            ('325199', NG, 188.0, None),
        ]
    )
    survey_2022 = _table([('325311', NG, 330.0, None)])
    ghgrp = _ghgrp(y2018={'325311': 19.5}, y2022={'325311': 19.8, '325312': 5.0})

    out = fill_suppressed_feedstock(survey_2018, 2018, {2022: survey_2022}, ghgrp)

    row = out[out['ActivityConsumedBy'] == '325311'].iloc[0]
    # 325312 is not under 325311, so it stays out of the ratio.
    assert row['FlowAmount'] == pytest.approx(330.0 * 19.5 / 19.8)
    assert row['Suppressed'] == GHGRP_FILL_MARKER
    # Survives the suppressed-row drop that follows it in the method.
    kept = estimate_suppressed_mecs_energy(out)  # type: ignore[arg-type]
    assert '325311' in set(kept['ActivityConsumedBy'])


def test_fill_capped_at_parent_room() -> None:
    survey_2022 = _table(
        [
            ('325', NG, 705.0, None),
            ('325120', NG, 0.0, 'D'),
            ('325311', NG, 330.0, None),
            ('325199', NG, 291.0, None),
        ]
    )
    survey_2018 = _table([('325120', NG, 155.0, None)])

    out = fill_suppressed_feedstock(survey_2022, 2022, {2018: survey_2018}, {})

    value = out.loc[out['ActivityConsumedBy'] == '325120', 'FlowAmount'].iloc[0]
    assert value == pytest.approx(705.0 - 330.0 - 291.0)


def test_no_ghgrp_means_ratio_one_and_unmatched_cells_stay_suppressed() -> None:
    survey_2018 = _table([('325211', HGL, 0.0, 'D'), ('326', NG, 0.0, 'Q')])
    survey_2022 = _table([('325211', HGL, 290.0, None), ('326', NG, 0.0, 'Q')])

    out = fill_suppressed_feedstock(survey_2018, 2018, {2022: survey_2022}, {})

    filled = out.set_index('ActivityConsumedBy')
    assert filled.at['325211', 'FlowAmount'] == pytest.approx(290.0)
    assert filled.at['326', 'Suppressed'] == 'Q'


def test_only_national_feedstock_rows_change() -> None:
    fuel = _table([('325311', NG, 0.0, 'D')], Description='Table 3.1')
    region = _table([('325311', NG, 0.0, 'D')], Location='1')
    survey_2022 = _table([('325311', NG, 330.0, None)])

    out = fill_suppressed_feedstock(
        pd.concat([fuel, region], ignore_index=True), 2018, {2022: survey_2022}, {}
    )

    assert (out['Suppressed'] == 'D').all()


def test_ghgrp_ratio_matches_naics_prefix() -> None:
    ghgrp = _ghgrp(y2018={'325211': 10.0, '3252': 99.0}, y2022={'325211': 20.0})
    assert EIA_MECS._ghgrp_ratio('325211', ghgrp, 2018, 2022) == pytest.approx(0.5)
    assert EIA_MECS._ghgrp_ratio('331110', ghgrp, 2018, 2022) == 1.0
