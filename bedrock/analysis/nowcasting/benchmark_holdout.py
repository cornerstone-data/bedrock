"""Scoring a seed against **observed** benchmark detail, not against BEA's carry-forward.

⚠️ **Every score in §S4, §S5 and the trade retest is graded against the wrong
answer key.**  They score a seed at BEA's published *summary* Use for 2018-2024.
Two things are wrong with that, and Wes named both:

1. ⚠️ **BEA has not incorporated the 2022 Economic Census.**  Its 2018-2024
   tables are the 2017 benchmark carried forward on BEA's own annual methods.
   ❌ **So those tests measure agreement with BEA's carry-forward, not
   accuracy** -- and a seed that correctly caught real structural change would
   *lose* against a key that has not seen it.
2. ⚠️ **Summary collapses the detail the seed actually moves.**  Several survey
   items land on one summary code, so distinct movements are averaged away
   before they are scored, and different items get identical targets.

✅ **This module is the answer key that has neither problem.**

The test
--------

BEA publishes **three detail benchmarks** -- 2007, 2012 and 2017 -- and
``io_2017`` reads all of them onto the **same 2017 code axis**
(:func:`~bedrock.extract.iot.io_2017.load_benchmark_detail_U_intermediate_usa`
selects ``USA_2017_COMMODITY_CODES`` for every year).  So::

    seed the 2012 block with a source's 2012 -> 2017 movement,
    and score it against the observed 2017 block

✅ **Out of sample**, ✅ **at detail**, ✅ **against an observation rather than an
extrapolation**, and ✅ **nothing in the test is derived from what is being
tested**.

⚠️ **The level is not being tested, the mix is.**  Every seeded column is
renormalised to the **observed 2017 column total** before scoring, because Step
3 observes the level through ``GO - VAPRO`` and would never take it from a
survey.  A source that gets the level right and the mix wrong scores zero here,
correctly.

⚠️ What this test cannot do
----------------------------

⚠️ **It measures a 2012 -> 2017 span, and the seeds are wanted for 2018-2025.**
It is evidence that a source's movement tracks BEA's, on the one span where both
are observed; it is not proof the same holds later.  In particular the
2021-22 price surge has no counterpart in 2012-2017.

⚠️ **A source needs a 2012 observation.**  ERS FIWS (1910-2025) and the trade
BES (quinquennial, 2 and 7) have one; ``Census_SAS_Expenses`` starts at **2013**
and cannot be tested here without substituting 2013 for 2012 and saying so.

⚠️ **2007 is available too** and is deliberately not used as the base: the
2007 -> 2012 span is a recession, and ``inputs_structure``'s interpolation work
already showed the 2007 mix is a poor predictor.  It is left available for a
second holdout rather than wired in.

Run::

    uv run python -m bedrock.analysis.nowcasting.benchmark_holdout --drift
"""

from __future__ import annotations

import argparse
import functools
from collections.abc import Callable

import numpy as np
import pandas as pd

#: The benchmark years ``io_2017`` reads onto the 2017 code axis.
BENCHMARKS = (2007, 2012, 2017)

#: The span this scores. ``BASE`` is what a seed starts from, ``TARGET`` is the
#: observed answer.
BASE = 2012
TARGET = 2017

#: A source's movement, as ``bea_industry -> (commodity -> relative index)``.
#: Every seed in this package already produces exactly this shape.
IndexFor = Callable[[str], "pd.Series[float]"]


@functools.cache
def block(year: int) -> pd.DataFrame:
    """The benchmark detail Use intermediate block, commodity x industry, in $M."""
    from bedrock.extract.iot.io_2017 import (  # noqa: PLC0415
        load_benchmark_detail_U_intermediate_usa,
    )

    frame = load_benchmark_detail_U_intermediate_usa(year) / 1e6
    frame.index = frame.index.astype(str)
    frame.columns = frame.columns.astype(str)
    return frame


@functools.cache
def intensity() -> "pd.Series[float]":
    """``N`` at BEA detail -- total kg CO2e per dollar, direct plus indirect."""
    from bedrock.analysis.nowcasting.services_transport_expense_resource import (  # noqa: PLC0415
        impact_intensity,
    )

    return impact_intensity().reindex(block(TARGET).index).fillna(0.0)


def _weights(weighting: str, rows: list[str]) -> "pd.Series[float]":
    if weighting == 'impact':
        return intensity().reindex(rows).fillna(0.0)
    return pd.Series(1.0, index=rows)


def mix_drift(weighting: str = 'impact') -> pd.DataFrame:
    """How far each column's mix actually moved 2012 -> 2017.

    ⚠️ **This is the size of the prize, and the bar.**  A seed has to beat the
    frozen-2012 mix, and a column that barely drifts leaves nothing to win.
    Reported per BEA detail industry, weighted by the column's own dollars.
    """
    early, late = block(BASE), block(TARGET)
    columns = [c for c in late.columns if c in early.columns]
    rows = list(late.index)
    weights = _weights(weighting, rows)
    records = []
    for column in columns:
        first = early[column].reindex(rows).fillna(0.0)
        second = late[column].reindex(rows).fillna(0.0)
        if first.sum() <= 0 or second.sum() <= 0:
            continue
        moved = float(
            (weights * (first / first.sum() - second / second.sum()).abs()).sum() / 2
        )
        records.append(
            {
                'industry': column,
                'drift': moved,
                'dollars_2017_M': float(second.sum()),
            }
        )
    frame = pd.DataFrame(records).set_index('industry')
    frame['weight_%'] = 100 * frame['dollars_2017_M'] / frame['dollars_2017_M'].sum()
    return frame.sort_values('drift', ascending=False)


def seed_from_base(index_for: IndexFor, columns: list[str]) -> pd.DataFrame:
    """The 2012 block moved on a source's 2012 -> 2017 index.

    ⚠️ **Renormalised to the observed 2017 column total**, so only the mix is on
    trial -- see the module docstring.
    """
    early, late = block(BASE), block(TARGET)
    seed = early[columns].astype(float).copy()
    for column in columns:
        index = index_for(column)
        if index is None or len(index) == 0:
            continue
        touched = [code for code in index.index if code in seed.index]
        if not touched:
            continue
        seed.loc[touched, column] = (
            seed.loc[touched, column] * index.reindex(touched).to_numpy()
        )
    totals = seed.sum(axis=0)
    target_totals = late[columns].sum(axis=0)
    return (
        seed.div(totals.where(totals != 0, np.nan), axis=1)
        .mul(target_totals, axis=1)
        .fillna(0.0)
    )


def holdout_score(
    index_for: IndexFor,
    columns: list[str],
    weighting: str = 'impact',
) -> pd.DataFrame:
    """Frozen 2012 against a seeded 2012, scored on the observed 2017 block.

    Per BEA detail industry: the frozen and seeded dissimilarity from the
    observed mix, the gain, and the column's weight.  ⚠️ **A positive gain here
    means the source moved the mix toward what BEA later observed**, which is
    the claim every seed in this package is making.
    """
    early, late = block(BASE), block(TARGET)
    columns = [c for c in columns if c in early.columns and c in late.columns]
    seeded = seed_from_base(index_for, columns)
    rows = list(late.index)
    weights = _weights(weighting, rows)

    records = []
    for column in columns:
        frozen = early[column].reindex(rows).fillna(0.0)
        estimate = seeded[column].reindex(rows).fillna(0.0)
        truth = late[column].reindex(rows).fillna(0.0)
        if frozen.sum() <= 0 or estimate.sum() <= 0 or truth.sum() <= 0:
            continue
        truth_share = truth / truth.sum()
        d_frozen = float(
            (weights * (frozen / frozen.sum() - truth_share).abs()).sum() / 2
        )
        d_seeded = float(
            (weights * (estimate / estimate.sum() - truth_share).abs()).sum() / 2
        )
        records.append(
            {
                'industry': column,
                'frozen': d_frozen,
                'seeded': d_seeded,
                'gain_%': 100 * (d_frozen - d_seeded) / d_frozen if d_frozen else 0.0,
                'weight': float((weights * truth).sum()),
                'dollars_2017_M': float(truth.sum()),
            }
        )
    return pd.DataFrame(records).set_index('industry')


def aggregate(scored: pd.DataFrame) -> dict[str, float]:
    """The block-level verdict from :func:`holdout_score`, weighted by column."""
    if scored.empty:
        return {'columns': 0, 'frozen': float('nan'), 'seeded': float('nan')}
    total = float(scored['weight'].sum())
    frozen = float((scored['frozen'] * scored['weight']).sum()) / total
    seeded = float((scored['seeded'] * scored['weight']).sum()) / total
    return {
        'columns': float(len(scored)),
        'frozen': frozen,
        'seeded': seeded,
        'gain_%': 100 * (frozen - seeded) / frozen if frozen else float('nan'),
        'wins': float((scored['seeded'] < scored['frozen']).sum()),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--drift', action='store_true', help='how far the mix moved 2012 -> 2017'
    )
    args = parser.parse_args()
    pd.set_option('display.width', 200)

    if args.drift or not args.drift:
        for weighting in ('dollar', 'impact'):
            frame = mix_drift(weighting)
            economy = float(
                (frame['drift'] * frame['dollars_2017_M']).sum()
                / frame['dollars_2017_M'].sum()
            )
            print(f'\n=== {weighting}-weighted mix drift, 2012 -> 2017 ===')
            print(f'    dollar-weighted economy-wide drift: {economy:.4f}')
            print('\n    worst-drifting columns:')
            print(frame.head(10).round(4).to_string())
            print('\n    least-drifting columns:')
            print(frame.tail(5).round(4).to_string())


if __name__ == '__main__':
    main()
