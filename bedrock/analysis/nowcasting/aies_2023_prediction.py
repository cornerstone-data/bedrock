"""A pre-registered prediction of BEA's 2023 revision, and the scorer for it (#1013).

⚠️ **Written and committed before the answer is published.** BEA's 2026 annual
update lands **2026-09-30** and, per the SCB preview
(https://apps.bea.gov/scb/issues/2026/06-june/0626-nea-preview.htm), it
"is evaluating the AIES data for 2023 and will incorporate as appropriate on a
best-change basis".  The same article says the AIES detail "was not available in
time to incorporate into last year's annual update", which is why the current
vintage has no AIES at all.

✅ **That makes 2023 a genuine out-of-sample test of #1013.**
:mod:`~bedrock.transform.iot.aies_go_chaining` conditions 2023 detail gross
output on AIES.  If the conditioning is right, BEA's own revision should move in
the same direction -- and BEA is a fully independent scorer, since they have
neither seen nor care about our estimate.

⚠️ **2024 is NOT part of this test.** The preview gives no indication AIES 2024
will be incorporated, so 2024 should be expected *not* to move toward us.  If it
does, that is a bonus rather than the prediction.  The 2023 arm may become
redundant on 2026-09-30; the 2024 arm is the lasting contribution either way.

The prediction
--------------

For each detail industry, three numbers are frozen to
:data:`PREDICTION_CSV` before the release:

``bea_2023_prior``
    BEA's 2023 gross output in the vintage we hold today, $M.
``predicted_2023``
    What this module says it should be -- ``aies_go_chaining`` applied.
``census_2023``
    The AIES receipts the prediction is built from, $M.

⚠️ **Score on the mix, not the level.** The conditioner holds BEA's own
manufacturing total by construction, so it makes no claim about the sector
total, and BEA's revision will move that total for reasons unrelated to AIES.
:func:`score` therefore reports the share-based movement as the headline and the
level-based one alongside it.

What would falsify it
---------------------

⚠️ Stated now, so the bar cannot move later:

- ✅ **Confirmed** if BEA's revised 2023 is closer to ``predicted_2023`` than
  ``bea_2023_prior`` was, for a **clear majority** of the industries this module
  moves, weighted by how much it moves them.
- ❌ **Falsified** if BEA revises 2023 materially and the movement is
  uncorrelated with ours, or runs the other way.
- ⚠️ **No verdict** if BEA leaves 2023 detail essentially unrevised -- that
  means they declined to incorporate AIES for these industries and the test
  simply did not run.  It is *not* evidence either way, and must not be reported
  as confirmation.

Run::

    uv run python -m bedrock.analysis.nowcasting.aies_2023_prediction --freeze
    uv run python -m bedrock.analysis.nowcasting.aies_2023_prediction --score
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

PREDICTION_CSV = Path('bedrock/analysis/nowcasting/aies_2023_prediction.csv')

#: The year under test. 2024 is deliberately excluded -- see the module note.
YEAR = 2023

#: How much an industry must move to count in the weighted score, $M. Below
#: this the revision is noise and including it would dilute the test.
MATERIAL_USD_M = 100.0


def build() -> pd.DataFrame:
    """The frozen prediction: prior, predicted, and the census behind it."""
    from bedrock.transform.iot.aies_go_chaining import (  # noqa: PLC0415
        PREFIXES,
        apply_aies_chaining,
        receipts_by_bea,
    )
    from bedrock.transform.iot.derived_intermediate_and_value_added import (  # noqa: PLC0415
        detail_gross_output_panel,
    )
    from bedrock.transform.iot.ec_go_adjustment import (  # noqa: PLC0415
        apply_ec_adjustment,
    )

    raw = detail_gross_output_panel(ec_adjusted=False)
    prior = apply_ec_adjustment(raw)
    predicted = apply_aies_chaining(prior, raw)
    census = receipts_by_bea(YEAR)

    members = [c for c in raw.index.astype(str) if str(c).startswith(PREFIXES)]
    frame = pd.DataFrame(
        {
            'bea_2023_prior': prior.loc[members, YEAR].astype(float),
            'predicted_2023': predicted.loc[members, YEAR].astype(float),
            'census_2023': census.reindex(members).astype(float),
        }
    )
    frame.index.name = 'industry'
    frame['delta'] = frame['predicted_2023'] - frame['bea_2023_prior']
    frame['delta_pct'] = (
        100.0 * frame['delta'] / frame['bea_2023_prior'].replace(0.0, np.nan)
    )
    return frame.sort_values('delta', key=abs, ascending=False)


def freeze() -> int:
    frame = build()
    PREDICTION_CSV.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(PREDICTION_CSV, float_format='%.3f')
    material = frame[frame['delta'].abs() > MATERIAL_USD_M]
    print(f'froze {len(frame)} industries to {PREDICTION_CSV}')
    print(f'  {len(material)} move more than ${MATERIAL_USD_M:,.0f}M')
    print(
        f'  sum |delta| {frame["delta"].abs().sum() / 1e3:,.1f} bn, '
        f'net {frame["delta"].sum() / 1e3:,.1f} bn'
    )
    print()
    print('  the ten the prediction commits hardest to:')
    for code, row in frame.head(10).iterrows():
        print(
            f'    {code:>8}  prior {row.bea_2023_prior / 1e3:8,.1f}bn  '
            f'-> {row.predicted_2023 / 1e3:8,.1f}bn  '
            f'({row.delta_pct:+6.1f}%)'
        )
    return 0


def score(new_panel: pd.DataFrame | None = None) -> pd.DataFrame:
    """Did BEA's revision move toward the prediction? Run after 2026-09-30.

    ``new_panel`` defaults to whatever ``detail_gross_output_panel`` returns
    now, so this scores correctly once the September vintage is pulled.
    """
    if not PREDICTION_CSV.exists():
        raise FileNotFoundError(
            f'{PREDICTION_CSV} is missing -- the prediction must be frozen '
            f'BEFORE the release for this to mean anything'
        )
    frozen = pd.read_csv(PREDICTION_CSV, index_col='industry')
    if new_panel is None:
        from bedrock.transform.iot.derived_intermediate_and_value_added import (  # noqa: PLC0415, E501
            detail_gross_output_panel,
        )
        from bedrock.transform.iot.ec_go_adjustment import (  # noqa: PLC0415
            apply_ec_adjustment,
        )

        new_panel = apply_ec_adjustment(detail_gross_output_panel(ec_adjusted=False))

    shared = [c for c in frozen.index.astype(str) if c in new_panel.index]
    out = frozen.loc[shared].copy()
    out['bea_2023_revised'] = new_panel.loc[shared, YEAR].astype(float)

    # Share-based is the headline: the conditioner holds BEA's sector total, so
    # it never claimed the level.
    for col in ('bea_2023_prior', 'predicted_2023', 'bea_2023_revised'):
        out[f'{col}_share'] = out[col] / out[col].sum()
    out['gap_before'] = (
        out['bea_2023_prior_share'] - out['predicted_2023_share']
    ).abs()
    out['gap_after'] = (
        out['bea_2023_revised_share'] - out['predicted_2023_share']
    ).abs()
    out['moved_toward'] = out['gap_after'] < out['gap_before']

    material = out[out['delta'].abs() > MATERIAL_USD_M]
    weight = material['delta'].abs()
    print(f'scored {len(out)} industries, {len(material)} material')
    if not material.empty:
        print(
            f'  moved toward the prediction: '
            f'{100 * float(material["moved_toward"].mean()):.1f}% unweighted, '
            f'{100 * float((material["moved_toward"] * weight).sum() / weight.sum()):.1f}% '
            f'weighted by how far we moved them'
        )
    closed = 1.0 - out['gap_after'].sum() / out['gap_before'].sum()
    print(f'  share of the total mix gap closed: {100 * closed:.1f}%')
    revised = float((out['bea_2023_revised'] - out['bea_2023_prior']).abs().sum())
    print(f'  BEA revised 2023 by {revised / 1e3:,.1f} bn in absolute terms')
    if revised < 1e3:
        print('  ⚠️ BEA barely revised 2023 -- NO VERDICT, the test did not run')
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--freeze', action='store_true', help='write the CSV')
    parser.add_argument('--score', action='store_true', help='run after 2026-09-30')
    args = parser.parse_args()
    if args.score:
        pd.set_option('display.width', 200)
        score()
        return 0
    return freeze()


if __name__ == '__main__':
    raise SystemExit(main())
