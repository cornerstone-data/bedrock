"""Compare gas E from a rebuilt FBS against the pinned parquet.

Loads the pinned Cornerstone GHG FBS and a regenerated FBS for the same year
stem, then checks that ``load_E_from_flowsa``-equivalent pivots match
(Flowable x SectorProducedBy sums). Run after a local rebuild, before bumping
``cornerstone_ghg_fbs_filename``.

Usage::

    uv run python -m bedrock.analysis.ghg_fbs_pin.compare_gas_e \\
        [--retained path/to/retained.parquet] [--method GHG_national_Cornerstone_2024]
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

REL_TOL = 1e-6
# Floor so near-zero float dust (kg) does not flag thousands of empty cells.
ABS_TOL_KG = 1.0


def _gas_sector_totals(fbs: pd.DataFrame) -> pd.Series:
    return (
        fbs.groupby(['Flowable', 'SectorProducedBy'], observed=True)['FlowAmount']
        .sum()
        .sort_index()
    )


def compare(reference: pd.DataFrame, retained: pd.DataFrame) -> pd.DataFrame:
    """Return rows where gas x sector totals differ beyond relative/abs tol."""
    totals = pd.concat(
        {
            'reference': _gas_sector_totals(reference),
            'retained': _gas_sector_totals(retained),
        },
        axis=1,
    ).fillna(0.0)
    diff = totals['retained'] - totals['reference']
    scale = totals['reference'].abs().clip(lower=1.0)
    threshold = (REL_TOL * scale).clip(lower=ABS_TOL_KG)
    # Filter after assign: empty.loc[...].assign(series) reindexes to the
    # full series and invents NaN rows (pandas), which looked like 4099 diffs.
    return totals.assign(diff=diff).loc[diff.abs() > threshold]


def main() -> None:
    parser = argparse.ArgumentParser(description=(__doc__ or '').splitlines()[0])
    parser.add_argument(
        '--method',
        default=None,
        help='GHG FBS method stem (default: active config year stem).',
    )
    parser.add_argument(
        '--retained',
        type=Path,
        default=None,
        help='Retained-activity parquet. If omitted, rebuild with generateFlowBySector.',
    )
    parser.add_argument(
        '--rebuild',
        action='store_true',
        help='Force generateFlowBySector even when --retained is set.',
    )
    args = parser.parse_args()

    from bedrock.transform.allocation.derived import (  # noqa: PLC0415
        _load_cornerstone_ghg_fbs_from_gcs,
        _select_cornerstone_ghg_fbs_base_name,
    )
    from bedrock.transform.flowbysector import FlowBySector  # noqa: PLC0415

    method = args.method or _select_cornerstone_ghg_fbs_base_name()
    reference = _load_cornerstone_ghg_fbs_from_gcs(base_name=method)

    if args.retained is not None and not args.rebuild:
        retained = pd.read_parquet(args.retained)
    else:
        retained = pd.DataFrame(
            FlowBySector.generateFlowBySector(
                method,
                download_sources_ok=True,
                retain_activity_columns=True,
            )
        )

    if 'ActivityProducedBy' not in retained.columns:
        raise SystemExit(
            'Retained FBS lacks ActivityProducedBy; method YAML retain flag '
            'or retain_activity_columns=True did not reach the output'
        )

    bad = compare(reference, retained)
    print(
        f'{method}: {len(bad)} of '
        f'{reference.groupby(["Flowable", "SectorProducedBy"]).ngroups} '
        'Flowable x SectorProducedBy totals differ'
    )
    if not bad.empty:
        print((bad / 1e9).round(6).to_string())
        raise SystemExit(1)
    print('Gas x sector totals match the pinned FBS within tolerance.')
    activities = retained.groupby('MetaSources')['ActivityProducedBy'].nunique()
    print('Activities per MetaSources (top 10):')
    print(activities.sort_values(ascending=False).head(10).to_string())


if __name__ == '__main__':
    main()
