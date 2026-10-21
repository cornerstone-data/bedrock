"""Trial: rebuild a GHG FBS with its GHGI activity columns kept.

Builds the FBS with ``retain_activity_columns=True`` and checks it against
the parquet the active config loads:

1. totals per ``MetaSources`` × ``Flowable`` match the baseline,
2. ``ActivityProducedBy`` is filled, and how many activities each
   ``MetaSources`` now separates.

``generateFlowBySector`` writes its parquet into the FBS directory under the
method's own name. This module moves that file into ``output/`` with a
``_retained`` suffix, so the GHG FBS loaders never pick it up.

A totals mismatch is either drift since the baseline was built or a fault of
the retained build. ``--control`` also builds without the flag at the same
commit to tell the two apart.

Usage::

    python -m bedrock.analysis.emissions_source_crosswalk.trial_retained_activity_fbs \
        [--method <stem>] [--control]
"""

from __future__ import annotations

import argparse
import shutil
import time
from pathlib import Path

import pandas as pd

OUTPUT_DIR = Path(__file__).resolve().parent / 'output'
REL_TOL = 1e-6


def main() -> None:
    parser = argparse.ArgumentParser(description=(__doc__ or '').splitlines()[0])
    parser.add_argument('--method', default=None, help='GHG FBS method stem.')
    parser.add_argument(
        '--control',
        action='store_true',
        help='Also rebuild without retained columns at this commit.',
    )
    args = parser.parse_args()

    from bedrock.analysis.emissions_source_crosswalk.inventory_fbs_sources import (  # noqa: PLC0415
        _method_for_active_config,
    )
    from bedrock.transform.allocation.derived import (  # noqa: PLC0415
        _load_cornerstone_ghg_fbs_from_gcs,
    )

    method = args.method or _method_for_active_config()
    OUTPUT_DIR.mkdir(exist_ok=True)

    baseline = _load_cornerstone_ghg_fbs_from_gcs(base_name=method)
    retained = build_fbs(method, retain_activity_columns=True, tag='retained')
    report(method, baseline, retained, label='retained vs baseline')

    if args.control:
        control = build_fbs(method, retain_activity_columns=False, tag='control')
        report(method, control, retained, label='retained vs control')


def build_fbs(method: str, *, retain_activity_columns: bool, tag: str) -> pd.DataFrame:
    """Generate the FBS and move its parquet out of the FBS directory."""
    from bedrock.transform.flowbysector import FlowBySector  # noqa: PLC0415
    from bedrock.utils.config.settings import FBS_DIR  # noqa: PLC0415

    started = time.time()
    fbs = pd.DataFrame(
        FlowBySector.generateFlowBySector(
            method,
            download_sources_ok=True,
            retain_activity_columns=retain_activity_columns,
        )
    )
    print(f'{method} [{tag}] built in {time.time() - started:.0f}s')

    for written in FBS_DIR.glob(f'{method}_v*'):
        if written.stat().st_mtime >= started:
            shutil.move(written, OUTPUT_DIR / f'{written.stem}_{tag}{written.suffix}')
    return fbs


def report(
    method: str, reference: pd.DataFrame, retained: pd.DataFrame, *, label: str
) -> None:
    keys = ['MetaSources', 'Flowable']
    totals = pd.concat(
        {
            'reference': reference.groupby(keys, observed=True)['FlowAmount'].sum(),
            'retained': retained.groupby(keys, observed=True)['FlowAmount'].sum(),
        },
        axis=1,
    ).fillna(0.0)
    diff = totals['retained'] - totals['reference']
    bad = totals[diff.abs() > REL_TOL * totals['reference'].abs().clip(lower=1.0)]
    print(f'\n{label}: {len(bad)} of {len(totals)} MetaSources × gas totals differ')
    if not bad.empty:
        print((bad.assign(diff=diff[bad.index]) / 1e9).round(4).to_string())
        bad.to_csv(OUTPUT_DIR / f'{method}_{label.replace(" ", "_")}_diffs.csv')

    if 'ActivityProducedBy' not in retained.columns:
        print('ActivityProducedBy is missing: the flag did not reach the output')
        return
    missing = retained['ActivityProducedBy'].isna()
    share = retained.loc[missing, 'FlowAmount'].sum() / retained['FlowAmount'].sum()
    print(f'Rows with no ActivityProducedBy: {missing.sum()} ({share:.1%} of kg)')
    per_meta = (
        retained.groupby('MetaSources', observed=True)['ActivityProducedBy']
        .nunique()
        .sort_values(ascending=False)
    )
    print('Activities per MetaSources (top 15):')
    print(per_meta.head(15).to_string())


if __name__ == '__main__':
    main()
