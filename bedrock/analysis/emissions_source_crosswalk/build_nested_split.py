"""Per-sector split keys for FBS lines attributed through a nested EPA table.

Agricultural soils (``UMD_GHGIA_T_5_10.direct`` / ``.indirect``) and
non-energy use (``UMD_GHGIA_T_3_14.emissions``) are each one UMD line,
attributed to sectors in proportion to an EPA GHGI table built from several
activity sets. The UMD line is first mapped to sector groups (non-energy use
maps to 18 overlapping ones: ``31``, ``325``, ``324110``, ``F01000``, ...), and
each group ``G`` is attributed on its own, so sector ``s`` in ``G`` gets

    UMD(s) = UMD_G * EPA(s) / EPA(G),   EPA(s) = sum_k EPA_k(s)

Splitting ``UMD(s)`` by ``EPA_k(s) / EPA(s)`` decomposes it by activity set
``k`` without changing any sector total. When the group weights differ from
the EPA table's own (non-energy use), that per-sector mix no longer adds up to
the table's national mix: sectors the UMD groups weight up carry their own
fuel mix with them. So the per-sector mix is only the starting point, and is
then fitted by iterative proportional scaling so that each sector keeps its
FBS total and each activity set matches the EPA table's national share. Where
the group weights equal the table's (soils), the fit changes nothing.

This module builds those EPA tables on their own (``nested_split_*.yaml``,
with activity columns kept) and writes the fitted shares for every sector of
the production line. A sector the EPA table does not reach starts from the
mix of the EPA sectors sharing its longest NAICS prefix (at least 3 digits),
else the table's national mix; ``basis`` records which.

Usage::

    python -m bedrock.analysis.emissions_source_crosswalk.build_nested_split \
        <retained production FBS parquet> [--split-fbs <built helper parquet>]
"""

from __future__ import annotations

import argparse
import shutil
import time
from pathlib import Path
from typing import cast

import pandas as pd

HERE = Path(__file__).resolve().parent
OUTPUT_DIR = HERE / 'output'
HELPER_METHOD = 'nested_split_GHG_national_Cornerstone_2024'
SHARES_PATH = OUTPUT_DIR / f'{HELPER_METHOD}_shares.csv'

# Production MetaSources → the EPA table its sector pattern comes from.
NESTED_SOURCES = {
    'UMD_GHGIA_T_5_10.direct': 'EPA_GHGI_T_5_18',
    'UMD_GHGIA_T_5_10.indirect': 'EPA_GHGI_T_5_19',
    'UMD_GHGIA_T_3_14.emissions': 'EPA_GHGI_T_3_25',
}
SECTOR = 'SectorProducedBy'

# Production lines whose residential and commercial parts sit in one activity.
# Households (F01*) are the residential part and every other sector the
# commercial part, so the sector already tells them apart.
RESIDENTIAL_COMMERCIAL = {
    (
        'UMD_GHGIA_T_3_12.residential_commercial_non_co2',
        'Natural Gas Residential and Commercial',
    ): 'Natural Gas',
    (
        'UMD_GHGIA_T_3_12.residential_commercial_non_co2',
        'Petroleum Residential and Commercial',
    ): 'Petroleum',
}

# Production line whose FBA activities the method YAML renames to one name
# before attribution → (FBA, the activities merged). They share one mapping and
# one attribution, so every sector splits in the FBA's national proportions.
RENAMED_ACTIVITIES = {
    ('UMD_GHGIA_T_3_8.direct_jet', 'General Aviation Aircraft'): (
        'UMD_GHGIA_T_3_8',
        (
            'General Aviation Aircraft Aviation Gasoline',
            'General Aviation Aircraft Jet Fuel',
        ),
    ),
}
MIN_PREFIX = 3
FIT_TOL = 1e-10  # stop once the column gap is this small (share of the line total)
FIT_ACCEPT = (
    1e-3  # accept a smaller remaining gap: sectors with one set can block an exact fit
)
FIT_MAX_ITER = 10_000


def main() -> None:
    parser = argparse.ArgumentParser(description=(__doc__ or '').splitlines()[0])
    parser.add_argument('production_fbs', type=Path)
    parser.add_argument(
        '--split-fbs',
        type=Path,
        default=None,
        help='Reuse an already built helper FBS instead of building it.',
    )
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(exist_ok=True)
    split_fbs = pd.read_parquet(args.split_fbs) if args.split_fbs else build_split_fbs()
    production = pd.read_parquet(args.production_fbs)

    shares = pd.concat(
        [
            split_shares(split_fbs, production),
            renamed_activity_shares(production),
            residential_commercial_shares(production),
        ],
        ignore_index=True,
    )
    _print_coverage(shares, production)
    shares.to_csv(SHARES_PATH, index=False)
    print(f'Wrote {SHARES_PATH}')


def build_split_fbs() -> pd.DataFrame:
    """Generate the helper FBS and move its files out of the FBS directory."""
    from bedrock.transform.flowbysector import FlowBySector  # noqa: PLC0415
    from bedrock.utils.config.settings import FBS_DIR  # noqa: PLC0415

    started = time.time()
    fbs = pd.DataFrame(
        FlowBySector.generateFlowBySector(
            HELPER_METHOD,
            external_config_path=str(HERE),
            download_sources_ok=True,
            retain_activity_columns=True,
        )
    )
    print(f'{HELPER_METHOD} built in {time.time() - started:.0f}s')
    for written in FBS_DIR.glob(f'{HELPER_METHOD}_v*'):
        if written.stat().st_mtime >= started:
            shutil.move(written, OUTPUT_DIR / written.name)
    return fbs


def split_shares(split_fbs: pd.DataFrame, production: pd.DataFrame) -> pd.DataFrame:
    """Activity-set shares for every sector of each production line.

    ``split_label`` is the helper's ``MetaSources`` (``EPA_GHGI_T_5_18.pasture``)
    and is what the crosswalk keys on in place of ``ActivityProducedBy``.
    """
    parts = []
    for meta, table in NESTED_SOURCES.items():
        rows = split_fbs[split_fbs['MetaSources'].str.startswith(f'{table}.')]
        _require_one_unit(rows, table)
        epa = rows.pivot_table(
            index=SECTOR,
            columns='MetaSources',
            values='FlowAmount',
            aggfunc='sum',
            fill_value=0.0,
            observed=True,
        )
        epa = epa[epa.sum(axis=1) > 0]
        sectors = production.loc[production['MetaSources'] == meta, SECTOR].unique()
        seeds = []
        for sector in sectors:
            mix, basis = _mix_for(str(sector), epa)
            seeds.append(
                pd.DataFrame(
                    {
                        'MetaSources': meta,
                        SECTOR: sector,
                        'split_label': mix.index,
                        'share': mix.to_numpy(),
                        'basis': basis,
                    }
                )
            )
        amounts = (
            production[production['MetaSources'] == meta]
            .groupby(SECTOR, observed=True)['FlowAmount']
            .sum()
        )
        national = epa.sum() / epa.sum().sum()
        parts.append(fit_to_national(pd.concat(seeds), amounts, national, meta))
    shares = pd.concat(parts, ignore_index=True)
    return shares[shares['share'] > 0].reset_index(drop=True)


def fit_to_national(
    seed: pd.DataFrame, amounts: pd.Series, national: pd.Series, label: str
) -> pd.DataFrame:
    """Rescale per-sector shares so the line's national mix equals ``national``.

    Iterative proportional fitting on the sector × activity-set matrix
    ``amount(s) * share(s, k)``: columns are scaled to ``national(k)`` of the
    line total, rows back to each sector's FBS amount, until both hold.
    Zero cells stay zero, so a sector never gains an activity set its EPA mix
    lacks. Sectors with no FBS amount keep their seed shares.
    """
    basis = seed.groupby(SECTOR)['basis'].first()
    shares = seed.pivot_table(
        index=SECTOR,
        columns='split_label',
        values='share',
        aggfunc='sum',
        fill_value=0.0,
    )
    live = amounts[amounts > 0].index.intersection(shares.index)
    matrix = shares.loc[live].mul(amounts[live], axis=0)
    target = national.reindex(matrix.columns, fill_value=0.0) * amounts[live].sum()
    if ((matrix.sum() == 0) & (target > 0)).any():
        raise ValueError(f'{label}: an activity set has a national share but no sector')
    before = matrix.sum() / matrix.sum().sum()
    for _ in range(FIT_MAX_ITER):
        matrix = matrix.mul((target / matrix.sum()).fillna(0.0), axis=1)
        matrix = matrix.mul(amounts[live] / matrix.sum(axis=1), axis=0)
        gap = (matrix.sum() - target).abs().max() / target.sum()
        if gap < FIT_TOL:
            break
    if gap > FIT_ACCEPT:
        raise ValueError(f'{label}: national fit left a gap of {gap:.2e} of the line')

    fitted = pd.concat(
        [matrix.div(amounts[live], axis=0), shares.drop(index=live)]
    ).reindex(shares.index)
    moved = (fitted.loc[live] - shares.loc[live]).abs().max().max()
    print(
        f'{label}: national mix '
        + ', '.join(
            f'{k.split(".")[-1]} {before[k]:.1%}→{target[k] / target.sum():.1%}'
            for k in matrix.columns
        )
        + f'; largest per-sector share change {moved:.1%}; remaining gap {gap:.1e}'
    )
    out = (
        fitted.rename_axis(columns=None)
        .reset_index()
        .melt(id_vars=SECTOR, var_name='split_label', value_name='share')
    )
    return out.assign(MetaSources=label, basis=out[SECTOR].map(basis))[
        ['MetaSources', SECTOR, 'split_label', 'share', 'basis']
    ]


def renamed_activity_shares(production: pd.DataFrame) -> pd.DataFrame:
    """National FBA shares for activities the method merges under one name."""
    from bedrock.extract.flowbyactivity import getFlowByActivity  # noqa: PLC0415

    year = int(production['Year'].iloc[0])
    parts = []
    for (meta, activity), (fba_name, merged) in RENAMED_ACTIVITIES.items():
        fba = getFlowByActivity(fba_name, year)
        amounts = (
            fba[fba['ActivityProducedBy'].isin(merged)]
            .groupby('ActivityProducedBy')['FlowAmount']
            .sum()
            .reindex(list(merged), fill_value=0.0)
        )
        rows = production[
            (production['MetaSources'] == meta)
            & (production['ActivityProducedBy'] == activity)
        ]
        _require_same_total(amounts, rows, f'{meta} / {activity}')
        print(
            f'{meta} / {activity}: '
            + ', '.join(f'{a} {v / amounts.sum():.1%}' for a, v in amounts.items())
        )
        for sector in rows[SECTOR].unique():
            parts.append(
                pd.DataFrame(
                    {
                        'MetaSources': meta,
                        'activity': activity,
                        SECTOR: sector,
                        'split_label': amounts.index,
                        'share': (amounts / amounts.sum()).to_numpy(),
                        'basis': f'fba:{fba_name}',
                    }
                )
            )
    return pd.concat(parts, ignore_index=True)


def residential_commercial_shares(production: pd.DataFrame) -> pd.DataFrame:
    """Residential for household sectors, commercial for the rest."""
    parts = []
    for (meta, activity), fuel in RESIDENTIAL_COMMERCIAL.items():
        rows = production[
            (production['MetaSources'] == meta)
            & (production['ActivityProducedBy'] == activity)
        ]
        for sector in rows[SECTOR].unique():
            household = str(sector).startswith('F01')
            parts.append(
                pd.DataFrame(
                    {
                        'MetaSources': [meta],
                        'activity': [activity],
                        SECTOR: [sector],
                        'split_label': [
                            f'{fuel} Residential' if household else f'{fuel} Commercial'
                        ],
                        'share': [1.0],
                        'basis': ['sector:households' if household else 'sector:other'],
                    }
                )
            )
    return pd.concat(parts, ignore_index=True)


def _require_same_total(amounts: pd.Series, rows: pd.DataFrame, label: str) -> None:
    """The FBA activities must add up to the FBS line they were merged into."""
    fba_total = float(amounts.sum()) * 1e9  # FBA in MMT CO2e, FBS in kg
    fbs_total = float(rows['FlowAmount'].sum())
    if abs(fba_total - fbs_total) > 1e-6 * abs(fbs_total):
        raise ValueError(
            f'{label}: FBA total {fba_total:.4e} kg differs from the FBS line '
            f'{fbs_total:.4e} kg, so the merge is not a plain rename'
        )


def _mix_for(sector: str, epa: pd.DataFrame) -> tuple[pd.Series, str]:
    """The sector's own EPA mix, else its longest-prefix relatives', else national."""
    if sector in epa.index:
        row = cast(pd.Series, epa.loc[sector])
        return row / row.sum(), 'sector'
    for length in range(len(sector) - 1, MIN_PREFIX - 1, -1):
        prefix = sector[:length]
        related = epa[epa.index.astype(str).str.startswith(prefix)]
        if not related.empty:
            total = related.sum()
            return total / total.sum(), f'prefix:{prefix}'
    total = epa.sum()
    return total / total.sum(), 'national'


def _print_coverage(shares: pd.DataFrame, production: pd.DataFrame) -> None:
    """Share of each production line split on each basis, by kg."""
    for meta in NESTED_SOURCES:
        amounts = (
            production[production['MetaSources'] == meta]
            .groupby(SECTOR, observed=True)['FlowAmount']
            .sum()
        )
        basis = shares[shares['MetaSources'] == meta].groupby(SECTOR)['basis'].first()
        by_basis = amounts.groupby(
            basis.reindex(amounts.index).map(lambda b: b.split(':')[0])
        ).sum()
        print(
            f'{meta}: '
            + ', '.join(f'{b} {v / amounts.sum():.1%}' for b, v in by_basis.items())
        )
        fallbacks = basis[basis != 'sector']
        weighted = amounts.reindex(fallbacks.index) / amounts.sum()
        for sector, b in fallbacks.items():
            if weighted[sector] > 0.001:
                print(f'  {sector} ({weighted[sector]:.1%}) via {b}')


def _require_one_unit(rows: pd.DataFrame, table: str) -> None:
    units = rows['Unit'].unique()
    if len(units) != 1:
        raise ValueError(f'{table} mixes units {list(units)}; shares need one unit')


if __name__ == '__main__':
    main()
