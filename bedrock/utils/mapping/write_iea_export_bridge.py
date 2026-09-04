"""Write ``iea_export_bridge.csv`` — which IEA categories move each export row (#771).

The services half of detail exports is anchored on BEA's published 2017
``F04000`` column and moved by BEA's International Economic Accounts (IEA)
service-category totals.  This script fits the link between the two: for every
commodity row, the share of its 2017 exports attributable to each IEA category,
``s(category | commodity)``.

Fitted by RAS on the category x commodity support the
``Sector_Crosswalk_BEA_IEA_exports`` sets define — category totals as row
margins, the published column as column margins — then **normalized within
each commodity**, which is the direction that matters: the anchor construction
``exports(c, t) = published_2017(c) x Σ_cat s(cat|c) · growth_cat(t)``
reproduces 2017 exactly for every row regardless of how the fit resolved the
margins.

⚠️ **The two margins are genuinely inconsistent and this construction chooses
the published side.**  Fitted jointly, ITA's category totals miss the published
rows' capacity by ~254bn gross at 2017 (Engineering +17bn, other-business
+12bn, advertising +6bn overclaimed; the IP-license and audiovisual families
underclaimed) — ITA's service definitions are not BEA's commodity bridge.
Anchoring on the published rows makes that inconsistency shade only the growth
*weights*, never the 2017 level.

Rerun to refresh after a crosswalk or IEA-extract change, then commit the diff::

    uv run python -m bedrock.utils.mapping.write_iea_export_bridge
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from bedrock.extract.iot.io_2017 import _load_2017_detail_supply_use_usa

#: Per direction: (bridge csv, crosswalk, IEA 2017 extract, anchor column,
#: anchor table).  Exports anchor on the Use table's export column; imports
#: on the Supply table's imports column.  The S-coded rows (rest-of-world
#: adjustment, noncomparable imports, used/scrap) are excluded by the
#: service-row filter in :func:`build` — #766 owns noncomparable imports.
DIRECTIONS: dict[str, tuple[str, str, str, str, str]] = {
    'Exports': (
        'bedrock/analysis/nowcasting/trade_data/iea_export_bridge.csv',
        'bedrock/utils/mapping/activitytosectormapping/'
        'Sector_Crosswalk_BEA_IEA_exports.csv',
        'bedrock/extract/input_data/BEA_IEA/2017/BEA_IEA_2017_Exports.csv',
        'F04000',
        'Use_SUT_detail',
    ),
    'Imports': (
        'bedrock/analysis/nowcasting/trade_data/iea_import_bridge.csv',
        'bedrock/utils/mapping/activitytosectormapping/'
        'Sector_Crosswalk_BEA_IEA_imports.csv',
        'bedrock/extract/input_data/BEA_IEA/2017/BEA_IEA_2017_Imports.csv',
        'MCIF',
        'Supply_detail',
    ),
}

#: RAS sweeps.  The support is ~45 x 90 and converges in tens of sweeps; the
#: cap is generous because the margins are inconsistent and never fully close.
SWEEPS = 500


def category_totals_2017(direction: str = 'Exports') -> pd.Series:
    frame = pd.read_csv(DIRECTIONS[direction][2])
    frame = frame[
        (frame['TradeDirection'] == direction)
        & (frame['Affiliation'] == 'AllAffiliations')
        & (frame['AreaOrCountry'] == 'AllCountries')
    ]
    return frame.set_index('TypeOfService')['DataValue'].astype(float)


def build(direction: str = 'Exports') -> pd.DataFrame:
    _, crosswalk_path, _, anchor_column, anchor_table = DIRECTIONS[direction]
    crosswalk = pd.read_csv(crosswalk_path, dtype=str)
    totals = category_totals_2017(direction)
    categories = sorted(set(crosswalk['Activity']) & set(totals.index))

    table = _load_2017_detail_supply_use_usa(anchor_table)  # type: ignore[arg-type]
    table.columns = table.columns.str.strip()
    published = pd.to_numeric(table[anchor_column], errors='coerce').fillna(0.0)
    published = published[[c for c in published.index if str(c)[0] in '45678']]

    support = {
        category: sorted(crosswalk.loc[crosswalk['Activity'] == category, 'Sector'])
        for category in categories
    }
    rows = sorted({sector for sectors in support.values() for sector in sectors})
    column_margin = published.reindex(rows).fillna(0.0).clip(lower=0.0)
    row_margin = totals[categories].astype(float)

    incidence = pd.DataFrame(0.0, index=categories, columns=rows)
    for category, sectors in support.items():
        incidence.loc[category, sectors] = 1.0

    # Seed on the published column shape so zero-published in-support cells
    # start at a token dollar rather than zero (a zero can never be scaled up).
    fitted = incidence.mul(column_margin, axis=1)
    fitted = fitted.mask(incidence.gt(0) & fitted.eq(0), 1.0)
    for _ in range(SWEEPS):
        fitted = fitted.mul(
            row_margin / fitted.sum(axis=1).replace(0, np.nan), axis=0
        ).fillna(0.0)
        fitted = fitted.mul(
            (column_margin / fitted.sum(axis=0).replace(0, np.nan)).fillna(1.0),
            axis=1,
        )

    shares = fitted.div(fitted.sum(axis=0).replace(0, np.nan), axis=1).fillna(0.0)
    stacked = pd.Series(
        shares.rename_axis(index='category', columns='commodity').stack()
    )
    long = stacked.rename('share').reset_index()
    long = long[long['share'] > 1e-6]
    return long.sort_values(['commodity', 'category'])


def main() -> int:
    for direction, (output, *_rest) in DIRECTIONS.items():
        table = build(direction)
        out = Path(output)
        out.parent.mkdir(parents=True, exist_ok=True)
        table.to_csv(out, index=False, float_format='%.6f')
        check = table.groupby('commodity')['share'].sum()
        print(
            f'{direction}: {len(table)} coefficients for {check.size} '
            f'commodities -> {out}; worst share-sum deviation from 1: '
            f'{float((check - 1).abs().max()):.2e}'
        )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
