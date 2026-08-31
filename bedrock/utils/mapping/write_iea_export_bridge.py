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

OUTPUT = Path('bedrock/analysis/nowcasting/trade_data/iea_export_bridge.csv')
CROSSWALK = Path(
    'bedrock/utils/mapping/activitytosectormapping/Sector_Crosswalk_BEA_IEA_exports.csv'
)
IEA_2017 = Path('bedrock/extract/input_data/BEA_IEA/2017/BEA_IEA_2017_Exports.csv')

#: RAS sweeps.  The support is ~45 x 90 and converges in tens of sweeps; the
#: cap is generous because the margins are inconsistent and never fully close.
SWEEPS = 500


def category_totals_2017() -> pd.Series:
    frame = pd.read_csv(IEA_2017)
    frame = frame[
        (frame['TradeDirection'] == 'Exports')
        & (frame['Affiliation'] == 'AllAffiliations')
        & (frame['AreaOrCountry'] == 'AllCountries')
    ]
    return frame.set_index('TypeOfService')['DataValue'].astype(float)


def build() -> pd.DataFrame:
    crosswalk = pd.read_csv(CROSSWALK, dtype=str)
    totals = category_totals_2017()
    categories = sorted(set(crosswalk['Activity']) & set(totals.index))

    use = _load_2017_detail_supply_use_usa('Use_SUT_detail')
    use.columns = use.columns.str.strip()
    published = pd.to_numeric(use['F04000'], errors='coerce').fillna(0.0)

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
    table = build()
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    table.to_csv(OUTPUT, index=False, float_format='%.6f')
    check = table.groupby('commodity')['share'].sum()
    print(
        f'wrote {len(table)} coefficients for {check.size} commodities to '
        f'{OUTPUT}; worst share-sum deviation from 1: '
        f'{float((check - 1).abs().max()):.2e}'
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
