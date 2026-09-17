"""Which NEI pollutant, if any, stands in for combustion CO2 where NEI lacks it?

`#919 <https://github.com/cornerstone-data/bedrock/issues/919>`_. NEI reports CO2
for 15,430 stationary-combustion facilities and omits it for 27,643 more, which
carry 20.9% of combustion mass. Those are the ones needing an estimate, and the
question is what to estimate them from.

⚠️ **The evaluation unit is a sector total, not a facility.** Per facility no
pollutant works - regressing GHGRP subpart C CO2e on NEI combustion NOx gives an
r-squared of 0.03 pooled, and the CO2-per-NOx ratio spreads 10x to 60,000x even
within a single dominant SCC, because NOx tracks combustion controls as much as
fuel carbon. An allocation vector needs sector sums, where the facility-level
error averages out, and that is what is measured here.

⚠️ **Choosing the proxy on the data it is scored against inflates the result** -
16.2% median error that way against 19.4% honest. So each repeat splits three
ways: fit the sector CO2-per-proxy ratio on A, *choose* the proxy on B, evaluate
the held-out CO2 total on C. Nothing that picks the proxy sees the evaluation
half.

The finding is that per-sector selection buys 1.3 percentage points over simply
using NOx everywhere (19.4% against 20.7%), so the proxy is worth keeping
configurable but is not where the accuracy lives. What matters more is the
**quality gate**: sectors range from 2.8% error to over 55%, and the ones that
cannot be estimated should be left to the existing method rather than filled
badly.

Run it directly; it needs a network on a cold stewi cache::

    python -m bedrock.analysis.time_series_B_matrix.nei_combustion_proxy_test
"""

import logging

import numpy as np
import pandas as pd
import stewi

logging.disable(logging.INFO)
pd.set_option('display.width', 220)

CANDIDATES = [
    'Nitrogen Oxides',
    'Carbon Monoxide',
    'Sulfur Dioxide',
    'Volatile Organic Compounds',
    'PM10 Primary (Filt + Cond)',
    'PM2.5 Primary (Filt + Cond)',
]
REPEATS = 40

proc = stewi.getInventory('NEI', 2022, 'flowbyprocess', download_if_missing=True)
comb = proc[proc['Process'].astype(str).str[0].isin(['1', '2'])]
fac = stewi.getInventoryFacilities('NEI', 2022, download_if_missing=True)[
    ['FacilityID', 'NAICS']
]
wide = (
    comb[comb['FlowName'].isin(CANDIDATES + ['Carbon Dioxide'])]
    .groupby(['FacilityID', 'FlowName'])['FlowAmount']
    .sum()
    .unstack('FlowName')
    .join(fac.set_index('FacilityID'))
)
wide['n3'] = wide['NAICS'].astype(str).str[:3]
wide = wide[wide['Carbon Dioxide'] > 0]

rng = np.random.default_rng(1)


def predict(fit: pd.DataFrame, held: pd.DataFrame, proxy: str) -> float | None:
    if fit[proxy].sum() <= 0 or held['Carbon Dioxide'].sum() <= 0:
        return None
    ratio = fit['Carbon Dioxide'].sum() / fit[proxy].sum()
    return abs(held[proxy].sum() * ratio / held['Carbon Dioxide'].sum() - 1)


rows = []
for sector, group in wide.groupby('n3'):
    group = group[group[CANDIDATES].notna().all(axis=1)]
    if len(group) < 90:
        continue
    chosen: list[str] = []
    errs: dict[str, list[float]] = {name: [] for name in CANDIDATES + ['PICKED']}
    for _ in range(REPEATS):
        order = rng.permutation(len(group))
        third = len(group) // 3
        a, b, c = (
            group.iloc[order[:third]],
            group.iloc[order[third : 2 * third]],
            group.iloc[order[2 * third :]],
        )
        scored = {p: predict(a, b, p) for p in CANDIDATES}
        scores: dict[str, float] = {p: v for p, v in scored.items() if v is not None}
        if not scores:
            continue
        pick = min(scores, key=lambda p: scores[p])
        chosen.append(pick)
        for p in CANDIDATES:
            v = predict(a, c, p)
            if v is not None:
                errs[p].append(v)
        v = predict(a, c, pick)
        if v is not None:
            errs['PICKED'].append(v)
    row = {'sector': sector, 'n': len(group)}
    row.update({p: np.median(v) if v else np.nan for p, v in errs.items()})
    row['modal_pick'] = pd.Series(chosen).mode().iloc[0] if chosen else None
    rows.append(row)

out = pd.DataFrame(rows).set_index('sector')
short = {c: c.split()[0][:7] for c in CANDIDATES}
disp = (out[['n'] + CANDIDATES + ['PICKED']] * 1).copy()
for c in CANDIDATES + ['PICKED']:
    disp[c] = (out[c] * 100).round(1)
disp['modal_pick'] = out['modal_pick'].map(lambda s: short.get(s, s))
print('Honest three-way split: fit on A, choose proxy on B, evaluate on C.')
print('Error % on a held-out sector CO2 total, median of 40 repeats.\n')
print(disp.rename(columns=short).to_string())
print('\nmedian across sectors:')
med = {short.get(c, c): round(float(np.nanmedian(out[c])) * 100, 1) for c in CANDIDATES}
med['PICKED'] = round(float(np.nanmedian(out['PICKED'])) * 100, 1)
print(pd.Series(med).sort_values().to_string())
