"""
Does a PxI-built commodity mix reproduce the published 2017 mix? (Step 4a, #570)

The decisive test for using ``Census_EC_PxI`` to move the mix: build each
industry's commodity composition from Economic Census product lines through the
services concordance, and compare it against the published 2017 detail Supply
block, which is the answer.

⚠️ **This is the test the level comparison could not be.** PxI totals differ
from the Supply column by construction — PxI is the raw product data BEA starts
from, before imputations, nonemployer and tax-misreporting coverage, cost of
resales and the secondary in/out adjustments, and it is a weighted sample. Those
adjustments are largely industry-level and scale a whole column, so the *shares*
can be right where the level is not. Only this test can say whether they are.

Reported per industry:

``coverage``
    mapped PxI value over the Supply column total. Not expected to be 1.0, for
    the reasons above; it says how much of the industry the concordance sees.
``L1``
    half the sum of absolute share differences — the fraction of the mix that
    would have to move to match the published one. 0.0 is exact, 1.0 is disjoint.

Run: ``uv run python bedrock/analysis/nowcasting/pxi_mix_test.py``
"""

from __future__ import annotations

import argparse

import pandas as pd

from bedrock.analysis.nowcasting.frozen_mix_diagnostic import detail_block
from bedrock.extract.flowbyactivity import getFlowByActivity

NAICS_TO_BEA = 'bedrock/utils/mapping/naics/NAICS_to_BEA_Crosswalk_2017.csv'
SEED = 'bedrock/utils/mapping/census_pxi/pxi_services_product_seed_2017.csv'
CORRECTIONS = (
    'bedrock/utils/mapping/census_pxi/pxi_services_product_corrections_2017.csv'
)

#: Seed rows carrying one of these are not usable as a product → commodity map.
#: ``review-no-dominant`` rows are superseded by the corrections file, which is
#: where the reviewed target (and any split) lives.
EXCLUDED_FLAGS = frozenset(
    {'nonproduct', 'trade-margin', 'review-no-dominant', 'own-commodity'}
)

#: ⚠️ ``Census_EC_PxI`` carries an **all-sectors total** under
#: ``ActivityProducedBy == '00'`` — 34.36tn beside 32.89tn of six-digit detail,
#: 51.1% of the file. Any groupby that does not filter to six digits sums the
#: total alongside the detail it totals.
NAICS_CODE_LENGTH = 6


def concordance() -> (
    tuple[dict[str, list[tuple[str, float]]], set[str], dict[tuple[str, str], str]]
):
    """
    Product description → [(BEA 2017 detail commodity, weight)], plus the
    ``own-commodity`` set.

    ⚠️ **A global product → commodity map cannot express a product that is
    primary to several industries at once.** "Patient care, related to ICD-10
    major category" is the main output of physicians' offices, outpatient
    centres, home health *and* hospitals — four different BEA commodities.
    Mapping it globally to whichever sells most makes every other one appear to
    produce hospital output and none of its own: all three scored ``L1 = 1.000``
    against the published block before this class existed.

    So ``own-commodity`` products carry no target. They resolve **against the
    seller**, which is the manual's own framing — the question is which products
    are *secondary to an industry*, not what a product is in the abstract.
    """
    seed = pd.read_csv(SEED)
    # ⚠️ dtype=str or pandas reads the BEA and industry codes as floats and
    # '515200' silently becomes '515200.0', which matches nothing
    corrections = pd.read_csv(CORRECTIONS, dtype={'bea': str, 'industry': str})
    own = set(seed.loc[seed['flag'] == 'own-commodity', 'Description'])
    # rows carrying an industry are per-(industry, product) overrides: the same
    # product is a different commodity depending on who makes it. Cable
    # programming's licensing revenue is the broadcasting commodity in BEA's
    # accounts, while motion picture's licensing is its own output.
    scoped = (
        corrections[corrections.get('industry').notna()]
        if 'industry' in corrections
        else corrections.iloc[:0]
    )
    override = {
        (str(r['industry']).strip(), r['product']): str(r['bea']).strip()
        for _, r in scoped.iterrows()
    }
    corrections = corrections.drop(index=scoped.index)
    mapping = {
        row['Description']: [(str(row['bea_2017_commodity']).strip(), 1.0)]
        for _, row in seed.iterrows()
        if row['flag'] not in EXCLUDED_FLAGS and not pd.isna(row['bea_2017_commodity'])
    }
    for product, group in corrections.groupby('product'):
        if product in own:
            continue  # the seller resolves it; a fixed split would override that
        mapping[product] = [
            (str(b).strip(), float(w)) for b, w in zip(group['bea'], group['weight'])
        ]
    return mapping, own, override


def built_mix() -> pd.Series:
    """PxI product value pushed through the concordance, by (industry, commodity)."""
    crosswalk = pd.read_csv(NAICS_TO_BEA, dtype=str).dropna(
        subset=['NAICS_2017_Code', 'BEA_2017_Detail_Code']
    )
    naics_to_bea = (
        crosswalk.assign(n=crosswalk['NAICS_2017_Code'].str.strip())
        .groupby('n')['BEA_2017_Detail_Code']
        .first()
        .str.strip()
        .to_dict()
    )
    mapping, own, override = concordance()

    pxi = getFlowByActivity('Census_EC_PxI', 2017)
    pxi['naics'] = pxi['ActivityProducedBy'].astype(str)
    pxi = pxi[
        (pxi['FlowAmount'] > 0)
        & (pxi['naics'].str.len() == NAICS_CODE_LENGTH)
        & (pxi['Description'].isin(mapping) | pxi['Description'].isin(own))
    ]
    pxi['industry'] = pxi['naics'].map(naics_to_bea)
    pxi = pxi[pxi['industry'].notna()]

    rows = []
    for industry, group in pxi.groupby('industry'):
        for description, value in (
            group.groupby('Description')['FlowAmount'].sum().items()
        ):
            if (industry, description) in override:
                rows.append((industry, override[(industry, description)], value))
                continue
            if description in own:
                # primary to this industry: it is this industry's own commodity
                rows.append((industry, industry, value))
                continue
            for commodity, weight in mapping[description]:
                rows.append((industry, commodity, value * weight))
    return (
        pd.DataFrame(rows, columns=['industry', 'commodity', 'value'])
        .groupby(['industry', 'commodity'])['value']
        .sum()
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--min-coverage',
        type=float,
        default=0.5,
        help='only report industries the concordance sees at least this much of. '
        'Below about 0.5 the mix is an artifact of what happens to be mapped '
        'rather than a measurement: 511110 and 511120 enter at 44%% carrying '
        'only their advertising lines, so 541800 looks like their main output',
    )
    args = parser.parse_args()

    built = built_mix()
    block = detail_block()
    industries = sorted(set(built.index.get_level_values(0)) & set(block.columns))

    print(f'{"industry":<10}{"coverage":>10}{"top commodity":>26}{"L1":>8}')
    scored = []
    for industry in industries:
        column = block[industry]
        if column.sum() <= 0:
            continue
        published = column[column > 0] / column[column > 0].sum()
        estimate = built.loc[industry]
        coverage = estimate.sum() / 1e6 / column.sum()
        if coverage < args.min_coverage:
            continue
        estimate = estimate / estimate.sum()
        index = published.index.union(estimate.index)
        l1 = (
            estimate.reindex(index).fillna(0) - published.reindex(index).fillna(0)
        ).abs().sum() / 2
        top = (
            'same'
            if estimate.idxmax() == published.idxmax()
            else f'{estimate.idxmax()} vs {published.idxmax()}'
        )
        scored.append(l1)
        print(f'{industry:<10}{coverage:>9.1%}{top:>26}{l1:>8.3f}')

    if scored:
        series = pd.Series(scored)
        print(
            f'\n{len(series)} industries | median L1 {series.median():.3f} | '
            f'under 0.05: {(series < 0.05).sum()} | over 0.30: {(series > 0.30).sum()}'
        )


if __name__ == '__main__':
    main()
