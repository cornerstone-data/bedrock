"""Would 2022 manufacturing output be very different if EC 2022 were used? (#724)

The GO control (:mod:`~bedrock.transform.iot.nowcast_supply_go_control`) pins the
Supply detail industry axis to BEA's detail gross output — but for 2022+ that
series is a best-change extrapolation that **has not seen the 2022 Economic
Census**, and neither has the summary Supply block that levels it. Wes's
question, scoped to manufacturing first: measure how different 2022 (and so
2023, which BEA chains off 2022) would be if the EC were used.

Levels cannot answer this — the EC-to-BEA level wedge *is* BEA's adjustment set
(misclassification, own-account software, nonemployers), which is #724's larger
program. **Growth can**: comparing each industry's EC 2017→2022 shipments growth
against BEA's GO growth cancels whatever part of the wedge is stable across the
two census years. That is the same chain-don't-substitute logic the Step 4a mix
work established twice.

The data was already on disk
-----------------------------

#724 recorded "we do not pull the dataset C1 points at" — true of a *product*
table but not of the industry statistics: ``Census_EC_Expenses`` pulls
``ecnbasic`` **RCPTOT** (value of shipments/receipts, thousand USD) for 2017 and
2022 at every NAICS level. The 360 six-digit 2017 manufacturing industries sum
to **5,502bn against BEA's 5,464bn GO — a 0.7% wedge**, against the 36% apparent
wedge the suppressed PxI product lines produced. The blocker in the issue is a
blocker for product detail, not for this check.

Construction
------------

1. **Units that survive the NAICS vintage change.** 330 six-digit codes exist
   identically in both vintages and are their own units. The rest (30 codes at
   440bn in 2017, 16 at 538bn in 2022 - the 315/316 apparel-leather
   restructure, ``336111/336112`` -> ``336110`` auto assembly,
   ``322121/322122`` -> ``322120`` paper mills, and the
   ``3331xx``/``3352xx``/``3346xx`` merges) are healed by taking the **whole
   five-digit family as one unit** at its published parent row, consuming its
   matched siblings. Whole-family rather than parent-minus-matched-children,
   because the revision also makes one-sided carve-outs (``325315`` compost is
   new in 2022 out of siblings that all still exist), where a residual has a
   2022 value and no 2017 base; the family total is published in both years,
   so its growth is always defined. ``bridged_share`` marks the rows priced
   this way.

2. **Pooled, fixed-share allocation onto BEA.** A unit lands on the union of
   the BEA detail industries its 2017 members map to, split by those BEA
   industries' **published 2017 GO shares**, held fixed for both years - the
   split BEA itself uses when re-basing across NAICS vintages. Pooled at the
   unit level because census and BEA disagree about closely-related splits even
   in the benchmark year (census books 2.5x BEA's level on ``336111``
   automobiles, and correspondingly less on light trucks); pooling keeps the
   growth census-measured while the split stays BEA's, which is the only part
   BEA is authoritative on here. Inside a unit every BEA industry receives the
   same EC growth - unavoidable, since the EC does not see below six-digit
   NAICS.

3. **Compare growth, then re-level.** ``implied_2022 = GO_2017 x g_EC`` per
   BEA industry, rescaled to BEA's 2022 manufacturing total so only the *mix*
   is scored; the total-level gap is reported separately.

What it found (2026-08-30, FBAs ``50d6606``)
---------------------------------------------

- **Level**: EC manufacturing shipments grew **+27.0%** 2017->2022 against BEA
  detail GO's **+25.6%**. If the EC is right, BEA's 2022 manufacturing level is
  **1.1% short** - the direction a census BEA has not absorbed would produce.
- **Summary-group totals** (the part the summary Supply control levels): mostly
  close - 13 of 19 groups within +/-3%, and the giants nearly on
  (``311FT`` +2.7%, ``324`` -0.7%, ``325`` +0.4%). Worst: ``315AL`` -11.5%
  (small), ``337`` +7.4%, ``333`` +6.0%, ``323`` +5.1%, ``3364OT`` -4.2%.
- **Detail mix** (what the GO control imports): the real finding.
  Value-weighted mean |implied - GO| is **5.1%**; industries off by >5% carry
  **29.5%** of 2022 manufacturing GO; imposing EC growth on the 2017 levels
  reallocates **182,406 $M of 7,132,488** (half-gross). So the census, where it
  exists, disagrees with BEA's extrapolated detail split by several times the
  between-group disagreement - the detail axis is where EC 2022 matters.
- Named, unbridged disagreements with a stable 2017 wedge: ``334118`` computer
  terminals/peripherals (census -49%, BEA -10%), ``327993`` insulation (census
  +27%, BEA +74%), ``336414`` guided missiles (census -17%, BEA +11%),
  ``325120`` industrial gas (census +72%, BEA +39%), ``334112`` storage
  (census -1%, BEA +25%), ``336992`` armored vehicles (census +8%, BEA -11%).
- **2023 inherits all of it**: BEA chains 2023 off 2022, so the 2022
  disagreement carries forward essentially unchanged.

Read the per-industry table with the two caveats printed beside it: shipments
are not gross output (resales, own-account production, inventory valuation all
differ, and ``coverage_2017`` prints each industry's wedge so a moving-wedge
suspicion is checkable), and a unit bridging a restructure hands one growth rate
to every BEA industry inside it.

Run::

    uv run python -m bedrock.analysis.nowcasting.ec_manufacturing_output_check
    uv run python -m bedrock.analysis.nowcasting.ec_manufacturing_output_check --top 30
"""

from __future__ import annotations

import argparse
import functools
import typing as ta

import numpy as np
import pandas as pd

from bedrock.extract.flowbyactivity import getFlowByActivity

# ⚠️ Every read below is the RAW arm: this module measures the census AGAINST
# BEA's unadjusted extrapolation, and it is also what ec_go_adjustment builds
# its factors from -- reading the default arm here would be circular.
from bedrock.transform.iot.derived_intermediate_and_value_added import (
    detail_gross_output_panel,
)
from bedrock.utils.taxonomy.mappings.bea_v2017_industry__bea_v2017_summary import (
    load_bea_v2017_industry_to_bea_v2017_summary,
)

#: The two census vintages. Fixed: this module is a 2017→2022 growth check.
BASE_YEAR, CENSUS_YEAR = 2017, 2022

#: NAICS→BEA crosswalk, the same file the Supply methods use.
CROSSWALK = 'bedrock/utils/mapping/naics/NAICS_to_BEA_Crosswalk_2017.csv'

#: Manufacturing six-digit NAICS.
MANUFACTURING_PREFIXES = ('31', '32', '33')

#: RCPTOT is thousand USD; BEA GO panels are million USD.
THOUSAND_TO_MILLION = 1e-3


def _rcptot(year: int, prefixes: tuple[str, ...] = MANUFACTURING_PREFIXES) -> pd.Series:
    """Published ``RCPTOT`` by NAICS code at every level, thousand USD.

    Read through :func:`getFlowByActivity` rather than a parquet glob so the
    GCS fallback works where the local cache is cold -- CI most of all.
    """
    frame = pd.DataFrame(
        getFlowByActivity('Census_EC_Expenses', int(year), download_FBA_if_missing=True)
    )
    receipts = frame[frame['FlowName'] == 'RCPTOT']
    series = receipts.groupby(receipts['ActivityConsumedBy'].astype(str))[
        'FlowAmount'
    ].sum()
    return series[series.index.str.startswith(prefixes)]


@functools.cache
def _allocation(prefixes: tuple[str, ...] = MANUFACTURING_PREFIXES) -> pd.DataFrame:
    """(naics, bea, share): six-digit NAICS onto BEA detail, 2017-GO-share split.

    The share is **within one NAICS code across the BEA industries it feeds**,
    anchored on published 2017 GO and held fixed for both years, which is what
    BEA itself does when re-basing a benchmark across NAICS vintages.
    """
    crosswalk = pd.read_csv(CROSSWALK, dtype=str)
    six = crosswalk[crosswalk['NAICS_2017_Code'].str.len() == 6][
        ['NAICS_2017_Code', 'BEA_2017_Detail_Code']
    ].dropna()
    six = six[six['NAICS_2017_Code'].str.startswith(prefixes)]
    six = six.drop_duplicates().rename(
        columns={'NAICS_2017_Code': 'naics', 'BEA_2017_Detail_Code': 'bea'}
    )
    go = detail_gross_output_panel(ec_adjusted=False)[BASE_YEAR]
    six['weight'] = six['bea'].map(go).fillna(0.0)
    totals = six.groupby('naics')['weight'].transform('sum')
    # a NAICS whose BEA industries all have zero GO keeps an equal split
    counts = six.groupby('naics')['weight'].transform('size')
    six['share'] = np.where(
        totals > 0, six['weight'] / totals.replace(0, np.nan), 1.0 / counts
    )
    return six[['naics', 'bea', 'share']]


def units(prefixes: tuple[str, ...] = MANUFACTURING_PREFIXES) -> pd.DataFrame:
    """Comparable units across the vintage change: (unit, r17, r22, members17).

    A six-digit code published in both vintages is its own unit — unless its
    five-digit family contains **any** restructured code, in which case the
    whole family becomes one unit at the published parent level, consuming its
    matched siblings.  Whole-family, not parent-minus-matched-children, because
    the revision also makes one-sided carve-outs (``325315`` compost is new in
    2022 out of siblings that all still exist), where a residual has a 2022
    value and no 2017 base.  The family total is published in both years, so
    its growth is always defined; the price is that matched siblings inside a
    restructured family receive the family growth, which the ``bridged`` flag
    marks.  Families still unhealed at five digits escalate to four, then
    three.
    """
    r17, r22 = _rcptot(BASE_YEAR, prefixes), _rcptot(CENSUS_YEAR, prefixes)
    six17 = {c for c in r17.index if len(c) == 6}
    six22 = {c for c in r22.index if len(c) == 6}
    matched = sorted(six17 & six22)
    changed = sorted((six17 - six22) | (six22 - six17))

    # Escalate each changed code to the shortest prefix that is published in
    # both years AND whose six-digit membership under it is identical across
    # vintages once the family is taken whole.
    family_of: dict[str, str] = {}
    for code in changed:
        for width in (5, 4, 3):
            prefix = code[:width]
            if prefix not in r17.index or prefix not in r22.index:
                continue
            family_of[code] = prefix
            break
        else:
            raise ValueError(
                f'{code} has no parent published in both vintages down to three '
                f'digits; the FBA is missing a level it always publishes'
            )
    # A family absorbs every changed code sharing its prefix, so widen each
    # family to the widest prefix any of its members escalated to.
    families = sorted(set(family_of.values()))
    widened: dict[str, str] = {}
    for family in families:
        container = min((f for f in families if family.startswith(f)), key=len)
        widened[family] = container
    family_of = {code: widened[family] for code, family in family_of.items()}
    families = sorted(set(family_of.values()))

    rows = []
    consumed: set[str] = set()
    for family in families:
        members17 = sorted(c for c in six17 if c.startswith(family))
        consumed.update(c for c in matched if c.startswith(family))
        rows.append(
            {
                'unit': f'{family}*',
                'r17': float(r17[family]),
                'r22': float(r22[family]),
                'members17': tuple(members17),
                'bridged': True,
            }
        )
    rows.extend(
        {
            'unit': code,
            'r17': float(r17[code]),
            'r22': float(r22[code]),
            'members17': (code,),
            'bridged': False,
        }
        for code in matched
        if code not in consumed
    )
    return pd.DataFrame(rows)


def implied_bea_growth(
    prefixes: tuple[str, ...] = MANUFACTURING_PREFIXES,
) -> pd.DataFrame:
    """Per BEA detail manufacturing industry: EC growth vs BEA GO growth.

    A unit lands on the **union of the BEA industries its 2017 members map
    to**, split by those BEA industries' published 2017 GO shares.  Pooled at
    the unit level, not member by member: the census and BEA disagree about how
    to split closely-related codes even in the benchmark year — Census books
    2.5x BEA's level on ``336111`` automobiles and correspondingly less on
    ``336112`` light trucks — and a member-weighted allocation imports the
    census's split.  Pooling keeps the *growth* census-measured while the
    *split* stays BEA's own, which is the only part BEA is authoritative on
    here.  Inside a unit every BEA industry then receives the same EC growth.
    """
    allocation = _allocation(prefixes)
    table = units(prefixes)

    pieces: list[pd.DataFrame] = []
    go17 = detail_gross_output_panel(ec_adjusted=False)[BASE_YEAR]
    for row in table.itertuples():
        members = [str(member) for member in ta.cast('tuple[str, ...]', row.members17)]
        reachable = allocation[allocation['naics'].isin(members)]
        if reachable.empty:
            raise ValueError(f'{row.unit} has no BEA mapping in {CROSSWALK}')
        targets = sorted(set(reachable['bea']))
        weights = go17.reindex(targets).fillna(0.0).astype(float)
        weights = (
            weights / weights.sum()
            if float(weights.sum()) > 0
            else pd.Series(1.0 / len(targets), index=targets)
        )
        pieces.append(
            pd.DataFrame(
                {
                    'bea': targets,
                    'r17': row.r17 * weights.to_numpy(),
                    'r22': row.r22 * weights.to_numpy(),
                    'bridged_r17': (row.r17 if row.bridged else 0.0)
                    * weights.to_numpy(),
                }
            )
        )
    by_bea = pd.concat(pieces).groupby('bea')[['r17', 'r22', 'bridged_r17']].sum()

    go = detail_gross_output_panel(ec_adjusted=False)
    frame = by_bea.copy()
    frame['go17'] = go[BASE_YEAR].reindex(frame.index).astype(float)
    frame['go22'] = go[CENSUS_YEAR].reindex(frame.index).astype(float)
    frame = frame.dropna(subset=['go17', 'go22'])
    frame = frame[(frame['go17'] > 0) & (frame['r17'] > 0)]

    frame['g_ec'] = frame['r22'] / frame['r17']
    frame['g_bea'] = frame['go22'] / frame['go17']
    frame['implied22'] = frame['go17'] * frame['g_ec']
    # score the mix only: rescale implied to BEA's own 2022 total
    scale = float(frame['go22'].sum()) / float(frame['implied22'].sum())
    frame['implied22_mix'] = frame['implied22'] * scale
    frame['diff_pct'] = 100 * (frame['implied22_mix'] / frame['go22'] - 1)
    frame['coverage_2017'] = frame['r17'] * THOUSAND_TO_MILLION / frame['go17']
    # share of the industry's receipts that arrived through a vintage-bridged
    # family unit -- a row near 1.0 carries its whole family's growth rather
    # than its own, so a large diff there says "the census cannot see this
    # split", not "the census disagrees about this industry".
    frame['bridged_share'] = frame['bridged_r17'] / frame['r17']
    frame = frame.drop(columns='bridged_r17')
    return frame.sort_values('diff_pct', key=lambda s: s.abs(), ascending=False)


def report(top: int = 15) -> pd.DataFrame:
    frame = implied_bea_growth()
    weights = frame['go22']
    total_ec = float(frame['r22'].sum()) / float(frame['r17'].sum())
    total_bea = float(frame['go22'].sum()) / float(frame['go17'].sum())
    moved = float((frame['implied22_mix'] - frame['go22']).abs().sum()) / 2

    print(
        f'\nEC 2022 vs BEA extrapolation, manufacturing ({len(frame)} BEA detail '
        f'industries, growth 2017->2022)'
    )
    print(
        f'  level: EC shipments growth {100 * (total_ec - 1):+.1f}% vs BEA GO '
        f'{100 * (total_bea - 1):+.1f}%  ->  BEA 2022 manufacturing sits '
        f'{100 * (total_bea / total_ec - 1):+.1f}% off an EC-grown level'
    )
    print(
        f'  mix:   value-weighted mean |diff| '
        f'{np.average(frame["diff_pct"].abs(), weights=weights):.2f}%; '
        f'reallocation if EC growth were imposed {moved:,.0f} $M half-gross '
        f'of {weights.sum():,.0f}'
    )
    for threshold in (2, 5, 10):
        hit = frame['diff_pct'].abs() > threshold
        print(
            f'  industries |diff| > {threshold:>2}%: {int(hit.sum()):>3} of '
            f'{len(frame)}, carrying {100 * weights[hit].sum() / weights.sum():.1f}% '
            f'of 2022 GO'
        )
    print(f'\n  worst {top} (GO 2022 > 5bn):')
    big = frame[frame['go22'] > 5_000]
    print(
        big[
            [
                'go17',
                'go22',
                'g_ec',
                'g_bea',
                'diff_pct',
                'coverage_2017',
                'bridged_share',
            ]
        ]
        .head(top)
        .round(3)
        .to_string()
    )

    # The summary-group rollup is the half that speaks to the *summary Supply*
    # control totals: at detail the industry axis is the GO control's, but the
    # group totals the whole build hangs off are the summary block's, and this
    # is what the EC says about them.
    parents = {
        code: parent[0]
        for code, parent in load_bea_v2017_industry_to_bea_v2017_summary().items()
    }
    groups = (
        frame.assign(g=pd.Series(frame.index, index=frame.index).map(parents))
        .groupby('g')[['r17', 'r22', 'go17', 'go22']]
        .sum()
    )
    groups['g_ec'] = groups['r22'] / groups['r17']
    groups['g_bea'] = groups['go22'] / groups['go17']
    groups['diff_pct'] = 100 * (groups['g_ec'] / groups['g_bea'] - 1)
    print('\n  summary groups (EC growth vs BEA GO growth; diff is the group')
    print('  total the summary Supply control would need to move):')
    print(
        groups[['go22', 'g_ec', 'g_bea', 'diff_pct']]
        .sort_values('diff_pct', key=lambda s: s.abs(), ascending=False)
        .round(3)
        .to_string()
    )
    return frame


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--top', type=int, default=15)
    args = parser.parse_args()
    pd.set_option('display.width', 240)
    report(args.top)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
