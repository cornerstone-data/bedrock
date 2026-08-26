"""
Can an annual survey move the Supply mix between censuses? (Step 4a, #570)

Step 4a now builds the commodity mix from the Economic Census in 2017 and 2022
(``pxi_mix_test.py``). Every year in between — and after — carries the last
census mix unchanged. The obvious way to fill that gap is the Annual Survey of
Manufactures: ``Census_ASM_PxI`` publishes industry x product value of shipments
for 2018-2021 on the **same 2017 NAPCS collection codes** the Economic Census
uses, so it needs no second concordance and no vintage bridge. It is the
strongest annual product-line instrument that exists — manufacturing's own
census-run survey, on the identical code family.

**It does not work, and the reason generalises.** ASM and the Economic Census
agree closely on what the mix *is* and carry no common signal about how it
*moves*. Both directions of the test are null:

- ASM's own 2018 -> 2021 movement is essentially uncorrelated with the movement
  the 2017 -> 2022 censuses measured (r = 0.13; on the cells the census moved
  most, the two instruments agree on *sign* only 43% of the time). Chaining ASM
  into the 2017 mix makes the 2022 estimate **worse** than holding 2017.
- Interpolating between the two censuses is a coin flip against holding 2017
  when graded on ASM's own annual observation — 90-97 wins of 181 industries in
  every year.

The number that explains both: at BEA detail the manufacturing mix travels a
median ``L1`` of **0.0122 in five years**, while the two instruments sit
**0.0116** apart on the same industry in adjacent years. The signal is at the
noise floor of the instruments measuring it. No amount of annual data fixes
that, because the disagreement is not sampling error that averages away — it is
what each survey means by a product line.

✅ **So hold the manufacturing mix between censuses.** That is not a failure to
find data; it is a measurement saying the correction is not there to make.

⚠️ **This bounds the other annual sources rather than dismissing them.** If the
best-placed annual product survey has no usable timing signal for the mix, the
weaker Step 3 candidates are unlikely to beat it *on manufacturing*. It says
nothing about agriculture, where ERS publishes cash receipts by commodity and
the mix is a genuinely different shape — see the plan.

⚠️ **Three traps, all of which faked a result before being fixed:**

1. **Suppression is not stable across years**, so a mix built from whatever
   published in each year moves because Census withheld differently, not
   because the economy did. 2018 publishes 96.7% of the industry control and
   2019-2021 publish 79-82%. Every mix here is built on the **common support**
   — the 3,490 (industry, product) cells published in all four years — which
   holds a steady 74.6-75.1% of control in every year.
2. **The least-suppressed NAICS level reverses when the industry axis is kept.**
   Commodity *output* sums the industry axis away and so takes five-digit,
   where more value publishes (5.7tn against 5.0tn in 2021). A *mix* needs the
   industry, and 87 of 709 five-digit codes straddle two BEA industries: at
   five-digit only 3.3tn resolves to a single BEA industry against 4.8tn at
   six-digit. This builds at six.
3. **Suppression recovery guesses on exactly the axis the mix needs.**
   :func:`Census_ASM.estimate_suppressed_asm_pxi` distributes an industry's
   withheld residual equally across its withheld products — which fixes the
   industry level and invents the commodity split. Nothing here uses it.

Run: ``uv run python bedrock/analysis/nowcasting/annual_mix_test.py``
"""

from __future__ import annotations

import argparse

import numpy as np
import pandas as pd

from bedrock.analysis.nowcasting.frozen_mix_diagnostic import detail_block
from bedrock.analysis.nowcasting.pxi_mix_test import industry_map
from bedrock.extract.flowbyactivity import getFlowByActivity

NAPCS_TO_BEA = 'bedrock/utils/mapping/census_pxi/napcs_to_bea_2017.csv'
NAPCS_BRIDGE = 'bedrock/utils/mapping/census_pxi/napcs_2022_to_2017.csv'
NAICS_TO_BEA = 'bedrock/utils/mapping/naics/NAICS_to_BEA_Crosswalk_2017.csv'

#: The all-products row. Published for every industry at every NAICS level, so
#: it is the suppression control — and an aggregate of the detail beside it,
#: which is why it is filtered out of every mix built here.
PRODUCT_TOTAL = '0000000000'

#: The years ASM publishes product lines that we hold. ASM is not conducted in
#: an Economic Census year, so 2022 is absent by design and 2017 has no ASM.
ASM_YEARS = (2018, 2019, 2020, 2021)

#: ⚠️ Six, not the five-digit level commodity output takes. See trap 2 above.
NAICS_CODE_LENGTH = 6

#: The 19 BEA summary groups that make up manufacturing. Named rather than
#: matched by shape: ``3361MV`` and ``3364OT`` do not start with a NAICS
#: two-digit prefix, and ``311FT``/``313TT``/``315AL`` are not numeric.
MANUFACTURING = frozenset(
    {
        '311FT',
        '313TT',
        '315AL',
        '321',
        '322',
        '323',
        '324',
        '325',
        '326',
        '327',
        '331',
        '332',
        '333',
        '334',
        '335',
        '3361MV',
        '3364OT',
        '337',
        '339',
    }
)


def goods_concordance() -> pd.DataFrame:
    """NAPCS collection code -> BEA 2017 detail commodity, with split weights.

    The goods counterpart to ``pxi_mix_test.concordance()``, which is keyed on
    the *description* text because the services seed was reviewed that way.
    This one is keyed on the **code**, which is what lets ASM reuse it: ASM
    publishes no description at all.
    """
    frame = pd.read_csv(NAPCS_TO_BEA, dtype={'napcs_code': str})
    frame['bea'] = frame['bea_2017_commodity'].astype(str).str.strip()
    return frame[['napcs_code', 'bea', 'weight']]


def _bea_industry() -> tuple[dict[str, str], dict[str, str]]:
    """NAICS 2017 -> BEA detail industry (unambiguous only), and detail -> summary.

    ⚠️ A NAICS code that straddles two BEA industries is **dropped**, not split.
    There is no basis in this data for a split, and the alternative — taking the
    first match — silently assigns a whole industry's product lines to a
    neighbour. 48 of 1,061 six-digit codes are dropped this way.
    """
    crosswalk = pd.read_csv(NAICS_TO_BEA, dtype=str).dropna(
        subset=['NAICS_2017_Code', 'BEA_2017_Detail_Code']
    )
    crosswalk = crosswalk.assign(
        n=crosswalk['NAICS_2017_Code'].str.strip(),
        b=crosswalk['BEA_2017_Detail_Code'].str.strip(),
        s=crosswalk['BEA_2017_Summary_Code'].str.strip(),
    )
    unique = crosswalk.groupby('n')['b'].nunique()
    to_industry = (
        crosswalk[crosswalk['n'].isin(unique[unique == 1].index)]
        .drop_duplicates('n')
        .set_index('n')['b']
        .to_dict()
    )
    to_summary = crosswalk.drop_duplicates('b').set_index('b')['s'].to_dict()
    return to_industry, to_summary


def _rows(source: str, year: int) -> pd.DataFrame:
    """Six-digit product detail from a PxI source, totals and zeros stripped."""
    frame = getFlowByActivity(source, year)
    frame = frame.copy()
    frame['naics'] = frame['ActivityProducedBy'].astype(str).str.strip()
    frame['code'] = frame['FlowName'].astype(str).str.strip()
    # ⚠️ match ^\d+$ before selecting a level: ASM's '31-33' rollup is five
    # characters and survives a length filter alongside real five-digit NAICS.
    frame = frame[frame['naics'].str.match(rf'^\d{{{NAICS_CODE_LENGTH}}}$')]
    return frame[frame['code'] != PRODUCT_TOTAL]


def common_support(years: tuple[int, ...] = ASM_YEARS) -> set[tuple[str, str]]:
    """The (NAICS, product) cells ASM published in **every** year in ``years``.

    ⚠️ The point of the restriction. ASM withholds a cell when publishing it
    would disclose a company, and what it withholds changes year to year — 2018
    publishes 96.7% of the industry control against 79.0% in 2019. A mix built
    on each year's own publication therefore moves when Census changes its mind,
    which is indistinguishable from the economy moving. On the common support
    the covered fraction of control is flat at 74.6-75.1% across all four years,
    so what is left is the economy.
    """
    support: set[tuple[str, str]] | None = None
    for year in years:
        frame = _rows('Census_ASM_PxI', year)
        published = frame[frame['FlowAmount'] > 0]
        cells = set(zip(published['naics'], published['code']))
        support = cells if support is None else support & cells
    return support or set()


def _build(
    frame: pd.DataFrame,
    to_industry: dict[str, str],
    code_column: str = 'code',
) -> pd.Series:
    """Product value pushed through the goods concordance, by (industry, commodity)."""
    frame = frame.copy()
    frame['industry'] = frame['naics'].map(to_industry)
    frame = frame[frame['industry'].notna()]
    mapped = frame.merge(
        goods_concordance(), left_on=code_column, right_on='napcs_code'
    )
    mapped['value'] = mapped['FlowAmount'] * mapped['weight']
    return mapped.groupby(['industry', 'bea'])['value'].sum()


def asm_mix(year: int, support: set[tuple[str, str]]) -> pd.Series:
    """ASM's commodity mix for ``year``, on the cross-year common support."""
    frame = _rows('Census_ASM_PxI', year)
    keep = [(n, c) in support for n, c in zip(frame['naics'], frame['code'])]
    to_industry, _ = _bea_industry()
    return _build(frame[keep], to_industry)


def census_mix(year: int) -> tuple[pd.Series, set[str]]:
    """The Economic Census mix for ``year``, and the industries whose boundary moved.

    2022 needs both vintage bridges — ``napcs_2022_to_2017.csv`` for the product
    codes and the NAICS concordance for the industries, via
    ``pxi_mix_test.industry_map``. Only 10.5% of 2022 manufacturing value sits on
    a code that is unchanged since 2017, so the product bridge is doing nearly
    all the work; :func:`bridge_audit` measures whether that manufactures the
    movement it is used to detect.
    """
    to_industry, _ = _bea_industry()
    frame = _rows('Census_EC_PxI', year)
    frame = frame[frame['FlowAmount'] > 0]
    if year == 2017:
        return _build(frame, to_industry), set()

    resolved, boundary = industry_map(year)
    bridge = pd.read_csv(NAPCS_BRIDGE, dtype={'code_2022': str, 'code_2017': str})
    parents = bridge.groupby('code_2022')['code_2017'].nunique()
    # a 2022 code with several 2017 parents cannot be resolved to one goods
    # concordance row without a split this data cannot support
    single = bridge[bridge['code_2022'].isin(parents[parents == 1].index)]
    to_2017 = (
        single.drop_duplicates('code_2022')
        .set_index('code_2022')['code_2017']
        .to_dict()
    )
    known = set(goods_concordance()['napcs_code'])
    frame = frame.copy()
    frame['code17'] = [
        code if code in known else to_2017.get(code) for code in frame['code']
    ]
    frame = frame[frame['code17'].notna()]
    return _build(frame, resolved, code_column='code17'), boundary


def shares(values: pd.Series) -> pd.Series:
    return values / values.sum()


def l1(left: pd.Series, right: pd.Series) -> float:
    """Half the sum of absolute share differences, over the union of the index."""
    index = left.index.union(right.index)
    return float(
        (left.reindex(index).fillna(0) - right.reindex(index).fillna(0)).abs().sum() / 2
    )


def _panel(
    support: set[tuple[str, str]],
) -> tuple[dict[int, pd.Series], pd.Series, pd.Series, list[str], dict[str, str]]:
    """Every mix this script compares, on the industries all of them cover."""
    _, to_summary = _bea_industry()
    annual = {year: asm_mix(year, support) for year in ASM_YEARS}
    base, _ = census_mix(2017)
    later, boundary = census_mix(2022)

    covered = set(base.index.get_level_values(0)) & set(later.index.get_level_values(0))
    for mix in annual.values():
        covered &= set(mix.index.get_level_values(0))
    industries = [
        industry
        for industry in sorted(covered)
        if to_summary.get(industry) in MANUFACTURING and industry not in boundary
        # ⚠️ a one-commodity mix has share 1.0 in every vintage: its movement is
        # 0.000 by construction and averaging it in drags every median to zero
        and len(base.loc[industry]) > 1
    ]
    return annual, base, later, industries, to_summary


def report_coverage(support: set[tuple[str, str]]) -> None:
    """What each year publishes, and what the common support holds of it."""
    print('\nASM publication and the common support')
    print(f'{"":<3}{"year":<7}{"published":>11}{"common":>10}{"of control":>12}')
    for year in ASM_YEARS:
        frame = getFlowByActivity('Census_ASM_PxI', year)
        frame['naics'] = frame['ActivityProducedBy'].astype(str).str.strip()
        frame['code'] = frame['FlowName'].astype(str).str.strip()
        frame = frame[frame['naics'].str.match(rf'^\d{{{NAICS_CODE_LENGTH}}}$')]
        control = frame[frame['code'] == PRODUCT_TOTAL]['FlowAmount'].sum()
        detail = frame[frame['code'] != PRODUCT_TOTAL]
        published = detail[detail['FlowAmount'] > 0]['FlowAmount'].sum()
        keep = [(n, c) in support for n, c in zip(detail['naics'], detail['code'])]
        common = detail[keep]['FlowAmount'].sum()
        print(
            f'{"":<3}{year:<7}{published / control:>10.1%}'
            f'{common / 1e9:>9.2f}tn{common / control:>11.1%}'
        )
    print(f'{"":<3}{len(support)} cells published in all {len(ASM_YEARS)} years')
    print(
        '  ! the published share swings 17 points across the years and the '
        'common share does not: that is why the mix is built on the latter'
    )


def report_agreement(
    annual: dict[int, pd.Series], base: pd.Series, industries: list[str]
) -> None:
    """Do the two instruments agree about what the mix *is*?

    The precondition for everything else. If ASM and the Economic Census
    disagreed about the level of the mix there would be no point asking whether
    ASM's movement is usable — but they agree far more closely with each other
    than either does with the published Supply block, which is the signature of
    a shared concordance gap rather than an instrument difference.
    """
    block = detail_block()
    rows = []
    for industry in industries:
        if industry not in block.columns:
            continue
        column = block[industry]
        column = column[column > 0]
        if column.sum() <= 0:
            continue
        rows.append(
            (
                l1(shares(annual[2018].loc[industry]), shares(base.loc[industry])),
                l1(shares(base.loc[industry]), shares(column)),
                l1(shares(annual[2018].loc[industry]), shares(column)),
            )
        )
    frame = pd.DataFrame(rows, columns=['instruments', 'census', 'asm'])
    print(f'\ninstrument agreement, {len(frame)} manufacturing detail industries')
    print(f'{"":<3}ASM 2018 vs census 2017      {frame["instruments"].median():.4f}')
    print(f'{"":<3}census 2017 vs published     {frame["census"].median():.4f}')
    print(f'{"":<3}ASM 2018 vs published        {frame["asm"].median():.4f}')
    print(
        '  ! the instruments are 5x closer to each other than to the published '
        'block: the gap to BEA is the concordance, and both share it'
    )


def report_extrapolation(
    annual: dict[int, pd.Series],
    base: pd.Series,
    later: pd.Series,
    industries: list[str],
) -> None:
    """Graded on the 2022 census: does ASM's movement beat holding 2017?

    ⚠️ **The answer key is an observation BEA has not used.** There is no
    published detail Supply block after 2017, so the 2022 Economic Census is the
    only thing that can grade a 2018-2021 mix estimate at detail.

    Three estimates of the 2022 mix, all scored by ``L1`` against it:

    ``frozen``
        hold the 2017 census mix. What Step 4a does today.
    ``chained``
        the 2017 census mix scaled by the ratio of ASM's 2021 shares to its
        2018 shares. Chaining rather than substituting is deliberate — a
        time-invariant instrument offset cancels in the ratio, so this is the
        form that gives ASM its best chance.
    ``substituted``
        ASM's own 2021 mix, used as-is.
    """
    print('\ngraded on the 2022 census mix')
    rows = []
    for industry in industries:
        target = shares(later.loc[industry])
        anchor = shares(base.loc[industry])
        ratio = (
            shares(annual[2021].loc[industry]) / shares(annual[2018].loc[industry])
        ).replace([np.inf, -np.inf], np.nan)
        chained = (anchor * ratio.reindex(anchor.index)).fillna(0)
        if chained.sum() <= 0:
            continue
        rows.append(
            (
                l1(anchor, target),
                l1(shares(chained), target),
                l1(shares(annual[2021].loc[industry]), target),
                float(later.loc[industry].sum()),
            )
        )
    frame = pd.DataFrame(rows, columns=['frozen', 'chained', 'substituted', 'weight'])
    print(
        f'{"":<3}{len(frame)} industries{"median":>16}{"value-weighted":>18}{"wins":>8}'
    )
    for name in ('frozen', 'chained', 'substituted'):
        wins = (
            ''
            if name == 'frozen'
            else f'{int((frame[name] < frame["frozen"]).sum())}/{len(frame)}'
        )
        weighted = float(np.average(frame[name], weights=frame['weight']))
        print(
            f'{"":<3}{name:<14}{frame[name].median():>16.4f}{weighted:>18.4f}{wins:>8}'
        )
    print(
        '  ! neither ASM form beats holding 2017, on either measure. A coin '
        'flip on wins and worse on the value-weighted mean'
    )


def report_direction(
    annual: dict[int, pd.Series],
    base: pd.Series,
    later: pd.Series,
    industries: list[str],
) -> None:
    """Does ASM move the mix in the *direction* the censuses did?

    The sharper question, and the one that decides whether a damping factor
    could rescue the chain. A movement of the wrong size is fixable; a movement
    of the wrong sign is not.

    Every ASM span is tried, because a single null span invites the obvious
    objection that 2020-2021 is a supply shock and 2022 a rebound. ⚠️ It is not
    that: the **pre-COVID** 2018-2019 span is the *worst* of the six.
    """
    print('\ndirection: each ASM span against the 2017 -> 2022 census movement')
    print(f'{"":<3}{"span":<12}{"r":>8}{"same sign":>11}{"slope":>8}')
    spans = (
        (2018, 2019),
        (2018, 2020),
        (2018, 2021),
        (2019, 2020),
        (2019, 2021),
        (2020, 2021),
    )
    for start, end in spans:
        cells = []
        for industry in industries:
            anchor = shares(base.loc[industry])
            target = shares(later.loc[industry])
            index = anchor.index.union(target.index)
            census = target.reindex(index).fillna(0) - anchor.reindex(index).fillna(0)
            moved = shares(annual[end].loc[industry]).reindex(index).fillna(0) - shares(
                annual[start].loc[industry]
            ).reindex(index).fillna(0)
            cells.append(pd.DataFrame({'census': census, 'asm': moved}))
        frame = pd.concat(cells)
        frame = frame[(frame['census'].abs() > 1e-6) | (frame['asm'].abs() > 1e-6)]
        # the cells that actually moved: sign agreement over the whole panel is
        # dominated by cells where neither instrument moved at all
        big = frame[frame['census'].abs() > 0.005]
        agree = float((np.sign(big['census']) == np.sign(big['asm'])).mean())
        slope = float(
            (frame['census'] * frame['asm']).sum() / (frame['asm'] ** 2).sum()
        )
        print(
            f'{"":<3}{start}-{end:<7}{frame["census"].corr(frame["asm"]):>8.3f}'
            f'{agree:>11.1%}{slope:>8.3f}'
        )
    print(
        '  ! every span is near zero and every sign agreement is below 50%. '
        'On the cells that moved, ASM is worse than a coin at calling which way'
    )


def report_interpolation(
    annual: dict[int, pd.Series],
    base: pd.Series,
    later: pd.Series,
    industries: list[str],
) -> None:
    """The symmetric test: do the censuses predict ASM's interior years?

    Turning the question around removes the objection that ASM was being asked
    to *extrapolate* past its last year. Here the censuses bracket the ASM
    years, which is the easier direction, and the target is ASM's own
    observation — so the cross-instrument offset is common to both estimates
    and cancels out of the comparison.

    Interpolation is geometric, the form
    [[bedrock-step3-interpolation-form]] settled for the intermediate block.
    """
    print('\ngraded on ASM: does interpolating 2017 -> 2022 beat holding 2017?')
    print(f'{"":<3}{"year":<7}{"frozen":>10}{"geometric":>11}{"wins":>10}')
    for year in ASM_YEARS:
        weight = (year - 2017) / 5.0
        frozen, geometric, wins = [], [], 0
        for industry in industries:
            anchor = shares(base.loc[industry])
            end = shares(later.loc[industry])
            index = anchor.index.union(end.index)
            anchor = anchor.reindex(index).fillna(0)
            end = end.reindex(index).fillna(0)
            path = (anchor.clip(lower=1e-12) ** (1 - weight)) * (
                end.clip(lower=1e-12) ** weight
            )
            if path.sum() <= 0:
                continue
            target = shares(annual[year].loc[industry])
            held, moved = l1(anchor, target), l1(shares(path), target)
            frozen.append(held)
            geometric.append(moved)
            wins += moved < held
        print(
            f'{"":<3}{year:<7}{np.median(frozen):>10.4f}{np.median(geometric):>11.4f}'
            f'{f"{wins}/{len(frozen)}":>10}'
        )
    print(
        '  ! a coin flip in every year. The censuses do not predict ASM either, '
        'so neither instrument can arbitrate the other'
    )


def bridge_audit() -> None:
    """Is the 2017 -> 2022 movement real, or an artefact of the product bridge?

    ⚠️ Worth asking before believing any of the above, because only **10.5%** of
    2022 manufacturing value sits on a NAPCS code that is unchanged since 2017 —
    the bridge carries the rest, and a bridge that scrambles products would
    manufacture exactly the movement it is being used to detect.

    ✅ It does not. Built from unchanged codes alone the census movement is
    *larger* (0.0139) than the full bridged build (0.0122), so if anything the
    bridge damps movement. ⚠️ On 10.5% of value this is a thin sample and shows
    direction, not magnitude.
    """
    known = set(goods_concordance()['napcs_code'])
    to_industry, to_summary = _bea_industry()

    later = _rows('Census_EC_PxI', 2022)
    later = later[later['FlowAmount'] > 0].copy()
    direct = later[later['code'].isin(known)].copy()
    share_direct = direct['FlowAmount'].sum() / later['FlowAmount'].sum()

    resolved, _ = industry_map(2022)
    built_later = _build(direct, resolved)
    base = _rows('Census_EC_PxI', 2017)
    base = base[(base['FlowAmount'] > 0) & (base['code'].isin(set(direct['code'])))]
    built_base = _build(base, to_industry)

    both = set(built_base.index.get_level_values(0)) & set(
        built_later.index.get_level_values(0)
    )
    values = [
        l1(shares(built_base.loc[i]), shares(built_later.loc[i]))
        for i in sorted(both)
        if to_summary.get(i) in MANUFACTURING and len(built_base.loc[i]) > 1
    ]
    print('\nis the census movement a bridge artefact?')
    print(f'{"":<3}2022 value on codes unchanged since 2017   {share_direct:>8.1%}')
    print(
        f'{"":<3}movement built from those codes alone      '
        f'{np.median(values):>8.4f}  ({len(values)} industries)'
    )
    print(
        '  ! larger than the bridged build, not smaller. The bridge is damping '
        'the movement, not creating it'
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--section',
        choices=(
            'coverage',
            'agreement',
            'extrapolation',
            'direction',
            'interpolation',
            'bridge',
        ),
        help='run one section rather than all of them',
    )
    args = parser.parse_args()

    support = common_support()
    annual, base, later, industries, _ = _panel(support)
    print(
        f'\n{len(industries)} manufacturing detail industries carried by all six mixes'
    )

    sections = {
        'coverage': lambda: report_coverage(support),
        'agreement': lambda: report_agreement(annual, base, industries),
        'extrapolation': lambda: report_extrapolation(annual, base, later, industries),
        'direction': lambda: report_direction(annual, base, later, industries),
        'interpolation': lambda: report_interpolation(annual, base, later, industries),
        'bridge': lambda: bridge_audit(),
    }
    for name, run in sections.items():
        if args.section in (None, name):
            run()


if __name__ == '__main__':
    main()
