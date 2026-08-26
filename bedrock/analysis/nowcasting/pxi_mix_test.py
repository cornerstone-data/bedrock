"""
Does a PxI-built commodity mix reproduce the published 2017 mix, and does it
move by 2022? (Step 4a, #570)

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

**2022** (``--year 2022``) is the point of the exercise. There is no published
2022 detail Supply block, so ``L1`` there is not an error against an answer —
it is **movement**: how far the mix the Economic Census measures in 2022 has
travelled from the one it measured in 2017. The 2022 Economic Census is an
observation BEA has not used, and the Supply mix is otherwise carried at its
2017 shares.

Two vintage breaks sit between 2017 and 2022, and both were checked before any
2022 number was believed — the project has twice been burnt by a code scheme
that changed underneath a series (the 2012 MatFuel recode, the 2014 EIA form-176
line split):

**Products.** Census renumbers its NAPCS collection codes each census.
``napcs_2022_to_2017.csv`` bridges them; see
``bedrock/utils/mapping/write_napcs_vintage_bridge.py`` for why keying on the
code and bridging beats keying on the description text, and for the audit
showing that no concordance code changed *meaning*.

**Industries.** ⚠️ NAICS was restructured in 2022, and this is the sharper
break of the two. 93 of the 892 six-digit NAICS in the 2022 file — **16% of its
value** — do not exist in NAICS 2017. ``NAICS_Year_Concordance.csv`` resolves
all of them, but resolution is not the same as safety: **55 codes carrying 9.5%
of 2022 value have 2017 parents that land on different BEA industries**, because
NAICS 2022 abolished electronic shopping (``454000``) and distributed it across
retail by merchandise line, and because ``516210`` media streaming draws on four
BEA industries at once. Where that happens the industry boundary itself moved,
so a mix change cannot be told apart from a reclassification. Those industries
are reported as ``boundary`` and excluded from the movement summary rather than
being quietly scored — see :func:`industry_map`.

Run: ``uv run python bedrock/analysis/nowcasting/pxi_mix_test.py``
     ``uv run python bedrock/analysis/nowcasting/pxi_mix_test.py --year 2022``
"""

from __future__ import annotations

import argparse

import pandas as pd

from bedrock.analysis.nowcasting.frozen_mix_diagnostic import (
    detail_block,
    summary_block,
)
from bedrock.extract.flowbyactivity import getFlowByActivity

NAICS_TO_BEA = 'bedrock/utils/mapping/naics/NAICS_to_BEA_Crosswalk_2017.csv'
NAICS_YEARS = 'bedrock/utils/mapping/naics/NAICS_Year_Concordance.csv'
NAPCS_BRIDGE = 'bedrock/utils/mapping/census_pxi/napcs_2022_to_2017.csv'
SEED = 'bedrock/utils/mapping/census_pxi/pxi_services_product_seed_2017.csv'
CORRECTIONS = (
    'bedrock/utils/mapping/census_pxi/pxi_services_product_corrections_2017.csv'
)

#: The vintage the concordance, the crosswalk and the answer key are all in.
BASE_YEAR = 2017

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
        corrections[corrections['industry'].notna()]
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


def _pxi(year: int) -> pd.DataFrame:
    """Six-digit ``Census_EC_PxI`` rows for ``year``, with tidy key columns."""
    pxi = getFlowByActivity('Census_EC_PxI', year)
    pxi = pxi[pxi['FlowAmount'] > 0].copy()
    pxi['naics'] = pxi['ActivityProducedBy'].astype(str).str.strip()
    pxi['product'] = pxi['FlowName'].astype(str).str.strip()
    return pxi[pxi['naics'].str.len() == NAICS_CODE_LENGTH]


def industry_map(year: int) -> tuple[dict[str, str], set[str]]:
    """
    NAICS of ``year`` → BEA 2017 detail industry, and the industries whose
    boundary moved.

    For :data:`BASE_YEAR` this is the crosswalk as-is. For 2022 it composes
    NAICS 2022 → NAICS 2017 → BEA, which is necessary — 16% of the 2022 file is
    in codes NAICS 2017 does not have — but not sufficient.

    ⚠️ **A resolved code is not a safe code.** Where a 2022 NAICS draws on 2017
    parents that belong to *different* BEA industries, the industry boundary
    moved between the two censuses and no split of the value is defensible from
    this data. Those codes are dropped, and every BEA industry they could have
    landed in is returned as ``boundary`` so the caller can refuse to score it.
    """
    crosswalk = pd.read_csv(NAICS_TO_BEA, dtype=str).dropna(
        subset=['NAICS_2017_Code', 'BEA_2017_Detail_Code']
    )
    to_bea = (
        crosswalk.assign(n=crosswalk['NAICS_2017_Code'].str.strip())
        .groupby('n')['BEA_2017_Detail_Code']
        .first()
        .str.strip()
        .to_dict()
    )
    if year == BASE_YEAR:
        return to_bea, set()

    years = pd.read_csv(NAICS_YEARS, dtype=str).dropna(
        subset=['NAICS_2022_Code', 'NAICS_2017_Code']
    )
    resolved: dict[str, str] = {}
    boundary: set[str] = set()
    for code, group in years.assign(
        a=years['NAICS_2022_Code'].str.strip(), b=years['NAICS_2017_Code'].str.strip()
    ).groupby('a'):
        targets = {to_bea[p] for p in set(group['b']) if p in to_bea}
        if len(targets) == 1:
            resolved[str(code)] = targets.pop()
        elif targets:
            # the boundary moved: every candidate industry is now unscoreable
            boundary |= targets
    # a 2022 code that is still a 2017 code keeps its direct reading
    for code, bea in to_bea.items():
        resolved.setdefault(code, bea)
    return resolved, boundary


def product_map(year: int) -> tuple[dict[str, str], int]:
    """
    NAPCS code of ``year`` → the :data:`BASE_YEAR` product description the
    concordance is keyed on, and the count of codes dropped as ambiguous.

    The concordance is a reviewed artefact keyed on 2017 descriptions, so rather
    than restate it per vintage, each year's codes are resolved *back* to a 2017
    description and the existing mapping is reused unchanged. That makes the
    2017 run an identity by construction.
    """
    base = _pxi(BASE_YEAR)
    description = base.groupby('product')['Description'].first().to_dict()
    if year == BASE_YEAR:
        return description, 0

    bridge = pd.read_csv(NAPCS_BRIDGE, dtype={'code_2022': str, 'code_2017': str})
    mapping, own, _ = concordance()
    known = set(mapping) | own

    def target(text: str) -> tuple[str, ...] | None:
        """What the concordance would do with this product, for agreement tests."""
        if text in own:
            return ('own',)
        return tuple(sorted(b for b, _ in mapping[text])) if text in mapping else None

    resolved: dict[str, str] = {}
    dropped = 0
    for code, group in bridge.groupby('code_2022'):
        parents = [
            description[p]
            for p in set(group['code_2017'])
            if p in description and description[p] in known
        ]
        if not parents:
            continue
        # several 2017 products behind one 2022 code is fine while they mean the
        # same thing to the concordance; it is only a problem when they disagree
        if len({target(text) for text in parents}) == 1:
            resolved[str(code)] = parents[0]
        else:
            dropped += 1
    for code, text in description.items():
        resolved.setdefault(code, text)
    return resolved, dropped


def built_mix(year: int = BASE_YEAR) -> tuple[pd.Series, set[str]]:
    """
    PxI product value pushed through the concordance, by (industry, commodity),
    with the industries whose NAICS boundary moved into ``year``.
    """
    to_bea, boundary = industry_map(year)
    to_base_product, dropped = product_map(year)
    mapping, own, override = concordance()

    pxi = _pxi(year)
    pxi['base_product'] = pxi['product'].map(to_base_product)
    pxi = pxi[pxi['base_product'].notna()]
    pxi = pxi[pxi['base_product'].isin(mapping) | pxi['base_product'].isin(own)].copy()
    pxi['industry'] = pxi['naics'].map(to_bea)
    pxi = pxi[pxi['industry'].notna()]

    rows = []
    for industry_key, group in pxi.groupby('industry'):
        industry = str(industry_key)
        for description_key, value in (
            group.groupby('base_product')['FlowAmount'].sum().items()
        ):
            description = str(description_key)
            if (industry, description) in override:
                rows.append((industry, override[(industry, description)], value))
                continue
            if description in own:
                # primary to this industry: it is this industry's own commodity
                rows.append((industry, industry, value))
                continue
            for commodity, weight in mapping[description]:
                rows.append((industry, commodity, value * weight))
    if dropped:
        print(f'  ! {dropped} {year} product codes dropped: 2017 parents disagree')
    return (
        pd.DataFrame(rows, columns=['industry', 'commodity', 'value'])
        .groupby(['industry', 'commodity'])['value']
        .sum(),
        boundary,
    )


def _detail_to_summary() -> dict[str, str]:
    """BEA 2017 detail code → its summary parent, for both axes of the block."""
    crosswalk = pd.read_csv(NAICS_TO_BEA, dtype=str).dropna(
        subset=['BEA_2017_Detail_Code', 'BEA_2017_Summary_Code']
    )
    return (
        crosswalk.assign(
            d=crosswalk['BEA_2017_Detail_Code'].str.strip(),
            s=crosswalk['BEA_2017_Summary_Code'].str.strip(),
        )
        .drop_duplicates('d')
        .set_index('d')['s']
        .to_dict()
    )


def _shares(values: pd.Series) -> pd.Series:
    return values / values.sum()


def _l1(left: pd.Series, right: pd.Series) -> float:
    """Half the sum of absolute share differences, over the union of the index."""
    index = left.index.union(right.index)
    return float(
        (left.reindex(index).fillna(0) - right.reindex(index).fillna(0)).abs().sum() / 2
    )


def adopted_mix(year: int = 2022) -> pd.DataFrame:
    """
    The commodity mix to actually use for ``year``: shares per (industry,
    commodity), with the vintage each column came from.

    ``year`` is newer data, so it is what the mix should be built from wherever
    it is a *measurement*. The vintage guard does not decide whether to adopt —
    it decides which columns can take it:

    ``census``
        the Economic Census measured this industry in both vintages on a stable
        NAICS boundary. Its ``year`` mix is used.
    ``held``
        the boundary moved under it, or it stops appearing in ``year``
        altogether. ⚠️ Its ``year`` number is a **reclassification, not a mix
        change** — ``516210`` streaming draws on four BEA industries at once and
        NAICS 2022 dissolved electronic shopping across retail — so the 2017 mix
        is carried instead. Holding is the conservative reading: the industry
        keeps the last mix that was actually measured on its own boundary.

    Shares, not levels. The Supply column total is set by the commodity-output
    control upstream; this only says how a column divides.
    """
    base, _ = built_mix(BASE_YEAR)
    boundary: set[str] = set()
    if year == BASE_YEAR:
        current = base
    else:
        current, boundary = built_mix(year)

    present = set(current.index.get_level_values(0))
    frames = []
    for industry in sorted(set(base.index.get_level_values(0))):
        usable = industry not in boundary and industry in present
        source = current if usable else base
        shares = _shares(source.loc[industry])
        frames.append(
            pd.DataFrame(
                {
                    'industry': industry,
                    'commodity': shares.index,
                    'share': shares.to_numpy(),
                    'vintage': year if usable else BASE_YEAR,
                    'source': 'census' if usable else 'held',
                }
            )
        )
    return pd.concat(frames, ignore_index=True)


def published_comparison(
    base: pd.Series,
    built: pd.Series,
    boundary: set[str],
    year: int,
    industries: set[str],
) -> None:
    """
    Does the Economic Census say the mix moved further than BEA published?

    The reason to want 2022 at all. BEA carries the Supply mix forward from the
    benchmark with very little movement — across all 71 summary industries the
    published mix travels a median ``L1`` of 0.005 between 2017 and 2022 — and
    the 2022 Economic Census is the one independent observation of whether that
    is the economy holding still or the absence of annual product data.

    ⚠️ **Both sides must be measured in the same space.** The built mix is
    detail × detail and the published block is summary × summary; aggregating
    washes movement out on its own, so comparing the two as they stand would
    manufacture a difference. Both axes are rolled up to summary here.

    ⚠️ **And the roll-up silently re-imports the boundary problem.** A summary
    industry whose detail children are not the *same set* in both years has a
    composition change masquerading as a mix change: ``514`` scores 0.417 that
    way, 38x BEA, purely because ``519130`` stops appearing under it. Those are
    excluded — which is not a neutral loss, since they are the industries that
    actually changed.

    ``industries`` is the set that cleared ``--min-coverage``. Restricting to it
    matters: rolling up every industry the concordance touches drags in columns
    it barely sees, and ``HS`` then scores 0.194 against a published 0.0002 on
    the strength of a handful of mapped products.
    """
    to_summary = _detail_to_summary()
    base = base[base.index.get_level_values(0).isin(industries)]
    built = built[built.index.get_level_values(0).isin(industries)]

    def rolled(mix: pd.Series) -> pd.Series:
        frame = mix.reset_index()
        frame['si'] = frame['industry'].map(to_summary)
        frame['sc'] = frame['commodity'].map(to_summary)
        return frame.dropna(subset=['si', 'sc']).groupby(['si', 'sc'])['value'].sum()

    def children(mix: pd.Series) -> dict[str, set[str]]:
        out: dict[str, set[str]] = {}
        for detail in set(mix.index.get_level_values(0)):
            parent = to_summary.get(detail)
            if parent is not None:
                out.setdefault(parent, set()).add(detail)
        return out

    left, right = rolled(base), rolled(built)
    kids_base, kids_built = children(base), children(built)
    published_base, published_built = summary_block(BASE_YEAR), summary_block(year)

    print(f'\nagainst the published summary block, {BASE_YEAR} -> {year}')
    print(f'{"":<11}{"summary":<10}{"PxI":>9}{"BEA":>9}{"ratio":>8}  note')
    rows: list[tuple[float, float]] = []
    excluded = 0
    for summary in sorted(set(left.index.get_level_values(0))):
        if summary not in right.index.get_level_values(0):
            continue
        if summary not in published_base.columns or summary not in published_built:
            continue
        observed = published_base[summary].clip(lower=0)
        later = published_built[summary].clip(lower=0)
        if observed.sum() <= 0 or later.sum() <= 0:
            continue
        # ⚠️ score both sides on the same commodities. The built mix sees only
        # what the concordance maps, the published column sees everything, and
        # a mix with fewer commodities has mechanically less room to move -- so
        # comparing them as they stand rewards our own thin coverage. Restrict
        # the published column to the support the built mix actually has.
        support = right.loc[summary].index.union(left.loc[summary].index)
        observed = observed.reindex(support).fillna(0)
        later = later.reindex(support).fillna(0)
        if len(support) < 2 or observed.sum() <= 0 or later.sum() <= 0:
            continue
        pxi = _l1(_shares(right.loc[summary]), _shares(left.loc[summary]))
        bea = _l1(_shares(later), _shares(observed))
        ratio = f'{pxi / bea:.1f}x' if bea > 1e-9 else '   inf'

        moved = kids_base.get(summary, set()) ^ kids_built.get(summary, set())
        note = ''
        if moved:
            note = f'excluded: children differ {sorted(moved)}'
        elif kids_base.get(summary, set()) & boundary:
            note = 'excluded: boundary child'
        if note:
            excluded += 1
        else:
            rows.append((pxi, bea))
        print(f'{"":<11}{summary:<10}{pxi:>9.4f}{bea:>9.4f}{ratio:>8}  {note}')

    if not rows:
        return
    frame = pd.DataFrame(rows, columns=['pxi', 'bea'])
    print(
        f'\n{len(frame)} comparable summary industries ({excluded} excluded) | '
        f'median PxI {frame["pxi"].median():.4f} vs BEA {frame["bea"].median():.4f}'
        f' = {frame["pxi"].median() / frame["bea"].median():.1f}x | '
        f'PxI moves more in {int((frame["pxi"] > frame["bea"]).sum())} of {len(frame)}'
    )
    print(
        '! a lower bound: the excluded industries are where BEA itself '
        'publishes the largest movement, so dropping them drops the movers'
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--year',
        type=int,
        default=BASE_YEAR,
        choices=(2017, 2022),
        help='the Economic Census vintage to build the mix from. 2017 scores '
        'against the published detail Supply block; 2022 has no published '
        'block and reports movement away from the 2017-built mix instead',
    )
    parser.add_argument(
        '--min-coverage',
        type=float,
        default=0.5,
        help='only report industries the concordance sees at least this much of. '
        'Below about 0.5 the mix is an artifact of what happens to be mapped '
        'rather than a measurement: 511110 and 511120 enter at 44%% carrying '
        'only their advertising lines, so 541800 looks like their main output. '
        '⚠️ always measured on 2017 coverage, whatever --year says, so the '
        'industry set does not shift between vintages',
    )
    parser.add_argument(
        '--write',
        metavar='PATH',
        help='write the mix to adopt for --year to PATH as csv: one row per '
        '(industry, commodity) share, tagged with the vintage it came from '
        'and whether that column is a census measurement or a held 2017 mix',
    )
    parser.add_argument(
        '--vs-published',
        action='store_true',
        help='also compare the movement the Economic Census measures against '
        'the movement BEA published, both rolled up to summary. Only '
        'meaningful with --year 2022',
    )
    args = parser.parse_args()

    block = detail_block()
    base, _ = built_mix(BASE_YEAR)
    built, boundary = (base, set()) if args.year == BASE_YEAR else built_mix(args.year)

    industries = sorted(set(base.index.get_level_values(0)) & set(block.columns))
    moving = args.year != BASE_YEAR

    header = 'movement' if moving else 'L1'
    print(
        f'\n{args.year} mix | {"industry":<10}{"coverage":>10}'
        f'{"top commodity":>26}{header:>10}  note'
    )
    present = set(built.index.get_level_values(0))
    #: every industry that cleared --min-coverage, however it was then noted --
    #: the comparison against the published block re-filters on its own terms
    scored_industries: list[str] = []
    scored: list[float] = []
    held: list[str] = []
    single: list[str] = []
    absent: list[str] = []
    for industry in industries:
        column = block[industry]
        if column.sum() <= 0:
            continue
        published = _shares(column[column > 0])
        # ⚠️ gate on 2017 coverage in both runs. Gating 2022 on its own coverage
        # would let an industry enter or leave the set because the economy grew,
        # and the movement column would then be comparing different sets.
        coverage = base.loc[industry].sum() / 1e6 / column.sum()
        if coverage < args.min_coverage:
            continue
        if industry not in present:
            # ⚠️ report it. An industry that simply stops appearing is the
            # loudest possible signal about the vintage break and the easiest
            # to miss, because the row just is not printed.
            absent.append(industry)
            continue
        scored_industries.append(industry)
        estimate = _shares(built.loc[industry])

        if moving:
            value = _l1(estimate, _shares(base.loc[industry]))
        else:
            value = _l1(estimate, published)
        top = (
            'same'
            if estimate.idxmax() == published.idxmax()
            else f'{estimate.idxmax()} vs {published.idxmax()}'
        )

        # ⚠️ a mix of one commodity has share 1.0 in every vintage, so its
        # movement is 0.000 by construction and averaging it in drags the
        # median to zero. That is a limit of what the concordance sees for the
        # industry, not a finding that its mix is stable.
        if industry in boundary:
            note = 'boundary'
            held.append(industry)
        elif moving and len(base.loc[industry]) < 2:
            note = 'single'
            single.append(industry)
        else:
            note = ''
            scored.append(value)
        print(f'{"":<11}{industry:<10}{coverage:>9.1%}{top:>26}{value:>10.3f}  {note}')

    if scored:
        series = pd.Series(scored)
        label = 'median movement' if moving else 'median L1'
        print(
            f'\n{len(series)} industries | {label} {series.median():.3f} | '
            f'under 0.05: {(series < 0.05).sum()} | over 0.30: {(series > 0.30).sum()}'
        )
    if single:
        print(
            f'  {len(single)} excluded as single-commodity — no mix to move: '
            f'{", ".join(single)}'
        )
    if held:
        print(
            f'! {len(held)} excluded — the NAICS 2022 boundary moved under them, '
            f'so a mix change cannot be told from a reclassification: '
            f'{", ".join(held)}'
        )
    if absent:
        print(
            f'! {len(absent)} in the 2017 mix but absent from {args.year} '
            f'altogether: {", ".join(absent)}'
        )

    if args.write:
        adopted = adopted_mix(args.year)
        adopted.to_csv(args.write, index=False)
        columns = adopted.drop_duplicates('industry')['source'].value_counts()
        print(
            f'\nwrote {len(adopted)} shares over '
            f'{adopted["industry"].nunique()} industries -> {args.write}'
        )
        print(
            f'  {columns.get("census", 0)} columns from the {args.year} census, '
            f'{columns.get("held", 0)} holding {BASE_YEAR}'
        )

    if args.vs_published and moving:
        published_comparison(base, built, boundary, args.year, set(scored_industries))


if __name__ == '__main__':
    main()
