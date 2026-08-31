"""Subsidies by commodity - the Supply table's ``SUB`` column.

The other half of Step 4d
(`#580 <https://github.com/cornerstone-data/bedrock/issues/580>`_), completing
the Supply bridge's ``T015`` block::

    T015 = MDTY + TOP + SUB          taxes less subsidies      695,565  (2017)

``TOP`` is :mod:`bedrock.transform.iot.nowcast_product_taxes`, and this module is
its sibling rather than its mirror image - the construction is genuinely
different, for reasons the first section gives.

⚠️ **Sign convention: ``SUB`` is stored NEGATIVE**, as BEA publishes it in the
Supply table, while NIPA publishes the same money positive and the Use table's
``T00SUB`` row positive as well. Every function here that returns a NIPA reading
returns it **positive**; :func:`sub_column` is the one that flips it. Getting
this wrong produces a plausible-looking table that fails ``T015`` by twice the
subsidy total, and comparing the two sides without it yields a tidy
"200.0017% error" that is nothing but the sign.

**The total is observed.** NIPA T31300 ``A107RC``: 59,875 in 2017 against a
published Supply column of 59,876. Like ``TOP``, the annual level carries no
modelling content at all - only the commodity split does.

Why this is not ``TOP`` with the sign flipped
----------------------------------------------

⚠️ **The 2017 share vector is safe for ``TOP`` and dangerous for ``SUB``.**
``TOP``'s composition moves slowly. ``SUB``'s does not: the total is 11.7x its
2017 level in 2020 and 10.5x in 2021, and the *composition* inverts with it.

===================  ======  ======  =========  =========  ======  ======
share of the total     2017    2019     **2020**   **2021**   2022    2024
===================  ======  ======  =========  =========  ======  ======
housing               59.7%   55.1%    **6.3%**      7.2%   37.5%   63.9%
other                 19.6%   12.9%   **84.1%**     84.2%   49.8%   26.1%
agricultural          19.3%   30.8%      6.7%       4.5%   11.6%    8.7%
===================  ======  ======  =========  =========  ======  ======

Holding the 2017 commodity shares constant puts **~420bn of 2020 pandemic
support onto housing**, against an actual 6.3%, because the 2017 column is 66.2%
housing (``531HST`` alone is 59.8%) and the 587bn of PPP has no 2017 counterpart
to attach to.

Anchor and move, per NIPA type
-------------------------------

Unlike ``TOP``, NIPA's own type lines **partition** the subsidy total rather than
covering a third of it, so every dollar is typed every year:

=================  ============  =========================================
NIPA T31300 line   series        BEA 2017 commodities
=================  ============  =========================================
agricultural       ``L31204``    ``1111B0`` ``111900``
housing            ``L31205``    ``531HST`` ``531HSO`` ``531ORE``
maritime           ``L31206``    ``483000``
air carriers       ``L31207``    ``481000``
other              ``L31208``    the remaining eight anchored commodities
state and local    ``B114RC``    folded into *other* - 0.9% of 2017
=================  ============  =========================================

Each commodity is **anchored** on its published 2017 value and **moved** by its
own type's annual NIPA growth - the same construction the two margin columns use,
and for the same reason: 2017 is then an identity, and a nowcast year moves with
the source rather than with a frozen aggregate. That already carries most of the
pandemic signal, because the type lines are where it lives: air carriers go 238
to 19,966 in 2020 (**84x**, the payroll support programme) and agricultural 11,532
to 46,457 (**4.0x**), while housing moves only 1.23x.

⚠️ **The type assignment is ours, not BEA's**, and the two readings do not agree
to the dollar - NIPA housing is 35,771 in 2017 where the three housing
commodities carry 39,636. Anchoring on the published column rather than on NIPA's
type totals is the deliberate choice: in the one year BEA publishes a commodity
split, BEA's split wins over our reading of which type a commodity belongs to.
The gap is absorbed by the final scale to the NIPA grand total.

⚠️ 2020-2021: *other* is 84% of the column and the anchor is worthless there
--------------------------------------------------------------------------

The eight anchored *other* commodities are 64% insurance carriers. Moving them by
the *other* line's own growth puts **~377bn of PPP onto ``5241XX`` in 2020** -
wrong by roughly the magnitude of the housing failure the frozen vector produces,
just in a different direction. Growth of the right line is not enough when the
line's *composition* changed completely.

✅ So for 2020 and 2021 only, *other* is allocated on **BEA's own published
allocation of PPP across industries** - `Paycheck Protection Program Subsidies by
Industry in the National Accounts <https://www.bea.gov/federal-recovery-programs-and-bea-statistics/covid-19-recovery>`_,
2023 Comprehensive Update vintage, annual, 21 NAICS sectors. BEA states it *"used
data from the Small Business Administration to allocate the forgivable portion of
the business loans across industries"*, so the SBA-to-NIPA mapping is already
done, on the basis of the very line being decomposed. Within a sector the split
is that year's ``T007``.

⚠️ **PPP is not the whole line, and this is the standing assumption to attack
first.** 447.5bn against 587.3bn is **76% of 2020**; 235.8bn against 527.3bn is
only **45% of 2021**. The remainder is Employee Retention Credit, Provider Relief,
Restaurant Revitalization and Shuttered Venue Operators, and applying the PPP
vector to the whole line assumes they distribute like PPP. They do not.
`#689 <https://github.com/cornerstone-data/bedrock/issues/689>`_ sources them
properly from USAspending, where the CFDA axis names each programme directly.

⚠️ **2022-2024 have no treatment.** PPP is zero from 2022, so those years fall
back to the anchored vector while *other* is still 2-5x its 2017 level - 63,763
in 2022 against 11,733 in 2017. That is the same insurance-carrier concentration
at smaller scale, and it is also #689's.

⚠️ **Within a sector the weight is output, and PPP went to small firms.** BEA's
table stops at 19 industries, so something has to split each one across its
commodities, and ``T007`` is the only annual commodity-level scale the build has.
It biases toward the large-firm end of each sector: ``541700`` scientific R&D
takes 18,153 $M of 2020 professional-services PPP on its output share, and R&D is
not where small-business lending went. Payroll by commodity would be the right
weight and arrives with Step 2. The sector totals - which is where BEA's actual
information is - are unaffected either way.
"""

from __future__ import annotations

import functools
import os
from collections.abc import Iterable, Mapping

import pandas as pd

from bedrock.extract.flowbyactivity import getFlowByActivity
from bedrock.extract.iot.io_2017 import _load_2017_detail_supply_use_usa
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.io.gcp import download_extract_input_from_gcs_if_not_exists
from bedrock.utils.io.local_extract_input_data import local_extract_input_dir
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES

#: The one year with a published detail Supply table.
ANCHOR_YEAR = 2017

#: Years ``SUB`` is built for. NIPA publishes the whole window.
SUB_YEARS = range(2017, 2025)

#: The years *other* is allocated on the PPP vector instead of the 2017 anchor.
PANDEMIC_YEARS = (2020, 2021)

_NIPA_SOURCE = 'BEA_NIPA'
SUBSIDY_TABLE = 'T31300'

#: Total government subsidies (line 1). The column control.
SUBSIDY_TOTAL_SERIES = 'A107RC'

#: NIPA type lines and the BEA 2017 commodities each is anchored on. Together
#: with :data:`STATE_LOCAL_SERIES` these **partition** the subsidy total, which
#: is what makes this a per-type move rather than ``TOP``'s named-plus-residual.
SUBSIDY_TYPES: Mapping[str, tuple[str, tuple[str, ...]]] = {
    'agricultural': ('L31204', ('1111B0', '111900')),
    'housing': ('L31205', ('531HST', '531HSO', '531ORE')),
    'maritime': ('L31206', ('483000',)),
    'air carriers': ('L31207', ('481000',)),
    'other': (
        'L31208',
        (
            '5241XX',
            '482000',
            '52A000',
            '622000',
            '335912',
            '314900',
            '325414',
            '515200',
        ),
    ),
}

#: State and local subsidies (line 8), 555 in 2017 against a 59,875 total. NIPA
#: gives no type for them and the published column no separate home, so they ride
#: the *other* line's commodities via the final scale to the grand total.
STATE_LOCAL_SERIES = 'B114RC'

#: The *other* type, named because it is the one the anchor cannot carry in
#: 2020-2021.
OTHER_TYPE = 'other'

# --- BEA's PPP-by-industry table -------------------------------------------

#: Cached under ``extract/input_data/BEA_PPP_Subsidies/`` and mirrored to
#: ``gs://cornerstone-default/extract/input-data/BEA_PPP_Subsidies/``. Not an FBA
#: on purpose: PPP ended, the 2022 column is 0.0 and this Comprehensive Update
#: vintage is final, so there is nothing for an FBA's refresh machinery to do.
PPP_SOURCE = 'BEA_PPP_Subsidies'
PPP_WORKBOOK = 'NEA-CU23-Subsidies-by-Industry-A.xlsx'
PPP_SOURCE_URL = (
    'https://www.bea.gov/sites/default/files/2023-09/'
    'NEA-CU23-Subsidies-by-Industry-A.xlsx'
)

#: Workbook line numbers that are subtotals of other lines, not sectors of their
#: own. Line 1 is the total; 7 and 8 are durable and nondurable goods beneath
#: line 6 manufacturing. Summing without dropping these double counts.
_PPP_SUBTOTAL_LINES = (1, 7, 8)

#: PPP industry row -> the NAICS prefixes of the BEA 2017 commodities it covers.
#: BEA 2017 Detail codes are NAICS-based, so a prefix rule reaches all 390
#: private commodities; the 12 that no prefix reaches are listed in
#: :data:`_PPP_EXCLUDED_COMMODITIES` and are excluded on purpose.
_PPP_ROW_PREFIXES: Mapping[str, tuple[str, ...]] = {
    'Agriculture, forestry, fishing, and hunting': ('11',),
    'Mining': ('21',),
    'Utilities': ('22',),
    'Construction': ('23',),
    'Manufacturing': ('31', '32', '33'),
    'Wholesale trade': ('42',),
    'Retail trade': ('44', '45', '4B'),
    'Transportation and warehousing': ('48', '49'),
    'Information': ('51',),
    'Finance and insurance': ('52',),
    'Real estate and rental and leasing': ('53',),
    'Professional, scientific, and technical services': ('54',),
    'Management of companies and enterprises': ('55',),
    'Administrative and waste management services': ('56',),
    'Educational services': ('61',),
    'Health care and social assistance': ('62',),
    'Arts, entertainment, and recreation': ('71',),
    'Accommodation and food services': ('72',),
    'Other services, except government': ('81',),
}

#: Commodities kept out of the PPP base, and why. BEA's table is explicitly
#: *"Subsidies to Private Industries"*, so government, scrap, used goods and the
#: import/rest-of-world specials take none of it.
#:
#: ⚠️ ``4200ID`` customs duties is the trap: it starts ``42`` and a bare prefix
#: rule silently files it under wholesale trade. ``491000`` postal service is a
#: federal government enterprise wearing a NAICS code, and ``531HST``/``531HSO``
#: are excluded because imputed and tenant housing already carry the *housing*
#: subsidy line - leaving them in would let real-estate PPP land on the largest
#: imputed-rent commodity in the table and double up with that line.
_PPP_EXCLUDED_COMMODITIES: frozenset[str] = frozenset(
    {
        '4200ID',
        '491000',
        '531HST',
        '531HSO',
        'S00102',
        'S00203',
        'S00300',
        'S00401',
        'S00402',
        'S00500',
        'S00600',
        'S00900',
        'GSLGE',
        'GSLGH',
        'GSLGO',
    }
)

#: Slack on the identity checks, in USD - the published table is in whole
#: millions, so an exact comparison trips on BEA's own rounding.
_ROUNDING_TOLERANCE = 1.0 * MILLION_CURRENCY_TO_CURRENCY


# --- NIPA, the annual level ------------------------------------------------


@functools.cache
def nipa_subsidy_lines(year: int) -> pd.Series:
    """NIPA table 3.13 by series code for *year*, USD, **positive** as published."""
    fba = getFlowByActivity(_NIPA_SOURCE, int(year))
    description = fba['Description'].astype(str)
    rows = fba[description.str.startswith(f'{SUBSIDY_TABLE}:')].copy()
    if rows.empty:
        raise ValueError(
            f'{_NIPA_SOURCE} {year} carries no {SUBSIDY_TABLE} rows. That table is '
            f'listed in BEA_NIPA.yaml, so an empty result means the extract '
            f'changed rather than that subsidies stopped being paid.'
        )
    series = (
        rows['Description']
        .astype(str)
        .str.split(':')
        .str[1]
        .str.split(' - ')
        .str[0]
        .str.strip()
    )
    return (
        pd.to_numeric(rows['FlowAmount'], errors='coerce')
        .groupby(series)
        .sum()
        .rename_axis('nipa_series')
        .rename(f'{SUBSIDY_TABLE}_{year}')
    )


def nipa_line(code: str, year: int) -> float:
    """One NIPA T31300 series in *year*, USD positive, raising on a missing code."""
    lines = nipa_subsidy_lines(year)
    if code not in lines.index:
        raise ValueError(
            f'NIPA {SUBSIDY_TABLE} {year} has no series {code!r}. It was published '
            f'in {ANCHOR_YEAR}; if BEA has retired or renamed the line, '
            f'SUBSIDY_TYPES needs updating rather than defaulting to zero.'
        )
    return float(lines[code])


def sub_control_total(year: int) -> float:
    """The ``SUB`` column total for *year*, USD, **positive** as NIPA publishes it.

    ⚠️ :func:`sub_column` stores this negative. 59,875 in 2017 against a published
    Supply column of −59,876.
    """
    total = nipa_line(SUBSIDY_TOTAL_SERIES, year)
    if total <= 0:
        raise ValueError(
            f'NIPA total subsidies is {total:,.0f} USD in {year}. This module reads '
            f'NIPA positive and flips the sign once, in sub_column; a non-positive '
            f'reading here means the source is already signed and would be flipped '
            f'twice.'
        )
    return total


def subsidy_type_totals(year: int) -> pd.Series:
    """Each NIPA type line for *year*, USD positive, in :data:`SUBSIDY_TYPES` order."""
    return pd.Series(
        {
            subsidy_type: nipa_line(series, year)
            for subsidy_type, (series, _) in SUBSIDY_TYPES.items()
        },
        dtype=float,
    ).rename_axis('subsidy_type')


# --- the 2017 anchor -------------------------------------------------------


@functools.cache
def published_sub_by_commodity() -> pd.Series:
    """The published 2017 Supply ``SUB`` column, USD, **negative**.

    Non-zero on 15 commodities of 402, and 66.2% of it is housing.
    """
    supply = _load_2017_detail_supply_use_usa('Supply_detail').rename(
        columns=lambda column: column.strip()
    )
    commodities = [code for code in USA_2017_COMMODITY_CODES if code in supply.index]
    sub = supply.loc[commodities, 'SUB'].astype(float) * MILLION_CURRENCY_TO_CURRENCY
    column = (
        sub.reindex(USA_2017_COMMODITY_CODES)
        .fillna(0.0)
        .rename_axis('commodity')
        .rename('SUB')
    )
    if (column > 0).any():
        raise ValueError(
            f'The published 2017 Supply SUB column is positive on '
            f'{sorted(column.index[column > 0])}. BEA stores this column negative; '
            f'a positive cell means the workbook was read on the Use table\'s sign '
            f'convention, which doubles the subsidy wedge in T015.'
        )
    return column


@functools.cache
def anchor_shares() -> pd.DataFrame:
    """Commodity x type share of each type's 2017 subsidy. Columns sum to 1.

    ⚠️ Every commodity carrying 2017 ``SUB`` must be typed, or its subsidy is
    dropped from every later year while the column total still ties to NIPA.
    """
    published = published_sub_by_commodity().abs()
    typed = [c for _, commodities in SUBSIDY_TYPES.values() for c in commodities]

    duplicated = sorted({c for c in typed if typed.count(c) > 1})
    if duplicated:
        raise ValueError(
            f'SUBSIDY_TYPES assigns {duplicated} to more than one type, so their '
            f'subsidy would be counted once per type.'
        )
    untyped = sorted(set(published.index[published > 0]) - set(typed))
    if untyped:
        raise ValueError(
            f'{untyped} carry {ANCHOR_YEAR} SUB but belong to no type in '
            f'SUBSIDY_TYPES, so they would be dropped from every year after the '
            f'anchor while the column total still tied to NIPA.'
        )

    shares = pd.DataFrame(
        0.0,
        index=published.index,
        columns=pd.Index(list(SUBSIDY_TYPES), name='subsidy_type'),
    )
    for subsidy_type, (_, commodities) in SUBSIDY_TYPES.items():
        selection = list(commodities)
        type_total = float(published[selection].sum())
        if type_total <= 0:
            raise ValueError(
                f'SUBSIDY_TYPES type {subsidy_type!r} maps to commodities carrying '
                f'no {ANCHOR_YEAR} subsidy, so its line has no basis to split on.'
            )
        shares.loc[selection, subsidy_type] = published[selection] / type_total
    return shares


def type_growth(year: int) -> pd.Series:
    """Each type's NIPA level in *year* over its 2017 level. 1.0 across the board in 2017.

    The pandemic signal is mostly here: air carriers 84x in 2020, agricultural
    4.0x, housing only 1.23x.
    """
    current = subsidy_type_totals(year)
    anchor = subsidy_type_totals(ANCHOR_YEAR)
    if (anchor <= 0).any():
        raise ValueError(
            f'NIPA type lines {sorted(anchor.index[anchor <= 0])} are zero in '
            f'{ANCHOR_YEAR}, so they have no anchor to move from.'
        )
    return (current / anchor).rename(f'growth_{year}')


# --- BEA's PPP allocation, for 2020-2021 -----------------------------------


def ppp_workbook_path() -> str:
    """Local path to BEA's PPP-by-industry workbook, pulling from GCS if absent."""
    local_dir = local_extract_input_dir(PPP_SOURCE, year=None)
    path = os.path.join(local_dir, PPP_WORKBOOK)
    if not os.path.exists(path):
        os.makedirs(local_dir, exist_ok=True)
        download_extract_input_from_gcs_if_not_exists(
            {'source': PPP_SOURCE, 'year': None, 'url': PPP_SOURCE_URL},
            local_dir=local_dir,
            object_name=PPP_WORKBOOK,
        )
    if not os.path.exists(path):
        raise FileNotFoundError(
            f'{PPP_WORKBOOK} is neither cached at {path} nor available from '
            f'gs://cornerstone-default/extract/input-data/{PPP_SOURCE}/. It is a '
            f'final BEA vintage and can be re-fetched from {PPP_SOURCE_URL}.'
        )
    return path


@functools.cache
def ppp_by_sector() -> pd.DataFrame:
    """BEA's PPP subsidies by industry, USD positive, industries x years.

    Annual levels for 2019-2022. 447.5bn in 2020 and 235.8bn in 2021, zero from
    2022 - the programme ended.
    """
    raw = pd.read_excel(ppp_workbook_path(), header=None)
    numbered = raw[raw[0].notna() & raw[0].astype(str).str.match(r'^\d+$')].copy()
    numbered['line'] = numbered[0].astype(int)

    years = [2019, 2020, 2021, 2022]
    table = numbered.loc[~numbered['line'].isin(_PPP_SUBTOTAL_LINES), [1, 2, 3, 4, 5]]
    table.columns = pd.Index(['industry', *years])
    table['industry'] = table['industry'].astype(str).str.strip()
    frame = table.set_index('industry')
    frame = frame.apply(pd.to_numeric, errors='coerce').fillna(0.0)
    frame = frame * 1e9  # the workbook is in billions

    published_total = (
        float(pd.to_numeric(numbered.loc[numbered['line'] == 1, 3]).iloc[0]) * 1e9
    )
    industry_total = float(frame[2020].sum())
    if abs(industry_total - published_total) > 0.5e9:
        raise ValueError(
            f'The PPP industry rows sum to {industry_total / 1e9:,.1f}bn in 2020 '
            f'against a published total of {published_total / 1e9:,.1f}bn. Either a '
            f'subtotal row is being counted as an industry or an industry is being '
            f'dropped; _PPP_SUBTOTAL_LINES is what decides.'
        )
    return frame


@functools.cache
def ppp_base_commodities() -> pd.Series:
    """Commodity -> PPP industry row, for the 387 commodities in the PPP base.

    ⚠️ Raises if any commodity outside :data:`_PPP_EXCLUDED_COMMODITIES` fails to
    match a prefix. A silently unmatched commodity takes no PPP and the shares
    still sum to 1, so the loss would be invisible in every total.
    """
    mapping: dict[str, str] = {}
    unmatched: list[str] = []
    for code in USA_2017_COMMODITY_CODES:
        code = str(code)
        if code in _PPP_EXCLUDED_COMMODITIES:
            continue
        for row, prefixes in _PPP_ROW_PREFIXES.items():
            if code.startswith(prefixes):
                mapping[code] = row
                break
        else:
            unmatched.append(code)
    if unmatched:
        raise ValueError(
            f'{sorted(unmatched)} match no NAICS prefix in _PPP_ROW_PREFIXES and are '
            f'not listed as excluded. BEA 2017 Detail codes are NAICS-based, so an '
            f'unmatched code is either a new special code or a prefix rule that has '
            f'drifted - decide which, rather than letting it take no PPP silently.'
        )
    return pd.Series(mapping, name='ppp_industry').rename_axis('commodity')


def ppp_commodity_shares(year: int) -> pd.Series:
    """PPP shares by commodity for *year*, summing to 1.

    BEA's sector amounts, split within a sector by that year's ``T007``. Domestic
    output is the right within-sector weight here in a way it is not for a tax
    base: PPP went to *producers*, in proportion to their payroll, and output is
    the only annual commodity-level scale the build has.
    """
    # deferred: eeio.nowcast imports this module to fill the SUB column, so a
    # top-level import here would close the cycle
    from bedrock.transform.iot.nowcast_supply_go_control import (  # noqa: PLC0415
        seed_commodity_output,
    )

    if int(year) not in ppp_by_sector().columns:
        raise ValueError(
            f'BEA publishes PPP by industry for '
            f'{sorted(ppp_by_sector().columns)}; {year} is outside that.'
        )
    sectors = ppp_by_sector()[int(year)]
    industry_of = ppp_base_commodities()
    output = (
        seed_commodity_output(int(year), False)
        .reindex(industry_of.index)
        .fillna(0.0)
        .clip(lower=0.0)
    )

    amounts = pd.Series(0.0, index=industry_of.index, name='ppp')
    for industry, amount in sectors.items():
        members = industry_of.index[industry_of == industry]
        weights = output[members]
        weight_total = float(weights.sum())
        if weight_total <= 0:
            raise ValueError(
                f'PPP industry {industry!r} has {amount:,.0f} USD to place but its '
                f'commodities carry no {year} T007 to split it on.'
            )
        amounts.loc[members] = float(amount) * weights / weight_total

    total = float(amounts.sum())
    if total <= 0:
        raise ValueError(f'PPP allocates nothing in {year}; shares are undefined.')
    return (
        amounts.reindex(USA_2017_COMMODITY_CODES)
        .fillna(0.0)
        .div(total)
        .rename('ppp_share')
    )


# --- the column ------------------------------------------------------------


def sub_decomposition(year: int) -> pd.DataFrame:
    """``SUB`` for *year* by type, USD **negative**, plus the ``SUB`` row margin.

    One column per :data:`SUBSIDY_TYPES` type. In :data:`PANDEMIC_YEARS` the
    ``other`` column is BEA's PPP allocation rather than the 2017 anchor moved.
    """
    if int(year) not in SUB_YEARS:
        raise ValueError(
            f'SUB is built for {SUB_YEARS.start}-{SUB_YEARS.stop - 1}; {year} is '
            f'outside the years the BEA_NIPA extract carries.'
        )
    published = published_sub_by_commodity().abs()
    totals = subsidy_type_totals(year)
    shares = anchor_shares()
    growth = type_growth(year)

    # anchor: the type's own 2017 mass, moved by the type's own NIPA growth
    anchored = pd.Series(
        {
            subsidy_type: float(published[list(commodities)].sum())
            * float(growth[subsidy_type])
            for subsidy_type, (_, commodities) in SUBSIDY_TYPES.items()
        }
    )
    parts = shares.mul(anchored, axis='columns')

    if int(year) in PANDEMIC_YEARS:
        parts[OTHER_TYPE] = ppp_commodity_shares(year) * float(totals[OTHER_TYPE])

    scale = sub_control_total(year) / float(parts.to_numpy().sum())
    parts = parts * scale

    out = -parts
    out['SUB'] = out.sum(axis='columns')
    out.index.name = 'commodity'
    return out


def sub_column(year: int = ANCHOR_YEAR) -> pd.Series:
    """The Supply table's ``SUB`` column for *year*. USD, **negative**.

    Sums to −:func:`sub_control_total` exactly.

    ⚠️ **Negative is the Supply table's convention, not a bug and not ours.** The
    Use table's ``T00SUB`` row carries the same money positive. ``T015`` *adds*
    this column rather than subtracting it.
    """
    column = sub_decomposition(year)['SUB'].rename('SUB')

    control = -sub_control_total(year)
    if abs(float(column.sum()) - control) > _ROUNDING_TOLERANCE:
        raise ValueError(
            f'SUB {year} sums to {column.sum():,.0f} USD against a NIPA control of '
            f'{control:,.0f}. Every type is scaled to that total by construction, so '
            f'a gap means one of them was scaled twice.'
        )
    if (column > 0).any():
        raise ValueError(
            f'SUB {year} is positive on {sorted(column.index[column > 0])}. This '
            f'column is stored negative; a positive cell is a sign flip, and it '
            f'fails T015 by twice the amount rather than by the amount.'
        )
    return column


def control_total_table(years: Iterable[int] | None = None) -> pd.DataFrame:
    """The annual NIPA type lines behind the column, in $M positive, for diagnostics."""
    years = list(SUB_YEARS if years is None else years)
    rows = {}
    for year in years:
        totals = subsidy_type_totals(year)
        control = sub_control_total(year)
        rows[year] = {
            **totals.to_dict(),
            'state and local': nipa_line(STATE_LOCAL_SERIES, year),
            'SUB': control,
        }
    return (pd.DataFrame(rows).T / MILLION_CURRENCY_TO_CURRENCY).rename_axis('year')
