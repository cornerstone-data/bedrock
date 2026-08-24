# Census_EC.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Pulls U.S. Census Bureau Economic Census Data
"""
import json
from typing import Any

import numpy as np
import pandas as pd

from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.utils.mapping.location import US_FIPS
from bedrock.utils.logging.flowsa_log import log

#: ``ecnmatfuel`` codes that are industry **totals**, not materials.  The named
#: codes sum to ``00772000`` exactly, so leaving these in a sum double counts
#: the whole table.  Kept in the FBA because they are the control a suppression
#: recovery subtracts published children from.
MATFUEL_TOTAL_CODES = ('00772000', '00772002')

#: ``ecnmatfuel`` residual buckets - real spend that Census could not place on a
#: named material.  Together roughly a third of delivered cost, which is the
#: ceiling on what this source can attribute to a commodity.
#: NAICS prefix length defining an industry's peer group for the suppression
#: prior.  **Three, and that is measured rather than reasoned.**  Masking
#: published cells and recovering them scores every prefix length.  NAICS-3 and
#: NAICS-4 are close and both beat the alternatives; NAICS-3 wins on average
#: (WAPE 0.602 / 0.718 across the two vintages, against 0.649 / 0.705 for
#: NAICS-4 and 0.640 / 1.033 for an economy-wide prior) and on median relative
#: error in 2022.  Reproduce with ``materials_structure.py --holdout``.
NAICS_PEER_GROUP_LENGTH = 3

MATFUEL_RESIDUAL_CODES = (
    '00970098',  # All other supplies
    '00970099',  # Cost of all other materials, components, parts, containers
    '00971000',  # Materials, ingredients, containers, and supplies, nsk
    '00973000',  # Undistributed - minerals, purchased machinery, parts
    '00974000',  # Undistributed fuels
    '00960018',  # Other fuels (liquefied petroleum gas, coke, wood, etc.)
    '00999828',  # Water purchased
)


def census_EC_URL_helper(*, build_url, year, config, **_):
    """
    This helper function uses the "build_url" input from generateflowbyactivity.py,
    which is a base url for data imports that requires parts of the url text
    string to be replaced with info specific to the data year. This function
    does not parse the data, only modifies the urls from which data
    is obtained.
    :param build_url: string, base url
    :param year: year
    :return: list, urls to call, concat, parse, format into
        Flow-By-Activity format
    """
    urls_census = []
    for k, v in config['datasets'].items():
        for dataset in v.get(year, []):
            url = build_url.replace('__dataset__', k).replace(
                '__group__', f'group({dataset})'
            )
            if year == '2012':
                # for 2012 need both us and state call separately
                url += '&for=us:*'
                urls_census.append(url)
                url = url.replace('&for=us:*', '&for=state:*')
                urls_census.append(url)
            else:
                urls_census.append(url)

    return urls_census


def census_EC_call(*, resp, **_):
    """
    Convert response for calling url to pandas dataframe, begin
        parsing df into FBA format
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    census_json = json.loads(resp.text)
    url = resp.url
    desc = url[url.find("(") + 1 : url.find(")")]  # extract the group from the url
    # convert response to dataframe
    df = pd.DataFrame(data=census_json[1 : len(census_json)], columns=census_json[0])
    df = df.assign(Description=desc)
    return df


def census_EC_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    # concat dataframes
    df = pd.concat(df_list, sort=False)

    if year == '2017':
        df = df.query('TAXSTAT_LABEL == "All establishments"').query(
            'TYPOP_LABEL == "All establishments"'
        )
        class_label = 'CLASSCUST_LABEL'
    else:
        class_label = 'CLASSCUST_TTL'

    df = (
        df.filter(
            [
                f'NAICS{year}',
                class_label,
                'ESTAB',
                'RCPTOT',
                'RCPTOT_F',
                'GEO_ID',
                'RCPTOT_DIST',
                'YEAR',
                'Description',
            ]
        )
        .rename(
            columns={
                f'NAICS{year}': 'ActivityProducedBy',
                f'{class_label}': 'ActivityConsumedBy',
                'ESTAB': 'Number of establishments',
                'RCPTOT': 'Sales, value of shipments, or revenue',
                'RCPTOT_DIST': 'Distribution of sales, value of shipments, or revenue',
                'RCPTOT_F': 'Note',
                'YEAR': 'Year',
            }
        )
        .assign(Location=lambda x: x['GEO_ID'].str[-2:])
        .melt(
            id_vars=[
                'ActivityProducedBy',
                'ActivityConsumedBy',
                'Location',
                'Year',
                'Description',
                'Note',
            ],
            value_vars=[
                'Number of establishments',
                'Sales, value of shipments, or revenue',
                'Distribution of sales, value of shipments, or revenue',
            ],
            value_name='FlowAmount',
            var_name='FlowName',
        )
        .assign(FlowAmount=lambda x: x['FlowAmount'].astype(float))
    )

    # Updated suppressed data field
    df = df.assign(
        Suppressed=np.where(df.Note.isin(["D"]), df.Note, np.nan),
        FlowAmount=np.where(df.Note.isin(["D"]), 0, df.FlowAmount),
    ).drop(columns='Note')

    conditions = [
        df['FlowName'] == 'Number of establishments',
        df['FlowName'] == 'Sales, value of shipments, or revenue',
        df['FlowName'] == 'Distribution of sales, value of shipments, or revenue',
    ]
    df['Unit'] = np.select(conditions, ['p', 'USD', 'Percent'])
    df['Class'] = np.select(conditions, ['Other', 'Money', 'Money'])
    df['FlowAmount'] = np.where(
        df['FlowName'] == 'Sales, value of shipments, or revenue',
        df['FlowAmount'] * 1000,
        df['FlowAmount'],
    )
    df['Location'] = np.where(
        df['Location'] == 'US',
        US_FIPS,
        df['Location'].str.pad(5, side='right', fillchar='0'),
    )

    # add location system based on year of data
    df = assign_fips_location_system(df, year)
    # hard code data
    df['SourceName'] = 'Census_EC'
    df['FlowType'] = "ELEMENTARY_FLOW"
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    df['Compartment'] = None
    return df


def census_EC_PxI_parse(*, df_list, year, **_):
    """
    Parse Economic Census *Products by Industry* (``ecnnapcsprd``) into FBA form.

    This is the product-line-by-kind-of-business table: for each NAICS industry,
    the NAPCS product lines it sells and the dollars against each. It is BEA's
    own input for the trade-margin method (2009 IO manual ch. 8) and answers
    "what does this kind of business sell", which is the question the change in
    private inventories merchandise-trade rule asks - see
    ``analysis/nowcasting/inventories_estimation_plan.md`` and #529/#615.

    ⚠️ **The product identifier is not NAPCS**, despite the API field being
    called ``NAPCS2017``. Census labels it *"2017 NAPCS collection code"*, and
    it is a separate code set - called the **Census_2017_PxI_product_code**
    throughout bedrock so the two are not confused. Official NAPCS codes are
    hierarchical at lengths 2/3/5/7/9/11; these are uniformly 10 digits, and
    **none of the 620 trade product codes appear in the published NAPCS 2017
    structure or definitions**. Joining on title instead recovers 144 of 620
    lines (12% of value; for goods lines alone, 14 of 341 and 11%). The official
    NAPCS files are therefore reference material, not a source of coverage.

    The codes are also unstable across vintages - 525 are new in 2022, 548 gone,
    and the large "new" ones are recodings of products already in 2017 - so
    anything built on this source keys on the **description**, not the code.

    ``ActivityProducedBy`` is the industry and ``FlowName`` the product line.
    ⚠️ The orientation is worth checking before this is used for attribution:
    ``ecnnapcsprd`` is products *by* industry, while its sibling ``ecnnapcsind``
    is industry *by* product, and the two want opposite Produced/Consumed
    assignments. Only ``ecnnapcsprd`` is pulled here, so the assignment is
    consistent, but a second dataset must not be added to this method without
    revisiting it.

    :param df_list: list of dataframes to concat and format
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity specifications
    """
    df = pd.concat(df_list, sort=False)

    df = (
        df.filter(
            [
                f'NAICS{year}',
                'INDGROUP',
                f'NAPCS{year}',
                f'NAPCS{year}_LABEL',
                'NAPCSDOL',
                'NAPCSDOL_F',
                'NAPCSDOL_S',
                'GEO_ID',
                'YEAR',
            ]
        )
        .rename(
            columns={
                f'NAICS{year}': 'Industry',
                f'NAPCS{year}': 'Product',
                f'NAPCS{year}_LABEL': 'Description',
                'NAPCSDOL': 'FlowAmount',
                'NAPCSDOL_F': 'Note',
                'NAPCSDOL_S': 'Spread',
                'YEAR': 'Year',
            }
        )
        .assign(Location=lambda x: x['GEO_ID'].str[-2:])
    )

    df = df.assign(
        FlowName=df['Product'],
        ActivityProducedBy=df['Industry'],
        ActivityConsumedBy='',
        MeasureofSpread='Relative standard error',
        FlowAmount=lambda x: x['FlowAmount'].astype(float),
    )

    # Census suppression flags. D/S withhold the cell; A and s mark estimates
    # too unreliable to publish. All are zeroed and recorded rather than
    # dropped, so a consumer can tell a suppressed cell from a true zero.
    suppressed = df.Note.isin(["D", "s", "A", "S"])
    df = df.assign(
        Suppressed=np.where(suppressed, df.Note, np.nan),
        # NAPCSDOL is published in thousands of dollars.
        FlowAmount=np.where(suppressed, 0, df.FlowAmount * 1000),
    ).drop(columns='Note')

    df['Location'] = np.where(
        df['Location'] == 'US',
        US_FIPS,
        df['Location'].str.pad(5, side='right', fillchar='0'),
    )

    df = assign_fips_location_system(df, year)
    df['Unit'] = 'USD'
    df['Class'] = 'Money'
    df['SourceName'] = 'Census_EC_PxI'
    df['FlowType'] = "ELEMENTARY_FLOW"
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    df['Compartment'] = None
    return df


def census_EC_MatFuel_parse(*, df_list, year, **_):
    """
    Parse Economic Census *Materials Consumed by Kind* (``ecnmatfuel``) into FBA form.

    The commodity breakout of a manufacturing industry's materials bill: for
    each NAICS-6 industry, the 8-digit material and fuel codes it consumed and
    the delivered cost of each.  Quinquennial, and the only source that reaches
    the part of the intermediate column the annual surveys publish as one cell -
    ``EXPS_MAT_DVAL`` is 82.5% of manufacturing's column and has no commodity
    split (#564).  Feeds Step 3 (#497); see
    ``analysis/nowcasting/intermediate_estimation_plan.md``.

    **Orientation is the opposite of** :func:`census_EC_PxI_parse` **and that is
    deliberate.**  PxI asks what an industry *sells*, so the industry is the
    producer.  This asks what an industry *buys*, so the industry goes in
    ``ActivityConsumedBy`` and the material in ``ActivityProducedBy`` - which is
    also the Use table's own orientation, commodity down and industry across.
    Getting this backwards would transpose every downstream attribution without
    raising anything.

    ⚠️ **``00772000`` "Total Materials" is the industry total, not a material.**
    The named codes sum to it exactly - median ratio 1.000 across 386 of 388
    industries in 2017 - so **summing this FBA unfiltered double counts the
    whole table**.  It is kept rather than dropped because it is the control any
    suppression recovery must subtract published children from, exactly as NAICS
    ``00`` serves :func:`estimate_suppressed_ec_pxi`.  ``00772002`` "Total Fuels"
    is the fuels-side equivalent.  :data:`MATFUEL_TOTAL_CODES` names both.

    ⚠️ **A third of the cost sits in named residual buckets**, chiefly
    ``00970099`` "Cost of all other materials and components, parts, containers,
    and supplies consumed" and ``00971000`` "Materials, ingredients, containers,
    and supplies, nsk".  They are the ceiling on what this source can place, and
    :data:`MATFUEL_RESIDUAL_CODES` names them so a consumer can measure its own
    coverage rather than discovering the ceiling late.

    ⚠️ **The two vintages are on different NAICS bases** - ``NAICS2017`` against
    ``NAICS2022`` - and share 345 industries and 291 materials, 89.9% and 90.6%
    of each year's cost.  Check presence before differencing them.

    :param df_list: list of dataframes to concat and format
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity specifications
    """
    df = pd.concat(df_list, sort=False)

    df = (
        df.filter(
            [
                f'NAICS{year}',
                f'NAICS{year}_LABEL',
                'MATFUEL',
                'MATFUEL_LABEL',
                'MATFUELCOST',
                'MATFUELCOST_F',
                'M_FI',
                'GEO_ID',
                'YEAR',
            ]
        )
        .rename(
            columns={
                f'NAICS{year}': 'ActivityConsumedBy',
                f'NAICS{year}_LABEL': 'IndustryName',
                'MATFUEL': 'ActivityProducedBy',
                'MATFUEL_LABEL': 'Description',
                'MATFUELCOST': 'FlowAmount',
                'MATFUELCOST_F': 'Note',
                'YEAR': 'Year',
            }
        )
        .assign(Location=lambda x: x['GEO_ID'].str[-2:])
    )

    df = df.assign(
        # M or F - a material or a fuel.  Fuels are a different economic object
        # (they are consumed, not embodied) and some consumers want only one.
        FlowName=np.where(df['M_FI'].eq('F'), 'Fuel consumed', 'Material consumed'),
        FlowAmount=lambda x: pd.to_numeric(x['FlowAmount'], errors='coerce'),
    )

    # Census withholds a cell that would disclose an individual company. The
    # value is not zero - it is still inside the published 00772000 total - so
    # it is zeroed and recorded, never dropped.
    suppressed = df['Note'].isin(['D', 'S', 's', 'A'])
    df = df.assign(
        Suppressed=np.where(suppressed, df['Note'], np.nan),
        # MATFUELCOST is published in thousands of dollars.
        FlowAmount=np.where(suppressed, 0, df['FlowAmount'].fillna(0.0) * 1000),
    ).drop(columns=['Note', 'M_FI'])

    df['Location'] = np.where(
        df['Location'] == 'US',
        US_FIPS,
        df['Location'].str.pad(5, side='right', fillchar='0'),
    )

    df = assign_fips_location_system(df, year)
    df['Unit'] = 'USD'
    df['Class'] = 'Money'
    df['SourceName'] = 'Census_EC_MatFuel'
    df['FlowType'] = 'ELEMENTARY_FLOW'
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    df['Compartment'] = None
    return df


def estimate_suppressed_ec_pxi(fba: pd.DataFrame, **_: Any) -> pd.DataFrame:
    """
    Recover suppressed ``Census_EC_PxI`` cells from the published product totals.

    Census withholds an industry x product cell when publishing it would
    disclose an individual company. The value is not zero - it is still inside
    the published totals - so leaving it at zero biases any product mix derived
    from this table toward whatever happens to be publishable. In 2017 that is
    **54% of trade rows**, though only **4.3% of value**: the suppressed cells
    are a long tail of small industry x product combinations.

    ``ecnnapcsprd`` publishes each product line twice: once on NAICS ``00`` (the
    national total for that product, across all industries) and once per
    contributing 6-digit industry. There is **no intermediate 3/4/5-digit
    hierarchy**, so recovery is a single level - subtract the published
    industries from the ``00`` total and share the residual over the industries
    that were withheld.

    ⚠️ This is why :func:`flowbyclean.estimate_suppressed_sectors_equal_attribution`
    cannot be pointed at this source. That function walks a dense NAICS
    hierarchy, taking children at ``level + 1``; here the codes jump 2 to 6, so
    every level finds no children and does nothing.

    Three outcomes, recorded in ``SuppressionRecovery`` so a consumer can tell a
    measurement from an estimate:

    - ``exact`` - the product had exactly one withheld industry, so the residual
      belongs to it entirely and this is a recovered measurement, not a guess.
      791 of 3,522 product lines in 2017.
    - ``split`` - two or more withheld, residual shared equally. A placeholder:
      equal shares are certainly wrong per cell, but the mass lands in the right
      product.
    - ``unrecoverable`` - the ``00`` total is itself suppressed (547 products in
      2017), so there is nothing to subtract from. Left at zero.

    Measured on 2017, against the 2,976 products that have a published ``00``
    total: the 6-digit detail goes from **90.5% to 100.0%** of control, and
    2,829 of those products close to within $1M.

    ⚠️ **Validate only against published totals.** A suppressed ``00`` row reads
    as ``FlowAmount = 0``, so comparing every product against its total makes the
    547 unrecoverable ones look like a $1.78T overshoot - published children
    against a zero parent. They are not an error; they are products whose
    control is withheld. Filter on ``Suppressed.isna()`` in the ``00`` rows
    before any closure check.

    ⚠️ **The ``00`` rows are dropped on the way out.** Once the residual is
    distributed the 6-digit detail sums to the product total by construction, so
    keeping both would double count the whole table - summing this FBA
    unfiltered gives $67T against a true $34.4T. Callers wanting the control
    total should take it before calling this.

    :param fba: the ``Census_EC_PxI`` FlowByActivity
    :return: 6-digit detail only, with suppressed cells filled where possible
    """
    df = fba.copy()
    naics = df['ActivityProducedBy'].astype(str)
    is_total = naics.str.len() == 2
    suppressed = df['Suppressed'].notna()

    totals = (
        df[is_total]
        .assign(_sup=lambda x: x['Suppressed'].notna())
        .set_index('FlowName')[['FlowAmount', '_sup']]
    )
    detail = df[~is_total].copy()

    published = (
        detail[~detail['Suppressed'].notna()].groupby('FlowName')['FlowAmount'].sum()
    )
    n_withheld = detail[detail['Suppressed'].notna()].groupby('FlowName').size()

    residual = (
        totals['FlowAmount']
        .sub(published.reindex(totals.index).fillna(0.0), fill_value=0.0)
        .reindex(n_withheld.index)
    )
    # A negative residual means the published children already exceed the
    # published total - a vintage or rounding artefact, not something to invent
    # negative mass from.
    overshoot = int((residual < 0).sum())
    residual = residual.clip(lower=0.0)

    total_suppressed = totals['_sup'].reindex(n_withheld.index).fillna(False)
    per_child = (residual / n_withheld).where(~total_suppressed)

    detail_sup = detail['Suppressed'].notna()
    fill = detail['FlowName'].map(per_child)
    detail.loc[detail_sup, 'FlowAmount'] = fill[detail_sup].fillna(0.0)

    how = detail['FlowName'].map(
        n_withheld.map(lambda n: 'exact' if n == 1 else 'split')
    )
    no_control = (
        detail['FlowName'].map(total_suppressed).astype('boolean').fillna(False)
    )
    how = how.where(~no_control, 'unrecoverable')
    detail['SuppressionRecovery'] = np.where(detail_sup, how, np.nan)

    counts = detail.loc[detail_sup, 'SuppressionRecovery'].value_counts()
    log.info(
        'Census_EC_PxI suppression recovery: %s exact, %s split, %s '
        'unrecoverable; %s products had published children exceeding their '
        'total (residual clipped to 0). Dropped %s product-total rows to avoid '
        'double counting.',
        int(counts.get('exact', 0)),
        int(counts.get('split', 0)),
        int(counts.get('unrecoverable', 0)),
        overshoot,
        int(is_total.sum()),
    )
    return detail.reset_index(drop=True)


def estimate_suppressed_ec_matfuel(fba: pd.DataFrame, **_: Any) -> pd.DataFrame:
    """
    Recover suppressed ``Census_EC_MatFuel`` cells from the published industry totals.

    Census withholds an industry x material cell when publishing it would
    disclose an individual company - **412 cells in 2017 and 330 in 2022**. The
    value is not zero: it is still inside that industry's published total, so
    leaving it at zero biases the materials mix toward whatever happens to be
    publishable, which is systematically the *large* materials.

    ✅ **The control is exact, and that is measured rather than assumed.** For
    every industry with no withheld child, the named materials sum to
    ``00772000`` "Total Materials" to within 0.1% - **238 of 238 industries in
    2017 and 247 of 247 in 2022**. Fuels have their own exact control in
    ``00772002`` "Total Fuels", and every industry carrying fuel rows carries
    that control (28 of 28, and 21 of 21). So the two kinds are recovered
    separately against their own totals; pooling them would mix two hierarchies
    that Census keeps apart.

    ⚠️ **The split weights are not equal shares.** ``estimate_suppressed_ec_pxi``
    divides a residual equally because it has nothing better; here there is
    something better, and equal shares would be a poor choice because the
    withheld set usually has **two** members (96 of 150 industries in 2017), so
    an equal split is close to a coin flip on which material gets the mass.
    Instead the residual is shared in proportion to each material's
    **economy-wide published cost in the same vintage** - paper gets more of an
    unexplained residual than diamonds do, which is the right prior and costs
    nothing.

    ⚠️ **A recovered cell is a rough placement, not a measurement, and the
    holdout says how rough.**  Masking published cells and recovering them gives
    a weighted absolute percentage error of **0.60 in 2017 and 0.72 in 2022** --
    the industry total is exact by construction, so all of that is *allocation*
    error across materials within the column.  Anything reading these cells must
    treat ``SuppressionRecovery`` as a quality flag, and any diagnostic sensitive
    to the within-column mix should restrict to industries with nothing withheld
    rather than trusting the fill.

    ⚠️ **Deliberately within-vintage.** The other census year is an obvious and
    much stronger prior for the same ``(industry, material)`` cell, and it is
    **not** used: filling 2022 from 2017 would make the two vintages more alike
    and bias the 2017 -> 2022 movement measurement toward zero, which is the
    headline finding this source exists to support. A recovery must not quietly
    manufacture the answer the analysis is testing for.

    Three outcomes, recorded in ``SuppressionRecovery`` so a consumer can tell a
    measurement from an estimate - the same vocabulary as the PxI recovery:

    - ``exact`` - one withheld cell in the industry, so the residual belongs to
      it entirely and this is a recovered measurement, not a guess. Rare here:
      2 industries in 2017, none in 2022.
    - ``split`` - two or more withheld, residual shared on the weights above.
    - ``unrecoverable`` - the industry's own total is withheld (2 industries in
      2017, none in 2022), so there is nothing to subtract from. Left at zero.

    ⚠️ **The control rows are dropped on the way out.** Once the residual is
    distributed the detail sums to the total by construction, so keeping both
    double counts the entire table - $5.77tn against a true $2.87tn in 2017.
    Callers wanting the control should take it before calling this.

    :param fba: the ``Census_EC_MatFuel`` FlowByActivity
    :return: material detail only, with withheld cells filled where possible
    """
    df = fba.copy()
    is_control = df['ActivityProducedBy'].isin(MATFUEL_TOTAL_CODES)
    # 'Material consumed' / 'Fuel consumed' - the M_FI flag, which the parse
    # turned into FlowName. Each kind has its own total and its own hierarchy.
    kind = df['FlowName']
    control_kind = df['ActivityProducedBy'].map(
        {'00772000': 'Material consumed', '00772002': 'Fuel consumed'}
    )

    controls = (
        df[is_control]
        .assign(_kind=control_kind[is_control], _sup=lambda x: x['Suppressed'].notna())
        .set_index(['ActivityConsumedBy', '_kind'])[['FlowAmount', '_sup']]
    )
    detail = df[~is_control].copy()
    key = pd.MultiIndex.from_arrays(
        [detail['ActivityConsumedBy'], kind[~is_control]], names=controls.index.names
    )
    detail_withheld = detail['Suppressed'].notna()

    published_by_control = (
        detail[~detail_withheld]
        .groupby(key[~detail_withheld])
        .sum(numeric_only=True)['FlowAmount']
    )
    residual = controls['FlowAmount'].sub(
        published_by_control.reindex(controls.index).fillna(0.0), fill_value=0.0
    )
    # Published children exceeding their own total is a rounding artefact, not
    # licence to invent negative mass.
    overshoot = int((residual < 0).sum())
    residual = residual.clip(lower=0.0)

    # Prior: what the industry's **peers** actually buy of that material, this
    # vintage only. Peers are the other industries sharing its NAICS-4 group.
    #
    # ⚠️ An economy-wide prior was tried first and is wrong: it weights by how
    # large a material is across all of manufacturing, so an idiosyncratic
    # industry gets the economy's shopping list rather than its own. The peer
    # prefix length is chosen by holdout (see NAICS_PEER_GROUP_LENGTH), and
    # economy-wide survives only as the fallback for a material no peer
    # publishes.
    published = detail[~detail_withheld]
    peer = published['ActivityConsumedBy'].str[:NAICS_PEER_GROUP_LENGTH]
    by_peer = published.groupby([peer, published['ActivityProducedBy']])[
        'FlowAmount'
    ].sum()
    economy_wide = published.groupby('ActivityProducedBy')['FlowAmount'].sum()

    lookup = pd.MultiIndex.from_arrays(
        [
            detail['ActivityConsumedBy'].str[:NAICS_PEER_GROUP_LENGTH],
            detail['ActivityProducedBy'],
        ]
    )
    weights = (
        pd.Series(by_peer.reindex(lookup).to_numpy(), index=detail.index)
        .fillna(detail['ActivityProducedBy'].map(economy_wide))
        .fillna(0.0)
        .where(detail_withheld, 0.0)
    )
    # A material withheld everywhere has no published mass to weight by; fall
    # back to equal shares for that industry rather than dropping it.
    weight_total = weights.groupby(key).transform('sum')
    n_withheld = detail_withheld.groupby(key).transform('sum')
    share = np.where(
        weight_total > 0,
        weights / weight_total.replace(0, np.nan),
        1.0 / n_withheld.replace(0, np.nan),
    )

    control_suppressed = (
        pd.Series(controls['_sup'].reindex(key).to_numpy(), index=detail.index)
        .astype('boolean')
        .fillna(True)
    )
    fill = pd.Series(residual.reindex(key).to_numpy(), index=detail.index) * share
    detail.loc[detail_withheld, 'FlowAmount'] = (
        fill[detail_withheld]
        .where(~control_suppressed[detail_withheld], 0.0)
        .fillna(0.0)
    )

    how = np.where(n_withheld == 1, 'exact', 'split')
    how = np.where(control_suppressed, 'unrecoverable', how)
    detail['SuppressionRecovery'] = pd.Series(how, index=detail.index).where(
        detail_withheld
    )

    counts = detail.loc[detail_withheld, 'SuppressionRecovery'].value_counts()
    log.info(
        'Census_EC_MatFuel suppression recovery: %s exact, %s split, %s '
        'unrecoverable; %s industry totals had published children exceeding '
        'them (residual clipped to 0). Dropped %s control rows to avoid double '
        'counting.',
        int(counts.get('exact', 0)),
        int(counts.get('split', 0)),
        int(counts.get('unrecoverable', 0)),
        overshoot,
        int(is_control.sum()),
    )
    return detail.reset_index(drop=True)


def move_pxi_product_to_activity(fba: pd.DataFrame, **_: Any) -> pd.DataFrame:
    """Put the product line into ``ActivityConsumedBy`` so it can be mapped.

    ``Census_EC_PxI`` carries the industry in ``ActivityProducedBy`` and the
    product in ``FlowName``, which is the right shape for reading the data but
    the wrong one for attribution: ``activity_to_sector_mapping`` joins its
    crosswalk on the *activity* columns and never on ``FlowName``. The product
    is what maps to a BEA commodity, so it has to move into an activity column
    before ``Sector_Crosswalk_Census_EC_PxI`` can reach it.

    The industry is parked in ``FlowName`` rather than dropped, because the
    trade attribution still needs it: a NIPA trade line is matched to its PxI
    weights through the holding industry's NAICS.

    ⚠️ Run this **before** sector mapping, as ``clean_fba``. Running it after
    leaves the crosswalk with nothing to join on and yields an empty weight set
    rather than an error - the same silent-empty failure the NAICS prefix guard
    in ``write_inventories_trade_crosswalk`` exists to catch.
    """
    return fba.assign(
        ActivityConsumedBy=normalize_pxi_product(fba['Description']),
        ActivityProducedBy=_nipa_line_for_industry(fba['ActivityProducedBy']),
        FlowName=fba['ActivityProducedBy'],
    )


def _nipa_line_for_industry(naics: pd.Series) -> pd.Series:
    """Label each PxI row with the NIPA inventory line whose industry holds it.

    ⚠️ This is what makes ``attribute_on: ['PrimarySector', 'ActivityProducedBy']``
    work. Without it the source carries no ``ActivityProducedBy`` at all, the
    join key is ``(sector, 'N/A', location)`` on one side and the trade line
    name on the other, and **nothing matches** - every trade set then reports
    "Could not attribute ... due to lack of flows" and drops to a 100% loss.

    Matching on ``PrimarySector`` alone is not the alternative: that weights
    each commodity by its economy-wide product total rather than by what the
    holding industry actually sells, which is the defect #547 records for PCE.

    The industry to line correspondence is read from the inventories crosswalk's
    ``Note``, which carries the NAICS each NIPA line stands for. Longest prefix
    wins, so ``42343`` picks the computers line over the broader ``4234``.
    """
    from bedrock.utils.config.settings import crosswalkpath  # noqa: PLC0415

    cw = pd.read_csv(
        crosswalkpath / 'Sector_Crosswalk_BEA_NIPA_Inventories.csv', dtype=str
    )
    pairs: list[tuple[str, str]] = []
    for _, row in cw.drop_duplicates('Activity').iterrows():
        note = str(row.get('Note', ''))
        if 'NAICS ' not in note:
            continue
        advertised = note.split('NAICS ', 1)[1].split(';')[0].strip()
        pairs.extend((prefix, row['Activity']) for prefix in advertised.split('/'))
    # Longest prefix first so a specific line beats the broader one it sits in.
    pairs.sort(key=lambda pair: len(pair[0]), reverse=True)

    codes = naics.astype(str)
    out = pd.Series(pd.NA, index=naics.index, dtype='object')
    for prefix, activity in pairs:
        out = out.mask(out.isna() & codes.str.startswith(prefix), activity)
    return out


def normalize_pxi_product(description: pd.Series) -> pd.Series:
    """Reduce a product label to the key ``Sector_Crosswalk_Census_EC_PxI`` uses.

    ⚠️ The crosswalk is keyed on the **description**, not on the
    ``Census_2017_PxI_product_code``, because the code churns across vintages -
    525 are new in 2022 and 548 gone, with the large "new" ones being recodings
    of products already present in 2017. Keying on it would drop roughly 15% of
    coverage at the boundary.

    The same label appears as both "Wholesale sales of X" and "Retail sales of
    X", so the prefix comes off and the rest is lowercased: one concordance
    entry then serves both sides of the trade.

    ⚠️ This must stay in step with the normalisation in
    ``write_pxi_product_bea_crosswalk``. They are two halves of one join, and a
    mismatch does not raise - it silently yields **no** overlap, an empty
    attribution source, and eventually an obscure pandas error about setting a
    DataFrame into a single column.
    """
    return (
        description.astype(str)
        .str.strip()
        .str.lower()
        .str.rstrip('.')
        .str.replace(r'^(wholesale|retail) sales of ', '', regex=True)
        .str.strip()
    )


def prepare_pxi_for_attribution(fba: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
    """Recover suppressed cells, then reshape for attribution - in that order.

    ⚠️ Both steps have to happen in one hook because ``prepare_fbs`` runs
    ``clean_fba_before_mapping`` **before** ``estimate_suppressed``, which is the
    opposite of what these two need. Wiring them to those two sockets separately
    moves the product into ``ActivityConsumedBy`` first, and the suppression
    recovery then looks for its ``00`` product totals in a column that no longer
    holds NAICS - it silently finds none, reports "Dropped 0 product-total rows"
    instead of 3,523, and leaves every withheld cell at zero.

    Recovery must come first regardless: it needs the ``00`` parent rows to
    subtract published children from, and the reshape is what removes them.
    """
    return move_pxi_product_to_activity(estimate_suppressed_ec_pxi(fba, **kwargs))


if __name__ == "__main__":
    import bedrock

    bedrock.extract.generateflowbyactivity.main(source='Census_EC', year=2017)
    fba = bedrock.extract.flowbyactivity.getFlowByActivity('Census_EC', 2017)
