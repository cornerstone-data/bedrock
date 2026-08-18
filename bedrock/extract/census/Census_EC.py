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
        ActivityConsumedBy=fba['FlowName'],
        FlowName=fba['ActivityProducedBy'],
        ActivityProducedBy=None,
    )


if __name__ == "__main__":
    import bedrock

    bedrock.extract.generateflowbyactivity.main(source='Census_EC', year=2017)
    fba = bedrock.extract.flowbyactivity.getFlowByActivity('Census_EC', 2017)
