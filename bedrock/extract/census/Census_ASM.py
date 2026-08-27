# Census_ASM.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Pulls Census Annual Survey of Manufacturers
--year = 'year' e.g. 2015
"""

import json
from typing import Any

import numpy as np
import pandas as pd

from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.utils.logging.flowsa_log import log
from bedrock.utils.mapping.location import US_FIPS

#: The all-products row: each industry's total value of shipments, published on
#: every NAICS level. This is the suppression control - see
#: :func:`estimate_suppressed_asm_pxi`.
ASM_PRODUCT_TOTAL = '0000000000'


def asm_URL_helper(*, build_url, year, **_):
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
    # This section gets the census data by county instead of by state.
    # This is only for years 2010 and 2011. This is done because the State
    # query that gets all counties returns too many results and errors out.

    url = build_url
    # url = url.replace("%3A%2A", ":*")
    urls_census.append(url)

    return urls_census


def asm_call(*, resp, **_):
    """
    Convert response for calling url to pandas dataframe, begin
        parsing df into FBA format
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    cbp_json = json.loads(resp.text)
    # convert response to dataframe
    df_census = pd.DataFrame(data=cbp_json[1 : len(cbp_json)], columns=cbp_json[0])
    return df_census


def asm_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    # concat dataframes
    df = pd.concat(df_list, sort=False)

    df = df.rename(
        columns={
            'NAICS2017': 'ActivityProducedBy',
            'RCPTOT': 'FlowAmount',
            'YEAR': 'Year',
        }
    )

    df['Location'] = US_FIPS
    df['FlowName'] = 'Sales'
    df['Unit'] = 'Thousand USD'
    df['Class'] = 'Money'

    # add location system based on year of data
    df = assign_fips_location_system(df, year)
    # hard code data
    df['SourceName'] = 'Census_ASM'
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    df['Compartment'] = None
    return df


def asm_pxi_URL_helper(*, build_url, year, **_):
    """
    The ASM value-of-shipments endpoint, one url per year.

    Unlike :func:`asm_URL_helper` the year is a ``time=`` predicate rather than
    part of the path, so it is substituted here.
    """
    return [build_url.replace('__year__', str(year))]


def asm_pxi_parse(*, df_list, year, **_):
    """
    Format ASM industry x product value of shipments into an FBA.

    ``ActivityProducedBy`` is the NAICS industry and ``FlowName`` the 2017 NAPCS
    collection code, matching :mod:`Census_EC_PxI` so one product -> commodity
    concordance serves both.

    ⚠️ **Every NAICS level is kept**, 3- through 6-digit plus the ``31-33``
    all-manufacturing row. The rollups are not redundant here: 57% of six-digit
    cells publish as zero under disclosure suppression, and a five-digit parent
    minus its published children is the only available control for putting that
    value back. Filtering to six digits at extract time would throw the control
    away and silently understate every commodity.
    """
    df = pd.concat(df_list, sort=False)

    df = df.rename(
        columns={
            'NAICS2017': 'ActivityProducedBy',
            'NAPCS2017': 'FlowName',
            'NAPCSDOL': 'FlowAmount',
        }
    )
    df['FlowAmount'] = pd.to_numeric(df['FlowAmount'], errors='coerce')
    df['Year'] = year
    df['Location'] = US_FIPS
    # f.o.b. plant, net of discounts, excluding freight and excise taxes - so
    # basic value rather than producer. Verified against the published 2017
    # table: tobacco and distilleries, where excise is ~75% of value, both build
    # to 0.99 of basic and 0.56 of producer.
    df['Unit'] = 'Thousand USD'
    df['Class'] = 'Money'
    df['FlowType'] = 'TECHNOSPHERE_FLOW'

    df = assign_fips_location_system(df, year)
    df['SourceName'] = 'Census_ASM_PxI'
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    df['Compartment'] = None
    return df


#: The ``Census_ASM_Expenses`` cost variables, mapped to the expense each names.
#: ``CSTMTOT`` is deliberately absent: it is the sum of the five cost cells and
#: would double the table, exactly as ``00772000`` does in ``Census_EC_MatFuel``.
#: It is kept in the pull as a control and dropped here.
ASM_EXPENSE_FLOWS = {
    'CSTMPRT': 'Cost of materials, parts and containers',
    'CSTFU': 'Cost of fuels',
    'CSTELEC': 'Cost of purchased electricity',
    'CSTCNT': 'Cost of contract work',
    'CSTRSL': 'Cost of goods purchased for resale',
    'PCHADVT': 'Purchased advertising services',
    'PCHCMPQ': 'Purchased computers and peripherals expensed',
    'PCHCSVC': 'Purchased communication services',
    'PCHDAPR': 'Purchased data processing and hosting services',
    'PCHEXSO': 'Purchased software expensed',
    'PCHPRTE': 'Purchased professional and technical services',
    'PCHRFUS': 'Purchased refuse removal services',
    'PCHRPR': 'Purchased repair and maintenance services',
    'PCHTEMP': 'Purchased personnel supply services',
}

#: Rows that are totals of the cells beside them, not expenses in their own
#: right.  ``PCHTT`` totals the ``PCH*`` services, ``CSTMTOT`` the costs;
#: ``RCPTOT`` and ``VALADD`` are output measures kept for scaling.
ASM_EXPENSE_CONTROLS = ('CSTMTOT', 'PCHTT', 'RCPTOT', 'VALADD')

#: ⚠️ Not an expense on a commodity.  ``PCHTAX`` is purchased-services *taxes*,
#: which belong to the taxes-on-production block rather than to intermediate
#: use, so it is pulled for completeness and excluded from the flows above.
ASM_NON_COMMODITY = ('PCHTAX',)


def asm_expenses_parse(*, df_list, year, **_):
    """Format the ASM expense-by-kind table into a long FBA.

    ``ActivityConsumedBy`` is the purchasing NAICS industry and ``FlowName`` the
    expense kind -- the Use table's own orientation, and the same one
    :func:`Census_EC.census_EC_MatFuel_parse` uses, so the two can be
    differenced without a transpose.

    ⚠️ **Every NAICS level is kept**, 3- through 6-digit plus the ``31-33``
    all-manufacturing rollup, on the same reasoning as
    :func:`asm_pxi_parse`: the parents are the only available control, and
    ``'31-33'`` is five characters, so a consumer must match ``^\d+$`` before
    filtering on length.

    ⚠️ **The control columns are kept as rows** rather than dropped, so a
    consumer can check additivity, but they are named in
    :data:`ASM_EXPENSE_CONTROLS` and summing this FBA unfiltered counts the
    table roughly three times.
    """
    df = pd.concat(df_list, sort=False)

    value_columns = [
        column
        for column in (
            *ASM_EXPENSE_FLOWS,
            *ASM_EXPENSE_CONTROLS,
            *ASM_NON_COMMODITY,
        )
        if column in df.columns
    ]
    long = df.melt(
        id_vars=['NAICS2017'],
        value_vars=value_columns,
        var_name='FlowName',
        value_name='FlowAmount',
    ).rename(columns={'NAICS2017': 'ActivityConsumedBy'})
    long['FlowAmount'] = pd.to_numeric(long['FlowAmount'], errors='coerce')
    # ⚠️ A missing cell here is withheld, not zero, and ASM publishes no flag
    # column to tell the two apart. Dropped rather than zero-filled so a
    # consumer cannot silently read suppression as an absent expense.
    withheld = int(long['FlowAmount'].isna().sum())
    long = long[long['FlowAmount'].notna()].copy()

    long['Description'] = long['FlowName'].map(
        ASM_EXPENSE_FLOWS | {code: f'{code} (control)' for code in ASM_EXPENSE_CONTROLS}
    )
    long['ActivityProducedBy'] = None
    long['Year'] = year
    long['Location'] = US_FIPS
    long['Unit'] = 'Thousand USD'
    long['Class'] = 'Money'
    long['FlowType'] = 'TECHNOSPHERE_FLOW'

    long = assign_fips_location_system(long, year)
    long['SourceName'] = 'Census_ASM_Expenses'
    long['DataReliability'] = 5
    long['DataCollection'] = 5
    long['Compartment'] = None

    log.info(
        'Census_ASM_Expenses %s: %s rows over %s industries and %s expense '
        'kinds; %s cells withheld and dropped.',
        year,
        len(long),
        long['ActivityConsumedBy'].nunique(),
        long['FlowName'].nunique(),
        withheld,
    )
    return long


def _numeric_naics_level(fba: pd.DataFrame) -> pd.DataFrame:
    """Add ``naics``/``level``, dropping the ``31-33`` all-manufacturing rows.

    ⚠️ **``'31-33'`` is five characters**, so it survives a ``len == 5`` filter
    alongside real five-digit NAICS and adds a whole second copy of
    manufacturing to whatever is summed. Matching ``^\\d+$`` first is what keeps
    it out. This is the sixth aggregate-row trap in the commodity-output work,
    after ``'TRADE '`` (trailing space, six characters), the ``GSLG*`` rows
    (five), ``T017``, ``Census_EC_PxI``'s ``'00'`` product total, and
    ``ASM_PRODUCT_TOTAL`` itself.
    """
    naics = fba['ActivityProducedBy'].astype(str)
    numeric = naics.str.match(r'^\d+$')
    out = fba[numeric].copy()
    out['naics'] = naics[numeric]
    out['level'] = out['naics'].str.len()
    return out


def _least_suppressed_level(detail: pd.DataFrame) -> int:
    """The NAICS level whose product detail publishes the most value.

    ⚠️ **This is not the finest level, and choosing the finest costs 15% of the
    table.** Published product value by NAICS level, 2021, against a control of
    6,080bn: three-digit 3,534bn, four-digit 4,581bn, six-digit 4,953bn,
    **five-digit 5,695bn**. Disclosure suppression bites hardest where the cell
    is smallest, so the six-digit detail is more withheld than the five-digit
    rollup that contains it; and going coarser than five loses value again
    because ASM tabulates a product against fewer industries there.

    The industry axis is **summed away** on the road to commodity output, so its
    granularity is free to choose - which makes taking the least-suppressed
    level a pure gain rather than a trade against detail. 2018 publishes only
    six-digit rows and is picked accordingly.
    """
    published = detail.groupby('level')['FlowAmount'].sum()
    return int(published.idxmax())


def estimate_suppressed_asm_pxi(fba: pd.DataFrame, **_: Any) -> pd.DataFrame:
    """Recover suppressed ``Census_ASM_PxI`` cells from the industry totals.

    Census withholds an industry x product cell when publishing it would
    disclose an individual company. **57% of six-digit cells publish as zero**,
    and unlike ``Census_EC_PxI`` there is no flag separating a withheld cell
    from a true zero - ``NAPCSDOL_IMP`` is 0 everywhere and ``NAPCSDOL_S`` is a
    standard error. A zero is therefore treated as withheld, which is safe
    because a genuinely absent product leaves no residual to distribute.

    ✅ **The control is the industry's all-products row, not a NAICS parent.**
    ``ASM_PRODUCT_TOTAL`` is published for **every industry at every level** -
    360 of 360 six-digit industries in all four years - because a total across
    all products discloses nothing. It sums to the same national figure at each
    level (5,734bn in 2019), so it is a complete control where the NAICS
    hierarchy is not.

    ⚠️ **This is the transpose of** :func:`Census_EC.estimate_suppressed_ec_pxi`.
    There the control is a product's total across industries, so the recovered
    mass lands in exactly the right *commodity* and recovery is near-lossless
    for commodity output. Here the control is an industry's total across
    products, so the residual's split **across commodities is an equal-share
    guess**. It fixes the level and only approximates the mix - a weaker claim,
    and the reason this function is worth measuring rather than trusting.

    Measured against BEA's published summary Supply ``T007`` over the 19
    manufacturing groups, level ratio and weighted error:

    ==== ================= =================
    year least-suppressed  + recovery
    ==== ================= =================
    2018 0.949 / 6.1%      **0.971 / 4.5%**
    2019 0.912 / 9.4%      **0.948 / 6.1%**
    2020 0.912 / 9.3%      **0.950 / 6.8%**
    2021 0.908 / 9.6%      **0.943 / 6.7%**
    ==== ================= =================

    ⚠️ **The rows are cut to one NAICS level and the totals are dropped.** After
    recovery the detail sums to the industry total by construction, so keeping
    ``ASM_PRODUCT_TOTAL`` would double the table; and keeping several NAICS
    levels would double it again, since each level covers all of manufacturing.
    A caller wanting the control should take it before calling this.

    :param fba: the ``Census_ASM_PxI`` FlowByActivity
    :return: one NAICS level of product detail, suppressed cells filled
    """
    df = _numeric_naics_level(fba)
    is_total = df['FlowName'].astype(str) == ASM_PRODUCT_TOTAL
    level = _least_suppressed_level(df[~is_total])

    rows = df[df['level'] == level]
    control = rows[is_total.reindex(rows.index)].set_index('naics')['FlowAmount']
    detail = rows[~is_total.reindex(rows.index)].copy()

    published = detail[detail['FlowAmount'] > 0].groupby('naics')['FlowAmount'].sum()
    withheld = detail['FlowAmount'] <= 0
    n_withheld = detail[withheld].groupby('naics').size()

    residual = control.sub(published.reindex(control.index).fillna(0.0), fill_value=0.0)
    # A negative residual means the published cells already exceed the industry
    # total - rounding, not something to invent negative mass from.
    overshoot = int((residual < 0).sum())
    per_cell = (residual.clip(lower=0.0) / n_withheld.reindex(residual.index)).replace(
        [np.inf, -np.inf], np.nan
    )

    detail.loc[withheld, 'FlowAmount'] = (
        detail.loc[withheld, 'naics'].map(per_cell).fillna(0.0)
    )
    # ⚠️ Built by masking rather than np.where: mixing a str against np.nan
    # asks numpy for a common dtype that does not exist and raises
    # DTypePromotionError. Every recovered cell is 'split' - there is no 'exact'
    # case here, because an industry with exactly one withheld product is still
    # only pinned on the industry axis, not the commodity one.
    detail['SuppressionRecovery'] = pd.Series(
        'split', index=detail.index, dtype='object'
    ).where(withheld)

    log.info(
        'Census_ASM_PxI suppression recovery: built from %s-digit NAICS (%s '
        'industries), %s withheld cells filled from %s industry totals, '
        'lifting product detail from %.1f%% to %.1f%% of control; %s '
        'industries whose published cells exceeded their own total. Dropped %s '
        'product-total rows to avoid double counting.',
        level,
        len(control),
        int(withheld.sum()),
        int((control > 0).sum()),
        100 * published.sum() / control.sum(),
        100 * detail['FlowAmount'].sum() / control.sum(),
        overshoot,
        int(is_total.sum()),
    )
    return detail.drop(columns=['naics', 'level']).reset_index(drop=True)


def move_asm_product_to_activity(fba: pd.DataFrame, **_: Any) -> pd.DataFrame:
    """Put the NAPCS code into ``ActivityProducedBy`` so it can be mapped.

    ``Census_ASM_PxI`` carries the industry in ``ActivityProducedBy`` and the
    product in ``FlowName``, which is the right shape for reading the data and
    for the suppression recovery above - both need the NAICS hierarchy. It is
    the wrong shape for attribution: ``activity_to_sector_mapping`` joins on the
    activity columns and never on ``FlowName``, and it is the **product** that
    carries a BEA commodity.

    The industry is parked in ``FlowName`` rather than dropped, so a consumer
    can still see which industries produced a commodity - that is the secondary
    production the Supply table's off-diagonal records.

    ⚠️ Run this **after** the recovery and **before** sector mapping. Reversing
    the two leaves the recovery looking for NAICS in a column that now holds
    NAPCS codes: it finds no hierarchy, reports every industry withheld, and
    fills the table with zeros rather than raising. This is the same ordering
    constraint :func:`Census_EC.prepare_pxi_for_attribution` exists to enforce.
    """
    return fba.assign(
        ActivityProducedBy=fba['FlowName'].astype(str),
        FlowName=fba['ActivityProducedBy'].astype(str),
    )


#: The two BEA commodities the "complete passenger vehicles" NAPCS codes cannot
#: tell apart. Autos first - :func:`split_motor_vehicle_output` reads it in
#: order.
MOTOR_VEHICLE_COMMODITIES = ('336111', '336112')


def split_motor_vehicle_output(fbs: pd.DataFrame, **_: Any) -> pd.DataFrame:
    """Re-split cars and light trucks using NIPA table 7.2.5U (#570).

    ⚠️ **The product data cannot make this split.** The three NAPCS codes
    reaching ``336111`` are *"Manufacturing of complete passenger vehicles"*,
    which does not distinguish a car from an SUV or a pickup, so no concordance
    refinement can help - the source does not separate them. Built raw, 2017
    autos come out at 94,600m against 32,758m published while light trucks land
    at 202,068m against 215,421m.

    So the split is taken from where it *is* published annually:
    :func:`~bedrock.extract.bea.BEA_NIPA.motor_vehicle_auto_share`, NIPA's auto
    output over auto plus truck output.

    The two commodities' combined value is held fixed and only its division
    changes; within each producing industry (parked in ``Flowable`` by
    :func:`move_asm_product_to_activity`) the pair total is reallocated, so the
    industry axis - the secondary production the Supply table's off-diagonal
    records - is preserved exactly.

    ⚠️ **What this does and does not fix, measured on 2017.** Scored against the
    published detail Supply block, the pair's absolute error falls from 75,195m
    to 48,489m and the whole manufacturing build from 11.26% to 10.80% weighted
    error. It does not close the gap, because the pair is also **19.5% over in
    total** (296,668m built against 248,179m published), which is a level
    problem the split cannot touch.

    ⚠️ **On 2017 alone the ratio is not identified.** Once the pair is over in
    total, *any* auto share at or below the published 0.132 gives the same
    48,489m - NIPA's 0.178, ``B148RC``/``A953RC``'s 0.156 and the published
    0.132 score identically. The evidence for this hook is therefore not the
    2017 level but the annual movement: the share runs 0.178, 0.135, 0.113,
    0.092, 0.095, 0.055, 0.046, 0.043 over 2017-2024, and no frozen share
    tracks a car-to-truck shift that large.

    ⚠️ **Heavy trucks are deliberately left out.** ``A716RC`` covers heavy trucks
    and buses too - BEA ``336120``, not ``336112`` - so pooling ``336120`` in
    would be the concept-consistent reading, and it does score marginally better
    (10.67%). It is not taken: NIPA publishes no split *within* trucks, so that
    variant has to divide the truck side by the built proportions, which is an
    assumption of ours rather than a published figure, and its 0.13pp gain rides
    on the pool's overage landing in a commodity that happened to be short.
    """
    from bedrock.extract.bea.BEA_NIPA import (  # noqa: PLC0415
        MOTOR_VEHICLE_TABLE,
        motor_vehicle_auto_share,
    )

    auto, truck = MOTOR_VEHICLE_COMMODITIES
    pair = fbs['SectorProducedBy'].isin(MOTOR_VEHICLE_COMMODITIES)
    if not pair.any():
        return fbs

    (year,) = fbs.loc[pair, 'Year'].unique()
    share = motor_vehicle_auto_share(year)

    # The pair total per producing industry is what is held fixed. Rows for the
    # two commodities are collapsed to one row each per industry, which is what
    # a reallocation across commodities means - there is nothing left to keep
    # them apart within an industry once the split is imposed from outside.
    # Everything but the commodity and the amount is carried from the first row
    # of the group: the metadata columns (DataReliability, AttributionSources,
    # SuppressionRecovery) describe the same product data on both sides of a
    # split that is imposed from outside it, so there is nothing to choose.
    reallocated: list[dict[str, Any]] = []
    for _industry, rows in fbs.loc[pair].groupby('Flowable', dropna=False):
        total = float(rows['FlowAmount'].sum())
        template = dict(rows.iloc[0])
        for commodity, amount in (
            (auto, total * share),
            (truck, total * (1 - share)),
        ):
            reallocated.append(
                template | {'SectorProducedBy': commodity, 'FlowAmount': amount}
            )

    log.info(
        f'Motor vehicle split for {year}: auto share {share:.4f} from NIPA '
        f'{MOTOR_VEHICLE_TABLE}; {int(pair.sum())} rows -> {len(reallocated)} '
        f'across {auto}/{truck}'
    )
    return pd.concat(
        [fbs.loc[~pair], pd.DataFrame(reallocated, columns=fbs.columns)],
        ignore_index=True,
    )


def prepare_asm_pxi_for_output(fba: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
    """Recover suppressed cells, then reshape for mapping - in that order.

    ⚠️ Both steps have to happen in one hook because ``prepare_fbs`` runs
    ``clean_fba_before_mapping`` **before** ``estimate_suppressed``, the
    opposite of the order these two need. Wiring them to those two sockets
    separately moves the product into the activity column first, and the
    recovery then finds no NAICS to walk.
    """
    return move_asm_product_to_activity(estimate_suppressed_asm_pxi(fba, **kwargs))
