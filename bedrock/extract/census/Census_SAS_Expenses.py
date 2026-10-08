# Census_SAS_Expenses.py (bedrock)
# !/usr/bin/env python3
# coding=utf-8

"""
Service Annual Survey, Table 5 "Estimated Selected Expenses for Employer Firms".

**The service-sector counterpart to** ``Census_ASM_Expenses``, and the only
source in the build that observes a service industry's purchased-input cells
year by year rather than once a benchmark.

Why this exists as a source separate from :mod:`Census_SAS`. That source reads
one workbook - the latest, ``sas-22.xlsx`` - because Tables 2, 3 and 8 publish
their whole history in it. **Table 5 does not.** Each SAS vintage prints only
the years its own release covers, so the latest workbook shows 2020-2022 and
nothing earlier, which is why #564 recorded Table 5 as "2020-2022 only". It is
not: the 2017 vintage carries 2013-2017 under the same sheet name, the same item
names and the same industry list. This source splices the two.

✅ **What that buys, measured** (#705,
``analysis/nowcasting/intermediate_estimation_plan.md`` §Sourcing the columns
that actually drift): **63 industries at 2- to 4-digit NAICS** - ``5412``,
``5413``, ``5415``, ``81``, ``722``, ``622`` and the rest published separately,
not one row per sector - across roughly 35 expense items, of which about 19 map
to a BEA commodity. Against the 2017 detail Use table those items reach 38-72%
of the columns they describe, because a service industry has no materials bill
for them to hide behind.

⚠️ **There is no *usable* 2018 or 2019, and the reason is not the one this
docstring used to give.** ❌ It said both vintages publish the sheet for those
years with only a handful of items populated. **Neither is true.** ``sas-17``
carries 2013-2017 and ``sas-22`` carries 2020-2022; **2018 and 2019 are in
neither**, because they live in ``sas-18.xlsx`` and ``sas-19.xlsx``, which this
source does not fetch. ✅ **There they are fully populated** - 8 of 8 cells for
every NAICS - **on a cut item list**: Census reduced the detailed-expense
content for those two collection years and restored it in 2020.

======  =================  ================
sheet    items per NAICS    distinct items
======  =================  ================
sas-17         24-28              40
sas-19          8-12              24
sas-22         18-22              33
======  =================  ================

❌ **What sas-19 drops is exactly the seed's input list**: purchased
electricity, communication services, fuels, professional and technical
services, advertising, repairs to buildings, repairs to machinery, water and
sewer, lease and rental of buildings, lease and rental of machinery, and data
processing. ✅ **What it keeps is the total ``Expenses`` line**, which is a
denominator rather than a mix -- usable for :func:`industry_growth` and as a
control, and not fetched today. The gap in the *mix* is real and is why
:data:`SAS_EXPENSE_UNOBSERVED_YEARS` exists;
do not read an absent 2019 cell as a fall to zero.

⚠️ **The two vintages sit on different Economic Census benchmarks, and this is
the defect that governs how the source may be used.** ``sas-17`` states its
estimates are adjusted to the **2012** Economic Census and ``sas-22`` to the
**2017** one, and no restated 2017 exists - the detailed-expense series
*restarts* at 2020. So a ratio taken across the gap carries a rebenchmark as
well as whatever the economy did. The benchmark each row was built on is written
into ``Description`` for exactly this reason, so a consumer can see the seam
rather than infer it.

⚠️ **Three mappable items were discontinued after 2017** - lease and rental
payments for machinery, purchased communication services, and water/sewer/refuse
- so the item *sets* also differ across the seam. Intersect before differencing.

⚠️ **Amounts are millions of dollars**, unlike the Census API sources in this
directory, which publish thousands.

⚠️ **Every NAICS level is published**, 2- through 6-digit, and parents cover
their children. Filter before summing or the table multiplies.

⚠️ **No ``source_catalog.yaml`` entry yet**, on the same reasoning as
``Census_ASM_Expenses``: nothing attributes this to sectors, it is read directly
by ``analysis/nowcasting/inputs_structure.py``. Inheriting :mod:`Census_SAS`'s
entry would be wrong - that source's activities are revenue as well as expense.
Write a real entry when the first FBS consumes this.
"""

from __future__ import annotations

import functools
import io
import os
from typing import Any

import numpy as np
import pandas as pd

from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.utils.io.gcp import download_extract_input_from_gcs_if_not_exists
from bedrock.utils.io.local_extract_input_data import local_extract_input_dir
from bedrock.utils.logging.flowsa_log import log
from bedrock.utils.mapping.location import US_FIPS

#: The sheet, in both vintages. SAS numbers its tables consistently across
#: releases, so the name is stable even though the year columns are not.
SAS_EXPENSE_SHEET = 'Table 5'

#: ⚠️ **The cut-list years** -- collection years Census consolidated the
#: questionnaire for, published in ``sas-19.xlsx`` with 23 items instead of 40.
#: The name is historical: they were "unobserved" while ``sas-19`` went
#: unfetched, and every consumer built on the FULL item list must go on
#: refusing them, so the constant keeps its name and its members.  What changed
#: is that a seed CAN now be built for them -- by constraining at the published
#: aggregates and assigning within them on 2017 proportions, which is
#: :mod:`bedrock.analysis.nowcasting.services_transport_expense_seed`'s
#: cut-list bridge, not this module's concern.
SAS_EXPENSE_UNOBSERVED_YEARS = (2018, 2019)

#: The same two years under the name the bridge uses.
SAS_CUT_LIST_YEARS = SAS_EXPENSE_UNOBSERVED_YEARS

#: The cut-list workbook.  ⚠️ ``sas-19`` is benchmarked to the **2017
#: Economic Census** and restates 2013-2018 on that basis, so ratios from its
#: restated 2017 to its 2018 and 2019 stay inside one instrument and one
#: benchmark.  ``sas-18.xlsx`` (2012-EC basis) is deliberately not read; its
#: 2018 duplicates this workbook's on the older benchmark.
SAS_CUT_LIST_WORKBOOK = 'sas-19.xlsx'

#: Census's suppression flags, as :mod:`Census_SAS` handles them. ``(s)`` is a
#: suffix on an otherwise real number rather than a replacement for one.
_SUPPRESSION_FLAGS = ('S', 'Z', 'D', 'ZZ', 'NA')

#: SAS publishes millions of dollars.
_MILLIONS = 1_000_000.0


def _vintage_for(config: dict[str, Any], url: str) -> tuple[str, dict[str, Any]]:
    """The ``vintages`` entry a url belongs to, keyed by workbook filename."""
    filename = os.path.basename(url)
    try:
        return filename, config['vintages'][filename]
    except KeyError:  # pragma: no cover - a config typo, not a data condition
        raise ValueError(
            f'{filename} is not named in the vintages block of '
            f'Census_SAS_Expenses.yaml; the url helper and the cache both key '
            f'on the workbook filename.'
        ) from None


def _read_table_5(
    handle: Any,
    filename: str,
    benchmark: str,
    years: list[int] | None = None,
) -> pd.DataFrame:
    """Read the sheet, finding its header row rather than assuming one.

    ⚠️ **The header row moves between vintages** - row 4 in ``sas-17`` and row 5
    in ``sas-22``, because the later workbook carries an extra note line. Both
    are found by looking for the row whose first cell is ``NAICS``, which is
    stable where a row offset is not.

    ⚠️ ``years`` carves the vintage down to the years it *contributes*, per the
    config's per-vintage ``years:`` key.  ``sas-19`` restates 2013-2018 on the
    2017 benchmark, and without this carve those columns would silently replace
    ``sas-17``'s in every earlier FBA year -- the duplicate rule in the parser
    keeps the later benchmark.  The restated history stays reachable through
    :func:`restated_cut_vintage`; it is just not the FBA.
    """
    raw = pd.read_excel(handle, sheet_name=SAS_EXPENSE_SHEET, header=None, nrows=12)
    first = raw[0].astype(str).str.strip()
    matches = first.index[first == 'NAICS']
    if len(matches) == 0:
        raise ValueError(
            f'{filename} has no NAICS header row in the first 12 rows of '
            f'{SAS_EXPENSE_SHEET}; the workbook layout has changed.'
        )
    header = int(matches[0])

    if hasattr(handle, 'seek'):
        handle.seek(0)
    df = pd.read_excel(
        handle, sheet_name=SAS_EXPENSE_SHEET, header=header, dtype={'NAICS': str}
    )
    df.columns = [str(c).strip() for c in df.columns]

    # The footnote block sits below the data and reuses the NAICS column for its
    # markers - 'Footnotes', then '1', '2', 'S', '(s)'. Both conditions are
    # needed: a marker row has no Item, and no marker is a two-digit code.
    df = df[df['Item'].notna()].copy()
    df['NAICS'] = df['NAICS'].astype(str).str.strip()
    df = df[df['NAICS'].str.fullmatch(r'\d{2,6}')]
    df['Item'] = df['Item'].astype(str).str.strip()

    if years is not None:
        wanted = {str(y) for y in years}
        df = df[[c for c in df.columns if not c[:4].isdigit() or c[:4] in wanted]]

    # The vintage and the benchmark it was built on ride along on every row.
    # This is the seam a consumer has to see (module docstring), so it is data
    # rather than a comment.
    df['Description'] = (
        f'{SAS_EXPENSE_SHEET}: Estimated Selected Expenses for Employer Firms '
        f'({filename}, benchmarked to the {benchmark})'
    )
    return df


def census_sas_expenses_url_helper(*, config: dict[str, Any], **_: Any) -> list[str]:
    """One url per vintage, from the ``vintages`` block.

    ⚠️ The two workbooks are not in the same directory on ``www2.census.gov`` -
    the 2017 vintage sits under ``services/`` and the 2022 one under ``sas/`` -
    so each entry carries its whole url rather than a filename appended to a
    shared base.
    """
    return [vintage['url'] for vintage in config['vintages'].values()]


def census_sas_expenses_call(
    *,
    resp: Any,
    url: str,
    config: dict[str, Any],
    source: str = 'Census_SAS_Expenses',
    **_: Any,
) -> pd.DataFrame:
    """Cache the downloaded workbook under extract-input, then read Table 5.

    Only reached with ``extract_data_from_raw_sources: True``. Each vintage is
    cached under its own published filename so that a new SAS release is a
    config change rather than a silent substitution, as in :mod:`Census_SAS`.
    """
    filename, vintage = _vintage_for(config, url)
    local_path = os.path.join(local_extract_input_dir(source, year=None), filename)
    with open(local_path, 'wb') as f:
        f.write(resp.content)
    return _read_table_5(
        io.BytesIO(resp.content),
        filename,
        vintage['benchmark'],
        years=vintage.get('years'),
    )


def census_sas_expenses_load_gcs(
    *,
    url: str,
    config: dict[str, Any],
    source: str = 'Census_SAS_Expenses',
    **kwargs: Any,
) -> pd.DataFrame:
    """Read one vintage from the local cache, or GCS extract-input if missing."""
    filename, vintage = _vintage_for(config, url)
    directory = local_extract_input_dir(source, year=None)
    local_path = os.path.join(directory, filename)
    if not os.path.exists(local_path):
        download_extract_input_from_gcs_if_not_exists(
            # one workbook per vintage covering several years, so both sit
            # directly under extract/input-data/Census_SAS_Expenses/
            {**kwargs, 'source': source, 'url': url, 'year': None},
            local_dir=directory,
            object_name=filename,
        )
    if not os.path.exists(local_path):
        raise FileNotFoundError(
            f'{filename} is neither cached at {local_path} nor available from '
            f'gs://cornerstone-default/extract/input-data/{source}/. Set '
            f'extract_data_from_raw_sources: True in {source}.yaml to fetch it '
            f'from Census and cache it, then upload it so others need not.'
        )
    return _read_table_5(
        local_path, filename, vintage['benchmark'], years=vintage.get('years')
    )


def census_sas_expenses_parse(*, df_list: list, **_: Any) -> pd.DataFrame:
    """Melt the spliced vintages into FBA form.

    ``ActivityConsumedBy`` is the NAICS industry throughout - every row of
    Table 5 is a purchase by the industry, so unlike :func:`Census_SAS.census_sas_parse`
    there is no revenue sheet to redirect into ``ActivityProducedBy``.
    """
    df = pd.concat(df_list, sort=False)

    estimates = [c for c in df.columns if c.endswith('Estimate')]
    spreads = [c for c in df.columns if c.endswith('Coefficient of Variation')]
    id_vars = ['NAICS', 'Item', 'Description']

    def _long(columns: list[str], name: str) -> pd.DataFrame:
        out = df.melt(
            id_vars=id_vars, value_vars=columns, var_name='column', value_name=name
        )
        # ⚠️ A string, not an int. ``call_all_years`` selects each year with
        # ``dfs.query('Year == @year')`` and passes ``year`` as a str, so an
        # int column silently matches nothing and writes empty parquets.
        # ``process_data_frame`` casts to the schema's int afterwards.
        out['Year'] = out['column'].str[:4]
        # ⚠️ The concatenated frame carries every vintage's year columns, so each
        # vintage melts to a full grid and is blank outside its own years. Those
        # blanks are the other workbook's years, not withheld cells - dropping
        # them here is what keeps the two vintages from colliding below.
        return out.drop(columns='column').dropna(subset=[name])

    long = _long(estimates, 'FlowAmount').merge(
        _long(spreads, 'Spread'), on=[*id_vars, 'Year'], how='left'
    )

    # ⚠️ Two vintages overlap on no year today, but a future release could, and
    # a duplicated industry-item-year would double on any groupby. Keep the
    # later benchmark where they ever do.
    duplicated = long.duplicated(subset=['NAICS', 'Item', 'Year'], keep=False)
    if bool(duplicated.any()):
        log.warning(
            'Census_SAS_Expenses: %s industry-item-year rows appear in more than '
            'one vintage; keeping the later benchmark.',
            int(duplicated.sum()),
        )
        long = long.sort_values('Description').drop_duplicates(
            subset=['NAICS', 'Item', 'Year'], keep='last'
        )

    amount = long['FlowAmount'].astype(str).str.strip()
    # A flagged cell is withheld, not zero. Recorded as zero with the flag kept,
    # as Census_SAS and Census_EC_MatFuel both do, so a recovery step can find
    # them rather than having to distinguish them from a published zero.
    flagged = amount.isin(_SUPPRESSION_FLAGS)
    # '(s)' marks an estimate that does not meet publication standards but is
    # published anyway - a real number with a caveat, so the number is kept.
    low_quality = amount.str.endswith('(s)')
    long = long.assign(
        Suppressed=np.select([flagged, low_quality], [amount, '(s)'], default=None),
        FlowAmount=np.select(
            [flagged, low_quality],
            ['0', amount.str.replace(',', '', regex=False).str[:-3]],
            default=amount.str.replace(',', '', regex=False),
        ),
    )
    long['FlowAmount'] = pd.to_numeric(long['FlowAmount'], errors='coerce')
    long = long[long['FlowAmount'].notna()]

    return (
        long.rename(columns={'NAICS': 'ActivityConsumedBy', 'Item': 'FlowName'})
        .assign(
            ActivityProducedBy=None,
            Class='Money',
            SourceName='Census_SAS_Expenses',
            FlowAmount=lambda x: x['FlowAmount'] * _MILLIONS,
            Spread=lambda x: pd.to_numeric(x['Spread'], errors='coerce'),
            MeasureofSpread='Coefficient of Variation',
            Unit='USD',
            FlowType='ELEMENTARY_FLOW',
            Compartment=None,
            Location=US_FIPS,
            DataReliability=5,
            DataCollection=5,
        )
        .pipe(assign_fips_location_system, 2024)
        .reset_index(drop=True)
    )


@functools.cache
def restated_cut_vintage() -> pd.DataFrame:
    """The whole ``sas-19`` workbook as tidy ``naics x item x year``, in $M.

    ⚠️ **This is the one road to the restated 2013-2017.**  The FBA carves each
    vintage to the years it contributes (2018-2019 for ``sas-19``), because
    letting the restated history in would silently replace ``sas-17``'s rows
    under every earlier year.  The cut-list bridge in
    :mod:`~bedrock.analysis.nowcasting.services_transport_expense_seed` needs
    the restated **2017** as the base of its within-``sas-19`` ratios, and the
    rebenchmark measurement needs 2013-2016 too, so the cached workbook is read
    directly here rather than through the FBA.

    Suppressed cells (``S``/``Z``/``D``/``ZZ``/``NA``) are dropped -- an absent
    movement is not a movement of zero -- and ``(s)``-flagged low-quality cells
    are kept as numbers, both exactly as :func:`census_sas_expenses_parse` does.
    """
    directory = local_extract_input_dir('Census_SAS_Expenses', year=None)
    local_path = os.path.join(directory, SAS_CUT_LIST_WORKBOOK)
    if not os.path.exists(local_path):
        download_extract_input_from_gcs_if_not_exists(
            {'source': 'Census_SAS_Expenses', 'year': None},
            local_dir=directory,
            object_name=SAS_CUT_LIST_WORKBOOK,
        )
    frame = _read_table_5(local_path, SAS_CUT_LIST_WORKBOOK, '2017 Economic Census')
    estimates = [c for c in frame.columns if c.endswith('Estimate')]
    long = frame.melt(
        id_vars=['NAICS', 'Item'],
        value_vars=estimates,
        var_name='column',
        value_name='value',
    )
    long['year'] = long['column'].str[:4].astype(int)
    amount = long['value'].astype(str).str.strip()
    long = long[~amount.isin(_SUPPRESSION_FLAGS)].copy()
    cleaned = (
        long['value']
        .astype(str)
        .str.strip()
        .str.replace(',', '', regex=False)
        .str.replace(r'\(s\)$', '', regex=True)
    )
    long['value'] = pd.to_numeric(cleaned, errors='coerce')
    long = long[long['value'].notna()]
    return pd.DataFrame(
        long.rename(columns={'NAICS': 'naics', 'Item': 'item'})
        .groupby(['naics', 'item', 'year'], as_index=False)['value']
        .sum()
    )


if __name__ == '__main__':
    import bedrock

    bedrock.extract.generateflowbyactivity.main(
        source='Census_SAS_Expenses', year='2013-2022'
    )
    fba = bedrock.extract.flowbyactivity.getFlowByActivity('Census_SAS_Expenses', 2022)
