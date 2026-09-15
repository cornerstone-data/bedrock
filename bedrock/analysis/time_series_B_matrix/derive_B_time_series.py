"""Why does ``E`` move differently from ``x``? The nowcast span, 2017-2024.

`#906 <https://github.com/cornerstone-data/bedrock/issues/906>`_.

``B = (E / x) @ Vnorm``, so the direct intensity ``E / x`` is where a rocky B
trend comes from: a year in which emissions and gross output move by different
amounts. This module builds the span on the **nowcast** models - one
``2025_usa_cornerstone_v0_4_nowcast_<year>`` config per calendar year, so ``E``,
``x`` and ``Vnorm`` all sit on the same year's detail MUT - and then attributes
the ``E``-versus-``x`` gap to the FBS rows that produced it.

**The breakdown is by** ``MetaSources`` **and** ``AttributionSources``, not by
gas. A gas split says *what* was emitted; it cannot say why the number moved.
The two source columns can, because together they name the inventory row and
the vector that spread it across sectors:

``MetaSources``
    which inventory table the emissions came from - ``UMD_GHGIA_T_3_11.
    petroleum_industrial``, ``EPA_GHGI_T_2_1``, and ~65 others.

``AttributionSources``
    what the row was spread across sectors *on*. This is the column that
    explains co-movement with ``x``:

    ====================================  ====================================
    value                                 what the sector split rides on
    ====================================  ====================================
    ``Nowcast_Detail_Use_AfterRedef``     a row of the nowcast **Use** table
    ``BEA_Detail_GrossOutput_IO``         gross output - that is ``x`` itself
    ``Energy_manufacturing_national_*``   the MECS-based energy FBS
    ``Direct``                            nothing; the inventory names sectors
    ``EPA_GHGI_*`` / ``UMD_GHGIA_*``      another inventory table's shares
    ====================================  ====================================

⚠️ **An IO-attributed stratum is not a stratum that tracks** ``x``. Emissions
attributed on ``Nowcast_Detail_Use_AfterRedef`` are spread by each sector's
*purchases of a fuel commodity* - a row of ``U`` - while ``x`` is a row sum of
``V``. Those two move apart whenever a sector's fuel intensity changes relative
to its own output, which is exactly the effect that makes ``B`` rocky. So the
classification below is a statement about *what a stratum's split is derived
from*, and :func:`output_elasticity` is the measurement of whether it actually
tracked ``x``. Reading the class as the answer would beg the question.

The decomposition
-----------------

For sector *s* between consecutive years, with output growth
``g = x[s, t] / x[s, t-1]``, the emissions that *would* have been reported had
every source grown with output are ``E[s, t-1] * g``. The shortfall or excess
is the **divergence**, and because it is linear in ``E`` it splits exactly over
the source strata *k*::

    D[k, s] = E[k, s, t] - E[k, s, t-1] * g[s]

Summed over every sector and stratum, plus the term for the economy's changing
sector mix, this closes on the economy-wide gap exactly::

    E[t] - E[t-1] * G  ==  sum(D[k, s])  +  composition
    composition        ==  sum_s E[s, t-1] * g[s]  -  E[t-1] * G

where ``G`` is economy-wide output growth. :func:`verify_identity` asserts it.

⚠️ **Read** :func:`price_effect` **before reading any source's divergence.**
The nowcast configs set ``apply_io_year_adjustments: False``, so ``x`` is
nominal - the row sum of that year's Make in that year's dollars. Nominal
gross output runs $34.5T to $50.7T over 2017-2024 against $40.9T in constant
2017 dollars, and on the run of 2026-09-14 that price wedge was **73% of the
2021 gap and 73% of the 2022 gap**. A source ranked on nominal ``x`` alone is
being charged for inflation. Every divergence table therefore has a
``*_real`` companion built against the deflated series, and the two close
exactly: ``total_gap_nominal == total_gap_real + price``.

⚠️ **Every figure here is industry emissions only.** ``F01000`` personal
consumption expenditures - household fuel burning and personal vehicles - is a
final-demand column with no gross output, so it cannot enter ``E / x`` and is
dropped by the production path too. That is **21% of published CO2e** in 2017.
:func:`emissions_without_output` reports the excluded mass per year rather than
letting the totals quietly come up short against an inventory.

Running it
----------

Every artifact this reads - the eight nowcast GHG FBS parquets and the eight
after-redefinition nowcast MUTs - is resolved **local-first** from
``transform/output_data``, at one vintage pinned across the whole span. A
span-wide pin is not tidiness: a cross-year comparison assembled from mixed
build vintages measures the rebuild, not the years. :func:`resolve_span_vintage`
picks the newest local vintage that covers every requested year, and
``--fbs-vintage`` / ``--mut-vintage`` override it::

    python -m bedrock.analysis.time_series_B_matrix.derive_B_time_series
    python -m bedrock.analysis.time_series_B_matrix.derive_B_time_series --list-vintages
"""

from __future__ import annotations

import argparse
import logging
import re
from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from bedrock.transform.allocation.derived import map_fbs_sectors_to_model_schema
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_Vnorm_scrap_corrected,
    derive_cornerstone_x,
)
from bedrock.utils.config.config_controllers import temp_usa_config
from bedrock.utils.config.settings import FBS_DIR
from bedrock.utils.emissions.gwp import GWP100_AR6_CEDA
from bedrock.utils.taxonomy.cornerstone.industries import INDUSTRY_DESC

logger = logging.getLogger(__name__)

YEARS: tuple[int, ...] = tuple(range(2017, 2025))

#: One config per calendar year; each pins usa_base_io_data_year ==
#: usa_ghg_data_year, so E and the detail MUT share a year by construction.
CONFIG_TEMPLATE = '2025_usa_cornerstone_v0_4_nowcast_{year}'

#: Artifact stems. Both live flat in ``transform/output_data`` as
#: ``<stem>_<vintage>.parquet``.
FBS_STEM = 'GHG_national_Cornerstone_nowcast_{year}'
MUT_STEM = 'Nowcast_Detail_Make_after_redef_{year}'

#: Modules holding ``@functools.cache`` results that are config-dependent and
#: must not leak from one year's config into the next.
CACHE_BEARING_MODULES: tuple[str, ...] = (
    'bedrock.transform.eeio.derived_cornerstone',
    'bedrock.transform.allocation.derived',
    'bedrock.extract.iot.nowcast_mut_storage',
    'bedrock.extract.iot.io_2017',
    'bedrock.transform.iot.derived_gross_industry_output',
)

#: The two FBS columns this analysis is keyed on.
STRATUM: tuple[str, str] = ('MetaSources', 'AttributionSources')

OUTPUT_DIR = Path(__file__).parent / 'output'
CACHE_DIR = OUTPUT_DIR / 'cache'

_YEAR_SUFFIX = re.compile(r'_(19|20)\d{2}$')


# --- what a stratum's sector split is derived from --------------------------

#: ``AttributionSources`` value (year suffix stripped) -> class. The class says
#: what the sector split was *derived from*, never whether it tracked ``x``;
#: :func:`output_elasticity` measures that.
ATTRIBUTION_CLASS: dict[str, str] = {
    'Nowcast_Detail_Use_AfterRedef': 'io_use_table',
    'BEA_Detail_GrossOutput_IO': 'io_gross_output',
    'Energy_manufacturing_national_nowcast': 'energy_survey',
    'Direct': 'direct',
}

#: Classes whose sector split comes out of the IO tables themselves, so their
#: year-to-year movement is not independent evidence about emissions.
IO_DERIVED_CLASSES = frozenset({'io_use_table', 'io_gross_output'})

#: ⚠️ **EPA renumbered these attribution tables mid-span, and without the merge
#: the renumbering reads as an emissions collapse.** The soils vector is
#: ``T_5_17`` in 2017-18 and ``T_5_18`` from 2019; the indirect-soils vector
#: moved ``T_5_18`` to ``T_5_19`` at the same time, so ``T_5_18`` means
#: *direct* in some years and *indirect* in others. Non-energy use moved
#: ``T_3_25b`` to ``T_3_25`` for 2023. Left unmerged, 2019 shows −308 Mt on
#: ``T_5_17`` against +290 Mt on ``T_5_18`` while the emissions themselves run
#: flat at ~290 Mt, and 2023 does the same at ~90 Mt for non-energy use.
#:
#: Merging on the table number alone would be wrong, because the number does
#: not identify the role. Merging onto the **role** is safe: the pair's
#: ``MetaSources`` half already carries it (``UMD_GHGIA_T_5_10.direct`` against
#: ``.indirect``), and in any one year each MetaSource pairs with exactly one
#: EPA table. The raw value is kept in ``AttributionSources`` throughout.
ATTRIBUTION_ROLE_ALIAS: dict[str, str] = {
    'EPA_GHGI_T_5_17': 'EPA_GHGI_soils',
    'EPA_GHGI_T_5_18': 'EPA_GHGI_soils',
    'EPA_GHGI_T_5_19': 'EPA_GHGI_soils',
    'EPA_GHGI_T_3_25': 'EPA_GHGI_NEU',
    'EPA_GHGI_T_3_25b': 'EPA_GHGI_NEU',
}


def canonical_attribution(value: str) -> str:
    """Strip a trailing calendar year so a stratum is comparable across years.

    ``Energy_manufacturing_national_nowcast_2022`` and its 2023 sibling are one
    stratum; without this every year would look like a different source.
    """
    return _YEAR_SUFFIX.sub('', str(value))


def attribution_role(value: str) -> str:
    """The vintage-stable name of an attribution vector.

    :data:`ATTRIBUTION_ROLE_ALIAS` folds EPA's mid-span table renumberings onto
    one role; everything else is :func:`canonical_attribution` unchanged. This
    is the column every rollup keys on, because the raw table number turns a
    relabelling into a several-hundred-megatonne swing.
    """
    canonical = canonical_attribution(value)
    return ATTRIBUTION_ROLE_ALIAS.get(canonical, canonical)


def classify_attribution(value: str) -> str:
    """The class of an ``AttributionSources`` value; see :data:`ATTRIBUTION_CLASS`."""
    canonical = canonical_attribution(value)
    if canonical in ATTRIBUTION_CLASS:
        return ATTRIBUTION_CLASS[canonical]
    if canonical.startswith(('EPA_GHGI_', 'UMD_GHGIA_')):
        return 'inventory_table'
    return 'unclassified'


# --- vintage resolution -----------------------------------------------------


def local_vintages(stem_template: str, years: tuple[int, ...] = YEARS) -> pd.DataFrame:
    """Vintages of *stem_template* present in ``transform/output_data``.

    One row per vintage: the years it covers and its newest file's mtime.
    """
    rows: list[dict[str, object]] = []
    for year in years:
        stem = stem_template.format(year=year)
        for path in Path(FBS_DIR).glob(f'{stem}_*.parquet'):
            vintage = path.stem[len(stem) + 1 :]
            rows.append(
                {
                    'vintage': vintage,
                    'year': year,
                    'mtime': path.stat().st_mtime,
                }
            )
    if not rows:
        return pd.DataFrame(columns=['vintage', 'years', 'n_years', 'mtime'])
    found = pd.DataFrame(rows)
    return (
        found.groupby('vintage')
        .agg(
            years=('year', lambda s: tuple(sorted(s))),
            n_years=('year', 'nunique'),
            mtime=('mtime', 'max'),
        )
        .reset_index()
        .sort_values(['n_years', 'mtime'], ascending=False)
        .reset_index(drop=True)
    )


def resolve_span_vintage(stem_template: str, years: tuple[int, ...] = YEARS) -> str:
    """The newest local vintage of *stem_template* covering **every** year.

    Raises rather than falling back to a partial vintage: a span assembled from
    two builds measures the rebuild, not the years.
    """
    available = local_vintages(stem_template, years)
    covering = available[available['n_years'] == len(years)]
    if covering.empty:
        best = (
            available.head(3)[['vintage', 'n_years']].to_string(index=False)
            if not available.empty
            else '(none on disk)'
        )
        raise FileNotFoundError(
            f'No local vintage of {stem_template.format(year="<year>")!r} covers '
            f'all of {years[0]}-{years[-1]}. Best partial matches:\n{best}\n'
            f'Pass --fbs-vintage/--mut-vintage explicitly, or download the '
            f'missing years into {FBS_DIR}.'
        )
    vintage = str(covering.iloc[0]['vintage'])
    logger.info('Resolved %s -> %s', stem_template.format(year='<year>'), vintage)
    return vintage


@dataclass(frozen=True)
class SpanVintages:
    """The one FBS vintage and one MUT vintage this run is pinned to."""

    fbs: str
    mut: str


def resolve_vintages(
    fbs_vintage: str | None = None,
    mut_vintage: str | None = None,
    years: tuple[int, ...] = YEARS,
) -> SpanVintages:
    """Pin both artifact families, resolving whichever was not passed."""
    return SpanVintages(
        fbs=fbs_vintage or resolve_span_vintage(FBS_STEM, years),
        mut=mut_vintage or resolve_span_vintage(MUT_STEM, years),
    )


# --- Step 1: FBS -> CO2e, keeping the source columns ------------------------

#: Flowable names as published -> the names :data:`GWP100_AR6_CEDA` is keyed on.
#: Identical to ``load_E_from_flowsa``; kept here because this module needs the
#: pre-pivot frame, which that function does not return.
GAS_MAP: dict[str, str] = {
    'Carbon dioxide': 'CO2',
    'Methane': 'CH4_fossil',
    'Nitrous oxide': 'N2O',
    'Nitrogen trifluoride': 'NF3',
    'Sulfur hexafluoride': 'SF6',
    'HFC, PFC and SF6 F-HTFs': 'HFCs',
    'HFCs and PFCs, unspecified': 'HFCs',
    'Carbon tetrafluoride': 'CF4',
    'Hexafluoroethane': 'C2F6',
    'PFC': 'PFCs',
    'Perfluorocyclobutane': 'c-C4F8',
    'Perfluoropropane': 'C3F8',
}


def load_fbs(year: int, vintage: str) -> pd.DataFrame:
    """The nowcast GHG FBS for *year* at *vintage*, local-first.

    ⚠️ Deliberately **not** ``load_E_from_flowsa``. That function lists the GCS
    bucket even when the parquet is already on disk, and it returns a
    gas-by-sector pivot with ``MetaSources`` and ``AttributionSources`` already
    aggregated away - the two columns this analysis exists to keep.
    """
    name = f'{FBS_STEM.format(year=year)}_{vintage}.parquet'
    path = Path(FBS_DIR) / name
    if not path.exists():
        from bedrock.utils.io.gcp import download_gcs_file  # noqa: PLC0415

        logger.info('%s not on disk; downloading from GCS', name)
        path.parent.mkdir(parents=True, exist_ok=True)
        download_gcs_file(name, 'transform/output_data', str(path))
    return pd.read_parquet(path)


def fbs_to_co2e(fbs: pd.DataFrame) -> pd.DataFrame:
    """Map sectors into Cornerstone and convert every flow to CO2e.

    Mirrors ``load_E_from_flowsa`` gas for gas - including the fossil versus
    non-fossil CH4 split, which is **not** cosmetic here: the two carry
    different GWPs (29.8 against 27.0), so collapsing them would change the
    CO2e total.

    Must be called inside the year's config context: the sector mapping reads
    ``implement_electricity_disaggregation``.
    """
    mapped = map_fbs_sectors_to_model_schema(fbs)
    mapped['Flowable'] = mapped['Flowable'].map(GAS_MAP).fillna(mapped['Flowable'])

    meta = mapped['MetaSources'].astype(str)
    sector = mapped['SectorProducedBy'].astype(str)
    ch4_non_fossil = meta.str.contains('_5_', regex=False, na=False) | (
        meta.str.contains('2_1', regex=False, na=False)
        & sector.str.match(r'^(1|562|2213)', na=False)
    )
    mapped.loc[ch4_non_fossil & (mapped['Flowable'] == 'CH4_fossil'), 'Flowable'] = (
        'CH4_non_fossil'
    )

    # Widened to plain str keys: GWP100_AR6_CEDA is typed on a Literal of the
    # gas names it knows, and the two basket rows below are not among them.
    gwp: dict[str, float] = {str(k): float(v) for k, v in GWP100_AR6_CEDA.items()}
    gwp['HFCs'] = 1.0  # already CO2e as published
    gwp['PFCs'] = 1.0
    factors = mapped['Flowable'].map(gwp)
    unmapped = sorted(set(mapped.loc[factors.isna(), 'Flowable'].astype(str)))
    if unmapped:
        raise ValueError(
            f'No GWP for flowables {unmapped}. A flow with no factor would be '
            f'silently dropped from CO2e, understating E.'
        )
    mapped['CO2e'] = mapped['FlowAmount'] * factors
    return mapped


def stratified_E(year: int, vintage: str) -> pd.DataFrame:
    """``E`` for *year* as a long frame: sector x ``MetaSources`` x attribution.

    Columns: ``year``, ``sector``, ``MetaSources``, ``AttributionSources``
    (raw), ``attribution`` (year suffix stripped), ``attribution_class``,
    ``CO2e``. Must be called inside the year's config context.
    """
    co2e = fbs_to_co2e(load_fbs(year, vintage))
    grouped = (
        co2e.groupby(
            [
                co2e['SectorProducedBy'].astype(str),
                *[co2e[c].astype(str) for c in STRATUM],
            ],
            observed=True,
        )['CO2e']
        .sum()
        .reset_index()
    )
    grouped.columns = pd.Index(['sector', *STRATUM, 'CO2e'])
    grouped['year'] = year
    # `attribution` is the vintage-stable role every rollup keys on; the raw
    # table number stays in `AttributionSources` for provenance.
    grouped['attribution'] = grouped['AttributionSources'].map(attribution_role)
    grouped['attribution_class'] = grouped['AttributionSources'].map(
        classify_attribution
    )
    unclassified = sorted(
        set(grouped.loc[grouped['attribution_class'] == 'unclassified', 'attribution'])
    )
    if unclassified:
        logger.warning(
            'Unclassified AttributionSources in %d: %s - add them to '
            'ATTRIBUTION_CLASS rather than leaving them pooled.',
            year,
            unclassified,
        )
    return grouped[
        [
            'year',
            'sector',
            *STRATUM,
            'attribution',
            'attribution_class',
            'CO2e',
        ]
    ]


# --- Step 2: the span panel -------------------------------------------------


def deflate_x(x: pd.DataFrame, base_year: int) -> pd.DataFrame:
    """``x`` restated in constant *base_year* dollars, per Cornerstone industry.

    ⚠️ **The nowcast path leaves x nominal, and over this span that matters
    more than any single emissions source.** Each nowcast config sets
    ``apply_io_year_adjustments: False``, so ``x`` is the row sum of that
    year's Make in that year's dollars - no deflation anywhere. Nominal gross
    output rises ~47% over 2017-2024 while emissions are flat, so a nominal
    ``E / x`` falls even where nothing physical or structural changed. Running
    the source attribution on nominal ``x`` alone would charge every source
    with "failing to track output" when a large part of the gap is prices.

    The deflator is the same BEA industry price index the production inflate
    path uses, read at detail and reindexed to the 405 Cornerstone industries -
    all 405 are present, so nothing falls back to 1.0.
    """
    from bedrock.utils.economic.inflation_helpers_cornerstone import (  # noqa: PLC0415
        _industry_price_index_levels,
    )

    levels = _industry_price_index_levels()
    missing = [s for s in x.index if s not in levels.index]
    if missing:
        raise ValueError(
            f'{len(missing)} Cornerstone industries have no price index '
            f'({missing[:5]}...). Deflating only some of x would make the real '
            f'series a mix of two dollar years.'
        )
    real = {}
    for year in x.columns:
        ratio = (levels[int(year)] / levels[int(base_year)]).reindex(x.index)
        real[year] = x[year] / ratio
    return pd.DataFrame(real).rename_axis(index='sector', columns='year')


@dataclass
class Span:
    """Everything the analysis runs on, one row/column per year."""

    #: long: year, sector, MetaSources, AttributionSources, attribution,
    #: attribution_class, CO2e
    E: pd.DataFrame
    #: sector x year, **nominal** USD - row sums of each year's
    #: after-redefinition Make, in that year's dollars. This is the x the
    #: production B uses.
    x: pd.DataFrame
    #: the same, restated in constant first-year dollars by :func:`deflate_x`
    x_real: pd.DataFrame
    #: year -> Vnorm (405 x 405) for that year's nowcast Make
    Vnorm: dict[int, pd.DataFrame]
    vintages: SpanVintages

    def __post_init__(self) -> None:
        """Recompute the derived attribution columns from the raw FBS value.

        A cached ``E`` was written before :data:`ATTRIBUTION_ROLE_ALIAS` gained
        an entry would otherwise carry a stale role and re-introduce a
        renumbering as a real movement. ``AttributionSources`` is the raw value
        and never changes, so deriving from it on every load is cheap and
        leaves no stale-cache failure mode.
        """
        self.E = self.E.assign(
            attribution=self.E['AttributionSources'].map(attribution_role),
            attribution_class=self.E['AttributionSources'].map(classify_attribution),
        )

    def output(self, real: bool) -> pd.DataFrame:
        """``x_real`` when *real*, else nominal ``x``."""
        return self.x_real if real else self.x


def build_span(
    years: tuple[int, ...] = YEARS,
    vintages: SpanVintages | None = None,
) -> Span:
    """Build ``E``, ``x`` and ``Vnorm`` for every year, one config at a time."""
    pinned = vintages or resolve_vintages(years=years)
    E_parts: list[pd.DataFrame] = []
    x_parts: dict[int, pd.Series] = {}
    Vnorm: dict[int, pd.DataFrame] = {}

    for year in years:
        logger.info('--- %d ---', year)
        with temp_usa_config(
            CONFIG_TEMPLATE.format(year=year),
            cache_bearing_modules=CACHE_BEARING_MODULES,
            nowcast_mut_vintage=pinned.mut,
        ):
            E_year = stratified_E(year, pinned.fbs)
            x_year = derive_cornerstone_x()
            Vnorm[year] = derive_cornerstone_Vnorm_scrap_corrected()
        E_parts.append(E_year)
        x_parts[year] = x_year
        logger.info(
            '  E %.1f Mt CO2e over %d strata | x $%.2fT over %d sectors',
            E_year['CO2e'].sum() / 1e9,
            len(E_year),
            float(x_year.sum()) / 1e12,
            len(x_year),
        )

    x = pd.DataFrame(x_parts).rename_axis(index='sector', columns='year')
    # The price panel is config-routed, so read it under one year's config
    # rather than whichever happened to be installed last.
    with temp_usa_config(
        CONFIG_TEMPLATE.format(year=years[0]),
        cache_bearing_modules=CACHE_BEARING_MODULES,
        nowcast_mut_vintage=pinned.mut,
    ):
        x_real = deflate_x(x, base_year=years[0])
    logger.info(
        'x nominal $%.2fT -> $%.2fT; real (%d $) $%.2fT -> $%.2fT',
        x[years[0]].sum() / 1e12,
        x[years[-1]].sum() / 1e12,
        years[0],
        x_real[years[0]].sum() / 1e12,
        x_real[years[-1]].sum() / 1e12,
    )

    return Span(
        E=pd.concat(E_parts, ignore_index=True),
        x=x,
        x_real=x_real,
        Vnorm=Vnorm,
        vintages=pinned,
    )


# --- Step 3: how much of E rides on the IO tables ---------------------------


def attribution_shares(span: Span) -> pd.DataFrame:
    """Year x attribution: CO2e, its share of the year's E, and the class."""
    by_year = span.E.groupby(['year', 'attribution_class', 'attribution'])['CO2e'].sum()
    totals = span.E.groupby('year')['CO2e'].sum()
    out = by_year.reset_index()
    out['share'] = out['CO2e'] / out['year'].map(totals)
    return out.sort_values(['year', 'CO2e'], ascending=[True, False]).reset_index(
        drop=True
    )


def io_derived_share(span: Span) -> pd.DataFrame:
    """Per year, the fraction of ``E`` whose sector split comes from the IO tables.

    The single headline number for "how much of E is not independent evidence
    about emissions": these strata were spread across sectors using the same
    tables that produce ``x``.
    """
    E = span.E.copy()
    E['io_derived'] = E['attribution_class'].isin(IO_DERIVED_CLASSES)
    pivot = (
        E.groupby(['year', 'io_derived'])['CO2e']
        .sum()
        .unstack('io_derived')
        .rename(columns={True: 'io_derived', False: 'independent'})
    )
    for column in ('io_derived', 'independent'):
        if column not in pivot:
            pivot[column] = 0.0
    pivot = pivot.fillna(0.0)
    pivot['total'] = pivot['io_derived'] + pivot['independent']
    pivot['io_derived_share'] = pivot['io_derived'] / pivot['total']
    return pivot[['io_derived', 'independent', 'total', 'io_derived_share']]


def emissions_without_output(span: Span) -> pd.DataFrame:
    """Per year, the ``E`` sitting on rows that have no ``x``, and its share.

    ⚠️ **This is a fifth of published emissions and it is outside the whole
    decomposition.** ``F01000`` personal consumption expenditures - household
    fuel burning and personal vehicles - is a final-demand column, not an
    industry, so it has no gross output, never enters ``E / x``, and is dropped
    from ``B`` by the production path as well. Every "economy-wide" figure
    below is therefore industry emissions only, and the gap between this
    function's ``total`` and the identity table's ``E_from`` is exactly this
    mass. It is reported rather than silently netted out, because a reader
    comparing these totals to a published inventory needs to know why they are
    short.
    """
    per_sector = span.E.groupby(['year', 'sector'])['CO2e'].sum().reset_index()
    per_sector['has_output'] = per_sector['sector'].isin(span.x.index)
    pivot = (
        per_sector.groupby(['year', 'has_output'])['CO2e']
        .sum()
        .unstack('has_output')
        .rename(columns={True: 'on_industries', False: 'no_output_row'})
    )
    for column in ('on_industries', 'no_output_row'):
        if column not in pivot:
            pivot[column] = 0.0
    pivot = pivot.fillna(0.0)
    pivot['total'] = pivot['on_industries'] + pivot['no_output_row']
    pivot['excluded_share'] = pivot['no_output_row'] / pivot['total']
    excluded = sorted(set(per_sector.loc[~per_sector['has_output'], 'sector']))
    logger.debug('Rows with no x, excluded from the decomposition: %s', excluded)
    return pivot[['on_industries', 'no_output_row', 'total', 'excluded_share']]


# --- Step 4: the E-versus-x decomposition -----------------------------------


def divergence(span: Span, real: bool = False) -> pd.DataFrame:
    """Per (year-pair, sector, stratum), the emissions that did not track output.

    ``D = E[t] - E[t-1] * g``, with ``g = x[s, t] / x[s, t-1]``. Sectors whose
    prior-year output is zero are dropped - growth is undefined there - and
    :func:`verify_identity` reports the CO2e that removes.
    """
    E = span.E
    keys = ['sector', *STRATUM, 'attribution', 'attribution_class']
    wide = (
        E.pivot_table(index=keys, columns='year', values='CO2e', aggfunc='sum')
        .fillna(0.0)
        .sort_index(axis='columns')
    )
    years = [int(y) for y in wide.columns]

    records: list[pd.DataFrame] = []
    for prior, current in zip(years, years[1:]):
        x = span.output(real)
        g = (x[current] / x[prior]).replace([np.inf, -np.inf], np.nan)
        sectors = wide.index.get_level_values('sector')
        growth = pd.Series(g.reindex(sectors).to_numpy(), index=wide.index)
        counterfactual = wide[prior] * growth
        block = pd.DataFrame(
            {
                'year_from': prior,
                'year_to': current,
                'E_from': wide[prior],
                'E_to': wide[current],
                'x_growth': growth,
                'E_counterfactual': counterfactual,
                'divergence': wide[current] - counterfactual,
            }
        ).reset_index()
        records.append(block.dropna(subset=['x_growth']))

    out = pd.concat(records, ignore_index=True)
    out['delta_E'] = out['E_to'] - out['E_from']
    return out


def divergence_by(
    detail: pd.DataFrame,
    by: str | list[str] = 'attribution',
    top: int | None = None,
) -> pd.DataFrame:
    """Roll :func:`divergence` up to *by*, ranked by absolute divergence.

    ⚠️ *top* truncates to the *n* largest **absolute** movers per year, which
    is the wrong ranking for a percentage question - a small source can move a
    long way in percentage terms and never appear. Pass it only for a log
    preview, and leave it off for anything saved.
    """
    keys = [by] if isinstance(by, str) else list(by)
    rolled = (
        detail.groupby(['year_from', 'year_to', *keys])[
            ['E_from', 'E_to', 'E_counterfactual', 'divergence', 'delta_E']
        ]
        .sum()
        .reset_index()
    )
    rolled['divergence_pct_of_E_from'] = np.where(
        rolled['E_from'] > 0, rolled['divergence'] / rolled['E_from'] * 100, np.nan
    )
    # Sort key for "which sectors moved furthest relative to their own size",
    # as opposed to the signed column, which sorts overshoot above undershoot.
    # ⚠️ Read it next to E_from: a sector carrying a few kilotonnes can post a
    # vast percentage off a rounding-scale numerator.
    rolled['divergence_pct_of_E_abs'] = rolled['divergence_pct_of_E_from'].abs()
    rolled = rolled.sort_values(
        ['year_to', 'divergence'],
        key=lambda s: s.abs() if s.name == 'divergence' else s,
    )
    if top is not None:
        rolled = (
            rolled.assign(_rank=rolled['divergence'].abs())
            .sort_values(['year_to', '_rank'], ascending=[True, False])
            .groupby('year_to')
            .head(top)
            .drop(columns='_rank')
        )
    return rolled.reset_index(drop=True)


def composition_effect(span: Span, real: bool = False) -> pd.DataFrame:
    """The part of the E-versus-x gap that is the economy's changing sector mix.

    ``sum_s E[s, t-1] * g[s] - E[t-1] * G``: what emissions would have done had
    every sector held its own intensity and only the output mix moved.
    """
    x = span.output(real)
    by_sector = span.E.groupby(['year', 'sector'])['CO2e'].sum().unstack('year')
    by_sector = by_sector.reindex(x.index).fillna(0.0)
    years = [int(y) for y in x.columns]

    rows = []
    for prior, current in zip(years, years[1:]):
        g = (x[current] / x[prior]).replace([np.inf, -np.inf], np.nan)
        usable = g.notna()
        G = float(x.loc[usable, current].sum() / x.loc[usable, prior].sum())
        E_prior = by_sector.loc[usable, prior]
        rows.append(
            {
                'year_from': prior,
                'year_to': current,
                'E_from': float(E_prior.sum()),
                'E_to': float(by_sector.loc[usable, current].sum()),
                'x_growth_economy': G,
                'sector_counterfactual': float((E_prior * g[usable]).sum()),
                'economy_counterfactual': float(E_prior.sum() * G),
            }
        )
    out = pd.DataFrame(rows)
    out['composition'] = out['sector_counterfactual'] - out['economy_counterfactual']
    out['total_gap'] = out['E_to'] - out['economy_counterfactual']
    return out


def verify_identity(
    span: Span, detail: pd.DataFrame, real: bool = False
) -> pd.DataFrame:
    """Assert ``total_gap == sum(divergence) + composition`` for every year-pair.

    The decomposition is exact by construction, so a residual means a sector or
    stratum was dropped between the two paths - the failure mode worth catching
    before anyone reads the ranking.
    """
    summed = detail.groupby(['year_from', 'year_to'])['divergence'].sum()
    checked = composition_effect(span, real=real).merge(
        summed.rename('divergence_sum').reset_index(),
        on=['year_from', 'year_to'],
        how='left',
    )
    checked['residual'] = (
        checked['total_gap'] - checked['divergence_sum'] - checked['composition']
    )
    checked['residual_pct_of_E_from'] = checked['residual'] / checked['E_from'] * 100
    worst = checked['residual_pct_of_E_from'].abs().max()
    if worst > 0.01:
        raise ValueError(
            f'The E-versus-x decomposition does not close: worst residual is '
            f'{worst:.4f}% of prior-year emissions. Every term is linear in E, '
            f'so a residual means a sector or stratum is in one path and not '
            f'the other.\n{checked.to_string(index=False)}'
        )
    logger.info('Identity closes; worst residual %.2e%% of E', worst)
    return checked


# --- Step 5: did a stratum actually track output? ---------------------------


def output_elasticity(detail: pd.DataFrame, by: str = 'attribution') -> pd.DataFrame:
    """Regress ``dlog E`` on ``dlog x`` per stratum, pooled over sectors and years.

    A slope near 1 means the stratum moved with output; near 0 means it moved
    independently of it. Weighted by prior-year emissions, so a stratum's
    verdict is set by the sectors where it actually has mass.

    ⚠️ This is the measurement the :data:`ATTRIBUTION_CLASS` labels do *not*
    make. A stratum attributed on a row of the Use table can still come out
    well below 1, because it rides a sector's fuel purchases rather than its
    output - which is precisely the effect that makes ``B`` rocky.
    """
    usable = detail[
        (detail['E_from'] > 0) & (detail['E_to'] > 0) & (detail['x_growth'] > 0)
    ].copy()
    usable['dlog_E'] = np.log(usable['E_to'] / usable['E_from'])
    usable['dlog_x'] = np.log(usable['x_growth'])

    rows = []
    for stratum, group in usable.groupby(by, observed=True):
        w = group['E_from'].to_numpy(dtype=float)
        dx = group['dlog_x'].to_numpy(dtype=float)
        dy = group['dlog_E'].to_numpy(dtype=float)
        if len(group) < 3 or w.sum() <= 0 or np.allclose(dx, dx[0]):
            slope = r2 = np.nan
        else:
            x_bar = float(np.average(dx, weights=w))
            y_bar = float(np.average(dy, weights=w))
            cov = float(np.average((dx - x_bar) * (dy - y_bar), weights=w))
            var = float(np.average((dx - x_bar) ** 2, weights=w))
            slope = cov / var if var > 0 else np.nan
            resid = dy - (y_bar + slope * (dx - x_bar))
            ss_res = float(np.average(resid**2, weights=w))
            ss_tot = float(np.average((dy - y_bar) ** 2, weights=w))
            r2 = 1 - ss_res / ss_tot if ss_tot > 0 else np.nan
        rows.append(
            {
                by: stratum,
                'n_obs': len(group),
                'E_from_total': float(w.sum()),
                'slope_dlogE_on_dlogx': slope,
                'r2': r2,
                'mean_abs_dlog_E': float(np.average(np.abs(dy), weights=w)),
            }
        )
    out = pd.DataFrame(rows).sort_values('E_from_total', ascending=False)
    if 'attribution_class' not in out.columns and by == 'attribution':
        classes = detail.drop_duplicates('attribution').set_index('attribution')[
            'attribution_class'
        ]
        out['attribution_class'] = out[by].map(classes)
    return out.reset_index(drop=True)


# --- Step 6: B, as a by-product ---------------------------------------------


def B_total(span: Span, real: bool = False) -> pd.DataFrame:
    """Total-CO2e commodity intensity, ``(E / x) @ Vnorm``, commodity x year.

    One row per commodity rather than per gas - the gas split is what #906
    asked to drop. *real* divides by the deflated output instead, giving
    CO2e per constant first-year dollar.
    """
    x_all = span.output(real)
    columns: dict[int, pd.Series] = {}
    for year in span.Vnorm:
        E_sector = span.E[span.E['year'] == year].groupby('sector')['CO2e'].sum()
        Vnorm = span.Vnorm[year]
        x = x_all[year].reindex(Vnorm.index).fillna(0.0)
        E_aligned = E_sector.reindex(Vnorm.index).fillna(0.0)
        intensity = (E_aligned / x).replace([np.inf, -np.inf], 0.0).fillna(0.0)
        columns[year] = intensity @ Vnorm
    return pd.DataFrame(columns).rename_axis(index='commodity', columns='year')


def B_change(span: Span, real: bool = True) -> pd.DataFrame:
    """Year-on-year change in the emission factor itself, one row per year-pair.

    ``B`` is the published EF - CO2e per dollar of that commodity, indirect
    effects included via ``Vnorm`` - so this is the table to sort when the
    question is *where did the factor move*, rather than where the emissions
    behind it moved. Long rather than wide, so it sorts on year and commodity
    together, and defaults to **real** dollars: a nominal EF falls whenever
    prices rise, which over this span would put inflation at the top of the
    ranking (see :func:`price_effect`).

    ⚠️ **Sort on** ``abs_pct_change`` **but read it next to** ``B_from``. A
    commodity with a near-zero factor posts a huge percentage off a
    rounding-scale numerator; filtering on ``B_from`` first is what makes the
    ranking mean anything. ``pct_change`` is left NaN where ``B_from`` is zero,
    since there is no percentage of nothing.
    """
    B = B_total(span, real=real)
    years = [int(y) for y in B.columns]
    frames = []
    for prior, current in zip(years, years[1:]):
        block = pd.DataFrame(
            {
                'year_from': prior,
                'year_to': current,
                'commodity': B.index,
                'B_from': B[prior].to_numpy(),
                'B_to': B[current].to_numpy(),
            }
        )
        frames.append(block)
    out = pd.concat(frames, ignore_index=True)
    out['delta_B'] = out['B_to'] - out['B_from']
    out['pct_change'] = np.where(
        out['B_from'] != 0, out['delta_B'] / out['B_from'] * 100, np.nan
    )
    out['abs_pct_change'] = np.abs(out['pct_change'])
    out = _with_names(out, 'commodity')
    return out.sort_values(
        ['year_to', 'abs_pct_change'], ascending=[True, False]
    ).reset_index(drop=True)


def B_by_attribution(span: Span) -> pd.DataFrame:
    """``B`` split by attribution: (year, attribution) x commodity.

    Sums to :func:`B_total` per year, and shows which source carries a
    commodity's intensity - and therefore which source moved when it jumped.
    """
    frames = []
    for year in span.Vnorm:
        Vnorm = span.Vnorm[year]
        x = span.x[year].reindex(Vnorm.index).fillna(0.0)
        wide = (
            span.E[span.E['year'] == year]
            .pivot_table(
                index='attribution', columns='sector', values='CO2e', aggfunc='sum'
            )
            .reindex(columns=Vnorm.index)
            .fillna(0.0)
        )
        intensity = wide.divide(x, axis='columns').replace([np.inf, -np.inf], 0.0)
        block = intensity.fillna(0.0) @ Vnorm
        block['year'] = year
        frames.append(block.reset_index().set_index(['year', 'attribution']))
    return pd.concat(frames).rename_axis(columns='commodity')


# --- reporting --------------------------------------------------------------


#: Widened to plain str keys; INDUSTRY_DESC is typed on a Literal of the 405
#: codes, and these frames carry codes as ordinary strings.
_INDUSTRY_NAME: dict[str, str] = {str(k): str(v) for k, v in INDUSTRY_DESC.items()}


def _with_names(frame: pd.DataFrame, column: str = 'sector') -> pd.DataFrame:
    """Attach the Cornerstone description next to a sector or commodity code."""
    out = frame.copy()
    position = out.columns.get_loc(column)
    if not isinstance(position, int):
        raise ValueError(
            f'{column!r} is not a single column of this frame, so there is no '
            f'one place to insert its name beside.'
        )
    out.insert(
        position + 1,
        'name',
        out[column].map(lambda code: _INDUSTRY_NAME.get(str(code), '')),
    )
    return out


def price_effect(span: Span) -> pd.DataFrame:
    """How much of the E-versus-x gap is prices rather than anything emitted.

    Runs :func:`composition_effect` twice, once on nominal ``x`` and once on
    the deflated series, and differences the two counterfactuals. ``price``
    is the emissions that a nominal denominator alone appears to remove:
    positive means nominal ``x`` grew faster than real, so intensity fell for
    price reasons. Read it before reading any source's divergence - in a year
    like 2021-22 it is the largest single term in the whole decomposition.
    """
    nominal = composition_effect(span, real=False)
    real = composition_effect(span, real=True)
    merged = nominal.merge(
        real,
        on=['year_from', 'year_to'],
        suffixes=('_nominal', '_real'),
    )
    merged['price'] = (
        merged['economy_counterfactual_real'] - merged['economy_counterfactual_nominal']
    )
    merged['price_share_of_gap'] = merged['price'] / merged['total_gap_nominal']
    return merged[
        [
            'year_from',
            'year_to',
            'x_growth_economy_nominal',
            'x_growth_economy_real',
            'total_gap_nominal',
            'total_gap_real',
            'price',
            'price_share_of_gap',
        ]
    ]


def report(
    span: Span, detail: pd.DataFrame, detail_real: pd.DataFrame
) -> dict[str, pd.DataFrame]:
    """Every table this analysis produces, logged and returned for saving.

    *detail* is the decomposition against nominal ``x`` - the denominator
    the production B actually uses - and *detail_real* the same against
    constant first-year dollars. Both are reported because they answer
    different questions, and the gap between them is :func:`price_effect`.
    """
    tables: dict[str, pd.DataFrame] = {}

    tables['identity_check'] = verify_identity(span, detail)
    logger.info(
        'E versus x, economy-wide ($ and CO2e both nowcast-year):\n%s',
        tables['identity_check'][
            [
                'year_to',
                'E_from',
                'E_to',
                'x_growth_economy',
                'total_gap',
                'composition',
                'divergence_sum',
            ]
        ].to_string(index=False),
    )

    tables['emissions_without_output'] = emissions_without_output(span)

    tables['io_derived_share'] = io_derived_share(span)
    logger.info(
        'Share of E whose sector split comes from the IO tables:\n%s',
        tables['io_derived_share'].to_string(),
    )

    tables['price_effect'] = price_effect(span)
    logger.info(
        'How much of the gap is prices rather than emissions:\n%s',
        tables['price_effect'].to_string(index=False),
    )

    tables['attribution_shares'] = attribution_shares(span)

    tables['divergence_by_attribution'] = divergence_by(detail, 'attribution')
    logger.info(
        'Divergence by attribution source (CO2e that did not track output):\n%s',
        tables['divergence_by_attribution']
        .pivot(index='attribution', columns='year_to', values='divergence')
        .to_string(),
    )

    # The raw table numbers, kept so EPA's mid-span renumbering stays visible
    # somewhere rather than only in ATTRIBUTION_ROLE_ALIAS. Read it as evidence
    # about vintages, never as evidence about emissions.
    tables['divergence_by_attribution_raw'] = divergence_by(
        detail, 'AttributionSources'
    )

    tables['divergence_by_class'] = divergence_by(detail, 'attribution_class')
    # Saved tables are never truncated - a top-n by absolute movement is the
    # wrong ranking for the percentage question, and a reader sorting the CSV
    # on divergence_pct_of_E_abs would be sorting a pre-filtered file without
    # knowing it. Truncation belongs in the log preview only.
    tables['divergence_by_metasource'] = divergence_by(detail, 'MetaSources')
    logger.info(
        'Top MetaSources by |divergence|, per year (preview; the CSV is complete):\n%s',
        divergence_by(detail, 'MetaSources', top=15)[
            ['year_to', 'MetaSources', 'E_from', 'E_to', 'divergence']
        ].to_string(index=False),
    )

    tables['divergence_by_sector'] = _with_names(divergence_by(detail, 'sector'))
    tables['divergence_by_sector_real'] = _with_names(
        divergence_by(detail_real, 'sector')
    )
    tables['divergence_by_sector_stratum'] = divergence_by(
        detail, ['sector', 'attribution', 'MetaSources']
    )

    tables['identity_check_real'] = verify_identity(span, detail_real, real=True)
    tables['divergence_by_attribution_real'] = divergence_by(detail_real, 'attribution')
    logger.info(
        'The same, against x in constant %d dollars:\n%s',
        int(span.x.columns[0]),
        tables['divergence_by_attribution_real']
        .pivot(index='attribution', columns='year_to', values='divergence')
        .to_string(),
    )
    tables['divergence_by_metasource_real'] = divergence_by(detail_real, 'MetaSources')

    tables['output_elasticity'] = output_elasticity(detail, 'attribution')
    logger.info(
        'Did each attribution source track output? (slope 1 = tracks, 0 = does not)'
        '\n%s',
        tables['output_elasticity'].to_string(index=False),
    )
    tables['output_elasticity_by_metasource'] = output_elasticity(detail, 'MetaSources')
    tables['output_elasticity_real'] = output_elasticity(detail_real, 'attribution')
    logger.info(
        'Did each source track REAL output? (the same question, prices out)\n%s',
        tables['output_elasticity_real'].to_string(index=False),
    )

    tables['B_total'] = B_total(span)
    tables['B_total_real'] = B_total(span, real=True)
    tables['B_by_attribution'] = B_by_attribution(span)
    # The EF movement table: sort on abs_pct_change, filter on B_from.
    tables['B_change_real'] = B_change(span, real=True)
    logger.info(
        'Largest EF movements in constant %d dollars, per year (preview of a '
        'complete CSV; filtered to commodities over 0.1 kg CO2e/$ so the '
        'ranking is not led by rounding-scale factors):\n%s',
        int(span.x.columns[0]),
        tables['B_change_real'][tables['B_change_real']['B_from'] > 1e-4]
        .groupby('year_to')
        .head(3)[['year_to', 'commodity', 'name', 'B_from', 'B_to', 'abs_pct_change']]
        .to_string(index=False),
    )
    tables['x'] = span.x
    tables['x_real'] = span.x_real

    return tables


# --- plots ------------------------------------------------------------------


def plot_E_and_x_indexed(span: Span) -> None:
    """E by attribution class against x, both indexed to the first year."""
    by_class = (
        span.E.groupby(['year', 'attribution_class'])['CO2e'].sum().unstack('year')
    )
    x_total = span.x.sum()
    x_real_total = span.x_real.sum()
    base = by_class.columns[0]

    fig, ax = plt.subplots(figsize=(10, 6))
    for label, series in by_class.iterrows():
        if series[base] <= 0:
            continue
        ax.plot(series.index, series / series[base] * 100, marker='o', label=str(label))
    ax.plot(
        x_total.index,
        x_total / x_total.iloc[0] * 100,
        marker='s',
        color='black',
        linewidth=2.5,
        label='x (gross output, nominal)',
    )
    ax.plot(
        x_real_total.index,
        x_real_total / x_real_total.iloc[0] * 100,
        marker='s',
        color='dimgrey',
        linewidth=2.5,
        linestyle='--',
        label=f'x (gross output, constant {base} $)',
    )
    ax.axhline(100, color='grey', linewidth=0.8, linestyle=':')
    ax.set_title(f'E by attribution class against x, indexed to {base} = 100')
    ax.set_xlabel('year')
    ax.set_ylabel(f'index ({base} = 100)')
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / 'E_vs_x_indexed.png', dpi=150)
    plt.close(fig)


#: Short names for the attribution vectors, for chart labels.
_ATTRIBUTION_ABBREV: dict[str, str] = {
    'Nowcast_Detail_Use_AfterRedef': 'Use',
    'BEA_Detail_GrossOutput_IO': 'GO',
    'Energy_manufacturing_national_nowcast': 'MECS',
    'Direct': 'Direct',
}

_TABLE_STEM = re.compile(r'^(UMD_GHGIA|EPA_GHGI)_T_([A-Za-z0-9_]+?)(?:\.(.*))?$')


def abbreviate_source(value: str) -> str:
    """``UMD_GHGIA_T_3_11.ng_manufacturing`` -> ``UMD 3-11 ng_manufacturing``.

    Anything that does not look like a GHGI table name is returned unchanged,
    so a new source shows up in full rather than being silently mangled.
    """
    match = _TABLE_STEM.match(str(value))
    if not match:
        return _ATTRIBUTION_ABBREV.get(str(value), str(value))
    family, table, suffix = match.groups()
    short = f'{"UMD" if family.startswith("UMD") else "EPA"} {table.replace("_", "-")}'
    if suffix:
        short = f'{short} {suffix[:22]}'
    return short


def source_pair_label(meta: str, attribution: str) -> str:
    """``<inventory table> -> <what spread it across sectors>``, abbreviated."""
    return f'{abbreviate_source(meta)} → {abbreviate_source(attribution)}'


def divergence_by_source_pair(
    detail: pd.DataFrame, threshold: float = 0.03
) -> pd.DataFrame:
    """Divergence per year by ``MetaSources`` x ``AttributionSources`` pair.

    A pair is kept in its own right when, in at least one year, it carries
    *threshold* or more of that year's **gross** divergence - the sum of the
    absolute value of every pair's term, which is the right denominator here
    because the signed total nets opposing movements to something much smaller
    than the movements themselves. Everything below that in every year is
    bundled into ``other``, which is a real sum and not a remainder: the
    bundled and kept terms still add to the year's signed divergence.

    ⚠️ **Two legend entries can be the same stratum in different GHGI
    vintages.** EPA renumbered the soils tables - the direct-soils vector is
    ``EPA_GHGI_T_5_17`` in 2017 and ``EPA_GHGI_T_5_18`` from 2019, and the
    indirect one moved ``T_5_18`` to ``T_5_19`` - so ``T_5_18`` means *direct*
    in some years and *indirect* in others. They are deliberately not merged
    on the table number, because the number alone does not identify the role;
    the ``MetaSources`` half of the pair (``UMD_GHGIA_T_5_10.direct`` against
    ``.indirect``) is what does. Non-energy use moved ``T_3_25b`` to ``T_3_25``
    for 2024 the same way.
    """
    pairs = (
        detail.groupby(['year_to', 'MetaSources', 'attribution'])['divergence']
        .sum()
        .reset_index()
    )
    pairs['label'] = [
        source_pair_label(m, a)
        for m, a in zip(pairs['MetaSources'], pairs['attribution'])
    ]
    gross = pairs.groupby('year_to')['divergence'].apply(lambda s: s.abs().sum())
    pairs['share_of_gross'] = pairs['divergence'].abs() / pairs['year_to'].map(gross)
    peak = pairs.groupby('label')['share_of_gross'].max()
    kept = set(peak.index[peak >= threshold])
    pairs['bundled'] = ~pairs['label'].isin(kept)
    pairs.loc[pairs['bundled'], 'label'] = 'other'
    logger.info(
        'Source pairs: %d kept at the %.0f%% threshold, %d bundled into "other" '
        '(%.1f%% of gross divergence)',
        len(kept),
        threshold * 100,
        peak.size - len(kept),
        pairs.loc[pairs['bundled'], 'divergence'].abs().sum()
        / pairs['divergence'].abs().sum()
        * 100,
    )
    return pairs


def plot_divergence_stack(
    detail: pd.DataFrame,
    filename: str = 'divergence_by_source.png',
    title_suffix: str = '',
    threshold: float = 0.03,
) -> pd.DataFrame:
    """Each year's E-versus-x gap, stacked by inventory table and attribution.

    Returns the plotted frame so the bundling is inspectable rather than only
    visible in the picture.
    """
    pairs = divergence_by_source_pair(detail, threshold)
    plotted = pairs.groupby(['year_to', 'label'])['divergence'].sum().reset_index()
    pivot = (
        plotted.pivot(index='year_to', columns='label', values='divergence').fillna(0.0)
        / 1e9
    )
    # Biggest movers first, 'other' always last, so the legend reads in order.
    order = (
        pivot.drop(columns='other', errors='ignore')
        .abs()
        .sum()
        .sort_values(ascending=False)
        .index.tolist()
    )
    if 'other' in pivot.columns:
        order.append('other')
    pivot = pivot[order]

    fig, ax = plt.subplots(figsize=(14, 8))
    # tab20 alone repeats once past 20 series, which puts two different sources
    # in the same blue; chain the three 20-colour qualitative maps instead.
    palette = [
        c
        for name in ('tab20', 'tab20b', 'tab20c')
        for c in plt.get_cmap(name).colors  # type: ignore[attr-defined]
    ]
    colours = [palette[i % len(palette)] for i in range(len(order))]
    bottom_pos = np.zeros(len(pivot))
    bottom_neg = np.zeros(len(pivot))
    # Positive and negative halves stack from zero in opposite directions, so a
    # source that flips sign between years keeps one colour and one legend entry.
    for colour, column in zip(colours, pivot.columns):
        values = pivot[column].to_numpy(dtype=float)
        if column == 'other':
            colour = (0.6, 0.6, 0.6)
        positive = np.clip(values, 0, None)
        negative = np.clip(values, None, 0)
        ax.bar(
            pivot.index, positive, bottom=bottom_pos, color=colour, label=str(column)
        )
        ax.bar(pivot.index, negative, bottom=bottom_neg, color=colour)
        bottom_pos += positive
        bottom_neg += negative
    ax.axhline(0, color='black', linewidth=0.8)
    # Autoscale sees each half-stack, not the stacked extent, so set the limits
    # from the stacks themselves or the tallest bar gets clipped at the frame.
    headroom = 0.08 * max(bottom_pos.max() - bottom_neg.min(), 1.0)
    ax.set_ylim(bottom_neg.min() - headroom, bottom_pos.max() + headroom)
    ax.set_title(
        'Emissions that did not track output, by inventory table and what '
        f'spread it across sectors{title_suffix}\n'
        f'pairs under {threshold:.0%} of gross divergence in every year are '
        'bundled into "other"',
        fontsize=11,
    )
    ax.set_xlabel('year')
    ax.set_ylabel('Mt CO2e above (+) or below (-) output-tracking')
    ax.legend(fontsize=7, ncol=2, loc='center left', bbox_to_anchor=(1.0, 0.5))
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / filename, dpi=150, bbox_inches='tight')
    plt.close(fig)
    return pairs


def plot_elasticity(elasticity: pd.DataFrame) -> None:
    """Each attribution source's slope of dlog E on dlog x, sized by its mass."""
    frame = elasticity.dropna(subset=['slope_dlogE_on_dlogx'])
    if frame.empty:
        logger.warning('No stratum had enough observations to fit a slope')
        return
    classes = frame.get('attribution_class', pd.Series('', index=frame.index))
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(
        frame['attribution'].astype(str),
        frame['slope_dlogE_on_dlogx'],
        color=[
            'tab:blue' if c in IO_DERIVED_CLASSES else 'tab:orange' for c in classes
        ],
    )
    ax.axvline(
        1.0, color='black', linestyle='--', linewidth=1, label='tracks x exactly'
    )
    ax.axvline(0.0, color='grey', linestyle=':', linewidth=1, label='independent of x')
    ax.set_title('Did each attribution source move with output?')
    ax.set_xlabel('slope of dlog(E) on dlog(x), emissions-weighted')
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / 'output_elasticity.png', dpi=150)
    plt.close(fig)


# --- cache ------------------------------------------------------------------


def save_span(span: Span) -> None:
    """Persist the panel so the analysis can be re-run without rebuilding it."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    span.E.to_parquet(CACHE_DIR / 'E_stratified.parquet')
    span.x.to_parquet(CACHE_DIR / 'x.parquet')
    span.x_real.to_parquet(CACHE_DIR / 'x_real.parquet')
    for year, Vnorm in span.Vnorm.items():
        Vnorm.to_parquet(CACHE_DIR / f'Vnorm_{year}.parquet')
    (CACHE_DIR / 'vintages.txt').write_text(
        f'fbs={span.vintages.fbs}\nmut={span.vintages.mut}\n'
    )
    logger.info('Cached the span to %s', CACHE_DIR)


def load_span(years: tuple[int, ...] = YEARS) -> Span:
    """Reload a cached span."""
    pinned = dict(
        line.split('=', 1)
        for line in (CACHE_DIR / 'vintages.txt').read_text().split()
        if '=' in line
    )

    def _read(name: str) -> pd.DataFrame:
        frame = pd.read_parquet(CACHE_DIR / name)
        frame.columns = pd.Index([int(c) for c in frame.columns], name='year')
        return frame.rename_axis(index='sector')

    return Span(
        E=pd.read_parquet(CACHE_DIR / 'E_stratified.parquet'),
        x=_read('x.parquet'),
        x_real=_read('x_real.parquet'),
        Vnorm={
            year: pd.read_parquet(CACHE_DIR / f'Vnorm_{year}.parquet') for year in years
        },
        vintages=SpanVintages(fbs=pinned['fbs'], mut=pinned['mut']),
    )


def save_tables(tables: dict[str, pd.DataFrame]) -> None:
    """Write every table to CSV next to the plots.

    ⚠️ **One unwritable file must not cost the other twenty.** A CSV open in
    Excel is locked on Windows, and a bare loop aborts on the first
    ``PermissionError`` - leaving every table after it in the iteration order
    silently absent, which reads exactly like the analysis never produced them.
    Each write is therefore attempted on its own, and the failures are
    collected and raised together at the end, naming the files to close.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    failed: dict[str, str] = {}
    for name, frame in tables.items():
        try:
            frame.to_csv(OUTPUT_DIR / f'{name}.csv')
        except OSError as exc:
            failed[name] = str(exc)
    logger.info(
        'Wrote %d of %d tables to %s',
        len(tables) - len(failed),
        len(tables),
        OUTPUT_DIR,
    )
    if failed:
        listing = '\n'.join(
            f'  {name}.csv: {reason}' for name, reason in failed.items()
        )
        raise OSError(
            f'{len(failed)} of {len(tables)} tables could not be written - every '
            f'other table was written and is current. Close these files (an open '
            f'spreadsheet locks them on Windows) and re-run:\n{listing}'
        )


# --- main -------------------------------------------------------------------


def main(
    years: tuple[int, ...] = YEARS,
    use_cache: bool = False,
    fbs_vintage: str | None = None,
    mut_vintage: str | None = None,
) -> dict[str, pd.DataFrame]:
    """Build the span, decompose ``E`` against ``x``, write tables and plots."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if use_cache:
        span = load_span(years)
        logger.info(
            'Loaded cached span (fbs=%s, mut=%s)', span.vintages.fbs, span.vintages.mut
        )
    else:
        span = build_span(years, resolve_vintages(fbs_vintage, mut_vintage, years))
        save_span(span)

    detail = divergence(span, real=False)
    detail_real = divergence(span, real=True)
    tables = report(span, detail, detail_real)
    tables['divergence_detail'] = detail
    tables['divergence_detail_real'] = detail_real

    # Plot before saving: the two plot calls return the frames they charted,
    # and those belong in `tables` before `save_tables` walks it. Saving first
    # wrote every table except the two that had not been added yet.
    plot_E_and_x_indexed(span)
    tables['divergence_by_source_plotted'] = plot_divergence_stack(
        detail, 'divergence_by_source.png', ' (nominal x)'
    )
    tables['divergence_by_source_plotted_real'] = plot_divergence_stack(
        detail_real,
        'divergence_by_source_real.png',
        f' (x in constant {int(span.x.columns[0])} $)',
    )
    plot_elasticity(tables['output_elasticity'])
    logger.info('Wrote plots to %s', OUTPUT_DIR)

    save_tables(tables)

    return tables


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s | %(message)s')
    parser = argparse.ArgumentParser(
        description=(
            'Attribute the E-versus-x gap to FBS MetaSources and '
            'AttributionSources, nowcast models 2017-2024 (#906)'
        )
    )
    parser.add_argument(
        '--years',
        type=int,
        nargs='+',
        default=list(YEARS),
        help='calendar years to build (default: 2017-2024)',
    )
    parser.add_argument(
        '--use-cache',
        action='store_true',
        help='reload the cached span instead of rebuilding it',
    )
    parser.add_argument('--fbs-vintage', help='pin the GHG FBS vintage')
    parser.add_argument('--mut-vintage', help='pin the nowcast MUT vintage')
    parser.add_argument(
        '--list-vintages',
        action='store_true',
        help='show the local vintages of both artifact families and exit',
    )
    args = parser.parse_args()

    span_years = tuple(args.years)
    if args.list_vintages:
        for stem in (FBS_STEM, MUT_STEM):
            print(f'\n{stem.format(year="<year>")}')
            print(local_vintages(stem, span_years).to_string(index=False))
    else:
        main(
            years=span_years,
            use_cache=args.use_cache,
            fbs_vintage=args.fbs_vintage,
            mut_vintage=args.mut_vintage,
        )
