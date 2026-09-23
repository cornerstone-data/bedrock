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
picks a local vintage that covers every requested year **and whose hash is
reachable from origin/main**, newest commit first; it never ranks on file
mtime, for the reason recorded there. ``--fbs-vintage`` / ``--mut-vintage``
override it, and ``--list-vintages`` shows what is on disk with the provenance
of each::

    python -m bedrock.analysis.time_series_B_matrix.B_change_diagnostics
    python -m bedrock.analysis.time_series_B_matrix.B_change_diagnostics --list-vintages
"""

from __future__ import annotations

import argparse
import functools
import logging
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path

import facilitymatcher
import facilitymatcher.colocation as colocation
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import stewi

from bedrock.transform.allocation.derived import map_fbs_sectors_to_model_schema
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_Vnorm_scrap_corrected,
    derive_cornerstone_x,
)
from bedrock.transform.ghg import ghgrp_subpart_w
from bedrock.utils.config.config_controllers import temp_usa_config
from bedrock.utils.config.settings import FBS_DIR, MODULEPATH
from bedrock.utils.emissions.gwp import GWP100_AR6_CEDA
from bedrock.utils.math.formulas import (
    compute_L_matrix,
    rebase_coefficient_matrix,
)
from bedrock.utils.taxonomy.cornerstone.industries import INDUSTRY_DESC

logger = logging.getLogger(__name__)

YEARS: tuple[int, ...] = tuple(range(2017, 2025))

#: Last year GHGRP subpart C is available for. This now reaches the end of the
#: nowcast span, so D14 covers every year of it.
GHGRP_LAST_YEAR = 2024

#: Years GHGRP cannot be downloaded for, because EPA never published them. 2024
#: was released under FOIA and is built from those static files into ``stewi``
#: locally (#931), so it is present or it is not - there is nothing to fetch.
GHGRP_LOCAL_BUILD_YEARS: tuple[int, ...] = (2024,)

#: Last year NEI point sources carry Carbon Dioxide. StEWI serves NEI 2023
#: (roster / NAICS / criteria pollutants), but EPA omitted CO2 from that year
#: (#932), so D15 mass and ``fuel_class`` weights cannot use it. Carry-forward
#: for 2023+ is #970.
NEI_LAST_YEAR = 2022

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

#: Chart labels for :data:`ATTRIBUTION_CLASS`. The class keys are fine as
#: identifiers and are what the CSVs carry, but on a legend they read as
#: quantities rather than as what they are - **every one of these is a slice
#: of E, told apart by the weight used to spread it across sectors**. A
#: reader should not have to guess that ``io_gross_output`` is emissions
#: attributed *using* gross output rather than gross output itself.
ATTRIBUTION_CLASS_LABEL: dict[str, str] = {
    'direct': 'E: inventory names the sector directly',
    'io_use_table': 'E: spread by a Use table row (fuel purchases)',
    'energy_survey': 'E: spread by the MECS manufacturing energy survey',
    'inventory_table': 'E: spread by another GHG inventory table '
    '(soils, non-energy use)',
    'io_gross_output': 'E: spread by gross output',
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


#: Ordered best-first, for display and for ranking equally-covering vintages.
#: Only ``on_main`` lets an unpinned run *choose* between candidates; see
#: :func:`resolve_span_vintage`.
_PROVENANCE_ORDER: tuple[str, ...] = (
    'on_main',
    'on_branch',
    'dangling',
    'no_such_commit',
)


@functools.cache
def _commit_time_on_main(git_hash: str) -> int | None:
    """Commit timestamp of *git_hash* if it is reachable from ``origin/main``.

    ``None`` means "do not trust this vintage unattended": either the hash names
    no commit in this clone, or it names one that no released history contains.
    """
    try:
        subprocess.check_output(
            ['git', 'merge-base', '--is-ancestor', git_hash, 'origin/main'],
            cwd=MODULEPATH,
            stderr=subprocess.DEVNULL,
        )
        return int(
            subprocess.check_output(
                ['git', 'show', '-s', '--format=%ct', git_hash],
                cwd=MODULEPATH,
                stderr=subprocess.DEVNULL,
            )
            .decode()
            .strip()
        )
    except Exception:
        return None


def vintage_provenance(vintage: str) -> str:
    """Where the code behind a ``v<version>_<hash>`` build vintage came from.

    ``on_main`` - reachable from ``origin/main``. ``on_branch`` - reachable from
    some other ref: ordinary for anything built on a PR branch, since a
    squash-merge lands under a *new* hash and the artifact keeps the branch's.
    ``dangling`` - the commit object is here but no ref reaches it, so the
    branch is gone and nobody can check out what built it. ``no_such_commit`` -
    not in this clone at all.

    Only the first two are reproducible, and the distinction is not academic:
    the FBS behind #912's phantom negative emission factors was ``dangling``.
    """
    git_hash = vintage.rsplit('_', 1)[-1]
    if _commit_time_on_main(git_hash) is not None:
        return 'on_main'
    try:
        subprocess.check_output(
            ['git', 'cat-file', '-e', f'{git_hash}^{{commit}}'],
            cwd=MODULEPATH,
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        return 'no_such_commit'
    refs = subprocess.run(
        ['git', 'branch', '-a', '--contains', git_hash],
        cwd=MODULEPATH,
        capture_output=True,
        check=False,
    )
    return 'on_branch' if refs.stdout.strip() else 'dangling'


def local_vintages(stem_template: str, years: tuple[int, ...] = YEARS) -> pd.DataFrame:
    """Vintages of *stem_template* present in ``transform/output_data``.

    One row per vintage: the years it covers, its newest file's mtime, and the
    :func:`vintage_provenance` of the hash it is stamped with. Ordered the way
    :func:`resolve_span_vintage` chooses - span coverage, then provenance, then
    **commit** date. ``mtime`` is reported but never ranked on; see there.
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
        return pd.DataFrame(
            columns=['vintage', 'years', 'n_years', 'mtime', 'provenance', 'committed']
        )
    found = pd.DataFrame(rows)
    grouped = (
        found.groupby('vintage')
        .agg(
            years=('year', lambda s: tuple(sorted(s))),
            n_years=('year', 'nunique'),
            mtime=('mtime', 'max'),
        )
        .reset_index()
    )
    grouped['provenance'] = grouped['vintage'].map(vintage_provenance)
    grouped['committed'] = grouped['vintage'].map(
        lambda v: _commit_time_on_main(v.rsplit('_', 1)[-1]) or 0
    )
    grouped['_rank'] = grouped['provenance'].map(_PROVENANCE_ORDER.index)
    return (
        grouped.sort_values(
            ['n_years', '_rank', 'committed'], ascending=[False, True, False]
        )
        .drop(columns='_rank')
        .reset_index(drop=True)
    )


def resolve_span_vintage(stem_template: str, years: tuple[int, ...] = YEARS) -> str:
    """The local vintage of *stem_template* covering **every** year.

    Raises rather than falling back to a partial vintage: a span assembled from
    two builds measures the rebuild, not the years.

    ⚠️ **Never breaks a tie on file mtime.** A build's mtime records when it was
    written, not which code wrote it, and the two disagree the moment anyone
    checks out an older branch to rebuild something. That is not hypothetical:
    #912 reported negative fossil-combustion emission factors on ``114000`` and
    ``327910`` that turned out to be an unmerged side-branch FBS outranking the
    published one by three quarters of an hour of mtime. So an unpinned run
    resolves only among vintages whose hash is reachable from ``origin/main``,
    newest **commit** first, and raises if more than one candidate survives with
    none of them on main. A single off-main vintage is allowed through - there
    is no ambiguity to resolve - but says so loudly in the log.
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

    on_main = covering[covering['provenance'] == 'on_main']
    if len(on_main):
        chosen = on_main.iloc[0]
    elif len(covering) == 1:
        chosen = covering.iloc[0]
    else:
        raise FileNotFoundError(
            f'{len(covering)} local vintages of '
            f'{stem_template.format(year="<year>")!r} cover '
            f'{years[0]}-{years[-1]} and none is reachable from origin/main, so '
            f'there is no defensible way to choose between them:\n'
            f'{covering[["vintage", "provenance"]].to_string(index=False)}\n'
            f'Pin one with --fbs-vintage/--mut-vintage. Fetch origin/main first '
            f'if one of these is in fact published.'
        )

    vintage, provenance = str(chosen['vintage']), str(chosen['provenance'])
    logger.info(
        'Resolved %s -> %s (%s)',
        stem_template.format(year='<year>'),
        vintage,
        provenance,
    )
    if provenance in ('dangling', 'no_such_commit'):
        logger.warning(
            'Vintage %s is %s: no ref in this clone reaches the commit that '
            'built it, so no finding published off this run can be reproduced. '
            'Rebuild from main before publishing anything from it.',
            vintage,
            provenance,
        )
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
    #: commodity x year, USD - commodity output, the column-side counterpart
    #: of ``x``. Nominal, matching the year's own Make.
    q: pd.DataFrame
    #: year -> the Leontief inverse, ``(I - (Adom + Aimp))^-1``, commodity x
    #: commodity. Total rather than domestic-only, matching the production
    #: path. Needed for ``N``, and for the own-loop term ``L[j, j]``.
    L: dict[int, pd.DataFrame]
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
    L: dict[int, pd.DataFrame] = {}
    q_parts: dict[int, pd.Series] = {}

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
            aq = derive_cornerstone_Aq_scaled()
            L[year] = compute_L_matrix(A=aq.Adom + aq.Aimp)
            q_parts[year] = aq.scaled_q
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
        q=pd.DataFrame(q_parts).rename_axis(index='commodity', columns='year'),
        Vnorm=Vnorm,
        L=L,
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


def commodity_rho(span: Span, weight_year: int | None = None) -> pd.DataFrame:
    """``rho`` on the **commodity** axis: ``PI[base] / PI[y]``, year by year.

    ``rho`` is the house inflation adjustment factor — the Excel ``Rho`` panel,
    ``get_rho_inflation_ratio``, and the ``rho_ty = PI_by / PI_ty`` of the US
    methods paper. ⚠️ It is the **reciprocal** of the forward ratio
    ``get_cornerstone_industry_price_ratio`` returns.

    ``x_real / x`` recovers the industry-axis ``rho`` the span was built with,
    so this cannot drift from the one :func:`deflate_x` already applies to
    ``B``. ``A`` and ``L`` are commodity x commodity, so it has to be carried
    across: each commodity takes the average of its supplying industries,
    weighted by their base-year shares of its supply, the same form as
    :func:`~bedrock.utils.economic.inflation_helpers_cornerstone.get_vnorm_adjusted_commodity_price_ratio`
    but built from the span so no ``functools.cache`` can carry a stale config
    across a year switch.

    ⚠️ **The weighting is applied to the forward ratio and the result
    inverted**, not to ``rho`` directly: a weighted mean of reciprocals is not
    the reciprocal of a weighted mean, and the production helper averages the
    forward ratio. The two differ by up to 0.04 percentage points on figures
    built from this — a different estimator rather than noise.

    Base-year weights fix the supplier mix, so it measures prices and not mix;
    *weight_year* overrides that for sensitivity work and moves the answer by
    under 0.02 percentage points.

    ⚠️ This is the *commodity* axis; ``B_total(real=True)`` deflates on the
    *industry* axis before mapping through ``V_norm``. The two coincide only
    where industry prices are uniform within a commodity's supplying mix — a
    gap under 0.14% at the median but reaching 36% at the tail. Sized per year
    by ``L_deflator_axis_gap.csv`` in
    :mod:`bedrock.analysis.nowcasting.L_dollar_basis`.
    """
    base = int(span.x.columns[0])
    rho_ind = span.x_real / span.x
    if not np.allclose(rho_ind[base].dropna(), 1.0):
        raise ValueError(
            f'x_real / x is not 1.0 in the base year {base} - x_real was '
            f'deflated to a different base than the span starts at.'
        )
    Vnorm = span.Vnorm[weight_year or base]
    supply = Vnorm.sum(axis=0)
    weights = Vnorm.divide(supply.where(supply > 1e-9, 1.0), axis=1)
    forward = pd.DataFrame(
        {
            year: (1.0 / rho_ind[year]).reindex(Vnorm.index).fillna(1.0) @ weights
            for year in span.L
        }
    )
    rho = 1.0 / forward
    # A commodity no industry supplies would average over nothing and come back
    # 0, which cannot divide. Neutral 1.0, as the production helper does.
    return rho.where(supply.gt(1e-9), 1.0, axis=0)


def L_real(span: Span) -> dict[int, pd.DataFrame]:
    """Each year's Leontief inverse in constant base-year dollars.

    ``rebase_coefficient_matrix`` is the same similarity transform that
    deflates ``A``, and it carries through the inverse, so this needs no ``A``
    and no re-solve.
    """
    rho = commodity_rho(span)
    return {
        year: rebase_coefficient_matrix(matrix=L, rho=rho[year])
        for year, L in span.L.items()
    }


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


def N_total(span: Span, real: bool = False) -> pd.DataFrame:
    """Total-CO2e commodity factor including indirect effects, commodity x year.

    ``N = B @ L`` with ``L = (I - (Adom + Aimp))^-1``, matching the production
    path's total-requirements form. This is the factor the smoothing project is
    ultimately trying to hold steady; ``B`` is only the direct part of it.

    *real* moves **both** sides onto constant base-year dollars - ``B`` through
    :func:`deflate_x` and ``L`` through :func:`L_real`. ⚠️ **Both or neither**
    (#957). Until 2026-09-21 this deflated ``B`` and left ``L`` at each year's
    own prices, which is neither a current-price nor a constant-price factor;
    ``A`` is a ratio of current dollars to current dollars but it still moves
    with the *relative* price ``p_i / p_j``, so leaving it alone does not leave
    it neutral. The hybrid put median ``|dN|`` at 18.4% in 2021 against 8.1%
    on a consistent basis and flipped the sign of the move for 531 of 2,835
    commodity-years.

    On a consistent basis the deflation cancels out of the level entirely -
    ``N_real[j] = N_nominal[j] / rho_j``, because the ``rho_i`` in
    ``B_real = B / rho`` meets its inverse in
    ``L_real[i, j] = L[i, j] rho_i / rho_j``. So this is the
    same factor the reporting path produces via
    ``inflation_adjust_ef_denom_to_new_base_year``, and unlike the hybrid it
    *is* comparable across years as a level.

    ⚠️ It is not comparable across *bases*. ``delta_B_pct_of_N`` is invariant
    to the choice of base year, since the ``rho_j`` cancels between ``dB`` and
    ``N``, but it is **not** invariant to the hybrid-to-real switch: its
    denominator moves, by a median 0% to 15% depending on the year. The
    ranking it drives is stable - 29 or 30 of the top 30 survive in every year
    - but a level quoted from a run before 2026-09-21 will not reproduce.

    Method and measurements: ``bedrock/analysis/nowcasting/About_the_L_dollar_basis.md``.
    """
    B = B_total(span, real=real)
    L_by_year = L_real(span) if real else span.L
    columns: dict[int, pd.Series] = {}
    for year in L_by_year:
        L = L_by_year[year]
        columns[year] = B[year].reindex(L.index).fillna(0.0) @ L
    return pd.DataFrame(columns).rename_axis(index='commodity', columns='year')


def B_change(span: Span, real: bool = True) -> pd.DataFrame:
    """Year-on-year change in the emission factor, one row per year-pair.

    ``B`` is the **direct** commodity factor and ``N = B @ L`` the total one,
    indirect effects included. The project's target is a steady ``N``, so a
    move in ``B`` matters in proportion to how much of ``N`` it drives. Long
    rather than wide, so it sorts on year and commodity together, and defaults
    to **real** dollars: a nominal factor falls whenever prices rise, which
    over this span would put inflation at the top of the ranking (see
    :func:`price_effect`).

    **Rank on** ``abs_delta_B_pct_of_N``, the default sort. It is the change in
    a commodity's own direct factor weighted by that factor's share of its own
    total factor, and the weighting cancels to something simpler than it
    sounds::

        delta_B_pct_of_N = pct_change_B * own_direct_share_of_N
                         = (dB / B) * (B * L[j,j] / N)
                         = dB * L[j,j] / N

    - ``own_direct_share_of_N`` - how much of this commodity's total factor is
      its own direct emissions, own-loop included via ``L[j, j]``.
    - ``delta_B_pct_of_N`` - the same move expressed against ``N`` instead of
      against ``B``.

    ⚠️ **This is a strictly smaller number than** ``pct_change_B``, and
    deliberately so. A commodity whose direct emissions are a tenth of its
    footprint can post a 50% move in ``B`` and shift ``N`` by 5%; on
    ``abs_pct_change_B`` it outranks a commodity that *is* its own footprint and
    moved 10%, which changed ``N`` by the same 10%. Re-using a 5% gate on the
    two columns therefore selects very different sets.

    ``pct_change_N`` is carried alongside so the weighted figure can be checked
    against what ``N`` actually did. They will not match: ``N`` also moves when
    *other* commodities' factors move, or when ``L`` does.

    ⚠️ **``L`` moves ``N`` more than the factors do - by 1.1x to 3.0x.**
    Holding ``L`` at the prior year isolates the part of the move the factors
    explain: ``pct_change_N_L_held`` is that part and ``pct_change_N_L_effect``
    the remainder, so a disagreement between ``delta_B_pct_of_N`` and
    ``pct_change_N`` can be read rather than guessed at. On a real basis the
    factor part runs 1.4% to 4.6% a year and the ``L`` part 2.7% to 6.1%.

    ⚠️ **These three columns were restated on 2026-09-21** (#957). They were
    computed against a hybrid ``N`` - real ``B``, nominal ``L`` - which put the
    ``L`` effect at 14.8 points in 2021 where a consistent basis puts it at
    6.0, and made ``L`` look 2x to 4x the factors rather than 1.1x to 3.0x. The
    old figures are not comparable with these and the worst year moved from
    2021 to 2020. ``delta_B_pct_of_N`` shifted too, by a median 0% to 15% a
    year, because its ``N`` denominator moved; the ranking it drives held, 29
    or 30 of the top 30 in every year.

    ⚠️ **``L`` is out of scope for the smoothing project.** ``B = (E/x) @
    Vnorm`` has exactly three inputs, and ``L`` is not one of them - it enters
    only through ``N``. It comes from ``A = U_norm @ V_norm``, the nowcast's
    own IO product, so no emissions-side change can move it. Diagnose it here,
    remediate it in Nowcast Phase 2.

    ⚠️ Read any of these next to ``B_from``. A commodity with a near-zero
    factor posts a large percentage off a rounding-scale numerator; filtering
    on ``B_from`` is what makes the ranking mean anything. Percentages are NaN
    rather than fabricated where the base is zero.
    """
    B = B_total(span, real=real)
    N = N_total(span, real=real)
    # the same basis N was built on, or the L effect is measured against a
    # denominator that does not share its dollar year
    L_by_year = L_real(span) if real else span.L
    years = [int(y) for y in B.columns]
    frames = []
    for prior, current in zip(years, years[1:]):
        L_prior = L_by_year[prior]
        own_loop = pd.Series(np.diag(L_prior.to_numpy()), index=L_prior.index).reindex(
            B.index
        )
        # N for the current year's factors run through the PRIOR year's L.
        # Differencing this against N_from isolates the part of the N move that
        # the emission factors explain, leaving the rest to L.
        N_to_L_held = (
            (B[current].reindex(L_prior.index).fillna(0.0) @ L_prior)
            .reindex(B.index)
            .to_numpy()
        )
        block = pd.DataFrame(
            {
                'year_from': prior,
                'year_to': current,
                'commodity': B.index,
                'B_from': B[prior].to_numpy(),
                'B_to': B[current].to_numpy(),
                'N_from': N[prior].to_numpy(),
                'N_to': N[current].to_numpy(),
                'N_to_L_held': N_to_L_held,
                'L_own_loop': own_loop.to_numpy(),
            }
        )
        frames.append(block)
    out = pd.concat(frames, ignore_index=True)
    out['delta_B'] = out['B_to'] - out['B_from']
    out['delta_N'] = out['N_to'] - out['N_from']
    out['pct_change_B'] = np.where(
        out['B_from'] != 0, out['delta_B'] / out['B_from'] * 100, np.nan
    )
    out['abs_pct_change_B'] = np.abs(out['pct_change_B'])
    out['pct_change_N'] = np.where(
        out['N_from'] != 0, out['delta_N'] / out['N_from'] * 100, np.nan
    )
    out['abs_pct_change_N'] = np.abs(out['pct_change_N'])
    # Split the N move into the part the factors explain and the part L does.
    out['pct_change_N_L_held'] = np.where(
        out['N_from'] != 0,
        (out['N_to_L_held'] - out['N_from']) / out['N_from'] * 100,
        np.nan,
    )
    out['pct_change_N_L_effect'] = out['pct_change_N'] - out['pct_change_N_L_held']
    out['own_direct_share_of_N'] = np.where(
        out['N_from'] != 0,
        out['B_from'] * out['L_own_loop'] / out['N_from'],
        np.nan,
    )
    out['delta_B_pct_of_N'] = np.where(
        out['N_from'] != 0,
        out['delta_B'] * out['L_own_loop'] / out['N_from'] * 100,
        np.nan,
    )
    out['abs_delta_B_pct_of_N'] = np.abs(out['delta_B_pct_of_N'])
    out = _with_names(out, 'commodity')
    return out.sort_values(
        ['year_to', 'abs_delta_B_pct_of_N'], ascending=[True, False]
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


def vnorm_share_of_B_movement(span: Span) -> pd.DataFrame:
    """**D13.** How much of each year's gross ``B`` movement is the Make?

    ``B = (E / x) @ Vnorm`` has two movable parts. Holding one and moving the
    other splits the year-on-year change::

        total     = (E/x)_t @ V_t  -  (E/x)_{t-1} @ V_{t-1}
        Vnorm eff = (E/x)_?  @ V_t  -  (E/x)_?  @ V_{t-1}

    and ``?`` is the direction choice every index number faces. Both one-sided
    forms are reported because they disagree materially - 4.8% against 6.1% in
    2021 - and the headline is their average, which is the usual answer when
    neither year has a claim to being the base.

    ``vnorm_share_weighted`` is **the figure to quote.** The unweighted share
    counts a kg/$ swing on a commodity nobody buys the same as one on
    electricity, and runs roughly twice as high for that reason.

    ⚠️ **This is annual churn, not cumulative drift**, and the two answer
    different questions. :func:`make_reallocation` measures how far the Make has
    moved from a fixed base year; this measures how much it reshuffled since
    last year. The Make can stop drifting while its year-on-year churn rises,
    and over 2023-24 it does exactly that.

    ⚠️ ``Vnorm`` is a direct term in ``B`` but is **out of scope for
    remediation** - it is the nowcast Make, and no emissions-side change moves
    it. Measured here, handed to Nowcast Phase 2.
    """
    years = sorted(int(y) for y in span.Vnorm)
    rows = []
    for prior, current in zip(years, years[1:]):
        V = span.Vnorm[current]
        V_prior = (
            span.Vnorm[prior].reindex(index=V.index, columns=V.columns).fillna(0.0)
        )

        def intensity(year: int) -> pd.Series:
            x = span.x[year].reindex(V.index).fillna(0.0)
            E = (
                span.E[span.E['year'] == year]
                .groupby('sector')['CO2e']
                .sum()
                .reindex(V.index)
                .fillna(0.0)
            )
            return (E / x).replace([np.inf, -np.inf], 0.0).fillna(0.0)

        i_prior, i_current = intensity(prior), intensity(current)
        total = (i_current @ V) - (i_prior @ V_prior)
        # Current-intensity and prior-intensity readings of the same effect.
        paasche = (i_current @ V) - (i_current @ V_prior)
        laspeyres = (i_prior @ V) - (i_prior @ V_prior)
        symmetric = 0.5 * (paasche + laspeyres)
        q = span.q[current].reindex(total.index).fillna(0.0)

        gross = float(total.abs().sum())
        gross_weighted = float((total.abs() * q).sum())
        row: dict[str, float] = {
            'year_from': prior,
            'year_to': current,
            'commodities': len(total),
        }
        for name, effect in (
            ('vnorm_share', symmetric),
            ('vnorm_share_paasche', paasche),
            ('vnorm_share_laspeyres', laspeyres),
        ):
            row[f'{name}_unweighted'] = float(effect.abs().sum()) / gross * 100
            row[f'{name}_weighted'] = (
                float((effect.abs() * q).sum()) / gross_weighted * 100
            )
        # Commodities the Make moved more than E/x did - unremediable on either
        # the emissions or the output side.
        row['vnorm_dominant_commodities'] = int(
            (symmetric.abs() > (total - symmetric).abs()).sum()
        )
        rows.append(row)
    out = pd.DataFrame(rows)
    ordered = ['year_from', 'year_to', 'commodities']
    ordered += ['vnorm_share_weighted', 'vnorm_share_unweighted']
    ordered += [c for c in out.columns if c not in ordered]
    return out[ordered]


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


# --- D14: an external floor on the fuel-combustion allocation ---------------

#: The 50 states and DC - the geography the US GHG inventory covers.
STATES_AND_DC = frozenset(
    'AL AK AZ AR CA CO CT DE FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT '
    'NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY DC'.split()
)

#: Non-state codes that are nevertheless **inside** the inventory boundary.
#: ``DM`` is Gulf of Mexico federal waters - NEI carries offshore platforms under
#: it, with BOEM ids and real coordinates, and the GHG inventory counts them.
OFFSHORE_CODES = frozenset({'DM'})


def _jurisdiction_class(state: pd.Series) -> pd.Series:
    """Label each row ``state``, ``offshore`` or ``territory``.

    **Facility data is kept whole**; this only makes the geography legible, so
    the boundary is a decision someone took rather than a filter nobody noticed.

    ⚠️ **The boundary that governs is BEA's, not the GHG inventory's.** These
    emissions become ``B = E / x``, and ``x`` is BEA gross output. BEA's
    economic territory for the NIPAs and the industry accounts extends past the
    50 states to everywhere the US holds exclusive economic rights - the
    Exclusive Economic Zone and the Outer Continental Shelf - so offshore oil
    and gas extraction **is** in the denominator. Dropping it from the numerator
    would put the two on different geographies and inflate the factor.

    ⚠️ **So do not reduce this to "the 50 states and DC".** That reads like the
    obvious tidy-up and it deletes ``DM``, Gulf of Mexico federal waters: 627
    offshore platforms carrying 4.9 Mt in 2022, **all of it oil and gas
    extraction**, 6.9% of that sector's facility basis - in the one sector this
    panel most turns on.

    ⚠️ The same reasoning **excludes territories**. Puerto Rico and the Virgin
    Islands sit outside BEA's NIPA economic territory - the boundary #913 raises
    for eGRID - so they have no ``x`` in this model to divide into, and emissions
    with no denominator would inflate whatever sector they land in.
    :func:`in_model_geography` drops them: 0.61 Mt over 30 facilities in 2022,
    0.09% of the union.
    """
    return pd.Series(
        np.where(
            state.isin(STATES_AND_DC),
            'state',
            np.where(state.isin(OFFSHORE_CODES), 'offshore', 'territory'),
        ),
        index=state.index,
    )


#: EPA table 3-11 is industrial stationary fuel combustion, the one inventory
#: family GHGRP subpart C can be compared against directly.
COMBUSTION_METASOURCE = 'T_3_11'

#: GHGRP flow names -> the gas names :data:`GWP100_AR6_CEDA` knows. Biogenic CO2
#: is deliberately absent: the GHG inventory books it separately, so including
#: it would compare a fossil allocation against a fossil-plus-biogenic floor.
GHGRP_FLOW_MAP = {
    'Carbon Dioxide': 'CO2',
    'Methane': 'CH4_fossil',
    'Nitrous Oxide': 'N2O',
}


def _naics_to_bea_detail() -> pd.Series:
    """NAICS 2017 code -> BEA detail code, for codes that map unambiguously.

    A NAICS that fans out to several BEA details is dropped rather than picked
    from: there is no basis in a facility record for choosing between them, and
    the mass involved is small - 0.5% of subpart C in 2022.
    """
    path = (
        Path(__file__).parents[2]
        / 'utils'
        / 'mapping'
        / 'naics'
        / 'NAICS_to_BEA_Crosswalk_2017.csv'
    )
    pairs = (
        pd.read_csv(path, dtype=str)[['NAICS_2017_Code', 'BEA_2017_Detail_Code']]
        .dropna()
        .drop_duplicates()
    )
    fanout = pairs.groupby('NAICS_2017_Code')['BEA_2017_Detail_Code'].nunique()
    unique = pairs[pairs['NAICS_2017_Code'].isin(fanout[fanout == 1].index)]
    return unique.set_index('NAICS_2017_Code')['BEA_2017_Detail_Code']


def ghgrp_served_years(years: tuple[int, ...]) -> tuple[int, ...]:
    """Drop years ``stewi`` has no way to serve, naming each one.

    Every year EPA published arrives by download. 2024 did not happen: EPA
    stopped at 2023, so that year is built into ``stewi`` from the FOIA'd
    Envirofacts views and lives only in the local store. On a machine without
    that build there is nothing to download, and ``stewi`` would fail deep
    inside the year loop. Skipping it keeps the floor test worth running over
    the years that are there.
    """
    local = stewi.getAvailableInventoriesandYears('flowbyprocess').get('GHGRP', [])
    served = []
    for year in years:
        if year in GHGRP_LOCAL_BUILD_YEARS and str(year) not in local:
            logger.warning(
                'GHGRP %d skipped: EPA never published it, so stewi has nothing '
                'to download. Build it from the FOIA archive first - '
                'stewi.GHGRP A then B, -Y %d -A <EF_Views_*.zip>.',
                year,
                year,
            )
            continue
        served.append(year)
    return tuple(served)


def ghgrp_subpart_C(years: tuple[int, ...]) -> pd.DataFrame:
    """GHGRP subpart C by BEA detail sector and year, Mt CO2e. Requires network.

    Subpart C is **stationary fuel combustion**, reported facility by facility
    under the GHGRP. Facilities report it directly rather than having it
    allocated to them, so it moves only when fuel burned moves.

    ⚠️ ``download_if_missing=True`` is not optional. Without it ``stewi`` tries
    to regenerate the inventory from the live GHGRP API, which no longer serves
    the tables the 2017-2021 builds need, and those five years fail with errors
    that read like missing data rather than a missing flag.

    ⚠️ **2024 does not download**, it is built locally - see
    :func:`ghgrp_served_years`. Subparts E, BB, CC, L and O are absent from that
    build because EPA only publishes them in aggregated spreadsheets that stop
    at 2023. None of them is subpart C, so the floor is unaffected.

    ⚠️ **Electric power is excluded.** ``221100`` runs on eGRID in this model,
    not on table 3-11, so leaving it in would compare a floor against an
    allocation that was never meant to carry it.

    ⚠️ Facility NAICS is matched to the **longest** BEA-mapped prefix, 6 digits
    down to 2. In 2022 that resolves 95.7% of subpart C mass on an exact
    6-digit match and 3.6% on a shorter one.
    """
    gwp = {str(k): float(v) for k, v in GWP100_AR6_CEDA.items()}
    to_bea = _naics_to_bea_detail()
    lookup = set(to_bea.index)

    def bea_of(naics: object) -> str | None:
        text = str(naics)
        for length in (6, 5, 4, 3, 2):
            prefix = text[:length]
            if prefix in lookup:
                return str(to_bea[prefix])
        return None

    columns = []
    for year in ghgrp_served_years(years):
        flows = stewi.getInventory(
            'GHGRP', year=year, stewiformat='flowbyprocess', download_if_missing=True
        )
        combustion = flows[
            (flows['Process'] == 'C') & flows['FlowName'].isin(GHGRP_FLOW_MAP)
        ].copy()
        combustion['CO2e'] = combustion['FlowAmount'] * combustion['FlowName'].map(
            GHGRP_FLOW_MAP
        ).map(gwp)
        facilities = stewi.getInventoryFacilities(
            'GHGRP', year, download_if_missing=True
        )[['FacilityID', 'NAICS', 'State']]
        combustion = combustion.merge(facilities, on='FacilityID', how='left')
        combustion['sector'] = combustion['NAICS'].map(bea_of)
        combustion = _drop_outside_geography(combustion, 'CO2e', 'GHGRP', year)
        resolved = combustion.dropna(subset=['sector'])
        logger.info(
            'GHGRP %d: subpart C %.1f Mt, %.1f%% resolved to a BEA sector',
            year,
            combustion['CO2e'].sum() / 1e9,
            resolved['CO2e'].sum() / combustion['CO2e'].sum() * 100,
        )
        columns.append(
            (resolved.groupby('sector')['CO2e'].sum() / 1e9).rename(int(year))
        )
    floor = pd.concat(columns, axis=1)
    return floor.drop(index='221100', errors='ignore')


# --- D14b: the subpart W half of the combustion floor (#927) ---------------
#
# The views themselves are acquired in bedrock.extract.epa.EPA_GHGRP_SubpartW and
# classified in bedrock.transform.ghg.ghgrp_subpart_w - this is pipeline data, not
# a diagnostic. What stays here is the diagnostic use of it: putting subpart W
# combustion on the BEA sector axis, and adding it to the subpart C floor.


def ghgrp_subpart_W_by_sector(years: tuple[int, ...]) -> pd.DataFrame:
    """Subpart W combustion by BEA detail sector and year, Mt CO2e.

    The subpart C floor's counterpart, on the same sector axis and under the
    same geography gate, so the two can simply be added.
    """
    burned = ghgrp_subpart_w.subpart_W_combustion(years)
    columns = []
    for year in sorted(set(burned['year'])):
        placed = (
            burned[burned['year'] == year]
            .groupby('FacilityID')['CO2e']
            .sum()
            .reset_index()
            .join(_facility_sectors('GHGRP', year), on='FacilityID')
        )
        # A subpart W reporter that stewi's facility file does not carry has no
        # state and no NAICS, so it can be placed neither in a geography nor in
        # a sector. Say so, rather than letting the geography gate report it as
        # a territory - it is a facility file gap, not a boundary.
        unknown = placed['State'].isna()
        if unknown.any():
            logger.info(
                'GHGRP subpart W %d: %.2f Mt over %d facilities absent from the '
                'stewi facility file, so they carry no NAICS and no state.',
                year,
                placed.loc[unknown, 'CO2e'].sum() / 1e9,
                int(unknown.sum()),
            )
        placed = _drop_outside_geography(placed[~unknown], 'CO2e', 'subpart W', year)
        resolved = placed.dropna(subset=['sector'])
        columns.append(
            (resolved.groupby('sector')['CO2e'].sum() / 1e9).rename(int(year))
        )
    if not columns:
        return pd.DataFrame()
    return pd.concat(columns, axis=1).drop(index='221100', errors='ignore')


def ghgrp_combustion_floor(years: tuple[int, ...]) -> pd.DataFrame:
    """The whole facility-reported combustion floor: subpart C plus subpart W.

    ⚠️ **Subpart C alone is not the floor, and for oil and gas it is the wrong
    segment's fuel.** In 2017 the facilities carrying a NAICS of 211 reported
    44.93 Mt CO2e under subpart C and 177.18 Mt under subpart W. The 44.93 is
    78% gas processing plants, 11% offshore production and 10% facilities that
    file no subpart W report at all - and **none of it is onshore production**,
    which reports its combustion under subpart W. Adding that on the same
    facility-NAICS rule takes ``211000``'s 2017 floor from 44.9 Mt to 113.4 Mt.

    The two halves do not overlap: the segments that report combustion under
    subpart W are absent from the subpart C fuel tables, and the processing and
    transmission segments that report under subpart C are absent from the
    subpart W combustion tables. Adding them is a union, not a sum of two
    overlapping sets.

    ⚠️ **Two vintages meet here on the archive years.** Subpart C arrives
    through ``stewi``, which holds EPA's published build for 2019-2023; subpart W
    arrives from the FOIA'd export, a later vintage of the same years in which
    facilities have restated. The difference measured at the total is 0.01% and
    the sector axis is identical, so it does not move a floor - but it is why
    the two are logged separately rather than only as a sum.
    """
    subpart_c = ghgrp_subpart_C(years)
    subpart_w = ghgrp_subpart_W_by_sector(years)
    if subpart_w.empty:
        return subpart_c
    shared = [year for year in subpart_c.columns if year in subpart_w.columns]
    floor = subpart_c.add(subpart_w.reindex(columns=shared), fill_value=0.0)
    floor = floor.reindex(columns=subpart_c.columns)
    logger.info(
        'GHGRP combustion floor, Mt CO2e: subpart C %s, + subpart W combustion '
        '%s. At 211000 the W half is %s against a C half of %s.',
        subpart_c.sum().round(0).to_dict(),
        subpart_w.sum().round(0).to_dict(),
        subpart_w.reindex(['211000']).iloc[0].round(1).to_dict(),
        subpart_c.reindex(['211000']).iloc[0].round(1).to_dict(),
    )
    return floor


def combustion_floor_test(
    detail: pd.DataFrame,
    floor: pd.DataFrame,
    min_floor_Mt: float = 1.0,
    tolerance: float = 0.05,
) -> pd.DataFrame:
    """**D14.** Did we allocate a sector less fuel combustion than it reported?

    The GHGRP only covers facilities over the 25,000 tCO2e reporting threshold,
    so the floor is a **lower bound** on what a sector burned, never an estimate
    of the total. Whatever table 3-11 allocates to a sector should be at least
    this much. That makes it the only external check in this module needing no
    answer key, no deflator and no benchmark year, and it is available for every
    year of the span, 2024 included.

    ⚠️ **Pass it the floor from :func:`ghgrp_combustion_floor`, not subpart C
    alone.** Oil and gas production reports its combustion under subpart W, so a
    subpart C floor holds none of it and ``211000`` was being scored against gas
    processing plants' fuel - 44.9 Mt in 2017 where the whole reported floor is
    113.4 Mt (#927).

    ⚠️ **A sector below the floor in every year is not necessarily a defect,
    and not necessarily benign either.** ``boundary_offset`` names a pattern -
    a constant offset, which cannot make a factor rocky - and it has two very
    different causes. Petroleum refineries, iron and steel and wet corn milling
    sit below the floor throughout because the GHGRP counts combustion of
    process-derived fuels, refinery still gas and coke oven and blast furnace
    gas, that the GHG inventory books outside table 3-11: a definition
    difference, and reading it as an error would be the wrong conclusion from
    the right test. ``211000`` is the other kind. Lease and plant fuel **are**
    inside table 3-11, and the allocation misses them because a purchase row
    cannot see fuel that nobody sold (#927) - so the same verdict there is a
    level defect, and the largest shortfall in the table.

    The column carrying the finding is therefore ``verdict``:

    ==================  ========================================================
    ``clears``          at or above the floor every year; nothing to answer
    ``boundary_offset`` below it *every* year - a definition difference
    ``intermittent``    clears some years and breaches others, which only
                        year-to-year volatility can produce
    ==================  ========================================================

    Rank the intermittent group on ``ratio_spread``. *min_floor_Mt* drops
    sectors whose floor is too small to bear a ratio, and *tolerance* is the
    slack allowed before a year counts as a breach.
    """
    combustion = detail[
        detail['MetaSources'].str.contains(COMBUSTION_METASOURCE, na=False)
    ]
    allocated = (
        combustion.groupby(['year_to', 'sector'])['E_to'].sum().unstack('sector') / 1e9
    )
    # The first year of the span is only ever an E_from, so recover it from the
    # first year-pair rather than losing it.
    first_pair = int(allocated.index.min())
    allocated.loc[first_pair - 1] = (
        combustion[combustion['year_to'] == first_pair]
        .groupby('sector')['E_from']
        .sum()
        / 1e9
    )
    allocated = allocated.sort_index().T

    years = [int(y) for y in floor.columns if int(y) in set(allocated.columns)]
    ours = allocated.reindex(columns=years)
    theirs = floor.reindex(columns=years)
    shared = ours.index.intersection(theirs.index)
    ours, theirs = ours.loc[shared].fillna(0.0), theirs.loc[shared].fillna(0.0)

    ratio = (ours / theirs).where(theirs > min_floor_Mt)
    ratio = ratio[ratio.notna().all(axis=1)]
    if ratio.empty:
        raise ValueError(
            f'No sector carries a GHGRP floor above {min_floor_Mt} Mt in all of '
            f'{years}. The floor and the allocation are probably not on the same '
            f'sector axis - check the NAICS-to-BEA resolution in the log.'
        )
    breached = ratio < (1 - tolerance)

    out = pd.DataFrame(
        {
            'years_below_floor': breached.sum(axis=1),
            'years': len(years),
            'min_ratio': ratio.min(axis=1),
            'max_ratio': ratio.max(axis=1),
            'ratio_spread': ratio.max(axis=1) / ratio.min(axis=1),
            'floor_Mt_last': theirs.loc[ratio.index, years[-1]],
            'shortfall_Mt': (theirs.loc[ratio.index] - ours.loc[ratio.index])
            .where(breached, 0.0)
            .sum(axis=1),
        }
    )
    out['verdict'] = np.where(
        out['years_below_floor'] == 0,
        'clears',
        np.where(
            out['years_below_floor'] == len(years), 'boundary_offset', 'intermittent'
        ),
    )
    out = out.join(ratio.add_prefix('ratio_'))
    out = _with_names(out.rename_axis('sector').reset_index())
    return out.sort_values(
        ['verdict', 'ratio_spread'], ascending=[True, False]
    ).reset_index(drop=True)


# --- D16: sectors whose own facilities report more than we assign them (#962) --

#: Subpart ``D`` is electricity, which runs on eGRID in this model, so it is out
#: of both sides. Everything else a facility reports is in, whichever inventory
#: table books it - that is the point of the test.
GHGRP_FLOOR_EXCLUDED_SUBPARTS = frozenset({'D'})

#: Below this much ``Direct`` mass a sector's shortfall has no process component
#: worth arguing about, so the whole of it is a vector's to restate.
DIRECT_IMMATERIAL_Mt = 0.05


def ghgrp_facility_floor(years: tuple[int, ...]) -> pd.DataFrame:
    """Everything the GHGRP's facilities reported, by BEA detail sector, Mt CO2e.

    Not :func:`ghgrp_combustion_floor`, and the difference is the whole point.
    That one is the combustion half, built to be commensurable with table 3-11.
    This one takes **every subpart except electricity**, so it can be compared
    against everything the inventory gives a sector - ``allocated`` and
    ``Direct`` together.

    ⚠️ **That comparison is the one that survives a boundary argument.** A
    half-ratio invites the objection that subpart H reports a kiln's fuel and its
    calcination as one number, or that refinery still gas is booked outside table
    3-11. Against the sector's whole assignment none of that matters, because the
    test assumes nothing about which subpart answers which inventory table - see
    :func:`under_attributed_sectors` and #948.
    """
    gwp = {str(k): float(v) for k, v in GWP100_AR6_CEDA.items()}
    columns = []
    for year in ghgrp_served_years(years):
        flows = stewi.getInventory(
            'GHGRP', year, stewiformat='flowbyprocess', download_if_missing=True
        )
        flows = flows[
            ~flows['Process'].isin(GHGRP_FLOOR_EXCLUDED_SUBPARTS)
            & flows['FlowName'].isin(GHGRP_FLOW_MAP)
        ].copy()
        flows['CO2e'] = flows['FlowAmount'] * flows['FlowName'].map(GHGRP_FLOW_MAP).map(
            gwp
        )
        placed = flows.merge(
            _facility_sectors('GHGRP', year)[['NAICS', 'State', 'sector']],
            left_on='FacilityID',
            right_index=True,
            how='left',
        )
        placed = _drop_outside_geography(placed, 'CO2e', 'D16', year)
        resolved = placed.dropna(subset=['sector'])
        columns.append(
            (resolved.groupby('sector')['CO2e'].sum() / 1e9).rename(int(year))
        )
    return pd.concat(columns, axis=1)


def _by_year(detail: pd.DataFrame, mask: pd.Series) -> pd.DataFrame:
    """A sector-by-year table of ``E`` for the rows *mask* selects, Mt CO2e.

    The first year of the span is only ever an ``E_from``, so it is recovered
    from the first year-pair rather than lost - the same recovery
    :func:`combustion_floor_test` makes.
    """
    part = detail[mask]
    table = part.groupby(['year_to', 'sector'])['E_to'].sum().unstack('sector') / 1e9
    first_pair = int(table.index.min())
    table.loc[first_pair - 1] = (
        part[part['year_to'] == first_pair].groupby('sector')['E_from'].sum() / 1e9
    )
    return table.sort_index().T


def under_attributed_sectors(
    detail: pd.DataFrame, floor: pd.DataFrame, min_gap_Mt: float = 0.05
) -> pd.DataFrame:
    """**D16.** Sectors given less than their own facilities reported (#962).

    The GHGRP covers only facilities over 25,000 tCO2e, so a sector's total is a
    **lower bound** on what its facilities emitted. Where that lower bound clears
    the *whole* inventory assignment - ``allocated`` and ``Direct`` together -
    the sector is under-attributed, and no argument about which subpart answers
    which table can explain it away. #948 §2.

    ⚠️ **The gap does not have one fix, and the split is the finding.** Following
    #948 §3, ``allocated`` is what a facility basis can **restate** and ``Direct``
    is what it can only **relocate**:

    ==============  ==========================================================
    ``restate``     ``Direct`` is immaterial, so no process mass is in dispute
                    and the table 3-11 vector simply gives the sector too
                    little. A better combustion split closes it - this is what
                    #929 is for.
    ``relocate``    ``Direct`` is material: the inventory books mass at another
                    sector that these facilities report. A vector cannot touch
                    it. #953.
    ==============  ==========================================================

    In 2022 that split is **17.9 Mt over 17 sectors** to restate against **126.7
    Mt over 6** to relocate, and petroleum refineries alone is 96.7 Mt of the
    second. ⚠️ **So do not read the residual gap after #929 lands as the
    integration having failed** - seven eighths of it was never a vector's to
    close.

    Ranked by the mass the facilities report over what the inventory assigns.
    """
    assigned = _by_year(detail, detail['sector'].notna())
    shared = [year for year in floor.columns if year in assigned.columns]
    inventory = assigned.reindex(columns=shared)
    direct = (
        _by_year(detail, detail['AttributionSources'] == 'Direct')
        .reindex(columns=shared)
        .reindex(index=inventory.index)
        .fillna(0.0)
    )
    in_scope = [
        sector
        for sector in inventory.index
        if str(sector)[:2] in FACILITY_SCOPE_PREFIXES
        and sector not in NOT_FACILITY_COMPARABLE
    ]
    inventory = inventory.loc[in_scope].fillna(0.0)
    direct = direct.loc[in_scope]
    reported = floor.reindex(index=in_scope, columns=shared).fillna(0.0)

    gap = (reported - inventory).where(reported > inventory, 0.0)
    under = gap[(gap > min_gap_Mt).any(axis=1)]
    if under.empty:
        return pd.DataFrame()
    last = shared[-1]
    out = pd.DataFrame(
        {
            'years_under': (gap.loc[under.index] > min_gap_Mt).sum(axis=1),
            'years': len(shared),
            'gap_Mt_last': under[last],
            'gap_Mt_mean': under.mean(axis=1),
            'allocated_Mt_last': (inventory - direct).loc[under.index, last],
            'direct_Mt_last': direct.loc[under.index, last],
            'inventory_Mt_last': inventory.loc[under.index, last],
            'reported_Mt_last': reported.loc[under.index, last],
        }
    )
    out['fix'] = np.where(
        direct.loc[under.index, last] < DIRECT_IMMATERIAL_Mt, 'restate', 'relocate'
    )
    out = out.join(under.add_prefix('gap_'))
    out = _with_names(out.rename_axis('sector').reset_index())
    return out.sort_values('gap_Mt_mean', ascending=False).reset_index(drop=True)


# --- D15: a facility-reported basis for stationary combustion ---------------

#: SCC level 1: 1 is external combustion, 2 is internal combustion, 3 is an
#: industrial process. All three are on-site emissions a facility reports, and
#: all three are needed: a cement kiln burns fuel and calcines limestone in one
#: vessel, and NEI books the vessel under 3.
ONSITE_SCC_BRANCHES = ('1', '2', '3')

#: The combustion half of that, where fuel burned is separable.
COMBUSTION_SCC_BRANCHES = ('1', '2')

#: Mobile and non-road source codes, which NEI files as point sources at the
#: site that hosts them. Their first digit is ``2``, the same as stationary
#: internal combustion, so the branch test alone lets them in - and 33.7 Mt of
#: 2022 CO2 with them, 33.6 Mt of it aircraft at airports under ``2275``. The
#: GHG inventory books all of it to mobile combustion, not to the airport
#: operator, so a *stationary* basis that carries it exceeds the inventory by
#: construction: ``48A000`` read 7.63x before this (#925).
MOBILE_SCC_PREFIX = '22'

#: Sectors the facility side never carries, so the inventory side must not
#: either. ``221100`` runs on eGRID here and is filtered out of the facility
#: union by construction; ``F01000`` is personal consumption, which has no gross
#: output and is outside this module's premise entirely. Scoring the facility
#: basis against a denominator containing them reports a coverage failure where
#: there was only a scope boundary - together they were 58% of the sectors that
#: looked uncovered, and neither was ever a candidate.
NOT_FACILITY_COMPARABLE = frozenset({'221100', 'F01000'})

#: The industries this basis is for: mining, utilities and manufacturing. These
#: are where emissions happen at a plant somebody reports. Everything else that a
#: vector currently places - government buildings, livestock, trucking, real
#: estate - is mobile, biological or diffuse, and no facility reports it because
#: none emits it. Scoring the basis across all of those measures the boundary
#: rather than the basis: in scope it reaches 94% of the allocated mass, and
#: across every sector it reaches 34%.
FACILITY_SCOPE_PREFIXES = ('21', '22', '31', '32', '33')

#: GHGRP subpart D is electricity generation, which runs on eGRID in this model.
#: Every other subpart is on-site emissions at an industrial facility, so the
#: default is to take them all rather than to guess which ones "are combustion" -
#: subpart H is one CO2 number covering a kiln's fuel and its calcination
#: together, and no field in it separates them.
GHGRP_EXCLUDED_SUBPARTS = frozenset({'D'})

#: SCC level 3 == '007' is **process gas** - refinery still gas, coke oven gas,
#: blast furnace gas. Fuel the facility made itself as a byproduct.
PROCESS_GAS_SCC_LEVEL3 = '007'

#: First year NEI's SCC coding supports ``fuel_class``. Before 2021 nearly all
#: on-site CO2 sat on industrial-process SCCs (branch 3); from 2021 most of that
#: mass sits on combustion SCCs instead, with the national total flat. The labels
#: changed, not the tonnes, so purchased / self_supplied / process read from SCC
#: digits are only defined from this year (#926). Union levels still use SCC
#: branches 1-3 in every year.
NEI_FUEL_CLASS_FIRST_YEAR = 2021


def _facility_sectors(inventory: str, year: int) -> pd.DataFrame:
    """Facility -> BEA sector and state, for *inventory* in *year*.

    Sector comes from the **facility**, never from the process code. A
    combustion SCC names the equipment and the fuel - "industrial boiler,
    natural gas" - and the same code appears in every industry, so an
    SCC-to-NAICS crosswalk cannot place it. NEI reports a NAICS for 100% of
    facilities, 96-97% of it on NAICS 2017 codes against 88-90% on 2012, which
    is why the 2017 crosswalk is the one used here.
    """
    to_bea = _naics_to_bea_detail()
    lookup = set(to_bea.index)

    def bea_of(naics: object) -> str | None:
        text = str(naics)
        for length in (6, 5, 4, 3, 2):
            if text[:length] in lookup:
                return str(to_bea[text[:length]])
        return None

    facilities = stewi.getInventoryFacilities(
        inventory, year, download_if_missing=True
    )[['FacilityID', 'NAICS', 'State']]
    facilities['sector'] = facilities['NAICS'].map(bea_of)
    return facilities.set_index('FacilityID')


def in_model_geography(state: pd.Series) -> pd.Series:
    """Rows inside BEA's economic territory: the 50 states, DC and offshore.

    The gate that :func:`_jurisdiction_class` describes. Territories are out
    because this model has no ``x`` for them; offshore is in because it does.
    """
    return state.isin(STATES_AND_DC | OFFSHORE_CODES)


def _drop_outside_geography(
    frame: pd.DataFrame, amount: str, label: str, year: int
) -> pd.DataFrame:
    """Drop rows outside the model geography, saying what went and why."""
    keep = in_model_geography(frame['State'])
    dropped = frame[~keep]
    if not dropped.empty:
        logger.info(
            '%s %d: dropping %.2f Mt over %d facilities in %s - outside BEA economic '
            'territory, so there is no x to divide them into. Offshore is kept.',
            label,
            year,
            dropped[amount].sum() / 1e9,
            dropped['FacilityID'].nunique(),
            sorted(dropped['State'].dropna().unique()),
        )
    return frame[keep]


def _same_site_after_FRS(
    ghgrp: pd.DataFrame, nei: pd.DataFrame, year: int
) -> pd.Series:
    """NEI facilities at a GHGRP site the FRS bridge did not link (#925).

    ``FRS_ID`` is the only thing saying that a GHGRP report and an NEI report
    describe one plant, and it says so only when FRS has filed both programmes
    under one registry record. Where it has filed them under two, the site
    enters the union twice: once at its GHGRP total and once at its NEI total.

    :func:`facilitymatcher.colocation.canonical_registry_map` folds the
    duplicate registry records that FRS's *own* facility attributes reveal, and
    that is where the fix belongs - it is a property of FRS, not of this
    analysis. It cannot reach the sites where FRS's attributes disagree but the
    two programmes' do, so the same rule is applied a second time here, to the
    addresses GHGRP and NEI report for themselves: same state, same normalised
    street address, and either a shared name token or the same NAICS
    three-digit prefix. The corroboration is what keeps a **tenant** at a host
    site - a slag processor at a steel mill, an industrial gas plant at a
    refinery - from being folded into its host and having its emissions
    deleted rather than deduplicated.

    :return: Series mapping an NEI ``FacilityID`` to the ``FRS_ID`` of the
        GHGRP site it shares an address with
    """

    def keys(inventory: str, ids: pd.Series) -> pd.DataFrame:
        facilities = stewi.getInventoryFacilities(
            inventory, year, download_if_missing=True
        )
        out = pd.DataFrame(
            {
                'FacilityID': facilities['FacilityID'].astype(str),
                'State': facilities['State'],
                'address': colocation.normalize_address(facilities['Address']),
                'tokens': colocation.normalize_name(facilities['FacilityName']).map(
                    colocation.name_tokens
                ),
                'sector3': facilities['NAICS'].fillna('').astype(str).str[:3],
            }
        )
        out = out[out['address'].str.match(r'^\d') & out['FacilityID'].isin(set(ids))]
        return out

    linked = set(nei['FRS_ID'].dropna())
    left = ghgrp[ghgrp['FRS_ID'].notna() & ~ghgrp['FRS_ID'].isin(linked)]
    covered = set(ghgrp['FRS_ID'].dropna())
    right = nei[nei['FRS_ID'].isna() | ~nei['FRS_ID'].isin(covered)]
    pairs = keys('GHGRP', left['FacilityID']).merge(
        keys('NEI', right['FacilityID']),
        on=['State', 'address'],
        suffixes=('_g', '_n'),
    )
    if pairs.empty:
        return pd.Series(dtype='object')
    shares_token = [bool(a & b) for a, b in zip(pairs['tokens_g'], pairs['tokens_n'])]
    pairs = pairs[
        pd.Series(shares_token, index=pairs.index)
        | ((pairs['sector3_g'] == pairs['sector3_n']) & (pairs['sector3_g'] != ''))
    ]
    frs_of_ghgrp = ghgrp.drop_duplicates('FacilityID').set_index('FacilityID')['FRS_ID']
    pairs = pairs.assign(FRS_ID=pairs['FacilityID_g'].map(frs_of_ghgrp))
    # An NEI record at an address two GHGRP facilities share belongs to that
    # site whichever of them it is folded onto; pick the lowest so a rerun
    # gives the same answer.
    out = pairs.dropna(subset=['FRS_ID']).groupby('FacilityID_n')['FRS_ID'].min()
    logger.info(
        'D15 %d: %d NEI facilities sit at the address of a GHGRP facility the '
        'FRS bridge did not link to them, over %d sites (#925)',
        year,
        len(out),
        out.nunique(),
    )
    return out


def facility_combustion(year: int) -> pd.DataFrame:
    """**D15.** Stationary combustion as reported by facilities, by sector.

    Built best-evidence-first and deduplicated on ``FRS_ID``:

    1. **GHGRP** - measured under a mandatory GHG programme. Subpart C is
       stationary combustion; onshore production, gathering and boosting and
       distribution report theirs under subpart W instead (#927).
    2. **NEI combustion SCCs** - the facilities GHGRP's 25,000 tCO2e reporting
       threshold leaves out. NEI carries roughly three times as many.

    ⚠️ **``fuel_class`` is the column that matters for allocation.** Combustion
    of fuel a facility *bought* can be spread by a row of the Use table, because
    a purchase is what the Use table records. Combustion of fuel the facility
    **made itself** - refinery still gas, coke oven gas, blast furnace gas -
    never appears as a purchase anywhere, so no Use row can carry it and
    attributing it with one is a category error. Those emissions are real and
    must be counted; they simply cannot ride the same vector. 48 Mt of NEI
    combustion CO2 sits on process-gas SCCs in 2022, concentrated in petroleum
    refineries (29 Mt), chemicals (11 Mt) and primary metals (3 Mt).

    ⚠️ **NEI SCC ``fuel_class`` is defined from**
    :data:`NEI_FUEL_CLASS_FIRST_YEAR` **only (#926).** Before that year the same
    CO2 mass sat almost entirely on industrial-process SCCs; from 2021 most of
    it sits on combustion SCCs, with the national total flat. NEI rows in
    earlier years stay in the union for levels but are labelled ``unclassified``
    on the NEI path, and the NEI share is not imputed onto matched GHGRP.

    ⚠️ **Self-supplied fuel is wider than byproduct gas, and an SCC cannot see
    the rest of it.** An oil and gas producer burning its own field gas is
    burning natural gas, and the SCC says natural gas.
    :func:`bedrock.transform.ghg.ghgrp_subpart_w.fuel_class`
    reaches that case from the GHGRP instead, where subpart W has the reporter
    name the fuel and a gas processing plant is identified by its segment - so
    lease and plant fuel are classified on evidence rather than missed (#927).
    ``fuel_class_basis`` says which route produced each row, and the NEI share
    is the fallback from :data:`NEI_FUEL_CLASS_FIRST_YEAR` onward rather than
    the only answer.

    ⚠️ **What is left unclassified is still a floor, not a measurement.** A
    GHGRP facility that neither reports subpart W nor matches an NEI record
    keeps its whole total labelled ``unclassified`` with ``fuel_class_known``
    False, because subpart C's fuel list is the Table C-1 emission factors and
    says nothing about who owned the fuel. The same label applies to NEI rows
    before :data:`NEI_FUEL_CLASS_FIRST_YEAR`.
    """
    sectors_nei = _facility_sectors('NEI', year)
    nei = stewi.getInventory(
        'NEI', year, stewiformat='flowbyprocess', download_if_missing=True
    )
    process = nei['Process'].astype(str)
    nei = nei[
        (nei['FlowName'] == 'Carbon Dioxide')
        & process.str[0].isin(ONSITE_SCC_BRANCHES)
        & (process.str[:2] != MOBILE_SCC_PREFIX)
    ].copy()
    nei_fuel_class = year >= NEI_FUEL_CLASS_FIRST_YEAR
    if nei_fuel_class:
        branch = nei['Process'].astype(str).str[0]
        nei['fuel_class'] = np.where(
            ~branch.isin(COMBUSTION_SCC_BRANCHES),
            'process',
            np.where(
                nei['Process'].astype(str).str[3:6] == PROCESS_GAS_SCC_LEVEL3,
                'self_supplied',
                'purchased',
            ),
        )
        nei = (
            nei.groupby(['FacilityID', 'fuel_class'])['FlowAmount']
            .sum()
            .rename('CO2e')
            .reset_index()
            .join(sectors_nei, on='FacilityID')
            .assign(source='NEI', fuel_class_known=True, fuel_class_basis='NEI SCC')
        )
    else:
        # Levels stay; SCC-derived fuel_class does not (#926).
        nei = (
            nei.groupby('FacilityID')['FlowAmount']
            .sum()
            .rename('CO2e')
            .reset_index()
            .join(sectors_nei, on='FacilityID')
            .assign(
                source='NEI',
                fuel_class='unclassified',
                fuel_class_known=False,
                fuel_class_basis='',
            )
        )

    gwp = {str(k): float(v) for k, v in GWP100_AR6_CEDA.items()}
    flows = stewi.getInventory(
        'GHGRP', year, stewiformat='flowbyprocess', download_if_missing=True
    )
    flows = flows[
        ~flows['Process'].isin(GHGRP_EXCLUDED_SUBPARTS)
        & flows['FlowName'].isin(GHGRP_FLOW_MAP)
    ].copy()
    flows['CO2e'] = flows['FlowAmount'] * flows['FlowName'].map(GHGRP_FLOW_MAP).map(gwp)
    # Held per subpart as well as per facility: a gas processing plant's fuel is
    # its subpart C mass alone, and its subpart W mass is fugitives (#927).
    per_facility = flows.groupby('FacilityID')['CO2e'].sum()
    subpart_c = flows[flows['Process'] == 'C'].groupby('FacilityID')['CO2e'].sum()
    ghgrp = (
        per_facility.reset_index()
        .join(_facility_sectors('GHGRP', year), on='FacilityID')
        .assign(
            source='GHGRP',
            fuel_class='unclassified',
            fuel_class_known=False,
            fuel_class_basis='',
        )
    )

    matches = facilitymatcher.get_matches_for_inventories(['NEI', 'GHGRP'])

    def frs_of(source: str) -> pd.Series:
        rows = matches[matches['Source'] == source].drop_duplicates('FacilityID')
        return rows.set_index('FacilityID')['FRS_ID']

    ghgrp['FRS_ID'] = ghgrp['FacilityID'].map(frs_of('GHGRP'))
    nei['FRS_ID'] = nei['FacilityID'].map(frs_of('NEI'))
    # Where FRS registered one site twice, the bridge links neither report to
    # the other and the site is counted twice. Recover those from the
    # addresses the two programmes report for themselves (#925).
    relabelled = _same_site_after_FRS(ghgrp, nei, year)
    if not relabelled.empty:
        nei['FRS_ID'] = nei['FacilityID'].map(relabelled).fillna(nei['FRS_ID'])

    # A facility that classified its own fuel does not need NEI's process-gas
    # share inferred onto it - it said what it burned, and for oil and gas NEI
    # cannot see the answer anyway, because field gas is natural gas to an SCC.
    reported = ghgrp_subpart_w.fuel_class(year, per_facility, subpart_c)
    said = reported.merge(
        ghgrp.drop(
            columns=['CO2e', 'fuel_class', 'fuel_class_known', 'fuel_class_basis']
        ),
        on='FacilityID',
        how='inner',
    ).assign(fuel_class_known=True)
    ghgrp = ghgrp[~ghgrp['FacilityID'].isin(set(reported['FacilityID']))]

    if nei_fuel_class:
        # GHGRP wins on the LEVEL where both report a facility - it is the
        # measured one - but only NEI knows the fuel, so the process-gas share
        # of the matched NEI record is carried over onto the GHGRP total.
        # Without this step every refinery and steel mill lands in GHGRP
        # unclassified, and the self-supplied total collapses to a quarter of
        # what NEI alone can see.
        nei_by_frs = (
            nei.dropna(subset=['FRS_ID'])
            .groupby(['FRS_ID', 'fuel_class'])['CO2e']
            .sum()
            .unstack('fuel_class')
            .fillna(0.0)
        )
        mix = nei_by_frs.div(nei_by_frs.sum(axis=1), axis=0).replace(
            [np.inf, -np.inf], np.nan
        )
        ghgrp['fuel_class_known'] = ghgrp['FRS_ID'].map(mix.notna().any(axis=1))
        pieces = []
        for cls in ('purchased', 'self_supplied', 'process'):
            share = ghgrp['FRS_ID'].map(mix[cls]) if cls in mix else None
            if share is None:
                continue
            pieces.append(
                ghgrp.assign(
                    CO2e=ghgrp['CO2e'] * share.fillna(0.0),
                    fuel_class=cls,
                    fuel_class_basis='NEI SCC share',
                )
            )
        # A facility NEI never saw keeps its whole total, labelled unclassified.
        unmatched = ghgrp['FRS_ID'].map(mix.sum(axis=1)).isna()
        pieces.append(ghgrp[unmatched])
        ghgrp = pd.concat([said, *pieces], ignore_index=True)
    else:
        # Pre-2021 NEI mix would inherit the SCC reclassification artefact (#926).
        ghgrp = pd.concat([said, ghgrp], ignore_index=True)
    ghgrp = ghgrp[ghgrp['CO2e'] > 0]

    covered = set(ghgrp['FRS_ID'].dropna())
    nei_only = nei[nei['FRS_ID'].isna() | ~nei['FRS_ID'].isin(covered)]

    union = pd.concat([ghgrp, nei_only], ignore_index=True)
    union = union[union['sector'].notna() & (union['CO2e'] > 0)]
    # Electric power runs on eGRID in this model, not table 3-11.
    union = union[union['sector'] != '221100']
    union = _drop_outside_geography(union, 'CO2e', 'D15', year)
    union = union.assign(year=year, jurisdiction=_jurisdiction_class(union['State']))
    breakdown = union.groupby('jurisdiction')['CO2e'].sum() / 1e9
    logger.info(
        'D15 %d geography kept: %s',
        year,
        ', '.join(f'{k} {v:.2f} Mt' for k, v in breakdown.round(2).items()),
    )
    return union


def nei_scc_reclassification_summary(
    years: tuple[int, ...] | None = None,
    *,
    include_union: bool = False,
) -> pd.DataFrame:
    """**#926.** Year-by-year NEI CO2 by SCC branch and ungated ``fuel_class``.

    Shows the 2020/2021 reclassification that makes NEI-derived ``fuel_class``
    discontinuous: national CO2 is flat while combustion branches (1-2) jump
    from a few percent to most of the mass. ``fuel_class_*_Mt`` columns are the
    assignment SCC digits *would* produce in every year - including before
    :data:`NEI_FUEL_CLASS_FIRST_YEAR` - so the pre-2021 collapse into ``process``
    is visible. Production :func:`facility_combustion` does not use that
    assignment before the gate year.

    When *include_union* is True, also counts facilities in the D15 union
    (GHGRP + NEI-only), which is slower because it builds each year fully.
    """
    years = years or tuple(y for y in YEARS if y <= NEI_LAST_YEAR)
    rows: list[dict[str, object]] = []
    for year in years:
        sectors = _facility_sectors('NEI', year)
        nei = stewi.getInventory(
            'NEI', year, stewiformat='flowbyprocess', download_if_missing=True
        )
        nei = nei[
            (nei['FlowName'] == 'Carbon Dioxide')
            & nei['Process'].astype(str).str[0].isin(ONSITE_SCC_BRANCHES)
        ].copy()
        process = nei['Process'].astype(str)
        branch = process.str[0]
        # Ungated: what SCC digits imply in every year, for the evidence table.
        ungated = np.where(
            ~branch.isin(COMBUSTION_SCC_BRANCHES),
            'process',
            np.where(
                process.str[3:6] == PROCESS_GAS_SCC_LEVEL3,
                'self_supplied',
                'purchased',
            ),
        )
        by_facility = (
            nei.assign(fuel_class=ungated)
            .groupby('FacilityID')
            .agg(CO2e=('FlowAmount', 'sum'))
            .join(sectors, how='left')
        )
        naics = by_facility['NAICS'].astype(str).str.replace(r'\.0$', '', regex=True)
        six_digit = naics.str.fullmatch(r'\d{6}').fillna(False)
        total = float(nei['FlowAmount'].sum())
        row: dict[str, object] = {
            'year': year,
            'nei_co2_Mt': total / 1e9,
            'scc_1_Mt': float(nei.loc[branch == '1', 'FlowAmount'].sum()) / 1e9,
            'scc_2_Mt': float(nei.loc[branch == '2', 'FlowAmount'].sum()) / 1e9,
            'scc_3_Mt': float(nei.loc[branch == '3', 'FlowAmount'].sum()) / 1e9,
            'combustion_share_%': (
                float(nei.loc[branch.isin(COMBUSTION_SCC_BRANCHES), 'FlowAmount'].sum())
                / total
                * 100
                if total
                else float('nan')
            ),
            'facilities_with_co2': int(by_facility.shape[0]),
            'naics_6digit_%': float(six_digit.mean() * 100) if len(by_facility) else float('nan'),
            'fuel_class_purchased_Mt': float(
                nei.loc[ungated == 'purchased', 'FlowAmount'].sum()
            )
            / 1e9,
            'fuel_class_self_supplied_Mt': float(
                nei.loc[ungated == 'self_supplied', 'FlowAmount'].sum()
            )
            / 1e9,
            'fuel_class_process_Mt': float(
                nei.loc[ungated == 'process', 'FlowAmount'].sum()
            )
            / 1e9,
            'nei_fuel_class_defined': year >= NEI_FUEL_CLASS_FIRST_YEAR,
        }
        if include_union:
            union = facility_combustion(year)
            row['union_facilities'] = int(union['FacilityID'].nunique())
            row['union_Mt'] = float(union['CO2e'].sum()) / 1e9
        rows.append(row)
        logger.info(
            'NEI SCC #926 %d: %.1f Mt total, combustion %.1f%%, %d facilities, '
            '6-digit NAICS %.1f%%%s',
            year,
            row['nei_co2_Mt'],
            row['combustion_share_%'],
            row['facilities_with_co2'],
            row['naics_6digit_%'],
            (
                f', union {row["union_facilities"]} facilities'
                if include_union
                else ''
            ),
        )
    return pd.DataFrame(rows).set_index('year')


def facility_basis_comparison(
    span: Span, facility: pd.DataFrame, detail: pd.DataFrame | None = None
) -> pd.DataFrame:
    """**D15.** The facility basis against the inventory it would replace.

    ⚠️ **Compared against everything the inventory gives the sector, not against
    table 3-11 alone.** A cement kiln burns fuel and calcines limestone in the
    same vessel, and both GHGRP and NEI report the vessel: subpart H is one CO2
    number with no combustion/process field, and NEI puts 69 of cement's 69.2 Mt
    on process SCCs. Scoring facility data against a combustion-only total
    therefore manufactures a coverage failure - cement scored 0.04 - and, at the
    other end, manufactures a 150% overshoot for refineries and steel whose
    facility totals legitimately include process units. Neither was real.

    ⚠️ **Widening the comparison does not widen what can be improved.** The
    process rows this pulls in - ``UMD_GHGIA_T_2_S1.direct``, ``T_4_31`` and the
    rest, 524 Mt in 2022 - are **100% ``Direct``-attributed**: the inventory
    already names the sector, with no Use row, no MECS and no vector in between.
    A facility basis cannot improve an assignment that was never derived. So the
    columns are kept apart:

    ``inventory_Mt``
        everything the inventory assigns the sector.
    ``allocated_Mt``
        the part a vector placed, and therefore the only part a facility basis
        could restate. **This is the number to rank on.**
    ``direct_Mt``
        the part the inventory placed itself. Counted, never improvable.

    *detail* is optional and only used to carry the table 3-11 subtotal through,
    so the older combustion-only reading stays visible next to the new one.
    """
    year = int(facility['year'].iloc[0])
    emissions = span.E[
        (span.E['year'] == year) & ~span.E['sector'].isin(NOT_FACILITY_COMPARABLE)
    ]
    if emissions.empty:
        raise ValueError(
            f'No emissions for {year} in the span. D15 can only compare a year '
            f'the span was built for.'
        )
    by_sector = emissions.groupby('sector')['CO2e']
    is_direct = emissions['attribution'] == 'Direct'

    out = pd.DataFrame(
        {
            'inventory_Mt': by_sector.sum() / 1e9,
            'direct_Mt': emissions[is_direct].groupby('sector')['CO2e'].sum() / 1e9,
            'allocated_Mt': emissions[~is_direct].groupby('sector')['CO2e'].sum() / 1e9,
            'facility_Mt': facility.groupby('sector')['CO2e'].sum() / 1e9,
            'derived_Mt': facility[facility['fuel_class'] == 'self_supplied']
            .groupby('sector')['CO2e']
            .sum()
            / 1e9,
            'facilities': facility.groupby('sector').size(),
            'states': facility.groupby('sector')['State'].nunique(),
        }
    )
    if detail is not None:
        combustion = detail[
            detail['MetaSources'].str.contains(COMBUSTION_METASOURCE, na=False)
            & (detail['year_to'] == year)
        ]
        out['table_3_11_Mt'] = combustion.groupby('sector')['E_to'].sum() / 1e9
    out = out[out['inventory_Mt'].notna() & (out['inventory_Mt'] > 0)].fillna(0.0)

    out['coverage'] = out['facility_Mt'] / out['inventory_Mt']
    out['residual_Mt'] = (out['allocated_Mt'] - out['facility_Mt']).clip(lower=0.0)
    out['basis'] = np.where(
        out['coverage'] < 0.05,
        'no facility data',
        np.where(
            out['coverage'] > 1.5,
            'facility exceeds the inventory',
            np.where(out['coverage'] >= 0.5, 'facility', 'facility + residual'),
        ),
    )

    # Shares are taken on the ALLOCATED mass only: restating a Direct row would
    # move a number the inventory already set, which is not on offer here.
    total_allocated = out['allocated_Mt'].sum()
    out['allocated_share_%'] = out['allocated_Mt'] / total_allocated * 100
    anchored = (out['basis'] != 'no facility data') & (out['allocated_Mt'] > 0)
    group_share = out.loc[anchored, 'allocated_share_%'].sum()
    facility_total = out.loc[anchored, 'facility_Mt'].sum()
    out['hybrid_share_%'] = out['allocated_share_%']
    if facility_total > 0:
        out.loc[anchored, 'hybrid_share_%'] = (
            out.loc[anchored, 'facility_Mt'] / facility_total * group_share
        )
    out['share_shift_pp'] = out['hybrid_share_%'] - out['allocated_share_%']

    out = _with_names(out.rename_axis('sector').reset_index())
    out['in_scope'] = (
        out['sector'].astype(str).str.strip().str[:2].isin(FACILITY_SCOPE_PREFIXES)
    )
    return out.sort_values(
        ['in_scope', 'inventory_Mt'], ascending=[False, False]
    ).reset_index(drop=True)


def facility_scope_split(
    basis: pd.DataFrame, facility: pd.DataFrame, floor: pd.Series
) -> pd.DataFrame:
    """**D15b.** What the inventory gives a sector, against what its own
    facilities reported - as a total, and split into its two halves.

    **Read** ``ghgrp_vs_inventory`` **first.** GHGRP only covers facilities over
    25,000 tCO2e, so a sector's GHGRP total is a **floor** on what its
    facilities emit. Where that floor clears the whole inventory assignment -
    ``allocated_Mt`` and ``direct_Mt`` together - the sector is
    **under-attributed**, and the statement survives every boundary objection
    the half-ratios attract, because it assumes nothing about which subpart
    answers which inventory table. ``under_attributed_Mt`` is the same finding
    in Mt, which is the order to act in.

    ⚠️ **A half-ratio above 1 does not imply under-attribution.** Fertilizer
    runs 1.32 on the process half and 0.93 on the combustion half, and lands at
    **0.85 on the total**; other basic inorganic chemicals run 1.05 and 0.38 and
    land at **0.41**. Both are over-attributed on the process side and
    under-covered overall. The correction runs the other way for cement, whose
    process half of 1.61 is inflated because subpart H reports a kiln's fuel
    together with its calcination - on the total it is **1.17**, and that is the
    figure to quote.

    ⚠️ **A large ratio is sometimes a reallocation between two sectors, not a
    level error.** Petroleum refineries read 2.21 and oil and gas extraction
    0.73; **together they read 1.04**. The inventory books the refining segment
    of its petroleum systems tables to extraction - ``211000`` takes 56.4 Mt of
    ``UMD_GHGIA_T_3_25`` and ``T_3_26`` where ``324110`` takes 3.55 - so check
    the obvious counterpart sector before reading a ratio as a level.

    ⚠️ **D15's own total ratio is a boundary comparison, not a replacement
    test.**
    ``facility_Mt`` spans both the mass a vector placed and the mass the
    inventory assigned itself, so its denominator has to span both as well -
    score GHGRP against ``allocated_Mt`` alone and natural gas distribution
    reads 160x on its own fugitives. But carrying ``direct_Mt`` on both sides
    only makes the totals commensurable. It does **not** make the facility
    union a candidate replacement for the part the inventory assigned itself,
    and this table is the measurement of how far it is from one.

    GHGRP subpart C is stationary fuel combustion and every other subpart is
    process or fugitive, so each half scores against the half of the inventory
    it corresponds to:

    ``C_vs_table_3_11``
        combustion against combustion, no process mass on either side. Subpart
        C is threshold-limited, so it is a **floor**: above 1 the current
        method allocates less than the sector's own facilities reported and
        there is no boundary question left to argue. **The only column here
        that settles direction.**
    ``other_vs_direct``
        every other subpart against what the inventory assigned the sector
        itself. In aggregate this lands near 1, but that is offsetting errors -
        per sector it ran 0.00 to 17.35 in 2022, because the two inventories
        draw the process boundary in different places. A flag for
        investigation, never a verdict.
        `#953 <https://github.com/cornerstone-data/bedrock/issues/953>`_.

    ⚠️ Two sectors reverse against the total ratio, both against the basis.
    Petrochemicals scores 0.39 on the total and 1.00 here, because a third of
    its allocated mass is vector-placed *non-combustion* that subpart C was
    never going to match. Cement scores 0.04 because a kiln reports its fuel
    under subpart H alongside its calcination, which is the artefact
    :func:`facility_union` widens the comparison to avoid.

    *floor* is one year of :func:`ghgrp_subpart_C`. Its NAICS-to-BEA resolution
    is the one :func:`facility_union` uses - equal sector by sector in 2022 -
    so the non-combustion half is the union's own GHGRP total less this, and
    never goes negative.

    ⚠️ **Subpart C alone is no longer the whole combustion floor.**
    :func:`ghgrp_combustion_floor` adds the subpart W combustion tables, where
    onshore production, gathering and boosting and gas distribution report their
    fuel (#927). Passing that instead moves mass between the two halves and
    leaves ``ghgrp_total_Mt`` and ``ghgrp_vs_inventory`` **exactly unchanged**,
    but it flips the combustion verdict for the oil and gas sectors: `211000`
    goes from 0.52 to **1.49** and `21311A` from 0.13 to **1.26**, both from
    clearing the floor to breaching it. The tables in
    ``About_facility_emissions_basis.md`` §2 quote the subpart C reading, so the
    two move together or not at all.
    """
    if 'table_3_11_Mt' not in basis:
        raise ValueError(
            'D15b needs the table 3-11 subtotal, so D15 has to be built with '
            'its optional *detail* argument.'
        )
    out = basis.set_index('sector').copy()
    ghgrp = facility[facility['source'] == 'GHGRP']
    out['ghgrp_Mt'] = (ghgrp.groupby('sector')['CO2e'].sum() / 1e9).reindex(
        out.index, fill_value=0.0
    )
    out['ghgrp_C_Mt'] = floor.reindex(out.index).fillna(0.0)
    out['ghgrp_other_Mt'] = (out['ghgrp_Mt'] - out['ghgrp_C_Mt']).clip(lower=0.0)

    out['C_vs_table_3_11'] = out['ghgrp_C_Mt'] / out['table_3_11_Mt'].replace(
        0.0, np.nan
    )
    out['other_vs_direct'] = out['ghgrp_other_Mt'] / out['direct_Mt'].replace(
        0.0, np.nan
    )
    # Above the floor on the like-for-like half: the current method allocates
    # less combustion than the sector's own facilities reported under a
    # threshold-limited programme. Nothing about the process boundary can
    # explain this one away.
    out['breaches_floor'] = out['ghgrp_C_Mt'] > out['table_3_11_Mt']

    # The accuracy test, and the one to read first. GHGRP only covers
    # facilities over 25,000 tCO2e, so a sector's GHGRP total is a FLOOR on
    # what its facilities emit. Where that floor clears the whole inventory
    # assignment - allocated and Direct together - the sector is
    # under-attributed, and unlike either half-ratio the statement needs no
    # assumption about which subpart answers which inventory table.
    out['ghgrp_total_Mt'] = out['ghgrp_C_Mt'] + out['ghgrp_other_Mt']
    out['ghgrp_vs_inventory'] = out['ghgrp_total_Mt'] / out['inventory_Mt'].replace(
        0.0, np.nan
    )
    out['under_attributed_Mt'] = out['ghgrp_total_Mt'] - out['inventory_Mt']

    columns = [
        'name',
        'allocated_Mt',
        'direct_Mt',
        'inventory_Mt',
        'table_3_11_Mt',
        'ghgrp_C_Mt',
        'ghgrp_other_Mt',
        'ghgrp_total_Mt',
        'ghgrp_vs_inventory',
        'under_attributed_Mt',
        'C_vs_table_3_11',
        'other_vs_direct',
        'breaches_floor',
        'in_scope',
    ]
    return (
        out.reset_index()[['sector', *columns]]
        .sort_values(['in_scope', 'ghgrp_vs_inventory'], ascending=[False, False])
        .reset_index(drop=True)
    )


#: NEI cannot separate the biomass carbon a mill's recovery furnace emits.
_PAPER_BIOGENIC = (
    'NEI publishes one Carbon Dioxide flow and does not separate the biogenic '
    'part, while the GHGRP does and D15 excludes it. At a pulp or paper mill '
    'the recovery furnace burning black liquor is most of the carbon dioxide: '
    'NEI reports 93.8 Mt at NAICS 3221 facilities in 2022 against 35.4 Mt the '
    'inventory assigns the three paper sectors together. The NEI side of this '
    'sector therefore carries biomass carbon the inventory books outside the '
    'fossil total, and no field in NEI separates it'
)

#: Overshoots D15 has a cause on record for. The key is the sector; the value
#: is the counterpart sector the inventory books the mass to, or None, and what
#: the difference is.
#:
#: A named cause has to be **checkable**, which is the whole point of the
#: guard: where a counterpart is named, the excuse holds only if the two
#: sectors *together* come in under the tolerance. Where none is named, the
#: entry explains the overshoot without netting it out, and the sector is
#: reported rather than excused.
NAMED_BOUNDARY_DIFFERENCES: dict[str, tuple[str | None, str]] = {
    '324110': (
        '211000',
        'the inventory books the refining segment of its petroleum systems '
        'tables to extraction - 211000 takes 56.4 Mt of UMD_GHGIA_T_3_25 and '
        'T_3_26 where 324110 takes 3.55 - so the facility side reads the two '
        'sectors the way the plants are built and the inventory reads them the '
        'way the tables are written',
    ),
    '21311A': (
        '211000',
        'gathering and boosting and gas processing report their own fuel and '
        'fugitives under subpart W, and the inventory books natural gas '
        'systems to extraction (#927)',
    ),
    '322110': (None, _PAPER_BIOGENIC),
    '322120': (None, _PAPER_BIOGENIC),
    '322130': (None, _PAPER_BIOGENIC),
}


def facility_overshoot_guard(
    basis: pd.DataFrame, tolerance: float = 0.5, min_inventory_Mt: float = 1.0
) -> pd.DataFrame:
    """**D15c.** No sector may exceed its inventory total without a named cause.

    The facility union is built by taking the GHGRP whole and adding the NEI
    facilities it does not already cover, and ``FRS_ID`` is the only thing
    saying which those are. Where the match list is wrong the same plant enters
    twice, and the symptom is a sector whose facilities report more than the
    whole inventory gives it. That is what this checks, and it is the check
    that has to stay green for any level-based claim built on D15 (#925).

    ⚠️ **An overshoot is not automatically an error.** The inventory and the
    facility programs draw sector boundaries in different places, and where
    they do, a sector *should* come out over. The guard's job is to make the
    difference between "we have a reason" and "we do not" explicit, so:

    ``within tolerance``
        under ``1 + tolerance``. Nothing to answer.
    ``reallocation``
        :data:`NAMED_BOUNDARY_DIFFERENCES` names a counterpart sector the
        inventory books the mass to, **and the two together come in under the
        tolerance**. The excuse is tested, not asserted - if the pair is still
        over, the verdict falls back to ``named, still over``.
    ``named``
        a boundary difference is on record with its evidence, but it cannot be
        netted out against another sector. Reported, never excused.
    ``unexplained``
        nothing on record. **This is what the guard exists to surface**, and
        what ``--check-facility-overshoot`` fails on.

    *min_inventory_Mt* drops sectors too small to bear a ratio.
    """
    frame = basis.set_index('sector')
    over = frame[
        (frame['inventory_Mt'] > min_inventory_Mt) & (frame['coverage'] > 1 + tolerance)
    ]
    rows = []
    for sector, row in over.iterrows():
        counterpart, reason = NAMED_BOUNDARY_DIFFERENCES.get(str(sector), (None, ''))
        pair_coverage = np.nan
        if counterpart is not None and counterpart in frame.index:
            pair = frame.loc[[sector, counterpart]]
            pair_coverage = pair['facility_Mt'].sum() / pair['inventory_Mt'].sum()
        if not reason:
            verdict = 'unexplained'
        elif counterpart is None:
            verdict = 'named'
        elif pair_coverage <= 1 + tolerance:
            verdict = 'reallocation'
        else:
            verdict = 'named, still over'
        rows.append(
            {
                'sector': sector,
                'name': row['name'],
                'inventory_Mt': row['inventory_Mt'],
                'facility_Mt': row['facility_Mt'],
                'coverage': row['coverage'],
                'overshoot_Mt': row['facility_Mt'] - row['inventory_Mt'],
                'counterpart': counterpart or '',
                'pair_coverage': pair_coverage,
                'verdict': verdict,
                'reason': reason,
            }
        )
    out = pd.DataFrame(
        rows,
        columns=[
            'sector',
            'name',
            'inventory_Mt',
            'facility_Mt',
            'coverage',
            'overshoot_Mt',
            'counterpart',
            'pair_coverage',
            'verdict',
            'reason',
        ],
    )
    return out.sort_values('overshoot_Mt', ascending=False).reset_index(drop=True)


def _report_coverage(bands: pd.DataFrame, year: int) -> None:
    """Log D15d: which sectors clear the test, and what stops the rest."""
    graded = bands[bands['verdict'] != 'no facility data']
    vector = graded[graded['verdict'] == 'vector']
    logger.info(
        'D15d %d: %d of %d sectors may take the facility vector downward, '
        '%.1f Mt of %.1f (%.0f%%). Everywhere else it is a floor (#928).',
        year,
        len(vector),
        len(graded),
        vector['total_Mt'].sum(),
        graded['total_Mt'].sum(),
        vector['total_Mt'].sum() / graded['total_Mt'].sum() * 100,
    )
    blocked = graded[graded['verdict'] == 'floor']
    if not blocked.empty:
        logger.info(
            'D15d %d: coverage is %.3f at the median blocked sector and is '
            'not what stops them - %s. NEI-only mass above the GHGRP '
            'threshold is:\n%s',
            year,
            blocked['coverage'].median(),
            blocked['blocked_by'].value_counts().to_dict(),
            blocked[
                ['sector', 'name', 'total_Mt', 'coverage', 'unresolved', 'blocked_by']
            ]
            .head(10)
            .round(3)
            .to_string(index=False),
        )
    absent = bands[bands['verdict'] == 'no facility data']
    if not absent.empty:
        logger.info(
            'D15d %d: %d in-scope sectors have no facility data and keep the '
            'current basis by name: %s',
            year,
            len(absent),
            ', '.join(absent['sector'].astype(str)),
        )


def _report_overshoot(guard: pd.DataFrame, year: int) -> int:
    """Log D15c and return the number of sectors with no cause on record."""
    if guard.empty:
        logger.info('D15c %d: no sector exceeds its inventory total.', year)
        return 0
    by_verdict = guard.groupby('verdict')['overshoot_Mt'].agg(['size', 'sum'])
    logger.info(
        'D15c %d: %d sectors exceed their inventory total, %.1f Mt in ' 'all:\n%s',
        year,
        len(guard),
        guard['overshoot_Mt'].sum(),
        by_verdict.round(1).to_string(),
    )
    unexplained = guard[guard['verdict'] == 'unexplained']
    if not unexplained.empty:
        logger.warning(
            'D15c %d: %d sectors exceed their inventory total with no cause '
            'on record, %.1f Mt. A level-based claim on any of them is not '
            'supported until one is named or the overshoot is removed '
            '(#925):\n%s',
            year,
            len(unexplained),
            unexplained['overshoot_Mt'].sum(),
            unexplained[['sector', 'name', 'coverage', 'overshoot_Mt']]
            .round(2)
            .to_string(index=False),
        )
    return len(unexplained)


#: The GHGRP reporting threshold, 25,000 t CO2e, in the kg StEWI reports.
#: A facility above it is one the programme should have seen, so NEI-only mass
#: above it cannot be explained by the threshold - see
#: :func:`facility_coverage_bands`.
GHGRP_THRESHOLD_KG = 25_000 * 1_000

#: The fuel classes that are combustion. ``process`` is the calcining and
#: chemistry half, which the GHGRP threshold has nothing to do with.
COMBUSTION_FUEL_CLASSES = ('purchased', 'self_supplied')


def facility_coverage_bands(
    union: pd.DataFrame,
    floor: pd.Series,
    basis: pd.DataFrame | None = None,
    min_Mt: float = 0.5,
    coverage_floor: float = 0.95,
    unresolved_ceiling: float = 0.05,
) -> pd.DataFrame:
    """**D15d.** Which sectors may take the facility vector *downward*? (#928)

    A facility union is a **lower bound** on a sector, so it is informative in
    one direction only. Facility mass above the allocation means the sector is
    under-allocated and no coverage argument touches it - that is D14 and D16.
    Facility mass *below* the allocation could be over-allocation, or could be
    emissions nobody reported, and the data cannot say which. So the default
    rule is that facility data is a **floor**: it may raise a sector to what its
    own facilities reported and may never lower it.

    A sector escapes that restriction only where what the facilities report is
    close to a census of the sector, and this table is the test. Two quantities,
    both now measurable because #925 fixed the matching they rest on:

    ``coverage``
        ``ghgrp_Mt / (ghgrp_Mt + nei_below_Mt)`` - how much of the sector's
        reported combustion comes from the mandatory programme, against what the
        25,000 tCO2e threshold leaves to NEI alone.
    ``unresolved``
        NEI-only mass at facilities **above** that threshold, as a share of the
        sector's facility total. Those facilities cannot be below the threshold,
        so this is not coverage at all - it is a GHGRP twin the match list still
        misses, or a facility that should report and does not. Either way the
        sector's facility total is not a census while it is large.

    ⚠️ **Coverage is not the binding constraint, and that is the finding.** In
    2022 it is 0.976 at the median sector and 0.999 at the 90th percentile;
    moving its gate from 0.90 to 0.98 changes the verdict for one sector.
    ``unresolved`` is what decides: 0.091 at the median, 0.391 at the 90th
    percentile, and **every large sector that fails the joint gate fails on it**
    - oil and gas extraction at 0.120 with coverage 0.983, petrochemicals at
    0.313 with coverage 1.000, wet corn milling at 0.363 with coverage 0.999.

    ⚠️ **Imputing CO2 where NEI does not report it does not move this** (#967).
    It was the obvious way to widen ``coverage``, and it fails twice over: on
    the population it is used on - NEI facilities reporting no CO2 that have a
    GHGRP twin to check against - a per-sector CO2-per-criteria-pollutant ratio
    lands 67% out at the median sector and **+445% on the sub-threshold mass
    that is the whole point**, because the non-reporting population is not the
    reporting one at the same NOx. And it would not matter if it worked: taking
    the imputation at face value against discounting its measured bias moves the
    median sector's coverage by **0.019**.

    ⚠️ **The GHGRP side is the combustion floor, not the union's GHGRP rows.**
    ``facility_combustion`` labels a GHGRP facility's fuel from its own subpart
    W report or, failing that, from the fuel mix of its matched NEI record - and
    **24.7% of GHGRP mass in 2022 has neither**, so it stays ``unclassified``.
    Taking only the classified part as the numerator would make coverage depend
    on whether a facility matched into NEI, which is the very thing
    ``unresolved`` exists to keep separate. :func:`ghgrp_combustion_floor` is
    the definitional answer instead: subpart C is stationary combustion, plus
    the subpart W fuel tables for the segments that report there (#927).

    :param union: one year of :func:`facility_combustion`
    :param floor: one year of :func:`ghgrp_combustion_floor`, a Series of Mt by
        sector
    :param basis: one year of :func:`facility_basis_comparison`, to name the
        sectors with no facility data rather than let them fall through
    ⚠️ *min_Mt* gates the **exception, not the use**. A sector with facility
    data but too little of it to bear a ratio - 192 of them in 2022, 10.6 Mt
    between them - stays a ``floor`` with ``blocked_by`` saying so, rather than
    dropping out of the table. Facility data is used for every sector that has
    any; what has to be tested for is permission to go *down*.

    :param min_Mt: below this much facility combustion a sector cannot be
        tested for the exception, and stays a floor
    """
    nei = union[
        (union['source'] == 'NEI') & union['fuel_class'].isin(COMBUSTION_FUEL_CLASSES)
    ]
    per_facility = nei.groupby(['FacilityID', 'sector'])['CO2e'].sum().reset_index()
    big = per_facility['CO2e'] > GHGRP_THRESHOLD_KG

    out = pd.DataFrame(
        {
            'ghgrp_Mt': floor,
            'nei_below_Mt': per_facility[~big].groupby('sector')['CO2e'].sum() / 1e9,
            'nei_above_Mt': per_facility[big].groupby('sector')['CO2e'].sum() / 1e9,
        }
    ).fillna(0.0)
    out['total_Mt'] = out.sum(axis=1)
    out = out[out['total_Mt'] > 0]
    out['coverage'] = out['ghgrp_Mt'] / (out['ghgrp_Mt'] + out['nei_below_Mt'])
    out['unresolved'] = out['nei_above_Mt'] / out['total_Mt']
    # A sector too small to bear a ratio is not thereby excluded from the
    # facility data - it keeps the default, which is a floor. Only the
    # exception needs enough mass to be tested for.
    testable = out['total_Mt'] >= min_Mt
    out['verdict'] = np.where(
        testable
        & (out['coverage'] >= coverage_floor)
        & (out['unresolved'] <= unresolved_ceiling),
        'vector',
        'floor',
    )
    out['blocked_by'] = np.where(
        out['verdict'] == 'vector',
        '',
        np.where(
            ~testable,
            'under the reporting floor',
            np.where(
                out['coverage'] < coverage_floor,
                np.where(out['unresolved'] > unresolved_ceiling, 'both', 'coverage'),
                'unresolved',
            ),
        ),
    )
    if basis is not None:
        absent = basis[basis['basis'] == 'no facility data']
        absent = absent[
            absent['sector'].astype(str).str[:2].isin(FACILITY_SCOPE_PREFIXES)
        ]
        named = pd.DataFrame(
            {
                'ghgrp_Mt': 0.0,
                'nei_below_Mt': 0.0,
                'nei_above_Mt': 0.0,
                'total_Mt': 0.0,
                'coverage': np.nan,
                'unresolved': np.nan,
                'verdict': 'no facility data',
                'blocked_by': '',
            },
            index=pd.Index(absent['sector'], name='sector'),
        )
        out = pd.concat([out, named[~named.index.isin(out.index)]])
    out = _with_names(out.rename_axis('sector').reset_index())
    return out.sort_values('total_Mt', ascending=False).reset_index(drop=True)


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

    tables['vnorm_share_of_B_movement'] = vnorm_share_of_B_movement(span)
    logger.info(
        'Share of gross B movement that is the Make rather than E / x '
        '(quote the weighted column):\n%s',
        tables['vnorm_share_of_B_movement'][
            [
                'year_to',
                'vnorm_share_weighted',
                'vnorm_share_unweighted',
                'vnorm_dominant_commodities',
            ]
        ].to_string(index=False),
    )
    # The EF movement table: rank on abs_delta_B_pct_of_N, filter on B_from.
    tables['B_change_real'] = B_change(span, real=True)
    logger.info(
        'Largest EF movements in constant %d dollars, per year (preview of a '
        'complete CSV; filtered to commodities over 0.1 kg CO2e/$ so the '
        'ranking is not led by rounding-scale factors):\n%s',
        int(span.x.columns[0]),
        tables['B_change_real'][tables['B_change_real']['B_from'] > 1e-4]
        .groupby('year_to')
        .head(3)[
            [
                'year_to',
                'commodity',
                'name',
                'B_from',
                'own_direct_share_of_N',
                'abs_pct_change_B',
                'abs_delta_B_pct_of_N',
            ]
        ]
        .to_string(index=False),
    )
    tables['x'] = span.x
    tables['x_real'] = span.x_real

    return tables


# --- plots ------------------------------------------------------------------


def make_reallocation(span: Span, base_year: int | None = None) -> pd.Series:
    """Emissions the Make has shuffled between commodities since *base_year*, Mt.

    ⚠️ **``Vnorm`` cannot appear on an indexed-levels chart, because it nets to
    nothing in the aggregate.** The output-weighted total of ``B`` is total
    emissions by construction - measured, 5,023 Mt against 5,017 Mt in 2022,
    the 0.13% gap being the scrap correction - and freezing ``Vnorm`` at 2017
    moves that total by 0.1%. The Make redistributes emissions across
    commodities; it does not create or destroy them. A ``Vnorm`` level line
    would sit flat at 100 and say nothing, and ``q`` would not help: ``q`` and
    ``x`` are the same money counted along two axes and total to the dollar.

    What *is* visible is the redistribution itself::

        0.5 * sum_j | B_j(Vnorm_t) q_j  -  B_j(Vnorm_base) q_j |

    holding each year's own ``E / x`` and ``q`` fixed so only the Make moves.
    Zero in the base year by construction, and rising as the Make drifts away
    from it. Units are Mt CO2e: the mass of emissions sitting on a different
    commodity than the base-year Make would have put it on.
    """
    base = int(base_year if base_year is not None else list(span.x.columns)[0])
    base_Vnorm = span.Vnorm[base]
    out: dict[int, float] = {}
    for year in span.Vnorm:
        Vnorm = span.Vnorm[year]
        x = span.x[year].reindex(Vnorm.index).fillna(0.0)
        q = span.q[year].reindex(Vnorm.columns).fillna(0.0)
        E = (
            span.E[span.E['year'] == year]
            .groupby('sector')['CO2e']
            .sum()
            .reindex(Vnorm.index)
            .fillna(0.0)
        )
        intensity = (E / x).replace([np.inf, -np.inf], 0.0).fillna(0.0)
        actual = (intensity @ Vnorm) * q
        counterfactual = (intensity @ base_Vnorm) * q
        out[int(year)] = 0.5 * float((actual - counterfactual).abs().sum())
    return pd.Series(out).rename(f'reallocated_vs_{base}').rename_axis('year')


def plot_E_and_x_indexed(span: Span) -> None:
    """All three drivers of ``B`` on one axis pair: ``E``, ``x`` and the Make."""
    by_class = (
        span.E.groupby(['year', 'attribution_class'])['CO2e'].sum().unstack('year')
    )
    x_total = span.x.sum()
    x_real_total = span.x_real.sum()
    base = by_class.columns[0]
    shares = by_class[base] / by_class[base].sum()

    fig, ax = plt.subplots(figsize=(12, 7))
    # Total first, so the reader sees that the five coloured lines partition it
    # rather than being five unrelated quantities.
    total = by_class.sum()
    ax.plot(
        total.index,
        total / total.iloc[0] * 100,
        marker='D',
        color='tab:red',
        linewidth=2.5,
        label='E: total GHG inventory (all five below)',
    )
    for label, series in by_class.iterrows():
        if series[base] <= 0:
            continue
        share = float(shares.get(label, 0.0))
        # ⚠️ A class carrying a fraction of a percent must not be drawn as a
        # peer of one carrying half the inventory: io_gross_output is 1.2 Mt,
        # and its swings are a fifth of a megatonne rendered as the most
        # dramatic line on the chart.
        minor = share < 0.01
        ax.plot(
            series.index,
            series / series[base] * 100,
            marker='.' if minor else 'o',
            linewidth=1.0 if minor else 1.8,
            alpha=0.55 if minor else 1.0,
            label='{}  ({:.2g}% of E)'.format(
                ATTRIBUTION_CLASS_LABEL.get(str(label), str(label)), share * 100
            ),
        )
    ax.plot(
        x_total.index,
        x_total / x_total.iloc[0] * 100,
        marker='s',
        color='black',
        linewidth=2.5,
        label='x: gross output, nominal (dollars, not emissions)',
    )
    ax.plot(
        x_real_total.index,
        x_real_total / x_real_total.iloc[0] * 100,
        marker='s',
        color='dimgrey',
        linewidth=2.5,
        linestyle='--',
        label=f'x: gross output, constant {base} $ (dollars, not emissions)',
    )
    ax.axhline(100, color='grey', linewidth=0.8, linestyle=':')
    ax.set_title(
        f'The three drivers of B, indexed to {base} = 100\n'
        'E split by what spread it across sectors, in emissions;  '
        'x in dollars;  the Make as the mass it has reallocated'
    )
    ax.set_xlabel('year')
    ax.set_ylabel(f'index ({base} = 100)')

    # Vnorm on its own axis and in its own units: it nets to nothing in the
    # aggregate, so it has no level line to index. See make_reallocation.
    twin = ax.twinx()
    reallocated = make_reallocation(span, int(base)) / 1e9
    twin.fill_between(
        reallocated.index,
        0.0,
        reallocated.to_numpy(),
        color='tab:green',
        alpha=0.13,
        zorder=0,
    )
    twin.plot(
        reallocated.index,
        reallocated.to_numpy(),
        marker='^',
        color='tab:green',
        linewidth=2.0,
        linestyle='-.',
        label=f'Vnorm: emissions reallocated vs {base} (right axis)',
    )
    twin.set_ylabel(f'Mt CO2e on a different commodity than the {base} Make')
    twin.set_ylim(bottom=0)

    handles, labels = ax.get_legend_handles_labels()
    twin_handles, twin_labels = twin.get_legend_handles_labels()
    ax.legend(
        handles + twin_handles, labels + twin_labels, fontsize=8, loc='upper left'
    )
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


def sector_stratum_span(detail: pd.DataFrame) -> pd.DataFrame:
    """One row per (sector, inventory table, attribution) cell across the span.

    Adds the column the finest-grained table was missing: whether a cell
    **drifts** or **oscillates**::

        oscillation = 1 - |sum(divergence)| / sum(|divergence|)

    0 means every year's divergence pointed the same way - the cell is on a
    trend. 1 means the movements cancel exactly - the cell goes back and forth
    and ends where it started.

    ⚠️ **That distinction is the justified-versus-unjustified question in
    measurable form, and it does not fall out of magnitude alone.** A cell can
    be enormous and entirely legitimate: electric power's own direct emissions
    against `221100` carry the largest gross divergence on the span, 808 Mt,
    at an oscillation of 0.06 - emissions falling faster than output, every
    year, in the same direction. Smoothing that would erase decarbonisation.
    A cell of a sixteenth the size can be the better target: industrial
    petroleum into petroleum refineries oscillates at 0.92, moving 47.6 Mt in
    total and arriving 3.8 Mt from where it started.

    ⚠️ ``oscillation`` is unreliable where the total is small - two rounding
    movements that happen to cancel score 1.0. Read it next to ``total``.
    """
    # ⚠️ Net the cell out WITHIN each year before taking absolute values.
    # `detail` is keyed on the raw `AttributionSources`, and a vintage-suffixed
    # vector splits one cell-year across two rows - the MECS energy FBS carries
    # a year in its name, so the 2018 pair has an `..._2017` row holding the
    # `E_from` side and an `..._2018` row holding the `E_to` side, each with a
    # large divergence that cancels against its sibling. Summing |divergence|
    # over raw rows counts both halves of that cancellation as movement: it put
    # petroleum refineries' gas combustion at 740 Mt of gross movement against
    # an actual 21 Mt, a factor of 35.
    per_year = (
        detail.assign(
            cell=lambda d: d['sector']
            + ' | '
            + d['MetaSources']
            + ' -> '
            + d['attribution']
        )
        .groupby(['cell', 'sector', 'MetaSources', 'attribution', 'year_to'])[
            'divergence'
        ]
        .sum()
        .reset_index()
    )
    cells = (
        per_year.groupby(['cell', 'sector', 'MetaSources', 'attribution'])
        .agg(
            total=('divergence', lambda s: float(s.abs().sum())),
            net=('divergence', 'sum'),
            years=('year_to', 'nunique'),
        )
        .reset_index()
    )
    cells['oscillation'] = np.where(
        cells['total'] > 0, 1 - cells['net'].abs() / cells['total'], np.nan
    )
    cells['share_of_gross'] = cells['total'] / cells['total'].sum()
    return cells.sort_values('total', ascending=False).reset_index(drop=True)


def plot_sector_stratum_divergence(
    detail: pd.DataFrame,
    filename: str = 'sector_stratum_divergence.png',
    title_suffix: str = '',
    top: int = 25,
) -> pd.DataFrame:
    """Two views of the finest-grained divergence: drift-or-oscillate, and when.

    Left: every cell placed by how much it moves against whether the movement
    goes anywhere. The top-right corner - large and oscillating - is the
    smoothing project's target list; the bottom-right is large and trending,
    which is a real signal and should be left alone.

    Right: the *top* cells year by year, signed, so the pattern behind the
    oscillation score is visible rather than taken on trust.
    """
    cells = sector_stratum_span(detail)
    ranked = cells.head(top)

    fig, (left, right) = plt.subplots(
        1, 2, figsize=(17, 9), gridspec_kw={'width_ratios': [1.05, 1]}
    )

    # --- left: magnitude against drift-or-oscillate -------------------------
    x = cells['total'].to_numpy(dtype=float) / 1e9
    y = cells['oscillation'].to_numpy(dtype=float)
    left.scatter(x, y, s=12, alpha=0.35, color='tab:blue', edgecolors='none')
    left.scatter(
        ranked['total'] / 1e9,
        ranked['oscillation'],
        s=45,
        color='tab:red',
        edgecolors='black',
        linewidths=0.4,
        zorder=3,
        label=f'top {top} by gross movement',
    )
    # Label to the LEFT: the big cells sit against the right edge of a log
    # axis, so a right-hand label runs off the panel and into its neighbours.
    for _, row in cells.head(8).iterrows():
        left.annotate(
            _abbreviate_cell(row['sector'], row['MetaSources'], row['attribution']),
            (row['total'] / 1e9, row['oscillation']),
            textcoords='offset points',
            xytext=(-10, 0),
            ha='right',
            fontsize=6.5,
            va='center',
            color='black',
        )
    left.set_xscale('log')
    left.set_ylim(-0.03, 1.03)
    left.axhline(0.5, color='grey', linestyle=':', linewidth=0.8)
    left.set_xlabel('gross movement over the span, Mt CO2e (log)')
    left.set_ylabel('oscillation:  0 = one direction,  1 = cancels out')
    left.set_title(
        'Does the movement go anywhere?\n'
        'high and right = rocky with nothing underneath;  '
        'low and right = a real trend',
        fontsize=10,
    )
    left.legend(fontsize=7, loc='lower left')

    # --- right: the top cells, year by year, signed -------------------------
    signed = (
        detail.assign(
            cell=lambda d: d['sector']
            + ' | '
            + d['MetaSources']
            + ' -> '
            + d['attribution']
        )
        .pivot_table(
            index='cell', columns='year_to', values='divergence', aggfunc='sum'
        )
        .reindex(ranked['cell'])
        .fillna(0.0)
        / 1e9
    )
    # ⚠️ Row-normalise. On a shared colour scale the largest cell sets the
    # limits and every other row renders white, which hides the very thing the
    # panel exists to show - electric power alone swings 131 Mt in a year,
    # against single-digit Mt for most of the rest. Each row is therefore drawn
    # as a share of its own gross movement, and the magnitude it is a share
    # *of* is printed in the row label.
    gross = np.abs(signed.to_numpy()).sum(axis=1, keepdims=True)
    shares = np.divide(
        signed.to_numpy(), gross, out=np.zeros_like(signed.to_numpy()), where=gross > 0
    )
    image = right.imshow(shares, cmap='RdBu_r', vmin=-1, vmax=1, aspect='auto')
    right.set_xticks(range(len(signed.columns)))
    right.set_xticklabels([str(c) for c in signed.columns])
    right.set_yticks(range(len(signed)))
    right.set_yticklabels(
        [
            '{}  ({:,.0f} Mt)'.format(
                _abbreviate_cell(r['sector'], r['MetaSources'], r['attribution']),
                r['total'] / 1e9,
            )
            for _, r in ranked.iterrows()
        ],
        fontsize=6.5,
    )
    right.set_xlabel('year')
    right.set_title(
        f'The {top} largest cells, signed\n'
        'a row that alternates colour is oscillating; one colour is a trend',
        fontsize=10,
    )
    fig.colorbar(
        image,
        ax=right,
        label="share of that cell's own gross movement",
        shrink=0.7,
    )

    fig.suptitle(f'Divergence by sector and source{title_suffix}', fontsize=12, y=0.99)
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / filename, dpi=150, bbox_inches='tight')
    plt.close(fig)
    return cells


def _abbreviate_cell(sector: str, meta: str, attribution: str) -> str:
    """``221100 electric_power -> Direct``, short enough for an axis label."""
    name = _INDUSTRY_NAME.get(str(sector), '')
    return (
        f'{sector} {name[:18]} | '
        f'{abbreviate_source(meta)} → {abbreviate_source(attribution)}'
    )


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
    span.q.to_parquet(CACHE_DIR / 'q.parquet')
    for year, Vnorm in span.Vnorm.items():
        Vnorm.to_parquet(CACHE_DIR / f'Vnorm_{year}.parquet')
    for year, L in span.L.items():
        L.to_parquet(CACHE_DIR / f'L_{year}.parquet')
    (CACHE_DIR / 'vintages.txt').write_text(
        f'fbs={span.vintages.fbs}\nmut={span.vintages.mut}\n'
    )
    logger.info('Cached the span to %s', CACHE_DIR)


def load_span(years: tuple[int, ...] = YEARS) -> Span:
    """Reload a cached span.

    Raises rather than filling a gap when the cache predates a field: a span
    silently missing ``L`` would drop every N-weighted column without saying
    so, and the tables would look complete.
    """
    pinned = dict(
        line.split('=', 1)
        for line in (CACHE_DIR / 'vintages.txt').read_text().split()
        if '=' in line
    )

    def _read(name: str) -> pd.DataFrame:
        frame = pd.read_parquet(CACHE_DIR / name)
        frame.columns = pd.Index([int(c) for c in frame.columns], name='year')
        return frame.rename_axis(index='sector')

    missing = [y for y in years if not (CACHE_DIR / f'L_{y}.parquet').exists()]
    if not (CACHE_DIR / 'q.parquet').exists():
        missing = list(years)
    if missing:
        raise FileNotFoundError(
            f'The cached span is missing L or q for {missing} - it was written '
            f'before they were part of the span. Re-run without --use-cache to '
            f'rebuild it.'
        )

    return Span(
        E=pd.read_parquet(CACHE_DIR / 'E_stratified.parquet'),
        x=_read('x.parquet'),
        x_real=_read('x_real.parquet'),
        q=_read('q.parquet').rename_axis(index='commodity'),
        Vnorm={
            year: pd.read_parquet(CACHE_DIR / f'Vnorm_{year}.parquet') for year in years
        },
        L={year: pd.read_parquet(CACHE_DIR / f'L_{year}.parquet') for year in years},
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
    facility_data: bool = False,
) -> dict[str, pd.DataFrame]:
    """Build the span, decompose ``E`` against ``x``, write tables and plots.

    *facility_data* adds D14 and D15, off by default because they are the only
    diagnostics here that reach outside the repository: they download the GHGRP
    and NEI inventories through ``stewi``, which takes minutes on a cold cache
    and needs a network. Everything else runs from local artifacts.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if use_cache:
        span = load_span(years)
        logger.info(
            'Loaded cached span (fbs=%s, mut=%s)', span.vintages.fbs, span.vintages.mut
        )
        # A pin the cache cannot honour has to fail, not be ignored: the whole
        # point of passing one is to control which build the findings describe.
        asked = {'fbs': fbs_vintage, 'mut': mut_vintage}
        clashed = {
            family: (want, getattr(span.vintages, family))
            for family, want in asked.items()
            if want and want != getattr(span.vintages, family)
        }
        if clashed:
            raise ValueError(
                '--use-cache would ignore the vintage you pinned: '
                + '; '.join(
                    f'{family} pinned {want!r} but the cache holds {held!r}'
                    for family, (want, held) in clashed.items()
                )
                + '. Drop --use-cache to rebuild at the pinned vintage.'
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
    tables['sector_stratum_span'] = plot_sector_stratum_divergence(
        detail_real,
        'sector_stratum_divergence.png',
        f' (x in constant {int(span.x.columns[0])} $)',
    )
    plot_elasticity(tables['output_elasticity'])
    logger.info('Wrote plots to %s', OUTPUT_DIR)

    if facility_data:
        # GHGRP now reaches the end of the span, but keep the bound explicit so
        # that a span extended past it degrades rather than failing.
        floor_years = tuple(y for y in years if y <= GHGRP_LAST_YEAR)
        tables['ghgrp_subpart_C'] = ghgrp_subpart_C(floor_years)
        tables['ghgrp_subpart_W_combustion'] = ghgrp_subpart_w.subpart_W_combustion(
            floor_years
        )
        floor = ghgrp_combustion_floor(floor_years)
        tables['ghgrp_combustion_floor'] = floor
        tables['combustion_floor_test'] = combustion_floor_test(detail_real, floor)

        # D16 scores the sector's WHOLE assignment against its facilities.
        tables['ghgrp_facility_floor'] = ghgrp_facility_floor(floor_years)
        tables['under_attributed_sectors'] = under_attributed_sectors(
            detail_real, tables['ghgrp_facility_floor']
        )
        under = tables['under_attributed_sectors']
        if not under.empty:
            by_fix = under.groupby('fix')['gap_Mt_last'].agg(['size', 'sum'])
            logger.info(
                'D16: %d in-scope sectors report more to the GHGRP than the '
                'inventory assigns them, %.0f Mt in %d. Only the restate half '
                "is #929's to close; the rest is relocation (#953):" + chr(10) + '%s',
                len(under),
                under['gap_Mt_last'].sum(),
                int(floor_years[-1]),
                by_fix.round(1).to_string(),
            )

        # D15 needs NEI as well as GHGRP, and NEI trails it by a year.
        basis_year = min(max(floor_years), NEI_LAST_YEAR)
        # #926 evidence: SCC branch / ungated fuel_class / facility counts by
        # year. Union counts need a full D15 per year, so they are opt-in via
        # the dedicated summary when diagnosing the seam.
        tables['nei_scc_reclassification'] = nei_scc_reclassification_summary(
            tuple(y for y in years if y <= NEI_LAST_YEAR),
            include_union=False,
        )
        facility = facility_combustion(basis_year)
        tables['facility_combustion'] = facility
        tables['facility_basis'] = facility_basis_comparison(
            span, facility, detail_real
        )
        # Subpart C alone, deliberately: D15b's halves and the §2 tables that
        # quote them are on that reading. See its docstring for what the
        # complete combustion floor would move, and #927.
        tables['facility_scope_split'] = facility_scope_split(
            tables['facility_basis'], facility, tables['ghgrp_subpart_C'][basis_year]
        )
        derived = facility[facility['fuel_class'] == 'self_supplied']
        logger.info(
            'D15 %d: %.1f Mt over %d facilities in %d jurisdictions, of which %.1f '
            'Mt (%.1f%%) is fuel the facility never bought and no Use row can '
            'carry.',
            basis_year,
            facility['CO2e'].sum() / 1e9,
            facility['FacilityID'].nunique(),
            facility['State'].nunique(),
            derived['CO2e'].sum() / 1e9,
            derived['CO2e'].sum() / facility['CO2e'].sum() * 100,
        )
        tables['facility_overshoot'] = facility_overshoot_guard(
            tables['facility_basis']
        )
        _report_overshoot(tables['facility_overshoot'], basis_year)
        tables['facility_coverage'] = facility_coverage_bands(
            facility, floor[basis_year], tables['facility_basis']
        )
        _report_coverage(tables['facility_coverage'], basis_year)
        logger.info(
            'D15: table 3-11 by the basis it could rest on:\n%s',
            tables['facility_basis']
            .groupby('basis')
            .agg(
                sectors=('sector', 'size'),
                allocated_Mt=('allocated_Mt', 'sum'),
                residual_Mt=('residual_Mt', 'sum'),
            )
            .round(1)
            .to_string(),
        )
        scope = tables['facility_scope_split'].query('in_scope')
        breach = scope[scope['breaches_floor'] & (scope['table_3_11_Mt'] > 0)]
        logger.info(
            'D15b %d: the two halves of the D15 ratio, in scope. Combustion, '
            'table 3-11 %.1f Mt vs subpart C %.1f Mt (%.2f). Process, Direct '
            '%.1f Mt vs every other subpart %.1f Mt (%.2f) - but that aggregate '
            'is offsetting errors, and per sector the process half runs %.2f to '
            '%.2f, so the union is no replacement for Direct (#953). On the '
            'like-for-like half %d sectors allocate less combustion than their '
            'own facilities reported: %.0f Mt against %.0f Mt.',
            basis_year,
            scope['table_3_11_Mt'].sum(),
            scope['ghgrp_C_Mt'].sum(),
            scope['ghgrp_C_Mt'].sum() / scope['table_3_11_Mt'].sum(),
            scope['direct_Mt'].sum(),
            scope['ghgrp_other_Mt'].sum(),
            scope['ghgrp_other_Mt'].sum() / scope['direct_Mt'].sum(),
            scope.loc[scope['direct_Mt'] > 1.0, 'other_vs_direct'].min(),
            scope.loc[scope['direct_Mt'] > 1.0, 'other_vs_direct'].max(),
            len(breach),
            breach['table_3_11_Mt'].sum(),
            breach['ghgrp_C_Mt'].sum(),
        )
        intermittent = tables['combustion_floor_test'].query(
            'verdict == "intermittent"'
        )
        logger.info(
            'D14: sectors that clear the GHGRP combustion floor in some years '
            'and breach it in others, worst first. A sector below it in EVERY '
            'year is a boundary difference, not a defect:\n%s',
            intermittent[
                [
                    'sector',
                    'name',
                    'years_below_floor',
                    'min_ratio',
                    'max_ratio',
                    'ratio_spread',
                ]
            ].to_string(index=False),
        )

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
    parser.add_argument(
        '--facility-data',
        action='store_true',
        help=(
            'also run D14 (the GHGRP subpart C floor test) and D15 (the '
            'facility-reported combustion basis). Downloads GHGRP and NEI '
            'through stewi, so it needs a network and takes minutes on a cold '
            'cache'
        ),
    )
    parser.add_argument(
        '--check-facility-overshoot',
        action='store_true',
        help=(
            'run D15 alone and exit non-zero if any sector exceeds its '
            'inventory total with no cause on record in '
            'NAMED_BOUNDARY_DIFFERENCES (#925). Implies --facility-data and '
            'reuses the cached span'
        ),
    )
    args = parser.parse_args()

    span_years = tuple(args.years)
    if args.check_facility_overshoot:
        span = load_span(span_years)
        basis_year = min(min(max(span_years), GHGRP_LAST_YEAR), NEI_LAST_YEAR)
        guard = facility_overshoot_guard(
            facility_basis_comparison(span, facility_combustion(basis_year))
        )
        raise SystemExit(1 if _report_overshoot(guard, basis_year) else 0)
    if args.list_vintages:
        for stem in (FBS_STEM, MUT_STEM):
            print(f'\n{stem.format(year="<year>")}')
            table = local_vintages(stem, span_years)
            for col in ('mtime', 'committed'):
                table[col] = pd.to_datetime(
                    table[col].replace(0, np.nan), unit='s'
                ).dt.strftime('%Y-%m-%d %H:%M')
            print(table.fillna('-').to_string(index=False))
    else:
        main(
            years=span_years,
            use_cache=args.use_cache,
            fbs_vintage=args.fbs_vintage,
            mut_vintage=args.mut_vintage,
            facility_data=args.facility_data,
        )
