"""Which GHGRP combustion was fuel the facility **bought**, and which was not.

`#927 <https://github.com/cornerstone-data/bedrock/issues/927>`_. Combustion of
fuel a facility bought can be spread by a row of the Use table, because a
purchase is what the Use table records. Combustion of fuel that **never changed
hands** cannot: no Use row can carry a purchase that was never made, and
attributing it with one is a category error rather than an inaccuracy.

That class is wider than byproduct gas. Refinery still gas, coke oven gas and
blast furnace gas are the familiar half, and an SCC identifies them. The other
half is an oil and gas operation burning its own stream - **lease fuel** at the
well pad and **plant fuel** at the gas processing plant - and there an SCC is no
help at all, because field gas is natural gas and the SCC says natural gas.

Two routes reach it here, and the difference between them is kept in the output
rather than flattened:

``GHGRP subpart W fuel``
    onshore production, gathering and boosting and natural gas distribution
    report combustion under subpart W, where the reporter **names the fuel** from
    a list that separates field and process gas from pipeline-quality natural
    gas. Nothing is inferred.

``GHGRP segment``
    a gas processing plant reports combustion under subpart C, where the label is
    a Table C-1 default. What the facility *is* settles it instead: a plant burns
    the stream it is processing. An **inference**, and marked as one.

The raw views come from :mod:`bedrock.extract.epa.EPA_GHGRP_SubpartW`; this
module does the classification, and nothing here reaches the network.
"""

from __future__ import annotations

import re

import numpy as np
import pandas as pd

from bedrock.extract.epa.EPA_GHGRP_SubpartW import (
    W_COMBUSTION_VIEW,
    W_FACILITY_VIEW,
    facility_id,
    ghgrp_view,
)
from bedrock.utils.emissions.gwp import GWP100_AR6_CEDA
from bedrock.utils.logging.flowsa_log import log

#: The two fuel classes a combustion record can fall in, and why the split is
#: not cosmetic.
#:
#: ``self_supplied`` is wider than byproduct gas: it is any fuel that never
#: changed hands, so it covers refinery still gas, coke oven gas and blast
#: furnace gas **and** the lease and plant fuel an oil and gas operation burns
#: out of its own stream. All of it has one defect in common - no row of the Use
#: table can carry a purchase that was never made - which is why it is one class
#: rather than two.
FUEL_CLASSES = ('purchased', 'self_supplied')

#: Subpart W fuel labels naming gas the facility did not buy: field gas, process
#: gas, and any gas that is **not of pipeline quality** - off-spec gas cannot be
#: taken delivery of from a pipeline, so it came out of the ground on site. This
#: is the reporter's own word for the fuel, not an inference from the sector,
#: which is what makes subpart W able to answer #927 where subpart C cannot.
SELF_SUPPLIED_FUEL = re.compile(
    r'field gas|process gas|process vent gas|not (?:of )?pipeline quality',
    re.IGNORECASE,
)

#: Biogenic fuels, dropped for the reason the GHGRP flow map drops biogenic CO2:
#: the GHG inventory books it separately, so counting it here would put a
#: fossil-plus-biogenic number next to a fossil allocation. 0.1 Mt over the span,
#: all of it landfill gas and biodiesel at gathering sites.
BIOGENIC_FUEL = re.compile(r'^biomass fuels', re.IGNORECASE)

#: Plausible t CO2 per thousand standard cubic feet of gas burned. Pipeline
#: natural gas is 0.0544 and field gas sits near it, so a row outside this band
#: is a **unit-entry error**, not a different fuel: a few reporters enter scf
#: where the form asks for Mscf, and one 2023 basin row reads 23,533 Bcf against
#: 1.69 Mt of CO2. The emissions columns are unaffected, so the screen applies to
#: volumes only.
GAS_FACTOR_BAND = (0.02, 0.15)

#: The subpart W industry segments whose fuel is self-supplied by the nature of
#: the operation, even though they report combustion under subpart C where the
#: fuel label says pipeline natural gas.
#:
#: A gas processing plant burns the stream it is processing - that is what EIA
#: calls **plant fuel** - and 234.7 of the 236.1 Mt of CO2 those plants reported
#: over 2019-2024 is labelled ``Natural Gas (Weighted U.S. Average)``, the
#: default emission factor, against 1.15 Mt labelled ``Fuel Gas``. The label is a
#: factor choice, not a procurement statement, so the classification has to come
#: from **what the facility is** rather than from what it wrote.
#:
#: ⚠️ This one is an **inference**, unlike the lease-fuel side, and it is
#: labelled as such in ``fuel_class_basis``. Transmission compression is
#: deliberately absent: a compressor station burns gas out of the pipeline it is
#: moving, which is a purchase somewhere in the chain, and EIA books it as
#: pipeline fuel rather than as lease or plant fuel.
SELF_SUPPLYING_SEGMENTS = ('Onshore natural gas processing',)

#: Columns :func:`subpart_W_combustion` returns, so an empty span still has a
#: shape its consumers can work against.
COMBUSTION_COLUMNS = (
    'FacilityID',
    'year',
    'segment',
    'fuel_type',
    'fuel_class',
    'Mscf',
    'CO2e',
)


def _segment(column: pd.Series) -> pd.Series:
    """``Onshore ... production [98.230(a)(2)]`` -> ``Onshore ... production``."""
    return column.str.split(' [', regex=False).str[0]


def subpart_W_combustion(years: tuple[int, ...]) -> pd.DataFrame:
    """Subpart W combustion by facility, fuel and industry segment.

    Onshore production, gathering and boosting, and natural gas distribution
    report their stationary combustion **under subpart W**, not under subpart C:
    of 2,801 onshore production facility-years, 4 appear in the subpart C fuel
    tables, and of 2,157 gathering and boosting facility-years, none do. So
    anything built by filtering ``Process == 'C'`` does not contain one tonne of
    their fuel.

    ⚠️ **The fuel label carries the purchased/self-supplied split directly.**
    Subpart W asks which fuel was burned from a list that names ``Field gas
    and/or process gas`` and ``... natural gas that is not of pipeline quality``
    separately from ``Natural gas (pipeline quality)``. That is the distinction
    #927 needs, written by the reporter rather than inferred - and it is not
    available anywhere in subpart C, whose list is the Table C-1 emission
    factors.

    The unit rows reconcile to the facility totals in ``ef_w_combust_equip_summ``
    exactly, in every segment and year, so **every tonne of subpart W combustion
    carries a fuel type**; nothing sits in an unlabelled residual.

    Returns one row per facility, year, segment and fuel, with ``CO2e`` in
    kilograms to match the ``stewi`` convention, and ``Mscf`` carrying the
    reported volume where it survives :data:`GAS_FACTOR_BAND`.
    """
    gwp = {str(k): float(v) for k, v in GWP100_AR6_CEDA.items()}
    frames = []
    for year in years:
        raw = ghgrp_view(W_COMBUSTION_VIEW, year)
        if raw is None:
            continue
        frame = pd.DataFrame(
            {
                'FacilityID': facility_id(raw['FACILITY_ID']),
                'year': year,
                'segment': _segment(raw['INDUSTRY_SEGMENT']),
                'fuel_type': raw['FUEL_COMBUSTED_TYPE'].fillna(''),
                'unit': raw['UNIT_OF_MEASURE'],
                'quantity': pd.to_numeric(
                    raw['QUANTITY_OF_FUEL_COMBUSTED'], errors='coerce'
                ),
                'CO2': pd.to_numeric(raw['CO2_EMISSIONS'], errors='coerce'),
                'CH4': pd.to_numeric(raw['CH4_EMISSIONS'], errors='coerce'),
                'N2O': pd.to_numeric(raw['N2O_EMISSIONS'], errors='coerce'),
            }
        )
        biogenic = frame['fuel_type'].str.contains(BIOGENIC_FUEL)
        if biogenic.any():
            log.info(
                'GHGRP %d subpart W: dropping %.3f Mt on %d biogenic fuel rows, '
                'which the GHG inventory books outside the fossil total.',
                year,
                frame.loc[biogenic, 'CO2'].sum() / 1e6,
                int(biogenic.sum()),
            )
        frames.append(frame[~biogenic])

    if not frames:
        return pd.DataFrame(columns=list(COMBUSTION_COLUMNS))

    burned = pd.concat(frames, ignore_index=True)
    burned = burned[burned['FacilityID'].notna()]
    # Subpart W reports metric tons; stewi carries flows in kilograms, and these
    # rows are assembled next to stewi ones.
    burned['CO2e'] = 1e3 * (
        burned['CO2'].fillna(0)
        + burned['CH4'].fillna(0) * gwp['CH4_fossil']
        + burned['N2O'].fillna(0) * gwp['N2O']
    )
    burned['fuel_class'] = np.where(
        burned['fuel_type'].str.contains(SELF_SUPPLIED_FUEL),
        'self_supplied',
        'purchased',
    )

    # Only a row carrying both a volume and emissions can be checked at all.
    measurable = (
        (burned['unit'] == 'thousand standard cubic feet')
        & burned['quantity'].gt(0)
        & burned['CO2'].gt(0)
    )
    implied = (burned['CO2'] / burned['quantity']).where(measurable)
    low, high = GAS_FACTOR_BAND
    credible = measurable & implied.between(low, high)
    burned['Mscf'] = burned['quantity'].where(credible)
    rejected = measurable & ~credible
    if rejected.any():
        log.info(
            'GHGRP subpart W: %d of %d gas rows (%.1f%%) imply a factor outside '
            '%.2f-%.2f t CO2 per Mscf and are entered in the wrong unit. Their '
            'volumes are dropped, their emissions kept.',
            int(rejected.sum()),
            int(measurable.sum()),
            100 * rejected.sum() / max(int(measurable.sum()), 1),
            low,
            high,
        )
    return burned[list(COMBUSTION_COLUMNS)]


def self_supplying_facilities(year: int) -> set[str]:
    """Facilities whose subpart C fuel is self-supplied by what they are.

    :data:`SELF_SUPPLYING_SEGMENTS` says which segments and why. Returns an empty
    set when the year cannot be reached, so the classification degrades to "not
    known" rather than to a wrong answer.
    """
    raw = ghgrp_view(W_FACILITY_VIEW, year)
    if raw is None:
        return set()
    segment = _segment(raw['INDUSTRY_SEGMENT'])
    named = facility_id(raw.loc[segment.isin(SELF_SUPPLYING_SEGMENTS), 'FACILITY_ID'])
    return set(named.dropna())


def fuel_class(year: int, total: pd.Series, subpart_c: pd.Series) -> pd.DataFrame:
    """GHGRP facilities whose fuel class comes from their own report.

    *total* is each facility's whole GHGRP mass for the year and *subpart_c* the
    stationary-combustion part of it, both indexed by ``FacilityID`` and on
    whatever scale the caller is working in.

    The two routes this module's docstring describes produce the two halves:

    - a **subpart W** reporter is split on its own fuel labels, and whatever is
      left of its GHGRP mass after its reported combustion is venting, flaring
      and leaks - not fuel at all - so it lands in ``process``;
    - a **processing plant** has only its subpart C mass classed as fuel, for
      the same reason: its subpart W mass is fugitives.

    ⚠️ **The second route is an inference and the first is not.** Both are better
    evidence than a process-gas share carried over from a matched NEI record, but
    only the first is the reporter's own statement, so ``fuel_class_basis``
    carries which one produced each row.
    """
    burned = subpart_W_combustion((year,))
    told = (
        burned.groupby(['FacilityID', 'fuel_class'])['CO2e']
        .sum()
        .unstack('fuel_class')
        .reindex(columns=list(FUEL_CLASSES))
        .fillna(0.0)
    )
    rows = []
    if not told.empty:
        fuel = told.sum(axis=1)
        # Venting, flaring and leaks: everything the facility reported that its
        # combustion units did not burn. Clipped because the subpart W views can
        # be a later vintage than the inventory the totals come from, so a
        # restated facility can exceed its own total by a rounding.
        rest = (total.reindex(told.index).fillna(0.0) - fuel).clip(lower=0.0)
        told = told.assign(process=rest)
        rows.append(
            told.reset_index()
            .melt(id_vars='FacilityID', var_name='fuel_class', value_name='CO2e')
            .assign(fuel_class_basis='GHGRP subpart W fuel')
        )

    plants = self_supplying_facilities(year) - set(told.index)
    if plants:
        plant_fuel = subpart_c.reindex(sorted(plants)).dropna()
        rest = (total.reindex(plant_fuel.index).fillna(0.0) - plant_fuel).clip(
            lower=0.0
        )
        rows.append(
            pd.concat(
                [
                    plant_fuel.rename('CO2e')
                    .to_frame()
                    .assign(fuel_class='self_supplied'),
                    rest.rename('CO2e').to_frame().assign(fuel_class='process'),
                ]
            )
            .reset_index()
            .assign(fuel_class_basis='GHGRP segment')
        )

    if not rows:
        return pd.DataFrame(
            columns=['FacilityID', 'fuel_class', 'CO2e', 'fuel_class_basis']
        )
    classified = pd.concat(rows, ignore_index=True)
    log.info(
        'GHGRP %d: %d facilities classify their own fuel - %.1f Mt self-supplied, '
        '%.1f Mt purchased, %.1f Mt not fuel at all. By route: %s',
        year,
        classified['FacilityID'].nunique(),
        classified.loc[classified['fuel_class'] == 'self_supplied', 'CO2e'].sum() / 1e9,
        classified.loc[classified['fuel_class'] == 'purchased', 'CO2e'].sum() / 1e9,
        classified.loc[classified['fuel_class'] == 'process', 'CO2e'].sum() / 1e9,
        (classified.groupby('fuel_class_basis')['CO2e'].sum() / 1e9).round(1).to_dict(),
    )
    return classified[['FacilityID', 'fuel_class', 'CO2e', 'fuel_class_basis']]
