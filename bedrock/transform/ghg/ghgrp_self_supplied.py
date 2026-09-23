"""Fuel that never changed hands, by facility — the carve-out candidates.

A row of the Use table records a **purchase**. Fuel a facility made itself and
burned on site was never bought from anyone, so no Use row can carry it, and
attributing it with one is a category error rather than an inaccuracy (#927).
Three such fuels are large enough to matter, and each comes from a different
part of the GHGRP:

``lease and plant fuel``
    an oil and gas operation burning its own stream.
    :func:`bedrock.transform.ghg.ghgrp_subpart_w.lease_and_plant_fuel`, #980.

``still gas``
    a refinery burning the gas its own processing produces. The GHGRP calls it
    ``Fuel Gas`` and reports it under **subpart C**, so it is combustion on both
    sides. #983.

``catalyst coke``
    coke laid down on a cracking catalyst and burned off in the regenerator.
    The GHGRP reports it under **subpart Y**, a *process* subpart, while the GHG
    inventory books it in table 3-11 as the fuel ``Petroleum Coke``. #984.

⚠️ **What this module emits is evidence, not a settled level.** Two of the three
have open level questions that must be resolved before a carve-out is applied,
and both are recorded on the function that produces them rather than left to the
reader:

- GHGRP ``Fuel Gas`` **exceeds** the inventory's whole still gas line, because
  it also covers chemical-plant fuel gas that the inventory books elsewhere. The
  level has to come from the inventory; only the *recipient* comes from here.
- subpart Y is an **upper bound** on catalyst coke, since it also carries flares,
  reforming and coking units.

Use ``basis`` to tell them apart; it names the table each row came from.
"""

from __future__ import annotations

import pandas as pd

from bedrock.extract.epa.EPA_GHGRP_tables import (
    C_FUEL_TABLE,
    Y_SUBPART_TABLE,
    ghgrp_table,
)
from bedrock.utils.logging.flowsa_log import log

#: The subpart C fuel label for refinery still gas. The reporter picks it from
#: a list, so it is exact rather than a pattern - and matching loosely is a trap
#: worth naming: ``Di-still-ate Fuel Oil`` contains "still".
STILL_GAS_FUEL = 'Fuel Gas'

#: CO2 columns in the subpart C fuel table. Tier 4 is absent by design: those
#: units report CO2 from CEMS, outside this view. It is 1.9% of subpart C.
C_FUEL_CO2_COLUMNS = (
    'TIER1_CO2_COMBUSTION_EMISSIONS',
    'TIER2_CO2_COMBUSTION_EMISSIONS',
    'TIER3_CO2_COMBUSTION_EMISSIONS',
)


def _facility_id(column: pd.Series) -> pd.Series:
    """``1004957.0`` and ``1004957`` onto the one key ``stewi`` uses."""
    return column.astype(str).str.split('.').str[0]


def still_gas(years: tuple[int, ...]) -> pd.DataFrame:
    """Refinery still gas by facility and year, kg CO2 (#983).

    Subpart C reports it as the fuel ``Fuel Gas``, so it is combustion on the
    GHGRP side and combustion on the inventory side - no boundary correction is
    needed, unlike catalyst coke.

    ⚠️ **This is the recipient, not the level.** In 2018 GHGRP ``Fuel Gas``
    totals 120.9 Mt while the inventory's whole still gas line is 100.4 Mt, and
    refineries alone report 99.1 Mt of it - 98.7% of the line. The excess is
    chemical-plant fuel gas, which the inventory books somewhere else. So
    spreading the inventory line across *these* shares would push refineries
    **below their own reported total**. Take the level from the inventory and
    the recipient from here.
    """
    frames = []
    for year in years:
        raw = ghgrp_table(C_FUEL_TABLE, year)
        if raw is None:
            continue
        for column in C_FUEL_CO2_COLUMNS:
            raw[column] = pd.to_numeric(raw[column], errors='coerce')
        raw['CO2'] = raw[list(C_FUEL_CO2_COLUMNS)].sum(axis=1, min_count=1)
        fuel = raw['FUEL_TYPE'].fillna('').str.strip()
        rows = raw[fuel.str.casefold() == STILL_GAS_FUEL.casefold()]
        frames.append(
            pd.DataFrame(
                {
                    'FacilityID': _facility_id(rows['FACILITY_ID']),
                    'year': year,
                    # Subpart C reports metric tons; stewi carries kilograms.
                    'CO2e': rows['CO2'] * 1e3,
                }
            )
        )
    return _finish(frames, 'still gas', 'GHGRP subpart C fuel type')


def catalyst_coke(years: tuple[int, ...]) -> pd.DataFrame:
    """Refinery process CO2 by facility and year, kg (#984).

    Subpart Y, whose dominant source is catalytic cracking catalyst
    regeneration: coke laid down on the catalyst and burned off in place, which
    nobody bought.

    ⚠️ **An upper bound on catalyst coke, not a measurement of it.** Subpart Y
    also covers flares, catalytic reforming, coking units, asphalt blowing and
    sulfur recovery, and this view has no source-category breakout. Two things
    bound the error: subpart Y sits *below* the inventory's industrial petroleum
    coke line in both comparable years (53.4 against 59.5 in 2018), and refinery
    flaring is small in the inventory - ``324110`` receives 3.55 Mt from the
    petroleum systems tables. Separating it properly needs a finer Envirofacts
    view.

    ⚠️ **It is also on the wrong side of the per-half test.** The inventory
    books this as *combustion*, in table 3-11 under the fuel ``Petroleum Coke``,
    while the GHGRP books it under a *process* subpart. Any comparison that
    scores subpart C against table 3-11 and every other subpart against
    ``Direct`` counts it twice wrongly - understating the combustion half and
    overstating the process half by the same amount.
    """
    frames = []
    for year in years:
        raw = ghgrp_table(Y_SUBPART_TABLE, year)
        if raw is None:
            continue
        raw['q'] = pd.to_numeric(raw['GHG_QUANTITY'], errors='coerce')
        # "Biogenic Carbon dioxide" is a separate row and is left out for the
        # reason the GHGRP flow map drops biogenic CO2 elsewhere.
        rows = raw[
            raw['GHG_NAME'].fillna('').str.fullmatch('Carbon Dioxide', case=False)
        ]
        frames.append(
            pd.DataFrame(
                {
                    'FacilityID': _facility_id(rows['FACILITY_ID']),
                    'year': year,
                    'CO2e': rows['q'] * 1e3,
                }
            )
        )
    return _finish(frames, 'catalyst coke', 'GHGRP subpart Y total (upper bound)')


def _finish(frames: list[pd.DataFrame], fuel: str, basis: str) -> pd.DataFrame:
    """Aggregate to facility-year, label the provenance, and report the span."""
    columns = ['FacilityID', 'year', 'fuel', 'basis', 'CO2e']
    if not frames:
        return pd.DataFrame(columns=columns)
    out = pd.concat(frames, ignore_index=True)
    out = out[out['CO2e'] > 0]
    if out.empty:
        return pd.DataFrame(columns=columns)
    out = (
        out.groupby(['FacilityID', 'year'], as_index=False)['CO2e']
        .sum()
        .assign(fuel=fuel, basis=basis)
    )
    log.info(
        '%s by year, Mt CO2e: %s',
        fuel,
        (out.groupby('year')['CO2e'].sum() / 1e9).round(1).to_dict(),
    )
    return out[columns]
