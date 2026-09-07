"""Physical electricity trade for Trade FBS (#668).

Dollarizes EIA Electric Power Annual Table 2.14 national MWh with same-year
Census HS 2716 unit values. Quantity is EIA; price is Census merchandise
(``bedrock.extract.census.Census_USATrade`` unit-value CSV / refresh).
"""

from __future__ import annotations

import functools
from typing import Any, Literal

import pandas as pd

from bedrock.extract.census.Census_USATrade import (
    HS2716_ELECTRICAL_ENERGY,
    hs2716_unit_value_usd_per_mwh,
)
from bedrock.extract.disaggregation.egrid_generation import (
    eia_table_2_14_export_mwh,
    eia_table_2_14_import_mwh,
)
from bedrock.extract.flowbyactivity import FlowByActivity
from bedrock.utils.mapping.location import US_FIPS

TradeDirection = Literal['exports', 'imports']

#: Crosswalk Activities for national dollarized Table 2.14 rows.
ELECTRICITY_EXPORTS_ACTIVITY = 'Electricity exports'
ELECTRICITY_IMPORTS_ACTIVITY = 'Electricity imports'

_EXPORT_FLOW = 'electricity exports'
_IMPORT_FLOW = 'electricity imports'


def electricity_trade_mwh(year: int, direction: TradeDirection) -> float:
    """National Canada+Mexico EIA Table 2.14 MWh for *year* and *direction*."""
    if direction == 'exports':
        return eia_table_2_14_export_mwh(int(year))
    if direction == 'imports':
        return eia_table_2_14_import_mwh(int(year))
    raise ValueError(f'direction must be exports or imports, got {direction!r}')


def electricity_trade_usd(year: int, direction: TradeDirection) -> float:
    """EIA Table 2.14 MWh × same-year Census HS 2716 unit value, USD."""
    return electricity_trade_mwh(year, direction) * hs2716_unit_value_usd_per_mwh(
        year, direction
    )


def dollarize_electricity_trade_fba(fba: FlowByActivity, **_: Any) -> FlowByActivity:
    """Collapse Table 2.14 CA/MX MWh to national USD rows for Trade FBS.

    ``clean_fba_before_activity_sets`` on ``EIA_ElectricPowerAnnual``: replaces
    Canada/Mexico Table 2.14 import and export rows with one national USD row
    per direction (Activities ``Electricity exports`` / ``Electricity imports``).
    Other tables in the FBA are dropped so activity-set selection only sees
    trade dollars.
    """
    if fba.empty:
        return fba
    year = int(fba['Year'].iloc[0]) if 'Year' in fba.columns else None
    if year is None and 'year' in fba.config:
        year = int(fba.config['year'])
    if year is None:
        raise ValueError('Cannot dollarize electricity trade without Year')

    template = fba.iloc[[0]].copy()
    wanted_flows: set[str] = set()
    for aset in (fba.config.get('activity_sets') or {}).values():
        flow = (aset.get('selection_fields') or {}).get('FlowName')
        if isinstance(flow, str):
            wanted_flows.add(flow)

    rows: list[pd.DataFrame] = []
    for flow_name, direction, produced, consumed in (
        (_EXPORT_FLOW, 'exports', None, ELECTRICITY_EXPORTS_ACTIVITY),
        (_IMPORT_FLOW, 'imports', ELECTRICITY_IMPORTS_ACTIVITY, None),
    ):
        if wanted_flows and flow_name not in wanted_flows:
            continue
        mask = (fba['FlowName'].astype(str) == flow_name) & fba['Description'].astype(
            str
        ).str.contains('Table 2.14', na=False)
        if not mask.any():
            continue
        usd = electricity_trade_usd(year, direction)  # type: ignore[arg-type]
        row = template.copy()
        row['FlowName'] = flow_name
        row['FlowAmount'] = usd
        row['Unit'] = 'USD'
        row['Class'] = 'Money'
        row['Location'] = US_FIPS
        row['ActivityProducedBy'] = produced
        row['ActivityConsumedBy'] = consumed
        row['Description'] = (
            f'Table 2.14 national {direction}: EIA MWh × Census HS '
            f'{HS2716_ELECTRICAL_ENERGY} unit value'
        )
        rows.append(row)

    if not rows:
        raise ValueError(
            f'{fba.full_name}: no Table 2.14 electricity trade rows to dollarize'
        )
    out = pd.concat(rows, ignore_index=True)
    return FlowByActivity(
        out,
        full_name=fba.full_name,
        config=fba.config,
        convert_df_to_flowby=True,
    )


#: Census vehicle children, and the parent they are relabelled onto (#702).
VEHICLE_CHILDREN = ('336111', '336112')
VEHICLE_PARENT = '336110'

#: Census aerospace leaves, and the suppressed-detail residual they are
#: relabelled onto on the **export** side only (#701).
AEROSPACE_CHILDREN = ('336411', '336412', '336413', '336414', '336415', '336419')
AEROSPACE_PARENT = '33641X'

#: The Census goods crosswalk, read at runtime so the family parents the clean
#: function relabels onto and the parents the crosswalk splits can never
#: disagree.  Written by ``bedrock.utils.mapping.write_census_family_parents``.
CENSUS_CROSSWALK_CSV = (
    'bedrock/utils/mapping/activitytosectormapping/'
    'Sector_Crosswalk_Census_USATrade.csv'
)

#: The ``Note`` value that marks a generated family-parent row (#763).
FAMILY_PARENT_NOTE = 'FAMILY_PARENT'


@functools.cache
def census_family_parents() -> dict[str, str]:
    """Census activity -> its four-digit family parent, for the import re-split.

    [#763](https://github.com/cornerstone-data/bedrock/issues/763) validated
    the construction on the 2012 holdout and closed without wiring it: each
    family's within-split anchored on the published 2017 mix, moved by the
    Census family level.  The import method already splits **1:m** activities
    by frozen 2017 Supply ``MCIF``, which is that construction exactly - but
    almost every family is mapped **1:1 or m:1**, so no weight is ever applied
    and Census's own within-family split stands.  Relabelling a family's
    activities onto one parent turns the family into a single 1:m row.

    Only activities whose crosswalk targets all sit in **one** family are
    relabelled.  An activity straddling two families would move mass between
    them, which is a level change; this construction may only move mass within
    a family.
    """
    frame = pd.read_csv(CENSUS_CROSSWALK_CSV, dtype=str).fillna('')
    frame = frame[frame['SectorSourceName'] == 'BEA_2017_Code']
    parents = set(frame.loc[frame['Note'] == FAMILY_PARENT_NOTE, 'Activity'])
    leaves = frame[frame['Note'] != FAMILY_PARENT_NOTE]
    targets = leaves.groupby('Activity')['Sector'].apply(lambda s: sorted(set(s)))
    out: dict[str, str] = {}
    for activity, sectors in targets.items():
        families = {str(sector)[:4] for sector in sectors}
        if len(families) == 1:
            family = families.pop()
            if family in parents:
                out[str(activity)] = family
    return out


def consolidate_activities(
    frame: pd.DataFrame, children: tuple[str, ...], parent: str
) -> pd.DataFrame:
    """Relabel a set of Census activities onto one parent so 1:m splits them.

    Deliberately a *relabel*, not an aggregation: rows keep their own
    ``FlowName`` and ``Description``, so every flow the caller selects is
    consolidated the same way and the attribution step does the summing.
    """
    if frame.empty or 'ActivityProducedBy' not in frame.columns:
        return frame
    activities = frame['ActivityProducedBy'].astype(str)
    if not activities.isin(children).any():
        return frame
    out = frame.copy()
    out['ActivityProducedBy'] = activities.where(~activities.isin(children), parent)
    return out


def consolidate_vehicle_activities(frame: pd.DataFrame) -> pd.DataFrame:
    """Relabel Census ``336111`` / ``336112`` onto the parent ``336110``.

    ⚠️ **Census's own child split is not BEA's** and taking it directly is what
    put $111B on the wrong commodity (#702, #670). Census classifies imports by
    HS code mapped to NAICS, which lands light trucks and SUVs under passenger
    vehicles; BEA reallocates them to ``336112`` on a product basis. Published
    2017 detail ``MCIF`` is 66,068 / 128,742 against Census's 177,108 / 18,481
    - the *pair total* agrees to 0.4%, so the disagreement is entirely about
    the split.

    ⚠️ **Census changes axis at 2023**, publishing only the parent ``336110``
    from then on. So 2017-2022 rode Census's split and 2023-2024 rode the
    crosswalk's 1:m family, which put an **$80B discontinuity** between 2022
    and 2023 into two commodities that carry both trade and transport margins.
    Relabelling every year onto the parent removes the discontinuity by letting
    one rule - the 1:m family - decide the split in all years.

    Deliberately a *relabel*, not an aggregation: rows keep their own
    ``FlowName`` and ``Description``, so every flow the caller selects
    (``GEN_CIF_YR``, ``ALL_VAL_YR_DOM``, ``ALL_VAL_YR_FGN``, ``GEN_VAL_YR``, ``CAL_DUT_YR``) is
    consolidated the same way, and the attribution step does the summing.
    """
    return consolidate_activities(frame, VEHICLE_CHILDREN, VEHICLE_PARENT)


def _as_fba(fba: FlowByActivity, frame: pd.DataFrame) -> FlowByActivity:
    """Rebuild a cleaned frame as a :class:`FlowByActivity`.

    ⚠️ Rebuilt rather than mutated in place so the frame keeps its ``config`` -
    a bare DataFrame returned from a clean hook loses it and fails downstream
    on ``KeyError: 'year'``.
    """
    return FlowByActivity(
        frame,
        full_name=fba.full_name,
        config=fba.config,
        convert_df_to_flowby=True,
    )


def consolidate_vehicle_activities_fba(fba: FlowByActivity, **_: Any) -> FlowByActivity:
    """:func:`consolidate_vehicle_activities` as a ``clean_fba`` hook.

    The **imports** hook. Exports use
    :func:`consolidate_export_activities_fba`, which also consolidates
    aerospace.
    """
    return _as_fba(fba, consolidate_vehicle_activities(pd.DataFrame(fba)))


def consolidate_import_activities(frame: pd.DataFrame) -> pd.DataFrame:
    """Relabel every Census activity onto its family parent (#763).

    Supersedes the vehicle-only relabel on the import side: ``336111`` /
    ``336112`` are simply one family among 62, and ``336110`` - which is all
    Census publishes from 2023 - folds to the same parent, so the 2022/2023
    axis change stops mattering for every family rather than just for vehicles.
    """
    parents = census_family_parents()
    if frame.empty or 'ActivityProducedBy' not in frame.columns:
        return frame
    activities = frame['ActivityProducedBy'].astype(str)
    mapped = activities.map(parents)
    if not mapped.notna().any():
        return frame
    out = frame.copy()
    out['ActivityProducedBy'] = mapped.fillna(activities)
    return out


def consolidate_import_activities_fba(fba: FlowByActivity, **_: Any) -> FlowByActivity:
    """:func:`consolidate_import_activities` as a ``clean_fba`` hook.

    ⚠️ ``duties.map_census_import_flow_to_detail`` must apply the *same*
    consolidation.  The duty rate is a ratio of two mapped Census flows and it
    multiplies this FBS's own mass, so if one side folds families and the other
    does not, the rate is built on one commodity axis and applied to another.
    """
    return _as_fba(fba, consolidate_import_activities(pd.DataFrame(fba)))


def consolidate_export_activities_fba(fba: FlowByActivity, **_: Any) -> FlowByActivity:
    """Vehicles **and** aerospace onto their parents, for the export side (#701).

    Vehicles are consolidated on both sides for the #702 reasons. Aerospace is
    consolidated on **exports only**, because the two sides of the Census
    extract do not have the same defect:

    - **Exports.** Census publishes a suppressed-detail residual ``33641X``
      carrying 107,365 million USD of 2017 aerospace exports and assigns only
      12,828 - **10.7%** - to a named leaf. The mix of the part it does assign
      is **39.6%** different from the published export mix: ``336413`` takes
      41% of the assigned dollars against a 19% published export share, and
      the two space leaves take 21% against 2.6%. Trusting those rows and
      splitting only the residual leaves that bias in place, which is what put
      ``336413`` at 1.18x published. Relabelling the whole family onto the
      residual lets one rule - the 1:m frozen export mix - decide all of it,
      exactly as #702 did for vehicles.
    - **Imports.** There is no ``33641X`` on the import side at all. Census
      publishes every aerospace leaf directly, so there is no residual to
      split and no reason to overwrite an observation. What is wrong there is
      the c.i.f.-versus-``MCIF`` *level*, which is #670's, not a split.
    """
    frame = consolidate_vehicle_activities(pd.DataFrame(fba))
    return _as_fba(
        fba, consolidate_activities(frame, AEROSPACE_CHILDREN, AEROSPACE_PARENT)
    )


if __name__ == '__main__':
    import pandas as pd

    from bedrock.extract.census.Census_USATrade import (
        refresh_hs2716_electricity_unit_value_csv,
    )

    written = refresh_hs2716_electricity_unit_value_csv()
    print(f'Wrote {written}')
    print(pd.read_csv(written).to_string(index=False))

#: The fitted 2017 bridge (#771): which IEA categories move each export row,
#: written by ``bedrock.utils.mapping.write_iea_export_bridge``.
IEA_EXPORT_BRIDGE_CSV = 'bedrock/analysis/nowcasting/trade_data/iea_export_bridge.csv'
IEA_IMPORT_BRIDGE_CSV = 'bedrock/analysis/nowcasting/trade_data/iea_import_bridge.csv'

#: The IEA extract for the anchor year, used for the growth denominators.
IEA_EXPORTS_2017_CSV = (
    'bedrock/extract/input_data/BEA_IEA/2017/BEA_IEA_2017_Exports.csv'
)

IEA_IMPORTS_2017_CSV = (
    'bedrock/extract/input_data/BEA_IEA/2017/BEA_IEA_2017_Imports.csv'
)

#: The activity-to-sector crosswalk for IEA imports; `_bridge_iea_services`
#: reads the S00300 rows off it so the noncomparable pass-through and the
#: FBS attribution can never disagree about which leaves are noncomparable.
IEA_IMPORTS_CROSSWALK_CSV = (
    'bedrock/utils/mapping/activitytosectormapping/'
    'Sector_Crosswalk_BEA_IEA_imports.csv'
)


def bridge_iea_service_imports(fba: FlowByActivity, **_: Any) -> FlowByActivity:
    """The imports mirror of :func:`bridge_iea_service_exports` (#771).

    Same anchor-and-move: each service commodity's imports are its published
    2017 Supply-table imports value times a growth blend of the IEA import
    categories the fitted bridge feeds it from.  The prior construction
    already weighted within-set by published imports, but the sets carried
    the same defects as the export side - the repair category forced onto
    rows BEA barely uses, orphaned rows omitted outright - measured at 32.8%
    gross on the services imports column.

    ⚠️ Noncomparable imports (``S00300``) do NOT go through the bridge - the
    bridge never emits S-coded rows.  The sixteen IEA leaves the crosswalk
    routes to ``S00300`` (port charges, travel-other, construction abroad,
    the non-software IP licenses, ...) pass through as one direct row
    instead: their plain sum **is** the #766 construction, 261,261 against
    260,421 published at 2017.  Every one of them maps to ``S00300`` alone,
    so the sum equals what the pre-bridge proportional attribution produced.
    Dropping them - which the first version of this mirror did - zeroes the
    entire supply of the largest T11 residual row (369,911m of 2023 use
    against no supply at all).
    """
    return _bridge_iea_services(
        fba,
        flow='Imports',
        bridge_csv=IEA_IMPORT_BRIDGE_CSV,
        base_csv=IEA_IMPORTS_2017_CSV,
        anchor_table='Supply_detail',
        anchor_column='MCIF',
        noncomparable_csv=IEA_IMPORTS_CROSSWALK_CSV,
    )


def bridge_iea_service_exports(fba: FlowByActivity, **_: Any) -> FlowByActivity:
    """Anchor-and-move the service exports onto BEA commodities (#771).

    ``clean_fba_before_activity_sets`` on ``BEA_IEA``: replaces the
    service-category export rows with one row per BEA commodity whose amount is
    the commodity's **published 2017 exports times a growth index** — the index
    a share-weighted blend of the IEA categories the fitted bridge says feed
    that row, each category's growth taken against its own 2017 total.

    Why not attribute categories onto commodities directly: ITA's service
    definitions are not BEA's commodity bridge.  Fitted jointly at 2017, the
    category totals and the published rows disagree by ~254bn gross —
    splitting each category across a commodity set fabricated exports wherever
    the set was wrong (electronics repair carried 815x its published exports).
    Anchoring each row on its published 2017 value makes 2017 exact by
    construction and confines the category-definition mismatch to the growth
    weights.

    ⚠️ The rest-of-world adjustment row (``S00900``, where BEA books most
    traveler spending) and the used/scrap rows are deliberately absent — the
    first is re-derived after the balance, the second two have no IEA source.
    """
    return _bridge_iea_services(
        fba,
        flow='Exports',
        bridge_csv=IEA_EXPORT_BRIDGE_CSV,
        base_csv=IEA_EXPORTS_2017_CSV,
        anchor_table='Use_SUT_detail',
        anchor_column='F04000',
    )


def _bridge_iea_services(
    fba: FlowByActivity,
    flow: str,
    bridge_csv: str,
    base_csv: str,
    anchor_table: str,
    anchor_column: str,
    noncomparable_csv: str | None = None,
) -> FlowByActivity:
    import numpy as np  # noqa: PLC0415

    if fba.empty:
        return fba

    bridge = pd.read_csv(bridge_csv, dtype={'commodity': str})
    base_raw = pd.read_csv(base_csv)
    base = (
        base_raw[
            (base_raw['TradeDirection'] == flow)
            & (base_raw['Affiliation'] == 'AllAffiliations')
            & (base_raw['AreaOrCountry'] == 'AllCountries')
        ]
        .set_index('TypeOfService')['DataValue']
        .astype(float)
    )

    frame = pd.DataFrame(fba)
    exports = frame[frame['FlowName'].astype(str) == flow]
    now = (
        exports.groupby(exports['ActivityProducedBy'].astype(str))['FlowAmount']
        .sum()
        .astype(float)
    )
    # FBA amounts are USD; the 2017 extract csv is $M — growth is a ratio, so
    # only consistency within each side matters.
    growth = (now / 1e6) / base.reindex(now.index)
    growth = growth.replace([np.inf, -np.inf], np.nan)

    from bedrock.extract.iot.io_2017 import (  # noqa: PLC0415
        _load_2017_detail_supply_use_usa,
    )

    anchor = _load_2017_detail_supply_use_usa(anchor_table)  # type: ignore[arg-type]
    anchor.columns = anchor.columns.str.strip()
    published = pd.to_numeric(anchor[anchor_column], errors='coerce').fillna(0.0)

    bridge['g'] = bridge['category'].map(growth)
    # a category unpublished in year t contributes its 2017 weight at growth 1
    # (absence is not a collapse) — same rule the SAS panel applies.
    bridge['g'] = bridge['g'].fillna(1.0)
    index = bridge.groupby('commodity').apply(
        lambda t: float((t['share'] * t['g']).sum() / t['share'].sum())
    )
    amounts = published.reindex(index.index).fillna(0.0) * index * 1e6  # USD

    # The categories the crosswalk routes to S00300 bypass the bridge: each
    # maps to S00300 alone, so their plain sum reproduces the pre-bridge
    # proportional attribution exactly (#766).
    if noncomparable_csv is not None:
        crosswalk = pd.read_csv(noncomparable_csv, dtype=str)
        noncomparable = sorted(
            set(crosswalk.loc[crosswalk['Sector'] == 'S00300', 'Activity'])
        )
        amounts['S00300'] = float(now.reindex(noncomparable).fillna(0.0).sum())

    template = fba.iloc[[0]].copy()
    rows = []
    for commodity, amount in amounts.items():
        if amount <= 0:
            continue
        row = template.copy()
        row['FlowName'] = flow
        row['FlowAmount'] = float(amount)
        row['Unit'] = 'USD'
        row['Class'] = 'Money'
        row['Location'] = US_FIPS
        row['ActivityProducedBy'] = commodity
        row['ActivityConsumedBy'] = None
        row['Description'] = (
            'sum of the IEA leaves crosswalked to S00300 (#766)'
            if commodity == 'S00300'
            else f'published 2017 {anchor_column} x IEA category growth blend '
            f'(#771 bridge, {flow})'
        )
        rows.append(row)
    out = pd.concat(rows, ignore_index=True)
    return FlowByActivity(
        out, full_name=fba.full_name, config=fba.config, convert_df_to_flowby=True
    )
