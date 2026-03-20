from __future__ import annotations

import logging

import pandas as pd

from bedrock.transform.flowbysector import FlowBySector, getFlowBySector
from bedrock.utils.config.common import load_crosswalk
from bedrock.utils.emissions.ghg import GHG_MAPPING
from bedrock.utils.emissions.gwp import GWP100_AR6_CEDA
from bedrock.utils.mapping.sectormapping import (
    get_activitytosector_mapping,
)
from bedrock.utils.taxonomy.cornerstone.industries import INDUSTRIES

logger = logging.getLogger(__name__)


def derive_E_usa() -> pd.DataFrame:
    return load_E_from_flowsa()


def map_to_CEDA(fbs: pd.DataFrame) -> pd.DataFrame:
    """Map FBS sectors from NAICS to CEDA v7 sectors."""
    # Because the schema for the FBS is mixed digit, first need to expand the schema all the way
    # to 6 digits prior to mapping back to the CEDA schema. In doing this mapping we only need
    # to assign a 1:1 mapping (hence drop duplicates, keep = first). When the mapping is reversed
    # back to CEDA we don't want to expand the FBS.

    # Prepare NAICS:BEA mapping file
    cw = load_crosswalk('NAICS_2017_Crosswalk')
    cols_to_stack = ["NAICS_3", "NAICS_4", "NAICS_5"]
    cw_stack = (
        cw.astype({c: "string" for c in cols_to_stack + ["NAICS_6"]})
        .melt(
            id_vars="NAICS_6",
            value_vars=cols_to_stack,
            var_name="level",
            value_name="NAICS",
        )
        .dropna(subset=["NAICS_6", "NAICS"])[["NAICS", "NAICS_6"]]
        .drop_duplicates(subset='NAICS', keep='first')
        .reset_index(drop=True)
    )
    fbs2 = fbs.merge(
        cw_stack,
        how='left',
        left_on='SectorProducedBy',
        right_on='NAICS',
        validate="m:1",
    )
    fbs2['NAICS_6'] = fbs2['NAICS_6'].fillna(fbs2['SectorProducedBy'])

    mapping = get_activitytosector_mapping('Cornerstone_2025').drop_duplicates(
        subset='Sector', keep='first'
    )
    fbs2 = (
        fbs2.merge(
            mapping[['Activity', 'Sector']],
            how='left',
            left_on='NAICS_6',
            right_on=['Sector'],
            validate="m:1",
        )
        .assign(SectorProducedBy=lambda x: x['Activity'].fillna(x['NAICS_6']))
        .drop(columns=['Activity', 'NAICS', 'NAICS_6', 'Sector'])
    )

    ## re assign SPB and aggregate using exisiting functions
    fbs3 = pd.DataFrame(FlowBySector(fbs2).aggregate_flowby())

    # TODO: add test to confirm no data loss

    return fbs3


def load_E_from_flowsa() -> pd.DataFrame:
    """Load E_usa (GHG × sectors) via the Cornerstone FlowBySector method."""
    fbs = getFlowBySector(methodname='GHG_national_Cornerstone_2023')

    fbs = map_to_CEDA(fbs)

    # Align flow names with temporary mapping
    gas_map = {
        # CO2
        'Carbon dioxide': 'CO2',
        # CH4
        'Methane': 'CH4_fossil',
        # N2O
        'Nitrous oxide': 'N2O',
        # NF3
        'Nitrogen trifluoride': 'NF3',
        # SF6
        'Sulfur hexafluoride': 'SF6',
        # HFCs (all beginning with HFC- or explicitly HFC)
        'HFC, PFC and SF6 F-HTFs': 'HFCs',  # mixed basket → assign to HFCs?
        # 'HFC-125': 'HFCs',
        # 'HFC-134a': 'HFCs',
        # 'HFC-143a': 'HFCs',
        # 'HFC-227ea': 'HFCs',
        # 'HFC-23': 'HFCs',
        # 'HFC-236fa': 'HFCs',
        # 'HFC-32': 'HFCs',
        'HFCs and PFCs, unspecified': 'HFCs',  # ambiguous → can also map to 'PFCs'
        # PFCs
        'Carbon tetrafluoride': 'CF4',
        'Hexafluoroethane': 'C2F6',
        'PFC': 'PFCs',
        'Perfluorocyclobutane': 'c-C4F8',
        'Perfluoropropane': 'C3F8',
    }
    fbs['Flowable'] = fbs['Flowable'].map(gas_map).fillna(fbs['Flowable'])

    # CH4: use CH4_non_fossil when meta source is table 5_* or when in 2_1 and sector starts with 1 or 562 or 2213
    # to align with CH4_NON_FOSSIL defined in extract/allocation/epa.py
    meta = fbs['MetaSources'].astype(str)
    sector = fbs['SectorProducedBy'].astype(str)
    ch4_non_fossil_mask = meta.str.contains('_5_', regex=False, na=False) | (
        meta.str.contains('2_1', regex=False, na=False)
        & sector.str.match(r'^(1|562|2213)', na=False)
    )
    fbs.loc[ch4_non_fossil_mask & (fbs['Flowable'] == 'CH4_fossil'), 'Flowable'] = (
        'CH4_non_fossil'
    )

    # Convert values to CO2e
    ghg_mapping: dict[str, float] = {k: v for k, v in GWP100_AR6_CEDA.items()}
    ghg_mapping['HFCs'] = 1  # should already be in CO2e
    ghg_mapping['PFCs'] = 1  # should already be in CO2e
    fbs['CO2e'] = fbs['FlowAmount'] * fbs['Flowable'].map(ghg_mapping)

    # fbs.to_csv('GHG_CEDA_fbs_bea.csv')

    # aggregate and set FlowName as index, sectors as columns
    E_usa = fbs.pivot_table(
        index='Flowable',
        columns='SectorProducedBy',
        values='CO2e',
        aggfunc='sum',
        fill_value=0,
    )

    # Collapse across flows
    reverse = {m: g for g, members in GHG_MAPPING.items() for m in members}
    # some flows are not in GHG_MAPPING for some reason
    reverse['HFC-227ea'] = 'HFCs'
    reverse['c-C4F8'] = 'PFCs'
    reverse['CH4_fossil'] = 'CH4'
    reverse['CH4_non_fossil'] = 'CH4'
    new_index = E_usa.index.map(lambda x: reverse.get(x, x))
    E_usa = E_usa.groupby(new_index).agg('sum')

    # Reindex to Cornerstone schema
    target_columns = [str(sector) for sector in INDUSTRIES]
    E_usa = E_usa.reindex(columns=target_columns, fill_value=0)

    return E_usa


