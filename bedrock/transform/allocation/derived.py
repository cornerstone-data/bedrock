from __future__ import annotations

import logging
import re
import time

import pandas as pd

from bedrock.extract.allocation.epa_constants import return_emissions_source_table_numbers
from bedrock.transform.allocation.constants import EmissionsSource
from bedrock.transform.allocation.registry import ALLOCATED_EMISSIONS_REGISTRY
from bedrock.transform.flowbysector import getFlowBySector
from bedrock.utils.config.settings import FBS_DIR
from bedrock.utils.emissions.ghg import GHG_MAPPING
from bedrock.utils.emissions.gwp import GWP100_AR6_CEDA
from bedrock.utils.mapping.sectormapping import map_to_BEA_sectors
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from bedrock.utils.taxonomy.correspondence import create_correspondence_matrix
from bedrock.utils.taxonomy.mappings.bea_v2017_industry__bea_v2017_commodity import (
    load_bea_v2017_industry_to_bea_v2017_commodity,
)

logger = logging.getLogger(__name__)


def derive_E_usa() -> pd.DataFrame:
    # aggregate E from 15 gases to 7 gases
    return create_correspondence_matrix(GHG_MAPPING).T @ derive_E_usa_by_gas()


def derive_E_usa_by_gas() -> pd.DataFrame:
    return (
        derive_E_usa_emissions_sources()
        .groupby(lambda es: EmissionsSource(es).gas, axis=0)  # type: ignore
        .sum()
    )


def derive_E_usa_emissions_sources() -> pd.DataFrame:
    E_usa = pd.DataFrame(
        0.0,
        index=[es.value for es in EmissionsSource],
        # NOTE: CEDA_V7_SECTORS is used as industry index here, E_usa will be divided by `g` already having CEDA_V7_SECTORS
        columns=CEDA_V7_SECTORS,
    )

    total_start = time.time()
    for es, allocator in ALLOCATED_EMISSIONS_REGISTRY.items():
        logger.info(f"Allocating {es}")
        allocated = allocator()
        if allocated.isna().any():
            raise ValueError(f"NaNs found in {es} allocator")
        E_usa.loc[es.value, :] += allocated

    logger.info(
        f"[TIMING] All {len(ALLOCATED_EMISSIONS_REGISTRY)} allocations completed in {time.time() - total_start:.1f}s"
    )

    return E_usa


# %%
def load_E_from_flowsa() -> pd.DataFrame:

    fbs = getFlowBySector(methodname='GHG_national_2022_m1')
    fbs = fbs.assign(Sector=fbs['SectorProducedBy']).drop(
        columns=['SectorProducedBy', 'SectorConsumedBy']
    )

    # map from NAICS to BEA schema
    fbs = map_to_BEA_sectors(
        fbs, region='national', io_level='detail', output_year=2022, bea_year=2017
    )

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

    # Convert values to CO2e
    ghg_mapping: dict[str, float] = {k: v for k, v in GWP100_AR6_CEDA.items()}
    ghg_mapping['CH4'] = GWP100_AR6_CEDA['CH4_fossil']
    ghg_mapping['HFCs'] = 1  # should already be in CO2e
    ghg_mapping['PFCs'] = 1  # should already be in CO2e
    fbs['CO2e'] = fbs['FlowAmount'] * fbs['Flowable'].map(ghg_mapping)

    # aggregate and set FlowName as index, sectors as columns
    E_usa = fbs.pivot_table(
        index='Flowable',
        columns='Sector',
        values='CO2e',
        aggfunc='sum',
        fill_value=0,
    )

    # Collapse across flows
    reverse = {m: g for g, members in GHG_MAPPING.items() for m in members}
    # some flows are not in GHG_MAPPING for some reason
    reverse['CH4_fossil'] = 'CH4'
    reverse['HFC-227ea'] = 'HFCs'
    reverse['c-C4F8'] = 'PFCs'
    new_index = E_usa.index.map(lambda x: reverse.get(x, x))
    E_usa = E_usa.groupby(new_index).agg('sum')

    # Collapse across sectors
    mapping = load_bea_v2017_industry_to_bea_v2017_commodity()
    E_usa = E_usa.groupby({k: v[0] for k, v in mapping.items()}, axis=1).sum()
    # ^^ drops F01000

    # Target column set is CEDA_V7_SECTORS
    # set(E_usa.columns) - set(CEDA_V7_SECTORS)
    # {'33131B', '335220'}

    # set(CEDA_V7_SECTORS) - set(E_usa.columns)
    # {'335221', '335222', '335224', '335228', '4200ID', '814000'}

    return E_usa


def load_E_from_flowsa_long(fbs_methodname: str) -> pd.DataFrame:
    """
    Load an FBS dataframe and modify to align with CEDA.
    Originally written with GHG FBS in mind, currently defaults to SectorProductedBy column
    """

    fbs = getFlowBySector(methodname=fbs_methodname)
    fbs = fbs.assign(Sector=fbs['SectorProducedBy']).drop(
        columns=['SectorProducedBy', 'SectorConsumedBy']
    )

    # map from NAICS to BEA schema
    fbs = map_to_BEA_sectors(
        fbs, region='national', io_level='detail', output_year=2022, bea_year=2017
    )

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

    # Convert values to CO2e
    ghg_mapping: dict[str, float] = {k: v for k, v in GWP100_AR6_CEDA.items()}
    ghg_mapping['CH4'] = GWP100_AR6_CEDA['CH4_fossil']
    ghg_mapping['HFCs'] = 1  # should already be in CO2e
    ghg_mapping['PFCs'] = 1  # should already be in CO2e
    fbs['FlowAmount'] = fbs['FlowAmount'] * fbs['Flowable'].map(ghg_mapping)

    # subset df and aggregate
    fbs2 = fbs[['Flowable', 'Sector', 'FlowAmount', 'MetaSources']]
    fbs2.loc[:, 'MetaSources'] = fbs2['MetaSources'].str.split('.').str[0]
    # some flows are not in GHG_MAPPING for some reason
    fbs2.loc[:, 'Flowable'] = fbs2['Flowable'].replace(
        {'CH4_fossil': 'CH4', 'HFC-227ea': 'HFCs', 'c-C4F8': 'PFCs'}
    )
    # Aggregate FlowAmount by the three other columns
    fbs3 = fbs2.groupby(['Flowable', 'Sector', 'MetaSources'], as_index=False)[
        'FlowAmount'
    ].sum()

    # Target column set is CEDA_V7_SECTORS
    # set(E_usa.columns) - set(CEDA_V7_SECTORS)
    # {'33131B', '335220'}

    # set(CEDA_V7_SECTORS) - set(E_usa.columns)
    # {'335221', '335222', '335224', '335228', '4200ID', '814000'}

    return fbs3


def derive_E_usa_long() -> pd.DataFrame:
    """
    Return a long-form DataFrame of ghg emissions data allocated to BEA sectors, maintains emission source as
    column info
    """

    df = derive_E_usa_emissions_sources()

    # melt
    dfm = df.stack().reset_index()
    dfm.columns = ["emissions_source", "BEA", "FlowAmount"]

    # add Flowable
    dfm["Flowable"] = dfm["emissions_source"].map(lambda es: EmissionsSource(es).gas)

    # return GHGI table numbers
    lookup = return_emissions_source_table_numbers()

    pattern = "(" + "|".join(map(re.escape, lookup["emissions_source"])) + ")$"
    dfm["lookup_key"] = dfm["emissions_source"].str.extract(pattern)

    # merge
    dfm = dfm.merge(
        lookup.rename(columns={"emissions_source": "lookup_key"}),
        on="lookup_key",
        how="left",
    )

    # split out table numbers, reorder, drop 0 values
    table_number_cols = [col for col in dfm.columns if col.startswith("table_number_")]
    dfm = dfm[["Flowable", "BEA", "emissions_source", "FlowAmount"] + table_number_cols]
    dfm = dfm[dfm["FlowAmount"] != 0].reset_index(drop=True)

    # save to FlowBySector output directory
    dfm.to_parquet(FBS_DIR / "E_usa.parquet", index=False)

    return dfm


if __name__ == "__main__":
    df1 = load_E_from_flowsa()
    df2 = derive_E_usa()
    row_diff = df1.sum(axis=1) - df2.sum(axis=1)
    row_rel_diff = df1.sum(axis=1) / df2.sum(axis=1)
