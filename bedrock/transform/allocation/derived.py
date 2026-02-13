from __future__ import annotations

import logging
import time

import pandas as pd

from bedrock.transform.allocation.constants import EmissionsSource
from bedrock.transform.allocation.registry import ALLOCATED_EMISSIONS_REGISTRY
from bedrock.transform.flowbysector import getFlowBySector
from bedrock.utils.config.settings import FBS_DIR
from bedrock.utils.emissions.ghg import GHG_MAPPING
from bedrock.utils.emissions.gwp import GWP100_AR6_CEDA
from bedrock.utils.mapping.sectormapping import map_to_BEA_sectors
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from bedrock.utils.taxonomy.correspondence import create_correspondence_matrix

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
        'Methane': 'CH4',
        # N2O
        'Nitrous oxide': 'N2O',
        # NF3
        'Nitrogen trifluoride': 'NF3',
        # SF6
        'Sulfur hexafluoride': 'SF6',
        # HFCs (all beginning with HFC- or explicitly HFC)
        'HFC, PFC and SF6 F-HTFs': 'HFCs',  # mixed basket → assign to HFCs?
        'HFC-125': 'HFCs',
        'HFC-134a': 'HFCs',
        'HFC-143a': 'HFCs',
        'HFC-227ea': 'HFCs',
        'HFC-23': 'HFCs',
        'HFC-236fa': 'HFCs',
        'HFC-32': 'HFCs',
        'HFCs and PFCs, unspecified': 'HFCs',  # ambiguous → can also map to 'PFCs'
        # PFCs
        'Carbon tetrafluoride': 'PFCs',
        'Hexafluoroethane': 'PFCs',
        'PFC': 'PFCs',
        'Perfluorocyclobutane': 'PFCs',
        'Perfluoropropane': 'PFCs',
    }
    fbs['Flowable'] = fbs['Flowable'].map(gas_map)

    # aggregate and set FlowName as index, sectors as columns
    E_usa = fbs.pivot_table(
        index='Flowable',
        columns='Sector',
        values='FlowAmount',
        aggfunc='sum',
        fill_value=0,
    )

    # FlowName: 'CO2', 'CH4', 'N2O', 'HFCs', 'PFCs', 'SF6', 'NF3'
    # Sectors: '1111A0', '1111B0', '111200', '111300', '111400', '111900', '112120',...
    #    '813B00', '814000', 'S00500', 'S00600', '491000', 'S00102', 'GSLGE',
    #    'GSLGH', 'GSLGO', 'S00203'

    E_usa = E_usa.reindex(index=list(dict.fromkeys(GHG_MAPPING.keys())), fill_value=0)
    E_usa = E_usa.reindex(columns=list(dict.fromkeys(CEDA_V7_SECTORS)), fill_value=0)
    # ^^ this may drop some sectors from the flowsa dataset

    # Convert values to CO2e
    ghg_mapping: dict[str, float] = {
        k: v for k, v in GWP100_AR6_CEDA.items() if k in E_usa.index
    }
    ghg_mapping['CH4'] = GWP100_AR6_CEDA['CH4_fossil']
    ghg_mapping['HFCs'] = 1
    ghg_mapping['PFCs'] = 1
    # ^^ not accurate, would need to perform CO2e conversions on original flows

    E_usa = E_usa.mul(E_usa.index.map(ghg_mapping), axis=0)

    return E_usa


def derive_E_usa_long() -> pd.DataFrame:
    """
    Return a long-form DataFrame of ghg emissions data allocated to BEA sectors
    """

    df = derive_E_usa_emissions_sources()

    # melt
    dfm = df.stack().reset_index()
    dfm.columns = ["emissions_source", "BEA", "FlowAmount"]

    # add column of the ghg emission type
    dfm["Flowable"] = dfm["emissions_source"].map(lambda es: EmissionsSource(es).gas)  # type: ignore

    # clean up df
    dfm = dfm[["Flowable", "BEA", "emissions_source", "FlowAmount"]]
    dfm = dfm[dfm["FlowAmount"] != 0].reset_index(drop=True)

    # save to FlowBySector output directory
    dfm.to_parquet(FBS_DIR / "E_usa.parquet", index=False)

    return dfm


if __name__ == "__main__":
    df1 = load_E_from_flowsa()
    df2 = derive_E_usa()
    row_diff = df1.sum(axis=1) - df2.sum(axis=1)
    row_rel_diff = df1.sum(axis=1) / df2.sum(axis=1)
