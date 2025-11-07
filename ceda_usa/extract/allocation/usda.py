"""
USDA YEAR = 2022
"""

from __future__ import annotations

import os
import typing as ta

import pandas as pd

from ceda_usa.utils.gcp import GCS_CEDA_INPUT_DIR, load_from_gcs
from ceda_usa.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTOR

IN_DIR = os.path.join(os.path.dirname(__file__), "..", "input_data")


def load_crop_land_area_harvested() -> pd.Series[float]:
    # harvested = pd.concat(
    #     [
    #         _load_crop_land_area_harvested_2022(),
    #         _load_fruits_treenuts_area_bearing_2022(),
    #         _load_vegetables_melons_area_harvested_2022(),
    #     ]
    # )
    # the load functions above don't provide reasonable area operated of livestock
    # so we choose to use 2018 data, unit is acres
    harvested = pd.Series(
        [86552100, 151633000, 5252810, 4770430, 102512817],
        index=["1111A0", "1111B0", "111200", "111300", "111900"],
    ).astype(float)
    assert harvested.index.is_unique
    return harvested


def load_animal_operation_land() -> pd.Series[float]:  # acres
    # _load_livestock_area_operated_2022 doesn't provide reasonable area operated of livestock
    # so we choose to use 2018 data, unit is acres
    animal_operation_land = pd.Series(
        [17398455, 376699018, 5916544, 78659536],
        index=["112120", "1121A0", "112300", "112A00"],
    ).astype(float)

    assert animal_operation_land.index.is_unique
    return animal_operation_land


def _load_crop_land_area_harvested_2022() -> pd.Series[float]:
    """
    download USDA Census of Agriculture 2022 data from quickstats.nass.usda.gov
        Select Commodity:
            Program: CENSUS
            Sector: CROPS
            Group: FIELD CROPS
            Commodity: (all)
            Category: AREA HARVESTED
            Data Item: (all)
            Domain: AREA HARVESTED
        Select Location:
            Geographic Level: NATIONAL
            State: US TOTAL
        Select Time:
            Year: 2022
            Period Type: ANNUAL
            Period: YEAR
    or via this permanent link: https://quickstats.nass.usda.gov/results/9BDE0EC5-C418-3862-9EA8-19CE47D43E6F
    """
    df = (
        load_from_gcs(
            gs_url=os.path.join(
                GCS_CEDA_INPUT_DIR, "USDA", "USDA_Census_crop_2022.csv"
            ),
            local_dir=IN_DIR,
            loader=lambda pth: pd.read_csv(pth),
        )
        .loc[:, ["Data Item", "Value"]]
        .replace([" (D)", " (Z)"], 0)
    )
    df = df.loc[df["Data Item"].str.endswith(" - ACRES HARVESTED")]
    df.index = pd.Index(
        df["Data Item"].str.split(" - ").str[0].map(CROP_TO_CEDA_V7_SECTOR_MAPPING)
    )
    df["Value"] = df["Value"].str.replace(",", "").astype(float)

    return df.groupby(level=0).sum()["Value"]


def _load_fruits_treenuts_area_bearing_2022() -> pd.Series[float]:
    """
    download USDA Census of Agriculture 2022 data from quickstats.nass.usda.gov
        Select Commodity:
            Program: CENSUS
            Sector: CROPS
            Group: FRUITS & TREE NUTS
            Commodity: (all)
            Category: AREA BEARING
            Data Item: (all)
            Domain: AREA BEARING & NON-BEARING
        Select Location:
            Geographic Level: NATIONAL
            State: US TOTAL
        Select Time:
            Year: 2022
            Period Type: ANNUAL
            Period: YEAR
    or via this permanent link: https://quickstats.nass.usda.gov/results/7201E731-3AFD-3BA1-B059-9C02EB37AA32
    """
    df = (
        load_from_gcs(
            gs_url=os.path.join(
                GCS_CEDA_INPUT_DIR, "USDA", "USDA_Census_fruits_treenuts_2022.csv"
            ),
            local_dir=IN_DIR,
            loader=lambda pth: pd.read_csv(pth),
        )
        .loc[:, ["Data Item", "Value"]]
        .replace(" (D)", 0)
    )
    df = df.loc[df["Data Item"].str.endswith(" - ACRES BEARING")]
    df["Value"] = df["Value"].str.replace(",", "").astype(float).fillna(0.0)

    return pd.Series(df["Value"].sum(), index=["111300"])


def _load_vegetables_melons_area_harvested_2022() -> pd.Series[float]:
    """
    download USDA Census of Agriculture 2022 data from quickstats.nass.usda.gov
        Select Commodity:
            Program: CENSUS
            Sector: CROPS
            Group: VEGETABLES
            Commodity: VEGETABLE TOTALS
            Category:
                AREA HARVESTED
                AREA IN PRODUCTION
            Data Item:
                VEGETABLE TOTALS, IN THE OPEN - ACRES HARVESTED
                VEGETABLE TOTALS, IN THE OPEN - ACRES IN PRODUCTION
            Domain:
                AREA HARVESTED, FRESH MARKET & PROCESSING
                AREA IN PRODUCTION
        Select Location:
            Geographic Level: NATIONAL
            State: US TOTAL
        Select Time:
            Year: 2022
            Period Type: ANNUAL
            Period: YEAR
    or via this permanent link: https://quickstats.nass.usda.gov/results/F46D2118-A8D1-3739-B352-C9635EDCEC90
    """
    df = load_from_gcs(
        gs_url=os.path.join(
            GCS_CEDA_INPUT_DIR, "USDA", "USDA_Census_vegetables_2022.csv"
        ),
        local_dir=IN_DIR,
        loader=lambda pth: pd.read_csv(pth),
    ).loc[:, ["Data Item", "Value"]]
    df = df.loc[df["Data Item"].str.endswith(" - ACRES HARVESTED")]
    df["Value"] = df["Value"].str.replace(",", "").astype(float).fillna(0.0)

    return pd.Series(df["Value"].sum(), index=["111200"])


def _load_livestock_area_operated_2022() -> pd.Series[float]:
    """
    download USDA Census of Agriculture 2022 data from quickstats.nass.usda.gov
        Select Commodity:
            Program: CENSUS
            Sector: ANIMALS & PRODUCTS
            Group: (all)
            Commodity: (all)
            Category: INVENTORY
            Data Item: (all)
            Domain: AREA OPERATED
        Select Location:
            Geographic Level: NATIONAL
            State: US TOTAL
        Select Time:
            Year: 2022
            Period Type: POINT IN TIME
            Period: END OF DEC
    or via this permanent link: https://quickstats.nass.usda.gov/results/BA48CEBE-6950-3882-AFD5-4DD8D3A37247
    https://quickstats.nass.usda.gov/results/79D457AE-E98F-3004-8F32-6AE0C33594C0
    """
    df = (
        load_from_gcs(
            gs_url=os.path.join(
                GCS_CEDA_INPUT_DIR, "USDA", "USDA_Census_livestock_2022.csv"
            ),
            local_dir=IN_DIR,
            loader=lambda pth: pd.read_csv(pth),
        )
        .loc[:, ["Data Item", "Value"]]
        .replace([" (D)", " (Z)"], 0)
    )
    df = df.loc[df["Data Item"].str.endswith(" - INVENTORY")]
    df.index = pd.Index(
        df["Data Item"].str.split(" - ").str[0].map(LIVESTOCK_TO_CEDA_V7_SECTOR_MAPPING)
    )
    df["Value"] = df["Value"].str.replace(",", "").astype(float)

    return df.groupby(level=0).sum()["Value"]


CROP_TO_CEDA_V7_SECTOR_MAPPING: dict[str, ta.Literal[CEDA_V7_SECTOR]] = {
    # created by Mo Li
    "SOYBEANS": "1111A0",
    "CANOLA": "1111A0",
    "FLAXSEED": "1111A0",
    "MUSTARD, SEED": "1111A0",
    "RAPESEED": "1111A0",
    "SAFFLOWER": "1111A0",
    "SESAME": "1111A0",
    "SUNFLOWER": "1111A0",
    "CAMELINA": "1111A0",
    "BEANS, DRY EDIBLE, (EXCL LIMA), INCL CHICKPEAS": "1111B0",
    "BEANS, DRY EDIBLE, (EXCL CHICKPEAS & LIMA)": "1111B0",
    "BEANS, DRY EDIBLE, LIMA": "1111B0",
    "CHICKPEAS": "1111B0",
    "LENTILS": "1111B0",
    "PEAS, DRY EDIBLE": "1111B0",
    "PEAS, DRY, SOUTHERN (COWPEAS)": "1111B0",
    "WHEAT": "1111B0",
    "CORN": "1111B0",
    "CORN, GRAIN": "1111B0",
    "CORN, SILAGE": "1111B0",
    "POPCORN, SHELLED": "1111B0",
    "RICE": "1111B0",
    "BARLEY": "1111B0",
    "BUCKWHEAT": "1111B0",
    "MILLET, PROSO": "1111B0",
    "OATS": "1111B0",
    "RYE": "1111B0",
    "SORGHUM, GRAIN": "1111B0",
    "SORGHUM, SILAGE": "1111B0",
    "SORGHUM, SYRUP": "1111B0",
    "TRITICALE": "1111B0",
    "WILD RICE": "1111B0",
    "EMMER & SPELT": "1111B0",
    "VEGETABLE TOTALS": "111200",
    "ORCHARDS": "111300",
    "BERRY TOTALS": "111300",
    "CUT CHRISTMAS TREES": "111400",
    "SHORT TERM WOODY CROPS": "111400",
    "TOBACCO": "111900",
    "COTTON": "111900",
    "SUGARCANE, SUGAR": "111900",
    "SUGARCANE, SEED": "111900",
    "HAY & HAYLAGE": "111900",
    "SUGARBEETS": "111900",
    "SUGARBEETS, SEED": "111900",
    "PEANUTS": "111900",
    "DILL, OIL": "111900",
    "GRASSES & LEGUMES TOTALS, SEED": "111900",
    "GUAR": "111900",
    "HERBS, DRY": "111900",
    "HOPS": "111900",
    "JOJOBA": "111900",
    "MINT, OIL": "111900",
    "MISCANTHUS": "111900",
    "MINT, TEA LEAVES": "111900",
    "SWITCHGRASS": "111900",
    "FIELD CROPS, OTHER": "111900",
}

LIVESTOCK_TO_CEDA_V7_SECTOR_MAPPING: dict[str, ta.Literal[CEDA_V7_SECTOR]] = {
    # created by Mo Li
    "CATTLE, INCL CALVES": "1121A0",
    "CATTLE, (EXCL COWS)": "1121A0",
    "CATTLE, COWS": "112120",
    "HOGS": "112A00",
    "POULTRY TOTALS": "112300",
    "CHICKENS, LAYERS": "112300",
    "CHICKENS, BROILERS": "112300",
    "CHICKENS, PULLETS, REPLACEMENT": "112300",
    "CHICKENS, ROOSTERS": "112300",
    "TURKEYS": "112300",
    "CHUKARS": "112300",
    "DUCKS": "112300",
    "EMUS": "112300",
    "GEESE": "112300",
    "GUINEAS": "112300",
    "OSTRICHES": "112300",
    "PARTRIDGES, HUNGARIAN": "112300",
    "PEAFOWL, HENS & COCKS": "112300",
    "PHEASANTS": "112300",
    "PIGEONS & SQUAB": "112300",
    "POULTRY, OTHER": "112300",
    "QUAIL": "112300",
    "RHEAS": "112300",
    "SHEEP, INCL LAMBS": "112A00",
    "GOATS": "112A00",
    "HONEY, BEE COLONIES": "112A00",
    "EQUINE, HORSES & PONIES": "112A00",
    "EQUINE, MULES & BURROS & DONKEYS": "112A00",
    "MINK, LIVE": "112A00",
    "RABBITS, LIVE": "112A00",
    "ALPACAS": "112A00",
    "BISON": "112A00",
    "DEER": "112A00",
    "ELK": "112A00",
    "LLAMAS": "112A00",
}
