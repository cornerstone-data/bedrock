from __future__ import annotations

import numpy as np
import pandas as pd

from bedrock.extract.allocation.bea import load_bea_use_table
from bedrock.extract.allocation.epa import load_mmt_co2e_across_fuel_types
from bedrock.extract.allocation.mecs import load_mecs_2_1, load_mecs_3_1
from bedrock.utils.economic.units import MEGATONNE_TO_KG
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS

ALLOCATION_SECTORS = [
    "1111A0",
    "1111B0",
    "111200",
    "111300",
    "111400",
    "111900",
    "112120",
    "1121A0",
    "112300",
    "112A00",
    "113000",
    "114000",
    "115000",
    "211000",
    "212100",
    "212230",
    "2122A0",
    "212310",
    "2123A0",
    "213111",
    "21311A",
    "221200",
    "233210",
    "233262",
    "230301",
    "230302",
    "2332A0",
    "233412",
    "2334A0",
    "233230",
    "2332D0",
    "233240",
    "233411",
    "2332C0",
    "321100",
    "321200",
    "321910",
    "3219A0",
    "327100",
    "327200",
    "327310",
    "327320",
    "327330",
    "327390",
    "327400",
    "327910",
    "327991",
    "327992",
    "327993",
    "327999",
    "331110",
    "331200",
    # "331314",
    "331313",
    # "33131B",
    "331410",
    "331420",
    "331490",
    "331510",
    "331520",
    "332114",
    "33211A",
    "332119",
    "332200",
    "332310",
    "332320",
    "332410",
    "332420",
    "332430",
    "332500",
    "332600",
    "332710",
    "332720",
    "332800",
    "332913",
    "33291A",
    "332991",
    "332996",
    "33299A",
    "332999",
    "333111",
    "333112",
    "333120",
    "333130",
    "333242",
    "33329A",
    "333314",
    "333316",
    "333318",
    "333414",
    "333415",
    "333413",
    "333511",
    "333514",
    "333517",
    "33351B",
    "333611",
    "333612",
    "333613",
    "333618",
    "333912",
    "333914",
    "333920",
    "333991",
    "333993",
    "333994",
    "33399A",
    "33399B",
    "334111",
    "334112",
    "334118",
    "334210",
    "334220",
    "334290",
    "334413",
    "334418",
    "33441A",
    "334510",
    "334511",
    "334512",
    "334513",
    "334514",
    "334515",
    "334516",
    "334517",
    "33451A",
    "334300",
    "334610",
    "335110",
    "335120",
    "335210",
    "335221",
    "335222",
    "335224",
    "335228",
    "335311",
    "335312",
    "335313",
    "335314",
    "335911",
    "335912",
    "335920",
    "335930",
    "335991",
    "335999",
    "336111",
    "336112",
    "336120",
    "336211",
    "336212",
    "336213",
    "336214",
    "336310",
    "336320",
    "336350",
    "336360",
    "336370",
    "336390",
    "3363A0",
    "336411",
    "336412",
    "336413",
    "336414",
    "33641A",
    "336500",
    "336611",
    "336612",
    "336991",
    "336992",
    "336999",
    "337110",
    "337121",
    "337122",
    "337127",
    "33712N",
    "337215",
    "33721A",
    "337900",
    "339112",
    "339113",
    "339114",
    "339115",
    "339116",
    "339910",
    "339920",
    "339930",
    "339940",
    "339950",
    "339990",
    "311111",
    "311119",
    "311210",
    "311221",
    "311225",
    "311224",
    "311230",
    "311300",
    "311410",
    "311420",
    "311513",
    "311514",
    "31151A",
    "311520",
    "311615",
    "31161A",
    "311700",
    "311810",
    "3118A0",
    "311910",
    "311920",
    "311930",
    "311940",
    "311990",
    "312110",
    "312120",
    "312130",
    "312140",
    "312200",
    "313100",
    "313200",
    "313300",
    "314110",
    "314120",
    "314900",
    "315000",
    "316000",
    "322110",
    "322120",
    "322130",
    "322210",
    "322220",
    "322230",
    "322291",
    "322299",
    "323110",
    "323120",
    "324110",
    "324121",
    "324122",
    "324190",
    "325110",
    "325120",
    "325130",
    "325180",
    "325190",
    "325211",
    "3252A0",
    "325411",
    "325412",
    "325413",
    "325414",
    "325310",
    "325320",
    "325510",
    "325520",
    "325610",
    "325620",
    "325910",
    "3259A0",
    "326110",
    "326120",
    "326130",
    "326140",
    "326150",
    "326160",
    "326190",
    "326210",
    "326220",
    "326290",
]

SECTOR_TO_NAICS_MAPPING: dict[str, str] = {
    "327200": "327211",  # flag glass
    "327310": "327310",  # cement manufacturing
    "331110": "331110",  # iron and steel mills
    "324190": "324199",  # other petroleum and coal products manufacturing
    "331200": "3312",  # steel production
    "336111": "336",  # transportation equipment manufacturing
    "336112": "336",  # transportation equipment manufacturing
    "336120": "336",  # transportation equipment manufacturing
    "336211": "336",  # transportation equipment manufacturing
    "336212": "336",  # transportation equipment manufacturing
    "336213": "336",  # transportation equipment manufacturing
    "336214": "336",  # transportation equipment manufacturing
    "336310": "336",  # transportation equipment manufacturing
    "336320": "336",  # transportation equipment manufacturing
    "336350": "336",  # transportation equipment manufacturing
    "336360": "336",  # transportation equipment manufacturing
    "336370": "336",  # transportation equipment manufacturing
    "336390": "336",  # transportation equipment manufacturing
    "3363A0": "336",  # transportation equipment manufacturing
    "336411": "336",  # transportation equipment manufacturing
    "336412": "336",  # transportation equipment manufacturing
    "336413": "336",  # transportation equipment manufacturing
    "336414": "336",  # transportation equipment manufacturing
    "33641A": "336",  # transportation equipment manufacturing
    "336500": "336",  # transportation equipment manufacturing
    "336611": "336",  # transportation equipment manufacturing
    "336612": "336",  # transportation equipment manufacturing
    "336991": "336",  # transportation equipment manufacturing
    "336992": "336",  # transportation equipment manufacturing
    "336999": "336",  # transportation equipment manufacturing
    "311221": "311221",  # wet corn milling
    "324110": "324110",  # petroleum refineries
    "324121": "324121",  # other petroleum and coal products manufacturing
    "324122": "324122",  # other petroleum and coal products manufacturing
    "325110": "325110",  # petrochemical manufacturing
    "325180": "325180",  # other basic inorganic chemical manufacturing
    "325190": "325194",  # cyclic crudes and intermediates
    "325211": "325211",  # plastics material and resin manufacturing
    "3252A0": "325212",  # synthetic rubber manufacturing
    "325310": "325311",  # nitrogenous fertilizer manufacturing
}

FUEL_RATIOS_2013 = {
    # These values are fallbacks for sectors whose MECS-derived fuel ratios are NaN
    # see Source_EIA at https://docs.google.com/spreadsheets/d/1qBsIhrw1es_VF_WbmViCfXcz7iNC4ArQ/edit#gid=1117753791
    "311221": 0.93,  # S9
    "324110": 0.43,  # S32
    "324199": 0.45,  # S33
    "324121": 0.45,  # reuse of S33 becuase it's used to map to 324199, this will likely unused since MECS data will return a valid fuel ratio
    "324122": 0.45,  # reuse of S33 becuase it's used to map to 324199, this will likely unused since MECS data will return a valid fuel ratio
    "325110": 0.95,  # S35
    "325180": 0.91,  # S37, other basic inorganic chemical manufacturing, formerly 325181
    "325194": 0.76,  # S40, cyclic crudes and intermediates, formerly 325192
    "325211": 0.99,  # S43
    "325212": 0.84,  # S44
    "325311": 0.50,  # S46
    "327211": 0.00,  # S53
    "327310": 0.98,  # S57
    "331110": 0.96,  # S62, iron and steel mills, formerly 331111
    "3312": 0.70,  # S64
    "336": 0.83,  # S79
}


def allocate_industrial_petrol() -> pd.Series[float]:
    emissions = load_mmt_co2e_across_fuel_types().loc["Total Petroleum", "Ind"]
    assert isinstance(emissions, float)

    # calculate new fuel ratios using MECS data
    mecs_index = pd.Index(
        set([v for v in SECTOR_TO_NAICS_MAPPING.values() if not isinstance(v, list)])
    )
    mecs_2_1 = load_mecs_2_1().loc[mecs_index, "Other(e)"]
    mecs_3_1 = load_mecs_3_1().loc[mecs_index, "Other(f)"]
    ratios = mecs_3_1 / (mecs_2_1 + mecs_3_1)

    fuel_ratios = pd.Series(
        {
            k: ratios[v] if not np.isnan(ratios[v]) else FUEL_RATIOS_2013[v]
            for k, v in SECTOR_TO_NAICS_MAPPING.items()
        }
    )

    # find total expenditure on petrol for energy and non-energy use
    use = (
        load_bea_use_table().loc[pd.Index(ALLOCATION_SECTORS), "324110"].astype(float)
    )  # Petroleum refineries
    expenditure_on_petrol = use.sum()
    expenditure_on_non_energy_petrol = use.mul(1 - fuel_ratios).sum()
    expenditure_on_energy_petrol = (
        expenditure_on_petrol - expenditure_on_non_energy_petrol
    )

    pct = use / expenditure_on_energy_petrol
    fuel_ratios = fuel_ratios.reindex(pct.index, fill_value=1.0)

    allocated = emissions * pct * fuel_ratios
    return allocated.reindex(CEDA_V7_SECTORS, fill_value=0.0) * MEGATONNE_TO_KG
