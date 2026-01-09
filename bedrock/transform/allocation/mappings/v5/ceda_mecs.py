import typing as ta

from bedrock.ceda_usa.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTOR

# https://www.eia.gov/tools/faqs/faq.cfm?id=72&t=2
MMBTU_PER_SHORT_TONNE_COAL = 20


NON_MECS_INDUSTRIES: ta.List[CEDA_V7_SECTOR] = [
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
]


INDUSTRY_TO_ENERGY_DISTRIBUTION_MAPPING = {
    # 1:1 CEDA industry to energy distribution
    ("321100",): ("321113",),
    ("321200",): ("3212",),
    ("327310",): ("327310",),
    ("331200",): ("3312",),
    ("331313",): ("3313",),  # CEDA 331313 covers all sub-industries under NAICS 3313
    ("331510",): ("331511",),
    ("334413",): ("334413",),
    ("336111",): ("336111",),
    ("336112",): ("336112",),
    ("336411",): ("336411",),
    ("339115",): ("339",),
    ("339116",): ("339",),
    ("339910",): ("339",),
    ("339920",): ("339",),
    ("339930",): ("339",),
    ("339940",): ("339",),
    ("339950",): ("339",),
    ("339990",): ("339",),
    ("311221",): ("311221",),
    ("311300",): ("31131",),
    ("312200",): ("3122",),
    ("315000",): ("315",),
    ("316000",): ("316",),
    ("322110",): ("322110",),
    ("322130",): ("322130",),
    ("325110",): ("325110",),
    ("325120",): ("325120",),
    ("325211",): ("325211",),
    ("325412",): ("325412",),
    ("3259A0",): ("325992",),
    ("327993",): ("327993",),
    # Multi mappings 1:many, many: 1 or many:many, but no subtraction
    # These industries are aggregated on the RHS into a total and allocatedd
    # using the BEA Use table
    ("323110", "323120"): ("323",),
    ("321910", "3219A0"): ("3219",),
    ("327200",): (
        "327211",
        "327212",
        "327213",
        "327215",
    ),
    ("327400",): ("327410", "327420"),
    ("331110",): ("331111", "331112"),
    ("331410", "331420", "331490"): ("3314",),
    (
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
    ): ("332",),
    ("331520",): ("331521", "331524"),
    (
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
    ): ("333",),
    (
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
    ): ("335",),
    (
        "337110",
        "337121",
        "337122",
        "337127",
        "33712N",
        "337215",
        "33721A",
        "337900",
    ): ("337",),
    (
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
    ): ("339",),
    (
        "311410",
        "311420",
    ): ("3114",),
    (
        "311513",
        "311514",
        "31151A",
        "311520",
    ): ("3115",),
    (
        "311615",
        "31161A",
    ): ("3116",),
    (
        "312110",
        "312120",
        "312130",
        "312140",
    ): ("3121",),
    ("313100", "313200", "313300"): ("313",),
    (
        "314110",
        "314120",
        "314900",
    ): ("314",),
    ("322120",): (
        "322121",
        "322122",
    ),
    (
        "324110",
        "324121",
        "324122",
        "324190",
    ): ("324",),
    ("325180",): (
        "325181",
        "325182",
        "325188",
    ),
    ("325190",): (
        "325192",
        "325193",
        "325199",
    ),
    ("3252A0",): ("325212", "325222"),
    ("325310",): (
        "325311",
        "325312",
    ),
    (
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
    ): ("326",),
}


INDUSTRY_TO_ENERGY_DISTRIBUTION_SUBTRACTION_MAPPING: dict[
    tuple[str, ...], tuple[tuple[str, ...], tuple[str, ...]]
] = {
    # For each of these industries, they map to a subset of the total
    # energy distribution from MECS 3.1
    # The ordering is {(CEDA industries): ((mapped values), (subtract values))}
    ("327100", "327320", "327330", "327390", "327910", "327991", "327992", "327999"): (
        ("327",),
        (
            "327211",
            "327212",
            "327213",
            "327215",
            "327310",
            "327410",
            "327420",
            "327993",
        ),
    ),
    (
        "334111",
        "334112",
        "334118",
        "334210",
        "334220",
        "334290",
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
    ): (("334",), ("334413",)),
    (
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
        "336500",
        "336611",
        "336612",
        "336991",
        "336992",
        "336999",
    ): (
        ("336",),
        (
            "336111",
            "336112",
            "3364",
        ),
    ),
    (
        "336412",
        "336413",
        "336414",
        "33641A",
    ): (("3364",), ("336411",)),
    (
        "311111",
        "311119",
        "311700",
        "311810",
        "3118A0",
        "311910",
        "311920",
        "311930",
        "311940",
        "311990",
    ): (
        ("311",),
        (
            "3112",
            "31131",
            "3114",
            "3115",
            "3116",
        ),
    ),
    (
        "311210",
        "311225",
        "311224",
        "311230",
    ): (("3112",), ("311221",)),
    (
        "322210",
        "322220",
        "322230",
        "322291",
        "322299",
    ): (
        ("322",),
        (
            "322110",
            "322121",
            "322122",
            "322130",
        ),
    ),
    (
        "325130",
        "325320",
        "325510",
        "325520",
        "325610",
        "325620",
        "325910",
    ): (
        ("325",),
        (
            "325110",
            "325120",
            "325181",
            "325182",
            "325188",
            "325192",
            "325193",
            "325199",
            "325211",
            "325212",
            "325222",
            "325311",
            "325312",
            "3254",
            "325992",
        ),
    ),
    (
        "325411",
        "325413",
        "325414",
    ): (("3254",), ("325412",)),
}

INDUSTRY_TO_NON_ENERGY_DISTRIBUTION_MAPPING_NATURAL_GAS: dict[str, str | list[str]] = {
    "321100": "321",  # wood products
    "321200": "321",  # wood products
    "321910": "321",  # wood products
    "3219A0": "321",  # wood products
    "331110": "331110",  # iron and steel mills
    "331200": "3312",  # steel production
    "331313": "3313",  # aluminum production
    "331410": "3314",  # nonferrous metal production
    "331420": "3314",  # nonferrous metal production
    "331490": "3314",  # nonferrous metal production
    "331510": "3315",  # foundries
    "331520": "3315",  # foundries
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
    "311210": "3112",  # grain and oilseed milling
    "311225": "3112",  # grain and oilseed milling
    "311224": "3112",  # grain and oilseed milling
    "311410": "3114",  # fruit and vegetable preserving
    "311420": "3114",  # fruit and vegetable preserving
    "322130": "322130",  # paperboard mills
    "325120": "325120",  # industrial gas manufacturing
    "325180": "325180",  # other basic inorganic chemical manufacturing
    "325190": [  # other basic organic chemical manufacturing
        "325193",  # ethyl alcohol
        "325194",  # cyclic crudes and intermediates
        "325199",  # all other basic organic chemicals
    ],
    "325211": "325211",  # plastics material and resin manufacturing
    "3252A0": [  # synthetic rubber manufacturing
        "325212",  # synthetic rubber manufacturing
        "325220",  # noncellulosic organic fiber manufacturing
    ],
    "325310": [
        "325311",  # nitrogenous fertilizer manufacturing
        "325312",  # phosphatic fertilizer manufacturing
    ],
}

INDUSTRY_TO_NON_ENERGY_DISTRIBUTION_MAPPING_PETROL: dict[str, str | list[str]] = {
    "327200": "327211",  # flag glass
    "327310": "327310",  # cement manufacturing
    "331110": "331110",  # iron and steel mills
    "331200": "3312",  # steel production
    "334413": "334413",  # semiconductor and related device manufacturing
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
    "324190": "324199",  # other petroleum and coal products manufacturing
    "325110": "325110",  # petrochemical manufacturing
    "325180": "325180",  # other basic inorganic chemical manufacturing
    "325190": [  # other basic organic chemical manufacturing
        "325193",  # ethyl alcohol
        "325194",  # cyclic crudes and intermediates
        "325199",  # all other basic organic chemicals
    ],
    "325211": "325211",  # plastics material and resin manufacturing
    "3252A0": [  # synthetic rubber manufacturing
        "325212",  # synthetic rubber manufacturing
        "325220",  # noncellulosic organic fiber manufacturing
    ],
    "325310": "325311",  # nitrogenous fertilizer manufacturing
}
