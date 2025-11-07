import typing as ta

GWPReport = ta.Literal["AR6", "AR5", "AR4", "AR2"]

GWPGhg = ta.Literal[
    "CO2", "CH4", "N2O", "SF6", "CH4_fossil", "CH4_non_fossil"
]  # subset of GHG supported by GWP
# 100-year Global Warming Potential Values
GWP100: ta.Dict[GWPReport, ta.Dict[GWPGhg, float]] = {
    "AR2": {"CO2": 1, "CH4": 21, "N2O": 310, "SF6": 23900},
    # AR4: https://archive.ipcc.ch/publications_and_data/ar4/wg1/en/ch2s2-10-2.html
    "AR4": {"CO2": 1, "CH4": 25, "N2O": 298, "SF6": 22800},
    # AR5: Table 8.A.1 from IPCC AR5 Synthesis Report:
    # https://archive.ipcc.ch/pdf/assessment-report/ar5/wg1/WG1AR5_Chapter08_FINAL.pdf
    "AR5": {"CO2": 1, "CH4": 28, "N2O": 265, "SF6": 23500},
    # AR6: Table 7.15 from IPCC AR6 WG1 report
    # https://www.ipcc.ch/report/ar6/wg1/downloads/report/IPCC_AR6_WGI_Chapter07.pdf
    "AR6": {
        "CO2": 1,
        "CH4": 27.0,  # TODO: Remove
        "N2O": 273,
        "SF6": 24300,
        "CH4_fossil": 29.8,
        "CH4_non_fossil": 27.0,
    },
}

GWP100_AR4 = GWP100["AR4"]
GWP100_AR5 = GWP100["AR5"]
GWP100_AR6 = GWP100["AR6"]

IPCC_GHG_CEDA_EXCL_CH4 = ta.Literal[
    "CO2",
    "N2O",
    "SF6",
    "HFC-23",
    "HFC-32",
    "HFC-41",
    "HFC-43-10mee",
    "HFC-125",
    "HFC-134a",
    "HFC-143a",
    "HFC-152a",
    "HFC-227ea",
    "HFC-236fa",
    "HFC-245fa",
    "HFC-365mfc",
    "CF4",
    "C2F6",
    "C3F8",
    "C4F6",
    "C5F8",
    "CH2F2",
    "CH3F",
    "CH2FCF3",
    "c-C4F8",
    "NF3",
]
IPCC_GHG_AR5_CEDA = ta.Union[IPCC_GHG_CEDA_EXCL_CH4, ta.Literal["CH4"]]
IPCC_GHG_AR6_CEDA = ta.Union[
    IPCC_GHG_CEDA_EXCL_CH4, ta.Literal["CH4_fossil", "CH4_non_fossil"]
]

GWP100_AR5_CEDA: ta.Dict[IPCC_GHG_AR5_CEDA, float] = {
    "CO2": GWP100_AR5["CO2"],
    "CH4": GWP100_AR5["CH4"],
    "N2O": GWP100_AR5["N2O"],
    "SF6": GWP100_AR5["SF6"],
    # non-major GHGs form GHG Protocol's detailed document on GWPs for non-major GHGs:
    # https://ghgprotocol.org/sites/default/files/2024-08/Global-Warming-Potential-Values%20%28August%202024%29.pdf
    "HFC-23": 12400,
    "HFC-32": 677,
    "HFC-41": 116,
    "HFC-43-10mee": 1650,
    "HFC-125": 3170,
    "HFC-134a": 1300,
    "HFC-143a": 4800,
    "HFC-152a": 138,
    "HFC-227ea": 3350,
    "HFC-236fa": 8060,
    "HFC-245fa": 858,
    "HFC-365mfc": 804,
    "CF4": 6630,
    "C2F6": 11100,
    "C3F8": 8900,
    "C4F6": 1,  # less than 1 due to its short atmospheric lifetime, which is only about 1.1 days
    "C5F8": 1,  # undetermined GWP-100 value, use 1 as a placeholder
    "CH2F2": 677,
    "CH3F": 116,
    "CH2FCF3": 1300,
    "c-C4F8": 9540,
    "NF3": 16100,
}

GWP100_AR6_CEDA: ta.Dict[IPCC_GHG_AR6_CEDA, float] = {
    "CO2": GWP100_AR6["CO2"],
    "CH4_fossil": GWP100_AR6["CH4_fossil"],
    "CH4_non_fossil": GWP100_AR6["CH4_non_fossil"],
    "N2O": GWP100_AR6["N2O"],
    "SF6": GWP100_AR6["SF6"],
    # non-major GHGs form GHG Protocol's detailed document on GWPs for non-major GHGs:
    # https://ghgprotocol.org/sites/default/files/2024-08/Global-Warming-Potential-Values%20%28August%202024%29.pdf
    "HFC-23": 14600,
    "HFC-32": 771,
    "HFC-41": 135,
    "HFC-43-10mee": 1600,
    "HFC-125": 3740,
    "HFC-134a": 1530,
    "HFC-143a": 5810,
    "HFC-152a": 164,
    "HFC-227ea": 3600,
    "HFC-236fa": 8690,
    "HFC-245fa": 962,
    "HFC-365mfc": 914,
    "CF4": 7380,
    "C2F6": 12400,
    "C3F8": 9290,
    "C4F6": 1,  # less than 1 due to its short atmospheric lifetime, which is only about 1.1 days
    "C5F8": 1,  # undetermined GWP-100 value, use 1 as a placeholder
    "CH2F2": 771,
    "CH3F": 135,
    "CH2FCF3": 1530,
    "c-C4F8": 10200,
    "NF3": 17400,
}


def derive_ar5_to_ar6_multiplier() -> dict[str, float]:
    return {
        **{
            gas: GWP100_AR6_CEDA[gas] / GWP100_AR5_CEDA[gas]
            for gas in list(ta.get_args(IPCC_GHG_CEDA_EXCL_CH4))
        },
        **{
            "CH4_fossil": GWP100_AR6_CEDA["CH4_fossil"] / GWP100_AR5_CEDA["CH4"],
            "CH4_non_fossil": GWP100_AR6_CEDA["CH4_non_fossil"]
            / GWP100_AR5_CEDA["CH4"],
        },
    }
