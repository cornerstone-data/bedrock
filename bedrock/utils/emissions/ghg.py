# some convenience imports
import typing as ta

GHG_LITERAL = ta.Literal["CO2", "CH4", "N2O", "HFCs", "PFCs", "SF6", "NF3"]
GHG: ta.List[GHG_LITERAL] = list(ta.get_args(GHG_LITERAL))
GHG_DETAILED = [
    # order matters!
    "CO2",
    "CH4",
    "N2O",
    "HFC-23",
    "HFC-32",
    "HFC-125",
    "HFC-134a",
    "HFC-143a",
    "HFC-236fa",
    "CF4",
    "C2F6",
    "C3F8",
    "C4F8",
    "SF6",
    "NF3",
]

GHG_MAPPING = {  # GHG->GHG_DETAILED
    # order matters!
    "CO2": ["CO2"],
    "CH4": ["CH4"],
    "N2O": ["N2O"],
    "HFCs": ["HFC-23", "HFC-32", "HFC-125", "HFC-134a", "HFC-143a", "HFC-236fa"],
    "PFCs": ["CF4", "C2F6", "C3F8", "C4F8"],
    "SF6": ["SF6"],
    "NF3": ["NF3"],
}
