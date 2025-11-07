import typing as ta

from ceda_usa.utils.taxonomy.bea.v2012_industry import BEA_2012_INDUSTRY_CODE

TRANSPORTATION_SOURCE_TO_BEA_INDUSTRY_MAPPING: ta.Dict[
    ta.Tuple[str, str], BEA_2012_INDUSTRY_CODE
] = {
    ("Refrigerated Transport", "Rail"): "482000",
    ("Refrigerated Transport", "Ships and Boats"): "483000",
    ("Refrigerated Transport", "Medium- and Heavy-Duty Trucks"): "484000",
    ("Mobile AC", "Heavy-Duty Vehicles"): "484000",
    ("Comfort Cooling for Trains and Buses", "School and Tour Buses"): "485000",
    # TODO: figure out what to do with F01000
    # "Passenger Cars": "F01000",
    # "Light-Duty Trucks": "F01000",
}
