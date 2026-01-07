import enum


class TRANSPORTATION_FUEL_TYPES(enum.Enum):
    # The strign values are intentionally cased to match the table a17 values
    # from the EPA. This allows us to index the table using the enum values.
    GASOLINE = "Motor Gasoline"
    DIESEL = "Distillate Fuel Oil"
    JET_FUEL = "Jet Fuel"
    LPG = "LPG"
    AVIATION_GASOLINE = "Aviation Gasoline"
    RESIDUAL_FUEL_OIL = "Residual Fuel"
    NATURAL_GAS = "Natural Gas"

    # These help avoid a unsortable warning when using these
    # as Series keys
    def __gt__(self, other: "TRANSPORTATION_FUEL_TYPES") -> bool:
        return self.value > other.value

    def __lt__(self, other: "TRANSPORTATION_FUEL_TYPES") -> bool:
        return self.value < other.value
