"""Types for year-varying waste weight derivation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, TypedDict


class WasteWeightLongRow(TypedDict):
    IndustryCode: str
    CommodityCode: str
    PercentUsed: float | None
    PercentMake: float | None
    Note: str


@dataclass
class WeightDerivationProvenance:
    target_year: int
    rcra_source_year: int
    ec_source_year: int
    sas_source_year: int | None = None
    aies_source_year: int | None = None
    aies_table: Literal["EXP01", "BASIC"] | None = None
    mut_dollar_year: int | None = None
    fallback_notes: list[str] = field(default_factory=list)
    naics_map_version: str = ""

    def to_dict(self) -> dict[str, object]:
        return {
            "target_year": self.target_year,
            "rcra_source_year": self.rcra_source_year,
            "ec_source_year": self.ec_source_year,
            "sas_source_year": self.sas_source_year,
            "aies_source_year": self.aies_source_year,
            "aies_table": self.aies_table,
            "mut_dollar_year": self.mut_dollar_year,
            "fallback_notes": list(self.fallback_notes),
            "naics_map_version": self.naics_map_version,
        }
