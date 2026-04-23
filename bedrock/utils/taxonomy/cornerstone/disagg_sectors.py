"""Registry of Cornerstone sectors that disaggregate from a single aggregate code."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DisaggSectorTaxonomy:
    name: str
    industry_aggregate_code: str
    commodity_aggregate_code: str
    industry_new_codes: tuple[str, ...]
    commodity_new_codes: tuple[str, ...]


DISAGG_SECTORS: dict[str, DisaggSectorTaxonomy] = {
    "waste": DisaggSectorTaxonomy(
        name="waste",
        industry_aggregate_code="562000",
        commodity_aggregate_code="562000",
        industry_new_codes=(
            "562111",
            "562HAZ",
            "562212",
            "562213",
            "562910",
            "562920",
            "562OTH",
        ),
        commodity_new_codes=(
            "562111",
            "562HAZ",
            "562212",
            "562213",
            "562910",
            "562920",
            "562OTH",
        ),
    ),
}
