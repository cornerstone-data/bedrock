"""Year resolvers for waste weight derivation (2024 pilot + future multi-year)."""

from __future__ import annotations

from typing import Literal

WiredRcra = Literal[2017, 2019, 2021]
WIRED_RCRA_YEARS: tuple[int, ...] = (2017, 2019, 2021)
SAS_YEARS: frozenset[int] = frozenset(range(2013, 2023))


def resolve_rcra_year(target_year: int) -> int:
    """Odd biennial ≤ target among wired CRHW years; 2023/2024 → 2021 until CRHW 2023."""
    candidates = [y for y in WIRED_RCRA_YEARS if y <= target_year]
    if not candidates:
        raise ValueError(f"No wired RCRA year ≤ {target_year}")
    return max(candidates)


def resolve_ec_year(target_year: int, *, ec_2022_wired: bool = True) -> int:
    """EC customer-class year. 2022+ uses EC 2022 once wired (2024 pilot default)."""
    if target_year <= 2017:
        return 2017
    if 2018 <= target_year <= 2021:
        return 2017
    if target_year >= 2022 and ec_2022_wired:
        return 2022
    # Explicit freeze when caller passes ec_2022_wired=False
    return 2017


def resolve_sas_year(target_year: int) -> int:
    """SAS calendar year for expense/revenue; carry 2022 for 2023+."""
    if target_year in SAS_YEARS:
        return target_year
    if target_year > 2022:
        return 2022
    raise ValueError(f"No SAS year for {target_year}")


def resolve_industry_mix_source(
    target_year: int,
) -> tuple[Literal["sas_table3", "aies_exp01", "aies_basic"], int]:
    """Primary industry-mix source and its survey year."""
    if target_year <= 2022:
        return "sas_table3", resolve_sas_year(target_year)
    if target_year == 2023:
        return "aies_exp01", 2023
    if target_year == 2024:
        return "aies_basic", 2024
    raise ValueError(f"No industry-mix source for {target_year}")


def resolve_weights_year(
    waste_weights_year: int | Literal["match_io"] | None,
    *,
    usa_base_io_data_year: int,
) -> int:
    """Resolve USAConfig.waste_weights_year to an int year."""
    if waste_weights_year is None or waste_weights_year == 2017:
        return 2017
    if waste_weights_year == "match_io":
        return int(usa_base_io_data_year)
    return int(waste_weights_year)
