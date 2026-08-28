"""Which NAICS vintage is each Census USATrade source year actually on?

``review_census_crosswalk`` section 5 cannot answer this question, even though
it looks like it does.  It takes NAICS 2017 as the reference and reports
everything else as a "legacy" code to be mapped, so a file published wholly on
a **different** vintage reads as healthy with a short exception list.  This
module scores each year's own code set against every vintage and reports which
one actually fits.

⚠️ **On the cached data the answer is not what the Crosswalk notes assume.**
The **2017** file matches NAICS **2012** on 398 codes against NAICS 2017 on
388, and contains **zero** codes that are 2017-only.  Census foreign trade
adopts a new NAICS vintage a year late: 2017 trade is published on NAICS 2012,
2018 onward on 2017, 2023 onward on 2022.

✅ **It costs no mass today.**  All ten 2012-only codes are in the Crosswalk and
all ten land on the right BEA commodity, because the 2012 -> 2017 splits they
carry are ones BEA does not distinguish either -- ``211111``/``211112`` both go
to ``211000``, and ``335221``/``335222``/``335224``/``335228`` all go to
``335220``.  So this is a **documentation defect and a live trap**, not a
current error.  It matters because 2017 is the benchmark and the scorecard
year, and because Census will shift vintage again.

❌ **A vintage disagreement is not automatically an error.**  It is an error
only where a code that changed *meaning* across the two vintages carries mass
and is unmapped or mis-mapped.  Read this alongside section 5 of
``review_census_crosswalk``, which says whether the affected codes are mapped.

Run from repo root::

    uv run python -m bedrock.analysis.nowcasting.trade_data.naics_vintage

    # also list the codes that differ between the fitted and assumed vintages
    uv run python -m bedrock.analysis.nowcasting.trade_data.naics_vintage --codes
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

_ROOT = Path(__file__).resolve().parents[3]
_NAICS_CONCORDANCE = (
    _ROOT / "utils" / "mapping" / "naics" / "NAICS_Year_Concordance.csv"
)
_CROSSWALK = (
    _ROOT
    / "utils"
    / "mapping"
    / "activitytosectormapping"
    / "Sector_Crosswalk_Census_USATrade.csv"
)
_SOURCE_DIR = _ROOT / "extract" / "input_data" / "Census_USATrade"

#: The vintage the Crosswalk's own notes assume for each source year.  Census
#: foreign trade lags the NAICS revision by a year, which is why 2017 is not
#: on NAICS 2017.
ASSUMED_VINTAGE = {
    2017: "NAICS_2012_Code",
    2018: "NAICS_2017_Code",
    2019: "NAICS_2017_Code",
    2020: "NAICS_2017_Code",
    2021: "NAICS_2017_Code",
    2022: "NAICS_2017_Code",
    2023: "NAICS_2022_Code",
    2024: "NAICS_2022_Code",
}

YEARS = tuple(range(2017, 2025))


def _source_codes(year: int) -> set[str]:
    """Six-digit NAICS codes in the cached Census USATrade imports file."""
    path = _SOURCE_DIR / str(year) / f"Census_USATrade_{year}_imports.csv"
    if not path.exists():
        return set()
    df = pd.read_csv(path, dtype={"NAICS": str})
    codes = df["NAICS"].astype(str).str.strip()
    return set(codes[codes.str.fullmatch(r"\d{6}")])


def _source_mass(year: int) -> pd.Series:
    """Six-digit import mass by NAICS code, million USD."""
    path = _SOURCE_DIR / str(year) / f"Census_USATrade_{year}_imports.csv"
    df = pd.read_csv(path, dtype={"NAICS": str})
    df["NAICS"] = df["NAICS"].astype(str).str.strip()
    df = df.loc[df["NAICS"].str.fullmatch(r"\d{6}")]
    value = pd.to_numeric(df["GEN_CIF_YR"], errors="coerce") / 1e6
    return value.groupby(df["NAICS"]).sum()


def infer_vintage(years: tuple[int, ...] = YEARS) -> pd.DataFrame:
    """Score each year's code set against every NAICS vintage.

    The best-fitting column is the vintage Census published on.  ``agrees``
    reports whether that matches :data:`ASSUMED_VINTAGE`.
    """
    conc = pd.read_csv(_NAICS_CONCORDANCE, dtype=str)
    vintages = [
        c for c in conc.columns if c.startswith("NAICS_") and c.endswith("_Code")
    ]
    valid = {v: set(conc[v].dropna()) for v in vintages}

    rows = []
    for year in years:
        codes = _source_codes(year)
        if not codes:
            continue
        counts = {v: len(codes & valid[v]) for v in vintages}
        best = max(counts, key=lambda v: counts[v])
        assumed = ASSUMED_VINTAGE.get(year, "")
        row = {"year": year, "n_codes": len(codes)}
        row.update({_short(v): counts[v] for v in vintages})
        row["best_fit"] = _short(best)
        row["assumed"] = _short(assumed) if assumed else ""
        row["agrees"] = best == assumed
        rows.append(row)
    return pd.DataFrame(rows)


def _short(vintage: str) -> str:
    return vintage.replace("NAICS_", "").replace("_Code", "")


def vintage_only_codes(year: int, vintage: str, other: str) -> pd.DataFrame:
    """Codes in ``year`` valid under ``vintage`` but not under ``other``.

    Adds the mass each carries, whether the Crosswalk maps it, and the
    successor codes in the other vintage -- which is what decides whether a
    vintage mismatch actually costs anything.
    """
    conc = pd.read_csv(_NAICS_CONCORDANCE, dtype=str)
    crosswalk = pd.read_csv(_CROSSWALK, dtype=str)
    mapping = dict(
        zip(
            crosswalk["Activity"].astype(str).str.strip(),
            crosswalk["Sector"].astype(str).str.strip(),
        )
    )
    in_vintage = set(conc[vintage].dropna())
    in_other = set(conc[other].dropna())
    codes = sorted((_source_codes(year) & in_vintage) - in_other)
    mass = _source_mass(year)

    rows = []
    for code in codes:
        successors = sorted(set(conc.loc[conc[vintage].eq(code), other].dropna()))
        rows.append(
            {
                "code": code,
                "mass_M": float(mass.get(code, 0.0)),
                "bea_target": mapping.get(code, "UNMAPPED"),
                f"{_short(other)}_successors": ", ".join(successors) or "(none)",
            }
        )
    return pd.DataFrame(rows).sort_values("mass_M", ascending=False)


def main() -> None:
    df = infer_vintage()
    print("Which NAICS vintage is each Census USATrade year actually published on?")
    print()
    if df.empty:
        print("(no cached Census_USATrade source files found)")
        return
    with pd.option_context("display.width", 160):
        print(df.to_string(index=False))

    disagree = df.loc[~df["agrees"]]
    print()
    if disagree.empty:
        print("All cached years match the vintage ASSUMED_VINTAGE records.")
    else:
        for row in disagree.itertuples():
            print(
                f"MISMATCH {int(row.year)}: fits {row.best_fit}, "
                f"ASSUMED_VINTAGE says {row.assumed}"
            )
        print(
            "A mismatch costs mass only where a code that changed meaning is "
            "unmapped or mapped to the wrong BEA commodity -- check the codes below."
        )

    if "--codes" in sys.argv:
        for row in df.itertuples():
            year, fitted = int(row.year), row.best_fit
            assumed = row.assumed
            if not assumed or fitted == assumed:
                continue
            print()
            print(f"## {year}: valid {fitted} but not {assumed}")
            detail = vintage_only_codes(
                year, f"NAICS_{fitted}_Code", f"NAICS_{assumed}_Code"
            )
            with pd.option_context("display.width", 180):
                print(detail.to_string(index=False, float_format=lambda x: f"{x:,.1f}"))
            unmapped = detail.loc[detail["bea_target"].eq("UNMAPPED")]
            print(
                f"   {len(unmapped)} unmapped, carrying "
                f"{unmapped['mass_M'].sum():,.1f}M"
            )


if __name__ == "__main__":
    main()
