"""Can a product mix re-split imports inside a NAICS family? (#670) -- NO.

``row_exposure --decompose`` found families where our import **level** is right
and the **mix** inside the family is wrong -- ``3341`` computers at level 0.98,
``3371`` furniture at 1.03.  That is the shape a product concordance should
fix, so this module tested two candidate weights against the published answer
key rather than assuming one would work.

❌ **Both lose, and not narrowly.**

===============  =====================  ===============================
arm              families improved      net toward published split
===============  =====================  ===============================
PxI (supply)     10 of 58               **-266,566M** (away)
PCE bridge       10 of 55               **-305,856M** (away)
===============  =====================  ===============================

⚠️ **The reason is that the identity mapping is already good.**  ``l1_current``
is 0.002 on ``3121``, 0.004 on ``3313``, 0.007 on ``3352``.  The importer's
NAICS reproduces BEA's within-family split closely for most families, the mix
error is concentrated in a handful, and neither product proxy improves even
those.  So BEA's import allocation really is HS-driven, and **neither a
production-side nor a consumption-side product mix substitutes for it.**

✅ **What this rules in.**  Leave the import split alone.  The remaining route
for the import mix is BEA's actual HS-to-I-O concordance, and the case for
paying for it is now much weaker: the row damage this issue is about is
roughly half on the **export** side (``336411`` is ``MCIF`` +4.9bn against
``F04000`` -50.1bn), and that half is untouched by anything here.

The two arms
------------
**PxI** (:func:`pxi_mix`) is ``Census_EC_PxI`` joined to
``census_pxi/napcs_to_bea_2017.csv``: the **domestic production** product mix.
Its natural home is the Supply mix and exports, not imports.

**PCE bridge** (:func:`pce_bridge_mix`) allocates each NIPA consumption
category across BEA commodities: the **consumption** product mix, at producers'
value because ``MCIF`` is a basic-value column.  It covers only what households
buy, so it is silent on capital goods and its silence is not evidence.

⚠️ **NAPCS is on a five-year vintage, exactly like NAICS** -- the 2022
Economic Census is on NAPCS 2022 and is bridged back through
``napcs_2022_to_2017.csv`` before meeting the 2017 concordance.  See
``naics_vintage`` for the same trap one classification along.

The answer key
--------------
Published 2017 detail ``MCIF``, sound **in the benchmark year only**.  Scored
as within-family L1 on shares::

    L1 = 0.5 * sum_c | our_share_c - published_share_c |

0.0 is the published mix exactly, 1.0 disjoint.  The level is held at the
Census family total in every arm, so this measures the split and nothing else.

Run from repo root::

    uv run python -m bedrock.analysis.nowcasting.trade_data.family_resplit

    # every family with at least two members, not just the exposed ones
    uv run python -m bedrock.analysis.nowcasting.trade_data.family_resplit --all
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

from bedrock.analysis.nowcasting.trade_data.naics_vintage import _source_mass
from bedrock.analysis.nowcasting.trade_data.row_exposure import _published_mcif
from bedrock.extract.flowbyactivity import getFlowByActivity
from bedrock.extract.iot.io_2017 import load_2017_pce_bridge_detail_usa

YEAR = 2017
MILLION = 1e6
OUT_DIR = Path(__file__).resolve().parent / "output"
RESPLIT_CSV = OUT_DIR / "family_resplit.csv"

_ROOT = Path(__file__).resolve().parents[3]
_CROSSWALK = (
    _ROOT
    / "utils"
    / "mapping"
    / "activitytosectormapping"
    / "Sector_Crosswalk_Census_USATrade.csv"
)

#: Families ``row_exposure --decompose`` put in the concordance's best case:
#: level within 3% of published, with a real mix error.
TARGET_FAMILIES = ("3341", "3371", "5415")

#: Report a family only if it carries at least this much published MCIF, so a
#: tiny family cannot dominate the verdict on share noise.
MIN_FAMILY_USD = 1e9

#: NAPCS vintage the concordance is written in.
BASE_NAPCS_YEAR = 2017

#: Census_EC_PxI carries an all-sectors total under ActivityProducedBy '00'.
NAICS_CODE_LENGTH = 6

_NAPCS_TO_BEA = _ROOT / "utils" / "mapping" / "census_pxi" / "napcs_to_bea_2017.csv"
_NAPCS_BRIDGE = _ROOT / "utils" / "mapping" / "census_pxi" / "napcs_2022_to_2017.csv"


def current_mix() -> pd.Series:
    """Import mass by BEA commodity under today's identity Crosswalk, USD."""
    crosswalk = pd.read_csv(_CROSSWALK, dtype=str)
    mapping = dict(
        zip(
            crosswalk["Activity"].astype(str).str.strip(),
            crosswalk["Sector"].astype(str).str.strip(),
        )
    )
    mass = _source_mass(YEAR) * MILLION
    bea = pd.Series({code: mapping.get(code) for code in mass.index})
    keep = bea.notna()
    return mass[keep].groupby(bea[keep]).sum()


def pxi_mix(year: int = YEAR) -> pd.Series:
    """Domestic product mix by BEA commodity from Census EC PxI, USD.

    ❌ **Not** ``pxi_mix_test.built_mix``.  That routes through
    ``pxi_services_product_seed_2017.csv`` -- a **services** product seed -- so
    it returns essentially nothing for manufacturing families: 282M against a
    100bn family on ``3341``, and exactly zero on ``3371`` and ``3254``.  A
    verdict computed on that is noise, not a no-go.

    This joins ``Census_EC_PxI`` straight onto
    ``census_pxi/napcs_to_bea_2017.csv`` on the NAPCS code, which is where the
    manufacturing coverage actually is -- 44 rows into ``3371``, 33 into
    ``3341``, 125 into ``3254``.

    ⚠️ Two traps, both already known and both live here:

    - ``ActivityProducedBy == '00'`` is an **all-sectors total** sitting beside
      the six-digit detail it totals, 51.1% of the file.  Filtered out.
    - **NAPCS is on a five-year vintage.**  The 2022 Economic Census is on
      NAPCS 2022 and is bridged back through ``napcs_2022_to_2017.csv`` before
      it meets the 2017 concordance.
    """
    pxi = pd.DataFrame(getFlowByActivity("Census_EC_PxI", year))
    pxi["napcs"] = pxi["FlowName"].astype(str).str.strip()
    naics = pxi["ActivityProducedBy"].astype(str).str.strip()
    pxi = pxi.loc[naics.str.fullmatch(rf"\d{{{NAICS_CODE_LENGTH}}}")].copy()

    if year != BASE_NAPCS_YEAR:
        bridge = pd.read_csv(_NAPCS_BRIDGE, dtype=str)
        to_2017 = dict(zip(bridge["code_2022"], bridge["code_2017"]))
        pxi["napcs"] = pxi["napcs"].map(to_2017).fillna(pxi["napcs"])

    concordance = pd.read_csv(_NAPCS_TO_BEA, dtype={"napcs_code": str})
    concordance = concordance.dropna(subset=["bea_2017_commodity"])
    concordance["bea_2017_commodity"] = (
        concordance["bea_2017_commodity"].astype(str).str.strip()
    )
    concordance["weight"] = pd.to_numeric(
        concordance["weight"], errors="coerce"
    ).fillna(1.0)

    joined = pxi.merge(
        concordance[["napcs_code", "bea_2017_commodity", "weight"]],
        left_on="napcs",
        right_on="napcs_code",
        how="inner",
    )
    joined["value"] = (
        pd.to_numeric(joined["FlowAmount"], errors="coerce").fillna(0.0)
        * joined["weight"]
    )
    return joined.groupby("bea_2017_commodity")["value"].sum()


def pce_bridge_mix() -> pd.Series:
    """Consumption-side product mix by BEA commodity, USD, producers' value.

    ✅ **The demand-side counterpart to PxI, and the right shape for imports.**
    The PCE bridge allocates each NIPA consumption category across BEA
    commodities -- which is the same *product* question BEA answers with its
    HS-to-I-O concordance, asked from the buyer's side rather than the
    producer's.

    Producers' value is used deliberately: ``MCIF`` is a basic-value import
    column, so the margins the bridge carries beside it (transportation,
    wholesale, retail) are not part of the quantity being split.

    ⚠️ **It only covers what households buy.**  Semiconductor machinery,
    aircraft and most capital goods have no PCE line at all, so this arm scores
    a subset of families by construction and its silence on a family is not
    evidence against it.
    """
    bridge = load_2017_pce_bridge_detail_usa()
    code = bridge["Commodity Code"].astype(str).str.strip()
    value = pd.to_numeric(bridge["Producers' Value"], errors="coerce").fillna(0.0)
    # Used autos books a negative scrap line; a negative share is not a mix.
    return value.clip(lower=0.0).groupby(code).sum()


def _l1(ours: pd.Series, published: pd.Series) -> float:
    if ours.sum() <= 0 or published.sum() <= 0:
        return float("nan")
    return 0.5 * float((ours / ours.sum() - published / published.sum()).abs().sum())


def score_families(families: list[str]) -> pd.DataFrame:
    """Within-family L1 against published MCIF, identity arm vs PxI arm."""
    published = _published_mcif()
    current = current_mix()
    pxi = pxi_mix()
    pce = pce_bridge_mix()

    rows = []
    for family in families:
        members = sorted(
            c for c in published.index if str(c)[:4] == family and published[c] > 0
        )
        if len(members) < 2:
            continue
        pub = published.reindex(members).fillna(0.0)
        if float(pub.sum()) < MIN_FAMILY_USD:
            continue
        cur = current.reindex(members).fillna(0.0)
        alt = pxi.reindex(members).fillna(0.0)
        dem = pce.reindex(members).fillna(0.0)
        if float(cur.sum()) <= 0:
            continue

        l1_current = _l1(cur, pub)
        l1_pxi = _l1(alt, pub) if float(alt.sum()) > 0 else float("nan")
        l1_pce = _l1(dem, pub) if float(dem.sum()) > 0 else float("nan")
        rows.append(
            {
                "family": family,
                "members": len(members),
                "published_M": float(pub.sum()) / MILLION,
                "l1_current": l1_current,
                "l1_pxi": l1_pxi,
                "l1_pce": l1_pce,
                "pxi_delta": l1_pxi - l1_current,
                "pce_delta": l1_pce - l1_current,
                "pxi": "BETTER" if l1_pxi < l1_current else "worse",
                "pce": (
                    "n/a"
                    if pd.isna(l1_pce)
                    else ("BETTER" if l1_pce < l1_current else "worse")
                ),
                # dollars the split moves onto the right commodity, at the
                # family's own Census level
                "pxi_gain_M": (l1_current - l1_pxi) * float(cur.sum()) / MILLION,
                "pce_gain_M": (l1_current - l1_pce) * float(cur.sum()) / MILLION,
            }
        )
    return pd.DataFrame(rows).sort_values("pce_gain_M", ascending=False)


def main() -> None:
    published = _published_mcif()
    if "--all" in sys.argv:
        families = sorted({str(c)[:4] for c in published.index if str(c)[:1].isdigit()})
        label = "all families"
    else:
        families = list(TARGET_FAMILIES)
        label = "the families --decompose flagged as mix-dominated"

    print(f"Grading the PxI product split against published {YEAR} MCIF, {label}.")
    print("L1 is within-family share distance; lower is better. Level held fixed.")
    print()
    df = score_families(families)
    if df.empty:
        print("(no family met the reporting floor)")
        return

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(RESPLIT_CSV, index=False)
    with pd.option_context("display.width", 170):
        print(df.to_string(index=False, float_format=lambda x: f"{x:,.3f}"))

    print()
    for arm, gain in (("pxi", "pxi_gain_M"), ("pce", "pce_gain_M")):
        scored = df.loc[df[arm].ne("n/a")]
        better = scored.loc[scored[arm].eq("BETTER")]
        print(
            f"{arm.upper():4s}: {len(better)} of {len(scored)} scoreable families "
            f"improve; net {scored[gain].sum():,.0f}M toward the published split."
        )
    print(f"Wrote {RESPLIT_CSV}")


if __name__ == "__main__":
    main()
