"""Can a product mix re-split imports inside a NAICS family? (#670)

``row_exposure --decompose`` found families where our import **level** is right
and the **mix** inside the family is wrong.  This module grades two candidate
weights against the published answer key instead of assuming one works.

⚠️ **Read the per-family table, never the aggregate.**  Summed over all 58
families both arms look like a rout -- PxI nets -266,566M and the PCE bridge
-305,856M against published.  That number is an artefact of applying a proxy
**everywhere**, which nobody proposed.  Half the families have an identity
mapping that is already near-exact (median ``l1_current`` 0.067, and 24 of 58
below 0.05), so a proxy can only damage them.

✅ **Where the identity mapping is actually bad, the proxies win, and they win
big.**  The arms sort almost perfectly on ``l1_current``:

=========  ==============  ==========  ==========  =====================
family     l1_current      PxI         PCE         best gain
=========  ==============  ==========  ==========  =====================
``3118``   **0.841**       **0.014**   0.168       +756M
``3259``   **0.575**       0.061       **0.028**   +7,993M
``3361``   **0.533**       0.613       **0.258**   **+57,430M**
``3119``   0.309           **0.189**   0.216       +1,123M
``3332``   0.303           **0.168**   0.191       +3,622M
``3399``   0.218           0.438       **0.058**   +13,885M
``3341``   0.143           0.228       **0.101**   +4,154M
=========  ==============  ==========  ==========  =====================

**14 of 58 families improve under at least one arm**, and they are the families
carrying the mix error that ``row_exposure`` flagged.  ``3361`` alone -- the
motor-vehicle family behind #702 -- moves **57.4bn** onto the published split.

❌ **What is NOT yet established: when to use which arm.**  Choosing the
families by which arm wins *on 2017* is fitting to the answer key, which is the
one thing [[benchmark holdout]] policy forbids.  A rule is only usable if it is
validated on a span the key did not choose -- and #700 put the **2007 and 2012**
benchmark detail panels on disk precisely so that is possible.  Until a
selection rule survives 2012 -> 2017, this module reports a measurement, not a
recommendation.

The two arms
------------
**PxI** (:func:`pxi_mix`) is ``Census_EC_PxI`` joined to
``census_pxi/napcs_to_bea_2017.csv``: the **domestic production** product mix.
It wins on food and chemicals -- families whose imports resemble what US plants
make.

**PCE bridge** (:func:`pce_bridge_mix`) allocates each NIPA consumption
category across BEA commodities: the **consumption** product mix, at producers'
value because ``MCIF`` is a basic-value column.  It wins on consumer durables --
vehicles, misc manufacturing, computers -- which is exactly where you would
expect a demand-side split to carry information, and it covers only what
households buy, so silence on capital goods is not evidence against it.

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


# --- the selection rule and its 2012 holdout (#763) -------------------------

#: The rule's thresholds, declared before looking at any key (#763). A family
#: takes the **PCE arm** when households are the majority buyer of its
#: commodities -- consumer share of total use above 0.5 -- because a
#: demand-side split carries information exactly there; it takes the **PxI
#: arm** when households are a marginal buyer (share below 0.2) and imports
#: resemble what US plants make; between the two, no proxy applies. The
#: consumer share is read off the same-year published Use table's structure --
#: never off the MCIF key being predicted.
PCE_CONSUMER_SHARE = 0.5
PXI_CONSUMER_SHARE = 0.2

#: An arm is credible on a family only if it puts mass on at least this many
#: of the family's members; silence is not a split.
MIN_ARM_MEMBERS = 2


def benchmark_mcif(year: int) -> pd.Series:
    """Published detail ``MCIF`` for a benchmark year, USD, 2017 codes.

    The #700 benchmark panel carries 2007, 2012 and 2017 on the 2017 code
    basis in one frame, so the years difference without a crosswalk.
    """
    from bedrock.extract.iot.io_2017 import (  # noqa: PLC0415
        _load_benchmark_detail_supply_use_usa,
    )

    supply = _load_benchmark_detail_supply_use_usa("Supply_detail", year)
    supply.columns = supply.columns.str.strip()
    return (
        pd.to_numeric(supply["MCIF"], errors="coerce").fillna(0.0).clip(lower=0.0)
        * MILLION
    )


def consumer_share(year: int) -> pd.Series:
    """Per-commodity household share of total use, from the benchmark Use SUT.

    ``F01000 / T019`` on the published table for *year* -- structure the rule
    may look at, because it is not the import key being predicted.
    """
    from bedrock.extract.iot.io_2017 import (  # noqa: PLC0415
        _load_benchmark_detail_supply_use_usa,
    )

    use = _load_benchmark_detail_supply_use_usa("Use_SUT_detail", year)
    use.columns = use.columns.str.strip()
    pce = pd.to_numeric(use["F01000"], errors="coerce").fillna(0.0)
    total = pd.to_numeric(use["T019"], errors="coerce").fillna(0.0)
    return (pce / total.replace(0.0, float("nan"))).fillna(0.0)


def rule_pick(family_members: list[str], share: pd.Series, arm: pd.Series) -> str:
    """Which arm the rule assigns a family: 'pce', 'pxi' or 'none'."""
    weights = arm.reindex(family_members).fillna(0.0)
    covered = int((weights > 0).sum())
    family_share = float(share.reindex(family_members).fillna(0.0).mean())
    if family_share > PCE_CONSUMER_SHARE and covered >= MIN_ARM_MEMBERS:
        return "pce"
    if family_share < PXI_CONSUMER_SHARE:
        return "pxi"
    return "none"


def holdout() -> pd.DataFrame:
    """Grade the rule on the 2012 benchmark the key did not choose (#763).

    Two tests, both against the published **2012** detail ``MCIF``:

    - **selection stability**: the rule's pick per family, computed from the
      2012 Use structure and again from the 2017 one -- a rule is a rule if it
      picks the same arm at both benchmarks;
    - **arm transport**: for rule-picked families, the within-family L1 of the
      2017-vintage arm mix against the 2012 key, beside the **stricter**
      baseline of the published 2017 mix itself carried back
      (``l1_carry``). ⚠️ The arms exist only at 2017 vintage -- 2012 PCE
      bridge and 2012 EC product lines are not extracted -- so this tests
      whether the arm's information is *structural* rather than 2017-fitted,
      against a baseline far harder than the identity split the arm would
      actually replace (``l1_current`` ran 0.2-0.8 on these families at 2017).
    """
    key_2012 = benchmark_mcif(2012)
    key_2017 = benchmark_mcif(2017)
    share_2012 = consumer_share(2012)
    share_2017 = consumer_share(2017)
    pce = pce_bridge_mix()
    pxi = pxi_mix()

    families = sorted({str(c)[:4] for c in key_2012.index if str(c)[:1].isdigit()})
    rows = []
    for family in families:
        members = sorted(
            c for c in key_2012.index if str(c)[:4] == family and key_2012[c] > 0
        )
        if len(members) < 2 or float(key_2012.reindex(members).sum()) < MIN_FAMILY_USD:
            continue
        arms = {"pce": pce, "pxi": pxi}
        pick_2012 = {
            arm: rule_pick(members, share_2012, mix) for arm, mix in arms.items()
        }
        pick12 = (
            "pce"
            if pick_2012["pce"] == "pce"
            else ("pxi" if pick_2012["pxi"] == "pxi" else "none")
        )
        pick_2017v = {
            arm: rule_pick(members, share_2017, mix) for arm, mix in arms.items()
        }
        pick17 = (
            "pce"
            if pick_2017v["pce"] == "pce"
            else ("pxi" if pick_2017v["pxi"] == "pxi" else "none")
        )
        if pick17 == "none" and pick12 == "none":
            continue
        chosen = arms[pick17] if pick17 != "none" else arms[pick12]
        k12 = key_2012.reindex(members).fillna(0.0)
        rows.append(
            {
                "family": family,
                "members": len(members),
                "key_2012_M": float(k12.sum()) / MILLION,
                "pick_2012": pick12,
                "pick_2017": pick17,
                "stable": pick12 == pick17,
                "l1_arm_vs_2012": _l1(chosen.reindex(members).fillna(0.0), k12),
                "l1_carry_vs_2012": _l1(key_2017.reindex(members).fillna(0.0), k12),
            }
        )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["arm_beats_carry"] = df["l1_arm_vs_2012"] < df["l1_carry_vs_2012"]
    return df.sort_values("key_2012_M", ascending=False)


def main() -> None:
    if "--holdout" in sys.argv:
        df = holdout()
        print("#763: the selection rule on the 2012 benchmark the key did not choose.")
        print(
            f"Rule (declared a priori): PCE arm where consumer share > "
            f"{PCE_CONSUMER_SHARE}, PxI where < {PXI_CONSUMER_SHARE}, no proxy "
            f"between; share from the same-year Use structure, never the key."
        )
        print()
        with pd.option_context("display.width", 170):
            print(df.to_string(index=False, float_format=lambda x: f"{x:,.3f}"))
        stable = df["stable"].sum()
        beats = df["arm_beats_carry"].sum()
        print()
        print(
            f"selection stable 2012 vs 2017: {stable}/{len(df)} families; "
            f"arm beats the carried-back 2017 published mix on the 2012 key: "
            f"{beats}/{len(df)}."
        )
        return

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
