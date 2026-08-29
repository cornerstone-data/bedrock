"""How should a Census export residual be split across a BEA family? (#762)

Census publishes **suppressed-detail residuals** beside its digit-6 NAICS --
``33641X`` carries 121.0 bn of 2017 aircraft exports, more than nine times the
directly-coded aircraft rows put together.  ``Sector_Crosswalk_Census_USATrade``
maps each residual 1:m onto its family, and #729 set the export 1:m attribution
source to ``Detail_Supply`` **``T007``**, commodity output.

Commodity output is not export composition, so the premise of #762 was that
``T007`` is the wrong weight -- ``336414`` guided missiles takes 10,370 M
against 1,899 published, a 5.5x overstatement that only appeared once #758 made
the mass arrive at all.

❌ **Measured, that premise does not hold.**  ``T007`` is the **best** of the
three arms on every residual:

===========  ========  ==========  ========  =============
activity     ``t007``  ``direct``  ``pxi``   residual
===========  ========  ==========  ========  =============
``33641X``   **0.166**  0.386      0.211     120,967 M
``11211X``   **0.036**  --         --        415 M
===========  ========  ==========  ========  =============

✅ **And the aircraft error is mostly not a split problem at all.**  Census
exports for ``3364`` total **134,411 M** against BEA's published ``F04000`` of
**113,759 M** -- a **level** gap of 20,652 M, +18%, before any split question
arises.

⚠️ **That level gap is re-exports (#762), not a valuation adjustment.**  BEA's
I-O export column is net of re-exports so gross trade matches domestic supply
(Concepts and Methods ch. 7); ours is gross.  Census publishes the split as a
``DF`` dimension on the endpoint this module reads -- 2017 re-exports are
**238,801 M, 15.4%** of gross -- and netting them out moves the ``3364`` family
level from **1.182 to 1.057** and the whole goods column from **+18.1% to
-0.5%** against published.

❌ **Do not reach for a national scalar instead.**  BEA's Census-to-BoP
adjustment (NIPA Handbook ch. 8) is about **+0.7%** on 2017 goods exports and
NIPA Table 4.3C (ITA to NIPA: gold, territories and Puerto Rico, statistical
differences) is **-0.66%**.  Both are national aggregates with no commodity
detail, and scaling to raw ITA is worse still -- it was rejected in #647,
because ITA sits above NIPA which sits above the I-O concept.

⚠️ **So the useful follow-up is the re-export removal, not a better weight.**
``T007`` at L1 0.166 is imperfect and no alternative here beats it.

The arms
--------
``t007``
    What the build does today: same-year ``Detail_Supply`` ``T007``.

``direct``
    Split the residual in proportion to the family's **own directly-coded
    export rows**.  Attractive because it answers the question with export
    data rather than output data, and because the residual *is* suppressed
    detail from those same codes.

``pxi``
    ``Census_EC_PxI`` product mix through ``napcs_to_bea_2017.csv``.  ⚠️ PxI
    lost badly as an **import** weight (#758) -- but exports are the
    supply-side flow it is built for, so this is the side where it should have
    a chance.

``published``
    Published ``F04000`` shares.  ❌ **Not a candidate** -- it is the answer
    key, and scores 0.000 by construction.  Reported as the ceiling, so the
    other arms can be read against what is achievable rather than against zero.

The answer key
--------------
Published 2017 detail ``F04000``, sound **in the benchmark year only**.  Scored
as within-family L1 on shares::

    L1 = 0.5 * sum_c | arm_share_c - published_share_c |

⚠️ **A weight that wins here is fitted to 2017 until it is checked on another
span.**  #700 put the 2007 and 2012 benchmark detail panels on disk for exactly
this; treat a win as a candidate, not a decision.  Same caution as #763 on the
import side.

Run from repo root::

    uv run python -m bedrock.analysis.nowcasting.trade_data.export_attribution
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from bedrock.analysis.nowcasting.trade_data.family_resplit import pxi_mix
from bedrock.analysis.nowcasting.trade_data.probe_2017_trade_totals import (
    bea_matrix_column,
)

YEAR = 2017
MILLION = 1e6
OUT_DIR = Path(__file__).resolve().parent / "output"
ATTRIBUTION_CSV = OUT_DIR / "export_attribution.csv"

_ROOT = Path(__file__).resolve().parents[3]
_CROSSWALK = (
    _ROOT
    / "utils"
    / "mapping"
    / "activitytosectormapping"
    / "Sector_Crosswalk_Census_USATrade.csv"
)
_SOURCE_DIR = _ROOT / "extract" / "input_data" / "Census_USATrade"


def _published_f04000() -> pd.Series:
    """Published 2017 detail ``F04000`` by commodity, USD.

    ⚠️ ``bea_matrix_column`` returns the SUT's own unit, **million USD**.
    """
    labeled = bea_matrix_column("F04000", matrix="Use_SUT_detail")
    frame = labeled.frame
    series = pd.Series(
        pd.to_numeric(frame["value"], errors="coerce").to_numpy(),
        index=frame["code"].astype(str).to_numpy(),
    )
    return series.groupby(level=0).sum().astype(float) * MILLION


def _census_exports(year: int = YEAR) -> pd.Series:
    """Census export value by raw NAICS activity, USD."""
    path = _SOURCE_DIR / str(year) / f"Census_USATrade_{year}_exports.csv"
    df = pd.read_csv(path, dtype={"NAICS": str})
    df["NAICS"] = df["NAICS"].astype(str).str.strip()
    if "DF" not in df.columns:
        raise ValueError(
            f'{path.name} missing DF column — delete stale export CSVs under '
            f'extract/input_data/Census_USATrade/ and re-fetch (#762)'
        )
    df["DF"] = df["DF"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    df = df.loc[df["DF"] == "1"]
    value = pd.to_numeric(df["ALL_VAL_YR"], errors="coerce").fillna(0.0)
    return value.groupby(df["NAICS"]).sum()


def _crosswalk() -> pd.DataFrame:
    return pd.read_csv(_CROSSWALK, dtype=str).assign(
        Activity=lambda d: d["Activity"].astype(str).str.strip(),
        Sector=lambda d: d["Sector"].astype(str).str.strip(),
    )


def residual_activities() -> dict[str, list[str]]:
    """Residual Census codes that fan out 1:m, and their BEA targets."""
    crosswalk = _crosswalk()
    residual = crosswalk.loc[~crosswalk["Activity"].str.fullmatch(r"\d{6}")]
    grouped = residual.groupby("Activity")["Sector"].apply(list)
    return {str(a): t for a, t in grouped.items() if len(t) > 1}


def _t007_weight(targets: list[str]) -> pd.Series:
    """Same-year Detail_Supply T007 -- what the build uses today."""
    labeled = bea_matrix_column("T007", matrix="Supply_SUT_detail")
    frame = labeled.frame
    series = (
        pd.Series(
            pd.to_numeric(frame["value"], errors="coerce").to_numpy(),
            index=frame["code"].astype(str).to_numpy(),
        )
        .groupby(level=0)
        .sum()
    )
    return series.reindex(targets).fillna(0.0).astype(float)


def _direct_weight(activity: str, targets: list[str]) -> pd.Series:
    """The family's own directly-coded export rows, mapped to BEA targets."""
    crosswalk = _crosswalk()
    exports = _census_exports()
    family = activity[:4]
    direct = crosswalk.loc[
        crosswalk["Activity"].str.fullmatch(r"\d{6}")
        & crosswalk["Activity"].str.startswith(family)
    ]
    mass: dict[str, float] = dict.fromkeys(targets, 0.0)
    for _, row in direct.iterrows():
        if row["Sector"] in mass:
            mass[row["Sector"]] += float(exports.get(row["Activity"], 0.0))
    return pd.Series(mass).reindex(targets).fillna(0.0)


def _l1(arm: pd.Series, published: pd.Series) -> float:
    if arm.sum() <= 0 or published.sum() <= 0:
        return float("nan")
    return 0.5 * float((arm / arm.sum() - published / published.sum()).abs().sum())


def score() -> pd.DataFrame:
    """One row per residual activity, L1 per arm against published F04000."""
    published = _published_f04000()
    exports = _census_exports()
    pxi = pxi_mix()

    rows = []
    for activity, targets in residual_activities().items():
        targets = sorted(set(targets))
        pub = published.reindex(targets).fillna(0.0)
        if float(pub.sum()) <= 0:
            continue
        arms = {
            "t007": _t007_weight(targets),
            "direct": _direct_weight(activity, targets),
            "pxi": pxi.reindex(targets).fillna(0.0),
            "published": pub,
        }
        row = {
            "activity": activity,
            "targets": len(targets),
            "residual_M": float(exports.get(activity, 0.0)) / MILLION,
            "published_family_M": float(pub.sum()) / MILLION,
        }
        for name, weight in arms.items():
            row[f"l1_{name}"] = _l1(weight, pub)
        rows.append(row)

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    candidates = ["l1_t007", "l1_direct", "l1_pxi"]
    df["best"] = df[candidates].idxmin(axis=1).str.replace("l1_", "", regex=False)
    df["gain_vs_t007_M"] = (df["l1_t007"] - df[candidates].min(axis=1)) * df[
        "residual_M"
    ]
    return df.sort_values("residual_M", ascending=False)


def main() -> None:
    print(
        f"Grading export 1:m residual attribution against published {YEAR} F04000.\n"
        "L1 is within-family share distance; lower is better. "
        "'published' is the answer key, not a candidate."
    )
    df = score()
    if df.empty:
        print("(no 1:m residual activity found)")
        return

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(ATTRIBUTION_CSV, index=False)
    with pd.option_context("display.width", 190):
        print()
        print(df.to_string(index=False, float_format=lambda x: f"{x:,.3f}"))

    beats = df.loc[df["best"].ne("t007")]
    print()
    print(
        f"{len(beats)} of {len(df)} residuals have an arm that beats t007; "
        f"net {df['gain_vs_t007_M'].sum():,.0f}M of residual mass better placed."
    )
    print(f"Wrote {ATTRIBUTION_CSV}")


if __name__ == "__main__":
    main()
