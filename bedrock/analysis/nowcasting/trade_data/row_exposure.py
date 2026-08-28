"""Trade error measured against the commodity ROW it lands on (#670, #701).

``score_2017_trade_detail`` ranks trade error against the import pool.  That is
the right frame for "how good is the trade build", and the wrong one for "will
Step 5 converge to something true", because the balance imposes

    T001[c] = T016[c] - sum_FD Y[c]

**hard**.  ``MCIF`` sits inside ``T016`` and ``F04000`` is one of the ``Y``
columns, so a trade error does not average away across commodities -- it moves
that one commodity's intermediate total one-for-one, and the RAS then converges
by inflating or draining the row to meet the control.

⚠️ **A small share of the pool can be an enormous share of a row.** On the 2017
build ``334610`` magnetic and optical media carries an ``MCIF`` excess of about
10.2 B USD against an intermediate use of 2.3 B -- 448% of its own row.  Ranked
against the 2.2 T USD import pool the same error is 0.5% and invisible.  This
module ranks on the denominator that decides whether the balanced table is
right.

Run from repo root::

    uv run python -m bedrock.analysis.nowcasting.trade_data.row_exposure

    # add the family-headroom view that separates split errors from the rest
    uv run python -m bedrock.analysis.nowcasting.trade_data.row_exposure --family

    # decompose each family's error into a level part and a mix part
    uv run python -m bedrock.analysis.nowcasting.trade_data.row_exposure --decompose

Writes under ``bedrock/analysis/nowcasting/trade_data/output/`` (gitignored).

⚠️ **Read the two columns, not just the net.**  Roughly half the worst rows are
driven by the **export** side, not by an import misroute.  ``336411`` aircraft
carries an ``MCIF`` error of only +4.9 B against an ``F04000`` error of
**-50.1 B**, so its 162% exposure is a missing export column and a product
concordance would do nothing for it.  ``334118`` is wrong on both and they
compound.  Check ``mcif_error_usd`` against ``f04000_error_usd`` before choosing
an instrument.

Reading the three views
-----------------------
**Row exposure** (default) is the triage: ``net_error / own_use``.  Anything
above ~25% is a row the balance cannot repair, and belongs upstream of Step 5
rather than in the accuracy backlog.

**Family headroom** (``--family``) is the targeting.  For each exposed
commodity it asks whether a sibling inside the same 4-digit NAICS family has
enough published ``MCIF`` to legitimately hold the misrouted mass:

- ``excess_vs_family`` well **below 1** -- the #702 signature, a within-family
  split error.  Fixable with a product concordance
  (``utils/mapping/census_pxi/napcs_to_bea_2017.csv``), no new data.
- ``excess_vs_family`` **at or above 1** -- the family cannot hold it, so the
  mass is arriving from another family or being counted twice.  A within-family
  reallocation is the wrong tool; check the wrong-vintage join class (#675)
  before reaching for HS microdata.

**Level vs mix** (``--decompose``) is the decisive one, and it is what says
whether a concordance is the right tool at all.  ``level_ratio`` far from 1.00
with a small ``mix_error`` is a **coverage gap wearing a misroute's clothes** --
the family is short overall and re-splitting inside it recovers nothing.
``concordance_can_fix_M`` is the honest upper bound on what a product
concordance buys for that family.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

from bedrock.analysis.nowcasting.trade_data.probe_2017_trade_totals import (
    bea_matrix_column,
)
from bedrock.analysis.nowcasting.trade_data.score_2017_trade_detail import score
from bedrock.extract.iot.io_2017 import load_benchmark_detail_U_intermediate_usa

YEAR = 2017
OUT_DIR = Path(__file__).resolve().parent / "output"
EXPOSURE_CSV = OUT_DIR / "row_exposure.csv"
FAMILY_CSV = OUT_DIR / "row_exposure_family.csv"
DECOMPOSE_CSV = OUT_DIR / "row_exposure_decomposition.csv"

MILLION = 1e6

#: Report rows whose own intermediate use is at least this, so that a commodity
#: with almost no use does not top the table on a rounding difference.
MIN_USE_USD = 100e6

#: Flagged in the printed table: above this share of its own row, the balance
#: cannot repair the error, it can only redistribute it.
EXPOSURE_FLAG = 0.25


def _published_mcif() -> pd.Series:
    """Published 2017 detail ``MCIF`` by commodity, USD.

    ⚠️ ``bea_matrix_column`` hands back the SUT's own unit, **million USD**,
    while ``score`` and the Use loader are in USD.  Convert here so every
    figure in this module is USD and the ratios are unitless.
    """
    labeled = bea_matrix_column("MCIF", matrix="Supply_SUT_detail")
    frame = labeled.frame
    series = pd.Series(
        pd.to_numeric(frame["value"], errors="coerce").to_numpy(),
        index=frame["code"].astype(str).to_numpy(),
    )
    return series.groupby(level=0).sum().astype(float) * MILLION


def _own_intermediate_use() -> pd.Series:
    """Published 2017 intermediate use per commodity (``T001``), USD."""
    u = load_benchmark_detail_U_intermediate_usa(YEAR)
    use = u.sum(axis=1).astype(float)
    use.index = [i[0] if isinstance(i, tuple) else str(i) for i in use.index]
    return use.groupby(level=0).sum()


def exposure() -> pd.DataFrame:
    """Per-commodity trade error as a share of that commodity's own use.

    ``net_error`` is the displacement the row control actually sees:
    ``MCIF`` error **minus** ``F04000`` error, because an over-large export
    column pushes ``T001`` down while an over-large import column pushes it up.
    A commodity over on both partially cancels, which is why neither column
    scored alone is the right number (#701).
    """
    _, _, detail = score()
    detail = detail.copy()
    detail["commodity"] = detail["commodity"].astype(str)
    detail["error_usd"] = detail["candidate_usd"] - detail["reference_usd"]

    wide = detail.pivot_table(
        index="commodity", columns="direction", values="error_usd", aggfunc="sum"
    ).fillna(0.0)
    for direction in ("imports", "exports"):
        if direction not in wide.columns:
            wide[direction] = 0.0

    levels = detail.loc[detail["direction"].eq("imports")].set_index("commodity")
    use = _own_intermediate_use()
    out = pd.DataFrame(
        {
            "mcif_error_usd": wide["imports"],
            "f04000_error_usd": wide["exports"],
            "mcif_candidate_usd": levels["candidate_usd"]
            .reindex(wide.index)
            .fillna(0.0),
            "mcif_published_usd": levels["reference_usd"]
            .reindex(wide.index)
            .fillna(0.0),
        }
    )
    out["net_row_error_usd"] = out["mcif_error_usd"] - out["f04000_error_usd"]
    out["own_intermediate_use_usd"] = use.reindex(out.index).fillna(0.0)
    out = out.loc[out["own_intermediate_use_usd"] >= MIN_USE_USD]
    out["row_exposure"] = (
        out["net_row_error_usd"].abs() / out["own_intermediate_use_usd"]
    )
    # ``S00*`` are not goods misroutes and do not belong in the same triage:
    # S00300 noncomparable imports is #606, S00401/S00402 scrap and used goods
    # are #703.  Kept in the frame so the row control still balances, flagged
    # so nobody reaches for a trade concordance to fix them.
    out["special"] = pd.Series(
        [str(c).startswith("S00") for c in out.index], index=out.index, dtype=bool
    )
    return out.sort_values("row_exposure", ascending=False)


def family_headroom(commodities: list[str]) -> pd.DataFrame:
    """Is there a sibling in the same 4-digit family big enough to hold it?"""
    mcif = _published_mcif()
    rows = []
    for code in commodities:
        family = code[:4]
        siblings = {
            c: float(v)
            for c, v in mcif.items()
            if len(c) >= 4 and c[:4] == family and c != code
        }
        family_total = sum(siblings.values()) + float(mcif.get(code, 0.0))
        top = sorted(siblings.items(), key=lambda kv: -kv[1])[:2]
        rows.append(
            {
                "commodity": code,
                "family": family,
                "published_mcif_usd": float(mcif.get(code, 0.0)),
                "family_published_mcif_usd": family_total,
                "largest_sibling": (
                    f"{top[0][0]}={top[0][1] / MILLION:,.0f}M" if top else "(none)"
                ),
                "next_sibling": (
                    f"{top[1][0]}={top[1][1] / MILLION:,.0f}M" if len(top) > 1 else "-"
                ),
            }
        )
    return pd.DataFrame(rows)


def family_decomposition(df: pd.DataFrame, families: list[str]) -> pd.DataFrame:
    """Split each family's import error into a LEVEL part and a MIX part.

    This is the question a concordance can and cannot answer:

    - **level** -- ``our family total / published family total``.  If the
      family is short overall, no re-split inside it recovers the difference;
      the mass is arriving under another family or not at all.
    - **mix** -- ``0.5 * sum |our share - published share|`` inside the family,
      which is the fraction of the family's mass sitting on the wrong sibling.
      ✅ **This is the part a product concordance fixes**, and only this part.

    A family with mix near 0 and level far from 1 is a coverage problem
    wearing a misroute's clothes.
    """
    goods = df.loc[~df["special"]]
    rows = []
    for family in families:
        members = [c for c in goods.index if str(c)[:4] == family]
        if not members:
            continue
        cand = goods.loc[members, "mcif_candidate_usd"].astype(float)
        pub = goods.loc[members, "mcif_published_usd"].astype(float)
        cand_total, pub_total = float(cand.sum()), float(pub.sum())
        if pub_total <= 0 or cand_total <= 0:
            mix = float("nan")
        else:
            mix = 0.5 * float((cand / cand_total - pub / pub_total).abs().sum())
        rows.append(
            {
                "family": family,
                "members": len(members),
                "candidate_M": cand_total / MILLION,
                "published_M": pub_total / MILLION,
                "level_ratio": cand_total / pub_total if pub_total else float("nan"),
                "mix_error": mix,
                "concordance_can_fix_M": (mix * min(cand_total, pub_total)) / MILLION,
            }
        )
    return pd.DataFrame(rows).sort_values("concordance_can_fix_M", ascending=False)


def _print_exposure(df: pd.DataFrame, limit: int = 25) -> None:
    show = df.head(limit).copy()
    show["net_error_M"] = show["net_row_error_usd"] / MILLION
    show["own_use_M"] = show["own_intermediate_use_usd"] / MILLION
    show["exposure_pct"] = show["row_exposure"] * 100.0
    show["flag"] = show["row_exposure"].gt(EXPOSURE_FLAG).map({True: "<<", False: ""})
    show["kind"] = show["special"].map({True: "S00*", False: "goods"})
    cols = ["net_error_M", "own_use_M", "exposure_pct", "kind", "flag"]
    with pd.option_context("display.width", 160):
        print(
            show[cols].to_string(
                float_format=lambda x: f"{x:,.1f}",
            )
        )


def main() -> None:
    want_family = "--family" in sys.argv
    want_decompose = "--decompose" in sys.argv
    print(
        f"{YEAR} trade error against each commodity's OWN intermediate use "
        f"(net of F04000; commodities with use >= {MIN_USE_USD / 1e6:,.0f}M)..."
    )
    df = exposure()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(EXPOSURE_CSV)

    over = df.loc[df["row_exposure"] > EXPOSURE_FLAG]
    print()
    _print_exposure(df)
    print()
    print(
        f"{len(over)} of {len(df)} commodities carry a net trade error above "
        f"{EXPOSURE_FLAG:.0%} of their own intermediate use."
    )
    print(f"Wrote {EXPOSURE_CSV}")

    if want_family:
        goods = over.loc[~over["special"]]
        fam = family_headroom(list(goods.index[:20]))
        fam["excess_vs_family"] = (
            goods["net_row_error_usd"].abs().reindex(fam["commodity"]).to_numpy()
            / fam["family_published_mcif_usd"].replace(0.0, float("nan")).to_numpy()
        )
        fam.to_csv(FAMILY_CSV, index=False)
        print()
        print("Family headroom (excess_vs_family < 1 => within-family split error):")
        with pd.option_context("display.width", 200):
            print(fam.to_string(index=False, float_format=lambda x: f"{x:,.2f}"))
        print(f"Wrote {FAMILY_CSV}")

    if want_decompose:
        goods = over.loc[~over["special"]]
        families = sorted({str(c)[:4] for c in goods.index})
        dec = family_decomposition(df, families)
        dec.to_csv(DECOMPOSE_CSV, index=False)
        print()
        print(
            "Level vs mix by family (mix is the only part a concordance fixes; "
            "level_ratio far from 1.00 with low mix = coverage gap, not misroute):"
        )
        with pd.option_context("display.width", 200):
            print(dec.to_string(index=False, float_format=lambda x: f"{x:,.2f}"))
        print(f"Wrote {DECOMPOSE_CSV}")


if __name__ == "__main__":
    main()
