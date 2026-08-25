"""
2017 Trade FBS vs SUT F040/MCIF — fast Crosswalk/extract iteration gate.

Scores mapped ``Trade_Exports_2017`` / ``Trade_Imports_2017`` Detail mass
directly against published Use ``F04000`` and Supply ``MCIF``. Does **not**
build ``NIPA_final_dom_uses``, Inventories, or the ``S00900`` F040 identity.

Use this while revising Census/IEA Crosswalks or ``Census_USATrade`` parse.
Use ``score_2017_trade_detail`` (full nowcast columns) before updating the
pinned baseline.

Run from repo root::

    uv run python -m bedrock.analysis.nowcasting.trade_data.score_2017_trade_fbs
    uv run python -m bedrock.analysis.nowcasting.trade_data.score_2017_trade_fbs 336411 336412 311810
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

from bedrock.extract.iot.io_2017 import _load_2017_detail_supply_use_usa
from bedrock.transform.eeio.nowcast import _trade_fbs_commodity_vector
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES

YEAR = 2017
OUT_DIR = Path(__file__).resolve().parent / "output"
SUMMARY_CSV = OUT_DIR / "score_2017_trade_fbs_summary.csv"
DETAIL_CSV = OUT_DIR / "score_2017_trade_fbs_detail.csv"
HOLE_CUTOFF_USD = 1e9
_FRAME = tuple(USA_2017_COMMODITY_CODES)


def _is_special(code: str) -> bool:
    return code.startswith("S00")


def _status(cand: float, ref: float) -> str:
    if abs(ref) < 1.0 and abs(cand) < 1.0:
        return "ZERO"
    if abs(ref) >= 1.0 and abs(cand) < 1.0:
        return "MISS"
    if abs(ref) < 1.0 and abs(cand) >= 1.0:
        return "EXTRA"
    ratio = abs(cand / ref) if ref else float("inf")
    if 0.95 <= ratio <= 1.05:
        return "MATCH"
    return "PARTIAL"


def _corr(a: pd.Series, b: pd.Series) -> float:
    if a.std(ddof=0) == 0 or b.std(ddof=0) == 0:
        return float("nan")
    return float(a.corr(b))


def _score_side(
    direction: str,
    cand: pd.Series,
    ref: pd.Series,
) -> tuple[dict[str, float | int | str], pd.DataFrame]:
    idx = pd.Index(_FRAME, name="commodity")
    cand = cand.reindex(idx).fillna(0.0).astype(float)
    ref = ref.reindex(idx).fillna(0.0).astype(float)
    keep = pd.Index([c for c in idx if not _is_special(str(c))])
    detail = pd.DataFrame(
        {
            "direction": direction,
            "commodity": idx,
            "candidate_usd": cand.to_numpy(),
            "reference_usd": ref.to_numpy(),
        }
    )
    detail["status"] = [
        _status(c, r) for c, r in zip(detail["candidate_usd"], detail["reference_usd"])
    ]
    summary = {
        "direction": direction,
        "cand_sum_usd": float(cand.sum()),
        "ref_sum_usd": float(ref.sum()),
        "national_pct": float((cand.sum() - ref.sum()) / abs(ref.sum()) * 100.0),
        "pearson_all": _corr(cand, ref),
        "pearson_non_special": _corr(cand[keep], ref[keep]),
        "n_miss": int((detail["status"] == "MISS").sum()),
        "n_extra": int((detail["status"] == "EXTRA").sum()),
        "n_match": int((detail["status"] == "MATCH").sum()),
        "n_partial": int((detail["status"] == "PARTIAL").sum()),
        "note": "Trade FBS only; no S00900 identity on F040",
    }
    return summary, detail


def score() -> tuple[pd.DataFrame, pd.DataFrame]:
    exports = _trade_fbs_commodity_vector(f"Trade_Exports_{YEAR}", False)
    imports = _trade_fbs_commodity_vector(f"Trade_Imports_{YEAR}", False)
    use = _load_2017_detail_supply_use_usa("Use_SUT_detail")
    supply = _load_2017_detail_supply_use_usa("Supply_detail")
    f040 = (
        pd.to_numeric(use["F04000"], errors="coerce").reindex(_FRAME).fillna(0.0)
        * MILLION_CURRENCY_TO_CURRENCY
    )
    mcif = (
        pd.to_numeric(supply["MCIF"], errors="coerce").reindex(_FRAME).fillna(0.0)
        * MILLION_CURRENCY_TO_CURRENCY
    )

    rows: list[dict[str, object]] = []
    details: list[pd.DataFrame] = []
    for direction, cand, ref in (
        ("exports", exports, f040),
        ("imports", imports, mcif),
    ):
        summary, detail = _score_side(direction, cand, ref)
        rows.append(summary)
        details.append(detail)

    return pd.DataFrame(rows), pd.concat(details, ignore_index=True)


def main() -> None:
    focus = [a for a in sys.argv[1:] if not a.startswith("-")]
    print(
        f"Scoring {YEAR} Trade FBS vs SUT F040/MCIF "
        f"(no NIPA / Inventories / S00900; hole cutoff |ref| >= {HOLE_CUTOFF_USD:,.0f})..."
    )
    summary, detail = score()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    summary.to_csv(SUMMARY_CSV, index=False)
    detail.to_csv(DETAIL_CSV, index=False)

    with pd.option_context("display.width", 160, "display.max_columns", 20):
        print()
        print(summary.to_string(index=False))
        print()
        holes = detail.loc[
            (detail["status"] == "MISS")
            & (detail["reference_usd"].abs() >= HOLE_CUTOFF_USD)
        ].sort_values("reference_usd", key=lambda s: s.abs(), ascending=False)
        print(f"MISS holes (|reference| >= {HOLE_CUTOFF_USD / 1e9:.0f} B USD):")
        if holes.empty:
            print("(none)")
        else:
            show = holes.copy()
            show["reference_M"] = show["reference_usd"] / 1e6
            print(
                show[["direction", "commodity", "reference_M"]].to_string(
                    index=False, float_format=lambda x: f"{x:,.1f}"
                )
            )

        codes = focus or [
            "336411",
            "336412",
            "336413",
            "336414",
            "33641A",
            "311810",
            "1121A0",
        ]
        focus_rows = detail.loc[detail["commodity"].isin(codes)].copy()
        focus_rows["cand_M"] = focus_rows["candidate_usd"] / 1e6
        focus_rows["ref_M"] = focus_rows["reference_usd"] / 1e6
        focus_rows["ratio"] = focus_rows.apply(
            lambda r: (r["candidate_usd"] / r["reference_usd"])
            if abs(r["reference_usd"]) >= 1.0
            else float("nan"),
            axis=1,
        )
        print()
        print("Focus commodities:")
        print(
            focus_rows[
                ["direction", "commodity", "cand_M", "ref_M", "ratio", "status"]
            ].to_string(index=False, float_format=lambda x: f"{x:,.2f}")
        )

    print()
    print(f"Wrote {SUMMARY_CSV}")
    print(f"Wrote {DETAIL_CSV}")


if __name__ == "__main__":
    main()
