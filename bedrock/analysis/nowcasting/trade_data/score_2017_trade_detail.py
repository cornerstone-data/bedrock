"""
2017 nowcast F040 / MCIF scorecard for bedrock#528 Phase 3.

Slices the live ``use_fd_detail_sut`` and ``supply_bridge_detail_sut``
TableMatch columns (USD) and reports national percent, Pearson/Spearman,
top-20 Jaccard, and MISS holes ranked by |reference|.

This is the nowcast-column gate (Trade overlay + S00900 identity on F040;
Trade imports on MCIF). It is not the FBA totals probe and not
``TableMatch.ok()`` on the full FD/bridge blocks.

Run from repo root::

    uv run python -m bedrock.analysis.nowcasting.trade_data.score_2017_trade_detail

Writes under ``bedrock/analysis/nowcasting/trade_data/output/`` (gitignored).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from bedrock.analysis.nowcasting.sections import get_section
from bedrock.analysis.nowcasting.table_match import CellStatus, TableMatch
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES

YEAR = 2017
OUT_DIR = Path(__file__).resolve().parent / "output"
SUMMARY_CSV = OUT_DIR / "score_2017_trade_detail_summary.csv"
HOLES_CSV = OUT_DIR / "score_2017_trade_detail_holes.csv"

#: Pearson specials: Detail codes starting ``S00`` (#557 ``S00xxx`` family).
#: Do not drop ``533000``, wholesale ``423*`` / ``424A00``, or ``484000``.
_FRAME = tuple(USA_2017_COMMODITY_CODES)
_SPECIALS = tuple(c for c in _FRAME if c.startswith("S00"))

#: Document holes at or above this |reference| (USD).
HOLE_CUTOFF_USD = 1e9

_COLUMNS = (
    ("exports", "use_fd_detail_sut", "F04000"),
    ("imports", "supply_bridge_detail_sut", "MCIF"),
)


def _is_special(code: str) -> bool:
    return code.startswith("S00")


def _align(match: TableMatch, col: str) -> tuple[pd.Series, pd.Series, pd.Series]:
    idx = pd.Index(_FRAME, name="commodity")
    cand = match.candidate[col].reindex(idx).fillna(0.0).astype(float)
    ref = match.reference[col].reindex(idx).fillna(0.0).astype(float)
    status = match.status[col].reindex(idx)
    return cand, ref, status


def _corr(a: pd.Series, b: pd.Series, method: str) -> float:
    if method == "spearman":
        a, b = a.rank(), b.rank()
    elif method != "pearson":
        raise ValueError(f"unsupported corr method {method!r}")
    if a.std(ddof=0) == 0 or b.std(ddof=0) == 0:
        return float("nan")
    return float(a.corr(b))


def _top20_jaccard(a: pd.Series, b: pd.Series) -> float:
    ta = set(a.abs().nlargest(20).index)
    tb = set(b.abs().nlargest(20).index)
    union = ta | tb
    return float("nan") if not union else len(ta & tb) / len(union)


def _score_column(match: TableMatch, col: str) -> dict[str, float | int]:
    cand, ref, status = _align(match, col)
    keep = pd.Index([c for c in cand.index if not _is_special(str(c))])
    miss = status.eq(int(CellStatus.MISS))
    return {
        "cand_sum_usd": float(cand.sum()),
        "ref_sum_usd": float(ref.sum()),
        "national_pct": float((cand.sum() - ref.sum()) / abs(ref.sum()) * 100.0),
        "pearson_all": _corr(cand, ref, "pearson"),
        "spearman_all": _corr(cand, ref, "spearman"),
        "pearson_non_special": _corr(cand[keep], ref[keep], "pearson"),
        "spearman_non_special": _corr(cand[keep], ref[keep], "spearman"),
        "jaccard_top20": _top20_jaccard(cand, ref),
        "jaccard_top20_non_special": _top20_jaccard(cand[keep], ref[keep]),
        "n_miss": int(miss.sum()),
        "n_match": int(status.eq(int(CellStatus.MATCH)).sum()),
        "n_partial": int(status.eq(int(CellStatus.PARTIAL)).sum()),
    }


def _holes(match: TableMatch, col: str, direction: str) -> pd.DataFrame:
    cand, ref, status = _align(match, col)
    miss = status.eq(int(CellStatus.MISS))
    holes = pd.DataFrame(
        {
            "direction": direction,
            "column": col,
            "commodity": ref.index,
            "candidate_usd": cand.to_numpy(),
            "reference_usd": ref.to_numpy(),
            "abs_reference_usd": ref.abs().to_numpy(),
        }
    )
    holes = holes.loc[miss.to_numpy() & (holes["abs_reference_usd"] >= HOLE_CUTOFF_USD)]
    return holes.sort_values("abs_reference_usd", ascending=False).reset_index(
        drop=True
    )


def score() -> tuple[pd.DataFrame, pd.DataFrame]:
    rows: list[dict[str, object]] = []
    hole_frames: list[pd.DataFrame] = []
    for direction, section_name, col in _COLUMNS:
        match = get_section(section_name).run(YEAR)
        metrics = _score_column(match, col)
        rows.append(
            {
                "direction": direction,
                "section": section_name,
                "column": col,
                "year": YEAR,
                **metrics,
            }
        )
        hole_frames.append(_holes(match, col, direction))
    summary = pd.DataFrame(rows)
    holes = pd.concat(hole_frames, ignore_index=True) if hole_frames else pd.DataFrame()
    return summary, holes


def main() -> None:
    print(
        f"Scoring {YEAR} nowcast F04000 / MCIF "
        f"(specials for Pearson: {len(_SPECIALS)} S00* codes; "
        f"hole cutoff |ref| >= {HOLE_CUTOFF_USD:,.0f} USD)..."
    )
    summary, holes = score()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    summary.to_csv(SUMMARY_CSV, index=False)
    holes.to_csv(HOLES_CSV, index=False)

    with pd.option_context("display.width", 160, "display.max_columns", 20):
        print()
        print(summary.to_string(index=False))
        print()
        print(f"Holes (|reference| >= {HOLE_CUTOFF_USD / 1e9:.0f} B USD):")
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
    print()
    print(f"Wrote {SUMMARY_CSV}")
    print(f"Wrote {HOLES_CSV}")


if __name__ == "__main__":
    main()
