"""Build 2017-CSV vs year-Y derived waste-weight delta tables.

Default year=2024 preserves the prior path: writes markdown into
``<!-- AUTO:WEIGHT_DELTA_2017_VS_2024 -->`` markers and
``cache/weight_delta_2017_vs_2024.csv``.

For any year Y (Phase 3.2.B extremes: 2021, 2022):
``cache/weight_delta_2017_vs_{Y}.csv`` with build_delta_frame columns plus
intersection diagonal Δ and locked dominating-slice label.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd

import bedrock.extract.disaggregation.waste_weight_config as waste_weight_config
from bedrock.extract.disaggregation.derive_waste_weights import derive_waste_weights
from bedrock.extract.disaggregation.disagg_weights import (
    DisaggWeights,
    load_disagg_weights,
)
from bedrock.extract.disaggregation.waste_static_rules import WASTE_CHILDREN
from bedrock.extract.disaggregation.waste_weight_config import (
    cornerstone_bundled_waste_disagg_config,
)

PKG = Path(__file__).resolve().parents[1]
BY_YEAR_MD = PKG / "impact_nowcast_updated_weights_by_year.md"
COMPANION_MD = PKG / "impact_nowcast_updated_weights.md"

_MARKER_RE = re.compile(
    r"<!-- AUTO:WEIGHT_DELTA_2017_VS_2024 -->.*?<!-- /AUTO:WEIGHT_DELTA_2017_VS_2024 -->",
    re.DOTALL,
)

CHILDREN = list(WASTE_CHILDREN)

# Locked Phase 3.2.B dominating-slice vocabulary
SLICE_INDUSTRY = "industry_mix"  # use_col
SLICE_WHO_BUYS = "who_buys"  # use_row / FD
SLICE_INTERSECTION = "intersection"  # diagonal cell


def cache_csv_path(year: int) -> Path:
    return PKG / "cache" / f"weight_delta_2017_vs_{year}.csv"


def _child_vector_from_parent_row(tbl: pd.DataFrame) -> pd.Series:
    """Extract 7-child shares from a 1×N (or N×1) DisaggWeights slice."""
    if "562000" in tbl.index:
        s = tbl.loc["562000"]
    elif "562000" in tbl.columns:
        s = tbl["562000"]
    else:
        if set(CHILDREN).issubset(set(tbl.columns)):
            s = tbl[CHILDREN].iloc[0] if len(tbl) == 1 else tbl.sum(axis=0)
        elif set(CHILDREN).issubset(set(tbl.index)):
            s = tbl.loc[CHILDREN].iloc[:, 0] if tbl.shape[1] == 1 else tbl.sum(axis=1)
        else:
            raise KeyError(f"Cannot find 562000/children in table {tbl.shape}")
    return pd.Series({c: float(s.get(c, 0.0)) for c in CHILDREN}, dtype=float)


def _load_2017_weights() -> DisaggWeights:
    cfg = cornerstone_bundled_waste_disagg_config()
    pkg_root = Path(waste_weight_config.__file__).resolve().parents[2]
    cfg = cfg.model_copy(
        update={
            "use_weights_file": str(pkg_root / cfg.use_weights_file),
            "make_weights_file": str(pkg_root / cfg.make_weights_file),
        }
    )
    return load_disagg_weights(
        cfg,
        original_code="562000",
        new_codes=CHILDREN,
        disagg_sectors=CHILDREN,
    )


def _pct(x: float) -> str:
    return f"{100.0 * x:.2f}%"


def _pp(x: float) -> str:
    return f"{100.0 * x:+.2f} pp"


def _vector_table(
    title: str,
    s2017: pd.Series,
    s_year: pd.Series,
    *,
    col_2017: str,
    col_year: str,
) -> str:
    lines = [
        f"**{title}**",
        "",
        f"| Child | {col_2017} | {col_year} | Δ (pp) |",
        "|-------|----------:|----------:|-------:|",
    ]
    for c in CHILDREN:
        a = float(s2017.get(c, 0.0))
        b = float(s_year.get(c, 0.0))
        lines.append(f"| {c} | {_pct(a)} | {_pct(b)} | {_pp(b - a)} |")
    lines.append("")
    return "\n".join(lines)


def _intersection_summary(
    m2017: pd.DataFrame, m_year: pd.DataFrame, *, year: int
) -> str:
    a = m2017.reindex(index=CHILDREN, columns=CHILDREN).fillna(0.0).astype(float)
    b = m_year.reindex(index=CHILDREN, columns=CHILDREN).fillna(0.0).astype(float)
    delta = b - a
    flat = delta.stack()
    imax_obj = flat.abs().idxmax()
    if not isinstance(imax_obj, tuple) or len(imax_obj) != 2:
        raise TypeError(f"Expected (row, col) MultiIndex label, got {imax_obj!r}")
    recv = str(imax_obj[0])
    ship = str(imax_obj[1])
    vmax = float(flat.loc[imax_obj])
    diag_lines = [
        f"| Child | 2017 diag | {year} diag | Δ (pp) |",
        "|-------|----------:|----------:|-------:|",
    ]
    for c in CHILDREN:
        d0 = float(a.at[c, c])
        d1 = float(b.at[c, c])
        diag_lines.append(f"| {c} | {_pct(d0)} | {_pct(d1)} | {_pp(d1 - d0)} |")
    return (
        "**Use waste×waste intersection (shipper→receiver shares)**\n\n"
        f"Max abs cell Δ (context only; not dominating-slice input): "
        f"`{recv}`←`{ship}` = {_pp(vmax)} "
        f"(2017={_pct(float(a.at[recv, ship]))}, "
        f"{year}={_pct(float(b.at[recv, ship]))}).\n\n"
        "Diagonal cells (receiver = shipper):\n\n" + "\n".join(diag_lines) + "\n"
    )


def intersection_diag_delta(m2017: pd.DataFrame, m_year: pd.DataFrame) -> pd.Series:
    """Locked Phase 3.2.B intersection Δ: diagonal cell only."""
    a = m2017.reindex(index=CHILDREN, columns=CHILDREN).fillna(0.0).astype(float)
    b = m_year.reindex(index=CHILDREN, columns=CHILDREN).fillna(0.0).astype(float)
    return pd.Series({c: float(b.at[c, c] - a.at[c, c]) for c in CHILDREN}, dtype=float)


def dominating_slice(
    use_col_delta: float, use_row_delta: float, intersection_diag_d: float
) -> str:
    """Argmax of absolute child-share Δ among industry / who-buys / intersection."""
    scores = {
        SLICE_INDUSTRY: abs(use_col_delta),
        SLICE_WHO_BUYS: abs(use_row_delta),
        SLICE_INTERSECTION: abs(intersection_diag_d),
    }
    return max(scores, key=lambda k: scores[k])


def build_delta_frame(
    use_col_17: pd.Series,
    use_col_y: pd.Series,
    use_row_17: pd.Series,
    use_row_y: pd.Series,
    make_col_17: pd.Series,
    make_col_y: pd.Series,
    *,
    year: int = 2024,
) -> pd.DataFrame:
    """Legacy-compatible frame (no intersection). Prefer build_delta_frame_full."""
    y = str(year)
    rows = []
    for c in CHILDREN:
        rows.append(
            {
                "child": c,
                "use_col_2017": float(use_col_17.get(c, 0.0)),
                f"use_col_{y}": float(use_col_y.get(c, 0.0)),
                "use_col_delta": float(use_col_y.get(c, 0.0) - use_col_17.get(c, 0.0)),
                "use_row_2017": float(use_row_17.get(c, 0.0)),
                f"use_row_{y}": float(use_row_y.get(c, 0.0)),
                "use_row_delta": float(use_row_y.get(c, 0.0) - use_row_17.get(c, 0.0)),
                "make_col_2017": float(make_col_17.get(c, 0.0)),
                f"make_col_{y}": float(make_col_y.get(c, 0.0)),
                "make_col_delta": float(
                    make_col_y.get(c, 0.0) - make_col_17.get(c, 0.0)
                ),
            }
        )
    return pd.DataFrame(rows)


def build_delta_frame_full(
    use_col_17: pd.Series,
    use_col_y: pd.Series,
    use_row_17: pd.Series,
    use_row_y: pd.Series,
    make_col_17: pd.Series,
    make_col_y: pd.Series,
    m2017: pd.DataFrame,
    m_year: pd.DataFrame,
    *,
    year: int,
) -> pd.DataFrame:
    a = m2017.reindex(index=CHILDREN, columns=CHILDREN).fillna(0.0).astype(float)
    b = m_year.reindex(index=CHILDREN, columns=CHILDREN).fillna(0.0).astype(float)
    idelta = intersection_diag_delta(m2017, m_year)
    y = str(year)
    rows = []
    for c in CHILDREN:
        uc_d = float(use_col_y.get(c, 0.0) - use_col_17.get(c, 0.0))
        ur_d = float(use_row_y.get(c, 0.0) - use_row_17.get(c, 0.0))
        id_d = float(idelta.get(c, 0.0))
        rows.append(
            {
                "child": c,
                "use_col_2017": float(use_col_17.get(c, 0.0)),
                f"use_col_{y}": float(use_col_y.get(c, 0.0)),
                "use_col_delta": uc_d,
                "use_row_2017": float(use_row_17.get(c, 0.0)),
                f"use_row_{y}": float(use_row_y.get(c, 0.0)),
                "use_row_delta": ur_d,
                "make_col_2017": float(make_col_17.get(c, 0.0)),
                f"make_col_{y}": float(make_col_y.get(c, 0.0)),
                "make_col_delta": float(
                    make_col_y.get(c, 0.0) - make_col_17.get(c, 0.0)
                ),
                "intersection_diag_2017": float(a.at[c, c]),
                f"intersection_diag_{y}": float(b.at[c, c]),
                "intersection_diag_delta": id_d,
                "dominating_slice": dominating_slice(uc_d, ur_d, id_d),
            }
        )
    return pd.DataFrame(rows)


def render_markdown_block(
    use_col_17: pd.Series,
    use_col_y: pd.Series,
    use_row_17: pd.Series,
    use_row_y: pd.Series,
    make_col_17: pd.Series,
    make_col_y: pd.Series,
    inter_md: str,
    provenance_note: str,
    *,
    year: int,
) -> str:
    body = "\n".join(
        [
            "<!-- AUTO:WEIGHT_DELTA_2017_VS_2024 -->",
            "",
            "Comparison of **bundled 2017 production weight CSVs** (control) vs "
            f"**{year} derived** weights (treatment `match_io`). Values are percent "
            "shares among the seven Cornerstone waste children (each vector sums "
            f"to ~100%). Δ is {year} − 2017 in percentage points.",
            "",
            provenance_note,
            "",
            _vector_table(
                "Industry mix — Use column sum (industry output)",
                use_col_17,
                use_col_y,
                col_2017="2017 CSV",
                col_year=f"{year} derive",
            ),
            _vector_table(
                "Commodity mix — Use row sum (commodity output)",
                use_row_17,
                use_row_y,
                col_2017="2017 CSV",
                col_year=f"{year} derive",
            ),
            _vector_table(
                "Make column sum",
                make_col_17,
                make_col_y,
                col_2017="2017 CSV",
                col_year=f"{year} derive",
            ),
            inter_md,
            "<!-- /AUTO:WEIGHT_DELTA_2017_VS_2024 -->",
        ]
    )
    return body


def compute(year: int = 2024) -> tuple[pd.DataFrame, str]:
    w17 = _load_2017_weights()
    wy, prov = derive_waste_weights(year, mut_dollar_year=year)

    use_col_17 = _child_vector_from_parent_row(w17.use_disagg_industry_columns_all_rows)
    use_col_y = _child_vector_from_parent_row(wy.use_disagg_industry_columns_all_rows)
    use_row_17 = _child_vector_from_parent_row(
        w17.use_disagg_commodity_rows_all_columns
    )
    use_row_y = _child_vector_from_parent_row(wy.use_disagg_commodity_rows_all_columns)
    make_col_17 = _child_vector_from_parent_row(
        w17.make_disagg_commodity_columns_all_rows
    )
    make_col_y = _child_vector_from_parent_row(
        wy.make_disagg_commodity_columns_all_rows
    )

    df = build_delta_frame_full(
        use_col_17,
        use_col_y,
        use_row_17,
        use_row_y,
        make_col_17,
        make_col_y,
        w17.use_intersection,
        wy.use_intersection,
        year=year,
    )
    out_csv = cache_csv_path(year)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)

    notes = "; ".join(prov.fallback_notes[:6]) if prov.fallback_notes else "none"
    provenance_note = (
        f"_{year} derive provenance:_ RCRA={prov.rcra_source_year}, "
        f"EC={prov.ec_source_year}, SAS={prov.sas_source_year}, "
        f"AIES={prov.aies_source_year}/{prov.aies_table}; notes: {notes}."
    )
    inter_md = _intersection_summary(
        w17.use_intersection, wy.use_intersection, year=year
    )
    block = render_markdown_block(
        use_col_17,
        use_col_y,
        use_row_17,
        use_row_y,
        make_col_17,
        make_col_y,
        inter_md,
        provenance_note,
        year=year,
    )
    return df, block


def _upsert(path: Path, block: str) -> None:
    if not path.is_file():
        raise SystemExit(f"Missing report: {path}")
    text = path.read_text(encoding="utf-8")
    if _MARKER_RE.search(text):
        text = _MARKER_RE.sub(block, text, count=1)
    else:
        raise SystemExit(
            f"{path.name} missing AUTO:WEIGHT_DELTA_2017_VS_2024 markers; "
            "add an empty marker pair where the section should go."
        )
    path.write_text(text, encoding="utf-8")
    print(f"UPDATED {path}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--year",
        type=int,
        default=2024,
        help="Treatment derive year Y (default 2024; also upserts markdown)",
    )
    parser.add_argument(
        "--upsert-markdown",
        action="store_true",
        help="Force markdown upsert even when year != 2024",
    )
    args = parser.parse_args(argv)
    year = int(args.year)
    _df, block = compute(year)
    print(f"WROTE {cache_csv_path(year)}")
    if year == 2024 or args.upsert_markdown:
        _upsert(BY_YEAR_MD, block)
        _upsert(COMPANION_MD, block)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
