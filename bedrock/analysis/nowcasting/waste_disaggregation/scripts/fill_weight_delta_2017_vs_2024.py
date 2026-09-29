"""Build 2017-CSV vs 2024-derived waste-weight delta tables for reports.

Writes markdown into ``<!-- AUTO:WEIGHT_DELTA_2017_VS_2024 -->`` markers in
the by-year and 2024 companion impact reports. Also caches a CSV under
``cache/weight_delta_2017_vs_2024.csv``.

Shares are percent of the parent waste split among the seven Cornerstone
children (sum to 1 within each vector).
"""

from __future__ import annotations

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
CACHE_CSV = PKG / "cache" / "weight_delta_2017_vs_2024.csv"
BY_YEAR_MD = PKG / "impact_nowcast_updated_weights_by_year.md"
COMPANION_MD = PKG / "impact_nowcast_updated_weights.md"

_MARKER_RE = re.compile(
    r"<!-- AUTO:WEIGHT_DELTA_2017_VS_2024 -->.*?<!-- /AUTO:WEIGHT_DELTA_2017_VS_2024 -->",
    re.DOTALL,
)

CHILDREN = list(WASTE_CHILDREN)


def _child_vector_from_parent_row(tbl: pd.DataFrame) -> pd.Series:
    """Extract 7-child shares from a 1×N (or N×1) DisaggWeights slice."""
    if "562000" in tbl.index:
        s = tbl.loc["562000"]
    elif "562000" in tbl.columns:
        s = tbl["562000"]
    else:
        # already child×child or child-indexed
        if set(CHILDREN).issubset(set(tbl.columns)):
            s = tbl[CHILDREN].iloc[0] if len(tbl) == 1 else tbl.sum(axis=0)
        elif set(CHILDREN).issubset(set(tbl.index)):
            s = tbl.loc[CHILDREN].iloc[:, 0] if tbl.shape[1] == 1 else tbl.sum(axis=1)
        else:
            raise KeyError(f"Cannot find 562000/children in table {tbl.shape}")
    return pd.Series({c: float(s.get(c, 0.0)) for c in CHILDREN}, dtype=float)


def _load_2017_weights() -> DisaggWeights:
    cfg = cornerstone_bundled_waste_disagg_config()
    # Config paths are relative to the bedrock package root.
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
    s2024: pd.Series,
    *,
    col_2017: str,
    col_2024: str,
) -> str:
    lines = [
        f"**{title}**",
        "",
        f"| Child | {col_2017} | {col_2024} | Δ (pp) |",
        "|-------|----------:|----------:|-------:|",
    ]
    for c in CHILDREN:
        a = float(s2017.get(c, 0.0))
        b = float(s2024.get(c, 0.0))
        lines.append(f"| {c} | {_pct(a)} | {_pct(b)} | {_pp(b - a)} |")
    lines.append("")
    return "\n".join(lines)


def _intersection_summary(m2017: pd.DataFrame, m2024: pd.DataFrame) -> str:
    a = m2017.reindex(index=CHILDREN, columns=CHILDREN).fillna(0.0).astype(float)
    b = m2024.reindex(index=CHILDREN, columns=CHILDREN).fillna(0.0).astype(float)
    delta = b - a
    flat = delta.stack()
    imax_obj = flat.abs().idxmax()
    if not isinstance(imax_obj, tuple) or len(imax_obj) != 2:
        raise TypeError(f"Expected (row, col) MultiIndex label, got {imax_obj!r}")
    recv = str(imax_obj[0])
    ship = str(imax_obj[1])
    vmax = float(flat.loc[imax_obj])
    diag_lines = [
        "| Child | 2017 diag | 2024 diag | Δ (pp) |",
        "|-------|----------:|----------:|-------:|",
    ]
    for c in CHILDREN:
        d0 = float(a.at[c, c])
        d1 = float(b.at[c, c])
        diag_lines.append(f"| {c} | {_pct(d0)} | {_pct(d1)} | {_pp(d1 - d0)} |")
    return (
        "**Use waste×waste intersection (shipper→receiver shares)**\n\n"
        f"Max abs cell Δ: `{recv}`←`{ship}` = {_pp(vmax)} "
        f"(2017={_pct(float(a.at[recv, ship]))}, "
        f"2024={_pct(float(b.at[recv, ship]))}).\n\n"
        "Diagonal cells (receiver = shipper):\n\n" + "\n".join(diag_lines) + "\n"
    )


def build_delta_frame(
    use_col_17: pd.Series,
    use_col_24: pd.Series,
    use_row_17: pd.Series,
    use_row_24: pd.Series,
    make_col_17: pd.Series,
    make_col_24: pd.Series,
) -> pd.DataFrame:
    rows = []
    for c in CHILDREN:
        rows.append(
            {
                "child": c,
                "use_col_2017": float(use_col_17.get(c, 0.0)),
                "use_col_2024": float(use_col_24.get(c, 0.0)),
                "use_col_delta": float(use_col_24.get(c, 0.0) - use_col_17.get(c, 0.0)),
                "use_row_2017": float(use_row_17.get(c, 0.0)),
                "use_row_2024": float(use_row_24.get(c, 0.0)),
                "use_row_delta": float(use_row_24.get(c, 0.0) - use_row_17.get(c, 0.0)),
                "make_col_2017": float(make_col_17.get(c, 0.0)),
                "make_col_2024": float(make_col_24.get(c, 0.0)),
                "make_col_delta": float(
                    make_col_24.get(c, 0.0) - make_col_17.get(c, 0.0)
                ),
            }
        )
    return pd.DataFrame(rows)


def render_markdown_block(
    use_col_17: pd.Series,
    use_col_24: pd.Series,
    use_row_17: pd.Series,
    use_row_24: pd.Series,
    make_col_17: pd.Series,
    make_col_24: pd.Series,
    inter_md: str,
    provenance_note: str,
) -> str:
    body = "\n".join(
        [
            "<!-- AUTO:WEIGHT_DELTA_2017_VS_2024 -->",
            "",
            "Comparison of **bundled 2017 production weight CSVs** (control) vs "
            "**2024 derived** weights (treatment `match_io`). Values are percent "
            "shares among the seven Cornerstone waste children (each vector sums "
            "to ~100%). Δ is 2024 − 2017 in percentage points.",
            "",
            provenance_note,
            "",
            _vector_table(
                "Industry mix — Use column sum (industry output)",
                use_col_17,
                use_col_24,
                col_2017="2017 CSV",
                col_2024="2024 derive",
            ),
            _vector_table(
                "Commodity mix — Use row sum (commodity output)",
                use_row_17,
                use_row_24,
                col_2017="2017 CSV",
                col_2024="2024 derive",
            ),
            _vector_table(
                "Make column sum",
                make_col_17,
                make_col_24,
                col_2017="2017 CSV",
                col_2024="2024 derive",
            ),
            inter_md,
            "<!-- /AUTO:WEIGHT_DELTA_2017_VS_2024 -->",
        ]
    )
    return body


def compute() -> str:
    w17 = _load_2017_weights()
    w24, prov = derive_waste_weights(2024, mut_dollar_year=2024)

    use_col_17 = _child_vector_from_parent_row(w17.use_disagg_industry_columns_all_rows)
    use_col_24 = _child_vector_from_parent_row(w24.use_disagg_industry_columns_all_rows)
    use_row_17 = _child_vector_from_parent_row(
        w17.use_disagg_commodity_rows_all_columns
    )
    use_row_24 = _child_vector_from_parent_row(
        w24.use_disagg_commodity_rows_all_columns
    )
    make_col_17 = _child_vector_from_parent_row(
        w17.make_disagg_commodity_columns_all_rows
    )
    make_col_24 = _child_vector_from_parent_row(
        w24.make_disagg_commodity_columns_all_rows
    )

    df = build_delta_frame(
        use_col_17, use_col_24, use_row_17, use_row_24, make_col_17, make_col_24
    )
    CACHE_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(CACHE_CSV, index=False)

    notes = "; ".join(prov.fallback_notes[:6]) if prov.fallback_notes else "none"
    provenance_note = (
        f"_2024 derive provenance:_ RCRA={prov.rcra_source_year}, "
        f"EC={prov.ec_source_year}, SAS={prov.sas_source_year}, "
        f"AIES={prov.aies_source_year}/{prov.aies_table}; notes: {notes}."
    )
    inter_md = _intersection_summary(w17.use_intersection, w24.use_intersection)
    return render_markdown_block(
        use_col_17,
        use_col_24,
        use_row_17,
        use_row_24,
        make_col_17,
        make_col_24,
        inter_md,
        provenance_note,
    )


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


def main() -> int:
    block = compute()
    _upsert(BY_YEAR_MD, block)
    _upsert(COMPANION_MD, block)
    print(f"WROTE {CACHE_CSV}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
