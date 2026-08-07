"""
2017 trade Detail compare via ``compare_NIPA_to_IOT`` for bedrock#527.

Same Census+BEA → BEA Detail extract as ``probe_2017_trade_detail.py``, but
scores against the **SUT** -- Use ``F04000`` for exports and Supply ``MCIF``
for imports -- with ``compare()``, so matched-cell disagreement is separated
from unmatched mass (reference holes / extract-only codes).

The SUT is the target because the nowcast builds an SUT; the MUT view is a
later conversion step. ``F05000`` is a MUT-only column and is not a target
here -- see ``_mut_imports_abs_crosscheck``.

Run from repo root::

    uv run python -m bedrock.analysis.nowcasting.trade_data.probe_2017_trade_compare

Writes under ``bedrock/analysis/nowcasting/trade_data/output/``:

* ``probe_2017_trade_compare_imports_cells.csv`` (+ ``*_unmatched.csv``)
* ``probe_2017_trade_compare_exports_cells.csv`` (+ ``*_unmatched.csv``)
* ``probe_2017_trade_compare_reports.txt``
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import pandas as pd

from bedrock.analysis.nowcasting.compare_NIPA_to_IOT import (
    Comparison,
    LabeledSeries,
    bea_matrix_column,
    compare,
    frame_series,
)
from bedrock.analysis.nowcasting.trade_data import probe_2017_trade_detail as detail
from bedrock.utils.config.common import load_env_file_key
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_DESC

OUT_DIR = Path(__file__).resolve().parent / "output"
SUT_USE: Literal["Use_SUT_detail"] = "Use_SUT_detail"
SUPPLY: Literal["Supply_SUT_detail"] = "Supply_SUT_detail"
# Kept only for the cross-check; not a target. See _mut_imports_abs_crosscheck.
MUT: Literal["Use_MUT_detail_after_redef"] = "Use_MUT_detail_after_redef"


def _extract_detail_vectors() -> tuple[pd.Series, pd.Series]:
    """Census CIF goods + mapped BEA services → BEA Detail (million USD)."""
    if not detail.CENSUS_CONC.is_file() or not detail.SERVICE_CONC.is_file():
        raise FileNotFoundError(
            f"USEEIO concordances not found under {detail._USEEIO_CONC}. "
            "Clone cornerstone-data/USEEIO or set path."
        )

    census_key = load_env_file_key("API_Key", "Census")
    bea_key = load_env_file_key("API_Key", "BEA")
    census_conc = pd.read_csv(detail.CENSUS_CONC)
    service_conc = pd.read_csv(detail.SERVICE_CONC)

    print("Fetching Census goods + BEA services...")
    goods_imp = detail._census_table("imports", "GEN_CIF_YR", census_key).assign(
        musd=lambda x: x["usd"] / 1e6
    )
    goods_exp = detail._census_table("exports", "ALL_VAL_YR", census_key).assign(
        musd=lambda x: x["usd"] / 1e6
    )
    svc_imp = detail._bea_services("Imports", bea_key)
    svc_exp = detail._bea_services("Exports", bea_key)
    api_codes = set(service_conc["API BEA Service"].astype(str))
    svc_imp_m = svc_imp.loc[svc_imp["TypeOfService"].isin(api_codes)]
    svc_exp_m = svc_exp.loc[svc_exp["TypeOfService"].isin(api_codes)]

    print("Mapping to BEA Detail...")
    g_imp_d, _ = detail._equal_split_to_detail(
        goods_imp, census_conc, "musd", "NAICS", "NAICS"
    )
    g_exp_d, _ = detail._equal_split_to_detail(
        goods_exp, census_conc, "musd", "NAICS", "NAICS"
    )
    s_imp_d, _ = detail._equal_split_to_detail(
        svc_imp_m.rename(columns={"TypeOfService": "api"}),
        service_conc,
        "musd",
        "api",
        "API BEA Service",
    )
    s_exp_d, _ = detail._equal_split_to_detail(
        svc_exp_m.rename(columns={"TypeOfService": "api"}),
        service_conc,
        "musd",
        "api",
        "API BEA Service",
    )
    return (
        g_imp_d.add(s_imp_d, fill_value=0.0),
        g_exp_d.add(s_exp_d, fill_value=0.0),
    )


def _candidate_from_extract(extract: pd.Series, label: str) -> LabeledSeries:
    """Nonzero Detail cells as a LabeledSeries (million USD) with BEA names."""
    s = extract[extract > 0].astype(float)
    names = USA_2017_COMMODITY_DESC
    df = pd.DataFrame(
        {
            "code": s.index.astype(str),
            "name": [names.get(c, "") for c in s.index.astype(str)],
            "value": s.values,
        }
    )
    return frame_series(
        df, code="code", name="name", value="value", label=label, unit="Million USD"
    )


def _use_exports() -> LabeledSeries:
    """Exports from the SUT Use table, which is the nowcast target."""
    return bea_matrix_column(
        "F04000",
        matrix=SUT_USE,
        label="Use SUT F04000 exports",
    )


def _supply_imports() -> LabeledSeries:
    """Imports from the SUT Supply table.

    The SUT has no import *column* in Use -- ``F05000`` is MUT-only. Imports
    enter on the Supply side as ``MCIF``, already positive, so there is no sign
    flip to undo. ``MCIF`` is the column a nowcast has to reproduce; the MUT
    view of imports is produced later by the SUT->MUT conversion, not here.
    """
    return bea_matrix_column("MCIF", matrix=SUPPLY, label="Supply SUT MCIF imports")


def _mut_imports_abs_crosscheck() -> LabeledSeries:
    """``|F05000|`` from the MUT, kept only as a cross-check.

    Not the target -- a different framework and valuation (PRO, after
    redefinition) -- but the two views reconcile cleanly, which is worth
    recording because it is the whole import side of the SUT->MUT conversion::

        Supply MCIF   2,649,430
        Supply MADJ     -23,116
        MCIF + MADJ   2,626,314
        MUT |F05000|  2,626,305      9 apart, 0.0003%

    So ``|F05000|`` is not a third number to source; it falls out of the two
    Supply columns this probe already targets. Verified 2017.
    """
    ref = bea_matrix_column("F05000", matrix=MUT, label="Use MUT |F05000| imports")
    frame = ref.frame.copy()
    frame["value"] = frame["value"].abs()
    return LabeledSeries(
        frame,
        label=ref.label,
        unit=ref.unit,
        meta={**ref.meta, "sign": "absolute"},
    )


def _run_one(
    flow: str, candidate: LabeledSeries, reference: LabeledSeries
) -> Comparison:
    print(f"\n{'=' * 72}\nComparing {flow}\n{'=' * 72}")
    result = compare(candidate=candidate, reference=reference)
    print(result.report(n_worst=15, tol_pct=5.0, n_unmatched=20))

    r_only = result.alignment.reference_only.sort_values("value", ascending=False)
    if len(r_only):
        use_tot = float(result.totals["reference_total"])
        hole = float(r_only["value"].sum())
        if use_tot:
            print(
                f"\nReference-only mass (Use with no extract code match): "
                f"{hole:,.0f} M USD ({100 * hole / use_tot:.1f}% of Use total)"
            )
        print("Top reference-only:")
        show = r_only.head(15)[["code", "name", "value"]]
        print(show.to_string(index=False, float_format=lambda x: f"{x:,.1f}"))

    return result


def main() -> None:
    extract_imp, extract_exp = _extract_detail_vectors()

    cand_imp = _candidate_from_extract(
        extract_imp, "Census CIF goods + BEA services -> Detail (imports)"
    )
    cand_exp = _candidate_from_extract(
        extract_exp, "Census FAS goods + BEA services -> Detail (exports)"
    )

    print("Loading SUT Use F04000 and Supply MCIF via compare_NIPA_to_IOT...")
    ref_exp = _use_exports()
    ref_imp = _supply_imports()

    result_imp = _run_one("imports", cand_imp, ref_imp)
    result_exp = _run_one("exports", cand_exp, ref_exp)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    paths = []
    for flow, result in (("imports", result_imp), ("exports", result_exp)):
        cells_path = OUT_DIR / f"probe_2017_trade_compare_{flow}_cells.csv"
        result.to_csv(str(cells_path))
        paths.append(cells_path)
        paths.append(Path(str(cells_path).replace(".csv", "_unmatched.csv")))

    report_path = OUT_DIR / "probe_2017_trade_compare_reports.txt"
    report_path.write_text(
        "\n\n".join(
            [
                "=== IMPORTS ===\n" + result_imp.report(n_worst=25, tol_pct=5.0),
                "=== EXPORTS ===\n" + result_exp.report(n_worst=25, tol_pct=5.0),
            ]
        ),
        encoding="utf-8",
    )
    paths.append(report_path)

    print("\nWrote:")
    for p in paths:
        print(f"  {p}")


if __name__ == "__main__":
    main()
