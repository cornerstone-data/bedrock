"""
2017 trade totals probe for bedrock#528.

Loads national goods + services from Census_USATrade and BEA_IEA FBAs
(USD) and compares to 2017 SUT targets — Use F04000 and Supply
MCIF/MADJ/MDTY — plus ITA goods+services control totals from BEA_ITA.

Run from repo root::

    uv run python -m bedrock.analysis.nowcasting.trade_data.probe_2017_trade_totals

Writes ``bedrock/analysis/nowcasting/trade_data/output/probe_2017_trade_totals.csv``.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from bedrock.analysis.nowcasting.compare_NIPA_to_IOT.loaders import bea_matrix_column
from bedrock.extract.bea.BEA_ITA import ita_gs_totals_usd
from bedrock.extract.flowbyactivity import getFlowByActivity

YEAR = 2017
OUT_DIR = Path(__file__).resolve().parent / "output"
OUT_CSV = OUT_DIR / "probe_2017_trade_totals.csv"


@dataclass(frozen=True)
class ExtractTotals:
    census_goods_imports_cif_musd: float
    census_goods_exports_fas_musd: float
    bea_services_imports_musd: float
    bea_services_exports_musd: float
    census_import_naics_rows: int
    census_export_naics_rows: int

    @property
    def combined_imports_musd(self) -> float:
        return self.census_goods_imports_cif_musd + self.bea_services_imports_musd

    @property
    def combined_exports_musd(self) -> float:
        return self.census_goods_exports_fas_musd + self.bea_services_exports_musd


def _usd_to_musd(amount: float) -> float:
    return float(amount) / 1e6


def fetch_extracts() -> ExtractTotals:
    census = getFlowByActivity("Census_USATrade", YEAR)
    bea = getFlowByActivity("BEA_IEA", YEAR)

    census_imp = census.loc[census["FlowName"] == "GEN_CIF_YR"]
    census_exp = census.loc[census["FlowName"] == "ALL_VAL_YR_DOM"]
    bea_imp = bea.loc[
        (bea["FlowName"] == "Imports")
        & (bea["ActivityProducedBy"] == "AllTypesOfService")
    ]
    bea_exp = bea.loc[
        (bea["FlowName"] == "Exports")
        & (bea["ActivityProducedBy"] == "AllTypesOfService")
    ]
    if census_imp.empty or census_exp.empty:
        raise RuntimeError(
            "Census_USATrade FBA missing GEN_CIF_YR or ALL_VAL_YR_DOM rows"
        )
    if bea_imp.empty or bea_exp.empty:
        raise RuntimeError("BEA_IEA FBA missing AllTypesOfService Imports/Exports rows")

    return ExtractTotals(
        census_goods_imports_cif_musd=_usd_to_musd(census_imp["FlowAmount"].sum()),
        census_goods_exports_fas_musd=_usd_to_musd(census_exp["FlowAmount"].sum()),
        bea_services_imports_musd=_usd_to_musd(bea_imp["FlowAmount"].sum()),
        bea_services_exports_musd=_usd_to_musd(bea_exp["FlowAmount"].sum()),
        census_import_naics_rows=int(census_imp["ActivityProducedBy"].nunique()),
        census_export_naics_rows=int(census_exp["ActivityProducedBy"].nunique()),
    )


def fetch_benchmarks() -> dict[str, float]:
    """SUT targets first; the MUT column is kept only as a cross-check.

    The nowcast builds an SUT, so the columns it has to reproduce are Supply
    ``MCIF``, ``MADJ`` and ``MDTY``. ``F05000`` is a MUT-only column that the
    later SUT->MUT conversion produces; scoring against it here would answer a
    different question.
    """
    f040_sut = bea_matrix_column("F04000", matrix="Use_SUT_detail")
    mcif = bea_matrix_column("MCIF", matrix="Supply_SUT_detail")
    madj = bea_matrix_column("MADJ", matrix="Supply_SUT_detail")
    mdty = bea_matrix_column("MDTY", matrix="Supply_SUT_detail")
    f050_mut = bea_matrix_column("F05000", matrix="Use_MUT_detail_after_redef")
    ita = ita_gs_totals_usd(YEAR)
    return {
        "use_F04000_exports_musd": float(f040_sut.total),
        "supply_MCIF_musd": float(mcif.total),
        "supply_MADJ_musd": float(madj.total),
        "supply_MDTY_musd": float(mdty.total),
        "mut_F05000_imports_abs_musd": abs(float(f050_mut.total)),
        "ita_exports_gs_musd": ita["exports"] / 1e6,
        "ita_imports_gs_musd": ita["imports"] / 1e6,
    }


def build_comparison(extract: ExtractTotals, bench: dict[str, float]) -> pd.DataFrame:
    rows = [
        {
            "series": "Census goods imports (CIF, NAICS-6)",
            "million_usd": extract.census_goods_imports_cif_musd,
            "notes": f"n_naics={extract.census_import_naics_rows}",
        },
        {
            "series": "BEA services imports (AllTypesOfService)",
            "million_usd": extract.bea_services_imports_musd,
            "notes": "IntlServTrade AllCountries",
        },
        {
            "series": "Combined extract imports (Census CIF + BEA services)",
            "million_usd": extract.combined_imports_musd,
            "notes": "May double-count freight/insurance vs BOP",
        },
        {
            "series": "Supply MCIF (imports, c.i.f.)",
            "million_usd": bench["supply_MCIF_musd"],
            "notes": "SUT TARGET; BAS / CIF-family, matches GEN_CIF_YR basis",
        },
        {
            "series": "Supply MADJ (c.i.f./f.o.b. adjustment)",
            "million_usd": bench["supply_MADJ_musd"],
            "notes": "SUT target; no direct annual source, see plan open Q7",
        },
        {
            "series": "Supply MDTY (import duties)",
            "million_usd": bench["supply_MDTY_musd"],
            "notes": "SUT target; rate from Census CAL_DUT_YR, level from NIPA",
        },
        {
            "series": "Use MUT |F05000| imports",
            "million_usd": bench["mut_F05000_imports_abs_musd"],
            "notes": "CROSS-CHECK ONLY; MUT-only column, PRO, after redef",
        },
        {
            "series": "ITA imports goods+services",
            "million_usd": bench["ita_imports_gs_musd"],
            "notes": "BOP calendar 2017",
        },
        {
            "series": "Census goods exports (ALL_VAL / FAS-family)",
            "million_usd": extract.census_goods_exports_fas_musd,
            "notes": f"n_naics={extract.census_export_naics_rows}",
        },
        {
            "series": "BEA services exports (AllTypesOfService)",
            "million_usd": extract.bea_services_exports_musd,
            "notes": "IntlServTrade AllCountries",
        },
        {
            "series": "Combined extract exports (Census + BEA services)",
            "million_usd": extract.combined_exports_musd,
            "notes": "",
        },
        {
            "series": "Use MUT F04000 exports",
            "million_usd": bench["use_F04000_exports_musd"],
            "notes": "PRO; SUT PUR total nearly identical",
        },
        {
            "series": "ITA exports goods+services",
            "million_usd": bench["ita_exports_gs_musd"],
            "notes": "BOP calendar 2017",
        },
    ]
    df = pd.DataFrame(rows)
    df["year"] = YEAR
    df["ratio_to_supply_MCIF"] = df["million_usd"] / bench["supply_MCIF_musd"]
    df["ratio_to_use_F040"] = df["million_usd"] / bench["use_F04000_exports_musd"]
    return df


def main() -> None:
    print(f"Loading Census_USATrade + BEA_IEA FBAs for {YEAR}...")
    extract = fetch_extracts()
    print("Loading SUT benchmarks: Use F04000, Supply MCIF/MADJ/MDTY...")
    bench = fetch_benchmarks()
    df = build_comparison(extract, bench)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_CSV, index=False)

    print()
    print(df.to_string(index=False, float_format=lambda x: f"{x:,.1f}"))
    print()
    print(
        "Import combined / Supply MCIF = "
        f"{extract.combined_imports_musd / bench['supply_MCIF_musd']:.3f}"
        "   <- the SUT target"
    )
    print(
        "Import combined / MUT |F050| = "
        f"{extract.combined_imports_musd / bench['mut_F05000_imports_abs_musd']:.3f}"
        "   (cross-check only)"
    )
    print(
        "Export combined / Use F040 = "
        f"{extract.combined_exports_musd / bench['use_F04000_exports_musd']:.3f}"
    )
    print(f"Wrote {OUT_CSV}")


if __name__ == "__main__":
    main()
