"""Top sector-level Rho divergences for the report."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

OUT = Path(__file__).resolve().parent / "output"
IO_YEAR = 2017
YEARS = [2022, 2023, 2024]


def _load_rho(name: str) -> pd.DataFrame:
    df = pd.read_csv(OUT / name, index_col=0)
    df.columns = [int(c) for c in df.columns]
    return df


def main() -> None:
    useeior = _load_rho("useeior_Rho.csv")
    path_b = _load_rho("path_b_phoebe_rho.csv")
    path_c1 = _load_rho("path_c1_phoebe_rho_levels.csv")
    path_c2 = _load_rho("path_c2_phoebe_rho_ratio.csv")
    phoebe = _load_rho("phoebe_workbook_Rho.csv") if (OUT / "phoebe_workbook_Rho.csv").is_file() else None

    report: dict = {}
    for year in YEARS:
        common = useeior.index.intersection(path_b.index)
        u = useeior.loc[common, year]
        b = path_b.loc[common, year]
        c1 = path_c1.loc[common, year]
        diff_bu = (b - u).abs().sort_values(ascending=False)
        diff_c1c1 = (c1 - path_b.reindex(c1.index)[year]).abs().sort_values(ascending=False)
        report[str(year)] = {
            "top10_bedrock_ms_vs_useeior": diff_bu.head(10).to_dict(),
            "top10_vnorm_vs_market_shares": diff_c1c1.head(10).to_dict(),
        }
        if phoebe is not None and str(year) in phoebe.columns.astype(str):
            pw = phoebe[str(year)] if str(year) in phoebe.columns else phoebe[year]
            common_p = u.index.intersection(pw.index)
            report[str(year)]["top5_phoebe_wb_vs_useeior_r"] = (
                (pw.reindex(common_p) - u.reindex(common_p)).abs().sort_values(ascending=False).head(5).to_dict()
            )

    # Industry CPI divergence drivers for 2024
    br_ind = pd.read_csv(OUT / "bedrock_phoebe_industry_cpi.csv", index_col=0)
    ui_ind = pd.read_csv(OUT / "useeior_MultiYearIndustryCPI.csv", index_col=0)
    br_ind.columns = [int(c) for c in br_ind.columns]
    ui_ind.columns = [int(c) for c in ui_ind.columns]
    common = br_ind.index.intersection(ui_ind.index)
    d2024 = (br_ind.loc[common, 2024] - ui_ind.loc[common, 2024]).abs().sort_values(ascending=False)
    report["industry_cpi_top10_2024"] = d2024.head(10).to_dict()

    out_path = OUT / "sector_divergence_detail.json"
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
