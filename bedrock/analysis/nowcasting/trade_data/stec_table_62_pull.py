"""Pull BEA STEC Table 6.2 (service type x major industry) via iTablecore API.

Source: https://apps.bea.gov/iTablecore/data/app/GetStep
Table 6.2 id = 406 (International Services > STEC).

Writes long-format CSV to output/stec_table_62_imports_{year}.csv
"""

from __future__ import annotations

import json
import re
import subprocess
from html import unescape
from pathlib import Path
from typing import Any

import pandas as pd

OUT_DIR = Path(__file__).resolve().parent / "output"
ITABLE_URL = "https://apps.bea.gov/iTablecore/data/app/GetStep"

# Filter_#1 year ListKey values from BEA iTable metadata.
YEAR_KEYS: dict[int, str] = {
    2012: "14",
    2017: "9",
}

# Single-year filter: one column per major industry (col 3 = all industries).
INDUSTRIES: list[tuple[str, int]] = [
    ("All industries", 3),
    ("Mining", 4),
    ("Manufacturing", 5),
    ("Wholesale trade", 6),
    ("Retail trade", 7),
    ("Information", 8),
    ("Finance and insurance", 9),
    ("Real estate and rental and leasing", 10),
    ("Professional, scientific, and technical services", 11),
]

# Import service-type rows in Table 6.2 (line numbers in BEA table).
IMPORT_SERVICE_ROWS: dict[str, int] = {
    "imports_total_selected": 14,
    "construction": 15,
    "insurance": 16,
    "financial": 17,
    "charges_ip": 18,
    "telecom_computer_info": 19,
    "other_business_total": 20,
    "rd_services": 21,
    "prof_mgmt_consulting": 22,
    "technical_trade_other_business": 23,
    "personal_cultural_recreational": 24,
}


def _curl_post(payload: dict[str, object]) -> dict[str, Any]:
    req_path = OUT_DIR / "_bea_req_tmp.json"
    resp_path = OUT_DIR / "_bea_resp_tmp.json"
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    req_path.write_text(json.dumps(payload), encoding="utf-8")
    subprocess.run(
        [
            "curl.exe",
            "-sL",
            "-X",
            "POST",
            ITABLE_URL,
            "-H",
            "Content-Type: application/json",
            "--data-binary",
            f"@{req_path}",
            "-o",
            str(resp_path),
        ],
        check=True,
    )
    return json.loads(resp_path.read_text(encoding="utf-8"))


def _parse_value(raw: str) -> float | None:
    s = unescape(raw).strip().replace(",", "")
    s = re.sub(r"<[^>]+>", "", s)
    if s in {"", "...", "n.a."}:
        return None
    if s.startswith("(") and s.endswith(")"):
        inner = s[1:-1].strip()
        if inner == "D":
            return None
        if inner == "*":
            return None
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _fetch_table(year: int) -> dict[str, Any]:
    year_key = YEAR_KEYS[year]
    payload = {
        "appid": 62,
        "stepnum": 2,
        "data": [
            ["Product", "4"],
            ["TableList", "406"],
            ["Filter_#1", year_key],
            ["Filter_#2", "0"],
            ["Filter_#3", "0"],
        ],
    }
    step = _curl_post(payload)
    prompt = step["Prompts"][0]
    outer = json.loads(prompt["PromtData"])
    table = json.loads(outer["Table"])
    return table


def _cells_by_row_col(table: dict[str, Any]) -> dict[tuple[int, int], str]:
    out: dict[tuple[int, int], str] = {}
    for cell in table["TD"]:
        out[(int(cell["Row_ID"]), int(cell["Column_ID"]))] = cell["Cell_Value"]
    return out


def pull_imports_year(year: int) -> list[dict[str, object]]:
    cells = _cells_by_row_col(_fetch_table(year))
    rows: list[dict[str, object]] = []
    for service_key, row_id in IMPORT_SERVICE_ROWS.items():
        label = cells.get((row_id, 2), "").strip()
        for industry, col_id in INDUSTRIES:
            val = _parse_value(cells.get((row_id, col_id), ""))
            rows.append(
                {
                    "year": year,
                    "service_type": service_key,
                    "service_label": label,
                    "major_industry": industry.strip(),
                    "musd": val,
                }
            )
    return rows


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    all_rows: list[dict[str, object]] = []
    for year in (2012, 2017):
        year_rows = pull_imports_year(year)
        df = pd.DataFrame(year_rows)
        out_path = OUT_DIR / f"stec_table_62_imports_{year}.csv"
        df.to_csv(out_path, index=False)
        all_rows.extend(year_rows)
        print(f"Wrote {out_path} ({len(df)} rows)")

    combined = pd.DataFrame(all_rows)
    combined_path = OUT_DIR / "stec_table_62_imports_2012_2017.csv"
    combined.to_csv(combined_path, index=False)
    print(f"Wrote {combined_path}")

    # Quick sanity: financial imports by finance industry
    fin = combined[
        (combined["service_type"] == "financial")
        & (combined["major_industry"] == "Finance and insurance")
    ]
    print("\nFinancial services imports, Finance and insurance industry ($M):")
    for _, r in fin.iterrows():
        print(f"  {int(r['year'])}: {r['musd']:,.0f}")


if __name__ == "__main__":
    main()
