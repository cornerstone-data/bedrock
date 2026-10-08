"""Fill auto sections in impact_nowcast_updated_weights_by_year.md from GCS evidence."""

from __future__ import annotations

import json
import re
from pathlib import Path

from bedrock.analysis.nowcasting.waste_disaggregation.impact_pins import (
    GCS_MUT_VINTAGE,
    IMPACT_YEARS,
)
from bedrock.analysis.nowcasting.waste_disaggregation.scripts.run_waste_weight_impact_efs import (  # noqa: E501
    _safe_vintage_label,
    impact_cache_dir,
)

REPORT = (
    Path(__file__).resolve().parents[1] / "impact_nowcast_updated_weights_by_year.md"
)
FIG_DIR = Path(__file__).resolve().parents[1] / "figures"

_SUMMARY_RE = re.compile(
    r"<!-- AUTO:PER_YEAR_SUMMARY -->.*?<!-- /AUTO:PER_YEAR_SUMMARY -->",
    re.DOTALL,
)
_FIGURES_RE = re.compile(
    r"<!-- AUTO:FIGURES_BY_YEAR -->.*?<!-- /AUTO:FIGURES_BY_YEAR -->",
    re.DOTALL,
)


def _row(year: int) -> str:
    summary_path = impact_cache_dir(year, GCS_MUT_VINTAGE) / "summary.json"
    if not summary_path.is_file():
        return f"| {year} | _pending_ | _pending_ | _pending_ | |"
    data = json.loads(summary_path.read_text(encoding="utf-8"))
    n = data.get("n_stats") or {}
    med = n.get("median_pct")
    p95 = n.get("p95_abs_pct")
    waste = data.get("waste_sectors") or []
    best = None
    for w in waste:
        perc = w.get("N_perc_diff")
        if perc is None:
            continue
        if best is None or abs(perc) > abs(best[1]):
            best = (w.get("sector") or w.get("index"), float(perc))
    med_s = f"{med:.2f}%" if med is not None else "_pending_"
    p95_s = f"{p95:.2f}%" if p95 is not None else "_pending_"
    mover = f"{best[0]} ({best[1] * 100:+.1f}%)" if best is not None else "_pending_"
    note = "EC freeze" if year <= 2021 else ("EC 2022" if year == 2022 else "AIES")
    return f"| {year} | {med_s} | {p95_s} | {mover} | {note} |"


def _fig_prefix(year: int) -> str:
    return f"impact_{year}_{_safe_vintage_label(GCS_MUT_VINTAGE)}"


ISSUE_FIGURES_URL = "https://github.com/cornerstone-data/bedrock/issues/1031"


def _year_figures(year: int) -> str:
    prefix = _fig_prefix(year)
    names = (
        f"{prefix}_N_perc_diff_hist.png",
        f"{prefix}_D_perc_diff_hist.png",
        f"{prefix}_waste_sectors_N_D_pct.png",
    )
    lines = [f"### {year}", ""]
    missing = [n for n in names if not (FIG_DIR / n).is_file()]
    if missing:
        lines.append(f"_Figures pending locally: {', '.join(missing)}_")
        lines.append("")
    lines.append(
        f"Charts (N / D histograms + waste-sector bars): "
        f"[issue #1031]({ISSUE_FIGURES_URL}) — `{prefix}_*.png` "
        "(local copies under `figures/` stay untracked)."
    )
    lines.append("")
    return "\n".join(lines)


def _summary_block() -> str:
    header = (
        "| Year | N median % | N p95 \\|%\\| | Waste N max \\|%\\| mover | Notes |\n"
        "|------|------------|-------------|-------------------------|-------|\n"
    )
    rows = "\n".join(_row(y) for y in IMPACT_YEARS)
    return (
        "<!-- AUTO:PER_YEAR_SUMMARY -->\n"
        f"{header}{rows}\n"
        "<!-- /AUTO:PER_YEAR_SUMMARY -->"
    )


def _figures_block() -> str:
    figs = "\n".join(_year_figures(y) for y in IMPACT_YEARS)
    return "<!-- AUTO:FIGURES_BY_YEAR -->\n" f"{figs}" "<!-- /AUTO:FIGURES_BY_YEAR -->"


def main() -> int:
    if not REPORT.is_file():
        raise SystemExit(f"Missing report: {REPORT}")
    text = REPORT.read_text(encoding="utf-8")
    if not _SUMMARY_RE.search(text) or not _FIGURES_RE.search(text):
        raise SystemExit(
            "Report missing AUTO markers "
            "(PER_YEAR_SUMMARY / FIGURES_BY_YEAR); refusing to overwrite prose."
        )
    text = _SUMMARY_RE.sub(_summary_block(), text, count=1)
    text = _FIGURES_RE.sub(_figures_block(), text, count=1)
    REPORT.write_text(text, encoding="utf-8")
    print(f"UPDATED {REPORT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
