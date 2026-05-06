"""Reconstruct ``ef_run_index.csv`` from existing Sheets in a Drive folder.

For users who triggered EF diagnostics manually via the GH Actions workflow
(rather than via ``dispatch_ef_time_series.py``), this script lists every
Google Sheet in the diagnostics Drive folder, parses each title to recover
the run dimensions, and writes ``ef_run_index.csv`` with one row per Sheet.

Sheet title formats supported:
- Manual single-cell:
  ``[YYYY-MM-DD, BASELINE based, A matrix with APPROACH] EFs diagnostics``
- Time-series dispatch (``dispatch_ef_time_series.py``):
  ``[YYYY-MM-DD, YYYY, BASELINE based, A matrix with APPROACH, SCENARIO] EFs diagnostics``

Usage:
    python -m bedrock.analysis.a_matrix_time_series.rebuild_run_index_from_drive \\
        --folder-id <DRIVE_FOLDER_ID>
"""

from __future__ import annotations

import argparse
import logging
import re
import typing as ta

import google.auth
import pandas as pd
from googleapiclient.discovery import build

from bedrock.analysis.a_matrix_time_series.constants import RESULTS_DIR

logger = logging.getLogger(__name__)

EF_RUN_INDEX_PATH = RESULTS_DIR / "ef_run_index.csv"

# Output columns match what compile_ef_diagnostics.py + dispatch script
# already use; required = approach, baseline, sheet_id.
INDEX_COLUMNS: tuple[str, ...] = (
    "approach",
    "baseline",
    "sheet_id",
    "sheet_title",
    "created_at",
    "scenario",
    "year",
)

# Title regex:
#   prefix '[' then 1 or 2 leading date-like tokens (date + optional year)
#   then 'BASELINE based, A matrix with APPROACH' and optional trailing ', SCENARIO'
#   then '] EFs diagnostics'
TITLE_RE = re.compile(
    r"""
    ^\[
    (?P<date>\d{4}-\d{2}-\d{2})       # YYYY-MM-DD
    (?:,\s*(?P<year>\d{4}))?          # optional ', YYYY' (time-series cell)
    ,\s*(?P<baseline>[A-Za-z]+)\sbased
    ,\s*A\smatrix\swith\s(?P<approach_text>[^,\]]+)
    (?:,\s*(?P<scenario>[^\]]+))?     # optional ', SCENARIO'
    \]\sEFs\sdiagnostics$
    """,
    re.VERBOSE,
)

# Maps the human-readable approach phrase to the canonical config key used
# in ef_run_index.csv.
APPROACH_BY_TEXT: dict[str, str] = {
    "USEEIO method": "useeio",
    "2017 benchmark A": "useeio",
    "summary tables": "summary_tables",
    "industry price index": "industry_price_index",
    "commodity price index": "commodity_price_index",
}

BASELINE_BY_TEXT: dict[str, str] = {
    "CEDA": "ceda",
    "USEEIO": "useeio",
}


def _drive_client() -> ta.Any:
    """Build a read-only Drive API client."""
    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    return build("drive", "v3", credentials=credentials)


def _list_sheets_in_folder(folder_id: str) -> list[dict[str, str]]:
    """List Google Sheets in the given Drive folder (non-trashed only).

    Returns one dict per Sheet with keys ``id``, ``name``, ``createdTime``.
    """
    if not folder_id:
        raise ValueError("folder_id is required")
    client = _drive_client()
    query = (
        f"'{folder_id}' in parents and trashed=false "
        "and mimeType='application/vnd.google-apps.spreadsheet'"
    )
    files: list[dict[str, str]] = []
    page_token: str | None = None
    while True:
        resp = (
            client.files()
            .list(
                q=query,
                fields="nextPageToken, files(id, name, createdTime)",
                pageSize=1000,
                pageToken=page_token,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
        files.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return files


def _parse_title(title: str) -> dict[str, str] | None:
    """Parse a Sheet title into run-index columns. Returns None if unparseable."""
    m = TITLE_RE.match(title)
    if m is None:
        return None
    approach_text = m.group("approach_text").strip()
    approach = APPROACH_BY_TEXT.get(approach_text)
    if approach is None:
        logger.warning("Unrecognized approach %r in title %r", approach_text, title)
        return None
    baseline_text = m.group("baseline").upper()
    baseline = BASELINE_BY_TEXT.get(baseline_text)
    if baseline is None:
        logger.warning("Unrecognized baseline %r in title %r", baseline_text, title)
        return None
    return {
        "approach": approach,
        "baseline": baseline,
        "sheet_title": title,
        "scenario": (m.group("scenario") or "").strip(),
        "year": m.group("year") or "",
    }


def rebuild_index(folder_id: str, output_path: str | None = None) -> pd.DataFrame:
    """Build ``ef_run_index.csv`` from Sheet titles in ``folder_id``.

    Returns the resulting DataFrame and writes it to ``output_path``
    (default: ``output/results/ef_run_index.csv``).
    """
    out = output_path or str(EF_RUN_INDEX_PATH)
    files = _list_sheets_in_folder(folder_id)
    logger.info("Found %d Sheet(s) in folder %s", len(files), folder_id)

    rows: list[dict[str, str]] = []
    skipped: list[str] = []
    for f in files:
        parsed = _parse_title(f["name"])
        if parsed is None:
            skipped.append(f["name"])
            continue
        rows.append(
            {
                "approach": parsed["approach"],
                "baseline": parsed["baseline"],
                "sheet_id": f["id"],
                "sheet_title": parsed["sheet_title"],
                "created_at": f.get("createdTime", ""),
                "scenario": parsed["scenario"],
                "year": parsed["year"],
            }
        )

    df = pd.DataFrame(rows, columns=pd.Index(INDEX_COLUMNS))
    df = df.sort_values(["scenario", "approach", "year", "baseline"]).reset_index(
        drop=True
    )
    EF_RUN_INDEX_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    logger.info("Wrote %d row(s) to %s", len(df), out)
    if skipped:
        logger.warning(
            "Skipped %d Sheet(s) with unparseable titles: %s",
            len(skipped),
            ", ".join(repr(t) for t in skipped[:5])
            + (" ..." if len(skipped) > 5 else ""),
        )
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--folder-id",
        required=True,
        help=(
            "Drive folder ID containing the diagnostics Sheets. Find it in "
            "the Drive folder URL: drive.google.com/drive/folders/<ID>."
        ),
    )
    parser.add_argument(
        "--output",
        default=str(EF_RUN_INDEX_PATH),
        help=f"Output CSV path (default: {EF_RUN_INDEX_PATH}).",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    rebuild_index(args.folder_id, args.output)


if __name__ == "__main__":
    main()
