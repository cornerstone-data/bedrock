"""Dispatch bedrock v0.3 USA commodity gross output (q) to a Google Sheet.

Loads snapshot object ``scaled_q_USA`` (commodity ``q`` = V column sums), joins
Cornerstone commodity metadata, and writes ``q`` / ``README`` tabs (plus
``extra_codes`` when needed) used by the ceda v0.3 assessment weighted-N
waterfalls.

By default creates or updates a spreadsheet named ``q_v0_3_cornerstone`` in the
v0.3 waterfall Drive folder.

Typical usage::

    uv run python -m bedrock.analysis.v0_3.export_q_v0_3_cornerstone

    uv run python -m bedrock.analysis.v0_3.export_q_v0_3_cornerstone \\
        --folder-id <DRIVE_FOLDER_ID>
"""

from __future__ import annotations

import argparse
import logging

import pandas as pd

from bedrock.analysis.v0_3.constants import V03_WATERFALL_DRIVE_FOLDER_ID
from bedrock.utils.io.gcp import (
    DRIVE_MIME_SPREADSHEET,
    create_spreadsheet_in_folder,
    delete_default_sheet1,
    list_drive_folder,
    update_sheet_tab,
)
from bedrock.utils.snapshots.loader import (
    current_snapshot_key,
    load_current_snapshot,
    load_snapshot,
)
from bedrock.utils.snapshots.names import SnapshotName
from bedrock.utils.taxonomy.cornerstone.commodities import COMMODITIES, COMMODITY_DESC

logger = logging.getLogger(__name__)

DEFAULT_CONFIG = "2025_usa_cornerstone_v0_3"
DEFAULT_RELEASE = "v0_3_0"
DEFAULT_DRIVE_FOLDER_ID = V03_WATERFALL_DRIVE_FOLDER_ID
SNAPSHOT_OBJECT: SnapshotName = "scaled_q_USA"
PUBLISH_LOCATION = "US"
SHEET_TITLE = "q_v0_3_cornerstone"


def _series_from_snapshot(frame: pd.DataFrame) -> pd.Series[float]:
    squeezed = frame.squeeze()
    if not isinstance(squeezed, pd.Series):
        raise TypeError(
            f"Expected Series after squeeze of {SNAPSHOT_OBJECT}, got {type(squeezed)}"
        )
    return squeezed.astype(float)


def build_q_frame(q: pd.Series) -> tuple[pd.DataFrame, list[str]]:
    """Align ``q`` to canonical commodity order; return frame + any extra codes."""
    q = pd.Series(q, name="q")
    q.index = q.index.astype(str)
    meta = pd.DataFrame(
        [
            {
                "Code": code,
                "Code_Loc": f"{code}/{PUBLISH_LOCATION}",
                "Location": PUBLISH_LOCATION,
                "Name": COMMODITY_DESC[code],
                "q": float(q[code]) if code in q.index else float("nan"),
            }
            for code in COMMODITIES
        ]
    )
    extra = sorted(set(q.index) - set(COMMODITIES))
    return meta, extra


def _load_q(snapshot_key: str | None) -> tuple[pd.Series[float], str]:
    if snapshot_key is None:
        key = current_snapshot_key()
        q = _series_from_snapshot(load_current_snapshot(SNAPSHOT_OBJECT))
    else:
        key = snapshot_key
        q = _series_from_snapshot(load_snapshot(SNAPSHOT_OBJECT, key=key))
    return q, key


def _readme_frame(
    *,
    config: str,
    release: str,
    snapshot_key: str,
    n_commodities: int,
    n_missing: int,
    n_extra: int,
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "config": [config],
            "release": [release],
            "snapshot_key": [snapshot_key],
            "snapshot_object": [SNAPSHOT_OBJECT],
            "n_commodities": [n_commodities],
            "n_missing_in_q": [n_missing],
            "n_extra_in_q": [n_extra],
            "units_note": ["Commodity gross output (q = V column sums), USD"],
        }
    )


def _resolve_sheet_id(folder_id: str, title: str) -> str:
    """Return existing spreadsheet id with ``title`` in ``folder_id``, or create one."""
    matches = [
        row
        for row in list_drive_folder(folder_id, mime_type=DRIVE_MIME_SPREADSHEET)
        if row.get("name") == title
    ]
    if matches:
        sheet_id = matches[0]["id"]
        logger.info('reusing spreadsheet "%s" (%s)', title, sheet_id)
        return sheet_id
    sheet_id = create_spreadsheet_in_folder(title=title, folder_id=folder_id)
    logger.info('created spreadsheet "%s" (%s)', title, sheet_id)
    return sheet_id


def dispatch_q_sheet(
    *,
    folder_id: str = DEFAULT_DRIVE_FOLDER_ID,
    sheet_title: str = SHEET_TITLE,
    snapshot_key: str | None = None,
    config: str = DEFAULT_CONFIG,
    release: str = DEFAULT_RELEASE,
) -> str:
    """Write q / README tabs to a Drive spreadsheet; return sheet id."""
    q, key = _load_q(snapshot_key)
    meta, extra = build_q_frame(q)
    missing = int(meta["q"].isna().sum())
    if missing:
        raise ValueError(
            f"{missing} commodities missing from {SNAPSHOT_OBJECT} "
            f"(snapshot_key={key})"
        )

    sheet_id = _resolve_sheet_id(folder_id, sheet_title)
    update_sheet_tab(sheet_id, "q", meta, clean_nans=True)
    update_sheet_tab(
        sheet_id,
        "README",
        _readme_frame(
            config=config,
            release=release,
            snapshot_key=key,
            n_commodities=len(meta),
            n_missing=missing,
            n_extra=len(extra),
        ),
    )
    if extra:
        update_sheet_tab(
            sheet_id,
            "extra_codes",
            pd.DataFrame({"Code": extra, "q": [float(q[c]) for c in extra]}),
            clean_nans=True,
        )
    try:
        delete_default_sheet1(sheet_id)
    except Exception as e:  # noqa: BLE001
        logger.warning("Sheet1 cleanup skipped (%s: %s)", type(e).__name__, e)
    return sheet_id


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(
        description=(
            "Dispatch bedrock v0.3 USA scaled_q_USA to a Google Sheet for ceda "
            "weighted-N waterfalls."
        )
    )
    p.add_argument(
        "--folder-id",
        default=DEFAULT_DRIVE_FOLDER_ID,
        help=(
            "Drive folder ID for the spreadsheet "
            f"(default: v0.3 waterfall folder {DEFAULT_DRIVE_FOLDER_ID})."
        ),
    )
    p.add_argument(
        "--sheet-title",
        default=SHEET_TITLE,
        help=f"Spreadsheet title in Drive (default: {SHEET_TITLE}).",
    )
    p.add_argument(
        "--snapshot-key",
        default=None,
        help=(
            "Snapshot SHA to load (default: bedrock.utils.snapshots/.SNAPSHOT_KEY "
            "via load_current_snapshot)."
        ),
    )
    p.add_argument(
        "--config",
        default=DEFAULT_CONFIG,
        help=f"Provenance config label written to README (default: {DEFAULT_CONFIG})",
    )
    p.add_argument(
        "--release",
        default=DEFAULT_RELEASE,
        help=f"Provenance release label written to README (default: {DEFAULT_RELEASE})",
    )
    args = p.parse_args(argv)

    sheet_id = dispatch_q_sheet(
        folder_id=args.folder_id,
        sheet_title=args.sheet_title,
        snapshot_key=args.snapshot_key,
        config=args.config,
        release=args.release,
    )
    print(f"dispatched sheet_id={sheet_id}")
    print(f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    main()
