"""Export bedrock v0.3 USA commodity gross output (q) to Excel.

Loads snapshot object ``scaled_q_USA`` (commodity ``q`` = V column sums), joins
Cornerstone commodity metadata, and writes a two-sheet workbook used by the
ceda v0.3 assessment weighted-N waterfalls.

Typical drop path for ceda figures::

    uv run python -m bedrock.analysis.v0_3.export_q_v0_3_cornerstone \\
        --out ../ceda/projects/v0_3_assessment/output/q_v0_3_cornerstone.xlsx

Default ``--out`` is this package's gitignored ``output/q_v0_3_cornerstone.xlsx``.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from bedrock.utils.snapshots.loader import (
    current_snapshot_key,
    load_current_snapshot,
    load_snapshot,
)
from bedrock.utils.snapshots.names import SnapshotName
from bedrock.utils.taxonomy.cornerstone.commodities import COMMODITIES, COMMODITY_DESC

DEFAULT_OUT = Path(__file__).resolve().parent / "output" / "q_v0_3_cornerstone.xlsx"
DEFAULT_CONFIG = "2025_usa_cornerstone_v0_3"
DEFAULT_RELEASE = "v0_3_0"
SNAPSHOT_OBJECT: SnapshotName = "scaled_q_USA"
PUBLISH_LOCATION = "US"


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


def export_q_xlsx(
    out_path: Path,
    *,
    snapshot_key: str | None = None,
    config: str = DEFAULT_CONFIG,
    release: str = DEFAULT_RELEASE,
) -> Path:
    """Write ``q`` + ``README`` sheets; return ``out_path``."""
    if snapshot_key is None:
        key = current_snapshot_key()
        q = _series_from_snapshot(load_current_snapshot(SNAPSHOT_OBJECT))
    else:
        key = snapshot_key
        q = _series_from_snapshot(load_snapshot(SNAPSHOT_OBJECT, key=key))

    meta, extra = build_q_frame(q)
    missing = int(meta["q"].isna().sum())
    if missing:
        raise ValueError(
            f"{missing} commodities missing from {SNAPSHOT_OBJECT} "
            f"(snapshot_key={key})"
        )

    out_path = out_path.resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        meta.to_excel(writer, sheet_name="q", index=False)
        pd.DataFrame(
            {
                "config": [config],
                "release": [release],
                "snapshot_key": [key],
                "snapshot_object": [SNAPSHOT_OBJECT],
                "n_commodities": [len(meta)],
                "n_missing_in_q": [missing],
                "n_extra_in_q": [len(extra)],
                "units_note": ["Commodity gross output (q = V column sums), USD"],
            }
        ).to_excel(writer, sheet_name="README", index=False)
        if extra:
            pd.DataFrame({"Code": extra, "q": [float(q[c]) for c in extra]}).to_excel(
                writer, sheet_name="extra_codes", index=False
            )

    return out_path


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(
        description=(
            "Export bedrock v0.3 USA scaled_q_USA to Excel for ceda weighted-N "
            "waterfalls."
        )
    )
    p.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUT,
        help=f"Output xlsx path (default: {DEFAULT_OUT})",
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

    out = export_q_xlsx(
        args.out,
        snapshot_key=args.snapshot_key,
        config=args.config,
        release=args.release,
    )
    q_df = pd.read_excel(out, sheet_name="q")
    print(f"wrote {out}")
    print(f"n={len(q_df)}  Σq={q_df['q'].sum():.6e}")


if __name__ == "__main__":
    main()
