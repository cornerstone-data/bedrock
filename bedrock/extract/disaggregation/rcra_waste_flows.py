"""RCRAInfo Biennial Report -> waste-child Use-intersection shares.

Primary path (2024 pilot diagnostics): build the 7×7 matrix directly from BR
``Shipper ID`` -> ``Receiver ID`` rows (preferring receiver-reported tons),
bypassing CRHW FBS. CRHW/stewi ``flowbyfacility`` is generation-only and leaves
``SectorConsumedBy`` empty — a stewi/CRHW extension that preserves shipment edges
is a **follow-up decision**, not required for this temporary diagnostics path.
"""

from __future__ import annotations

from pathlib import Path
from typing import TypedDict

import numpy as np
import pandas as pd

from bedrock.extract.disaggregation.sas_waste_metrics import (
    map_sas_naics_to_cornerstone,
)
from bedrock.extract.disaggregation.waste_static_rules import (
    RCRA_FIVE_DIGIT_RESIDUAL,
    WASTE_CHILDREN,
)
from bedrock.utils.logging.flowsa_log import log

# Columns needed from consolidated stewi ``br_reporting_{year}.csv``
_BR_USECOLS = (
    "Handler ID",
    "Primary NAICS",
    "Shipper ID",
    "Receiver ID",
    "Shipped Tons",
    "Received Tons",
    "Shipper Waste Stream Included in NBR",
    "Receiver Waste Stream Included in NBR",
)


class RCRAFlowRecord(TypedDict):
    shipper: str
    receiver: str
    mass: float


def map_rcra_naics_to_cornerstone(code: str) -> str:
    """Map RCRA NAICS to one Cornerstone child; fail closed if unmapped."""
    digits = "".join(c for c in str(code) if c.isdigit())
    if not digits:
        raise ValueError(f"Unmapped RCRA NAICS (empty): {code!r}")
    child = map_sas_naics_to_cornerstone(digits)
    if child is not None:
        return child
    if len(digits) >= 5 and digits[:5] in RCRA_FIVE_DIGIT_RESIDUAL:
        return RCRA_FIVE_DIGIT_RESIDUAL[digits[:5]]
    raise ValueError(f"Unmapped RCRA NAICS: {code!r}")


def allocate_rcra_mass(naics_code: str, mass: float) -> dict[str, float]:
    """Allocate mass to Cornerstone children; equal-split if multiple (future)."""
    child = map_rcra_naics_to_cornerstone(naics_code)
    return {child: float(mass)}


def try_map_waste_child(code: object) -> str | None:
    """Map to a waste child, or ``None`` if not a mappable 562* NAICS."""
    try:
        child = map_rcra_naics_to_cornerstone(str(code))
    except ValueError:
        return None
    return child if child in WASTE_CHILDREN else None


def _stewi_br_paths(rcra_year: int) -> tuple[Path, Path]:
    """Return (br_reporting csv path, raw extract directory) from stewi locals."""
    from stewi.RCRAInfo import DIR_RCRA_BY_YEAR, OUTPUT_PATH  # noqa: PLC0415

    return DIR_RCRA_BY_YEAR / f"br_reporting_{rcra_year}.csv", Path(OUTPUT_PATH)


def ensure_br_reporting_csv(rcra_year: int) -> Path:
    """Ensure consolidated ``br_reporting_{year}.csv`` exists.

    EPA's public export now ships year-keyed zips (``BR_REPORTING_2021.zip``).
    Stewi's older ``BR_REPORTING`` table name no longer resolves; we download the
    year-specific zip and consolidate into stewi's ``RCRAInfo_by_year`` path.
    """
    path, output_path = _stewi_br_paths(rcra_year)
    if path.is_file() and path.stat().st_size > 0:
        return path

    from stewi.RCRAInfo import (  # noqa: PLC0415
        RCRA_DATA_PATH,
        download_and_extract_zip,
    )

    table = f"BR_REPORTING_{rcra_year}"
    log.info(
        "BR reporting CSV missing for %s (%s); downloading %s via stewi",
        rcra_year,
        path,
        table,
    )
    download_and_extract_zip([table])

    linewidthsdf = pd.read_csv(RCRA_DATA_PATH / "RCRA_FlatFile_LineComponents.csv")
    fields = linewidthsdf["Data Element Name"].tolist()
    files = sorted(output_path.glob(f"BR_REPORTING*{rcra_year}*.csv"))
    if not files:
        # some extracts drop the year from part filenames after unzip
        files = sorted(output_path.glob("BR_REPORTING*.csv"))
    if not files:
        raise FileNotFoundError(
            f"No BR_REPORTING CSV found under {output_path} after downloading {table}"
        )

    frames: list[pd.DataFrame] = []
    for filepath in files:
        log.info("Reading BR extract %s", filepath)
        df = pd.read_csv(
            filepath,
            header=0,
            usecols=list(range(0, len(fields))),
            names=fields,
            low_memory=False,
            encoding="utf-8",
        )
        if "Report Cycle" in df.columns:
            cycle = pd.to_numeric(df["Report Cycle"], errors="coerce")
            df = df.loc[cycle == rcra_year]
        frames.append(df)

    path.parent.mkdir(parents=True, exist_ok=True)
    full = pd.concat(frames, ignore_index=True)
    log.info(
        "Writing consolidated BR reporting %s -> %s (%s rows)",
        rcra_year,
        path,
        len(full),
    )
    full.to_csv(path, index=False)
    return path


def _clean_id_series(s: pd.Series) -> pd.Series:
    out = s.astype(str).str.strip()
    out = out.str.replace(r"\.0$", "", regex=True)
    bad = out.str.lower().isin({"", "nan", "none", "nat", "<na>"})
    return out.mask(bad)


def _is_nbr_yes_series(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.upper().isin({"Y", "YES", "1", "TRUE"})


def build_facility_naics_map(br: pd.DataFrame) -> dict[str, str]:
    """Handler ID -> Primary NAICS from BR rows (last non-null wins)."""
    ids = _clean_id_series(br["Handler ID"])
    naics = (
        br["Primary NAICS"]
        .astype(str)
        .str.replace(r"\D", "", regex=True)
        .replace("", pd.NA)
    )
    tmp = pd.DataFrame({"id": ids, "naics": naics}).dropna()
    # last observation wins
    return dict(zip(tmp["id"], tmp["naics"], strict=False))


def build_intersection_mass_from_br(
    br: pd.DataFrame,
    *,
    facility_naics: dict[str, str] | None = None,
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Accumulate shipper->receiver mass among waste children from BR rows.

    Prefer **receiver-reported** tons (USEEIO v2.0): when ``Received Tons`` > 0
    and both IDs present, use that mass. Else fall back to ``Shipped Tons``.

    Orientation (PR #563): ``mat.loc[receiver_child, shipper_child] += mass``.
    """
    naics_map = (
        facility_naics if facility_naics is not None else build_facility_naics_map(br)
    )

    ship_id = _clean_id_series(br["Shipper ID"])
    recv_id = _clean_id_series(br["Receiver ID"])
    received_raw = (
        br["Received Tons"]
        if "Received Tons" in br.columns
        else pd.Series(0.0, index=br.index)
    )
    shipped_raw = (
        br["Shipped Tons"]
        if "Shipped Tons" in br.columns
        else pd.Series(0.0, index=br.index)
    )
    received = pd.to_numeric(received_raw, errors="coerce").fillna(0.0)
    shipped = pd.to_numeric(shipped_raw, errors="coerce").fillna(0.0)

    has_ids = ship_id.notna() & recv_id.notna()
    use_recv = has_ids & (received > 0)
    if "Receiver Waste Stream Included in NBR" in br.columns:
        nbr_r = br["Receiver Waste Stream Included in NBR"]
        # Blank flag -> do not filter; explicit non-Y -> drop
        flagged = nbr_r.notna() & (nbr_r.astype(str).str.strip() != "")
        use_recv = use_recv & (~flagged | _is_nbr_yes_series(nbr_r))

    use_ship = has_ids & (~use_recv) & (shipped > 0)
    if "Shipper Waste Stream Included in NBR" in br.columns:
        nbr_s = br["Shipper Waste Stream Included in NBR"]
        flagged = nbr_s.notna() & (nbr_s.astype(str).str.strip() != "")
        use_ship = use_ship & (~flagged | _is_nbr_yes_series(nbr_s))

    mass = np.where(use_recv, received, np.where(use_ship, shipped, 0.0))
    keep = use_recv | use_ship

    stats = {
        "rows_seen": int(len(br)),
        "rows_used_received": int(use_recv.sum()),
        "rows_used_shipped": int(use_ship.sum()),
        "rows_skipped_missing_ids": int((~has_ids).sum()),
        "rows_with_tons_or_ids": int(keep.sum()),
    }

    work = pd.DataFrame(
        {
            "ship_id": ship_id,
            "recv_id": recv_id,
            "mass": mass,
        }
    ).loc[keep]

    work["ship_naics"] = work["ship_id"].map(naics_map)
    work["recv_naics"] = work["recv_id"].map(naics_map)
    work["ship_child"] = work["ship_naics"].map(try_map_waste_child)
    work["recv_child"] = work["recv_naics"].map(try_map_waste_child)
    both = work["ship_child"].notna() & work["recv_child"].notna() & (work["mass"] > 0)
    stats["rows_skipped_non_waste_endpoint"] = int((~both).sum())
    work = work.loc[both]

    mat = pd.DataFrame(0.0, index=WASTE_CHILDREN, columns=WASTE_CHILDREN, dtype=float)
    if not work.empty:
        grouped = work.groupby(["recv_child", "ship_child"], as_index=False)[
            "mass"
        ].sum()
        for _, row in grouped.iterrows():
            mat.loc[row["recv_child"], row["ship_child"]] += float(row["mass"])

    stats["rows_used_waste_intersection"] = int(len(work))
    return mat, stats


def load_br_reporting(rcra_year: int) -> pd.DataFrame:
    """Load consolidated BR reporting CSV for *rcra_year* (download if needed)."""
    path = ensure_br_reporting_csv(rcra_year)
    header = pd.read_csv(path, nrows=0)
    usecols = [c for c in _BR_USECOLS if c in header.columns]
    missing = [
        c
        for c in ("Shipper ID", "Receiver ID", "Primary NAICS", "Handler ID")
        if c not in usecols
    ]
    if missing:
        raise KeyError(f"BR reporting {rcra_year} missing required columns {missing}")
    ton_cols = [c for c in ("Received Tons", "Shipped Tons") if c in usecols]
    if not ton_cols:
        raise KeyError(
            f"BR reporting {rcra_year} missing both Received Tons and Shipped Tons"
        )
    log.info("Reading BR reporting %s from %s (cols=%s)", rcra_year, path, usecols)
    return pd.read_csv(path, usecols=usecols, low_memory=False)


def load_rcra_intersection_shares(rcra_year: int) -> tuple[pd.DataFrame, list[str]]:
    """Return (7×7 Use-intersection share matrix, provenance notes).

    Builds shipper->receiver shares directly from Biennial Report rows (temporary
    diagnostics path; bypasses CRHW FBS). See module docstring.
    """
    notes: list[str] = [
        f"RCRA intersection from BR shipper->receiver rows (year={rcra_year}); "
        "bypasses CRHW FBS - temporary diagnostics path; stewi/CRHW shipment-edge "
        "extension is a follow-up decision"
    ]
    br = load_br_reporting(rcra_year)
    mat, stats = build_intersection_mass_from_br(br)
    notes.append(
        "BR intersection stats: " + ", ".join(f"{k}={v}" for k, v in stats.items())
    )
    total = float(mat.to_numpy().sum())
    if total <= 0:
        raise ValueError(
            f"All-zero RCRA intersection from BR shipper->receiver for {rcra_year}; "
            f"stats={stats}"
        )
    return mat / total, notes
