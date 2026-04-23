"""Derive a time series of B matrices (2019-2023) and analyze trends.

B = (E / x) @ Vnorm

Inputs:
- FBS (FlowBySector) parquets from GCS: m2, v2.1.0, years 2019-2023
- x (gross industry output after redefinition) for each year
- Vnorm from 2017 BEA benchmark (fixed)

Usage:
    python -m bedrock.analysis.time_series_B_matrix.derive_B_time_series
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, cast

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from bedrock.utils.taxonomy.bea.matrix_mappings import (  # noqa: PLC0415
        USA_GROSS_INDUSTRY_OUTPUT_YEARS,
    )

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

YEARS = [2019, 2020, 2021, 2022, 2023]
FBS_GCS_SUB_BUCKET = "flowsa/FlowBySector"
FBS_VERSION = "v2.1.0"

OUTPUT_DIR = Path(__file__).parent / "output"


# ── Step 1: Data Acquisition — FBS ──────────────────────────────────────────


def list_available_fbs() -> pd.DataFrame:
    """List FBS files on GCS and print available m2 parquets for inspection."""
    from bedrock.utils.io.gcp import list_bucket_files  # noqa: PLC0415

    df = list_bucket_files(FBS_GCS_SUB_BUCKET)
    m2 = df[df["base_name"].str.contains("m2", case=False, na=False)]
    parquets = m2[m2["extension"] == ".parquet"]
    display = parquets[["base_name", "version", "hash", "full_path"]]
    logger.info("Available m2 FBS parquets on GCS:\n%s", display.to_string())
    return parquets


def download_fbs_parquets() -> dict[int, pd.DataFrame]:
    """Download FBS parquets for each year from GCS and return as DataFrames.

    Uses list_bucket_files to find exact filenames (including hash) that match
    the GHG_national_{year}_m2 base name with the target version, then downloads
    each via direct GCS download.
    """
    from bedrock.utils.io.gcp import (  # noqa: PLC0415
        download_gcs_file,
        list_bucket_files,
    )

    local_dir = OUTPUT_DIR / "fbs_cache"
    local_dir.mkdir(parents=True, exist_ok=True)

    # List all FBS files on GCS once
    bucket_df = list_bucket_files(FBS_GCS_SUB_BUCKET)

    fbs_by_year: dict[int, pd.DataFrame] = {}
    for year in YEARS:
        base_name = f"GHG_national_{year}_m2"
        matches = bucket_df[
            (bucket_df["base_name"] == base_name)
            & (bucket_df["version"] == FBS_VERSION)
            & (bucket_df["extension"] == ".parquet")
        ]
        if matches.empty:
            raise FileNotFoundError(
                f"No FBS parquet found on GCS for {base_name} {FBS_VERSION}"
            )
        # Take the most recent if multiple
        row = matches.sort_values(by="created", ascending=False).iloc[0]
        gcs_filename = row["full_path"].split("/")[
            -1
        ]  # e.g. GHG_national_2019_m2_v2.1.0_c25c206.parquet
        local_path = str(local_dir / gcs_filename)

        logger.info("Loading FBS for %d: %s", year, gcs_filename)
        if not os.path.exists(local_path):
            download_gcs_file(gcs_filename, FBS_GCS_SUB_BUCKET, local_path)
        df = pd.read_parquet(local_path)
        fbs_by_year[year] = df
        logger.info(
            "  %d: %d rows, %d unique sectors",
            year,
            len(df),
            df["SectorProducedBy"].nunique(),
        )

    return fbs_by_year


def check_fbs_sector_schema(fbs_by_year: dict[int, pd.DataFrame]) -> str:
    """Determine whether FBS sectors are CEDA v7 or Cornerstone schema.

    Returns 'cornerstone' or 'ceda' and logs diagnostic info.
    """
    from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS  # noqa: PLC0415
    from bedrock.utils.taxonomy.cornerstone.industries import (  # noqa: PLC0415
        INDUSTRIES,
    )

    ceda_set = {str(s) for s in CEDA_V7_SECTORS}
    cs_set = {str(s) for s in INDUSTRIES}

    for year, fbs in fbs_by_year.items():
        sectors = set(fbs["SectorProducedBy"].dropna().unique())
        ceda_overlap = len(sectors & ceda_set) / max(len(sectors), 1)
        cs_overlap = len(sectors & cs_set) / max(len(sectors), 1)
        logger.info(
            "  %d: %d unique sectors | CEDA overlap: %.1f%% | Cornerstone overlap: %.1f%%",
            year,
            len(sectors),
            ceda_overlap * 100,
            cs_overlap * 100,
        )

    # Use first year's FBS to determine schema
    first_fbs = fbs_by_year[YEARS[0]]
    sectors = set(first_fbs["SectorProducedBy"].dropna().unique())
    ceda_overlap = len(sectors & ceda_set)
    cs_overlap = len(sectors & cs_set)

    # FBS uses NAICS codes that need mapping — neither will match directly.
    # The map_to_CEDA function handles the NAICS→CEDA/Cornerstone mapping.
    schema = "cornerstone" if cs_overlap > ceda_overlap else "ceda"
    logger.info(
        "FBS sector schema detected: %s (will use map_to_CEDA for NAICS→schema mapping)",
        schema,
    )
    return schema


# ── Step 2: Data Acquisition — x (Gross Output) ────────────────────────────


def derive_x_time_series() -> dict[int, pd.Series]:
    """Derive gross industry output x for each year in Cornerstone schema.

    Output is deflated to constant BASE_YEAR dollars using BEA sector-level
    price indices, so that B = E / x reflects real intensity changes rather
    than nominal inflation.
    """
    from bedrock.transform.eeio.cornerstone_expansion import (  # noqa: PLC0415
        CS_INDUSTRY_LIST,
        cs_industry_to_bea_map,
        expand_vector,
    )
    from bedrock.transform.eeio.derived_cornerstone import (  # noqa: PLC0415
        _distribute_waste_parent_x_using_v_row_shares,
    )
    from bedrock.transform.iot.derived_gross_industry_output import (  # noqa: PLC0415
        derive_gross_output_after_redefinition,
    )
    from bedrock.utils.economic.inflate_cornerstone_to_target_year import (  # noqa: PLC0415
        get_cornerstone_price_ratio,
    )

    base_year = YEARS[0]
    x_by_year: dict[int, pd.Series] = {}
    for year in YEARS:
        logger.info("Deriving x for %d", year)
        x_bea = derive_gross_output_after_redefinition(
            target_year=cast("USA_GROSS_INDUSTRY_OUTPUT_YEARS", year),
        )
        x_cs = expand_vector(x_bea, CS_INDUSTRY_LIST, cs_industry_to_bea_map())
        x_cs.index.name = "sector"
        x_cs = _distribute_waste_parent_x_using_v_row_shares(x_cs)

        # Deflate to constant base-year dollars
        if year != base_year:
            # price_ratio = price(year) / price(base_year); divide to get real dollars
            price_ratio = get_cornerstone_price_ratio(base_year, year)
            x_cs = x_cs / price_ratio.reindex(x_cs.index, fill_value=1.0)

        x_by_year[year] = x_cs
        logger.info(
            "  %d: %d sectors, total=$%.2fT (constant %d $)",
            year,
            len(x_cs),
            x_cs.sum() / 1e12,
            base_year,
        )

    return x_by_year


# ── Step 3: Process FBS into E Matrix ───────────────────────────────────────

# Gas name mapping (from load_E_from_flowsa in transform/allocation/derived.py)
GAS_MAP = {
    "Carbon dioxide": "CO2",
    "Methane": "CH4_fossil",
    "Nitrous oxide": "N2O",
    "Nitrogen trifluoride": "NF3",
    "Sulfur hexafluoride": "SF6",
    "HFC, PFC and SF6 F-HTFs": "HFCs",
    "HFCs and PFCs, unspecified": "HFCs",
    "Carbon tetrafluoride": "CF4",
    "Hexafluoroethane": "C2F6",
    "PFC": "PFCs",
    "Perfluorocyclobutane": "c-C4F8",
    "Perfluoropropane": "C3F8",
}

# Reverse mapping for collapsing detailed gases into 7 aggregate groups
_GHG_MAPPING = {
    "CO2": ["CO2"],
    "CH4": ["CH4"],
    "N2O": ["N2O"],
    "HFCs": ["HFC-23", "HFC-32", "HFC-125", "HFC-134a", "HFC-143a", "HFC-236fa"],
    "PFCs": ["CF4", "C2F6", "C3F8", "C4F8"],
    "SF6": ["SF6"],
    "NF3": ["NF3"],
}
_GHG_REVERSE = {m: g for g, members in _GHG_MAPPING.items() for m in members}
_GHG_REVERSE.update(
    {
        "HFC-227ea": "HFCs",
        "c-C4F8": "PFCs",
        "CH4_fossil": "CH4",
        "CH4_non_fossil": "CH4",
    }
)


def _map_fbs_to_cornerstone(fbs: pd.DataFrame) -> pd.DataFrame:
    """Map FBS sectors from mixed-digit NAICS to Cornerstone schema.

    Explicitly uses the Cornerstone_2025 activity-to-sector mapping
    regardless of the global usa_config flag, ensuring the canonical
    schema is always Cornerstone (405 sectors).

    Logic mirrors ``map_to_CEDA()`` in transform/allocation/derived.py
    but hardcodes the Cornerstone mapping.
    """
    from bedrock.transform.flowbysector import FlowBySector  # noqa: PLC0415
    from bedrock.utils.config.common import load_crosswalk  # noqa: PLC0415
    from bedrock.utils.mapping.sectormapping import (  # noqa: PLC0415
        get_activitytosector_mapping,
    )

    # Step 1: expand mixed-digit NAICS to 6-digit
    cw = load_crosswalk("NAICS_2017_Crosswalk")
    cols_to_stack = ["NAICS_3", "NAICS_4", "NAICS_5"]
    cw_stack = (
        cw.astype({c: "string" for c in cols_to_stack + ["NAICS_6"]})
        .melt(
            id_vars="NAICS_6",
            value_vars=cols_to_stack,
            var_name="level",
            value_name="NAICS",
        )
        .dropna(subset=["NAICS_6", "NAICS"])[["NAICS", "NAICS_6"]]
        .drop_duplicates(subset="NAICS", keep="first")
        .reset_index(drop=True)
    )
    fbs2 = fbs.merge(
        cw_stack,
        how="left",
        left_on="SectorProducedBy",
        right_on="NAICS",
        validate="m:1",
    )
    fbs2["NAICS_6"] = fbs2["NAICS_6"].fillna(fbs2["SectorProducedBy"])

    # Step 2: map NAICS-6 → Cornerstone sectors (always Cornerstone_2025)
    mapping = get_activitytosector_mapping("Cornerstone_2025").drop_duplicates(
        subset="Sector",
        keep="first",
    )
    fbs2 = (
        fbs2.merge(
            mapping[["Activity", "Sector"]],
            how="left",
            left_on="NAICS_6",
            right_on="Sector",
            validate="m:1",
        )
        .assign(SectorProducedBy=lambda x: x["Activity"].fillna(x["NAICS_6"]))
        .drop(columns=["Activity", "NAICS", "NAICS_6", "Sector"])
    )

    return pd.DataFrame(FlowBySector(fbs2).aggregate_flowby())


def fbs_to_E(fbs: pd.DataFrame, target_columns: list[str]) -> pd.DataFrame:
    """Convert a single year's FBS DataFrame to an E matrix (ghg x industry).

    Applies gas mapping, CH4 fossil/non-fossil split, GWP conversion,
    Cornerstone sector mapping, and aggregation to produce (7 ghg x 405 industry) matrix.
    """
    from bedrock.utils.emissions.gwp import GWP100_AR6_CEDA  # noqa: PLC0415

    fbs = _map_fbs_to_cornerstone(fbs)

    # Gas name mapping
    fbs["Flowable"] = fbs["Flowable"].map(GAS_MAP).fillna(fbs["Flowable"])

    # CH4 fossil vs non-fossil split
    meta = fbs["MetaSources"].astype(str)
    sector = fbs["SectorProducedBy"].astype(str)
    ch4_non_fossil_mask = meta.str.contains("_5_", regex=False, na=False) | (
        meta.str.contains("2_1", regex=False, na=False)
        & sector.str.match(r"^(1|562|2213)", na=False)
    )
    fbs.loc[ch4_non_fossil_mask & (fbs["Flowable"] == "CH4_fossil"), "Flowable"] = (
        "CH4_non_fossil"
    )

    # Convert to CO2e
    ghg_gwp: dict[str, float] = {k: v for k, v in GWP100_AR6_CEDA.items()}
    ghg_gwp["HFCs"] = 1  # already in CO2e
    ghg_gwp["PFCs"] = 1
    fbs["CO2e"] = fbs["FlowAmount"] * fbs["Flowable"].map(ghg_gwp)

    # Pivot to (ghg x sector)
    E = fbs.pivot_table(
        index="Flowable",
        columns="SectorProducedBy",
        values="CO2e",
        aggfunc="sum",
        fill_value=0,
    )

    # Collapse detailed gases into 7 aggregate groups
    new_index = E.index.map(lambda x: _GHG_REVERSE.get(x, x))
    E = E.groupby(new_index).agg("sum")

    # Align columns to target schema
    E = E.reindex(columns=target_columns, fill_value=0)
    E.index.name = "ghg"
    E.columns.name = "sector"

    return E


def derive_E_time_series(
    fbs_by_year: dict[int, pd.DataFrame],
) -> dict[int, pd.DataFrame]:
    """Convert FBS DataFrames to E matrices for each year."""
    from bedrock.utils.taxonomy.cornerstone.industries import (  # noqa: PLC0415
        INDUSTRIES,
    )

    target_columns = [str(s) for s in INDUSTRIES]

    E_by_year: dict[int, pd.DataFrame] = {}
    for year, fbs in fbs_by_year.items():
        logger.info("Processing FBS → E for %d", year)
        E = fbs_to_E(fbs, target_columns)
        E_by_year[year] = E
        logger.info(
            "  %d: E shape=%s, total CO2e=%.2f Mt", year, E.shape, E.sum().sum() / 1e6
        )

    return E_by_year


# ── Step 4: Derive Vnorm (Fixed) ───────────────────────────────────────────


def derive_Vnorm() -> pd.DataFrame:
    """Derive Vnorm from the 2017 BEA benchmark (scrap-corrected)."""
    from bedrock.transform.eeio.derived_cornerstone import (  # noqa: PLC0415
        derive_cornerstone_Vnorm_scrap_corrected,
    )

    logger.info("Deriving Vnorm (2017 benchmark, scrap-corrected)")
    Vnorm = derive_cornerstone_Vnorm_scrap_corrected()
    logger.info("  Vnorm shape=%s", Vnorm.shape)
    return Vnorm


# ── Step 5: Compute B Matrix Time Series ───────────────────────────────────


def compute_B_time_series(
    E_by_year: dict[int, pd.DataFrame],
    x_by_year: dict[int, pd.Series],
    Vnorm: pd.DataFrame,
) -> dict[int, pd.DataFrame]:
    """Compute B = (E / x) @ Vnorm for each year."""
    B_by_year: dict[int, pd.DataFrame] = {}
    for year in YEARS:
        E = E_by_year[year]
        x = x_by_year[year]

        # Align E columns with x index
        common_sectors = E.columns.intersection(x.index)
        E_aligned = E[common_sectors]
        x_aligned = x[common_sectors]

        Bi = E_aligned.divide(x_aligned, axis=1).fillna(0.0)
        # Replace inf from division by zero
        Bi = Bi.replace([np.inf, -np.inf], 0.0)

        B = Bi @ Vnorm
        B_by_year[year] = B
        logger.info(
            "  %d: B shape=%s, total intensity=%.6f", year, B.shape, B.sum().sum()
        )

    return B_by_year


# ── Step 6: Analysis ────────────────────────────────────────────────────────


def analyze_aggregate_trends(
    E_by_year: dict[int, pd.DataFrame],
    x_by_year: dict[int, pd.Series],
) -> pd.DataFrame:
    """6a. Economy-wide emissions intensity by gas and year.

    Computes total_emissions(gas) / total_industry_output for each year,
    giving a properly weighted average intensity (CO2e per $ of output).
    """
    records = []
    for year in YEARS:
        E = E_by_year[year]
        total_output = x_by_year[year].sum()
        for gas in E.index:
            total_emissions = E.loc[gas].sum()
            records.append(
                {
                    "year": year,
                    "gas": gas,
                    "intensity": total_emissions / total_output,
                }
            )
    df = pd.DataFrame(records).pivot(index="gas", columns="year", values="intensity")
    logger.info("Aggregate trends (economy-wide intensity by gas):\n%s", df.to_string())
    return df


def analyze_sector_changes(B_by_year: dict[int, pd.DataFrame]) -> pd.DataFrame:
    """6b. Commodities with largest absolute change (2019 vs 2023)."""
    B_start = B_by_year[YEARS[0]]
    B_end = B_by_year[YEARS[-1]]

    delta = B_end - B_start
    # Flatten to (gas, commodity) pairs
    records = []
    for gas in delta.index:
        for commodity in delta.columns:
            val_start = B_start.loc[gas, commodity]
            val_end = B_end.loc[gas, commodity]
            abs_change = val_end - val_start
            rel_change = abs_change / val_start if val_start != 0 else np.nan
            records.append(
                {
                    "gas": gas,
                    "commodity": commodity,
                    "B_start": val_start,
                    "B_end": val_end,
                    "abs_change": abs_change,
                    "rel_change": rel_change,
                }
            )
    df = pd.DataFrame(records)
    df["abs_abs_change"] = df["abs_change"].abs()

    top_changes = df.nlargest(20, "abs_abs_change").drop(columns=["abs_abs_change"])
    logger.info(
        "Top 20 (gas, commodity) by absolute change (%d→%d):\n%s",
        YEARS[0],
        YEARS[-1],
        top_changes.to_string(),
    )
    return df


def analyze_non_monotonic(B_by_year: dict[int, pd.DataFrame]) -> pd.DataFrame:
    """6b (cont). Flag commodities with non-monotonic behavior (spikes/dips)."""
    # Build a panel: one row per (year, gas, commodity)
    rows = []
    for year, B in B_by_year.items():
        stacked = B.stack().reset_index()
        stacked.columns = pd.Index(["gas", "commodity", "intensity"])
        stacked["year"] = year
        rows.append(stacked)
    panel = pd.concat(rows, ignore_index=True)

    # For each (gas, commodity), check if the time series is monotonic
    results = []
    for (gas, commodity), group in panel.groupby(["gas", "commodity"], sort=False):
        ts = np.asarray(group.sort_values(by="year")["intensity"].values, dtype=float)
        if np.all(ts == 0):
            continue
        diffs = np.diff(ts)
        is_monotone = bool(np.all(diffs >= 0)) or bool(np.all(diffs <= 0))
        if not is_monotone:
            results.append(
                {
                    "gas": gas,
                    "commodity": commodity,
                    "min": float(ts.min()),
                    "max": float(ts.max()),
                    "range": float(ts.max() - ts.min()),
                    "values": list(ts),
                }
            )

    df = pd.DataFrame(results)
    if not df.empty:
        df = df.sort_values("range", ascending=False)
        logger.info(
            "Top 20 non-monotonic (gas, commodity) pairs:\n%s",
            df.head(20).to_string(),
        )
    return df


def decompose_E_vs_x(
    E_by_year: dict[int, pd.DataFrame],
    x_by_year: dict[int, pd.Series],
    Vnorm: pd.DataFrame,
) -> pd.DataFrame:
    """6c. Decompose B change into emission effect vs output effect.

    For each commodity c and gas g:
      delta_B = B_end - B_start
      emission_effect = ((E_end / x_start) @ Vnorm) - B_start   (E changed, x held)
      output_effect   = ((E_start / x_end) @ Vnorm) - B_start   (x changed, E held)
    """
    y0, y1 = YEARS[0], YEARS[-1]
    E0, E1 = E_by_year[y0], E_by_year[y1]
    x0, x1 = x_by_year[y0], x_by_year[y1]

    common = (
        E0.columns.intersection(x0.index)
        .intersection(E1.columns)
        .intersection(x1.index)
    )

    def _compute_B(E: pd.DataFrame, x: pd.Series) -> pd.DataFrame:
        Bi = (
            E[common]
            .divide(x[common], axis=1)
            .fillna(0.0)
            .replace([np.inf, -np.inf], 0.0)
        )
        return Bi @ Vnorm

    B_start = _compute_B(E0, x0)
    B_end = _compute_B(E1, x1)
    B_E_change = _compute_B(E1, x0)  # only E changed
    B_x_change = _compute_B(E0, x1)  # only x changed

    records = []
    for gas in B_start.index:
        for commodity in B_start.columns:
            b0 = B_start.loc[gas, commodity]
            b1 = B_end.loc[gas, commodity]
            emission_effect = B_E_change.loc[gas, commodity] - b0
            output_effect = B_x_change.loc[gas, commodity] - b0
            records.append(
                {
                    "gas": gas,
                    "commodity": commodity,
                    "B_start": b0,
                    "B_end": b1,
                    "delta_B": b1 - b0,
                    "emission_effect": emission_effect,
                    "output_effect": output_effect,
                }
            )

    df = pd.DataFrame(records)
    df["abs_delta"] = df["delta_B"].abs()
    top = df.nlargest(20, "abs_delta").drop(columns=["abs_delta"])
    logger.info(
        "Decomposition (top 20 by |delta_B|, %d→%d):\n%s",
        y0,
        y1,
        top.to_string(),
    )
    return df


# ── Step 6d: Visualization ──────────────────────────────────────────────────


def plot_aggregate_trends(agg: pd.DataFrame) -> None:
    """Line plot of total intensity by gas over time."""
    fig, ax = plt.subplots(figsize=(10, 6))

    colors = {
        "CO2": "#e67e22",
        "CH4": "#2980b9",
        "N2O": "#c0392b",
        "HFCs": "#27ae60",
        "PFCs": "#8e44ad",
        "SF6": "#f39c12",
        "NF3": "#7f8c8d",
    }

    # All-gas total line
    total = agg.sum(axis=0)
    total_pct_change = (float(total.iloc[-1]) / float(total.iloc[0]) - 1) * 100
    ax.plot(YEARS, total.values, marker="s", color="black", linewidth=2.5, markersize=6)
    ax.annotate(
        f"Total ({total_pct_change:+.0f}%)",
        xy=(YEARS[-1], float(total.iloc[-1])),
        xytext=(8, 0),
        textcoords="offset points",
        fontsize=9,
        fontweight="bold",
        color="black",
        va="center",
    )

    for gas in agg.index:
        values = agg.loc[gas]
        pct_change = (float(values.iloc[-1]) / float(values.iloc[0]) - 1) * 100
        color = colors.get(gas, "gray")
        ax.plot(
            YEARS, values.values, marker="o", color=color, linewidth=2, markersize=5
        )

        # Annotate gas name + % change at the end of each line
        last_val = float(values.iloc[-1])
        ax.annotate(
            f"{gas} ({pct_change:+.0f}%)",
            xy=(YEARS[-1], last_val),
            xytext=(8, 0),
            textcoords="offset points",
            fontsize=9,
            fontweight="bold",
            color=color,
            va="center",
        )

    ax.set_xticks(YEARS)
    ax.set_xlabel("Year")
    ax.set_ylabel("Economy-wide intensity (CO2e per $ output)")
    ax.set_title("Economy-wide GHG Intensity by Gas (2019-2023)")
    ax.grid(True, alpha=0.2)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.set_xlim(YEARS[0] - 0.3, YEARS[-1] + 1.5)
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "aggregate_trends.png", dpi=150)
    logger.info("Saved aggregate_trends.png")
    plt.close(fig)


def plot_top_sector_time_series(
    B_by_year: dict[int, pd.DataFrame], top_n: int = 10
) -> None:
    """Line plots of B coefficient for top commodities by gas."""
    gases = list(B_by_year[YEARS[0]].index)
    for gas in gases:
        # Find top_n commodities by average B across years
        avg_B = cast(
            pd.Series,
            pd.concat([B.loc[gas] for B in B_by_year.values()], axis=1).mean(axis=1),
        ).nlargest(top_n)

        fig, ax = plt.subplots(figsize=(12, 6))
        for commodity in avg_B.index:
            values = [B_by_year[y].loc[gas, commodity] for y in YEARS]
            ax.plot(YEARS, values, marker="o", label=commodity)

        ax.set_xlabel("Year")
        ax.set_ylabel(f"B intensity ({gas} CO2e per $)")
        ax.set_title(f"Top {top_n} Commodities by {gas} Intensity")
        ax.legend(bbox_to_anchor=(1.05, 1), loc="upper left", fontsize=8)
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        fig.savefig(OUTPUT_DIR / f"top_sectors_{gas}.png", dpi=150)
        plt.close(fig)

    logger.info("Saved top_sectors_*.png for each gas")


def plot_sector_change_time_series(
    B_by_year: dict[int, pd.DataFrame],
    top_n: int = 15,
) -> None:
    """Line plots of the sectors with largest absolute B change (all gases combined).

    Shows time series for the top_n (gas, commodity) pairs ranked by
    |B_end - B_start|. Each subplot groups by gas so related sectors are
    visually comparable.
    """
    B_start = B_by_year[YEARS[0]]
    B_end = B_by_year[YEARS[-1]]
    delta = (B_end - B_start).abs()

    # Flatten and rank
    stacked = delta.stack().rename("abs_change").reset_index()  # type: ignore[call-overload]
    stacked.columns = pd.Index(["gas", "commodity", "abs_change"])
    ranked = stacked.nlargest(top_n, "abs_change")

    # Group by gas for subplots
    gases_present = ranked["gas"].unique()
    n_gases = len(gases_present)
    fig, axes = plt.subplots(n_gases, 1, figsize=(12, 5 * n_gases), squeeze=False)

    for i, gas in enumerate(gases_present):
        ax = axes[i, 0]
        gas_rows = ranked[ranked["gas"] == gas]
        for _, row in gas_rows.iterrows():
            commodity = row["commodity"]
            values = [float(B_by_year[y].loc[gas, commodity]) for y in YEARS]
            ax.plot(YEARS, values, marker="o", label=commodity)

        ax.set_xlabel("Year")
        ax.set_ylabel(f"B intensity ({gas} CO2e per $)")
        ax.set_title(
            f"Sectors with Largest {gas} Intensity Change ({YEARS[0]}→{YEARS[-1]})"
        )
        ax.legend(bbox_to_anchor=(1.05, 1), loc="upper left", fontsize=8)
        ax.grid(True, alpha=0.3)

    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "sector_change_time_series.png", dpi=150)
    logger.info("Saved sector_change_time_series.png")
    plt.close(fig)

    # Also plot a horizontal bar chart of the changes (signed)
    fig2, ax2 = plt.subplots(figsize=(10, max(6, top_n * 0.4)))
    labels = [f"{r['gas']} | {r['commodity']}" for _, r in ranked.iterrows()]
    signed_changes = [
        float(
            B_end.loc[r["gas"], r["commodity"]] - B_start.loc[r["gas"], r["commodity"]]
        )
        for _, r in ranked.iterrows()
    ]
    colors = ["#d32f2f" if v > 0 else "#388e3c" for v in signed_changes]
    y_pos = range(len(labels))
    ax2.barh(list(y_pos), signed_changes, color=colors)
    ax2.set_yticks(list(y_pos))
    ax2.set_yticklabels(labels, fontsize=8)
    ax2.invert_yaxis()
    ax2.set_xlabel(f"Change in B intensity ({YEARS[0]}→{YEARS[-1]})")
    ax2.set_title(f"Top {top_n} Sector Changes (red = increase, green = decrease)")
    ax2.axvline(0, color="black", linewidth=0.8)
    ax2.grid(True, alpha=0.3, axis="x")
    fig2.tight_layout()
    fig2.savefig(OUTPUT_DIR / "sector_change_bar.png", dpi=150)
    logger.info("Saved sector_change_bar.png")
    plt.close(fig2)


def plot_stacked_bar(agg: pd.DataFrame) -> None:
    """Stacked bar chart of total B by gas across years."""
    fig, ax = plt.subplots(figsize=(10, 6))
    years = agg.columns
    bottom = np.zeros(len(years))

    for gas in agg.index:
        values = agg.loc[gas].values.astype(float)
        ax.bar(years, values, bottom=bottom, label=gas)
        bottom += values

    ax.set_xlabel("Year")
    ax.set_ylabel("Total B intensity (CO2e per $)")
    ax.set_title("Total GHG Intensity by Gas (Stacked, 2019-2023)")
    ax.legend()
    ax.grid(True, alpha=0.3, axis="y")
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "stacked_bar.png", dpi=150)
    logger.info("Saved stacked_bar.png")
    plt.close(fig)


def plot_all_sectors_line(
    B_by_year: dict[int, pd.DataFrame],
    x_by_year: dict[int, pd.Series],
) -> None:
    """Comprehensive line charts showing all sectors, indexed to base year = 100.

    For each gas and for the total (all gases):
    - Spaghetti plot: every sector as a light trace, indexed to
      2019 = 100 so trajectories are comparable across sectors.
    - Bold overlay for the median, 25th/75th percentile, and output-weighted mean.
    """

    def _build_indexed(intensity: pd.DataFrame) -> pd.DataFrame:
        """Return a (sector x year) DataFrame indexed to base year = 100.

        All sectors are included. Sectors with zero base-year value are
        set to index 100 (flat, representing no change from zero).
        """
        base = intensity[YEARS[0]]
        positive_base = base[base > 0].index
        indexed = intensity.copy()
        # Index sectors with positive base year
        indexed.loc[positive_base] = (  # type: ignore[call-overload]
            intensity.loc[positive_base].divide(base[positive_base], axis=0) * 100
        )
        # Sectors with zero base year → 100 (flat)
        zero_base = base.index.difference(positive_base)
        indexed.loc[zero_base] = 100.0  # type: ignore[call-overload]
        return indexed

    def _spaghetti(
        indexed: pd.DataFrame,
        raw_intensity: pd.DataFrame,
        x_by_year: dict[int, pd.Series],
        label: str,
        filename: str,
    ) -> None:
        """Plot all-sector spaghetti + percentile summary (two panels)."""
        if indexed.empty:
            return

        # Convert from index (100 = baseline) to % change (0 = baseline)
        pct_change = indexed - 100

        fig, (ax_pct, ax_abs) = plt.subplots(
            1,
            2,
            figsize=(16, 6),
            gridspec_kw={"width_ratios": [1, 1]},
        )

        # ── Left panel: % change ──
        for _, row in pct_change.iterrows():
            ax_pct.plot(YEARS, row.values, color="steelblue", alpha=0.15, linewidth=0.8)

        median_pct = pct_change.median(axis=0)
        p25_pct = pct_change.quantile(0.25, axis=0)
        p75_pct = pct_change.quantile(0.75, axis=0)

        ax_pct.plot(
            YEARS, median_pct.values, color="darkblue", linewidth=2.5, label="Median"
        )
        ax_pct.fill_between(
            YEARS,
            p25_pct.values.astype(float),
            p75_pct.values.astype(float),
            color="steelblue",
            alpha=0.25,
            label="25th–75th pctl",
        )

        # Annotate median % at each year
        for yr, val in zip(YEARS, median_pct.values):
            ax_pct.annotate(
                f"{float(val):+.0f}%",
                xy=(yr, float(val)),
                xytext=(0, 12),
                textcoords="offset points",
                fontsize=8,
                fontweight="bold",
                color="darkblue",
                ha="center",
            )

        # Output-weighted mean (% change)
        wmean_pct_vals = []
        for year in YEARS:
            x = x_by_year[year]
            weights = x.reindex(pct_change.index, fill_value=0)
            total_w = weights.sum()
            if total_w > 0:
                wmean_pct_vals.append(
                    float((pct_change[year] * weights).sum() / total_w)
                )
            else:
                wmean_pct_vals.append(0.0)
        ax_pct.plot(
            YEARS,
            wmean_pct_vals,
            color="crimson",
            linewidth=2.5,
            linestyle="--",
            label="Output-weighted mean",
        )

        for yr, val in zip(YEARS, wmean_pct_vals):
            ax_pct.annotate(
                f"{val:+.0f}%",
                xy=(yr, val),
                xytext=(0, -14),
                textcoords="offset points",
                fontsize=8,
                fontweight="bold",
                color="crimson",
                ha="center",
            )

        ax_pct.axhline(0, color="gray", linestyle="--", linewidth=0.8, alpha=0.6)
        ax_pct.set_xticks(YEARS)
        ax_pct.set_xlabel("Year")
        ax_pct.set_ylabel(f"% change from {YEARS[0]}")
        ax_pct.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f"{y:+.0f}%"))
        ax_pct.set_title("Relative change")
        ax_pct.legend(loc="lower left")
        ax_pct.grid(True, alpha=0.3)
        ax_pct.spines["top"].set_visible(False)
        ax_pct.spines["right"].set_visible(False)

        # ── Right panel: absolute intensity ──
        for _, row in raw_intensity.iterrows():
            ax_abs.plot(YEARS, row.values, color="steelblue", alpha=0.15, linewidth=0.8)

        median_abs = raw_intensity.median(axis=0)
        p25_abs = raw_intensity.quantile(0.25, axis=0)
        p75_abs = raw_intensity.quantile(0.75, axis=0)

        ax_abs.plot(
            YEARS, median_abs.values, color="darkblue", linewidth=2.5, label="Median"
        )
        ax_abs.fill_between(
            YEARS,
            p25_abs.values.astype(float),
            p75_abs.values.astype(float),
            color="steelblue",
            alpha=0.25,
            label="25th–75th pctl",
        )

        # Annotate median absolute value at each year
        for yr, val in zip(YEARS, median_abs.values):
            ax_abs.annotate(
                f"{float(val):.3f}",
                xy=(yr, float(val)),
                xytext=(0, 12),
                textcoords="offset points",
                fontsize=8,
                fontweight="bold",
                color="darkblue",
                ha="center",
            )

        # Output-weighted mean (absolute)
        wmean_abs_vals = []
        for year in YEARS:
            x = x_by_year[year]
            weights = x.reindex(raw_intensity.index, fill_value=0)
            total_w = weights.sum()
            if total_w > 0:
                wmean_abs_vals.append(
                    float((raw_intensity[year] * weights).sum() / total_w)
                )
            else:
                wmean_abs_vals.append(0.0)
        ax_abs.plot(
            YEARS,
            wmean_abs_vals,
            color="crimson",
            linewidth=2.5,
            linestyle="--",
            label="Output-weighted mean",
        )

        for yr, val in zip(YEARS, wmean_abs_vals):
            ax_abs.annotate(
                f"{val:.3f}",
                xy=(yr, val),
                xytext=(0, -14),
                textcoords="offset points",
                fontsize=8,
                fontweight="bold",
                color="crimson",
                ha="center",
            )

        ax_abs.set_xticks(YEARS)
        ax_abs.set_xlabel("Year")
        ax_abs.set_ylabel("B intensity (CO2e per $ output)")
        ax_abs.set_title("Absolute intensity")
        ax_abs.legend(loc="upper right")
        ax_abs.grid(True, alpha=0.3)
        ax_abs.spines["top"].set_visible(False)
        ax_abs.spines["right"].set_visible(False)

        fig.suptitle(
            f"{label} — All Sectors ({len(indexed)} sectors)",
            fontsize=13,
        )
        fig.tight_layout(rect=(0, 0, 1, 0.95))
        fig.savefig(OUTPUT_DIR / filename, dpi=150)
        plt.close(fig)

    # ── Total (all gases) ──
    total = pd.DataFrame(
        {year: B.sum(axis=0) for year, B in B_by_year.items()},
    )
    indexed_total = _build_indexed(total)
    _spaghetti(
        indexed_total, total, x_by_year, "Total (all gases)", "all_sectors_total.png"
    )

    logger.info("Saved all_sectors_total.png")


# ── Cache helpers ───────────────────────────────────────────────────────────


def save_intermediates(
    E_by_year: dict[int, pd.DataFrame],
    x_by_year: dict[int, pd.Series],
    B_by_year: dict[int, pd.DataFrame],
) -> None:
    """Save intermediate DataFrames to parquet for quick re-iteration."""
    cache_dir = OUTPUT_DIR / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    for year in YEARS:
        E_by_year[year].to_parquet(cache_dir / f"E_{year}.parquet")
        x_by_year[year].to_frame("x").to_parquet(cache_dir / f"x_{year}.parquet")
        B_by_year[year].to_parquet(cache_dir / f"B_{year}.parquet")

    logger.info("Saved intermediate data to %s", cache_dir)


def load_intermediates() -> (
    tuple[dict[int, pd.DataFrame], dict[int, pd.Series], dict[int, pd.DataFrame]]
):
    """Load cached intermediates if available."""
    cache_dir = OUTPUT_DIR / "cache"
    E_by_year: dict[int, pd.DataFrame] = {}
    x_by_year: dict[int, pd.Series] = {}
    B_by_year: dict[int, pd.DataFrame] = {}

    for year in YEARS:
        E_by_year[year] = pd.read_parquet(cache_dir / f"E_{year}.parquet")
        x_by_year[year] = pd.read_parquet(cache_dir / f"x_{year}.parquet")["x"]
        B_by_year[year] = pd.read_parquet(cache_dir / f"B_{year}.parquet")

    logger.info("Loaded cached intermediates from %s", cache_dir)
    return E_by_year, x_by_year, B_by_year


# ── Main ────────────────────────────────────────────────────────────────────


def main(use_cache: bool = False) -> dict[int, pd.DataFrame]:
    """Run the full pipeline: derive B time series and analyze.

    Parameters
    ----------
    use_cache : bool
        If True, skip derivation and load previously cached E, x, B.

    Returns
    -------
    dict mapping year → B matrix DataFrame
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if use_cache:
        E_by_year, x_by_year, B_by_year = load_intermediates()
    else:
        # Step 1: Download FBS
        logger.info("=" * 60)
        logger.info("STEP 1: Downloading FBS from GCS")
        logger.info("=" * 60)
        fbs_by_year = download_fbs_parquets()
        check_fbs_sector_schema(fbs_by_year)

        # Step 2: Derive x
        logger.info("=" * 60)
        logger.info("STEP 2: Deriving x (gross output) time series")
        logger.info("=" * 60)
        x_by_year = derive_x_time_series()

        # Step 3: FBS → E
        logger.info("=" * 60)
        logger.info("STEP 3: Converting FBS → E matrices")
        logger.info("=" * 60)
        E_by_year = derive_E_time_series(fbs_by_year)

        # Step 4: Vnorm
        logger.info("=" * 60)
        logger.info("STEP 4: Deriving Vnorm (2017 benchmark)")
        logger.info("=" * 60)
        Vnorm = derive_Vnorm()

        # Step 5: Compute B
        logger.info("=" * 60)
        logger.info("STEP 5: Computing B = (E / x) @ Vnorm")
        logger.info("=" * 60)
        B_by_year = compute_B_time_series(E_by_year, x_by_year, Vnorm)

        # Cache intermediates
        save_intermediates(E_by_year, x_by_year, B_by_year)

    # Vnorm is needed for decomposition — derive_Vnorm is cached internally,
    # so this is cheap on the non-cache path (returns the same object).
    Vnorm = derive_Vnorm()

    # Step 6: Analysis
    logger.info("=" * 60)
    logger.info("STEP 6: Analysis")
    logger.info("=" * 60)

    agg = analyze_aggregate_trends(E_by_year, x_by_year)
    analyze_sector_changes(B_by_year)
    analyze_non_monotonic(B_by_year)
    decompose_E_vs_x(E_by_year, x_by_year, Vnorm)

    # Visualizations
    logger.info("Generating plots...")
    plot_aggregate_trends(agg)
    plot_all_sectors_line(B_by_year, x_by_year)

    return B_by_year


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Derive B matrix time series (2019-2023)"
    )
    parser.add_argument(
        "--use-cache",
        action="store_true",
        help="Load cached intermediates instead of re-deriving",
    )
    parser.add_argument(
        "--list-fbs",
        action="store_true",
        help="List available m2 FBS files on GCS and exit",
    )
    args = parser.parse_args()

    if args.list_fbs:
        list_available_fbs()
    else:
        main(use_cache=args.use_cache)
