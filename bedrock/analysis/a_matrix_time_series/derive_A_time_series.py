"""Derive A matrices for every (approach × year) combination.

Step 1 of epic #337. Produces parquet caches as the raw substrate for
Steps 2–6, and creates a per-run Google Sheet in the analysis Drive folder
(`1UcPmwLnL6MwTq9pMYJw5d43FJQOFQVO_`) with two summary tabs:

- ``cache_summary`` — per (approach, year, matrix_kind) shape + integrity stats
- ``sanity_2017_identity_check`` — confirms ``useeio`` and
  ``commodity_price_index`` collapse to the BEA-2017 base A at
  ``model_base_year = 2017``. ``summary_tables`` reads BEA's *summary*
  aggregation rather than the *detail* base, so it isn't expected to be
  identity at any year (recorded but not pass-fail tested). ``ceda_default``
  does a non-inverse round trip via the legacy CEDA io_year, same treatment.

The new sheet ID is logged and persisted to ``last_run_sheet_id.txt`` so the
downstream steps (#341–#344) can append their own summary tabs to the same
report.

Usage:
    python -m bedrock.analysis.a_matrix_time_series.derive_A_time_series
"""

from __future__ import annotations

import datetime as dt
import logging
import os
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

import bedrock.utils.config.usa_config as cfg_module
from bedrock.analysis.a_matrix_time_series.constants import (
    ANALYSIS_DRIVE_FOLDER_ID,
    APPROACH_YEAR_COVERAGE,
    LAST_RUN_SHEET_ID_PATH,
    LATEST_TARGET_YEAR,
    ORIGINAL_YEAR,
    PLOTS_DIR,
    RESULTS_DIR,
)
from bedrock.utils.config.usa_config import USAConfig
from bedrock.utils.io.gcp import create_spreadsheet_in_folder, update_sheet_tab

logger = logging.getLogger(__name__)

APPROACH_YAMLS: dict[str, str] = {
    "useeio": "2025_usa_cornerstone_A_useeio.yaml",
    "summary_tables": "2025_usa_cornerstone_A_summary_tables.yaml",
    "commodity_price_index": "2025_usa_cornerstone_A_commodity_price_index.yaml",
    "ceda_default": "2025_usa_cornerstone_taxonomy.yaml",  # CEDA baseline with Cornerstone schema
    "useeio_nowcast": "2025_usa_cornerstone_A_useeio_nowcast.yaml",  # external reference
}
APPROACHES: list[str] = list(APPROACH_YAMLS.keys())


def _years_for(approach: str, all_years: list[int]) -> list[int]:
    """Filter ``all_years`` to the set this approach has data for.

    ``useeio_nowcast`` has no 2024 upstream — skip silently rather than
    fail or extrapolate. Other approaches default to the full range.
    """
    allowed = APPROACH_YEAR_COVERAGE.get(approach)
    if allowed is None:
        return all_years
    return [y for y in all_years if y in allowed]


# Includes ORIGINAL_YEAR (the BEA detail base year) for the 2017-identity
# sanity check tab.
TARGET_YEARS: list[int] = list(range(ORIGINAL_YEAR, LATEST_TARGET_YEAR + 1))

# Modules whose @functools.cache outputs are config-dependent and must be
# invalidated between (approach, year) iterations.
_CACHE_BEARING_MODULE_PATHS = (
    "bedrock.transform.eeio.derived_cornerstone",
    "bedrock.transform.eeio.cornerstone_bea_intermediates",
    "bedrock.transform.eeio.derived_useeio_nowcast",
    "bedrock.utils.economic.inflation_helpers_cornerstone",
)


def _clear_config_dependent_caches() -> None:
    """Clear `@functools.cache` outputs in all config-dependent modules."""
    import importlib  # noqa: PLC0415

    for module_path in _CACHE_BEARING_MODULE_PATHS:
        module = importlib.import_module(module_path)
        for name in dir(module):
            obj = getattr(module, name, None)
            if callable(obj) and hasattr(obj, "cache_clear"):
                obj.cache_clear()


def _set_config(approach: str, year: int) -> None:
    """Install a config for the given (approach, year), bypassing pydantic
    Literal validation on ``model_base_year`` since we run for 2017–2024 but
    the schema only allows 2022–2024.

    Uses ``USAConfig.model_construct`` to skip validation. Also clears the
    ``USA_CONFIG_FILE`` env var and ``_usa_config`` module global so each call
    is fresh — ``set_global_usa_config`` raises if invoked twice.
    """
    yaml_path = Path(cfg_module.CONFIG_DIR) / APPROACH_YAMLS[approach]
    with open(yaml_path) as f:
        data = yaml.safe_load(f) or {}
    data["model_base_year"] = year
    # The package `__init__.py` flips these on the initial config; replacing
    # `_usa_config` here would otherwise drop them back to field defaults.
    data["update_inflation_factors"] = True
    # `commodity_price_index.yaml` already sets this; setting it for every
    # approach is harmless (only `commodity_price_index` reads it via
    # `get_vnorm_adjusted_commodity_price_ratio`).
    data["apply_inflation_to_V"] = True

    cfg_module._usa_config = USAConfig.model_construct(**data)
    os.environ[cfg_module.USA_CONFIG_ENV_VAR] = APPROACH_YAMLS[approach]


def _reset_config() -> None:
    cfg_module._usa_config = None
    os.environ.pop(cfg_module.USA_CONFIG_ENV_VAR, None)


def _derive_one_pair(
    approach: str, year: int
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series]:
    """Run the cornerstone A pipeline for one (approach, year) pair.

    Returns ``(Adom, Aimp, q)``. Caches are cleared and the config is
    reinstalled before the call to guarantee a fresh derivation.
    """
    _reset_config()
    _clear_config_dependent_caches()
    _set_config(approach, year)

    from bedrock.transform.eeio.derived_cornerstone import (  # noqa: PLC0415
        derive_cornerstone_Aq_scaled,
    )

    aq = derive_cornerstone_Aq_scaled()
    return aq.Adom, aq.Aimp, aq.scaled_q


def _save_parquets(
    approach: str, year: int, adom: pd.DataFrame, aimp: pd.DataFrame, q: pd.Series
) -> tuple[Path, Path]:
    """Write the (Adom, Aimp, q) triple as two parquets under ``output/results/``.

    Adom + Aimp are stacked into a single multi-index DataFrame (index level
    ``dom_or_imp`` ∈ {dom, imp}); q is written separately because its single
    column makes a stacked layout awkward.
    """
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    a_path = RESULTS_DIR / f"A_{approach}_{year}.parquet"
    q_path = RESULTS_DIR / f"q_{approach}_{year}.parquet"

    combined = pd.concat({"dom": adom, "imp": aimp}, axis=0, names=["dom_or_imp"])
    combined.to_parquet(a_path)
    q.to_frame("q").to_parquet(q_path)

    return a_path, q_path


def _matrix_stats(
    approach: str, year: int, kind: str, df: pd.DataFrame, file_path: Path
) -> dict[str, object]:
    """Per-matrix summary row for the cache_summary tab."""
    arr = df.to_numpy()
    return {
        "approach": approach,
        "year": year,
        "matrix_kind": kind,
        "n_rows": int(df.shape[0]),
        "n_cols": int(df.shape[1]),
        "nan_count": int(np.isnan(arr).sum()),
        "neg_count": int((arr < 0).sum()),
        "max_col_sum": float(np.nansum(arr, axis=0).max()) if arr.size else 0.0,
        "file_size_bytes": int(file_path.stat().st_size),
    }


_EXPECTED_IDENTITY_AT_2017 = {
    "useeio",
    "commodity_price_index",
}
"""Approaches whose 2017 output should equal the BEA-2017 base A within rtol.

``summary_tables`` reads BEA *summary*-level tables rather than the *detail*
base; the two aggregations differ even at the same year, so it isn't expected
to be identity. ``ceda_default`` does ``scale(2017→io_year)`` then
``inflate(io_year→2017)`` — not an inverse pair, so its 2017 output is also
expected to deviate."""


def _build_sanity_2017(
    matrices_2017: dict[str, tuple[pd.DataFrame, pd.DataFrame, pd.Series]],
    rtol: float = 1e-10,
) -> pd.DataFrame:
    """At ``model_base_year = 2017``, the four alternative approaches must
    collapse to the BEA-2017 base A (identity transformations); ``ceda_default``
    does a non-inverse round trip and is recorded but not pass-fail tested.

    Reference is ``useeio`` (returns ``base`` directly — simplest semantics).
    """
    if not matrices_2017 or "useeio" not in matrices_2017:
        return pd.DataFrame()

    ref_dom, ref_imp, ref_q = matrices_2017["useeio"]

    def _max_rel_dev(a: pd.DataFrame, b: pd.DataFrame) -> float:
        diff = a.to_numpy() - b.to_numpy()
        denom = np.where(np.abs(b.to_numpy()) > 0, np.abs(b.to_numpy()), 1.0)
        return float(np.abs(diff / denom).max())

    rows = []
    for name, (dom, imp, q) in sorted(matrices_2017.items()):
        max_dom = _max_rel_dev(dom, ref_dom)
        max_imp = _max_rel_dev(imp, ref_imp)
        max_q = _max_rel_dev(q.to_frame(), ref_q.to_frame())
        expected_identity = name in _EXPECTED_IDENTITY_AT_2017
        max_rel_dev = max(max_dom, max_imp, max_q)
        rows.append(
            {
                "approach": name,
                "reference": "useeio",
                "expected_identity": expected_identity,
                "max_rel_dev_Adom": max_dom,
                "max_rel_dev_Aimp": max_imp,
                "max_rel_dev_q": max_q,
                "passes": (bool(max_rel_dev <= rtol) if expected_identity else None),
            }
        )
    return pd.DataFrame(rows)


def _git_sha_short() -> str:
    try:
        sha = subprocess.check_output(
            ["git", "rev-parse", "--short=7", "HEAD"], text=True
        ).strip()
        return sha or "unknown"
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"


def _publish_run_report(summary_df: pd.DataFrame, sanity_df: pd.DataFrame) -> str:
    """Create the run-report Sheet and write the two summary tabs.

    Returns the new sheet ID.
    """
    title = (
        f"a_matrix_time_series__{_git_sha_short()}__"
        f"{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    )
    sheet_id = create_spreadsheet_in_folder(
        title=title, folder_id=ANALYSIS_DRIVE_FOLDER_ID
    )
    update_sheet_tab(sheet_id, "cache_summary", summary_df)
    if not sanity_df.empty:
        update_sheet_tab(sheet_id, "sanity_2017_identity_check", sanity_df)
    return sheet_id


def main() -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    summary_rows: list[dict[str, object]] = []
    matrices_2017: dict[str, tuple[pd.DataFrame, pd.DataFrame, pd.Series]] = {}

    for approach in APPROACHES:
        for year in _years_for(approach, TARGET_YEARS):
            logger.info("Deriving A matrix: approach=%s year=%d", approach, year)
            try:
                adom, aimp, q = _derive_one_pair(approach, year)
            except Exception as e:  # noqa: BLE001 — record any per-pair failure
                logger.warning("Pair (%s, %d) failed: %s", approach, year, e)
                summary_rows.append(
                    {
                        "approach": approach,
                        "year": year,
                        "matrix_kind": "FAILED",
                        "error": f"{type(e).__name__}: {e}",
                    }
                )
                continue

            a_path, q_path = _save_parquets(approach, year, adom, aimp, q)
            summary_rows.append(_matrix_stats(approach, year, "Adom", adom, a_path))
            summary_rows.append(_matrix_stats(approach, year, "Aimp", aimp, a_path))
            summary_rows.append(
                _matrix_stats(approach, year, "q", q.to_frame("q"), q_path)
            )

            if year == 2017:
                matrices_2017[approach] = (adom, aimp, q)

    summary_df = pd.DataFrame(summary_rows)
    sanity_df = _build_sanity_2017(matrices_2017)
    summary_df.to_csv(RESULTS_DIR / "cache_summary.csv", index=False)
    if not sanity_df.empty:
        sanity_df.to_csv(RESULTS_DIR / "sanity_2017_identity_check.csv", index=False)

    try:
        sheet_id = _publish_run_report(summary_df, sanity_df)
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "Sheet publish skipped (%s: %s). Local parquet caches and CSVs in "
            "%s are complete; Steps 2-5 that read last_run_sheet_id.txt won't "
            "have a target until the run is re-published with valid Drive auth.",
            type(e).__name__,
            e,
            RESULTS_DIR,
        )
        return

    LAST_RUN_SHEET_ID_PATH.write_text(sheet_id + "\n")
    sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}"
    logger.info("Run report: %s", sheet_url)
    print(f"Run report Sheet: {sheet_url}")
    print(f"Sheet ID written to: {LAST_RUN_SHEET_ID_PATH}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
