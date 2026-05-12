"""Phase A.1 — Derive A matrices for the two LMDI ingredients and persist.

Produces, for each ``(method, year)`` with method ∈ {``summary_tables``,
``industry_price_index``} and year ∈ {2019..2023}:

- ``output/results/A_snapshots/{method}_{year}_A.parquet`` — long-format Adom
  and Aimp cells: columns ``i`` (input commodity), ``j`` (output commodity),
  ``kind`` (``dom``/``imp``), ``value``.
- ``output/results/A_snapshots/{method}_{year}_q.parquet`` — scaled q (output
  vector), columns ``sector``, ``q``.

These feed ``compute_lmdi_phys.py`` (Phase A.2) which derives the
physical-shift multiplier ``Q_phys = A_summary / A_pi`` at the cell level and
aggregates it via LMDI weights.

Why not reuse existing time-series N runs: the remote diagnostics workflow
persists only ``N``, not ``A``. Re-deriving locally costs ~10–15 s per cell.

Usage:
    python -m bedrock.analysis.a_matrix_time_series.signal_noise.derive_A_snapshots
"""

from __future__ import annotations

import logging
import time

import pandas as pd

from bedrock.analysis.a_matrix_time_series.constants import RESULTS_DIR

logger = logging.getLogger(__name__)

A_SNAPSHOTS_DIR = RESULTS_DIR / "A_snapshots"

# Methods we need for LMDI:
# - summary_tables → A_summary (price + physical channels combined)
# - commodity_price_index → A_pi (price-only channel; LMDI's price term)
#
# Commodity PI is the right denominator because A is in commodity space; using
# industry PI would leave residual price motion in volatile commodity sectors
# (oil & gas, petroleum, agriculture) that would masquerade as physical
# signal. See signal_noise_plan.md §"Why commodity PI rather than industry PI".
CONFIGS: dict[str, str] = {
    "summary_tables": "2025_usa_cornerstone_A_summary_tables",
    "commodity_price_index": "2025_usa_cornerstone_A_commodity_price_index",
}
# Full LMDI window. Year 2017 is the A-matrix detail-benchmark base: at
# year=2017 both methods reduce to the identity (no scaling), so
# Q_phys(2017) = 1 by construction — useful anchor.
YEARS: tuple[int, ...] = (2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024)


def _clear_derived_caches() -> None:
    """Clear ``@functools.cache`` decorators in ``derived_cornerstone`` so a
    config switch (or model_base_year change) doesn't reuse a prior result.

    ``derive_cornerstone_Aq_scaled`` is the top-level cached function that
    branches on ``model_base_year``. Clearing all caches in the module is
    over-eager but cheap and immune to future cache additions.
    """
    from bedrock.transform.eeio import derived_cornerstone  # noqa: PLC0415

    for name in dir(derived_cornerstone):
        obj = getattr(derived_cornerstone, name)
        if hasattr(obj, "cache_clear"):
            obj.cache_clear()


def _A_to_long(df: pd.DataFrame, kind: str) -> pd.DataFrame:
    """Stack a 405×405 A matrix into long form (i, j, kind, value).

    Both axes are named ``sector``; rename to (i, j) so reset_index doesn't
    collide.
    """
    stacked = df.rename_axis(index="i", columns="j").stack()
    long = pd.DataFrame(
        {
            "i": stacked.index.get_level_values("i"),
            "j": stacked.index.get_level_values("j"),
            "kind": kind,
            "value": stacked.to_numpy(),
        }
    )
    return long


def _derive_one(config_name: str, year: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Derive (A long-format, q) for one ``(config_name, year)``."""
    from bedrock.transform.eeio.derived_cornerstone import (  # noqa: PLC0415
        derive_cornerstone_Aq_scaled,
    )
    from bedrock.utils.config.usa_config import (  # noqa: PLC0415
        reset_usa_config,
        set_global_usa_config,
    )

    reset_usa_config(should_reset_env_var=True)
    _clear_derived_caches()
    set_global_usa_config(
        config_name,
        diagnostics_cli_overrides={"model_base_year": year},
    )

    aq = derive_cornerstone_Aq_scaled()

    A_long = pd.concat(
        [_A_to_long(aq.Adom, "dom"), _A_to_long(aq.Aimp, "imp")],
        ignore_index=True,
    )
    q = aq.scaled_q.rename("q").reset_index()
    q.columns = pd.Index(["sector", "q"])
    return A_long, q


def main() -> None:
    A_SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    for method, config_name in CONFIGS.items():
        for year in YEARS:
            out_A = A_SNAPSHOTS_DIR / f"{method}_{year}_A.parquet"
            out_q = A_SNAPSHOTS_DIR / f"{method}_{year}_q.parquet"
            logger.info("Deriving %s × %d", method, year)
            tic = time.time()
            A_long, q = _derive_one(config_name, year)
            A_long.to_parquet(out_A, index=False)
            q.to_parquet(out_q, index=False)
            logger.info(
                "  %s rows, %.1fs → %s",
                f"{len(A_long):,}",
                time.time() - tic,
                out_A.name,
            )
    logger.info("All snapshots done in %.1fs", time.time() - t0)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
