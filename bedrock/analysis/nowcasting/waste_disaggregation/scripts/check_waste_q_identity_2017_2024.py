"""#1165 multi-year check: waste child Make col ≈ Udom+Uimp+Y within 1%.

Runs on production year configs ``2026_usa_cornerstone_v1_0_{2017..2024}``
(nowcast MUT ``v1.0.0_b17908f``, waste on, ``match_io``).

From repo root::

    uv run python -m bedrock.analysis.nowcasting.waste_disaggregation.scripts.check_waste_q_identity_2017_2024
"""

from __future__ import annotations

import argparse
import sys
import warnings
from dataclasses import dataclass

import pandas as pd

from bedrock.extract.disaggregation.waste_static_rules import WASTE_CHILDREN
from bedrock.publish.cache_reset import clear_all_publish_caches
from bedrock.transform.eeio.sector_disagg_pipeline import (
    clear_waste_disagg_weights_cache,
    derive_U_after_waste,
    derive_V_after_waste,
    derive_y_after_waste,
    get_waste_disagg_provenance,
)
from bedrock.transform.eeio.waste_disaggregation import compute_waste_q_targets
from bedrock.utils.config.usa_config import (
    reset_usa_config,
    set_global_usa_config,
)

YEARS = tuple(range(2017, 2025))
TOL_REL = 0.01
CONFIG_TMPL = "2026_usa_cornerstone_v1_0_{year}"


@dataclass
class YearResult:
    year: int
    config: str
    ok: bool
    max_rel: float
    failures: list[str]
    error: str | None = None


def _config_for_year(year: int) -> str:
    return CONFIG_TMPL.format(year=year)


def check_year(year: int, *, tol: float = TOL_REL) -> YearResult:
    config = _config_for_year(year)
    clear_all_publish_caches()
    clear_waste_disagg_weights_cache()
    reset_usa_config(should_reset_env_var=True)
    set_global_usa_config(config)

    try:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            V = derive_V_after_waste()
            Udom, Uimp = derive_U_after_waste()
            Y = derive_y_after_waste()
        present = [c for c in WASTE_CHILDREN if c in V.columns and c in Udom.index]
        if not present:
            return YearResult(
                year=year,
                config=config,
                ok=False,
                max_rel=float("nan"),
                failures=[],
                error="no waste children present in V/U after waste",
            )
        q = compute_waste_q_targets(Udom, Uimp, Y, waste_codes=present)
        make_cols = V[present].sum(axis=0).astype(float)
        failures: list[str] = []
        max_rel = 0.0
        print(f"\n=== {year} ({config}) ===")
        prov = get_waste_disagg_provenance()
        if prov is not None:
            print(f"provenance: {prov}")
        rows = []
        for c in present:
            qt = float(q.loc[c])
            qm = float(make_cols.loc[c])
            denom = max(1.0, abs(qt))
            rel = abs(qm - qt) / denom
            max_rel = max(max_rel, rel)
            status = "PASS" if rel <= tol else "FAIL"
            if status == "FAIL":
                failures.append(f"{c}: Make={qm:.6g} q_target={qt:.6g} rel={rel:.4%}")
            rows.append(
                {
                    "sector": c,
                    "Make_col_$": qm,
                    "q_target_$": qt,
                    "rel_abs_diff": rel,
                    "status": status,
                }
            )
        print(pd.DataFrame(rows).to_string(index=False))
        waste_warns = [
            str(w.message) for w in caught if "waste" in str(w.message).lower()
        ]
        if waste_warns:
            print(f"waste warnings ({len(waste_warns)}), showing up to 5:")
            for msg in waste_warns[:5]:
                print(f"  - {msg}")
        return YearResult(
            year=year,
            config=config,
            ok=not failures,
            max_rel=max_rel,
            failures=failures,
        )
    except Exception as exc:  # noqa: BLE001 — report per-year and continue
        print(f"\n=== {year} ({config}) ERROR ===")
        print(f"{type(exc).__name__}: {exc}")
        return YearResult(
            year=year,
            config=config,
            ok=False,
            max_rel=float("nan"),
            failures=[],
            error=f"{type(exc).__name__}: {exc}",
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Check waste child commodity identity for 2017–2024 nowcast MUT."
    )
    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        default=list(YEARS),
        help="Years to check (default: 2017..2024)",
    )
    parser.add_argument(
        "--tol",
        type=float,
        default=TOL_REL,
        help="Relative tolerance (default: 0.01)",
    )
    args = parser.parse_args(argv)

    results = [check_year(y, tol=args.tol) for y in args.years]

    print("\n========== SUMMARY ==========")
    summary = pd.DataFrame(
        [
            {
                "year": r.year,
                "ok": r.ok,
                "max_rel": r.max_rel,
                "n_fail": len(r.failures),
                "error": r.error or "",
            }
            for r in results
        ]
    )
    print(summary.to_string(index=False))
    for r in results:
        if r.failures:
            print(f"\n{r.year} failures:")
            for line in r.failures:
                print(f"  - {line}")

    all_ok = all(r.ok and r.error is None for r in results)
    print("\nOVERALL:", "PASS" if all_ok else "FAIL")
    return 0 if all_ok else 1


if __name__ == "__main__":
    pd.set_option("display.width", 140)
    pd.set_option("display.max_columns", 20)
    pd.set_option("display.float_format", lambda x: f"{x:.6g}")
    sys.exit(main())
