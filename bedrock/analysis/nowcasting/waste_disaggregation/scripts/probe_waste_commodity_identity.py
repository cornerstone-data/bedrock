"""Analysis-only probe: waste child Make vs Use+FD identity after balance.

Run (from repo root, with data available)::

    uv run python -m bedrock.analysis.nowcasting.waste_disaggregation.scripts.probe_waste_commodity_identity
"""

from __future__ import annotations

import warnings

import pandas as pd

from bedrock.extract.disaggregation.waste_static_rules import WASTE_CHILDREN
from bedrock.publish.cache_reset import clear_all_publish_caches
from bedrock.transform.eeio.sector_disagg_pipeline import (
    clear_waste_disagg_weights_cache,
    derive_U_after_waste,
    derive_V_after_waste,
    derive_VA_after_waste,
    derive_y_after_waste,
    get_waste_disagg_provenance,
)
from bedrock.transform.eeio.waste_disaggregation import compute_waste_q_targets
from bedrock.utils.config.usa_config import (
    reset_usa_config,
    set_global_usa_config,
)


def _probe(config_name: str) -> None:
    clear_all_publish_caches()
    clear_waste_disagg_weights_cache()
    reset_usa_config(should_reset_env_var=True)
    set_global_usa_config(config_name)
    print(f"\n=== {config_name} ===")
    prov = get_waste_disagg_provenance()
    if prov is not None:
        print(f"provenance: {prov}")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        V = derive_V_after_waste()
        Udom, Uimp = derive_U_after_waste()
        Y = derive_y_after_waste()
        VA = derive_VA_after_waste()
    present = [c for c in WASTE_CHILDREN if c in V.columns]
    q = compute_waste_q_targets(Udom, Uimp, Y, waste_codes=present)
    print("q_target:")
    print(q.to_string())
    print("Make column totals:")
    print(V[present].sum(axis=0).to_string())
    print("VA waste column totals:")
    print(VA[present].sum(axis=0).to_string())
    gap = (V[present].sum(axis=0) - q).abs() / q.abs().clip(lower=1.0)
    print("rel |Make-q_target|:")
    print(gap.to_string())
    waste_warns = [str(w.message) for w in caught if "waste" in str(w.message).lower()]
    if waste_warns:
        print(f"waste warnings ({len(waste_warns)}):")
        for msg in waste_warns[:20]:
            print(f"  - {msg}")


def main() -> None:
    for cfg in (
        "test_usa_config_waste_disagg",
        # match_io nowcast configs vary by checkout; skip if unavailable
    ):
        try:
            _probe(cfg)
        except Exception as exc:  # noqa: BLE001 — probe is best-effort
            print(f"probe failed for {cfg}: {type(exc).__name__}: {exc}")


if __name__ == "__main__":
    pd.set_option("display.width", 120)
    pd.set_option("display.max_columns", 20)
    main()
