"""Export waste-disaggregated Cornerstone matrices for offline electricity disaggregation.

Requires active :class:`~bedrock.utils.config.usa_config.USAConfig` to match the methodology
flags in ``2025_usa_cornerstone_full_model.yaml`` and loadable waste disaggregation weights.
"""

from __future__ import annotations

import pathlib

from bedrock.transform.allocation.derived import derive_E_usa
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_U_set,
    derive_cornerstone_V,
    derive_cornerstone_VA,
    derive_cornerstone_Ytot_full_cs_matrix,
    get_waste_disagg_weights,
)
from bedrock.utils.config.usa_config import get_usa_config

_DISAGG_ROOT = pathlib.Path(__file__).resolve().parent

_FULL_MODEL_MATRIX_EXPORT_FLAGS: dict[str, bool] = {
    "use_cornerstone_2026_model_schema": True,
    "load_E_from_flowsa": True,
    "new_ghg_method": True,
    "use_E_data_year_for_x_in_B": True,
    "implement_waste_disaggregation": True,
}


def assert_cornerstone_matrix_export_preconditions() -> None:
    """Raise ``RuntimeError`` if the current config cannot dump cornerstone matrix CSVs."""
    cfg = get_usa_config()
    mismatches: list[str] = []
    for key, expected in _FULL_MODEL_MATRIX_EXPORT_FLAGS.items():
        actual = getattr(cfg, key)
        if actual is not expected:
            mismatches.append(f"{key}={actual!r} (expected {expected!r})")
    if mismatches:
        raise RuntimeError(
            "Cornerstone matrix export requires USAConfig methodology flags matching "
            "`2025_usa_cornerstone_full_model.yaml`: "
            + "; ".join(mismatches)
            + ". Load that YAML via set_global_usa_config(...) before exporting."
        )
    if get_waste_disagg_weights() is None:
        raise RuntimeError(
            "Cornerstone matrix export requires waste disaggregation weights "
            "(implement_waste_disaggregation and readable weight CSVs)."
        )


def export_cornerstone_matrices_to_csv(
    output_dir: pathlib.Path | None = None,
) -> pathlib.Path:
    """Write Make, Use, VA, full Y, and E to CSV under *output_dir* (default: electricity_disagg_inputs).

    Returns the directory written.
    """
    assert_cornerstone_matrix_export_preconditions()
    out = (
        _DISAGG_ROOT / "electricity_disagg_inputs"
        if output_dir is None
        else pathlib.Path(output_dir)
    )
    out.mkdir(parents=True, exist_ok=True)

    V = derive_cornerstone_V()
    uset = derive_cornerstone_U_set()
    VA = derive_cornerstone_VA()
    Y = derive_cornerstone_Ytot_full_cs_matrix()
    E = derive_E_usa()

    V.to_csv(out / "cornerstone_V.csv", index=True)
    uset.Udom.to_csv(out / "cornerstone_Udom.csv", index=True)
    uset.Uimp.to_csv(out / "cornerstone_Uimp.csv", index=True)
    VA.to_csv(out / "cornerstone_VA.csv", index=True)
    Y.to_csv(out / "cornerstone_Ytot_full_cs.csv", index=True)
    E.to_csv(out / "cornerstone_E.csv", index=True)
    return out


if __name__ == "__main__":
    # Offline use: set USA_CONFIG_FILE or call set_global_usa_config first.
    from bedrock.utils.config.usa_config import set_global_usa_config

    set_global_usa_config("2025_usa_cornerstone_full_model.yaml")
    dest = export_cornerstone_matrices_to_csv()
    print(f"Wrote cornerstone matrix CSVs to {dest.resolve()}")
