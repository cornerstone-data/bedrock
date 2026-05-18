"""Export waste-disaggregated Cornerstone matrices for offline electricity disaggregation.

Requires ``USA_CONFIG_FILE=2025_usa_cornerstone_full_model.yaml`` and loadable waste
disaggregation weights. Intended as a one-time offline script to generate electricity
disaggregation inputs, not as a pipeline integration point.
"""

from __future__ import annotations

import os
import pathlib

from bedrock.publish.excel.writer import clear_publish_caches
from bedrock.transform.allocation.derived import derive_E_usa
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_U_set,
    derive_cornerstone_V,
    derive_cornerstone_VA,
    derive_cornerstone_Ytot_full_cs_matrix,
    get_waste_disagg_weights,
)
from bedrock.utils.config.usa_config import USA_CONFIG_ENV_VAR

_DISAGG_ROOT: pathlib.Path = pathlib.Path(__file__).resolve().parent

_REQUIRED_CONFIG_FILE: str = "2025_usa_cornerstone_full_model.yaml"


def assert_cornerstone_matrix_export_preconditions() -> None:
    """Raise ``RuntimeError`` if the active config file or waste weights are wrong."""
    if os.environ.get(USA_CONFIG_ENV_VAR) != _REQUIRED_CONFIG_FILE:
        raise RuntimeError(
            "Cornerstone matrix export requires "
            f"{USA_CONFIG_ENV_VAR}={_REQUIRED_CONFIG_FILE!r}; got "
            f"{os.environ.get(USA_CONFIG_ENV_VAR)!r}. "
            f'Load that YAML via set_global_usa_config("{_REQUIRED_CONFIG_FILE}") '
            "before exporting."
        )
    if get_waste_disagg_weights() is None:
        raise RuntimeError(
            "Cornerstone matrix export requires waste disaggregation weights "
            "(implement_waste_disaggregation and readable weight CSVs)."
        )


def export_cornerstone_matrices_to_csv(
    output_dir: pathlib.Path | None = None,
) -> pathlib.Path:
    """Write V, Udom, Uimp, VA, full Y, and E to CSV under *output_dir*.

    Default *output_dir* is ``bedrock/extract/disaggregation/electricity_disagg_inputs/``.
    Returns the directory written.
    """
    assert_cornerstone_matrix_export_preconditions()
    clear_publish_caches()

    out = (
        _DISAGG_ROOT / "electricity_disagg_inputs"
        if output_dir is None
        else pathlib.Path(output_dir)
    )
    out.mkdir(parents=True, exist_ok=True)

    # Follows the derivation process of derived_cornerstone.py,
    # except for derive_cornerstone_Ytot_full_cs_matrix() which recreates the full Y matrix in CS space
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
    from bedrock.utils.config.usa_config import set_global_usa_config

    if not os.environ.get(USA_CONFIG_ENV_VAR):
        set_global_usa_config(_REQUIRED_CONFIG_FILE)
    dest = export_cornerstone_matrices_to_csv()
    print(f"Wrote cornerstone matrix CSVs to {dest.resolve()}")
