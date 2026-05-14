"""Export waste-disaggregated Cornerstone matrices for offline electricity disaggregation.

Requires active :class:`~bedrock.utils.config.usa_config.USAConfig` to match the methodology
subset in ``2025_usa_cornerstone_full_model.yaml`` (parsed via ``USAConfig``) and loadable
waste disaggregation weights.
"""

from __future__ import annotations

import os
import pathlib

import yaml
from pydantic import ValidationError

from bedrock.extract.disaggregation.electricity_disagg_cornerstone_materializer import (
    materialize_electricity_disagg_cornerstone_frames,
)
from bedrock.transform.eeio.derived_cornerstone import get_waste_disagg_weights
from bedrock.utils.config.usa_config import CONFIG_DIR, USAConfig, get_usa_config

_DISAGG_ROOT = pathlib.Path(__file__).resolve().parent

CORNERSTONE_FULL_MODEL_EXPORT_YAML = '2025_usa_cornerstone_full_model.yaml'

_EXPORT_METHODOLOGY_KEYS: tuple[str, ...] = (
    'use_cornerstone_2026_model_schema',
    'load_E_from_flowsa',
    'new_ghg_method',
    'use_E_data_year_for_x_in_B',
    'implement_waste_disaggregation',
)


def _load_expected_cornerstone_export_config() -> USAConfig:
    path = os.path.join(CONFIG_DIR, CORNERSTONE_FULL_MODEL_EXPORT_YAML)
    with open(path, encoding='utf-8') as f:
        data = yaml.safe_load(f)
    try:
        return USAConfig.model_validate(data, strict=True)
    except ValidationError as e:
        raise RuntimeError(
            f'Failed to parse methodology YAML {CORNERSTONE_FULL_MODEL_EXPORT_YAML!r}: {e}'
        ) from e


def assert_cornerstone_matrix_export_preconditions() -> None:
    """Raise ``RuntimeError`` if the current config cannot dump cornerstone matrix CSVs."""

    cfg = get_usa_config()
    expected = _load_expected_cornerstone_export_config()
    mismatches: list[str] = []
    for key in _EXPORT_METHODOLOGY_KEYS:
        actual = getattr(cfg, key)
        exp_val = getattr(expected, key)
        if actual != exp_val:
            mismatches.append(f'{key}={actual!r} (expected {exp_val!r} from YAML pin)')
    if mismatches:
        raise RuntimeError(
            'Cornerstone matrix export requires USAConfig methodology flags matching '
            f'`{CORNERSTONE_FULL_MODEL_EXPORT_YAML}`: '
            + '; '.join(mismatches)
            + '. Load that YAML via set_global_usa_config(...) before exporting.'
        )
    if get_waste_disagg_weights() is None:
        raise RuntimeError(
            'Cornerstone matrix export requires waste disaggregation weights '
            '(implement_waste_disaggregation and readable weight CSVs).'
        )


def export_electricity_disagg_cornerstone_csvs(
    output_dir: pathlib.Path | None = None,
) -> pathlib.Path:
    """Clear publish caches, assert preconditions, materialize, write six CSVs.

    Import ``clear_publish_caches`` from ``publish`` inside this function to avoid
    extract→publish import-time coupling.
    """
    from bedrock.publish.excel.writer import clear_publish_caches  # noqa: PLC0415

    clear_publish_caches()
    assert_cornerstone_matrix_export_preconditions()
    frames = materialize_electricity_disagg_cornerstone_frames()
    out = (
        _DISAGG_ROOT / 'electricity_disagg_inputs'
        if output_dir is None
        else pathlib.Path(output_dir)
    )
    out.mkdir(parents=True, exist_ok=True)

    frames['V'].to_csv(out / 'cornerstone_V.csv', index=True)
    frames['Udom'].to_csv(out / 'cornerstone_Udom.csv', index=True)
    frames['Uimp'].to_csv(out / 'cornerstone_Uimp.csv', index=True)
    frames['VA'].to_csv(out / 'cornerstone_VA.csv', index=True)
    frames['Y'].to_csv(out / 'cornerstone_Ytot_full_cs.csv', index=True)
    frames['E'].to_csv(out / 'cornerstone_E.csv', index=True)
    return out


def export_cornerstone_matrices_to_csv(
    output_dir: pathlib.Path | None = None,
) -> pathlib.Path:
    """Write Make, Use, VA, full Y, and E to CSV under *output_dir* (default: electricity_disagg_inputs).

    Returns the directory written.
    """
    return export_electricity_disagg_cornerstone_csvs(output_dir)


if __name__ == '__main__':
    # Offline use: set USA_CONFIG_FILE or call set_global_usa_config first.
    from bedrock.utils.config.usa_config import set_global_usa_config

    set_global_usa_config(CORNERSTONE_FULL_MODEL_EXPORT_YAML)
    dest = export_cornerstone_matrices_to_csv()
    print(f'Wrote cornerstone matrix CSVs to {dest.resolve()}')
