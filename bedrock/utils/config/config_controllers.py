"""Utilities for switching the process-wide USA config in analysis scripts
that run the same pipeline under multiple configurations sequentially.

``set_global_usa_config`` raises if called twice in the same process.  These
helpers bypass that guard by writing directly to the module globals, enabling
patterns like::

    for config_name in ("useeio_phoebe_23", "2025_usa_cornerstone_full_model"):
        reset_usa_config()
        set_usa_config(config_name)
        results[config_name] = run_pipeline()
"""

from __future__ import annotations

import importlib
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

import yaml

import bedrock.utils.config.usa_config as _cfg_module
from bedrock.utils.config.usa_config import USAConfig


def force_set_usa_config(config_name: str, **field_overrides: Any) -> None:
    """Load a named YAML config and install it as the process-wide USA config.

    Bypasses the 'already set' guard in ``set_global_usa_config``; call
    ``reset_usa_config()`` first when swapping configs mid-script.

    ``field_overrides`` are merged onto the YAML data before constructing the
    config object via ``model_construct`` (skipping Literal validation), which
    is necessary when overriding schema-constrained fields such as
    ``model_base_year``.
    """
    yaml_path = Path(_cfg_module.CONFIG_DIR) / (
        config_name if config_name.endswith(".yaml") else config_name + ".yaml"
    )
    with open(yaml_path) as f:
        data = yaml.safe_load(f) or {}
    data.update(field_overrides)
    _cfg_module._usa_config = USAConfig.model_construct(**data)
    os.environ[_cfg_module.USA_CONFIG_ENV_VAR] = yaml_path.name


def clear_caches(*module_paths: str) -> None:
    """Clear all ``@functools.cache`` entries in the named modules."""
    for path in module_paths:
        module = importlib.import_module(path)
        for name in dir(module):
            obj = getattr(module, name, None)
            if callable(obj) and hasattr(obj, "cache_clear"):
                obj.cache_clear()


@contextmanager
def temp_usa_config(
    config_name: str,
    cache_bearing_modules: tuple[str, ...] = (),
    **field_overrides: Any,
) -> Generator[None, None, None]:
    """Context manager that installs a config, yields, then resets.

    Clears caches in ``cache_bearing_modules`` before setting the config and
    again after restoring, so config-dependent ``@functools.cache`` results
    from the previous iteration are not reused.

    Example::

        with temp_usa_config("useeio_phoebe_23", cache_bearing_modules=(...,)):
            ratio = _ratio_from_margins(derive_2017_margins_cornerstone_usa())
    """
    _cfg_module.reset_usa_config()
    clear_caches(*cache_bearing_modules)
    force_set_usa_config(config_name, **field_overrides)
    try:
        yield
    finally:
        _cfg_module.reset_usa_config()
        clear_caches(*cache_bearing_modules)
