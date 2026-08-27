"""Resolve D/N/q/class-MWh for one implementation from freeze, cache, or derive."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from bedrock.analysis.electricity.current.diagnostics.deck.data import (
    ImplBundle,
    StepSnapshot,
    class_mwh_from_parquet,
    fill_mixed_c_col,
    series_from_parquet,
)
from bedrock.analysis.electricity.current.diagnostics.deck.pairs import (
    CONFIG_FOR_STEP,
    STEPS,
    Implementation,
    StepId,
)
from bedrock.analysis.electricity.current.diagnostics.deck.paths import impl_cache_dir
from bedrock.analysis.electricity.historical.original_elec_disagg_implementation import (
    paths as original_paths,
)
from bedrock.analysis.electricity.historical.pre_mecs_industrial_weights import (
    paths as pre_mecs_paths,
)

logger = logging.getLogger(__name__)


def _read_c_col(folder: Path) -> float | None:
    meta = folder / 'run_metadata.json'
    if not meta.is_file():
        return None
    payload = json.loads(meta.read_text(encoding='utf-8'))
    raw = payload.get('c_col')
    if raw is None:
        return None
    return float(raw)


def _load_folder(folder: Path, *, mixed: bool) -> StepSnapshot | None:
    if not folder.is_dir():
        return None
    d_path = folder / 'D.parquet'
    n_path = folder / 'N.parquet'
    q_path = folder / 'q.parquet'
    x_path = folder / 'x.parquet'
    class_path = folder / 'class_generation_mwh.parquet'
    if not n_path.is_file() and not d_path.is_file():
        return None
    class_mwh = class_mwh_target = None
    if class_path.is_file():
        class_mwh, class_mwh_target = class_mwh_from_parquet(class_path)
    return StepSnapshot(
        d=series_from_parquet(d_path) if d_path.is_file() else None,
        n=series_from_parquet(n_path) if n_path.is_file() else None,
        q=series_from_parquet(q_path, 'q') if q_path.is_file() else None,
        x=series_from_parquet(x_path, 'x') if x_path.is_file() else None,
        mixed=mixed,
        c_col=_read_c_col(folder),
        class_mwh=class_mwh,
        class_mwh_target=class_mwh_target,
    )


def _candidate_dirs(impl: Implementation, step_id: StepId) -> list[Path]:
    config = CONFIG_FOR_STEP[step_id]
    dirs = [impl_cache_dir(impl.id, config)]
    if impl.id == 'original':
        dirs.append(original_paths.config_dir(config))
    elif impl.id == 'eia_gtd':
        dirs.append(pre_mecs_paths.config_dir(config))
    return dirs


def load_step(impl: Implementation, step_id: StepId) -> StepSnapshot | None:
    mixed = step_id == 'mixed_units'
    for folder in _candidate_dirs(impl, step_id):
        snap = _load_folder(folder, mixed=mixed)
        if snap is not None:
            return snap
    return None


def load_footing_from_snapshot(impl: Implementation) -> StepSnapshot:
    """D/N from the release snapshot used as that pipeline's EF footing."""
    from bedrock.utils.math.formulas import (  # noqa: PLC0415
        compute_d,
        compute_L_matrix,
        compute_M_matrix,
        compute_n,
    )
    from bedrock.utils.snapshots.loader import load_snapshot  # noqa: PLC0415

    cache = impl_cache_dir('footing_snapshot', impl.snapshot_key)
    cached = _load_folder(cache, mixed=False)
    if cached is not None and cached.d is not None and cached.n is not None:
        return cached

    b = load_snapshot('B_USA_non_finetuned', impl.snapshot_key)
    adom = load_snapshot('Adom_USA', impl.snapshot_key)
    aimp = load_snapshot('Aimp_USA', impl.snapshot_key)
    d = compute_d(B=b)
    n = compute_n(M=compute_M_matrix(B=b, L=compute_L_matrix(A=adom + aimp)))
    cache.mkdir(parents=True, exist_ok=True)
    d.rename('D').to_frame().to_parquet(cache / 'D.parquet')
    n.rename('N').to_frame().to_parquet(cache / 'N.parquet')
    return StepSnapshot(d=d.astype(float), n=n.astype(float), mixed=False)


def dollar_weights_patch() -> object:
    import bedrock.transform.eeio.electricity_gtd_allocation as gtd  # noqa: PLC0415

    orig = gtd.allocate_purchaser_gtd

    def _wrapped(*args: object, **kwargs: object) -> object:
        kwargs = dict(kwargs)
        kwargs['industrial_weights'] = 'dollars'
        return orig(*args, **kwargs)

    return patch.object(gtd, 'allocate_purchaser_gtd', _wrapped)


def _write_class_mwh(folder: Path) -> None:
    from bedrock.analysis.electricity.current.eia_gtd.purchaser_tables import (  # noqa: PLC0415
        allocated_class_mwh,
        class_mwh_targets,
    )
    from bedrock.transform.eeio.electricity_gtd_allocation import (  # noqa: PLC0415
        get_reanchored_eia_purchaser_allocation,
    )
    from bedrock.utils.config.usa_config import get_usa_config  # noqa: PLC0415

    alloc = get_reanchored_eia_purchaser_allocation()
    if alloc is None:
        return
    eia_year = int(get_usa_config().model_base_year)
    model = allocated_class_mwh(alloc)
    targets = class_mwh_targets(alloc, eia_year)
    rows = []
    for cls in model.index:
        rows.append(
            {
                'class': str(cls),
                'value': float(model.loc[cls]),
                'unit': 'MWh',
                'target': float(targets.get(str(cls), float('nan'))),
            }
        )
    pd.DataFrame(rows).to_parquet(folder / 'class_generation_mwh.parquet')


def derive_step(impl: Implementation, step_id: StepId) -> StepSnapshot:
    """Live-derive one config and write the deck cache."""
    from bedrock.publish.cache_reset import clear_all_publish_caches  # noqa: PLC0415
    from bedrock.transform.eeio.cornerstone_disagg_pipeline import (  # noqa: PLC0415
        electricity_conversion_factors,
        electricity_mixed_units_enabled,
    )
    from bedrock.transform.eeio.derived_cornerstone import (  # noqa: PLC0415
        derive_cornerstone_Aq_mixed_units,
        derive_cornerstone_Aq_scaled,
    )
    from bedrock.utils.config.usa_config import (  # noqa: PLC0415
        reset_usa_config,
        set_global_usa_config,
    )
    from bedrock.utils.validation.diagnostics_helpers import (  # noqa: PLC0415
        pull_efs_for_diagnostics,
    )

    config = CONFIG_FOR_STEP[step_id]
    reset_usa_config()
    clear_all_publish_caches()
    set_global_usa_config(config)
    patch_cm = dollar_weights_patch() if impl.industrial_weights == 'dollars' else None
    if patch_cm is not None:
        patch_cm.start()
    try:
        if electricity_mixed_units_enabled():
            aq_mon = derive_cornerstone_Aq_scaled()
            c_col, _c_row = electricity_conversion_factors(aq_mon)
            aq = derive_cornerstone_Aq_mixed_units()
        else:
            aq = derive_cornerstone_Aq_scaled()
            c_col = None
        efs = pull_efs_for_diagnostics()
    finally:
        if patch_cm is not None:
            patch_cm.stop()

    folder = impl_cache_dir(impl.id, config)
    folder.mkdir(parents=True, exist_ok=True)
    efs.D_new.to_parquet(folder / 'D.parquet')
    efs.N_new.to_parquet(folder / 'N.parquet')
    aq.scaled_q.astype(float).rename('q').to_frame().to_parquet(folder / 'q.parquet')
    metadata = {
        'config': config,
        'impl': impl.id,
        'c_col': c_col,
        'mixed_units': bool(electricity_mixed_units_enabled()),
    }
    (folder / 'run_metadata.json').write_text(
        json.dumps(metadata, indent=2), encoding='utf-8'
    )
    if step_id in ('three_way', 'mixed_units') and impl.id != 'original':
        _write_class_mwh(folder)
    mixed = step_id == 'mixed_units'
    loaded = _load_folder(folder, mixed=mixed)
    if loaded is None:
        raise RuntimeError(f'failed to reload derived snapshot at {folder}')
    return loaded


def load_impl_bundle(
    impl: Implementation,
    *,
    derive: bool = False,
    load_snapshot_footing: bool = False,
) -> ImplBundle:
    steps: dict[StepId, StepSnapshot] = {}
    for step_id in STEPS:
        snap = load_step(impl, step_id)
        if snap is None and step_id == 'footing' and load_snapshot_footing:
            try:
                snap = load_footing_from_snapshot(impl)
            except Exception as exc:
                logger.warning(
                    'Skipping %s footing snapshot %s: %s',
                    impl.id,
                    impl.snapshot_key,
                    exc,
                )
        if snap is None and derive and impl.id != 'original':
            snap = derive_step(impl, step_id)
        if snap is not None:
            steps[step_id] = snap
    bundle = ImplBundle(impl_id=impl.id, steps=steps)
    fill_mixed_c_col(bundle)
    return bundle
