"""Orchestrate electricity disagg BLy dispersion diagnostics."""

from __future__ import annotations

from pathlib import Path

import click

from bedrock.analysis.electricity_disagg_diagnostics.bly import sector_bly_new
from bedrock.analysis.electricity_disagg_diagnostics.dispersion import (
    ChainedDispersion,
    compute_chained_dispersion,
)
from bedrock.analysis.electricity_disagg_diagnostics.local_data import (
    seed_cache_from_local_dir,
)
from bedrock.analysis.electricity_disagg_diagnostics.manifest import (
    load_manifest,
    validate_manifest,
)
from bedrock.analysis.electricity_disagg_diagnostics.net_change import (
    ChainedNetChange,
    compute_chained_net_change,
)
from bedrock.analysis.electricity_disagg_diagnostics.paths import (
    LOCAL_DATA_DIR,
    ensure_dirs,
)
from bedrock.analysis.electricity_disagg_diagnostics.waterfall import (
    write_net_change_waterfall_pngs,
    write_waterfall_pngs,
)
from bedrock.utils.validation.analysis.plotting import setup_mpl


def run_pipeline(
    *,
    refresh: bool,
    write_plots: bool,
    local_dir: Path | None = None,
) -> tuple[ChainedDispersion, ChainedNetChange]:
    ensure_dirs()
    manifest = load_manifest()
    use_cache_only = local_dir is not None
    if local_dir is not None:
        print(f'=== Import local workbooks from {local_dir} ===')
        seed_cache_from_local_dir(manifest, local_dir)
    validate_manifest(manifest, refresh=refresh and not use_cache_only)

    footing = sector_bly_new(
        manifest.footing.sheet_id, refresh=refresh and not use_cache_only
    )
    step_series = [
        sector_bly_new(step.sheet_id, refresh=refresh and not use_cache_only)
        for step in manifest.steps
    ]
    step_labels = [step.label for step in manifest.steps]
    result = compute_chained_dispersion(footing, step_series, step_labels)
    net_result = compute_chained_net_change(
        footing,
        step_series,
        step_labels,
        footing_label=manifest.footing.label,
    )

    _print_summary(result)
    _print_net_summary(net_result)
    if write_plots:
        mmt_path, pct_path = write_waterfall_pngs(result)
        print(f'  wrote {mmt_path.name}')
        print(f'  wrote {pct_path.name}')
        net_mmt_path, net_pct_path = write_net_change_waterfall_pngs(net_result)
        print(f'  wrote {net_mmt_path.name}')
        print(f'  wrote {net_pct_path.name}')
    return result, net_result


def _print_summary(result: ChainedDispersion) -> None:
    print('=== Electricity disagg BLy dispersion (chained) ===')
    print(f'  footing total BLy: {result.footing_total_mmt:,.3f} MMT CO2e')
    for label, mmt, pct in zip(
        result.step_labels,
        result.step_values_mmt,
        result.step_values_pct,
        strict=True,
    ):
        print(f'  {label.replace(chr(10), " ")}: {mmt:,.4f} MMT ({pct:.3f}%)')
    print(
        f'  combined FINAL: {result.combined_mmt:,.4f} MMT ({result.combined_pct:.3f}%)'
    )
    print(f'  offset: {result.offset_mmt:,.4f} MMT ({result.offset_pct:.3f}%)')
    print(
        f'  sum(steps)={sum(result.step_values_mmt):,.4f}  '
        f'combined={result.combined_mmt:,.4f}'
    )


def _print_net_summary(result: ChainedNetChange) -> None:
    print('=== Electricity disagg BLy net change (chained) ===')
    print(f'  footing total BLy: {result.stage_totals_mmt[0]:,.3f} MMT CO2e')
    for label, total, delta in zip(
        result.step_labels,
        result.stage_totals_mmt[1:],
        result.step_deltas_mmt,
        strict=True,
    ):
        short = ' '.join(label.split())
        print(f'  after {short}: {total:,.3f} MMT (step net {delta:+,.4f} MMT)')
    print(
        f'  FINAL net change: {result.combined_delta_mmt:+,.4f} MMT '
        f'({result.combined_delta_pct:+.3f}%)'
    )
    delta_bars = [b for b in result.build_bars() if b.kind == 'delta']
    print(f'  delta bars on chart: {len(delta_bars)}')


@click.command()
@click.option('--refresh', is_flag=True, help='Re-fetch diagnostics tabs from Sheets.')
@click.option(
    '--local-dir',
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=None,
    help=(
        'Use downloaded .xlsx workbooks from this directory (no Google API). '
        f'Default layout: {LOCAL_DATA_DIR.name}/{{config_name}}.xlsx'
    ),
)
def main(refresh: bool, local_dir: Path | None) -> None:
    setup_mpl(font_size=13)
    run_pipeline(refresh=refresh, write_plots=True, local_dir=local_dir)


if __name__ == '__main__':
    main()
