"""Orchestrate electricity disagg BLy dispersion diagnostics."""

from __future__ import annotations

import click

from bedrock.analysis.electricity_disagg_diagnostics.bly import sector_bly_new
from bedrock.analysis.electricity_disagg_diagnostics.dispersion import (
    ChainedDispersion,
    compute_chained_dispersion,
)
from bedrock.analysis.electricity_disagg_diagnostics.manifest import (
    load_manifest,
    validate_manifest,
)
from bedrock.analysis.electricity_disagg_diagnostics.paths import ensure_dirs
from bedrock.analysis.electricity_disagg_diagnostics.waterfall import write_waterfall_pngs


def run_pipeline(*, refresh: bool, write_plots: bool) -> ChainedDispersion:
    ensure_dirs()
    manifest = load_manifest()
    validate_manifest(manifest, refresh=refresh)

    footing = sector_bly_new(manifest.footing.sheet_id, refresh=refresh)
    step_series = [
        sector_bly_new(step.sheet_id, refresh=refresh) for step in manifest.steps
    ]
    step_labels = [step.label for step in manifest.steps]
    result = compute_chained_dispersion(footing, step_series, step_labels)

    _print_summary(result)
    if write_plots:
        mmt_path, pct_path = write_waterfall_pngs(result)
        print(f'  wrote {mmt_path.name}')
        print(f'  wrote {pct_path.name}')
    return result


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
    print(f'  combined FINAL: {result.combined_mmt:,.4f} MMT ({result.combined_pct:.3f}%)')
    print(f'  offset: {result.offset_mmt:,.4f} MMT ({result.offset_pct:.3f}%)')
    print(
        f'  sum(steps)={sum(result.step_values_mmt):,.4f}  '
        f'combined={result.combined_mmt:,.4f}'
    )


@click.command()
@click.option('--refresh', is_flag=True, help='Re-fetch diagnostics tabs from Sheets.')
def main(refresh: bool) -> None:
    from bedrock.utils.validation.analysis.plotting import setup_mpl

    setup_mpl(font_size=13)
    run_pipeline(refresh=refresh, write_plots=True)


if __name__ == '__main__':
    main()
