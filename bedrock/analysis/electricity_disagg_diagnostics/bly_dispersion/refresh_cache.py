"""Prefetch all manifest diagnostics tabs into the shared parquet cache."""

from __future__ import annotations

import click

from bedrock.analysis.electricity_disagg_diagnostics.manifest import load_manifest
from bedrock.utils.validation.analysis.bly_plots import TAB_BLY
from bedrock.utils.validation.analysis.fetch import load_tab


def refresh_all(*, refresh: bool = True) -> None:
    manifest = load_manifest()
    sheet_ids = {manifest.footing.sheet_id}
    sheet_ids.update(step.sheet_id for step in manifest.steps)
    sheet_ids.add(manifest.final.sheet_id)
    for sid in sorted(sheet_ids):
        load_tab(sid, TAB_BLY, refresh=refresh)
        load_tab(sid, 'config_summary', refresh=refresh)
        print(f'  cached {sid}')


@click.command()
@click.option(
    '--refresh/--no-refresh',
    default=True,
    help='Force network refresh (default: true).',
)
def main(refresh: bool) -> None:
    print('=== Refresh electricity disagg diagnostics cache ===')
    refresh_all(refresh=refresh)
    print('Done.')


if __name__ == '__main__':
    main()
