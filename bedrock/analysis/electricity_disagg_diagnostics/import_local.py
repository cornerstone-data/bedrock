"""Seed the diagnostics parquet cache from downloaded Excel workbooks."""

from __future__ import annotations

from pathlib import Path

import click

from bedrock.analysis.electricity_disagg_diagnostics.local_data import (
    seed_cache_from_local_dir,
)
from bedrock.analysis.electricity_disagg_diagnostics.manifest import load_manifest
from bedrock.analysis.electricity_disagg_diagnostics.paths import LOCAL_DATA_DIR


@click.command()
@click.option(
    '--local-dir',
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=LOCAL_DATA_DIR,
    show_default=True,
    help='Directory containing downloaded diagnostics .xlsx files.',
)
def main(local_dir: Path) -> None:
    """Import local workbooks named ``{config_name}.xlsx`` into the cache."""
    print('=== Import local electricity disagg diagnostics ===')
    manifest = load_manifest()
    seed_cache_from_local_dir(manifest, local_dir)
    print('Done. Run:')
    print('  uv run python -m bedrock.analysis.electricity_disagg_diagnostics.run_all')


if __name__ == '__main__':
    main()
