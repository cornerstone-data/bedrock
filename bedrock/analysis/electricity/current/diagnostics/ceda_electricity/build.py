"""Build one or both CEDA electricity diagnostics PPTX decks."""

from __future__ import annotations

from pathlib import Path

import click

from bedrock.analysis.electricity.current.diagnostics.ceda_electricity.paths import (
    ensure_ceda_dirs,
)
from bedrock.analysis.electricity.current.diagnostics.ceda_electricity.pptx_write import (
    write_deck_a,
    write_deck_b,
)

DECKS = {
    'reagg_vs_baseline': write_deck_a,
    'g1_g5_ladder': write_deck_b,
}


@click.command()
@click.option(
    '--deck',
    'deck_key',
    type=click.Choice(sorted(DECKS)),
    default=None,
    help='Build one deck.',
)
@click.option('--all', 'build_all', is_flag=True, help='Build both PPTX decks.')
@click.option(
    '--refresh',
    is_flag=True,
    help='Re-fetch Google Sheet tabs (ignore local parquet cache).',
)
def main(deck_key: str | None, build_all: bool, refresh: bool) -> None:
    if build_all:
        keys = list(DECKS)
    elif deck_key is None:
        raise click.UsageError('pass --deck <name> or --all')
    else:
        keys = [deck_key]
    ensure_ceda_dirs()
    for key in keys:
        path: Path = DECKS[key](refresh=refresh)
        click.echo(f'Wrote {path}')


if __name__ == '__main__':
    main()
