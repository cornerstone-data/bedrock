"""Build one or all five-slide comparison PPTX files."""

from __future__ import annotations

from pathlib import Path

import click

from bedrock.analysis.electricity.current.diagnostics.deck.pairs import (
    IMPLEMENTATIONS,
    PAIRS,
    Pair,
)
from bedrock.analysis.electricity.current.diagnostics.deck.paths import (
    DECK_OUT_DIR,
    ensure_deck_dirs,
)
from bedrock.analysis.electricity.current.diagnostics.deck.pptx_write import write_pptx
from bedrock.analysis.electricity.current.diagnostics.deck.sources import (
    load_impl_bundle,
)


def build_pair(
    pair: Pair,
    *,
    derive: bool = False,
    out_dir: Path | None = None,
) -> Path:
    ensure_deck_dirs()
    dest = out_dir or DECK_OUT_DIR
    dest.mkdir(parents=True, exist_ok=True)
    vs_footing = pair.hist_mode == 'vs_footing'
    top = load_impl_bundle(
        IMPLEMENTATIONS[pair.top],
        derive=derive,
        load_snapshot_footing=vs_footing,
    )
    bottom = load_impl_bundle(
        IMPLEMENTATIONS[pair.bottom],
        derive=derive,
        load_snapshot_footing=vs_footing,
    )
    png_dir = dest / 'png'
    return write_pptx(pair, top, bottom, dest / pair.filename, png_dir=png_dir)


@click.command()
@click.option(
    '--pair',
    'pair_key',
    type=click.Choice(sorted(PAIRS)),
    default=None,
    help='Build one pair. Default with --all is every pair.',
)
@click.option('--all', 'build_all', is_flag=True, help='Build all four PPTX files.')
@click.option(
    '--derive',
    is_flag=True,
    help='Live-derive missing current and production steps (slow).',
)
def main(pair_key: str | None, build_all: bool, derive: bool) -> None:
    if build_all:
        keys = list(PAIRS)
    elif pair_key is None:
        raise click.UsageError('pass --pair <name> or --all')
    else:
        keys = [pair_key]
    written: list[Path] = []
    for key in keys:
        path = build_pair(PAIRS[key], derive=derive)
        written.append(path)
        click.echo(f'Wrote {path}')


if __name__ == '__main__':
    main()
