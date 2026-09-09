"""Excel dumps of the stored nowcast products, for reading and sharing.

Reads the parquets Step 5 and Step 7 wrote to ``transform/output_data`` and
writes them to ``analysis/nowcasting/output`` as workbooks. Three files, because
they are three different shapes and two different units:

``Balanced_SUT_<year>_<vintage>.xlsx``
    The Step 5 pair, one sheet each, commodity x industry-and-final-demand.
    ⚠️ **BEA million dollars**, unlike everything else here.

``Nowcast_MUT_after_redef_<year>_<vintage>.xlsx``
    Three of the Step 7 quartet - Make, Use at producer prices, and the import
    matrix. All matrices on the same axes, all USD.

``Nowcast_Margins_after_redef_<year>_<vintage>.xlsx``
    The fourth. Kept separate because it is not a matrix at all: one row per
    (buyer, commodity) transaction, 51,682 of them at 2024, against 29 columns.
    Putting it beside the matrices would invite reading it as one.

The filename carries the vintage of the parquets inside it, and every workbook
opens on a ``Provenance`` sheet naming the source parquet, the branch and
commit that wrote it, and the units - so a workbook that has been mailed on
is still traceable to the build that produced it.

⚠️ **Vintage is resolved by modification time, per artifact family.** The
balanced SUTs and the after-redefinitions MUTs are written by different steps
and carry different git hashes even within one rebuild, so there is no single
hash to ask for. ``--check`` reports what would be selected, and how far apart
the two families' timestamps are; a gap of hours rather than minutes means the
MUTs were not built from the SUTs beside them.

Run::

    uv run python -m bedrock.analysis.nowcasting.export_tables
    uv run python -m bedrock.analysis.nowcasting.export_tables --year 2023
    uv run python -m bedrock.analysis.nowcasting.export_tables --check
"""

from __future__ import annotations

import argparse
import json
import typing as ta
from datetime import datetime
from pathlib import Path

import pandas as pd

#: Where the build writes its parquets.
SOURCE_DIR = Path(__file__).resolve().parents[2] / 'transform' / 'output_data'

#: Where these workbooks go. Untracked working artifacts, like everything else
#: under ``output/``.
OUTPUT_DIR = Path(__file__).parent / 'output'

#: ``(sheet name, filename stem)`` per workbook, and the workbook's unit label.
#: Stems are the artifact names without the ``_v<version>_<hash>`` suffix.
WORKBOOKS: dict[str, tuple[str, tuple[tuple[str, str], ...]]] = {
    'Balanced_SUT': (
        'BEA million USD',
        (
            ('Supply', 'Balanced_Detail_Supply_{year}'),
            ('Use', 'Balanced_Detail_Use_SUT_{year}'),
        ),
    ),
    'Nowcast_MUT_after_redef': (
        'USD',
        (
            ('Make', 'Nowcast_Detail_Make_after_redef_{year}'),
            ('Use', 'Nowcast_Detail_Use_after_redef_{year}'),
            ('Import', 'Nowcast_Detail_Import_after_redef_{year}'),
        ),
    ),
    'Nowcast_Margins_after_redef': (
        'USD',
        (('Margins', 'Nowcast_Detail_Margins_after_redef_{year}'),),
    ),
}


def resolve(stem: str) -> Path:
    """The most recently written parquet for an artifact stem.

    Several vintages of the same artifact sit side by side once a year has been
    rebuilt, and the filename carries the git hash rather than a date, so the
    only ordering available is the filesystem's.
    """
    candidates = sorted(
        SOURCE_DIR.glob(f'{stem}_v*.parquet'), key=lambda p: p.stat().st_mtime
    )
    if not candidates:
        raise FileNotFoundError(
            f'no parquet for {stem!r} in {SOURCE_DIR}. Build the year first, or '
            f'download the artifacts from GCS.'
        )
    return candidates[-1]


def vintage_of(path: Path, stem: str) -> str:
    """The ``v<version>_<hash>`` suffix a stored parquet carries."""
    return path.stem[len(stem) + 1 :]


def provenance(path: Path, units: str) -> dict[str, str]:
    """What the sidecar says about a parquet, flattened for a sheet."""
    record = {
        'source_file': path.name,
        'units': units,
        'modified': datetime.fromtimestamp(path.stat().st_mtime).strftime(
            '%Y-%m-%d %H:%M:%S'
        ),
    }
    sidecar = path.with_name(f'{path.stem}_metadata.json')
    if not sidecar.exists():
        record['metadata'] = 'no sidecar found'
        return record
    meta = json.loads(sidecar.read_text())
    tool_meta = meta.get('tool_meta', {})
    record.update(
        {
            'created': str(meta.get('date_created', '')),
            'tool_version': str(meta.get('tool_version', '')),
            'git_hash': str(meta.get('git_hash', '')),
            'step': str(tool_meta.get('step', '')),
            'branch': str(tool_meta.get('branch', '')),
            'commit': str(tool_meta.get('commit', '')),
        }
    )
    for key in ('protocol', 'engine_result', 'import_control'):
        if key in tool_meta:
            record[key] = str(tool_meta[key])
    return record


def write_workbook(name: str, year: int, out_dir: Path) -> Path:
    """One workbook: a provenance sheet, then a sheet per stored table."""
    units, sheets = WORKBOOKS[name]
    records, frames, vintages = [], [], []
    for sheet, stem in sheets:
        resolved = stem.format(year=year)
        source = resolve(resolved)
        frame = pd.read_parquet(source)
        frames.append((sheet, frame))
        vintages.append(vintage_of(source, resolved))
        records.append(
            {'sheet': sheet, 'rows': frame.shape[0], 'columns': frame.shape[1]}
            | provenance(source, units)
        )

    # The workbook wears the vintage its sheets came from, so a file that has
    # been mailed on can still be traced back to the build. Sheets within one
    # workbook are written by a single step and normally agree; if a partial
    # rebuild has left them disagreeing, the name says so rather than picking.
    vintage = '+'.join(dict.fromkeys(vintages))
    path = out_dir / f'{name}_{year}_{vintage}.xlsx'

    with pd.ExcelWriter(path, engine='openpyxl') as writer:
        pd.DataFrame(records).set_index('sheet').T.to_excel(
            writer, sheet_name='Provenance'
        )
        for sheet, frame in frames:
            frame.to_excel(writer, sheet_name=sheet)
    return path


def check(year: int) -> None:
    """Report what would be selected, and whether the families agree in time."""
    stamps: dict[str, list[float]] = {}
    for name, (units, sheets) in WORKBOOKS.items():
        print(f'{name}  [{units}]')
        for sheet, stem in sheets:
            source = resolve(stem.format(year=year))
            mtime = source.stat().st_mtime
            stamps.setdefault(name, []).append(mtime)
            when = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M')
            frame = pd.read_parquet(source)
            print(f'  {sheet:8s} {when}  {str(frame.shape):14s} {source.name}')
    spread = max(max(v) for v in stamps.values()) - min(min(v) for v in stamps.values())
    print(f'\nwidest gap between selected artifacts: {spread / 60:,.0f} minutes')
    if spread > 6 * 3600:
        print(
            '⚠️  more than six hours apart - check these came from one rebuild '
            'before trusting the pair'
        )


def main(argv: ta.Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--year', type=int, default=2024)
    parser.add_argument('--out-dir', type=Path, default=OUTPUT_DIR)
    parser.add_argument(
        '--check',
        action='store_true',
        help='report the artifacts that would be read, and write nothing',
    )
    args = parser.parse_args(argv)

    if args.check:
        check(args.year)
        return 0

    args.out_dir.mkdir(parents=True, exist_ok=True)
    for name in WORKBOOKS:
        path = write_workbook(name, args.year, args.out_dir)
        size = path.stat().st_size / 1024**2
        print(f'wrote {path}  ({size:,.1f} MB)')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
