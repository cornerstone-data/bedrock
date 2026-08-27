"""``python -m bedrock.analysis.electricity.current.eia_gtd`` → D0 tables."""

from __future__ import annotations

import argparse
import logging

from bedrock.analysis.electricity.current.diagnostics.paths import OUT_DIR
from bedrock.analysis.electricity.current.eia_gtd.purchaser_tables import (
    MIXED_CONFIG,
    SPLIT_CONFIG,
    build_live_report,
)

REPORT_MD = OUT_DIR / 'eia_gtd_purchaser_tables.md'


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description='Write D0 class-MWh / leftover T&D tables from the reanchored allocation.'
    )
    parser.add_argument(
        '--config',
        default=MIXED_CONFIG,
        choices=(MIXED_CONFIG, SPLIT_CONFIG),
        help='3-way or mixed-units YAML (default: mixed units).',
    )
    args = parser.parse_args(argv)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    md = build_live_report(args.config)
    REPORT_MD.write_text(md, encoding='utf-8')
    print(md)
    print(f'Wrote {REPORT_MD}')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    main()
