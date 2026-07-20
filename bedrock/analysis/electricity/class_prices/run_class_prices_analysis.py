"""CLI for class-priced vs uniform mixed-unit EF comparison (Track B)."""

from __future__ import annotations

import argparse
import pathlib

from bedrock.analysis.electricity.class_prices.compare_paths import (
    compare_class_vs_uniform_mixed_efs,
)
from bedrock.utils.config.usa_config import set_global_usa_config


def _default_output_dir() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parent / 'output'


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Compare class-priced vs uniform mixed-unit electricity EFs'
    )
    parser.add_argument(
        '--config_name',
        default='2025_usa_cornerstone_v0_2_electricity_mixed_units.yaml',
        help='USA config YAML (must have mixed-units gate on)',
    )
    parser.add_argument(
        '--output_dir',
        type=pathlib.Path,
        default=_default_output_dir(),
        help='Directory for markdown + CSV output',
    )
    args = parser.parse_args()
    set_global_usa_config(args.config_name)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    result = compare_class_vs_uniform_mixed_efs()
    csv_path = args.output_dir / 'class_vs_uniform_summary.csv'
    result.summary.to_csv(csv_path, index=False)
    md_path = args.output_dir / 'class_vs_uniform_report.md'
    lines = [
        '# Class-priced vs uniform mixed-unit EF comparison',
        '',
        f'Config: `{args.config_name}`',
        '',
        f'c_col (MWh/$): {result.class_result.c_col:.6g}',
        '',
        '## Summary',
        '',
        result.summary.to_markdown(index=False),
        '',
    ]
    md_path.write_text('\n'.join(lines), encoding='utf-8')
    print(f'Wrote {csv_path}')
    print(f'Wrote {md_path}')


if __name__ == '__main__':
    main()
