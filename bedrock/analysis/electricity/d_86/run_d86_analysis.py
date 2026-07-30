"""CLI entry point for Methods #86 toy analysis."""

from __future__ import annotations

from bedrock.analysis.electricity.d_86.write_report import write_report


def main() -> None:
    path = write_report()
    print(f'Report written to {path}')


if __name__ == '__main__':
    main()
