"""CLI for methods #85 resolution analysis reports."""

from __future__ import annotations

import argparse
import logging

from bedrock.analysis.electricity.disaggregation_matrices import (
    assert_disaggregation_export_config,
)
from bedrock.utils.config.usa_config import set_global_usa_config

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            'Run methods discussion #85 analysis reports (Decisions 3, 5, 7). '
            'Requires implement_waste/reallocation/disaggregation flags. '
            'Baseline path may write weight CSVs under extract/disaggregation/'
            'electricity_disagg_inputs/.'
        )
    )
    parser.add_argument(
        '--config_name',
        default='2025_usa_cornerstone_full_model_electricity_disaggregation.yaml',
        help='USA config YAML filename under bedrock/utils/config/configs/',
    )
    parser.add_argument(
        '--decision',
        choices=('3', '5', '7', 'all'),
        default='all',
        help='Which decision report(s) to generate',
    )
    parser.add_argument(
        '--figures',
        action='store_true',
        help='Also write Decisions 3/5 figures A, C, D to d_85/output/',
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    set_global_usa_config(args.config_name)
    assert_disaggregation_export_config()

    paths: list[str] = []
    if args.decision in ('3', 'all'):
        from bedrock.analysis.electricity.d_85.decision3_table83_analysis import (  # noqa: PLC0415
            build_report as build_d3_report,
        )

        paths.append(str(build_d3_report()))
    if args.decision in ('5', 'all'):
        from bedrock.analysis.electricity.d_85.decision5_table24_analysis import (  # noqa: PLC0415
            build_report as build_d5_report,
        )

        paths.append(str(build_d5_report()))
    if args.decision in ('7', 'all'):
        from bedrock.analysis.electricity.d_85.decision7_ugo305_scaling_analysis import (  # noqa: PLC0415
            build_report as build_d7_report,
        )

        paths.append(str(build_d7_report()))

    if args.figures and args.decision in ('3', '5', 'all'):
        from bedrock.analysis.electricity.d_85.disagg_scenarios import (  # noqa: PLC0415
            run_decision3_scenarios,
            run_decision5_scenarios,
        )
        from bedrock.analysis.electricity.d_85.figures import (  # noqa: PLC0415
            build_decision_figures,
        )

        d3 = run_decision3_scenarios() if args.decision in ('3', 'all') else {}
        d5 = run_decision5_scenarios() if args.decision in ('5', 'all') else {}
        if args.decision == '3':
            d5 = {'baseline': d3['baseline']}
        elif args.decision == '5':
            d3 = {'baseline': d5['baseline']}
        figure_paths = build_decision_figures(d3, d5)
        paths.extend(str(p) for p in figure_paths.values())

    for path in paths:
        logger.info('Wrote report: %s', path)


if __name__ == '__main__':
    main()
