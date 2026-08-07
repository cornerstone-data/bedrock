"""Legacy re-exports; prefer ``toy_paths`` for the three-section analysis."""

from bedrock.analysis.electricity.d_86.toy_paths import (
    Section1ProductionResult,
    Section2FlowMixedResult,
    Section3DirectMixedResult,
    assert_section2_matches_section3,
    run_section1_production,
    run_section2_flow_mixed,
    run_section3_direct_mixed,
)

__all__ = [
    'Section1ProductionResult',
    'Section2FlowMixedResult',
    'Section3DirectMixedResult',
    'assert_section2_matches_section3',
    'run_section1_production',
    'run_section2_flow_mixed',
    'run_section3_direct_mixed',
]
