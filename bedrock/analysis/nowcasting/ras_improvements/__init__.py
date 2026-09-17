"""#839 RAS hygiene analysis: census, vintage diffs, and reports.

Production hooks live in
:mod:`bedrock.transform.iot.nowcast_sut_assembly`
(``sweep_offset_residue``, ``assert_post_balance_hygiene``, save sidecar).
This package is the repeatable analysis and documentation for those items.
"""

from __future__ import annotations

from bedrock.analysis.nowcasting.ras_improvements.common import (
    OUTPUT_DIR,
    illicit_mask,
    resolve_artifact_dir,
)

__all__ = [
    'OUTPUT_DIR',
    'illicit_mask',
    'resolve_artifact_dir',
]
