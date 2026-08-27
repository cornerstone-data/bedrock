"""``python -m bedrock.analysis.electricity.current.vs_pre_mecs_industrial_weights``

Live mixed-units ``q`` / ``N`` vs the pre-MECS freeze. Not a CI gate.
"""

from __future__ import annotations

import logging

import pandas as pd

from bedrock.analysis.electricity.current.diagnostics.paths import OUT_DIR
from bedrock.analysis.electricity.current.vs_pre_mecs_industrial_weights.compare_to_pre_mecs import (
    MIXED_CONFIG,
    compare_n_to_freeze,
    compare_q_to_freeze,
)
from bedrock.publish.cache_reset import clear_all_publish_caches
from bedrock.transform.eeio.derived_cornerstone import derive_cornerstone_Aq_mixed_units
from bedrock.utils.config.usa_config import reset_usa_config, set_global_usa_config
from bedrock.utils.validation.diagnostics_helpers import pull_efs_for_diagnostics

REPORT_MD = OUT_DIR / 'vs_pre_mecs_industrial_weights.md'


def _markdown_table(frame: pd.DataFrame) -> str:
    cols = [str(c) for c in frame.columns]
    header = '| ' + ' | '.join(['index', *cols]) + ' |'
    sep = '| ' + ' | '.join('---' for _ in range(len(cols) + 1)) + ' |'
    lines = [header, sep]
    for idx, row in frame.iterrows():
        cells = [str(idx)]
        for c in frame.columns:
            val = row[c]
            if isinstance(val, float):
                cells.append(f'{val:.6g}')
            else:
                cells.append(str(val))
        lines.append('| ' + ' | '.join(cells) + ' |')
    return '\n'.join(lines)


def _top_abs(frame: pd.DataFrame, n: int = 25) -> pd.DataFrame:
    return (
        frame.assign(_abs=frame['abs_diff'].abs())
        .sort_values('_abs', ascending=False)
        .drop(columns='_abs')
        .head(n)
    )


def main() -> None:
    reset_usa_config()
    clear_all_publish_caches()
    set_global_usa_config(MIXED_CONFIG)
    aq = derive_cornerstone_Aq_mixed_units()
    efs = pull_efs_for_diagnostics()
    q_cmp = compare_q_to_freeze(aq.scaled_q.astype(float))
    n_cmp = compare_n_to_freeze(efs.N_new)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    q_nonzero = q_cmp['abs_diff'].abs() > 1e-9
    n_nonzero = n_cmp['abs_diff'].abs() > 1e-9
    lines = [
        '# Live mixed units vs pre-MECS freeze',
        '',
        f'Config: `{MIXED_CONFIG}`. Not a CI gate. Class totals should hold; '
        'manufacturing ``q`` / ``N`` should move under Table 7.7 weights.',
        '',
        f'``q`` rows with abs_diff > 0: **{int(q_nonzero.sum())}** / {len(q_cmp)}.',
        f'``N`` cells with abs_diff > 0: **{int(n_nonzero.sum())}** / {len(n_cmp)}.',
        '',
        '## Largest ``q`` abs diffs',
        '',
        _markdown_table(_top_abs(q_cmp)),
        '',
        '## Largest ``N`` abs diffs',
        '',
        _markdown_table(_top_abs(n_cmp)),
        '',
    ]
    md = '\n'.join(lines)
    REPORT_MD.write_text(md, encoding='utf-8')
    print(md)
    print(f'Wrote {REPORT_MD}')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    main()
