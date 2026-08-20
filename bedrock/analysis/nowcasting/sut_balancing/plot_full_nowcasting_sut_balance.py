"""Walkthrough figures for ``test_full_nowcasting_sut_balance``.

Imports the toy from the test (same seeds, masks, targets). Does not run
pytest. Writes a multi-page PDF plus PNGs under ``step5/output/`` (gitignored).
The PDF is picture-then-glossary: after each snapshot, a page defines every
term on that picture and describes what happened, for a non-technical reader.

From the repo root::

    uv run python bedrock/analysis/nowcasting/step5/plot_full_nowcasting_sut_balance.py
"""

from __future__ import annotations

import textwrap
import warnings
from pathlib import Path

import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.colors import ListedColormap
from matplotlib.figure import Figure

from bedrock.transform.iot.__tests__.test_full_nowcasting_sut_balance import (
    _full_hard_set,
    _full_masks,
    _full_supply,
    _full_use,
)
from bedrock.transform.iot.__tests__.test_nowcast_sut_gras import INDUSTRIES, _t2
from bedrock.transform.iot.nowcast_sut_gras import engine
from bedrock.utils.economic.balance import (
    Aggregator,
    SutMask,
    Target,
    TargetSet,
    TargetTerm,
    gras_balance,
    offset_targets,
    restore_fixed_blocks,
    split_fixed_blocks,
)

OUTPUT_DIR = Path(__file__).resolve().parent / 'output'
MASK_CMAP = ListedColormap(['#f4f4f4', '#2a6f97'])
SIGN_CMAP = ListedColormap(['#b2182b', '#f4f4f4', '#2166ac'])

# Filename → (pipeline stage, what this page shows). Also captions.txt + PDF index.
PAGES: dict[str, tuple[str, str]] = {
    '00_seed_X': (
        'Setup — seed',
        'Starting toy SUT (X) before split or GRAS. Use is 6x3 (commodities, VA, '
        'industries i1/4200ID, one FD column F01000). Supply is 2x7 (same commodities, '
        'bridge/tax columns). TRADE has a trailing space (BEA).',
    ),
    '01_masks_use': (
        'Setup — SutMask',
        'Use mask layers. structural_zero = cells that must stay 0 (seed sparsity). '
        'This toy has no fixed_value holds. free = GRAS may move these. sign_lock: T00SUB '
        'stays negative; V00100/V00300 are unlocked so T4 can compensate in-column.',
    ),
    '02_masks_supply': (
        'Setup — SutMask',
        'Supply mask layers, same rules. 4200ID is all structural zeros. SUB is '
        'sign-locked nonpositive. The kernel only scales free cells; empty-free margins '
        'with a nonzero target still raise.',
    ),
    '03_frozen_F': (
        'Offset — split X = F+Z',
        'F after split_fixed: nonzero cells the mask holds exactly. This toy has no '
        'fixed_value cells, so F is all zeros. restore_fixed adds F back after the engine.',
    ),
    '04_free_Z': (
        'Offset — split X = F+Z',
        'Z = X - F, the matrix gras_balance actually scales. With F empty, Z matches the '
        'seed on free cells and is 0 on structural zeros. Engine never sees F.',
    ),
    '05_kernel_vectors': (
        'GRAS kernel',
        'Stage 1 of the test: ndarray gras_balance on Use only. row_t and col_t are '
        'current Z sums (hold). No T1/T11 overwrite yet. GRAS is asked to hit margins '
        'it already has.',
    ),
    '06_stage1_use_after_gras': (
        'GRAS kernel',
        'Stage 1 result: Use after gras_balance to those hold vectors. Supply is still '
        'the unscaled seed. ~free stays 0. The matrix should barely move because targets '
        'were already the current sums.',
    ),
    '07_targets_on_Z': (
        'SUT orchestration (hard)',
        'Residual TargetSet vs entry Z, before engine (stage 2 set; F=0 so residual = '
        'published values). Hard T1/T11-T17 already match. Soft T2 wants F01000=8; Z '
        'sums to 6. Softness is not imposed yet.',
    ),
    '08_stage2_Z': (
        'SUT orchestration (hard)',
        'Stage 2: engine(impose_soft=False). Use then Supply, hard T1 and T11-T17 only. '
        'T2 is skipped, so FD stays at hold. This is balanced Z; F is still out.',
    ),
    '09_stage2_restored': (
        'SUT orchestration (hard)',
        'Stage 2 restored X = F + Z. With F=0 this equals Z. Identities are checked on '
        'this table after restore.',
    ),
    '10_stage2_minus_seed': (
        'SUT orchestration (hard)',
        'Cellwise restored - seed. Nonzeros are where the hard Use-then-Supply protocol '
        'moved mass. F01000 should not move (T2 skipped).',
    ),
    '11_targets_stage2': (
        'SUT orchestration (hard)',
        'Targets on stage 2 restored X (pre-offset values). Hard residuals should be ~0. '
        'T2 is still off (evaluate F01000=6 vs target 8).',
    ),
    '12_stage3_Z': (
        'KRAS-style soft',
        'Stage 3: engine(impose_soft=True) on a fresh split, with T2 and T4. T2 blends FD '
        'toward 8 once from entry Z. T4 scales V00100 then puts -d on sign-flex cells in '
        'the same industry column. Title lists skipped / soft_deferred.',
    ),
    '13_stage3_restored': (
        'KRAS-style soft',
        'Stage 3 restored X = F + Z. Compare to page 09 (hard-only restored).',
    ),
    '14_stage3_minus_stage2': (
        'KRAS-style soft',
        'Softness only: stage 3 restored - stage 2 restored. Use F01000 is T2; V00100 vs '
        'V00300 is the T4 column-neutral closer (T1 column sums stay put). Supply moves '
        'only as T11/T12-T14 follow Use.',
    ),
    '15_targets_stage3': (
        'KRAS-style soft',
        'Targets on stage 3 restored X. Hard identities still hold. T2 is closer to 8 '
        'than in stage 2 (weight 0.5, not all the way). T4 group residual is smaller than '
        'the entry gap.',
    ),
}


EXPLAIN: dict[str, dict[str, object]] = {
    '00_seed_X': {
        'terms': [
            (
                'Supply and Use tables (SUT)',
                'Two spreadsheets that describe the same economy from two sides. '
                'The Use table says who bought each product. The Supply table says '
                'who made or imported it, plus trade margins and taxes.',
            ),
            (
                'Cell',
                'One number where a row meets a column. In this walkthrough the units '
                'are just "dollars" on a tiny made-up economy. Red is positive, blue '
                'is negative, white is zero.',
            ),
            (
                'Commodity (c1, c2)',
                'A product. These are the rows of both pictures.',
            ),
            (
                'Industry (i1, 4200ID)',
                'A producing sector. These are columns of Use, and some columns of Supply. '
                '4200ID is customs in the real US tables; here it is just a second industry.',
            ),
            (
                'Final demand (F01000)',
                'Purchases that are not one industry buying from another — for example '
                'households. This toy has a single fake final-demand column.',
            ),
            (
                'Value added (V00100, V00300, T00TOP, T00SUB)',
                'The "income" rows of Use: wages (V00100), operating surplus (V00300), '
                'product taxes (T00TOP), and subsidies (T00SUB, stored negative).',
            ),
            (
                'Supply bridge columns (TRADE, TRANS, SUB, TOP, MDTY)',
                'Extra Supply columns that turn factory prices into shop prices: trade '
                'and transport margins, subsidies, product taxes, and customs duties. '
                'The TRADE label has a trailing space because that is how the statistical '
                'agency spells it.',
            ),
        ],
        'what': (
            'This is the starting picture, before anyone has balanced anything. The test '
            'uses a tiny fake economy so every cell is readable. A real US table has '
            'hundreds of products and industries. Nothing has been adjusted yet; these '
            'are just the numbers we start from.'
        ),
    },
    '01_masks_use': {
        'terms': [
            (
                'Mask',
                'A set of yes/no rules laid on top of the spreadsheet. It does not change '
                'the dollars by itself. It says which cells the balancer is allowed to touch.',
            ),
            (
                'Structural zero',
                'A cell that is empty on purpose and must stay empty. Example: this industry '
                'does not use that product. The pattern of zeros is treated as a fact about '
                'the economy, not a number to fill in.',
            ),
            (
                'Fixed value',
                'A cell a source measured exactly, so we must not change it. This toy has '
                'none, so the panel is blank.',
            ),
            (
                'Free',
                'A cell the balancer may change. It is everything that is not a structural '
                'zero or a fixed value.',
            ),
            (
                'Sign lock',
                'The cell may get larger or smaller, but it must not flip from plus to minus '
                '(or minus to plus). Subsidies (T00SUB) stay negative. Wages and surplus '
                '(V00100, V00300) are unlocked so a later step can use them as a cushion '
                'inside an industry column.',
            ),
        ],
        'what': (
            'Before we rescale the table, we freeze its shape. Zeros stay zeros. Most other '
            'Use cells may move. Think of this as taping over the squares that must not be '
            'erased, then handing the rest to the balancer.'
        ),
    },
    '02_masks_supply': {
        'terms': [
            (
                'Supply table',
                'The other spreadsheet: for each product, who produced it and which margin '
                'or tax columns apply.',
            ),
            (
                'Empty-free margin',
                'A whole row or column with nothing the balancer is allowed to move, but a '
                'target that is not zero. That is a contradiction, and the program stops. '
                'It is not allowed to invent a new producer just to hit a total.',
            ),
        ],
        'what': (
            'Same rules as Use, now on Supply. The 4200ID column is all structural zeros '
            'in this toy (nobody produces anything there). The subsidy column stays '
            'non-positive. Only teal "free" cells can change in later pictures.'
        ),
    },
    '03_frozen_F': {
        'terms': [
            (
                'Split',
                'We cut the starting table X into two layers that add back up to X: a '
                'held layer F and a movable layer Z. After balancing Z we glue F back on.',
            ),
            (
                'F (frozen / held)',
                'The dollars we refuse to change, copied out of X. In a real run these are '
                'the "fixed value" cells. This toy has none, so F is all zeros — a blank '
                'sheet. That is expected, not a bug.',
            ),
            (
                'X',
                'The full table you would publish: X = F + Z.',
            ),
        ],
        'what': (
            'This page is the "do not touch" pile. Because the toy never marked a cell as '
            'measured-and-fixed, the pile is empty. Later pages still show the split so '
            'you can see the same recipe that a real year would use when some cells really '
            'are locked.'
        ),
    },
    '04_free_Z': {
        'terms': [
            (
                'Z (free / movable)',
                'What is left after peeling F off X. The balancer only ever sees Z. '
                'Structural zeros are already 0 here.',
            ),
            (
                'GRAS',
                'The scaling method used later: it stretches rows and columns of Z so they '
                'add up to chosen totals, while staying as close as possible to this picture '
                'and without filling in taped-over zeros.',
            ),
        ],
        'what': (
            'With F empty, Z looks like the seed on every free cell. This is the clay. '
            'Everything after this either scales Z (GRAS) or, at the end, adds F back to '
            'make a finished table.'
        ),
    },
    '05_kernel_vectors': {
        'terms': [
            (
                'GRAS kernel',
                'The engine for one spreadsheet at a time. It does not know about Supply '
                'versus Use, taxes, or "hard versus soft." It only knows: here is a grid, '
                'here are the row totals I want, here are the column totals I want, here '
                'are the cells I may change.',
            ),
            (
                'row_t / col_t',
                'The target totals for each row and each column. "Hold" means we copied '
                'the totals Z already has, so we are asking GRAS to hit numbers it already '
                'meets.',
            ),
            (
                'Stage 1 of the test',
                'A rehearsal that calls GRAS on Use alone, with no economic identities yet. '
                'It checks that the scaler works on this toy before the two tables are '
                'tied together.',
            ),
        ],
        'what': (
            'We have not yet said "total supply must equal total use" or "industry output '
            'must match." We only hand GRAS the Use grid and tell it to keep the current '
            'row and column sums. That is a gentle first step, like asking a mixer to run '
            'on a batter that is already the right thickness.'
        ),
    },
    '06_stage1_use_after_gras': {
        'terms': [
            (
                'Left panel (Use)',
                'The Use table after the GRAS rehearsal. Compare it with the seed Use '
                'on page 00: it should look almost the same.',
            ),
            (
                'Right panel (Supply)',
                'Still the original Supply. Stage 1 never runs GRAS on Supply, so this '
                'side is a control picture.',
            ),
            (
                'White / empty cells',
                'Still zero. The mask forbade filling them in, and GRAS obeyed.',
            ),
        ],
        'what': (
            'Because the requested totals were already the current sums, the picture '
            'should barely move. Empty cells stay empty. This page is mainly proof that '
            'the scaler ran and respected the mask. The interesting movement comes in '
            'the next layers, when the two tables have to agree with each other.'
        ),
    },
    '07_targets_on_Z': {
        'terms': [
            (
                'Target',
                'A budget line the tables should satisfy: a named total (or a small set of '
                'totals) from an account or an identity. Each row of this table is one target.',
            ),
            (
                'Hard versus soft',
                'Hard means "this must hold, like an accounting identity." Soft means "this '
                'is an estimate from another source; we will move toward it but not ignore '
                'the identities." T1 and T11–T17 are hard. T2 is soft.',
            ),
            (
                'T1',
                'Each industry\'s gross output: the Use column for that industry must add '
                'to this number.',
            ),
            (
                'T11',
                'For each product, total Supply minus total Use should be zero (the two '
                'sides of the economy agree).',
            ),
            (
                'T12–T17',
                'More identities: subsidies, product taxes, customs, trade and transport '
                'margin columns that sum to zero, and the basic-to-shop-price wedge.',
            ),
            (
                'T2',
                'A sourced guess at the final-demand column total. Here it wants F01000 '
                'to sum to 8, but the current table sums to 6.',
            ),
            (
                'evaluate versus values',
                '"values" is what we asked for. "evaluate" is what the current tables '
                'actually add up to. The gap is what a later step would try to close.',
            ),
            (
                'Weight',
                'For a soft target, how much we trust that source this year, from 0 '
                '(ignore it) to 1 (treat it like hard). T2 is 0.5 in this toy.',
            ),
        ],
        'what': (
            'This is the scoreboard before the two-table balancer runs. Every hard line '
            'already matches (gap 0) because this toy was built that way. The only '
            'disagreement is T2: final demand is 6 and the "survey" says 8. Stage 2 will '
            'leave that disagreement in place on purpose. Stage 3 will move partway toward 8.'
        ),
    },
    '08_stage2_Z': {
        'terms': [
            (
                'SUT orchestration',
                'The conductor around GRAS. It builds the row and column totals for Use, '
                'runs GRAS, then does the same for Supply, then repeats until the product '
                'identity (T11) is close enough. It knows the named targets T1 and T11–T17.',
            ),
            (
                'impose_soft=False',
                'Do not apply the sourced guesses (T2, T4, …). Skip them. Identities still run.',
            ),
            (
                'skipped / soft_deferred',
                'Soft targets that were not written into the totals. Skipped = turned off. '
                'Deferred = a hard identity already owns that slot, so the soft line stands down.',
            ),
        ],
        'what': (
            'This is Z after the hard-only conductor. Use and Supply have been scaled so '
            'the identities hold. Final demand was not pulled toward 8, because T2 was '
            'skipped. Compare this to the seed: some cells moved so the two tables agree; '
            'the empty cells are still empty.'
        ),
    },
    '09_stage2_restored': {
        'terms': [
            (
                'Restore',
                'Put the held layer F back: published table = F + balanced Z. Checks and '
                'reports always use this glued-together table, not Z alone.',
            ),
            (
                'X = F + Z',
                'The arithmetic of publishing. F is the locked dollars; Z is what the '
                'balancer was allowed to change. Add them cell by cell.',
            ),
            (
                'This toy',
                'F is all zeros, so restored X looks identical to the previous Z picture. '
                'The restore step is still shown so the recipe matches a real year.',
            ),
        ],
        'what': (
            'In this toy F is zero, so restored X looks like Z. On a real year the glued '
            'table would show the locked measured cells sitting back in their original '
            'places, with the balancer\'s changes only in the free cells.'
        ),
    },
    '10_stage2_minus_seed': {
        'terms': [
            (
                'Difference picture',
                'Each cell is "after minus before." White 0 means that cell did not move. '
                'Red means the cell got larger; blue means it got smaller.',
            ),
            (
                'F01000 column',
                'Final demand. It should stay white here because we skipped the sourced '
                'T2 guess. Any color in this column would mean identities leaked into a '
                'slot we meant to leave alone.',
            ),
            (
                'Seed',
                'The starting tables from page 00. Subtracting them from the restored '
                'hard-only tables shows only the identity-driven edits.',
            ),
        ],
        'what': (
            'This is the "who moved?" view for identities only. Final demand (F01000) '
            'should stay put because we skipped T2. Movement elsewhere is the price of '
            'making Supply and Use agree on products, taxes, and margins. If a cell is '
            'white, the hard rules did not need it.'
        ),
    },
    '11_targets_stage2': {
        'terms': [
            (
                'Scoreboard after restore',
                'Same target list as before, now measured on the finished hard-only tables.',
            ),
            (
                'max |eval − values|',
                'The size of the remaining gap. About zero means that target is met. A '
                'positive number means the tables still disagree with that line.',
            ),
            (
                'T2 still off',
                'evaluate is still 6, values is still 8. Softness has not run, so this '
                'gap is intentional.',
            ),
        ],
        'what': (
            'Hard lines should still show a gap of about zero — the identities held. T2 '
            'is still 6 versus 8. That is success for this stage: we proved we can balance '
            'the books without quietly "fixing" a sourced total we have not asked to impose yet.'
        ),
    },
    '12_stage3_Z': {
        'terms': [
            (
                'KRAS-style softness',
                'Not a new scaler. Same GRAS, same conductor, but sourced totals are now '
                'allowed to tug on leftover slots. "Who gives way" is the point: identities '
                'stay exact; estimates move partway.',
            ),
            (
                'Blend once from the start',
                'For T2 we mix the starting total with the sourced total using the weight, '
                'once, then keep that mix for every later GRAS pass. We do not creep closer '
                'each round, or a slow-to-finish year would treat the same weight as almost hard.',
            ),
            (
                'T4 closer',
                'Wages (V00100) are a group total, not a single row GRAS can hit directly. '
                'After each Use pass we scale free wage cells toward the mix, then take the '
                'same dollars back out of other unlocked cells in the same industry column '
                'so that industry\'s output (T1) does not drift.',
            ),
        ],
        'what': (
            'We start over from the original seed (a fresh split), then run the conductor '
            'with softness on. T2 pulls final demand from 6 toward 8, stopping halfway at '
            'weight 0.5. T4 nudges wages and compensates inside the column. Identities are '
            'still in charge. The title\'s skipped/deferred lists should be empty here '
            'because T2 and T4 were imposed.'
        ),
    },
    '13_stage3_restored': {
        'terms': [
            (
                'Restored after softness',
                'Again X = F + Z, now with T2 and T4 applied. Compare with the hard-only '
                'restored picture (page 09).',
            ),
            (
                'F01000 on Use',
                'The final-demand column. It should now add to something between 6 and 8 '
                '(halfway, given weight 0.5), not the original 6.',
            ),
            (
                'V00100 / V00300',
                'Wages and surplus in the Use table. T4 may have shifted dollars between '
                'them inside an industry without changing that industry\'s column total.',
            ),
        ],
        'what': (
            'This is the table you would publish if this toy were a real year and you '
            'trusted those sourced totals at the stated weights. Identities still hold; '
            'final demand and wages have moved relative to the hard-only run. The next '
            'page subtracts the two restored tables so those moves are easier to see.'
        ),
    },
    '14_stage3_minus_stage2': {
        'terms': [
            (
                'Softness-only difference',
                'Stage 3 minus stage 2. Everything that is not white is caused by T2/T4, '
                'not by the identities (those were already satisfied in stage 2).',
            ),
            (
                'Column-neutral',
                'T4 may move wages up in an industry only if it moves other free cells in '
                'that same industry down by the same amount, so the industry column still '
                'adds to T1.',
            ),
        ],
        'what': (
            'Look at Use F01000: that is T2 sharing the extra final-demand dollars across '
            'products. Look at V00100 versus V00300 in industry i1: that is T4 raising '
            'wages and taking the same money out of surplus so the column still adds up. '
            'Supply only moves because product and tax identities have to follow Use. '
            'The 4200ID column staying at zero means we did not invent production there.'
        ),
    },
    '15_targets_stage3': {
        'terms': [
            (
                'Final scoreboard',
                'Every named total, measured on the finished tables after both identities '
                'and sourced guesses have run.',
            ),
            (
                'Hard lines (T1, T11–T17)',
                'Should still show a gap of about zero. Softness is not allowed to break '
                'the books.',
            ),
            (
                'T2 after the blend',
                'Should be closer to 8 than the stage-2 scoreboard, but not all the way, '
                'because the weight is 0.5 — we only took half of the survey\'s advice.',
            ),
            (
                'T4 group residual',
                'The remaining gap on the wage group. Smaller than at the start means the '
                'closer did some work; not necessarily zero, because T4 is soft.',
            ),
        ],
        'what': (
            'This is the whole story in one table: the books still balance (hard lines), '
            'and the sourced guesses were allowed to influence leftover cells without '
            'overwriting those books. Calibration of the weights, and swapping in real '
            'survey totals for these placeholders, is later work — not this picture.'
        ),
    },
}


def _annotate(fig: Figure, name: str) -> Figure:
    """Prefix the pipeline stage and put the caption under the figure."""
    stage, caption = PAGES[name]
    if fig._suptitle is not None:
        current = fig._suptitle.get_text()
    elif fig.axes:
        current = fig.axes[0].get_title()
        fig.axes[0].set_title('')
    else:
        current = name
    fig.suptitle(f'[{stage}]  {current}', fontsize=11)
    fig.supxlabel(caption, fontsize=8, ha='left', x=0.02)
    return fig


def _index_figure() -> Figure:
    fig, ax = plt.subplots(figsize=(11.5, 9.5), layout='constrained')
    ax.axis('off')
    ax.set_title(
        'test_full_nowcasting_sut_balance walkthrough',
        fontsize=14,
        pad=12,
    )
    lines = [
        'How to read this PDF',
        'Each snapshot is a picture, then a following page that defines every term',
        'on that picture and says what just happened, in plain language.',
        'Stage names the Step 5 layer (not the test number): setup/mask, offset split,',
        'GRAS kernel, SUT orchestration (hard identities), KRAS-style soft (sourced totals).',
        'Colors: red = positive dollars, blue = negative, white = zero. Teal on mask',
        'pages = yes. This is a tiny made-up economy so every cell is readable.',
        '',
        'Picture list',
        '',
    ]
    for name, (stage, caption) in PAGES.items():
        lines.append(f'{name}.png')
        lines.append(f'    Stage: {stage}')
        lines.append(textwrap.fill(caption, width=108, initial_indent='    ', subsequent_indent='    '))
        lines.append('')
    ax.text(
        0.02,
        0.98,
        '\n'.join(lines),
        va='top',
        ha='left',
        fontsize=7.5,
        family='sans-serif',
    )
    return fig


def _explain_figure(name: str) -> Figure:
    """Plain-language terms + narrative for the snapshot that precedes this page."""
    stage, _short = PAGES[name]
    spec = EXPLAIN[name]
    fig, ax = plt.subplots(figsize=(11.5, 9.5), layout='constrained')
    ax.axis('off')
    ax.set_title(
        f'[{stage}]  After the picture  ({name})',
        fontsize=13,
        pad=10,
        loc='left',
    )
    blocks = [
        'Terms used on the previous page',
        'Each name below appears on (or is implied by) the picture you just saw.',
        '',
    ]
    for term, definition in spec['terms']:  # type: ignore[misc]
        blocks.append(f'•  {term}')
        blocks.append(
            textwrap.fill(str(definition), width=100, initial_indent='   ', subsequent_indent='   ')
        )
        blocks.append('')
    blocks.append('What is happening')
    blocks.append('')
    blocks.append(textwrap.fill(str(spec['what']), width=100))
    ax.text(
        0.03,
        0.97,
        '\n'.join(blocks),
        va='top',
        ha='left',
        fontsize=9.2,
        family='sans-serif',
        linespacing=1.38,
    )
    return fig


def _tick(label: object) -> str:
    text = str(label)
    return repr(text) if text != text.strip() else text


def _heatmap(
    ax: plt.Axes,
    frame: pd.DataFrame,
    title: str,
    *,
    vmin: float,
    vmax: float,
    cmap: str | ListedColormap = 'RdBu_r',
    integer: bool = False,
) -> None:
    values = frame.to_numpy(dtype=float)
    ax.imshow(
        values,
        cmap=cmap,
        vmin=vmin,
        vmax=vmax,
        aspect='auto',
        interpolation='nearest',
    )
    ax.set_xticks(
        range(len(frame.columns)),
        [_tick(c) for c in frame.columns],
        rotation=45,
        ha='right',
    )
    ax.set_yticks(range(len(frame.index)), [_tick(r) for r in frame.index])
    ax.set_title(title, fontsize=10)
    ax.tick_params(labelsize=8)
    for i in range(values.shape[0]):
        for j in range(values.shape[1]):
            val = float(values[i, j])
            if integer:
                text = '' if val == 0.0 else f'{int(val):d}'
            elif abs(val) < 5e-13:
                text = '0'
            else:
                text = f'{val:.3g}'
            ax.text(j, i, text, ha='center', va='center', fontsize=7)


def _dollar_limit(frames: list[pd.DataFrame]) -> float:
    peak = max(float(np.nanmax(np.abs(f.to_numpy(dtype=float)))) for f in frames)
    return max(peak, 1.0)


def _panel_figure(
    use: pd.DataFrame,
    supply: pd.DataFrame,
    title: str,
    *,
    cmap: str = 'RdBu_r',
) -> Figure:
    vmax = _dollar_limit([use, supply])
    fig, axes = plt.subplots(1, 2, figsize=(11.5, 4.6), layout='constrained')
    fig.suptitle(title, fontsize=12)
    _heatmap(axes[0], use, 'Use', vmin=-vmax, vmax=vmax, cmap=cmap)
    _heatmap(axes[1], supply, 'Supply', vmin=-vmax, vmax=vmax, cmap=cmap)
    return fig


def _mask_figure(mask: SutMask, block: str) -> Figure:
    fig, axes = plt.subplots(2, 2, figsize=(10.5, 6.5), layout='constrained')
    fig.suptitle(f'{block} mask layers (1 = True / lock sign)', fontsize=12)
    layers = (
        (axes[0, 0], mask.structural_zero.astype(float), 'structural_zero', MASK_CMAP, 0.0, 1.0),
        (axes[0, 1], mask.fixed_value.astype(float), 'fixed_value', MASK_CMAP, 0.0, 1.0),
        (axes[1, 0], mask.free.astype(float), 'free', MASK_CMAP, 0.0, 1.0),
        (axes[1, 1], mask.sign_lock.astype(float), 'sign_lock', SIGN_CMAP, -1.0, 1.0),
    )
    for ax, frame, title, cmap, vmin, vmax in layers:
        _heatmap(
            ax,
            frame,
            title,
            vmin=vmin,
            vmax=vmax,
            cmap=cmap,
            integer=True,
        )
    return fig


def _vector_figure(row_t: pd.Series, col_t: pd.Series, title: str) -> Figure:
    vmax = _dollar_limit([row_t.to_frame(), col_t.to_frame().T])
    fig, axes = plt.subplots(1, 2, figsize=(11.5, 2.8), layout='constrained')
    fig.suptitle(title, fontsize=12)
    _heatmap(
        axes[0],
        row_t.to_frame('row_t'),
        'row_t (Use)',
        vmin=-vmax,
        vmax=vmax,
    )
    _heatmap(
        axes[1],
        col_t.to_frame('col_t').T,
        'col_t (Use)',
        vmin=-vmax,
        vmax=vmax,
    )
    return fig


def _target_rows(targets: TargetSet, blocks: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for target in targets:
        evaluated = target.evaluate(blocks)
        residual = evaluated - target.values.astype(float)
        rows.append(
            {
                'name': target.name,
                'hard': target.hard,
                'weight': target.weight,
                'max|eval-values|': float(residual.abs().max()),
                'values': ', '.join(f'{i}={v:.3g}' for i, v in target.values.items()),
                'evaluate': ', '.join(f'{i}={v:.3g}' for i, v in evaluated.items()),
            }
        )
    return pd.DataFrame(rows)


def _table_figure(frame: pd.DataFrame, title: str) -> Figure:
    fig, ax = plt.subplots(figsize=(11.5, 0.55 * (len(frame) + 4)), layout='constrained')
    ax.axis('off')
    ax.set_title(title, fontsize=12, pad=8)
    table = ax.table(
        cellText=frame.astype(str).values,
        colLabels=list(frame.columns),
        loc='center',
        cellLoc='left',
    )
    table.auto_set_font_size(False)
    table.set_fontsize(7)
    return fig


def _stage3_targets(hard_plus_t2: TargetSet, use: pd.DataFrame) -> TargetSet:
    aggregator = Aggregator.from_mapping({'g': list(INDUSTRIES)}, list(use.columns))
    t4 = Target(
        terms=(TargetTerm('use', 'column', 1.0, aggregator, restrict_to=('V00100',)),),
        values=pd.Series({'g': 6.0}),
        source='test',
        name='T4',
        hard=False,
        weight=0.6,
    )
    return TargetSet.of(*hard_plus_t2.targets, t4)


def _run() -> list[tuple[str, Figure]]:
    original_use = _full_use()
    original_supply = _full_supply()
    masks = _full_masks(original_use, original_supply)
    hard = _full_hard_set(original_use, original_supply)
    t2 = _t2(pd.Series({'F01000': 8.0}), weight=0.5)
    hard_plus_t2 = TargetSet.of(*hard.targets, t2)
    stage3_set = _stage3_targets(hard_plus_t2, original_use)

    seeds = {'use': original_use.copy(), 'supply': original_supply.copy()}
    frozen, free = split_fixed_blocks(seeds, masks)
    use_z = free['use']
    row_t = use_z.sum(axis=1).astype(float)
    col_t = use_z.sum(axis=0).astype(float)
    kernel = gras_balance(
        matrix=use_z.to_numpy(dtype=np.float64),
        row_targets=row_t.to_numpy(dtype=np.float64),
        col_targets=col_t.to_numpy(dtype=np.float64),
        free_mask=masks['use'].free.to_numpy(),
        sign_flex=masks['use'].sign_lock.to_numpy() == 0,
        project_infeasible=False,
        close_rows_exactly=False,
    )
    stage1_use = pd.DataFrame(kernel.matrix, index=use_z.index, columns=use_z.columns)

    frozen2, free2 = split_fixed_blocks(
        {'use': original_use.copy(), 'supply': original_supply.copy()}, masks
    )
    residual2 = offset_targets(hard_plus_t2, frozen2)
    out2 = engine(
        free2,
        residual2,
        masks,
        impose_soft=False,
        close_rows_on_last=False,
        atol=1e-6,
    )
    restored2 = restore_fixed_blocks(out2.blocks, frozen2)

    frozen3, free3 = split_fixed_blocks(
        {'use': original_use.copy(), 'supply': original_supply.copy()}, masks
    )
    residual3 = offset_targets(stage3_set, frozen3)
    out3 = engine(
        free3,
        residual3,
        masks,
        impose_soft=True,
        close_rows_on_last=False,
        atol=1e-6,
    )
    restored3 = restore_fixed_blocks(out3.blocks, frozen3)

    notes2 = f'skipped={out2.skipped}  soft_deferred={out2.soft_deferred}'
    notes3 = f'skipped={out3.skipped}  soft_deferred={out3.soft_deferred}'

    raw = [
        (
            '00_seed_X',
            _panel_figure(original_use, original_supply, '0. Seed X (published toy)'),
        ),
        ('01_masks_use', _mask_figure(masks['use'], 'Use')),
        ('02_masks_supply', _mask_figure(masks['supply'], 'Supply')),
        (
            '03_frozen_F',
            _panel_figure(
                frozen['use'],
                frozen['supply'],
                '1. Frozen F after split_fixed (nonzero holds)',
            ),
        ),
        (
            '04_free_Z',
            _panel_figure(
                free['use'],
                free['supply'],
                '1. Free Z after split_fixed (engine seed)',
            ),
        ),
        (
            '05_kernel_vectors',
            _vector_figure(
                row_t,
                col_t,
                '2. Stage 1 kernel vectors (hold = current Use Z sums)',
            ),
        ),
        (
            '06_stage1_use_after_gras',
            _panel_figure(
                stage1_use,
                free['supply'],
                '2. Stage 1: Use after gras_balance (Supply unchanged)',
            ),
        ),
        (
            '07_targets_on_Z',
            _table_figure(
                _target_rows(residual2, free2),
                '3. Residual targets vs entry Z (stage 2 set, before engine)',
            ),
        ),
        (
            '08_stage2_Z',
            _panel_figure(
                out2.blocks['use'],
                out2.blocks['supply'],
                f'4. Stage 2 Z  impose_soft=False  {notes2}',
            ),
        ),
        (
            '09_stage2_restored',
            _panel_figure(
                restored2['use'],
                restored2['supply'],
                '4. Stage 2 restored X = F + Z',
            ),
        ),
        (
            '10_stage2_minus_seed',
            _panel_figure(
                restored2['use'] - original_use,
                restored2['supply'] - original_supply,
                '4. Stage 2 restored - seed X (who the hard protocol moved)',
            ),
        ),
        (
            '11_targets_stage2',
            _table_figure(
                _target_rows(hard_plus_t2, restored2),
                '4. Targets on stage 2 restored X (pre-offset values)',
            ),
        ),
        (
            '12_stage3_Z',
            _panel_figure(
                out3.blocks['use'],
                out3.blocks['supply'],
                f'5. Stage 3 Z  impose_soft=True  {notes3}',
            ),
        ),
        (
            '13_stage3_restored',
            _panel_figure(
                restored3['use'],
                restored3['supply'],
                '5. Stage 3 restored X = F + Z',
            ),
        ),
        (
            '14_stage3_minus_stage2',
            _panel_figure(
                restored3['use'] - restored2['use'],
                restored3['supply'] - restored2['supply'],
                '5. Stage 3 restored - stage 2 restored (softness: T2 FD / T4 V00100)',
            ),
        ),
        (
            '15_targets_stage3',
            _table_figure(
                _target_rows(stage3_set, restored3),
                '5. Targets on stage 3 restored X (pre-offset values)',
            ),
        ),
    ]
    return [(name, _annotate(fig, name)) for name, fig in raw]


def _write_captions() -> Path:
    path = OUTPUT_DIR / 'captions.txt'
    lines = [
        'test_full_nowcasting_sut_balance walkthrough',
        'Each PNG has a matching explanation page in the PDF (picture, then terms).',
        'Stage is the Step 5 layer, not the pytest stage number.',
        '',
    ]
    for name, (stage, caption) in PAGES.items():
        spec = EXPLAIN[name]
        lines.extend((f'{name}.png', f'  Stage: {stage}', f'  {caption}', '', '  Terms:'))
        for term, definition in spec['terms']:  # type: ignore[misc]
            lines.append(f'    {term}: {definition}')
        lines.extend(('', '  What is happening:', f'    {spec["what"]}', ''))
    path.write_text('\n'.join(lines) + '\n', encoding='utf-8')
    return path


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    # Matplotlib color mapping on tiny integer masks trips numpy interp.
    warnings.filterwarnings(
        'ignore',
        message='invalid value encountered in subtract',
        category=RuntimeWarning,
    )
    pages = _run()
    captions_path = _write_captions()
    pdf_path = OUTPUT_DIR / 'full_nowcasting_sut_balance.pdf'
    with PdfPages(pdf_path) as pdf:
        index = _index_figure()
        pdf.savefig(index)
        plt.close(index)
        for name, fig in pages:
            png = OUTPUT_DIR / f'{name}.png'
            fig.savefig(png, dpi=140)
            pdf.savefig(fig)
            plt.close(fig)
            explain = _explain_figure(name)
            pdf.savefig(explain)
            plt.close(explain)
            print(png)
    print(captions_path)
    print(pdf_path)


if __name__ == '__main__':
    main()
