"""Summary-level frozen redefinition-ratio span test (2018–2024).

Learns 2017 summary MUT ratios from published before/after tables, applies them
to published before-redef summary MUT for later years, and scores against
published after-redef summary MUT from the matched 1997–2024 BEA vintage.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from bedrock.analysis.nowcasting.after_redef_MUTs.redefinitions_ratio_implementation.summary_rollup import (
    RollupGateResult,
    compare_rollup_block,
    rollup_import_to_summary,
    rollup_make_to_summary,
    rollup_use_intermediate_to_summary,
    rollup_va_to_summary,
)
from bedrock.analysis.nowcasting.table_match import Tolerance, compare_tables
from bedrock.extract.iot.io_2017 import (
    load_2017_Uimp_before_redef_usa,
    load_2017_Utot_before_redef_usa,
    load_2017_V_before_redef_usa,
    load_2017_value_added_before_redef_usa,
    load_summary_Uimp_before_redef_usa,
    load_summary_Uimp_usa_2024_vintage,
    load_summary_Utot_before_redef_usa,
    load_summary_Utot_usa_2024_vintage,
    load_summary_V_before_redef_usa,
    load_summary_V_usa_2024_vintage,
    load_summary_value_added_before_redef_usa,
    load_summary_value_added_usa_2024_vintage,
)
from bedrock.transform.iot.nowcast_redefinition_ratios import (
    ATOL,
    MARGINS_VALUE_COLUMNS,
    RedefinitionRatios,
    apply_redefinition_ratios,
    compute_redefinition_ratios,
    industry_gross_output,
)
from bedrock.utils.taxonomy.bea.v2017_industry_summary import (
    USA_2017_SUMMARY_INDUSTRY_CODES,
)

REPORT_PATH = Path(__file__).resolve().parent / 'summary-span-test-report.md'
DEFAULT_SPAN_YEARS = (2018, 2019, 2020, 2021, 2022, 2023, 2024)
SUMMARY_INDUSTRIES = frozenset(USA_2017_SUMMARY_INDUSTRY_CODES)
HIGHLIGHT_SECTORS = frozenset({'22', '23', '721', '722', '42', 'HS'})


@dataclass(frozen=True)
class BuiltMutFrames:
    V: pd.DataFrame
    U: pd.DataFrame
    VA: pd.DataFrame
    Uimp: pd.DataFrame


@dataclass(frozen=True)
class SpanBlockScore:
    year: int
    block: str
    l1_relative_error: float | None
    n_cells_scored: int
    n_industries_off_gt_1pct: int
    n_industries_off_gt_25pct: int
    n_industries_off_gt_50pct: int
    worst_industries: tuple[str, ...]


@dataclass(frozen=True)
class SpanTestReport:
    rollup_ok: bool
    rollup_results: tuple[RollupGateResult, ...]
    round_trip_ok: bool
    scores: tuple[SpanBlockScore, ...]


def empty_margins_frame() -> pd.DataFrame:
    """Empty margins stub required by the ratio API (margins not scored here)."""
    idx = pd.MultiIndex.from_tuples([], names=['Industry Code', 'Commodity Code'])
    return pd.DataFrame(columns=list(MARGINS_VALUE_COLUMNS), index=idx, dtype=float)


def run_rollup_gate(year: int = 2017) -> list[RollupGateResult]:
    """Compare detail→summary rollup of 2017 before MUT to published summary."""
    if year != 2017:
        raise ValueError('rollup gate is defined for 2017 only')
    rolled = {
        'Make': rollup_make_to_summary(load_2017_V_before_redef_usa()),
        'Use': rollup_use_intermediate_to_summary(load_2017_Utot_before_redef_usa()),
        'VA': rollup_va_to_summary(load_2017_value_added_before_redef_usa()),
        'Import': rollup_import_to_summary(load_2017_Uimp_before_redef_usa()),
    }
    published = {
        'Make': load_summary_V_before_redef_usa(2017),
        'Use': load_summary_Utot_before_redef_usa(2017),
        'VA': load_summary_value_added_before_redef_usa(2017),
        'Import': load_summary_Uimp_before_redef_usa(2017),
    }
    return [
        compare_rollup_block(rolled[label], published[label], label=label)
        for label in ('Make', 'Use', 'VA', 'Import')
    ]


def learn_2017_summary_ratios() -> RedefinitionRatios:
    """Learn summary MUT ratios from published 2017 before/after tables."""
    V_b = load_summary_V_before_redef_usa(2017)
    U_b = load_summary_Utot_before_redef_usa(2017)
    VA_b = load_summary_value_added_before_redef_usa(2017)
    Uimp_b = load_summary_Uimp_before_redef_usa(2017)
    V_a = load_summary_V_usa_2024_vintage(2017)
    U_a = load_summary_Utot_usa_2024_vintage(2017)
    VA_a = load_summary_value_added_usa_2024_vintage(2017)
    Uimp_a = load_summary_Uimp_usa_2024_vintage(2017)
    empty = empty_margins_frame()
    return compute_redefinition_ratios(
        V_b,
        U_b,
        VA_b,
        Uimp_b,
        empty,
        V_a,
        U_a,
        VA_a,
        Uimp_a,
        empty,
        industry_set=SUMMARY_INDUSTRIES,
    )


def check_2017_round_trip(ratios: RedefinitionRatios) -> bool:
    """Apply learned ratios to 2017 before; return True if all blocks match after."""
    V_b = load_summary_V_before_redef_usa(2017)
    U_b = load_summary_Utot_before_redef_usa(2017)
    VA_b = load_summary_value_added_before_redef_usa(2017)
    Uimp_b = load_summary_Uimp_before_redef_usa(2017)
    empty = empty_margins_frame()
    V_hat, U_hat, VA_hat, Uimp_hat, _ = apply_redefinition_ratios(
        V_b, U_b, VA_b, Uimp_b, empty, ratios=ratios, x=None
    )
    targets = {
        'Make': (V_hat, load_summary_V_usa_2024_vintage(2017)),
        'Use': (U_hat, load_summary_Utot_usa_2024_vintage(2017)),
        'VA': (VA_hat, load_summary_value_added_usa_2024_vintage(2017)),
        'Import': (Uimp_hat, load_summary_Uimp_usa_2024_vintage(2017)),
    }
    ok = True
    for label, (got, exp) in targets.items():
        match = compare_tables(got, exp, tolerance=Tolerance(atol=ATOL, rtol=0.0))
        block_ok = match.ok(
            max_partial=0, max_miss=0, max_extra=0, max_margin_partial=0
        )
        if not block_ok:
            print(f'2017 round-trip FAILED for {label}')
            ok = False
    return ok


def apply_frozen_ratios_year(year: int, ratios: RedefinitionRatios) -> BuiltMutFrames:
    """Apply frozen 2017 summary ratios to published before-redef summary MUT."""
    V_b = load_summary_V_before_redef_usa(year)
    U_b = load_summary_Utot_before_redef_usa(year)
    VA_b = load_summary_value_added_before_redef_usa(year)
    Uimp_b = load_summary_Uimp_before_redef_usa(year)
    empty = empty_margins_frame()
    x_t = industry_gross_output(V_b)
    V_hat, U_hat, VA_hat, Uimp_hat, _ = apply_redefinition_ratios(
        V_b, U_b, VA_b, Uimp_b, empty, ratios=ratios, x=x_t
    )
    return BuiltMutFrames(V=V_hat, U=U_hat, VA=VA_hat, Uimp=Uimp_hat)


def _industry_axis(block: str) -> int:
    # Make: industries on rows; Use/VA/Import: industries on columns.
    return 0 if block == 'Make' else 1


def score_span_year_block(
    built: pd.DataFrame,
    published: pd.DataFrame,
    *,
    block: str,
    year: int,
) -> SpanBlockScore:
    """L1 relative error and industry off-counts for one block/year."""
    left, right = built.align(published, fill_value=0.0)
    left_f = left.astype(float)
    right_f = right.astype(float)
    mask = right_f.abs() > ATOL
    n_cells = int(mask.to_numpy().sum())
    if n_cells == 0:
        print(f'WARNING: no scored cells for {block} {year} (|published| > ATOL)')
        return SpanBlockScore(
            year=year,
            block=block,
            l1_relative_error=None,
            n_cells_scored=0,
            n_industries_off_gt_1pct=0,
            n_industries_off_gt_25pct=0,
            n_industries_off_gt_50pct=0,
            worst_industries=(),
        )

    abs_diff = (left_f - right_f).abs()
    denom = float(right_f.where(mask, 0.0).abs().to_numpy().sum())
    numer = float(abs_diff.where(mask, 0.0).to_numpy().sum())
    l1 = numer / denom if denom > 0 else None

    axis = _industry_axis(block)
    industries = list(right_f.index if axis == 0 else right_f.columns)
    off_1 = off_25 = off_50 = 0
    contrib: list[tuple[float, str]] = []
    for industry in industries:
        if axis == 0:
            pub = right_f.loc[industry]
            diff = abs_diff.loc[industry]
        else:
            pub = right_f.loc[:, industry]
            diff = abs_diff.loc[:, industry]
        cell_mask = pub.abs() > ATOL
        if not bool(cell_mask.any()):
            continue
        rel = (diff.where(cell_mask) / pub.where(cell_mask).abs()).abs()
        max_rel = float(np.nanmax(rel.to_numpy(dtype=float)))
        if max_rel > 0.01:
            off_1 += 1
        if max_rel > 0.25:
            off_25 += 1
        if max_rel > 0.50:
            off_50 += 1
        mass = float(diff.where(cell_mask, 0.0).to_numpy().sum())
        contrib.append((mass, str(industry)))

    contrib.sort(reverse=True)
    worst = tuple(code for _, code in contrib[:10])
    return SpanBlockScore(
        year=year,
        block=block,
        l1_relative_error=l1,
        n_cells_scored=n_cells,
        n_industries_off_gt_1pct=off_1,
        n_industries_off_gt_25pct=off_25,
        n_industries_off_gt_50pct=off_50,
        worst_industries=worst,
    )


def score_year(built: BuiltMutFrames, year: int) -> list[SpanBlockScore]:
    published = {
        'Make': load_summary_V_usa_2024_vintage(year),
        'Use': load_summary_Utot_usa_2024_vintage(year),
        'VA': load_summary_value_added_usa_2024_vintage(year),
        'Import': load_summary_Uimp_usa_2024_vintage(year),
    }
    frames = {
        'Make': built.V,
        'Use': built.U,
        'VA': built.VA,
        'Import': built.Uimp,
    }
    return [
        score_span_year_block(frames[block], published[block], block=block, year=year)
        for block in ('Make', 'Use', 'VA', 'Import')
    ]


def _use_l1_range(
    scores: tuple[SpanBlockScore, ...] | list[SpanBlockScore],
) -> tuple[float, float] | None:
    l1s = [
        s.l1_relative_error
        for s in scores
        if s.block == 'Use' and s.l1_relative_error is not None
    ]
    if not l1s:
        return None
    return min(l1s), max(l1s)


def _span_verdict_label(
    *,
    round_trip_ok: bool,
    use_l1: tuple[float, float] | None,
    scores: tuple[SpanBlockScore, ...] | list[SpanBlockScore],
) -> str:
    """Coarse band aligned with summary-span-test-plan decision table."""
    if not round_trip_ok:
        return 'setup-fail'
    if use_l1 is None:
        return 'incomplete'
    _lo, hi = use_l1
    if hi >= 0.10:
        return 'reject'
    max_use_gt1 = max(
        (s.n_industries_off_gt_1pct for s in scores if s.block == 'Use'),
        default=0,
    )
    if hi < 0.01 and max_use_gt1 < 20:
        return 'supportive'
    return 'mixed'


def write_report(report: SpanTestReport, path: Path = REPORT_PATH) -> None:
    """Write markdown span-test report next to this module."""
    use_l1 = _use_l1_range(report.scores)
    verdict = _span_verdict_label(
        round_trip_ok=report.round_trip_ok,
        use_l1=use_l1,
        scores=report.scores,
    )
    use_l1_text = (
        f'**{use_l1[0]:.1%}** – **{use_l1[1]:.1%}**' if use_l1 is not None else 'n/a'
    )

    if verdict == 'setup-fail':
        overall = (
            '**Verdict: setup fail — do not interpret span scores until the '
            '2017 summary round-trip passes.**'
        )
    elif verdict == 'reject':
        overall = (
            '**Verdict: reject / pivot — economy-wide L1 is too large for '
            'frozen 2017 ratios to be a credible year-`t` story.**'
        )
    elif verdict == 'supportive':
        overall = (
            '**Verdict: mostly supportive — round-trip passes and later-year '
            'gaps stay small without widespread industry blow-ups.**'
        )
    elif verdict == 'incomplete':
        overall = '**Verdict: incomplete — no Use L1 scores were produced.**'
    else:
        overall = (
            '**Verdict: mixed — do not treat 2017 reconstruction alone as '
            'enough for year-`t`.**'
        )

    lines: list[str] = [
        '# Summary redefinition span test report',
        '',
        'This report asks a follow-on question after '
        '[`ratio-reconstruction-report.md`](ratio-reconstruction-report.md): '
        'the ratio method in [`ratio-plan.md`](ratio-plan.md) can rebuild the '
        '**2017** after-redefinitions tables cell by cell — but does freezing '
        'those 2017 movements still look reasonable in **later years**?',
        '',
        'We answer that at the **summary** (coarse industry) level using '
        'BEA’s published before- and after-redefinitions Make / Use / value '
        'added / import tables for 2017–2024. Margins are not scored here '
        '(no matching annual summary margins series). Detail-level year-`t` '
        'checks still wait on Step 6 before-redefinitions inputs.',
        '',
        '## 1. Summary',
        '',
        '### Overall conclusion',
        '',
        overall,
        '',
        f'- The summary ratio path is '
        f'{"**wired correctly**" if report.round_trip_ok else "**not validated**"} '
        f'(2017 round-trip '
        f'**{"PASS" if report.round_trip_ok else "FAIL"}**).',
    ]
    if use_l1 is not None:
        lines.append(
            f'- Freezing 2017 movements tracks BEA’s later published after '
            f'tables **on average** (Use L1 about {use_l1_text} over scored '
            f'span years). See section 4 for industry-level fit.'
        )
    lines.extend(
        [
            f'- The detail→summary rollup gate is '
            f'**{"ok" if report.rollup_ok else "False"}** (section 2). That is '
            f'a concordance diagnostic only; learning and scoring use '
            f'published summary tables, so it does **not** by itself reject '
            f'the ratio method.',
            '',
            'Relative to '
            '[`ratio-reconstruction-report.md`](ratio-reconstruction-report.md) '
            'and [`ratio-plan.md`](ratio-plan.md): keep the 2017 operator story '
            'when round-trip passes; judge year-`t` carry from sections 4–5 '
            'using the bands in '
            '[`summary-span-test-plan.md`](summary-span-test-plan.md).',
            '',
            '### Scorecard',
            '',
            '| Check | Result | What it means |',
            '| --- | --- | --- |',
            f'| Rollup gate (section 2) | **{report.rollup_ok}** | Detail '
            'added up to summary vs published summary before within $0.5M |',
            f'| 2017 summary round-trip (section 3) | '
            f'**{report.round_trip_ok}** | Learned 2017 summary ratios rebuild '
            'published 2017 after |',
            f'| Use L1 relative error (section 4) | {use_l1_text} | Min–max '
            'of Use table L1 across scored span years |',
            '',
            '**Definition — L1 relative error.** For one year and one table:',
            '',
            '```text',
            'sum(|built − published|) / sum(|published|)',
            '```',
            '',
            'Only cells with published after greater than $0.5M are included. '
            'That ratio is the total dollar gap as a share of scored published '
            'after dollars. The **range** above is the minimum and maximum of '
            'that Use number across scored span years — not a confidence '
            'interval.',
            '',
            '### How to read the rest of this report',
            '',
            '- **Support keeping the ratio approach** if round-trip passes and '
            'later-year Use gaps stay small without sector meltdowns.',
            '- **Treat as mixed / discuss before merge** if L1 stays modest '
            'but many industries are still >1% off, or drift worsens.',
            '- **Reject / pivot** if L1 hits double digits in several years, '
            'or crisis / watch sectors explode.',
            '',
            f'**This run falls in the {verdict} band.** Details: sections 2–6.',
            '',
            '## 2. Rollup gate (2017 before detail → summary)',
            '',
            '**What this test is doing.** It takes the detailed 2017 '
            'before-redefinitions tables, adds child industries/commodities '
            'into their first summary parent, and compares that rolled-up '
            'result to BEA’s published summary before-redefinitions tables.',
            '',
            '**Why we do it.** The span test learns and scores on **published '
            'summary** tables, not on rolled detail. This gate only checks '
            'whether our detail→summary concordance is close enough that a '
            'reader can trust comparisons that mix those two worlds. It is a '
            'setup / taxonomy diagnostic, not a test of the ratio formula in '
            '[`ratio-plan.md`](ratio-plan.md).',
            '',
            '**Inputs.** Detailed 2017 before Make, Use, value added, and '
            'import; published summary 2017 before for the same four blocks; '
            'BEA detail→summary parent maps (first parent only).',
            '',
            '**What the outputs mean.**',
            '',
            '- `ok` — every compared cell matches within $0.5M, with no missing '
            'or extra cells.',
            '- `max abs diff` — largest single-cell dollar gap.',
            '- `partial` / `miss` / `extra` — cells that disagree, exist only '
            'on one side, or exist only on the other.',
            '',
            '| Block | ok | max abs diff | partial | miss | extra |',
            '| --- | --- | ---: | ---: | ---: | ---: |',
        ]
    )
    for r in report.rollup_results:
        lines.append(
            f'| {r.label} | {r.ok} | {r.max_abs_diff:,.0f} | {r.n_partial} | '
            f'{r.n_miss} | {r.n_extra} |'
        )

    if report.rollup_ok:
        rollup_conclusion = (
            '**Pass — detail→summary concordance matches published summary '
            'before within tolerance.** Safe to treat rolled-detail and '
            'published-summary views as aligned for taxonomy checks.'
        )
    else:
        rollup_conclusion = (
            '**Fail — concordance is imperfect; this does not reject the '
            'ratio method.** Learning and scoring in sections 3–4 always use '
            '**published** summary before/after pairs. Treat this as a warning '
            'against trusting any story that *depends* on rolling detail up '
            'to summary for acceptance.'
        )

    lines.extend(
        [
            '',
            '### Conclusion',
            '',
            rollup_conclusion,
            '',
            '## 3. 2017 summary round-trip',
            '',
            '**What this test is doing.** Same idea as the detail acceptance '
            'in [`ratio-reconstruction-report.md`](ratio-reconstruction-report.md), '
            'but on summary tables: learn movement ratios from the 2017 '
            'published summary before/after pair, apply them back to the 2017 '
            'before tables, and ask whether published 2017 after is recovered.',
            '',
            '**Why we do it.** If this fails, later-year span scores are '
            'untrustworthy — the operator, loaders, or industry filter would '
            'be broken before we even leave 2017. If it passes, we know the '
            'summary path implements the same ratio story as '
            '[`ratio-plan.md`](ratio-plan.md) on the tables we will freeze.',
            '',
            '**Inputs.** Published summary 2017 before Make / Use / value '
            'added / import; published summary 2017 after from the matched '
            '1997–2024 BEA vintage files; empty margins stub (API only).',
            '',
            '**What the output means.**',
            '',
            f'Published before + learned ratios vs 2024-vintage after: '
            f'**{"PASS" if report.round_trip_ok else "FAIL"}**',
            '',
            'PASS means all four blocks match within $0.5M cell by cell. '
            'FAIL means at least one block does not.',
            '',
            '### Conclusion',
            '',
        ]
    )
    if report.round_trip_ok:
        lines.append(
            '**PASS — the summary ratio path is accepted for 2017.** This is '
            'the summary analogue of the detail full-grid accept in '
            '[`ratio-reconstruction-report.md`](ratio-reconstruction-report.md). '
            'Span scores in section 4 are interpretable: failures there are '
            'about **generalization**, not a broken learn/apply implementation.'
        )
    else:
        lines.append(
            '**FAIL — do not use section 4 to judge year-`t` carry.** Fix '
            'loaders, industry filter, or the ratio operator first. A FAIL '
            'here is an implementation / data bug, not evidence that “2017 '
            'ratios don’t generalize.”'
        )

    lines.extend(
        [
            '',
            '## 4. Span scores (2018–2024)',
            '',
            '**What this test is doing.** Freeze the 2017 summary ratios from '
            'section 3. For each later year, take that year’s published '
            'summary **before** tables, apply the frozen ratios scaled by '
            'that year’s industry gross output, and compare the result to '
            'that year’s published summary **after** tables (same BEA vintage '
            'as learning).',
            '',
            '**Why we do it.** This is the first out-of-sample check of the '
            'production Step 7 story in [`ratio-plan.md`](ratio-plan.md): '
            'learn movements once in 2017, carry them forward. The 2017 '
            'reconstruction report only shows in-sample fit; reviewers asked '
            'whether that freeze still tracks BEA’s later after tables at '
            'summary level (without waiting on detail Step 6).',
            '',
            '**Inputs.** Frozen 2017 summary ratios; published summary before '
            'for each score year; published summary after for each score year '
            '(2024-vintage workbooks); industry gross output from that year’s '
            'before Make.',
            '',
            '**What the columns mean.**',
            '',
            '- **L1 rel err** — sum of absolute dollar gaps ÷ sum of absolute '
            'published after dollars, counting only cells whose published '
            'after amount exceeds $0.5M. A single number for “how far off is '
            'the whole table?” `n/a` means no cells cleared that floor.',
            '- **cells** — how many published cells entered that L1 calculation.',
            '- **>1% / >25% / >50% inds** — how many industries have at least '
            'one scored cell whose relative error exceeds that threshold '
            '(Make: industry rows; Use / value added / import: industry '
            'columns). High counts mean the gap is not just a few big cells.',
            '- **worst** — industries contributing the most absolute dollar '
            'error (top five shown).',
            '',
            '| Year | Block | L1 rel err | cells | >1% inds | >25% | >50% | worst |',
            '| ---: | --- | ---: | ---: | ---: | ---: | ---: | --- |',
        ]
    )
    for s in report.scores:
        l1 = 'n/a' if s.l1_relative_error is None else f'{s.l1_relative_error:.2%}'
        worst = ', '.join(s.worst_industries[:5]) if s.worst_industries else '—'
        lines.append(
            f'| {s.year} | {s.block} | {l1} | {s.n_cells_scored} | '
            f'{s.n_industries_off_gt_1pct} | {s.n_industries_off_gt_25pct} | '
            f'{s.n_industries_off_gt_50pct} | {worst} |'
        )

    if verdict == 'reject':
        span_conclusion = (
            '**Reject / pivot on out-of-sample carry** — economy-wide L1 is '
            'too large (see scorecard). Freezing 2017 movements does not track '
            'BEA’s later after tables well enough for year-`t`.'
        )
    elif verdict == 'supportive':
        span_conclusion = (
            '**Mostly supportive on out-of-sample carry** — L1 stays low and '
            'industry off-counts do not show cascading failures.'
        )
    elif verdict == 'setup-fail':
        span_conclusion = (
            '**Do not interpret** — 2017 round-trip failed; span numbers are '
            'not a valid generalization test.'
        )
    elif verdict == 'incomplete':
        span_conclusion = '**Incomplete** — no Use span scores to judge.'
    else:
        span_conclusion = (
            '**Mixed on out-of-sample carry — average error stays small; '
            'industry-level fit does not.** Use L1 remains low single digits '
            f'({use_l1_text}) but many industries still have cells >1% off. '
            'Not “mostly supportive”; not “reject/pivot” on L1 alone. '
            '**Discuss before treating frozen 2017 ratios as settled for '
            'year-`t`.** See section 5 for watch-sector concentration.'
        )

    lines.extend(
        [
            '',
            '### Conclusion',
            '',
            span_conclusion,
            '',
            '## 5. Worst sectors notes',
            '',
            '**What this section is doing.** It flags when pre-chosen '
            '“watch” industries appear in each block/year’s top-10 dollar '
            'error contributors. Watch codes: `22` (utilities), `23` '
            '(construction), `42` (wholesale), `721` (accommodation), '
            '`722` (food services / restaurants), `HS` (housing).',
            '',
            '**Why we do it.** Economy-wide L1 (section 4) can look fine '
            'while a few hard sectors absorb most of the miss — the same '
            'totals trap the reconstruction docs warn about. These sectors '
            'are also ones where redefinitions and mix shifts often matter.',
            '',
            '**Inputs.** The per-block/year worst-industry lists from '
            'section 4, filtered to the watch set above.',
            '',
            '**What the lines mean.** A bullet means that watch code was '
            'among the ten industries with the largest absolute dollar gap '
            'for that year and table. Absence of a code does not mean that '
            'sector matched perfectly — only that it was not in the top ten.',
            '',
        ]
    )
    highlights: list[str] = []
    highlight_codes: set[str] = set()
    for s in report.scores:
        hit = [c for c in s.worst_industries if c in HIGHLIGHT_SECTORS]
        if hit:
            highlight_codes.update(hit)
            highlights.append(
                f'- {s.year} {s.block}: highlighted among worst — {", ".join(hit)}'
            )
    if highlights:
        lines.extend(highlights)
        worst_conclusion = (
            '**Systematic watch-sector misses — reinforces a mixed (or worse) '
            'verdict, not a clean pass.** Recurring codes among top-10 dollar '
            f'errors: {", ".join(sorted(highlight_codes))}. Errors are not '
            'random dust: the freeze is repeatedly wrong in these sectors '
            'relative to BEA’s later after tables.'
        )
    else:
        lines.append(
            '- No highlighted sectors (`22`, `23`, `721`, `722`, `42`, `HS`) '
            'appeared in the top-10 worst lists.'
        )
        worst_conclusion = (
            '**No watch-sector concentration in the top-10 lists** — weaker '
            'evidence against the freeze on this filter alone; still read '
            'together with section 4 industry off-counts.'
        )

    lines.extend(
        [
            '',
            '### Conclusion',
            '',
            worst_conclusion,
            '',
            '## 6. Caveat',
            '',
            '**What this section is doing.** States the limit of what “match '
            'published after” can prove.',
            '',
            '**Why we include it.** Without this, section 4 reads like a '
            'ground-truth exam. It is not. BEA’s published summary after '
            'tables are annual estimates, not an independent lab measurement '
            'of the “true” redefinition operator for year `t` (same spirit as '
            '`frozen_mix_diagnostic.py`).',
            '',
            '**Inputs / outputs.** Narrative only — no extra numbers.',
            '',
            'Published summary after-redefinitions tables are BEA’s annual '
            'estimate, not independent ground truth. Using the matched '
            '1997–2024 after files for both learning and scoring removes '
            'release-revision noise from the frozen-ratio question; it does '
            '**not** make those after tables the true redefinitions for year '
            '`t`.',
            '',
            '### Conclusion',
            '',
            '**Span results measure agreement with BEA’s later after tables — '
            'not absolute truth of redefinitions.** Weigh sections 3–5 with '
            'the in-sample 2017 success in '
            '[`ratio-reconstruction-report.md`](ratio-reconstruction-report.md). '
            'Do **not** read a span “miss” as proof the 2017 operator in '
            '[`ratio-plan.md`](ratio-plan.md) is wrong for 2017, or a span '
            '“pass” as proof the method is BEA’s production rule for year `t`.',
            '',
        ]
    )
    path.write_text('\n'.join(lines), encoding='utf-8')
    print(f'Wrote {path}')


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description='Summary MUT frozen redefinition-ratio span test.'
    )
    parser.add_argument(
        '--years',
        nargs='+',
        type=int,
        default=list(DEFAULT_SPAN_YEARS),
        help='Score years (each must be in 2018–2024).',
    )
    parser.add_argument(
        '--rollup-only',
        action='store_true',
        help='Run 2017 rollup gate only; skip learn/apply/score.',
    )
    args = parser.parse_args(argv)

    for year in args.years:
        if year < 2018 or year > 2024:
            print(f'invalid score year {year}; must be in 2018–2024')
            return 1

    rollup_results = run_rollup_gate(2017)
    rollup_ok = all(r.ok for r in rollup_results)
    print('Rollup gate:')
    for r in rollup_results:
        print(
            f'  {r.label}: ok={r.ok} max_abs={r.max_abs_diff:,.0f} '
            f'partial={r.n_partial} miss={r.n_miss} extra={r.n_extra}'
        )

    if args.rollup_only:
        report = SpanTestReport(
            rollup_ok=rollup_ok,
            rollup_results=tuple(rollup_results),
            round_trip_ok=False,
            scores=(),
        )
        write_report(report)
        return 0

    ratios = learn_2017_summary_ratios()
    print(
        f'Learned ratios: V={len(ratios.V)} U={len(ratios.U)} '
        f'VA={len(ratios.VA)} Uimp={len(ratios.Uimp)}'
    )
    round_trip_ok = check_2017_round_trip(ratios)
    print(f'2017 round-trip: {"PASS" if round_trip_ok else "FAIL"}')

    scores: list[SpanBlockScore] = []
    for year in args.years:
        built = apply_frozen_ratios_year(year, ratios)
        year_scores = score_year(built, year)
        scores.extend(year_scores)
        for s in year_scores:
            l1 = 'n/a' if s.l1_relative_error is None else f'{s.l1_relative_error:.2%}'
            print(
                f'{s.year} {s.block}: L1={l1} '
                f'>1%={s.n_industries_off_gt_1pct} '
                f'>25%={s.n_industries_off_gt_25pct} '
                f'>50%={s.n_industries_off_gt_50pct}'
            )

    report = SpanTestReport(
        rollup_ok=rollup_ok,
        rollup_results=tuple(rollup_results),
        round_trip_ok=round_trip_ok,
        scores=tuple(scores),
    )
    write_report(report)
    return 0


if __name__ == '__main__':
    sys.exit(main())
