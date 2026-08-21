"""
What does carrying a stale detail mix forward actually cost? (Step 4a, #570)

The one **out-of-sample** measurement available for Step 4a's central question.
Everything else in ``output_estimation_plan.md`` that ranked sectors by "how much
mix work is needed" was a proxy scored in-sample on 2017, and three such proxies
disagreed with each other by an order of magnitude. This does not: it takes the
**2012** benchmark Make table, carries its commodity mix onto **2017** published
industry output, and scores the result against the **2017** benchmark, which is
the answer.

The horizon is right, too. Step 4a carries the 2017 detail mix to 2018-2024, so a
five-year 2012 to 2017 span is the same kind of extrapolation over most of the
same distance.

**The method under test** is the one Step 4a uses::

    q_hat[c] = sum_i (V12[i,c] / g12[i]) * x17[i]

then rescaled inside each summary group to that group's observed total — because
summary `q` is published for every Phase 1 year and is a control, not something
to estimate. ``A`` below is the no-mix-information baseline: allocate the same
group total by industry output alone.

⚠️ **After redefinitions, in producer prices.** BEA has moved the 2012 benchmark
off static download into an interactive application, so the only 2012 detail Make
available here is the redefined one in ``CEDA6IO.xlsx``; it is paired with the
2017 redefined table so both sides sit in the same space. Redefinitions reassign
secondary production, and economy-wide they cut the off-diagonal share from
**9.54% to 5.53%** — 1.73x — so a naive reading would understate Step 4a's
before-redefinitions exposure.

✅ **But that bias is much smaller than it looks for this test**, because what is
scored is the *within-group* split, and cross-group secondary production is
absorbed by the summary control either way. Within-group off-diagonal barely
moves: the largest gap is ``5415`` at 0.053, then ``213`` at 0.023, and most
groups are at 0.000. The per-group flags are printed by ``--flags``.

⚠️ **`213` is the one queued sector this test genuinely cannot speak to on its
own terms.** Its interesting secondary production — oil and gas extraction doing
its own drilling — is exactly what redefinitions reassign, so ``213`` reads as
96% diagonal here against 80% before redefinitions. The conclusion survives
anyway, and for a reason this test *can* support: that production is
**cross-group** (``211`` into ``213``), so the published summary `q(213)` absorbs
it, and all that remains is the two-way split, which lands to $4.6m on a $118bn
group.

Coverage: 398 of 402 commodities, 99.85% of 2017 `q`. Five codes changed between
benchmarks and are bridged by ``RENAME`` — no NAICS round-trip is needed, because
98.8% of 2012 detail codes are byte-identical in 2017.

Run: ``uv run python -m bedrock.analysis.nowcasting.mix_holdout_test``
     ``uv run python -m bedrock.analysis.nowcasting.mix_holdout_test --flags``
"""

from __future__ import annotations

import argparse

import numpy as np
import pandas as pd

from bedrock.analysis.nowcasting.frozen_mix_diagnostic import (
    detail_block,
    detail_to_summary,
)
from bedrock.extract.iot.io_2012 import load_2012_VR_usa
from bedrock.extract.iot.io_2017 import load_2017_V_after_redef_usa

#: The only five detail codes that changed between the 2012 and 2017 benchmarks.
#: ``33391A`` was renumbered; the four ``3352xx`` motor/generator codes merged.
RENAME = {
    '33391A': '333914',
    '335221': '335220',
    '335222': '335220',
    '335224': '335220',
    '335228': '335220',
}

BILLION = 1e9


def aligned_make_tables() -> tuple[pd.DataFrame, pd.DataFrame]:
    """2012 and 2017 Make tables on one shared industry x commodity index."""
    v12 = load_2012_VR_usa().rename(index=RENAME, columns=RENAME)
    # groupby on both axes: the 3352xx merge maps four codes onto one
    v12 = v12.groupby(v12.index).sum().T.groupby(lambda c: c).sum().T

    v17 = load_2017_V_after_redef_usa()
    v17.index = v17.index.astype(str)
    v17.columns = v17.columns.astype(str)

    industries = sorted(set(v12.index) & set(v17.index))
    commodities = sorted(set(v12.columns) & set(v17.columns))
    return v12.loc[industries, commodities], v17.loc[industries, commodities]


def groups(commodities: list[str]) -> dict[str, list[str]]:
    detail_summary = detail_to_summary()
    out: dict[str, list[str]] = {}
    for code in commodities:
        summary = detail_summary.get(code)
        if summary:
            out.setdefault(summary, []).append(code)
    return out


def apply_summary_control(
    estimate: pd.Series, actual: pd.Series, grouped: dict[str, list[str]]
) -> pd.Series:
    """Rescale each summary group to its observed total.

    This is not a courtesy to the estimate — summary `q` is published for every
    Phase 1 year, so the group total is an input. Scoring without it would
    measure an error Step 4a never carries.
    """
    out = estimate.copy()
    for kids in grouped.values():
        target = actual.reindex(kids).sum()
        built = out.reindex(kids).sum()
        if built > 0:
            out.loc[kids] = out.loc[kids] * target / built
    return out


def run() -> pd.DataFrame:
    v12, v17 = aligned_make_tables()
    commodities = list(v12.columns)
    grouped = groups(commodities)

    industry_output_2012 = v12.sum(axis=1)
    mix_2012 = v12.div(industry_output_2012.replace(0, np.nan), axis=0).fillna(0.0)
    industry_output_2017 = v17.sum(axis=1)
    actual = v17.sum(axis=0)

    carried = mix_2012.mul(industry_output_2017, axis=0).sum(axis=0)
    no_mix = industry_output_2017.reindex(commodities).fillna(0.0)

    print(
        f'2012 mix -> 2017, after redefinitions, producer prices\n'
        f'{len(commodities)} commodities, q17 {actual.sum() / 1e12:.3f}tn\n'
    )
    for label, series, control in (
        ('C: 2012 mix x 2017 industry output', carried, False),
        ('A: industry output alone', no_mix, False),
    ):
        gap = (series - actual).abs().sum() / 2
        print(
            f'  no control   {label:<38} {gap / BILLION:>8,.1f}bn  {gap / actual.sum():>6.2%}'
        )
    print()
    scored = {}
    for label, series in (('C', carried), ('A', no_mix)):
        controlled = apply_summary_control(series, actual, grouped)
        scored[label] = controlled
        gap = (controlled - actual).abs().sum() / 2
        print(
            f'  controlled   {label}: {"2012 mix" if label == "C" else "industry output alone":<35}'
            f' {gap / BILLION:>8,.1f}bn  {gap / actual.sum():>6.2%}'
        )

    rows = []
    for group, kids in grouped.items():
        if len(kids) < 2:
            continue
        total = actual.reindex(kids).sum()
        if total <= 0:
            continue
        err_c = (scored['C'].reindex(kids) - actual.reindex(kids)).abs().sum() / 2
        err_a = (scored['A'].reindex(kids) - actual.reindex(kids)).abs().sum() / 2
        rows.append(
            (
                group,
                len(kids),
                total / BILLION,
                err_c / BILLION,
                err_c / total,
                err_a / BILLION,
            )
        )
    return pd.DataFrame(
        rows,
        columns=['group', 'kids', 'q_bn', 'mix_err_bn', 'mix_err_pct', 'no_mix_err_bn'],
    ).sort_values('mix_err_bn', ascending=False)


def redefinition_flags() -> pd.DataFrame:
    """Within-group off-diagonal share, before vs after redefinitions.

    How much of what Step 4a faces this test is blind to. A large ``gap`` means
    the group's within-group secondary production is reassigned by redefinitions
    and the measured error understates the before-redefinitions case.
    """
    v17 = load_2017_V_after_redef_usa()
    v17.index = v17.index.astype(str)
    v17.columns = v17.columns.astype(str)
    before = detail_block()
    rows = []
    for group, kids in groups(list(before.index)).items():
        if len(kids) < 2:
            continue
        after_kids = [k for k in kids if k in v17.index and k in v17.columns]
        before_kids = [k for k in kids if k in before.index and k in before.columns]
        if not after_kids or not before_kids:
            continue
        a, b = v17.loc[after_kids, after_kids], before.loc[before_kids, before_kids]
        if not a.values.sum() or not b.values.sum():
            continue
        off_after = 1 - np.trace(a.values) / a.values.sum()
        off_before = 1 - np.trace(b.values) / b.values.sum()
        rows.append((group, off_before, off_after, off_before - off_after))
    return pd.DataFrame(
        rows, columns=['group', 'off_before', 'off_after', 'gap']
    ).sort_values('gap', ascending=False)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--flags',
        action='store_true',
        help='print how much before-redefinitions secondary production this test cannot see',
    )
    args = parser.parse_args()

    scored = run()
    fmt = lambda v: f'{v:,.2f}'  # noqa: E731
    print('\nper summary group, worst first:')
    print(scored.head(15).to_string(index=False, float_format=fmt))
    worse = scored[scored['mix_err_bn'] > scored['no_mix_err_bn']]
    if len(worse):
        print('\n!! groups where the STALE MIX IS WORSE than no mix at all:')
        print(worse.to_string(index=False, float_format=fmt))
    if args.flags:
        print('\nwithin-group off-diagonal, before vs after redefinitions:')
        print(redefinition_flags().head(12).to_string(index=False, float_format=fmt))


if __name__ == '__main__':
    main()
