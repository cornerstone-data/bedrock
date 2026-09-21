"""What moved each key sector's domestic total EF between two nowcast years.

:mod:`a_influence` answers *which cells a level of ``N`` rests on*, for one
year. This module answers the next question: **which cells moved it**, between
two years, one target sector at a time.

The decomposition
-----------------

With ``N_d = D L_d`` and ``L_d = (I - A_d)^-1``, the move in one target's
domestic total EF splits into an emissions side and a structure side. Writing
``1`` for the base year and ``2`` for the end year and ``D̄ = (D₁ + D₂)/2``,
the standard symmetric form is::

    ΔN_c  =  (D₂ - D₁) L̄[:, c]        <- emissions effect
           +  D̄ (L₂ - L₁)[:, c]        <- structure effect

and the structure effect opens up over ``A_d`` cells **exactly**, because::

    L₂ - L₁ = L₂ (A₂ - A₁) L₁ = L₁ (A₂ - A₁) L₂

Both orderings are exact identities (``L⁻¹ = I - A``), so averaging them keeps
the attribution exact and removes the ordering bias::

    contribution of cell (i, j) to target c
        = ½ [ (D̄L₂)_i ΔA_ij (L₁)_jc  +  (D̄L₁)_i ΔA_ij (L₂)_jc ]

⚠️ **Unlike** :func:`~a_influence.a_cell_leverage`, **this is a partition, not a
ranking.** The cell contributions sum to the structure effect exactly, so a
share of them *is* a share of the move, and cells may be negative. That is the
whole reason to prefer it here: "which cell drove the change" is a question
about a sum, and leverage does not sum.

⚠️ ``A_d`` only. The import block is excluded throughout, so every number here
is domestic technology. See :mod:`nowcast_key_sector_nd_series` for why ``N_d``
is not a footprint and must not be published as one.

⚠️ **Everything is in one dollar year before anything is differenced** -
``D``, ``A`` and ``L`` alike. Until 2026-09-21 only ``D`` was, on the view
that a dollar-to-dollar ratio needs no deflating; it does, because ``A`` moves
with the *relative* price, and the structure effect was absorbing that rather
than surviving it. See :func:`rebase` and
`About_the_nd_driver_basis.md <About_the_nd_driver_basis.md>`_.

::

    uv run python -m bedrock.analysis.nowcasting.results.nd_drivers
    uv run python -m bedrock.analysis.nowcasting.results.nd_drivers --check
    uv run python -m bedrock.analysis.nowcasting.results.nd_drivers --top 20
"""

from __future__ import annotations

import argparse
import typing as ta
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from bedrock.analysis.nowcasting.results._ef_smoke_lib import (
    SECTOR_LABELS,
    SECTORS,
    activate_live_config,
)

OUT_DIR = Path(__file__).resolve().parent
BASE_YEAR, END_YEAR = 2017, 2024

#: Relative tolerance on the additive identities. Two dense inverses in, so
#: this is float64 noise rather than a modelling tolerance.
TOLERANCE = 1e-9


class YearParts(ta.NamedTuple):
    """Everything one model year contributes to the decomposition."""

    D: pd.Series[float]
    A: pd.DataFrame
    L: pd.DataFrame
    N: pd.Series[float]
    vintage: str


def parts_for(year: int) -> YearParts:
    """``D``, ``A_d``, ``L_d`` and ``N_d`` for one nowcast year."""
    from bedrock.transform.eeio.derived import (  # noqa: PLC0415
        derive_Aq_usa,
        derive_B_usa_non_finetuned,
    )
    from bedrock.utils.math.formulas import (  # noqa: PLC0415
        compute_d,
        compute_L_matrix,
        compute_M_matrix,
        compute_n,
    )

    vintage = activate_live_config(year)
    B = derive_B_usa_non_finetuned()
    aq = derive_Aq_usa()
    A = aq.Adom
    L = compute_L_matrix(A=A)
    D = pd.Series(compute_d(B=B), dtype=float)
    N = pd.Series(compute_n(M=compute_M_matrix(B=B, L=L)), dtype=float)
    print(
        f'  {year}: vintage {vintage}  N_d.sum={float(N.sum()):.4g}  '
        f'D.sum={float(D.sum()):.4g}'
    )
    return YearParts(D=D, A=A, L=L, N=N, vintage=vintage)


def rebase(parts: YearParts, *, old_year: int, new_year: int) -> YearParts:
    """Move every part of the year onto *new_year* dollars - ``D``, ``A``, ``L``.

    ⚠️ **The decomposition is wrong without this.** ``D`` and ``N`` are per
    dollar of output, so two years of them are in two different dollars, and
    differencing them charges inflation to the emissions effect.

    ❌ **``A`` and ``L`` are rebased too, and until 2026-09-21 they were not**
    ([#958](https://github.com/cornerstone-data/bedrock/issues/958)). The
    reasoning then was that ``A`` is a ratio of current dollars to current
    dollars, so nothing needs doing to it, *"which is also why the structure
    effect is the term that survives the mistake intact"*. That is exactly
    backwards. ``a[i, j] = (p_i q_ij) / (p_j x_j)`` moves with the **relative**
    price ``p_i / p_j``; only uniform inflation cancels, and over 2017-2024 it
    was anything but. **The structure effect is the term that absorbs the
    price movement, not the one that survives it.**

    Both take the same transform, ``M[i, j] * r_j / r_i`` - deflate the input,
    re-inflate the output - with ``r`` the very ratio ``D`` is multiplied by.
    It carries through the inverse (``I - A_r = diag(1/r) (I - A) diag(r)``),
    so ``L`` needs no re-solve, and each year's rebased pair still satisfies
    ``L_r = (I - A_r)^-1``, which is what keeps the cell partition
    ``L2 - L1 = L2 (A2 - A1) L1`` exact.

    ✅ **``N`` is now basis-independent.** With both sides rebased the ``r_i``
    cancels and ``D_r @ L_r`` equals ``(D @ L) * r`` exactly, so recomputing
    ``N`` here and deflating the published ``N`` by the target's own price are
    the same number. That retires the old warning against quoting a level from
    here against one from :mod:`nowcast_key_sector_nd_series`: the two agree.
    ``--check`` asserts it.
    """
    if old_year == new_year:
        return parts
    from bedrock.utils.economic.inflation_helpers_ceda import (  # noqa: PLC0415
        obtain_inflation_factors_from_reference_data,
    )
    from bedrock.utils.math.formulas import (  # noqa: PLC0415
        rebase_coefficient_matrix,
    )
    from bedrock.utils.validation.diagnostics_helpers import (  # noqa: PLC0415
        inflation_adjust_ef_denom_to_new_base_year,
    )

    deflated = pd.Series(
        inflation_adjust_ef_denom_to_new_base_year(
            parts.D, new_base_year=new_year, old_base_year=old_year
        ),
        dtype=float,
    )
    # the same ratio, read off the same index, so the two sides cannot drift.
    # fillna(1.0) mirrors inflation_adjust_ef_denom_to_new_base_year: a sector
    # with no price index is left alone rather than dropped.
    levels = obtain_inflation_factors_from_reference_data()
    ratio = (levels[old_year] / levels[new_year]).reindex(parts.A.index).fillna(1.0)
    A = rebase_coefficient_matrix(matrix=parts.A, price_ratio=ratio)
    L = rebase_coefficient_matrix(matrix=parts.L, price_ratio=ratio)
    N = pd.Series(deflated @ L, dtype=float)

    # The cancellation, asserted rather than trusted: with both sides rebased,
    # D_r @ L_r must equal (D @ L) * r. It fails the moment one side is left
    # on its own year's prices, which is the defect this guard exists to catch.
    expected = parts.N * ratio.reindex(parts.N.index).fillna(1.0)
    scale = expected.abs().max()
    if scale > 0:
        worst = float((N - expected).abs().max() / scale)
        if worst > 1e-9:
            raise ValueError(
                f'rebasing {old_year} to {new_year} left D and L on different '
                f'dollar bases: D_r @ L_r departs from (D @ L) * r by {worst:.2e} '
                f'relative. See #958.'
            )

    return YearParts(
        D=deflated,
        A=A,
        L=L,
        N=N,
        vintage=parts.vintage,
    )


def _aligned(one: YearParts, two: YearParts) -> tuple[pd.Index, YearParts, YearParts]:
    """Both years on the commodities they share, so the algebra is well posed."""
    axis = one.A.index.intersection(two.A.index)
    dropped = (len(one.A.index) - len(axis), len(two.A.index) - len(axis))
    if any(dropped):
        print(f'  axis: {len(axis)} shared commodities; dropped {dropped} (base, end)')

    def narrow(p: YearParts) -> YearParts:
        return YearParts(
            D=p.D.reindex(axis).fillna(0.0),
            A=p.A.reindex(index=axis, columns=axis).fillna(0.0),
            L=p.L.reindex(index=axis, columns=axis).fillna(0.0),
            N=p.N.reindex(axis).fillna(0.0),
            vintage=p.vintage,
        )

    return axis, narrow(one), narrow(two)


def decompose(
    one: YearParts, two: YearParts, targets: list[str], top: int = 15
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Per-target effects, and the top ``A_d`` cells behind the structure effect.

    Returns ``(summary, cells)``. ``summary`` carries one row per target;
    ``cells`` carries ``top`` rows per target, ranked by absolute contribution.
    """
    axis, one, two = _aligned(one, two)
    targets = [t for t in targets if t in axis]

    d1, d2 = one.D.to_numpy(float), two.D.to_numpy(float)
    l1, l2 = one.L.to_numpy(float), two.L.to_numpy(float)
    dA = two.A.to_numpy(float) - one.A.to_numpy(float)
    d_bar = 0.5 * (d1 + d2)
    l_bar = 0.5 * (l1 + l2)

    # left-hand vectors, reused across every target
    left2, left1 = d_bar @ l2, d_bar @ l1
    # get_loc is typed as int | slice | mask; the axis is unique, so it is an int
    positions = {str(code): n for n, code in enumerate(axis)}
    pos = {code: positions[code] for code in targets}

    summary_rows: list[dict[str, object]] = []
    cell_rows: list[dict[str, object]] = []
    for code in targets:
        c = pos[code]
        n1, n2 = float(one.N.iloc[c]), float(two.N.iloc[c])
        emissions = float((d2 - d1) @ l_bar[:, c])
        structure = float(d_bar @ (l2[:, c] - l1[:, c]))

        contrib = 0.5 * (
            np.outer(left2, l1[:, c]) * dA + np.outer(left1, l2[:, c]) * dA
        )
        summary_rows.append(
            {
                'sector': code,
                'sector_label': SECTOR_LABELS.get(code, code),
                'N_d_base': n1,
                'N_d_end': n2,
                'change': n2 - n1,
                'pct_change': (n2 / n1 - 1.0) * 100.0 if n1 else np.nan,
                'emissions_effect': emissions,
                'structure_effect': structure,
                'structure_share_of_change': (
                    structure / (n2 - n1) * 100.0 if (n2 - n1) else np.nan
                ),
                'cell_sum_check': float(contrib.sum()),
                'L_col_sum_base': float(l1[:, c].sum()),
                'L_col_sum_end': float(l2[:, c].sum()),
                'L_col_sum_pct_change': (
                    (l2[:, c].sum() / l1[:, c].sum() - 1.0) * 100.0
                    if l1[:, c].sum()
                    else np.nan
                ),
            }
        )

        flat = pd.Series(contrib.ravel())
        keep = flat.abs().nlargest(top).index
        n_cols = len(axis)
        for rank, k in enumerate(keep, start=1):
            i, j = divmod(int(k), n_cols)
            cell_rows.append(
                {
                    'sector': code,
                    'sector_label': SECTOR_LABELS.get(code, code),
                    'rank': rank,
                    'input': axis[i],
                    'bought_by': axis[j],
                    'contribution': float(contrib[i, j]),
                    'share_of_structure_effect': (
                        contrib[i, j] / structure * 100.0 if structure else np.nan
                    ),
                    'A_base': float(one.A.to_numpy(float)[i, j]),
                    'A_end': float(two.A.to_numpy(float)[i, j]),
                    'delta_A': float(dA[i, j]),
                }
            )

    return pd.DataFrame(summary_rows), pd.DataFrame(cell_rows)


def check(summary: pd.DataFrame) -> int:
    """The two additive identities the decomposition rests on."""
    bad = 0
    effects = summary['emissions_effect'] + summary['structure_effect']
    scale = summary['change'].abs().clip(lower=1e-12)
    worst_split = float(((effects - summary['change']).abs() / scale).max())
    if worst_split > TOLERANCE:
        bad += 1
        print(f'FAIL: emissions + structure != change; worst rel {worst_split:.2e}')
    else:
        print(f'OK: emissions + structure = change   (worst rel {worst_split:.2e})')

    sstat = summary['structure_effect'].abs().clip(lower=1e-12)
    worst_cells = float(
        ((summary['cell_sum_check'] - summary['structure_effect']).abs() / sstat).max()
    )
    if worst_cells > TOLERANCE:
        bad += 1
        print(
            'FAIL: A-cell contributions do not sum to the structure effect; '
            f'worst rel {worst_cells:.2e}'
        )
    else:
        print(f'OK: A-cell contributions partition it (worst rel {worst_cells:.2e})')
    return bad


def annual(years: list[int], targets: list[str], top: int) -> pd.DataFrame:
    """The same split for every consecutive pair, so the timing is visible.

    Every year is rebased to the last year's dollars first, which makes the
    per-span changes telescope: they sum to the endpoint change exactly.

    ⚠️ **The split telescopes only approximately.** Each span forms its own
    ``D̄`` and ``L̄``, so the chained emissions and structure effects sum to
    something close to, but not identical with, the endpoint decomposition.
    That is ordinary index-number drift, and the chained version is the better
    of the two for reading timing, because a single 2017-2024 pair averages
    over a period in which the structure moved in both directions.
    """
    dollars = years[-1]
    parts = {}
    for year in years:
        parts[year] = rebase(parts_for(year), old_year=year, new_year=dollars)

    frames = []
    for base, end in zip(years, years[1:]):
        summary, _ = decompose(parts[base], parts[end], targets, top=top)
        summary.insert(2, 'base_year', base)
        summary.insert(3, 'end_year', end)
        summary.insert(4, 'span', f'{base}–{end}')
        rc = check(summary)
        if rc:
            raise SystemExit(f'identities failed on {base}-{end}')
        frames.append(summary)
    out = pd.concat(frames, ignore_index=True)
    out['emissions_pp'] = out['emissions_effect'] / out['N_d_base'] * 100.0
    out['structure_pp'] = out['structure_effect'] / out['N_d_base'] * 100.0
    return out


def annual_panel(annual_df: pd.DataFrame, png_name: str) -> None:
    """One small panel per sector: emissions and structure, year by year."""
    from bedrock.utils.validation.analysis.plotting import setup_mpl  # noqa: PLC0415

    setup_mpl(font_size=9)
    codes = [c for c in SECTORS if c in set(annual_df['sector'])]
    ncols = 5
    nrows = -(-len(codes) // ncols)
    fig, axes = plt.subplots(nrows, ncols, figsize=(16.0, 3.1 * nrows), sharex=True)
    flat = axes.ravel()
    spans = list(dict.fromkeys(annual_df['span']))
    x = np.arange(len(spans))
    for ax, code in zip(flat, codes):
        sub = annual_df[annual_df['sector'] == code].set_index('span').reindex(spans)
        em = sub['emissions_pp'].to_numpy(float)
        st = sub['structure_pp'].to_numpy(float)
        ax.bar(x, em, 0.62, label='emissions', color='#2c6e49')
        ax.bar(x, st, 0.62, bottom=em, label='structure', color='#8aa8bd')
        ax.axhline(0, color='black', lw=0.8)
        ax.set_title(SECTOR_LABELS.get(code, code), fontsize=10)
        ax.set_xticks(x)
        ax.set_xticklabels([s.split('–')[1] for s in spans], rotation=0)
        ax.tick_params(labelsize=8)
    for ax in flat[len(codes) :]:
        ax.set_visible(False)
    for ax in axes[:, 0] if nrows > 1 else [flat[0]]:
        ax.set_ylabel('% of the year’s opening level')
    handles, labels = flat[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc='lower center', ncol=2, frameon=False)
    fig.suptitle(
        'Year-on-year change in domestic total EF (N_d), split into emissions '
        'intensity and structure',
        fontsize=13,
    )
    fig.tight_layout(rect=(0, 0.04, 1, 0.96))
    path = OUT_DIR / png_name
    fig.savefig(path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print('wrote', path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    parser.add_argument('--base', type=int, default=BASE_YEAR)
    parser.add_argument('--end', type=int, default=END_YEAR)
    parser.add_argument('--top', type=int, default=15, help='Cells per sector.')
    parser.add_argument('--check', action='store_true')
    parser.add_argument(
        '--annual',
        action='store_true',
        help='Decompose every consecutive year pair instead of the endpoints.',
    )
    args = parser.parse_args()

    if args.annual:
        years = list(range(args.base, args.end + 1))
        print(f'deriving {years[0]}..{years[-1]} ...')
        table = annual(years, list(SECTORS), top=args.top)
        path = OUT_DIR / f'nd_drivers_annual_{args.base}_{args.end}.csv'
        table.to_csv(path, index=False)
        print('wrote', path)
        annual_panel(table, f'nd_drivers_annual_{args.base}_{args.end}.png')
        pivot = table.pivot(index='sector_label', columns='span', values='pct_change')
        print('\n=== year-on-year % change in N_d ===')
        print(pivot.round(1).to_string())
        spivot = table.pivot(
            index='sector_label', columns='span', values='structure_pp'
        )
        print('\n=== of which structure (percentage points) ===')
        print(spivot.round(1).to_string())
        return 0

    print(f'deriving {args.base} and {args.end} ...')
    one, two = parts_for(args.base), parts_for(args.end)
    # both years onto end-year dollars before anything is differenced
    one = rebase(one, old_year=args.base, new_year=args.end)
    print(f'  rebased {args.base} to {args.end}$: N_d.sum={float(one.N.sum()):.4g}')
    summary, cells = decompose(one, two, list(SECTORS), top=args.top)

    summary = summary.rename(
        columns={
            'N_d_base': f'N_d_{args.base}',
            'N_d_end': f'N_d_{args.end}',
            'L_col_sum_base': f'L_col_sum_{args.base}',
            'L_col_sum_end': f'L_col_sum_{args.end}',
        }
    )
    cells = cells.rename(columns={'A_base': f'A_{args.base}', 'A_end': f'A_{args.end}'})
    summary_path = OUT_DIR / f'nd_drivers_summary_{args.base}_{args.end}.csv'
    cells_path = OUT_DIR / f'nd_drivers_cells_{args.base}_{args.end}.csv'
    summary.to_csv(summary_path, index=False)
    cells.to_csv(cells_path, index=False)
    print('wrote', summary_path)
    print('wrote', cells_path)

    rc = check(summary)
    if args.check:
        return rc

    pd.set_option('display.width', 200)
    show = summary[
        [
            'sector_label',
            f'N_d_{args.base}',
            f'N_d_{args.end}',
            'pct_change',
            'emissions_effect',
            'structure_effect',
            'structure_share_of_change',
            'L_col_sum_pct_change',
        ]
    ]
    print('\n=== per sector ===')
    print(show.round(3).to_string(index=False))

    for code in summary['sector']:
        sub = cells[cells['sector'] == code].head(8)
        label = SECTOR_LABELS.get(code, code)
        row = summary[summary['sector'] == code].iloc[0]
        print(
            f'\n--- {label} ({code})   structure effect {row["structure_effect"]:+.4f}'
        )
        print(
            sub[
                [
                    'input',
                    'bought_by',
                    'contribution',
                    'share_of_structure_effect',
                    f'A_{args.base}',
                    f'A_{args.end}',
                ]
            ]
            .round(5)
            .to_string(index=False)
        )
    return rc


if __name__ == '__main__':
    raise SystemExit(main())
