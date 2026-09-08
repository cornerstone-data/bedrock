"""Why N moved, two complementary ways.

Both tables come from one derivation of B and A, because deriving them takes
minutes and the two views have to describe the same build.

**Table 1 - which input moved it.**  ``N`` changes for two independent reasons
and the headline number cannot tell them apart: **A**, the technology matrix
the nowcast rebuilds from annual data, and **B**, the emission intensities that
move when the GHG FBS series is regenerated.  Holding one at the baseline and
moving the other separates them.

⚠️ The two middle rows do not sum to the first.  ``N = 1'B (I-A)^-1`` is not
additive in A and B, so the remainder is their interaction: emissions moving in
sectors whose input structure also moved.  Quote the rows, not a sum.

``D = 1'B`` is the control on the B row.  If B moves ``D`` and ``N`` alike, the
emissions change is a level shift; if ``D`` moves much more, the change sits in
sectors nothing else depends on, so it never propagates.

**Table 2 - the mechanism, in units you can say out loud.**  ``N`` factors
exactly into two parts, per sector::

    N_j = sum_i d_i L_ij
        = (sum_i L_ij)              x  (sum_i d_i L_ij / sum_i L_ij)
        = output multiplier         x  average intensity of what is pulled

So a fall in N is either "a dollar drags less of the economy behind it"
(multiplier) or "what it drags is cleaner" (average intensity), and this says
how much of each.  The median column sum of ``A`` is reported alongside as the
thing the multiplier is built from.

CLI::

    uv run python -m bedrock.analysis.nowcasting.results.n_change_decomposition
    uv run python -m bedrock.analysis.nowcasting.results.n_change_decomposition \\
        --year 2023 --baseline v0_3_1 --csv
"""

from __future__ import annotations

import argparse
import typing as ta
from pathlib import Path

import pandas as pd

from bedrock.analysis.nowcasting.results._ef_smoke_lib import (
    NOWCAST_COMPARE_YEAR,
    V03_SNAPSHOT,
    _as_float_series,
    activate_live_config,
    config_stem,
)
from bedrock.utils.math.formulas import (
    compute_d,
    compute_L_matrix,
    compute_M_matrix,
    compute_n,
)
from bedrock.utils.snapshots import releases
from bedrock.utils.snapshots.loader import load_snapshot

OUT_DIR = Path(__file__).resolve().parent


class Build(ta.NamedTuple):
    """Baseline and live matrices on one shared sector axis."""

    B_old: pd.DataFrame
    B_new: pd.DataFrame
    A_old: pd.DataFrame
    A_new: pd.DataFrame
    vintage: str
    sectors: pd.Index
    dropped: int


def _n_from(B: pd.DataFrame, A: pd.DataFrame) -> pd.Series:
    """``N`` for one (B, A) pairing, through the formulas the model uses."""
    L = compute_L_matrix(A=A)
    return _as_float_series(compute_n(M=compute_M_matrix(B=B, L=L)))


def _median_pct(new: pd.Series, base: pd.Series) -> float:
    """Median percent change, dropping sectors the baseline puts at zero."""
    both = new.index.intersection(base.index)
    rel = (new.loc[both] - base.loc[both]) / base.loc[both].replace(0.0, pd.NA)
    return float(pd.to_numeric(rel, errors='coerce').dropna().median() * 100.0)


def load_build(year: int, baseline_key: str) -> Build:
    """Derive the live build once and align it to the baseline's sector axis."""
    from bedrock.transform.eeio.derived import (  # noqa: PLC0415
        derive_Aq_usa,
        derive_B_usa_non_finetuned,
    )

    B_old = load_snapshot('B_USA_non_finetuned', baseline_key)
    Adom_old = load_snapshot('Adom_USA', baseline_key)
    Aimp_old = load_snapshot('Aimp_USA', baseline_key)

    vintage = activate_live_config(year)
    B_new = derive_B_usa_non_finetuned()
    aq = derive_Aq_usa()

    # One sector axis for every combination, or the mixed pairings compare
    # different economies rather than different builds.
    sectors = (
        Adom_old.index.intersection(aq.Adom.index)
        .intersection(B_old.columns)
        .intersection(B_new.columns)
    )
    dropped = len(aq.Adom.index) - len(sectors)

    def sq(frame: pd.DataFrame) -> pd.DataFrame:
        return frame.reindex(index=sectors, columns=sectors).fillna(0.0)

    def cols(frame: pd.DataFrame) -> pd.DataFrame:
        return frame.reindex(columns=sectors).fillna(0.0)

    return Build(
        B_old=cols(B_old),
        B_new=cols(B_new),
        # The model's L uses domestic plus imported requirements together.
        A_old=sq(Adom_old) + sq(Aimp_old),
        A_new=sq(aq.Adom) + sq(aq.Aimp),
        vintage=vintage,
        sectors=sectors,
        dropped=dropped,
    )


def ab_decomposition(b: Build, label: str) -> pd.DataFrame:
    """Table 1: hold one of A / B at the baseline and move the other."""
    base = _n_from(b.B_old, b.A_old)
    return pd.DataFrame(
        [
            {
                'what moves': 'both B and A',
                'median change in N': _median_pct(_n_from(b.B_new, b.A_new), base),
            },
            {
                'what moves': f'A only, B held at {label}',
                'median change in N': _median_pct(_n_from(b.B_old, b.A_new), base),
            },
            {
                'what moves': f'B only, A held at {label}',
                'median change in N': _median_pct(_n_from(b.B_new, b.A_old), base),
            },
            {
                'what moves': "direct intensity D = 1'B",
                'median change in N': _median_pct(
                    _as_float_series(compute_d(B=b.B_new)),
                    _as_float_series(compute_d(B=b.B_old)),
                ),
            },
        ]
    )


def multiplier_intensity(b: Build) -> pd.DataFrame:
    """Table 2: N as output multiplier x average intensity of what is pulled."""

    def parts(B: pd.DataFrame, A: pd.DataFrame) -> dict[str, pd.Series]:
        L = compute_L_matrix(A=A)
        N = _as_float_series(compute_n(M=compute_M_matrix(B=B, L=L)))
        multiplier = L.sum(axis=0).astype(float)
        return {
            'a_colsum': A.sum(axis=0).astype(float),
            'multiplier': multiplier,
            'avg_intensity': N / multiplier.replace(0.0, pd.NA),
            'N': N,
        }

    old, new = parts(b.B_old, b.A_old), parts(b.B_new, b.A_new)
    rows = [
        ('intermediate input per $ of output (col sum of A)', 'a_colsum'),
        ('output multiplier (col sum of L)', 'multiplier'),
        ('avg intensity of what a $ pulls, kg CO2e/$', 'avg_intensity'),
        ('N, kg CO2e/$', 'N'),
    ]
    return pd.DataFrame(
        [
            {
                'quantity': label,
                'baseline': float(pd.to_numeric(old[key], errors='coerce').median()),
                'nowcast': float(pd.to_numeric(new[key], errors='coerce').median()),
                'change %': _median_pct(new[key], old[key]),
            }
            for label, key in rows
        ]
    )


def main() -> None:
    ap = argparse.ArgumentParser(
        description='Explain the change in N: A vs B, and multiplier vs intensity.'
    )
    ap.add_argument('--year', type=int, default=NOWCAST_COMPARE_YEAR)
    ap.add_argument(
        '--baseline',
        default=V03_SNAPSHOT,
        help='snapshot key to hold the other side at (default: the v0.3 release)',
    )
    ap.add_argument(
        '--csv', action='store_true', help='also write CSVs beside this file'
    )
    args = ap.parse_args()

    baseline = getattr(releases, args.baseline, args.baseline)
    b = load_build(args.year, baseline)
    short = str(baseline)[:7]

    print(f'\nnowcast {config_stem(args.year)} vs snapshot {baseline}')
    print(
        f'MUT vintage {b.vintage}   {len(b.sectors)} sectors on both sides'
        + (f'   ({b.dropped} dropped)' if b.dropped else '')
    )

    t1 = ab_decomposition(b, short)
    print('\n--- which input moved N ---')
    print(
        t1.to_string(index=False, formatters={'median change in N': '{:+.2f}%'.format})
    )
    print(
        'The middle two rows do not sum to the first: N is not additive in A '
        'and B,\nso the remainder is their interaction.'
    )

    t2 = multiplier_intensity(b)
    print('\n--- the mechanism: N = output multiplier x average intensity ---')
    print(
        t2.to_string(
            index=False,
            formatters={
                'baseline': '{:.4f}'.format,
                'nowcast': '{:.4f}'.format,
                'change %': '{:+.2f}%'.format,
            },
        )
    )
    print(
        'Medians are taken per row, so the two components do not multiply to '
        'the N row\nexactly; they are the size of each effect, not an identity.'
    )

    if args.csv:
        for name, table in (('ab', t1), ('mechanism', t2)):
            path = OUT_DIR / f'n_change_{name}_{args.year}.csv'
            table.to_csv(path, index=False)
            print(f'wrote {path}')


if __name__ == '__main__':
    main()
