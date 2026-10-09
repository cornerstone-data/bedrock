"""B Smoothing Phase 1 outcome: churn in ``B`` at the start and end of the project.

Compares three runs of ``B_change_diagnostics`` over 2017-2024, all on real
(constant-dollar) factors:

- ``baseline``: the run the project started from (FBS ``v0.3.0_99655e9``,
  MUT ``v0.3.0_4276083``, the v0.4 build).
- ``control``: the v0.5 tables with the purchase-based GHG FBS
  (``v0.3.0_0e1b0f0``), so the step from ``baseline`` is the new tables and
  the other emissions changes, without facility data.
- ``final``: the v0.5 tables with the facility GHG FBS, including the 2024
  spill anchor (#1074), so the step from ``control`` is the facility data.

Each run is an output folder (``B_change_real.csv``) and the cache folder it
was built from (``E_stratified.parquet``, ``x_real.parquet``). Both live under
``output/``, which is not committed; the defaults name the folders used for
``About_B_smoothing_phase1_outcome.md``.

Writes to ``output/phase1_outcome/``:

- ``acceptance_by_year.csv``: the plan's two acceptance measures per year,
  median ``abs_delta_B_pct_of_N`` and the share of commodities over 5%.
- ``churn_by_group.csv``: the same pooled over 2018-2024 by sector group,
  with the emissions-weighted mean.
- ``churn_by_sector.csv``: mean ``abs_delta_B_pct_of_N`` per commodity, with
  2024 emissions.
- ``reshuffle_table_3_11.csv``: the summed absolute year-on-year change in
  each sector's share of EPA table 3-11 combustion, in percentage points.
- ``E_x_comovement.csv``: emissions-weighted root-mean-square log change in
  ``E``, in real ``x`` and in ``E / x`` per industry, and the correlation
  between the ``E`` and ``x`` changes.
- four figures, also copied to ``images/`` with ``--publish``.

::

    uv run python -m bedrock.analysis.time_series_B_matrix.phase1_outcome
    uv run python -m bedrock.analysis.time_series_B_matrix.phase1_outcome --check
"""

from __future__ import annotations

import argparse
import shutil
import typing as ta
from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

HERE = Path(__file__).parent
OUTPUT = HERE / 'output'
IMAGES = HERE / 'images'
YEARS = range(2018, 2025)
GATE = 5.0
OIL_AND_GAS = {'211000', '213111', '21311A'}
FACILITY_GROUPS = ('Manufacturing', 'Oil and gas', 'Other mining', 'Utilities')


@dataclass(frozen=True)
class Run:
    label: str
    output: str
    cache: str
    fbs: str
    mut: str


RUNS = (
    Run(
        'Phase 1 baseline', 'baseline_v0.4', 'cache', 'v0.3.0_99655e9', 'v0.3.0_4276083'
    ),
    Run(
        'v0.5 without facility data',
        'v05_nonfac_full',
        'cache_v05_nonfac',
        'v0.3.0_0e1b0f0',
        'v0.3.0_3096818',
    ),
    Run('v0.5', 'v05_1074_full', 'cache_v05_1074', 'v0.3.0_a7206ca', 'v0.3.0_3096818'),
)
COLORS = ('#8c8c8c', '#9ecae1', '#08519c')


def sector_group(code: str) -> str:
    """Sector group of a BEA detail code, as used for the Phase 1 scoring."""
    prefix = code[:2]
    if code in OIL_AND_GAS:
        return 'Oil and gas'
    groups = {
        '21': 'Other mining',
        '22': 'Utilities',
        '31': 'Manufacturing',
        '32': 'Manufacturing',
        '33': 'Manufacturing',
        '11': 'Agriculture',
        '23': 'Construction',
        '42': 'Trade',
        '44': 'Trade',
        '45': 'Trade',
        '4B': 'Trade',
        '48': 'Transportation',
        '49': 'Transportation',
        'GS': 'Government',
        'S0': 'Government',
    }
    return groups.get(prefix, 'Services')


def check_vintages(run: Run) -> None:
    """Raise if the cache was built from other vintages than the run names."""
    text = (OUTPUT / run.cache / 'vintages.txt').read_text()
    for vintage in (run.fbs, run.mut):
        if vintage not in text:
            raise ValueError(
                f'{run.cache}/vintages.txt does not name {vintage}: {text.strip()}'
            )


def load_B_change(run: Run) -> pd.DataFrame:
    b = pd.read_csv(OUTPUT / run.output / 'B_change_real.csv', dtype={'commodity': str})
    b = b[b.year_to.isin(YEARS)].dropna(subset=['abs_delta_B_pct_of_N'])
    b['group'] = b.commodity.map(sector_group)
    b['run'] = run.label
    return b


def _log(frame: pd.DataFrame) -> pd.DataFrame:
    return ta.cast(pd.DataFrame, np.log(frame))


def _order_runs(frame: pd.DataFrame) -> pd.DataFrame:
    """Order (measure, run) columns as RUNS, keeping the measure order."""
    measures = list(
        dict.fromkeys(ta.cast(pd.MultiIndex, frame.columns).get_level_values(0))
    )
    return frame.reindex(
        columns=pd.MultiIndex.from_tuples(
            [(m, r.label) for m in measures for r in RUNS]
        )
    )


def emissions_by_sector(run: Run) -> pd.DataFrame:
    """Year x sector CO2e in kg."""
    E = pd.read_parquet(OUTPUT / run.cache / 'E_stratified.parquet')
    return E.groupby(['year', 'sector']).CO2e.sum().unstack('sector')


def acceptance_by_year(changes: pd.DataFrame) -> pd.DataFrame:
    g = changes.groupby(['year_to', 'run']).abs_delta_B_pct_of_N
    out = pd.concat(
        {
            'median': g.median(),
            f'over {GATE:g}%': g.apply(lambda s: 100 * (s > GATE).mean()),
        },
        axis=1,
    ).unstack('run')
    out = _order_runs(ta.cast(pd.DataFrame, out))
    out.loc['mean'] = out.mean()
    return out


def churn_by_group(changes: pd.DataFrame, weights: pd.DataFrame) -> pd.DataFrame:
    """Pooled 2018-2024; the weighted mean uses the final run's emissions in year_from."""
    w = ta.cast('pd.Series[float]', weights.stack()).rename('w').reset_index()
    w.columns = pd.Index(['year_from', 'commodity', 'w'])
    b = changes.merge(w, on=['year_from', 'commodity'], how='left').fillna({'w': 0.0})

    def stats(g: pd.DataFrame) -> pd.Series:
        x = g.abs_delta_B_pct_of_N
        return pd.Series(
            {
                'commodities': g.commodity.nunique(),
                'median': x.median(),
                'emissions-weighted mean': (x * g.w).sum() / g.w.sum(),
                f'over {GATE:g}%': 100 * (x > GATE).mean(),
            }
        )

    b['family'] = np.where(
        b.group.isin(FACILITY_GROUPS), 'With facility or sector data', 'Without'
    )
    frames = [
        b.groupby(['family', 'run'])[['commodity', 'abs_delta_B_pct_of_N', 'w']].apply(
            stats
        ),
        b.groupby(['group', 'run'])[['commodity', 'abs_delta_B_pct_of_N', 'w']].apply(
            stats
        ),
    ]
    return _order_runs(ta.cast(pd.DataFrame, pd.concat(frames).unstack('run')))


def churn_by_sector(changes: pd.DataFrame, emissions_2024: pd.Series) -> pd.DataFrame:
    t = (
        changes.groupby(['commodity', 'name', 'run'])
        .abs_delta_B_pct_of_N.mean()
        .unstack('run')
    )
    t = t[[r.label for r in RUNS]]
    t.insert(
        0,
        'E 2024 (Mt CO2e)',
        t.index.get_level_values('commodity').map(emissions_2024 / 1e9),
    )
    t['change from baseline (%)'] = 100 * (t[RUNS[2].label] / t[RUNS[0].label] - 1)
    t['change from control (%)'] = 100 * (t[RUNS[2].label] / t[RUNS[1].label] - 1)
    return t.sort_values('E 2024 (Mt CO2e)', ascending=False)


def reshuffle_table_3_11(run: Run) -> pd.Series:
    """Sum over sectors of |year-on-year change in share| of table 3-11 CO2e, pp."""
    E = pd.read_parquet(OUTPUT / run.cache / 'E_stratified.parquet')
    t = E[E.MetaSources.str.startswith('UMD_GHGIA_T_3_11')]
    shares = t.groupby(['year', 'sector']).CO2e.sum().unstack(fill_value=0)
    shares = 100 * shares.div(shares.sum(axis=1), axis=0)
    return shares.diff().abs().sum(axis=1).loc[list(YEARS)]


def E_x_comovement(run: Run) -> pd.Series:
    """Emissions-weighted RMS of year-on-year log changes per industry, pooled.

    The three RMS values satisfy rms(dln E/x)^2 = rms(dln E)^2 + rms(dln x)^2
    - 2 cov, so the correlation says how much co-movement of emissions and
    output cancels in the factor.
    """
    E = emissions_by_sector(run)
    x = pd.read_parquet(OUTPUT / run.cache / 'x_real.parquet')
    if x.shape[0] != E.shape[0]:
        x = x.T
    x.index = x.index.astype(int)
    common = E.columns.intersection(x.columns)
    E, x = E[common], x[common]
    ok = (E > 0) & (x > 0)
    dE = _log(E.where(ok)).diff().loc[list(YEARS)]
    dx = _log(x.where(ok)).diff().loc[list(YEARS)]
    w = E.shift(1).loc[list(YEARS)]
    mask = dE.notna() & dx.notna()
    a, b, ww = dE[mask].stack(), dx[mask].stack(), w[mask].stack()
    ww = ww / ww.sum()
    vE, vx, cov = (ww * a * a).sum(), (ww * b * b).sum(), (ww * a * b).sum()
    return pd.Series(
        {
            'rms dln E (%)': 100 * np.sqrt(vE),
            'rms dln x (%)': 100 * np.sqrt(vx),
            'correlation': cov / np.sqrt(vE * vx),
            'rms dln E/x (%)': 100 * np.sqrt(vE + vx - 2 * cov),
            'rms dln E/x if uncorrelated (%)': 100 * np.sqrt(vE + vx),
            'industry-years': int(mask.values.sum()),
        }
    )


def _direct_rms_E_over_x(run: Run) -> float:
    E = emissions_by_sector(run)
    x = pd.read_parquet(OUTPUT / run.cache / 'x_real.parquet')
    x = x if x.shape[0] == E.shape[0] else x.T
    x.index = x.index.astype(int)
    common = E.columns.intersection(x.columns)
    E, x = E[common], x[common]
    ok = (E > 0) & (x > 0)
    d = _log((E / x).where(ok)).diff().loc[list(YEARS)]
    w = E.shift(1).loc[list(YEARS)].where(d.notna())
    return float(100 * np.sqrt((w * d * d).sum().sum() / w.sum().sum()))


def plot_acceptance(acc: pd.DataFrame, path: Path) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(11, 4))
    for ax, measure, unit in zip(
        axes, ['median', f'over {GATE:g}%'], ['% of N', '% of commodities']
    ):
        for run, color in zip(RUNS, COLORS):
            ax.plot(
                list(YEARS),
                acc[(measure, run.label)].loc[list(YEARS)],
                marker='o',
                color=color,
                label=run.label,
            )
        ax.set_title(
            f'{measure} of |change in B| as % of N'
            if measure == 'median'
            else f'Commodities over the {GATE:g}% gate'
        )
        ax.set_ylabel(unit)
        ax.grid(alpha=0.3)
    axes[0].legend(frameon=False, loc='upper left')
    fig.suptitle('Acceptance measures by year: no lasting reduction over the project')
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def plot_comovement(co: pd.DataFrame, reshuffle: pd.DataFrame, path: Path) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(11, 4))
    cols = ['rms dln E (%)', 'rms dln x (%)', 'rms dln E/x (%)']
    xs = np.arange(len(cols))
    for i, (run, color) in enumerate(zip(RUNS, COLORS)):
        axes[0].bar(
            xs + (i - 1) * 0.27,
            co.loc[run.label].reindex(cols).to_numpy(dtype=float),
            width=0.27,
            color=color,
            label=run.label,
        )
    axes[0].set_xticks(xs, ['emissions E', 'real output x', 'direct factor E / x'])
    axes[0].set_ylabel('emissions-weighted RMS year-on-year change (%)')
    axes[0].set_title('Emissions move less, output more, the factor more')
    for i, run in enumerate(RUNS):
        axes[0].text(
            2 + (i - 1) * 0.27,
            co[cols[2]].astype(float)[run.label] + 0.2,
            f"r={co.loc[run.label, 'correlation']:.2f}",
            ha='center',
            fontsize=8,
        )
    axes[0].legend(frameon=False, fontsize=8, loc='lower left')
    for run, color in zip(RUNS, COLORS):
        axes[1].plot(
            list(YEARS), reshuffle[run.label], marker='o', color=color, label=run.label
        )
    axes[1].set_ylabel('sum of |change in sector share| (pp)')
    axes[1].set_title('Table 3-11 combustion reshuffled between sectors')
    axes[1].grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def plot_groups(groups: pd.DataFrame, path: Path) -> None:
    base, final = RUNS[0].label, RUNS[2].label
    g = groups.drop(index=['With facility or sector data', 'Without'])
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.5), sharey=True)
    for ax, measure in zip(axes, ['median', 'emissions-weighted mean']):
        chg = (100 * (g[(measure, final)] / g[(measure, base)] - 1)).sort_values()
        colors = ['#08519c' if v < 0 else '#cb181d' for v in chg]
        ax.barh(chg.index, chg.values, color=colors)
        ax.axvline(0, color='black', lw=0.8)
        ax.set_title(f'{measure}, v0.5 against the baseline')
        ax.set_xlabel('% change in |change in B| as % of N')
        ax.grid(axis='x', alpha=0.3)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def plot_sectors(sectors: pd.DataFrame, path: Path, n: int = 25) -> None:
    top = sectors.head(n).iloc[::-1]
    labels = [f'{c} {name[:38]}' for c, name in top.index]
    y = np.arange(len(top))
    fig, ax = plt.subplots(figsize=(9, 9))
    base, final = top[RUNS[0].label], top[RUNS[2].label]
    for i in range(len(top)):
        ax.plot(
            [base.iloc[i], final.iloc[i]],
            [y[i], y[i]],
            color='#cb181d' if final.iloc[i] > base.iloc[i] else '#08519c',
            lw=2,
        )
    ax.scatter(base, y, color=COLORS[0], label=RUNS[0].label, zorder=3)
    ax.scatter(final, y, color=COLORS[2], label=RUNS[2].label, zorder=3)
    ax.set_yticks(y, labels, fontsize=8)
    ax.set_xscale('log')
    ticks = [1, 2, 3, 5, 10, 15]
    ax.set_xticks(ticks, [str(t) for t in ticks])
    ax.set_xlabel('mean |change in B| as % of N, 2018-2024 (log scale)')
    ax.set_title(f'Churn in the {n} largest-emitting sectors (2024)')
    ax.legend(frameon=False, loc='lower right')
    ax.grid(axis='x', alpha=0.3)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


FIGURES = (
    'phase1_acceptance_by_year.png',
    'phase1_E_x_comovement.png',
    'phase1_churn_by_group.png',
    'phase1_churn_top_sectors.png',
)


def main(publish: bool = False) -> None:
    out = OUTPUT / 'phase1_outcome'
    out.mkdir(parents=True, exist_ok=True)
    for run in RUNS:
        check_vintages(run)
    changes = pd.concat([load_B_change(run) for run in RUNS])
    final_E = emissions_by_sector(RUNS[2])

    acc = acceptance_by_year(changes)
    groups = churn_by_group(changes, final_E)
    sectors = churn_by_sector(changes, ta.cast('pd.Series[float]', final_E.loc[2024]))
    reshuffle = pd.DataFrame({run.label: reshuffle_table_3_11(run) for run in RUNS})
    reshuffle.loc['median'] = reshuffle.median()
    co = pd.DataFrame({run.label: E_x_comovement(run) for run in RUNS}).T

    acc.to_csv(out / 'acceptance_by_year.csv')
    groups.to_csv(out / 'churn_by_group.csv')
    sectors.to_csv(out / 'churn_by_sector.csv')
    reshuffle.to_csv(out / 'reshuffle_table_3_11.csv')
    co.to_csv(out / 'E_x_comovement.csv')

    plot_acceptance(acc, out / FIGURES[0])
    plot_comovement(co, reshuffle.loc[list(YEARS)], out / FIGURES[1])
    plot_groups(groups, out / FIGURES[2])
    plot_sectors(sectors, out / FIGURES[3])
    if publish:
        for name in FIGURES:
            shutil.copy(out / name, IMAGES / name)
    with pd.option_context('display.width', 200):
        print(acc.round(3).to_string())
        print(co.round(3).to_string())
        print(reshuffle.round(1).to_string())


def check() -> None:
    """Verify the RMS identity and the run vintages; raise on failure."""
    for run in RUNS:
        check_vintages(run)
        co = E_x_comovement(run)
        direct = _direct_rms_E_over_x(run)
        if abs(co['rms dln E/x (%)'] - direct) > 1e-6:
            raise AssertionError(
                f'{run.label}: identity gives {co["rms dln E/x (%)"]:.6f}, direct {direct:.6f}'
            )
        print(
            f'{run.label}: vintages {run.fbs} / {run.mut}; rms dln E/x {direct:.3f}% matches the identity'
        )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        '--check', action='store_true', help='verify vintages and the RMS identity only'
    )
    parser.add_argument(
        '--publish', action='store_true', help='copy the figures to images/'
    )
    args = parser.parse_args()
    if args.check:
        check()
    else:
        main(publish=args.publish)
