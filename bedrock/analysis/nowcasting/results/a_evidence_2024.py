"""Is the nowcast ``A`` matrix tied to primary data, where it matters for ``N``?

Both the Cornerstone **v0.3** release and the **2024 nowcast** are stamped 2024,
and they reach their direct-requirements matrix two different ways.  The configs
say so outright:

===========================  ================================  =====================
                             ``2025_usa_cornerstone_v0_3``     ``..._v0_4_nowcast_2024``
===========================  ================================  =====================
detail IO source             published BEA 2017 benchmark      ``nowcast``
base IO year                 2017                              2024
``apply_io_year_adjustments``  ``True`` -- summary-block         ``False``
                             scaling + commodity price index
===========================  ================================  =====================

This module measures what that difference is worth, in five passes.

1. **Reconstruction.**  Can the 2024 matrix be rebuilt from the published 2017
   one with a factor per summary block-pair and a factor per commodity?  If yes,
   it carries no cell-level information about 2024.  ⚠️ The test needs a control,
   because the pipeline does not reproduce the 2017 benchmark exactly: the
   nowcast's *own* 2017 residual is the noise floor any 2024 residual has to
   clear.
2. **``N``.**  Emission factors for the significant sectors, ``B`` held fixed at
   the v0.3 snapshot so only the ``A`` method moves.
3. **Leverage.**  Which ``A`` cells those ``N`` values rest on, via
   :mod:`~bedrock.analysis.nowcasting.results.a_influence`.
4. **Provenance.**  Those cells joined to
   :mod:`~bedrock.analysis.nowcasting.seed_coverage`, which names the survey
   behind each one and counts how many cells share it.
5. **Imports.**  The domestic/import split of the same leverage, against observed
   commodity trade.

⚠️ **``B`` is held fixed on purpose.**  The two models also differ in their GHG
attribution and in ``x``.  Letting ``B`` move would mix the emissions side into a
measurement of the economics, so every number here uses the v0.3 snapshot's
``B``.  The full-model difference is reported once, for context, and used for
nothing.

CLI::

    uv run python -m bedrock.analysis.nowcasting.results.a_evidence_2024
    uv run python -m bedrock.analysis.nowcasting.results.a_evidence_2024 --ladder
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import cast

import numpy as np
import pandas as pd
from matplotlib.figure import Figure

from bedrock.analysis.nowcasting.results import a_influence as influence
from bedrock.utils.math.formulas import (
    compute_input_contribution,
    compute_L_matrix,
    compute_n,
)
from bedrock.utils.snapshots import releases
from bedrock.utils.snapshots.loader import load_snapshot

OUT_DIR = Path(__file__).resolve().parent
FBS_DIR = Path(__file__).resolve().parents[3] / 'transform' / 'output_data'

YEAR = 2024
BENCHMARK_YEAR = 2017

#: Two methods, two colours -- the deck's own, so these figures sit beside
#: ``a_method_figures_with_nowcast`` without a palette change.
NOWCAST_COLOR = '#d62728'
V03_COLOR = '#1f77b4'

#: ⚠️ Reserved state colour, not a series: "no annual source reaches this cell".
#: The same cool grey :mod:`seed_coverage` uses, chosen there so deuteranopia
#: does not collapse it onto the green ramp.
CARRIED_COLOR = '#8e99a8'

#: How specific the observation behind a cell is.  One ordered scale, so it takes
#: :mod:`seed_coverage`'s validated ramp ends rather than three unrelated hues.
STATE_COLORS = {
    'primary': '#052e16',
    'allocated': '#56c98a',
    'carried': CARRIED_COLOR,
}
STATE_ORDER = ('primary', 'allocated', 'carried')

#: Categorical, one slot per survey.  Validated with the ``dataviz`` six checks
#: at the light surface (all pass; worst adjacent CVD dE 8.8 protan, normal-vision
#: floor 16.0).  ``carried`` keeps the reserved grey above.
SOURCE_COLORS = {
    'census_held': '#2f6fb0',
    'aies': '#2a9d8f',
    'ers': '#e76f51',
    'eia923': '#8f6fc4',
    'census_recovered': '#b8860b',
    'carried': CARRIED_COLOR,
    'unmapped': CARRIED_COLOR,
}

#: What each survey label means, for figure legends and the writeup.
SOURCE_LABELS = {
    'census_held': 'Economic Census 2022 (held)',
    'aies': 'AIES annual expenses',
    'ers': 'USDA ERS farm income',
    'eia923': 'EIA-923 plant fuel',
    'census_recovered': 'Economic Census (recovered)',
    'carried': 'carried 2017 structure',
    'unmapped': 'carried 2017 structure',
}


def _tick(t0: float, message: str) -> None:
    print(f'[{time.time() - t0:6.1f}s] {message}', flush=True)


# ------------------------------------------------------------------ models


class Models:
    """The three ``A`` matrices, one ``B``, and everything derived from them."""

    def __init__(self, year: int = YEAR) -> None:
        from bedrock.analysis.nowcasting.results._ef_smoke_lib import (  # noqa: PLC0415
            aq_from_live_config,
        )

        self.year = year
        self.B = load_snapshot('B_USA_non_finetuned', releases.v0_3_1)
        self.D = self.B.sum(axis=0)

        aq, self.vintage = aq_from_live_config(year)
        self.nowcast = aq.Adom + aq.Aimp
        self.nowcast_dom, self.nowcast_imp = aq.Adom, aq.Aimp
        self.q = aq.scaled_q

        self.v03_dom = load_snapshot('Adom_USA', releases.v0_3_1)
        self.v03_imp = load_snapshot('Aimp_USA', releases.v0_3_1)
        self.v03 = self.v03_dom + self.v03_imp

        self.L = compute_L_matrix(A=self.nowcast)
        self.N = compute_n(M=self.B @ self.L)
        self.v03_L = compute_L_matrix(A=self.v03)
        self.v03_N = compute_n(M=self.B @ self.v03_L)

        self.targets = influence.significant_codes(self.nowcast.index)
        self.absent = influence.missing_codes(self.nowcast.index)


def published_benchmark_A() -> pd.DataFrame:
    """The published BEA 2017 detail ``A``, on the Cornerstone axis.

    Reached by running the v0.3 config with its year adjustments switched off,
    which is exactly the matrix v0.3 starts from before it scales and inflates.
    ⚠️ Leaves the process config reset, so callers must reinstall theirs.
    """
    from bedrock.publish.cache_reset import clear_all_publish_caches  # noqa: PLC0415
    from bedrock.utils.config.config_controllers import (  # noqa: PLC0415
        force_set_usa_config,
    )
    from bedrock.utils.config.usa_config import reset_usa_config  # noqa: PLC0415

    clear_all_publish_caches()
    reset_usa_config()
    force_set_usa_config('2025_usa_cornerstone_v0_3', apply_io_year_adjustments=False)
    from bedrock.transform.eeio.derived_cornerstone import (  # noqa: PLC0415
        derive_cornerstone_Aq,
    )

    derive_cornerstone_Aq.cache_clear()
    aq = derive_cornerstone_Aq()
    return aq.Adom + aq.Aimp


# ------------------------------------------------- 1. reconstruction test


def _summary_pairs(codes: list[str]) -> tuple[np.ndarray, int]:
    """``(pair id per cell, number of summary blocks)`` for a code list."""
    from bedrock.analysis.nowcasting.block_provenance import (  # noqa: PLC0415
        _detail_to_summary,
    )

    lookup = _detail_to_summary()
    summary = np.array([lookup.get(code, code) for code in codes])
    _, index = np.unique(summary, return_inverse=True)
    blocks = int(index.max()) + 1
    return index[:, None] * blocks + index[None, :], blocks


def reconstruction_residual(
    candidate: pd.DataFrame, benchmark: pd.DataFrame, iterations: int = 200
) -> dict[str, float]:
    """How much of ``log(candidate / benchmark)`` a mechanical rescale explains.

    The model fitted is ``A_new = A_2017 x block(S(i), S(j)) x price(i)`` -- one
    factor per summary block-pair and one per commodity, which is what
    summary-ratio scaling followed by a commodity price index produces.  What is
    left is cell-level information the benchmark did not already contain.

    Weighted by the 2017 cell size, so the answer is about the coefficients that
    carry the economy rather than the long tail of tiny ones.
    """
    pair, blocks = _summary_pairs(list(candidate.index))
    new, base = candidate.to_numpy(), benchmark.to_numpy()
    both = (base > 0) & (new > 0)

    ratio = np.where(
        both, np.log(np.where(both, new / np.where(both, base, 1.0), 1.0)), 0.0
    )
    weight = np.where(both, base, 0.0)
    before = float(np.sqrt((weight * ratio**2).sum() / weight.sum()))

    row = np.zeros(ratio.shape[0])
    flat_pair, flat_weight = pair.ravel(), weight.ravel()
    size = blocks * blocks
    block = np.zeros(size)
    for _ in range(iterations):
        residual = (ratio - row[:, None]).ravel()
        numerator = np.bincount(
            flat_pair, weights=flat_weight * residual, minlength=size
        )
        denominator = np.bincount(flat_pair, weights=flat_weight, minlength=size)
        block = np.where(
            denominator > 0, numerator / np.maximum(denominator, 1e-30), 0.0
        )
        remainder = ratio - block[pair]
        row_weight = weight.sum(axis=1)
        row = np.where(
            row_weight > 0,
            (weight * remainder).sum(axis=1) / np.maximum(row_weight, 1e-30),
            0.0,
        )

    left = np.where(both, ratio - block[pair] - row[:, None], 0.0)
    after = float(np.sqrt((weight * left**2).sum() / weight.sum()))
    return {
        'cells': int(both.sum()),
        'rms_log_ratio': before,
        'rms_residual': after,
        'explained': 1.0 - (after / before) ** 2,
    }


# ---------------------------------------------------- 4. provenance join


def leverage_with_provenance(models: Models, year: int = YEAR) -> pd.DataFrame:
    """Every Use cell's leverage on the significant sectors, with its source."""
    from bedrock.analysis.nowcasting import seed_coverage  # noqa: PLC0415

    leverage = influence.use_cell_leverage_from_model(
        N=models.N, D=models.D, L=models.L, q=models.q, targets=models.targets
    )
    stacked = cast('pd.Series[float]', leverage.stack())
    stacked = stacked.loc[stacked.to_numpy(dtype=float) != 0.0]
    frame = pd.DataFrame(
        {
            'commodity': [pair[0] for pair in stacked.index],
            'industry': [pair[1] for pair in stacked.index],
            'leverage': stacked.to_numpy(float),
        }
    )

    pedigree = seed_coverage.pedigree_cells(year)[
        ['commodity', 'industry', 'n', 'source', 'reliability']
    ]
    merged = frame.merge(pedigree, on=['commodity', 'industry'], how='left')
    merged['source'] = merged['source'].fillna('unmapped')
    # `unmapped` is a cell the provenance map does not reach at all -- it holds
    # 2017 structure for the same reason a `carried` cell does, so it is read as
    # carried rather than as an unknown.
    carried = merged['source'].isin(['carried', 'unmapped'])
    merged['state'] = np.where(
        carried, 'carried', np.where(merged['n'] <= 1.0, 'primary', 'allocated')
    )
    merged['share'] = merged['leverage'] / merged['leverage'].sum()
    return merged.sort_values('leverage', ascending=False).reset_index(drop=True)


# --------------------------------------------------------- 5. stage ladder


def stage_ladder(models: Models, cells: pd.DataFrame, year: int = YEAR) -> pd.DataFrame:
    """Each cell's dollars at every stage of the build, seed to model input.

    Chained through the artifacts' ``_metadata.json`` sidecars rather than by
    guessing filenames: ⚠️ the balanced SUT carries a **different vintage hash**
    from the MUT built on it, so a filename guess silently reads the wrong run.

    ⚠️ ``balanced -> before_redef`` is a **valuation and unit change** (purchaser
    to producer prices, BEA ``$M`` to USD), not a revision of the data.  Read
    that step as a restatement.
    """
    from bedrock.transform.iot.nowcast import (  # noqa: PLC0415
        derive_initial_U_intermediate,
    )
    from bedrock.transform.iot.nowcast_interior_fit import fit_interior  # noqa: PLC0415

    vintage = models.vintage
    after = pd.read_parquet(
        FBS_DIR / f'Nowcast_Detail_Use_after_redef_{year}_{vintage}.parquet'
    )
    before = pd.read_parquet(
        FBS_DIR / f'Nowcast_Detail_Use_before_redef_{year}_{vintage}.parquet'
    )

    sidecar = json.loads(
        (
            FBS_DIR / f'Nowcast_Detail_Use_before_redef_{year}_{vintage}_metadata.json'
        ).read_text()
    )
    stems = sidecar['tool_meta'].get('balanced_inputs', [])
    use_stem = next(stem for stem in stems if 'Use' in stem)
    balanced = pd.read_parquet(FBS_DIR / f'{use_stem}.parquet')

    seed = derive_initial_U_intermediate(year)
    fitted = fit_interior(year).interior

    million = 1e6
    stages = {
        'seed (Step 3)': seed,
        'interior fit': fitted,
        'balanced (RAS)': balanced * million,
        'producer prices': before,
        'after redefinitions': after,
    }

    records = []
    for _, cell in cells.iterrows():
        row, column = cell['commodity'], cell['industry']
        entry = {
            'commodity': row,
            'industry': column,
            'leverage': cell['leverage'],
            'source': cell['source'],
            'state': cell['state'],
        }
        for name, table in stages.items():
            entry[name] = (
                float(table.loc[row, column])
                if row in table.index and column in table.columns
                else np.nan
            )
        records.append(entry)
    return pd.DataFrame(records)


# ------------------------------------------------------------- 6. imports


def import_split(models: Models) -> pd.DataFrame:
    """Per-commodity import proportion of intermediate use, both models.

    ``r_i = sum_j Aimp[i,j] q_j / sum_j A[i,j] q_j`` -- the share of what the
    economy buys of commodity ``i`` that crossed a border, weighted the way the
    model actually uses it.
    """
    out = {}
    for name, (dom, imp) in {
        'nowcast': (models.nowcast_dom, models.nowcast_imp),
        'v03': (models.v03_dom, models.v03_imp),
    }.items():
        total = (dom + imp).mul(models.q, axis=1).sum(axis=1)
        imported = imp.mul(models.q, axis=1).sum(axis=1)
        out[f'{name}_intermediate_usd'] = total
        out[f'{name}_import_usd'] = imported
        out[f'{name}_import_share'] = imported / total.replace(0, np.nan)
    return pd.DataFrame(out)


def observed_imports(year: int = YEAR) -> pd.Series[float]:
    """Observed commodity imports for *year*, from the Trade_Imports FBS.

    Census ``GEN_CIF_YR`` goods plus the BEA international-services bridge plus
    EIA electricity -- the same extract the nowcast's own import control is built
    from.

    ⚠️ **Partly circular for the nowcast side and the caption must say so.**  Our
    import matrix is conditioned on this series, so agreement is close to
    tautological for us.  The asymmetry is the point: v0.3's import intensities
    come from the 2017 published import matrix scaled forward and are free to
    disagree with what actually crossed the border in 2024 -- this measures by
    how much.
    """
    from bedrock.transform.flowbysector import getFlowBySector  # noqa: PLC0415

    frame = pd.DataFrame(getFlowBySector(f'Trade_Imports_{year}'))
    return frame.groupby('SectorProducedBy')['FlowAmount'].sum()


# ------------------------------------------------------------------ figures


def _save(figure: Figure, name: str) -> Path:
    import matplotlib.pyplot as plt  # noqa: PLC0415

    path = OUT_DIR / name
    figure.savefig(path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close(figure)
    print(f'  wrote {path.name}')
    return path


def figure_reconstruction(scores: pd.DataFrame) -> Path:
    """How much cell-level information each 2024 matrix holds beyond 2017."""
    import matplotlib.pyplot as plt  # noqa: PLC0415

    figure, axes = plt.subplots(figsize=(9.0, 5.2))
    order = ['v0.3 2024', 'nowcast 2024']
    colors = [V03_COLOR, NOWCAST_COLOR]
    residual = scores['rms_residual'].astype(float)
    values = [float(residual[k]) for k in order]
    floor = float(residual['nowcast 2017 (control)'])
    headroom = max(values)

    bars = axes.bar(order, values, color=colors, width=0.5, zorder=3)
    axes.set_ylim(0, headroom * 1.24)
    axes.axhline(floor, color='#444444', linestyle='--', linewidth=1.6, zorder=4)
    axes.text(
        0.5,
        floor + headroom * 0.035,
        f'pipeline noise floor  {floor:.3f}\nthe nowcast rebuilding its own 2017 benchmark',
        ha='center',
        va='bottom',
        fontsize=10.5,
        color='#444444',
    )
    for bar, value in zip(bars, values, strict=True):
        axes.annotate(
            f'{value:.3f}\n{value / floor:.1f}x the floor',
            xy=(bar.get_x() + bar.get_width() / 2, value),
            xytext=(0, 8),
            textcoords='offset points',
            ha='center',
            fontsize=12.5,
        )
    axes.set_ylabel('cell-level information left over\n(weighted rms of log ratio)')
    axes.set_title(
        'What the 2024 matrix knows that the 2017 benchmark did not',
        fontsize=15,
        pad=12,
    )
    axes.text(
        0.5,
        -0.16,
        'Residual after fitting  A$_{2024}$ = A$_{2017}$ × (summary block factor) '
        '× (commodity price factor).\n'
        'v0.3 sits at the floor: its detail cells carry no 2024 observation.',
        transform=axes.transAxes,
        ha='center',
        va='top',
        fontsize=11,
        color='#4b5563',
    )
    axes.grid(axis='y', alpha=0.25, zorder=0)
    axes.set_axisbelow(True)
    return _save(figure, 'a_reconstruction_residual_2024.png')


def figure_n_difference(frame: pd.DataFrame) -> Path:
    """Per-sector N % difference, nowcast against v0.3, A-isolated."""
    import matplotlib.pyplot as plt  # noqa: PLC0415

    from bedrock.utils.validation.analysis.ef_hist_panels import (  # noqa: PLC0415
        draw_per_sector_pct_hist_panel,
    )

    figure, axes = plt.subplots(figsize=(11.0, 6.0))
    draw_per_sector_pct_hist_panel(
        axes,
        frame['pct_diff'].to_numpy(float),
        title='Total emission factor N: 2024 nowcast vs Cornerstone v0.3',
        color=NOWCAST_COLOR,
    )
    axes.set_xlabel('% difference in N  (nowcast − v0.3)')
    axes.text(
        0.5,
        -0.19,
        f'{len(frame)} significant sectors.  B held fixed at the v0.3 snapshot, '
        'so only the A-matrix method moves.',
        transform=axes.transAxes,
        ha='center',
        va='top',
        fontsize=11,
        color='#4b5563',
    )
    return _save(figure, 'a_n_pct_diff_significant_2024.png')


def figure_provenance(joined: pd.DataFrame, table_share: pd.Series[float]) -> Path:
    """Leverage-weighted provenance, against the table's own dollar share."""
    import matplotlib.pyplot as plt  # noqa: PLC0415
    from matplotlib.patches import Patch  # noqa: PLC0415

    by_state = joined.groupby('state')['leverage'].sum()
    by_state = by_state / by_state.sum()

    bars = {
        'v0.3 2024\ndetail cells': {'primary': 0.0, 'allocated': 0.0, 'carried': 1.0},
        'nowcast 2024\nwhole Use table\n(by dollars)': {
            state: float(table_share.get(state, 0.0)) for state in STATE_ORDER
        },
        'nowcast 2024\nweighted by leverage\non significant N': {
            state: float(by_state.get(state, 0.0)) for state in STATE_ORDER
        },
    }

    figure, axes = plt.subplots(figsize=(10.5, 6.0))
    labels = list(bars)
    bottoms = np.zeros(len(labels))
    for state in STATE_ORDER:
        values = np.array([bars[label][state] for label in labels])
        axes.bar(
            labels,
            values,
            bottom=bottoms,
            color=STATE_COLORS[state],
            width=0.55,
            zorder=3,
            # A 2px surface gap between stacked segments.
            edgecolor='white',
            linewidth=2,
        )
        for index, value in enumerate(values):
            if value > 0.035:
                axes.annotate(
                    f'{value:.0%}',
                    xy=(index, bottoms[index] + value / 2),
                    ha='center',
                    va='center',
                    fontsize=12,
                    color='white' if state == 'primary' else '#1f2937',
                )
        bottoms += values

    observed = 1.0 - bars[labels[2]]['carried']
    axes.annotate(
        f'{observed:.0%} of the leverage\ncarries an observation\nmade since 2017',
        xy=(2.0, observed),
        xytext=(2.0, 0.80),
        ha='center',
        fontsize=11.5,
        color='#1f2937',
        arrowprops={'arrowstyle': '->', 'color': '#4b5563', 'linewidth': 1.3},
    )
    axes.set_ylim(0, 1.0)
    axes.set_ylabel('share')
    axes.yaxis.set_major_formatter(lambda v, _: f'{v:.0%}')
    axes.set_title('Where the coefficients behind N come from', fontsize=15, pad=12)
    axes.legend(
        handles=[
            Patch(
                facecolor=STATE_COLORS['primary'],
                label='primary — the datum is the cell',
            ),
            Patch(
                facecolor=STATE_COLORS['allocated'],
                label='allocated — datum shared across cells',
            ),
            Patch(
                facecolor=STATE_COLORS['carried'],
                label='carried — 2017 structure, no annual source',
            ),
        ],
        loc='upper center',
        bbox_to_anchor=(0.5, -0.20),
        ncol=3,
        frameon=False,
        fontsize=10.5,
    )
    axes.grid(axis='y', alpha=0.25, zorder=0)
    axes.set_axisbelow(True)
    return _save(figure, 'a_provenance_of_significant_n_2024.png')


def figure_sources(joined: pd.DataFrame) -> Path:
    """Which survey stands behind the leverage, by name."""
    import matplotlib.pyplot as plt  # noqa: PLC0415

    by_source = joined.groupby('source')['leverage'].sum()
    by_source = (by_source / by_source.sum()).drop(
        labels=[s for s in ('carried', 'unmapped') if s in by_source.index]
    )
    by_source = by_source.sort_values()

    figure, axes = plt.subplots(figsize=(9.5, 4.8))
    axes.barh(
        [SOURCE_LABELS.get(s, s) for s in by_source.index],
        by_source.to_numpy() * 100,
        color=[SOURCE_COLORS.get(s, '#888888') for s in by_source.index],
        height=0.6,
        zorder=3,
    )
    for index, value in enumerate(by_source.to_numpy() * 100):
        axes.annotate(
            f'{value:.1f}%',
            xy=(value, index),
            xytext=(5, 0),
            textcoords='offset points',
            va='center',
            fontsize=12,
        )
    axes.set_xlabel('% of the leverage on the significant sectors’ N')
    axes.set_title('Which survey stands behind the coefficients', fontsize=15, pad=12)
    axes.set_xlim(0, by_source.max() * 118)
    axes.grid(axis='x', alpha=0.25, zorder=0)
    axes.set_axisbelow(True)
    return _save(figure, 'a_leverage_by_source_2024.png')


def import_composition(
    splits: pd.DataFrame, observed: pd.Series[float]
) -> pd.DataFrame:
    """Each commodity's share of total imports: both models against observed trade.

    ⚠️ A *composition*, not a level, and deliberately so.  The trade series counts
    everything that crossed the border, including what went straight to final
    demand, while ``Aimp`` covers only intermediate use -- so the two are not
    comparable as levels, nor as a ratio to intermediate use.  Their **shares of
    the total** are comparable, and they answer the question the domestic/import
    split of ``A`` has to get right: which commodities are the imported ones.
    """
    frame = splits.join(observed.rename('observed_usd'), how='inner')
    frame = frame[frame['observed_usd'] > 0].copy()
    frame['observed_share'] = frame['observed_usd'] / frame['observed_usd'].sum()
    for name in ('nowcast', 'v03'):
        column = f'{name}_import_usd'
        frame[f'{name}_share'] = frame[column] / frame[column].sum()
    return frame


def import_agreement(composition: pd.DataFrame) -> pd.DataFrame:
    """How closely each model's import composition tracks the observed one."""
    rows = {}
    for name in ('nowcast', 'v03'):
        model = composition[f'{name}_share']
        rows[name] = {
            # Spearman by hand: Pearson on ranks.  scipy is not a dependency
            # here and pandas' method='spearman' reaches for it.
            'spearman': float(model.rank().corr(composition['observed_share'].rank())),
            # Half the L1 distance between two compositions -- the share of all
            # imports the model puts on the wrong commodity.
            'misplaced_share': float(
                (model - composition['observed_share']).abs().sum() / 2
            ),
        }
    return pd.DataFrame(rows).T


def figure_import_split(splits: pd.DataFrame, observed: pd.Series[float]) -> Path:
    """Import composition against observed trade, both models on one axis."""
    import matplotlib.pyplot as plt  # noqa: PLC0415

    composition = import_composition(splits, observed)
    agreement = import_agreement(composition)

    figure, axes = plt.subplots(figsize=(7.8, 7.2))
    floor = 1e-6
    for name, color, label in (
        ('v03', V03_COLOR, 'Cornerstone v0.3'),
        ('nowcast', NOWCAST_COLOR, '2024 nowcast'),
    ):
        axes.scatter(
            composition['observed_share'].clip(lower=floor) * 100,
            composition[f'{name}_share'].clip(lower=floor) * 100,
            s=26,
            alpha=0.72,
            color=color,
            label=(
                f'{label} — misplaces '
                f'{agreement.loc[name, "misplaced_share"]:.0%} of imports'
            ),
            edgecolor='white',
            linewidth=0.6,
            zorder=3,
        )
    axes.plot(
        [floor * 100, 100],
        [floor * 100, 100],
        color='#6b7280',
        linestyle='--',
        linewidth=1.3,
        zorder=2,
    )
    axes.set_xscale('log')
    axes.set_yscale('log')
    axes.set_xlabel('observed 2024 imports, % of all imports')
    axes.set_ylabel('model imports in A, % of all imports')
    axes.set_title('Which commodities are the imported ones', fontsize=15, pad=12)
    axes.legend(frameon=False, fontsize=10.5, loc='upper left')
    axes.text(
        0.5,
        -0.13,
        'Composition, not level: the trade series includes imports going straight to\n'
        'final demand, so only shares are comparable.  The nowcast import matrix is\n'
        'conditioned on this same series, so its fit is partly built in; v0.3 carries\n'
        'the 2017 import matrix forward and is free to disagree.',
        transform=axes.transAxes,
        ha='center',
        va='top',
        fontsize=10,
        color='#4b5563',
    )
    axes.grid(alpha=0.22, zorder=0, which='both')
    axes.set_axisbelow(True)
    return _save(figure, 'a_import_composition_vs_trade_2024.png')


#: The build stages a Use cell passes through, seed to model input, in order.
LADDER_STAGES = (
    'seed (Step 3)',
    'interior fit',
    'balanced (RAS)',
    'producer prices',
    'after redefinitions',
)

#: What each step between them is.  ⚠️ Only two of the four are revisions of the
#: data: the interior fit imposes the observed output control, and the RAS
#: balances.  ``producer prices`` is a valuation restatement (purchaser to
#: producer) and carries no new information about the transaction.
LADDER_STEPS = (
    ('seed → interior fit', 0, 1, 'observed output control'),
    ('interior fit → RAS', 1, 2, 'balancing'),
    ('RAS → producer prices', 2, 3, 'valuation restatement'),
    ('producer prices → after redef', 3, 4, 'redefinitions'),
)


def ladder_steps(ladder: pd.DataFrame) -> pd.DataFrame:
    """Leverage-weighted share of the seed-to-model move contributed by each step."""
    full = ladder.dropna(subset=list(LADDER_STAGES))
    moves = pd.DataFrame(
        {
            name: (full[LADDER_STAGES[b]] - full[LADDER_STAGES[a]]).abs()
            for name, a, b, _ in LADDER_STEPS
        }
    )
    shares = moves.div(moves.sum(axis=1).replace(0, np.nan), axis=0)
    weight = full['leverage']
    out = pd.DataFrame(
        {
            'share_of_move': shares.mul(weight, axis=0).sum() / weight.sum(),
            'kind': [kind for _, _, _, kind in LADDER_STEPS],
        }
    )
    signed = pd.DataFrame(
        {
            name: (full[LADDER_STAGES[b]] - full[LADDER_STAGES[a]])
            / full[LADDER_STAGES[a]]
            for name, a, b, _ in LADDER_STEPS
        }
    )
    out['weighted_mean_pct'] = 100 * signed.mul(weight, axis=0).sum() / weight.sum()
    out['median_abs_pct'] = 100 * signed.abs().median()
    out['cells'] = len(full)
    return out


def figure_ladder(ladder: pd.DataFrame) -> Path:
    """What actually moves a coefficient between the survey and the model."""
    import matplotlib.pyplot as plt  # noqa: PLC0415
    from matplotlib.patches import Patch  # noqa: PLC0415

    steps = ladder_steps(ladder)
    # Two kinds of step, and the distinction is the point: an estimate that is
    # revised, against a number that is merely restated in other prices.
    kind_color = {
        'observed output control': '#2f6fb0',
        'balancing': '#e76f51',
        'valuation restatement': CARRIED_COLOR,
        'redefinitions': '#8f6fc4',
    }
    figure, axes = plt.subplots(figsize=(10.0, 4.9))
    names = list(steps.index)
    values = steps['share_of_move'].to_numpy(dtype=float) * 100.0
    axes.barh(
        names,
        values,
        color=[kind_color[k] for k in steps['kind']],
        height=0.58,
        zorder=3,
    )
    for index, (value, median) in enumerate(
        zip(values, steps['median_abs_pct'], strict=True)
    ):
        axes.annotate(
            f'{value:.0f}%   (median cell moves {median:.1f}%)',
            xy=(value, index),
            xytext=(6, 0),
            textcoords='offset points',
            va='center',
            fontsize=11,
        )
    axes.invert_yaxis()
    axes.set_xlim(0, max(values) * 1.75)
    axes.set_xlabel('share of the total seed → model movement')
    axes.xaxis.set_major_formatter(lambda v, _: f'{v:.0f}%')
    axes.set_title(
        'What moves a coefficient between the survey and the model',
        fontsize=15,
        pad=12,
    )
    axes.legend(
        handles=[Patch(facecolor=c, label=k) for k, c in kind_color.items()],
        loc='lower right',
        frameon=False,
        fontsize=10,
    )
    axes.text(
        0.0,
        -0.30,
        f'Top {int(steps["cells"].iloc[0])} cells by leverage on the significant sectors’ N.  '
        'Balancing and redefinitions together move them ~6%; '
        'the rest is the observed output control and a purchaser→producer price '
        'restatement, which revises nothing.',
        transform=axes.transAxes,
        ha='left',
        va='top',
        fontsize=10.5,
        color='#4b5563',
    )
    axes.grid(axis='x', alpha=0.25, zorder=0)
    axes.set_axisbelow(True)
    return _save(figure, 'a_stage_ladder_2024.png')


# ------------------------------------------------------- sector narratives


#: Below this, an industry's intermediate inputs are too small a share of its
#: output to be believed, and its whole column is suspect rather than newly
#: measured.  ⚠️ Issue #850: the ``GO - VAPRO`` control drifts toward zero across
#: the span for a growing set of industries and goes negative for six of them at
#: 2024, so a large fall in N for one of these is the defect, not better data.
IMPLAUSIBLE_INTERMEDIATE_SHARE = 0.20


#: Observed share of a sector's input structure below which its move in ``N`` is
#: not an observation story, whatever the size of the move.
#: ⚠️ Ranking on ``observed x |N move|`` alone puts ``334111`` first at +180%,
#: and its three largest inputs are all *carried* cells whose 2017 coefficients
#: were near zero -- a +5,000% relative move on a rounding error.  Gate on
#: evidence first, then rank by size.
MIN_OBSERVED_INPUT_SHARE = 0.60


def input_provenance_shares(
    models: Models,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Per ``A`` cell, the share of its value that a survey observed.

    ``A[i, c] = sum_j Unorm[i, j] Vnorm[j, c]``, so the provenance of an input
    coefficient is the provenance of the Use cells behind it, weighted the way
    the Make matrix combines them.  Masking ``Unorm`` before the product gives
    the observed part of each coefficient in one matrix multiply.
    """
    from bedrock.analysis.nowcasting import seed_coverage  # noqa: PLC0415

    Unorm, Vnorm = influence.bea_use_pieces()
    pedigree = seed_coverage.pedigree_cells(models.year)
    state = np.where(
        pedigree['source'].eq('carried'),
        'carried',
        np.where(pedigree['n'] <= 1.0, 'primary', 'allocated'),
    )
    pedigree = pedigree.assign(state=state)

    def mask(states: set[str]) -> pd.DataFrame:
        flags = pd.DataFrame(0.0, index=Unorm.index, columns=Unorm.columns)
        subset = pedigree[pedigree['state'].isin(states)]
        rows = subset['commodity'].to_numpy()
        columns = subset['industry'].to_numpy()
        keep = np.isin(rows, flags.index.to_numpy()) & np.isin(
            columns, flags.columns.to_numpy()
        )
        flags.values[
            flags.index.get_indexer(rows[keep]),
            flags.columns.get_indexer(columns[keep]),
        ] = 1.0
        return flags

    total = Unorm @ Vnorm
    denominator = total.replace(0.0, np.nan)
    observed = ((Unorm * mask({'primary', 'allocated'})) @ Vnorm) / denominator
    primary = ((Unorm * mask({'primary'})) @ Vnorm) / denominator
    return observed.fillna(0.0), primary.fillna(0.0)


def sector_ranking(models: Models) -> pd.DataFrame:
    """Every target sector: how observed its inputs are, and how far N moved.

    ``intermediate_share`` is the sanity column -- see
    :data:`IMPLAUSIBLE_INTERMEDIATE_SHARE`.  Sort by ``score`` for the sectors
    where an observation actually changed the answer, but read ``suspect``
    first.
    """
    observed, primary = input_provenance_shares(models)
    inputs = compute_input_contribution(A=models.nowcast, N=models.N)

    records = []
    for sector in models.targets:
        column = inputs[sector]
        indirect = float(column.sum())
        if indirect <= 0.0:
            continue
        share = float(models.nowcast[sector].sum())
        records.append(
            {
                'sector': sector,
                'N_nowcast': float(models.N[sector]),
                'N_v03': float(models.v03_N[sector]),
                'N_pct': 100.0
                * (float(models.N[sector]) - float(models.v03_N[sector]))
                / float(models.v03_N[sector]),
                'indirect_share_of_N': indirect / float(models.N[sector]),
                'inputs_observed': float(
                    (column * observed[sector].reindex(column.index).fillna(0.0)).sum()
                    / indirect
                ),
                'inputs_primary': float(
                    (column * primary[sector].reindex(column.index).fillna(0.0)).sum()
                    / indirect
                ),
                'intermediate_share': share,
                'suspect': share < IMPLAUSIBLE_INTERMEDIATE_SHARE,
            }
        )
    frame = pd.DataFrame(records).set_index('sector')
    frame['score'] = frame['inputs_observed'] * frame['N_pct'].abs()
    return frame.sort_values('score', ascending=False)


def sector_rows(models: Models, sector: str, top: int = 8) -> pd.DataFrame:
    """The inputs behind one sector's footprint, with both models and the source.

    ``contribution`` is the exact share of ``N_c - D_c`` the input carries, from
    :func:`~...a_influence.tier1_contribution`.  ``A_nowcast`` and ``A_v03`` are
    the two models' coefficients for the same cell, and ``A_2017`` the published
    benchmark both of them start from -- so the pair of ratios shows whether the
    two methods even agree on the direction the input moved since 2017.
    """
    from bedrock.analysis.nowcasting import seed_coverage  # noqa: PLC0415

    inputs = compute_input_contribution(A=models.nowcast, N=models.N)[sector]
    indirect = float(inputs.sum())

    # Plain dicts rather than a MultiIndex lookup: the provenance table is a
    # long frame and `.loc[(row, column)]` on it is both slower per hit and
    # awkward to type.
    pedigree = seed_coverage.pedigree_cells(models.year)
    keys = list(zip(pedigree['commodity'], pedigree['industry'], strict=True))
    sources = dict(zip(keys, pedigree['source'].astype(str), strict=True))
    fanouts: dict[tuple[str, str], float] = dict(
        zip(keys, pedigree['n'].to_numpy(dtype=float), strict=True)
    )

    now_column = models.nowcast[sector].astype(float)
    v03_column = models.v03[sector].astype(float)

    records = []
    ranked = inputs.sort_values(ascending=False).head(top)
    for raw_code, contribution in ranked.items():
        code = str(raw_code)
        now = float(now_column[code])
        old = float(v03_column[code])
        records.append(
            {
                'input': code,
                'contribution': float(contribution) / indirect,
                'A_nowcast': now,
                'A_v03': old,
                'A_pct': 100.0 * (now - old) / old if old else np.nan,
                'N_of_input': float(models.N[code]),
                'source': sources.get((code, sector), 'unmapped'),
                'fanout': fanouts.get((code, sector), float('nan')),
            }
        )
    return pd.DataFrame(records)


# --------------------------------------------------------------------- run


def main() -> int:
    parser = argparse.ArgumentParser(description='A-matrix evidence, 2024.')
    parser.add_argument('--year', type=int, default=YEAR)
    parser.add_argument(
        '--ladder',
        action='store_true',
        help='Also build the stage ladder (rebuilds the Step 3 seed and the '
        'interior fit; adds several minutes).',
    )
    parser.add_argument('--top', type=int, default=40, help='Cells in the ladder.')
    parser.add_argument(
        '--sectors',
        action='store_true',
        help='Also rank the target sectors and print the inputs behind the clearest ones.',
    )
    args = parser.parse_args()

    from bedrock.utils.validation.analysis.plotting import setup_mpl  # noqa: PLC0415

    setup_mpl(font_size=13)
    t0 = time.time()

    benchmark = published_benchmark_A()
    _tick(t0, f'published {BENCHMARK_YEAR} benchmark A {benchmark.shape}')

    models = Models(args.year)
    _tick(t0, f'models loaded; nowcast MUT vintage {models.vintage}')
    print(
        f'  targets: {len(models.targets)}   absent from the model axis: {models.absent}'
    )

    # --- 1. reconstruction ---------------------------------------------------
    from bedrock.analysis.nowcasting.results._ef_smoke_lib import (  # noqa: PLC0415
        aq_from_live_config,
    )

    control_aq, _ = aq_from_live_config(BENCHMARK_YEAR)
    control = control_aq.Adom + control_aq.Aimp
    scores = pd.DataFrame(
        {
            'v0.3 2024': reconstruction_residual(models.v03, benchmark),
            'nowcast 2024': reconstruction_residual(models.nowcast, benchmark),
            'nowcast 2017 (control)': reconstruction_residual(control, benchmark),
        }
    ).T
    scores.to_csv(OUT_DIR / 'a_reconstruction_scores.csv')
    print('\n=== 1. can the 2024 matrix be rebuilt from 2017 by rescaling? ===')
    print(scores.round(4).to_string())
    _tick(t0, 'reconstruction done')

    # The Models object left the nowcast config installed; the control run above
    # swapped it to 2017, so put the analysis year back before anything else
    # reads a config-dependent cache.
    models = Models(args.year)

    # --- 2. N ----------------------------------------------------------------
    comparison = pd.DataFrame(
        {'N_nowcast': models.N, 'N_v03': models.v03_N, 'D': models.D}
    ).loc[models.targets]
    comparison['pct_diff'] = (
        100.0 * (comparison['N_nowcast'] - comparison['N_v03']) / comparison['N_v03']
    )
    comparison.to_csv(OUT_DIR / 'a_n_significant_2024.csv')
    print('\n=== 2. N on the significant sectors (B held at v0.3) ===')
    print(
        f'  median |% diff| {comparison["pct_diff"].abs().median():.1f}   '
        f'p95 {comparison["pct_diff"].abs().quantile(0.95):.1f}   '
        f'share moving >10%: {(comparison["pct_diff"].abs() > 10).mean():.1%}'
    )

    # --- 3/4. leverage and provenance ---------------------------------------
    joined = leverage_with_provenance(models, args.year)
    joined.to_csv(OUT_DIR / 'a_leverage_provenance_2024.csv', index=False)
    by_state = joined.groupby('state')['leverage'].sum()
    by_state = by_state / by_state.sum()
    print('\n=== 3/4. leverage-weighted provenance ===')
    print((by_state * 100).round(1).to_string())
    print(f'  observed since 2017: {1 - by_state.get("carried", 0.0):.1%}')
    top = joined.head(25)
    print('\n  top 12 cells by leverage:')
    print(
        top.head(12)[['commodity', 'industry', 'share', 'source', 'n']]
        .round(5)
        .to_string(index=False)
    )
    _tick(t0, 'provenance join done')

    from bedrock.analysis.nowcasting import seed_coverage  # noqa: PLC0415

    pedigree = seed_coverage.pedigree_cells(args.year)
    pedigree['state'] = np.where(
        pedigree['source'] == 'carried',
        'carried',
        np.where(pedigree['n'] <= 1.0, 'primary', 'allocated'),
    )
    table_share = pedigree.groupby('state')['dollars'].sum()
    table_share = table_share / table_share.sum()

    # --- 6. imports ----------------------------------------------------------
    splits = import_split(models)
    splits.to_csv(OUT_DIR / 'a_import_split_2024.csv')
    weighted = joined.merge(
        splits[['nowcast_import_share', 'v03_import_share']],
        left_on='commodity',
        right_index=True,
        how='left',
    )
    print('\n=== 6. imports ===')
    for name in ('nowcast', 'v03'):
        share = float(
            (weighted['leverage'] * weighted[f'{name}_import_share'].fillna(0.0)).sum()
            / weighted['leverage'].sum()
        )
        print(f'  {name:8s} leverage-weighted import share of A: {share:.3%}')
    observed = observed_imports(args.year)
    agreement = import_agreement(import_composition(splits, observed))
    agreement.to_csv(OUT_DIR / 'a_import_agreement_2024.csv')
    print(agreement.round(3).to_string())
    _tick(t0, 'imports done')

    # --- figures -------------------------------------------------------------
    print('\n=== figures ===')
    figure_reconstruction(scores)
    figure_n_difference(comparison)
    figure_provenance(joined, table_share)
    figure_sources(joined)
    figure_import_split(splits, observed)

    # --- 5. ladder (optional) ------------------------------------------------
    if args.ladder:
        ladder = stage_ladder(models, joined.head(args.top), args.year)
        ladder.to_csv(OUT_DIR / 'a_stage_ladder_2024.csv', index=False)
        steps = ladder_steps(ladder)
        steps.to_csv(OUT_DIR / 'a_stage_ladder_steps_2024.csv')
        print('\n=== 5. stage ladder: what moves a coefficient ===')
        print(steps.round(2).to_string())
        figure_ladder(ladder)
        _tick(t0, 'ladder done')

    if args.sectors:
        ranking = sector_ranking(models)
        ranking.to_csv(OUT_DIR / 'a_sector_ranking_2024.csv')
        evidenced = ranking[
            ~ranking['suspect']
            & (ranking['inputs_observed'] >= MIN_OBSERVED_INPUT_SHARE)
        ].sort_values('N_pct', key=lambda c: c.abs(), ascending=False)
        print('\n=== 7. sectors where an observation changed the answer ===')
        print(
            f'  {int(ranking["suspect"].sum())} of {len(ranking)} targets held back by '
            f'the #850 intermediate-control defect; {len(evidenced)} clear that and the '
            f'{MIN_OBSERVED_INPUT_SHARE:.0%} observed-input floor'
        )
        print(
            evidenced.head(10)[
                ['N_pct', 'inputs_observed', 'inputs_primary', 'intermediate_share']
            ]
            .round(3)
            .to_string()
        )
        for sector in evidenced.head(4).index:
            print(f'\n  --- {sector} ---')
            print(sector_rows(models, sector).round(4).to_string(index=False))
        _tick(t0, 'sector narratives done')

    _tick(t0, 'complete')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
