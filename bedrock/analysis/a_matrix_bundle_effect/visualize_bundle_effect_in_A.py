"""Visualize how year-scaling distorts USA electricity input coefficients across
detail-level commodities, organized by summary bundle.

Compares bedrock's two derived Detail A matrices:
  * 2017 Detail A             — BEA 2017 benchmark, before scale+inflate
                                (``derive_cornerstone_Aq()``)
  * <model_base_year> Detail A — after scale+inflate to the target year, default 2023
                                (``derive_cornerstone_Aq_scaled()``)

For each detail commodity j it reads the electricity (221100) input coefficient
A[221100, j] in both matrices, and groups commodities by their parent BEA 2017
summary bundle J. Each year's summary-bundle anchor is the output-weighted mean
of that year's coefficient across the bundle's members — i.e. the bundle-aggregate
electricity coefficient implied by the detail A. When the active scaling method
reflects structure from the summary tables, detail coefficients get pulled toward
the post-scale anchor; the plots make that dispersion-to-anchor movement visible.

Renders two plot kinds (three PNGs):
  1. Per-bundle dispersion: 2017 detail vs post-scale detail for commodities within
     each summary bundle, against the 2017 and post-scale bundle anchors.
  2. 2017 detail vs post-scale scatter for all detail commodities.

Run from repo root:
    uv run python -m bedrock.analysis.a_matrix_bundle_effect.visualize_bundle_effect_in_A
"""

from __future__ import annotations

import logging
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_Aq,
    derive_cornerstone_Aq_scaled,
)
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.taxonomy.bea.v2017_commodity_summary import (
    USA_2017_SUMMARY_COMMODITY_DESC,
)
from bedrock.utils.taxonomy.bea_v2017_to_ceda_v7_helpers import (
    get_bea_v2017_summary_to_cornerstone_corresp_df,
)
from bedrock.utils.taxonomy.cornerstone.commodities import COMMODITY_DESC

warnings.filterwarnings('ignore')

logger = logging.getLogger(__name__)

ELEC = '221100'  # electricity input commodity row

_OUTPUT_DIR = Path(__file__).resolve().parent / 'output'
_COMMODITY_DESC: dict[str, str] = {str(k): str(v) for k, v in COMMODITY_DESC.items()}
_SUMMARY_DESC: dict[str, str] = {
    str(k): str(v) for k, v in USA_2017_SUMMARY_COMMODITY_DESC.items()
}


@dataclass
class DistortionData:
    """Per-detail-commodity electricity coefficients (2017 vs post-scale) and per-bundle anchors.

    Attributes:
        detail_df: one row per detail commodity j with columns ``j``, ``name``, ``J``
            (parent summary bundle), ``a_2017``, ``a_scaled``, ``q_2017`` (2017 output),
            and ``q_scaled`` (post-scale output).
        summary_anchor_2017: per-bundle output-weighted mean of ``a_2017`` (the bundle-aggregate
            electricity coefficient implied by the 2017 detail A).
        summary_anchor_scaled: same for ``a_scaled`` (implied by the post-scale detail A).
        scaled_year: the ``model_base_year`` the scaled matrix represents (e.g. 2023).
    """

    detail_df: pd.DataFrame
    summary_anchor_2017: dict[str, float]
    summary_anchor_scaled: dict[str, float]
    scaled_year: int


def load_distortion_data() -> DistortionData:
    """Load bedrock's 2017 and post-scale Detail A and compute electricity distortion per commodity.

    For each detail commodity j with parent summary bundle J:
      a_2017   = Adom_2017[221100, j]   (BEA 2017 benchmark, before scale+inflate)
      a_scaled = Adom_scaled[221100, j] (after scale+inflate to model_base_year)
    Each year's bundle anchor for J is the mean of that year's coefficient over the
    commodities mapped to J, weighted by that year's commodity output q.
    """
    cfg = get_usa_config()
    scaled_year = int(cfg.model_base_year)

    base = derive_cornerstone_Aq()
    adom_2017 = base.Adom
    q_2017 = base.scaled_q
    scaled = derive_cornerstone_Aq_scaled()
    adom_scaled = scaled.Adom
    q_scaled = scaled.scaled_q

    detail_to_summary = _detail_to_summary_map()

    records = []
    for j in adom_2017.columns:
        J = detail_to_summary.get(str(j))
        if J is None:
            continue
        records.append(
            {
                'j': str(j),
                'name': _commodity_name(str(j)),
                'J': J,
                'a_2017': float(cast(float, adom_2017.loc[ELEC, j])),
                'a_scaled': float(cast(float, adom_scaled.loc[ELEC, j])),
                'q_2017': float(q_2017.loc[j]) if j in q_2017.index else 0.0,
                'q_scaled': float(q_scaled.loc[j]) if j in q_scaled.index else 0.0,
            }
        )
    detail_df = pd.DataFrame(records)

    return DistortionData(
        detail_df=detail_df,
        summary_anchor_2017=_bundle_anchors(detail_df, 'a_2017', 'q_2017'),
        summary_anchor_scaled=_bundle_anchors(detail_df, 'a_scaled', 'q_scaled'),
        scaled_year=scaled_year,
    )


def plot_bundle_dispersion(
    data: DistortionData,
    out_path: Path,
    n_bundles: int | None = 8,
    min_bundle_size: int = 2,
) -> None:
    """Render per-bundle dispersion: 2017 detail vs post-scale markers per bundle row.

    Args:
        data: result of `load_distortion_data`.
        out_path: PNG output path.
        n_bundles: if not None, show only the top-N bundles by within-bundle 2017 spread.
                   If None, show all bundles with at least `min_bundle_size` detail members.
        min_bundle_size: drop bundles with fewer detail members than this. Pass 1 to include
                         singleton (1:1) summary->detail mappings.
    """
    df = data.detail_df
    scaled_year = data.scaled_year
    bundle_stats = (
        df.groupby('J')
        .agg(n=('j', 'count'), spread=('a_2017', lambda x: x.max() - x.min()))
        .reset_index()
    )
    bundle_stats = bundle_stats[bundle_stats.n >= min_bundle_size]
    bundle_stats = bundle_stats.sort_values(by='spread', ascending=False)
    if n_bundles is not None:
        bundle_stats = bundle_stats.head(n_bundles)

    bundles = list(bundle_stats.J)

    height_per_row = 0.85
    fig_height = max(7.0, height_per_row * len(bundles) + 2.5)
    fig, ax = plt.subplots(figsize=(13, fig_height))

    y_positions = np.arange(len(bundles))[::-1]
    DODGE = 0.13

    # alternating background bands so each bundle is visually a single row
    for i, y in enumerate(y_positions):
        if i % 2 == 0:
            ax.axhspan(y - 0.5, y + 0.5, color='#f0f4f8', alpha=0.7, zorder=0)

    a2017_label_used = False
    scaled_label_used = False
    anchor_2017_label_used = False
    anchor_scaled_label_used = False

    for y, J in zip(y_positions, bundles):
        anchor_2017 = data.summary_anchor_2017[J]
        anchor_scaled = data.summary_anchor_scaled[J]
        sub = df[df.J == J].sort_values(by='a_2017')

        # connector lines pairing each j's 2017 dot to its post-scale dot
        for _, r in sub.iterrows():
            ax.plot(
                [r.a_2017, r.a_scaled],
                [y + DODGE, y - DODGE],
                color='gray',
                alpha=0.25,
                linewidth=0.7,
                zorder=1,
            )

        # span connectors per row
        if len(sub) > 1:
            ax.plot(
                [sub.a_2017.min(), sub.a_2017.max()],
                [y + DODGE, y + DODGE],
                color='#4a90e2',
                alpha=0.3,
                linewidth=1.2,
                zorder=1,
            )
            ax.plot(
                [sub.a_scaled.min(), sub.a_scaled.max()],
                [y - DODGE, y - DODGE],
                color='#c0392b',
                alpha=0.3,
                linewidth=1.2,
                zorder=1,
            )

        # markers
        ax.scatter(
            sub.a_2017,
            [y + DODGE] * len(sub),
            s=110,
            color='#4a90e2',
            alpha=0.65,
            edgecolor='none',
            zorder=2,
            label='2017 Detail A[221100, j]' if not a2017_label_used else None,
        )
        a2017_label_used = True
        ax.scatter(
            sub.a_scaled,
            [y - DODGE] * len(sub),
            s=110,
            color='#c0392b',
            alpha=0.65,
            edgecolor='none',
            zorder=2,
            label=(
                f'{scaled_year} Detail A[221100, j]  (post scale+inflate)'
                if not scaled_label_used
                else None
            ),
        )
        scaled_label_used = True
        ax.scatter(
            anchor_2017,
            y + DODGE,
            marker='D',
            s=160,
            facecolor='white',
            edgecolor='#1f3a93',
            linewidth=1.4,
            zorder=4,
            label=(
                '2017 summary-bundle anchor  (output-weighted mean)'
                if not anchor_2017_label_used
                else None
            ),
        )
        anchor_2017_label_used = True
        ax.scatter(
            anchor_scaled,
            y - DODGE,
            marker='D',
            s=160,
            color='#e67e22',
            edgecolor='black',
            linewidth=0.8,
            zorder=4,
            label=(
                f'{scaled_year} summary-bundle anchor  (output-weighted mean)'
                if not anchor_scaled_label_used
                else None
            ),
        )
        anchor_scaled_label_used = True

        # label the rightmost (max) 2017 dot per bundle (only when there's a non-trivial spread)
        if len(sub) > 1:
            rmax = sub.loc[sub['a_2017'].idxmax()]
            ax.annotate(
                rmax['name'][:32],
                xy=(rmax.a_2017, y + DODGE),
                xytext=(rmax.a_2017, y + 0.42),
                fontsize=9,
                color='#1f3a93',
                fontweight='bold',
                ha='center',
                arrowprops=dict(
                    arrowstyle='-', color='#1f3a93', linewidth=0.6, alpha=0.7
                ),
            )

    ax.set_yticks(y_positions)
    ax.set_yticklabels([_bundle_label(J) for J in bundles], fontsize=10)
    ax.set_xlabel(
        r'electricity coefficient (\$ of electricity input per \$ of buyer output)',
        fontsize=11,
    )
    title_suffix = (
        f'Top {len(bundles)} bundles ranked by within-bundle spread of 2017 electricity coefficient'
        if n_bundles is not None
        else f'All {len(bundles)} bundles, ranked by within-bundle spread'
    )
    ax.set_title(
        f'Detail A[221100, j] moves from 2017 toward the {scaled_year} summary-bundle anchor under scale+inflate\n'
        f'{title_suffix}',
        fontsize=11,
        pad=42,
    )
    ax.legend(loc='lower right', fontsize=9, framealpha=0.95)
    xmax = max(0.18, df.a_2017.max() * 1.08)
    ax.set_xlim(-0.005, xmax)
    ax.set_ylim(min(y_positions) - 0.5, max(y_positions) + 0.95)
    ax.grid(axis='x', alpha=0.3)
    ax.axvline(0, color='gray', alpha=0.5, linewidth=0.5)

    plt.tight_layout()
    plt.savefig(out_path, dpi=140, bbox_inches='tight')
    plt.close(fig)
    logger.info('Wrote %s', out_path.resolve())


def plot_scatter(data: DistortionData, out_path: Path, top_n_color: int = 8) -> None:
    """2017 detail vs post-scale scatter for all detail commodities.

    The top `top_n_color` bundles by within-bundle 2017 spread are color-coded; the rest
    are rendered as small gray dots.
    """
    df = data.detail_df
    scaled_year = data.scaled_year
    bundle_stats = (
        df.groupby('J')
        .agg(spread=('a_2017', lambda x: x.max() - x.min()))
        .reset_index()
        .sort_values(by='spread', ascending=False)
        .head(top_n_color)
    )
    top_bundles = list(bundle_stats.J)
    color_set = plt.colormaps['tab10'](np.linspace(0, 1, len(top_bundles)))
    color_map = {J: color_set[i] for i, J in enumerate(top_bundles)}

    fig, ax = plt.subplots(figsize=(10, 10))
    for J in df.J.unique():
        sub = df[df.J == J]
        if J in color_map:
            ax.scatter(
                sub.a_2017,
                sub.a_scaled,
                s=60,
                color=color_map[J],
                alpha=0.7,
                edgecolor='none',
                label=_bundle_label(J),
            )
        else:
            ax.scatter(
                sub.a_2017,
                sub.a_scaled,
                s=20,
                color='lightgray',
                alpha=0.5,
                edgecolor='none',
            )

    lim = max(df.a_2017.max(), df.a_scaled.max()) + 0.01
    ax.plot([0, lim], [0, lim], 'k--', alpha=0.5, linewidth=1, label='y=x (no change)')

    outliers = {
        '325120': 'Industrial gases',
        '331313': 'Aluminum smelting',
        '327310': 'Cement',
        '325180': 'Inorg. chemicals',
        '325310': 'Fertilizer',
        '327200': 'Glass',
        '212230': 'Cu/Ni/Pb/Zn mining',
        '447000': 'Gasoline stations',
        '327320': 'Ready-mix concrete',
        '221200': 'Natural gas dist.',
    }
    for j, lbl in outliers.items():
        sub = df[df.j == j]
        if sub.empty:
            continue
        x = float(sub['a_2017'].iloc[0])
        y = float(sub['a_scaled'].iloc[0])
        ax.annotate(
            lbl,
            xy=(x, y),
            xytext=(x + 0.004, y - 0.003),
            fontsize=9,
            color='#222',
            arrowprops=dict(arrowstyle='-', color='#222', linewidth=0.5, alpha=0.7),
        )

    ax.set_xlabel(
        r'2017 Detail A[221100, j]   (\$ of electricity input per \$ of buyer output)',
        fontsize=10,
    )
    ax.set_ylabel(
        rf'{scaled_year} Detail A[221100, j]   (\$ of electricity input per \$ of buyer output)',
        fontsize=10,
    )
    ax.set_title(
        f'2017 vs {scaled_year} Detail A[221100, j] for all detail commodities\n'
        'Below y=x: lowered by scale+inflate  ·  Above: raised',
        fontsize=11,
    )
    ax.legend(loc='upper left', fontsize=8, framealpha=0.95, ncol=2)
    ax.set_xlim(-0.005, lim)
    ax.set_ylim(-0.005, lim)
    ax.grid(alpha=0.3)
    ax.set_aspect('equal')

    plt.tight_layout()
    plt.savefig(out_path, dpi=140, bbox_inches='tight')
    plt.close(fig)
    logger.info('Wrote %s', out_path.resolve())


def main() -> None:
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    data = load_distortion_data()
    plot_bundle_dispersion(
        data,
        _OUTPUT_DIR / 'electricity-bundle-effect-top8.png',
        n_bundles=8,
    )
    plot_bundle_dispersion(
        data,
        _OUTPUT_DIR / 'electricity-bundle-effect-all.png',
        n_bundles=None,
        min_bundle_size=1,
    )
    plot_scatter(data, _OUTPUT_DIR / 'electricity-bundle-effect-scatter.png')
    logger.info('Wrote plots to %s', _OUTPUT_DIR.resolve())


def _detail_to_summary_map() -> dict[str, str]:
    """Map each cornerstone detail commodity code to its parent BEA 2017 summary code."""
    corresp = get_bea_v2017_summary_to_cornerstone_corresp_df()
    out: dict[str, str] = {}
    for d in corresp.index:
        parents = corresp.columns[corresp.loc[d] == 1].tolist()
        if parents:
            out[str(d)] = str(parents[0])
    return out


def _bundle_anchors(
    detail_df: pd.DataFrame, value_col: str, weight_col: str
) -> dict[str, float]:
    """Output-weighted mean of ``value_col`` per summary bundle (the bundle-aggregate coefficient)."""
    anchors: dict[str, float] = {}
    for J, sub in detail_df.groupby('J'):
        w = sub[weight_col].to_numpy(dtype=float)
        a = sub[value_col].to_numpy(dtype=float)
        anchors[str(J)] = (
            float(np.average(a, weights=w)) if w.sum() > 0 else float(a.mean())
        )
    return anchors


def _commodity_name(code: str) -> str:
    """Cornerstone commodity description, falling back to the code itself."""
    return _COMMODITY_DESC.get(code) or code


def _bundle_label(code: str) -> str:
    """``CODE description`` for a summary bundle, falling back to the bare code."""
    desc = _SUMMARY_DESC.get(code, '')
    return f'{code} {desc}'.strip() if desc else code


if __name__ == '__main__':
    main()
