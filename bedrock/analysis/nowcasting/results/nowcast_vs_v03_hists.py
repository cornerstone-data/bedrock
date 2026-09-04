"""N/D % histograms: nowcast compare-year model vs the v0.3 release.

Nowcast side: ``NOWCAST_COMPARE_SNAPSHOT`` when a CI snapshot of the
``2025_usa_cornerstone_v0_4_nowcast_<year>`` config has been cut, otherwise
B + A derived live from that YAML. The v0.3 EFs are in 2024$, so the default
2024 compare year needs no inflation; other years are inflated to the v0.3
dollar year first. Writes CSV + PNGs next to this file (gitignored).

::

    uv run python -m bedrock.analysis.nowcasting.results.nowcast_vs_v03_hists
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from bedrock.analysis.nowcasting.results._ef_smoke_lib import (
    NOWCAST_COMPARE_SNAPSHOT,
    NOWCAST_COMPARE_YEAR,
    V03_SNAPSHOT,
    efs_from_live_config,
    efs_from_snapshot,
    perc_diff,
)
from bedrock.utils.snapshots.releases import ef_dollar_year_for_snapshot
from bedrock.utils.validation.analysis.ef_hist_panels import (
    draw_per_sector_pct_hist_panel,
)
from bedrock.utils.validation.analysis.plotting import setup_mpl
from bedrock.utils.validation.diagnostics_helpers import (
    inflation_adjust_ef_denom_to_new_base_year,
)

OUT_DIR = Path(__file__).resolve().parent


def main(
    *,
    year: int = NOWCAST_COMPARE_YEAR,
    snapshot: str | None = NOWCAST_COMPARE_SNAPSHOT,
) -> None:
    nc = efs_from_snapshot(snapshot) if snapshot else efs_from_live_config(year)
    v03 = efs_from_snapshot(V03_SNAPSHOT)
    v03_dollar_year = ef_dollar_year_for_snapshot(V03_SNAPSHOT)

    N_nc, D_nc = nc.N, nc.D
    label = f'nowcast-{year}'
    if year != v03_dollar_year:
        N_nc = inflation_adjust_ef_denom_to_new_base_year(
            N_nc, new_base_year=v03_dollar_year, old_base_year=year
        )
        D_nc = inflation_adjust_ef_denom_to_new_base_year(
            D_nc, new_base_year=v03_dollar_year, old_base_year=year
        )
        label += f' (inflated to {v03_dollar_year}$)'
    source = (
        f'snapshot {snapshot[:8]}' if snapshot else f'live config, MUT {nc.mut_vintage}'
    )
    print(f'{label}: {source}; v0.3 = snapshot {V03_SNAPSHOT[:8]} ({v03_dollar_year}$)')

    idx = N_nc.index.intersection(v03.N.index)
    n_pct = perc_diff(N_nc.reindex(idx), v03.N.reindex(idx))
    d_pct = perc_diff(D_nc.reindex(idx), v03.D.reindex(idx))

    stem = f'nowcast_{year}_vs_v03'
    pd.DataFrame(
        {
            'sector': idx,
            'N_nowcast': N_nc.reindex(idx).to_numpy(),
            'N_v03': v03.N.reindex(idx).to_numpy(),
            'N_perc_diff': n_pct.to_numpy(),
            'D_nowcast': D_nc.reindex(idx).to_numpy(),
            'D_v03': v03.D.reindex(idx).to_numpy(),
            'D_perc_diff': d_pct.to_numpy(),
            'dollar_year': v03_dollar_year,
        }
    ).to_csv(OUT_DIR / f'n_d_{stem}.csv', index=False)

    setup_mpl()
    for kind, pct, color in (
        ('N', n_pct, '#ff7f0e'),
        ('D', d_pct, '#1f77b4'),
    ):
        fig, ax = plt.subplots(figsize=(8.0, 5.2))
        draw_per_sector_pct_hist_panel(
            ax,
            (pct / 100.0).to_numpy(),
            title=f'{kind} % diff: {label} vs v0.3',
            color=color,
        )
        fig.tight_layout()
        out = OUT_DIR / f'{kind.lower()}_perc_diff_hist_{stem}.png'
        fig.savefig(out, dpi=150, bbox_inches='tight')
        plt.close(fig)
        print(
            f'{kind}: median={pct.median():+.2f}%  '
            f'p95(|.|)= {pct.abs().quantile(0.95):.2f}%  -> {out.name}'
        )


if __name__ == '__main__':
    main()
