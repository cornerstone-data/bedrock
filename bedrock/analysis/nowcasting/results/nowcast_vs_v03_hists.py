"""N/D % histograms: nowcast-2023 snapshot (inflated to 2024$) vs v0.3.

Uses the CI snapshot from PR #831 (``NOWCAST_2023_SNAPSHOT``) and
``releases.v0_3_0``. Writes CSV + PNGs next to this file (gitignored).

::

    uv run python -m bedrock.analysis.nowcasting.results.nowcast_vs_v03_hists
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from bedrock.analysis.nowcasting.results._ef_smoke_lib import (
    NOWCAST_2023_SNAPSHOT,
    V03_SNAPSHOT,
    efs_from_snapshot,
    perc_diff,
)
from bedrock.utils.validation.analysis.ef_hist_panels import (
    draw_per_sector_pct_hist_panel,
)
from bedrock.utils.validation.analysis.plotting import setup_mpl
from bedrock.utils.validation.diagnostics_helpers import (
    inflation_adjust_ef_denom_to_new_base_year,
)

OUT_DIR = Path(__file__).resolve().parent


def main() -> None:
    D_nc, N_nc = efs_from_snapshot(NOWCAST_2023_SNAPSHOT)
    D_v03, N_v03 = efs_from_snapshot(V03_SNAPSHOT)

    N_nc_2024 = inflation_adjust_ef_denom_to_new_base_year(
        N_nc, new_base_year=2024, old_base_year=2023
    )
    D_nc_2024 = inflation_adjust_ef_denom_to_new_base_year(
        D_nc, new_base_year=2024, old_base_year=2023
    )

    idx = N_nc_2024.index.intersection(N_v03.index)
    n_pct = perc_diff(N_nc_2024.reindex(idx), N_v03.reindex(idx))
    d_pct = perc_diff(D_nc_2024.reindex(idx), D_v03.reindex(idx))

    pd.DataFrame(
        {
            'sector': idx,
            'N_new_2024': N_nc_2024.reindex(idx).to_numpy(),
            'N_old_v03': N_v03.reindex(idx).to_numpy(),
            'N_perc_diff': n_pct.to_numpy(),
            'D_new_2024': D_nc_2024.reindex(idx).to_numpy(),
            'D_old_v03': D_v03.reindex(idx).to_numpy(),
            'D_perc_diff': d_pct.to_numpy(),
        }
    ).to_csv(OUT_DIR / 'n_d_nowcast_inflated_to_2024_vs_v03.csv', index=False)

    setup_mpl()
    for kind, pct, color, fname in (
        (
            'N',
            n_pct,
            '#ff7f0e',
            'n_perc_diff_hist_nowcast_to_2024_vs_v03.png',
        ),
        (
            'D',
            d_pct,
            '#1f77b4',
            'd_perc_diff_hist_nowcast_to_2024_vs_v03.png',
        ),
    ):
        fig, ax = plt.subplots(figsize=(8.0, 5.2))
        draw_per_sector_pct_hist_panel(
            ax,
            (pct / 100.0).to_numpy(),
            title=f'{kind} % diff: nowcast-2023 (inflated to 2024$) vs v0.3',
            color=color,
        )
        fig.tight_layout()
        out = OUT_DIR / fname
        fig.savefig(out, dpi=150, bbox_inches='tight')
        plt.close(fig)
        print(
            f'{kind}: median={pct.median():+.2f}%  '
            f'p95(|.|)= {pct.abs().quantile(0.95):.2f}%  -> {out.name}'
        )


if __name__ == '__main__':
    main()
