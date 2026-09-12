"""Compare reaggregated nowcast-2024 electricity vs plain nowcast 2024.

Baseline: ``2025_usa_cornerstone_v0_4_nowcast_2024`` (margins on, electricity
off), with ``nowcast_mut_vintage`` pinned to ``v0.3.0_4276083`` when the stock
YAML omits it.

Treatment: ``2025_usa_cornerstone_v0_4_nowcast_2024_electricity_reaggregation``.

Run:
    python -m bedrock.analysis.electricity.current.diagnostics.reaggregated_vs_plain_nowcast_2024

Outputs under ``local_data/reaggregated_vs_plain_nowcast_2024/``:
    - delta_a_q_x_221100.csv
    - delta_d_n_221100.csv
    - attribution_notes.md
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from bedrock.analysis.electricity.current.diagnostics.paths import LOCAL_DATA_DIR
from bedrock.publish.cache_reset import clear_all_publish_caches
from bedrock.utils.config.usa_config import reset_usa_config, set_global_usa_config

BASELINE_CONFIG = '2025_usa_cornerstone_v0_4_nowcast_2024'
REAGG_CONFIG = '2025_usa_cornerstone_v0_4_nowcast_2024_electricity_reaggregation'
VINTAGE_PIN = 'v0.3.0_4276083'
SECTOR = '221100'

OUT_DIR = LOCAL_DATA_DIR / 'reaggregated_vs_plain_nowcast_2024'

_ATTRIBUTION_NOTES = """\
# Reaggregated vs plain nowcast 2024 — attribution notes

Baseline: `{baseline}` (electricity off, margins on).
Treatment: `{reagg}` (realloc + 3-way + reaggregation, margins on).
Both use nowcast MUT vintage `{vintage}` when pinned.

## Best-effort drivers of ΔA / Δq / Δx / ΔD / ΔN at 221100

1. **Co-production reallocation** — moves electricity-related Make/Use mass
   before the G/T/D split; changes the 221100 footing that children inherit.
2. **`_water_fill_gen` / `_spill_generation_nonfuel`** — rewrite generation
   Use so VA_G stays non-negative; can shift fuel and non-fuel rows onto G
   (and spill non-fuel onto T/D).
3. **q-weighted B collapse** — reaggregation collapses G/T/D B columns with
   child `q` weights; D/N for 221100 are not a simple sum of child D/N.
4. **Utilities-seed / own-use** — fuel commodity rows are seeded onto
   generation; the aggregate `221100` own-use cell is redistributed across
   the G/T/D block then collapsed again at reaggregation.

## Analyses that should read pre-collapse 407 derives in-process

- Any G/T/D-level D/N / class-MWh diagnostics before reaggregation.
- Spill / VA_G checks on `derive_cornerstone_Aq_scaled()` (407 path).
- Purchaser-allocation tables that keep child commodity rows.

Placeholder: expand with numeric attribution once residual CSVs are reviewed.
"""


def _load_model_slice(config: str) -> dict[str, pd.Series | pd.DataFrame | float]:
    from bedrock.publish.model_objects import (  # noqa: PLC0415
        get_A,
        get_D,
        get_N,
        get_q,
        get_x,
    )
    import bedrock.utils.config.usa_config as uc  # noqa: PLC0415

    reset_usa_config(should_reset_env_var=True)
    clear_all_publish_caches()
    set_global_usa_config(config)
    cfg = uc.get_usa_config()
    # Stock nowcast_2024 research YAML may omit the production vintage pin.
    if cfg.nowcast_mut_vintage is None:
        pinned = {**cfg.model_dump(mode='python'), 'nowcast_mut_vintage': VINTAGE_PIN}
        uc._usa_config = uc.USAConfig.model_validate(pinned, strict=True)

    a = get_A()
    q = get_q()
    x = get_x()
    d = get_D().sum(axis=0).astype(float)
    n = get_N().sum(axis=0).astype(float)
    d.index = d.index.astype(str)
    n.index = n.index.astype(str)

    row = a.loc[SECTOR].astype(float) if SECTOR in a.index else pd.Series(dtype=float)
    col = a[SECTOR].astype(float) if SECTOR in a.columns else pd.Series(dtype=float)
    return {
        'a_row': row,
        'a_col': col,
        'q': float(q.loc[SECTOR]) if SECTOR in q.index else float('nan'),
        'x': float(x.loc[SECTOR]) if SECTOR in x.index else float('nan'),
        'd': float(d.loc[SECTOR]) if SECTOR in d.index else float('nan'),
        'n': float(n.loc[SECTOR]) if SECTOR in n.index else float('nan'),
        'd_all': d,
        'n_all': n,
    }


def _delta_a_q_x(
    base: dict[str, pd.Series | pd.DataFrame | float],
    reagg: dict[str, pd.Series | pd.DataFrame | float],
) -> pd.DataFrame:
    base_row = base['a_row']
    reagg_row = reagg['a_row']
    assert isinstance(base_row, pd.Series)
    assert isinstance(reagg_row, pd.Series)
    base_col = base['a_col']
    reagg_col = reagg['a_col']
    assert isinstance(base_col, pd.Series)
    assert isinstance(reagg_col, pd.Series)

    row_delta = reagg_row.reindex(base_row.index.union(reagg_row.index)).fillna(
        0.0
    ) - base_row.reindex(base_row.index.union(reagg_row.index)).fillna(0.0)
    col_delta = reagg_col.reindex(base_col.index.union(reagg_col.index)).fillna(
        0.0
    ) - base_col.reindex(base_col.index.union(reagg_col.index)).fillna(0.0)

    rows = [
        {
            'metric': 'q_221100',
            'baseline': base['q'],
            'reaggregated': reagg['q'],
            'delta': float(reagg['q']) - float(base['q']),  # type: ignore[arg-type]
        },
        {
            'metric': 'x_221100',
            'baseline': base['x'],
            'reaggregated': reagg['x'],
            'delta': float(reagg['x']) - float(base['x']),  # type: ignore[arg-type]
        },
        {
            'metric': 'a_row_221100_l1',
            'baseline': float(base_row.abs().sum()),
            'reaggregated': float(reagg_row.abs().sum()),
            'delta': float(row_delta.abs().sum()),
        },
        {
            'metric': 'a_col_221100_l1',
            'baseline': float(base_col.abs().sum()),
            'reaggregated': float(reagg_col.abs().sum()),
            'delta': float(col_delta.abs().sum()),
        },
    ]
    # Top absolute row/col cell movers (best-effort detail).
    for label, series in (('a_row_delta', row_delta), ('a_col_delta', col_delta)):
        top = series.abs().sort_values(ascending=False).head(15)
        for idx, _ in top.items():
            rows.append(
                {
                    'metric': f'{label}:{idx}',
                    'baseline': float(base_row.get(idx, 0.0))
                    if label.startswith('a_row')
                    else float(base_col.get(idx, 0.0)),
                    'reaggregated': float(reagg_row.get(idx, 0.0))
                    if label.startswith('a_row')
                    else float(reagg_col.get(idx, 0.0)),
                    'delta': float(series.loc[idx]),
                }
            )
    return pd.DataFrame(rows)


def _delta_d_n(
    base: dict[str, pd.Series | pd.DataFrame | float],
    reagg: dict[str, pd.Series | pd.DataFrame | float],
) -> pd.DataFrame:
    rows = [
        {
            'sector': SECTOR,
            'metric': 'D',
            'baseline': base['d'],
            'reaggregated': reagg['d'],
            'delta': float(reagg['d']) - float(base['d']),  # type: ignore[arg-type]
        },
        {
            'sector': SECTOR,
            'metric': 'N',
            'baseline': base['n'],
            'reaggregated': reagg['n'],
            'delta': float(reagg['n']) - float(base['n']),  # type: ignore[arg-type]
        },
    ]
    base_n = base['n_all']
    reagg_n = reagg['n_all']
    assert isinstance(base_n, pd.Series)
    assert isinstance(reagg_n, pd.Series)
    delta_n = reagg_n.reindex(base_n.index.union(reagg_n.index)).fillna(
        0.0
    ) - base_n.reindex(base_n.index.union(reagg_n.index)).fillna(0.0)
    top = delta_n.abs().sort_values(ascending=False).head(20)
    for sector, _ in top.items():
        if str(sector) == SECTOR:
            continue
        rows.append(
            {
                'sector': str(sector),
                'metric': 'N',
                'baseline': float(base_n.get(sector, 0.0)),
                'reaggregated': float(reagg_n.get(sector, 0.0)),
                'delta': float(delta_n.loc[sector]),
            }
        )
    return pd.DataFrame(rows)


def run() -> Path:
    """Derive both models, write CSVs + attribution notes, return output dir."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    base = _load_model_slice(BASELINE_CONFIG)
    reagg = _load_model_slice(REAGG_CONFIG)

    _delta_a_q_x(base, reagg).to_csv(OUT_DIR / 'delta_a_q_x_221100.csv', index=False)
    _delta_d_n(base, reagg).to_csv(OUT_DIR / 'delta_d_n_221100.csv', index=False)
    (OUT_DIR / 'attribution_notes.md').write_text(
        _ATTRIBUTION_NOTES.format(
            baseline=BASELINE_CONFIG,
            reagg=REAGG_CONFIG,
            vintage=VINTAGE_PIN,
        ),
        encoding='utf-8',
    )

    reset_usa_config(should_reset_env_var=True)
    clear_all_publish_caches()
    return OUT_DIR


if __name__ == '__main__':
    out = run()
    print(f'Wrote residual comparison under {out}')
