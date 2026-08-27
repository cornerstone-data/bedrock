"""Slide 7/8/27–30 tables from the reanchored ``EIAPurchaserAllocation``.

Class MWh is compared to D0 ``_class_mwh_targets``, not raw Table 2.2.
Leftover T&D is ``bill − gen_dollars``. Nibble is class totals vs those
targets; ``clipped`` is purchaser-level only.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from bedrock.publish.cache_reset import clear_all_publish_caches
from bedrock.transform.eeio.derived_cornerstone import derive_cornerstone_Aq_scaled
from bedrock.transform.eeio.electricity_gtd_allocation import (
    EIAPurchaserAllocation,
    _class_mwh_targets,
    get_reanchored_eia_purchaser_allocation,
)
from bedrock.utils.config.usa_config import (
    get_usa_config,
    reset_usa_config,
    set_global_usa_config,
)

MIXED_CONFIG = '2025_usa_cornerstone_v0_3_electricity_mixed_units'
SPLIT_CONFIG = '2025_usa_cornerstone_v0_3_electricity_disaggregation'

CLASS_ORDER: tuple[str, ...] = (
    'Residential',
    'Commercial',
    'Industrial',
    'Transportation',
    'Exports',
)

# Class nibble is logged in production, not stored as a Series. Treat a class
# as nibbled when allocated MWh sits below the D0 target by more than this.
NIBBLE_ATOL = 1e-6


def load_reanchored_allocation(config: str = MIXED_CONFIG) -> EIAPurchaserAllocation:
    """Flush caches, resolve ``config``, run A/q, and return the P5 allocation.

    ``get_reanchored_eia_purchaser_allocation`` stays ``None`` until
    ``reanchor_electricity_aq_after_year_scaling`` runs. Do not use
    ``get_2017_eia_purchaser_allocation`` for these tables.
    """
    reset_usa_config()
    clear_all_publish_caches()
    set_global_usa_config(config)
    derive_cornerstone_Aq_scaled()
    alloc = get_reanchored_eia_purchaser_allocation()
    if alloc is None:
        raise RuntimeError(
            'get_reanchored_eia_purchaser_allocation() is None after '
            f'derive_cornerstone_Aq_scaled under {config!r}. Need a 3-way/mixed '
            'YAML with apply_io_year_adjustments=True and USEEIO A-scale off.'
        )
    return alloc


def d0_targets(alloc: EIAPurchaserAllocation, eia_year: int) -> dict[str, float]:
    return _class_mwh_targets(eia_year, alloc.egrid_mwh)


def allocated_class_mwh(alloc: EIAPurchaserAllocation) -> pd.Series:
    return alloc.mwh.groupby(alloc.end_use_class).sum().astype(float)


def leftover_td_usd(alloc: EIAPurchaserAllocation) -> pd.Series:
    """Purchaser leftover T&D dollars: ``bill − gen_dollars``."""
    return (alloc.bill.astype(float) - alloc.gen_dollars.astype(float)).astype(float)


def d0_class_mwh_frame(
    alloc: EIAPurchaserAllocation,
    eia_year: int,
    *,
    targets: dict[str, float] | None = None,
    raw_table_22_mwh: dict[str, float] | None = None,
) -> pd.DataFrame:
    """Allocator class MWh vs D0 targets (and optional raw Table 2.2)."""
    if targets is None:
        targets = d0_targets(alloc, eia_year)
    got = allocated_class_mwh(alloc)
    rows: list[dict[str, Any]] = []
    for cls in CLASS_ORDER:
        target = float(targets.get(cls, 0.0))
        allocated = float(got.get(cls, 0.0))
        row: dict[str, Any] = {
            'end_use_class': cls,
            'd0_target_mwh': target,
            'allocator_mwh': allocated,
            'ratio_vs_d0': allocated / target if target else float('nan'),
            'nibble': bool(allocated + NIBBLE_ATOL < target),
        }
        if raw_table_22_mwh is not None and cls in raw_table_22_mwh:
            raw = float(raw_table_22_mwh[cls])
            row['raw_table_22_mwh'] = raw
            row['ratio_vs_raw_22'] = allocated / raw if raw else float('nan')
        rows.append(row)
    return pd.DataFrame(rows)


def leftover_td_purchaser_frame(alloc: EIAPurchaserAllocation) -> pd.DataFrame:
    leftover = leftover_td_usd(alloc)
    idx = alloc.bill.index
    return pd.DataFrame(
        {
            'purchaser': list(idx.astype(str)),
            'end_use_class': alloc.end_use_class.reindex(idx).astype(str).to_numpy(),
            'bill': alloc.bill.astype(float).to_numpy(),
            'gen_dollars': alloc.gen_dollars.reindex(idx).astype(float).to_numpy(),
            'leftover_td': leftover.reindex(idx).to_numpy(),
            't_dollars': alloc.t_dollars.reindex(idx).astype(float).to_numpy(),
            'd_dollars': alloc.d_dollars.reindex(idx).astype(float).to_numpy(),
            'clipped': alloc.clipped.reindex(idx).astype(bool).to_numpy(),
        }
    )


def leftover_td_class_frame(alloc: EIAPurchaserAllocation) -> pd.DataFrame:
    purchasers = leftover_td_purchaser_frame(alloc)
    grouped = purchasers.groupby('end_use_class', sort=False)
    out = grouped[
        ['bill', 'gen_dollars', 'leftover_td', 't_dollars', 'd_dollars']
    ].sum()
    out['n_clipped'] = grouped['clipped'].sum().astype(int)
    out['n_purchasers'] = grouped.size().astype(int)
    return out.reindex([c for c in CLASS_ORDER if c in out.index]).reset_index()


def class_nibble_frame(
    alloc: EIAPurchaserAllocation,
    eia_year: int,
    *,
    targets: dict[str, float] | None = None,
) -> pd.DataFrame:
    """Class nibble (totals vs D0) alongside purchaser ``clipped`` counts.

    Nibble has no Series flag. ``clipped`` marks purchasers that hit their
    bill cap during water-fill; a clipped purchaser does not imply class nibble.
    """
    d0 = d0_class_mwh_frame(alloc, eia_year, targets=targets)
    purchasers = leftover_td_purchaser_frame(alloc)
    clip_counts = (
        purchasers.groupby('end_use_class')['clipped'].sum().astype(int)
        if not purchasers.empty
        else pd.Series(dtype=int)
    )
    d0['n_clipped_purchasers'] = [
        int(clip_counts.get(cls, 0)) for cls in d0['end_use_class']
    ]
    return d0[
        [
            'end_use_class',
            'd0_target_mwh',
            'allocator_mwh',
            'ratio_vs_d0',
            'nibble',
            'n_clipped_purchasers',
        ]
    ]


def optional_implied_cents_kwh_frame(
    alloc: EIAPurchaserAllocation,
    table_24_cents_kwh: dict[str, float],
) -> pd.DataFrame:
    """Implied retail ¢/kWh from ``bill / MWh`` vs Table 2.4 (check only)."""
    bills = alloc.bill.astype(float).groupby(alloc.end_use_class).sum()
    mwh = allocated_class_mwh(alloc)
    rows: list[dict[str, Any]] = []
    for cls in CLASS_ORDER:
        if cls == 'Exports':
            continue
        class_bill = float(bills.get(cls, 0.0))
        class_mwh = float(mwh.get(cls, 0.0))
        implied = class_bill / (10.0 * class_mwh) if class_mwh > 0 else float('nan')
        listed = float(table_24_cents_kwh.get(cls, float('nan')))
        rows.append(
            {
                'end_use_class': cls,
                'implied_cents_kwh': implied,
                'table_24_cents_kwh': listed,
            }
        )
    return pd.DataFrame(rows)


def _fmt_mwh(value: float) -> str:
    return f'{value / 1e6:,.1f} TWh'


def _fmt_usd_b(value: float) -> str:
    return f'${value / 1e9:,.2f} B'


def _markdown_table(df: pd.DataFrame, float_cols: dict[str, str]) -> list[str]:
    cols = list(df.columns)
    header = '| ' + ' | '.join(str(c) for c in cols) + ' |'
    sep = '| ' + ' | '.join('---' for _ in cols) + ' |'
    lines = [header, sep]
    for _, row in df.iterrows():
        cells: list[str] = []
        for c in cols:
            val = row[c]
            if c in float_cols:
                cells.append(float_cols[c].format(val))
            elif isinstance(val, bool):
                cells.append('yes' if val else 'no')
            else:
                cells.append(str(val))
        lines.append('| ' + ' | '.join(cells) + ' |')
    return lines


def render_purchaser_tables_md(
    alloc: EIAPurchaserAllocation,
    eia_year: int,
    *,
    targets: dict[str, float] | None = None,
    raw_table_22_mwh: dict[str, float] | None = None,
    table_24_cents_kwh: dict[str, float] | None = None,
    config: str = MIXED_CONFIG,
) -> str:
    d0 = d0_class_mwh_frame(
        alloc, eia_year, targets=targets, raw_table_22_mwh=raw_table_22_mwh
    )
    leftover = leftover_td_class_frame(alloc)
    nibble = class_nibble_frame(alloc, eia_year, targets=targets)
    leftover_usd = leftover_td_usd(alloc)
    lines = [
        '# EIA-anchored purchaser allocation (D0)',
        '',
        f'Config: `{config}`. EIA / D0 year: **{eia_year}**. '
        f'eGRID MWh = **{alloc.egrid_mwh:,.0f}**. '
        f'Generation price `p` = **{alloc.p:.6g}** USD/MWh. '
        f'T&D split `td_share` = **{alloc.td_share:.4f}**.',
        '',
        'Allocator source: `get_reanchored_eia_purchaser_allocation()` after '
        '`reset_usa_config` -> `clear_all_publish_caches` -> '
        '`set_global_usa_config` -> `derive_cornerstone_Aq_scaled`.',
        '',
        '## Class MWh vs D0 targets',
        '',
        'D0 identity: `(class / Total End Use) * (eGRID - Table 2.14 exports)`, '
        'Industrial pool = Industrial + Direct Use, `F04000` = Exports. '
        'Do **not** read Model / raw Table 2.2 ~ 1.0 as the D0 claim. '
        'Nibble (class bills cannot cover `p *` class MWh) is the expected '
        'exception.',
        '',
        *_markdown_table(
            d0,
            {
                'd0_target_mwh': '{:,.0f}',
                'allocator_mwh': '{:,.0f}',
                'ratio_vs_d0': '{:.4f}',
                'raw_table_22_mwh': '{:,.0f}',
                'ratio_vs_raw_22': '{:.4f}',
            },
        ),
        '',
        f'Totals: D0 {_fmt_mwh(float(d0["d0_target_mwh"].sum()))}; '
        f'allocator {_fmt_mwh(float(d0["allocator_mwh"].sum()))}.',
        '',
        '## Leftover T&D = bill - gen_dollars',
        '',
        *_markdown_table(
            leftover,
            {
                'bill': '${:,.2f}',
                'gen_dollars': '${:,.2f}',
                'leftover_td': '${:,.2f}',
                't_dollars': '${:,.2f}',
                'd_dollars': '${:,.2f}',
            },
        ),
        '',
        f'Purchaser leftover T&D total {_fmt_usd_b(float(leftover_usd.sum()))}. '
        'Equals `t_dollars + d_dollars` by construction.',
        '',
        '## Nibble (class) vs clipped (purchaser)',
        '',
        'Nibble has **no Series flag**. A class is nibbled when '
        '`sum(mwh) < D0 target`. `clipped` is the only Series flag and is '
        'purchaser-level water-fill.',
        '',
        *_markdown_table(
            nibble,
            {
                'd0_target_mwh': '{:,.0f}',
                'allocator_mwh': '{:,.0f}',
                'ratio_vs_d0': '{:.4f}',
            },
        ),
        '',
    ]
    if table_24_cents_kwh is not None:
        check = optional_implied_cents_kwh_frame(alloc, table_24_cents_kwh)
        lines.extend(
            [
                '## Optional check - implied cents/kWh vs Table 2.4',
                '',
                'Implied cents/kWh = `bill / (10 * MWh)`. This is a check against '
                'retail Table 2.4, **not** the D0 identity and not production '
                '`c_row` (production `c_row` is flat `1/p`).',
                '',
                *_markdown_table(
                    check,
                    {
                        'implied_cents_kwh': '{:.2f}',
                        'table_24_cents_kwh': '{:.2f}',
                    },
                ),
                '',
            ]
        )
    return '\n'.join(lines)


def build_live_report(config: str = MIXED_CONFIG) -> str:
    alloc = load_reanchored_allocation(config)
    eia_year = int(get_usa_config().model_base_year)
    from bedrock.extract.disaggregation.egrid_generation import (  # noqa: PLC0415
        eia_table_2_2_end_use_mwh,
        eia_table_2_14_export_mwh,
    )
    from bedrock.transform.eeio.electricity_end_use_mapping import (  # noqa: PLC0415
        electricity_end_use_retail_prices_cents_kwh,
    )

    t22 = eia_table_2_2_end_use_mwh(eia_year)
    raw_22 = {
        'Residential': float(t22['Residential']),
        'Commercial': float(t22['Commercial']),
        'Industrial': float(t22['Industrial']),
        'Transportation': float(t22['Transportation']),
        'Exports': float(eia_table_2_14_export_mwh(eia_year)),
    }
    prices = electricity_end_use_retail_prices_cents_kwh(eia_year)
    table_24: dict[str, float] = {str(k): float(v) for k, v in prices.items()}
    return render_purchaser_tables_md(
        alloc,
        eia_year,
        raw_table_22_mwh=raw_22,
        table_24_cents_kwh=table_24,
        config=config,
    )
