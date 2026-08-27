"""Slide 7/8/27–30 tables from the reanchored ``EIAPurchaserAllocation``.

Class MWh is compared to ``_class_mwh_targets``, not raw Table 2.2.
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
# as nibbled when allocated MWh sits below the class target by more than this.
NIBBLE_ATOL = 1e-6


def load_reanchored_allocation(config: str = MIXED_CONFIG) -> EIAPurchaserAllocation:
    """Flush caches, resolve ``config``, run A/q, and return the reanchored allocation.

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


def class_mwh_targets(alloc: EIAPurchaserAllocation, eia_year: int) -> dict[str, float]:
    return _class_mwh_targets(eia_year, alloc.egrid_mwh)


def allocated_class_mwh(alloc: EIAPurchaserAllocation) -> pd.Series:
    return alloc.mwh.groupby(alloc.end_use_class).sum().astype(float)


def leftover_td_usd(alloc: EIAPurchaserAllocation) -> pd.Series:
    """Purchaser leftover T&D dollars: ``bill − gen_dollars``."""
    return (alloc.bill.astype(float) - alloc.gen_dollars.astype(float)).astype(float)


def class_mwh_targets_frame(
    alloc: EIAPurchaserAllocation,
    eia_year: int,
    *,
    targets: dict[str, float] | None = None,
    raw_table_22_mwh: dict[str, float] | None = None,
) -> pd.DataFrame:
    """Allocator class MWh vs ``_class_mwh_targets`` (and optional raw Table 2.2)."""
    if targets is None:
        targets = class_mwh_targets(alloc, eia_year)
    got = allocated_class_mwh(alloc)
    rows: list[dict[str, Any]] = []
    for cls in CLASS_ORDER:
        target = float(targets.get(cls, 0.0))
        allocated = float(got.get(cls, 0.0))
        row: dict[str, Any] = {
            'end_use_class': cls,
            'class_target_mwh': target,
            'allocator_mwh': allocated,
            'ratio_vs_class_target': allocated / target if target else float('nan'),
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
    """Class nibble (totals vs class targets) alongside purchaser ``clipped`` counts.

    Nibble has no Series flag. ``clipped`` marks purchasers that hit their
    bill cap during water-fill; a clipped purchaser does not imply class nibble.
    """
    class_mwh = class_mwh_targets_frame(alloc, eia_year, targets=targets)
    purchasers = leftover_td_purchaser_frame(alloc)
    clip_counts = (
        purchasers.groupby('end_use_class')['clipped'].sum().astype(int)
        if not purchasers.empty
        else pd.Series(dtype=int)
    )
    class_mwh['n_clipped_purchasers'] = [
        int(clip_counts.get(cls, 0)) for cls in class_mwh['end_use_class']
    ]
    return class_mwh[
        [
            'end_use_class',
            'class_target_mwh',
            'allocator_mwh',
            'ratio_vs_class_target',
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


def p_share_from_allocation(alloc: EIAPurchaserAllocation) -> float:
    """Recover the 2017 UGO generation share from ``p = share × bills / eGRID``."""
    bill_total = float(alloc.bill.sum())
    if bill_total <= 0 or float(alloc.egrid_mwh) <= 0:
        raise ValueError(
            'cannot recover generation share from non-positive bills or eGRID'
        )
    return float(alloc.p) * float(alloc.egrid_mwh) / bill_total


def dual_run_industrial_allocations(
    alloc: EIAPurchaserAllocation,
    eia_year: int,
) -> tuple[EIAPurchaserAllocation, EIAPurchaserAllocation]:
    """Re-allocate the same bills with MECS vs dollar Industrial manufacturing weights."""
    from bedrock.transform.eeio.electricity_gtd_allocation import (  # noqa: PLC0415
        ELECTRICITY_AGGREGATE,
        allocate_purchaser_gtd,
    )

    p_share = p_share_from_allocation(alloc)
    common = {
        'bills': alloc.bill,
        'self_use_key': ELECTRICITY_AGGREGATE,
        'eia_year': eia_year,
        'p_share_2017': p_share,
        'td_share_2017': float(alloc.td_share),
    }
    mecs = allocate_purchaser_gtd(**common, industrial_weights='mecs')
    dollars = allocate_purchaser_gtd(**common, industrial_weights='dollars')
    return mecs, dollars


def manufacturing_mecs_vs_dollar_frame(
    mecs_alloc: EIAPurchaserAllocation,
    dollar_alloc: EIAPurchaserAllocation,
) -> pd.DataFrame:
    """Per manufacturing sector: MECS vs dollar MWh, clip flags, zero-bill assignees.

    Cross-pool overflow marks residual Industrial purchasers whose generation
    dollars rose under MECS (water-fill from clipped energy-intensive manufacturers).
    """
    from bedrock.transform.eeio.electricity_gtd_allocation import (  # noqa: PLC0415
        industrial_manufacturing_pool,
    )

    idx = mecs_alloc.bill.index.union(dollar_alloc.bill.index)
    mfg_pool = industrial_manufacturing_pool()
    classes = mecs_alloc.end_use_class.reindex(idx)
    industrial = classes.astype(str) == 'Industrial'
    is_mfg = pd.Series([str(i) in mfg_pool for i in idx], index=idx)
    bill = mecs_alloc.bill.reindex(idx).astype(float).fillna(0.0)
    mecs_mwh = mecs_alloc.mwh.reindex(idx).astype(float).fillna(0.0)
    dollar_mwh = dollar_alloc.mwh.reindex(idx).astype(float).fillna(0.0)
    mecs_gen = mecs_alloc.gen_dollars.reindex(idx).astype(float).fillna(0.0)
    dollar_gen = dollar_alloc.gen_dollars.reindex(idx).astype(float).fillna(0.0)
    clipped_mecs = mecs_alloc.clipped.reindex(idx).fillna(False).astype(bool)
    clipped_dollars = dollar_alloc.clipped.reindex(idx).fillna(False).astype(bool)
    residual = industrial & ~is_mfg
    overflow = residual & (mecs_gen > dollar_gen + 1e-9)
    rows = pd.DataFrame(
        {
            'purchaser': idx.astype(str),
            'end_use_class': classes.astype(str).to_numpy(),
            'manufacturing': is_mfg.to_numpy(),
            'bill': bill.to_numpy(),
            'mecs_mwh': mecs_mwh.to_numpy(),
            'dollar_mwh': dollar_mwh.to_numpy(),
            'mwh_diff': (mecs_mwh - dollar_mwh).to_numpy(),
            'mecs_gen_dollars': mecs_gen.to_numpy(),
            'dollar_gen_dollars': dollar_gen.to_numpy(),
            'clipped_mecs': clipped_mecs.to_numpy(),
            'clipped_dollars': clipped_dollars.to_numpy(),
            'zero_bill_mecs_assignee': (
                is_mfg & (bill <= 0.0) & (mecs_mwh > 0.0)
            ).to_numpy(),
            'cross_pool_overflow_recipient': overflow.to_numpy(),
        }
    )
    return rows.loc[industrial.to_numpy()].reset_index(drop=True)


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
    class_mwh = class_mwh_targets_frame(
        alloc, eia_year, targets=targets, raw_table_22_mwh=raw_table_22_mwh
    )
    leftover = leftover_td_class_frame(alloc)
    nibble = class_nibble_frame(alloc, eia_year, targets=targets)
    leftover_usd = leftover_td_usd(alloc)
    lines = [
        '# EIA-anchored purchaser allocation',
        '',
        f'Config: `{config}`. EIA / class-target year: **{eia_year}**. '
        f'eGRID MWh = **{alloc.egrid_mwh:,.0f}**. '
        f'Generation price `p` = **{alloc.p:.6g}** USD/MWh. '
        f'T&D split `td_share` = **{alloc.td_share:.4f}**.',
        '',
        'Allocator source: `get_reanchored_eia_purchaser_allocation()` after '
        '`reset_usa_config` -> `clear_all_publish_caches` -> '
        '`set_global_usa_config` -> `derive_cornerstone_Aq_scaled`.',
        '',
        '## Class MWh vs class targets',
        '',
        'Class-target identity: `(class / Total End Use) * (eGRID - Table 2.14 exports)`, '
        'Industrial pool = Industrial + Direct Use, `F04000` = Exports. '
        'Do **not** read Model / raw Table 2.2 ~ 1.0 as the class-target claim. '
        'Nibble (class bills cannot cover `p *` class MWh) is the expected '
        'exception.',
        '',
        *_markdown_table(
            class_mwh,
            {
                'class_target_mwh': '{:,.0f}',
                'allocator_mwh': '{:,.0f}',
                'ratio_vs_class_target': '{:.4f}',
                'raw_table_22_mwh': '{:,.0f}',
                'ratio_vs_raw_22': '{:.4f}',
            },
        ),
        '',
        f'Totals: class targets {_fmt_mwh(float(class_mwh["class_target_mwh"].sum()))}; '
        f'allocator {_fmt_mwh(float(class_mwh["allocator_mwh"].sum()))}.',
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
        '`sum(mwh) < class target`. `clipped` is the only Series flag and is '
        'purchaser-level water-fill.',
        '',
        *_markdown_table(
            nibble,
            {
                'class_target_mwh': '{:,.0f}',
                'allocator_mwh': '{:,.0f}',
                'ratio_vs_class_target': '{:.4f}',
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
                'retail Table 2.4, **not** the class-target identity and not production '
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
    md = render_purchaser_tables_md(
        alloc,
        eia_year,
        raw_table_22_mwh=raw_22,
        table_24_cents_kwh=table_24,
        config=config,
    )
    mecs_alloc, dollar_alloc = dual_run_industrial_allocations(alloc, eia_year)
    compare = manufacturing_mecs_vs_dollar_frame(mecs_alloc, dollar_alloc)
    mfg = compare.loc[compare['manufacturing']]
    overflow = compare.loc[compare['cross_pool_overflow_recipient']]
    extra = [
        '',
        '## MECS vs dollar Industrial manufacturing weights',
        '',
        'Same bills through `allocate_purchaser_gtd` with '
        '`industrial_weights=mecs` vs `dollars`. Generation share recovered as '
        '`p × eGRID / bill_total`. Manufacturing rows use Table 7.7 purchased kWh; '
        'residual Industrial stays dollar-weighted. Class-wide water-fill can move '
        'generation dollars from clipped manufacturers onto residual ag/mining/'
        'construction.',
        '',
        f'Manufacturing purchasers: **{int(len(mfg))}**. '
        f'Clipped under MECS: **{int(mfg["clipped_mecs"].sum()) if not mfg.empty else 0}**. '
        f'Zero-bill MECS assignees: '
        f'**{int(mfg["zero_bill_mecs_assignee"].sum()) if not mfg.empty else 0}**. '
        f'Cross-pool overflow recipients: **{int(len(overflow))}**.',
        '',
    ]
    if not mfg.empty:
        extra.extend(
            _markdown_table(
                mfg.assign(_abs=mfg['mwh_diff'].abs())
                .sort_values('_abs', ascending=False)
                .drop(columns='_abs')
                .head(25),
                {
                    'bill': '${:,.2f}',
                    'mecs_mwh': '{:,.0f}',
                    'dollar_mwh': '{:,.0f}',
                    'mwh_diff': '{:,.0f}',
                    'mecs_gen_dollars': '${:,.2f}',
                    'dollar_gen_dollars': '${:,.2f}',
                },
            )
        )
        extra.append('')
    return md + '\n'.join(extra)
