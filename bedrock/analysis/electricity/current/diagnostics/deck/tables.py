"""Slide 1 class-MWh tables and slides 2/4 electricity D/N tables."""

from __future__ import annotations

from dataclasses import dataclass

from bedrock.analysis.electricity.current.diagnostics.deck.data import (
    MISSING,
    NA,
    SAME,
    ImplBundle,
    class_total_mwh,
    format_ef,
    format_ratio,
    format_twh,
    grouped_mwh,
    sector_ef_usd,
    values_match,
)
from bedrock.analysis.electricity.current.diagnostics.deck.pairs import (
    IMPLEMENTATIONS,
    ROW_DISPLAY,
    STEP_COLUMN_LABEL,
    STEPS,
    TABLE_ROW_SECTORS,
    Implementation,
    Pair,
    StepId,
    class_groups_for,
    na_sectors_at_step,
)
from bedrock.analysis.electricity.historical.original_vs_eia_anchored_deck.published import (
    PUBLISHED_IMPLS,
    published_class_mwh_rows,
    published_ef,
)


@dataclass(frozen=True)
class TableGrid:
    title: str
    headers: tuple[str, ...]
    rows: tuple[tuple[str, ...], ...]


def _class_row_values(
    impl: Implementation,
    bundle: ImplBundle,
) -> list[tuple[str, str, str, str]] | None:
    if impl.id in PUBLISHED_IMPLS:
        published_rows: list[tuple[str, str, str, str]] = []
        for pub_mwh, pub_target, label in published_class_mwh_rows(impl.id):
            published_rows.append(
                (
                    label,
                    format_twh(pub_mwh),
                    format_twh(pub_target),
                    format_ratio(pub_mwh, pub_target),
                )
            )
        return published_rows
    mixed = bundle.steps.get('mixed_units')
    three_way = bundle.steps.get('three_way')
    step = None
    if mixed is not None and mixed.class_mwh is not None:
        step = mixed
    elif three_way is not None and three_way.class_mwh is not None:
        step = three_way
    if step is None or step.class_mwh is None:
        return None
    model = step.class_mwh
    target = step.class_mwh_target
    groups = class_groups_for(impl.class_row_style)
    live_rows: list[tuple[str, str, str, str]] = []
    for label, members in groups:
        m = grouped_mwh(model, members)
        if target is None:
            live_rows.append((label, format_twh(m), MISSING, MISSING))
        else:
            t = grouped_mwh(target, members)
            live_rows.append((label, format_twh(m), format_twh(t), format_ratio(m, t)))
    m_tot = class_total_mwh(model)
    if target is None:
        live_rows.append(('Total', format_twh(m_tot), MISSING, MISSING))
    else:
        t_tot = class_total_mwh(target)
        live_rows.append(
            ('Total', format_twh(m_tot), format_twh(t_tot), format_ratio(m_tot, t_tot))
        )
    return live_rows


def class_mwh_grids(
    pair: Pair,
    top: ImplBundle,
    bottom: ImplBundle,
) -> tuple[TableGrid, TableGrid]:
    top_impl = IMPLEMENTATIONS[pair.top]
    bottom_impl = IMPLEMENTATIONS[pair.bottom]
    headers = ('Comparison', 'Model', 'Target', 'Ratio')
    top_rows = _class_row_values(top_impl, top)
    bottom_rows = _class_row_values(bottom_impl, bottom)
    if top_rows is None:
        top_rows = [('(no class MWh)', MISSING, MISSING, MISSING)]
    if bottom_rows is None:
        bottom_rows = [('(no class MWh)', MISSING, MISSING, MISSING)]
    bottom_marked: list[tuple[str, str, str, str]] = []
    top_by_label = {r[0]: r for r in top_rows}
    for row in bottom_rows:
        label, model, target, ratio = row
        other = top_by_label.get(label)
        if other is not None and other[1:] == row[1:] and model != MISSING:
            bottom_marked.append((label, SAME, SAME, SAME))
        else:
            bottom_marked.append(row)
    return (
        TableGrid(top_impl.title, headers, tuple(top_rows)),
        TableGrid(bottom_impl.title, headers, tuple(bottom_marked)),
    )


def _cell_for_step(
    bundle: ImplBundle,
    kind: str,
    sector: str,
    step_id: StepId,
) -> float | None | str:
    if sector in na_sectors_at_step(bundle.impl_id, step_id):
        return NA
    if bundle.impl_id in PUBLISHED_IMPLS:
        value = published_ef(bundle.impl_id, kind, sector, step_id)
        return NA if value is None else value
    step = bundle.steps.get(step_id)
    if step is None:
        return None
    return sector_ef_usd(step, kind, sector)


def _format_cell(
    value: float | None | str,
    other: float | None | str,
    *,
    mark_same: bool,
) -> str:
    if value == NA:
        return NA
    if isinstance(value, str):
        return value
    if value is None:
        return MISSING
    if mark_same and not isinstance(other, str) and values_match(value, other):
        return SAME
    return format_ef(value)


def ef_grids(
    pair: Pair,
    top: ImplBundle,
    bottom: ImplBundle,
    kind: str,
) -> tuple[TableGrid, TableGrid]:
    top_impl = IMPLEMENTATIONS[pair.top]
    bottom_impl = IMPLEMENTATIONS[pair.bottom]
    return (
        _ef_grid(top_impl, top, bottom, kind, mark_same=False),
        _ef_grid(bottom_impl, bottom, top, kind, mark_same=True),
    )


def _ef_grid(
    impl: Implementation,
    bundle: ImplBundle,
    other: ImplBundle,
    kind: str,
    *,
    mark_same: bool,
) -> TableGrid:
    headers = (
        'GHG',
        'Electricity sector',
        impl.footing_label,
        STEP_COLUMN_LABEL['reallocation'],
        STEP_COLUMN_LABEL['three_way'],
        STEP_COLUMN_LABEL['mixed_units'],
    )
    rows: list[tuple[str, ...]] = []
    for sector in TABLE_ROW_SECTORS:
        cells = [
            format_cell_pair(bundle, other, kind, sector, step, mark_same)
            for step in STEPS
        ]
        rows.append(('Total GHG', ROW_DISPLAY[sector], *cells))
    return TableGrid(impl.title, headers, tuple(rows))


def format_cell_pair(
    bundle: ImplBundle,
    other: ImplBundle,
    kind: str,
    sector: str,
    step_id: StepId,
    mark_same: bool,
) -> str:
    value = _cell_for_step(bundle, kind, sector, step_id)
    peer = _cell_for_step(other, kind, sector, step_id)
    return _format_cell(value, peer, mark_same=mark_same)
