"""Comparison pairs and electricity-step labels for the five-slide deck."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from bedrock.utils.snapshots import releases

ImplId = Literal['current', 'eia_gtd', 'original']
HistMode = Literal['pairwise', 'vs_footing']
StepId = Literal['footing', 'reallocation', 'three_way', 'mixed_units']
ClassRowStyle = Literal['eia', 'original']

STEPS: tuple[StepId, ...] = (
    'footing',
    'reallocation',
    'three_way',
    'mixed_units',
)

HIST_STEPS: tuple[StepId, ...] = ('reallocation', 'three_way', 'mixed_units')

STEP_COLUMN_LABEL: dict[StepId, str] = {
    'footing': 'footing',
    'reallocation': 'reallocation',
    'three_way': '3-way split',
    'mixed_units': 'unit conversion',
}

HIST_PANEL_TITLE: dict[StepId, str] = {
    'reallocation': 'Co-production reallocation',
    'three_way': '3-way monetary split',
    'mixed_units': 'Conversion to physical units',
}

FOOTING_CONFIG = '2025_usa_cornerstone_v0_3_electricity_footing'
REALLOC_CONFIG = '2025_usa_cornerstone_v0_3_electricity_reallocation'
DISAGG_CONFIG = '2025_usa_cornerstone_v0_3_electricity_disaggregation'
MIXED_CONFIG = '2025_usa_cornerstone_v0_3_electricity_mixed_units'

CONFIG_FOR_STEP: dict[StepId, str] = {
    'footing': FOOTING_CONFIG,
    'reallocation': REALLOC_CONFIG,
    'three_way': DISAGG_CONFIG,
    'mixed_units': MIXED_CONFIG,
}

CHILD_SECTORS: tuple[str, ...] = ('221110', '221121', '221122')
AGGREGATE_SECTOR = '221100'
STAR_SECTOR = '221100*'
GENERATION_SECTOR = '221110'

TABLE_ROW_SECTORS: tuple[str, ...] = (
    AGGREGATE_SECTOR,
    STAR_SECTOR,
    '221110',
    '221121',
    '221122',
)

ROW_DISPLAY: dict[str, str] = {
    AGGREGATE_SECTOR: '221100',
    STAR_SECTOR: '221100*',
    '221110': '221110 (G)',
    '221121': '221121 (T)',
    '221122': '221122 (D)',
}

# Sector is not in the model at this step (sample N/A), distinct from missing data.
NA_AT_STEP: dict[StepId, frozenset[str]] = {
    'footing': frozenset({STAR_SECTOR, '221110', '221121', '221122'}),
    'reallocation': frozenset({STAR_SECTOR, '221110', '221121', '221122'}),
    'three_way': frozenset({AGGREGATE_SECTOR}),
    'mixed_units': frozenset({AGGREGATE_SECTOR}),
}

CLASS_ORDER: tuple[str, ...] = (
    'Residential',
    'Commercial',
    'Industrial',
    'Transportation',
    'Exports',
)

EIA_CLASS_GROUPS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ('Residential', ('Residential',)),
    (
        'Com+Ind+Trans+Exports',
        ('Commercial', 'Industrial', 'Transportation', 'Exports'),
    ),
)

ORIGINAL_CLASS_GROUPS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ('Residential', ('Residential',)),
    ('Com+Ind+Trans sales', ('Commercial', 'Industrial', 'Transportation')),
)


@dataclass(frozen=True)
class Implementation:
    id: ImplId
    title: str
    footing_label: str
    snapshot_key: str
    class_row_style: ClassRowStyle
    industrial_weights: Literal['mecs', 'dollars'] | None


@dataclass(frozen=True)
class Pair:
    key: str
    top: ImplId
    bottom: ImplId
    hist_mode: HistMode
    filename: str
    slide1_note: str
    slide_ef_note: str
    slide4_extra_note: str
    slide5_caption: str
    slide3_caption: str


IMPLEMENTATIONS: dict[ImplId, Implementation] = {
    'current': Implementation(
        id='current',
        title='Current (post-MECS)',
        footing_label='v0.3.1',
        snapshot_key=releases.v0_3_1,
        class_row_style='eia',
        industrial_weights='mecs',
    ),
    'eia_gtd': Implementation(
        id='eia_gtd',
        title='EIA G/T/D (pre-MECS)',
        footing_label='v0.3.1',
        snapshot_key=releases.v0_3_1,
        class_row_style='eia',
        industrial_weights='dollars',
    ),
    'original': Implementation(
        id='original',
        title='Original Electricity Disaggregation',
        footing_label='v0.2',
        snapshot_key=releases.v0_2,
        class_row_style='original',
        industrial_weights=None,
    ),
}

PAIRS: dict[str, Pair] = {
    'current_vs_eia_gtd': Pair(
        key='current_vs_eia_gtd',
        top='current',
        bottom='eia_gtd',
        hist_mode='vs_footing',
        filename='current_vs_eia_gtd.pptx',
        slide1_note=(
            'Class totals keep EIA Table 2.2 identities. MECS changes shares '
            'inside manufacturing, which this slide does not show.'
        ),
        slide_ef_note=(
            'Baseline is the v0.3.1 electricity footing for both sides. '
            '221100 is the aggregate sector; 221100* re-aggregates G/T/D. '
            'Steps where a sector is not in the model are N/A. Matching '
            'values are marked same. Those same/not-same cells are the '
            'MECS vs dollar-weight differences.'
        ),
        slide4_extra_note='',
        slide5_caption=(
            'Top: current (post-MECS) vs v0.3.1 electricity footing. '
            'Bottom: EIA G/T/D (pre-MECS) vs the same footing '
            '(frozen 2026-08-24 panel). Compare the two rows for the MECS '
            'effect; cell-level N differences are on the table slide.'
        ),
        slide3_caption=(
            'Top: current (post-MECS) vs v0.3.1 electricity footing. '
            'Bottom: EIA G/T/D (pre-MECS) vs the same footing '
            '(frozen 2026-08-24 panel). Compare the two rows for the MECS '
            'effect; cell-level D differences are on the table slide.'
        ),
    ),
    'current_vs_original': Pair(
        key='current_vs_original',
        top='original',
        bottom='current',
        hist_mode='vs_footing',
        filename='current_vs_original.pptx',
        slide1_note=(
            'Goal of EIA-anchored disaggregation is to match EIA end-use class '
            'MWh. Totals differ because EIA-anchored uses EIA trends for 2017 '
            'eGRID (when missing) and treats exports differently.'
        ),
        slide_ef_note=(
            'Baseline differs between implementations. '
            '221100 is the original sector; 221100* is the re-aggregated '
            'G/T/D block. Steps where a sector is not in the model are N/A. '
            'Matching values are marked same.'
        ),
        slide4_extra_note=(
            'For EIA-anchored mixed units, 3-way and unit-conversion N may '
            'match because generation uses a unique price conversion.'
        ),
        slide5_caption=(
            'Top: original vs v0.2 footing (frozen 2026-07-30 panel). '
            'Bottom: current (post-MECS) vs v0.3.1 electricity footing.'
        ),
        slide3_caption=(
            'Top: original vs v0.2 footing (frozen 2026-07-30 panel). '
            'Bottom: current (post-MECS) vs v0.3.1 electricity footing.'
        ),
    ),
    'eia_gtd_vs_original': Pair(
        key='eia_gtd_vs_original',
        top='original',
        bottom='eia_gtd',
        hist_mode='vs_footing',
        filename='eia_gtd_vs_original.pptx',
        slide1_note=(
            'Goal of EIA-anchored disaggregation is to match EIA end-use class '
            'MWh. Totals differ because EIA-anchored uses EIA trends for 2017 '
            'eGRID (when missing) and treats exports differently.'
        ),
        slide_ef_note=(
            'Baseline differs between implementations. '
            '221100 is the original sector; 221100* is the re-aggregated '
            'G/T/D block. Steps where a sector is not in the model are N/A. '
            'Matching values are marked same.'
        ),
        slide4_extra_note=(
            'For EIA-anchored mixed units, 3-way and unit-conversion N may '
            'match because generation uses a unique price conversion.'
        ),
        slide5_caption=(
            'Top: original vs v0.2 footing (frozen 2026-07-30 panel). '
            'Bottom: EIA G/T/D (pre-MECS) vs v0.3.1 electricity footing '
            '(frozen 2026-08-24 panel). Less variance on the EIA row when '
            'generation uses a constant price.'
        ),
        slide3_caption=(
            'Top: original vs v0.2 footing (frozen 2026-07-30 panel). '
            'Bottom: EIA G/T/D (pre-MECS) vs v0.3.1 electricity footing '
            '(frozen 2026-08-24 panel).'
        ),
    ),
}


def class_groups_for(style: ClassRowStyle) -> tuple[tuple[str, tuple[str, ...]], ...]:
    if style == 'original':
        return ORIGINAL_CLASS_GROUPS
    return EIA_CLASS_GROUPS
