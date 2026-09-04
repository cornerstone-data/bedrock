"""Comparison pairs and electricity-step labels for the five-slide deck."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from bedrock.utils.snapshots import releases

ImplId = Literal[
    'mecs_mixed_units', 'eia_gtd', 'original', 'production', 'reaggregation'
]
HistMode = Literal['pairwise', 'vs_footing']
HistBaseline = Literal['own_footing', 'peer']
StepId = Literal['footing', 'reallocation', 'three_way', 'mixed_units', 'reaggregation']
ClassRowStyle = Literal['eia', 'original']
Schema = Literal['disagg', 'aggregate']

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
    'reaggregation': 'reaggregation',
}

HIST_PANEL_TITLE: dict[StepId, str] = {
    'reallocation': 'Co-production reallocation',
    'three_way': '3-way monetary split',
    'mixed_units': 'Conversion to physical units',
    'reaggregation': 'Reaggregation to 221100',
}

FOOTING_CONFIG = '2025_usa_cornerstone_v0_3_electricity_footing'
PRODUCTION_CONFIG = '2025_usa_cornerstone_v0_3'
REALLOC_CONFIG = '2025_usa_cornerstone_v0_3_electricity_reallocation'
DISAGG_CONFIG = '2025_usa_cornerstone_v0_3_electricity_disaggregation'
MIXED_CONFIG = '2025_usa_cornerstone_v0_3_electricity_mixed_units'
REAGG_CONFIG = '2025_usa_cornerstone_v0_3_electricity_reaggregation'

CONFIG_FOR_STEP: dict[StepId, str] = {
    'footing': FOOTING_CONFIG,
    'reallocation': REALLOC_CONFIG,
    'three_way': DISAGG_CONFIG,
    'mixed_units': MIXED_CONFIG,
    'reaggregation': REAGG_CONFIG,
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

AGGREGATE_ONLY_NA = frozenset({STAR_SECTOR, '221110', '221121', '221122'})

# Sector is not in the model at this step (sample N/A), distinct from missing data.
NA_AT_STEP: dict[StepId, frozenset[str]] = {
    'footing': frozenset({STAR_SECTOR, '221110', '221121', '221122'}),
    'reallocation': frozenset({STAR_SECTOR, '221110', '221121', '221122'}),
    'three_way': frozenset({AGGREGATE_SECTOR}),
    'mixed_units': frozenset({AGGREGATE_SECTOR}),
    'reaggregation': frozenset({STAR_SECTOR, '221110', '221121', '221122'}),
}

CLASS_ORDER: tuple[str, ...] = (
    'Residential',
    'Commercial',
    'Industrial',
    'Transportation',
    'Exports',
)

EIA_CLASS_GROUPS: tuple[tuple[str, tuple[str, ...]], ...] = tuple(
    (name, (name,)) for name in CLASS_ORDER
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
    schema: Schema = 'disagg'
    single_config: str | None = None


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
    hist_baseline: HistBaseline = 'own_footing'
    table_steps: tuple[StepId, ...] = STEPS
    hist_steps: tuple[StepId, ...] = HIST_STEPS


IMPLEMENTATIONS: dict[ImplId, Implementation] = {
    'mecs_mixed_units': Implementation(
        id='mecs_mixed_units',
        title='Post-MECS mixed-units disagg',
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
    'production': Implementation(
        id='production',
        title='Cornerstone v0.3 production (non-disagg)',
        footing_label='v0.3',
        snapshot_key=releases.v0_3_1,
        class_row_style='eia',
        industrial_weights=None,
        schema='aggregate',
        single_config=PRODUCTION_CONFIG,
    ),
    'reaggregation': Implementation(
        id='reaggregation',
        title='Reaggregated 221100',
        footing_label='v0.3.1',
        snapshot_key=releases.v0_3_1,
        class_row_style='eia',
        industrial_weights='mecs',
        schema='disagg',
    ),
}

PAIRS: dict[str, Pair] = {
    'mecs_mixed_units_vs_eia_gtd': Pair(
        key='mecs_mixed_units_vs_eia_gtd',
        top='mecs_mixed_units',
        bottom='eia_gtd',
        hist_mode='vs_footing',
        filename='mecs_mixed_units_vs_eia_gtd.pptx',
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
            'Top: Post-MECS mixed-units disagg vs v0.3.1 electricity footing. '
            'Bottom: EIA G/T/D (pre-MECS) vs the same footing '
            '(published PPTX panel). Compare the two rows for the MECS '
            'effect; cell-level N differences are on the table slide.'
        ),
        slide3_caption=(
            'Top: Post-MECS mixed-units disagg vs v0.3.1 electricity footing. '
            'Bottom: EIA G/T/D (pre-MECS) vs the same footing '
            '(published PPTX panel). Compare the two rows for the MECS '
            'effect; cell-level D differences are on the table slide.'
        ),
    ),
    'mecs_mixed_units_vs_original': Pair(
        key='mecs_mixed_units_vs_original',
        top='original',
        bottom='mecs_mixed_units',
        hist_mode='vs_footing',
        filename='mecs_mixed_units_vs_original.pptx',
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
            'Top: original vs v0.2 footing (published PPTX panel). '
            'Bottom: Post-MECS mixed-units disagg vs v0.3.1 electricity footing.'
        ),
        slide3_caption=(
            'Top: original vs v0.2 footing (published PPTX panel). '
            'Bottom: Post-MECS mixed-units disagg vs v0.3.1 electricity footing.'
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
            'Top: original vs v0.2 footing (published PPTX panel). '
            'Bottom: EIA G/T/D (pre-MECS) vs v0.3.1 electricity footing '
            '(published PPTX panel). Less variance on the EIA row when '
            'generation uses a constant price.'
        ),
        slide3_caption=(
            'Top: original vs v0.2 footing (published PPTX panel). '
            'Bottom: EIA G/T/D (pre-MECS) vs v0.3.1 electricity footing '
            '(published PPTX panel).'
        ),
    ),
    'mecs_mixed_units_vs_production': Pair(
        key='mecs_mixed_units_vs_production',
        top='mecs_mixed_units',
        bottom='production',
        hist_mode='vs_footing',
        hist_baseline='peer',
        filename='mecs_mixed_units_vs_production.pptx',
        slide1_note=(
            'Non-disagg production has no EIA end-use class split. '
            'Class MWh is only defined on the electricity-disaggregation path.'
        ),
        slide_ef_note=(
            'Production is 2025_usa_cornerstone_v0_3: aggregate 221100 at every '
            'column, industry-average margins on, no G/T/D. Post-MECS mixed-units '
            'disagg is the electricity-disagg chain (margins off). Matching '
            'values are marked same. G/T/D and 221100* are N/A on production.'
        ),
        slide4_extra_note='',
        slide5_caption=(
            'Top: Post-MECS mixed-units disagg vs Cornerstone v0.3 production '
            '(non-disagg). Bottom: production vs itself (0% check). '
            'Shared baseline is production D/N, not the electricity footing.'
        ),
        slide3_caption=(
            'Top: Post-MECS mixed-units disagg vs Cornerstone v0.3 production '
            '(non-disagg). Bottom: production vs itself (0% check). '
            'Shared baseline is production D/N, not the electricity footing.'
        ),
    ),
    'reaggregated_vs_production': Pair(
        key='reaggregated_vs_production',
        top='production',
        bottom='reaggregation',
        hist_mode='vs_footing',
        hist_baseline='peer',
        filename='reaggregated_vs_production.pptx',
        slide1_note=(
            'Slide 1 is 221100 commodity q and industry x. Production has no '
            'EIA class split; bottom values are the collapsed reaggregation step.'
        ),
        slide_ef_note=(
            'Reaggregated vs production. Production is 2025_usa_cornerstone_v0_3 '
            '(aggregate 221100, margins on). Reaggregation keeps realloc + 3-way '
            '+ MECS then collapses G/T/D to monetary 221100. Matching values are '
            'marked same. G/T/D and 221100* are N/A on production.'
        ),
        slide4_extra_note='',
        slide5_caption=(
            'Top: production vs itself (0% check). Bottom: Reaggregated vs '
            'production. Shared baseline is production D/N, not the electricity '
            'footing.'
        ),
        slide3_caption=(
            'Top: production vs itself (0% check). Bottom: Reaggregated vs '
            'production. Shared baseline is production D/N, not the electricity '
            'footing.'
        ),
        table_steps=('footing', 'reallocation', 'three_way', 'reaggregation'),
        hist_steps=('reallocation', 'three_way', 'reaggregation'),
    ),
}


def class_groups_for(style: ClassRowStyle) -> tuple[tuple[str, tuple[str, ...]], ...]:
    if style == 'original':
        return ORIGINAL_CLASS_GROUPS
    return EIA_CLASS_GROUPS


def config_for_step(impl: Implementation, step_id: StepId) -> str:
    if impl.single_config is not None:
        return impl.single_config
    return CONFIG_FOR_STEP[step_id]


def na_sectors_at_step(impl_id: ImplId, step_id: StepId) -> frozenset[str]:
    if IMPLEMENTATIONS[impl_id].schema == 'aggregate':
        return AGGREGATE_ONLY_NA
    return NA_AT_STEP[step_id]
