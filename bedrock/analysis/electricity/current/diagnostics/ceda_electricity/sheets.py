"""Hard-coded sheet IDs and display labels for the two CEDA decks."""

from __future__ import annotations

from dataclasses import dataclass

# Deck A — electricity reaggregation vs CEDA v8.1 baseline (in-sheet).
REAGG_SHEET_ID = '1XiWcT484SVWMTsgNdQVl30xvS20OPlUW-8LmsrYhuW8'
REAGG_LABEL = 'v0.4 electricity improvements (v0.3 settings)'
REAGG_CONFIG = 'v8_cornerstone_2026_v0_4_electricity_improvements_no_manual_adj'


@dataclass(frozen=True)
class LadderStep:
    key: str
    sheet_id: str
    short_label: str
    title: str


# Deck B — cumulative g1→g5 ladder, then g5+electricity improvements.
LADDER_STEPS: tuple[LadderStep, ...] = (
    LadderStep(
        key='g1',
        sheet_id='1CLCBl8tWORemuhaBdjw157sdqwuFa6i53ye_EcczWYU',
        short_label='g1\nv0.3 baseline',
        title='g1 v0.3 baseline',
    ),
    LadderStep(
        key='g2',
        sheet_id='1kQRV9gEPl7bMruyWdkxcwGS3qKRZgfM5PXT34eDa6e4',
        short_label='g2\n+ FIGARO/OECD\nTier 2',
        title='g2 FIGARO Tier2a + OECD Tier2b',
    ),
    LadderStep(
        key='g3',
        sheet_id='1IQnW8OHdWE0bwnUmac_ZYRuFeClj4beYF0G1ezsjw7Y',
        short_label='g3\n+ trade origin\ngap-fill',
        title='g3 trade-origin gap-fill',
    ),
    LadderStep(
        key='g4',
        sheet_id='1iGXG4a4t_f5GVxN8Q_TlndrU9-pG078YyWPxFGJ20SA',
        short_label='g4\n+ CHN 2023',
        title='g4 CHN 2023 IO+GHG',
    ),
    LadderStep(
        key='g5',
        sheet_id='112R97LDP4ZwWewWVpbPi8BYIi4-luJP0RQ2oTK0TGOs',
        short_label='g5\n+ ABSR',
        title='g5 ABSR (RAS + Cornerstone template)',
    ),
    LadderStep(
        key='g5e',
        sheet_id='1yl7hWDSQ3nlbXHgehWlazSVoxepFu08SYmvK-DKs5kI',
        short_label='g5e\n+ electricity',
        title='g5 + electricity improvements',
    ),
)

DECK_A_FILENAME = 'ceda_electricity_reagg_vs_baseline.pptx'
DECK_B_FILENAME = 'ceda_g1_g5e_absr_elec_ladder.pptx'

N_PERC_NO_MA = 'N_no_manual_adj_perc_diff'
D_PERC_NO_MA = 'D_no_manual_adj_perc_diff'
N_PERC_WITH_MA = 'N_with_manual_adj_perc_diff'
N_D_EFFECT = 'N_no_manual_adj_D_effect_pct'
N_A_EFFECT = 'N_no_manual_adj_A_effect_pct'
N_NEW = 'N_new'
D_NEW = 'D_new'
N_OLD_INFL = 'N_no_manual_adj_old_inflated'
D_OLD_INFL = 'D_no_manual_adj_old_inflated'
BLY_COL = 'BLy (MtCO2e)'
ELECTRICITY_SECTOR = '221100'
