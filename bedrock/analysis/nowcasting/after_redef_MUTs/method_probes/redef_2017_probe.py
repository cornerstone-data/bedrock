"""The two decisive 2017 redefinition tests.

(a) Whole-cell or partial: for off-diagonal Make cells that lose mass between
    before- and after-redefinitions, what fraction of the cell moves? If the
    mapped pairs move wholly, the pattern alone is the method - no ratios.
    Plus the invariant: commodity output unchanged, industry output moves.

(b) Whose recipe moves on the Use side: reconstruct the after-redef Use
    interior from the before table plus the observed Make movement, once with
    the donor industry's input recipe and once with the receiver's, and score
    both against the published after-redef Use. The do-nothing baseline (score
    the before table itself) anchors the improvement.
"""

import numpy as np
import pandas as pd

from bedrock.extract.iot.io_2017 import (
    load_2017_Utot_after_redef_usa,
    load_2017_Utot_before_redef_usa,
    load_2017_V_after_redef_usa,
    load_2017_V_before_redef_usa,
)

M = 1e6

V_b = load_2017_V_before_redef_usa().astype(float)
V_a = load_2017_V_after_redef_usa().astype(float)
U_b = load_2017_Utot_before_redef_usa().astype(float)
U_a = load_2017_Utot_after_redef_usa().astype(float)
# Align labels (indexes are named differently across loaders).
V_a = pd.DataFrame(V_a.to_numpy(), index=V_b.index, columns=V_b.columns)
U_a = pd.DataFrame(U_a.to_numpy(), index=U_b.index, columns=U_b.columns)

industries = list(V_b.index)
commodities = list(V_b.columns)

# ---------- (a) whole-cell vs partial -------------------------------------
print('=== (a) Make-side movement anatomy, 2017 detail ===')
diff = V_a - V_b

com_out_gap = (diff.sum(axis=0)).abs()
ind_out_move = diff.sum(axis=1)
print(
    f'commodity output invariance: max |column-sum change| ${com_out_gap.max()/M:,.1f}M'
)
print(
    f'industry output moved: gross ${ind_out_move.abs().sum()/2/M:,.0f}M across '
    f'{int((ind_out_move.abs() > 0.5*M).sum())} industries'
)

on_diag = pd.Series(
    {c: diff.at[c, c] for c in commodities if c in diff.index}, dtype=float
)
off = diff.copy()
for c in on_diag.index:
    off.at[c, c] = 0.0

losses = off[off < -0.5 * M].stack()  # (industry, commodity) cells that shrank
gains = off[off > 0.5 * M].stack()
print(
    f'off-diagonal cells losing mass: {len(losses):,} '
    f'(${-losses.sum()/M:,.0f}M); gaining: {len(gains):,} (${gains.sum()/M:,.0f}M)'
)
print(
    f'diagonal cells gaining: {int((on_diag > 0.5*M).sum())} '
    f'(${on_diag[on_diag > 0].sum()/M:,.0f}M); losing: {int((on_diag < -0.5*M).sum())}'
)

before_vals = pd.Series(
    [V_b.at[i, c] for i, c in losses.index], index=losses.index, dtype=float
)
frac = (-losses) / before_vals
bins = pd.cut(
    frac,
    [0, 0.25, 0.5, 0.75, 0.95, 0.999, 1.0001, np.inf],
    labels=['<25%', '25-50%', '50-75%', '75-95%', '95-99.9%', '~100%', '>100%'],
)
by_count = bins.value_counts().reindex(bins.cat.categories)
by_mass = (-losses).groupby(bins, observed=False).sum().reindex(bins.cat.categories) / M
print('\nmove fraction of the before-redef cell (losing cells):')
for label in bins.cat.categories:
    print(f'  {label:>9}: {by_count[label]:5d} cells   ${by_mass[label]:>12,.0f}M')

# ---------- (b) Use-side recipe test --------------------------------------
print('\n=== (b) Use reconstruction: donor vs receiver recipe ===')
x_b = V_b.sum(axis=1)  # industry output before redefinitions, USD

# Movement list: for each commodity, donors are its losing off-diagonal cells,
# the receiver is the diagonal (primary) industry.
moves = []  # (donor industry, receiver industry, amount moved)
skipped = 0.0
for (i, c), lost in (-losses).items():
    if c in V_b.index:  # the commodity's primary industry shares its code
        moves.append((i, c, float(lost)))
    else:
        skipped += lost
print(f'movements: {len(moves):,}; skipped (no primary industry): ${skipped/M:,.0f}M')

recipe = U_b.div(x_b.replace(0.0, np.nan), axis=1).fillna(0.0)  # per $ of output


def reconstruct(structure_of: str) -> pd.DataFrame:
    test = U_b.copy()
    for donor, receiver, amount in moves:
        col = donor if structure_of == 'donor' else receiver
        vec = recipe[col].to_numpy() * amount
        test[donor] = test[donor].to_numpy() - vec
        test[receiver] = test[receiver].to_numpy() + vec
    return test


target_mass = U_a.abs().sum().sum()


def score(name: str, table: pd.DataFrame) -> None:
    err = (table - U_a).abs()
    col_err = (table.sum(axis=0) - U_a.sum(axis=0)).abs()
    print(
        f'{name:>16}: cell L1 ${err.sum().sum()/M:>12,.0f}M '
        f'({err.sum().sum()/target_mass:6.2%} of after-table mass); '
        f'column-total L1 ${col_err.sum()/M:>10,.0f}M'
    )


score('do nothing', U_b)
score('donor recipe', reconstruct('donor'))
score('receiver recipe', reconstruct('receiver'))

# where the residual concentrates for the better variant
for name in ('donor', 'receiver'):
    resid = (reconstruct(name) - U_a).abs().sum(axis=0)
    top = resid.sort_values(ascending=False).head(8) / M
    print(f'\ntop residual industries ({name} recipe):')
    for code, val in top.items():
        print(f'  {code}: ${val:,.0f}M')
