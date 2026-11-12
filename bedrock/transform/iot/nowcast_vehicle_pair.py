"""Split the car and light-truck imports so that the car row balances (#1270).

No annual source separates cars (``336111``) from light trucks and utility
vehicles (``336112``) the way BEA's 2017 I-O schema does (#1294):

- Census trade classifies by HS code, which puts SUVs with passenger cars, and
  from 2023 publishes only the combined ``336110``. So the trade method splits
  the pair total by frozen 2017 shares (cars 33.9% of imports, 42.6% of
  exports).
- Output is split every year by NIPA's auto share (#570), which falls from
  0.178 in 2017 to 0.043 in 2024.

So the car row's supply runs ahead of its use. The Step 5 balance holds only
the imports column total, so it settles the gap on the car export cell, which
nearly doubled in 2021-2022 ($49-51bn against $25-27bn of Census exports).
Cars then export more than they produce, and their imports exceed their use.

The pair's import total is observed; its split is not. So the seed's car
imports are set to whatever closes the car row: the row's use (intermediate
plus final demand, exports included) less the rest of its supply. Light trucks
get the remainder, so the pair total stays on the conditioned Census value.
Customs duties move with the imports at the pair's own duty rate. The balance
still has the last word; the seed just stops handing it a car row that can't
close.
"""

from __future__ import annotations

import pandas as pd

from bedrock.utils.logging.flowsa_log import log

#: Cars first; the second row takes the remainder.
VEHICLE_PAIR: tuple[str, str] = ('336111', '336112')
IMPORTS = 'MCIF'
DUTIES = 'MDTY'


def split_vehicle_pair_imports(
    use: pd.DataFrame, supply: pd.DataFrame, pair: tuple[str, str] = VEHICLE_PAIR
) -> pd.DataFrame:
    """*supply* with the pair's imports and duties split so the first row balances.

    *use* and *supply* are the Step 5 seeds on commodity rows, same units.
    Supply's columns are industries plus the bridge columns (imports, margins,
    taxes), so a row's sum is total supply at purchasers' prices. Use's columns
    are industries plus final demand, so a row's sum over them is total use.

    The pair's import and duty totals are kept. The first row's imports are
    held between zero and the pair total; when the bound binds, the year is
    logged and the balance closes the rest.
    """
    cars, trucks = pair
    out = supply.copy()
    m_pair = float(out.loc[[cars, trucks], IMPORTS].sum())
    if m_pair <= 0.0:
        return out
    duty_rate = float(out.loc[[cars, trucks], DUTIES].sum()) / m_pair

    gap = float(use.loc[cars].sum()) - float(out.loc[cars].sum())
    m_cars_seed = float(out.loc[[cars], IMPORTS].sum())
    m_cars = m_cars_seed + gap / (1.0 + duty_rate)
    bounded = min(max(m_cars, 0.0), m_pair)
    if bounded != m_cars:
        log.warning(
            f'vehicle pair: car imports of {m_cars:,.0f} would leave [0, '
            f'{m_pair:,.0f}]; held at {bounded:,.0f}, and the balance closes '
            'the rest'
        )
    log.info(
        f'vehicle pair: car imports {m_cars_seed:,.0f} -> '
        f'{bounded:,.0f} of a {m_pair:,.0f} pair total (car row gap {gap:,.0f})'
    )
    out.loc[cars, IMPORTS] = bounded
    out.loc[trucks, IMPORTS] = m_pair - bounded
    out.loc[cars, DUTIES] = duty_rate * bounded
    out.loc[trucks, DUTIES] = duty_rate * (m_pair - bounded)
    return out
