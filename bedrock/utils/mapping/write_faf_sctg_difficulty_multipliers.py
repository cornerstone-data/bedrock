"""Generate the FAF SCTG difficulty multipliers for water and air margin (#611).

Water and air are the only two modes BEA allocates on **volume**, and they do not
allocate on raw ton-miles:

    *"We use a similar methodology for both air and water where we use ton-miles
    from the BTS/Census Commodity Flow Statistics. We do make adjustments to the
    data, though. We have a multiplier of 1, 2 or 3 based on the difficulty of
    transporting the commodity... These weights are only updated every 5 years."*
    - W. Nicolls, BEA, 2026-08-11

So the allocator is a **weighted** ton-mile share,
``m_c * tonmiles_c / sum(m_i * tonmiles_i)`` with ``m`` in ``{1, 2, 3}``.

⚠️ **BEA will not share the table, but gave the rule finely enough to rebuild
it.** This file is that reconstruction, not a copy:

    *"I can't share the table with you, but I can give you a push in the right
    direction. Air is simple, everything except animal is a 1 (animals is 3). For
    water, we see motorized vehicles and transport as the most difficult (highest
    multiplier). For everything else, if it would be palletized or put in a
    container, it would receive a 2 and if it sits loose on board, it receives a
    1. That should get you pretty close."* - W. Nicolls, BEA, 2026-08-17

Air is fully specified by that quote and needs no judgement. **Water needs one
call per SCTG**: whether the commodity rides loose in the hold or is palletized.
The rule is physical and the first reply gave worked examples - *"products like
grain or oil that sit free in the cargo hold stay unadjusted... Products that
don't sit free and are palletized, but are rearranged fairly easily get a
multiplier of 2 and heavy machinery receive a multiplier of 3"* - so bulk dry and
liquid cargoes take 1, general cargo takes 2, and vehicles and heavy machinery
take 3.

This governs 3.8% of ``TRANS``, so the cost of being somewhat wrong is bounded.
The weights are refreshed by BEA only every five years, which makes them a
constant in any annual construction.
"""

import pandas as pd

CROSSWALK_PATH = 'bedrock/utils/mapping/Crosswalk_FAF_SCTG_Difficulty_Multiplier.csv'

#: Air: everything is 1 except animals, which is 3. Stated outright by BEA.
AIR_HIGH = ('Live animals/fish',)

#: Water 1 - rides loose in the hold. Dry and liquid bulk: BEA named grain and
#: oil as the examples of cargo that "sits free in the cargo hold".
WATER_LOOSE = (
    'Animal feed',
    'Basic chemicals',
    'Building stone',
    'Cereal grains',
    'Coal',
    'Crude petroleum',
    'Fertilizers',
    'Fuel oils',
    'Gasoline',
    'Gravel',
    'Logs',
    'Metallic ores',
    'Natural gas and other fossil products',
    'Natural sands',
    'Nonmetallic minerals',
    'Waste/scrap',
)

#: Water 3 - the hardest to rearrange after a port call. BEA named "motorized
#: vehicles and transport" in the second reply and "heavy machinery" in the first.
WATER_HIGH = (
    'Machinery',
    'Motorized vehicles',
    'Transport equip.',
)

BASIS_AIR = {
    3: 'BEA: "Air is simple, everything except animal is a 1 (animals is 3)."',
    1: 'BEA: everything other than animals is 1.',
}
BASIS_WATER = {
    1: 'Bulk cargo that sits free in the hold. BEA named grain and oil as the '
    'examples of cargo that "stay unadjusted".',
    2: 'BEA: "if it would be palletized or put in a container, it would receive a 2".',
    3: 'BEA: "we see motorized vehicles and transport as the most difficult", and '
    'the first reply put heavy machinery at 3.',
}


def load_sctg_names() -> list[str]:
    """Every SCTG group FAF publishes, from the ported mode/SCTG crosswalk."""
    crosswalk = pd.read_csv(
        'bedrock/utils/mapping/activitytosectormapping/'
        'Sector_Crosswalk_BTS_FAF_Mode_and_SCTG.csv',
        dtype=str,
    )
    sctg = crosswalk[crosswalk['ActivitySourceName'] == 'FAF_SCTG']
    return sorted(set(sctg['Activity']))


def main() -> None:
    rows = []
    for sctg in load_sctg_names():
        air = 3 if sctg in AIR_HIGH else 1
        if sctg in WATER_HIGH:
            water = 3
        elif sctg in WATER_LOOSE:
            water = 1
        else:
            water = 2
        rows.append((sctg, air, water, BASIS_AIR[air], BASIS_WATER[water]))

    out = pd.DataFrame(
        rows,
        columns=[
            'sctg',
            'air_multiplier',
            'water_multiplier',
            'air_basis',
            'water_basis',
        ],
    )
    unknown = (set(AIR_HIGH) | set(WATER_HIGH) | set(WATER_LOOSE)) - set(out['sctg'])
    if unknown:
        raise ValueError(
            f'These SCTG names are assigned a multiplier but FAF does not publish '
            f'them: {sorted(unknown)}. A typo here silently leaves the commodity '
            f'on the default of 1 for air and 2 for water.'
        )

    print(f'{len(out)} SCTG groups')
    print(out.groupby('water_multiplier').size().rename('water counts').to_string())
    print(out.groupby('air_multiplier').size().rename('air counts').to_string())
    out.to_csv(CROSSWALK_PATH, index=False)
    print('written')


if __name__ == '__main__':
    main()
