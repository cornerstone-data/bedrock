"""Compose ``USDA_ERS_FIWS`` activities onto BEA 2017 detail commodities.

``Sector_Crosswalk_USDA_ERS_FIWS.csv`` maps each ERS commodity to a
**NAICS 2012** code, and ``NAICS_to_BEA_Crosswalk_2017.csv`` carries a
``NAICS_2012_Code`` column beside ``BEA_2017_Detail_Code``. Composing the two
gives ERS commodity -> BEA commodity without writing a new mapping by hand,
which is what lets ERS cash receipts weight the farm branch of ``F03000``
(#529) against a BEA target.

⚠️ **ERS publishes NAICS codes that NAICS does not.** ``Celery`` is
``111219I``, ``Hops`` is ``111998E`` -- ERS extends the 6-digit code with a
letter to separate crops inside a published NAICS cell. Those never match
``NAICS_to_BEA_Crosswalk_2017`` directly, so the resolution walks the code down
one character at a time and takes the longest prefix that does match, exactly as
``inputs_structure.classify`` does for the materials census.

⚠️ **``All Commodities`` is deliberately dropped.** Its sector is the range
``111-112``, which is a total rather than a code, and the two groups it covers
are already carried separately -- keeping it would double the farm branch.

CLI::

    uv run python -m bedrock.utils.mapping.write_fiws_bea_crosswalk
    uv run python -m bedrock.utils.mapping.write_fiws_bea_crosswalk --check
"""

from __future__ import annotations

import sys
from pathlib import Path

import click
import pandas as pd

MAPPING_DIR = Path('bedrock/utils/mapping')
FIWS_CROSSWALK = (
    MAPPING_DIR / 'activitytosectormapping' / 'Sector_Crosswalk_USDA_ERS_FIWS.csv'
)
NAICS_TO_BEA = MAPPING_DIR / 'naics' / 'NAICS_to_BEA_Crosswalk_2017.csv'
OUTPUT = (
    MAPPING_DIR / 'activitytosectormapping' / 'Sector_Crosswalk_USDA_ERS_FIWS_BEA.csv'
)

#: Sectors that are ranges or totals rather than codes.  See the module note.
SKIP_SECTORS = ('-',)


def _naics_to_bea() -> dict[str, list[str]]:
    """``NAICS 2012 -> [BEA 2017 detail]``, one entry per published NAICS."""
    frame = pd.read_csv(NAICS_TO_BEA, dtype=str)
    pairs = frame[['NAICS_2012_Code', 'BEA_2017_Detail_Code']].dropna()
    return (
        pairs.drop_duplicates()
        .groupby('NAICS_2012_Code')['BEA_2017_Detail_Code']
        .apply(lambda codes: sorted(set(codes)))
        .to_dict()
    )


def resolve(naics: str, lookup: dict[str, list[str]]) -> tuple[str | None, list[str]]:
    """Longest prefix of *naics* that is a published NAICS, and its commodities."""
    code = str(naics).strip()
    for length in range(len(code), 1, -1):
        prefix = code[:length]
        if prefix in lookup:
            return prefix, lookup[prefix]
    return None, []


def build() -> pd.DataFrame:
    """The composed crosswalk, in ``activitytosectormapping`` shape."""
    fiws = pd.read_csv(FIWS_CROSSWALK, dtype=str)
    lookup = _naics_to_bea()

    records = []
    for activity, sector in zip(
        fiws['Activity'].astype(str), fiws['Sector'].astype(str), strict=True
    ):
        if any(token in sector for token in SKIP_SECTORS):
            continue
        prefix, commodities = resolve(sector, lookup)
        for commodity in commodities:
            records.append(
                {
                    'ActivitySourceName': 'USDA_ERS_FIWS',
                    'Activity': activity,
                    'SectorSourceName': 'BEA_2017_Code',
                    'Sector': commodity,
                    'SectorType': None,
                    'Note': f'{sector} via NAICS {prefix}',
                }
            )
    return pd.DataFrame(records).drop_duplicates(subset=['Activity', 'Sector'])


def check() -> int:
    """Assert the composition still reaches the farm block.  Process exit code."""
    fiws = pd.read_csv(FIWS_CROSSWALK, dtype=str)
    out = build()
    failures = []

    skipped = [
        s for s in fiws['Sector'].astype(str) if any(t in s for t in SKIP_SECTORS)
    ]
    resolved = out['Activity'].nunique()
    expected = fiws['Activity'].nunique() - len(set(skipped))
    if resolved < expected:
        missing = sorted(set(fiws['Activity']) - set(out['Activity']))
        failures.append(f'{len(missing)} ERS activities reach no commodity: {missing}')

    farm = {
        '1111A0',
        '1111B0',
        '111200',
        '111300',
        '111400',
        '111900',
        '112120',
        '1121A0',
        '112300',
        '112A00',
    }
    reached = set(out['Sector'].astype(str))
    if not farm <= reached:
        failures.append(f'farm commodities not reached: {sorted(farm - reached)}')

    print(
        f'{len(out)} pairs, {resolved} activities, '
        f'{out["Sector"].nunique()} commodities'
    )
    print(f'farm block covered: {len(farm & reached)}/{len(farm)}')
    for failure in failures:
        print(f'FAIL {failure}')
    return 1 if failures else 0


@click.command()
@click.option('--check', 'run_check', is_flag=True, help='Assert without writing.')
def main(run_check: bool) -> None:
    """Write ``Sector_Crosswalk_USDA_ERS_FIWS_BEA.csv``."""
    if run_check:
        sys.exit(check())
    out = build()
    out.to_csv(OUTPUT, index=False)
    print(f'wrote {OUTPUT} - {len(out)} rows')


if __name__ == '__main__':
    main()
