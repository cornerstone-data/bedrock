"""Rebuild one nowcast year at this commit: trade, Steps 5-7, then Energy and GHG FBS.

Usage:
  uv run python scripts/rebuild_nowcast_year.py --year 2024 --stage tables
  uv run python scripts/rebuild_nowcast_year.py --year 2024 --stage ghg

The tables (trade, Steps 5-7) of every year build from the published 2017
benchmark, never from another nowcast year, so they run one CI job a year
(``.github/workflows/rebuild_nowcast.yml``). The Energy and GHG FBS do read
other years: manufacturing energy is a MECS year shared out on that year's
Use table. So the ``ghg`` stage runs once every year's tables are in place.

FlowByActivity and FlowBySector tables are built from source at this commit,
not downloaded: a published copy can predate the code (the 2017 trade FBS
did, #1270). The exceptions are the loaders that download on purpose
(``download_FBA_if_missing=True``, such as the Census expense surveys). Raw
inputs come from GCS and the source APIs, which need the Census, EIA and
USDA keys.
Nothing is uploaded either; the job keeps the outputs as a workflow artifact.

Steps 5-7 run under ``2026_usa_cornerstone_v1_0``, then the year's Energy and
GHG FBS under ``2026_usa_cornerstone_v1_0_<year>``, with
``nowcast_mut_vintage`` pointed at the tables this run just built.
"""

from __future__ import annotations

import argparse
import glob
import os
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
EXTRACT = REPO / 'bedrock' / 'extract' / 'output_data'
TRANSFORM = REPO / 'bedrock' / 'transform' / 'output_data'
TRADE_INPUT = REPO / 'bedrock' / 'extract' / 'input_data' / 'Census_USATrade'
SPAN_CONFIG = '2026_usa_cornerstone_v1_0.yaml'
#: Every flag the rebuild tests; each must be on in the span config.
FLAGS = (
    'electric_output_on_retail_revenue',
    'electricity_accounting_residual',
    'carry_power_petroleum_on_price',
    'reprice_oil_gas_extraction_seed',
    'hold_electric_gas_on_eia',
    'electric_output_on_eia_level',
    'electricity_delivery_margin',
    'fill_withheld_aies_codes',
    'hold_aies_outliers_on_bea',
    'chain_gas_distribution_on_aies',
    'chain_transport_margins_across_aies',
    'cap_margins_at_purchase',
    'pin_refinery_secondaries_at_go_mix',
    'air_margin_on_aies_level',
)
AFTER_MUT = 'Nowcast_Detail_Use_AfterRedef'
GHG_METHODS = (
    'Energy_manufacturing_national_nowcast_{y}',
    'Energy_highway_fuel_shares_national_{y}',
    'Energy_industrial_distillate_national_{y}',
    'GHG_national_Cornerstone_{y}',
)
#: GHGI tables the GHG FBS read that no source config here generates any more:
#: they exist only as published flowsa FBAs, the copies a local build reads.
PUBLISHED_ONLY_FBAS = (
    'EPA_GHGI_T_3_25',
    'EPA_GHGI_T_5_17',
    'EPA_GHGI_T_5_18',
    'EPA_GHGI_T_5_19',
)
#: The Census FBA must match the HS10 product-code totals to within this ($).
HS10_TOLERANCE = 2e8


def log(msg: str) -> None:
    print(f'[{time.strftime("%H:%M:%S")}] {msg}', flush=True)


def git_hash() -> str:
    return subprocess.run(
        ['git', 'rev-parse', '--short=7', 'HEAD'],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def served(directory: Path, prefix: str) -> str | None:
    files = sorted(
        glob.glob(str(directory / f'{prefix}_v*.parquet')), key=os.path.getmtime
    )
    return files[-1] if files else None


def built_here(directory: Path, prefix: str, tag: str) -> str:
    path = served(directory, prefix)
    if not path or tag not in os.path.basename(path):
        raise SystemExit(f'{prefix} not built at {tag}: {path}')
    return path


def check_flags() -> None:
    from bedrock.utils.config.usa_config import get_usa_config

    cfg = get_usa_config()
    off = [f for f in FLAGS if getattr(cfg, f, None) is not True]
    if off:
        raise SystemExit(f'{SPAN_CONFIG}: flags not on: {off}')
    log(f'{SPAN_CONFIG}: all {len(FLAGS)} flags on')


def check_trade_fba(year: int, tag: str) -> None:
    """The Census FBA reconciles to the HS10 totals (gold sits on its own activity)."""
    import pandas as pd

    path = built_here(EXTRACT, f'Census_USATrade_{year}', tag)
    got = pd.read_parquet(path).groupby('FlowName')['FlowAmount'].sum()
    for flow, field, name, df_code in (
        ('exports', 'ALL_VAL_YR', 'ALL_VAL_YR_DOM', '1'),
        ('imports', 'GEN_CIF_YR', 'GEN_CIF_YR', None),
    ):
        raw = pd.read_csv(
            TRADE_INPUT / str(year) / f'Census_USATrade_{year}_{flow}_hs10.csv',
            dtype=str,
        )
        if df_code:
            raw = raw[raw['DF'] == df_code]
        expect = pd.to_numeric(raw[field]).sum()
        log(
            f'  {os.path.basename(path)} {name}: {got[name] / 1e9:,.2f}bn '
            f'vs HS10 {expect / 1e9:,.2f}bn'
        )
        if abs(got[name] - expect) > HS10_TOLERANCE:
            raise SystemExit(f'{path} {name} off the HS10 total')


def build_trade(years: list[int], tag: str) -> None:
    """Trade FBA and FBS before Step 5, so Step 5 conditions on this commit's trade."""
    from bedrock.extract.flowbyactivity import getFlowByActivity
    from bedrock.transform.flowbysector import FlowBySector

    for year in years:
        getFlowByActivity('Census_USATrade', year)
        check_trade_fba(year, tag)
        for method in (f'Trade_Imports_{year}', f'Trade_Exports_{year}'):
            FlowBySector.generateFlowBySector(method, download_sources_ok=False)
            log(f'  {os.path.basename(built_here(TRANSFORM, method, tag))}')


def run_step(module: str, *args: str) -> None:
    log(f'start {module} {" ".join(args)}')
    t = time.time()
    env = {**os.environ, 'USA_CONFIG_FILE': SPAN_CONFIG}
    rc = subprocess.run(
        [sys.executable, '-m', module, *args], cwd=REPO, env=env
    ).returncode
    log(f'end {module}: exit {rc} in {(time.time() - t) / 60:.1f} min')
    if rc:
        raise SystemExit(rc)


def fetch_published_only_fbas() -> None:
    """Download the FBAs nothing here can generate, every year GCS holds."""
    from bedrock.extract.generateflowbyactivity import _download_fba_from_gcs
    from bedrock.utils.validation.exceptions import APIError

    for source in PUBLISHED_ONLY_FBAS:
        got = []
        for year in range(2012, 2025):
            try:
                _download_fba_from_gcs(source, str(year))
                got.append(year)
            except APIError:
                pass
        log(f'  {source}: published copies for {got}')


def build_ghg(year: int, mut: str, tag: str) -> None:
    """The year's Nowcast Use FBA from this run's MUT, then the Energy and GHG FBS."""
    from bedrock.extract.flowbyactivity import getFlowByActivity
    from bedrock.transform.flowbysector import FlowBySector
    from bedrock.utils.config.usa_config import get_usa_config

    get_usa_config().nowcast_mut_vintage = mut
    if get_usa_config().nowcast_mut_vintage != mut:
        raise SystemExit('MUT override did not take')
    getFlowByActivity(AFTER_MUT, year)
    log(f'  {os.path.basename(built_here(EXTRACT, f"{AFTER_MUT}_{year}", tag))}')
    for template in GHG_METHODS:
        method = template.format(y=year)
        t = time.time()
        FlowBySector.generateFlowBySector(method, download_sources_ok=False)
        log(
            f'  {os.path.basename(built_here(TRANSFORM, method, tag))} '
            f'in {(time.time() - t) / 60:.1f} min'
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--year', type=int, required=True)
    parser.add_argument(
        '--stage',
        choices=('tables', 'ghg'),
        required=True,
        help='tables: trade and Steps 5-7. ghg: the Energy and GHG FBS, which '
        'need every year\'s tables first (manufacturing energy for 2023 is '
        'MECS 2022 shared out on the 2022 Use table)',
    )
    args = parser.parse_args()
    year = args.year

    if subprocess.run(
        ['git', 'status', '--porcelain', '--untracked-files=no'],
        cwd=REPO,
        capture_output=True,
        text=True,
    ).stdout.strip():
        raise SystemExit('working tree is dirty; the build hash would lie')
    tag = git_hash()
    mut = f'v1.0.0_{tag}'
    log(f'{year} {args.stage} at {tag}; build {mut}')

    if args.stage == 'ghg':
        os.environ['USA_CONFIG_FILE'] = f'2026_usa_cornerstone_v1_0_{year}.yaml'
        from bedrock.utils.config import common

        common.download_fba_on_api_error = False
        fetch_published_only_fbas()
        build_ghg(year, mut, tag)
        log(f'GHG DONE {year}: build {mut}; nothing uploaded')
        return

    os.environ['USA_CONFIG_FILE'] = SPAN_CONFIG
    from bedrock.utils.config import common

    common.download_fba_on_api_error = False
    check_flags()

    # 2017 is the import and export anchor every later year is conditioned against.
    build_trade(sorted({2017, year}), tag)

    run_step('bedrock.transform.iot.nowcast_sut_assembly', '--years', str(year))
    for table in ('Supply', 'Use_SUT'):
        built_here(TRANSFORM, f'Balanced_Detail_{table}_{year}', tag)
    run_step('bedrock.transform.iot.nowcast_mut', '--year', str(year))
    run_step('bedrock.transform.iot.nowcast_redefinitions', '--year', str(year))
    log(f'TABLES DONE {year}: build {mut}; nothing uploaded')


if __name__ == '__main__':
    main()
