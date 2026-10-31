"""Which 2022-specific input makes own use bump in 2022 (#890).

Rebalances 2022 with one 2022-specific input switched off at a time and measures
the own-use bump ratio of :mod:`own_use_2022` against the build's own 2021 and
2023. Each variant runs in its own process, because the switches are a
process-wide config and ``functools.cache`` caches.

Variants
--------

``baseline``
    Nothing switched off. Reproduces the build's 2022, as a control on the
    driver itself.
``no_ec_gross_output``
    Economic Census conditioning of gross output (#724/#779) undone for 2022:
    2022 reverts to BEA's ``UGO305-A``. 2023-24 are not rebuilt.
``census_alpha_0``
    ``census_materials_index_alpha = 0``: the manufacturing materials and mining
    seeds hold their 2017 mix, so the 2022 Economic Census mix is not used.
``services_seed_2021``
    The services and transport expense seed for 2022 is its 2021 seed. 2022 is
    the first sas-22 (2022-census-benchmarked) observation the seed reads
    against a sas-17 base, so this holds that rebenchmark out of 2022.
``nonmaterial_seed_2021``
    The manufacturing non-materials (fuel, electricity, services) seed for 2022
    is its 2021 seed; with ``smooth_manufacturing_expense_path`` it is
    census-exact in 2022.

The 2022 census supply mix (Step 4a) has no clean single switch and is not a
variant; see #890.

⚠️ **It only runs on a commit the nowcast has been built on.** Every variant
reads the build's flow-by-sector parquets, and ``getFlowBySector`` regenerates a
missing one (hours, and it writes). So the driver refuses unless the balanced
2021-2023 parquets for the current commit exist, and nothing should be committed
between the build and this run. It writes only its own outputs, under
``--out``.

CLI::

    uv run python -m bedrock.analysis.nowcasting.own_use_2022_ablation --check
    uv run python -m bedrock.analysis.nowcasting.own_use_2022_ablation \\
        --out <dir>
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import typing as ta

import numpy as np
import pandas as pd

from bedrock.analysis.nowcasting.own_use_2022 import (
    BUMP_YEAR,
    WATCH_SUMMARY,
    bump_ratio,
    published_summary_own_use,
    summary_use,
)
from bedrock.utils.config.settings import FBS_DIR, GIT_HASH, PKG_VERSION_NUMBER

VARIANTS = (
    'baseline',
    'no_ec_gross_output',
    'census_alpha_0',
    'services_seed_2021',
    'nonmaterial_seed_2021',
)


def build_vintage() -> str:
    """The vintage the balanced parquets of the current commit are named by."""
    return f'v{PKG_VERSION_NUMBER}_{GIT_HASH}'


def _require_build(vintage: str) -> None:
    missing = [
        name
        for year in (BUMP_YEAR - 1, BUMP_YEAR, BUMP_YEAR + 1)
        for name in (f'Balanced_Detail_Use_SUT_{year}_{vintage}.parquet',)
        if not os.path.exists(os.path.join(str(FBS_DIR), name))
    ]
    if missing:
        raise RuntimeError(
            f'No nowcast build for this commit ({vintage}): {missing} not found. '
            f'Run the ablation on the commit the build ran on, with nothing '
            f'committed since, or every variant would regenerate the FBS.'
        )


def _patch(variant: str) -> None:
    """Switch one 2022-specific input off. Called before anything is computed."""
    if variant == 'baseline':
        return
    if variant == 'no_ec_gross_output':
        from bedrock.transform.iot import ec_go_adjustment  # noqa: PLC0415

        original = ec_go_adjustment.apply_ec_adjustment

        def without_2022(raw: pd.DataFrame) -> pd.DataFrame:
            out = original(raw)
            out[BUMP_YEAR] = raw[BUMP_YEAR]
            return out

        ec_go_adjustment.apply_ec_adjustment = without_2022
        return
    if variant == 'census_alpha_0':
        from bedrock.utils.config.usa_config import (  # noqa: PLC0415
            get_usa_config,
            reset_usa_config,
            set_global_usa_config_object,
        )

        config = get_usa_config().model_copy(
            update={'census_materials_index_alpha': 0.0}
        )
        # get_usa_config records the loaded file in the environment, which the
        # setter's already-set guard reads, so clear both.
        reset_usa_config(should_reset_env_var=True)
        set_global_usa_config_object(config, source_label='own_use_2022 alpha 0')
        return
    module_name, function_name = {
        'services_seed_2021': (
            'bedrock.analysis.nowcasting.services_transport_expense_seed',
            'services_transport_seed',
        ),
        'nonmaterial_seed_2021': (
            'bedrock.analysis.nowcasting.inputs_structure',
            'nonmaterial_seed',
        ),
    }[variant]
    module = __import__(module_name, fromlist=[function_name])
    seed = getattr(module, function_name)

    def held(year: int, *args: ta.Any, **kwargs: ta.Any) -> pd.DataFrame:
        return seed(BUMP_YEAR - 1 if year == BUMP_YEAR else year, *args, **kwargs)

    setattr(module, function_name, held)


def run_variant(variant: str, out: str) -> str:
    """Balance 2022 under *variant* and write its Use block, $M."""
    _require_build(build_vintage())
    _patch(variant)
    from bedrock.transform.iot.nowcast_sut_assembly import (  # noqa: PLC0415
        balance_year,
        prepared_balanced_blocks,
    )

    blocks = prepared_balanced_blocks(balance_year(BUMP_YEAR))
    os.makedirs(out, exist_ok=True)
    path = os.path.join(out, f'use_{BUMP_YEAR}_{variant}.parquet')
    blocks['use'][0].to_parquet(path)
    supply_path = os.path.join(out, f'supply_{BUMP_YEAR}_{variant}.parquet')
    blocks['supply'][0].to_parquet(supply_path)
    return path


def _summary_diagonal(use_m: pd.DataFrame) -> pd.Series:
    from bedrock.analysis.nowcasting import own_use_2022  # noqa: PLC0415

    detail = (
        use_m.reindex(
            index=list(own_use_2022.USA_2017_COMMODITY_CODES),
            columns=list(own_use_2022.USA_2017_INDUSTRY_CODES),
        ).fillna(0.0)
        * own_use_2022.MILLIONS
    )
    rows = detail.index.map(
        own_use_2022._first(
            own_use_2022.load_bea_v2017_commodity_to_bea_v2017_summary()
        )
    )
    cols = detail.columns.map(
        own_use_2022._first(own_use_2022.load_bea_v2017_industry_to_bea_v2017_summary())
    )
    s = detail.groupby(rows).sum().T.groupby(cols).sum().T
    codes = s.index.intersection(s.columns)
    return pd.Series(np.diag(s.loc[codes, codes]), index=codes)


def compare(out: str) -> pd.DataFrame:
    """Bump ratio of each variant's 2022 against the build's 2021 and 2023."""
    vintage = build_vintage()
    neighbours: dict[int, pd.Series] = {}
    for year in (BUMP_YEAR - 1, BUMP_YEAR + 1):
        s = summary_use(vintage, year)
        codes = s.index.intersection(s.columns)
        neighbours[year] = pd.Series(np.diag(s.loc[codes, codes]), index=codes)
    table = {}
    for variant in VARIANTS:
        path = os.path.join(out, f'use_{BUMP_YEAR}_{variant}.parquet')
        if not os.path.exists(path):
            continue
        diag = _summary_diagonal(pd.read_parquet(path))
        series = pd.DataFrame(
            {BUMP_YEAR - 1: neighbours[BUMP_YEAR - 1], BUMP_YEAR: diag,
             BUMP_YEAR + 1: neighbours[BUMP_YEAR + 1]}
        )  # fmt: skip
        table[variant] = series.apply(bump_ratio, axis=1)
    pub = published_summary_own_use()
    table['published'] = pub.apply(bump_ratio, axis=1)
    frame = pd.DataFrame(table)
    return frame.reindex([c for c in WATCH_SUMMARY if c in frame.index])


def check() -> None:
    """The guard refuses a commit with no build; the variants are all wired."""
    try:
        _require_build('v0.0.0_nobuild')
    except RuntimeError as err:
        assert 'No nowcast build' in str(err)
    else:
        raise AssertionError('the build guard let a missing build through')
    from bedrock.analysis.nowcasting import (  # noqa: PLC0415
        inputs_structure,
        services_transport_expense_seed,
    )
    from bedrock.transform.iot import ec_go_adjustment  # noqa: PLC0415

    assert callable(ec_go_adjustment.apply_ec_adjustment)
    assert callable(services_transport_expense_seed.services_transport_seed)
    assert callable(inputs_structure.nonmaterial_seed)
    print(f'check passed; this commit is {build_vintage()}')


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--out', help='directory for the variants\' 2022 tables')
    parser.add_argument('--variant', choices=VARIANTS, help='run one variant only')
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    if args.check:
        check()
        return
    if not args.out:
        parser.error('--out is required')
    if args.variant:
        print(run_variant(args.variant, args.out))
        return
    _require_build(build_vintage())
    for variant in VARIANTS:
        subprocess.run(
            [sys.executable, '-m', __spec__.name, '--variant', variant,
             '--out', args.out],
            check=True,
        )  # fmt: skip
    pd.set_option('display.width', 200)
    print(compare(args.out).round(2).to_string())


if __name__ == '__main__':
    main()
