"""Step 5 assembly: real-year seeds, masks and targets into the GRAS engine.

This is the module that finally points the balance at a *nowcast* year. The
engine (:mod:`~bedrock.transform.iot.nowcast_sut_gras`) has run on toys and on
the published-2017 replay; everything here is about assembling the three real
inputs for 2018-2023 on the labels the mask defines:

* **Use seed** - the fitted interior (#794's two-margin fit, so the seed
  already sits on both hard identities to within the fit's holds), the
  final-demand block (Step 1), and the six-row value-added block (Step 2).
* **Supply seed** - the GO-controlled commodity x industry block (Step 4a)
  and the bridge columns (Step 4c/4d), with the bridge's ``TRADE`` renamed to
  BEA's trailing-space ``TRADE `` and the derived subtotals dropped.
* **Targets** - :func:`~bedrock.transform.iot.nowcast_targets.build_target_set`
  with T1 **injected from the census-adjusted output panel** and T18 injected
  from the seed's own value-added column sums. Both injections keep the
  targets consistent with the interior fit's margins: the fit's column target
  is exactly ``T1 - T18``, so RAS starts from a seed that agrees with its own
  constraints instead of fighting them.

⚠️ **T18 hard-targets the seed's own value added, not a published allocated
series - deliberately.** BEA publishes no detail value added for 2018-2023;
the closest thing is an allocation of summary VA over the 2017 detail
structure, which is exactly the kind of carried-forward split the nowcast
exists to replace. The seed's VA columns are the Step-2 derived series -
built from observed compensation and tax data - and the interior fit already
balanced the intermediate block against them, so its column target is
``T1 - T18`` *for this T18*. Injecting any other series would put the target
set at war with the fit the seed came from and re-open the gap the fit closed.
When Step 2's VA improves, the fit and this injection both move with it.

Units: the nowcast seed derivations are **USD**; the balance, the mask panel
and the targets are **BEA million dollars**. Everything is converted to $M
here, at the seam, and nowhere else.

⚠️ **The dust sweep** (:func:`conform_seeds`). The mask machinery refuses any
seed that contradicts the mask - a single cell nonzero where the pattern says
structural zero, or on the wrong side of a sign lock, raises before the
balance starts. A year's seed carries publication-rounding dust on such cells,
which would be a hard stop over amounts below BEA's own rounding. Cells in
violation with ``|value| <= DUST_USD_M`` are therefore zeroed, and the sweep
is *returned* so the precheck can print every swept cell. Violations above
the threshold are **left in place** - they are real contradictions and the
right failure mode is the machinery's refusal, not a silent larger sweep.

Testing: no direct unit tests, on purpose. This module is wiring over heavy
cached derivations (the fit, the seeds, the GO panel), so a unit test would
either mock all of them - pinning the wiring to itself - or re-run them. The
per-year precheck gates (:mod:`~bedrock.analysis.nowcasting.ras_prechecks`)
are the
test: nine consistency checks on the real assembled objects, including the
exact ``T1 - T18`` / fit-column identity and the T1-arm reconciliation with
swept VA dust added back. The layers underneath (mask, targets) carry their
own unit suites.
"""

from __future__ import annotations

import argparse
import json
import posixpath
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import cast

import pandas as pd

from bedrock.transform.iot.derived_intermediate_and_value_added import (
    detail_gross_output_panel,
)
from bedrock.transform.iot.nowcast import (
    derive_initial_U_intermediate,
    derive_initial_value_added,
    derive_initial_Y_pur,
)
from bedrock.transform.iot.nowcast_interior_fit import FIT_YEARS, fit_interior
from bedrock.transform.iot.nowcast_mask import (
    BLOCKS,
    SUPPLY_BRIDGE_COLUMNS,
    VA_ROWS,
    balance_commodities,
    balance_industries,
    build_sut_mask,
    panel_labels,
)
from bedrock.transform.iot.nowcast_supply_go_control import go_controlled_supply_block
from bedrock.transform.iot.nowcast_sut_gras import SutBalanceResult, engine
from bedrock.transform.iot.nowcast_targets import (
    FD_TARGET_COLUMNS,
    build_target_set,
    industry_group_aggregator,
)
from bedrock.utils.config.settings import (
    FBS_DIR,
    GIT_BRANCH,
    GIT_HASH,
    GIT_HASH_LONG,
    PKG_VERSION_NUMBER,
)
from bedrock.utils.economic.balance.mask import SutMask
from bedrock.utils.economic.balance.offset import (
    offset_targets,
    restore_fixed_blocks,
    split_fixed_blocks,
)
from bedrock.utils.economic.balance.targets import TargetSet
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY

#: Sweep bound for mask-contradicting seed cells, in $M. BEA publishes no cell
#: below 1 million, so a violation at or under this is rounding, not signal.
DUST_USD_M = 1.0

#: The bridge derivation labels its trade column without BEA's trailing space.
_BRIDGE_RENAMES = {'TRADE': 'TRADE '}


def _million(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.astype(float) / MILLION_CURRENCY_TO_CURRENCY


def assemble_use_seed(year: int, *, fitted: bool = True) -> pd.DataFrame:
    """The Use block for *year* on the mask's labels, $M.

    ``fitted=True`` takes the two-margin fitted interior; ``False`` takes the
    raw Step-3 interior, which is what the fit itself starts from - useful for
    measuring what the fit bought.
    """
    rows, columns = panel_labels('use')
    panel = pd.DataFrame(0.0, index=list(rows), columns=list(columns))
    commodities = list(balance_commodities())
    industries = list(balance_industries())

    interior = (
        fit_interior(int(year)).interior
        if fitted
        else derive_initial_U_intermediate(int(year))
    )
    panel.loc[commodities, industries] = _million(interior).loc[commodities, industries]

    y = _million(derive_initial_Y_pur(int(year), download_sources_ok=True))
    fd = [c for c in y.columns if c in panel.columns]
    panel.loc[commodities, fd] = y.loc[commodities, fd]

    va = _million(derive_initial_value_added(int(year), download_sources_ok=True))
    panel.loc[list(VA_ROWS), industries] = va.loc[list(VA_ROWS), industries]
    return panel


def assemble_supply_seed(year: int) -> pd.DataFrame:
    """The Supply block for *year* on the mask's labels, $M."""
    rows, columns = panel_labels('supply')
    panel = pd.DataFrame(0.0, index=list(rows), columns=list(columns))
    commodities = list(balance_commodities())
    industries = list(balance_industries())

    block = _million(go_controlled_supply_block(int(year), download_sources_ok=True))
    panel.loc[commodities, industries] = block.loc[commodities, industries]

    from bedrock.transform.iot.nowcast import (  # noqa: PLC0415 - heavy import cycle
        derive_initial_supply_bridge,
    )

    bridge = _million(
        derive_initial_supply_bridge(int(year), download_sources_ok=True)
    ).rename(columns=_BRIDGE_RENAMES)
    keep = [c for c in SUPPLY_BRIDGE_COLUMNS if c in bridge.columns]
    panel.loc[commodities, keep] = bridge.loc[commodities, keep]
    return panel


def assemble_seeds(year: int, *, fitted: bool = True) -> dict[str, pd.DataFrame]:
    return {
        'use': assemble_use_seed(int(year), fitted=fitted),
        'supply': assemble_supply_seed(int(year)),
    }


def assemble_masks(year: int) -> dict[str, SutMask]:
    """The 2017-pattern masks, one per block. The pattern is deliberately
    2017's - see the mask module - so the same masks serve every year."""
    return {block: build_sut_mask(block, int(year)) for block in BLOCKS}


def assemble_targets(
    year: int, use_seed: pd.DataFrame, supply_seed: pd.DataFrame
) -> TargetSet:
    """The target set with hard and soft values injected from the seeds.

    T1 is the census-adjusted gross output panel - the same series the
    interior fit's column targets are built from, which is the injection #724
    owed the target set. T18 is the seed's own value-added column sums, which
    are the Step-2 derived series in $M; injecting them keeps ``T1 - T18``
    exactly equal to the fit's intermediate column target.

    The soft targets get real values from the same seeds, because each of
    these aggregates *is* the observed control its build was levelled to:
    the FD column totals are NIPA lines (Step 1), compensation is group-level
    observed (Step 2), the product-tax row totals are NIPA-levelled (#787),
    and the bridge column totals are the conditioned-import / NIPA-duty /
    NIPA-tax controls (Step 4). Injecting them keeps the soft layer pulling
    the balance back toward what was observed at entry rather than toward a
    2017 placeholder. T6/T8/T9 still whole-name defer to the hard identities
    T12-T14 inside the engine; their values are injected anyway so the set
    never carries a placeholder into a real run.
    """
    industries = list(balance_industries())
    go = (
        pd.to_numeric(
            detail_gross_output_panel(ec_adjusted=True)[int(year)], errors='coerce'
        )
        .reindex(industries)
        .fillna(0.0)
    )
    go.index.name = 'industry'
    vapro = use_seed.loc[list(VA_ROWS), industries].sum(axis=0).astype(float)
    vapro.index.name = 'industry'

    fd_totals = use_seed[list(FD_TARGET_COLUMNS)].sum(axis=0).astype(float)
    va_rows = use_seed.loc[list(VA_ROWS)]
    compensation = industry_group_aggregator().apply(
        cast('pd.Series[float]', va_rows.loc['V00100'])
    )
    tax_totals = pd.Series(
        {
            'T00TOP': float(va_rows.loc['T00TOP'][industries].sum()),
            'T00SUB': float(va_rows.loc['T00SUB'][industries].sum()),
        },
        dtype=float,
    )
    supply_totals = (
        supply_seed[['MCIF', 'MDTY', 'TOP', 'SUB']].sum(axis=0).astype(float)
    )
    return build_target_set(
        int(year),
        gross_output=go,
        value_added=vapro,
        fd_totals=fd_totals,
        compensation=compensation,
        tax_totals=tax_totals,
        supply_totals=supply_totals,
    )


def conform_seeds(
    seeds: dict[str, pd.DataFrame],
    masks: dict[str, SutMask],
    dust_usd_m: float = DUST_USD_M,
) -> pd.DataFrame:
    """Zero mask-contradicting seed cells at or under *dust_usd_m*, in place.

    Returns the sweep: one row per swept **or surviving** violation, with the
    block, cell, value and which layer it contradicts. Survivors (above the
    threshold) are not touched - the balance machinery will refuse them, and
    that refusal is the honest failure. The precheck prints this frame.
    """
    records = []
    for block, seed in seeds.items():
        mask = masks[block]
        structural = mask.structural_zero.to_numpy()
        locks = mask.sign_lock.to_numpy()
        values = seed.to_numpy()
        bad_zero = structural & (values != 0.0)
        bad_sign = ((locks == 1) & (values < 0.0)) | ((locks == -1) & (values > 0.0))
        for kind, bad in (('structural_zero', bad_zero), ('sign_lock', bad_sign)):
            for r, c in zip(*bad.nonzero()):
                value = float(values[r, c])
                swept = abs(value) <= dust_usd_m
                records.append(
                    {
                        'block': block,
                        'row': seed.index[r],
                        'column': seed.columns[c],
                        'value_usd_m': value,
                        'layer': kind,
                        'swept': swept,
                    }
                )
                if swept:
                    seed.iloc[r, c] = 0.0
    # Explicit dtypes so a violation-free (empty) sweep still boolean-masks:
    # an object-dtype empty 'swept' used as an indexer drops every column.
    return pd.DataFrame(
        records,
        columns=['block', 'row', 'column', 'value_usd_m', 'layer', 'swept'],
    ).astype({'value_usd_m': float, 'swept': bool})


@dataclass(frozen=True)
class YearBalance:
    """One year's assembled inputs and (when run) balanced output."""

    year: int
    seeds: dict[str, pd.DataFrame]
    masks: dict[str, SutMask]
    targets: TargetSet
    sweep: pd.DataFrame
    result: SutBalanceResult | None
    balanced: dict[str, pd.DataFrame] | None


def assemble(year: int, *, fitted: bool = True) -> YearBalance:
    """Seeds, masks, targets and the dust sweep for *year* - no balance run.

    The sweep runs **before** the target injection: T18 is injected from the
    seed's own value-added column sums, so sweeping afterwards would shift the
    seed under the injection by exactly the swept dust.
    """
    seeds = assemble_seeds(int(year), fitted=fitted)
    masks = assemble_masks(int(year))
    sweep = conform_seeds(seeds, masks)
    targets = assemble_targets(int(year), seeds['use'], seeds['supply'])
    return YearBalance(
        year=int(year),
        seeds=seeds,
        masks=masks,
        targets=targets,
        sweep=sweep,
        result=None,
        balanced=None,
    )


def balance_year(
    year: int,
    *,
    fitted: bool = True,
    impose_soft: bool = True,
    max_outer: int = 20,
) -> YearBalance:
    """Assemble and balance one year: split, offset, engine, restore."""
    assembled = assemble(int(year), fitted=fitted)
    frozen, free = split_fixed_blocks(assembled.seeds, assembled.masks)
    residual = offset_targets(assembled.targets, frozen)
    out = engine(
        free,
        residual,
        assembled.masks,
        impose_soft=impose_soft,
        max_outer=max_outer,
    )
    restored = restore_fixed_blocks(out.blocks, frozen)
    return YearBalance(
        year=assembled.year,
        seeds=assembled.seeds,
        masks=assembled.masks,
        targets=assembled.targets,
        sweep=assembled.sweep,
        result=out,
        balanced=restored,
    )


def hard_residual_report(balance: YearBalance) -> pd.DataFrame:
    """|evaluate(balanced) - target| per hard target, $M, worst first."""
    if balance.balanced is None:
        raise ValueError('balance_year first; this reports a balanced result')
    rows = []
    for target in balance.targets:
        if not target.hard:
            continue
        err = (target.evaluate(balance.balanced) - target.values).abs()
        rows.append(
            {
                'target': target.name,
                'margins': len(err),
                'max_abs_residual': float(err.max()),
                'total_abs_residual': float(err.sum()),
            }
        )
    frame = pd.DataFrame(rows).set_index('target')
    return frame.sort_values('max_abs_residual', ascending=False)


#: Artifact names for the balanced blocks, keyed by the mask's block names.
BALANCED_ARTIFACT_NAMES = {
    'supply': 'Balanced_Detail_Supply',
    'use': 'Balanced_Detail_Use_SUT',
}

#: Where the balanced products live on GCS, under ``GCS_CORNERSTONE``.
GCS_BALANCED_SUT_DIR = 'flowsa/BalancedSUT'


def save_balance(
    balance: YearBalance,
    out_dir: Path | None = None,
    *,
    protocol: str,
    upload: bool = False,
) -> list[Path]:
    """Persist the balanced blocks as versioned parquet + metadata sidecars.

    Writes ``<Name>_<year>_v<version>_<githash>.parquet`` and the
    ``*_metadata.json`` sidecar the repo's artifact tooling expects -
    the same convention as every other ``transform/output_data`` product -
    into *out_dir* (default ``transform/output_data``). With *upload*, both
    files also go to :data:`GCS_BALANCED_SUT_DIR` on GCS, which needs
    credentials (``scripts/google-login``).

    ⚠️ The balanced tables are in **BEA million dollars** - this module's
    seam converts to $M before the engine - unlike the USD FBS parquets they
    sit beside. The sidecar's ``units`` field says so.

    *protocol* is recorded verbatim in the sidecar; pass what the run
    actually did (e.g. ``'soft (impose_soft=True), max_outer=20'``), since
    ``YearBalance`` itself does not carry the engine flags.
    """
    if balance.balanced is None or balance.result is None:
        raise ValueError('balance_year first; only a balanced result is saved')
    directory = Path(out_dir) if out_dir is not None else FBS_DIR
    directory.mkdir(parents=True, exist_ok=True)

    result = balance.result
    engine_line = (
        f'outer iterations {result.outer_iterations}, '
        f'T11 max |residual| {result.t11_max_abs_residual:,.1f} $M, '
        f'skipped {result.skipped or "none"}, '
        f'soft deferred {result.soft_deferred or "none"}'
    )
    written: list[Path] = []
    for block, frame in balance.balanced.items():
        name = BALANCED_ARTIFACT_NAMES.get(block, f'Balanced_{block}')
        stem = f'{name}_{balance.year}_v{PKG_VERSION_NUMBER}'
        if GIT_HASH is not None:
            stem = f'{stem}_{GIT_HASH}'
        parquet_path = directory / f'{stem}.parquet'
        frame.to_parquet(parquet_path)
        meta = {
            'tool': 'bedrock',
            'category': 'BalancedSUT',
            'name_data': f'{name}_{balance.year}',
            'tool_version': PKG_VERSION_NUMBER,
            'git_hash': GIT_HASH,
            'ext': 'parquet',
            'date_created': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'tool_meta': {
                'step': 'Step 5 - GRAS balance of the nowcast SUT',
                'protocol': protocol,
                'units': 'BEA million USD',
                'branch': GIT_BRANCH,
                'commit': GIT_HASH_LONG,
                'engine_result': engine_line,
                'builder': 'bedrock.transform.iot.nowcast_sut_assembly',
            },
        }
        meta_path = directory / f'{stem}_metadata.json'
        meta_path.write_text(json.dumps(meta, indent=4))
        written.extend([parquet_path, meta_path])

    if upload:
        # Deferred import: the CLI must not need GCS credentials to balance.
        from bedrock.utils.io.gcp import (  # noqa: PLC0415
            GCS_CORNERSTONE,
            upload_file_to_gcs,
        )

        for path in written:
            upload_file_to_gcs(
                str(path),
                posixpath.join(GCS_CORNERSTONE, GCS_BALANCED_SUT_DIR, path.name),
            )
    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        '--years',
        default=f'{FIT_YEARS[0]}-{FIT_YEARS[-1]}',
        help='year or inclusive range, e.g. 2018 or 2018-2023',
    )
    parser.add_argument(
        '--raw-interior',
        action='store_true',
        help='seed the raw Step-3 interior instead of the two-margin fit',
    )
    parser.add_argument(
        '--hard-only',
        action='store_true',
        help='skip the soft targets (the exact-identity protocol)',
    )
    parser.add_argument(
        '--no-save',
        action='store_true',
        help='do not persist the balanced blocks to transform/output_data',
    )
    parser.add_argument(
        '--gcs',
        action='store_true',
        help='also upload the saved products to GCS (needs credentials)',
    )
    args = parser.parse_args(argv)
    first, _, last = args.years.partition('-')
    years = range(int(first), int(last or first) + 1)

    failures = 0
    for year in years:
        try:
            balance = balance_year(
                year, fitted=not args.raw_interior, impose_soft=not args.hard_only
            )
        except Exception as error:  # noqa: BLE001 - report and continue the span
            print(f'{year}: FAILED - {type(error).__name__}: {error}')
            failures += 1
            continue
        assert balance.result is not None
        print(
            f'{year}: outer iterations {balance.result.outer_iterations}, '
            f'T11 max |residual| {balance.result.t11_max_abs_residual:,.1f} $M, '
            f'skipped {balance.result.skipped or "none"}, '
            f'soft deferred {balance.result.soft_deferred or "none"}'
        )
        print(hard_residual_report(balance).to_string())
        if not args.no_save:
            protocol = (
                'hard-only (impose_soft=False)'
                if args.hard_only
                else 'soft (impose_soft=True)'
            ) + ', max_outer=20'
            written = save_balance(balance, protocol=protocol, upload=args.gcs)
            print(f'saved {len(written)} files to {written[0].parent}')
    return 1 if failures else 0


if __name__ == '__main__':
    sys.exit(main())
