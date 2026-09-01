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
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass

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
from bedrock.transform.iot.nowcast_targets import build_target_set
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


def assemble_targets(year: int, use_seed: pd.DataFrame) -> TargetSet:
    """The target set with T1 and T18 injected consistently with the seed.

    T1 is the census-adjusted gross output panel - the same series the
    interior fit's column targets are built from, which is the injection #724
    owed the target set. T18 is the seed's own value-added column sums, which
    are the Step-2 derived series in $M; injecting them keeps ``T1 - T18``
    exactly equal to the fit's intermediate column target.
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
    return build_target_set(int(year), gross_output=go, value_added=vapro)


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
    return pd.DataFrame(
        records,
        columns=['block', 'row', 'column', 'value_usd_m', 'layer', 'swept'],
    )


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
    """Seeds, masks, targets and the dust sweep for *year* - no balance run."""
    seeds = assemble_seeds(int(year), fitted=fitted)
    masks = assemble_masks(int(year))
    targets = assemble_targets(int(year), seeds['use'])
    sweep = conform_seeds(seeds, masks)
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
    impose_soft: bool = False,
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
        '--soft', action='store_true', help='impose the soft targets too'
    )
    args = parser.parse_args(argv)
    first, _, last = args.years.partition('-')
    years = range(int(first), int(last or first) + 1)

    failures = 0
    for year in years:
        try:
            balance = balance_year(
                year, fitted=not args.raw_interior, impose_soft=args.soft
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
    return 1 if failures else 0


if __name__ == '__main__':
    sys.exit(main())
