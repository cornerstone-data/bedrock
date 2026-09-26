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

Testing: assembly / ``balance_year`` wiring over heavy cached derivations is
still gated by the per-year precheck
(:mod:`~bedrock.analysis.nowcasting.ras_prechecks`). Post-balance hygiene
helpers and ``save_balance`` residue sweep carry hermetic unit tests under
``__tests__/``.
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

import numpy as np
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
    DEFAULT_PCE_CONSTRAINT,
    INVENTORY_CHANGE_COLUMN,
    PCE_ELECTRICITY_COL,
    PCE_ELECTRICITY_ROW,
    SUPPLY_BRIDGE_COLUMNS,
    VA_ROWS,
    PceConstraint,
    balance_commodities,
    balance_industries,
    build_sut_mask,
    panel_labels,
    published_2017_panel,
)
from bedrock.transform.iot.nowcast_supply_go_control import go_controlled_supply_block
from bedrock.transform.iot.nowcast_sut_gras import SutBalanceResult, engine
from bedrock.transform.iot.nowcast_targets import (
    FD_TARGET_COLUMNS,
    build_target_set,
    industry_group_aggregator,
    pce_electricity_cell_target,
)
from bedrock.utils.config.settings import (
    FBS_DIR,
    GIT_BRANCH,
    GIT_HASH,
    GIT_HASH_LONG,
    PKG_VERSION_NUMBER,
)
from bedrock.utils.config.usa_config import get_usa_config
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


def resolve_pce_constraint(
    pce_constraint: PceConstraint | None = None,
    constrain_electricity_pce_cell: bool | None = None,
) -> PceConstraint:
    """Resolve USAConfig / kwargs into a concrete :data:`PceConstraint`.

    Order (locked for #1008 analysis CLI):

    1. Explicit ``pce_constraint`` always wins (grade passes each candidate).
    2. Explicit ``constrain_electricity_pce_cell=False`` force-off → ``'none'``.
    3. Production: both kwargs ``None`` → config flag + mode field.
       Flag on with mode ``'none'`` raises (ship footgun).
    """
    if pce_constraint is not None:
        return pce_constraint
    if constrain_electricity_pce_cell is False:
        return 'none'
    flag = (
        True
        if constrain_electricity_pce_cell is True
        else get_usa_config().constrain_electricity_pce_cell
    )
    if not flag:
        return 'none'
    mode = get_usa_config().electricity_pce_constraint_mode
    if mode == 'none':
        raise ValueError(
            "constrain_electricity_pce_cell=True requires "
            "electricity_pce_constraint_mode != 'none'"
        )
    return mode


#: Sweep bound for **sign-lock** violations, in $M — deliberately looser than
#: :data:`DUST_USD_M`.
#:
#: ⚠️ **The two bounds answer different questions.** The structural-zero bound
#: is BEA's publication grain: a published cell is never smaller than $1M, so a
#: contradiction under that is rounding. But the sign-locked rows are not
#: published at detail at all - ``T00OSUB`` is a summary Use SUT row allocated
#: across detail industries on compensation shares - so there is no $1M grain to
#: appeal to and the meaningful floor is an order coarser.
#:
#: The case that set it: 2024 ``T00OSUB`` for air transportation comes out at
#: **+2.0 $M** in a row totalling **-4,884 $M** whose largest cell is -2,408.9,
#: as the pandemic payroll support is clawed back and the allocation leaves a
#: sliver on the wrong side of zero. It is the only sign-lock violation anywhere
#: in 2017-2024. Holding a number that size to a sign is asserting something the
#: allocation cannot support, so it is swept to zero like any other dust.
SIGN_DUST_USD_M = 10.0

#: Offset-residue sweep / illicit-sign gate, in $M ($50k). Strict ``<`` /
#: ``>`` — unlike seed :data:`DUST_USD_M`, which uses ``<=``. Issue #839.
RESIDUE_EPS_USD_M = 0.05

#: Structural-zero leak mass gate after restore, in $M. Named separately from
#: seed :data:`DUST_USD_M` even though the numeric grain matches. Issue #839.
ZERO_PATTERN_MASS_USD_M = 1.0

#: Unlocked residual VA row — negatives here are legitimate, not illicit dust.
_RESIDUAL_VA_ROW = 'V00300'

#: The bridge derivation labels its trade column without BEA's trailing space.
_BRIDGE_RENAMES = {'TRADE': 'TRADE '}


def _require_matching_labels(a: pd.DataFrame, b: pd.DataFrame, what: str) -> None:
    if not a.index.equals(b.index):
        raise ValueError(f'{what}: row labels differ')
    if not a.columns.equals(b.columns):
        raise ValueError(f'{what}: column labels differ')


def illicit_negative_mask(
    balanced: pd.DataFrame,
    mask: SutMask,
    pattern2017: pd.DataFrame,
) -> pd.DataFrame:
    """Boolean frame: Use negatives outside the #839 hygiene whitelist.

    Whitelist (never illicit): ``sign_lock == -1``,
    :data:`~bedrock.transform.iot.nowcast_mask.INVENTORY_CHANGE_COLUMN`,
    :data:`_RESIDUAL_VA_ROW`, and cells with ``pattern2017 < 0``.
    """
    _require_matching_labels(balanced, mask.sign_lock, 'illicit_negative_mask')
    _require_matching_labels(balanced, pattern2017, 'illicit_negative_mask pattern2017')
    illicit = (balanced < 0.0) & (mask.sign_lock != -1) & ~(pattern2017 < 0.0)
    if INVENTORY_CHANGE_COLUMN in balanced.columns:
        illicit = illicit.copy()
        illicit.loc[:, INVENTORY_CHANGE_COLUMN] = False
    if _RESIDUAL_VA_ROW in balanced.index:
        illicit = illicit.copy()
        illicit.loc[_RESIDUAL_VA_ROW, :] = False
    return illicit


def sweep_offset_residue(
    frame: pd.DataFrame,
    mask: SutMask,
    pattern2017: pd.DataFrame,
    eps: float = RESIDUE_EPS_USD_M,
) -> tuple[pd.DataFrame, int]:
    """Copy *frame* with illicit below-*eps* negatives zeroed (issue #839 item 1).

    Sweeps only cells in :func:`illicit_negative_mask` with
    ``0 < |x| < eps``. Exact zeros and non-illicit near-zeros (including
    positive dust on 2017-nonzero structure) are left alone. ``n_swept``
    counts cells that actually change.

    Returns ``(cleaned, n_swept)``. Does not mutate *frame*.
    """
    illicit = illicit_negative_mask(frame, mask, pattern2017)
    dust = illicit & (frame.abs() > 0.0) & (frame.abs() < float(eps))
    n_swept = int(dust.to_numpy().sum())
    cleaned = frame.copy()
    if n_swept:
        values = cleaned.to_numpy(dtype=float, copy=True)
        values[dust.to_numpy()] = 0.0
        cleaned = pd.DataFrame(values, index=cleaned.index, columns=cleaned.columns)
    return cleaned, n_swept


def zero_pattern_leak(
    balanced: pd.DataFrame,
    pattern2017: pd.DataFrame,
) -> tuple[int, float]:
    """Count and dollar mass of nonzero fills where the 2017 pattern is zero.

    Issue #839 item 2(a): audit against the **published** 2017 detail panel
    (``published_2017_panel``), not ``mask.structural_zero``. The mask drops
    deliberate trade/fiscal exemptions from Tier 0; fills there are expected
    (cell counts often tens to hundreds by year; dollar mass can be large on
    fiscal rows). Census/reporting uses this helper. The production **fail**
    gate for empty-cell integrity stays on :func:`structural_zero_leak`.
    """
    _require_matching_labels(balanced, pattern2017, 'zero_pattern_leak')
    leak = (pattern2017 == 0.0) & (balanced != 0.0)
    n_cells = int(leak.to_numpy().sum())
    mass = float(balanced.where(leak, 0.0).abs().sum().sum())
    return n_cells, mass


def structural_zero_leak(
    balanced: pd.DataFrame,
    mask: SutMask,
) -> tuple[int, float]:
    """Count and dollar mass of nonzero fills into ``mask.structural_zero``.

    Stricter than :func:`zero_pattern_leak`: Tier 0 cells the engine must hold
    at zero. Used as the standing mass fail gate in
    :func:`assert_post_balance_hygiene`.
    """
    _require_matching_labels(balanced, mask.structural_zero, 'structural_zero_leak')
    leak = mask.structural_zero & (balanced != 0.0)
    n_cells = int(leak.to_numpy().sum())
    mass = float(balanced.where(leak, 0.0).abs().sum().sum())
    return n_cells, mass


def illicit_sign_residue(
    balanced: pd.DataFrame,
    mask: SutMask,
    pattern2017: pd.DataFrame,
    eps: float = RESIDUE_EPS_USD_M,
) -> tuple[int, float]:
    """Illicit Use negatives above *eps*: count and max abs among that set.

    Whitelist (never illicit): see :func:`illicit_negative_mask`.
    ``max_abs`` is over the above-eps illicit set only (``0.0`` when empty).
    """
    illicit = illicit_negative_mask(balanced, mask, pattern2017)
    above = illicit & (balanced.abs() > float(eps))
    n_above = int(above.to_numpy().sum())
    if n_above == 0:
        return 0, 0.0
    max_abs = float(balanced.where(above, 0.0).abs().max().max())
    return n_above, max_abs


def assert_post_balance_hygiene(
    year: int,
    balanced: dict[str, pd.DataFrame],
    masks: dict[str, SutMask],
    *,
    pattern2017: pd.DataFrame | None = None,
    patterns2017: dict[str, pd.DataFrame] | None = None,
    zero_mass_bound: float = ZERO_PATTERN_MASS_USD_M,
    residue_eps: float = RESIDUE_EPS_USD_M,
) -> None:
    """Gate Tier-0 structural-zero fills and Use illicit signs.

    Item 2(a) **monitoring** (published-pattern fills, including trade/fiscal
    exemptions) is :func:`zero_pattern_leak` in the census CLIs — dollar mass
    there is often ≫ ``zero_mass_bound`` (legitimate fiscal moves). The
    standing **fail** gate here is :func:`structural_zero_leak` (engine must
    not fill frozen zeros). Item 2(b) fails on illicit Use negatives above
    ``residue_eps``.

    *patterns2017* / *pattern2017* supply Use pattern for 2(b); if omitted,
    loads :func:`~bedrock.transform.iot.nowcast_mask.published_2017_panel`.
    """

    def _pattern(block: str) -> pd.DataFrame:
        if patterns2017 is not None and block in patterns2017:
            return patterns2017[block]
        if block == 'use' and pattern2017 is not None:
            return pattern2017
        return published_2017_panel(block)  # type: ignore[arg-type]

    for block, frame in balanced.items():
        mask = masks[block]
        n_cells, mass = structural_zero_leak(frame, mask)
        if mass > float(zero_mass_bound):
            raise ValueError(
                f'{year} {block}: structural-zero leak mass '
                f'{mass:.6g} $M across {n_cells} cells exceeds '
                f'{zero_mass_bound} $M'
            )
    use = balanced['use']
    use_mask = masks['use']
    use_pattern = _pattern('use')
    n_above, max_abs = illicit_sign_residue(use, use_mask, use_pattern, residue_eps)
    if n_above > 0:
        raise ValueError(
            f'{year} use: {n_above} illicit negative cell(s) above '
            f'{residue_eps} $M (max abs {max_abs:.6g} $M)'
        )


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


def assemble_masks(
    year: int,
    *,
    pce_constraint: PceConstraint = DEFAULT_PCE_CONSTRAINT,
) -> dict[str, SutMask]:
    """The 2017-pattern masks, one per block. The pattern is deliberately
    2017's - see the mask module - so the same masks serve every year."""
    return {
        block: build_sut_mask(block, int(year), pce_constraint=pce_constraint)
        for block in BLOCKS
    }


def assemble_targets(
    year: int,
    use_seed: pd.DataFrame,
    supply_seed: pd.DataFrame,
    *,
    pce_constraint: PceConstraint = DEFAULT_PCE_CONSTRAINT,
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

    When ``pce_constraint='row_side_target'``, append soft T1008 for the
    ``221100 × F01000`` seed cell (#1008 Candidate B).
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
    base = build_target_set(
        int(year),
        gross_output=go,
        value_added=vapro,
        fd_totals=fd_totals,
        compensation=compensation,
        tax_totals=tax_totals,
        supply_totals=supply_totals,
    )
    if pce_constraint != 'row_side_target':
        return base
    if (
        PCE_ELECTRICITY_ROW not in use_seed.index
        or PCE_ELECTRICITY_COL not in use_seed.columns
    ):
        raise ValueError(
            f'{PCE_ELECTRICITY_ROW!r}×{PCE_ELECTRICITY_COL!r} missing from Use seed '
            f'{year}'
        )
    cell = float(
        np.asarray(use_seed.at[PCE_ELECTRICITY_ROW, PCE_ELECTRICITY_COL]).item()
    )
    return TargetSet((*base.targets, pce_electricity_cell_target(int(year), cell)))


#: #990 absolute published band half-width in $M ($5bn).
_PCE_EIA_BAND_HALF_M = 5_000.0


def _pce_eia_band_m(year: int) -> tuple[float, float]:
    """Level collar around EIA residential revenue for the eia_band closer ($M)."""
    from bedrock.analysis.electricity.current.eia_gtd.electricity_row_control import (  # noqa: PLC0415
        eia_epa_table_2_3_revenue_bn,
    )

    res_bn = float(eia_epa_table_2_3_revenue_bn(int(year))[0])
    center_m = res_bn * 1e3  # $bn → $M
    return (center_m - _PCE_EIA_BAND_HALF_M, center_m + _PCE_EIA_BAND_HALF_M)


def conform_seeds(
    seeds: dict[str, pd.DataFrame],
    masks: dict[str, SutMask],
    dust_usd_m: float = DUST_USD_M,
    sign_dust_usd_m: float | None = None,
) -> pd.DataFrame:
    """Zero mask-contradicting seed cells at or under the dust bound, in place.

    Returns the sweep: one row per swept **or surviving** violation, with the
    block, cell, value and which layer it contradicts. Survivors (above the
    threshold) are not touched - the balance machinery will refuse them, and
    that refusal is the honest failure. The precheck prints this frame.

    ⚠️ **Sign-lock violations take their own, looser bound**
    (:data:`SIGN_DUST_USD_M`), because the sign-locked rows are allocated to
    detail rather than published at it and so have no $1M grain to appeal to.
    Pass *sign_dust_usd_m* to override; it defaults to that constant.
    """
    if sign_dust_usd_m is None:
        sign_dust_usd_m = SIGN_DUST_USD_M
    bounds = {'structural_zero': dust_usd_m, 'sign_lock': sign_dust_usd_m}
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
                swept = abs(value) <= bounds[kind]
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


def assemble(
    year: int,
    *,
    fitted: bool = True,
    pce_constraint: PceConstraint | None = None,
    constrain_electricity_pce_cell: bool | None = None,
) -> YearBalance:
    """Seeds, masks, targets and the dust sweep for *year* - no balance run.

    The sweep runs **before** the target injection: T18 is injected from the
    seed's own value-added column sums, so sweeping afterwards would shift the
    seed under the injection by exactly the swept dust.
    """
    mode = resolve_pce_constraint(pce_constraint, constrain_electricity_pce_cell)
    seeds = assemble_seeds(int(year), fitted=fitted)
    masks = assemble_masks(int(year), pce_constraint=mode)
    sweep = conform_seeds(seeds, masks)
    targets = assemble_targets(
        int(year), seeds['use'], seeds['supply'], pce_constraint=mode
    )
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
    pce_constraint: PceConstraint | None = None,
    constrain_electricity_pce_cell: bool | None = None,
) -> YearBalance:
    """Assemble and balance one year: split, offset, engine, restore."""
    mode = resolve_pce_constraint(pce_constraint, constrain_electricity_pce_cell)
    assembled = assemble(
        int(year),
        fitted=fitted,
        pce_constraint=mode,
    )
    frozen, free = split_fixed_blocks(assembled.seeds, assembled.masks)
    residual = offset_targets(assembled.targets, frozen)
    band = _pce_eia_band_m(int(year)) if mode == 'eia_band' else None
    out = engine(
        free,
        residual,
        assembled.masks,
        impose_soft=impose_soft,
        max_outer=max_outer,
        pce_constraint=mode,
        pce_eia_band_m=band,
    )
    restored = restore_fixed_blocks(out.blocks, frozen)
    assert_post_balance_hygiene(assembled.year, restored, assembled.masks)
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
    use_pattern: pd.DataFrame | None = None
    for block, frame in balance.balanced.items():
        if block == 'use':
            if 'use' not in balance.masks:
                raise ValueError(
                    'save_balance needs balance.masks["use"] for residue sweep'
                )
            if use_pattern is None:
                use_pattern = published_2017_panel('use')
            cleaned, n_swept = sweep_offset_residue(
                frame,
                balance.masks['use'],
                use_pattern,
                RESIDUE_EPS_USD_M,
            )
        else:
            cleaned, n_swept = frame, 0
        name = BALANCED_ARTIFACT_NAMES.get(block, f'Balanced_{block}')
        stem = f'{name}_{balance.year}_v{PKG_VERSION_NUMBER}'
        if GIT_HASH is not None:
            stem = f'{stem}_{GIT_HASH}'
        parquet_path = directory / f'{stem}.parquet'
        cleaned.to_parquet(parquet_path)
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
                'residue_eps_usd_m': RESIDUE_EPS_USD_M,
                'residue_swept_cells': n_swept,
                'residue_sweep': 'illicit_below_eps',
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
