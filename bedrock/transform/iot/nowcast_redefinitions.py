"""Step 7: the after-redefinitions MUTs, by the measured method.

Every design choice here was picked by measurement, not preference - the
probes and scores live in
``bedrock/analysis/nowcasting/after_redef_MUTs/method_probes/``:

* **Make moves by pattern, not by rate.** At 2017 detail, redefinition
  movement is strictly off-diagonal to the same commodity's diagonal, and
  1,850 of 1,880 moving cells move at ~100% of the cell. The pattern (with
  the ~30 partial cells' 2017 fractions) applied to the year's *own* cells
  beats a cell-ratio carry 3-4x on the 2018-2024 summary span. The pattern
  is BEA ch9's always-redefine rules made concrete: construction,
  manufacturing-in-nonmanufacturing, trade-in-nontrade (never wholesale <->
  retail), rental, and services-in-nonservice.

* **Use moves by cell-ratio carry, then row closure.** Moving input recipes
  with the output is WRONG - redefined activities carry only ~$0.24 of
  intermediates per dollar of output. The 2017 after/before cell ratio
  applied to the year's own table wins the span test; closing each commodity
  row back to its before-redefinitions total (redefinitions never change
  commodity totals) both restores the identity and *improves* the score
  (0.99% -> 0.81% at 2021).

* **Value added carries by ratio, then absorbs the column residual.** The
  column identity ``intermediates + VA = after-redef industry output`` must
  close; closing it inside the interior hurts cells against the published
  answer, closing it on VA helps (1.99% -> 1.83% at 2021).

* **Final demand crosses unchanged** - measured exactly invariant at 2017
  detail ($0M on a $25.8tn block).

* **The import matrix is re-allocated, not carried**: commodity import
  totals are unchanged by redefinitions, so the matrix is the same
  #816 allocation (:func:`mut_use_to_import_matrix.import_matrix_from_use`)
  along the after-redefinitions producer-price Use rows.

Everything is learned from the published 2017 detail pair and applied to a
nowcast year's own before-redefinitions tables (Step 6's stored products).
Identities are exact by construction and gated: commodity output invariant
through the Make move, commodity Use rows invariant through the carry,
columns closed to the pattern-predicted industry output.

Run ``--check`` for the 2017 replay against the published after tables.
"""

from __future__ import annotations

import argparse
import json
import posixpath
import sys
import typing as ta
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from bedrock.extract.iot.nowcast_mut_storage import (
    GCS_NOWCAST_MUT_DIR,
    MutTable,
    default_nowcast_mut_vintage,
    nowcast_mut_artifact_name,
)
from bedrock.transform.iot.mut_use_to_import_matrix import import_matrix_from_use
from bedrock.utils.config.settings import (
    FBS_DIR,
    GIT_BRANCH,
    GIT_HASH,
    GIT_HASH_LONG,
    PKG_VERSION_NUMBER,
)
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES
from bedrock.utils.taxonomy.bea.v2017_value_added import USA_2017_VALUE_ADDED_CODES

#: Cells below this are publication rounding, not movement, when learning the
#: pattern; identities are gated at the same tolerance.
ATOL = 1.0 * MILLION_CURRENCY_TO_CURRENCY

#: The import matrix's MUT-only final-demand column.
_F05000 = 'F05000'


class RedefinitionAnchor(ta.NamedTuple):
    """Everything learned from the published 2017 before/after pair.

    ``fractions``: industry x commodity, the moved share of each
    before-redefinitions Make cell (0 = no movement; 1,850 of the 1,880
    moving detail cells are 1.0). ``use_ratios``: after/before per cell of
    the Use interior, 1 where the before cell is empty. ``va_ratios``: the
    same for the value-added rows.
    """

    fractions: pd.DataFrame
    use_ratios: pd.DataFrame
    va_ratios: pd.DataFrame


# --- learning ---------------------------------------------------------------


def learn_fractions(V_before: pd.DataFrame, V_after: pd.DataFrame) -> pd.DataFrame:
    """The moved fraction of every before-redefinitions Make cell. Unitless.

    Only off-diagonal cells that *lose* mass learn a fraction; everything
    else is 0. Gains land on the same commodity's diagonal by construction
    when the pattern is applied, so the diagonal is never learned - it is
    implied. Fractions are capped at 1: a cell cannot move more than itself.
    """
    before = V_before.astype(float)
    diff = V_after.astype(float).reindex_like(before).fillna(0.0) - before
    fractions = (-diff / before.where(before > 0.0)).clip(lower=0.0, upper=1.0)
    fractions = fractions.where(diff < -ATOL, 0.0).fillna(0.0)
    for code in fractions.columns:
        if code in fractions.index:
            fractions.at[code, code] = 0.0
    return fractions


def _carry_ratios(before: pd.DataFrame, after: pd.DataFrame) -> pd.DataFrame:
    ratios = after.astype(float).reindex_like(before) / before.astype(float)
    return ratios.replace([np.inf, -np.inf], np.nan).fillna(1.0)


def learn_anchor(
    V_before: pd.DataFrame,
    V_after: pd.DataFrame,
    U_before: pd.DataFrame,
    U_after: pd.DataFrame,
) -> RedefinitionAnchor:
    """The full anchor from one published before/after pair.

    *U_before*/*U_after* are the full Use frames - commodity rows plus the
    three MUT value-added rows, industry columns (final demand is invariant
    and never learned).
    """
    commodities = [c for c in U_before.index if c in set(USA_2017_COMMODITY_CODES)]
    va_rows = [r for r in U_before.index if r in set(USA_2017_VALUE_ADDED_CODES)]
    industries = [c for c in U_before.columns if c in set(USA_2017_INDUSTRY_CODES)]
    return RedefinitionAnchor(
        fractions=learn_fractions(V_before, V_after),
        use_ratios=_carry_ratios(
            U_before.loc[commodities, industries], U_after.loc[commodities, industries]
        ),
        va_ratios=_carry_ratios(
            U_before.loc[va_rows, industries], U_after.loc[va_rows, industries]
        ),
    )


# --- the Make move ----------------------------------------------------------


def make_after_redef(V_before: pd.DataFrame, fractions: pd.DataFrame) -> pd.DataFrame:
    """The after-redefinitions Make table from the year's own cells. USD.

    Each mapped (donor industry, commodity) cell loses ``fraction x cell``;
    the commodity's own diagonal receives every donation. Commodity output
    is invariant by construction and asserted anyway.
    """
    before = V_before.astype(float)
    f = fractions.reindex_like(before).fillna(0.0)
    moved = f * before
    after = before - moved
    donations = moved.sum(axis=0)
    for raw_code, amount in donations.items():
        if amount == 0.0:
            continue
        code = str(raw_code)
        assert code in after.index, (
            f'commodity {code!r} has ${amount / MILLION_CURRENCY_TO_CURRENCY:,.0f}M '
            'of redefined production but no primary industry row to receive it'
        )
        current = ta.cast(float, after.at[code, code])
        after.at[code, code] = current + float(amount)

    gap = (after.sum(axis=0) - before.sum(axis=0)).abs().max()
    assert gap <= ATOL, (
        f'redefinitions changed commodity output by up to '
        f'${gap / MILLION_CURRENCY_TO_CURRENCY:,.3f}M; they never may'
    )
    return after


# --- the Use move -----------------------------------------------------------


def _close_rows(
    carried: pd.DataFrame,
    before: pd.DataFrame,
    frozen: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Zero each commodity row's net change, weighted by the carry's own deltas.

    Redefinitions reallocate use across industry columns inside a row;
    commodity totals never change. Closing the carry's drift this way both
    restores that identity and scores better than leaving it (measured on
    the summary span). Rows the carry did not touch pass through untouched.

    *frozen* marks cells the anchor zeroes deliberately (carry ratio 0):
    they take no closure weight, so a cell the redefinition empties by rule
    is never pushed negative by the row's residual. Measured on the span,
    the exclusion is slightly better in every year and removes every
    negative it used to create. A row whose only changed cells are frozen
    falls back to unexcluded weights - its residual has nowhere else to go.

    ⚠️ A row whose carried deltas all share one sign returns exactly to its
    before values: conserving the row total means giving the whole net
    change back, and proportional-to-|delta| weights give it back cell for
    cell. Within-row reallocation survives only through mixed-sign rows.
    That conservatism is a property, not an accident - the closure was
    adopted on its measured span score.
    """
    delta = carried - before
    weight = delta.abs()
    if frozen is not None:
        excluded = weight.where(~frozen, 0.0)
        # Fallback: rows with residual but no unfrozen weight to place it on.
        needs_fallback = (excluded.sum(axis=1) == 0.0) & (
            delta.sum(axis=1).abs() > ATOL
        )
        weight = excluded.where(~needs_fallback, weight)
    weight = weight.div(weight.sum(axis=1).replace(0.0, np.nan), axis=0).fillna(0.0)
    return carried - weight.mul(delta.sum(axis=1), axis=0)


def use_after_redef(
    use_before: pd.DataFrame,
    anchor: RedefinitionAnchor,
    x_after: pd.Series,
) -> pd.DataFrame:
    """The after-redefinitions Use table (rows + VA + FD) from the year's own. USD.

    Interior: cell-ratio carry, rows closed back to the before totals.
    Value added: ratio carry, then each industry column scaled so
    ``interior + VA`` meets *x_after* - the Make-side predicted output.
    Final demand: copied unchanged (measured exactly invariant at 2017).
    """
    table = use_before.astype(float).copy()
    commodities = [c for c in table.index if c in set(USA_2017_COMMODITY_CODES)]
    va_rows = [r for r in table.index if r in set(USA_2017_VALUE_ADDED_CODES)]
    industries = [c for c in table.columns if c in set(USA_2017_INDUSTRY_CODES)]

    before = table.loc[commodities, industries]
    ratios = anchor.use_ratios.reindex_like(before).fillna(1.0)
    carried = before * ratios
    interior = _close_rows(carried, before, frozen=ratios == 0.0)
    table.loc[commodities, industries] = interior.to_numpy()

    va_before = table.loc[va_rows, industries]
    va_carried = va_before * anchor.va_ratios.reindex_like(va_before).fillna(1.0)
    va_target = x_after.reindex(industries).astype(float) - interior.sum(axis=0)
    va_colsum = va_carried.sum(axis=0)
    va_scale = (va_target / va_colsum).replace([np.inf, -np.inf], np.nan)
    # Sign guard: an industry whose total value added is negative (transit
    # runs operating losses) must not have its VA rows sign-flipped by a
    # positive target over a negative column sum. Such columns keep their
    # carried rows and absorb the whole residual additively on operating
    # surplus - the accounts' own residual row.
    flips = (va_colsum * va_target) < 0
    closed = va_carried * va_scale.where(~flips, 1.0).fillna(1.0)
    if flips.any() and 'V00300' in closed.index:
        residual = (va_target - va_colsum).where(flips, 0.0)
        closed.loc['V00300'] = closed.loc['V00300'] + residual
    table.loc[va_rows, industries] = closed.to_numpy()
    if 'V00100' in closed.index:
        worst_compensation = float(closed.loc['V00100'].min())
        assert worst_compensation >= -ATOL, (
            f'VA closure drove compensation negative '
            f'(${worst_compensation / MILLION_CURRENCY_TO_CURRENCY:,.1f}M); '
            'a sign convention has broken upstream'
        )

    row_gap = (
        (table.loc[commodities, industries].sum(axis=1) - before.sum(axis=1))
        .abs()
        .max()
    )
    assert row_gap <= ATOL, (
        f'the carry changed a commodity row total by '
        f'${row_gap / MILLION_CURRENCY_TO_CURRENCY:,.3f}M after closure'
    )
    column_gap = (
        table.loc[[*commodities, *va_rows], industries].sum(axis=0)
        - x_after.reindex(industries)
    ).abs()
    unclosable = column_gap[va_carried.sum(axis=0).abs() <= ATOL]
    closable_gap = column_gap.drop(unclosable.index).max()
    assert closable_gap <= ATOL, (
        f'a Use column missed the after-redefinitions output by '
        f'${closable_gap / MILLION_CURRENCY_TO_CURRENCY:,.3f}M after VA closure'
    )
    if not unclosable.empty and unclosable.max() > ATOL:
        worst = unclosable.idxmax()
        raise AssertionError(
            f'industry {worst!r} needs '
            f'${unclosable.max() / MILLION_CURRENCY_TO_CURRENCY:,.1f}M of column '
            'closure but has no value added to absorb it'
        )
    return table


def import_after_redef(
    use_after: pd.DataFrame, import_control: pd.Series
) -> pd.DataFrame:
    """The after-redefinitions import matrix. USD.

    Commodity import totals are unchanged by redefinitions; the matrix is
    the #816 allocation along the after-redefinitions producer-price rows.
    """
    commodities = [c for c in use_after.index if c in set(USA_2017_COMMODITY_CODES)]
    return import_matrix_from_use(use_after.loc[commodities], import_control)


def margins_after_redef(
    margins_before: pd.DataFrame,
    use_before: pd.DataFrame,
    use_after: pd.DataFrame,
) -> pd.DataFrame:
    """The after-redefinitions Margins table, coherent with the moved Use. USD.

    Redefinitions move purchases between industry buyers; each margins row
    describes one (buyer, commodity) transaction, so the whole row scales
    with its transaction: by the after/before ratio of that cell in the
    producer-price Use (1 where the before cell is empty - the carry cannot
    create transactions). Scaling preserves the goods-row identity
    ``PRO + Transportation + Wholesale + Retail = PUR`` exactly and keeps
    ``Producers' Value`` equal to the after-redefinitions Use cell.

    Margin-commodity rows follow the published convention (routed margin in
    ``Producers' Value`` against a direct-purchase ``Purchasers' Value``),
    and both of their values are **rebuilt from the after table, never
    scaled**: the Use cell for a margin commodity is direct plus routed, so
    scaling the row by that cell's ratio would distort the direct purchase
    by the routing's movement. Instead ``Producers' Value`` is set to the
    after-Use cell itself (coherence exact by construction) and
    ``Purchasers' Value`` to that cell minus the routing recomputed from the
    scaled goods rows' seller columns - the stored hyper-detailed layout
    carries one column per margin commodity precisely so this needs no rate
    machinery. Without seller columns (the published five-column layout)
    the rebuild is impossible and margin rows stay plainly scaled.
    """
    # Deferred import: the margin-commodity list lives with the rate panel.
    from bedrock.transform.iot.margin_rates import MARGIN_COMMODITIES  # noqa: PLC0415

    ratio = (
        use_after.astype(float) / use_before.astype(float).replace(0.0, np.nan)
    ).replace([np.inf, -np.inf], np.nan)

    buyers = margins_before.index.get_level_values('Industry Code')
    commodities = margins_before.index.get_level_values('Commodity Code')
    lookup = ratio.stack()
    keys = pd.MultiIndex.from_arrays([commodities, buyers])
    scale = pd.Series(
        lookup.reindex(keys).fillna(1.0).to_numpy(), index=margins_before.index
    )
    out = margins_before.astype(float).mul(scale, axis=0)

    margin_codes = set(MARGIN_COMMODITIES)
    is_margin_row = commodities.isin(margin_codes)
    goods = out.loc[~is_margin_row]
    present = [c for c in MARGIN_COMMODITIES if c in goods.columns]
    if present:
        booked = (
            goods[present]
            .groupby(level='Industry Code')
            .sum()
            .stack()
            .rename_axis(['Industry Code', 'Commodity Code'])
        )
        margin_rows = out.index[is_margin_row]
        routed = booked.reindex(margin_rows).fillna(0.0)
        after_cells = use_after.astype(float).stack()
        row_keys = pd.MultiIndex.from_arrays(
            [
                margin_rows.get_level_values('Commodity Code'),
                margin_rows.get_level_values('Industry Code'),
            ]
        )
        cell = pd.Series(
            after_cells.reindex(row_keys).fillna(0.0).to_numpy(), index=margin_rows
        )
        out.loc[margin_rows, "Producers' Value"] = cell.to_numpy(dtype=float)
        out.loc[margin_rows, "Purchasers' Value"] = cell.to_numpy(
            dtype=float
        ) - routed.to_numpy(dtype=float)

    # Scaling multiplies whatever closure residue the input rows carried, so
    # the gate divides it back out: an exact store (Step 6's) must stay exact,
    # while the published tables' per-cell rounding may not read as breakage.
    closure = (
        goods[["Producers' Value", 'Transportation', 'Wholesale', 'Retail']].sum(axis=1)
        - goods["Purchasers' Value"]
    ).abs()
    if len(closure):
        unscaled = closure / scale.loc[closure.index].abs().clip(lower=1.0)
        assert unscaled.max() <= 5 * ATOL, (
            f'scaling broke the goods-row margins identity by '
            f'${unscaled.max() / MILLION_CURRENCY_TO_CURRENCY:,.3f}M '
            '(scale-adjusted)'
        )
    return out


# --- the 2017 anchor from published tables ----------------------------------


def published_anchor() -> RedefinitionAnchor:
    """The anchor learned from the published 2017 detail before/after pair."""
    # Deferred imports: learning needs the published workbooks; applying the
    # anchor to a nowcast year must not.
    from bedrock.extract.iot.io_2017 import (  # noqa: PLC0415
        load_2017_Utot_after_redef_usa,
        load_2017_Utot_before_redef_usa,
        load_2017_V_after_redef_usa,
        load_2017_V_before_redef_usa,
        load_2017_value_added_before_redef_usa,
        load_2017_value_added_usa,
    )

    V_b = load_2017_V_before_redef_usa().astype(float)
    V_a = load_2017_V_after_redef_usa().astype(float)
    V_a = pd.DataFrame(V_a.to_numpy(), index=V_b.index, columns=V_b.columns)

    def full_use(interior: pd.DataFrame, value_added: pd.DataFrame) -> pd.DataFrame:
        interior = interior.astype(float)
        value_added = value_added.astype(float)
        value_added = pd.DataFrame(
            value_added.to_numpy(),
            index=list(value_added.index),
            columns=list(interior.columns),
        )
        interior = pd.DataFrame(
            interior.to_numpy(),
            index=list(interior.index),
            columns=list(interior.columns),
        )
        return pd.concat([interior, value_added])

    U_b = full_use(
        load_2017_Utot_before_redef_usa(), load_2017_value_added_before_redef_usa()
    )
    U_a = full_use(load_2017_Utot_after_redef_usa(), load_2017_value_added_usa())
    U_b.index = [str(i) for i in U_b.index]
    U_a.index = [str(i) for i in U_a.index]
    U_b.columns = [str(c) for c in U_b.columns]
    U_a.columns = [str(c) for c in U_a.columns]
    V_b.index = [str(i) for i in V_b.index]
    V_b.columns = [str(c) for c in V_b.columns]
    V_a.index = [str(i) for i in V_a.index]
    V_a.columns = [str(c) for c in V_a.columns]
    return learn_anchor(V_b, V_a, U_b, U_a)


# --- applying to a stored Step 6 year --------------------------------------


def _stored_before(
    table: MutTable, year: int, directory: Path, vintage: str | None = None
) -> tuple[pd.DataFrame, str]:
    """One stored before-redefinitions table and the filename it came from.

    With *vintage* the file is pinned exactly; without it the newest file on
    disk wins, and the sidecar records which one that was.
    """
    pattern = (
        f'Nowcast_Detail_{table}_before_redef_{year}_{vintage}.parquet'
        if vintage
        else f'Nowcast_Detail_{table}_before_redef_{year}_*.parquet'
    )
    matches = sorted(directory.glob(pattern), key=lambda p: p.stat().st_mtime)
    if not matches:
        raise FileNotFoundError(
            f'no {pattern} in {directory}; '
            'run bedrock.transform.iot.nowcast_mut first'
        )
    return pd.read_parquet(matches[-1]), matches[-1].name


def after_redef_tables(
    year: int,
    anchor: RedefinitionAnchor,
    directory: Path | None = None,
    vintage: str | None = None,
) -> tuple[dict[MutTable, pd.DataFrame], list[str]]:
    """The after-redefinitions quartet for one stored Step 6 year.

    Returns the four tables and the exact before-artifact filenames they were
    built from, for the sidecar.
    """
    where = Path(directory) if directory is not None else Path(FBS_DIR)
    make_before, make_src = _stored_before('Make', year, where, vintage)
    use_before, use_src = _stored_before('Use', year, where, vintage)
    import_before, import_src = _stored_before('Import', year, where, vintage)
    margins_before, margins_src = _stored_before('Margins', year, where, vintage)

    make = make_after_redef(make_before, anchor.fractions)
    use = use_after_redef(use_before, anchor, make.sum(axis=1))
    control = import_before.drop(columns=[_F05000], errors='ignore').sum(axis=1)
    imports = import_after_redef(use, control.clip(lower=0.0))
    commodities = [c for c in use_before.index if c in set(USA_2017_COMMODITY_CODES)]
    margins = margins_after_redef(
        margins_before, use_before.loc[commodities], use.loc[commodities]
    )
    tables: dict[MutTable, pd.DataFrame] = {
        'Make': make,
        'Use': use,
        'Import': imports,
        'Margins': margins,
    }
    return tables, [make_src, use_src, import_src, margins_src]


def save_after_redef(
    year: int,
    tables: dict[MutTable, pd.DataFrame],
    out_dir: Path | None = None,
    *,
    upload: bool = False,
    before_inputs: list[str] | None = None,
) -> list[Path]:
    """Persist the after-redefinitions tables, house parquet + sidecar style."""
    directory = Path(out_dir) if out_dir is not None else Path(FBS_DIR)
    directory.mkdir(parents=True, exist_ok=True)
    vintage = default_nowcast_mut_vintage()
    written: list[Path] = []
    for table, frame in tables.items():
        name = nowcast_mut_artifact_name(
            table, year=year, stage='after', vintage=vintage
        )
        parquet_path = directory / name
        frame.to_parquet(parquet_path)
        meta = {
            'tool': 'bedrock',
            'category': 'NowcastMUT',
            'name_data': Path(name).stem,
            'tool_version': PKG_VERSION_NUMBER,
            'git_hash': GIT_HASH,
            'ext': 'parquet',
            'date_created': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'tool_meta': {
                'step': 'Step 7 - redefinitions, before to after',
                'vintage': vintage,
                'units': 'USD',
                'branch': GIT_BRANCH,
                'commit': GIT_HASH_LONG,
                'before_inputs': before_inputs or [],
                'method': (
                    'Make: 2017 detail movement pattern on the year\'s own '
                    'cells; Use: 2017 cell-ratio carry with commodity-row '
                    'closure and VA column closure; FD invariant; imports '
                    're-allocated along the after-redef rows (#816 rule)'
                ),
                'builder': 'bedrock.transform.iot.nowcast_redefinitions',
            },
        }
        meta_path = directory / f'{Path(name).stem}_metadata.json'
        meta_path.write_text(json.dumps(meta, indent=4))
        written.extend([parquet_path, meta_path])
    if upload:
        # Deferred import: the CLI must not need GCS credentials to build.
        from bedrock.utils.io.gcp import (  # noqa: PLC0415
            GCS_CORNERSTONE,
            upload_file_to_gcs,
        )

        for path in written:
            upload_file_to_gcs(
                str(path),
                posixpath.join(GCS_CORNERSTONE, GCS_NOWCAST_MUT_DIR, path.name),
            )
    return written


# --- the 2017 replay --------------------------------------------------------


def check() -> int:
    """Learn at 2017, apply to 2017, diff against the published after tables."""
    from bedrock.extract.iot.io_2017 import (  # noqa: PLC0415
        load_2017_Uimp_after_redef_usa,
        load_2017_Utot_after_redef_usa,
        load_2017_Utot_before_redef_usa,
        load_2017_V_after_redef_usa,
        load_2017_V_before_redef_usa,
        load_2017_value_added_before_redef_usa,
        load_2017_value_added_usa,
    )

    anchor = published_anchor()
    V_b = load_2017_V_before_redef_usa().astype(float)
    V_b.index = [str(i) for i in V_b.index]
    V_b.columns = [str(c) for c in V_b.columns]
    V_a = load_2017_V_after_redef_usa().astype(float)
    make = make_after_redef(V_b, anchor.fractions)
    v_gap = (make.to_numpy() - V_a.to_numpy()).__abs__().max()
    print(f'Make replay max cell gap: ${v_gap / MILLION_CURRENCY_TO_CURRENCY:,.3f}M')

    U_b = load_2017_Utot_before_redef_usa().astype(float)
    W_b = load_2017_value_added_before_redef_usa().astype(float)
    use_before = pd.concat(
        [
            pd.DataFrame(
                U_b.to_numpy(),
                index=[str(i) for i in U_b.index],
                columns=[str(c) for c in U_b.columns],
            ),
            pd.DataFrame(
                W_b.to_numpy(),
                index=[str(i) for i in W_b.index],
                columns=[str(c) for c in U_b.columns],
            ),
        ]
    )
    use = use_after_redef(use_before, anchor, make.sum(axis=1))
    U_a = load_2017_Utot_after_redef_usa().astype(float)
    W_a = load_2017_value_added_usa().astype(float)
    commodities = [str(c) for c in U_a.index]
    industries = [str(c) for c in U_a.columns]
    u_gap = (
        (use.loc[commodities, industries].to_numpy() - U_a.to_numpy()).__abs__().max()
    )
    w_gap = (
        (use.loc[[str(r) for r in W_a.index], industries].to_numpy() - W_a.to_numpy())
        .__abs__()
        .max()
    )
    print(
        f'Use interior replay max cell gap: ${u_gap / MILLION_CURRENCY_TO_CURRENCY:,.3f}M'
    )
    print(
        f'Value-added replay max cell gap: ${w_gap / MILLION_CURRENCY_TO_CURRENCY:,.3f}M'
    )

    # Import leg. The allocation spreads over industry AND final-demand
    # columns; the replay frame above carries only industries, so append the
    # (invariant) published final-demand block first. The control is each
    # commodity's total imports - the row sum over both buyer kinds,
    # excluding the negative F05000 balancer itself.
    from bedrock.extract.iot.io_2017 import (  # noqa: PLC0415
        load_2017_Yimp_usa,
        load_2017_Ytot_before_redef_usa,
    )

    Y_b = load_2017_Ytot_before_redef_usa().astype(float)
    fd_block = (
        pd.DataFrame(
            Y_b.to_numpy(), index=commodities, columns=[str(c) for c in Y_b.columns]
        )
        .reindex(index=list(use.index))
        .fillna(0.0)
    )
    use = pd.concat([use, fd_block], axis=1)

    Uimp_a = load_2017_Uimp_after_redef_usa().astype(float)
    Yimp_a = load_2017_Yimp_usa().astype(float)
    control = pd.Series(
        Uimp_a.sum(axis=1).to_numpy(dtype=float)
        + Yimp_a.drop(columns=[_F05000], errors='ignore')
        .sum(axis=1)
        .to_numpy(dtype=float),
        index=commodities,
    ).clip(lower=0.0)
    imports = import_after_redef(use, control)
    imp_gap_m = (
        np.abs(
            imports.loc[commodities, industries].to_numpy() - Uimp_a.to_numpy()
        ).max()
        / MILLION_CURRENCY_TO_CURRENCY
    )
    print(
        f'Import replay max cell gap: ${imp_gap_m:,.1f}M '
        '(allocation rule, not a carried table - report only)'
    )

    # Margins leg: scale the published before table by the replayed Use cell
    # ratios and grade the goods rows against the published after table. The
    # published layout has no per-margin-commodity columns, so margin-
    # commodity rows are recomputable only in the hyper-detailed store;
    # here they pass through and the goods rows carry the grade.
    from bedrock.extract.iot.io_2017 import (  # noqa: PLC0415
        load_2017_margins_after_redef_usa,
        load_2017_margins_before_redef_usa,
    )
    from bedrock.transform.iot.margin_rates import MARGIN_COMMODITIES  # noqa: PLC0415

    pur, pro = "Purchasers' Value", "Producers' Value"
    mb = load_2017_margins_before_redef_usa().astype(float)
    ma = load_2017_margins_after_redef_usa().astype(float)
    scaled = margins_after_redef(mb, use_before.loc[commodities], use.loc[commodities])
    goods_idx = scaled.index[
        ~scaled.index.get_level_values('Commodity Code').isin(set(MARGIN_COMMODITIES))
    ].intersection(ma.index)
    m_pur = (scaled.loc[goods_idx, pur] - ma.loc[goods_idx, pur]).abs()
    m_pro = (scaled.loc[goods_idx, pro] - ma.loc[goods_idx, pro]).abs()
    print(
        f'Margins replay (goods rows): PUR L1 ${m_pur.sum() / 1e9:,.1f}B, '
        f'PRO L1 ${m_pro.sum() / 1e9:,.1f}B on '
        f'${ma.loc[goods_idx, pur].abs().sum() / 1e9:,.0f}B '
        '(proportional-scaling assumption - report only)'
    )

    # Ceilings are publication-rounding scale, not zero: the published pair's
    # own column sums disagree by up to $10M (cell rounding), while this
    # construction conserves exactly - so a perfect method still shows ~$20M
    # single-cell gaps against the printed tables.
    failures: list[str] = []
    for label, gap, ceiling_m in (
        ('Make', v_gap, 25.0),
        ('Use interior', u_gap, 25.0),
        ('value added', w_gap, 25.0),
    ):
        if gap > ceiling_m * MILLION_CURRENCY_TO_CURRENCY:
            failures.append(
                f'{label} replay gap '
                f'${gap / MILLION_CURRENCY_TO_CURRENCY:,.1f}M exceeds '
                f'${ceiling_m}M'
            )
    # Regression tripwires on the two report-only legs. Neither number is a
    # claim the module makes - the import leg grades an allocation rule
    # against a published matrix built differently, and the margins leg
    # grades the proportional-scaling assumption against BEA's re-booking -
    # but a silent regression past these generous ceilings should fail
    # loudly. Measured values when set: import max cell $40.4bn, margins
    # goods-row PUR L1 2.8% of mass.
    if imp_gap_m > 60_000.0:
        failures.append(
            f'import replay max cell ${imp_gap_m:,.0f}M exceeds the '
            '$60,000M regression tripwire'
        )
    margins_l1_share = float(m_pur.sum() / ma.loc[goods_idx, pur].abs().sum())
    if margins_l1_share > 0.04:
        failures.append(
            f'margins goods-row PUR L1 {margins_l1_share:.1%} exceeds the '
            '4% regression tripwire'
        )
    if failures:
        print('\nFAILED:')
        for failure in failures:
            print(f'  - {failure}')
        return 1
    print('\nOK: the 2017 replay holds')
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--check', action='store_true', help='run the 2017 replay')
    parser.add_argument(
        '--year',
        type=int,
        action='append',
        help='stored Step 6 year to convert; repeatable. Default 2017-2023',
    )
    parser.add_argument('--no-save', action='store_true')
    parser.add_argument(
        '--gcs', action='store_true', help='also upload to GCS (needs credentials)'
    )
    args = parser.parse_args(argv)
    if args.check:
        return check()

    anchor = published_anchor()
    for year in args.year if args.year else range(2017, 2024):
        tables, before_inputs = after_redef_tables(year, anchor)
        print(
            f'{year}: Make {tables["Make"].shape}, Use {tables["Use"].shape}, '
            f'Import {tables["Import"].shape}, '
            f'Margins {len(tables["Margins"]):,} rows'
        )
        if not args.no_save:
            written = save_after_redef(
                year, tables, upload=args.gcs, before_inputs=before_inputs
            )
            print(f'  saved {len(written)} files to {written[0].parent}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
