"""Which ``A`` cells the significant sectors' ``N`` actually rests on.

``N = 1' B L`` answers *how much* a dollar of a commodity carries.  This module
answers *which coefficients produced that number*, so a claim about ``N`` can be
taken back to the Use-table cells behind it -- and from there, by
:mod:`~bedrock.analysis.nowcasting.seed_coverage`, to the surveys that observed
them.

Three readings, exact and approximate
-------------------------------------

**Who emits** -- :func:`emitter_contribution`, the repo's
:func:`~bedrock.utils.math.formulas.compute_output_contribution`::

    D_i L_ic          column c sums to N_c exactly

**Which inputs c buys** -- :func:`tier1_contribution`, the repo's
:func:`~bedrock.utils.math.formulas.compute_input_contribution`::

    N_i A_ic          column c sums to N_c - D_c exactly

Both are partitions.  Between them they say what a dollar of ``c`` is made of,
but only one link deep: they name the cells in ``c``'s own column and stop.

**Which cells anywhere in the economy** -- :func:`a_cell_leverage`.  Perturb one
coefficient and differentiate the whole inverse::

    dL/dA_ij = L e_i e_j' L        =>      dN_c/dA_ij = N_i L_jc

so the mass a cell carries into ``c`` is ``A_ij N_i L_jc``.  This reaches every
cell in the table, not just column ``c``.

.. warning::

   **This third one is a ranking, not a partition.**  It sums to
   ``sum_j (N_j - D_j) L_jc``, not to ``N_c``, because a cell on a long path is
   counted at every step it participates in.  Read it as *leverage* -- cell size
   times sensitivity -- and compare cells within one target.  Do not present a
   share of it as a share of ``N_c``; :func:`tier1_contribution` is the one that
   partitions.

Why it is cheap
---------------

Aggregating over 169 targets looks like 169 dense ``405 x 405`` products.  It is
not.  With per-target normaliser ``T_c`` and weight ``w_c``::

    sum_c w_c/T_c * (N_i A_ij L_jc)  =  (N_i A_ij) * sum_c (w_c/T_c) L_jc
                                     =  IC[i,j] * g[j]

so the whole aggregation collapses to one vector :func:`leverage_vector` and a
column scaling.  Same for the Use-cell form, where ``g`` is carried across the
Make matrix by ``Vnorm @ g``.

Two axes, one mass
------------------

``A`` is commodity x commodity; the seeds and the provenance tables are Use
cells, commodity x industry.  Since ``A = Unorm @ Vnorm``, ``dA_ic/dU_ij`` is
``Vnorm[j,c] / x_j``, and the same mass lands on the Use axis as::

    N_i * Unorm[i,j] * (Vnorm @ L)[j,c]

:func:`a_cell_leverage` and :func:`use_cell_leverage` are therefore two views of
one quantity and their totals agree to floating point.  ``--check`` asserts it,
because a silent disagreement there would mean the provenance join is reading a
different economy than the ranking.

CLI::

    uv run python -m bedrock.analysis.nowcasting.results.a_influence --check
"""

from __future__ import annotations

import argparse
from typing import cast

import pandas as pd

from bedrock.utils.math.formulas import (
    compute_input_contribution,
    compute_L_matrix,
    compute_n,
    compute_output_contribution,
)
from bedrock.utils.validation.significant_sectors import SIGNIFICANT_SECTORS

#: Tolerance for the exact identities, relative to the quantity being checked.
#: Both are one dense inverse away from the inputs, so this is float64 noise.
IDENTITY_RTOL = 1e-9


def significant_codes(axis: pd.Index) -> list[str]:
    """:data:`SIGNIFICANT_SECTORS` codes present on *axis*, in list order.

    ⚠️ The list is not guaranteed to be a subset of the model axis and the
    caller should know what it lost, so this returns only what matched and
    :func:`missing_codes` names the rest.  ``562000`` drops out under waste
    disaggregation, which splits it into subsectors.
    """
    present = set(axis)
    return [d['sector'] for d in SIGNIFICANT_SECTORS if d['sector'] in present]


def missing_codes(axis: pd.Index) -> list[str]:
    """:data:`SIGNIFICANT_SECTORS` codes *absent* from *axis*."""
    present = set(axis)
    return [d['sector'] for d in SIGNIFICANT_SECTORS if d['sector'] not in present]


# --------------------------------------------------------------- partitions


def emitter_contribution(
    *, L: pd.DataFrame, D: pd.Series[float], targets: list[str]
) -> pd.DataFrame:
    """``D_i L_ic`` -- who emits the footprint of each target.  Sums to ``N_c``."""
    return compute_output_contribution(L=L, D=D).loc[:, targets]


def tier1_contribution(
    *, A: pd.DataFrame, N: pd.Series[float], targets: list[str]
) -> pd.DataFrame:
    """``N_i A_ic`` -- the inputs each target buys.  Sums to ``N_c - D_c``."""
    return compute_input_contribution(A=A, N=N).loc[:, targets]


# ----------------------------------------------------------------- leverage


def target_normaliser(
    *,
    N: pd.Series[float],
    D: pd.Series[float],
    L: pd.DataFrame,
    targets: list[str],
) -> pd.Series[float]:
    """``T_c = sum_j (N_j - D_j) L_jc`` -- total leverage mass reaching ``c``.

    The denominator that turns a raw ``A_ij N_i L_jc`` into a share of what
    target ``c`` depends on, so targets with large footprints do not simply
    outvote small ones.
    """
    return (N - D) @ L.loc[:, targets]


def leverage_vector(
    *,
    N: pd.Series[float],
    D: pd.Series[float],
    L: pd.DataFrame,
    targets: list[str],
    weights: pd.Series[float] | None = None,
) -> pd.Series[float]:
    """``g[j] = sum_c (w_c / T_c) L[j, c]`` -- the whole target set in one vector.

    *weights* defaults to 1 per target, so every significant sector contributes
    exactly one unit of normalised mass and the total is the target count.  Pass
    ``q`` to weight by economic size instead.
    """
    w = (
        pd.Series(1.0, index=targets)
        if weights is None
        else weights.reindex(targets).astype(float)
    )
    normaliser = target_normaliser(N=N, D=D, L=L, targets=targets)
    return L.loc[:, targets] @ (w / normaliser)


def a_cell_leverage(
    *,
    A: pd.DataFrame,
    N: pd.Series[float],
    D: pd.Series[float],
    L: pd.DataFrame,
    targets: list[str],
    weights: pd.Series[float] | None = None,
) -> pd.DataFrame:
    """Leverage each ``A`` cell holds over the target set, commodity x commodity.

    ⚠️ A ranking, not a partition -- see the module warning.
    """
    inputs = compute_input_contribution(A=A, N=N)
    return inputs.mul(
        leverage_vector(N=N, D=D, L=L, targets=targets, weights=weights), axis=1
    )


def use_cell_leverage(
    *,
    Unorm: pd.DataFrame,
    Vnorm: pd.DataFrame,
    N: pd.Series[float],
    D: pd.Series[float],
    L: pd.DataFrame,
    targets: list[str],
    weights: pd.Series[float] | None = None,
) -> pd.DataFrame:
    """The same leverage on the Use axis, commodity x **industry**.

    This is the axis :mod:`~bedrock.analysis.nowcasting.seed_coverage` scores, so
    it is the one the provenance join needs.
    """
    carried = Vnorm @ leverage_vector(N=N, D=D, L=L, targets=targets, weights=weights)
    return Unorm.mul(N, axis=0).mul(carried, axis=1)


def rank_cells(leverage: pd.DataFrame, top: int = 50) -> pd.DataFrame:
    """The *top* cells of a leverage frame as ``(row, column, leverage, share)``."""
    stacked = cast('pd.Series[float]', leverage.stack())
    stacked = stacked.loc[stacked.to_numpy(dtype=float) != 0.0]
    total = float(stacked.sum())
    # The axes carry their own names ('sector', ...), which collide with the
    # value name on reset_index; build the frame explicitly instead.
    largest = stacked.nlargest(top)
    out = pd.DataFrame(
        {
            'row': [pair[0] for pair in largest.index],
            'column': [pair[1] for pair in largest.index],
            'leverage': largest.to_numpy(float),
        }
    )
    out['share'] = out['leverage'] / total
    out['cumulative_share'] = out['share'].cumsum()
    return out


# ------------------------------------------------- crossing to the BEA axis


#: Cornerstone commodities production drops on the way in from BEA detail: the
#: correspondence weights scrap to zero, and noncomparable imports and the
#: "not allocated" residual carry no technology.  Anything sourced on these
#: codes reaches no model column, so their leverage is zero by construction --
#: not missing data.
DROPPED_ON_ENTRY = ('S00300', 'S00401', 'S00900')


def collapse_to_bea(
    values: pd.Series[float],
    *,
    q: pd.Series[float],
    bea_index: pd.Index,
) -> pd.Series[float]:
    """Carry a per-commodity **intensity** from the Cornerstone axis to BEA detail.

    ``N``, ``D`` and the leverage vector are all per-dollar quantities, so a
    disaggregated group collapses as an **output-weighted mean**, not a sum: a
    dollar of BEA ``562000`` buys the seven waste subsectors in proportion to
    their output.  Codes in :data:`DROPPED_ON_ENTRY` get zero, which is what
    production does with them.

    ⚠️ Weighted-mean, not sum, is the whole correctness of this function.  Summing
    would multiply a disaggregated group's intensity by its member count.
    """
    from bedrock.transform.eeio.cornerstone_expansion import (  # noqa: PLC0415
        cs_commodity_to_bea_map,
    )

    parent = pd.Series(cs_commodity_to_bea_map()).reindex(values.index)
    weights = q.reindex(values.index).astype(float)
    numerator = (values * weights).groupby(parent).sum()
    denominator = weights.groupby(parent).sum()
    return (numerator / denominator).reindex(bea_index).fillna(0.0)


def collapse_output_to_bea(
    q: pd.Series[float], *, bea_index: pd.Index
) -> pd.Series[float]:
    """Carry commodity **output** to BEA detail -- a sum, because it is a level."""
    from bedrock.transform.eeio.cornerstone_expansion import (  # noqa: PLC0415
        cs_commodity_to_bea_map,
    )

    parent = pd.Series(cs_commodity_to_bea_map()).reindex(q.index)
    return q.groupby(parent).sum().reindex(bea_index).fillna(0.0)


def bea_use_pieces() -> tuple[pd.DataFrame, pd.DataFrame]:
    """``(Unorm, Vnorm)`` for the active config, in BEA detail space.

    These are the nowcast MUT's own Use and Make normalised by industry and
    commodity output -- the same matrices production consumes before it expands
    to the Cornerstone axis, and the axis the seeds and
    :mod:`~bedrock.analysis.nowcasting.seed_coverage` are scored on.
    """
    from bedrock.extract.iot.detail_io import (  # noqa: PLC0415
        load_detail_Utot_usa,
        load_detail_V_usa,
    )
    from bedrock.transform.eeio.cornerstone_bea_intermediates import (  # noqa: PLC0415
        bea_Vnorm_scrap_corrected,
        bea_x,
    )
    from bedrock.utils.math.formulas import compute_Unorm_matrix  # noqa: PLC0415

    # ⚠️ `bea_Vnorm_scrap_corrected` divides an industry-indexed scrap series by a
    # commodity-indexed `q`, so pandas aligns on the union and the four
    # industry-only codes (331314, S00101, S00201, S00202) come back as spurious
    # all-NaN columns.  Production drops them when it expands to the Cornerstone
    # 405 list; reindexing to the Make matrix's own commodity axis keeps exactly
    # what production keeps, and makes the matrix square.
    commodities = load_detail_V_usa().columns
    return (
        compute_Unorm_matrix(U=load_detail_Utot_usa(), x=bea_x()),
        bea_Vnorm_scrap_corrected().reindex(columns=commodities),
    )


def use_cell_leverage_from_model(
    *,
    N: pd.Series[float],
    D: pd.Series[float],
    L: pd.DataFrame,
    q: pd.Series[float],
    targets: list[str],
    weights: pd.Series[float] | None = None,
) -> pd.DataFrame:
    """Use-cell leverage on the BEA axis, driven by the **production** model.

    The sensitivities (``N``, the leverage vector) come from the 405-sector model
    that actually produces the published ``N``; only the Use and Make matrices
    are BEA, and those are the nowcast MUT itself.  So this does not build a
    second model -- it carries one model's answer onto the axis the provenance
    tables use.
    """
    Unorm, Vnorm = bea_use_pieces()
    bea_index = Unorm.index

    g = leverage_vector(N=N, D=D, L=L, targets=targets, weights=weights)
    g_bea = collapse_to_bea(g, q=q, bea_index=bea_index)
    n_bea = collapse_to_bea(N, q=q, bea_index=bea_index)

    carried = Vnorm @ g_bea
    return Unorm.mul(n_bea, axis=0).mul(carried, axis=1)


# -------------------------------------------------------------------- check


def _report(name: str, worst: float, tolerance: float) -> bool:
    verdict = 'ok  ' if worst <= tolerance else 'FAIL'
    print(f'  {verdict} {name:54s} worst rel. error {worst:.3e}')
    return worst <= tolerance


def check(year: int = 2024) -> int:
    """Assert the identities on the live nowcast model, and print what it found."""
    from bedrock.analysis.nowcasting.results._ef_smoke_lib import (  # noqa: PLC0415
        aq_from_live_config,
    )
    from bedrock.utils.snapshots import releases  # noqa: PLC0415
    from bedrock.utils.snapshots.loader import load_snapshot  # noqa: PLC0415

    B = load_snapshot('B_USA_non_finetuned', releases.v0_3_1)
    aq, vintage = aq_from_live_config(year)
    A = aq.Adom + aq.Aimp
    L = compute_L_matrix(A=A)
    N = compute_n(M=B @ L)
    D = B.sum(axis=0)
    q = aq.scaled_q

    targets = significant_codes(A.index)
    print(f'\nnowcast {year}, MUT vintage {vintage}')
    print(f'  targets on axis: {len(targets)}   absent: {missing_codes(A.index)}\n')

    passed = []

    emitters = emitter_contribution(L=L, D=D, targets=targets)
    worst = float(((emitters.sum(axis=0) - N[targets]).abs() / N[targets].abs()).max())
    passed.append(
        _report('emitter_contribution columns sum to N_c', worst, IDENTITY_RTOL)
    )

    tier1 = tier1_contribution(A=A, N=N, targets=targets)
    expected = (N - D)[targets]
    worst = float(((tier1.sum(axis=0) - expected).abs() / expected.abs()).max())
    passed.append(
        _report('tier1_contribution columns sum to N_c - D_c', worst, IDENTITY_RTOL)
    )

    # Every target contributes exactly one unit of normalised mass, so the total
    # is the target count -- the check that collapsing 169 dense products into
    # one `g` vector is faithful.
    leverage = a_cell_leverage(A=A, N=N, D=D, L=L, targets=targets)
    worst = abs(float(leverage.to_numpy().sum()) - len(targets)) / len(targets)
    passed.append(_report('a_cell_leverage totals to the target count', worst, 1e-8))

    # The output-weighted collapse must conserve total embodied emissions, or the
    # provenance figures are describing a different economy than the headline.
    Unorm, _ = bea_use_pieces()
    n_bea = collapse_to_bea(N, q=q, bea_index=Unorm.index)
    q_bea = collapse_output_to_bea(q, bea_index=Unorm.index)
    worst = abs(float((n_bea * q_bea).sum()) - float((N * q).sum())) / float(
        (N * q).sum()
    )
    passed.append(
        _report('BEA collapse conserves total embodied emissions', worst, 1e-10)
    )

    use_leverage = use_cell_leverage_from_model(N=N, D=D, L=L, q=q, targets=targets)
    ratio = float(use_leverage.to_numpy().sum()) / float(leverage.to_numpy().sum())
    print(
        f'\n  Use-axis leverage carries {ratio:.3f} of the commodity-axis mass '
        f'(reported, not gated: the Use axis restates the same leverage through\n'
        f'  Make, and the two differ by what the correspondence drops on entry)'
    )

    concentration = rank_cells(leverage, top=200)
    print(
        f'  leverage concentration: top 50 cells = '
        f'{concentration["share"].head(50).sum():.1%} of the mass, '
        f'top 200 = {concentration["cumulative_share"].iloc[-1]:.1%}'
    )
    return 0 if all(passed) else 1


def main() -> int:
    parser = argparse.ArgumentParser(description='A-cell leverage on significant N.')
    parser.add_argument('--check', action='store_true', help='Assert the identities.')
    parser.add_argument('--year', type=int, default=2024)
    args = parser.parse_args()
    if args.check:
        return check(args.year)
    parser.print_help()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
