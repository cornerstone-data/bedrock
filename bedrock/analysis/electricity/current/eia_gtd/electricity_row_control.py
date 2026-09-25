"""What actually drives the electricity Use row, 2017-2024.

``About_price_proposal.md`` blocked its sections A and C on an unexplained
2021-22 "excursion": the derived Use electricity row sits +31% over EIA-861
non-residential retail revenue at 2017, balloons to +67.5% commercial in 2021,
then falls back below the 2017 base by 2024.  A scope difference does not do
that, so the movement was read as a nowcast defect.

It is not.  This module measures three things and the first two exonerate the
nowcast:

1. **The swing is BEA's.**  Our 221100 industry gross output tracks BEA's
   published ``UGO305-A`` to within -1.7% to -2.0% in every year of the span,
   and the two growth rates agree to 0.4pp or better every year.  BEA's own
   series rises 20.5% in 2021 and falls 8.8% in 2023.  Inside BEA's series the
   move is a fuel and purchased-power cost pass-through: intermediate inputs to
   221100 move +36.1%, +23.7%, then -30.4%, which is the gas price cycle.

2. **The intermediate row is a residual, so it absorbs all of it.**
   ``intermediate = q(221100) - Y(221100)`` is an identity here, and ``Y`` is
   EIA-anchored -- PCE matches EIA residential to +0.1% at 2017 and stays
   within a few percent across the span.  Every dollar of movement in the
   commodity output control therefore lands on the intermediate block, none of
   it on final demand.

3. **The comparison itself was mis-specified.**  BEA's 221100 *industry* gross
   output is $389.4bn in 2017 against EIA-861's $390.3bn of all-seller retail
   revenue -- they agree to -0.2%.  The *commodity* row is $455.2bn because
   government-owned utilities make electricity as a secondary product:
   ``S00202`` state and local ($63.4bn) and ``S00101`` federal ($15.7bn).
   Federal electric utilities are TVA, BPA and the power marketing
   administrations, whose output is overwhelmingly **sales for resale** -- yet
   only $1.98bn of electricity is bought by any utility in the table, so that
   wholesale leg is being matched against a retail benchmark.

What does survive as a nowcast defect is narrower, and lands in different years
than the proposal claimed: the *allocation* of the row across purchasers.  Each
year-over-year change decomposes into a column effect (the purchaser's whole
input column grew and electricity rode along) and a share effect (electricity
took a larger bite of that column).  2021's rise is mostly the column effect --
nominal input costs surged economy-wide.  The 2023 and 2024 falls are almost
entirely share effect, against columns that barely moved.

::

    python -m bedrock.analysis.electricity.current.eia_gtd.electricity_row_control
    # options: --years 2017-2024  --top 12  --csv  --check
    #          --mut-vintage v0.3.0_4276083   (skip the GCS probe for the newest
    #                                          upload; the year configs omit the pin)
"""

from __future__ import annotations

import argparse
import logging
import typing as ta
from pathlib import Path

import pandas as pd

from bedrock.utils.taxonomy.cornerstone.commodities import COMMODITY_DESC

logger = logging.getLogger(__name__)


class YearPanel(ta.NamedTuple):
    """One year's slice of the tables this module reads.

    ``make`` is the Make COLUMN -- who makes the electricity commodity.
    ``industry_output`` is the Make ROW total -- the electricity industry's
    whole output, which is what BEA's ``UGO305-A`` gross output measures.
    Keeping both is the point of this module.
    """

    make: 'pd.Series[float]'
    industry_output: float
    elec: 'pd.Series[float]'
    coltot: 'pd.Series[float]'
    y: 'pd.Series[float]'
    all_intermediate: float


PanelByYear = dict[int, YearPanel]

OUT_DIR = Path(__file__).resolve().parent
ELECTRICITY_ROW = '221100'
PCE_CODE = 'F01000'

#: Secondary makers of the electricity commodity, and why they matter here.
GOVERNMENT_MAKERS = {
    'S00202': 'State and local government electric utilities',
    'S00101': 'Federal electric utilities (TVA, BPA, the PMAs)',
}

#: Customer-class order for EPA Table 2.3 / former ``EIA_861_REVENUE_BN`` tuples.
_EIA_REVENUE_SECTORS = (
    'Residential',
    'Commercial',
    'Industrial',
    'Transportation',
)
_TABLE_2_3_DESCRIPTION = (
    'Table 2.3 Revenue from sales of electricity to ultimate customers'
)
_TABLE_2_3_PROVIDER = 'Total Electric Industry'
_USD_TO_BN = 1e-9


def eia_epa_table_2_3_revenue_bn(year: int) -> tuple[float, float, float, float]:
    """Retail revenue by customer class ($bn) from ``EIA_ElectricPowerAnnual`` Table 2.3.

    Reads the existing FBA (not a hand-transcribed literal). Provider is
    ``Total Electric Industry``. Returns
    ``(residential, commercial, industrial, transportation)``.
    """
    from bedrock.extract.flowbyactivity import getFlowByActivity  # noqa: PLC0415

    df = getFlowByActivity('EIA_ElectricPowerAnnual', year)
    mask = (
        (df['Year'] == year)
        & (
            df['Description']
            .astype(str)
            .str.startswith(_TABLE_2_3_DESCRIPTION, na=False)
        )
        & (df['ActivityProducedBy'] == _TABLE_2_3_PROVIDER)
    )
    subset = df.loc[mask]
    values: list[float] = []
    for sector in _EIA_REVENUE_SECTORS:
        rows = subset.loc[subset['ActivityConsumedBy'] == sector, 'FlowAmount']
        if rows.empty:
            raise ValueError(
                f'Table 2.3 missing sector {sector!r} for year {year}, '
                f'provider {_TABLE_2_3_PROVIDER!r}'
            )
        values.append(float(rows.iloc[0]) * _USD_TO_BN)
    return values[0], values[1], values[2], values[3]


#: Tolerances for ``--check``.  The offset to BEA gross output is a level
#: difference we expect and do not police; what must hold is that it is *flat*,
#: because a flat offset is what makes the swing BEA's rather than ours.
#:
#: ``q - Y - intermediate`` is the supply-use gap on this one commodity after
#: redefinitions, not an enforced identity, so it is policed as a share of the
#: row rather than in dollars.  It runs under $1.7bn in seven of eight years;
#: 2022 is $10.4bn, which is 1.6% of a $646bn row and is noted in
#: ``About_price_proposal.md`` rather than silently tightened away.
_RESIDUAL_RTOL_PCT = 2.0
_GROWTH_ATOL_PP = 1.0
_OFFSET_SPREAD_ATOL_PP = 1.0
_PCE_ANCHOR_ATOL_PCT = 10.0
#: Only print the per-column breakdown where the share effect is material.
_SHARE_EFFECT_REPORT_BN = 20.0


#: ``COMMODITY_DESC`` is keyed on literal codes; the Make column also carries
#: government-enterprise industries, so flatten both to one plain lookup.
_NAMES: dict[str, str] = {str(k): str(v) for k, v in COMMODITY_DESC.items()}
_NAMES.update(GOVERNMENT_MAKERS)


def _desc(code: str) -> str:
    return _NAMES.get(code, '')


def _row(frame: pd.DataFrame, label: str) -> 'pd.Series[float]':
    """One row as a float Series; a duplicated label would silently give a frame."""
    selected = frame.loc[label]
    if isinstance(selected, pd.DataFrame):
        raise ValueError(f'{label!r} is duplicated in the index, got {selected.shape}')
    return selected.astype(float)


def _activate(year: int, mut_vintage: str | None) -> str:
    """Install the nowcast config for ``year``; optionally pin the MUT vintage.

    The per-year configs deliberately omit ``nowcast_mut_vintage`` so loaders
    take the newest GCS upload.  That needs live credentials, which a diagnostic
    should not require, so ``--mut-vintage`` pins a build already on disk.
    """
    from bedrock.analysis.nowcasting.results._ef_smoke_lib import (  # noqa: PLC0415
        clear_year_caches,
        config_stem,
        resolved_mut_vintage,
    )
    from bedrock.utils.config.usa_config import (  # noqa: PLC0415
        get_usa_config,
        reset_usa_config,
        set_global_usa_config,
    )

    clear_year_caches()
    reset_usa_config()
    set_global_usa_config(f'{config_stem(year)}.yaml')
    if mut_vintage:
        object.__setattr__(get_usa_config(), 'nowcast_mut_vintage', mut_vintage)
        return mut_vintage
    return resolved_mut_vintage()


def load_year(year: int, mut_vintage: str | None) -> YearPanel:
    """Make column, Use row, Y row and column totals for the electricity commodity."""
    from bedrock.extract.iot.nowcast_mut_storage import (  # noqa: PLC0415
        load_nowcast_detail_Utot_usa,
        load_nowcast_detail_V_usa,
        load_nowcast_detail_Ytot_usa,
    )

    _activate(year, mut_vintage)
    V = load_nowcast_detail_V_usa().astype(float)
    U = load_nowcast_detail_Utot_usa().astype(float)
    Y = load_nowcast_detail_Ytot_usa().astype(float)
    for frame in (V, U, Y):
        frame.index = [str(i) for i in frame.index]
        frame.columns = [str(c) for c in frame.columns]
    return YearPanel(
        make=V[ELECTRICITY_ROW],
        industry_output=float(_row(V, ELECTRICITY_ROW).sum()),
        elec=_row(U, ELECTRICITY_ROW),
        coltot=U.sum(axis=0),
        y=_row(Y, ELECTRICITY_ROW),
        all_intermediate=float(U.to_numpy().sum()),
    )


def row_control_table(panel: PanelByYear, years: list[int]) -> pd.DataFrame:
    """The output control, its BEA source, and what the row is left to absorb."""
    from bedrock.transform.iot.derived_intermediate_and_value_added import (  # noqa: PLC0415
        detail_gross_output_panel,
        detail_intermediate_inputs_panel,
        detail_value_added_panel,
    )

    # The raw arm: the EC conditioning of #724 is manufacturing-only and must
    # not be read as movement in the utilities series.
    bea_go = detail_gross_output_panel(ec_adjusted=False).loc[ELECTRICITY_ROW]
    bea_va = detail_value_added_panel().loc[ELECTRICITY_ROW]
    bea_ii = detail_intermediate_inputs_panel().loc[ELECTRICITY_ROW]

    rows = {}
    for year in years:
        p = panel[year]
        make, elec, y_row = p.make, p.elec, p.y
        residential, commercial, industrial, transportation = (
            eia_epa_table_2_3_revenue_bn(year)
        )
        rows[year] = {
            'q_commodity': float(make.sum()) / 1e9,
            'go_industry': p.industry_output / 1e9,
            'own_commodity': float(make.get(ELECTRICITY_ROW, 0.0)) / 1e9,
            'bea_go': float(bea_go[year]) / 1e3,
            'bea_va': float(bea_va[year]) / 1e3,
            'bea_ii': float(bea_ii[year]) / 1e3,
            'gov_secondary': float(sum(make.get(c, 0.0) for c in GOVERNMENT_MAKERS))
            / 1e9,
            'intermediate': float(elec.sum()) / 1e9,
            'Y': float(y_row.sum()) / 1e9,
            'pce': float(y_row.get(PCE_CODE, 0.0)) / 1e9,
            'eia_retail': residential + commercial + industrial + transportation,
            'eia_residential': residential,
            'all_intermediate': p.all_intermediate / 1e9,
        }

    t = pd.DataFrame(rows).T
    t['ours_vs_bea_go%'] = (t['go_industry'] / t['bea_go'] - 1) * 100
    t['bea_go_g%'] = t['bea_go'].pct_change(fill_method=None) * 100
    t['ours_go_g%'] = t['go_industry'].pct_change(fill_method=None) * 100
    t['bea_ii_g%'] = t['bea_ii'].pct_change(fill_method=None) * 100
    t['eia_g%'] = t['eia_retail'].pct_change(fill_method=None) * 100
    t['pce_vs_eia_res%'] = (t['pce'] / t['eia_residential'] - 1) * 100
    t['elec_share_of_U%'] = t['intermediate'] / t['all_intermediate'] * 100
    t['residual_check'] = t['q_commodity'] - t['Y'] - t['intermediate']
    return t


def decomposition(panel: PanelByYear, years: list[int]) -> pd.DataFrame:
    """Split each year-over-year row move into a column effect and a share effect.

    ``elec_j * col_growth_j`` is what the purchaser's whole input column did and
    electricity rode along with; the remainder is electricity changing its share
    of that column, which is the only part the electricity method owns.
    """
    rows = []
    for a, b in zip(years, years[1:]):
        j = _joined(panel, a, b)
        growth = (j['col_b'] / j['col_a'].replace(0.0, float('nan')) - 1).fillna(0.0)
        column_effect = float((j['elec_a'] * growth).sum()) / 1e9
        total = float((j['elec_b'] - j['elec_a']).sum()) / 1e9
        rows.append(
            {
                'pair': f'{a}->{b}',
                'd_row': total,
                'column_effect': column_effect,
                'share_effect': total - column_effect,
                'share_%_of_move': (
                    (total - column_effect) / total * 100 if total else float('nan')
                ),
                'all_U_g%': (panel[b].all_intermediate / panel[a].all_intermediate - 1)
                * 100,
            }
        )
    return pd.DataFrame(rows).set_index('pair')


def _joined(panel: PanelByYear, a: int, b: int) -> pd.DataFrame:
    return pd.DataFrame(
        {
            'elec_a': panel[a].elec,
            'elec_b': panel[b].elec,
            'col_a': panel[a].coltot,
            'col_b': panel[b].coltot,
        }
    ).fillna(0.0)


def share_effect_by_column(
    panel: PanelByYear, a: int, b: int, top: int
) -> pd.DataFrame:
    """The purchasers whose electricity share of their own column moved most."""
    j = _joined(panel, a, b)
    growth = (j['col_b'] / j['col_a'].replace(0.0, float('nan')) - 1).fillna(0.0)
    j['share_effect'] = (j['elec_b'] - j['elec_a'] - j['elec_a'] * growth) / 1e9
    j['elec_g%'] = (j['elec_b'] / j['elec_a'].replace(0.0, float('nan')) - 1) * 100
    j['col_g%'] = growth * 100
    j['elec_a'] = j['elec_a'] / 1e9
    j['elec_b'] = j['elec_b'] / 1e9
    j = j.reindex(j['share_effect'].abs().sort_values(ascending=False).index).head(top)
    j.insert(0, 'name', [_desc(str(i))[:38] for i in j.index])
    return j[['name', 'elec_a', 'elec_b', 'elec_g%', 'col_g%', 'share_effect']]


def run_checks(t: pd.DataFrame) -> int:
    """The identities this module's argument rests on.  Returns the failure count."""
    failures = 0

    residual = float((t['residual_check'].abs() / t['q_commodity'] * 100).max())
    if residual > _RESIDUAL_RTOL_PCT:
        print(
            'FAIL  the intermediate block is not the residual of q less final '
            f'demand: worst supply-use gap {residual:,.2f}% of the row'
        )
        failures += 1

    drift = float((t['ours_go_g%'] - t['bea_go_g%']).abs().max())
    if drift > _GROWTH_ATOL_PP:
        print(f'FAIL  our 221100 GO growth departs from BEA by {drift:,.2f}pp')
        failures += 1

    offset = t['ours_vs_bea_go%']
    spread = float(offset.max() - offset.min())
    if spread > _OFFSET_SPREAD_ATOL_PP:
        print(
            'FAIL  the offset to BEA gross output is not flat: '
            f'{offset.min():,.2f}% to {offset.max():,.2f}%'
        )
        failures += 1

    anchor = float(t['pce_vs_eia_res%'].abs().max())
    if anchor > _PCE_ANCHOR_ATOL_PCT:
        print(f'FAIL  PCE is not anchored to EIA residential: worst {anchor:,.1f}%')
        failures += 1

    print(f'check: {failures} failure(s) over {len(t)} years')
    return failures


def _parse_years(spec: str) -> list[int]:
    lo, _, hi = spec.partition('-')
    return list(range(int(lo), int(hi or lo) + 1))


def _fmt(value: float) -> str:
    return f'{value:,.2f}'


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--years', default='2017-2024')
    parser.add_argument('--top', type=int, default=12)
    parser.add_argument('--csv', action='store_true')
    parser.add_argument('--check', action='store_true')
    parser.add_argument('--mut-vintage', default=None)
    args = parser.parse_args()

    years = _parse_years(args.years)
    panel = {year: load_year(year, args.mut_vintage) for year in years}
    t = row_control_table(panel, years)

    if args.check:
        raise SystemExit(1 if run_checks(t) else 0)

    print('=== the output control: ours, BEA UGO305-A, EIA-861 retail, $bn ===')
    print(
        t[
            [
                'go_industry',
                'bea_go',
                'ours_vs_bea_go%',
                'ours_go_g%',
                'bea_go_g%',
                'eia_retail',
                'eia_g%',
            ]
        ].to_string(float_format=_fmt)
    )

    print('\n=== BEA 221100: the swing is a fuel and purchased-power pass-through ===')
    print(t[['bea_go', 'bea_va', 'bea_ii', 'bea_ii_g%']].to_string(float_format=_fmt))

    print('\n=== the commodity row is wider than the industry, $bn ===')
    print(
        t[
            ['q_commodity', 'go_industry', 'gov_secondary', 'intermediate', 'Y', 'pce']
        ].to_string(float_format=_fmt)
    )
    makers = pd.DataFrame({year: panel[year].make for year in years})
    makers = makers[makers.abs().sum(axis=1) > 0] / 1e9
    makers.insert(0, 'name', [_desc(str(i))[:42] for i in makers.index])
    print('\nmakers of the electricity commodity, $bn:')
    print(makers.to_string(float_format=_fmt))

    print('\n=== PCE is anchored, so the intermediate block absorbs everything ===')
    print(
        t[['pce', 'eia_residential', 'pce_vs_eia_res%', 'elec_share_of_U%']].to_string(
            float_format=_fmt
        )
    )

    decomposed = decomposition(panel, years)
    print('\n=== year-over-year: column effect vs share effect, $bn ===')
    print(decomposed.to_string(float_format=_fmt))

    for a, b in zip(years, years[1:]):
        effect = abs(float(decomposed['share_effect'].loc[f'{a}->{b}']))
        if effect < _SHARE_EFFECT_REPORT_BN:
            continue
        print(f'\n=== {a}->{b}: purchasers whose electricity share moved most ===')
        print(
            share_effect_by_column(panel, a, b, args.top).to_string(float_format=_fmt)
        )

    if args.csv:
        control_path = OUT_DIR / 'electricity_row_control.csv'
        decomposition_path = OUT_DIR / 'electricity_row_decomposition.csv'
        t.to_csv(control_path)
        decomposed.to_csv(decomposition_path)
        print(f'\nwrote {control_path}')
        print(f'wrote {decomposition_path}')


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING, format='%(message)s')
    main()
