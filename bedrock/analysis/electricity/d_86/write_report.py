"""Generate the Methods #86 three-path toy analysis report."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from bedrock.analysis.electricity.d_86.toy_paths import (
    Section1ProductionResult,
    Section2FlowMixedResult,
    Section3DirectMixedResult,
    assert_section2_matches_section3,
    rebuild_scaled_dom_imp_flows,
    run_section1_production,
    run_section2_flow_mixed,
    run_section3_direct_mixed,
)
from bedrock.analysis.electricity.d_86.toy_scaling import (
    TOY_DETAIL_YEAR,
    TOY_MODEL_YEAR,
)
from bedrock.analysis.electricity.d_86.toy_tables import TOY_SECTORS
from bedrock.transform.eeio.electricity_disaggregation import GENERATION_SECTOR
from bedrock.utils.math.formulas import (
    backcompute_q_from_L_and_y,
    backcompute_y_from_A_and_q,
)
from bedrock.utils.validation.eeio_diagnostics import format_diagnostic_result

_OUTPUT_DIR = Path(__file__).resolve().parent / 'output'
_REPORT_PATH = _OUTPUT_DIR / 'd_86_analysis_report.md'


def _df(frame: pd.DataFrame, *, float_fmt: str = '.6g') -> str:
    out = frame.copy()
    for col in out.columns:
        if pd.api.types.is_numeric_dtype(out[col]):
            out[col] = out[col].map(lambda v: format(v, float_fmt))
    return out.to_markdown()


def _series(series: pd.Series[float], *, float_fmt: str = '.6g') -> str:
    return _df(series.to_frame(name=series.name or 'value'), float_fmt=float_fmt)


def _q_table(q: pd.Series[float], *, mixed: bool = False) -> str:
    frame = pd.DataFrame({'q': q, 'units': ['MWh' if mixed and s == GENERATION_SECTOR else 'USD' for s in q.index]})
    return _df(frame)


def _step(title: str) -> list[str]:
    return [f'### {title}', '']


def _intro() -> list[str]:
    return [
        '# Methods #86 — Three-path toy analysis (`Adom` + `Aimp` + `Atot`)',
        '',
        'Shared 3×3 toy with domestic/import Use split. All sections use:',
        '',
        '- `Adom = Unorm(Udom) @ Vnorm`, `Aimp = Unorm(Uimp) @ Vnorm`',
        '- `Atot = Adom + Aimp`',
        '- Detail year = '
        f'`{TOY_DETAIL_YEAR}`; model year = `{TOY_MODEL_YEAR}`',
        '',
        '| Section | Path | Mixed units? |',
        '| --- | --- | --- |',
        '| **1** | Main branch (`derive_cornerstone_Aq_scaled` → BLy/D/N) | No — all USD |',
        '| **2** | Analysis: scaled IO rebuild → mixed `V`/`U`/`Y` → rederive `A` | Yes — IO level |',
        '| **3** | PR4 (`build_electricity_mixed_units_aq`) direct on scaled `A`/`q` | Yes — matrix level |',
        '',
    ]


def _monetary_io_tables(m: Section1ProductionResult) -> list[str]:
    mon = m.monetary
    return [
        '**`V` [USD]:**',
        '',
        _df(mon.V),
        '',
        '**`Udom` [USD]:**',
        '',
        _df(mon.Udom),
        '',
        '**`Uimp` [USD]:**',
        '',
        _df(mon.Uimp),
        '',
        '**`Y` [USD]:**',
        '',
        _df(mon.Y),
        '',
        '**`VA` [USD]:**',
        '',
        _df(mon.VA),
        '',
    ]


def _derive_aq_step() -> list[str]:
    return [
        '**Equations:**',
        '',
        '- `q = column-sum(V)`, `x = row-sum(V)`',
        '- `Vnorm = V / q`, `Unorm = U / x`',
        '- `Adom = Unorm(Udom) @ Vnorm`, `Aimp = Unorm(Uimp) @ Vnorm`',
        '- `Atot = Adom + Aimp`',
        '',
    ]


def _scale_inflate_step(scaled: object) -> list[str]:
    si = scaled
    lines = [
        '**Scale** (``scale_cornerstone_A`` / ``scale_cornerstone_q``):',
        '',
        '- `A_scaled = A_detail ⊙ ratio_A`',
        '- `q_scaled = q_detail ⊙ ratio_q`',
        '',
        '**Inflate** (``inflate_cornerstone_A_matrix_with_commodity_pi``):',
        '',
        '- `A_target = diag(p) @ A_scaled @ diag(1/p)`',
        '- `q_target = q_scaled ⊙ p`',
        '',
        '**`ratio_q`:**',
        '',
        _series(si.adom.q_scale_ratio),
        '',
        '**`Adom_target` [model-year USD]:**',
        '',
        _df(si.adom.a_target),
        '',
        '**`Aimp_target` [model-year USD]:**',
        '',
        _df(si.aimp.a_target),
        '',
        '**`Atot_target` [model-year USD]:**',
        '',
        _df(si.atot_target),
        '',
        '**`q_target` [USD]:**',
        '',
        _q_table(si.q_target, mixed=False),
        '',
    ]
    return lines


def _l_tot_caption(*, mixed: bool) -> str:
    if mixed:
        return (
            '**`L_tot = (I − Atot_mixed)⁻¹`** [dimensionless; '
            'computed from hybrid-unit `Atot_mixed`]'
        )
    return '**`L_tot = (I − Atot)⁻¹`** [dimensionless; USD/USD monetary]'


def _ef_step(
    ef: object,
    *,
    mixed: bool,
    section_label: str,
    atot: pd.DataFrame | None = None,
) -> list[str]:
    unit_note = (
        'Hybrid units: `221110` in MWh / kg per MWh where applicable; others in USD.'
        if mixed
        else 'All values in USD (monetary).'
    )
    atot_label = 'Atot_mixed' if mixed else 'Atot_target'
    lines: list[str] = [
        f'**Context ({section_label}):** {unit_note}',
        '',
        '**Equations:**',
        '',
        '- `y_nab = q − rowsum(Adom ⊙ q)` (production uses `Adom`, not `Atot`)',
        f'- `L_tot = (I − {atot_label})⁻¹`',
        '- `L_dom = (I − Adom)⁻¹`',
        '- `D = rowsum(B)`, `M = B @ L_dom`, `N = rowsum(M)`',
        '- `BLy = diag(D) @ L_dom @ y_nab`',
        '',
    ]
    if atot is not None:
        lines += [
            f'**`Atot` used for `L_tot` (`{atot_label}`):**',
            '',
            _df(atot),
            '',
        ]
    lines += [
        _l_tot_caption(mixed=mixed) + ':',
        '',
        _df(ef.l_tot),
        '',
        '**`y_nab`:**',
        '',
        _q_table(ef.y_nab, mixed=mixed),
        '',
        '**`D`:**',
        '',
        _q_table(ef.d, mixed=mixed),
        '',
        '**`N`:**',
        '',
        _q_table(ef.n, mixed=mixed),
        '',
        '**`BLy` [kg CO₂e]:**',
        '',
        _series(ef.bly),
        '',
    ]
    return lines


def _matrix_side_by_side(
    *,
    label: str,
    left: pd.DataFrame | pd.Series,
    right: pd.DataFrame | pd.Series,
    left_title: str,
    right_title: str,
    mixed_q: bool = False,
) -> list[str]:
    """Side-by-side markdown for two aligned matrices or q vectors."""
    if isinstance(left, pd.Series):
        left_tbl = _q_table(left, mixed=mixed_q)
        right_tbl = _q_table(right, mixed=mixed_q)
        diff = left.astype(float) - right.astype(float)
        diff_tbl = _q_table(diff, mixed=mixed_q)
    else:
        left_tbl = _df(left)
        right_tbl = _df(right)
        diff_tbl = _df(left.astype(float) - right.astype(float))
    return [
        f'#### `{label}`',
        '',
        f'**{left_title}**',
        '',
        left_tbl,
        '',
        f'**{right_title}**',
        '',
        right_tbl,
        '',
        '**Difference (Section 2 − Section 3):**',
        '',
        diff_tbl,
        '',
    ]


def _section2_section3_comparison(
    s2: Section2FlowMixedResult,
    s3: Section3DirectMixedResult,
) -> list[str]:
    s2_label = 'Section 2 (V/U rederived mixed)'
    s3_label = 'Section 3 (direct A/q mixed)'
    lines = [
        'Side-by-side comparison of the closing mixed-unit objects from §2 and §3.',
        'On this diagonal-Make toy the paths agree numerically (differences ≈ 0).',
        '',
    ]
    lines += _matrix_side_by_side(
        label='Atot_mixed',
        left=s2.atot,
        right=s3.atot,
        left_title=s2_label,
        right_title=s3_label,
    )
    lines += _matrix_side_by_side(
        label='q_mixed',
        left=s2.q,
        right=s3.q,
        left_title=s2_label,
        right_title=s3_label,
        mixed_q=True,
    )
    lines += _matrix_side_by_side(
        label='L_tot',
        left=s2.ef.l_tot,
        right=s3.ef.l_tot,
        left_title=s2_label,
        right_title=s3_label,
    )
    return lines


def _identity_block(ef: object, *, mixed: bool, atot: pd.DataFrame, q: pd.Series, l_tot: pd.DataFrame, udom: pd.DataFrame, uimp: pd.DataFrame, y: pd.DataFrame) -> list[str]:
    if ef.commodity_identity is None:
        return [
            'Output identities are not evaluated on this path (no flow-table round-trip in production §3).',
            '',
        ]
    u_total = udom + uimp
    y_d = y.sum(axis=1).astype(float)
    q_check_c = u_total.sum(axis=1) + y_d
    y_tot = backcompute_y_from_A_and_q(A=atot, q=q)
    q_check_l = backcompute_q_from_L_and_y(L=l_tot, y=y_tot)
    sectors = TOY_SECTORS
    frame = pd.DataFrame(index=list(sectors))
    frame['units'] = ['MWh' if mixed and s == GENERATION_SECTOR else 'USD' for s in sectors]
    frame['q'] = q.reindex(sectors)
    frame['U·1 + y_d'] = q_check_c.reindex(sectors)
    frame['q − (U·1 + y_d)'] = frame['q'] - frame['U·1 + y_d']
    lines = [
        '**Commodity identity** `q ≈ (Udom + Uimp)·1 + y_d`:',
        '',
        _df(frame),
        '',
        format_diagnostic_result(ef.commodity_identity),
        '',
        '**Leontief identity** `q ≈ L_tot @ y_nab`:',
        '',
    ]
    frame2 = pd.DataFrame(index=list(sectors))
    frame2['units'] = frame['units']
    frame2['q'] = q.reindex(sectors)
    frame2['L @ y_nab'] = q_check_l.reindex(sectors)
    frame2['q − L @ y_nab'] = frame2['q'] - frame2['L @ y_nab']
    lines += [_df(frame2), '', format_diagnostic_result(ef.leontief_identity), '']
    return lines


def _section1(s1: Section1ProductionResult) -> list[str]:
    mon = s1.monetary
    flows = rebuild_scaled_dom_imp_flows(mon, s1.scaled)
    lines = [
        '## Section 1 — Current production path (main branch)',
        '',
        'Mirrors **main** through `derive_cornerstone_Aq_scaled` → monetary `B`, `L`, `y_nab` → BLy / D / N.',
        'No mixed units; no `V`/`U` rebuild in production after inflation.',
        '',
    ]
    lines += _step('1.1 Monetary IO tables (detail year)')
    lines += _monetary_io_tables(s1)
    lines += _step('1.2 Derive `Adom`, `Aimp`, `Atot`, `q`')
    lines += _derive_aq_step()
    lines += ['**`Adom`:**', '', _df(mon.Adom), '', '**`Aimp`:**', '', _df(mon.Aimp), '', '**`Atot`:**', '', _df(mon.Atot), '', '**`q` [USD]:**', '', _q_table(mon.q, mixed=False), '']
    lines += _step('1.3 Scale + inflate to model year')
    lines += _scale_inflate_step(s1.scaled)
    lines += _step('1.4 Rebuilt scaled flows (analysis illustration only — not in production)')
    lines += [
        'Production stops at scaled `A`/`q`. For identity checks in this section we rebuild',
        'diagonal-Make flows from scaled `Atot`/`q`/`y_nab`:',
        '',
        '**`V_scaled` [USD]:**',
        '',
        _df(flows.v),
        '',
        '**`Udom_scaled` [USD]:**',
        '',
        _df(flows.udom),
        '',
        '**`Uimp_scaled` [USD]:**',
        '',
        _df(flows.uimp),
        '',
        '**`Y_scaled` [USD]:**',
        '',
        _df(flows.y),
        '',
        '**`VA_scaled` [USD]:**',
        '',
        _df(flows.va),
        '',
    ]
    lines += _step('1.5 BLy, D, N (monetary)')
    lines += _ef_step(
        s1.ef,
        mixed=False,
        section_label='§1',
        atot=s1.scaled.atot_target,
    )
    lines += _step('1.6 Output identities (scaled monetary flows)')
    lines += _identity_block(
        s1.ef,
        mixed=False,
        atot=s1.scaled.atot_target,
        q=s1.scaled.q_target,
        l_tot=s1.ef.l_tot,
        udom=flows.udom,
        uimp=flows.uimp,
        y=flows.y,
    )
    return lines


def _section2(s2: Section2FlowMixedResult) -> list[str]:
    lines = [
        '## Section 2 — Mixed units at scaled IO tables (analysis path)',
        '',
        'After scaling (§1.3), rebuild `V`/`Udom`/`Uimp`/`Y`/`VA`, apply mixed conversion',
        'to generation-sector flows, then rederive `Adom`/`Aimp`/`Atot`/`q`.',
        '**Not implemented in production.**',
        '',
    ]
    lines += _step('2.1 Scaled monetary IO (same as §1.4)')
    sf = s2.scaled_flows
    lines += [
        '**`V_scaled` [USD]:**',
        '',
        _df(sf.v),
        '',
        '**`Udom_scaled` [USD]:**',
        '',
        _df(sf.udom),
        '',
        '**`Uimp_scaled` [USD]:**',
        '',
        _df(sf.uimp),
        '',
        '**`Y_scaled` [USD]:**',
        '',
        _df(sf.y),
        '',
        '**`VA_scaled` [USD]:**',
        '',
        _df(sf.va),
        '',
    ]
    lines += _step('2.2 Conversion factors at target year')
    lines += [
        f'`c_col = {s2.c_col:.6g}` MWh/$; eGRID anchor = `{s2.mwh_221110:.6g}` MWh.',
        '',
        '**`c_j`:**',
        '',
        _series(s2.c_row),
        '',
        'Flow rules: scale `V[221110,221110]`, `Udom[221110,·]`, `Uimp[221110,·]`, `Y[221110,·]`;',
        'leave `U[·,221110]` purchases in USD.',
        '',
    ]
    lines += _step('2.3 Mixed IO tables')
    mf = s2.mixed_flows
    lines += [
        '**`V_mixed` (`221110` diagonal in MWh):**',
        '',
        _df(mf.v),
        '',
        '**`Udom_mixed` (hybrid USD/MWh):**',
        '',
        _df(mf.udom),
        '',
        '**`Uimp_mixed`:**',
        '',
        _df(mf.uimp),
        '',
        '**`Y_mixed`:**',
        '',
        _df(mf.y),
        '',
    ]
    lines += _step('2.4 Rederive `Adom`, `Aimp`, `Atot`, `q`')
    lines += [
        '**Equations:** same as §1.2 on mixed flows.',
        '',
        '**`Adom_mixed`:**',
        '',
        _df(s2.adom),
        '',
        '**`Aimp_mixed`:**',
        '',
        _df(s2.aimp),
        '',
        '**`Atot_mixed`:**',
        '',
        _df(s2.atot),
        '',
        '**`q_mixed`:**',
        '',
        _q_table(s2.q, mixed=True),
        '',
    ]
    lines += _step('2.5 BLy, D, N (mixed)')
    lines += _ef_step(
        s2.ef,
        mixed=True,
        section_label='§2',
        atot=s2.atot,
    )
    udom_bal = s2.adom.multiply(s2.q, axis=1)
    uimp_bal = s2.aimp.multiply(s2.q, axis=1)
    y_nab_tot = backcompute_y_from_A_and_q(A=s2.atot, q=s2.q)
    y_bal = pd.DataFrame(0.0, index=s2.atot.index, columns=s2.monetary.Y.columns)
    y_bal.iloc[:, 0] = y_nab_tot.reindex(y_bal.index).astype(float)
    lines += _step('2.6 Output identities (row-balanced `U`/`Y` from rederived mixed `A`/`q`)')
    lines += [
        'Raw mixed IO tables need not row-balance exactly; identities use',
        '`Udom = Adom ⊙ q`, `Uimp = Aimp ⊙ q`, `y_nab` from `Atot`/`q`.',
        '',
    ]
    lines += _identity_block(
        s2.ef,
        mixed=True,
        atot=s2.atot,
        q=s2.q,
        l_tot=s2.ef.l_tot,
        udom=udom_bal,
        uimp=uimp_bal,
        y=y_bal,
    )
    return lines


def _section3(
    s3: Section3DirectMixedResult,
    s2: Section2FlowMixedResult,
) -> list[str]:
    lines = [
        '## Section 3 — Mixed units directly on scaled `A`, `q` (PR4 / `jv_PR4`)',
        '',
        'Mirrors **`build_electricity_mixed_units_aq(derive_cornerstone_Aq_scaled())`**:',
        'apply `apply_electricity_unit_conversion_to_A/q/B` to scaled blocks —',
        'no `V`/`U` rebuild, no flow-table round-trip.',
        '',
    ]
    lines += _step('3.1 Scaled target-year `A` and `q` (from §1.3)')
    lines += [
        '**`Adom_target` [USD]:**',
        '',
        _df(s3.scaled.adom.a_target),
        '',
        '**`Aimp_target` [USD]:**',
        '',
        _df(s3.scaled.aimp.a_target),
        '',
        '**`q_target` [USD]:**',
        '',
        _q_table(s3.scaled.q_target, mixed=False),
        '',
    ]
    lines += _step('3.2 Conversion factors')
    lines += [
        f'`c_col = {s3.c_col:.6g}` MWh/$; eGRID anchor = `{s3.mwh_221110:.6g}` MWh.',
        '',
        '**`c_j`:**',
        '',
        _series(s3.c_row),
        '',
        '**Direct transform on `Adom`/`Aimp`/`q`:**',
        '',
        '- `A[gen,j] *= c_j`; `A[gen,gen] *= c_gen/c_col`; `A[i,gen] /= c_col`',
        '- `q[gen] *= c_col`',
        '- `B[·,gen] /= c_col`',
        '',
    ]
    lines += _step('3.3 Mixed `Adom`, `Aimp`, `Atot`, `q`')
    lines += [
        '**`Adom_mixed`:**',
        '',
        _df(s3.adom),
        '',
        '**`Aimp_mixed`:**',
        '',
        _df(s3.aimp),
        '',
        '**`Atot_mixed`:**',
        '',
        _df(s3.atot),
        '',
        '**`q_mixed`:**',
        '',
        _q_table(s3.q, mixed=True),
        '',
    ]
    lines += _step('3.4 BLy, D, N (mixed)')
    lines += _ef_step(
        s3.ef,
        mixed=True,
        section_label='§3',
        atot=s3.atot,
    )
    lines += _step('3.5 Comparison with §2')
    lines += _section2_section3_comparison(s2, s3)
    return lines


def build_report_markdown() -> str:
    s1 = run_section1_production()
    s2 = run_section2_flow_mixed()
    s3 = run_section3_direct_mixed()
    assert_section2_matches_section3(s2, s3)
    return '\n'.join([*_intro(), *_section1(s1), *_section2(s2), *_section3(s3, s2)])


def write_report(path: Path | None = None) -> Path:
    out = path or _REPORT_PATH
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(build_report_markdown(), encoding='utf-8')
    return out


if __name__ == '__main__':
    print(f'Wrote {write_report()}')
