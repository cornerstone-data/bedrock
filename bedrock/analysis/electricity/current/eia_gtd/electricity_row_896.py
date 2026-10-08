"""Issue #896 Phase 1 — electricity Use-row diagnostic gate.

Decides whether the 2023–24 share collapse is a **target** problem
(``T016 − ΣY``) or an **allocation** problem (seeds / carry / GRAS), without
changing production MUT or the G/T/D allocator.

``interior_row_targets`` calls ``derive_initial_supply_bridge`` /
``derive_initial_Y_pur(..., download_sources_ok=True)``. ``--mut-vintage`` pins
MUT only; the target path needs live extract inputs. Missing T016 or auth
failure raises — never silently equated away. Always emit ``supply_use_gap_usd``
(2022 is a known ~$10.4bn gap).

::

    python -m bedrock.analysis.electricity.current.eia_gtd.electricity_row_896 \\
        --mode bands|gate|stages|seed_status|aies_seam|all \\
        --mut-vintage v0.3.0_4276083 --csv [--check] \\
        [--anchor-span 2022-2023]

``--years`` drives bands/gate. ``--anchor-span`` drives stages / seed_status
top-N / aies industry ranking (default ``2022-2023``). AIES **index** years stay
pinned at 2022/2023 (SAS→AIES survey break) and are not retargeted by
``--anchor-span``.
"""

from __future__ import annotations

import argparse
import logging
import typing as ta
from pathlib import Path

import pandas as pd

from bedrock.analysis.electricity.current.eia_gtd.electricity_row_control import (
    ELECTRICITY_ROW,
    OUT_DIR,
    PanelByYear,
    _activate,
    _joined,
    _parse_years,
    load_year,
)

logger = logging.getLogger(__name__)

BANDS: tuple[str, ...] = (
    'manufacturing',
    'services_transport',
    'agriculture',
    'mining',
    'utilities',
    'trade',
    'trade_unseeded',
    'held_2017',
)

CRISIS_SPANS: tuple[tuple[int, int], ...] = ((2022, 2023), (2023, 2024))
DEFAULT_MUT_VINTAGE = 'v0.3.0_4276083'
ASSEMBLY_NOTE = 'local_assemble_vs_pinned_mut'

_BAND_SUM_ATOL_BN = 0.05
_LEVEL_FLOOR_USD = 5e9
_SHARE_FLOOR = 0.0015  # 0.15pp as a fraction of all intermediate
_SHARE_PARALLEL_FLOOR = 0.0005  # 0.05pp
_SHARE_PARALLEL_RTOL = 0.30
_STALE_ROW_FRAC = 0.05
_STALE_CELL_FRAC = 0.05
_STALE_CELL_MIN_USD = 1e6
_MANUFACTURING_PREFIXES = frozenset({'31', '32', '33'})
_TRADE_SECTORS = frozenset({'42', '44RT'})


class BandShareEffectRow(ta.NamedTuple):
    band: str
    year_a: int
    year_b: int
    share_effect_bn: float
    mut_vintage: str


class GateYear(ta.NamedTuple):
    year: int
    target_usd: float
    realized_u_usd: float
    all_intermediate_usd: float
    target_share: float
    realized_share: float
    supply_use_gap_usd: float
    mut_vintage: str


class GateDecision(ta.NamedTuple):
    year_a: int
    year_b: int
    delta_target_usd: float
    delta_realized_usd: float
    delta_target_share: float
    delta_realized_share: float
    target_fraction_of_level: float | None
    share_parallel: bool
    classification: str
    classification_at_0_60: str
    classification_at_0_80: str


class StageCellMove(ta.NamedTuple):
    industry: str
    year: int
    step3_usd: float
    post_ipf_usd: float
    post_gras_usd: float
    delta_ipf_bn: float
    delta_gras_bn: float
    assembly_note: str
    mut_vintage: str


class SeedStatusRow(ta.NamedTuple):
    industry: str
    band: str
    elec_in_seed_overlay: bool
    use_cell_free: bool
    year_a: int
    year_b: int
    share_effect_bn: float


class ClaimSets(ta.NamedTuple):
    """Disjoint industry sets for the composed_seed claim map."""

    manufacturing: frozenset[str]
    services_transport: frozenset[str]
    agriculture: frozenset[str]
    mining: frozenset[str]
    utilities: frozenset[str]
    trade: frozenset[str]
    trade_unseeded: frozenset[str]


class AiesSeamRow(ta.NamedTuple):
    """SAS-2022 vs AIES-2023 electricity index (survey break — not ``--anchor-span``).

    ``year_a`` / ``year_b`` / ``share_effect_bn`` describe the ranking span used
    to pick industries (from ``--anchor-span``). Index columns stay the SAS→AIES
    break at 2022/2023 and are never swept into the anchor-span parameter.
    """

    industry: str
    index_2022: float | None
    index_2023: float | None
    index_ratio: float | None
    counterfactual_hold_2022_bn: float | None
    year_a: int
    year_b: int
    share_effect_bn: float


# ---------------------------------------------------------------------------
# Share effects
# ---------------------------------------------------------------------------


def full_share_effect_by_column(
    panel: PanelByYear, a: int, b: int
) -> 'pd.Series[float]':
    """Column share effects in $bn for **all** Use industries (no top-N trunc)."""
    j = _joined(panel, a, b)
    growth = (j['col_b'] / j['col_a'].replace(0.0, float('nan')) - 1).fillna(0.0)
    return (j['elec_b'] - j['elec_a'] - j['elec_a'] * growth) / 1e9


# ---------------------------------------------------------------------------
# Claim map
# ---------------------------------------------------------------------------


def build_claim_sets(industries: ta.Iterable[str] | None = None) -> ClaimSets:
    """Build the eight-way partition used by band CSVs and seed status."""
    from bedrock.analysis.nowcasting.agriculture_expense_seed import (  # noqa: PLC0415
        farm_industries,
    )
    from bedrock.analysis.nowcasting.inputs_structure import (  # noqa: PLC0415
        MINING_SEEDED,
        _manufacturing_bea_industries,
    )
    from bedrock.analysis.nowcasting.seed_coverage import (  # noqa: PLC0415
        _detail_to_sector,
    )
    from bedrock.analysis.nowcasting.services_transport_expense_seed import (  # noqa: PLC0415
        services_transport_industries,
    )
    from bedrock.analysis.nowcasting.trade_expense_supplement import (  # noqa: PLC0415
        RETAIL,
        WHOLESALE,
    )
    from bedrock.analysis.nowcasting.utilities_expense_seed import (  # noqa: PLC0415
        ELECTRIC,
    )

    manufacturing = frozenset(_manufacturing_bea_industries())
    services = frozenset(services_transport_industries())
    agriculture = frozenset(farm_industries())
    mining = frozenset(MINING_SEEDED)
    utilities = frozenset(ELECTRIC)
    trade = (frozenset(WHOLESALE) | frozenset(RETAIL)) - frozenset({'4200ID'})

    sector = _detail_to_sector()
    universe = (
        set(industries)
        if industries is not None
        else set(sector)
        | manufacturing
        | services
        | agriculture
        | mining
        | utilities
        | trade
    )
    trade_unseeded = frozenset(
        c
        for c in universe
        if sector.get(str(c)) in _TRADE_SECTORS and str(c) not in trade
    )

    sets = ClaimSets(
        manufacturing=manufacturing,
        services_transport=services,
        agriculture=agriculture,
        mining=mining,
        utilities=utilities,
        trade=trade,
        trade_unseeded=trade_unseeded,
    )
    _assert_claim_sets_disjoint(sets)
    return sets


def _assert_claim_sets_disjoint(sets: ClaimSets) -> None:
    named = [
        ('manufacturing', sets.manufacturing),
        ('services_transport', sets.services_transport),
        ('agriculture', sets.agriculture),
        ('mining', sets.mining),
        ('utilities', sets.utilities),
        ('trade', sets.trade),
        ('trade_unseeded', sets.trade_unseeded),
    ]
    for i, (n1, s1) in enumerate(named):
        for n2, s2 in named[i + 1 :]:
            clash = s1 & s2
            if clash:
                raise ValueError(
                    f'claim-map overlap between {n1} and {n2}: {sorted(clash)[:5]}'
                )
    if sets.trade & sets.trade_unseeded:
        raise ValueError('trade ∩ trade_unseeded must be empty')


def assign_band(industry: str, sets: ClaimSets) -> str:
    """Assign one Use industry column to exactly one band."""
    code = str(industry)
    if code in sets.trade:
        return 'trade'
    if code in sets.trade_unseeded:
        return 'trade_unseeded'
    if code in sets.agriculture:
        return 'agriculture'
    if code in sets.mining:
        return 'mining'
    if code in sets.utilities:
        return 'utilities'
    if code in sets.manufacturing or code[:2] in _MANUFACTURING_PREFIXES:
        return 'manufacturing'
    if code in sets.services_transport:
        return 'services_transport'
    return 'held_2017'


def band_membership(industries: ta.Iterable[str], sets: ClaimSets) -> dict[str, str]:
    return {str(i): assign_band(str(i), sets) for i in industries}


# ---------------------------------------------------------------------------
# Gate classification
# ---------------------------------------------------------------------------


def share_parallel(delta_target_share: float, delta_realized_share: float) -> bool:
    if abs(delta_realized_share) < _SHARE_PARALLEL_FLOOR:
        return False
    if delta_target_share * delta_realized_share <= 0:
        return False
    return abs(delta_target_share / delta_realized_share - 1.0) <= _SHARE_PARALLEL_RTOL


def classify_gate(
    *,
    delta_realized_usd: float,
    delta_realized_share: float,
    target_fraction_of_level: float | None,
    parallel: bool,
    level_cut: float = 0.70,
    ambiguous_lo: float = 0.40,
) -> str:
    """Classification rule from the #896 plan (no floating prose)."""
    if (
        abs(delta_realized_usd) < _LEVEL_FLOOR_USD
        and abs(delta_realized_share) < _SHARE_FLOOR
    ):
        return 'stable'
    if (
        abs(delta_realized_usd) < _LEVEL_FLOOR_USD
        and abs(delta_realized_share) >= _SHARE_FLOOR
    ):
        return 'target_collapse' if parallel else 'allocation'
    if (
        target_fraction_of_level is not None
        and target_fraction_of_level >= level_cut
        and parallel
    ):
        return 'target_collapse'
    if (
        target_fraction_of_level is not None
        and ambiguous_lo <= target_fraction_of_level < level_cut
        and parallel
    ):
        return 'ambiguous'
    return 'allocation'


def make_gate_decision(ya: GateYear, yb: GateYear) -> GateDecision:
    d_t = yb.target_usd - ya.target_usd
    d_r = yb.realized_u_usd - ya.realized_u_usd
    d_ts = yb.target_share - ya.target_share
    d_rs = yb.realized_share - ya.realized_share
    frac: float | None
    if abs(d_r) >= _LEVEL_FLOOR_USD:
        frac = d_t / d_r
    else:
        frac = None
    parallel = share_parallel(d_ts, d_rs)
    return GateDecision(
        year_a=ya.year,
        year_b=yb.year,
        delta_target_usd=d_t,
        delta_realized_usd=d_r,
        delta_target_share=d_ts,
        delta_realized_share=d_rs,
        target_fraction_of_level=frac,
        share_parallel=parallel,
        classification=classify_gate(
            delta_realized_usd=d_r,
            delta_realized_share=d_rs,
            target_fraction_of_level=frac,
            parallel=parallel,
            level_cut=0.70,
        ),
        classification_at_0_60=classify_gate(
            delta_realized_usd=d_r,
            delta_realized_share=d_rs,
            target_fraction_of_level=frac,
            parallel=parallel,
            level_cut=0.60,
        ),
        classification_at_0_80=classify_gate(
            delta_realized_usd=d_r,
            delta_realized_share=d_rs,
            target_fraction_of_level=frac,
            parallel=parallel,
            level_cut=0.80,
        ),
    )


# ---------------------------------------------------------------------------
# Mode builders
# ---------------------------------------------------------------------------


def _panel_industries(panel: PanelByYear) -> list[str]:
    years = sorted(panel)
    idx = panel[years[0]].elec.index.union(panel[years[-1]].elec.index)
    return [str(i) for i in idx]


def build_band_rows(
    panel: PanelByYear,
    years: list[int],
    mut_vintage: str,
    sets: ClaimSets,
) -> list[BandShareEffectRow]:
    membership = band_membership(_panel_industries(panel), sets)
    rows: list[BandShareEffectRow] = []
    for a, b in zip(years, years[1:]):
        effects = full_share_effect_by_column(panel, a, b)
        by_band = {band: 0.0 for band in BANDS}
        for industry, effect in effects.items():
            band = membership.get(str(industry), 'held_2017')
            by_band[band] = by_band.get(band, 0.0) + float(effect)
        for band in BANDS:
            rows.append(
                BandShareEffectRow(
                    band=band,
                    year_a=a,
                    year_b=b,
                    share_effect_bn=by_band[band],
                    mut_vintage=mut_vintage,
                )
            )
    return rows


def trade_diagnostic_bn(band_rows: list[BandShareEffectRow], a: int, b: int) -> float:
    """Full-sector trade share effect (not a band in the --check sum)."""
    return sum(
        r.share_effect_bn
        for r in band_rows
        if r.year_a == a and r.year_b == b and r.band in ('trade', 'trade_unseeded')
    )


def load_gate_years(
    years: list[int], mut_vintage: str, panel: PanelByYear
) -> list[GateYear]:
    from bedrock.transform.iot.nowcast_interior_fit import (  # noqa: PLC0415
        interior_row_targets,
    )

    out: list[GateYear] = []
    for year in years:
        _activate(year, mut_vintage)
        try:
            targets = interior_row_targets(year)
        except Exception as exc:  # noqa: BLE001 — surface extract/auth failures
            raise RuntimeError(
                f'interior_row_targets({year}) failed (need live Supply/FD extracts; '
                f'--mut-vintage pins MUT only): {exc}'
            ) from exc
        if ELECTRICITY_ROW not in targets.index or pd.isna(
            targets.loc[ELECTRICITY_ROW]
        ):
            raise RuntimeError(
                f'interior_row_targets({year}) has no {ELECTRICITY_ROW}; '
                'T016 / Y path is unsourced for electricity'
            )
        target = float(targets.loc[ELECTRICITY_ROW])
        p = panel[year]
        realized = float(p.elec.sum())
        all_u = float(p.all_intermediate)
        if all_u == 0.0 or pd.isna(all_u):
            raise RuntimeError(f'{year} all_intermediate is zero/NaN')
        out.append(
            GateYear(
                year=year,
                target_usd=target,
                realized_u_usd=realized,
                all_intermediate_usd=all_u,
                target_share=target / all_u,
                realized_share=realized / all_u,
                supply_use_gap_usd=target - realized,
                mut_vintage=mut_vintage,
            )
        )
    return out


def top_share_effect_industries(
    panel: PanelByYear, a: int, b: int, top: int
) -> list[tuple[str, float]]:
    effects = full_share_effect_by_column(panel, a, b)
    ordered = effects.reindex(effects.abs().sort_values(ascending=False).index)
    return [(str(i), float(v)) for i, v in ordered.head(top).items()]


def _assemble_elec_row_usd(year: int, *, fitted: bool) -> 'pd.Series[float]':
    from bedrock.transform.iot.nowcast_sut_assembly import (  # noqa: PLC0415
        assemble_use_seed,
    )
    from bedrock.utils.economic.units import (  # noqa: PLC0415
        MILLION_CURRENCY_TO_CURRENCY,
    )

    panel = assemble_use_seed(year, fitted=fitted)
    selected = panel.loc[ELECTRICITY_ROW]
    if isinstance(selected, pd.DataFrame):
        raise ValueError(f'{ELECTRICITY_ROW!r} duplicated in assemble_use_seed index')
    row = selected.astype(float) * MILLION_CURRENCY_TO_CURRENCY
    row.index = [str(i) for i in row.index]
    return row


def build_stage_rows(
    panel: PanelByYear,
    mut_vintage: str,
    top_industries: list[str],
    years: list[int],
) -> tuple[list[StageCellMove], list[str]]:
    """Vintage-safe stages: live assemble vs pinned MUT. Returns rows + warnings."""
    warnings: list[str] = []
    rows: list[StageCellMove] = []
    for year in years:
        _activate(year, mut_vintage)
        step3 = _assemble_elec_row_usd(year, fitted=False)
        post_ipf = _assemble_elec_row_usd(year, fitted=True)
        post_gras = panel[year].elec.copy()
        post_gras.index = [str(i) for i in post_gras.index]

        realized = float(post_gras.sum())
        aligned = pd.DataFrame({'ipf': post_ipf, 'gras': post_gras}).fillna(0.0)
        row_gap = float((aligned['ipf'] - aligned['gras']).abs().sum())
        if abs(realized) > 0 and row_gap > _STALE_ROW_FRAC * abs(realized):
            warnings.append(
                f'STALE_ASSEMBLY_WARNING year={year}: '
                f'sum|post_ipf-post_gras|={row_gap / 1e9:.3f}bn '
                f'({100 * row_gap / abs(realized):.1f}% of |realized_u|)'
            )

        max_cell = 0.0
        max_ind = ''
        for industry in top_industries:
            s3 = float(step3.get(industry, 0.0) or 0.0)
            ipf = float(post_ipf.get(industry, 0.0) or 0.0)
            gras = float(post_gras.get(industry, 0.0) or 0.0)
            cell_gap = abs(ipf - gras)
            if abs(gras) >= _STALE_CELL_MIN_USD:
                frac = cell_gap / abs(gras)
                if frac > _STALE_CELL_FRAC and cell_gap > max_cell:
                    max_cell = cell_gap
                    max_ind = industry
            rows.append(
                StageCellMove(
                    industry=industry,
                    year=year,
                    step3_usd=s3,
                    post_ipf_usd=ipf,
                    post_gras_usd=gras,
                    delta_ipf_bn=(ipf - s3) / 1e9,
                    delta_gras_bn=(gras - ipf) / 1e9,
                    assembly_note=ASSEMBLY_NOTE,
                    mut_vintage=mut_vintage,
                )
            )
        if max_ind:
            warnings.append(
                f'STALE_ASSEMBLY_WARNING year={year}: top-N max cell '
                f'|ipf-gras| on {max_ind} = {max_cell / 1e9:.3f}bn'
            )
    return rows, warnings


def _elec_in_overlay(band: str, industry: str, year: int) -> bool:
    if band in ('trade', 'trade_unseeded', 'held_2017'):
        return False
    if band == 'manufacturing':
        return True
    if band in ('utilities', 'mining'):
        # utilities: EIA fuel receipts; mining: materials_seed only — neither
        # overlays purchased-electricity ``221100``.
        return False
    if band == 'agriculture':
        from bedrock.analysis.nowcasting.agriculture_expense_seed import (  # noqa: PLC0415
            agriculture_seed,
        )

        seed = agriculture_seed(year)
        return ELECTRICITY_ROW in seed.index and industry in seed.columns
    if band == 'services_transport':
        from bedrock.analysis.nowcasting.services_transport_expense_seed import (  # noqa: PLC0415
            _bea_to_survey_industry,
            _panel_for,
            relative_index,
        )

        mapping = _bea_to_survey_industry()
        naics = mapping.get(industry)
        if naics is None:
            return False
        try:
            idx = relative_index(naics, year, panel=_panel_for(year))
        except Exception:  # noqa: BLE001
            return False
        return ELECTRICITY_ROW in idx.index
    return False


def build_seed_status_rows(
    sets: ClaimSets,
    top: list[tuple[str, float]],
    *,
    year_a: int,
    year_b: int,
    mask_year: int | None = None,
) -> list[SeedStatusRow]:
    from bedrock.transform.iot.nowcast_mask import build_sut_mask  # noqa: PLC0415

    year = year_b if mask_year is None else mask_year
    _activate(year, None)
    mask = build_sut_mask('use', year)
    free = mask.free
    free.index = [str(i) for i in free.index]
    free.columns = [str(c) for c in free.columns]

    rows: list[SeedStatusRow] = []
    for industry, effect in top:
        band = assign_band(industry, sets)
        cell_free = False
        if ELECTRICITY_ROW in free.index and industry in free.columns:
            cell_free = bool(free.loc[ELECTRICITY_ROW, industry])
        rows.append(
            SeedStatusRow(
                industry=industry,
                band=band,
                elec_in_seed_overlay=_elec_in_overlay(band, industry, year),
                use_cell_free=cell_free,
                year_a=year_a,
                year_b=year_b,
                share_effect_bn=effect,
            )
        )
    return rows


def build_aies_seam_rows(
    panel: PanelByYear,
    sets: ClaimSets,
    top: list[tuple[str, float]],
    *,
    year_a: int,
    year_b: int,
) -> list[AiesSeamRow]:
    """SAS→AIES electricity indexes are pinned at 2022/2023 (survey break).

    Do not retarget those index years from ``--anchor-span``. The ranking span
    (``year_a``/``year_b``) only chooses which services industries to report.
    """
    from bedrock.analysis.nowcasting.services_transport_expense_seed import (  # noqa: PLC0415
        _bea_to_survey_industry,
        _panel_for,
        relative_index,
    )

    mapping = _bea_to_survey_industry()
    use_2017 = panel[2017].elec if 2017 in panel else None
    rows: list[AiesSeamRow] = []
    for industry, effect in top:
        if assign_band(industry, sets) != 'services_transport':
            continue
        naics = mapping.get(industry)
        idx22 = idx23 = ratio = cf = None
        if naics is not None:
            try:
                # Survey break pin — not ``--anchor-span``.
                s22 = relative_index(naics, 2022, panel=_panel_for(2022))
                s23 = relative_index(naics, 2023, panel=_panel_for(2023))
                if ELECTRICITY_ROW in s22.index and ELECTRICITY_ROW in s23.index:
                    idx22 = float(s22.loc[ELECTRICITY_ROW])
                    idx23 = float(s23.loc[ELECTRICITY_ROW])
                    ratio = idx23 / idx22 if idx22 else None
                    if use_2017 is not None and industry in use_2017.index:
                        # Hold-2022 CF: Use2017 × (idx_2022 − idx_2023), $bn.
                        cf = float(use_2017[industry]) * (idx22 - idx23) / 1e9
            except Exception as exc:  # noqa: BLE001
                logger.warning('aies_seam %s: %s', industry, exc)
        rows.append(
            AiesSeamRow(
                industry=industry,
                index_2022=idx22,
                index_2023=idx23,
                index_ratio=ratio,
                counterfactual_hold_2022_bn=cf,
                year_a=year_a,
                year_b=year_b,
                share_effect_bn=effect,
            )
        )
    return rows


# ---------------------------------------------------------------------------
# --check
# ---------------------------------------------------------------------------


def run_checks(
    *,
    band_rows: list[BandShareEffectRow],
    panel: PanelByYear,
    years: list[int],
    gate_years: list[GateYear] | None,
    decisions: list[GateDecision] | None,
) -> int:
    """Arithmetic consistency only — classification outcome is never a failure."""
    failures = 0
    for a, b in zip(years, years[1:]):
        effects = full_share_effect_by_column(panel, a, b)
        full_sum = float(effects.sum())
        band_sum = sum(
            r.share_effect_bn
            for r in band_rows
            if r.year_a == a and r.year_b == b and r.band in BANDS
        )
        if abs(band_sum - full_sum) > _BAND_SUM_ATOL_BN:
            print(
                f'FAIL  band sum identity {a}->{b}: '
                f'bands={band_sum:.4f}bn full={full_sum:.4f}bn'
            )
            failures += 1

    if gate_years:
        for g in gate_years:
            for field in (
                g.target_usd,
                g.realized_u_usd,
                g.all_intermediate_usd,
                g.target_share,
                g.realized_share,
                g.supply_use_gap_usd,
            ):
                if field != field:  # NaN
                    print(f'FAIL  GateYear {g.year} has NaN')
                    failures += 1
                    break
            expected_share = g.realized_u_usd / g.all_intermediate_usd
            if abs(expected_share - g.realized_share) > 1e-12:
                print(f'FAIL  GateYear {g.year} realized_share denominator')
                failures += 1
            if abs(g.supply_use_gap_usd - (g.target_usd - g.realized_u_usd)) > 1.0:
                print(f'FAIL  GateYear {g.year} supply_use_gap arithmetic')
                failures += 1

    if decisions:
        by_year = {g.year: g for g in gate_years or []}
        for d in decisions:
            ya, yb = by_year.get(d.year_a), by_year.get(d.year_b)
            if ya is None or yb is None:
                continue
            if (
                abs(d.delta_realized_usd - (yb.realized_u_usd - ya.realized_u_usd))
                > 1.0
            ):
                print(f'FAIL  GateDecision {d.year_a}->{d.year_b} delta_realized')
                failures += 1

    print(f'check: {failures} failure(s)')
    return failures


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------


def _rows_to_frame(rows: ta.Sequence[ta.NamedTuple]) -> pd.DataFrame:
    return pd.DataFrame([r._asdict() for r in rows])


def _write_csv(name: str, rows: ta.Sequence[ta.NamedTuple], write: bool) -> Path | None:
    if not rows:
        print(f'(no rows for {name})')
        return None
    frame = _rows_to_frame(rows)
    print(frame.to_string(index=False, float_format=lambda v: f'{v:,.4f}'))
    if not write:
        return None
    path = OUT_DIR / name
    frame.to_csv(path, index=False)
    print(f'wrote {path}')
    return path


def _fmt_decision(d: GateDecision) -> str:
    frac = (
        f'{d.target_fraction_of_level:.3f}'
        if d.target_fraction_of_level is not None
        else 'None'
    )
    return (
        f'{d.year_a}->{d.year_b}: {d.classification} '
        f'(frac={frac}, share_parallel={d.share_parallel}, '
        f'at_0.60={d.classification_at_0_60}, at_0.80={d.classification_at_0_80})'
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_anchor_span(spec: str) -> tuple[int, int]:
    """Parse ``YYYY-YYYY`` for stages / seed_status / aies ranking.

    Raises ``ValueError`` if the form is wrong or ``year_a >= year_b``.
    """
    lo, sep, hi = spec.partition('-')
    if not sep or not lo or not hi:
        raise ValueError(
            f'--anchor-span must be YYYY-YYYY (got {spec!r}); default is 2022-2023'
        )
    year_a, year_b = int(lo), int(hi)
    if year_a >= year_b:
        raise ValueError(
            f'--anchor-span requires year_a < year_b (got {year_a}-{year_b})'
        )
    return year_a, year_b


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--mode',
        default='all',
        choices=('bands', 'gate', 'stages', 'seed_status', 'aies_seam', 'all'),
    )
    parser.add_argument('--years', default='2017-2024')
    parser.add_argument(
        '--anchor-span',
        default='2022-2023',
        help=(
            'YoY span for stages / seed_status top-N / aies industry ranking '
            '(default 2022-2023). AIES index years stay pinned at 2022/2023 '
            '(SAS→AIES survey break).'
        ),
    )
    parser.add_argument('--top', type=int, default=12)
    parser.add_argument('--csv', action='store_true')
    parser.add_argument('--check', action='store_true')
    parser.add_argument('--mut-vintage', default=DEFAULT_MUT_VINTAGE)
    args = parser.parse_args(argv)

    years = _parse_years(args.years)
    try:
        anchor_a, anchor_b = _parse_anchor_span(args.anchor_span)
    except ValueError as exc:
        raise SystemExit(f'error: {exc}') from exc
    mut_vintage = args.mut_vintage
    modes = (
        {'bands', 'gate', 'stages', 'seed_status', 'aies_seam'}
        if args.mode == 'all'
        else {args.mode}
    )
    need_top = bool(modes & {'stages', 'seed_status', 'aies_seam'})

    print(f'loading MUT panel {years[0]}-{years[-1]} vintage={mut_vintage}')
    panel = {year: load_year(year, mut_vintage) for year in years}
    if need_top and (anchor_a not in panel or anchor_b not in panel):
        raise SystemExit(
            f'error: --anchor-span {anchor_a}-{anchor_b} requires both years '
            f'in --years {years[0]}-{years[-1]}'
        )
    sets = build_claim_sets(_panel_industries(panel))

    band_rows: list[BandShareEffectRow] = []
    gate_years: list[GateYear] | None = None
    decisions: list[GateDecision] | None = None

    if modes & {'bands', 'gate'} or args.check:
        band_rows = build_band_rows(panel, years, mut_vintage, sets)

    if 'bands' in modes:
        print('\n=== band share effects ($bn) ===')
        _write_csv('electricity_row_896_bands.csv', band_rows, args.csv)
        for a, b in zip(years, years[1:]):
            td = trade_diagnostic_bn(band_rows, a, b)
            print(f'  trade_diagnostic_bn {a}->{b}: {td:,.3f}')

    if 'gate' in modes:
        print('\n=== gate: target vs realized ===')
        gate_years = load_gate_years(years, mut_vintage, panel)
        _write_csv('electricity_row_896_gate_years.csv', gate_years, args.csv)
        by_year = {g.year: g for g in gate_years}
        decisions = [
            make_gate_decision(by_year[a], by_year[b])
            for a, b in zip(years, years[1:])
            if a in by_year and b in by_year
        ]
        _write_csv('electricity_row_896_gate_decisions.csv', decisions, args.csv)
        print('\nclassifications:')
        for d in decisions:
            mark = ' *crisis*' if (d.year_a, d.year_b) in CRISIS_SPANS else ''
            print(f'  {_fmt_decision(d)}{mark}')

    top: list[tuple[str, float]] = []
    top_codes: list[str] = []
    if need_top:
        top = top_share_effect_industries(panel, anchor_a, anchor_b, args.top)
        top_codes = [i for i, _ in top]

    if 'stages' in modes:
        print(
            f'\n=== stages (assemble live vs pinned MUT; anchor {anchor_a}->{anchor_b}) ==='
        )
        stage_years = [y for y in (anchor_a, anchor_b) if y in panel]
        stage_rows, warnings = build_stage_rows(
            panel, mut_vintage, top_codes, stage_years
        )
        for w in warnings:
            print(w)
        _write_csv('electricity_row_896_stages.csv', stage_rows, args.csv)

    if 'seed_status' in modes:
        print(
            f'\n=== seed / mask status (top |share_effect| {anchor_a}->{anchor_b}) ==='
        )
        seed_rows = build_seed_status_rows(
            sets, top, year_a=anchor_a, year_b=anchor_b, mask_year=anchor_b
        )
        _write_csv('electricity_row_896_seed_status.csv', seed_rows, args.csv)

    if 'aies_seam' in modes:
        print(
            '\n=== AIES electricity seam (indexes pinned 2022/2023; '
            f'ranking {anchor_a}->{anchor_b}) ==='
        )
        seam_rows = build_aies_seam_rows(
            panel, sets, top, year_a=anchor_a, year_b=anchor_b
        )
        _write_csv('electricity_row_896_aies_seam.csv', seam_rows, args.csv)

    if args.check:
        if not band_rows:
            band_rows = build_band_rows(panel, years, mut_vintage, sets)
        if gate_years is None and 'gate' not in modes:
            # Denominator checks still useful when --check without gate mode.
            try:
                gate_years = load_gate_years(years, mut_vintage, panel)
                by_year = {g.year: g for g in gate_years}
                decisions = [
                    make_gate_decision(by_year[a], by_year[b])
                    for a, b in zip(years, years[1:])
                ]
            except RuntimeError as exc:
                print(f'WARN  skipping gate arithmetic checks: {exc}')
                gate_years, decisions = None, None
        raise SystemExit(
            1
            if run_checks(
                band_rows=band_rows,
                panel=panel,
                years=years,
                gate_years=gate_years,
                decisions=decisions,
            )
            else 0
        )


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING, format='%(message)s')
    main()
