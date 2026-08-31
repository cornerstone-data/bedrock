"""Condition manufacturing detail industry output on the 2022 Economic Census (#724).

The GO control pinned the Supply detail industry axis to BEA's detail gross
output — but for 2022+ that series is a best-change extrapolation that has not
seen the 2022 Economic Census, and
:mod:`~bedrock.analysis.nowcasting.ec_manufacturing_output_check` measured what
the census would change: **group totals mostly nothing** (13/19 summary groups
within ±3%), **the detail mix 5.1% value-weighted**, with 29.5% of
manufacturing GO in industries more than 5% off.  This module makes that move:
for manufacturing, 2022+ detail output carries the census 2017→2022 growth
within each summary group, on BEA's 2017 levels, with the group totals kept on
BEA.

⚠️ **This lands in the GO panel that T1 and T17 read, not in a seed.**  Both
targets are hard, so a seed-side move would be pulled straight back to BEA's
extrapolation by the balance.  Adjusting
:func:`~bedrock.transform.iot.derived_intermediate_and_value_added.detail_gross_output_panel`
moves T1's per-industry value, T17's right-hand side, the value-added
allocation weights and the GO control's column targets **together**, so the
build stays coherent by construction.  ⚠️ The one consumer outside the
chokepoint is :func:`~bedrock.transform.iot.nowcast_targets.published_gross_output`,
which reads the extracted FBA parquet — Step 5's assembly must inject
:func:`adjusted_gross_output_usd` through ``industry_output_target``'s
``gross_output`` parameter, or T1's values and the rest of the build will
disagree by exactly this adjustment.

Why this is licensed
--------------------

- **The wedge is stable where it can be checked.**  Shipments-to-GO coverage at
  2017 has median 0.988 and standard deviation 0.072 across the 232 mapped
  industries; the growth comparison this module carries assumes the wedge holds
  2017→2022, and that is the one assumption with no test — but its 2017 cross
  section is tight.
- **BEA's own history says composition is what census absorption fixes.**  The
  archive test (`ec_integration_revision`) shows BEA's EC-2017 integration
  moved 2017–2021 composition 3.2→4.8% value-weighted with totals nearly
  unchanged — the same shape and larger size than this adjustment.
- **This is what BEA will do at the next comprehensive update**, three years
  early: C1 names the Economic Census as manufacturing's benchmark source.

Construction
------------

1. **EC growth per BEA industry** from the check module's machinery: published
   ``ecnbasic`` ``RCPTOT`` at six-digit NAICS, vintage restructures bridged
   through published parent residuals, allocated onto BEA detail on fixed 2017
   GO shares.
2. **Bridged families keep BEA's within-family split**: the census cannot see
   below the restructured codes, so a bridged unit's members take the family's
   EC growth times BEA's own within-family relative movement.
3. **Screen**: an industry whose 2017 shipments-to-GO wedge falls outside
   :data:`COVERAGE_BOUNDS` keeps BEA's growth.  Measured, that is ``334111``
   alone (coverage 0.472 — the own-account and IP wedge of electronic
   computers), 0.35% of manufacturing value, and its EC and BEA growth agree
   (1.311 vs 1.302) so the exclusion is nearly a no-op.
4. **2022**: within each summary industry group, detail values are BEA 2017
   levels times EC growth, rescaled so the group total equals BEA's own —
   levels stay BEA at group and above; only the within-group mix moves.
5. **2023 and 2024 chain BEA's annual movement on top**: each industry carries
   its own published growth off the adjusted 2022, re-rescaled to the year's
   BEA group totals.  When BEA's comprehensive update absorbs EC 2022, the raw
   and adjusted panels should converge — a testable bet, recorded here.

Nothing outside manufacturing and nothing before 2022 is touched, and 2017
never is — the benchmark is observed.

The registry: this is a slot, not a one-off
--------------------------------------------

:data:`SECTOR_CONDITIONERS` is the extension point (Wes, 2026-08-30): each
entry supplies within-group growth factors for one sector's detail industries,
and :func:`apply_ec_adjustment` applies every registered entry under the same
invariants — group totals BEA, factors only move the within-group mix.
Manufacturing/EC-2022 is the first entry.  The named next candidates, with the
2023 within-group movement each could reach and the annual source that
observes it:

============  =========  ==========================================================
sector         2023 $M    source, and the shape of the work
============  =========  ==========================================================
construction   130,590    Census VIP (value put in place, by type, monthly/annual);
                          needs a VIP FBA and a type → ``233*``/``230*`` map
agriculture     32,101    USDA/ERS cash receipts by commodity, annual; the
                          ``111CA`` ten-way split, commodity → ``111*``/``112*``
utilities       23,487    EIA-923/861 revenue and AGA/EIA gas, annual; the ``22``
                          three-way split (``221100``/``221200``/``221300``)
============  =========  ==========================================================

⚠️ **Oil and gas is deliberately NOT a candidate**, despite excellent EIA
data: ``211`` is a single-industry summary group, so a within-group
conditioner has nothing to move.  EIA could only matter by overriding BEA's
group *level*, which is a different and larger decision than this module's.
Unlike the census these sources are annual, so their entries may condition
2018+ rather than 2022+ — each entry carries its own years.

Run::

    uv run python -m bedrock.transform.iot.ec_go_adjustment
"""

from __future__ import annotations

import argparse
import functools
import typing as ta

import numpy as np
import pandas as pd

from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES
from bedrock.utils.taxonomy.mappings.bea_v2017_industry__bea_v2017_summary import (
    load_bea_v2017_industry_to_bea_v2017_summary,
)

#: Years whose manufacturing detail mix is conditioned on the 2022 census.
#: 2018-2021 stay BEA: between censuses BEA's annual indicators are the best
#: available detail signal, and the census carries no annual information there.
EC_ADJUSTED_YEARS = (2022, 2023, 2024)


#: The census growth base and observation years.
BASE_YEAR, CENSUS_YEAR = 2017, 2022

#: Keep BEA's growth where the 2017 shipments-to-GO wedge is outside these
#: bounds — the growth carry assumes the wedge is stable, and an extreme wedge
#: is the one observable warning sign.  Measured: excludes ``334111`` alone.
COVERAGE_BOUNDS = (0.55, 1.45)

#: ⚠️ **Held on BEA pending Wes's explicit sign-off** — his flag, not a rule's.
#: ``334118`` (computer terminals and other peripherals) is the adjustment's
#: largest single move: the census says −49% of shipments 2017→2022 against
#: BEA's −10%, at a 0.753 coverage that passes the screen.  A collapse that
#: size may be real (peripherals hollowing out) or a wedge shift
#: (import-routing, own-brand reclassification); it is the one industry the
#: review asked to see by hand before imposing.  Remove from this set to let
#: the census move it.
PENDING_REVIEW = frozenset({'334118'})


@functools.cache
def _industry_parent() -> dict[str, str]:
    return {
        code: parents[0]
        for code, parents in load_bea_v2017_industry_to_bea_v2017_summary().items()
    }


@functools.cache
def ec_growth_factors(
    prefixes: tuple[str, ...] = ('31', '32', '33'),
    coverage_bounds: tuple[float, float] = COVERAGE_BOUNDS,
    pending_review: frozenset[str] = PENDING_REVIEW,
) -> pd.DataFrame:
    """Per BEA industry of one EC sector family: the census-conditioned growth.

    Columns: ``g_ec`` (the growth applied), ``g_bea`` (what it replaces),
    ``screened`` (kept BEA and why-relevant coverage).  Bridged-family members
    carry the family's EC growth times BEA's within-family relative movement.

    ⚠️ Imports the check module lazily: it reads the **raw** GO panel, and this
    module is called from inside the panel's adjusted arm — module-level
    imports would run the circle at import time.
    """
    from bedrock.analysis.nowcasting.ec_manufacturing_output_check import (  # noqa: PLC0415, E501
        implied_bea_growth,
        units,
    )
    from bedrock.transform.iot.derived_intermediate_and_value_added import (  # noqa: PLC0415, E501
        detail_gross_output_panel,
    )

    frame = implied_bea_growth(prefixes)[
        ['r17', 'g_ec', 'g_bea', 'coverage_2017']
    ].copy()

    # Bridged families: overwrite each member's growth with the family EC
    # growth times BEA's within-family relative movement, weighted on raw GO.
    raw = detail_gross_output_panel(ec_adjusted=False)
    go17 = raw[BASE_YEAR].astype(float)
    go22 = raw[CENSUS_YEAR].astype(float)
    table = units(prefixes)
    bridged = table[table['bridged']]
    allocation = _bridged_members_to_bea(bridged)
    for unit, members in allocation.items():
        inside = [m for m in members if m in frame.index]
        if not inside:
            continue
        family_bea = float(go22[inside].sum()) / float(go17[inside].sum())
        family_ec = float(
            table.loc[table['unit'] == unit, 'r22'].iloc[0]
            / table.loc[table['unit'] == unit, 'r17'].iloc[0]
        )
        for member in inside:
            relative = float(go22[member] / go17[member]) / family_bea
            frame.loc[member, 'g_ec'] = family_ec * relative

    low, high = coverage_bounds
    screened = (
        (frame['coverage_2017'] < low)
        | (frame['coverage_2017'] > high)
        | frame.index.isin(pending_review)
    )
    frame['screened'] = screened
    frame.loc[screened, 'g_ec'] = frame.loc[screened, 'g_bea']
    return frame


#: The per-sector conditioning registry — see the module docstring.  Each entry
#: is ``name -> (factors_fn, years)``: ``factors_fn() -> pd.DataFrame`` returns
#: a ``g_ec`` column indexed by BEA detail industry (growth from
#: :data:`BASE_YEAR` to the first year of ``years``), and the remaining years
#: chain BEA's own annual movement on top.  Entries must not overlap on
#: industries; :func:`apply_ec_adjustment` refuses if they do.
SECTOR_CONDITIONERS: dict[str, tuple[ta.Callable[[], pd.DataFrame], tuple[int, ...]]]
CROSSWALK = 'bedrock/utils/mapping/naics/NAICS_to_BEA_Crosswalk_2017.csv'

#: The committed alternative-measure table written by
#: ``bedrock.utils.mapping.write_ec_alt_measures`` — wholesale gross margin and
#: the 81 taxable-revenue-plus-tax-exempt-expenses two-part, both vintages.
ALT_MEASURES_CSV = 'bedrock/analysis/nowcasting/census_alt/ec_alt_measures.csv'

#: Wholesale: BEA detail industry -> its 4-digit NAICS margin lines.  The two
#: A-groups pool several lines; ``425000`` books commissions rather than margin
#: (``ecncomm``, no annual analogue) and carries the kind average, exactly as
#: ``nowcast_trade_margins`` does for the same code.
WHOLESALE_MARGIN_LINES: dict[str, tuple[str, ...]] = {
    '423100': ('4231',),
    '423400': ('4234',),
    '423600': ('4236',),
    '423800': ('4238',),
    '424200': ('4242',),
    '424400': ('4244',),
    '424700': ('4247',),
    '423A00': ('4232', '4233', '4235', '4237', '4239'),
    '424A00': ('4241', '4243', '4245', '4246', '4248', '4249'),
}

#: ⚠️ Held on BEA within the alt-measure families, with the measured reason.
#: ``424700`` petroleum wholesale: margin coverage 0.416 — MSBOs dominate the
#: BEA concept there and sit outside the census margin.  ``493000``
#: warehousing: coverage 0.308, mostly captive/contract space the EC does not
#: price.  ``485000`` transit: coverage 0.562 and a −23% claim that rides the
#: government-transit boundary rather than the private industry.
ALT_MEASURE_HOLDS = frozenset({'424700', '493000', '485000'})


@functools.cache
def _alt_measures() -> pd.DataFrame:
    frame = pd.read_csv(ALT_MEASURES_CSV, dtype={'naics': str})
    frame['value'] = frame['value'].astype(float)
    return frame


def _raw_panel() -> pd.DataFrame:
    from bedrock.transform.iot.derived_intermediate_and_value_added import (  # noqa: PLC0415, E501
        detail_gross_output_panel,
    )

    return detail_gross_output_panel(ec_adjusted=False)


@functools.cache
def wholesale_margin_factors() -> pd.DataFrame:
    """Wholesale 42 on the census MARGIN — BEA's own concept for trade output.

    Receipts failed the wedge test at coverage 3.9 because they carry cost of
    goods; the margin is what C1 says the benchmark used, and its 2017 wedge is
    0.74-0.95 on the merchant lines.  Same shape as :func:`ec_growth_factors`:
    ``g_ec`` per BEA industry, screened members keep ``g_bea``.
    """
    table = _alt_measures()
    margin = table[table['measure'] == 'wholesale_margin']
    by_year = {
        year: margin[margin['year'] == year].set_index('naics')['value']
        for year in (BASE_YEAR, CENSUS_YEAR)
    }
    raw = _raw_panel()
    rows = {}
    for bea, lines in WHOLESALE_MARGIN_LINES.items():
        m17 = float(by_year[BASE_YEAR].reindex(list(lines)).sum())
        m22 = float(by_year[CENSUS_YEAR].reindex(list(lines)).sum())
        go_base = float(str(raw.loc[bea, BASE_YEAR]))
        go_census = float(str(raw.loc[bea, CENSUS_YEAR]))
        rows[bea] = {
            'r17': m17,
            'g_ec': m22 / m17 if m17 else float('nan'),
            'g_bea': go_census / go_base,
            'coverage_2017': m17 / 1e3 / go_base,
        }
    frame = pd.DataFrame(rows).T.rename_axis('bea')
    kind_average = float(by_year[CENSUS_YEAR].sum() / by_year[BASE_YEAR].sum())
    frame.loc['425000'] = {
        'r17': 0.0,
        'g_ec': kind_average,
        'g_bea': float(str(raw.loc['425000', CENSUS_YEAR]))
        / float(str(raw.loc['425000', BASE_YEAR])),
        'coverage_2017': float('nan'),
    }
    frame['screened'] = frame.index.isin(ALT_MEASURE_HOLDS)
    frame.loc[frame['screened'], 'g_ec'] = frame.loc[frame['screened'], 'g_bea']
    return frame


@functools.cache
def services81_two_part_factors() -> pd.DataFrame:
    """``81`` on C1's own measure: taxable revenue plus tax-exempt expenses.

    Receipts alone put the wedge at 0.816 with a 0.338 IQR because the
    nonprofit half is measured by expenses; combined, the core industries sit
    at 0.80-0.91.  The standard screens still apply — they catch grantmaking's
    pass-through expenses (``813A00`` at 2.16) and the nonemployer-heavy
    personal services (``811400``, ``812100``, ``812900``).
    """
    table = _alt_measures()
    part = table[table['measure'] == 'services81_two_part']
    by_year = {
        year: part[part['year'] == year].set_index('naics')['value']
        for year in (BASE_YEAR, CENSUS_YEAR)
    }
    matched = sorted(set(by_year[BASE_YEAR].index) & set(by_year[CENSUS_YEAR].index))
    crosswalk = pd.read_csv(CROSSWALK, dtype=str)
    six = (
        crosswalk[crosswalk['NAICS_2017_Code'].str.len() == 6][
            ['NAICS_2017_Code', 'BEA_2017_Detail_Code']
        ]
        .dropna()
        .drop_duplicates()
    )
    mapping = dict(zip(six['NAICS_2017_Code'], six['BEA_2017_Detail_Code']))
    tab = pd.DataFrame(
        {
            'v17': by_year[BASE_YEAR][matched],
            'v22': by_year[CENSUS_YEAR][matched],
        }
    )
    tab['bea'] = pd.Series(tab.index, index=tab.index).map(mapping)
    by_bea = tab.dropna(subset=['bea']).groupby('bea')[['v17', 'v22']].sum()

    raw = _raw_panel()
    frame = by_bea[by_bea.index.isin(raw.index)].copy()
    frame['r17'] = frame['v17']
    frame['g_ec'] = frame['v22'] / frame['v17']
    frame['g_bea'] = raw.loc[frame.index, CENSUS_YEAR] / raw.loc[frame.index, BASE_YEAR]
    frame['coverage_2017'] = frame['v17'] / 1e3 / raw.loc[frame.index, BASE_YEAR]
    low, high = COVERAGE_BOUNDS
    frame['screened'] = (frame['coverage_2017'] < low) | (frame['coverage_2017'] > high)
    frame.loc[frame['screened'], 'g_ec'] = frame.loc[frame['screened'], 'g_bea']
    return frame[['r17', 'g_ec', 'g_bea', 'coverage_2017', 'screened']]


#: ⚠️ Wave 1 beyond manufacturing (Wes + C1, 2026-08-30): the families where
#: ``RCPTOT`` is already the output-shaped EC variable and C1's *annual* column
#: is SAS/QSS -- so BEA's 2022+ detail is survey-carried and the census
#: absorption gap is real, exactly as for manufacturing.  Families where C1
#: names a different EC variable (trade margins, construction work value, the
#: 81 taxable/tax-exempt split) or a different census entirely (Census of
#: Governments) are follow-ups in ``About_ec_sector_extension.md``, not
#: entries here.
SECTOR_CONDITIONERS = {
    'manufacturing': (ec_growth_factors, EC_ADJUSTED_YEARS),
    'health': (
        functools.partial(ec_growth_factors, ('62',), COVERAGE_BOUNDS, frozenset()),
        EC_ADJUSTED_YEARS,
    ),
    'admin_waste': (
        functools.partial(ec_growth_factors, ('56',), COVERAGE_BOUNDS, frozenset()),
        EC_ADJUSTED_YEARS,
    ),
    'accommodation_food': (
        functools.partial(ec_growth_factors, ('72',), COVERAGE_BOUNDS, frozenset()),
        EC_ADJUSTED_YEARS,
    ),
    'professional': (
        functools.partial(ec_growth_factors, ('54',), COVERAGE_BOUNDS, frozenset()),
        EC_ADJUSTED_YEARS,
    ),
    'arts': (
        functools.partial(ec_growth_factors, ('71',), COVERAGE_BOUNDS, frozenset()),
        EC_ADJUSTED_YEARS,
    ),
    # -- the C1 alternative-measure entries (see write_ec_alt_measures) --
    'wholesale': (wholesale_margin_factors, EC_ADJUSTED_YEARS),
    'other_services': (services81_two_part_factors, EC_ADJUSTED_YEARS),
    'transport': (
        functools.partial(
            ec_growth_factors, ('48', '49'), COVERAGE_BOUNDS, ALT_MEASURE_HOLDS
        ),
        EC_ADJUSTED_YEARS,
    ),
}


def _bridged_members_to_bea(bridged: pd.DataFrame) -> dict[str, list[str]]:
    """Unit name -> the BEA industries its 2017 NAICS members feed."""
    from bedrock.analysis.nowcasting.ec_manufacturing_output_check import (  # noqa: PLC0415, E501
        _allocation,
    )

    allocation = _allocation()
    out: dict[str, list[str]] = {}
    for _, row in bridged.iterrows():
        codes = list(ta.cast('tuple[str, ...]', row['members17']))
        members = allocation[allocation['naics'].isin(codes)]
        out[str(row['unit'])] = sorted(set(members['bea']))
    return out


def apply_ec_adjustment(raw: pd.DataFrame) -> pd.DataFrame:
    """The GO panel with manufacturing 2022+ conditioned on the census.

    ``raw`` is the unadjusted panel (industries x years, million USD).  Returns
    a new frame; the argument is not mutated.  Group totals are preserved to
    float precision in every adjusted year, and every non-manufacturing row and
    every year before 2022 is returned bit-identical.
    """
    adjusted = raw.copy()
    parents = pd.Series(
        {code: _industry_parent()[code] for code in raw.index}, name='group'
    )

    claimed: dict[str, str] = {}
    for name, (factors_fn, years) in SECTOR_CONDITIONERS.items():
        factors = factors_fn()
        members = [m for m in factors.index if m in raw.index]
        for member in members:
            if member in claimed:
                raise ValueError(
                    f'{member} is conditioned by both {claimed[member]!r} and '
                    f'{name!r}; the registry entries must partition the axis.'
                )
            claimed[member] = name
        groups = parents[members]
        observed, *chained_years = years

        # observation year: BEA base levels x the entry's growth, rescaled to
        # BEA's own group totals.
        base = raw[BASE_YEAR].astype(float)
        implied = base[members] * factors.loc[members, 'g_ec']
        anchor = _rescale_within_groups(
            implied, raw[observed].astype(float)[members], groups
        )
        if observed in adjusted.columns:
            adjusted.loc[members, observed] = anchor

        # later years: chain BEA's own annual movement onto the adjusted
        # observation year, then rescale to that year's BEA group totals.
        for year in chained_years:
            if year not in adjusted.columns:
                continue
            bea_growth = (
                raw[year].astype(float)[members]
                / raw[observed].astype(float)[members].replace(0, np.nan)
            ).fillna(1.0)
            adjusted.loc[members, year] = _rescale_within_groups(
                anchor * bea_growth, raw[year].astype(float)[members], groups
            )
    return adjusted


def _rescale_within_groups(
    values: pd.Series, reference: pd.Series, groups: pd.Series
) -> pd.Series:
    """Scale *values* so each group's total equals *reference*'s."""
    got = values.groupby(groups).transform('sum')
    want = reference.groupby(groups).transform('sum')
    return values * (want / got.replace(0, np.nan)).fillna(1.0)


def adjusted_gross_output_usd(year: int) -> pd.Series:
    """The adjusted GO vector for one year, USD, on the full industry axis.

    ⚠️ **This is what Step 5's target assembly must inject** through
    :func:`~bedrock.transform.iot.nowcast_targets.industry_output_target`'s
    ``gross_output`` parameter for 2022+.  ``published_gross_output`` reads the
    extracted FBA parquet, which does not carry this adjustment; using it for
    T1 while the rest of the build reads the adjusted panel would open a gap of
    exactly this adjustment.
    """
    from bedrock.transform.iot.derived_intermediate_and_value_added import (  # noqa: PLC0415, E501
        detail_gross_output_panel,
    )
    from bedrock.utils.economic.units import (  # noqa: PLC0415
        MILLION_CURRENCY_TO_CURRENCY,
    )

    panel = detail_gross_output_panel()
    return (
        panel[int(year)].reindex(list(USA_2017_INDUSTRY_CODES)).astype(float)
        * MILLION_CURRENCY_TO_CURRENCY
    )


def report() -> pd.DataFrame:
    """What the adjustment moved, per adjusted year."""
    from bedrock.transform.iot.derived_intermediate_and_value_added import (  # noqa: PLC0415, E501
        detail_gross_output_panel,
    )

    raw = detail_gross_output_panel(ec_adjusted=False)
    adjusted = detail_gross_output_panel()
    rows = []
    for name, (factors_fn, years) in SECTOR_CONDITIONERS.items():
        factors = factors_fn()
        members = [m for m in factors.index if m in raw.index]
        year = years[0]
        delta = adjusted.loc[members, year] - raw.loc[members, year]
        moved = float(delta.abs().sum()) / 2
        rows.append(
            {
                'entry': name,
                'year': year,
                'sector_total_$M': float(raw.loc[members, year].sum()),
                'moved_$M_half_gross': moved,
                'industries_over_2pct': int(
                    (
                        (delta.abs() / raw.loc[members, year].replace(0, np.nan)) > 0.02
                    ).sum()
                ),
                'max_move_pct': float(
                    (100 * delta / raw.loc[members, year].replace(0, np.nan))
                    .abs()
                    .max()
                ),
                'screened': int(factors['screened'].sum()),
            }
        )
    return pd.DataFrame(rows).set_index('entry')


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    pd.set_option('display.width', 220)
    print('\nEC-2022 conditioning of manufacturing detail GO (#724)')
    print(report().round(2).to_string())
    factors = ec_growth_factors()
    moved = (factors['g_ec'] / factors['g_bea'] - 1).abs()
    print('\nlargest growth replacements (|g_ec/g_bea - 1|):')
    print(
        factors.loc[moved.sort_values(ascending=False).head(12).index]
        .round(3)
        .to_string()
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
