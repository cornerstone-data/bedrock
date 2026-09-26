"""Chain manufacturing detail gross output on AIES receipts for 2023-24 (#1013).

⚠️ **What this fixes.** :func:`~bedrock.transform.iot.ec_go_adjustment.
apply_ec_adjustment` conditions manufacturing on the 2022 Economic Census and
then carries 2023 and 2024 forward on **BEA's own annual movement**.  That was
the only option when the module was written -- the Annual Survey of Manufactures
had been retired and its replacement was not yet published.  It is now, and BEA's
movement is measurably wrong.

Each industry measured against **its own 2017 ratio to Census receipts**, which
cancels the standing definitional wedge between census receipts and BEA gross
output:

=====  ===============  ================  ==================
year   median |drift|   industries >10%   sum |deviation|
=====  ===============  ================  ==================
2022             2.3%           4 of 138             $82bn
2023             6.1%          40 of 138            $271bn
2024             7.9%          59 of 138            $387bn
=====  ===============  ================  ==================

✅ **2022 is tight and 2023 breaks**, which is exactly where Census replaced the
Annual Survey of Manufactures with AIES and the first year off the Economic
Census benchmark.

⚠️ **It is a mix error, not a level error.** The aggregate ratio to census is
stable in every year (1.046 / 1.040 / 1.035 / 1.042), so $387bn of gross
misallocation nets to only -$35bn and nothing at the manufacturing total shows
it.  That is why this module preserves the manufacturing total and moves only the
distribution across industries.

Why census and not BEA
----------------------

Three industries have independent evidence, and all three favour census:

==========  ==========================  =========  =========
industry    what happened in 2024       census     BEA
==========  ==========================  =========  =========
``336411``  Boeing deliveries 528->348,     -8.8%     +8.2%
            door-plug crisis, 7-week
            machinists' strike
``331110``  hot-rolled coil ~$1,000 ->     -10.4%     -1.6%
            ~$700/ton, output flat
``324110``  crude runs +1.6%, crude         -7.5%    -11.0%
            cost -1.3%
==========  ==========================  =========  =========

⚠️ BEA has aircraft **growing 8.2%** in the worst year Boeing has had since the
MAX grounding.

⚠️ **The Boeing and steel figures are public record, not measurements from this
repository**, and are used only to say which *direction* is right where BEA and
census disagree on the sign.  The refineries case needs no outside knowledge and
is the strongest of the three.  Provenance for every number, marked source by
source, is in
``bedrock/analysis/nowcasting/About_1013_census_divergence_evidence.md``.  Refineries carries a fourth, physical check: on BEA's 2024 level
the implied margin over crude is $23.37/bbl against $31.02 in 2017 -- a 25%
compression in a year when margins had come off the 2022 spike but stayed
healthy.  On census's it is $34.14, slightly above 2017, which is what refiners
reported.

The invariant, and what it costs
--------------------------------

⚠️ **This module does NOT preserve BEA summary group totals**, and that is a
deliberate departure from :mod:`~bedrock.transform.iot.ec_go_adjustment`, whose
contract is that factors only move the within-group mix.  Measured: of the
$387bn of 2024 deviation, **$250bn is between summary groups** and only ~$137bn
within them.  Pooling to the BEA summary group leaves a 5.9% median drift with 3
of 17 groups over 10%, and pooling the whole aerospace family (``3364``) leaves
**+$37.4bn**, 1.048 -> 1.220 -- so the split-disagreement explanation does not
survive and a group-preserving conditioner would divide most of the correction
out, the same way holding the summary Supply row did in #1010.

✅ So the invariant is **the manufacturing total**, which census and BEA agree on
to within half a point in every year.  The cost is that conditioned summary
groups no longer reproduce BEA's published summary gross output for 2023-24.
That is the intended trade: BEA's published 2023-24 detail is the thing this
module says is wrong.

Run::

    uv run python -m bedrock.transform.iot.aies_go_chaining --check
    uv run python -m bedrock.transform.iot.aies_go_chaining --report
"""

from __future__ import annotations

import argparse
import functools
import glob

import numpy as np
import pandas as pd

#: The observation year the chain starts from: the most recent Economic Census,
#: where ``apply_ec_adjustment`` has already conditioned manufacturing and where
#: the ratio to census is tight (2.3% median drift).
ANCHOR_YEAR = 2022

#: The years AIES observes and BEA extrapolates.
CHAINED_YEARS = (2023, 2024)

#: Manufacturing only. AIES also covers trade and transport; whether the same
#: break is there is #1013 work item 2 and is deliberately not assumed here.
PREFIXES = ('31', '32', '33')

#: Census receipts flow names. The Economic Census and AIES spell the same
#: measure differently.
_EC_FLOW = 'RCPTOT'
_AIES_FLOW = 'RCPT_TOT_VAL'

_EC_GLOB = 'bedrock/extract/output_data/Census_EC_Expenses_{year}_*.parquet'
_AIES_GLOB = 'bedrock/extract/output_data/Census_AIES_Expenses_{year}_*.parquet'


def _f(value: object) -> float:
    """One frame or series scalar as a float.

    ``.at`` and ``Series`` indexing are typed as a wide union that includes
    ``timedelta64``, so arithmetic on them does not typecheck.
    """
    return float(np.asarray(value).item())


def _receipts_by_naics(year: int) -> pd.Series:
    """Six-digit NAICS -> total receipts, $M, for one year.

    ⚠️ The code sits on ``ActivityConsumedBy`` in both surveys, and several
    vintages of the same year sit in ``output_data``.  They agree on this flow
    -- asserted rather than assumed, because silently taking whichever file
    globs first is how a vintage trap starts.
    """
    is_ec = year <= ANCHOR_YEAR
    pattern = (_EC_GLOB if is_ec else _AIES_GLOB).format(year=year)
    flow = _EC_FLOW if is_ec else _AIES_FLOW
    paths = sorted(glob.glob(pattern))
    if not paths:
        raise FileNotFoundError(f'no census receipts parquet for {year}: {pattern}')

    seen: dict[str, pd.Series] = {}
    for path in paths:
        frame = pd.read_parquet(path)
        if 'ActivityConsumedBy' not in frame.columns:
            continue
        rows = frame[frame['FlowName'].astype(str) == flow]
        if rows.empty:
            continue
        codes = rows['ActivityConsumedBy'].astype(str)
        keep = codes.str.fullmatch(r'\d{6}') & codes.str.startswith(PREFIXES)
        series = (
            pd.Series(
                rows['FlowAmount'].astype(float).to_numpy() / 1e3,  # k$ -> $M
                index=codes.to_numpy(),
            )[keep.to_numpy()]
            .groupby(level=0)
            .sum()
        )
        if not series.empty:
            seen[path] = series

    if not seen:
        raise ValueError(f'{flow!r} not found for {year} in {paths}')
    reference = next(iter(seen.values()))
    for path, series in seen.items():
        aligned = series.reindex(reference.index)
        if not np.allclose(
            aligned.fillna(-1.0).to_numpy(), reference.fillna(-1.0).to_numpy()
        ):
            raise ValueError(
                f'census receipt vintages disagree for {year}: {path} differs '
                f'from {next(iter(seen))}. Resolve the vintage before chaining.'
            )
    return reference.sort_index()


@functools.cache
def receipts_by_bea(year: int) -> pd.Series:
    """BEA detail industry -> census receipts, $M.

    Uses the same 2017-gross-output-share allocation the census conditioning
    already uses, so a NAICS code feeding several BEA industries is split the
    way BEA splits it rather than the way the census does.
    """
    from bedrock.analysis.nowcasting.ec_manufacturing_output_check import (  # noqa: PLC0415, E501
        _allocation,
    )

    receipts = _receipts_by_naics(year)
    allocation = _allocation(PREFIXES)
    hit = allocation[allocation['naics'].isin(receipts.index)].copy()
    if hit.empty:
        raise ValueError(f'no NAICS in {year} census receipts maps to BEA detail')
    hit['value'] = hit['naics'].map(receipts).astype(float) * hit['share'].astype(float)
    return hit.groupby('bea')['value'].sum().sort_index()


def chain_factors(year: int, members: list[str]) -> pd.Series:
    """Growth from :data:`ANCHOR_YEAR` to *year* on census receipts.

    Indexed by BEA detail industry over *members*.  ``NaN`` where the census
    does not observe the industry in **both** years -- the caller keeps BEA's
    own movement there rather than inventing one.
    """
    base = receipts_by_bea(ANCHOR_YEAR)
    later = receipts_by_bea(year)
    shared = [
        m
        for m in members
        if m in base.index
        and m in later.index
        and float(base[m]) > 0.0
        and float(later[m]) > 0.0
    ]
    factors = pd.Series(np.nan, index=pd.Index(members, name='industry'), dtype=float)
    factors.loc[shared] = (
        later.reindex(shared).astype(float) / base.reindex(shared).astype(float)
    ).to_numpy()
    return factors


def apply_aies_chaining(adjusted: pd.DataFrame, raw: pd.DataFrame) -> pd.DataFrame:
    """Rewrite manufacturing 2023-24 on census movement; hold the sector total.

    ``adjusted`` is the panel after
    :func:`~bedrock.transform.iot.ec_go_adjustment.apply_ec_adjustment`; ``raw``
    is the unadjusted panel, used only for the sector total to hold and for the
    fallback movement.  Returns a new frame; neither argument is mutated.

    Industries the census does not observe keep BEA's own movement off the
    anchor year, and the final rescale is over **all** manufacturing industries
    so that the observed and unobserved move onto one consistent total.
    """
    out = adjusted.copy()
    members = [code for code in out.index.astype(str) if str(code).startswith(PREFIXES)]
    if not members:
        return out
    if ANCHOR_YEAR not in out.columns:
        raise KeyError(f'the panel has no anchor year {ANCHOR_YEAR}')

    anchor = out.loc[members, ANCHOR_YEAR].astype(float)
    raw_anchor = raw.loc[members, ANCHOR_YEAR].astype(float)

    for year in CHAINED_YEARS:
        if year not in out.columns:
            continue
        census = chain_factors(year, members)
        bea = (
            raw.loc[members, year].astype(float) / raw_anchor.replace(0.0, np.nan)
        ).fillna(1.0)
        growth = census.fillna(bea)
        implied = anchor * growth
        target = float(raw.loc[members, year].astype(float).sum())
        total = float(implied.sum())
        if not np.isfinite(total) or total <= 0.0:
            raise ValueError(f'{year} implied manufacturing total is {total}')
        out.loc[members, year] = (implied * (target / total)).to_numpy()
    return out


#: A group total this conditioner does not move is free to stay held, so the
#: release is not blanket. In $M; the panel's own dust scale.
RELEASE_DUST_USD_M = 1.0


def released_groups() -> frozenset[str]:
    """Summary groups whose published Supply row must give way, or empty.

    ⚠️ **Why a release is needed at all.**
    :func:`~bedrock.transform.iot.nowcast_supply_go_control.fit_block`
    renormalises the gross-output target to each summary group's own block
    total, so a change that moves mass *between* groups is divided out.
    Measured on 2024 with this conditioner on: manufacturing gross output moves
    **$792.1bn** in absolute terms but only **$624.5bn** reaches the commodity
    rows, and the shortfall is not spread evenly -- pass-through tracks
    inversely with an industry's share of its own group.

    ==========  =========================  ==============
    industry    share of its summary group  pass-through
    ==========  =========================  ==============
    ``324110``                      91.1%            0.07
    ``331110``                      41.6%            0.57
    ``336411``                      35.4%            0.70
    ``321100``                      30.2%            1.04
    ==========  =========================  ==============

    ⚠️ **Petroleum refineries gets 7% of its correction.** At 91.1% of group
    ``324`` it can only move against siblings worth 8.9%, so the cap is
    structural -- and it lands on the industry that motivated #1013 and matters
    most for the release year.

    ✅ **The release is not a trade-off; it is required by the framework.**
    Industry output and commodity output are two margins of one supply-use
    table.  Moving an industry's output while pinning the published summary
    commodity row asserts two incompatible totals, and the fit resolves the
    contradiction by handing the difference to the group's other industries --
    which is what the 0.07 above *is*.  There is no version of this where the
    industry side moves and the commodity side does not.

    ✅ **What is optional is only the scope.** The release covers exactly the
    groups this conditioner moves: a group whose total is unchanged has no
    contradiction to resolve, so its published cell is kept.  That is measured
    per group rather than applied blanket to manufacturing.
    """
    from bedrock.transform.iot.derived_intermediate_and_value_added import (  # noqa: PLC0415, E501
        detail_gross_output_panel,
    )
    from bedrock.transform.iot.ec_go_adjustment import (  # noqa: PLC0415
        _industry_parent,
        apply_ec_adjustment,
    )
    from bedrock.utils.config.usa_config import get_usa_config  # noqa: PLC0415

    if not get_usa_config().chain_manufacturing_on_aies:
        return frozenset()

    raw = detail_gross_output_panel(ec_adjusted=False)
    held = apply_ec_adjustment(raw)
    moved = apply_aies_chaining(held, raw)
    parents = _industry_parent()
    members = [
        code
        for code in raw.index.astype(str)
        if str(code).startswith(PREFIXES) and str(code) in parents
    ]
    groups = pd.Series([parents[m] for m in members], index=members)

    released: set[str] = set()
    for year in CHAINED_YEARS:
        if year not in raw.columns:
            continue
        delta = moved.loc[members, year].astype(float) - held.loc[members, year].astype(
            float
        )
        by_group = delta.groupby(groups).sum().abs()
        released.update(by_group[by_group > RELEASE_DUST_USD_M].index.astype(str))
    return frozenset(released)


def report() -> pd.DataFrame:
    """Per chained year: how far BEA and the chained panel sit from census."""
    from bedrock.transform.iot.derived_intermediate_and_value_added import (  # noqa: PLC0415, E501
        detail_gross_output_panel,
    )
    from bedrock.transform.iot.ec_go_adjustment import (  # noqa: PLC0415, E501
        apply_ec_adjustment,
    )

    raw = detail_gross_output_panel(ec_adjusted=False)
    # Built here rather than read off the config: the flag is off by default and
    # the point of the report is to show what turning it on would do.
    adjusted = apply_ec_adjustment(raw)
    chained = apply_aies_chaining(adjusted, raw)
    rows = []
    for year in (ANCHOR_YEAR, *CHAINED_YEARS):
        census = receipts_by_bea(year)
        base = receipts_by_bea(ANCHOR_YEAR)
        shared = [
            c
            for c in census.index
            if c in raw.index and c in base.index and float(base[c]) > 0
        ]
        anchor_ratio = adjusted.loc[shared, ANCHOR_YEAR].astype(float) / base.reindex(
            shared
        ).astype(float)
        expected = census.reindex(shared).astype(float) * anchor_ratio
        for label, panel in (
            ('bea', raw),
            ('ec_adjusted', adjusted),
            ('chained', chained),
        ):
            value = panel.loc[shared, year].astype(float)
            rows.append(
                {
                    'year': year,
                    'basis': label,
                    'industries': len(shared),
                    'sum_abs_dev_bn': float((value - expected).abs().sum()) / 1e3,
                    'net_dev_bn': float((value - expected).sum()) / 1e3,
                    'ratio_to_census': float(value.sum())
                    / float(census.reindex(shared).sum()),
                }
            )
    return pd.DataFrame(rows)


def check() -> int:
    """Assert the invariants the module claims. Returns an exit code."""
    from bedrock.transform.iot.derived_intermediate_and_value_added import (  # noqa: PLC0415, E501
        detail_gross_output_panel,
    )
    from bedrock.transform.iot.ec_go_adjustment import (  # noqa: PLC0415
        apply_ec_adjustment,
    )

    raw = detail_gross_output_panel(ec_adjusted=False)
    base = apply_ec_adjustment(raw)
    chained = apply_aies_chaining(base, raw)
    members = [c for c in raw.index.astype(str) if str(c).startswith(PREFIXES)]
    others = [c for c in raw.index.astype(str) if not str(c).startswith(PREFIXES)]
    failures: list[str] = []

    for column in raw.columns:
        if column in CHAINED_YEARS:
            continue
        if not np.allclose(
            chained[column].astype(float), base[column].astype(float), equal_nan=True
        ):
            failures.append(f'{column} moved but is not a chained year')

    for year in CHAINED_YEARS:
        if year not in raw.columns:
            continue
        got = float(chained.loc[members, year].astype(float).sum())
        want = float(raw.loc[members, year].astype(float).sum())
        if abs(got - want) > max(1.0, 1e-9 * abs(want)):
            failures.append(f'{year} manufacturing total {got:,.1f} != {want:,.1f}')
        moved = ~np.isclose(
            chained.loc[others, year].astype(float),
            base.loc[others, year].astype(float),
        )
        if moved.any():
            failures.append(f'{year} moved {int(moved.sum())} non-manufacturing rows')
        if (chained.loc[members, year].astype(float) < 0).any():
            failures.append(f'{year} produced a negative manufacturing gross output')

    frame = report()
    for year in CHAINED_YEARS:
        rows = frame[frame['year'] == year]
        if rows.empty:
            continue
        bea = float(rows[rows['basis'] == 'bea']['sum_abs_dev_bn'].iloc[0])
        if not bea > 0:
            failures.append(f'{year} has no measured BEA deviation to fix')

    for line in failures:
        print(f'FAIL {line}')
    if failures:
        return 1
    print(
        f'ok: {len(members)} manufacturing industries, chained {CHAINED_YEARS}, '
        f'sector total held, {len(others)} other rows untouched'
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true', help='assert the invariants')
    parser.add_argument('--report', action='store_true', help='deviation from census')
    args = parser.parse_args()
    if args.report:
        pd.set_option('display.width', 200)
        print(report().round(3).to_string(index=False))
    if args.check or not args.report:
        return check()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
