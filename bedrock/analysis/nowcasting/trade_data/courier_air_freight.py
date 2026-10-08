"""Courier trade is air freight, and the published summary row says so (#701).

#701 listed ``492000`` couriers and messengers as ``MISS`` on **both** halves of
the trade extract - neither its 44 million of imports nor its 9,411 million of
exports - and called it "a mapping gap, not an estimate", worth 8,008 million of
intermediate exposure. That is closed. This module is the measurement that
closes it, and the standing regression guard against it reopening.

BEA told us what the booking is
-------------------------------

From the distributive-services correspondence (2026-08-31, recorded in
``bea_correspondence.md``):

    Also, the imports/exports of couriers is actually related to air freight
    transportation. Our international trade data estimates more exports of air
    transportation than Census provides as revenue so we have to convert a
    portion of air couriers as air freight in order to meet our export
    controls. Imports also come from the international directorate, but those
    data are treated as non-margin like all other courier revenue.

Both IEA crosswalks now map ``TransportAirFreight`` 1:m onto ``481000`` and
``492000``, and #771's anchor-and-move makes each row its published 2017 value
times a growth blend of the categories that feed it. So couriers is present on
both sides and exact at 2017 by construction: exports 9,411, imports 44.

⚠️ **2017 is not evidence.** The anchor makes every service row exact at 2017
whatever the mover is. The claim that needs grading is the **mover** - that
courier exports move with ITA air freight - and that is a claim about
2018-2024.

There is an annual answer key for this row
------------------------------------------

``492000`` rolls up to summary ``487OS`` together with ``48A000`` and nothing
else, and the published summary Use SUT gives ``487OS`` exports for every year.
At 2017 the two sides agree exactly - detail 9,411 + 685 = summary 10,096 - so
the summary row is a genuine annual observation of a block that is 93% couriers,
not a detail figure carried forward.

Graded there, the shipped rule lands within **2.77%** every year:

======  ==========  =========  =======
year    published   modelled   error %
======  ==========  =========  =======
2018      11,158     11,323      +1.48
2019      10,881     10,949      +0.63
2020      11,656     11,333      -2.77
2021      15,175     15,151      -0.16
2022      17,016     17,417      +2.36
2023      16,043     16,478      +2.71
2024      16,780     17,063      +1.69
======  ==========  =========  =======

**1.68% mean absolute error**, against a row that rises 66% over the span.

The mover is discriminated, not assumed
---------------------------------------

A good fit is only evidence if a wrong mover would have failed, so every other
IEA transport category was graded the same way. Air freight wins by an order of
magnitude:

=========================  ========  ============
mover for ``492000``       MAPE %    max err %
=========================  ========  ============
``TransportAirFreight``        1.68          2.77
``TransportSeaFreight``       17.82         30.65
flat (frozen 2017 level)      27.37         42.16
``Transport``                 28.78         50.53
``TransportAir``              31.18         57.42
``TransportPostal``           33.28         60.63
``TransportAirPort``          35.92         62.21
``TransportAirPass``          41.47         77.11
T007 split (pre-#771)         66.06         67.07
=========================  ========  ============

**2020-21 is what separates them.** Courier exports *rose* through the pandemic
- 10,881 to 15,175, +39% - while air passenger fell to a third of its 2017
level and every mode-level aggregate fell with it. Only air freight moved the same way, because it is the
same traffic. A mover that cannot reproduce 2020-21 is not measuring this row.

The last line is the state #701 was written against: attributing the air-freight
category across ``481000``/``492000`` by same-year ``T007`` put 34.9% on
couriers where BEA puts 69.8%, and no growth rule repairs a level that is half
the answer. Anchoring on the published row is what fixed it.

Imports move the wrong way, and it does not matter
--------------------------------------------------

⚠️ On the import side the shipped mover is **worse than freezing the level**.
Published ``487OS`` ``MCIF`` runs 44, 44, 40, 43, 41, 46, 35, 30 - flat, then
falling - while ITA air freight imports rise 74% by 2021. Air freight scores
43.8% MAPE against a flat carry's 13.8%.

❌ **Not acted on, deliberately.** The largest error the mover makes is **35
million dollars**, in 2021, on a commodity carrying 93,464 million of
intermediate use - 0.04% of its own row. Carving one commodity's import mover
out of a uniform bridge architecture to chase that would be fitting the summary
answer key on a rounding error, and it would cost the property that makes the
bridge defensible: every service row is built the same way. The finding is
recorded so nobody re-derives it, not so it gets shipped.

BEA's note explains the asymmetry: courier exports are a *conversion* out of air
freight made to meet an export control, so they scale with the control; courier
imports "come from the international directorate" as their own small series and
never had an air-freight link to inherit.

What this cost the interior
---------------------------

``row_control_exposure``, the diagnostic #701 was raised from, with the exports
half run live rather than off its pinned CSV:

================================  ===========  ===========
quantity                          #701 ($M)    now ($M)
================================  ===========  ===========
gross error, sum \\|dT001\\|         1,164,084      575,013
landing in the intermediate block     488,408      170,835
  ... from ``MCIF``                   454,227      122,807
  ... from ``F04000``                 313,910      160,699
================================  ===========  ===========

3.3% of intermediate use down to **1.1%**. Couriers, the aircraft cluster,
``52A000`` and the ``S00300`` extras are all out of the top 25.

⚠️ **The default run of that diagnostic still reports the old numbers.** Its
exports candidate is a pinned CSV last written 2026-08-14, which predates #762,
#764, #766 and #771; read without ``--live`` it reproduces #701's F04000 column
to the dollar and looks like nothing has changed.

Run::

    uv run python -m bedrock.analysis.nowcasting.trade_data.courier_air_freight
    uv run python -m bedrock.analysis.nowcasting.trade_data.courier_air_freight --check
    uv run python -m bedrock.analysis.nowcasting.trade_data.courier_air_freight --built
"""

from __future__ import annotations

import argparse
import functools
import sys

import pandas as pd

from bedrock.extract.iot.io_2017 import (
    GCS_USA_SUP_DIR,
    LOCAL_USA_SUP_DIR,
    _load_2017_detail_supply_use_usa,
)
from bedrock.utils.io.gcp import load_from_gcs

#: The years the IEA extract covers, which is the span the bridge moves over.
YEARS: tuple[int, ...] = (2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024)

#: The anchor year every service row is pinned to (#771).
ANCHOR = 2017

#: The two detail commodities that make up summary ``487OS``, and nothing else -
#: which is what makes the summary row an observation of this block.
COURIER = '492000'
SIGHTSEEING = '48A000'
SUMMARY_ROW = '487OS'

#: The air transportation sibling ``TransportAirFreight`` is split with.
AIR = '481000'
SUMMARY_AIR = '481'

#: The mover the crosswalk gives couriers, and the alternatives it is graded
#: against. ``TransportSeaFreight`` is in the list as a control: it is freight,
#: so it shares the trade cycle, but it is the wrong mode.
SHIPPED_MOVER = 'TransportAirFreight'
CANDIDATE_MOVERS: tuple[str, ...] = (
    'TransportAirFreight',
    'TransportSeaFreight',
    'Transport',
    'TransportAir',
    'TransportPostal',
    'TransportAirPort',
    'TransportAirPass',
)

#: ``48A000``'s own blend from ``iea_export_bridge.csv``, held fixed while the
#: courier mover varies - only one row is in question.
SIGHTSEEING_BLEND: dict[str, float] = {
    'TravelBusinessOth': 0.210384,
    'TravelPersonalOth': 0.789616,
}

#: The share of ``TransportAirFreight`` exports that ``T007`` weights put on
#: couriers in 2017, which is the pre-#771 construction #701 measured.
T007_COURIER_SHARE = 0.34878

#: The published summary Use / Supply workbooks, one vintage across the span so
#: a revision seam cannot masquerade as a movement.
SUMMARY_USE = 'Use_Tables_Supply-Use_Framework_1997-2024_Summary.xlsx'
SUMMARY_SUPPLY = 'Supply_Tables_1997-2024_Summary.xlsx'

IEA_CSV = 'bedrock/extract/input_data/BEA_IEA/{year}/BEA_IEA_{year}_{direction}.csv'


@functools.cache
def iea_totals(direction: str) -> pd.DataFrame:
    """National IEA service totals, year x category, in millions of dollars."""
    columns = {}
    for year in YEARS:
        frame = pd.read_csv(IEA_CSV.format(year=year, direction=direction))
        frame = frame[
            (frame['TradeDirection'] == direction)
            & (frame['Affiliation'] == 'AllAffiliations')
            & (frame['AreaOrCountry'] == 'AllCountries')
        ]
        columns[year] = frame.set_index('TypeOfService')['DataValue'].astype(float)
    return pd.DataFrame(columns).T.rename_axis(index='year', columns='category')


def growth(direction: str) -> pd.DataFrame:
    """IEA category totals as an index on the anchor year."""
    totals = iea_totals(direction)
    return totals / totals.loc[ANCHOR]


@functools.cache
def _summary_sheet(workbook: str, year: int) -> pd.DataFrame:
    """One year's sheet of a published summary SUT workbook, by row code."""
    table = load_from_gcs(
        name=workbook,
        sub_bucket=GCS_USA_SUP_DIR,
        local_dir=LOCAL_USA_SUP_DIR,
        loader=lambda pth: pd.read_excel(
            pth, sheet_name=str(year), skiprows=5, dtype={'Unnamed: 0': str}
        ),
    )
    table = table.set_index(table.columns[0])
    table.index = table.index.astype(str).str.strip()
    table.columns = table.columns.astype(str).str.strip()
    return table


def published_summary(workbook: str, column: str, code: str) -> pd.Series:
    """A published summary cell as a year series, in millions of dollars."""
    return pd.Series(
        {
            year: float(
                pd.to_numeric(_summary_sheet(workbook, year)[column], errors='coerce')[
                    code
                ]
            )
            for year in YEARS
        },
        name=f'{code} {column}',
    ).rename_axis('year')


def published_detail(column: str, table: str) -> pd.Series:
    """The published 2017 detail column the bridge anchors on, millions."""
    detail = _load_2017_detail_supply_use_usa(table)  # type: ignore[arg-type]
    detail.columns = detail.columns.str.strip()
    return pd.to_numeric(detail[column], errors='coerce').fillna(0.0)


def anchors() -> dict[str, float]:
    """The 2017 published exports each half of ``487OS`` is anchored on."""
    exports = published_detail('F04000', 'Use_SUT_detail')
    return {
        COURIER: float(exports[COURIER]),
        SIGHTSEEING: float(exports[SIGHTSEEING]),
    }


def _cell(frame: pd.DataFrame, row: int | str, column: str) -> float:
    """One cell of a frame as a float; mypy cannot narrow ``.loc[a, b]``."""
    return float(frame[column].loc[row])


def _blend(index: pd.DataFrame, weights: dict[str, float]) -> pd.Series:
    """A share-weighted growth index, the same combination #771's bridge takes."""
    blended = pd.Series(0.0, index=index.index)
    for category, share in weights.items():
        blended = blended + share * index[category]
    return blended / sum(weights.values())


def modelled_exports(mover: str = SHIPPED_MOVER) -> pd.DataFrame:
    """``487OS`` exports under a candidate courier mover, against published."""
    index = growth('Exports')
    anchor = anchors()
    courier = anchor[COURIER] * index[mover]
    sightseeing = anchor[SIGHTSEEING] * _blend(index, SIGHTSEEING_BLEND)
    published = published_summary(SUMMARY_USE, 'F040', SUMMARY_ROW)
    frame = pd.DataFrame(
        {
            'published': published,
            'modelled': courier + sightseeing,
            COURIER: courier,
            SIGHTSEEING: sightseeing,
        }
    )
    frame['error'] = frame['modelled'] - frame['published']
    frame['error_%'] = 100 * frame['error'] / frame['published']
    return frame


def mover_scores() -> pd.DataFrame:
    """Every candidate mover graded on 2018-2024, best first.

    The two non-category arms are the constructions this row has actually had:
    a frozen level is what a row with no mover does, and the ``T007`` split is
    the pre-#771 attribution #701 measured at half the published level.
    """
    index = growth('Exports')
    anchor = anchors()
    published = published_summary(SUMMARY_USE, 'F040', SUMMARY_ROW)
    sightseeing = anchor[SIGHTSEEING] * _blend(index, SIGHTSEEING_BLEND)

    arms = {mover: anchor[COURIER] * index[mover] for mover in CANDIDATE_MOVERS}
    arms['flat (frozen 2017 level)'] = pd.Series(anchor[COURIER], index=index.index)
    courier_only = {'T007 split (pre-#771)'}
    arms['T007 split (pre-#771)'] = (
        anchor[COURIER] * T007_COURIER_SHARE * index[SHIPPED_MOVER]
    )

    rows = []
    for name, courier in arms.items():
        # The pre-#771 arm is scored as it was built: 48A000 had no mapped IEA
        # type then and produced nothing, so crediting it here would flatter it.
        modelled = courier if name in courier_only else courier + sightseeing
        error = 100 * (modelled / published - 1.0)
        graded = error.loc[error.index > ANCHOR]
        rows.append(
            {
                'mover': name,
                'MAPE_%': float(graded.abs().mean()),
                'max_err_%': float(graded.abs().max()),
                'worst_year': int(graded.abs().idxmax()),
            }
        )
    return pd.DataFrame(rows).set_index('mover').sort_values('MAPE_%')


def modelled_imports() -> pd.DataFrame:
    """``487OS`` imports under the shipped mover and under a frozen level.

    ``48A000`` has no published 2017 ``MCIF`` and no mapped import category, so
    the whole summary row is couriers on this side.
    """
    index = growth('Imports')
    anchor = float(published_detail('MCIF', 'Supply_detail')[COURIER])
    published = published_summary(SUMMARY_SUPPLY, 'MCIF', SUMMARY_ROW)
    frame = pd.DataFrame(
        {
            'published': published,
            'air_freight_mover': anchor * index[SHIPPED_MOVER],
            'flat': pd.Series(anchor, index=index.index),
        }
    )
    for arm in ('air_freight_mover', 'flat'):
        frame[f'{arm}_err_%'] = 100 * (frame[arm] / frame['published'] - 1.0)
    return frame


def air_sibling() -> pd.DataFrame:
    """``481000``'s own blend, so the split is graded on both halves.

    ⚠️ The two rows do not sum to a shared control: ``481000`` also carries all
    of air passenger, and ``TransportAirPort`` has no export-side Detail home at
    all. A courier error is not automatically an air-transportation error of the
    opposite sign.
    """
    index = growth('Exports')
    bridge = pd.read_csv(
        'bedrock/analysis/nowcasting/trade_data/iea_export_bridge.csv',
        dtype={'commodity': str},
    )
    weights = bridge.loc[bridge['commodity'] == AIR]
    blend = _blend(index, dict(zip(weights['category'], weights['share'], strict=True)))
    anchor = float(published_detail('F04000', 'Use_SUT_detail')[AIR])
    frame = pd.DataFrame(
        {
            'published': published_summary(SUMMARY_USE, 'F040', SUMMARY_AIR),
            'modelled': anchor * blend,
        }
    )
    frame['error_%'] = 100 * (frame['modelled'] / frame['published'] - 1.0)
    return frame


def built_trade() -> pd.DataFrame:
    """What the shipped Trade FBS parquets actually carry for the two rows.

    ⚠️ **A cache-staleness check, not a source.** Everything else here is
    recomputed from the bridge and the IEA extract, so it cannot be fooled by an
    FBS parquet keyed on a stale git hash. This reads the parquets to say
    whether the pipeline agrees with the arithmetic.
    """
    import glob  # noqa: PLC0415

    rows = []
    for direction in ('Exports', 'Imports'):
        for year in YEARS:
            paths = [
                path
                for path in glob.glob(
                    f'bedrock/transform/output_data/Trade_{direction}_{year}_*.parquet'
                )
                if 'metadata' not in path
            ]
            if not paths:
                continue
            frame = pd.read_parquet(sorted(paths)[-1])
            totals = frame.groupby(frame['SectorProducedBy'].astype(str))['FlowAmount']
            summed = totals.sum() / 1e6
            rows.append(
                {
                    'direction': direction,
                    'year': year,
                    COURIER: float(summed.get(COURIER, 0.0)),
                    SIGHTSEEING: float(summed.get(SIGHTSEEING, 0.0)),
                    AIR: float(summed.get(AIR, 0.0)),
                }
            )
    return pd.DataFrame(rows).set_index(['direction', 'year'])


def check() -> int:
    """Assert every figure the module docstring quotes."""
    failures: list[str] = []

    def expect(label: str, ok: bool, detail: str) -> None:
        print(f'  {"PASS" if ok else "FAIL"}  {label}  ({detail})')
        if not ok:
            failures.append(label)

    print('COURIERS IS ON BOTH SIDES, AND EXACT AT THE ANCHOR')
    exports = published_detail('F04000', 'Use_SUT_detail')
    imports = published_detail('MCIF', 'Supply_detail')
    expect(
        'published 2017 courier exports are 9,411',
        round(float(exports[COURIER])) == 9411,
        f'{float(exports[COURIER]):,.0f}',
    )
    expect(
        'published 2017 courier imports are 44',
        round(float(imports[COURIER])) == 44,
        f'{float(imports[COURIER]):,.0f}',
    )
    freight_2017 = _cell(iea_totals('Exports'), ANCHOR, SHIPPED_MOVER)
    share = float(exports[COURIER]) / freight_2017
    expect(
        'BEA puts 69.8% of the ITA air-freight leaf on couriers',
        abs(share - 0.698) < 0.005,
        f'{share:.1%} of {freight_2017:,.0f}',
    )
    expect(
        'the pre-#771 T007 split put half that there',
        abs(T007_COURIER_SHARE / share - 0.5) < 0.01,
        f'{T007_COURIER_SHARE:.1%}, {T007_COURIER_SHARE / share:.2f}x',
    )

    print()
    print('THE SUMMARY ROW IS AN OBSERVATION OF THIS BLOCK')
    anchor = anchors()
    summary_2017 = float(published_summary(SUMMARY_USE, 'F040', SUMMARY_ROW)[ANCHOR])
    expect(
        f'{SUMMARY_ROW} is {COURIER} + {SIGHTSEEING} and nothing else',
        abs(anchor[COURIER] + anchor[SIGHTSEEING] - summary_2017) < 0.5,
        f'{anchor[COURIER]:,.0f} + {anchor[SIGHTSEEING]:,.0f} = {summary_2017:,.0f}',
    )
    expect(
        'and it is 93% couriers',
        anchor[COURIER] / summary_2017 > 0.93,
        f'{anchor[COURIER] / summary_2017:.1%}',
    )

    print()
    print('THE SHIPPED MOVER, GRADED 2018-2024')
    table = modelled_exports()
    graded = table.loc[table.index > ANCHOR, 'error_%']
    expect(
        'mean absolute error is 1.68%',
        abs(float(graded.abs().mean()) - 1.68) < 0.05,
        f'{float(graded.abs().mean()):.2f}%',
    )
    expect(
        'and no year misses by more than 2.77%',
        float(graded.abs().max()) < 2.8,
        f'worst {float(graded.abs().max()):.2f}% in {int(graded.abs().idxmax())}',
    )
    expect(
        '2017 is exact by construction, so it is not evidence',
        abs(_cell(table, ANCHOR, 'error')) < 0.5,
        f'{_cell(table, ANCHOR, "error"):,.1f}',
    )

    print()
    print('A WRONG MOVER WOULD HAVE FAILED')
    scores = mover_scores()
    expect(
        'air freight is the best-scoring mover',
        str(scores.index[0]) == SHIPPED_MOVER,
        f'{scores.index[0]} at {float(scores.iloc[0]["MAPE_%"]):.2f}%',
    )
    runner_up = float(scores.iloc[1]['MAPE_%'])
    expect(
        'by an order of magnitude over the next arm',
        runner_up / float(scores.iloc[0]['MAPE_%']) > 9.0,
        f'{scores.index[1]} at {runner_up:.2f}%',
    )
    expect(
        'the pre-#771 T007 split is the worst arm on the board',
        str(scores.index[-1]).startswith('T007'),
        f'{scores.index[-1]} at {float(scores.iloc[-1]["MAPE_%"]):.2f}%',
    )
    # 2020-21 is the discriminating pair: couriers rose, everything else fell.
    published = published_summary(SUMMARY_USE, 'F040', SUMMARY_ROW)
    index = growth('Exports')
    expect(
        'couriers rose through 2021 while air passenger collapsed',
        published[2021] > published[2019]
        and _cell(index, 2021, 'TransportAirPass') < 0.4,
        f'{SUMMARY_ROW} {published[2019]:,.0f} -> {published[2021]:,.0f}, '
        f'air passenger {_cell(index, 2021, "TransportAirPass"):.2f}x',
    )

    print()
    print('IMPORTS MOVE THE WRONG WAY, ON DOLLARS TOO SMALL TO ACT ON')
    inbound = modelled_imports()
    inbound_graded = inbound.loc[inbound.index > ANCHOR]
    mover_mape = float(inbound_graded['air_freight_mover_err_%'].abs().mean())
    flat_mape = float(inbound_graded['flat_err_%'].abs().mean())
    expect(
        'a frozen level beats the air-freight mover on imports',
        flat_mape < mover_mape,
        f'flat {flat_mape:.1f}% against {mover_mape:.1f}%',
    )
    worst = float((inbound['air_freight_mover'] - inbound['published']).abs().max())
    own_use = _cell(
        _load_2017_detail_supply_use_usa('Use_SUT_detail'),
        COURIER,
        'T001',
    )
    expect(
        'the largest import error is 35 million dollars',
        worst < 36.0,
        f'{worst:,.1f} on a row of {own_use:,.0f} ({worst / own_use:.6f})',
    )

    print()
    print('THE SIBLING THE SPLIT COMES OUT OF IS NOT DAMAGED')
    sibling = air_sibling()
    sibling_graded = sibling.loc[sibling.index > ANCHOR, 'error_%']
    expect(
        f'{AIR} tracks its own summary row within 2.4% mean',
        float(sibling_graded.abs().mean()) < 2.5,
        f'{float(sibling_graded.abs().mean()):.2f}%',
    )

    print()
    if failures:
        print(f'FAILED: {len(failures)}')
        return 1
    print('OK   couriers is air freight, and every figure reproduces')
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true', help='reproduce the docstring')
    parser.add_argument(
        '--built', action='store_true', help='what the Trade FBS parquets carry'
    )
    args = parser.parse_args()
    if args.check:
        return check()

    print()
    print(f'{SUMMARY_ROW} exports: published against the shipped air-freight mover')
    print()
    print(modelled_exports().round(1).to_string())

    print()
    print('Every candidate mover for the courier row, graded 2018-2024')
    print()
    print(mover_scores().round(2).to_string())

    print()
    print(f'{SUMMARY_ROW} imports: the mover against a frozen level')
    print()
    print(modelled_imports().round(2).to_string())

    print()
    print(f'{AIR}, the sibling the air-freight category is split with')
    print()
    print(air_sibling().round(1).to_string())

    if args.built:
        print()
        print('What the shipped Trade FBS parquets carry (millions)')
        print()
        print(built_trade().round(1).to_string())
    print()
    return 0


if __name__ == '__main__':
    sys.exit(main())
