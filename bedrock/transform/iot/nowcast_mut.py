"""Step 6 driver: the before-redefinitions MUT quartet from the balanced SUT.

One call per nowcast year turns the Step 5 products - the balanced Supply and
purchaser-price Use SUT parquets in ``transform/output_data`` - into the four
tables Step 7 works on: Make, producer-price Use, the import matrix, and the
hyper-detailed Margins table. The converters themselves live in
:mod:`supply_to_make`, :mod:`sut_use_to_mut_use`,
:mod:`mut_use_to_import_matrix` and the margins machinery of
:mod:`margin_rates` / :mod:`use_price_bridge`; this module only loads, wires,
gates and saves.

**The Margins table is the placement authority.** For a nowcast year nobody
publishes the per-cell producer value, so it is built here first - 2017 rate
panel on the year's balanced purchaser Use, margin mass placed per margin
family - and the Use conversion then *reads* producer values from it, exactly
as the 2017 replay reads them from the published table. The stored layout
follows the published convention: goods rows close
``PRO + Transportation + Wholesale + Retail = PUR`` cell by cell, while
margin-commodity rows carry the margin routed onto them in ``Producers'
Value`` against a direct-purchase ``Purchasers' Value``. The per-margin-
commodity columns (the seller placement, #815) sit alongside the five
published columns.

⚠️ **2017-anchored, tax-inclusive.** The rates and placement shares are the
2017 benchmark's (:func:`margin_rates.build_rate_panel`), applied to the
year's own purchaser values - the year-portable design, not an approximation
of a published number that exists. The margin columns keep BEA's
tax-inclusive definition (``fiscal=None``): splitting a nowcast year's sales
tax out of its trade margins needs the year's own fiscal layer, which is the
#823 generalisation and not built here.

⚠️ **The import control is ``MCIF + MDTY``** per #816's benchmark finding;
whether a nowcast-year ``MCIF`` is c.i.f. per commodity (making that control
carry over unchanged) is #822 and open. The sidecar names the control used.

Two commodities, ``4200ID`` and ``S00900``, are not balance labels and enter
the conversion as zero rows; ``4200ID`` still receives the customs-duty
credit in ``F05000`` and the duty wedge in Make, by rule.

Testing: no direct unit tests, on purpose - like
:mod:`nowcast_sut_assembly`, this is wiring over heavy cached inputs, and the
per-year identity gates in :func:`mut_from_balanced` are the test: they run
on the real objects every build, and a violation refuses the save.
"""

from __future__ import annotations

import argparse
import functools
import json
import posixpath
import sys
import typing as ta
from datetime import datetime
from pathlib import Path

import pandas as pd

from bedrock.extract.iot.nowcast_mut_storage import (
    GCS_NOWCAST_MUT_DIR,
    MARGINS_VALUE_COLUMNS,
    MutTable,
    default_nowcast_mut_vintage,
    nowcast_mut_artifact_name,
)
from bedrock.transform.iot.margin_rates import (
    BUYER_CODES,
    MARGIN_COMMODITIES,
    RatePanel,
    build_rate_panel,
    goods_commodities,
)
from bedrock.transform.iot.mut_use_to_import_matrix import import_matrix_from_use
from bedrock.transform.iot.nowcast_va_taxes import va_tax_rows
from bedrock.transform.iot.supply_to_make import make_from_sut
from bedrock.transform.iot.sut_use_to_mut_use import (
    V00200_COMPONENTS,
    use_producer_from_sut,
)
from bedrock.transform.iot.use_price_bridge import MARGIN_FAMILIES, margin_columns
from bedrock.utils.config.settings import (
    FBS_DIR,
    GIT_BRANCH,
    GIT_HASH,
    GIT_HASH_LONG,
    PKG_VERSION_NUMBER,
)
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_final_demand import SUT_FINAL_DEMAND_CODES
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES

#: Years the Step 5 balance publishes products for; 2017 is the anchor and
#: stays on the published benchmark tables.
NOWCAST_MUT_YEARS: tuple[int, ...] = tuple(range(2018, 2024))

#: Identity-gate tolerance. BEA publishes nothing below $1M, and every gate
#: here checks a relation that holds by construction, so a breach is a bug,
#: not noise.
GATE_ATOL = 1.0 * MILLION_CURRENCY_TO_CURRENCY

#: The six value-added rows of the balanced Use SUT, in balance order.
_SUT_VA_ROWS = ('V00100', 'T00OTOP', 'T00OSUB', 'V00300', 'T00TOP', 'T00SUB')

#: Bridge columns the conversion consumes from the balanced Supply table.
_BRIDGE_COLUMNS = ('MCIF', 'MADJ', 'MDTY')

_PRO = "Producers' Value"
_PUR = "Purchasers' Value"

#: How the published three margin columns collapse from the margin families.
_FAMILY_COLUMN = {
    'transport': 'Transportation',
    'wholesale': 'Wholesale',
    'retail': 'Retail',
}


class MutTables(ta.NamedTuple):
    """One year's Step 6 output, USD, before redefinitions."""

    year: int
    make: pd.DataFrame
    use: pd.DataFrame
    imports: pd.DataFrame
    margins: pd.DataFrame
    #: The ``Balanced_Detail_*`` stems the build read, for the sidecar.
    sources: tuple[str, ...]


# --- loading the Step 5 products -------------------------------------------


def _balanced_path(kind: str, year: int, directory: Path) -> Path:
    """The newest ``Balanced_Detail_<kind>_<year>_*.parquet`` in *directory*."""
    matches = sorted(
        directory.glob(f'Balanced_Detail_{kind}_{year}_*.parquet'),
        key=lambda p: p.stat().st_mtime,
    )
    if not matches:
        raise FileNotFoundError(
            f'no Balanced_Detail_{kind}_{year}_*.parquet in {directory}; '
            'run bedrock.transform.iot.nowcast_sut_assembly for the year first'
        )
    return matches[-1]


def load_balanced_sut(
    year: int, directory: Path | None = None
) -> tuple[pd.DataFrame, pd.DataFrame, tuple[str, ...]]:
    """The year's balanced Supply and Use SUT, converted from $M to USD."""
    where = Path(directory) if directory is not None else Path(FBS_DIR)
    supply_path = _balanced_path('Supply', year, where)
    use_path = _balanced_path('Use_SUT', year, where)
    supply = pd.read_parquet(supply_path) * MILLION_CURRENCY_TO_CURRENCY
    use = pd.read_parquet(use_path) * MILLION_CURRENCY_TO_CURRENCY
    return supply, use, (supply_path.stem, use_path.stem)


def _conform_use_sut(use: pd.DataFrame) -> pd.DataFrame:
    """The balanced Use SUT on the full conversion axes, zero-filled.

    ``4200ID`` and ``S00900`` are not balance labels; their rows enter as
    zeros. Missing labels beyond those two mean the input is not a balanced
    Use SUT, so they raise instead of being papered over.
    """
    commodities = list(USA_2017_COMMODITY_CODES)
    unexpected = set(commodities) - set(use.index) - {'4200ID', 'S00900'}
    if unexpected:
        raise ValueError(
            f'balanced Use SUT is missing commodity rows {sorted(unexpected)}'
        )
    missing_va = [row for row in _SUT_VA_ROWS if row not in use.index]
    if missing_va:
        raise ValueError(f'balanced Use SUT is missing VA rows {missing_va}')
    fd_columns = [c for c in SUT_FINAL_DEMAND_CODES if c in use.columns]
    return use.reindex(
        index=commodities + list(_SUT_VA_ROWS),
        columns=list(USA_2017_INDUSTRY_CODES) + fd_columns,
    ).fillna(0.0)


def _supply_bridge(supply: pd.DataFrame) -> pd.DataFrame:
    missing = [c for c in _BRIDGE_COLUMNS if c not in supply.columns]
    if missing:
        raise ValueError(f'balanced Supply table is missing bridge columns {missing}')
    return (
        supply[list(_BRIDGE_COLUMNS)]
        .reindex(list(USA_2017_COMMODITY_CODES))
        .fillna(0.0)
    )


def _supply_block(supply: pd.DataFrame) -> pd.DataFrame:
    return (
        supply.drop(
            columns=[c for c in supply.columns if c not in USA_2017_INDUSTRY_CODES]
        )
        .reindex(
            index=list(USA_2017_COMMODITY_CODES),
            columns=list(USA_2017_INDUSTRY_CODES),
        )
        .fillna(0.0)
    )


# --- the margins table ------------------------------------------------------


def margins_table(
    use_sut: pd.DataFrame, panel: RatePanel | None = None
) -> pd.DataFrame:
    """The year's hyper-detailed Margins table, USD, from the purchaser Use.

    Index ``(Industry Code, Commodity Code)``; columns are the five published
    value columns followed by one column per margin commodity. Rows carrying
    nothing on any value column are left out.

    Goods rows: ``Producers' Value`` is the panel's ``pro`` rate on the cell's
    purchaser value, the margin columns are :func:`margin_columns`' placement
    (tax-inclusive), and the identity ``PRO + T + W + R = PUR`` closes cell by
    cell because the rate components sum to one. Margin-commodity rows follow
    the published convention: ``Purchasers' Value`` is the buyer's direct
    purchase of the margin service, ``Producers' Value`` adds the margin
    routed onto that commodity, and the margin columns are zero.
    """
    rates = panel if panel is not None else build_rate_panel(2017)
    goods = goods_commodities()
    buyers = [b for b in BUYER_CODES if b in use_sut.columns]
    purchaser = use_sut.loc[goods, buyers].astype(float)

    pro = rates.rates['pro'].reindex(index=goods, columns=buyers).fillna(1.0)
    # The panel spans all 422 buyer codes; the SUT has no F05000 column, so
    # alignment inside margin_columns grows an all-NaN column - restrict back
    # to the live buyers before anything is stored.
    placed = {
        code: frame.reindex(index=goods, columns=buyers).fillna(0.0)
        for code, frame in margin_columns(purchaser, rates, None).items()
    }

    wide: dict[str, pd.DataFrame] = {_PRO: pro * purchaser, _PUR: purchaser}
    for family, codes in MARGIN_FAMILIES.items():
        wide[_FAMILY_COLUMN[family]] = functools.reduce(
            lambda a, b: a + b, (placed[code] for code in codes)
        )
    for code in MARGIN_COMMODITIES:
        wide[code] = placed[code]

    direct = use_sut.reindex(index=list(MARGIN_COMMODITIES), columns=buyers).fillna(0.0)
    booked = pd.DataFrame(
        {code: frame.sum(axis=0) for code, frame in placed.items()}
    ).T.reindex(index=list(MARGIN_COMMODITIES), columns=buyers)

    columns = [*MARGINS_VALUE_COLUMNS, *MARGIN_COMMODITIES]
    goods_block = pd.DataFrame({name: wide[name].stack() for name in columns}).reindex(
        columns=columns
    )
    margin_block = pd.DataFrame(0.0, index=direct.stack().index, columns=columns)
    margin_block[_PUR] = direct.stack()
    margin_block[_PRO] = (direct + booked).stack()

    table = pd.concat([goods_block, margin_block])
    table = table.loc[(table != 0.0).any(axis=1)]
    # The wide frames stack commodity-major; the published table and the Use
    # conversion key on (buyer, commodity).
    table.index = table.index.set_names(['Commodity Code', 'Industry Code'])
    return table.swaplevel().sort_index()


# --- the build --------------------------------------------------------------


def _gate(label: str, gap_usd: float) -> None:
    if abs(gap_usd) > GATE_ATOL:
        raise AssertionError(
            f'{label} is off by ${gap_usd / MILLION_CURRENCY_TO_CURRENCY:,.3f}M; '
            'this identity holds by construction, so the input or the wiring '
            'is wrong'
        )


def mut_from_balanced(
    year: int,
    supply: pd.DataFrame,
    use: pd.DataFrame,
    *,
    panel: RatePanel | None = None,
    sources: tuple[str, ...] = (),
) -> MutTables:
    """The year's MUT quartet from its balanced Supply and Use SUT (USD).

    Every gate below checks a relation the construction guarantees; see
    :data:`GATE_ATOL`.
    """
    use_sut = _conform_use_sut(use)
    supply_block = _supply_block(supply)
    bridge = _supply_bridge(supply)
    commodities = list(USA_2017_COMMODITY_CODES)
    industries = list(USA_2017_INDUSTRY_CODES)
    fd_columns = [c for c in use_sut.columns if c not in industries]

    taxes = va_tax_rows(year, block=supply_block)
    make = make_from_sut(supply_block, taxes, year)

    margins = margins_table(use_sut, panel=panel)
    goods_rows = margins.loc[
        ~margins.index.get_level_values('Commodity Code').isin(MARGIN_COMMODITIES)
    ]
    closure = (
        goods_rows[MARGINS_VALUE_COLUMNS[:-1]].sum(axis=1) - goods_rows[_PUR]
    ).abs()
    _gate(f'{year} margins goods-row identity (worst cell)', float(closure.max()))
    stripped = float((goods_rows[_PUR] - goods_rows[_PRO]).sum())
    margin_rows = margins.loc[
        margins.index.get_level_values('Commodity Code').isin(MARGIN_COMMODITIES)
    ]
    routed = float((margin_rows[_PRO] - margin_rows[_PUR]).sum())
    _gate(f'{year} margin mass conservation', stripped - routed)

    converted = use_producer_from_sut(use_sut, bridge, margins, year)
    interior = converted.loc[commodities, industries + fd_columns]
    if interior.isna().any().any():
        raise AssertionError(f'{year} producer-price Use carries NaN cells')
    column_gap = (
        interior.sum(axis=0)
        - use_sut.loc[commodities, industries + fd_columns].sum(axis=0)
    ).abs()
    _gate(
        f'{year} Use buyer-total preservation (worst column)', float(column_gap.max())
    )
    v00200_gap = (
        converted.loc[['V00200'], industries].iloc[0]
        - use_sut.loc[list(V00200_COMPONENTS), industries].sum(axis=0)
    ).abs()
    _gate(f'{year} V00200 collapse identity (worst industry)', float(v00200_gap.max()))

    import_control = bridge['MCIF'].astype(float) + bridge['MDTY'].astype(float)
    imports = import_matrix_from_use(converted.loc[commodities], import_control)
    allocated = imports.drop(columns=['F05000'], errors='ignore').sum().sum()
    _gate(f'{year} import allocation total', float(allocated - import_control.sum()))

    return MutTables(
        year=year,
        make=make,
        use=converted,
        imports=imports,
        margins=margins,
        sources=sources,
    )


def build(year: int, directory: Path | None = None) -> MutTables:
    """Load the year's balanced SUT and convert it. The one-call entry point."""
    supply, use, sources = load_balanced_sut(year, directory)
    return mut_from_balanced(year, supply, use, sources=sources)


# --- saving -----------------------------------------------------------------

_TABLE_ATTR: dict[MutTable, str] = {
    'Make': 'make',
    'Use': 'use',
    'Import': 'imports',
    'Margins': 'margins',
}


def save_mut(
    tables: MutTables,
    out_dir: Path | None = None,
    *,
    upload: bool = False,
) -> list[Path]:
    """Persist the quartet as versioned parquet + metadata sidecars.

    Files land in *out_dir* (default ``transform/output_data``) under the
    names :func:`nowcast_mut_storage.nowcast_mut_artifact_name` resolves, so
    the detail loaders find a local build without touching GCS. With
    *upload*, both files also go to :data:`GCS_NOWCAST_MUT_DIR`, which needs
    credentials (``scripts/google-login``). Step 6 output is before
    redefinitions by construction; Step 7 owns the ``after`` stage.
    """
    directory = Path(out_dir) if out_dir is not None else Path(FBS_DIR)
    directory.mkdir(parents=True, exist_ok=True)
    vintage = default_nowcast_mut_vintage()

    written: list[Path] = []
    for table, attr in _TABLE_ATTR.items():
        frame: pd.DataFrame = getattr(tables, attr)
        name = nowcast_mut_artifact_name(
            table, year=tables.year, stage='before', vintage=vintage
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
                'step': 'Step 6 - SUT to MUT conversion, before redefinitions',
                'vintage': vintage,
                'units': 'USD',
                'branch': GIT_BRANCH,
                'commit': GIT_HASH_LONG,
                'balanced_inputs': list(tables.sources),
                'margin_anchor': (
                    '2017 rate panel and placement shares, tax-inclusive '
                    '(BEA definition); the nowcast-year fiscal split is #823'
                ),
                'import_control': 'MCIF + MDTY per #816; nowcast basis is #822',
                'builder': 'bedrock.transform.iot.nowcast_mut',
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


# --- CLI --------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        '--year',
        type=int,
        action='append',
        choices=NOWCAST_MUT_YEARS,
        help='year to convert; repeatable. Default: every year with balanced products',
    )
    parser.add_argument(
        '--no-save',
        action='store_true',
        help='run the conversion and its gates without persisting the tables',
    )
    parser.add_argument(
        '--gcs',
        action='store_true',
        help='also upload the saved products to GCS (needs credentials)',
    )
    args = parser.parse_args(argv)

    years = tuple(args.year) if args.year else NOWCAST_MUT_YEARS
    panel = build_rate_panel(2017)
    for year in years:
        supply, use, sources = load_balanced_sut(year)
        tables = mut_from_balanced(year, supply, use, panel=panel, sources=sources)
        interior = tables.use.loc[
            list(USA_2017_COMMODITY_CODES), list(USA_2017_INDUSTRY_CODES)
        ]
        print(
            f'{year}: Make {tables.make.shape}, Use {tables.use.shape}, '
            f'Import {tables.imports.shape}, Margins {len(tables.margins):,} rows; '
            f'intermediate total '
            f'${interior.sum().sum() / MILLION_CURRENCY_TO_CURRENCY:,.0f}M'
        )
        if not args.no_save:
            written = save_mut(tables, upload=args.gcs)
            print(f'  saved {len(written)} files to {written[0].parent}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
