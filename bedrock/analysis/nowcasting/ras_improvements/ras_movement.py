"""#755 seed→balanced RAS movement (Use intermediate vs FD).

Assemble once per year, then soft and/or hard engine protocols. Δ is from
in-memory restored Use (pre-``save_balance``). Does **not** call
``balance_year``.

Example::

    uv run python -m bedrock.analysis.nowcasting.ras_improvements.ras_movement
    uv run python -m bedrock.analysis.nowcasting.ras_improvements.ras_movement \\
        --years 2017-2023 --protocols soft,hard
"""

from __future__ import annotations

import argparse
import traceback
from collections.abc import Sequence
from pathlib import Path

import pandas as pd

from bedrock.transform.iot.nowcast_mask import balance_commodities, balance_industries
from bedrock.transform.iot.nowcast_sut_assembly import (
    YearBalance,
    assemble,
    assert_post_balance_hygiene,
)
from bedrock.transform.iot.nowcast_sut_gras import engine
from bedrock.utils.economic.balance.offset import (
    offset_targets,
    restore_fixed_blocks,
    split_fixed_blocks,
)
from bedrock.utils.taxonomy.bea.v2017_final_demand import SUT_FINAL_DEMAND_CODES

PACKAGE_DIR = Path(__file__).resolve().parent
#: #755 artifacts — sibling of hygiene ``output/ras_improvements/``.
OUTPUT_DIR = PACKAGE_DIR.parent / 'output' / 'ras_movement'

PROTOCOL_SOFT = 'soft'
PROTOCOL_HARD = 'hard'
_PROTOCOL_TO_IMPOSE: dict[str, bool] = {
    PROTOCOL_SOFT: True,
    PROTOCOL_HARD: False,
}

DELTA_COLUMNS = (
    'year',
    'protocol',
    'commodity',
    'seed_inter',
    'seed_fd',
    'bal_inter',
    'bal_fd',
    'delta_inter',
    'delta_fd',
    'absorption_share',
    'l1_inter',
    'l1_fd',
    'hygiene_ok',
    'hygiene_error',
)


def resolve_output_dir(*, create: bool = False) -> Path:
    """Module-local output directory (not hygiene ``common.OUTPUT_DIR``)."""
    if create:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return OUTPUT_DIR


def partition_labels() -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    """Commodity rows, intermediate industries, full SUT FD columns."""
    return (
        balance_commodities(),
        balance_industries(),
        tuple(SUT_FINAL_DEMAND_CODES),
    )


def absorption_share(
    delta_inter: float | pd.Series,
    delta_fd: float | pd.Series,
) -> float | pd.Series:
    """``|δ_inter| / (|δ_inter| + |δ_fd|)``; 0 when both zero."""
    abs_i = delta_inter.abs() if hasattr(delta_inter, 'abs') else abs(delta_inter)
    abs_f = delta_fd.abs() if hasattr(delta_fd, 'abs') else abs(delta_fd)
    denom = abs_i + abs_f
    if isinstance(denom, pd.Series):
        out = abs_i / denom
        return out.where(denom != 0.0, 0.0)
    if denom == 0.0:
        return 0.0
    return abs_i / denom


def commodity_ras_delta(
    seed: pd.DataFrame,
    balanced: pd.DataFrame,
    *,
    year: int,
    protocol: str,
    hygiene_ok: bool = True,
    hygiene_error: str = '',
    commodities: Sequence[str] | None = None,
    industries: Sequence[str] | None = None,
    fd_codes: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Per-commodity seed/balanced levels and RAS Δ on intermediate vs FD."""
    if commodities is None or industries is None or fd_codes is None:
        commodities, industries, fd_codes = partition_labels()
    commodities = tuple(
        c for c in commodities if c in seed.index and c in balanced.index
    )
    industries = tuple(
        c for c in industries if c in seed.columns and c in balanced.columns
    )
    fd_codes = tuple(c for c in fd_codes if c in seed.columns and c in balanced.columns)

    seed_inter = seed.loc[list(commodities), list(industries)].sum(axis=1)
    seed_fd = seed.loc[list(commodities), list(fd_codes)].sum(axis=1)
    bal_inter = balanced.loc[list(commodities), list(industries)].sum(axis=1)
    bal_fd = balanced.loc[list(commodities), list(fd_codes)].sum(axis=1)
    delta_inter = bal_inter - seed_inter
    delta_fd = bal_fd - seed_fd
    cell_delta = (
        balanced.loc[list(commodities), list(industries)]
        - seed.loc[list(commodities), list(industries)]
    )
    cell_delta_fd = (
        balanced.loc[list(commodities), list(fd_codes)]
        - seed.loc[list(commodities), list(fd_codes)]
    )
    l1_inter = cell_delta.abs().sum(axis=1)
    l1_fd = cell_delta_fd.abs().sum(axis=1)
    share = absorption_share(delta_inter, delta_fd)

    return pd.DataFrame(
        {
            'year': year,
            'protocol': protocol,
            'commodity': list(commodities),
            'seed_inter': seed_inter.to_numpy(),
            'seed_fd': seed_fd.to_numpy(),
            'bal_inter': bal_inter.to_numpy(),
            'bal_fd': bal_fd.to_numpy(),
            'delta_inter': delta_inter.to_numpy(),
            'delta_fd': delta_fd.to_numpy(),
            'absorption_share': (
                share.to_numpy() if isinstance(share, pd.Series) else share
            ),
            'l1_inter': l1_inter.to_numpy(),
            'l1_fd': l1_fd.to_numpy(),
            'hygiene_ok': hygiene_ok,
            'hygiene_error': hygiene_error,
        }
    )


def top_movers(delta: pd.DataFrame, n: int = 40) -> pd.DataFrame:
    """Top *n* commodities by ``|delta_inter|`` descending."""
    ranked = delta.reindex(
        delta['delta_inter'].abs().sort_values(ascending=False).index
    )
    return ranked.head(int(n)).reset_index(drop=True)


def build_commodity_ras_yoy(deltas: pd.DataFrame) -> pd.DataFrame:
    """YoY from per-year level+Δ aggregates only.

    ``ras_delta_*`` columns are year-``t`` ``delta_*`` passthrough (not YoY).
    Omits year ``t`` unless ``t-1`` is present in the same protocol span.
    """
    if deltas.empty:
        return pd.DataFrame(
            columns=[
                'commodity',
                'year',
                'protocol',
                'seed_yoy_inter',
                'seed_yoy_fd',
                'ras_delta_inter',
                'ras_delta_fd',
                'ras_delta_yoy_inter',
                'ras_delta_yoy_fd',
                'balanced_yoy_inter',
                'balanced_yoy_fd',
            ]
        )

    rows: list[dict[str, object]] = []
    for protocol, proto in deltas.groupby('protocol', sort=False):
        years = sorted(int(y) for y in proto['year'].astype(int).unique().tolist())
        year_set = set(years)
        by_year = {
            int(y): g.set_index('commodity')
            for y, g in proto.groupby(proto['year'].astype(int), sort=False)
        }
        for t in years:
            if (t - 1) not in year_set:
                continue
            cur = by_year[t]
            prev = by_year[t - 1]
            commodities = cur.index.intersection(prev.index)
            for commodity in commodities:
                c = cur.loc[commodity]
                p = prev.loc[commodity]
                rows.append(
                    {
                        'commodity': commodity,
                        'year': t,
                        'protocol': protocol,
                        'seed_yoy_inter': float(c['seed_inter'] - p['seed_inter']),
                        'seed_yoy_fd': float(c['seed_fd'] - p['seed_fd']),
                        'ras_delta_inter': float(c['delta_inter']),
                        'ras_delta_fd': float(c['delta_fd']),
                        'ras_delta_yoy_inter': float(
                            c['delta_inter'] - p['delta_inter']
                        ),
                        'ras_delta_yoy_fd': float(c['delta_fd'] - p['delta_fd']),
                        'balanced_yoy_inter': float(c['bal_inter'] - p['bal_inter']),
                        'balanced_yoy_fd': float(c['bal_fd'] - p['bal_fd']),
                    }
                )
    return pd.DataFrame(rows)


def protocol_soft_vs_hard(
    soft: pd.DataFrame,
    hard: pd.DataFrame,
) -> pd.DataFrame:
    """``soft − hard`` on Δ (seed shared → same as bal soft − bal hard)."""
    s = soft.set_index('commodity')
    h = hard.set_index('commodity')
    commodities = s.index.intersection(h.index)
    year = (
        int(soft['year'].iloc[0])
        if len(soft)
        else (int(hard['year'].iloc[0]) if len(hard) else 0)
    )
    return pd.DataFrame(
        {
            'year': year,
            'commodity': list(commodities),
            'soft_minus_hard_inter': (
                s.loc[commodities, 'delta_inter'] - h.loc[commodities, 'delta_inter']
            ).to_numpy(),
            'soft_minus_hard_fd': (
                s.loc[commodities, 'delta_fd'] - h.loc[commodities, 'delta_fd']
            ).to_numpy(),
            'hygiene_ok_soft': s.loc[commodities, 'hygiene_ok'].to_numpy(),
            'hygiene_ok_hard': h.loc[commodities, 'hygiene_ok'].to_numpy(),
            'hygiene_error_soft': s.loc[commodities, 'hygiene_error'].to_numpy(),
            'hygiene_error_hard': h.loc[commodities, 'hygiene_error'].to_numpy(),
        }
    )


def run_protocol(assembled: YearBalance, *, protocol: str) -> pd.DataFrame:
    """Split → offset → engine → restore → Δ → then hygiene assert."""
    if protocol not in _PROTOCOL_TO_IMPOSE:
        raise ValueError(f'unknown protocol {protocol!r}; use soft or hard')
    impose_soft = _PROTOCOL_TO_IMPOSE[protocol]
    frozen, free = split_fixed_blocks(assembled.seeds, assembled.masks)
    residual = offset_targets(assembled.targets, frozen)
    out = engine(free, residual, assembled.masks, impose_soft=impose_soft)
    restored = restore_fixed_blocks(out.blocks, frozen)
    hygiene_ok = True
    hygiene_error = ''
    try:
        assert_post_balance_hygiene(assembled.year, restored, assembled.masks)
    except ValueError as err:
        hygiene_ok = False
        hygiene_error = str(err)
    return commodity_ras_delta(
        assembled.seeds['use'],
        restored['use'],
        year=assembled.year,
        protocol=protocol,
        hygiene_ok=hygiene_ok,
        hygiene_error=hygiene_error,
    )


def diagnose_year(
    year: int,
    protocols: Sequence[str],
    *,
    top: int = 40,
    out_dir: Path | None = None,
) -> dict[str, pd.DataFrame]:
    """Assemble once; run each protocol; write per-year CSVs.

    Returns protocol → delta frame (for YoY aggregation). Does not call
    ``balance_year``.
    """
    dest = out_dir if out_dir is not None else resolve_output_dir(create=True)
    dest.mkdir(parents=True, exist_ok=True)
    assembled = assemble(int(year), fitted=True)
    by_protocol: dict[str, pd.DataFrame] = {}
    for protocol in protocols:
        print(f'{year}: engine protocol={protocol}', flush=True)
        delta = run_protocol(assembled, protocol=protocol)
        by_protocol[protocol] = delta
        delta_path = dest / f'commodity_ras_delta_{year}_{protocol}.csv'
        delta.to_csv(delta_path, index=False)
        movers = top_movers(delta, n=top)
        movers.to_csv(dest / f'top_movers_{year}_{protocol}.csv', index=False)
        print(
            f'{year} {protocol}: wrote {delta_path.name} '
            f'(hygiene_ok={bool(delta["hygiene_ok"].iloc[0]) if len(delta) else "n/a"})',
            flush=True,
        )

    if PROTOCOL_SOFT in by_protocol and PROTOCOL_HARD in by_protocol:
        cmp = protocol_soft_vs_hard(
            by_protocol[PROTOCOL_SOFT], by_protocol[PROTOCOL_HARD]
        )
        cmp_path = dest / f'protocol_soft_vs_hard_{year}.csv'
        cmp.to_csv(cmp_path, index=False)
        print(f'{year}: wrote {cmp_path.name}', flush=True)
    return by_protocol


def parse_years(spec: str) -> list[int]:
    """Assembly-style range: ``2018`` or ``2017-2023`` (not comma lists)."""
    first, _, last = spec.partition('-')
    return list(range(int(first), int(last or first) + 1))


def parse_protocols(spec: str) -> list[str]:
    names = [p.strip().lower() for p in spec.split(',') if p.strip()]
    if not names:
        raise ValueError('at least one protocol required')
    unknown = [p for p in names if p not in _PROTOCOL_TO_IMPOSE]
    if unknown:
        raise ValueError(f'unknown protocol(s) {unknown}; use soft and/or hard')
    # Preserve order, drop duplicates.
    seen: set[str] = set()
    out: list[str] = []
    for name in names:
        if name not in seen:
            seen.add(name)
            out.append(name)
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            '#755 RAS movement diagnosis (seed→balanced Use Δ). '
            'Default years 2017-2023 are the #755 span, not FIT_YEARS. '
            'Soft+hard over that span is ~half a workday wall clock; not CI.'
        )
    )
    parser.add_argument(
        '--years',
        default='2017-2023',
        help='year or inclusive range (e.g. 2022 or 2017-2023); not FIT_YEARS',
    )
    parser.add_argument(
        '--protocols',
        default=PROTOCOL_SOFT,
        help='comma list of soft and/or hard (default: soft)',
    )
    parser.add_argument(
        '--top',
        type=int,
        default=40,
        help='top movers by |delta_inter| (default: 40)',
    )
    args = parser.parse_args(argv)
    years = parse_years(args.years)
    protocols = parse_protocols(args.protocols)
    out_dir = resolve_output_dir(create=True)

    collected: list[pd.DataFrame] = []
    failures = 0
    for year in years:
        try:
            by_protocol = diagnose_year(year, protocols, top=args.top, out_dir=out_dir)
        except BaseException as err:  # noqa: BLE001 - continue the span
            failures += 1
            print(
                f'{year}: FAILED {type(err).__name__}: {err}\n'
                f'{traceback.format_exc()}',
                flush=True,
            )
            continue
        collected.extend(by_protocol.values())

    if collected:
        all_deltas = pd.concat(collected, ignore_index=True)
        yoy = build_commodity_ras_yoy(all_deltas)
        yoy_path = out_dir / 'commodity_ras_yoy.csv'
        yoy.to_csv(yoy_path, index=False)
        print(f'Wrote {yoy_path} ({len(yoy)} rows)', flush=True)
    else:
        print('No successful years; skipped commodity_ras_yoy.csv', flush=True)

    return 1 if failures else 0


if __name__ == '__main__':
    raise SystemExit(main())
