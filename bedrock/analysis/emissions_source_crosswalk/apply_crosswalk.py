"""Apply the FBS → ceda emissions-source crosswalk to a retained-activity FBS.

CLI / review entrypoint for this analysis package. A draft snapshot derive
(:mod:`bedrock.transform.allocation.emissions_source_E`) also reads
``fbs_to_ceda_emissions_source.csv`` without nested-split ``--shares``.

``fbs_to_ceda_emissions_source.csv`` maps ``MetaSources`` × ``ActivityProducedBy``
× ``Flowable`` to one of ceda's 123 ``EmissionsSource`` codes. ``*`` matches
anything, and the most specific row wins (source, then activity, then gas).
``status`` is one of:

- ``mapped``: one ceda source.
- ``bundled``: the FBS line spans the ``;``-separated ceda sources and needs
  a split key.
- ``no_ceda_source``: ceda has no source for this flow.
- ``no_species``: already CO2e with no gas species.

With ``--shares`` (written by :mod:`build_nested_split`), lines attributed
through a nested EPA table are first split by sector into their activity sets,
and ``ActivityProducedBy`` becomes the set's label (``EPA_GHGI_T_5_18.pasture``).

Reports CO2e by status, the FBS keys no row matches, and the ceda sources
nothing reaches.

Usage::

    python -m bedrock.analysis.emissions_source_crosswalk.apply_crosswalk \
        <retained FBS parquet> [--shares <shares csv>] [--ceda-root ../ceda]
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from bedrock.analysis.emissions_source_crosswalk.inventory_fbs_sources import (
    OUTPUT_DIR,
    load_ceda_sources,
)
from bedrock.utils.emissions.gwp import GWP100_AR5_CEDA, GWP100_AR6_CEDA

CROSSWALK_PATH = Path(__file__).resolve().parent / 'fbs_to_ceda_emissions_source.csv'
KEYS = ['MetaSources', 'ActivityProducedBy', 'Flowable']


def main() -> None:
    parser = argparse.ArgumentParser(description=(__doc__ or '').splitlines()[0])
    parser.add_argument('fbs', type=Path)
    parser.add_argument('--shares', type=Path, default=None)
    parser.add_argument(
        '--speciate-hfcs',
        action='store_true',
        help='Split unspecified HFC/PFC CO2e across species (AR5 basis).',
    )
    parser.add_argument(
        '--ceda-root',
        type=Path,
        default=Path(__file__).resolve().parents[3].parent / 'ceda',
    )
    args = parser.parse_args()

    ceda = load_ceda_sources(args.ceda_root)
    crosswalk = load_crosswalk(set(ceda['code']))
    fbs = pd.read_parquet(args.fbs)
    if args.shares is not None:
        fbs = split_nested_lines(
            fbs,
            pd.read_csv(args.shares, dtype=str, keep_default_na=False).astype(
                {'share': float}
            ),
        )
    if args.speciate_hfcs:
        fbs = speciate_unspecified_hfcs(fbs)
    keys = fbs_keys(fbs)
    mapped = apply_crosswalk(keys, crosswalk)

    OUTPUT_DIR.mkdir(exist_ok=True)
    mapped.to_csv(OUTPUT_DIR / f'{args.fbs.stem}_crosswalked.csv', index=False)
    by_ceda_source(mapped, ceda).to_csv(
        OUTPUT_DIR / f'{args.fbs.stem}_by_ceda_source.csv', index=False
    )
    _print_summary(mapped, ceda)


def load_crosswalk(ceda_codes: set[str]) -> pd.DataFrame:
    """Read the crosswalk and check every ceda code exists."""
    cw = pd.read_csv(CROSSWALK_PATH, dtype=str, keep_default_na=False)
    named = {
        c.strip() for cell in cw['ceda_source'] for c in cell.split(';') if c.strip()
    }
    unknown = named - ceda_codes
    if unknown:
        raise ValueError(
            f'Crosswalk names codes ceda does not define: {sorted(unknown)}'
        )
    duplicated = cw.duplicated(['meta_source', 'activity', 'flowable'])
    if duplicated.any():
        raise ValueError(f'Duplicate crosswalk keys:\n{cw[duplicated]}')
    cw['specificity'] = (
        4 * (cw['meta_source'] != '*')
        + 2 * (cw['activity'] != '*')
        + (cw['flowable'] != '*')
    )
    return cw


def split_nested_lines(fbs: pd.DataFrame, shares: pd.DataFrame) -> pd.DataFrame:
    """Split FBS lines by sector into the parts ``shares`` names.

    A shares row with an ``activity`` applies to that ``ActivityProducedBy``
    only; an empty ``activity`` applies to every row of its ``MetaSources``.
    """
    shares = shares.assign(activity=shares.get('activity', '')).fillna({'activity': ''})
    specific = set(shares.loc[shares['activity'] != '', 'MetaSources'])
    whole = set(shares.loc[shares['activity'] == '', 'MetaSources'])
    activity = fbs['ActivityProducedBy'].astype(object).fillna('')
    key = activity.where(fbs['MetaSources'].isin(specific), '')
    targeted = set(zip(shares['MetaSources'], shares['activity']))
    nested = pd.Series(
        [(m, a) in targeted for m, a in zip(fbs['MetaSources'], key)], index=fbs.index
    ) | fbs['MetaSources'].isin(whole - specific)
    rows = (
        fbs[nested]
        .assign(activity=key[nested])
        .merge(
            shares,
            on=['MetaSources', 'activity', 'SectorProducedBy'],
            how='left',
            validate='m:m',
        )
    )
    unshared = rows['share'].isna()
    if unshared.any():
        missing = rows.loc[unshared, ['MetaSources', 'activity', 'SectorProducedBy']]
        raise ValueError(f'No split shares for:\n{missing.drop_duplicates()}')
    rows['FlowAmount'] = rows['FlowAmount'] * rows['share']
    rows['ActivityProducedBy'] = rows['split_label']
    split = rows.drop(
        columns=['split_label', 'share', 'activity', 'basis'], errors='ignore'
    )

    before = fbs[nested].groupby('MetaSources', observed=True)['FlowAmount'].sum()
    after = split.groupby('MetaSources', observed=True)['FlowAmount'].sum()
    if not ((after - before).abs() <= 1e-9 * before.abs()).all():
        raise ValueError(f'Split changed totals:\n{pd.concat([before, after], axis=1)}')
    return pd.concat([fbs[~nested], split], ignore_index=True)


UNSPECIFIED = 'HFCs and PFCs, unspecified'
_AR5: dict[str, float] = {str(k): v for k, v in GWP100_AR5_CEDA.items()}
# FBS species name → AR5 GWP key; the HFC/PFC species an unspecified amount
# can be split across.
SPECIES_AR5_KEY = {
    'HFC-23': 'HFC-23',
    'HFC-32': 'HFC-32',
    'HFC-125': 'HFC-125',
    'HFC-134a': 'HFC-134a',
    'HFC-143a': 'HFC-143a',
    'HFC-227ea': 'HFC-227ea',
    'HFC-236fa': 'HFC-236fa',
    'Carbon tetrafluoride': 'CF4',
    'Hexafluoroethane': 'C2F6',
    'Perfluoropropane': 'C3F8',
    'Perfluorocyclobutane': 'c-C4F8',
}


def speciate_unspecified_hfcs(fbs: pd.DataFrame) -> pd.DataFrame:
    """Split unspecified HFC/PFC CO2e across the species reported beside it.

    The UMD tables give part of each HFC/PFC line only as AR5 CO2e. Each such
    amount is split in proportion to the AR5 CO2e of the species the same
    activity reports in the same sector, else in that activity across all
    sectors, then turned into kg of each species with AR5 GWPs. Amounts with
    no species beside them at all stay unspecified. AR5 CO2e is unchanged.
    """
    gwp = fbs['Flowable'].map(SPECIES_AR5_KEY).map(GWP100_AR5_CEDA)
    species = fbs[gwp.notna() & (fbs['Unit'] == 'kg')].assign(
        ar5=lambda d: d['FlowAmount'] * gwp[d.index]
    )
    unspecified = fbs['Flowable'] == UNSPECIFIED
    activity = ['MetaSources', 'ActivityProducedBy']
    by_sector = species.groupby(
        [*activity, 'SectorProducedBy', 'Flowable'], observed=True
    )['ar5'].sum()
    by_activity = species.groupby([*activity, 'Flowable'], observed=True)['ar5'].sum()

    new_rows, kept = [], []
    for idx, row in fbs[unspecified].iterrows():
        key = (row['MetaSources'], row['ActivityProducedBy'])
        mix = by_sector.get((*key, row['SectorProducedBy']))
        if mix is None or mix.sum() <= 0:
            mix = by_activity.get(key)
        if mix is None or mix.sum() <= 0:
            kept.append(idx)
            continue
        shares = mix / mix.sum()
        for flowable, share in shares.items():
            new = row.copy()
            new['Flowable'] = flowable
            new['Unit'] = 'kg'
            new['FlowAmount'] = (
                row['FlowAmount'] * share / _AR5[SPECIES_AR5_KEY[str(flowable)]]
            )
            new_rows.append(new)
    speciated = pd.DataFrame(new_rows)
    left = fbs.index.isin(kept)
    before = fbs.loc[unspecified, 'FlowAmount'].sum()
    after = (
        speciated['FlowAmount'] * speciated['Flowable'].map(SPECIES_AR5_KEY).map(_AR5)
    ).sum() + fbs.loc[left, 'FlowAmount'].sum()
    if abs(after - before) > 1e-9 * abs(before):
        raise ValueError(f'Speciation changed AR5 CO2e: {before:.6e} → {after:.6e}')
    print(
        f'Speciated {(before - fbs.loc[left, "FlowAmount"].sum()) / 1e9:.2f} of '
        f'{before / 1e9:.2f} Mt CO2e (AR5) of unspecified HFCs/PFCs'
    )
    return pd.concat([fbs[~unspecified], fbs[left], speciated], ignore_index=True)


def fbs_keys(fbs: pd.DataFrame) -> pd.DataFrame:
    """FBS totals per source × activity × gas, in kg and AR6 CO2e."""
    gwp_key = {
        'Carbon dioxide': 'CO2',
        'Methane': 'CH4_fossil',
        'Nitrous oxide': 'N2O',
        'Sulfur hexafluoride': 'SF6',
        'Nitrogen trifluoride': 'NF3',
        'Carbon tetrafluoride': 'CF4',
        'Hexafluoroethane': 'C2F6',
        'Perfluoropropane': 'C3F8',
        'Perfluorocyclobutane': 'c-C4F8',
    }
    gwp = fbs['Flowable'].map(
        lambda f: GWP100_AR6_CEDA.get(gwp_key.get(f, f), 1.0)  # type: ignore[call-overload]
    )
    co2e = fbs['FlowAmount'] * gwp.where(fbs['Unit'] == 'kg', 1.0)
    return (
        fbs.assign(
            ActivityProducedBy=fbs['ActivityProducedBy'].astype(object).fillna(''),
            co2e=co2e,
        )
        .groupby(KEYS, observed=True)
        .agg(kg=('FlowAmount', 'sum'), co2e=('co2e', 'sum'))
        .reset_index()
    )


def apply_crosswalk(keys: pd.DataFrame, crosswalk: pd.DataFrame) -> pd.DataFrame:
    """Attach the most specific crosswalk row to each FBS key."""
    rows = []
    for key in keys.itertuples(index=False):
        hits = crosswalk[
            crosswalk['meta_source'].isin([key.MetaSources, '*'])
            & crosswalk['activity'].isin([key.ActivityProducedBy, '*'])
            & crosswalk['flowable'].isin([key.Flowable, '*'])
        ]
        if hits.empty:
            rows.append({'ceda_source': '', 'status': 'unmatched', 'note': ''})
            continue
        best = hits[hits['specificity'] == hits['specificity'].max()]
        if len(best) > 1:
            raise ValueError(f'Ambiguous crosswalk rows for {tuple(key)}:\n{best}')
        rows.append(best.iloc[0][['ceda_source', 'status', 'note']].to_dict())
    return pd.concat([keys, pd.DataFrame(rows, index=keys.index)], axis=1)


def by_ceda_source(mapped: pd.DataFrame, ceda: pd.DataFrame) -> pd.DataFrame:
    """One row per ceda source: CO2e, and the FBS lines that feed it, largest first.

    Covers all 123 sources, so the ones nothing reaches show as zero. Only
    ``mapped`` lines count; bundled lines appear under ``status`` in the
    crosswalked table instead.
    """
    single = mapped[mapped['status'] == 'mapped']
    lines = (
        single.assign(
            line=single['MetaSources']
            + ' | '
            + single['ActivityProducedBy']
            + ' | '
            + single['Flowable']
        )
        .groupby(['ceda_source', 'line'])['co2e']
        .sum()
        .div(1e9)
        .reset_index()
        .sort_values('co2e', ascending=False)
    )
    feeds = {
        source: '; '.join(
            f'{line} ({mt:.2f})' for line, mt in zip(g['line'], g['co2e'])
        )
        for source, g in lines.groupby('ceda_source')
    }
    out = ceda[['code', 'description', 'gas']].rename(columns={'code': 'ceda_source'})
    out['Mt_CO2e_AR6'] = out['ceda_source'].map(
        lines.groupby('ceda_source')['co2e'].sum()
    )
    out['n_lines'] = out['ceda_source'].map(lines.groupby('ceda_source').size())
    out['fbs_lines'] = out['ceda_source'].map(feeds)
    return (
        out.fillna({'Mt_CO2e_AR6': 0.0, 'n_lines': 0, 'fbs_lines': ''})
        .astype({'n_lines': int})
        .sort_values('Mt_CO2e_AR6', ascending=False)
        .reset_index(drop=True)
    )


def _print_summary(mapped: pd.DataFrame, ceda: pd.DataFrame) -> None:
    total = mapped['co2e'].sum()
    by_status = mapped.groupby('status')['co2e'].agg(['count', 'sum'])
    by_status['Mt_CO2e'] = by_status.pop('sum') / 1e9
    by_status['share'] = by_status['Mt_CO2e'] * 1e9 / total
    print(f'Total {total / 1e9:,.1f} Mt CO2e (AR6) over {len(mapped)} keys')
    print(by_status.sort_values('Mt_CO2e', ascending=False).round(3).to_string())

    for status in ['unmatched', 'bundled', 'no_ceda_source', 'no_species']:
        rows = mapped[mapped['status'] == status]
        if rows.empty:
            continue
        grouped = (
            rows.groupby(['MetaSources', 'ActivityProducedBy'])['co2e'].sum() / 1e9
        ).sort_values(ascending=False)
        print(f'\n{status} (Mt CO2e):')
        print(grouped[grouped > 0.05].round(2).to_string())

    reached = {
        c.strip()
        for cell in mapped['ceda_source']
        for c in cell.split(';')
        if c.strip()
    }
    missing = sorted(set(ceda['code']) - reached)
    print(f'\nceda sources nothing reaches ({len(missing)} of {len(ceda)}):')
    print(' '.join(missing))


if __name__ == '__main__':
    main()
