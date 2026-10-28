"""Draft helpers: FBS -> ceda EmissionsSource tagging for USA ES export.

Used by :mod:`bedrock.transform.allocation.emissions_source_E` (#1134 draft).
Crosswalk-only: no nested-split shares CSV. Soils / NEU / aviation / res-com
stay on lumped ``ActivityProducedBy`` until method bake-in or shares land.
HFC speciation is logic-only (no CSV).

Crosswalk CSV (source of truth for review):
``bedrock/analysis/emissions_source_crosswalk/fbs_to_ceda_emissions_source.csv``.
ceda catalog: ``bedrock/transform/allocation/data/ceda_emissions_sources.csv``.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from bedrock.utils.emissions.gwp import GWP100_AR5_CEDA, GWP100_AR6_CEDA

DATA_DIR = Path(__file__).resolve().parent / 'data'
CROSSWALK_PATH = (
    Path(__file__).resolve().parents[2]
    / 'analysis'
    / 'emissions_source_crosswalk'
    / 'fbs_to_ceda_emissions_source.csv'
)
CEDA_SOURCES_PATH = DATA_DIR / 'ceda_emissions_sources.csv'
KEYS = ['MetaSources', 'ActivityProducedBy', 'Flowable']

UNSPECIFIED = 'HFCs and PFCs, unspecified'
_AR5: dict[str, float] = {str(k): v for k, v in GWP100_AR5_CEDA.items()}
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
_FLOWABLE_GWP_KEY = {
    'Carbon dioxide': 'CO2',
    'Methane': 'CH4_fossil',
    'CH4_fossil': 'CH4_fossil',
    'CH4_non_fossil': 'CH4_non_fossil',
    'Nitrous oxide': 'N2O',
    'Sulfur hexafluoride': 'SF6',
    'Nitrogen trifluoride': 'NF3',
    'Carbon tetrafluoride': 'CF4',
    'Hexafluoroethane': 'C2F6',
    'Perfluoropropane': 'C3F8',
    'Perfluorocyclobutane': 'c-C4F8',
}


def load_emissions_sources() -> pd.DataFrame:
    """ceda ``EmissionsSource`` catalog (code, description, category, gas)."""
    return pd.read_csv(CEDA_SOURCES_PATH, dtype=str)


def load_crosswalk() -> pd.DataFrame:
    """Read the analysis crosswalk; every ``ceda_source`` token must exist."""
    ceda_codes = set(load_emissions_sources()['code'])
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


def speciate_unspecified_hfcs(fbs: pd.DataFrame) -> pd.DataFrame:
    """Split unspecified HFC/PFC CO2e across species reported beside it (AR5)."""
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
    for _, row in fbs[unspecified].iterrows():
        key = (row['MetaSources'], row['ActivityProducedBy'])
        mix = by_sector.get((*key, row['SectorProducedBy']))
        if mix is None or mix.sum() <= 0:
            mix = by_activity.get(key)
        if mix is None or mix.sum() <= 0:
            kept.append(row.name)
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
        raise ValueError(f'Speciation changed AR5 CO2e: {before:.6e} -> {after:.6e}')
    return pd.concat([fbs[~unspecified], fbs[left], speciated], ignore_index=True)


def tag_fbs_rows(fbs: pd.DataFrame, crosswalk: pd.DataFrame) -> pd.DataFrame:
    """Attach the most specific crosswalk ``ceda_source`` / ``status`` to each row."""
    keys = (
        fbs.assign(
            ActivityProducedBy=fbs['ActivityProducedBy'].astype(object).fillna('')
        )[KEYS]
        .drop_duplicates()
        .reset_index(drop=True)
    )
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
    tagged_keys = pd.concat([keys, pd.DataFrame(rows)], axis=1)
    return fbs.assign(
        ActivityProducedBy=fbs['ActivityProducedBy'].astype(object).fillna('')
    ).merge(tagged_keys, on=KEYS, how='left', validate='m:1')


def add_co2e_columns(fbs: pd.DataFrame) -> pd.DataFrame:
    """Add ``kg``, ``co2e_ar5``, ``co2e_ar6`` from ``FlowAmount`` / ``Unit`` / ``Flowable``."""
    gwp_key = fbs['Flowable'].map(lambda f: _FLOWABLE_GWP_KEY.get(f, f))
    ar5 = gwp_key.map(lambda k: GWP100_AR5_CEDA.get(k, 1.0))  # type: ignore[arg-type]
    ar6 = gwp_key.map(lambda k: GWP100_AR6_CEDA.get(k, 1.0))  # type: ignore[arg-type]
    is_kg = fbs['Unit'] == 'kg'
    out = fbs.copy()
    out['kg'] = fbs['FlowAmount'].where(is_kg, other=float('nan'))
    out['co2e_ar5'] = fbs['FlowAmount'] * ar5.where(is_kg, 1.0)
    out['co2e_ar6'] = fbs['FlowAmount'] * ar6.where(is_kg, 1.0)
    return out
