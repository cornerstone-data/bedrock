"""Inventory the GHG FBS sources and match them to ceda's emissions sources.

ceda labels emissions with 123 ``EmissionsSource`` members, each one GHGI
category for one gas (``co2_cement_production``, ``hfc_134a_foams``, ...).
The bedrock GHG FBS keeps only ``MetaSources``, one per FBS activity set
(``UMD_GHGIA_T_2_S1.direct``), and drops the GHGI line items once they are
attributed. This module lists, for each ``MetaSources`` × gas in the FBS:

- the GHGI activities the method YAML selects into that activity set,
- the ceda sources those activities match by name and gas,
- whether the match is usable as it stands.

``status`` takes one of these values:

- ``one_to_one``: one ceda source. Several MetaSources may share it.
- ``separable_by_sector``: several ceda sources in a ``direct`` set whose
  activities map to disjoint NAICS sectors, so the FBS sector column already
  tells them apart.
- ``bundled``: several ceda sources that the FBS cannot tell apart. These
  need the GHGI activity carried through the FBS.
- ``split_needed``: one activity that matches several ceda sources of the
  same gas, so ceda is finer than the inventory.
- ``no_species``: flows already in CO2e with no gas species.
- ``unmatched``: no ceda source matched by name.

Matching is by tokens only, so this is a first pass for review, not a
crosswalk.

Usage::

    python -m bedrock.analysis.emissions_source_crosswalk.inventory_fbs_sources \
        [--method GHG_national_Cornerstone_2024] [--ceda-root ../ceda]
"""

from __future__ import annotations

import argparse
import ast
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pandas as pd

from bedrock.utils.emissions.gwp import GWP100_AR6_CEDA

OUTPUT_DIR = Path(__file__).resolve().parent / 'output'
MATCH_THRESHOLD = 0.75


def main() -> None:
    parser = argparse.ArgumentParser(description=(__doc__ or '').splitlines()[0])
    parser.add_argument(
        '--method',
        default=None,
        help='GHG FBS method stem; defaults to the one the active config loads.',
    )
    parser.add_argument(
        '--ceda-root',
        type=Path,
        default=Path(__file__).resolve().parents[3].parent / 'ceda',
        help='Path to a ceda checkout (reads its EmissionsSource enum).',
    )
    args = parser.parse_args()

    method = args.method or _method_for_active_config()
    inventory, unmatched_ceda = build_inventory(method, args.ceda_root)

    OUTPUT_DIR.mkdir(exist_ok=True)
    inventory.to_csv(OUTPUT_DIR / f'{method}_source_inventory.csv', index=False)
    unmatched_ceda.to_csv(OUTPUT_DIR / f'{method}_ceda_unmatched.csv', index=False)
    _print_summary(method, inventory, unmatched_ceda)


def build_inventory(method: str, ceda_root: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (MetaSources × gas inventory, ceda sources nothing matched)."""
    ceda = load_ceda_sources(ceda_root)
    activity_sets = flatten_activity_sets(method)
    totals = _fbs_totals(method)

    sets_by_meta = {s.meta_source: s for s in activity_sets}
    rows: list[dict[str, Any]] = []
    for key, amounts in totals.iterrows():
        meta, flowable = cast(tuple[str, str], key)
        gas = _FBS_TO_CEDA_GAS.get(str(flowable))
        aset = sets_by_meta.get(str(meta))
        row: dict[str, Any] = {
            'meta_source': meta,
            'flowable': flowable,
            'ceda_gas': gas,
            'kg': amounts['kg'],
            'Mt_CO2e_AR6': amounts['co2e'] / 1e9,
            'in_method_yaml': aset is not None,
            'attribution_method': aset.attribution_method if aset else None,
            'activities': '; '.join(aset.activities) if aset else '',
        }
        if gas is None:
            row.update(status='no_species', ceda_sources='')
        elif aset is None:
            row.update(status='unmatched', ceda_sources='')
        else:
            row.update(_classify(aset, gas, ceda))
        rows.append(row)

    inventory = pd.DataFrame(rows).sort_values('Mt_CO2e_AR6', ascending=False)
    matched = {
        code
        for codes in inventory['ceda_sources']
        for code in str(codes).split('; ')
        if code
    }
    unmatched_ceda = ceda[~ceda['code'].isin(matched)].reset_index(drop=True)
    return inventory.reset_index(drop=True), unmatched_ceda


@dataclass(frozen=True)
class ActivitySet:
    meta_source: str
    source_name: str
    attribution_method: str | None
    activities: tuple[str, ...]  # FBA PrimaryActivity names; empty = whole table
    context: str  # activity-set name and the YAML comment on its source


def flatten_activity_sets(method: str) -> list[ActivitySet]:
    """One entry per top-level FBS activity set (the ``MetaSources`` grain)."""
    from bedrock.utils.config.common import load_yaml_dict  # noqa: PLC0415

    spec = load_yaml_dict(method, flowbytype='FBS')
    comments = _source_comments(method)
    out: list[ActivitySet] = []
    for source_name, source in spec['source_names'].items():
        source_method = source.get('attribution_method')
        sets = source.get('activity_sets') or {}
        if not sets:
            out.append(
                ActivitySet(
                    meta_source=source_name,
                    source_name=source_name,
                    attribution_method=source_method,
                    activities=(),
                    context=comments.get(source_name, ''),
                )
            )
            continue
        for set_name, aset in sets.items():
            selected = (aset.get('selection_fields') or {}).get('PrimaryActivity')
            out.append(
                ActivitySet(
                    meta_source=f'{source_name}.{set_name}',
                    source_name=source_name,
                    attribution_method=aset.get('attribution_method', source_method),
                    activities=_activity_names(selected),
                    context=f'{set_name} {comments.get(source_name, "")}',
                )
            )
    return out


def load_ceda_sources(ceda_root: Path) -> pd.DataFrame:
    """Read ceda's ``EmissionsSource`` members without importing ceda."""
    path = ceda_root / 'ceda' / 'ceda_usa' / 'allocation' / 'constants.py'
    tree = ast.parse(path.read_text(encoding='utf-8'))
    cls = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == 'EmissionsSource'
    )
    rows = []
    for stmt in cls.body:
        if not (isinstance(stmt, ast.Assign) and isinstance(stmt.value, ast.Tuple)):
            continue
        code, description, category, gas = stmt.value.elts
        rows.append(
            {
                'code': ast.literal_eval(code),
                'description': ast.literal_eval(description),
                'category': ast.unparse(category).rsplit('.', 1)[-1],
                'gas': ast.literal_eval(gas),
            }
        )
    return pd.DataFrame(rows)


# --- helpers ---------------------------------------------------------------

_FBS_TO_CEDA_GAS: dict[str, str | None] = {
    'Carbon dioxide': 'CO2',
    'Methane': 'CH4',
    'Nitrous oxide': 'N2O',
    'Sulfur hexafluoride': 'SF6',
    'Nitrogen trifluoride': 'NF3',
    'Carbon tetrafluoride': 'CF4',
    'Hexafluoroethane': 'C2F6',
    'Perfluoropropane': 'C3F8',
    'Perfluorocyclobutane': 'C4F8',
    'HFC-23': 'HFC-23',
    'HFC-32': 'HFC-32',
    'HFC-125': 'HFC-125',
    'HFC-134a': 'HFC-134a',
    'HFC-143a': 'HFC-143a',
    'HFC-227ea': 'HFC-227ea',  # not a ceda gas; shows up as unmatched
    'HFC-236fa': 'HFC-236fa',
    'HFCs and PFCs, unspecified': None,
}

# FBS gas → GWP key, for ranking rows only (CH4 taken as fossil).
_GWP_KEY = {'Methane': 'CH4_fossil', 'Perfluorocyclobutane': 'c-C4F8'}

_STOPWORDS = {
    'and',
    'of',
    'the',
    'from',
    'for',
    'in',
    'all',
    'activities',
    'direct',
    'emissions',
    'mimics',
    'note',
    'does',
    'not',
    'have',
    'totals',
    'table',
    'umd',
    'ghgia',
    'epa',
    'ghgi',
    'u',
    's',
    'us',
    'ghg',
    'selected',
    'other',
    'others',
    'co2',
    'ch4',
    'n2o',
    'sf6',
    'nf3',
    'hfc',
    'hfcs',
    'pfc',
    'pfcs',
    'cf4',
    'c2f6',
    'c3f8',
    'c4f8',
    '23',
    '32',
    '125',
    '134a',
    '143a',
    '236fa',
}
_SYNONYMS = {
    'mobile': ['transportation', 'transport'],
    'transportation': ['transportation', 'transport'],
    'cars': ['cars', 'motor'],
    'trucks': ['trucks', 'motor'],
    'buses': ['buses', 'motor'],
    'motorcycles': ['motorcycles', 'motor'],
    'electric': ['electric', 'electricity', 'electrical', 'generation'],
    'transmission': ['transmission', 'distribution'],
    'petroleum': ['petroleum', 'petrol'],
    'liming': ['liming', 'agricultural', 'soils'],
    'petrochemicals': ['petrochemical', 'production'],
    'neu': ['non', 'energy', 'use', 'fuels'],
    'electronics': ['electronics', 'semiconductor', 'manufacture'],
    'magnesium': ['magnesium', 'processing'],
    'substancesd': ['substances'],  # typo in ceda's descriptions
    'diesel': ['distillate'],
    'ng': ['natural', 'gas'],
    'ods': ['substitution', 'ozone', 'depleting', 'substances'],
    'jet': ['jet', 'fuel'],
    'lpg': ['lpg'],
    'manufacturing': ['industrial', 'manufacturing'],
}


def _classify(aset: ActivitySet, gas: str, ceda: pd.DataFrame) -> dict[str, Any]:
    pool = ceda[ceda['gas'] == gas]
    context = _tokens(aset.context)
    names = aset.activities or ('',)
    per_activity = {name: _matches(_tokens(name) | context, pool) for name in names}
    found = sorted({code for codes in per_activity.values() for code in codes})
    if not found:
        return {'status': 'unmatched', 'ceda_sources': ''}
    if len(found) == 1:
        return {'status': 'one_to_one', 'ceda_sources': found[0]}
    if any(len(codes) > 1 for codes in per_activity.values()):
        status = 'split_needed'
    elif aset.attribution_method == 'direct' and _sectors_disjoint(
        [a for a, codes in per_activity.items() if codes]
    ):
        status = 'separable_by_sector'
    else:
        status = 'bundled'
    return {'status': status, 'ceda_sources': '; '.join(found)}


def _matches(tokens: set[str], pool: pd.DataFrame) -> list[str]:
    """ceda codes whose description tokens are mostly present in ``tokens``."""
    out = []
    for code, description in zip(pool['code'], pool['description']):
        wanted = _tokens(description)
        if wanted and len(wanted & tokens) / len(wanted) >= MATCH_THRESHOLD:
            out.append(code)
    return out


def _tokens(text: str) -> set[str]:
    words = re.sub(r'[^a-z0-9]+', ' ', text.lower().replace('&', ' and ')).split()
    out: set[str] = set()
    for word in words:
        out.update(_SYNONYMS.get(word, [word]))
    return out - _STOPWORDS


def _sectors_disjoint(activities: list[str]) -> bool:
    """True when no two activities map to nested or equal NAICS codes."""
    from bedrock.utils.mapping.sectormapping import (  # noqa: PLC0415
        get_activitytosector_mapping,
    )

    mapping = get_activitytosector_mapping('UMD_GHGIA')
    sectors = {
        a: set(mapping.loc[mapping['Activity'] == a, 'Sector'].astype(str))
        for a in activities
    }
    if any(not s for s in sectors.values()):
        return False
    names = list(sectors)
    for i, a in enumerate(names):
        for b in names[i + 1 :]:
            for x in sectors[a]:
                for y in sectors[b]:
                    if x.startswith(y) or y.startswith(x):
                        return False
    return True


def _activity_names(selected: Any) -> tuple[str, ...]:
    if selected is None:
        return ()
    if isinstance(selected, str):
        return (selected,)
    if isinstance(selected, dict):
        return tuple(str(k) for k in selected)
    return tuple(str(s) for s in selected)


def _source_comments(method: str) -> dict[str, str]:
    """``source_names`` keys → trailing YAML comment (lost by the YAML parser)."""
    from bedrock.utils.config import settings  # noqa: PLC0415
    from bedrock.utils.config.settings import return_folder_path  # noqa: PLC0415

    folder = return_folder_path(settings.transformpath, f'{method}.yaml')
    text = (Path(folder) / f'{method}.yaml').read_text(encoding='utf-8')
    return {
        m.group(1): m.group(2).strip()
        for m in re.finditer(r'^ {2}(\w+):[^#\n]*#(.*)$', text, re.M)
    }


def _fbs_totals(method: str) -> pd.DataFrame:
    fbs = _load_fbs(method)
    gwp_table: dict[str, float] = {str(k): v for k, v in GWP100_AR6_CEDA.items()}
    gwp = fbs['Flowable'].map(
        lambda f: gwp_table.get(_GWP_KEY.get(f, _FBS_TO_CEDA_GAS.get(f) or f), 1.0)
    )
    co2e = fbs['FlowAmount'] * gwp.where(fbs['Unit'] == 'kg', 1.0)
    return (
        fbs.assign(kg=fbs['FlowAmount'], co2e=co2e)
        .groupby(['MetaSources', 'Flowable'], observed=True)[['kg', 'co2e']]
        .sum()
    )


def _load_fbs(method: str) -> pd.DataFrame:
    """Newest local parquet of this stem, else the GCS loader."""
    from bedrock.transform.allocation.derived import (  # noqa: PLC0415
        _load_cornerstone_ghg_fbs_from_gcs,
    )
    from bedrock.utils.config.settings import FBS_DIR  # noqa: PLC0415

    local = sorted(
        FBS_DIR.glob(f'{method}_v*.parquet'), key=lambda p: p.stat().st_mtime
    )
    if local:
        print(f'FBS: {local[-1].name}')
        return pd.read_parquet(local[-1])
    return _load_cornerstone_ghg_fbs_from_gcs(base_name=method)


def _method_for_active_config() -> str:
    from bedrock.transform.allocation.derived import (  # noqa: PLC0415
        _select_cornerstone_ghg_fbs_base_name,
    )

    return _select_cornerstone_ghg_fbs_base_name()


def _print_summary(
    method: str, inventory: pd.DataFrame, unmatched_ceda: pd.DataFrame
) -> None:
    print(f'\n{method}: {len(inventory)} MetaSources × gas rows')
    by_status = inventory.groupby('status')['Mt_CO2e_AR6'].agg(['count', 'sum'])
    by_status['share'] = by_status['sum'] / by_status['sum'].sum()
    print(by_status.sort_values('sum', ascending=False).round(3).to_string())
    stale = inventory.loc[~inventory['in_method_yaml'], 'meta_source'].unique()
    if len(stale):
        print(f'\nIn the FBS but not the method YAML: {sorted(stale)}')
    print(f'\nceda sources with no match: {len(unmatched_ceda)} of 123')
    print(f'Wrote {OUTPUT_DIR}')


if __name__ == '__main__':
    main()
