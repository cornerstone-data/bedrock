"""Nowcasted US national Make/Use/Import tables.

Goal: build a full national Make, Use, and Import table for a series of years,
assembled from independently-sourced sections, ultimately converted to the
Cornerstone schema (after redefinitions) and RAS-rebalanced against known
controls. See https://github.com/orgs/cornerstone-data/projects/26 for the tracking board.

This module currently implements only the final-demand section of the
Use table, in purchaser (PUR) price, shaped like BEA's ``BEA_2017_Detail``
schema (commodity x ``BEA_2017_FINAL_DEMAND_CODE``). Source: the
``NIPA_FD_<year>`` FBS methods (``bedrock/transform/nipa/NIPA_FD_<year>.yaml``).

Why not just call ``FlowBySector.generateFlowBySector('NIPA_FD_<year>')`` and
pivot the result? The standard FBS pipeline aggregates every activity_set's
contribution into one schema-conforming table before returning - the
``SourceName``/activity_set identity that would tell us which official BEA
final-demand code (e.g. ``F06S00``) a row belongs to does not survive that
aggregation (``ActivityConsumedBy`` is dropped, and for most activity_sets here
it's just a human-readable label like "Federal Government Defense Investment
in Structures" even before that, not the code itself). So instead we replicate
the same per-activity_set splitting the framework does internally
(``FlowByActivity.prepare_fbs``'s ``activity_sets`` branch) ourselves, tag each
activity_set's own result with its final-demand code from
``ACTIVITY_SET_TO_FINAL_DEMAND_CODE`` (built directly off the yaml, since we
control both), and assemble the wide matrix ourselves.
"""

from __future__ import annotations

import functools

import pandas as pd

from bedrock.extract.flowbyactivity import FlowByActivity
from bedrock.transform.flowby import get_flowby_from_config
from bedrock.utils.config.common import get_catalog_info, load_yaml_dict
from bedrock.utils.taxonomy.bea.v2017_final_demand import BEA_2017_FINAL_DEMAND_CODES

# Maps each NIPA_FD_<year>.yaml activity_set name to its official BEA
# final-demand code. Several activity_sets share a code (e.g. all 6 FD_PCE*
# activity_sets -> F01000); their contributions are summed. Built directly off
# NIPA_FD_2017.yaml as of 2026-07-23 - keep in sync if activity_sets are
# renamed, added, or removed there.
ACTIVITY_SET_TO_FINAL_DEMAND_CODE: dict[str, str] = {
    'FD_PCE': 'F01000',
    'FD_PCE_tenant_landlord_durables': 'F01000',
    'FD_PCE_sporting_equipment': 'F01000',
    'FD_PCE_musical_instruments': 'F01000',
    'FD_PCE_recreational_books': 'F01000',
    'FD_PCE_luggage': 'F01000',
    'FD_IP_equipment': 'F02E00',
    'FD_IP_direct': 'F02N00',
    'FD_IP_proportional': 'F02N00',
    'FD_Structures1': 'F02R00',
    'FD_Structures2': 'F02S00',
    'FD_Structures2_used': 'F02S00',
    'FD_Gov_FedD_CE': 'F06C00',
    'FD_Gov_FedD_Equipment': 'F06E00',
    'FD_Gov_FedD_IP': 'F06N00',
    'FD_Gov_FedD_Structures': 'F06S00',
    'FD_Gov_FedND_CE': 'F07C00',
    'FD_Gov_FedND_Equipment': 'F07E00',
    'FD_Gov_FedND_IP': 'F07N00',
    'FD_Gov_FedND_Structures': 'F07S00',
    'FD_Gov_SLG_CE': 'F10C00',
    'FD_Gov_SLG_Equipment': 'F10E00',
    'FD_Gov_SLG_IP': 'F10N00',
    'FD_Gov_SLG_Structures': 'F10S00',
    # Not yet built - see issues #526 (exports/imports) and #529 (inventories).
    # 'F03000': change in private inventories
    # 'F04000': exports of goods and services
    # 'F05000': imports of goods and services
}


def _nipa_fd_activity_sets(
    year: int, download_sources_ok: bool = False
) -> list[FlowByActivity]:
    """
    Per-activity_set FlowByActivity children for ``NIPA_FD_<year>``, each still
    tagged with its originating activity_set name (via ``.full_name``) - the
    identity that's lost once the standard FlowBySector pipeline aggregates
    everything into one schema-conforming table. Mirrors what
    ``FlowByActivity.prepare_fbs()``'s ``activity_sets`` branch does internally.
    """
    method_config = load_yaml_dict(f'NIPA_FD_{year}', 'FBS')
    method_config.pop('sources_to_cache', None)
    method_config['cache'] = {}
    sources = method_config.pop('source_names')
    ((source_name, config),) = sources.items()  # BEA_NIPA is the only source
    full_config = {
        **method_config,
        'method_config_keys': set(method_config.keys()),
        **get_catalog_info(source_name),
        **config,
    }
    fba = get_flowby_from_config(
        name=source_name, config=full_config, download_sources_ok=download_sources_ok
    )
    return (
        fba.select_by_fields()
        .function_socket('clean_fba_before_activity_sets')
        .activity_sets()
    )


@functools.cache
def derive_initial_Y_pur(year: int, download_sources_ok: bool = False) -> pd.DataFrame:
    """
    Initial (pre-RAS-balanced) final-demand section of the Use table, purchaser
    price, BEA_2017_Detail schema (commodity x BEA_2017_FINAL_DEMAND_CODE).
    Built from ``NIPA_FD_<year>`` FBS output, one activity_set at a time - see
    module docstring for why.

    Columns not yet sourced (F03000 change in private inventories, F04000
    exports, F05000 imports - see issues #529 and #526) are present but
    all-zero.
    """
    contributions: dict[str, pd.Series] = {}
    for child in _nipa_fd_activity_sets(year, download_sources_ok):
        activity_set = child.full_name.rsplit('.', 1)[-1]
        code = ACTIVITY_SET_TO_FINAL_DEMAND_CODE.get(activity_set)
        if code is None:
            raise KeyError(
                f'Activity set {activity_set!r} (from NIPA_FD_{year}.yaml) has no '
                'entry in ACTIVITY_SET_TO_FINAL_DEMAND_CODE - add one before using '
                'it here.'
            )
        fbs = child.prepare_fbs(download_sources_ok=download_sources_ok)
        series = fbs.groupby('SectorProducedBy')['FlowAmount'].sum()
        contributions[code] = (
            contributions[code].add(series, fill_value=0)
            if code in contributions
            else series
        )

    y = pd.DataFrame(contributions).reindex(
        columns=BEA_2017_FINAL_DEMAND_CODES, fill_value=0.0
    )
    y = y.fillna(0.0).sort_index()
    y.index.name = 'commodity'
    y.columns.name = 'final_demand_code'
    return y
