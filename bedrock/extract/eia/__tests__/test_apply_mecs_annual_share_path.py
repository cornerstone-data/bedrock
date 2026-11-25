"""#1210 / #1318 MECS annual share path (clean_fba_w_sec, after parent-incomplete-child)."""

from __future__ import annotations

import inspect
from collections.abc import Sequence
from typing import Any

import pandas as pd
import pytest

from bedrock.extract.eia import EIA_MECS as mecs_mod
from bedrock.extract.eia.EIA_MECS import (
    GHGRP_FILL_MARKER,
    apply_mecs_annual_share_path,
)
from bedrock.extract.flowbyactivity import FlowByActivity

# 2022: 325 = 100 with children 325110 = 40, 325190 = 30 -> remainders 30, 40, 30.
ANCHOR_RAW = [('325', 100.0, None), ('325110', 40.0, None), ('325190', 30.0, None)]
# 2018: 325 = 100 with children 325110 = 30, 325190 = 50 -> remainders 20, 30, 50.
PRIOR_RAW = [('325', 100.0, None), ('325110', 30.0, None), ('325190', 50.0, None)]
# Spread is 5 (% RSE) in the fixtures: variance = (0.05 x raw)^2, and a
# remainder adds its children's variances.
ANCHOR_VAR = {'325': 25.0 + 4.0 + 2.25, '325110': 4.0, '325190': 2.25}
PRIOR_VAR = {'325': 25.0 + 2.25 + 6.25, '325110': 2.25, '325190': 6.25}


def _raw(rows: Sequence[tuple[str, float, str | None]], flow: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                'ActivityConsumedBy': code,
                'FlowName': flow,
                'FlowAmount': amount,
                'Suppressed': supp,
                'Spread': 5.0,
                'Description': 'Table 2.1',
                'Location': '00000',
                'Unit': 'TBtu',
            }
            for code, amount, supp in rows
        ]
    )


def _mapped(
    remainders: dict[str, float],
    flow: str,
    inventory_year: int,
    sectors: dict[str, list[str]] | None = None,
) -> FlowByActivity:
    '''Anchor rows as clean_fba_w_sec sees them: 325's remainder fans out to two
    sectors under one group_id.'''
    if sectors is None:
        sectors = {'325': ['325120', '325130']}
    rows = [
        {
            'group_id': gid,
            'group_total': remainders[code],
            'ActivityConsumedBy': code,
            'Flowable': flow,
            'FlowAmount': remainders[code] / len(sectors.get(code, [code])),
            'SectorConsumedBy': sector,
        }
        for gid, code in enumerate(remainders)
        for sector in sectors.get(code, [code])
    ]
    return FlowByActivity(
        pd.DataFrame(rows),
        full_name='EIA_MECS_Energy',
        config={
            'year': 2022,
            'target_schema_year': 2017,
            'standardize_units': False,
            'clean_parameter': {
                'inventory_year': inventory_year,
                'anchor_year': 2022,
                'prior_year': 2018,
            },
            'clean_source': {
                'EIA_MECS_Energy': {
                    'year': 2018,
                    'selection_fields': {
                        'Location': '00000',
                        'Description': 'Table 2.1',
                        'FlowName': {
                            'Other': 'Petroleum',
                            'HGL': 'HGL',
                            'Hydrogen': 'Hydrogen',
                        },
                    },
                }
            },
        },
    )


def _patch_loads(monkeypatch: pytest.MonkeyPatch, raw: dict[int, pd.DataFrame]) -> None:
    def _return_fba(
        cls: Any, full_name: str, year: int, config: dict[str, Any], **_k: Any
    ) -> FlowByActivity:
        return FlowByActivity(raw[year].copy(), full_name=full_name, config=config)

    monkeypatch.setattr(FlowByActivity, 'return_FBA', classmethod(_return_fba))
    monkeypatch.setattr(
        'bedrock.extract.eia.EIA_MECS.fill_suppressed_mecs_feedstock_with_ghgrp',
        lambda fba, **_k: fba,
    )
    monkeypatch.setattr(
        'bedrock.extract.eia.EIA_MECS.get_catalog_info',
        lambda name: {
            'activity_schema': {'naics': {'by_year': {2018: 2017, 2022: 2022}}}
        },
    )


def _group_totals(out: pd.DataFrame) -> dict[str, float]:
    return dict(
        out.drop_duplicates('group_id')
        .set_index('ActivityConsumedBy')['group_total']
        .astype(float)
    )


def _path_weight(sa: float, sp: float, _va: float, vp: float, w: float) -> float:
    '''Expected weight: lambda from the 2018 share variance and the share gap, times w.'''
    lam = max(0.0, 1.0 - vp / (sp - sa) ** 2) if sp != sa else 0.0
    if sa > 0 and sp > 0:
        return float(sa * (sp / sa) ** (lam * w))
    return sa + lam * w * (sp - sa)


def _expected(weights: dict[str, float], total: float = 100.0) -> dict[str, float]:
    return {c: total * v / sum(weights.values()) for c, v in weights.items()}


@pytest.mark.parametrize('inventory_year', [2017, 2018, 2019, 2020, 2021])
def test_between_surveys_follows_rse_weighted_path(
    monkeypatch: pytest.MonkeyPatch, inventory_year: int
) -> None:
    _patch_loads(
        monkeypatch, {2022: _raw(ANCHOR_RAW, 'HGL'), 2018: _raw(PRIOR_RAW, 'HGL')}
    )
    fba = _mapped({'325': 30.0, '325110': 40.0, '325190': 30.0}, 'HGL', inventory_year)
    out = apply_mecs_annual_share_path(fba)
    totals = _group_totals(out)
    w = min(2022 - inventory_year, 4) / 4
    share_a = {'325': 0.3, '325110': 0.4, '325190': 0.3}
    share_p = {'325': 0.2, '325110': 0.3, '325190': 0.5}
    weights = {
        c: _path_weight(
            share_a[c], share_p[c], ANCHOR_VAR[c] / 1e4, PRIOR_VAR[c] / 1e4, w
        )
        for c in share_a
    }
    assert totals == pytest.approx(_expected(weights))
    assert sum(totals.values()) == pytest.approx(100.0)
    assert min(totals.values()) >= 0
    fanned = out[out['ActivityConsumedBy'] == '325']
    assert list(fanned['FlowAmount']) == pytest.approx([totals['325'] / 2] * 2)


def test_anchor_year_keeps_2022_shares_when_both_published(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_loads(
        monkeypatch, {2022: _raw(ANCHOR_RAW, 'HGL'), 2018: _raw(PRIOR_RAW, 'HGL')}
    )
    fba = _mapped({'325': 30.0, '325110': 40.0, '325190': 30.0}, 'HGL', 2023)
    totals = _group_totals(apply_mecs_annual_share_path(fba))
    assert totals == pytest.approx({'325': 30.0, '325110': 40.0, '325190': 30.0})


def test_prior_unpublished_keeps_anchor_share(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prior = [('325', 100.0, None), ('325110', 30.0, None), ('325190', 50.0, 'D')]
    _patch_loads(monkeypatch, {2022: _raw(ANCHOR_RAW, 'HGL'), 2018: _raw(prior, 'HGL')})
    fba = _mapped({'325': 30.0, '325110': 40.0, '325190': 30.0}, 'HGL', 2018)
    totals = _group_totals(apply_mecs_annual_share_path(fba))
    # 325190 unpublished in 2018: children differ, but anchor has more published
    # kids so 325 still paths; 325190 keeps the anchor share.
    weights = {
        '325': _path_weight(
            0.3, 0.7, ANCHOR_VAR['325'] / 1e4, (25.0 + 2.25) / 1e4, 1.0
        ),
        '325110': _path_weight(0.4, 0.3, 4.0 / 1e4, 2.25 / 1e4, 1.0),
        '325190': 0.3,
    }
    assert totals == pytest.approx(_expected(weights))


def test_ghgrp_filled_prior_is_unpublished(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prior = [
        ('325', 100.0, None),
        ('325110', 30.0, None),
        ('325190', 50.0, GHGRP_FILL_MARKER),
    ]
    _patch_loads(monkeypatch, {2022: _raw(ANCHOR_RAW, 'HGL'), 2018: _raw(prior, 'HGL')})
    fba = _mapped({'325': 30.0, '325110': 40.0, '325190': 30.0}, 'HGL', 2018)
    totals = _group_totals(apply_mecs_annual_share_path(fba))
    # Fill stays in parent/child: prior 325 remainder is 20 (not 70). 325190
    # stays unpublished for the level rule → anchor share 0.3.
    weights = {
        '325': _path_weight(
            0.3, 0.2, ANCHOR_VAR['325'] / 1e4, (25.0 + 2.25) / 1e4, 1.0
        ),
        '325110': _path_weight(0.4, 0.3, 4.0 / 1e4, 2.25 / 1e4, 1.0),
        '325190': 0.3,
    }
    assert totals == pytest.approx(_expected(weights))


def test_ghgrp_filled_child_not_left_in_parent_remainder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Plastics-shaped (#1318 Wes 1): 2018 child is GHGRP-filled; mapped still
    # has the child. Fill must subtract from the parent so pre-renorm weights
    # sum to ~1 (not ~1.3 with the child counted twice).
    anchor = [
        ('325', 100.0, None),
        ('325110', 40.0, None),
        ('325211', 30.0, None),
    ]
    prior = [
        ('325', 100.0, None),
        ('325110', 40.0, None),
        ('325211', 30.0, GHGRP_FILL_MARKER),
    ]
    _patch_loads(monkeypatch, {2022: _raw(anchor, 'HGL'), 2018: _raw(prior, 'HGL')})
    fba = _mapped(
        {'325': 30.0, '325110': 40.0, '325211': 30.0}, 'HGL', 2018, sectors={}
    )
    totals = _group_totals(apply_mecs_annual_share_path(fba))
    # Prior pieces after split: 325=30, 325110=40, 325211=30 (fill kept).
    # Shares match the anchor, so path weight = sa; 325211 unpublished → 0.3.
    # If the fill were dropped, prior 325 would be 60 and weights would sum > 1.
    weights = {'325': 0.3, '325110': 0.4, '325211': 0.3}
    assert sum(weights.values()) == pytest.approx(1.0)
    assert totals == pytest.approx(_expected(weights))
    assert sum(totals.values()) == pytest.approx(100.0)


def test_anchor_unpublished_uses_prior_share_in_2022(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    anchor = [('325', 100.0, None), ('325110', 40.0, 'D'), ('325190', 30.0, None)]
    _patch_loads(monkeypatch, {2022: _raw(anchor, 'HGL'), 2018: _raw(PRIOR_RAW, 'HGL')})
    # Mapped still carries a fill-like amount for 325110; path must not keep it.
    fba = _mapped({'325': 30.0, '325110': 40.0, '325190': 30.0}, 'HGL', 2022)
    totals = _group_totals(apply_mecs_annual_share_path(fba))
    # 325110 is D in the anchor but still a mapped row, so parent 325 does not
    # take prior_finer. 325110 prior 0.3 + 325/325190 anchor 0.3 each → renorm
    # by 0.9 so 325110 is ~33.3 of the mapped fuel total (not the D fill 40).
    # Cap does not bind: prior claim ~33.3 <= parent PIC room 70.
    assert totals['325110'] == pytest.approx(100.0 * 0.3 / 0.9)
    assert totals['325110'] < 40.0
    assert sum(totals.values()) == pytest.approx(100.0)


def test_prior_level_unpublished_capped_at_parent_pic_room(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Wes-shaped: published parent 100, published child 70, D child fill 40
    # (PIC remainder after subtracting the fill is 0). Room must be
    # remainder + claimant fills = 30 (published parent − published kids),
    # not remainder alone (0 would wipe the child). Prior 50 wants more
    # than room; cap then renorm; fuel preserved.
    anchor = [
        ('325', 100.0, None),
        ('325110', 70.0, None),
        ('325120', 40.0, 'D'),
    ]
    prior = [
        ('325', 100.0, None),
        ('325110', 50.0, None),
        ('325120', 50.0, None),
    ]
    _patch_loads(monkeypatch, {2022: _raw(anchor, 'HGL'), 2018: _raw(prior, 'HGL')})
    # Mapped fill for the D child is positive; parent remainder row is 0 so
    # a remainder-only room would zero the child (the 0a49f2f bug).
    fba = _mapped(
        {'325': 0.0, '325110': 70.0, '325120': 40.0},
        'HGL',
        2022,
        sectors={},
    )
    totals = _group_totals(apply_mecs_annual_share_path(fba))
    assert totals['325120'] == pytest.approx(30.0)
    assert totals['325120'] > 0.0
    assert totals['325120'] <= 30.0
    assert totals['325'] + totals['325110'] + totals['325120'] == pytest.approx(110.0)
    assert sum(totals.values()) == pytest.approx(110.0)
    # No hard-coded NAICS in the path (cap is tree-general).
    src = '\n'.join(
        inspect.getsource(fn)
        for fn in (
            mecs_mod.apply_mecs_annual_share_path,
            mecs_mod._mecs_path_factors,
            mecs_mod._cap_prior_level_unpublished,
        )
    )
    assert '325120' not in src


def test_children_differ_uses_prior_level(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # 2022 drops 3121 (Q); mapped 312 holds the parent total. 2018 publishes
    # 3121 and a 312 parent; path folds 3121 into 312's prior amount (52).
    anchor = [('312', 204.0, None), ('325', 100.0, None)]
    prior = [
        ('312', 52.0, None),
        ('3121', 48.0, None),
        ('325', 100.0, None),
    ]
    _patch_loads(monkeypatch, {2022: _raw(anchor, 'HGL'), 2018: _raw(prior, 'HGL')})
    fba = _mapped({'312': 204.0, '325': 100.0}, 'HGL', 2018, sectors={})
    totals = _group_totals(apply_mecs_annual_share_path(fba))
    # 312 takes the prior level (remainder + 3121 = 52); 325 still paths, so
    # renorm leaves 312 near 52/152 of the mapped fuel total.
    assert totals['312'] == pytest.approx(304.0 * 52.0 / 152.0, rel=0.02)
    assert totals['312'] < 204.0


def test_children_differ_keeps_anchor_when_anchor_has_more_kids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Anchor publishes an extra child; prior does not — do not freeze on prior.
    anchor = [
        ('325', 100.0, None),
        ('325110', 40.0, None),
        ('325190', 30.0, None),
    ]
    prior = [('325', 100.0, None), ('325110', 30.0, None)]
    _patch_loads(monkeypatch, {2022: _raw(anchor, 'HGL'), 2018: _raw(prior, 'HGL')})
    fba = _mapped({'325': 30.0, '325110': 40.0, '325190': 30.0}, 'HGL', 2024)
    totals = _group_totals(apply_mecs_annual_share_path(fba))
    assert totals == pytest.approx({'325': 30.0, '325110': 40.0, '325190': 30.0})


def test_children_differ_keeps_anchor_when_prior_extra_kid_is_mapped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Prior publishes an extra child that the mapped set already carries (* / 0
    # row) — not a fold into the parent — so 2024 keeps the 2022 shares
    # (HGL chemical 325).
    anchor = [
        ('325', 100.0, None),
        ('325110', 50.0, None),
        ('325190', 30.0, None),
    ]
    prior = [
        ('325', 100.0, None),
        ('325110', 40.0, None),
        ('325190', 20.0, None),
        ('325199', 10.0, None),
    ]
    _patch_loads(monkeypatch, {2022: _raw(anchor, 'HGL'), 2018: _raw(prior, 'HGL')})
    fba = _mapped(
        {'325': 20.0, '325110': 50.0, '325190': 30.0, '325199': 0.0},
        'HGL',
        2024,
        sectors={},
    )
    totals = _group_totals(apply_mecs_annual_share_path(fba))
    assert totals == pytest.approx(
        {'325': 20.0, '325110': 50.0, '325190': 30.0, '325199': 0.0}
    )


def test_star_anchor_paths_not_flat_prior_level(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Wes #1318 (5): 2022 * is 0.25 with RSE 100%, not unpublished → RSE path.
    anchor = [
        ('335', 100.0, None),
        ('3351', 8.0, None),
        ('3359', 0.25, '*'),
    ]
    prior = [
        ('335', 100.0, None),
        ('3351', 20.0, None),
        ('3359', 8.0, None),
    ]
    _patch_loads(monkeypatch, {2022: _raw(anchor, 'HGL'), 2018: _raw(prior, 'HGL')})
    # Mapped remainders after PIC: 335=91.75, 3351=8, 3359=0.25.
    mapped = {'335': 91.75, '3351': 8.0, '3359': 0.25}
    fba = _mapped(mapped, 'HGL', 2018, sectors={})
    totals = _group_totals(apply_mecs_annual_share_path(fba))
    a_sum = 100.0
    p_sum = 100.0
    # * variance = (1.0 * 0.25)^2; remainder vars include children.
    weights = {
        '335': _path_weight(
            0.9175,
            0.72,
            (25.0 + (0.05 * 8.0) ** 2 + 0.25**2) / a_sum**2,
            (25.0 + (0.05 * 20.0) ** 2 + (0.05 * 8.0) ** 2) / p_sum**2,
            1.0,
        ),
        '3351': _path_weight(
            0.08, 0.2, (0.05 * 8.0) ** 2 / a_sum**2, (0.05 * 20.0) ** 2 / p_sum**2, 1.0
        ),
        '3359': _path_weight(
            0.0025, 0.08, 0.25**2 / a_sum**2, (0.05 * 8.0) ** 2 / p_sum**2, 1.0
        ),
    }
    assert totals == pytest.approx(_expected(weights, total=100.0))
    # Not frozen on the 2018 share (would be 8 after renorm to fuel total).
    assert totals['3359'] != pytest.approx(8.0)


def test_star_prior_paths_not_flat_anchor_level(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Reverse of Wes 335: 2018 * / 2022 published → path, not forever-2022.
    anchor = [
        ('325', 100.0, None),
        ('325212', 8.0, None),
        ('325211', 30.0, None),
    ]
    prior = [
        ('325', 100.0, None),
        ('325212', 0.25, '*'),
        ('325211', 30.0, None),
    ]
    _patch_loads(monkeypatch, {2022: _raw(anchor, 'HGL'), 2018: _raw(prior, 'HGL')})
    mapped = {'325': 62.0, '325212': 8.0, '325211': 30.0}
    fba = _mapped(mapped, 'HGL', 2018, sectors={})
    totals = _group_totals(apply_mecs_annual_share_path(fba))
    a_sum = 100.0
    p_sum = 100.0
    weights = {
        '325': _path_weight(
            0.62,
            0.6975,
            (25.0 + (0.05 * 8.0) ** 2 + (0.05 * 30.0) ** 2) / a_sum**2,
            (25.0 + 0.25**2 + (0.05 * 30.0) ** 2) / p_sum**2,
            1.0,
        ),
        '325212': _path_weight(
            0.08, 0.0025, (0.05 * 8.0) ** 2 / a_sum**2, 0.25**2 / p_sum**2, 1.0
        ),
        '325211': _path_weight(
            0.3, 0.3, (0.05 * 30.0) ** 2 / a_sum**2, (0.05 * 30.0) ** 2 / p_sum**2, 1.0
        ),
    }
    assert totals == pytest.approx(_expected(weights, total=100.0))
    assert totals['325212'] != pytest.approx(8.0)


def test_petroleum_2022_weights_stay_on_other_not_hydrogen(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Wes #1318 (2): H2 must not change 2022 petroleum weights. * Other is
    # published (0.25); published H2 stays compare-only.
    anchor_other = [('325', 100.0, None), ('325110', 40.0, None), ('325190', 0.25, '*')]
    anchor_h2 = [('325190', 21.0, None)]
    prior = [('325', 100.0, None), ('325110', 30.0, None), ('325190', 50.0, None)]
    anchor = pd.concat(
        [_raw(anchor_other, 'Other'), _raw(anchor_h2, 'Hydrogen')], ignore_index=True
    )
    _patch_loads(monkeypatch, {2022: anchor, 2018: _raw(prior, 'Other')})
    fba = _mapped(
        {'325': 30.0, '325110': 40.0, '325190': 0.25}, 'Petroleum', 2022, sectors={}
    )
    totals = _group_totals(apply_mecs_annual_share_path(fba))
    # * Other published → 2022 Other shares; H2 does not inflate 325190.
    assert totals == pytest.approx(
        {'325': 30.0, '325110': 40.0, '325190': 0.25}, abs=1e-9
    )


def test_petroleum_hydrogen_compare_sets_lambda_path_keeps_other_shares(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # 2022 Other shares unchanged by H2; path years use Other+H2 only for lambda.
    anchor_other = [
        ('325', 100.0, None),
        ('325110', 40.0, None),
        ('325190', 30.0, None),
    ]
    anchor_h2 = [('325190', 21.0, None)]
    prior = [('325', 100.0, None), ('325110', 30.0, None), ('325190', 50.0, None)]
    anchor = pd.concat(
        [_raw(anchor_other, 'Other'), _raw(anchor_h2, 'Hydrogen')], ignore_index=True
    )
    _patch_loads(monkeypatch, {2022: anchor, 2018: _raw(prior, 'Other')})
    mapped = {'325': 30.0, '325110': 40.0, '325190': 30.0}
    at_2022 = _group_totals(
        apply_mecs_annual_share_path(_mapped(mapped, 'Petroleum', 2022, sectors={}))
    )
    assert at_2022 == pytest.approx(mapped)
    # Other remainders 30/40/30; compare adds H2 21 on 325190 → sum 121.
    compare_sum = 121.0
    prior_sum = 100.0
    weights: dict[str, float] = {}
    for code, sa, sp, compare_amt, prior_var in (
        ('325', 0.3, 0.2, 30.0, 25.0 + 2.25 + 6.25),
        ('325110', 0.4, 0.3, 40.0, 2.25),
        ('325190', 0.3, 0.5, 51.0, 6.25),
    ):
        sa_c = compare_amt / compare_sum
        noise = prior_var / prior_sum**2
        lam = max(0.0, 1.0 - noise / (sp - sa_c) ** 2)
        weights[code] = float(sa * (sp / sa) ** lam)
    totals = _group_totals(
        apply_mecs_annual_share_path(_mapped(mapped, 'Petroleum', 2018, sectors={}))
    )
    assert totals == pytest.approx(_expected(weights, total=100.0))
    assert sum(totals.values()) == pytest.approx(100.0)


def test_prior_naics_2017_codes_recoded_to_2022(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # 2018 reports 322121 + 322122; 2022 reports their merge 322120, which
    # reaches clean_fba_w_sec as 322121.
    anchor = [('322', 100.0, None), ('322120', 60.0, None)]
    prior = [('322', 100.0, None), ('322121', 40.0, None), ('322122', 10.0, None)]
    _patch_loads(monkeypatch, {2022: _raw(anchor, 'HGL'), 2018: _raw(prior, 'HGL')})
    fba = _mapped({'322': 40.0, '322121': 60.0}, 'HGL', 2018)
    totals = {
        '322120' if c == '322121' else c: v
        for c, v in _group_totals(apply_mecs_annual_share_path(fba)).items()
    }
    weights = {
        '322': _path_weight(0.4, 0.5, (25.0 + 9.0) / 1e4, (25.0 + 4.0 + 0.25) / 1e4, 1),
        '322120': _path_weight(0.6, 0.5, 9.0 / 1e4, (4.0 + 0.25) / 1e4, 1.0),
    }
    assert totals == pytest.approx(_expected(weights))
    assert totals['322120'] < 60.0


def test_zero_anchor_piece_keeps_fuel_total(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # 2022: children fill 325, so its remainder is 0; 2018 remainder is 20.
    anchor = [('325', 70.0, None), ('325110', 40.0, None), ('325190', 30.0, None)]
    _patch_loads(monkeypatch, {2022: _raw(anchor, 'HGL'), 2018: _raw(PRIOR_RAW, 'HGL')})
    fba = _mapped({'325': 0.0, '325110': 40.0, '325190': 30.0}, 'HGL', 2018)
    totals = _group_totals(apply_mecs_annual_share_path(fba))
    assert totals['325'] == 0.0
    assert sum(totals.values()) == pytest.approx(70.0)


def test_reported_zero_in_prior_fades_in_linearly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prior = [('325', 100.0, None), ('325110', 0.0, None), ('325190', 50.0, None)]
    _patch_loads(monkeypatch, {2022: _raw(ANCHOR_RAW, 'HGL'), 2018: _raw(prior, 'HGL')})
    fba = _mapped({'325': 30.0, '325110': 40.0, '325190': 30.0}, 'HGL', 2020)
    totals = _group_totals(apply_mecs_annual_share_path(fba))
    weights = {
        '325': _path_weight(0.3, 0.5, ANCHOR_VAR['325'] / 1e4, 31.25 / 1e4, 0.5),
        '325110': _path_weight(0.4, 0.0, 4.0 / 1e4, 0.0, 0.5),
        '325190': _path_weight(0.3, 0.5, 2.25 / 1e4, 6.25 / 1e4, 0.5),
    }
    assert totals == pytest.approx(_expected(weights))
    assert 0 < totals['325110'] < 40.0


def test_sector_mix_from_prior_when_group_has_two_sectors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # One activity (336110) fans to two sectors; 2018 published both leaves.
    # Each sector gets its own group_id so Use attribution cannot re-split.
    anchor = [('336110', 42.0, None)]
    prior = [('336111', 18.0, None), ('336112', 24.0, None)]
    _patch_loads(monkeypatch, {2022: _raw(anchor, 'HGL'), 2018: _raw(prior, 'HGL')})
    fba = _mapped(
        {'336110': 42.0},
        'HGL',
        2018,
        sectors={'336110': ['336111', '336112']},
    )
    input_ids = set(fba['group_id'].astype(int))
    out = apply_mecs_annual_share_path(fba)
    amounts = out.set_index('SectorConsumedBy')['FlowAmount'].astype(float).to_dict()
    totals = out.set_index('SectorConsumedBy')['group_total'].astype(float).to_dict()
    acts = out.set_index('SectorConsumedBy')['ActivityConsumedBy'].astype(str).to_dict()
    assert amounts['336111'] == pytest.approx(42.0 * 18.0 / 42.0)
    assert amounts['336112'] == pytest.approx(42.0 * 24.0 / 42.0)
    assert totals['336111'] == pytest.approx(42.0 * 18.0 / 42.0)
    assert totals['336112'] == pytest.approx(42.0 * 24.0 / 42.0)
    assert acts['336111'] == '336111'
    assert acts['336112'] == '336112'
    out_ids = set(out['group_id'].astype(int))
    assert len(out_ids) == 2
    assert out_ids.isdisjoint(input_ids)
    assert out.groupby('group_id').size().eq(1).all()


def test_sector_mix_paper_split_when_prior_publishes_both(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # 322120 fans to 322121/322122; 2018 published both -> 80%/20% with new ids.
    anchor = [('322120', 50.0, None)]
    prior = [('322121', 40.0, None), ('322122', 10.0, None)]
    _patch_loads(monkeypatch, {2022: _raw(anchor, 'HGL'), 2018: _raw(prior, 'HGL')})
    fba = _mapped(
        {'322120': 50.0},
        'HGL',
        2018,
        sectors={'322120': ['322121', '322122']},
    )
    input_ids = set(fba['group_id'].astype(int))
    out = apply_mecs_annual_share_path(fba)
    amounts = out.set_index('SectorConsumedBy')['FlowAmount'].astype(float).to_dict()
    acts = out.set_index('SectorConsumedBy')['ActivityConsumedBy'].astype(str).to_dict()
    assert amounts['322121'] == pytest.approx(40.0)
    assert amounts['322122'] == pytest.approx(10.0)
    assert acts['322121'] == '322121'
    assert acts['322122'] == '322122'
    out_ids = set(out['group_id'].astype(int))
    assert len(out_ids) == 2
    assert out_ids.isdisjoint(input_ids)
    assert out.groupby('group_id').size().eq(1).all()


def test_sector_mix_leaves_use_when_prior_hits_one_sector(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Paper-shaped: both 2018 parts map to one sector under the identity mock
    # only if we publish a single sector code; here prior only has 32212.
    anchor = [('322120', 50.0, None)]
    prior = [('32212', 50.0, None)]
    _patch_loads(monkeypatch, {2022: _raw(anchor, 'HGL'), 2018: _raw(prior, 'HGL')})
    fba = _mapped(
        {'322120': 50.0},
        'HGL',
        2018,
        sectors={'322120': ['322121', '322122']},
    )
    out = apply_mecs_annual_share_path(fba)
    # Use mix (equal) unchanged: prior sector amounts miss both 322121/322122.
    assert list(out['FlowAmount']) == pytest.approx([25.0, 25.0])
    assert len(out['group_id'].unique()) == 1
