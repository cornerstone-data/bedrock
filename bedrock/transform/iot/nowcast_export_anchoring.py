"""Anchor the export column of chosen families on the published 2017 cells (#1270).

The export column (``F04000``) is Census domestic exports (#762) mapped to BEA
commodities, and nothing conditions it. For the computers family (``3341``)
that puts exports above domestic output. Terminals and peripherals (``334118``)
export $9.2bn in 2017 against BEA's $6.1bn and its own $8.1bn of output, and
the whole family runs 1.28 times published. Census re-exports of terminals
($16.4bn in 2017) are already excluded, so the excess is in the domestic
exports themselves.

A row that exports more than it produces leaves imports with nowhere to go:
total use excluding exports falls below imports, so the import matrix assigns
industries more of the commodity than they buy (the rows #1282 warns about).

The construction is the #763 one for imports
(:func:`~bedrock.transform.iot.nowcast_import_conditioning.anchored_mcif`):
each commodity's exports are its **published 2017 detail ``F04000`` cell**
moved by **its own Census line's growth**. 2017 reproduces BEA, and every year
moves by its own observed series.

⚠️ Applied only inside :data:`ANCHORED_EXPORT_FAMILIES` (each row moved by its
own Census growth) and :data:`FAMILY_GROWTH_EXPORT_FAMILIES` (each row moved by
its family's growth, where Census misclassifies rows). Every other commodity
keeps its Census value.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

#: NAICS families whose exports are anchored on the published 2017 cells, each
#: row moved by **its own** Census line's growth.
#: ``3341`` computers: Census domestic exports run 1.28 times BEA's 2017 family
#: total, and terminals export more than they produce (#1270).
ANCHORED_EXPORT_FAMILIES: frozenset[str] = frozenset({'3341'})

#: NAICS families whose exports are anchored on the published 2017 cells, every
#: row moved by the **family's** Census growth: BEA's 2017 split of the family,
#: moved by the family level. For families whose Census rows are misclassified,
#: so a row's own line is no mover.
#: ``3254`` pharmaceuticals: Census puts preparations on ``325411`` medicinal
#: ingredients ($7.3bn of exports in 2017 against $0.08bn published) and
#: ``325413`` diagnostics. With each row's own growth, biologics (``325414``)
#: would export $52.7bn in 2024 against $48.0bn of output (#1194, #1270).
FAMILY_GROWTH_EXPORT_FAMILIES: frozenset[str] = frozenset({'3254'})

#: Below this ($) a published or Census value counts as empty.
EMPTY_USD = 1e6


def anchor_family_exports(
    exports_now: pd.Series,
    exports_2017: pd.Series,
    published_2017: pd.Series,
    families: frozenset[str] = ANCHORED_EXPORT_FAMILIES,
) -> pd.Series:
    """Exports with each anchored family's cells on published 2017 x own growth.

    For a commodity inside *families*:

    - published 2017 and Census 2017 both carry it:
      ``published_2017 x (census_t / census_2017)``;
    - published 2017 is empty but Census has it: keep the Census value;
    - Census 2017 is empty but published has it: hold the published value.

    Every commodity outside *families* keeps ``exports_now``.
    """
    pub = published_2017.reindex(exports_now.index).fillna(0.0).clip(lower=0.0)
    base = exports_2017.reindex(exports_now.index).fillna(0.0)
    in_family = pd.Series(
        [str(c)[:4] in families for c in exports_now.index], index=exports_now.index
    )
    has_pub = pub >= EMPTY_USD
    has_base = base >= EMPTY_USD
    with np.errstate(divide='ignore', invalid='ignore'):
        growth = (exports_now / base).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    out = exports_now.copy()
    both = in_family & has_pub & has_base
    out[both] = pub[both] * growth[both]
    pub_only = in_family & has_pub & ~has_base
    out[pub_only] = pub[pub_only]
    return out


def anchor_family_growth_exports(
    exports_now: pd.Series,
    exports_2017: pd.Series,
    published_2017: pd.Series,
    families: frozenset[str] = FAMILY_GROWTH_EXPORT_FAMILIES,
) -> pd.Series:
    """Exports with each family on BEA's 2017 split, moved by the family's growth.

    For each family in *families*, every commodity's exports are
    ``published_2017 x (family census_t / family census_2017)``. A commodity BEA
    publishes no 2017 exports for gets none: its Census mass is the
    misclassification being corrected. A family with no Census 2017 mass keeps
    its Census values. Every commodity outside *families* keeps ``exports_now``.
    """
    pub = published_2017.reindex(exports_now.index).fillna(0.0).clip(lower=0.0)
    base = exports_2017.reindex(exports_now.index).fillna(0.0)
    family = pd.Series([str(c)[:4] for c in exports_now.index], index=exports_now.index)
    out = exports_now.copy()
    for code in sorted(families):
        rows = family == code
        base_total = float(base[rows].sum())
        if base_total < EMPTY_USD:
            continue
        growth = float(exports_now[rows].sum()) / base_total
        out[rows] = pub[rows] * growth
        logger.info(
            'exports %s: BEA 2017 split moved by the family growth %.3f', code, growth
        )
    return out


def anchored_exports(
    exports_now: pd.Series, exports_2017: pd.Series, published_2017: pd.Series
) -> pd.Series:
    """Both anchorings: own-growth rows, then family-growth families."""
    rows = anchor_family_exports(exports_now, exports_2017, published_2017)
    return anchor_family_growth_exports(rows, exports_2017, published_2017)


def published_2017_exports() -> pd.Series:
    """BEA's published 2017 detail export column (``F04000``), USD."""
    from bedrock.extract.iot.io_2017 import (  # noqa: PLC0415
        _load_2017_detail_supply_use_usa,
    )

    use = _load_2017_detail_supply_use_usa('Use_SUT_detail')
    use.columns = use.columns.str.strip()
    return pd.to_numeric(use['F04000'], errors='coerce').fillna(0.0) * 1e6
