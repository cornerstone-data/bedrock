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

⚠️ Applied only inside :data:`ANCHORED_EXPORT_FAMILIES`. Every other commodity
keeps its Census value.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

#: NAICS families whose exports are anchored on the published 2017 cells.
#: ``3341`` computers: Census domestic exports run 1.28 times BEA's 2017 family
#: total, and terminals export more than they produce (#1270).
ANCHORED_EXPORT_FAMILIES: frozenset[str] = frozenset({'3341'})

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


def published_2017_exports() -> pd.Series:
    """BEA's published 2017 detail export column (``F04000``), USD."""
    from bedrock.extract.iot.io_2017 import (  # noqa: PLC0415
        _load_2017_detail_supply_use_usa,
    )

    use = _load_2017_detail_supply_use_usa('Use_SUT_detail')
    use.columns = use.columns.str.strip()
    return pd.to_numeric(use['F04000'], errors='coerce').fillna(0.0) * 1e6
