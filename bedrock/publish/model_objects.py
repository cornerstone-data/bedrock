"""Cached EEIO model getters shared by publish exporters."""

# ruff: noqa: PLC0415
from __future__ import annotations

import functools
from collections.abc import Callable
from typing import NamedTuple

import pandas as pd

from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.math.formulas import compute_L_matrix, compute_M_matrix

PUBLISH_LOCATION: str = 'US'


class SheetSpec(NamedTuple):
    name: str
    getter: Callable[[], pd.DataFrame | pd.Series | None]


def _with_loc_suffix(idx: pd.Index) -> pd.Index:
    if idx.name != 'sector':
        return idx
    out = [f'{str(v)}/{PUBLISH_LOCATION}' for v in idx]
    return pd.Index(out, name=idx.name)


def apply_loc_suffix(obj: pd.DataFrame | pd.Series) -> pd.DataFrame | pd.Series:
    out = obj.copy()
    if isinstance(out, pd.DataFrame):
        out.index = _with_loc_suffix(out.index)
        out.columns = _with_loc_suffix(out.columns)
    else:
        out.index = _with_loc_suffix(out.index)
    return out


def assemble_extended_U(
    intermediate: pd.DataFrame,
    fd: pd.DataFrame | None,
    va: pd.DataFrame,
) -> pd.DataFrame:
    """Stack intermediate / FD / VA blocks into a useeior-style extended U."""
    if not intermediate.columns.equals(va.columns):
        raise ValueError(
            'assemble_extended_U: intermediate.columns and va.columns must match '
            f'(got {len(intermediate.columns)} vs {len(va.columns)} labels)'
        )
    if fd is not None and not intermediate.index.equals(fd.index):
        raise ValueError(
            'assemble_extended_U: intermediate.index and fd.index must match '
            f'(got {len(intermediate.index)} vs {len(fd.index)} labels)'
        )

    if fd is None:
        out = pd.concat([intermediate, va], axis=0)
    else:
        top = pd.concat([intermediate, fd], axis=1)
        va_zeros = pd.DataFrame(0.0, index=va.index, columns=fd.columns)
        bottom = pd.concat([va, va_zeros], axis=1)
        out = pd.concat([top, bottom], axis=0)

    out.index.name = 'sector'
    out.columns.name = 'sector'
    return out


def require_cornerstone_config() -> None:
    if not get_usa_config().use_cornerstone_2026_model_schema:
        raise NotImplementedError(
            'bedrock.publish only supports cornerstone-schema configs '
            '(use_cornerstone_2026_model_schema=True).'
        )


@functools.cache
def get_V() -> pd.DataFrame:
    from bedrock.transform.eeio.derived_cornerstone import derive_cornerstone_V

    return derive_cornerstone_V()


@functools.cache
def get_U() -> pd.DataFrame:
    from bedrock.transform.eeio.derived_cornerstone import (
        _derive_cornerstone_Ytot_with_trade,
        derive_cornerstone_U_set,
        derive_cornerstone_VA,
    )
    from bedrock.utils.taxonomy.cornerstone.final_demand import FINAL_DEMANDS

    uset = derive_cornerstone_U_set()
    intermediate = uset.Udom + uset.Uimp
    fd_block = _derive_cornerstone_Ytot_with_trade()[list(FINAL_DEMANDS)]
    return assemble_extended_U(
        intermediate=intermediate,
        fd=fd_block,
        va=derive_cornerstone_VA(),
    )


@functools.cache
def get_Udom() -> pd.DataFrame:
    from bedrock.transform.eeio.derived_cornerstone import (
        derive_cornerstone_U_set,
        derive_cornerstone_VA,
    )

    return assemble_extended_U(
        intermediate=derive_cornerstone_U_set().Udom,
        fd=None,
        va=derive_cornerstone_VA(),
    )


@functools.cache
def get_x() -> pd.Series:
    from bedrock.transform.eeio.derived_cornerstone import derive_cornerstone_x

    return pd.Series(derive_cornerstone_x(), name='x')


@functools.cache
def get_A() -> pd.DataFrame:
    from bedrock.transform.eeio.derived import derive_Aq_usa

    aq = derive_Aq_usa()
    A = aq.Adom + aq.Aimp
    A.index.name = 'sector'
    A.columns.name = 'sector'
    return A


@functools.cache
def get_Adom() -> pd.DataFrame:
    from bedrock.transform.eeio.derived import derive_Aq_usa

    return derive_Aq_usa().Adom


@functools.cache
def get_L() -> pd.DataFrame:
    return compute_L_matrix(A=get_A())


@functools.cache
def get_Ldom() -> pd.DataFrame:
    return compute_L_matrix(A=get_Adom())


@functools.cache
def get_B() -> pd.DataFrame:
    from bedrock.transform.eeio.derived import derive_B_usa_non_finetuned

    return derive_B_usa_non_finetuned()


@functools.cache
def get_C() -> pd.DataFrame:
    from bedrock.transform.eeio.derived import derive_C_usa

    return derive_C_usa()


@functools.cache
def get_D() -> pd.DataFrame:
    from bedrock.transform.eeio.derived import derive_D_usa

    return derive_D_usa()


@functools.cache
def get_M() -> pd.DataFrame:
    M = compute_M_matrix(B=get_B(), L=get_L())
    M.columns.name = 'sector'
    return M


@functools.cache
def get_Mdom() -> pd.DataFrame:
    M_d = compute_M_matrix(B=get_B(), L=get_Ldom())
    M_d.columns.name = 'sector'
    return M_d


@functools.cache
def get_N() -> pd.DataFrame:
    N = get_C() @ get_M()
    N.columns.name = 'sector'
    return N


@functools.cache
def get_Ndom() -> pd.DataFrame:
    N_d = get_C() @ get_Mdom()
    N_d.columns.name = 'sector'
    return N_d


@functools.cache
def get_q() -> pd.Series:
    from bedrock.transform.eeio.derived import derive_Aq_usa

    return pd.Series(derive_Aq_usa().scaled_q, name='q')


_CACHED_GETTERS: tuple[Callable[[], pd.DataFrame | pd.Series], ...] = (
    get_V,
    get_U,
    get_Udom,
    get_x,
    get_A,
    get_Adom,
    get_L,
    get_Ldom,
    get_B,
    get_C,
    get_D,
    get_M,
    get_Mdom,
    get_N,
    get_Ndom,
    get_q,
)


def clear_publish_caches() -> None:
    for fn in _CACHED_GETTERS:
        fn.cache_clear()  # type: ignore[attr-defined]
