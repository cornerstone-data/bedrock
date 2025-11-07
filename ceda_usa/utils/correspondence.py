import typing as ta

import pandas as pd

D = ta.TypeVar("D")
R = ta.TypeVar("R")


def create_correspondence_matrix(
    dct: ta.Dict[D, ta.List[R]],
    domain: ta.Optional[ta.List[D]] = None,
    range: ta.Optional[ta.List[R]] = None,
    is_injective: bool = True,
    is_surjective: bool = True,
    is_complete: bool = True,
) -> pd.DataFrame:
    col_vals = list(dct.keys())
    idx_vals = list(set(col_val for col_vals in dct.values() for col_val in col_vals))

    df = pd.DataFrame(0, index=idx_vals, columns=col_vals)
    for k, vs in dct.items():
        for v in vs:
            df.loc[v, k] = 1  # type: ignore

    if domain is not None:
        df = df.reindex(columns=domain, fill_value=0)
    if range is not None:
        df = df.reindex(index=range, fill_value=0)

    if is_injective:
        assert (df.sum(axis=1) <= 1).all(), "expected injective: each row sum <= 1"

    if is_surjective:
        assert (df.sum(axis=1) >= 1).all(), "expected surjective: each row sum >= 1"

    if is_complete:
        assert (df.sum(axis=0) >= 1).all(), "expected complete: each col sum >= 1"

    return df
