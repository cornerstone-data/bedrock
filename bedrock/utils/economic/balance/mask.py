"""The mask layer: three kinds of constraint that are not one boolean.

**Conflating the three layers is the mistake that made the mask look free.**

===================  ==========================================  ==============
Layer                Meaning                                     Cost
===================  ==========================================  ==============
structural zero      cell is zero and must stay zero             free
fixed value          cell is nonzero, measured, must not move    *this is it*
sign lock            cell may move but not across zero           cheap
===================  ==========================================  ==============

Only the middle one is the mask proper, and neither candidate engine has it:
ceda's ``free_mask`` does ``np.where(mask, matrix, 0.0)``
(``ras_balancing.py:573``), which sets a masked cell to zero rather than
holding it at its value - so a fixed *nonzero* cell cannot be expressed at all.
Diagonal scaling already preserves structural zeros, which is why that layer is
free under either engine and why it is easy to mistake the whole mask for free.

**Count cells and the mask looks cheap; count dollars and it does not.** On the
2017 detail SUT the final-demand block is 2.7% of the Use panel's nonzero cells
and **39.9% of its dollars** - a fifteenfold difference between the two ways of
reading the same mask. Freezing it costs 27 commodity rows every degree of
freedom on the Use side, with 51 more above 10x leverage.

**A structural zero is an assumption, not housekeeping.** The Supply block is
only 3.1% dense, so imposing the 2017 sparsity pattern there asserts *no
industry produces a commodity it did not produce in 2017*. That may be the
right modelling choice; it must not be an accidental one.

Sign convention
---------------

Subsidies are stored **negative** throughout the balance, matching the Supply
table's ``SUB`` column. BEA publishes the two sides with opposite signs - the
Use table's ``T00SUB`` row is stored ``+59,876`` with no negative cells, the
Supply ``SUB`` column ``-59,876`` with every cell negative - so a producer-price
column margin is *not* a plain sum of its cells unless one convention is
imposed on the way in. Normalising at seed assembly is checked once;
a signed aggregator is a chance to get it wrong at every call site. See
:func:`assert_subsidies_negative`.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


def _require_same_labels(a: pd.DataFrame, b: pd.DataFrame, what: str) -> None:
    if not a.index.equals(b.index):
        raise ValueError(f'{what}: row labels differ')
    if not a.columns.equals(b.columns):
        raise ValueError(f'{what}: column labels differ')


@dataclass(frozen=True, eq=False)
class SutMask:
    """The three layers, kept separate.

    ``structural_zero`` and ``fixed_value`` are boolean frames over the block.
    ``sign_lock`` is ``{-1, 0, +1}``: ``-1`` the cell must stay ``<= 0``, ``+1``
    it must stay ``>= 0``, ``0`` it is free to cross zero.

    A cell cannot be both a structural zero and a fixed value. The two are
    genuinely different claims - "this is zero and stays zero" versus "this was
    measured and stays measured" - and collapsing them is exactly the confusion
    this class exists to prevent, so it raises rather than picking one.
    """

    structural_zero: pd.DataFrame
    fixed_value: pd.DataFrame
    sign_lock: pd.DataFrame

    def __post_init__(self) -> None:
        _require_same_labels(self.structural_zero, self.fixed_value, 'SutMask')
        _require_same_labels(self.structural_zero, self.sign_lock, 'SutMask')
        if self.structural_zero.to_numpy().dtype != bool:
            raise TypeError('structural_zero must be boolean')
        if self.fixed_value.to_numpy().dtype != bool:
            raise TypeError('fixed_value must be boolean')
        locks = self.sign_lock.to_numpy()
        if not np.isin(locks, (-1, 0, 1)).all():
            raise ValueError('sign_lock must be -1, 0 or +1')
        overlap = self.structural_zero.to_numpy() & self.fixed_value.to_numpy()
        if overlap.any():
            rows, cols = np.nonzero(overlap)
            first = (
                self.structural_zero.index[rows[0]],
                self.structural_zero.columns[cols[0]],
            )
            raise ValueError(
                f'{int(overlap.sum())} cells are both structural zeros and '
                f'fixed values, first at {first}. A zero that must stay zero '
                f'and a measured value that must not move are different '
                f'claims; pick one'
            )

    @classmethod
    def from_pattern(
        cls,
        seed: pd.DataFrame,
        *,
        fixed_value: pd.DataFrame | None = None,
        sign_lock: pd.DataFrame | None = None,
    ) -> SutMask:
        """Structural zeros taken from where ``seed`` is zero.

        The convenience path for "the 2017 sparsity pattern, plus whatever else
        this year fixes". Cells that ``fixed_value`` claims are removed from the
        structural-zero layer, so a fixed zero is treated as measured rather
        than as pattern.
        """
        zero = seed == 0
        fixed = (
            pd.DataFrame(False, index=seed.index, columns=seed.columns)
            if fixed_value is None
            else fixed_value.astype(bool)
        )
        locks = (
            pd.DataFrame(0, index=seed.index, columns=seed.columns, dtype=int)
            if sign_lock is None
            else sign_lock.astype(int)
        )
        return cls(
            structural_zero=(zero & ~fixed),
            fixed_value=fixed,
            sign_lock=locks,
        )

    @property
    def index(self) -> pd.Index:
        return self.structural_zero.index

    @property
    def columns(self) -> pd.Index:
        return self.structural_zero.columns

    @property
    def shape(self) -> tuple[int, int]:
        rows, cols = self.structural_zero.shape
        return rows, cols

    @property
    def frozen(self) -> pd.DataFrame:
        """Cells the balance may not move: structural zeros plus fixed values.

        This is the **participation** mask an engine sees after the offset in
        :mod:`.offset` has removed the fixed mass - both candidate engines
        already support participation. It is not the fixed-value mask itself.
        """
        return self.structural_zero | self.fixed_value

    @property
    def free(self) -> pd.DataFrame:
        return ~self.frozen

    def validate_against(self, seed: pd.DataFrame) -> None:
        """Check the seed is consistent with the mask.

        A seed that is nonzero where the mask says structural zero is a
        contradiction between two inputs, and silently balancing it would
        propagate whichever one happens to win.
        """
        _require_same_labels(self.structural_zero, seed, 'seed vs mask')
        values = seed.to_numpy(dtype=float)
        bad = self.structural_zero.to_numpy() & (values != 0)
        if bad.any():
            rows, cols = np.nonzero(bad)
            first = (seed.index[rows[0]], seed.columns[cols[0]])
            raise ValueError(
                f'{int(bad.sum())} cells are nonzero in the seed but marked '
                f'structural zero, first at {first} = {values[rows[0], cols[0]]}'
            )
        locks = self.sign_lock.to_numpy()
        wrong_sign = ((locks == 1) & (values < 0)) | ((locks == -1) & (values > 0))
        if wrong_sign.any():
            rows, cols = np.nonzero(wrong_sign)
            first = (seed.index[rows[0]], seed.columns[cols[0]])
            raise ValueError(
                f'{int(wrong_sign.sum())} seed cells already violate their sign '
                f'lock, first at {first} = {values[rows[0], cols[0]]}'
            )

    def summary(self) -> pd.Series:
        """Cell counts per layer, for diagnostics."""
        return pd.Series(
            {
                'cells': int(self.structural_zero.size),
                'structural_zero': int(self.structural_zero.to_numpy().sum()),
                'fixed_value': int(self.fixed_value.to_numpy().sum()),
                'sign_locked': int((self.sign_lock.to_numpy() != 0).sum()),
                'free': int(self.free.to_numpy().sum()),
            }
        )


def assert_subsidies_negative(
    frame: pd.DataFrame, *, axis: str, label: str = 'SUB'
) -> None:
    """Assert the balance's subsidy sign convention holds on ``frame``.

    Subsidies are stored negative everywhere inside the balance. BEA publishes
    the Use table's ``T00SUB`` row positive and the Supply table's ``SUB``
    column negative, so this has to be imposed at seed assembly and checked
    once, rather than carried as a signed coefficient at every call site.

    ``axis`` is ``'row'`` if ``label`` names a row of ``frame``, ``'column'``
    if it names a column.
    """
    if axis == 'row':
        if label not in frame.index:
            raise KeyError(f'{label} is not a row of the frame')
        row = frame.loc[label]
        if isinstance(row, pd.DataFrame):
            raise ValueError(
                f'{label} matches {len(row)} rows; the sign convention has to '
                f'be checked on a single row'
            )
        values = pd.to_numeric(row, errors='raise')
    elif axis == 'column':
        if label not in frame.columns:
            raise KeyError(f'{label} is not a column of the frame')
        values = pd.to_numeric(frame[label], errors='raise')
    else:
        raise ValueError(f'axis must be row or column, got {axis!r}')
    positive = values[values > 0]
    if len(positive):
        raise ValueError(
            f'{label} carries {len(positive)} positive cells, first '
            f'{positive.index[0]} = {positive.iloc[0]}. The balance stores '
            f'subsidies negative, matching the Supply table; BEA stores the '
            f'Use table T00SUB row positive, so it must be negated at seed '
            f'assembly. Left unnormalised, a producer-price column margin is '
            f'wrong by 2 x T00SUB'
        )
