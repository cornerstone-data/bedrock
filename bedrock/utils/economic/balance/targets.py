"""Targets for a constrained matrix balance.

The constraint side of Step 5
(`#653 <https://github.com/cornerstone-data/bedrock/issues/653>`_). A balance
is a seed matrix plus a set of margins it must reproduce; this module is the
margins. It is deliberately engine-agnostic - nothing here knows whether the
solver ends up being GRAS, RAS, or something vendored, which is the point of
building it before `#588`'s Decision 1 is settled.

**A target is a statement about a margin, not about a cell.** Cells that a
source reports directly belong in the mask (:mod:`.mask`), and the rule that
separates the two is in ``mask_layer_plan.md`` §4: *mask a cell only if the
source reports that cell; if the source reports the margin, it belongs to the
target set instead - never both.* A source spent on a cell cannot also be spent
on a margin.

**Hard and soft.** Only identities are hard. Everything sourced is an estimate
from an account with its own vintage, so a target set held entirely hard is
infeasible by construction - NIPA, GDP-by-industry and the trade accounts will
not reconcile to the dollar. The soft weights decide *who gives way* when they
disagree, which is not the same question as *which number is more accurate*.
Weights are meaningful only relative to each other.

**Signs are load-bearing.** ``allow_negative`` exists because a target that can
legitimately be negative is not an edge case here: ``F03000`` (change in
private inventories) is **-37,568 in 2020**. An engine that clamps targets
non-negative silently produces a wrong answer rather than failing, so the
admissible sign is recorded on the target itself and checked at construction.

**Aggregators express what a source actually publishes.** ``aggregator=None``
binds the target at detail. Otherwise it is the ``R`` (or ``C``) in
``R @ X @ Cᵀ``: the truthful constraint for compensation of employees is *"these
N detail industries sum to the published group"*, because NIPA T60200D
publishes by industry group and not by 402 detail industries. Expressing that
is the capability a row/column-vector API cannot provide.

**``source`` is provenance and must survive into diagnostics.** When a balance
fails, the report has to name the account that pulled against the rest.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Literal

import numpy as np
import pandas as pd

# Which margin a target constrains. ``'row'`` sums across columns and yields
# one value per row; ``'column'`` sums down rows and yields one per column.
Axis = Literal['row', 'column']


@dataclass(frozen=True, eq=False)
class Aggregator:
    """A 0/1 matrix mapping detail labels to the groups a source publishes.

    ``matrix`` is ``(n_groups, n_detail)``, so ``matrix @ margin`` aggregates a
    detail margin up to published groups. Rows are disjoint in practice but
    that is not enforced - a label may legitimately appear in two groups if the
    source overlaps, and the balance simply carries both constraints.

    ``eq=False`` because the dataclass-generated ``__eq__`` would compare
    ndarrays and raise on the ambiguous truth value.
    """

    matrix: np.ndarray
    groups: tuple[str, ...]
    detail: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.matrix.shape != (len(self.groups), len(self.detail)):
            raise ValueError(
                f'aggregator matrix is {self.matrix.shape}, expected '
                f'{(len(self.groups), len(self.detail))} from its labels'
            )
        if len(set(self.groups)) != len(self.groups):
            raise ValueError('aggregator group labels are not unique')
        if len(set(self.detail)) != len(self.detail):
            raise ValueError('aggregator detail labels are not unique')

    @classmethod
    def from_mapping(
        cls, mapping: Mapping[str, Sequence[str]], detail: Sequence[str]
    ) -> Aggregator:
        """Build from ``{group: [detail labels]}``.

        Every detail label named in ``mapping`` must be in ``detail``; a label
        in ``detail`` that no group claims is allowed, and simply does not
        participate in this constraint. That is the normal case - a source that
        covers part of the economy should not be forced to cover all of it.
        """
        detail = list(detail)
        position = {label: i for i, label in enumerate(detail)}
        groups = list(mapping)
        matrix = np.zeros((len(groups), len(detail)), dtype=float)
        for g, group in enumerate(groups):
            for label in mapping[group]:
                if label not in position:
                    raise KeyError(
                        f'aggregator group {group!r} names {label!r}, which is '
                        f'not in the detail labels'
                    )
                matrix[g, position[label]] = 1.0
        return cls(matrix=matrix, groups=tuple(groups), detail=tuple(detail))

    def apply(self, margin: pd.Series) -> pd.Series:
        """Aggregate a detail margin to groups: ``R @ margin``.

        ``margin`` is reindexed onto ``detail`` first, so a margin carrying
        extra labels is fine and a margin *missing* one raises rather than
        silently contributing zero.
        """
        aligned = margin.reindex(self.detail)
        if aligned.isna().any():
            missing = [label for label, bad in zip(self.detail, aligned.isna()) if bad]
            raise KeyError(f'margin is missing aggregator detail labels: {missing}')
        values = self.matrix @ aligned.to_numpy(dtype=float)
        return pd.Series(values, index=pd.Index(self.groups, name=margin.index.name))


@dataclass(frozen=True, eq=False)
class Target:
    """One margin constraint on one block of the balance.

    ``values`` is indexed by margin label, or by *group* label when an
    ``aggregator`` is present. ``restrict_to`` narrows which labels on the
    summed axis participate: with ``axis='column'`` it selects rows, and with
    ``axis='row'`` it selects columns. ``None`` means all of them.

    ``restrict_to`` is what lets a single row's cells be constrained by
    industry group - the compensation target is *row* ``V00100`` summed over
    *column* groups, which is neither a plain row margin nor a plain column
    one. Together, ``restrict_to`` and ``aggregator`` are the ``R`` and ``C``
    of ``R @ X @ Cᵀ``.

    ``weight`` is ignored when ``hard`` is set, and is meaningful only relative
    to the other soft weights in the same set.
    """

    block: str
    axis: Axis
    values: pd.Series
    source: str
    aggregator: Aggregator | None = None
    restrict_to: tuple[str, ...] | None = None
    hard: bool = False
    weight: float = 1.0
    allow_negative: bool = False

    def __post_init__(self) -> None:
        if self.axis not in ('row', 'column'):
            raise ValueError(f'axis must be row or column, got {self.axis!r}')
        if not self.source:
            raise ValueError(
                'target needs a source: provenance has to survive into '
                'diagnostics, because a failed balance must name the account '
                'that pulled against the rest'
            )
        if self.weight <= 0:
            raise ValueError(f'weight must be positive, got {self.weight}')
        if self.values.index.has_duplicates:
            raise ValueError(f'{self.label} has duplicate margin labels')
        numeric = pd.to_numeric(self.values, errors='coerce')
        if numeric.isna().any():
            raise ValueError(f'{self.label} has non-numeric or missing values')
        if not self.allow_negative and (numeric < 0).any():
            negative = numeric[numeric < 0]
            raise ValueError(
                f'{self.label} has negative values at {list(negative.index)} '
                f'but allow_negative is False. A target that can legitimately '
                f'go negative must say so - F03000 is -37,568 in 2020, and an '
                f'engine that clamps it silently returns a wrong answer'
            )
        if self.aggregator is not None:
            expected = set(self.aggregator.groups)
            if set(self.values.index) != expected:
                raise ValueError(
                    f'{self.label} is aggregated, so its values must be indexed '
                    f'by the aggregator groups; got '
                    f'{sorted(set(self.values.index) ^ expected)} in symmetric '
                    f'difference'
                )

    @property
    def label(self) -> str:
        """Short identifier for messages: ``block.axis[source]``."""
        return f'{self.block}.{self.axis}[{self.source}]'

    @property
    def is_aggregated(self) -> bool:
        return self.aggregator is not None

    def with_values(self, values: pd.Series, *, source_suffix: str = '') -> Target:
        """Copy carrying new values, permitting negatives.

        Used for residual targets, which are a different object from the one
        the source published: subtracting frozen mass can carry a positive
        target across zero, so the sign guard that applies to a published
        target does not apply to its residual.
        """
        return replace(
            self,
            values=values,
            allow_negative=True,
            source=f'{self.source}{source_suffix}',
        )


@dataclass(frozen=True, eq=False)
class TargetSet:
    """An immutable collection of targets, queryable by block and axis."""

    targets: tuple[Target, ...]

    def __post_init__(self) -> None:
        seen: set[tuple[str, str, str]] = set()
        for target in self.targets:
            key = (target.block, target.axis, target.source)
            if key in seen:
                raise ValueError(
                    f'duplicate target {key}: the same source cannot constrain '
                    f'the same margin twice'
                )
            seen.add(key)

    @classmethod
    def of(cls, *targets: Target) -> TargetSet:
        return cls(targets=tuple(targets))

    def __iter__(self) -> Iterator[Target]:
        return iter(self.targets)

    def __len__(self) -> int:
        return len(self.targets)

    def for_block(self, block: str) -> TargetSet:
        return TargetSet(tuple(t for t in self.targets if t.block == block))

    def for_axis(self, axis: Axis) -> TargetSet:
        return TargetSet(tuple(t for t in self.targets if t.axis == axis))

    @property
    def hard(self) -> TargetSet:
        return TargetSet(tuple(t for t in self.targets if t.hard))

    @property
    def soft(self) -> TargetSet:
        return TargetSet(tuple(t for t in self.targets if not t.hard))

    @property
    def blocks(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(t.block for t in self.targets))
