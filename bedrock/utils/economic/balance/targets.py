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

**A target is a linear combination of margins, not a single one.** Most of the
hard constraints in ``target_set_plan.md`` §2 relate the Use panel to the
Supply panel, so a target that could name only one block could not express
them::

    Σ_k  coefficient_k · margin(block_k, axis_k, aggregator_k)  =  values

============  =====================================================
Constraint    Terms
============  =====================================================
T1            ``+1 · use.column`` = gross output, per industry
T11           ``+1 · supply.row − 1 · use.row`` = 0, per commodity
T12           ``+1 · use.row[T00SUB] − 1 · supply.col[SUB]`` = 0
T13           ``+1 · use.row[T00TOP] − 1 · supply.col[TOP, MDTY]`` = 0
T15/T16       ``+1 · supply.col[TRADE]`` = 0
============  =====================================================

Collapsing a margin to a scalar is an :class:`Aggregator` with a single group,
so the same machinery covers per-label and economy-wide constraints.

⚠️ **T12 is a difference, not a sum.** BEA stores the Use ``T00SUB`` row
positive and the Supply ``SUB`` column negative, so on the raw tables it is a
sum - and ``target_set_plan.md`` §2's table still states it that way. The
balance normalises both negative, which makes it a plain equality. See
§2a of that document.

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

**``source`` is provenance and must survive into diagnostics.** When a balance
fails, the report has to name the account that pulled against the rest. A
source beginning :data:`PLACEHOLDER_PREFIX` marks a target whose *shape* is
right and whose *value* is not yet sourced; :func:`TargetSet.placeholders`
finds them, and the feasibility precheck refuses to certify a set containing
one unless asked to.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Literal

import numpy as np
import pandas as pd

# Which margin a term refers to. ``'row'`` sums across columns and yields one
# value per row; ``'column'`` sums down rows and yields one per column.
Axis = Literal['row', 'column']

#: A target whose shape is correct but whose value is not yet sourced. Kept as
#: a ``source`` prefix rather than a flag so it cannot be lost in a copy, and
#: so it shows up in every diagnostic that prints provenance.
PLACEHOLDER_PREFIX = 'PLACEHOLDER:'


@dataclass(frozen=True, eq=False)
class Aggregator:
    """A 0/1 matrix mapping detail labels to the groups a source publishes.

    ``matrix`` is ``(n_groups, n_detail)``, so ``matrix @ margin`` aggregates a
    detail margin up to published groups. A single group containing a single
    label is the idiom for collapsing a margin to a scalar.

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

    @classmethod
    def total(
        cls, labels: Sequence[str], detail: Sequence[str], name: str
    ) -> Aggregator:
        """One group named ``name`` summing ``labels``: a margin to a scalar."""
        return cls.from_mapping({name: list(labels)}, detail)

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
class TargetTerm:
    """One ``coefficient · margin(block, axis)`` in a target.

    ``restrict_to`` narrows which labels on the *summed* axis participate: with
    ``axis='column'`` it selects rows, and with ``axis='row'`` it selects
    columns. That is what lets a single value-added row be constrained by
    industry group - neither a plain row margin nor a plain column one.

    Together, ``restrict_to`` and ``aggregator`` are the ``C`` and ``R`` of
    ``R @ X @ Cᵀ``.
    """

    block: str
    axis: Axis
    coefficient: float = 1.0
    aggregator: Aggregator | None = None
    restrict_to: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        if self.axis not in ('row', 'column'):
            raise ValueError(f'axis must be row or column, got {self.axis!r}')
        if self.coefficient == 0:
            raise ValueError('a term with coefficient 0 contributes nothing')

    def margin_of(self, frame: pd.DataFrame) -> pd.Series:
        """``coefficient ×`` this term's margin of ``frame``, aggregated."""
        if self.axis == 'row':
            selected = (
                frame
                if self.restrict_to is None
                else frame.loc[:, list(self.restrict_to)]
            )
            margin = selected.astype(float).sum(axis=1)
        else:
            selected = (
                frame if self.restrict_to is None else frame.loc[list(self.restrict_to)]
            )
            margin = selected.astype(float).sum(axis=0)
        if self.aggregator is not None:
            margin = self.aggregator.apply(margin)
        return self.coefficient * margin


@dataclass(frozen=True, eq=False)
class Target:
    """One constraint: a linear combination of margins equal to ``values``.

    ``values`` is indexed by the labels the terms share after aggregation - one
    entry per industry for T1, per commodity for T11, or a single entry for an
    economy-wide identity.

    ``weight`` is ignored when ``hard`` is set, and is meaningful only relative
    to the other soft weights in the same set.
    """

    terms: tuple[TargetTerm, ...]
    values: pd.Series
    source: str
    name: str = ''
    hard: bool = False
    weight: float = 1.0
    allow_negative: bool = False

    def __post_init__(self) -> None:
        if not self.terms:
            raise ValueError('a target needs at least one term')
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

    @classmethod
    def on_margin(
        cls,
        block: str,
        axis: Axis,
        values: pd.Series,
        source: str,
        *,
        aggregator: Aggregator | None = None,
        restrict_to: tuple[str, ...] | None = None,
        name: str = '',
        hard: bool = False,
        weight: float = 1.0,
        allow_negative: bool = False,
    ) -> Target:
        """The single-term case, which is most sourced targets."""
        term = TargetTerm(
            block=block, axis=axis, aggregator=aggregator, restrict_to=restrict_to
        )
        return cls(
            terms=(term,),
            values=values,
            source=source,
            name=name,
            hard=hard,
            weight=weight,
            allow_negative=allow_negative,
        )

    @property
    def label(self) -> str:
        """Short identifier for messages."""
        blocks = '+'.join(dict.fromkeys(t.block for t in self.terms))
        return f'{self.name or blocks}[{self.source}]'

    @property
    def blocks(self) -> tuple[str, ...]:
        """Every block this target reads, in term order, deduplicated."""
        return tuple(dict.fromkeys(t.block for t in self.terms))

    @property
    def is_cross_block(self) -> bool:
        return len(self.blocks) > 1

    @property
    def is_placeholder(self) -> bool:
        return self.source.startswith(PLACEHOLDER_PREFIX)

    def evaluate(self, blocks: Mapping[str, pd.DataFrame]) -> pd.Series:
        """The left-hand side: the linear combination, on ``values``' index."""
        total = pd.Series(0.0, index=self.values.index)
        for term in self.terms:
            if term.block not in blocks:
                raise KeyError(
                    f'{self.label} reads block {term.block!r}, which was not '
                    f'supplied; got {sorted(blocks)}'
                )
            contribution = term.margin_of(blocks[term.block]).reindex(self.values.index)
            if contribution.isna().any():
                missing = list(contribution.index[contribution.isna()])
                raise KeyError(
                    f'{self.label} names margin labels block {term.block!r} '
                    f'does not have: {missing}'
                )
            total = total + contribution
        return total

    def residual_against(self, blocks: Mapping[str, pd.DataFrame]) -> pd.Series:
        """``values`` less what ``blocks`` already contribute."""
        return pd.to_numeric(self.values, errors='raise') - self.evaluate(blocks)

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
    """An immutable collection of targets, queryable by block and mode."""

    targets: tuple[Target, ...]

    def __post_init__(self) -> None:
        seen: set[tuple[str, str, tuple[str, ...], tuple[str, ...]]] = set()
        for target in self.targets:
            # Same source constraining the same margins of the same blocks. The
            # margin labels are part of the identity: one source legitimately
            # constrains many margins, it just must not constrain one twice.
            key = (
                target.name,
                target.source,
                target.blocks,
                tuple(sorted(str(label) for label in target.values.index)),
            )
            if key in seen:
                raise ValueError(
                    f'duplicate target {target.label} on margins {key[3]}: the '
                    f'same source cannot constrain the same margin twice'
                )
            seen.add(key)

    @classmethod
    def of(cls, *targets: Target) -> TargetSet:
        return cls(targets=tuple(targets))

    def __iter__(self) -> Iterator[Target]:
        return iter(self.targets)

    def __len__(self) -> int:
        return len(self.targets)

    def touching(self, block: str) -> TargetSet:
        """Targets that read ``block``, whether or not they read others too."""
        return TargetSet(tuple(t for t in self.targets if block in t.blocks))

    @property
    def hard(self) -> TargetSet:
        return TargetSet(tuple(t for t in self.targets if t.hard))

    @property
    def soft(self) -> TargetSet:
        return TargetSet(tuple(t for t in self.targets if not t.hard))

    @property
    def cross_block(self) -> TargetSet:
        return TargetSet(tuple(t for t in self.targets if t.is_cross_block))

    @property
    def placeholders(self) -> TargetSet:
        """Targets whose shape is right and whose value is not yet sourced."""
        return TargetSet(tuple(t for t in self.targets if t.is_placeholder))

    @property
    def blocks(self) -> tuple[str, ...]:
        seen: dict[str, None] = {}
        for target in self.targets:
            for block in target.blocks:
                seen.setdefault(block, None)
        return tuple(seen)

    def summary(self) -> pd.DataFrame:
        """One row per target: mode, weight, size and provenance."""
        return pd.DataFrame(
            [
                {
                    'name': t.name,
                    'blocks': '+'.join(t.blocks),
                    'axes': '+'.join(dict.fromkeys(term.axis for term in t.terms)),
                    'terms': len(t.terms),
                    'margins': len(t.values),
                    'mode': 'H' if t.hard else f'S{t.weight:g}',
                    'placeholder': t.is_placeholder,
                    'source': t.source,
                }
                for t in self.targets
            ]
        )
