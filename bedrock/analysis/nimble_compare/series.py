"""The one data structure this package compares: a named, coded, 1-D series.

A ``LabeledSeries`` is a tidy frame with three meaningful columns --
``code``, ``name``, ``value`` -- plus a free-form ``level`` used to carry
hierarchy depth for sources (like NIPA sheets) that interleave subtotals with
leaves.  Everything in :mod:`loaders` produces one of these; everything in
:mod:`matching` and :mod:`compare` consumes them.  Either ``code`` or ``name``
may be blank on any given row; alignment falls back to whichever is present.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import cast

import pandas as pd

COLUMNS = ['code', 'name', 'value', 'level']

#: Group code -> the pre-aggregation codes and values behind it.  Populated by
#: :meth:`LabeledSeries.rollup` and :meth:`LabeledSeries.merge_codes` so a
#: comparison run at a coarse granularity can still show its detail composition.
Members = dict[str, list[tuple[str, float]]]

# BEA writes footnote references into the label text itself, in two styles:
# NIPA xls sheets use backslash-delimited markers ("Farms\1\", "Other retail\2\")
# while the flat-file SeriesLabels use trailing parentheses ("Accommodations (104)").
_FOOTNOTE_BACKSLASH = re.compile(r'\\\d+\\')
_FOOTNOTE_PAREN = re.compile(r'(?:\s*\(\d+\))+$')

# Terms BEA spells differently between tables. Applied after casefolding.
_SYNONYMS = [
    (re.compile(r'\bn\.e\.c\.'), 'not elsewhere classified'),
    (re.compile(r'\bnec\b'), 'not elsewhere classified'),
    (re.compile(r'\bless:\s*'), ''),
    (re.compile(r'\band\b'), '&'),
]

_PUNCT = re.compile(r'[^\w&]+')


def strip_footnotes(name: str) -> str:
    """Remove BEA footnote markers but keep the label otherwise intact."""
    if not isinstance(name, str):
        return ''
    return _FOOTNOTE_PAREN.sub('', _FOOTNOTE_BACKSLASH.sub('', name)).strip()


def normalize_name(name: str) -> str:
    """Aggressively fold a label to a comparison key.

    Lossy on purpose: casing, punctuation, footnotes, the Oxford comma and
    BEA's inconsistent abbreviations all disappear, so
    "Electrical equipment, appliances, and components" and
    "Electrical Equipment, Appliances & Components" collapse to one key.
    """
    s = strip_footnotes(name).casefold()
    for pattern, repl in _SYNONYMS:
        s = pattern.sub(repl, s)
    s = _PUNCT.sub(' ', s)
    return ' '.join(s.split())


def normalize_code(code: str) -> str:
    """Fold a code to a comparison key: upper-case, no separators."""
    if not isinstance(code, str):
        code = '' if code is None or pd.isna(code) else str(code)
    return re.sub(r'[^A-Z0-9]', '', code.upper())


def _key_set(labels: Iterable[str]) -> set[str]:
    """Normalize labels both ways, so a code or a name can be used to select rows."""
    keys = {normalize_code(label) for label in labels}
    keys |= {normalize_name(label) for label in labels}
    keys.discard('')
    return keys


@dataclass
class LabeledSeries:
    """A 1-D series of values labeled by code and/or name.

    :param frame: tidy frame with ``code``, ``name``, ``value``, ``level``
    :param label: short human name used in reports and column headers
    :param unit: unit string, compared between sides only as a warning
    """

    frame: pd.DataFrame
    label: str = 'series'
    unit: str = ''
    meta: dict[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        df = self.frame.copy()
        for col in COLUMNS:
            if col not in df.columns:
                df[col] = pd.NA
        df['code'] = df['code'].fillna('').astype(str).str.strip()
        df['name'] = df['name'].fillna('').astype(str).map(strip_footnotes)
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        df['level'] = pd.to_numeric(df['level'], errors='coerce').astype('Int64')
        df['code_key'] = df['code'].map(normalize_code)
        df['name_key'] = df['name'].map(normalize_name)
        self.frame = df.loc[(df['code'] != '') | (df['name'] != '')].reset_index(
            drop=True
        )

    def __len__(self) -> int:
        return len(self.frame)

    def __repr__(self) -> str:
        return (
            f'<LabeledSeries {self.label!r} n={len(self)} '
            f'total={self.total:,.0f} {self.unit}>'
        )

    @property
    def total(self) -> float:
        return float(self.frame['value'].sum())

    @property
    def members(self) -> Members:
        """Detail composition of each row, empty when no aggregation happened."""
        return cast('Members', self.meta.get('members', {}))

    def scale(self, factor: float, unit: str | None = None) -> 'LabeledSeries':
        """Return a copy with values multiplied -- for unit reconciliation."""
        df = self.frame.copy()
        df['value'] = df['value'] * factor
        return LabeledSeries(df, self.label, unit or self.unit, dict(self.meta))

    def query(self, expr: str) -> 'LabeledSeries':
        """Return a copy restricted to rows matching a pandas query."""
        return LabeledSeries(
            self.frame.query(expr), self.label, self.unit, dict(self.meta)
        )

    def leaves(
        self,
        *,
        keep: Iterable[str] = (),
        drop: Iterable[str] = (),
    ) -> 'LabeledSeries':
        """Drop hierarchy subtotals, keeping only rows with no children.

        A row is a subtotal when the row immediately below it sits at a deeper
        ``level``.  Sources without a ``level`` are returned unchanged, since
        every row is already a leaf as far as this package can tell.

        ``keep`` and ``drop`` override the level test by code or name, which is
        how you trade one side's granularity for the other's: when the candidate
        splits an industry the reference does not, keep the candidate's subtotal
        and drop its children.  Both are matched against normalized keys, so
        either the code or the label will do.
        """
        df = self.frame
        if df['level'].isna().all():
            return self
        keep_keys = _key_set(keep)
        drop_keys = _key_set(drop)
        level: list[int] = df['level'].tolist()
        code_keys: list[str] = df['code_key'].tolist()
        name_keys: list[str] = df['name_key'].tolist()
        selected: list[bool] = []
        for pos in range(len(df)):
            row_keys = {code_keys[pos], name_keys[pos]}
            if row_keys & drop_keys:
                selected.append(False)
            elif row_keys & keep_keys:
                selected.append(True)
            else:
                selected.append(
                    pos == len(level) - 1 or not (level[pos + 1] > level[pos])
                )
        out = LabeledSeries(df.loc[selected], self.label, self.unit, dict(self.meta))
        out.meta['dropped_subtotals'] = len(df) - len(out)
        return out

    def merge_codes(
        self,
        groups: dict[str, list[str]],
        names: dict[str, str] | None = None,
    ) -> 'LabeledSeries':
        """Sum groups of rows into one synthetic row each, passing the rest through.

        The escape hatch for partition mismatches -- BEA splitting real estate
        into ``HS``/``ORE`` where a NIPA table reports one "Real estate" line::

            ref.merge_codes({'RE': ['HS', 'ORE']}, {'RE': 'Real estate'})

        Unlike :meth:`rollup`, rows not named in ``groups`` survive untouched, so
        this composes onto an existing rollup instead of replacing it.
        """
        member_to_target = {
            normalize_code(m): target for target, ms in groups.items() for m in ms
        }
        df = self.frame.copy()
        target = df['code_key'].map(member_to_target)
        merged = (
            df.loc[target.notna()]
            .assign(code=target.loc[target.notna()])
            .groupby('code', as_index=False)['value']
            .sum()
        )
        merged['name'] = merged['code'].map(names or {}).fillna(merged['code'])
        out = LabeledSeries(
            pd.concat([df.loc[target.isna()], merged], ignore_index=True),
            self.label,
            self.unit,
            dict(self.meta),
        )
        out.meta['merged_groups'] = sorted(groups)
        # Compose through any earlier rollup so a merged group still reports the
        # original codes underneath it, not the intermediate ones it merged.
        prior = self.members
        composed: Members = {}
        for code, value in zip(df['code'], df['value']):
            new_code = member_to_target.get(normalize_code(code), code)
            composed.setdefault(new_code, []).extend(
                prior.get(code, [(code, float(value))])
            )
        out.meta['members'] = composed
        return out

    def rollup(self, mapping: dict[str, list[str]] | dict[str, str]) -> 'LabeledSeries':
        """Aggregate values into coarser groups keyed by ``mapping``.

        ``mapping`` goes from this series' code to one or more target codes, the
        orientation the ``bedrock.utils.taxonomy.mappings`` loaders already use.
        A code mapping to several targets is split evenly between them -- crude,
        but these correspondences are near-always 1:many in the other direction,
        so the split rarely fires.  Codes absent from ``mapping`` are dropped and
        counted in ``meta['unmapped_codes']``.

        The pre-aggregation codes behind each group are kept in
        ``meta['members']`` so a comparison made at this coarser granularity can
        still show what composed every cell -- and, importantly, which groups are
        1:1 with a single original code and so are not really aggregated at all.
        """
        codes: list[str] = []
        values: list[float] = []
        unmapped: list[str] = []
        members: Members = {}
        for code, value in zip(self.frame['code'], self.frame['value']):
            targets = mapping.get(code)
            if targets is None:
                unmapped.append(code)
                continue
            targets = [targets] if isinstance(targets, str) else list(targets)
            share = 1.0 / len(targets)
            codes.extend(targets)
            values.extend([float(value) * share] * len(targets))
            for target in targets:
                members.setdefault(target, []).append((code, float(value) * share))
        grouped = (
            pd.DataFrame({'code': codes, 'value': values})
            .groupby('code', as_index=False)
            .agg({'value': 'sum'})
        )
        out = LabeledSeries(grouped, self.label, self.unit, dict(self.meta))
        out.meta['unmapped_codes'] = unmapped
        out.meta['rolled_up_from'] = len(self)
        out.meta['members'] = members
        return out

    def with_names(self, code_to_name: dict[str, str]) -> 'LabeledSeries':
        """Fill in (or overwrite) names from a code -> name lookup."""
        df = self.frame.copy()
        filled = df['code'].map(code_to_name)
        df['name'] = filled.where(filled.notna(), df['name'])
        return LabeledSeries(df, self.label, self.unit, dict(self.meta))
