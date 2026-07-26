"""Align two :class:`~.series.LabeledSeries` when no exact crosswalk exists.

The alignment is a cascade of increasingly forgiving passes, each one-to-one and
each only touching rows the earlier passes left over:

===========  ==========================================================
``override`` pairs you supplied by hand -- always wins
``code``     normalized code equality
``crosswalk``candidate code -> reference code through a mapping you pass
``name``     normalized name equality (footnotes, case, punctuation folded)
``fuzzy``    best ``difflib`` name similarity above ``fuzzy_cutoff``
===========  ==========================================================

Every match carries the pass that produced it and a 0-1 score, so a report can
be read with the weak links visible rather than buried.  Rows that never match
are returned separately -- for BEA comparisons they are usually the interesting
part, because they mark where two tables partition an industry differently.

The fuzzy pass is **opt-in** (``on='fuzzy'``), because BEA's classifications are
built by splitting a name into "Other X" and "All other X" variants, so a
detail label that reads almost like a summary label is usually one *part* of it.
Matched across granularity, "Support activities for mining" scores 0.90 against
detail code 323120, "Support activities for **printing**" -- a different
industry off by a factor of 20.  Rows left unmatched invite you to look; a
plausible-looking wrong pair does not.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from difflib import SequenceMatcher

import pandas as pd

from bedrock.analysis.nimble_compare.series import (
    LabeledSeries,
    normalize_code,
    normalize_name,
)

MatchMethod = str
DEFAULT_FUZZY_CUTOFF = 0.88


@dataclass
class Alignment:
    """The result of :func:`align`.

    :param pairs: one row per matched pair, with both sides' code/name,
        ``method`` and ``score``
    :param candidate_only: candidate rows that never matched
    :param reference_only: reference rows that never matched
    :param ambiguous: keys that appeared more than once on a side and were
        therefore skipped rather than guessed at
    """

    pairs: pd.DataFrame
    candidate_only: pd.DataFrame
    reference_only: pd.DataFrame
    ambiguous: list[str] = field(default_factory=list)

    @property
    def match_counts(self) -> pd.Series:
        return self.pairs['method'].value_counts()


class _Side:
    """Column-major view of one side's frame.

    Alignment touches individual cells thousands of times over, so the columns
    are pulled out as plain Python lists once up front -- both faster than
    repeated ``.at[]`` and typed, which ``.at[]`` on an object column is not.
    """

    def __init__(self, series: LabeledSeries) -> None:
        frame = series.frame
        self.n = len(frame)
        self.code: list[str] = frame['code'].tolist()
        self.name: list[str] = frame['name'].tolist()
        self.value: list[float] = frame['value'].tolist()
        self.code_key: list[str] = frame['code_key'].tolist()
        self.name_key: list[str] = frame['name_key'].tolist()
        self.frame = frame

    def keys(self, which: str) -> list[str]:
        return self.code_key if which == 'code_key' else self.name_key


def _unique_key_index(
    keys: list[str], pool: set[int]
) -> tuple[dict[str, int], list[str]]:
    """Index ``pool`` positions by their key, reporting duplicated keys.

    Duplicates are deliberately not resolved: a key appearing twice means the
    two sides disagree about granularity in a way a silent pick would hide.
    """
    seen: dict[str, list[int]] = {}
    for i in pool:
        if keys[i]:
            seen.setdefault(keys[i], []).append(i)
    unique = {k: v[0] for k, v in seen.items() if len(v) == 1}
    dupes = [k for k, v in seen.items() if len(v) > 1]
    return unique, dupes


def align(
    candidate: LabeledSeries,
    reference: LabeledSeries,
    *,
    on: str = 'auto',
    crosswalk: dict[str, list[str]] | dict[str, str] | None = None,
    overrides: dict[str, str] | None = None,
    fuzzy_cutoff: float = DEFAULT_FUZZY_CUTOFF,
) -> Alignment:
    """Pair up candidate and reference rows.

    :param on: which passes to run --
        ``'auto'`` (default) every exact pass, no fuzzy;
        ``'fuzzy'`` the above plus the fuzzy pass;
        ``'code'`` code and crosswalk only;
        ``'name'`` name only
    :param crosswalk: candidate code -> reference code(s).  Only single-target
        entries are used for pairing, since a 1:many entry means the reference
        needs :meth:`LabeledSeries.rollup` first, not a pairing.
    :param overrides: candidate code-or-name -> reference code-or-name, matched
        against normalized keys, for the handful of pairs nothing else catches
    :param fuzzy_cutoff: minimum ``difflib`` ratio to accept a fuzzy name match
    """
    if on not in ('auto', 'fuzzy', 'code', 'name'):
        raise ValueError(f"on={on!r} is not one of 'auto', 'fuzzy', 'code', 'name'")
    use_code = on in ('auto', 'fuzzy', 'code')
    use_name = on in ('auto', 'fuzzy', 'name')
    use_fuzzy = on == 'fuzzy'

    c, r = _Side(candidate), _Side(reference)
    c_pool, r_pool = set(range(c.n)), set(range(r.n))
    pairs: list[dict[str, object]] = []
    ambiguous: list[str] = []

    def take(ci: int, ri: int, method: MatchMethod, score: float) -> None:
        pairs.append(
            {
                'candidate_code': c.code[ci],
                'candidate_name': c.name[ci],
                'reference_code': r.code[ri],
                'reference_name': r.name[ri],
                'candidate_value': c.value[ci],
                'reference_value': r.value[ri],
                'method': method,
                'score': score,
            }
        )
        c_pool.discard(ci)
        r_pool.discard(ri)

    def exact_pass(key: str, method: MatchMethod) -> None:
        c_index, c_dupes = _unique_key_index(c.keys(key), c_pool)
        r_index, r_dupes = _unique_key_index(r.keys(key), r_pool)
        ambiguous.extend(c_dupes + r_dupes)
        for value, ci in sorted(c_index.items()):
            ri = r_index.get(value)
            if ri is not None and ci in c_pool and ri in r_pool:
                take(ci, ri, method, 1.0)

    # --- override pass: matched on either key, so you can name whichever side
    # of the pair is easier to type
    if overrides:
        r_by_code, _ = _unique_key_index(r.code_key, r_pool)
        r_by_name, _ = _unique_key_index(r.name_key, r_pool)
        for c_raw, r_raw in overrides.items():
            c_code, c_name = normalize_code(c_raw), normalize_name(c_raw)
            ci = next(
                (
                    i
                    for i in sorted(c_pool)
                    if c.code_key[i] == c_code or c.name_key[i] == c_name
                ),
                None,
            )
            ri = r_by_code.get(
                normalize_code(r_raw), r_by_name.get(normalize_name(r_raw))
            )
            if ci is not None and ri is not None and ri in r_pool:
                take(ci, ri, 'override', 1.0)

    if use_code:
        exact_pass('code_key', 'code')

        if crosswalk:
            single = {
                normalize_code(k): normalize_code(v if isinstance(v, str) else v[0])
                for k, v in crosswalk.items()
                if isinstance(v, str) or len(v) == 1
            }
            r_by_code, r_dupes = _unique_key_index(r.code_key, r_pool)
            ambiguous.extend(r_dupes)
            for ci in sorted(c_pool):
                target = single.get(c.code_key[ci])
                ri = r_by_code.get(target) if target else None
                if ri is not None and ri in r_pool:
                    take(ci, ri, 'crosswalk', 1.0)

    if use_name:
        exact_pass('name_key', 'name')

    if use_fuzzy:
        # A name the exact pass found duplicated stays unmatched here too.
        # Otherwise the fuzzy pass would score those identical names at 1.0 and
        # pick one arbitrarily -- silently resolving the ambiguity that the
        # exact pass deliberately refused to guess at.
        blocked = set(ambiguous)
        scored: list[tuple[float, int, int]] = []
        r_names = [
            (i, r.name_key[i])
            for i in sorted(r_pool)
            if r.name_key[i] and r.name_key[i] not in blocked
        ]
        for ci in sorted(c_pool):
            c_name = c.name_key[ci]
            if not c_name or c_name in blocked:
                continue
            for ri, r_name in r_names:
                ratio = SequenceMatcher(None, c_name, r_name).ratio()
                if ratio >= fuzzy_cutoff:
                    scored.append((ratio, ci, ri))
        # greedy: strongest similarities claim their partner first
        for ratio, ci, ri in sorted(scored, key=lambda t: -t[0]):
            if ci in c_pool and ri in r_pool:
                take(ci, ri, 'fuzzy', round(ratio, 4))

    cols = ['code', 'name', 'value', 'level']
    return Alignment(
        pairs=pd.DataFrame(
            pairs,
            columns=[
                'candidate_code',
                'candidate_name',
                'reference_code',
                'reference_name',
                'candidate_value',
                'reference_value',
                'method',
                'score',
            ],
        ),
        candidate_only=c.frame.iloc[sorted(c_pool)][cols].reset_index(drop=True),
        reference_only=r.frame.iloc[sorted(r_pool)][cols].reset_index(drop=True),
        ambiguous=sorted(set(ambiguous)),
    )
