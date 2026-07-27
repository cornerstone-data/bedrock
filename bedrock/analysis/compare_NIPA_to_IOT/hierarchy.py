"""Label conventions that declare a hierarchy, so similarity need not guess it.

BEA builds classifications by carving a residual out of a parent and naming it
after the parent: ``Ambulatory health care services`` contains detail code
621900 ``Other ambulatory health care services``.  The label *says* it is a part.
Reading that from the words is exact and cheap; inferring it from string
similarity is neither, and gets it backwards -- the two names are maximally
similar precisely when one is a residual of the other.

Hence two tools used by :mod:`matching`:

:func:`split_residual`
    strip a residual marker and return the parent concept, but **only** as a
    candidate for confirmation.  "Other" is not self-evidently a marker: five
    BEA summary industries are genuinely named ``Other retail``,
    ``Other real estate``, ``Other transportation equipment`` and so on.  What
    makes a marker real is that removing it leaves a label the *other side*
    actually uses, which is why the caller must confirm against the opposite
    side rather than trusting a strip in isolation.

:func:`token_relation`
    classify how two labels differ, so the fuzzy pass can refuse the failure it
    is prone to.  ``Support activities for mining`` and ``Support activities for
    printing`` share a long prefix and differ by one short content word, which
    ``difflib`` scores at 0.90 -- but a *substituted* content word means two
    different industries, always.
"""

from __future__ import annotations

import re

from bedrock.analysis.compare_NIPA_to_IOT.series import normalize_name

#: Tokens that carry no discriminating meaning, so a difference confined to them
#: is a wording difference rather than a different concept.
FILLER_TOKENS = frozenset(
    {
        '&',
        'a',
        'activities',
        'and',
        'for',
        'in',
        'of',
        'or',
        'the',
        'to',
    }
)

#: Tokens that mark a residual or subordinate category rather than qualifying it.
#: A one-sided difference made of these means "part of", not "same as".
RESIDUAL_TOKENS = frozenset(
    {
        'all',
        'classified',
        'elsewhere',
        'misc',
        'nec',
        'other',
        'remaining',
    }
)

# Residual markers as they are actually written, per source dialect.  Kept
# separate because the conventions differ: "All other X" occurs 11 times in the
# BEA detail industry list and never once in the summary list or in a NIPA
# industry table, so it is a reliable detail-level residual marker and a
# meaningless rule to apply elsewhere.
_COMMON_MARKERS = (
    r'^all other\s+',
    r'^other\s+',
    r'^remaining\s+',
    r',?\s*not elsewhere classified$',
    r',?\s*n\.?e\.?c\.?$',
    r',?\s*other$',
)

RESIDUAL_MARKERS: dict[str, tuple[str, ...]] = {
    # BEA detail industry/commodity names (Use_SUT_detail and friends)
    'bea_io_detail': _COMMON_MARKERS,
    # BEA summary names -- same conventions, but note that several summary names
    # legitimately *begin* with "Other", so a strip here is only ever a
    # candidate to be confirmed against the opposite side
    'bea_io_summary': _COMMON_MARKERS,
    # NIPA xls sheets. Their industry stubs are summary-grade concepts and rarely
    # residual-marked; "Other retail", "Other services, except government" are
    # real category names, not residuals.
    'nipa': (r'^all other\s+', r'^remaining\s+', r',?\s*n\.?e\.?c\.?$'),
    'unknown': _COMMON_MARKERS,
}


def markers_for(dialect: str) -> tuple[str, ...]:
    """Residual-marker patterns registered for a source dialect."""
    return RESIDUAL_MARKERS.get(dialect, RESIDUAL_MARKERS['unknown'])


def split_residual(name: str, dialect: str = 'unknown') -> tuple[str, str] | None:
    """Strip one residual marker, returning ``(parent_key, marker)``.

    Returns ``None`` when no registered marker applies.  A non-``None`` result is
    a *hypothesis*: it means "this label reads like a residual of ``parent_key``",
    and is only trustworthy once ``parent_key`` is found on the opposite side.
    """
    key = normalize_name(name)
    for pattern in markers_for(dialect):
        stripped = re.sub(pattern, '', key, count=1).strip()
        if stripped and stripped != key:
            marker = key.replace(stripped, '').strip() or pattern
            return ' '.join(stripped.split()), marker
    return None


def _inflectional(a: str, b: str) -> bool:
    """True when two tokens differ only by a short suffix (service/services)."""
    short, long = sorted((a, b), key=len)
    return long.startswith(short) and len(long) - len(short) <= 2


def token_relation(name_a: str, name_b: str) -> str:
    """Classify how two labels differ, from the words alone.

    ``equal``
        the same tokens, allowing filler and inflection
    ``residual``
        one side adds only residual markers -- it is a *part* of the other, so
        the two must not be treated as the same thing
    ``qualified``
        one side adds ordinary qualifying words ("except internet"); the same
        concept described at different length, so a match is plausible
    ``different``
        each side has a content word the other lacks: a substitution, which
        means two different concepts however similar the strings look
    """
    tokens_a = set(normalize_name(name_a).split()) - FILLER_TOKENS
    tokens_b = set(normalize_name(name_b).split()) - FILLER_TOKENS
    extra_a = tokens_a - tokens_b
    extra_b = tokens_b - tokens_a

    # pair off inflectional variants so service/services is not a difference
    for a in sorted(extra_a):
        for b in sorted(extra_b):
            if _inflectional(a, b):
                extra_a.discard(a)
                extra_b.discard(b)
                break

    if not extra_a and not extra_b:
        return 'equal'
    if extra_a and extra_b:
        return 'different'
    extra = extra_a or extra_b
    return 'residual' if extra & RESIDUAL_TOKENS else 'qualified'
