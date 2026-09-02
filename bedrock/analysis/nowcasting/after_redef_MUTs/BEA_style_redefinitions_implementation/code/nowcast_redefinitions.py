"""Apply BEA redefinitions to a before-redefinitions MUT quartet plus value added.

Moves secondary Make output from the producing industry to the industry for
which the commodity is primary, then reallocates the matching Use column
(intermediate and value added) using destination-industry recipes, named
special-case rules, and a residual overlay. Amounts come from the input tables
of the year being transformed; which pairs redefine and which rule applies are
2017-learned structure loaded from disk.
"""

from __future__ import annotations

import logging
import typing as ta
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd

from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_value_added import USA_2017_VALUE_ADDED_CODES

logger = logging.getLogger(__name__)

ATOL = 0.5 * MILLION_CURRENCY_TO_CURRENCY


def _usd(value: ta.Any) -> float:
    """Coerce a pandas loc/at scalar to float for type-checkers and runtime."""
    return float(np.asarray(value, dtype=float).reshape(-1)[0])


RuleId = Literal['default', 'C1', 'C2', 'C3']
VaMix = Literal['source', 'dest']
RecipeKey = str | tuple[str, str] | tuple[str, str, str]
RuleSet = frozenset[str]

DEFAULT_ONLY: RuleSet = frozenset({'default'})
FULL: RuleSet = frozenset({'default', 'C1', 'C2', 'C3', 'C4', 'C6'})

SOFTWARE_PUBLISHERS = '511200'
COMPENSATION = 'V00100'
GOS = 'V00300'
TAXES = 'V00200'
C1_INTERMEDIATE_CUTOFF = 0.05

WHOLESALE_DEST = frozenset(
    {
        '423100',
        '423400',
        '423600',
        '423800',
        '423A00',
        '424200',
        '424400',
        '424700',
        '424A00',
        '425000',
    }
)
RETAIL_DEST = frozenset(
    {
        '441000',
        '444000',
        '445000',
        '446000',
        '447000',
        '448000',
        '452000',
        '454000',
        '4B0000',
    }
)
TRADE_DEST = WHOLESALE_DEST | RETAIL_DEST

NAMED_REALLOCATION_PAIRS: tuple[tuple[frozenset[str], frozenset[str]], ...] = (
    (frozenset({'721000'}), frozenset({'713200'})),
    (frozenset({'721000'}), frozenset({'722110', '722211', '722A00'})),
    (frozenset({'441000'}), frozenset({'811100'})),
    (frozenset({'522A00'}), frozenset({'532100'})),
    (frozenset({'531HSO'}), frozenset({'233411'})),
    (frozenset({'221100', 'S00101', 'S00202'}), frozenset({'233240'})),
    (frozenset({'517110', '517210', '517A00'}), frozenset({'233240'})),
)

MARGINS_VALUE_COLUMNS = (
    "Producers' Value",
    'Transportation',
    'Wholesale',
    'Retail',
    "Purchasers' Value",
)
MARGINS_INDEX_NAMES = ('Industry Code', 'Commodity Code')

_REDEF_DIR = Path(__file__).resolve().parents[2] / 'analysis' / 'nowcasting'
CLASSIFICATION_PATH = _REDEF_DIR / 'redefinitions_2017_classification.csv'
RECIPES_PATH = _REDEF_DIR / 'redefinitions_2017_recipes.csv'
OVERLAY_U_PATH = _REDEF_DIR / 'redefinitions_2017_overlay_U.parquet'
OVERLAY_VA_PATH = _REDEF_DIR / 'redefinitions_2017_overlay_VA.parquet'
OVERLAY_UIMP_PATH = _REDEF_DIR / 'redefinitions_2017_overlay_Uimp.parquet'
OVERLAY_MARGINS_PATH = _REDEF_DIR / 'redefinitions_2017_overlay_margins.parquet'

_CLASSIFICATION_STR_COLS = (
    'source_industry',
    'commodity',
    'destination_industry',
    'rule_id',
    'va_mix',
)
_RECIPE_STR_COLS = (
    'key_kind',
    'source_industry',
    'destination_industry',
    'commodity',
    'row_code',
)


@dataclass(frozen=True)
class RedefinitionPair:
    """One Make off-diagonal that BEA treats as a redefinition."""

    source_industry: str
    commodity: str
    destination_industry: str
    share: float
    delta: float
    rule_id: RuleId = 'default'
    va_mix: VaMix | None = None


@dataclass(frozen=True)
class RedefinitionOverlay:
    """Residual Use / VA / Import / Margins after the named reallocation rules.

    Units are USD. Each frame matches its loader's index and columns. There is
    no Make overlay.
    """

    U: pd.DataFrame
    VA: pd.DataFrame
    Uimp: pd.DataFrame
    margins: pd.DataFrame


def recipe_index() -> pd.Index:
    """Commodity codes followed by the three MUT value-added codes."""
    return pd.Index(
        list(USA_2017_COMMODITY_CODES) + list(USA_2017_VALUE_ADDED_CODES),
        name='row_code',
    )


def empty_recipe() -> pd.Series:
    """A zero share column on :func:`recipe_index`."""
    return pd.Series(0.0, index=recipe_index(), dtype=float)


def moved_amount(pair: RedefinitionPair, V_before: pd.DataFrame) -> float:
    """Redefined output ``R`` for *pair* from the year's Make table, in USD."""
    value = _usd(V_before.loc[pair.source_industry, pair.commodity])
    if abs(value) > ATOL:
        return float(pair.share) * value
    return 0.0


def destination_industry_output(
    U_before: pd.DataFrame, VA_before: pd.DataFrame, destination: str
) -> float:
    """Use-plus-VA column sum for the destination industry, in USD."""
    return _usd(U_before.loc[:, destination].sum()) + _usd(
        VA_before.loc[:, destination].sum()
    )


def _assign_recipe_rows(recipe: pd.Series, values: pd.Series) -> None:
    """Write *values* onto *recipe*, expanding the index for codes not in the taxonomy."""
    for code, value in values.items():
        recipe.loc[str(code)] = _usd(value)


def destination_industry_recipe(
    U_before: pd.DataFrame, VA_before: pd.DataFrame, destination: str
) -> pd.Series:
    """Direct-requirements mix of *destination* on :func:`recipe_index`."""
    x_d = destination_industry_output(U_before, VA_before, destination)
    recipe = empty_recipe()
    if abs(x_d) <= ATOL:
        return recipe
    _assign_recipe_rows(recipe, U_before.loc[:, destination] / x_d)
    _assign_recipe_rows(recipe, VA_before.loc[:, destination] / x_d)
    return recipe.astype(float)


def _sort_pairs(
    pairs: Sequence[RedefinitionPair], V_before: pd.DataFrame
) -> list[RedefinitionPair]:
    return sorted(
        pairs,
        key=lambda p: (
            -abs(moved_amount(p, V_before)),
            p.source_industry,
            p.destination_industry,
            p.commodity,
        ),
    )


def _c1_denom(VA: pd.DataFrame, industry: str) -> float:
    return _usd(VA.loc[COMPENSATION, industry]) + _usd(VA.loc[GOS, industry])


def _c1_weights(VA: pd.DataFrame, industry: str) -> tuple[float, float] | None:
    denom = _c1_denom(VA, industry)
    if abs(denom) <= ATOL:
        return None
    w1 = _usd(VA.loc[COMPENSATION, industry]) / denom
    return w1, 1.0 - w1


def _c1_vector(R: float, w1: float) -> pd.Series:
    vec = empty_recipe()
    vec.loc[COMPENSATION] = R * w1
    vec.loc[GOS] = R * (1.0 - w1)
    return vec


def _apply_would_skip_use(
    pair: RedefinitionPair,
    U_before: pd.DataFrame,
    VA_before: pd.DataFrame,
) -> bool:
    """Whether apply would skip this pair's Use/VA (dest-B ``x_d`` or C1 denom)."""
    if pair.rule_id == 'C1':
        industry = (
            pair.source_industry
            if pair.va_mix == 'source'
            else pair.destination_industry
        )
        return abs(_c1_denom(VA_before, industry)) <= ATOL
    x_d = destination_industry_output(U_before, VA_before, pair.destination_industry)
    return abs(x_d) <= ATOL


def _lookup_c3_recipe(
    pair: RedefinitionPair, recipes: Mapping[RecipeKey, pd.Series]
) -> pd.Series:
    key3: RecipeKey = (pair.source_industry, pair.destination_industry, pair.commodity)
    key2: RecipeKey = (pair.source_industry, pair.destination_industry)
    if key3 in recipes:
        return recipes[key3]
    if key2 in recipes:
        return recipes[key2]
    raise ValueError(
        f'C3 recipe missing for ({pair.source_industry}, '
        f'{pair.destination_industry}, {pair.commodity})'
    )


def _use_vector_for_unmix(
    pair: RedefinitionPair,
    R: float,
    U_before: pd.DataFrame,
    VA_before: pd.DataFrame,
    recipes: Mapping[RecipeKey, pd.Series],
) -> pd.Series:
    """Dest-positive vector apply would move, or zeros if apply would skip."""
    if abs(R) <= ATOL or _apply_would_skip_use(pair, U_before, VA_before):
        return empty_recipe()
    if pair.rule_id == 'C1':
        industry = (
            pair.source_industry
            if pair.va_mix == 'source'
            else pair.destination_industry
        )
        weights = _c1_weights(VA_before, industry)
        if weights is None:
            return empty_recipe()
        return _c1_vector(R, weights[0])
    if pair.rule_id == 'C2':
        if 'C2' not in recipes:
            raise ValueError('C2 recipe missing while unmixing a C2 pair')
        return recipes['C2'] * R
    if pair.rule_id == 'C3':
        return _lookup_c3_recipe(pair, recipes) * R
    return (
        destination_industry_recipe(U_before, VA_before, pair.destination_industry) * R
    )


def _column_delta(
    U_before: pd.DataFrame,
    U_after: pd.DataFrame,
    VA_before: pd.DataFrame,
    VA_after: pd.DataFrame,
    industry: str,
) -> pd.Series:
    delta = empty_recipe()
    _assign_recipe_rows(delta, U_after.loc[:, industry] - U_before.loc[:, industry])
    _assign_recipe_rows(delta, VA_after.loc[:, industry] - VA_before.loc[:, industry])
    return delta.astype(float)


def _touches_column(pair: RedefinitionPair, industry: str) -> bool:
    return pair.source_industry == industry or pair.destination_industry == industry


def _unmix_sign(pair: RedefinitionPair, industry: str) -> float:
    """+1 if *industry* is dest (credit), -1 if source (debit)."""
    if pair.destination_industry == industry:
        return 1.0
    if pair.source_industry == industry:
        return -1.0
    return 0.0


def recover_own_account_software_recipe(
    V_before: pd.DataFrame,
    U_before: pd.DataFrame,
    U_after: pd.DataFrame,
    VA_before: pd.DataFrame,
    VA_after: pd.DataFrame,
    classification: list[RedefinitionPair],
) -> pd.Series | None:
    """Recover the five-input own-account software share recipe, or None.

    *U_after* / *VA_after* are the published after-redefinitions tables.
    Candidates are dest ``511200`` with ``|R| > ATOL``; ``rule_id`` is ignored.
    Does not assign ``rule_id``.
    """
    candidates = [
        pair
        for pair in classification
        if pair.destination_industry == SOFTWARE_PUBLISHERS
        and abs(moved_amount(pair, V_before)) > ATOL
    ]
    if not candidates:
        return None
    sum_R = sum(moved_amount(pair, V_before) for pair in candidates)
    remaining = _column_delta(
        U_before, U_after, VA_before, VA_after, SOFTWARE_PUBLISHERS
    )
    candidate_ids = {
        (p.source_industry, p.commodity, p.destination_industry) for p in candidates
    }
    for pair in classification:
        if (
            pair.source_industry,
            pair.commodity,
            pair.destination_industry,
        ) in candidate_ids:
            continue
        if not _touches_column(pair, SOFTWARE_PUBLISHERS):
            continue
        if pair.rule_id == 'C3':
            continue
        R = moved_amount(pair, V_before)
        vec = _use_vector_for_unmix(pair, R, U_before, VA_before, {})
        remaining = remaining - _unmix_sign(pair, SOFTWARE_PUBLISHERS) * vec

    fifth = GOS
    if abs(_usd(remaining.loc[GOS])) <= ATOL:
        if abs(_usd(remaining.loc[TAXES])) > ATOL:
            fifth = TAXES
        else:
            return None
    use_abs = remaining.loc[list(USA_2017_COMMODITY_CODES)].abs()
    ranked = sorted(
        use_abs.index,
        key=lambda code: (-_usd(use_abs.loc[code]), str(code)),
    )
    three_use = ranked[:3]
    five = [COMPENSATION, fifth, *three_use]
    if any(abs(_usd(remaining.loc[code])) <= ATOL for code in five):
        return None
    recipe = empty_recipe()
    recipe.loc[five] = remaining.loc[five] / sum_R
    return recipe.astype(float)


def _c3_recipe_key(pair: RedefinitionPair, collision: bool) -> RecipeKey:
    if collision:
        return (pair.source_industry, pair.destination_industry, pair.commodity)
    return (pair.source_industry, pair.destination_industry)


def recover_named_reallocation_recipes(
    V_before: pd.DataFrame,
    U_before: pd.DataFrame,
    U_after: pd.DataFrame,
    VA_before: pd.DataFrame,
    VA_after: pd.DataFrame,
    classification: list[RedefinitionPair],
    recipes: dict[RecipeKey, pd.Series],
) -> dict[RecipeKey, pd.Series]:
    """Recover C3 pair recipes. Caller merges the return value into *recipes*.

    Does not assign ``rule_id``. *U_after* / *VA_after* are published after tables.
    """
    c3_pairs = [
        pair
        for pair in classification
        if pair.rule_id == 'C3' and abs(moved_amount(pair, V_before)) > ATOL
    ]
    if not c3_pairs:
        return {}
    dest_counts = Counter(p.destination_industry for p in c3_pairs)
    source_counts = Counter(p.source_industry for p in c3_pairs)
    sd_counts = Counter((p.source_industry, p.destination_industry) for p in c3_pairs)

    def chosen(pair: RedefinitionPair) -> tuple[str, str]:
        if dest_counts[pair.destination_industry] == 1:
            return 'dest', pair.destination_industry
        if source_counts[pair.source_industry] == 1:
            return 'source', pair.source_industry
        return 'dest', pair.destination_industry

    groups: dict[tuple[str, str], list[RedefinitionPair]] = defaultdict(list)
    for pair in c3_pairs:
        groups[chosen(pair)].append(pair)

    recovered: dict[RecipeKey, pd.Series] = {}
    for (role, industry), group in groups.items():
        remaining = _column_delta(U_before, U_after, VA_before, VA_after, industry)
        group_ids = {
            (p.source_industry, p.commodity, p.destination_industry) for p in group
        }
        for pair in classification:
            if (
                pair.source_industry,
                pair.commodity,
                pair.destination_industry,
            ) in group_ids:
                continue
            if not _touches_column(pair, industry):
                continue
            R = moved_amount(pair, V_before)
            vec = _use_vector_for_unmix(pair, R, U_before, VA_before, recipes)
            remaining = remaining - _unmix_sign(pair, industry) * vec
        for pair in _sort_pairs(group, V_before):
            R = moved_amount(pair, V_before)
            signed = remaining if role == 'dest' else -remaining
            raw = signed / R
            key = _c3_recipe_key(
                pair, sd_counts[(pair.source_industry, pair.destination_industry)] > 1
            )
            recovered[key] = raw.astype(float)
            peel = raw * R
            remaining = remaining - _unmix_sign(pair, industry) * peel
    return recovered


def is_named_reallocation(source: str, dest: str) -> bool:
    """Whether *(source, dest)* is on the named large-reallocation table."""
    return any(
        source in sources and dest in dests
        for sources, dests in NAMED_REALLOCATION_PAIRS
    )


def identify_destination(
    V_before: pd.DataFrame, V_after: pd.DataFrame, commodity: str
) -> str:
    """Industry whose dest-row increase absorbs the Make movement of *commodity*."""
    increase = V_after.loc[:, commodity] - V_before.loc[:, commodity]
    max_val = _usd(increase.max())
    candidates = [
        str(code) for code in increase.index if _usd(increase.loc[code]) == max_val
    ]
    if commodity in candidates:
        return commodity
    return min(candidates)


def classify_make_pairs(
    V_before: pd.DataFrame, V_after: pd.DataFrame
) -> list[RedefinitionPair]:
    """Redefinition pairs from Make off-diagonals that move by more than ``ATOL``."""
    pairs: list[RedefinitionPair] = []
    industries = list(V_before.index)
    commodities = list(V_before.columns)
    for source in industries:
        for commodity in commodities:
            if source == commodity:
                continue
            before = _usd(V_before.loc[source, commodity])
            after = _usd(V_after.loc[source, commodity])
            delta = before - after
            if abs(delta) <= ATOL:
                continue
            dest = identify_destination(V_before, V_after, commodity)
            share = 0.0 if abs(before) <= ATOL else delta / before
            pairs.append(
                RedefinitionPair(
                    source_industry=str(source),
                    commodity=str(commodity),
                    destination_industry=str(dest),
                    share=float(share),
                    delta=float(delta),
                    rule_id='default',
                    va_mix=None,
                )
            )
    return pairs


def _source_intermediate_mass(
    U_before: pd.DataFrame, U_after: pd.DataFrame, source: str
) -> float:
    return _usd((U_after.loc[:, source] - U_before.loc[:, source]).abs().sum())


def _c1_mix_l1(
    VA_before: pd.DataFrame,
    VA_after: pd.DataFrame,
    pair: RedefinitionPair,
    R: float,
    mix: VaMix,
) -> float:
    industry = pair.source_industry if mix == 'source' else pair.destination_industry
    weights = _c1_weights(VA_before, industry)
    if weights is None:
        return float('inf')
    w1 = weights[0]
    predicted_i_comp = -R * w1
    predicted_i_gos = -R * (1.0 - w1)
    predicted_d_comp = R * w1
    predicted_d_gos = R * (1.0 - w1)
    i, d = pair.source_industry, pair.destination_industry
    pub_i_comp = _usd(VA_after.loc[COMPENSATION, i]) - _usd(
        VA_before.loc[COMPENSATION, i]
    )
    pub_i_gos = _usd(VA_after.loc[GOS, i]) - _usd(VA_before.loc[GOS, i])
    pub_d_comp = _usd(VA_after.loc[COMPENSATION, d]) - _usd(
        VA_before.loc[COMPENSATION, d]
    )
    pub_d_gos = _usd(VA_after.loc[GOS, d]) - _usd(VA_before.loc[GOS, d])
    return (
        abs(predicted_i_comp - pub_i_comp)
        + abs(predicted_i_gos - pub_i_gos)
        + abs(predicted_d_comp - pub_d_comp)
        + abs(predicted_d_gos - pub_d_gos)
    )


def assign_wholesale_retail_rules(
    pairs: list[RedefinitionPair],
    V_before: pd.DataFrame,
    U_before: pd.DataFrame,
    U_after: pd.DataFrame,
    VA_before: pd.DataFrame,
    VA_after: pd.DataFrame,
) -> list[RedefinitionPair]:
    """Label wholesale/retail margin redefinitions ``C1`` and pick ``va_mix``."""
    labeled: list[RedefinitionPair] = []
    for pair in pairs:
        R = moved_amount(pair, V_before)
        if abs(R) <= ATOL:
            labeled.append(pair)
            continue
        if (
            pair.source_industry in TRADE_DEST
            or pair.destination_industry not in TRADE_DEST
        ):
            labeled.append(pair)
            continue
        intermediate = _source_intermediate_mass(
            U_before, U_after, pair.source_industry
        )
        if intermediate / abs(R) > C1_INTERMEDIATE_CUTOFF:
            labeled.append(pair)
            continue
        source_l1 = _c1_mix_l1(VA_before, VA_after, pair, R, 'source')
        dest_l1 = _c1_mix_l1(VA_before, VA_after, pair, R, 'dest')
        va_mix: VaMix = 'dest' if dest_l1 <= source_l1 else 'source'
        labeled.append(
            RedefinitionPair(
                source_industry=pair.source_industry,
                commodity=pair.commodity,
                destination_industry=pair.destination_industry,
                share=pair.share,
                delta=pair.delta,
                rule_id='C1',
                va_mix=va_mix,
            )
        )
    return labeled


def assign_named_reallocation_rules(
    pairs: list[RedefinitionPair], V_before: pd.DataFrame
) -> list[RedefinitionPair]:
    """Label pairs on the named large-reallocation table ``C3``."""
    labeled: list[RedefinitionPair] = []
    for pair in pairs:
        R = moved_amount(pair, V_before)
        if abs(R) <= ATOL or pair.destination_industry == SOFTWARE_PUBLISHERS:
            labeled.append(pair)
            continue
        if not is_named_reallocation(pair.source_industry, pair.destination_industry):
            labeled.append(pair)
            continue
        labeled.append(
            RedefinitionPair(
                source_industry=pair.source_industry,
                commodity=pair.commodity,
                destination_industry=pair.destination_industry,
                share=pair.share,
                delta=pair.delta,
                rule_id='C3',
                va_mix=None,
            )
        )
    return labeled


def assign_own_account_software_rules(
    pairs: list[RedefinitionPair],
    V_before: pd.DataFrame,
    recipe: pd.Series | None,
) -> list[RedefinitionPair]:
    """Label dest-``511200`` pairs ``C2`` when a stable software recipe exists."""
    if recipe is None:
        return list(pairs)
    labeled: list[RedefinitionPair] = []
    for pair in pairs:
        R = moved_amount(pair, V_before)
        if pair.destination_industry == SOFTWARE_PUBLISHERS and abs(R) > ATOL:
            labeled.append(
                RedefinitionPair(
                    source_industry=pair.source_industry,
                    commodity=pair.commodity,
                    destination_industry=pair.destination_industry,
                    share=pair.share,
                    delta=pair.delta,
                    rule_id='C2',
                    va_mix=None,
                )
            )
        else:
            labeled.append(pair)
    return labeled


def classify_redefinitions(
    V_before: pd.DataFrame,
    V_after: pd.DataFrame,
    U_before: pd.DataFrame,
    U_after: pd.DataFrame,
    VA_before: pd.DataFrame,
    VA_after: pd.DataFrame,
) -> tuple[list[RedefinitionPair], dict[RecipeKey, pd.Series]]:
    """Classify 2017 Make pairs and recover C2/C3 recipes.

    Does not call :func:`apply_redefinitions`.
    """
    pairs = classify_make_pairs(V_before, V_after)
    pairs = assign_wholesale_retail_rules(
        pairs, V_before, U_before, U_after, VA_before, VA_after
    )
    pairs = assign_named_reallocation_rules(pairs, V_before)
    software = recover_own_account_software_recipe(
        V_before, U_before, U_after, VA_before, VA_after, pairs
    )
    pairs = assign_own_account_software_rules(pairs, V_before, software)
    recipes: dict[RecipeKey, pd.Series] = {}
    if software is not None:
        recipes['C2'] = software
    recipes.update(
        recover_named_reallocation_recipes(
            V_before,
            U_before,
            U_after,
            VA_before,
            VA_after,
            pairs,
            recipes,
        )
    )
    return pairs, recipes


def write_classification(
    pairs: Sequence[RedefinitionPair], path: Path = CLASSIFICATION_PATH
) -> Path:
    """Write *pairs* to the classification CSV schema."""
    rows = [
        {
            'source_industry': pair.source_industry,
            'commodity': pair.commodity,
            'destination_industry': pair.destination_industry,
            'share': pair.share,
            'delta': pair.delta,
            'rule_id': pair.rule_id,
            'va_mix': '' if pair.va_mix is None else pair.va_mix,
        }
        for pair in pairs
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def load_classification(path: Path = CLASSIFICATION_PATH) -> list[RedefinitionPair]:
    """Load redefinition pairs from the tracked classification CSV."""
    frame = pd.read_csv(path, dtype={col: str for col in _CLASSIFICATION_STR_COLS})
    pairs: list[RedefinitionPair] = []
    for row in frame.itertuples(index=False):
        va_mix_raw = '' if pd.isna(row.va_mix) else str(row.va_mix)
        va_mix: VaMix | None
        if va_mix_raw in ('', 'None'):
            va_mix = None
        else:
            va_mix = va_mix_raw  # type: ignore[assignment]
        pairs.append(
            RedefinitionPair(
                source_industry=str(row.source_industry),
                commodity=str(row.commodity),
                destination_industry=str(row.destination_industry),
                share=_usd(row.share),
                delta=_usd(row.delta),
                rule_id=str(row.rule_id),  # type: ignore[arg-type]
                va_mix=va_mix,
            )
        )
    return pairs


def write_recipes(
    recipes: Mapping[RecipeKey, pd.Series], path: Path = RECIPES_PATH
) -> Path:
    """Write nonzero recipe shares in long CSV form."""
    rows: list[dict[str, str | float]] = []
    for key, series in recipes.items():
        if key == 'C2':
            key_kind, source, dest, commodity = 'C2', '', '', ''
        elif isinstance(key, tuple) and len(key) == 3:
            key_kind, source, dest, commodity = 'C3', key[0], key[1], key[2]
        elif isinstance(key, tuple) and len(key) == 2:
            key_kind, source, dest, commodity = 'C3', key[0], key[1], ''
        else:
            raise ValueError(f'unrecognised recipe key {key!r}')
        for row_code, share in series.items():
            if _usd(share) == 0.0:
                continue
            rows.append(
                {
                    'key_kind': key_kind,
                    'source_industry': source,
                    'destination_industry': dest,
                    'commodity': commodity,
                    'row_code': str(row_code),
                    'share': _usd(share),
                }
            )
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        'key_kind',
        'source_industry',
        'destination_industry',
        'commodity',
        'row_code',
        'share',
    ]
    pd.DataFrame(rows, columns=columns).to_csv(path, index=False)
    return path


def recipe_integrity_holds(recipe: pd.Series, R: float) -> bool:
    """Whether ``(recipe * R).sum()`` recovers *R* within ``ATOL``."""
    return abs(_usd((recipe * R).sum()) - R) <= ATOL


def log_recipe_integrity(
    pairs: Sequence[RedefinitionPair],
    recipes: Mapping[RecipeKey, pd.Series],
    V_before: pd.DataFrame,
) -> None:
    """Log C2/C3 recipes that do not recover ``R``. Stored shares are not renormalized."""
    c2_pairs = [pair for pair in pairs if pair.rule_id == 'C2']
    if 'C2' in recipes and c2_pairs:
        sum_R = sum(moved_amount(pair, V_before) for pair in c2_pairs)
        if not recipe_integrity_holds(recipes['C2'], sum_R):
            logger.info(
                'C2 recipe fails integrity at sum_R=%.6g; storing un-normalized shares',
                sum_R,
            )
    for pair in pairs:
        if pair.rule_id != 'C3' or abs(moved_amount(pair, V_before)) <= ATOL:
            continue
        try:
            recipe = _lookup_c3_recipe(pair, recipes)
        except ValueError:
            continue
        R = moved_amount(pair, V_before)
        if not recipe_integrity_holds(recipe, R):
            logger.info(
                'C3 recipe fails integrity for %s -> %s commodity %s; '
                'storing un-normalized shares',
                pair.source_industry,
                pair.destination_industry,
                pair.commodity,
            )


def load_recipes(path: Path = RECIPES_PATH) -> dict[RecipeKey, pd.Series]:
    """Load recipes from the tracked long CSV. Does not re-check integrity."""
    frame = pd.read_csv(path, dtype={col: str for col in _RECIPE_STR_COLS})
    if frame.empty:
        return {}
    grouped: dict[RecipeKey, pd.Series] = {}
    for _, row in frame.iterrows():
        kind = str(row['key_kind'])
        source = '' if pd.isna(row['source_industry']) else str(row['source_industry'])
        dest = (
            ''
            if pd.isna(row['destination_industry'])
            else str(row['destination_industry'])
        )
        commodity = '' if pd.isna(row['commodity']) else str(row['commodity'])
        if kind == 'C2':
            key: RecipeKey = 'C2'
        elif commodity:
            key = (source, dest, commodity)
        else:
            key = (source, dest)
        series = grouped.setdefault(key, empty_recipe())
        series.loc[str(row['row_code'])] = _usd(row['share'])
    return grouped


def write_overlay(overlay: RedefinitionOverlay, directory: Path = _REDEF_DIR) -> None:
    """Write the four overlay parquets next to the analysis module."""
    directory.mkdir(parents=True, exist_ok=True)
    overlay.U.to_parquet(directory / OVERLAY_U_PATH.name)
    overlay.VA.to_parquet(directory / OVERLAY_VA_PATH.name)
    overlay.Uimp.to_parquet(directory / OVERLAY_UIMP_PATH.name)
    overlay.margins.to_parquet(directory / OVERLAY_MARGINS_PATH.name)


def load_overlay(directory: Path = _REDEF_DIR) -> RedefinitionOverlay:
    """Load the four tracked overlay parquets, keeping indexes."""
    return RedefinitionOverlay(
        U=pd.read_parquet(directory / OVERLAY_U_PATH.name),
        VA=pd.read_parquet(directory / OVERLAY_VA_PATH.name),
        Uimp=pd.read_parquet(directory / OVERLAY_UIMP_PATH.name),
        margins=pd.read_parquet(directory / OVERLAY_MARGINS_PATH.name),
    )


def _align_subtract(published: pd.DataFrame, algorithm: pd.DataFrame) -> pd.DataFrame:
    left, right = published.align(algorithm, fill_value=0.0)
    return left.astype(float) - right.astype(float)


def compute_redefinition_overlay(
    V_before: pd.DataFrame,
    U_before: pd.DataFrame,
    VA_before: pd.DataFrame,
    Uimp_before: pd.DataFrame,
    margins_before: pd.DataFrame,
    *,
    classification: list[RedefinitionPair],
    recipes: dict[RecipeKey, pd.Series],
    U_published_after: pd.DataFrame,
    VA_published_after: pd.DataFrame,
    Uimp_published_after: pd.DataFrame,
    margins_published_after: pd.DataFrame,
) -> RedefinitionOverlay:
    """Residual overlay: published after minus the algorithm without C6."""
    _, U_alg, VA_alg, Uimp_alg, margins_alg = apply_redefinitions(
        V_before,
        U_before,
        VA_before,
        Uimp_before,
        margins_before,
        classification=classification,
        recipes=recipes,
        overlay=None,
        rules=FULL - {'C6'},
    )
    return RedefinitionOverlay(
        U=_align_subtract(U_published_after, U_alg),
        VA=_align_subtract(VA_published_after, VA_alg),
        Uimp=_align_subtract(Uimp_published_after, Uimp_alg),
        margins=_align_subtract(margins_published_after, margins_alg),
    )


def _split_recipe(
    vec: pd.Series, U: pd.DataFrame, VA: pd.DataFrame
) -> tuple[pd.Series, pd.Series]:
    return vec.reindex(U.index).fillna(0.0), vec.reindex(VA.index).fillna(0.0)


def _would_go_negative(
    source_U: pd.Series,
    source_VA: pd.Series,
    vec_U: pd.Series,
    vec_VA: pd.Series,
) -> bool:
    return bool(
        ((source_U - vec_U) < -ATOL).any() or ((source_VA - vec_VA) < -ATOL).any()
    )


def _apply_vector(
    U_after: pd.DataFrame,
    VA_after: pd.DataFrame,
    source: str,
    dest: str,
    vec: pd.Series,
) -> tuple[pd.Series, pd.Series]:
    vec_U, vec_VA = _split_recipe(vec, U_after, VA_after)
    U_after.loc[:, source] = U_after.loc[:, source] - vec_U
    U_after.loc[:, dest] = U_after.loc[:, dest] + vec_U
    VA_after.loc[:, source] = VA_after.loc[:, source] - vec_VA
    VA_after.loc[:, dest] = VA_after.loc[:, dest] + vec_VA
    return vec_U, vec_VA


def _repair_negative_source(
    U_after: pd.DataFrame,
    VA_after: pd.DataFrame,
    pair: RedefinitionPair,
    R: float,
    b: pd.Series,
) -> pd.Series:
    """Replace dest-B for *pair*. Returns the commodity ΔU leaving the source."""
    i, d = pair.source_industry, pair.destination_industry
    takes = empty_recipe()
    row_codes = list(U_after.index) + list(VA_after.index)
    for code in row_codes:
        want = R * _usd(b.loc[code]) if code in b.index else 0.0
        if code in U_after.index:
            available = _usd(U_after.loc[code, i])
        else:
            available = _usd(VA_after.loc[code, i])
        if want > 0:
            take = max(0.0, min(available + ATOL, want))
        else:
            take = want
        takes.loc[str(code)] = take
        if code in U_after.index:
            U_after.loc[code, i] = _usd(U_after.loc[code, i]) - take
            U_after.loc[code, d] = _usd(U_after.loc[code, d]) + take
        else:
            VA_after.loc[code, i] = _usd(VA_after.loc[code, i]) - take
            VA_after.loc[code, d] = _usd(VA_after.loc[code, d]) + take
    shortfall = R - _usd(takes.sum())
    b_VA = b.reindex(VA_after.index).fillna(0.0)
    if abs(shortfall) > ATOL:
        # b_VA is shares; compare the USD dest-VA mass, not the share sum to ATOL.
        if abs(_usd(b_VA.sum()) * R) <= ATOL:
            logger.info(
                'C4 shortfall %.6g left for overlay (%s -> %s, %s)',
                shortfall,
                i,
                d,
                pair.commodity,
            )
        else:
            sink = shortfall * (b_VA / _usd(b_VA.sum()))
            VA_after.loc[:, d] = VA_after.loc[:, d] + sink
            logger.info(
                'C4 repaired %s -> %s commodity %s shortfall %.6g into dest VA',
                i,
                d,
                pair.commodity,
                shortfall,
            )
    return takes.reindex(U_after.index).fillna(0.0)


def _ensure_margin_row(
    margins: pd.DataFrame, buyer: str, commodity: str
) -> pd.DataFrame:
    key = (buyer, commodity)
    if key in margins.index:
        return margins
    extra = pd.DataFrame(
        0.0,
        index=pd.MultiIndex.from_tuples([key], names=list(margins.index.names)),
        columns=margins.columns,
    )
    return pd.concat([margins, extra])


def _move_imports(
    Uimp_after: pd.DataFrame,
    Uimp_before: pd.DataFrame,
    U_before: pd.DataFrame,
    delta_U: pd.Series,
    source: str,
    dest: str,
) -> None:
    for commodity in U_before.index:
        u_cell = _usd(U_before.loc[commodity, source])
        intensity = (
            _usd(Uimp_before.loc[commodity, source]) / u_cell
            if abs(u_cell) > ATOL
            else 0.0
        )
        moved = intensity * _usd(delta_U.loc[commodity])
        Uimp_after.loc[commodity, source] = (
            _usd(Uimp_after.loc[commodity, source]) - moved
        )
        Uimp_after.loc[commodity, dest] = _usd(Uimp_after.loc[commodity, dest]) + moved


def _move_margins(
    margins_after: pd.DataFrame,
    margins_before: pd.DataFrame,
    U_before: pd.DataFrame,
    delta_U: pd.Series,
    source: str,
    dest: str,
) -> pd.DataFrame:
    result = margins_after
    for commodity in U_before.index:
        u_cell = _usd(U_before.loc[commodity, source])
        frac = _usd(delta_U.loc[commodity]) / u_cell if abs(u_cell) > ATOL else 0.0
        if frac == 0.0:
            continue
        src_key = (source, commodity)
        if src_key in margins_before.index:
            row = margins_before.loc[src_key]
        else:
            row = pd.Series(0.0, index=margins_before.columns)
        result = _ensure_margin_row(result, source, commodity)
        result = _ensure_margin_row(result, dest, commodity)
        moved = frac * row.astype(float)
        result.loc[src_key] = result.loc[src_key].astype(float) - moved
        result.loc[(dest, commodity)] = (
            result.loc[(dest, commodity)].astype(float) + moved
        )
    return result


def _add_overlay(
    U_after: pd.DataFrame,
    VA_after: pd.DataFrame,
    Uimp_after: pd.DataFrame,
    margins_after: pd.DataFrame,
    overlay: RedefinitionOverlay,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    U_after = U_after.add(overlay.U, fill_value=0.0)
    VA_after = VA_after.add(overlay.VA, fill_value=0.0)
    Uimp_after = Uimp_after.add(overlay.Uimp, fill_value=0.0)
    left, right = margins_after.align(overlay.margins, fill_value=0.0)
    margins_after = left.astype(float) + right.astype(float)
    return U_after, VA_after, Uimp_after, margins_after


def _scrub_float_dust(frame: pd.DataFrame) -> pd.DataFrame:
    """Zero residual float dust so table_match does not count it as EXTRA."""
    numeric = frame.astype(float)
    return numeric.mask(numeric.abs() < 1e-3, 0.0)


def apply_redefinitions(
    V_before: pd.DataFrame,
    U_before: pd.DataFrame,
    VA_before: pd.DataFrame,
    Uimp_before: pd.DataFrame,
    margins_before: pd.DataFrame,
    *,
    classification: list[RedefinitionPair],
    recipes: dict[RecipeKey, pd.Series],
    overlay: RedefinitionOverlay | None = None,
    rules: RuleSet = FULL,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Redefine Make and reallocate Use, VA, imports, and margins.

    *classification*, *recipes*, and *overlay* are 2017-learned structure.
    Amounts ``R`` and destination recipes come from the input tables.
    """
    if 'C6' in rules and overlay is None:
        raise ValueError("overlay is required when 'C6' is in rules")
    V_after = V_before.copy()
    U_after = U_before.copy()
    VA_after = VA_before.copy()
    Uimp_after = Uimp_before.copy()
    margins_after = margins_before.copy()

    ordered = _sort_pairs(classification, V_before)
    for pair in ordered:
        R = moved_amount(pair, V_before)
        i, d = pair.source_industry, pair.destination_industry
        V_after.loc[i, pair.commodity] = _usd(V_after.loc[i, pair.commodity]) - R
        V_after.loc[d, pair.commodity] = _usd(V_after.loc[d, pair.commodity]) + R

    if 'default' not in rules:
        if 'C6' in rules and overlay is not None:
            U_after, VA_after, Uimp_after, margins_after = _add_overlay(
                U_after, VA_after, Uimp_after, margins_after, overlay
            )
        return (
            V_after,
            _scrub_float_dust(U_after),
            _scrub_float_dust(VA_after),
            _scrub_float_dust(Uimp_after),
            _scrub_float_dust(margins_after),
        )

    frozen_b: dict[str, pd.Series] = {}
    for pair in ordered:
        dest = pair.destination_industry
        if dest not in frozen_b:
            frozen_b[dest] = destination_industry_recipe(U_before, VA_before, dest)

    for pair in ordered:
        R = moved_amount(pair, V_before)
        i, d = pair.source_industry, pair.destination_industry
        special = (
            pair.rule_id
            if pair.rule_id in {'C1', 'C2', 'C3'} and pair.rule_id in rules
            else None
        )
        operator = special or 'default'
        b = frozen_b[d]
        delta_U = pd.Series(0.0, index=U_after.index)
        applied = False

        if abs(R) <= ATOL:
            continue

        if operator == 'C1':
            industry = i if pair.va_mix == 'source' else d
            weights = _c1_weights(VA_before, industry)
            if weights is None:
                logger.info('skipping C1 pair %s -> %s: VA denom <= ATOL', i, d)
                continue
            vec = _c1_vector(R, weights[0])
            vec_U, vec_VA = _split_recipe(vec, U_after, VA_after)
            if _would_go_negative(U_after.loc[:, i], VA_after.loc[:, i], vec_U, vec_VA):
                logger.info('skipping C1 pair %s -> %s: would go negative', i, d)
                continue
            _apply_vector(U_after, VA_after, i, d, vec)
            applied = True
        elif operator == 'C2':
            if 'C2' not in recipes:
                raise ValueError('C2 recipe missing for a pair labeled C2')
            vec = recipes['C2'] * R
            vec_U, vec_VA = _split_recipe(vec, U_after, VA_after)
            if _would_go_negative(U_after.loc[:, i], VA_after.loc[:, i], vec_U, vec_VA):
                logger.info('skipping C2 pair %s -> %s: would go negative', i, d)
                continue
            _apply_vector(U_after, VA_after, i, d, vec)
            delta_U = vec_U
            applied = True
        elif operator == 'C3':
            vec = _lookup_c3_recipe(pair, recipes) * R
            vec_U, vec_VA = _split_recipe(vec, U_after, VA_after)
            if _would_go_negative(U_after.loc[:, i], VA_after.loc[:, i], vec_U, vec_VA):
                logger.info('skipping C3 pair %s -> %s: would go negative', i, d)
                continue
            _apply_vector(U_after, VA_after, i, d, vec)
            delta_U = vec_U
            applied = True
        else:
            x_d = destination_industry_output(U_before, VA_before, d)
            if abs(x_d) <= ATOL:
                logger.info('skipping dest-B pair %s -> %s: dest output <= ATOL', i, d)
                continue
            vec = b * R
            vec_U, vec_VA = _split_recipe(vec, U_after, VA_after)
            if _would_go_negative(U_after.loc[:, i], VA_after.loc[:, i], vec_U, vec_VA):
                if 'C4' in rules:
                    delta_U = _repair_negative_source(U_after, VA_after, pair, R, b)
                    applied = True
                else:
                    logger.info(
                        'skipping dest-B pair %s -> %s: would go negative', i, d
                    )
                    continue
            else:
                _apply_vector(U_after, VA_after, i, d, vec)
                delta_U = vec_U
                applied = True
                moved_U = _usd(vec_U.sum()) + _usd(vec_VA.sum())
                if abs(moved_U - R) > ATOL:
                    raise AssertionError(
                        f'dest-B identity failed for {i} -> {d}: moved {moved_U}, R={R}'
                    )

        if applied and operator != 'C1':
            _move_imports(Uimp_after, Uimp_before, U_before, delta_U, i, d)
            margins_after = _move_margins(
                margins_after, margins_before, U_before, delta_U, i, d
            )

    if 'C6' in rules and overlay is not None:
        U_after, VA_after, Uimp_after, margins_after = _add_overlay(
            U_after, VA_after, Uimp_after, margins_after, overlay
        )
    return (
        V_after,
        _scrub_float_dust(U_after),
        _scrub_float_dust(VA_after),
        _scrub_float_dust(Uimp_after),
        _scrub_float_dust(margins_after),
    )
