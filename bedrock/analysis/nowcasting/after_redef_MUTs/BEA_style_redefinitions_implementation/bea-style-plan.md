# Issue #572 — implementation plan

This is the **source of truth** for [Step 7: apply redefinitions to Make, Use, Import matrix and Margins in BEA detail space](https://github.com/cornerstone-data/bedrock/issues/572), interpreted as:

1. Follow BEA's redefinitions / reallocations algorithm (IO manual Ch. 4 and 9).
2. Identify if and how that algorithm fails to recreate the published 2017 before→after detail tables.
3. Add residual rules until the 2017 after-redefinitions detail tables match, if (1) is not enough.

It does **not** start from the issue's current “per-cell 2017 ratios” wording. That method round-trips 2017 by construction and does not implement the manual. See `summary.md` §4.2–4.3. Where `summary.md` still mentions a leftover-input C5, “clip and renormalize,” picking Import/Margins A vs B, or editing #572/`plan.md`, **this file wins**.

**Do not wait on Step 6** to start Phases A–D. The 2017 before/after loaders in `bedrock/extract/iot/io_2017.py` are the fixture. Step 6 is required only to apply the transform to a nowcast year (Phase E).

---

## Constraints inherited from the nowcast plan

- Apply in **BEA 2017 Detail** space, before Cornerstone collapse (Step 8).
- **One** redefinition point. No VA-specific path in Steps 1–6.
- **Totals cannot validate.** Score cell by cell with `table_match`.
- Acceptance still includes the four Use-intermediate magnitudes from `About_BEA_IOT_table_valuation_differences.md`:

  | check                          | value            |
  | ------------------------------ | ---------------- |
  | intermediate cells that differ | 5,740 of 161,604 |
  | gross movement                 | 553,635 million  |
  | largest single cell shift      | 42,893           |
  | net                            | −7               |

  After USD conversion, a cell “differs” when `abs(delta) > ATOL`. Integer million source cells of 1 are `$1e6` > ATOL, so these four numbers must match the About doc exactly when reported in million USD.

- Branch off `nowcast`. Target PRs at `nowcast`, not `main`.
- **Units:** all transform objects and overlays are in **USD** (loader units). Divide by `MILLION_CURRENCY_TO_CURRENCY` (`1e6`) only when reporting the four magnitude numbers or writing a human table.
- **Do not rewrite** `plan.md` or edit GitHub #572 in this PR. Land A–D first.
- **Skip a Y-before loader** in this PR. Chapter 9 does not move FD; `load_2017_Ytot_usa` being after-only is enough.
- **Keep** `load_2017_value_added_usa` after-only. Add `load_2017_value_added_before_redef_usa` beside it. Do not make the generic name config-switched. Phase A asserts the before-redef Use workbook actually contains `V00100` / `V00200` / `V00300`.
- **Names and docstrings stand alone.** Every public function, class, constant, CLI flag, and section callable — and its docstring — must be readable without this plan or any other external doc. Do not name callables after plan numbering (`phase_a_census`, `apply_c4`, `run_phase_2`). Prefer BEA / table language (`apply_redefinitions`, `destination_industry_recipe`, `wholesale_margin_reallocation`, `own_account_software_recipe`, `residual_overlay`). Names in that sentence are **examples of style**, not required extra exports. C4 stays inline inside `apply_redefinitions` (no public `repair_negative_source_inputs`). Docstrings state what the object does, its arguments, and its units; they do not say “see Phase B” or “C4 in the implementation plan.” Stored `rule_id` tokens (`default` / `C1` / `C2` / `C3`) and this file’s headings may keep plan IDs; the Python API must not require opening this file to understand a name.

---

## Contracts (types the transform consumes)

These live in `bedrock/transform/iot/nowcast_redefinitions.py` (dataclasses). Phase A writes them to the pinned paths below; load helpers in the same module read them. `apply_redefinitions` stays **argument-only** — it never opens a path.

Rounding bar — define **in the transform module**, do not import `ROUNDING_ATOL` from `sections.py` into `nowcast_redefinitions.py`. Phase D’s `table_match` call may use `sections.ROUNDING_ATOL`; they are the same number:

```text
# bedrock/transform/iot/nowcast_redefinitions.py
ATOL = 0.5 * MILLION_CURRENCY_TO_CURRENCY   # $0.5M
```

A cell “moves” when `abs(delta) > ATOL`. Do not use float `!= 0`.

### On-disk artifacts (pinned)

Root `.gitignore` has global `*.csv` / `*.parquet`. Artifacts must **not** go in `output/` (gitignored). Follow the trade-data negation pattern. Add these lines to the repo-root `.gitignore`:

```text
!bedrock/analysis/nowcasting/redefinitions_2017_classification.csv
!bedrock/analysis/nowcasting/redefinitions_2017_recipes.csv
!bedrock/analysis/nowcasting/redefinitions_2017_overlay_U.parquet
!bedrock/analysis/nowcasting/redefinitions_2017_overlay_VA.parquet
!bedrock/analysis/nowcasting/redefinitions_2017_overlay_Uimp.parquet
!bedrock/analysis/nowcasting/redefinitions_2017_overlay_margins.parquet
```

| Artifact | Path | Git |
| --- | --- | --- |
| classification | `bedrock/analysis/nowcasting/redefinitions_2017_classification.csv` | tracked |
| recipes | `bedrock/analysis/nowcasting/redefinitions_2017_recipes.csv` | tracked |
| overlay U | `bedrock/analysis/nowcasting/redefinitions_2017_overlay_U.parquet` | tracked |
| overlay VA | `bedrock/analysis/nowcasting/redefinitions_2017_overlay_VA.parquet` | tracked |
| overlay Uimp | `bedrock/analysis/nowcasting/redefinitions_2017_overlay_Uimp.parquet` | tracked |
| overlay margins | `bedrock/analysis/nowcasting/redefinitions_2017_overlay_margins.parquet` | tracked |
| leftover-cell report | `bedrock/analysis/nowcasting/output/redefinitions_2017_leftover_cells.csv` | **not** tracked (`output/`) |
| `DEFAULT_ONLY` residual | `bedrock/analysis/nowcasting/output/redefinitions_2017_default_residual.csv` | **not** tracked |

Constants for those six tracked paths live next to the load helpers in `nowcast_redefinitions.py` (as `pathlib.Path` relative to the repo, constructed from `__file__`).

### `RedefinitionPair`

One Make off-diagonal that is a redefinition.

| Field | Type | Rule |
| --- | --- | --- |
| `source_industry` | `str` | BEA detail industry code |
| `commodity` | `str` | BEA detail commodity code |
| `destination_industry` | `str` | **Always stored.** Usually equals `commodity` at detail. Phase A records any exception; the Make operator uses this field, not `commodity` |
| `share` | `float` | **Unclipped** `delta / V_before[source, commodity]` with `delta = V_before − V_after`. Do not clip to `[0, 1]` — that breaks the Make gate if `V_after < 0` (share `> 1`), the cell grew (share `< 0`), or `V_before ≤ ATOL`. If `abs(V_before) ≤ ATOL`, store `share = 0`. Clip to `[0, 1]` only in human reports |
| `delta` | `float` | USD. Always `V_before_2017[i,c] − V_after_2017[i,c]`. Diagnostic / census field. **The transform does not use `delta` to compute `R`** (see R rule below) |
| `rule_id` | `'default' \| 'C1' \| 'C2' \| 'C3'` | Assigned in Phase A from dest-code lists and the 2017 Use-delta shape (see C-rules). **No `'C4'` on disk.** C4 is runtime over dest-`B` when `'C4' in rules`. No C5 |
| `va_mix` | `'source' \| 'dest' \| None` | C1 only. Phase A scores both mixes by L1 of the four cells `|ΔV00100[i]| + |ΔV00300[i]| + |ΔV00100[d]| + |ΔV00300[d]|` under that mix vs published; stores the smaller; tie → `'dest'`. Other rules: `None` |

`classification` is `list[RedefinitionPair]`.

**CSV schema** (`redefinitions_2017_classification.csv`): header + one row per pair.

| column | dtype | notes |
| --- | --- | --- |
| `source_industry` | str | |
| `commodity` | str | |
| `destination_industry` | str | |
| `share` | float | unclipped |
| `delta` | float | USD |
| `rule_id` | str | `default` / `C1` / `C2` / `C3` |
| `va_mix` | str | `source` / `dest` / empty string `""` for `None`. Write `""`, never the literals `"None"` or NaN |

### R rule (pure; no `year=`)

```text
if abs(V_before[i, c]) > ATOL:
    R = pair.share * V_before[i, c]
else:
    R = 0.0
```

Do **not** apply stored `delta` when the current source cell is empty. On 2017 that means a pair with `|V_before| ≤ ATOL` and `|delta| > ATOL` contributes `R = 0` through the Make operator; those dollars are the Make column-sum leftover in the Make gate below, not a hidden 2017-only branch.

### `Recipe`

A column of **shares** (not USD). Index = `USA_2017_COMMODITY_CODES` + `USA_2017_VALUE_ADDED_CODES` (`V00100`, `V00200`, `V00300`). Stored as `dict[RecipeKey, pd.Series]`. Do not hardcode 402 / 405 / 408; use those taxonomy constants (and the loader index/columns for table shapes).

Integrity check **runs once at recovery/write**, not in `load_recipes` or `apply_redefinitions`. When it runs: `(recipe * R).sum() ≈ R` within `ATOL`. Do **not** use `sum(recipe) ≈ 1 ± ATOL / max(R, 1)` — that tolerance explodes for small `R`. C2 uses the pooled form `(recipe * sum_R).sum() ≈ sum_R` within `ATOL` (`sum_R` = sum of `R` over C2 pairs). If a recovered C2/C3 recipe fails that check, **store it un-normalized and skip integrity for that key** (see C3 recovery). Do not renormalize. No extra CSV flag column. Apply uses the stored shares as written. Tests of the identity fallback assert apply uses those shares, not a renormalized copy.

```text
RecipeKey = str | tuple[str, str] | tuple[str, str, str]
```

`RecipeKey` is either a `rule_id` (`'C2'`; `'default'` is not stored — default `B` is computed from the year-`t` dest column) or a `(source_industry, destination_industry)` pair for C3. If Phase A finds two C3 commodities that share the same `(source, dest)`, key by `(source, dest, commodity)` instead.

**CSV schema** (`redefinitions_2017_recipes.csv`): long format, one row per `(key, row_code)`.

| column | dtype | notes |
| --- | --- | --- |
| `key_kind` | str | `C2` or `C3` |
| `source_industry` | str | empty for C2 |
| `destination_industry` | str | empty for C2 |
| `commodity` | str | empty unless C3 collision forced a 3-tuple key |
| `row_code` | str | commodity or VA code; only **nonzero-share** rows (the rest of the taxonomy index are 0) |
| `share` | float | |

`load_recipes()` rebuilds each `Series` on the full commodity+VA index, zeros elsewhere. It does **not** re-run the integrity check. Read with `dtype=str` on `key_kind`, `source_industry`, `destination_industry`, `commodity`, `row_code` (same reason as `load_classification`: `721000` / `233240` / `511200` must stay strings or apply lookup misses).

### C2 five-input recipe

Not dest-`B`. One `Recipe` keyed `'C2'`, the same for every own-account-software pair.

Pinned codes:

| Role | Code | Notes |
| --- | --- | --- |
| Compensation | `V00100` | Fixed |
| Depreciation / CFC | `V00300` | Fifth VA/GOS row. Algorithm: `V00300` if `abs(remaining[V00300]) > ATOL`; else the other VA row with `abs(remaining) > ATOL` (expected `V00200`). If **both** `V00200` and `V00300` exceed `ATOL`, store `V00300` as the fifth row and leave the other VA row to C6 (recipe stays five rows). If neither exceeds `ATOL`, strip is not stable → no C2 recipe |
| Electricity, rent, office supplies | three Use commodity codes | After dest-`B`/C1 unmix on dest `511200`, the three largest `abs(remaining)` Use commodity rows. Tie for third (or any place): lexicographically smallest commodity code (same style as Make dest ties). Working hypotheses only: `221100` electricity; a real-estate/rental commodity; an office-supply commodity. **C2 uses those three remaining rows, not these English names and not “common across source pairs”** |

Dest for C2 pairs is `511200` (Software publishers). If Phase A finds a stable five-input strip (below), **every** classified pair with dest `511200` and `abs(R) > ATOL` gets `rule_id='C2'` (the strip is a dest-column property, not a per-pair vote). If it does not, all those pairs stay `default` and no C2 recipe is written. Do not try C3 for them (C3 dest sets do not include `511200`).

**C2 share recovery (pinned).** One recipe, five nonzero rows. Recover **before** C3 (so C3 can subtract C2; C2 must not wait on C3). Recover from dest column `511200`: start from published `(ΔU, ΔVA)` on that column with `Δ = published_after − published_before` (same convention as C3; **not** Make `delta = V_before − V_after`).

Inside `recover_own_account_software_recipe`, **C2 candidates** are dest `511200` and `abs(R) > ATOL`. Ignore `rule_id` (it is still `default` at this step). If that set is empty, return `None` (do not divide by `sum_R`). `sum_R` is the sum of `R` over those candidates. Unmix subtracts dest-`B` and C1 pairs that touch `511200` **except** those candidates. Do **not** subtract C3. Unmix contribution is **0** whenever apply would skip that pair (`abs(x_{other.d}) ≤ ATOL`, or C1 `denom ≤ ATOL`) — do not divide and pollute the recipe.

The five stored rows **are** `V00100`, the fifth VA/GOS row (algorithm in the table above), and the three largest `abs(remaining)` Use commodity rows (lexicographic tie-break). A strip is **stable** when each of those five has `abs(remaining) > ATOL`; otherwise no C2 recipe and dest-`511200` pairs stay `default`. Then `share[k] = remaining[k] / sum_R`. Other rows of `remaining` go to C6. If `(recipe * sum_R).sum()` is not within `ATOL` of `sum_R`, store the un-normalized five shares and skip integrity at write (no CSV flag; apply uses stored shares). Do not take a per-pair mean of `Δ/R`. C2 apply is the same dest-positive two-liner as C3 (`vec = recipes['C2'] * R`; source `-=`, dest `+=`). A `rule_id='C2'` pair with `'C2'` absent from `recipes` is a Phase A bug — raise `ValueError`; do not silently dest-`B`.

### C1 dest codes (explicit; not NAICS prefixes)

`42*` is wrong: `4200ID` is customs duties. `44*`/`45*` misses `4B0000`. These lists are `nowcast_trade_margins.GIVER_COMMODITIES` (`wholesale` / `retail`); copy the codes, do not import that module into the transform.

Wholesale dest set (exclude `4200ID`):

```text
423100 423400 423600 423800 423A00
424200 424400 424700 424A00 425000
```

Retail dest set:

```text
441000 444000 445000 446000 447000 448000 452000 454000 4B0000
```

C1 when source is **not** in either set, dest **is** in either set, `abs(R) > ATOL`, and the 2017 Use+VA delta on that pair is almost entirely `V00100`+`V00300`: `abs(intermediate).sum() / abs(R) ≤ 0.05`. `intermediate` is the **source-column** Use commodity subvector only (`abs(U_after[commodities, i] − U_before[commodities, i]).sum()`), not dest-column and not VA. That cutoff is **0.05**, not a Phase A measured substitute. Do not move intermediate. Skip this detector when `abs(R) ≤ ATOL` (empty-source pairs); they stay `default` and are Make leftover.

If the C1 denom `V00100[j] + V00300[j]` is `≤ ATOL`, **keep `rule_id='C1'`**, skip this pair's Use/VA reallocation (no intermediate, so Import/Margins A are no-ops), and leave the residual for C6.

**C1 split** — two-row mix on the industry named by `va_mix` (`'source'` or `'dest'`). Do not use the full three-row VA mix (that would break “sums to `R`”):

```text
denom = V00100[j] + V00300[j]    # j = source or dest per va_mix
if abs(denom) ≤ ATOL:
    skip this pair's C1 reallocation and leave it for C6
else:
    w1 = V00100[j] / denom
    VA_after[V00100, i] -= R * w1
    VA_after[V00300, i] -= R * (1 - w1)
    VA_after[V00100, d] += R * w1
    VA_after[V00300, d] += R * (1 - w1)
```

C1 negatives (source VA below `−ATOL`) are not C4: log, skip the pair's Use/VA, leave for C6. C4 wraps dest-`B` only.

### C3 named pairs (working-hypothesis codes)

C3 **labels** recovered Make pairs; dest is whatever Phase A’s Make absorption found, not an English pick. A pair gets `rule_id='C3'` only if `(source_industry, destination_industry)` matches a row below (source in the source set **and** dest in that row’s dest set). Pairs not on this list **never** get C3 — they stay `default` / `C1` / `C2` and leftovers go to C6.

| Manual (1997 list) | Source set (hypotheses) | Dest set (hypotheses) |
| --- | --- | --- |
| Gaming at casino hotels → casino gambling | `721000` | `713200` |
| Meals at lodging → food services | `721000` | `722110`, `722211`, `722A00` |
| Auto repair by new-car dealers → auto repair | `441000` | `811100` |
| Auto leasing by finance companies → auto leasing | `522A00` | `532100` |
| Own-account construction, owner-occupied housing | `531HSO` | `233411` |
| Own-account construction, electric utilities | `221100`, `S00101`, `S00202` | `233240` |
| Own-account construction, telephone companies | `517110`, `517210`, `517A00` | `233240` |

If Make absorption finds a dest **outside** that row’s dest set, do **not** relabel C3 to chase the English name; leave the pair as `default` (or C1/C2 if those detectors fire) and let C6 take the Use residual. Phase A reports any such mismatch.

The code table is the **only** C3 detector. Phase A’s Use-residual ranking is diagnostic; it does not assign `rule_id`. Skip the C3 detector when `abs(R) ≤ ATOL`.

### C3 recipe recovery (pinned; do not invent a fourth option)

Published Use/VA movement is column-level. On this table the collisions are: source `721000` is shared (gaming + lodging meals); dest `233240` is shared (electric-utility + telephone own-account construction). Default dest-`B` / C1 / C2 pairs can also touch those columns.

**Which column.** For each C3 pair, recover from the **dest** column if that dest is unique among C3 pairs; else from the **source** column if that source is unique among C3 pairs; else from the dest column and unmix as below. On the hypothesis table that picks dest for gaming/meals/auto-repair/auto-leasing/housing (`713200`, `722110`, `722211`, `722A00`, `811100`, `532100`, `233411`) and source for the six `233240` construction pairs.

**Unmix (always, on the chosen column).** Let `Δ` be published `(ΔU, ΔVA)` on the chosen column (`after − before`). Subtract every **other** classified pair that touches that column: contribution is `± recipe * R` (C2/C3) or `± R * b` (dest-`B`) or the C1 two-row VA vector; sign is **+** when the other pair’s dest is this column, **−** when its source is this column. For dest-`B`, `b` is that **other pair’s destination-industry** mix (`U_before[:, other.d] / x_{other.d}` and the matching VA mix) — the same `b` as the dest-`B` two-liner. Do **not** use `B` of the chosen (recovery) column. On source-column recovery (the `233240` pattern) this matters: a dest-`B` pair whose source is this column still subtracts its **own dest** `B`, not this column’s `B`. Non-C3 unmix always runs (even when the dest/source is unique among C3 — dest-`B` / C1 / C2 can still contaminate). Remaining C3 pairs that still share the column: peel by `(|R| descending, source_industry, destination_industry, commodity)` — same order as apply. If Phase A writes two C3 commodities that share `(source, dest)`, key those recipes `(source, dest, commodity)` and peel in that same sort (commodity breaks the tie). Unmix contribution is **0** whenever apply would skip that other pair (`abs(x_{other.d}) ≤ ATOL`, or C1 `denom ≤ ATOL`).

**Dest-positive shares (required; source-column `Δ` is inverted).** Dest-`B` / C1 / C2 / C3 all apply as `source -= vec; dest += vec`. After unmix, dest-column `remaining ≈ +R · recipe_true` and source-column `remaining ≈ −R · recipe_true`. Flip before dividing:

```text
signed = remaining if chosen_column is dest else -remaining
raw = signed / R
# peel: subtract dest-positive (raw * R) from remaining, with the same
# column sign used in Unmix (+ dest / − source), before the next C3 pair
```

Do **not** store `remaining / R` from a source column — that inverts the six `233240` construction pairs, fails identity by `~2|R|`, and makes C6 reverse the whole reallocation.

**Identity fallback (exact; pick is closed).** After the sign flip, if `abs((raw * R).sum() − R) > ATOL`, **store `raw` un-normalized and skip integrity for that key**. Do **not** renormalize. Do **not** drop the pair from C3. C6 closes leftover mix, not a missed sign flip.

**Apply-time lookup.** `recipes[(source, dest, commodity)]` if that 3-tuple is present, else `recipes[(source, dest)]`. A `rule_id='C3'` pair with neither key is a Phase A bug — raise `ValueError`; do not silently dest-`B`.

C3 operator (same two-liner as dest-`B`; commodity slice on `U`, VA codes on `VA`):

```text
vec = recipe * R
U_after[:, i]  -= vec[commodities];  U_after[:, d]  += vec[commodities]
VA_after[:, i] -= vec[VA];           VA_after[:, d] += vec[VA]
```

### C6 overlay

```text
RedefinitionOverlay
  U:       DataFrame, index=commodities, columns=industries, USD
  VA:      DataFrame, index=V00100/V00200/V00300, columns=industries, USD
  Uimp:    DataFrame, same axes as U, USD
  margins: DataFrame, MultiIndex (Industry Code, Commodity Code) ×
           [Producers' Value, Transportation, Wholesale, Retail, Purchasers' Value], USD
```

Each frame has the **same index/columns as its loader**. **No `overlay.V` in this PR.** Make has no C6. Make leftovers (empty-source `R = 0` pairs, dest-cell rounding, unclassified cells) are the Make gate below, not `overlay.V`.

Generated as:

```text
overlay.X = X_published_after − X_algorithm_without_C6
# align on the union of indexes/columns; fillna(0). Required so Margins
# rows the operator created (absent (d, c)) cancel instead of becoming EXTRA.
```

**Timing:** Phase A cannot emit the overlay (it needs C1–C4). PR-table phase 5 writes it. The analysis module grows a `--overlay` mode that imports the transform and writes the four parquets, same pattern as `margins_2017_baseline`.

`Uimp` is **intermediate-only**, same index/columns as `load_2017_Uimp_*` (commodity × industry). FD import (`load_2017_Yimp_usa`, after-only) is **out of scope**.

Leftover-cell CSV (not tracked) columns: `source_industry`, `destination_industry`, `commodity`, `row_code`, `published_delta`, `explained_by_C1_C4`, `residual` — named `(source, dest, commodity)` rows where published Use/VA delta is not explained by default/`C1`–`C4`. Report, not an operator. Phase A’s census can rank raw Use residuals without this file; `--check` writes it **after** the transform exists (it needs C1–C4). Do not treat the leftover CSV as a Phase A-only exit.

`DEFAULT_ONLY` residual CSV (not tracked) columns: `table` (`U`/`VA`/`Uimp`/`margins`), `row`, `column`, `published_after`, `algorithm`, `residual`. For margins, `row` is `industry|commodity` (pipe-separated) and `column` is the value-column name.

### Load helpers

In `nowcast_redefinitions.py`, argument-free:

```text
load_classification() -> list[RedefinitionPair]
load_recipes()        -> dict[RecipeKey, pd.Series]
load_overlay()        -> RedefinitionOverlay
```

They read the pinned tracked paths. `apply_redefinitions` does not call them.

`load_classification` uses `pd.read_csv(..., dtype={c: str for c in ('source_industry','commodity','destination_industry','rule_id','va_mix')})` so codes like `511200` stay strings. Empty `va_mix` → `None`.

Path construction (all six tracked files):

```text
_REDEF_DIR = Path(__file__).resolve().parents[2] / 'analysis' / 'nowcasting'
# parents[2] is the `bedrock` package root
```

Overlay parquet: `to_parquet` / `read_parquet` keeping the index. Margins MultiIndex names are `Industry Code`, `Commodity Code` (same as the loaders).

`compute_redefinition_overlay(V_before, U_before, VA_before, Uimp_before, margins_before, *, classification, recipes, U_published_after, VA_published_after, Uimp_published_after, margins_published_after) -> RedefinitionOverlay` lives in `nowcast_redefinitions.py`. Body: `algorithm = apply_redefinitions(..., overlay=None, rules=FULL - {'C6'})`; each overlay frame is `published_after − algorithm` on the **union** of indexes/columns, `fillna(0)` — same as overlay generation above, not a same-axes subtract. The analysis `--overlay` CLI and overlay test (1) **both call this function**. It does not write files; the CLI writes the four parquets.

### `rules=` on the function

```text
RuleSet = frozenset of {'default', 'C1', 'C2', 'C3', 'C4', 'C6'}

DEFAULT_ONLY = frozenset({'default'})
FULL         = frozenset({'default','C1','C2','C3','C4','C6'})
```

`apply_redefinitions(..., classification, recipes, overlay, rules=FULL)`. `overlay` is required when `'C6' in rules`; if `'C6' in rules` and `overlay is None`, raise `ValueError`. Ignored otherwise. There is no C5.

**`rules=` does not gate Make.** Make always applies every classified pair (the 2017 Make-only test would fail if `DEFAULT_ONLY` skipped C1–C3 Make moves). `rules=` gates Use / VA / Import / Margins / C6 only.

**Fall-through (this is the operator, not an option):**

- `'default' in rules` is required for any Use/VA work; dest-`B` is the base operator.
- If `pair.rule_id` is `'C1'`/`'C2'`/`'C3'` and that id is **in** `rules`, use that special Use/VA operator.
- If `pair.rule_id` is `'C1'`/`'C2'`/`'C3'` and that id is **not** in `rules`, **fall through to dest-`B`**. `DEFAULT_ONLY` therefore dest-`B`s **every** classified pair. That residual is the Chapter 9 failure vs published after.
- C4 is not a stored `rule_id`. When `'C4' in rules` and the operator about to run is dest-`B` (including fall-through), if dest-`B` would drive any source cell `< −ATOL`, **replace** dest-`B` with the C4 operator for that pair. When `'C4' not in rules`, skip that pair's Use/VA (log) instead of going negative — do not clip silently.
- C4 does **not** wrap C1/C2/C3. Those specials that would go negative: skip that pair's Use/VA, log, leave for C6.
- `'C6' in rules`: add the overlay after all pairs. Otherwise do not. U/VA/Uimp are dense same-axes (`after += overlay`). Margins `+=` **union-aligns and `fillna(0)`** — same as overlay generation — so apply-created `(d, c)` rows and overlay-only rows both land.

---

## Phase A — 2017 movement census (analysis only)

**Goal.** Know what the published pair actually does, and emit the contracts above.

**New module:** `bedrock/analysis/nowcasting/redefinitions_2017.py` with a `--check` CLI, same pattern as `margins_2017_baseline.py`.

**Loader gap:** add `load_2017_value_added_before_redef_usa()` in `io_2017.py`. Slice the same before-redef Use Excel at `USA_2017_VALUE_ADDED_CODES × USA_2017_INDUSTRY_CODES`. Assert those three rows exist. Unit-test that assertion on a tiny fake workbook slice is not required (the Excel is GCS); the loader itself `assert`s membership of `V00100`/`V00200`/`V00300` in the file’s index before slicing.

### Make (`V`)

For every off-diagonal `(i, c)`:

- `delta = V_before[i,c] − V_after[i,c]`
- **redefinition** if `abs(delta) > ATOL`; store unclipped `share` and `delta` (see `RedefinitionPair`); if `abs(V_before[i,c]) ≤ ATOL` then `share = 0`
- **other secondary** if `abs(delta) ≤ ATOL` and `abs(V_before[i,c]) > ATOL`
- **Destination:** the industry `d` maximizing `V_after[d,c] − V_before[d,c]` (the dest-row increase that absorbs `delta`). Tie-break: prefer `d == c`; if still tied, lexicographically smallest industry code. Phase A flags every tie and every `d ≠ c`.
- Report per-commodity `|q_before − q_after|` where `q = V.sum(axis=0)`. Expect max ≤ `$11M` (the GO round-trip already found this). This is the Make column-sum leftover, not a classification failure.

Do not re-derive classification from NAICS rules. Recover pairs from the 2017 pair, then assign `rule_id` in this **fixed order** (C1 before C2 recovery, C2 recipe before C3 recovery):

1. Make pairs (all `rule_id='default'`, share / delta / dest).
2. Assign C1 (wholesale/retail dest lists + source-column intermediate + `va_mix` L1). Skip when `abs(R) ≤ ATOL`.
3. Assign C3 from the C3 code table (`abs(R) > ATOL`; never dest `511200`).
4. `recover_own_account_software_recipe(...)` (unmix dest-`B` + C1 only; does not set `rule_id`).
5. If that returns a Series: every dest-`511200` pair with `abs(R) > ATOL` → `rule_id='C2'`. Else leave those pairs `default` and write no C2 recipe.
6. `recover_named_reallocation_recipes(..., recipes)` with C2 already inserted if recovered (unmix dest-`B` + C1 + C2). Does not set `rule_id`.
7. Write both CSVs.

`R` at classify/recover time is the R rule applied to published `V_before` (`share * V_before[i,c]` or 0). Dest-`B` inside both recoveries is the Phase B `x_d` / `b_U` / `b_VA` two-liner (a private helper in `nowcast_redefinitions.py` is allowed).

Phase A also writes a raw Use-delta residual ranking (pairs whose published Use+VA movement does not sum to `R`). The leftover-cell CSV with `explained_by_C1_C4` is a later `--check` output, not a Phase A exit.

### Use intermediate (`Utot`)

- Reproduce the four magnitude numbers exactly (report in million USD).
- For each redefinition pair, the published Use-column movement on `(source, dest)` plus VA should sum to `R` within `ATOL` if Chapter 9's identity held. Measure the residual per pair as a **diagnostic ranking only**. `rule_id='C3'` comes from the C3 code table, not from residual size.

### Value added

- `V00100` / `V00200` / `V00300` by industry, before vs after.
- No Y-before check in this PR.

### Import and Margins

- Same delta census, same `ATOL`.
- Margins index is `(Industry Code, Commodity Code)` including FD buyers and VA commodity rows, matching `_load_2017_margins_from_file`. Expect movement on **industry** buyers in the Make redefinition set, not on FD buyers. Record whether any VA-commodity margin rows move; if they do not, the Margins operator leaves them untouched.

**Exit criterion.** Tracked `classification.csv` and `recipes.csv`; magnitude assertions; per-commodity Make `|q_before − q_after|` report. No overlay yet (needs apply-side C1–C4). No leftover CSV yet (needs `apply_redefinitions`).

PR-table phase 1 lands `RedefinitionPair` / `RecipeKey` / path constants and the two recovery callables in `nowcast_redefinitions.py`, plus the census that **calls them** and writes `classification.csv` / `recipes.csv`. Phase A does **not** call `apply_redefinitions` (phase 2). Phases 3–4 add apply-side C1–C4 only; they do not re-recover recipes. “No transform required” means no `apply_redefinitions`, not “no transform module.”

---

## Phase B — Chapter 9 core transform

**Module:** `bedrock/transform/iot/nowcast_redefinitions.py`

```text
apply_redefinitions(
    V_before, U_before, VA_before, Uimp_before, margins_before,
    *,
    classification: list[RedefinitionPair],
    recipes: dict[RecipeKey, pd.Series],
    overlay: RedefinitionOverlay | None = None,
    rules: frozenset[str] = FULL,
) -> V_after, U_after, VA_after, Uimp_after, margins_after
```

`classification` / `recipes` / `overlay` are 2017-learned **structure**. Amounts `R` and, for `'default'`, destination `B` come from the *input* tables of the year being transformed.

Copy the five input frames first (`V_after = V_before.copy()` and the same for `U` / `VA` / `Uimp` / `margins`). Do not mutate the caller’s before tables.

### Pair application order

1. Freeze dest `B` (and C1 `w1`, C2/C3 recipes) on the **input** tables. Dest coefficients do not update as pairs land.
2. Compute `R` for every pair from `V_before` (R rule above).
3. Sort pairs by `(|R| descending, source_industry, destination_industry, commodity)`. That order is the only order.
4. Apply Make for every pair in that order (always).
5. Apply Use/VA/Import/Margins for each pair in that same order, in-place on the after tables. `available` for C4 is the **current** source cell (`U_after`/`VA_after` after prior pairs), not the original `U_before`.
6. If `'C6' in rules`, add the overlay last.

### Make

Always runs for every classified pair, regardless of `rules=`. For each pair with dest `d = pair.destination_industry`:

```text
R = pair.share * V_before[i, c] if abs(V_before[i, c]) > ATOL else 0.0
V_after[i, c]  -= R
V_after[d, c]  += R
```

Do **not** move “other secondary” off-diagonals. Do **not** use `R / x_i` as the Make operator.

### Use + VA (`'default'` / fall-through dest-`B`)

`x_d = U_before[:, d].sum() + VA_before[:, d].sum()` (Use+VA column sum). Dest-`B` conservation holds by construction only with this `x_d`. The Make row sum `V_before.loc[d].sum()` is a comment, not the divisor — do not switch if they disagree.

```text
b_U  = U_before[:, d] / x_d
b_VA = VA_before[:, d] / x_d
U_after[:, i]  -= R * b_U
U_after[:, d]  += R * b_U
VA_after[:, i] -= R * b_VA
VA_after[:, d] += R * b_VA
```

**Guards:**

- If `x_d == 0` or `abs(x_d) ≤ ATOL`, skip the pair’s Use/VA/Import/Margins and record it.
- If any source cell would go below `−ATOL` under dest-`B`: if `'C4' in rules`, **replace** this pair’s dest-`B` with C4 (do not apply dest-`B` first and patch). Else skip the pair’s Use/VA and record. Do not clip silently.
- Assert `| (ΔU[:,i] + ΔVA[:,i]).sum() + R | ≤ ATOL` (source lost `R`); dest gained `R`. **These two identities apply to dest-`B` pairs only.** C4 pairs need not conserve `R` on the source (shortfall went to dest VA); C6 eats the 2017 residual. VA economy-wide totals still hold within `ATOL` after C4's dest-VA sink (plus C6).
- Commodity row totals on `U` unchanged within `ATOL` for dest-`B` pairs. Economy-wide VA totals unchanged within `ATOL` on `FULL` **after C6**; C4 without C6 raises economy-wide VA by `shortfall`.

### C4 operator (runtime; replaces dest-`B` for that pair)

Never leave a source cell below `−ATOL`. `available_k` is the current source cell for row `k` (after prior pairs). `b` is dest-`B` frozen on the input tables. C4 only repairs **draws** (`want_k > 0`); if dest-`B` would *increase* the source cell (`want_k ≤ 0`), apply `want_k` unchanged.

```text
takes = {}
for k in commodities + VA rows:
    want_k = R * b[k]
    if want_k > 0:
        take_k = max(0, min(available_k + ATOL, want_k))  # source[k] − take_k ≥ −ATOL
    else:
        take_k = want_k
    source[k] -= take_k
    dest[k]   += take_k
    takes[k] = take_k
shortfall = R - sum(takes.values())     # ≥ 0 when some draws were clipped
if abs(shortfall) ≤ ATOL:
    pass
elif abs(b_VA.sum()) ≤ ATOL:
    log; leave shortfall for C6
else:
    dest_VA += shortfall * (b_VA / b_VA.sum())
```

Log every repair (`source`, `dest`, `commodity`, `shortfall`). Shortfall sink is **dest `b_VA` only** (no “or dest-like inputs”).

### Import — candidate A (pinned)

`Uimp` is intermediate-only, same axes as `U`. For each pair, after that pair’s Use operator has produced the **actual** `ΔU` leaving column `i` (dest-`B`, C1=0, C2/C3 recipe, or C4 takes — not the would-be dest-`B` vector):

```text
intensity[c] = Uimp_before[c, i] / U_before[c, i]   if abs(U_before[c, i]) > ATOL else 0
ΔUimp[c]     = intensity[c] * ΔU[c]                 # ΔU[c] is the amount leaving i
Uimp_after[c, i] -= ΔUimp[c]
Uimp_after[c, d] += ΔUimp[c]
```

Elementwise. Intensity is frozen on `Uimp_before` / `U_before` (not updated in-place). `ΔU` is the actual post-operator vector so two pairs sharing `(i, c)` cannot each take `frac = 1` of the original cell. No dest-column import mix. FD import is out of scope.

If this fails to reconstruct 2017 Import within a residual that C6 can close, keep A and let C6 take the Import residual. Do not implement candidate B in this PR.

### Margins — candidate A (pinned)

Index `(buyer, commodity)`. Value columns, all five, move together:

`Producers' Value`, `Transportation`, `Wholesale`, `Retail`, `Purchasers' Value`.

For each commodity `c` in the **actual** Use transfer out of industry buyer `i`:

```text
frac[c] = ΔU[c] / U_before[c, i]   if abs(U_before[c, i]) > ATOL else 0
for col in the five columns:
    row_i = margins_before.loc[(i, c), col]   # 0 if the row is absent
    margins_after.loc[(i, c), col] -= frac[c] * row_i
    margins_after.loc[(d, c), col] += frac[c] * row_i
```

`frac` uses original `U_before` (frozen) times this pair’s actual `ΔU`. Same two-pair caveat as Import: `ΔU` is after this pair’s Use operator, so `frac` cannot independently over-move the original cell.

- Industry buyers only (`i` and `d` from `classification`). FD buyer rows do not move.
- VA-commodity margin rows: default **do not move**. If Phase A finds they move, leave them to C6 — do not add a second margins operator.
- Absent `(d, c)` rows are created with zeros then added into.

### Tests (phase 2 / `DEFAULT_ONLY`)

Toy two-industry example from manual Tables 9.1–9.4, **exact** (unit test, no GCS). Numbers:

```text
Make before:  A: A=90, B=10 (x=100);  B: B=100 (x=100).  q_A=90, q_B=110
Make after:   A: A=90     (x=90);    B: B=110 (x=110).  q unchanged
Use before:   A: A=52, B=3,  VA=45 (x=100)
              B: A=20, B=30, VA=50 (x=100)
              FD: A=18, B=77
R = 10; B's recipe: 0.20 A, 0.30 B, 0.50 VA
Use after:    A: A=50, B=0,  VA=40 (x=90)
              B: A=22, B=33, VA=55 (x=110)
              FD unchanged; q unchanged
```

- 2017 Make-only (`eeio_integration`): see Make gate. Classified **source** cells with `R ≠ 0` must match to `ATOL` before any Use work. Do not require classified dest cells or `R = 0` source cells to match.
- 2017 Use+VA under `DEFAULT_ONLY` (`eeio_integration`): **do not assert equality**. Assert identities, write `redefinitions_2017_default_residual.csv`, do not cap PARTIALs.

**Exit criterion.** Make follows the Make gate. Use/VA/Import/Margins run, dest-`B` identities hold, residual is measured.

### Make gate (single procedure; not both “stop” and `overlay.V`)

Leftover dollars from empty-source pairs (`R = 0`) and from commodity-column rounding land on **classified dest cells** (and on `R = 0` source cells). They are not unclassified-only. The gate is:

1. Phase A reports per-commodity `|q_before − q_after|`. Expect max ≤ `$11M`.
2. Classified **source** `(i,c)` with `R ≠ 0` matches published to `ATOL`. A miss here → classification bug → **stop**. No `overlay.V`.
3. Classified **source** `(i,c)` with `R = 0` (`|V_before| ≤ ATOL`, stored `|delta| > ATOL`) is leftover, not a stop. Computed cell stays at `V_before`; published differs by `|delta|`.
4. Classified **dest** `(d,c)` may miss. That miss is the column-sum leftover `(V_before[i,c] + V_before[d,c]) − (V_after[i,c] + V_after[d,c])` (plus any empty-source `delta` that never moved). Count it toward the per-commodity `$11M` cap together with unclassified PARTIALs.
5. Unclassified Make cells may differ only as part of those column-sum gaps.
6. **Stop** only if a classified source cell with `R ≠ 0` misses ATOL, or leftover exceeds `$11M` on any commodity. Measure leftover as `|q_computed − q_published|` per commodity (same object as `|q_before − q_after|` after a within-column Make move). Do **not** sum absolute cell PARTIALs — opposite-sign dest/unclassified misses can exceed `$11M` abs while the column gap stays under the cap.
7. **This PR does not add `overlay.V`.** The full-table Make `assert_ok(max_partial=0)` would fail on rounding, so the Make section gate is this procedure, not naive `max_partial=0` on the full grid.
8. Do not loosen Use/VA/Import/Margins ATOL to hide Make rounding.

---

## Phase C — residual rules (the 2017 match)

Apply one rule family at a time. After each, re-run the 2017 reconstruction and record Δ residual (cells, dollars, worst pair).

| Order | Rule | Detector (Phase A assigns `rule_id`, except C4) | Operator |
| --- | --- | --- | --- |
| C1 | Wholesale / retail **margin** redefinitions | Dest in the explicit wholesale or retail code list; source not in either; `abs(intermediate).sum()/abs(R) ≤ 0.05` | Two-row split `w1 = V00100[j] / (V00100[j]+V00300[j])` on industry `j` from `va_mix`. If denom `≤ ATOL`, keep `rule_id='C1'`, skip Use/VA, leave for C6. No intermediate |
| C2 | Own-account software | Dest is `511200` and the dest-`511200` strip is stable (`V00100` + CFC + three largest `abs(remaining)` Use rows, each `> ATOL`) | `recipes['C2'] * R`. Raise if `'C2'` missing. If no stable strip, no C2 recipe; pairs stay `default` and C6 takes them |
| C3 | Named large custom reallocations | `(source, dest)` in the C3 code table above (not residual size) | Looked-up recipe × `R` (3-tuple if present, else `(source, dest)`). Recovery and identity fallback: C3 recipe recovery above |
| C4 | Negative-input repair | Runtime: dest-`B` would drive a source cell `< −ATOL` | **Replace** dest-`B` for that pair using the Phase B C4 loop (`take_k = max(0, min(available_k + ATOL, want_k))` for draws; `want_k ≤ 0` unchanged; shortfall → dest `b_VA`). Do not use a one-line `min(available − (−ATOL), want)`. Never go below `−ATOL`. Log every repair |
| C6 | Pair overlay | Whatever cells still miss | Add `overlay.U` / `.VA` / `.Uimp` / `.margins` after C1–C4 |

**No C5 in this PR.** “Implausible for the source primary” is not a predicate. Phase A’s leftover-cell list is the diagnostic; C6 is the operator.

**2012:** Do not run a second validation. If a 2012 before-redef MUT is later extracted, C6 must not be fitted to it; C1–C4 should still apply.

**Exit criterion.** Use / VA / Import / Margins 2017 after tables match published to `ATOL` under `FULL`. Make follows the Make gate (classified `R ≠ 0` source cells to `ATOL`; leftover ≤ `$11M` per commodity). Magnitude checks pass. See Phase D for how `table_match` is called.

---

## Phase D — diagnostics wiring

**2017 gate** — this is the #572 acceptance call for Use / VA / Import / Margins. Defaults of `assert_ok()` do **not** enforce cell-by-cell match (`max_partial=None` lets every `PARTIAL` through).

```text
Tolerance(atol=ROUNDING_ATOL, rtol=0)

compare_tables(...).assert_ok(
    max_partial=0,
    max_miss=0,
    max_extra=0,
    max_margin_partial=0,
)
```

Make uses the Make gate above, not a naive full-table `max_partial=0`.

### Named candidates (in `sections.py`)

Each is `Callable[[int], pd.DataFrame]`, `_require_2017`, and **runs the transform** — not a cached export, not before-vs-after of the published pair.

Shared body `_mut_after_redef_2017(year)` is `@functools.cache` (or equivalent) so the five section candidates plus the margins helper do not rerun the full stack. Returns `tuple[V, U, VA, Uimp, margins]`:

```text
_require_2017(year)
V, U, VA, Uimp, margins = apply_redefinitions(
    load_2017_V_before_redef_usa(),
    load_2017_Utot_before_redef_usa(),
    load_2017_value_added_before_redef_usa(),
    load_2017_Uimp_before_redef_usa(),
    load_2017_margins_before_redef_usa(),
    classification=load_classification(),
    recipes=load_recipes(),
    overlay=load_overlay(),
    rules=FULL,
)
return V, U, VA, Uimp, margins
```

| callable | returns |
| --- | --- |
| `make_after_redef_2017_candidate(year)` | `_mut_after_redef_2017(year)[0]` |
| `use_after_redef_2017_candidate(year)` | `[1]` |
| `va_mut_after_redef_2017_candidate(year)` | `[2]` |
| `uimp_after_redef_2017_candidate(year)` | `[3]` |
| `margins_after_redef_2017_candidate(year)` | `[4]` — used by the helper below, not by a `Section` |

References are the published **after-redef MUT** loaders. Do **not** reuse `USE_VA_DETAIL_SUT` (that is SUT five-row `T00OTOP`/`T00TOP`/`T00SUB`).

| section name | candidate vs reference | notes |
| --- | --- | --- |
| `make_after_redef_detail_mut` | `make_after_redef_2017_candidate` vs `load_2017_V_after_redef_usa` | `Section` is fine (`Section.run` does not call `assert_ok`). Gate = the 8-step Make gate. Any later `for section in SECTIONS: assert_ok(max_partial=0, …)` **must skip this section** — do not “fix” Make by tightening the full grid |
| `use_after_redef_detail_mut` | `use_after_redef_2017_candidate` vs `load_2017_Utot_after_redef_usa` | `Section` is fine; `rtol=0` |
| `va_after_redef_detail_mut` | `va_mut_after_redef_2017_candidate` vs `load_2017_value_added_usa` | New section; MUT rows `V00100`/`V00200`/`V00300` only |
| `uimp_after_redef_detail_mut` | `uimp_after_redef_2017_candidate` vs `load_2017_Uimp_after_redef_usa` | `Section` is fine |

Register all four in `SECTIONS`. Each uses `step='Step 7 - after-redef MUT'`, `tolerance=Tolerance(rtol=0, atol=ROUNDING_ATOL)`, and `_require_2017`. Axes:

| section | `title` | `rows` / `row_axis` | `columns` / `column_axis` |
| --- | --- | --- | --- |
| `make_after_redef_detail_mut` | Make after redefinitions, BEA 2017 detail MUT | `USA_2017_INDUSTRY_CODES` / `industry` | `USA_2017_COMMODITY_CODES` / `commodity` |
| `use_after_redef_detail_mut` | Use intermediate after redefinitions, BEA 2017 detail MUT | `USA_2017_COMMODITY_CODES` / `commodity` | `USA_2017_INDUSTRY_CODES` / `industry` |
| `va_after_redef_detail_mut` | Value added after redefinitions, BEA 2017 detail MUT | `USA_2017_VALUE_ADDED_CODES` / `value_added_code` | `USA_2017_INDUSTRY_CODES` / `industry` |
| `uimp_after_redef_detail_mut` | Import matrix after redefinitions, BEA 2017 detail MUT | `USA_2017_COMMODITY_CODES` / `commodity` | `USA_2017_INDUSTRY_CODES` / `industry` |

**Margins** cannot be a `Section` (`Section.rows` is `tuple[str, ...]`; margins are MultiIndex). One helper, not a fork:

```text
def compare_redef_margins_2017(year: int = 2017) -> TableMatch:
    _require_2017(year)
    return compare_tables(
        margins_after_redef_2017_candidate(year),
        load_2017_margins_after_redef_usa(),
        tolerance=Tolerance(atol=ROUNDING_ATOL, rtol=0),
    )
```

Call `compare_tables` **once** on the five-column frame. Do not loop per value column. Do not pretend it is a `Section`.

### Overlay tests (not a tautology)

The `eeio_integration` full-stack test does **both**, as two assertions, overlay loaded from disk:

1. Checked-in parquet vs a **fresh** `overlay = published_after − algorithm_without_C6` computed in the test (same function `--overlay` uses). Cell equality within `ATOL`. This fails if someone edits the parquet by hand or the algorithm drifts.
2. `apply_redefinitions(..., overlay=load_overlay(), rules=FULL)` vs published after, within `ATOL` (Use/VA/Import/Margins; Make gate as above).

C6 is a named residual, not a silent exact-match cheat. Do not compute the overlay in the same statement that asserts `algorithm_with_C6 == published` and call that (1).

Also: residual-by-rule report from the Phase A CLI (so it does not live only in fixtures). Unit tests in `bedrock/transform/iot/__tests__/test_nowcast_redefinitions.py`: toy Ch. 9; classification CSV round-trip; each C-rule on a hand matrix; C3 **must** include a shared-dest / unique-source case (the `233240` pattern: two sources, one dest, recover from each source column with the sign flip) — not only a unique-dest pair; C4 replace-not-patch on a hand matrix; pair-order stability; 2017 full-stack marked `eeio_integration` (needs GCS).

**Recovery callables (pure; no path I/O).** C2/C3 recovery lives in `nowcast_redefinitions.py`. The census CLI **and** the hand-matrix tests both call these (not a 2017-only script). They do **not** mutate `classification`. Phase A writes their return values to `recipes.csv`. Dest-`B` inside both is the Phase B two-liner (`x_d` = U+VA column sum). `R` is formed from `V_before` and `pair.share` (the pair has no `R` field). `U_after` / `VA_after` arguments are the **published** after tables, not algorithm output.

```text
def recover_own_account_software_recipe(
    V_before, U_before, U_after, VA_before, VA_after,
    classification: list[RedefinitionPair],
) -> pd.Series | None:
    # Share Series on the commodity+VA index, or None if the strip is not stable
    # or there are no C2 candidates (dest 511200 and abs(R) > ATOL).
    # Ignore rule_id. Does not set rule_id.

def recover_named_reallocation_recipes(
    V_before, U_before, U_after, VA_before, VA_after,
    classification: list[RedefinitionPair],
    recipes: dict[RecipeKey, pd.Series],  # C2 already inserted if recovered
) -> dict[RecipeKey, pd.Series]:
    # C3 keys only; caller merges into recipes.
```

---

## Phase E — nowcast years (blocked on Step 6; out of this PR)

Once Step 6 emits a before-redef MUT for year `t`:

```text
apply_redefinitions(*mut_before[t], classification=load_classification(),
                    recipes=load_recipes(), overlay=load_overlay(), rules=FULL)
```

`R` and default `B` come from year `t`. C3 recipes stay 2017-shaped, scaled by that year's `R`. Stored `delta` is never applied.

**C6 on 2018–2025 is not decided in this PR** and does **not** weaken the 2017 gate. For #572 / `FULL`: if C1–C4 leave residual, apply C6 and assert equality. Whether later years drop C6 or scale it by `R_t / R_2017` is a Step 6-era decision.

Step 8 runs on the after-redef output. Do not redefine in Cornerstone space.

---

## One PR onto `nowcast`

All of Phases A–D land in **one PR** from `jv_nowcast_after_redefinitions_MUTs` to `nowcast`. Do not split this work into five PRs. Phase E is out of this PR.

`DEFAULT_ONLY` and `FULL` are two **modes** of the same function (`rules=`). Both tests ship in this PR. “Phase 6” in earlier drafts meant `FULL`, not a sixth PR phase.

| Phase | Contents |
| --- | --- |
| 1 | Before-redef VA loader + recovery callables + types/paths in `nowcast_redefinitions.py` + Phase A census that **calls recovery** and writes `classification` / `recipes` + magnitude assertions + gitignore negations |
| 2 | `apply_redefinitions` Make + default Use/VA + pinned Import/Margins A; toy test; 2017 classified Make match; `DEFAULT_ONLY` Use residual report |
| 3 | Apply-side C1–C2 (do not re-recover recipes) |
| 4 | Apply-side C3–C4 (no C5; C4 runtime; do not re-recover recipes) |
| 5 | C6 overlay artifact; `table_match` / `compare_tables` gates; load helpers; named section candidates; #572 acceptance (`FULL` asserts cell-by-cell match on Use/VA/Import/Margins) |

Do not write only the overlay and skip measuring where Chapter 9 fails. Do not merge a half-transform that fails the 2017 gate.

---

## Files likely touched

| Path | Role |
| --- | --- |
| `bedrock/extract/iot/io_2017.py` | `load_2017_value_added_before_redef_usa` only |
| `bedrock/transform/iot/nowcast_redefinitions.py` | **New.** Types, recovery callables, load helpers, paths, `apply_redefinitions` |
| `bedrock/transform/iot/__tests__/test_nowcast_redefinitions.py` | **New.** |
| `bedrock/analysis/nowcasting/redefinitions_2017.py` | **New.** Census that calls the recovery callables; `--overlay` is phase 5 |
| `bedrock/analysis/nowcasting/redefinitions_2017_classification.csv` | **New.** Tracked |
| `bedrock/analysis/nowcasting/redefinitions_2017_recipes.csv` | **New.** Tracked |
| `bedrock/analysis/nowcasting/redefinitions_2017_overlay_{U,VA,Uimp,margins}.parquet` | **New.** Tracked (phase 5) |
| `.gitignore` | Six `!bedrock/analysis/nowcasting/redefinitions_2017_*` negations |
| `bedrock/analysis/nowcasting/sections.py` | New Step 7 Make / Use / VA-MUT / Import sections + named candidates. Not Margins; plus `compare_redef_margins_2017` |
| `bedrock/transform/iot/derived_gross_industry_output.py` | **Do not touch.** |

Do not edit `plan.md` or the #572 issue body in this PR.

---

## Risks

- **Chapter 11 balancing.** Reallocations are finalized *before* the transactions table is balanced. The published after table is not guaranteed to be “before + reallocations.” C6 absorbs that; call it residual, not a BEA rule.
- **Partial redefinitions.** Classification stores a `share`, not a boolean. Threshold is `ATOL`, not `!= 0`.
- **Destination ≠ commodity code.** Stored on every pair; Make uses that field. Tie-break is pinned.
- **Make leftover ≤ `$11M` per commodity.** Classified **source** cells with `R ≠ 0` gate at `ATOL`. Dest cells, `R = 0` source cells, and unclassified PARTIALs share that cap. No `overlay.V` in this PR.
- **Empty-source 2017 cells (`share = 0`).** Transform `R` is 0. Those dollars sit in the Make leftover, not a `year=` branch.
- `nowcast` **vs** `main` **git.** No merge-base. PRs still target `nowcast`. Out of scope to repair.
- **C2 commodity codes.** Three of five C2 rows are the largest `abs(remaining)` Use rows on dest `511200` after dest-`B`/C1 unmix. If that strip is not stable (any of the five `≤ ATOL`), no C2 recipe; pairs stay `default` and C6 takes them.
- **C3 codes are hypotheses.** Make absorption wins dest; pairs off the table never get C3.
