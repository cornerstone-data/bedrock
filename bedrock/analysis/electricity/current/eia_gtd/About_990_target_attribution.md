# About #990 — electricity intermediate-target attribution (Phase 2T)

Diagnostic for
[issue #990](https://github.com/cornerstone-data/bedrock/issues/990)
(parent [#896](https://github.com/cornerstone-data/bedrock/issues/896)):
attribute or fix the 2022–24 drop in the electricity intermediate-use target
(`T016 − ΣY` for commodity `221100`). Seeds and the G/T/D allocator are out of
scope. Window is **2022–24 only**.

This note is the schema / decision record. The emitter is disposable — delete
`target_attribution_221100.py` (and its tests) when #990 closes. Do **not** add
a standing mode to `electricity_row_896`.

## Run

```bash
python -m bedrock.analysis.electricity.current.eia_gtd.target_attribution_221100 \
    --csv [--check]
```

Live Supply / FD extracts only (`download_sources_ok=True`). No `--mut-vintage`.
Missing / NaN `T016` or `F01000` raises — never silently filled. `--check`
asserts parts identity and `|residual_unexplained| ≤ $0.05bn` per span; it does
**not** sum all five component rows.

CSV: `target_attribution_221100_2022_24.csv` (header notes run date + live
extract; not a pinned MUT / not a frozen gate CSV).

## Schema (`TargetAttributionRow`)

| Field | Meaning |
|---|---|
| `year_a`, `year_b` | Crisis spans `(2022,2023)`, `(2023,2024)` |
| `component` | `T016` \| `Y_PCE` \| `Y_other` \| `interior_row_target` \| `residual_unexplained` |
| `delta_usd` | Contribution to change in intermediate target (USD) |
| `fraction_of_delta_target` | `delta / interior Δ`; `None` if `\|interior Δ\| < $5bn` |
| `source_note` | Module / series tag |

**Signs:** target = `T016 − ΣY`. Contributions: `+ΔT016`, `−ΔY_PCE`,
`−ΔY_other`. `Y_PCE` = `F01000`; `Y_other` = `ΣY − Y_PCE` with the same ΣY
fillna as `interior_row_targets`.

## Fix vs Attributed (locked)

Per span, `dom` = largest `|fraction|` among `{T016, Y_PCE, Y_other}`.

**Attributed** when identity holds, `|fraction(dom)| ≥ 0.50`, published YoY for
`dom` has the same sign and tracks within `$5bn` absolute **or** 15% relative
(when `|bedrock Δ| ≥ $5bn`), and no named derivation defect:

| `dom` | Published counterpart |
|---|---|
| `T016` | BEA UGO305-A (`detail_gross_output_panel(ec_adjusted=False)`) |
| `Y_PCE` | EIA-861 residential revenue (`EIA_861_REVENUE_BN`) |
| `Y_other` | Cannot Attribute without a named BEA FD residual — forces Fix |

**Fix** only when Attributed fails and a named derivation defect is written
here (module + symptom). Do not invent a non-observed `T016`/`Y` correction to
flip the gate. Relative shrink / gate flip alone are not enough to claim Fix.

Overall #990 outcome is **Attributed** only if **both** crisis spans are
Attributed.

## Results (run 2026-09-23)

**Overall decision: Attributed.** No production code change. #899 / #900 remain
blocked only by process (comment + close path), not by an open Fix.

### Year levels ($bn)

| Year | T016 | Y_PCE | Y_other | Interior target | BEA UGO305-A | EIA-861 residential |
|---:|---:|---:|---:|---:|---:|---:|
| 2022 | 652.7 | 232.2 | 0.8 | 419.6 | 556.6 | 227.0 |
| 2023 | 596.6 | 236.8 | 1.4 | 358.3 | 507.5 | 232.0 |
| 2024 | 591.6 | 253.1 | 0.8 | 337.6 | 502.9 | 244.4 |

### Component contributions ($bn)

| Span | T016 | Y_PCE | Y_other | Interior Δ | Residual |
|---|---:|---:|---:|---:|---:|
| 2022→23 | **−56.1** (frac 0.92) | −4.6 (0.07) | −0.6 (0.01) | −61.3 | ~0 |
| 2023→24 | −5.0 (0.24) | **−16.3** (0.79) | +0.7 (−0.03) | −20.7 | ~0 |

### Span decisions

| Span | Outcome | Dominant | Published track |
|---|---|---|---|
| **2022→23** | **Attributed** | `T016` (\|frac\| 0.92) | BEA UGO305-A YoY −$49.1bn vs bedrock T016 −$56.1bn (within 15%) |
| **2023→24** | **Attributed** | `Y_PCE` (\|frac\| 0.79) | EIA-861 residential YoY +$12.4bn vs bedrock PCE +$16.3bn (within $5bn) |

**Plain sentences (one per span):**

- **2022→23:** The intermediate-target drop is mostly a fall in Supply `T016`,
  tracking BEA published industry gross output UGO305-A.
- **2023→24:** The intermediate-target drop is mostly a rise in residential PCE
  (`F01000`), tracking EIA-861 residential revenue (higher final demand shrinks
  `T016 − ΣY`).

No evidence of a bedrock derivation defect (NaN/auth, unsourced bridge, or
invented non-EIA PCE) on this dump. GO-control was not opened — Fix path unused.

## Unit tests

```bash
uv run pytest bedrock/analysis/electricity/current/eia_gtd/__tests__/test_target_attribution_221100.py
```

Pure fixtures: signed identity, residual atol, small-denominator `None`,
`Y_other`-dominant → Fix, published-band fail → Fix, overall needs both spans.
No live extract I/O.
