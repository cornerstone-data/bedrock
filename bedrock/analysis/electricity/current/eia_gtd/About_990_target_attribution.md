# About #990 — electricity intermediate-target attribution (Phase 2T)

Diagnostic for
[issue #990](https://github.com/cornerstone-data/bedrock/issues/990)
(parent [#896](https://github.com/cornerstone-data/bedrock/issues/896)):
attribute or fix the 2022–24 drop in the electricity intermediate-use target
(`T016 − ΣY` for commodity `221100`). Seeds and the G/T/D allocator are out of
scope.

This note is the schema / decision record. The module is a **standing
reportable checker** (not a CI gate, not a mode on `electricity_row_896`). Keep
it so marginal Attributed verdicts stay re-runnable when new years land.

## Run

```bash
python -m bedrock.analysis.electricity.current.eia_gtd.target_attribution_221100 \
    [--years 2017-2024] --csv [--check]
```

Default `--years` is all available nowcast years (`NOWCAST_YEARS`); every
**consecutive YoY pair** in that range is reported. When 2025 is added to
nowcast years, it is included automatically. `--check` is the validation path
(no unit-test suite on this analysis module).

Live Supply / FD extracts only (`download_sources_ok=True`). No `--mut-vintage`.
Missing / NaN `T016` or `F01000` raises — never silently filled. `--check`
asserts parts identity and `|residual_unexplained| ≤ $0.05bn` per span; it does
**not** sum all five component rows.

CSV: `target_attribution_221100_<lo>_<hi>.csv` (header notes run date + live
extract; not a pinned MUT / not a frozen gate CSV).

**#990 focus:** spans `2022→23` and `2023→24` (marked `*#990-focus*` in CLI
output). Close-out Attributed requires those two; other YoY pairs are context.

## Schema (`TargetAttributionRow`)

| Field | Meaning |
|---|---|
| `year_a`, `year_b` | Consecutive YoY span endpoints |
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
| `Y_PCE` | EIA EPA Table 2.3 residential revenue (`eia_epa_table_2_3_revenue_bn` via `EIA_ElectricPowerAnnual`) |
| `Y_other` | Cannot Attribute without a named BEA FD residual — forces Fix |

**Fix** only when Attributed fails and a named derivation defect is written
here (module + symptom). Do not invent a non-observed `T016`/`Y` correction to
flip the gate. Relative shrink / gate flip alone are not enough to claim Fix.

**#990 outcome** is **Attributed** only if **both** focus spans (2022→23 and
2023→24) are Attributed. The CLI also prints an all-spans overall for the year
range that was run — reportable, not a gate.

## Results (run 2026-09-23; focus spans)

**#990 focus decision: Attributed.** No change to model `T016` / `Y` sources
(the Fix path was unused). Review follow-up does replace the stale EIA revenue
literal with live EPA Table 2.3 via `EIA_ElectricPowerAnnual`. Issues #899 /
#900 remain the allocation track.

### Year levels ($bn)

| Year | T016 | Y_PCE | Y_other | Interior target | BEA UGO305-A | EPA Table 2.3 residential |
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
| **2023→24** | **Attributed** | `Y_PCE` (\|frac\| 0.79) | EPA Table 2.3 residential YoY +$12.4bn vs bedrock PCE +$16.3bn (within $5bn) |

**Plain sentences (one per span):**

- **2022→23:** The intermediate-target drop is mostly a fall in Supply `T016`,
  tracking BEA published industry gross output UGO305-A.
- **2023→24:** The intermediate-target drop is mostly a rise in residential PCE
  (`F01000`), tracking EIA EPA Table 2.3 residential revenue (higher final demand shrinks
  `T016 − ΣY`).

No evidence of a bedrock derivation defect (NaN/auth, unsourced bridge, or
invented non-EIA PCE) on this dump. GO-control was not opened — Fix path unused.

## Two quantities on 2023–24 (not a contradiction)

**Short version:** published series explain the row total’s YoY target move
(issue [#990](https://github.com/cornerstone-data/bedrock/issues/990),
PR [#992](https://github.com/cornerstone-data/bedrock/pull/992)); they do **not**
clear the within-row distribution problem
(issue [#896](https://github.com/cornerstone-data/bedrock/issues/896),
PR [#973](https://github.com/cornerstone-data/bedrock/pull/973);
residual bill work issues [#899](https://github.com/cornerstone-data/bedrock/issues/899) /
[#900](https://github.com/cornerstone-data/bedrock/issues/900)).

Earlier #896 / `electricity_row_control` work says the **2021→22 Use-row
excursion** tracked **BEA’s own gross output** (reproduced to ~0.37pp), and that
what remains **ours** on **2023→24** is the **realized share of intermediate
use** collapsing (column allocation / seeding — Phase 2A). This #990 note says
the **2023→24 intermediate target** (`T016 − ΣY`) drop is mostly **published
residential PCE** tracking EPA Table 2.3. Those claims are about **different
quantities**: share = how the Use row is distributed across purchasers relative
to all intermediate; target = the row’s allowed dollar total before that
distribution. Attributing the target move to published PCE does **not** retire
the share/allocation problem, and calling the share collapse “ours” does **not**
require a bedrock defect in `T016`/`Y`.

## On closing issue #990

This lands as a **validation**, not a model fix. Realized electricity share of
intermediate use still falls **2.02% → 1.70% → 1.54%** on the same MUT; the
model still produces those numbers. **Attributed** means the 2022–24
intermediate-**target** YoY moves **track published series** (BEA UGO305-A /
EPA Table 2.3 residential) within the locked bands — weaker than saying
allocation / within-row distribution is right. That is a legitimate #990 exit
and is what unblocks residual bill work (issues #899 / #900) on the 2017–24
window. Do not read close of #990 as close of issue #896.

## Reopen (2026-09-25) — product path vs target path

WesIngwersen reopened #990: the Attributed verdict above grades
**pre-balance** `Y_PCE` from `derive_initial_Y_pur`. `F01000` is Tier 2, so
GRAS moves `221100 × F01000`. Shipped MUT PCE ran **+10.06%** YoY 2022→23 vs
EIA residential **+2.20%** and Step-3 **+1.96%** — about a **$17.6bn** swing
into the cell (~26% of the electricity intermediate-block fall).

**Sibling issues:**

- [#1008](https://github.com/cornerstone-data/bedrock/issues/1008) — constrain
  `221100 × F01000` (or demonstrate the move is warranted). See
  [`About_1008_pce_electricity_pin.md`](About_1008_pce_electricity_pin.md) and
  `pce_electricity_pin.py` (measure + grade on **shipped** MUT via in-memory
  `mut_from_balanced`).
- [#1009](https://github.com/cornerstone-data/bedrock/issues/1009) — BEA GO vs
  EIA-861 on the **row total** (larger effect; out of scope for #1008).

Until #1008 grades the product path, treat the close-out above as true of the
**target** only — not of the delivered `F01000` cell.

## Product path (#1008) vs pre-balance Attributed (#990)

| Path | What is graded | Module |
|---|---|---|
| **Target / pre-balance** | `derive_initial_Y_pur` PCE cell YoY vs EIA | this note / `target_attribution_221100` |
| **Product / post-balance** | in-memory `mut_from_balanced` after Step 5 | [`About_1008_pce_electricity_pin.md`](About_1008_pce_electricity_pin.md) |

#990 Attributed on the target does **not** imply the shipped MUT cell tracks
EIA. #1008 pin candidates (`tier1_fixed` / `row_side_target` / `eia_band`) are
gated behind `constrain_electricity_pce_cell` (default off). Full-span
Acceptance grade (2017–2024) selected **`eia_band`** on the rebase-off arm
(smoke 2022→23 had selected `row_side_target`); see About_1008 for the dual-arm
matrix. Production flag stays off; set `electricity_pce_constraint_mode` to
`eia_band` only when explicitly shipping the flag.
