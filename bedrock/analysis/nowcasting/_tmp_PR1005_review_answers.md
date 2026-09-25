# PR #1005 — temporary draft answers to Wes review

Source: [pullrequestreview-5310818544](https://github.com/cornerstone-data/bedrock/pull/1005#pullrequestreview-5310818544).  
Local only — do not commit unless we decide to promote pieces into `About_trade_electricity_seed.md` or a GitHub reply.

Plan pointer: §2A.3 (A/B grade done) / §2A.3b (true pin — **expand #1005**) in
`issue_896_electricity_row_2dccb22e.plan.md`.

---

## Item 1 — “Pin ≠ carry”; third candidate is a frozen coefficient held as seeded

### What the review asks

Wes proposes freezing direct requirement per dollar of output:
write `a_2017 × column_total(year)` into trade×`221100` **and mark it seeded**,
so the cell moves only with the column’s own intermediate total and the balancer
cannot dump the 2023–24 row collapse onto trade. That is not the same as leaving
the cell on carry (mask, not value). Neither Candidate A nor B is this.

### Codebase finding

In this pipeline, that distinction is **not** available as a seed-only candidate:

- Intermediate survey seeds change **pre-fit dollars** in `composed_seed` only.
- They are **not** in `fixed_value_mask` (Tier-1 today = 1:1 final-demand only).
- `fit_interior` IPF still moves all free intermediate cells proportionally.
- An **identity** Use2017 overlay on trade×`221100` is a **no-op** vs leaving
  trade unwired (those cells are already Use2017 in `composed_seed`).

So a “pin” implemented only in `trade_electricity_seed` either collapses into
carry, or requires a **true pin** in production fit/mask (plan §2A.3b).

### Decision (settled)

**Expand #1005 to implement option C — the true pin** — on
`jv__trade-electricity-899`, not a separate follow-up PR.

| Was considered | Outcome |
|----------------|---------|
| **A** — Document only | Superseded (still explain pin≠carry in reply/About, but ship the mechanism) |
| **B** — Score carry baseline | **Also in this #1005 expansion** (Wes item 2) |
| **C** — True pin | **Chosen — implement in #1005** |

### What we will implement in #1005 (§2A.3b)

Production (~150–280 LOC across 3 modules):

1. [`nowcast_interior_fit.py`](../../transform/iot/nowcast_interior_fit.py) — cell-level hold in `fit_interior` (scale free cells only; offset frozen mass from row/col targets)
2. [`nowcast_mask.py`](../../transform/iot/nowcast_mask.py) — extend `fixed_value_mask` to ~18 trade×`221100` cells; update Tier-1 docstring (assumed coefficient, not “source reports”)
3. [`nowcast_intermediate.py`](../../transform/iot/nowcast_intermediate.py) — after `apply_column_control`, write `a_2017 × (GO−VAPRO)` so the pin equals Wes’s quantity (not carried `θ` shares)

Balance core (`mask` / offset / gras): no API change — light up flags.

**Not sufficient alone:** seed identity overlay; mask without IPF cell-hold; post-IPF overwrite.

Same PR also lands Item 2–4 below. Soft-prefer #995 before measuring pin + manufacturing zeros in one opaque MUT (escape hatch unchanged).

**Proposed GitHub reply (Item 1):** Agree pin≠carry is the untested null and is not A/B; identity seed would be a no-op here. Expanding this PR to implement a true pin via IPF cell-hold + `fixed_value_mask` + post–column-control `a_2017×control`, and to score carry / residual displacement so the “do not wire A/B” conclusion is relative to status quo and the pin’s displacement is measured.

---

## Item 2 — Score carry baseline on the same CV

### What the review asks

`CANDIDATES` is only A/B — carry is never scored. `anti_carry_ok` only checks a
candidate *differs* from carry. So A’s CV 0.786 vs ≤0.35 shows an absolute miss,
not that A is worse than doing nothing. Add **`carry` as a third grade-table row**
(same CV / metrics), even if the conclusion does not change. Repo discipline:
grade against frozen, not only against a threshold (`benchmark_holdout`, trade
regrade #729).

### Proposed solutions

| Option | What | Tradeoff |
|--------|------|----------|
| **2a — Grade-row only (recommended)** | Add `carry` to the grade CLI as a **baseline row**: same `TradeElectricityGradeSummary` / spans / CSV / About table. CV via Step-3 with **`trade_electricity_pin=False`** (pre-pin status quo — after pin lands this is **not** “unpatched”, which would be pin-on). Pre-control panel = Use2017×1 identity on seed-set (status quo dollars before control). Skip anti-carry / do not require A/B Slack pass for carry. `--check` **records** carry CV but does **not** FAIL when carry fails absolute bars. Print ΔCV vs carry for A/B (and later pin). | Small grade-only change; answers Wes directly; no production path change |
| **2b — Relative gate** | Same as 2a, plus change choose/wire so A may wire if `cv_A < cv_carry` even when `cv_A > 0.35` | Changes §2A.3 choose rule; bigger product call; defer unless Wes asks |
| **2c — Defer to pin** | Only score carry when pin lands | Leaves A/B “worse than nothing?” unanswered until C ships; weaker reply to this review item |

**Recommended: 2a** in this #1005 expansion (alongside C). Do **not** change the
absolute Slack wire gate yet (2b). Implement before or with the pin so About can
show `carry | A | B | pin` on one table.

### Concrete grade changes (2a)

1. Extend grade `Candidate` / CLI choices with `'carry'` (keep production
   `trade_electricity_seed` Literal as A/B only — carry is not a seed builder).
2. `physical_proxy_cv('carry')`: no A/B overlay; call Step-3 / assemble with
   `trade_electricity_pin=False` (same payroll ratio math as A/B). After pin
   lands, do **not** measure carry as “unpatched” pin-on.
3. `_precontrol_panel('carry')`: Use2017 `221100`×seed-set each year (identity).
4. `grade_candidate('carry')`: fill summary; `pass_slack_bars` may be False;
   do not use for choose/wire.
5. `run_checks`: for `carry`, skip `anti_carry_ok`; skip FAIL on
   `pass_slack_bars is False` / absolute CV bar; still allow empty-floor /
   NaN CV as infrastructure FAIL (same as broken grade).
6. Default `--candidate both` → grade `carry` + A + B (pin added when C lands).
7. About table: add carry row; note relative CV vs A/B.

### Decision (settled)

**Confirmed: 2a** — score carry as a baseline grade row; no relative wire gate (2b).

---

## Item 3 — Measure residual displacement if trade is pinned

### What the review asks

If trade stops absorbing the 2023–24 collapse, something else does — services,
construction, government. Acceptance for a pinned-coefficient candidate is not
“trade stops fluxing” (true by construction) but **where the dollars land, and
whether that is more defensible than trade**. If the answer is government /
construction (on carry by #896 plan), that may be worse. Also: pinning asserts
electricity intensity per dollar of trade output did not change 2017–24 — weaker
than an index, not neutral (retail efficiency can drift the frozen coeff high).

### Proposed solutions

| Option | What | Tradeoff |
|--------|------|----------|
| **3a — Band Δ on fitted trial dump (recommended)** | After true pin exists: for crisis spans **2022→23** and **2023→24**, compute `#896` band share-effect (`BandShareEffectRow` / `build_band_rows` from [`electricity_row_896.py`](../electricity/current/eia_gtd/electricity_row_896.py)) on **fitted** Use interiors — **unpinned (carry)** vs **pinned**. Emit a small displacement table (band → Δ share_effect_bn) in grade `--csv` / About. Document which bands absorb the residual and the efficiency-drift caveat. No MUT rebuild required for the grade verdict. | Reuses existing instrument; answers Wes; fits grade PR |
| **3b — Full MUT vs MUT** | Rebuild / load two MUT vintages (pin on vs off) and band on published MUT | Heavy; opaque-MUT race with #995; overkill for review answer |
| **3c — Narrative only** | Write the displacement concern in About without numbers | Weak; review asked for measurement |

**Recommended: 3a** in the same #1005 expansion as the true pin (depends on C).

### Concrete grade changes (3a)

1. Helper: build year panels from `assemble_use_seed(year, fitted=True)` under
   pin-off (status quo) vs pin-on (held cells + post–column-control write path,
   or grade-local injection mirroring production once it exists).
2. `build_band_rows(panel, sets, spans=[(2022,2023),(2023,2024)])` for each
   regime; subtract → `TradeElectricityDisplacementRow(band, year_a, year_b,
   share_effect_bn_unpinned, share_effect_bn_pinned, delta_bn)`.
3. Expect trade band Δ → ~0 / much smaller in absolute share-effect under pin;
   other bands take the residual — record, do not auto-FAIL `--check` on
   “government absorbs more” (product judgment in About).
4. About: table + short verdict (“more / less defensible than trade”) + intensity
   drift caveat.
5. Optional: `--check` only fails infrastructure (empty bands, missing years),
   not the product judgment.

### Decision (settled)

**Confirmed: 3a** — band share-effect Δ on fitted trial dumps (pin off vs on) for
crisis spans; About/CSV; no MUT-vs-MUT; no auto-FAIL on product judgment.

---

## Item 4 — Document AWTS/ARTS / AIES “no observation” finding in About

### What the review asks

Wes asked whether wholesale/retail reported cost of electricity in predecessor
surveys. Checked against the API: **no, and trade is excluded by design**:

- `aiesexp02` has `EXPS_ELEC_VAL` but universe is sectors 22, 31–33, 48, 49, 51–54,
  56, 61, 62, 71, 72, 81 — **zero rows for 42 and 44–45** (trade gets `aiesexp01`
  total opex only)
- `timeseries/aies/historical` has no electricity variables
- Economic Census `CSTELEC` for trade is **all zero** in 2017 and 2022
- No ARTS/AWTS electricity series in the API record

➡️ Worth stating in `About_trade_electricity_seed.md` — closes “did we miss a
source?” permanently. Choice is estimators vs pin.

### Proposed solutions

| Option | What | Tradeoff |
|--------|------|----------|
| **4a — About section only (recommended)** | Add a short “No survey observation for trade electricity” subsection to [`About_trade_electricity_seed.md`](About_trade_electricity_seed.md) with the four bullets above (cite API findings; no new scraper). Cross-link from grade CLI print / PR reply. | Cheap; permanent record; matches review |
| **4b — About + assert helper** | 4a plus a tiny grade `--check` that fails if someone later wires a Census trade×`CSTELEC` path without updating the doc | Over-engineered for a negative finding |
| **4c — Defer** | Leave for a later doc PR | Weaker review response |

**Recommended: 4a.**

### Decision (settled)

**Confirmed: 4a** — About subsection only; no new scraper; no grade assert.

---

## Item 5 — Incidental: `services_transport_expense_seed` vs `aiesexp02` sector list

### What the review asks

Incidental finding: `aiesexp02` publishes purchased electricity annually for transport,
information, finance, real estate, professional, admin, education, health, arts,
accommodation, and other services. Worth checking whether
`services_transport_expense_seed` reads all of them.

### Proposed solutions

| Option | What | Tradeoff |
|--------|------|----------|
| **5a — Out of #1005; note only (recommended)** | One sentence in the GitHub reply / About: “separate check whether services seed covers full `aiesexp02` electricity universe — not a gate for trade pin.” Open/follow a tracking note on #896 or services issue if desired. No code change in #1005. | Keeps #1005 scoped; review acknowledged |
| **5b — Quick audit in this PR** | Grep/compare `services_transport_expense_seed` sector coverage vs the listed NAICS; document gap in About or a one-line issue comment; still no production fix unless trivial | Light diligence; may surface §2A.2 work |
| **5c — Expand #1005 to fix services seed** | Read missing sectors into the seed | Out of #899 scope; conflates trade pin with services seam |

**Recommended: 5a** (or **5b** if you want a same-PR audit with no production change).

### Decision (settled)

**Confirmed: 5b** — same-PR quick audit of `services_transport_expense_seed` vs
`aiesexp02` electricity universe; document gaps in About or reply; **no**
production fix in #1005 unless the audit is trivially empty (already covers all).

---

## Item 6 — Coordination: `ec_alt_measures.csv` also in #995

### What the review asks

This PR and #995 both add `bedrock/analysis/nowcasting/census_alt/ec_alt_measures.csv`
(same 122 lines). It was gitignored under `*.csv` while `ec_go_adjustment` requires
it with no fallback — CI had been silently skipping related tests. Whichever lands
second can take either copy; flag so it is not treated as a real conflict.

### Proposed solutions

| Option | What | Tradeoff |
|--------|------|----------|
| **6a — Flag in reply only (recommended)** | Note in GitHub reply / PR description: identical to #995; second land takes either copy; not a content conflict. No local file churn for coordination alone. | Matches review; zero risk of fighting #995 |
| **6b — Drop our copy if already on base** | If downstack / #995 already has the file when we rebase, delete duplicate from this branch | Good hygiene when rebasing; do at push/rebase time, not as a separate “fix” |
| **6c — Change .gitignore / force-track now** | Broader CI fix for `*.csv` swallowing required inputs | Out of #899 scope; may belong with #995 |

**Recommended: 6a**, with **6b** applied naturally on rebase if #995 merges first.

### Decision (settled)

**Confirmed: 6a** — flag in reply/PR description only; not a content conflict; do not
block pin work on #995 for this file. Apply **6b** opportunistically on rebase if
#995 lands first.

---

## Decision summary (all items settled)

| Item | Decision | Lands in #1005 as |
|------|----------|-------------------|
| 1 True pin | **C** — expand this PR | Production: `fit_interior` cell-hold + `fixed_value_mask` + post–column-control write |
| 2 Carry baseline | **2a** | Grade row; no relative wire gate |
| 3 Residual displacement | **3a** | Fitted band Δ (2022→23 / 2023→24); About judgment |
| 4 No survey observation | **4a** | About subsection |
| 5 services vs aiesexp02 | **5b** | Quick audit + document; no production fix |
| 6 ec_alt_measures.csv | **6a** | Reply/PR note only |

**Next when ready to execute:** implement locally on `jv__trade-electricity-899` (no commit/push until asked), starting with true pin (§2A.3b) then grade carry/residual/About/audit.
