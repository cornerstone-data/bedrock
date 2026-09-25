# Follow-up to Wes review on #1005

Reply to [pullrequestreview-5310818544](https://github.com/cornerstone-data/bedrock/pull/1005#pullrequestreview-5310818544).

---

## Item 1 — Pin ≠ carry; true pin implemented

Agreed: freezing `a_2017 × column_total` **and holding the cell** is not the same as leaving trade on carry, and an identity Use2017 seed overlay would be a no-op here (those cells are already Use2017 in `composed_seed`; IPF would still move them).

This PR now implements the **true pin** (plan §2A.3b / option C):

1. `pin_trade_electricity_a2017` in `nowcast_intermediate` — after column control, before S00300, write `share_2017[j] × (GO−VAPRO)[j]` on trade×`221100`.
2. `fixed_value_mask` — Tier-1 flags those nonzero cells (assumed 2017 coefficient, not “source reports”).
3. `fit_interior` — scale **free** cells only; free-axis targets = `target − frozen_seed_mass`; wedge closes free-active residuals (column / observed-GO side still wins).

Default-on via `trade_electricity_pin=True` threaded through mask / derive / fit / `assemble_use_seed`. Grade counterfactual uses `False` (neither pin dollars nor holds).

A/B seed-index candidates remain **unwired** (failed Slack); the pin is the production commercial path.

## Item 2 — Carry baseline on the same CV

Done (**2a**). Grade table now includes `carry` with the same CV / spread schema.

| candidate | physical_proxy_cv | pass_slack |
|---|---:|---|
| carry (pin-off) | 0.789 | record only |
| A | 0.786 | False |
| B | 0.771 | False |
| pinned_a2017 | 0.788 | record only |

Carry CV is measured with `trade_electricity_pin=False` (pre-pin status quo — not “unpatched” after pin lands). Absolute Slack is **not** a wire gate for carry; `--check` records only. A is not worse than carry on CV; all miss ≤0.35 together. Relative wire gate (**2b**) deferred.

## Item 3 — Residual displacement

Done (**3a**). Crisis spans on fitted interiors (pin-off vs pin-on):

**2022→23:** trade share-effect −19.5 → ~0 (Δ **+$19.5bn**). Residual lands mainly on **services_transport (Δ −$10.5bn)** and **manufacturing (Δ −$4.7bn)**, with **held_2017 (Δ −$2.3bn)**.

**2023→24:** trade −3.3 → ~0 (Δ **+$3.3bn**); services takes most of the rest.

**Verdict:** more defensible than trade for the bulk (services have an electricity expense seed; manufacturing has CSTELEC / #995). Smaller spill onto construction/government carry is real and named in About — intensity-drift caveat also documented. No auto-FAIL on product judgment.

## Item 4 — No AWTS/ARTS/AIES observation for trade electricity

Documented in `About_trade_electricity_seed.md`: `aiesexp02` has no 42 / 44–45 rows for `EXPS_ELEC_VAL`; historical AIES has no electricity vars; EC `CSTELEC` for trade is all-zero; no ARTS/AWTS electricity series. Choice is estimators vs pin — not a missed Census series.

## Item 5 — Services seed vs `aiesexp02` electricity universe

Quick audit (**5b**): `services_transport_industries` covers BEA sectors `48TW`, `51`, `FIRE`, `PROF`, `6`, `7`, `81` (100 detail columns). That maps the services/transport-like `aiesexp02` electricity universe (48, 49, 51–54, 56, 61, 62, 71, 72, 81). Expected non-coverage: 22 and 31–33 (other seeds). **No real gap; no production fix in this PR.**

## Item 6 — `ec_alt_measures.csv` vs #995

Same file / same content as in #995. Not a content conflict — whichever lands second can take either copy.

---

**Bottom line:** expand this PR with true pin + carry baseline + displacement + AWTS/services notes; still **do not wire A/B**; pin is default-on for production Use. Soft-prefer #995 before measuring pin + manufacturing zeros in one opaque MUT (escape hatch unchanged).
