# Trade electricity seed (#899) — grade results + true pin

Production seed builders: [`trade_electricity_seed.py`](trade_electricity_seed.py)
(A/B only — **not wired**). True pin lives in transform
(`pin_trade_electricity_a2017` in `nowcast_intermediate`, `fixed_value_mask` +
IPF cell-hold in `nowcast_interior_fit` / `nowcast_mask`). Grade CLI:
[`trade_electricity_seed_grade.py`](trade_electricity_seed_grade.py).

```bash
python -m bedrock.analysis.nowcasting.trade_electricity_seed_grade \
    --csv --check --mut-vintage v0.3.0_4276083
```

Schemas and Slack bars: plan §2A.3 / §2A.3b. Absolute Slack is a **wire gate for
A/B only**; `carry` and `pinned_a2017` are record-only baselines.

## Grade window 2017–2024 (MUT `v0.3.0_4276083`, run 2026-09-24)

| candidate | max_uniform_yoy_spread (pp) | physical_proxy_cv | idiosyncratic_flag_count | held_missing_qcew | pass_slack_bars | role |
|---|---:|---:|---:|---:|---|---|
| `carry` | ~0 (identity) | **0.789** | 0 | 0 | False (record) | pre-pin status quo (`trade_electricity_pin=False`) |
| `uniform_eia_commercial` (A) | ~0 (sanity) | **0.786** | 0 | 0 | **False** | seed-index estimator |
| `qcew_payroll` (B) | **47.7** | **0.771** | **16** | 0 | **False** | seed-index estimator |
| `pinned_a2017` | ~0 (identity) | **0.788** | 0 | 0 | False (record) | production true pin (default-on) |

Bars: A needs sanity spread ≤ 0.01 pp, idio == 0, and `physical_proxy_cv` ≤ 0.35.
B needs spread ≤ 5 pp every span, idio == 0, CV ≤ 0.35, and no QCEW holds.

**Choose / wire:** neither A nor B passes → **do not wire** seed overlays into
`composed_seed`. **True pin is the production commercial path** (default-on).
Relative to carry: A/B/pin CVs are all ~0.77–0.79 — absolute Slack miss is shared
with status quo; pin does not improve the payroll CV bar (that is not what pin
is for — pin freezes trade flux, measured via displacement below).

Local CSVs (gitignored): `trade_electricity_grade_summary.csv`,
`trade_electricity_grade_spans.csv`, `trade_electricity_displacement.csv`.

### Notes

- A’s uniformity bars pass by construction; it fails only on
  `physical_proxy_cv`. A failed CV does **not** mean Table 2.3 is wrong — EPA
  commercial is a broader end-use class than `WHOLESALE∪RETAIL`.
- B fails uniformity and idiosyncratic cuts as well as CV — not a fallback win.
- Soft-prefer #995 before measuring pin + manufacturing zeros in one opaque MUT
  (escape hatch unchanged); landing pin **code** does not wait on #995.

## Residual displacement (crisis spans, fitted interiors)

Band share-effect $bn on fitted interiors: pin-off vs pin-on. Δ = pinned −
unpinned (positive Δ means the band’s share-effect becomes less negative / more
positive under pin).

| band | 2022→23 unpinned | 2022→23 pinned | Δ | 2023→24 unpinned | 2023→24 pinned | Δ |
|---|---:|---:|---:|---:|---:|---:|
| trade | −19.5 | ~0 | **+19.5** | −3.3 | ~0 | **+3.3** |
| services_transport | −12.6 | −23.1 | **−10.5** | −17.3 | −18.8 | −1.5 |
| manufacturing | −8.3 | −13.1 | **−4.7** | −5.7 | −6.4 | −0.7 |
| held_2017 | −9.0 | −11.3 | −2.3 | −1.2 | −1.6 | −0.4 |
| utilities | −4.2 | −4.9 | −0.8 | −1.1 | −1.2 | −0.1 |
| mining | −2.8 | −3.4 | −0.6 | −0.7 | −0.8 | −0.1 |
| agriculture | −0.4 | −0.5 | −0.2 | +0.1 | +0.0 | −0.0 |
| trade_unseeded | −0.3 | −0.3 | −0.1 | −0.0 | −0.0 | −0.0 |

**Product judgment:** Pin does what Wes asked — trade crisis share-effect → ~0.
The 2022→23 residual (~$19bn that trade no longer absorbs) lands mainly on
**services_transport (~$10bn)** and **manufacturing (~$5bn)**, with
**held_2017 / construction–government (~$2bn)**. That is **more defensible than
trade** for the bulk (services have an electricity expense seed; manufacturing
has CSTELEC / #995 hygiene), with a smaller spill onto carry bands that #896
explicitly leaves unseeded. 2023→24 is a smaller move (trade Δ +$3bn; services
takes most of it).

**Intensity-drift caveat:** pinning asserts electricity $ per $ of trade
intermediate output did not change 2017–24. Retail lighting/refrigeration
efficiency can drift the frozen coefficient high vs true kWh intensity.

## No survey observation for trade electricity (AWTS / ARTS / AIES)

Checked against the Census API — trade does **not** report purchased
electricity cost in predecessor or current expense surveys:

- `aiesexp02` has `EXPS_ELEC_VAL` but universe is sectors **22, 31–33, 48, 49,
  51–54, 56, 61, 62, 71, 72, 81** — **zero rows for 42 and 44–45** (trade gets
  `aiesexp01` total opex only).
- `timeseries/aies/historical` has no electricity variables.
- Economic Census `CSTELEC` for trade is **all zero** in 2017 and 2022.
- No ARTS/AWTS electricity series in the API record.

So the choice is estimators (A/B — graded, failed Slack) vs a true pin — not a
missed Census series.

## Services seed vs `aiesexp02` electricity universe (audit only)

Left-hand side: industries indexed by
[`services_transport_expense_seed`](services_transport_expense_seed.py)
(`services_transport_industries` — BEA sectors `48TW`, `51`, `FIRE`, `PROF`,
`6`, `7`, `81`; 100 detail columns).

Right-hand side (services/transport-like with `EXPS_ELEC_VAL`): **48, 49, 51–54,
56, 61, 62, 71, 72, 81**. **Expected non-coverage:** 22 and 31–33 (utilities /
manufacturing — other seeds).

**Audit result:** no real gap among the RHS list — those NAICS map into the seven
BEA sector groups the services seed already moves. No production fix in this PR;
§2A.2 remains the services-seam trigger path if crisis-band share-effect fires.

## True pin mechanism (production)

1. After `apply_column_control`, write
   `share_2017[j] × (GO−VAPRO)[j]` on trade×`221100`
   (`pin_trade_electricity_a2017`).
2. Flag those cells in `fixed_value_mask` (Tier-1; assumed coefficient, not
   “source reports”).
3. `fit_interior` scales **free** cells only; free-axis targets =
   `target − frozen_seed_mass`; wedge closes free-active residuals (column /
   observed-GO side still wins).

Counterfactual: `trade_electricity_pin=False` on assemble / derive / fit / mask
(grade carry + displacement unpinned side).
