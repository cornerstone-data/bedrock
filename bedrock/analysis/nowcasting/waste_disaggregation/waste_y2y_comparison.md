# Frozen-2017 control vs treatment year-to-year N

**Pin:** Shared Google Cloud Make/Use tables (MUT) vintage `v0.3.0_92b7a8a` — regenerated after the SAS suppression fix described below.  
**Script:** `scripts/summarize_waste_y2y_panels.py`  
**Artifacts:** `cache/y2y_waste_N.csv`, `cache/y2y_waste_D.csv`  
**Schema:** `{year, sector, arm, N|D, *_yoy_vs_prior, *_yoy_vs_2018}` — both year-over-year bases required.

Smoke: seven `summary.json` present; `resolved_nowcast_mut_vintage == v0.3.0_92b7a8a`; 98 rows = 7 years × 7 waste children × 2 arms.

---

## Plain-language takeaway (for Flip)

**Flip** = put year-matched waste weight shares into production. **HOLD** = keep today’s frozen 2017 shares.  
**N** = total (life-cycle) emission factor for a sector. **Waste children** = the seven detailed waste activities Cornerstone splits from BEA’s single waste sector `562000`.

**What this note answers:** Even if we **never** update waste weight shares, those waste-child emission factors still change year to year because the nowcast industry **dollar** tables (the MUT) move. How does that “frozen weights, moving dollars” path compare to the path where we **also** update the percent shares each year?

**Short answer:** Holding 2017 shares fixed does **not** freeze waste-child EFs — under today’s approach they already drift ~8% year-over-year on average (up to ~25% in a bad year; up to ~50% vs 2018 by 2024 for hazardous waste). Year-aligned weights with **prior-weighted** SAS recovery and the **AIES-only industry-mix chain** add only a little extra volatility (treatment mean \|YoY\| ≈ 9% vs control ≈ 8%). The equal-fill Flip interim’s +319% `562213` 2022→2023 rebound is **gone** from the production path.

### Two paths, same dollars

Both arms use the **same** MUT pin each year. The only intentional difference is the waste **percent shares** used to split BEA `562000` into seven Cornerstone children.

| Path | Weights | What still moves every year |
|------|---------|-----------------------------|
| **Control** (today’s production stance) | Always 2017 workbook shares | MUT / nowcast industry dollars |
| **Treatment** (Flip candidate) | Shares rebuilt for that year | Same MUT dollars **plus** the share slices below |

Share slices on the treatment arm (plain names):

- **Industry mix** — how large each waste child is among waste firms (SAS Table 3 expenses through 2022 with prior-weighted suppression recovery; **2023–2024 under Flip/`match_io`:** hold post-fill SAS 2022 levels and move 2024 by AIES-only ratios — see share-seam grade in [`waste_n_variance.md`](waste_n_variance.md)).
- **Commodity mix** — how waste output is split across children (SAS Table 2 revenue with prior-weighted recovery; **2022 shares carried into 2023–2024**).
- **Who-buys** — which customer classes buy waste services (Economic Census `ecnclcust`; frozen at 2017 for model years 2018–2021).
- **RCRA intersection** — waste-firm-to-waste-firm shipments (Biennial Report shipper→receiver path for now; follow-on BR→FBS work will replace the temporary bypass).

**Mixed in-year sources (2023–2024 treatment):** industry mix may be chained from SAS 2022 + AIES ratios while commodity mix stays on carried SAS Table 2 2022, who-buys on EC 2022, and waste×waste intersection on BR/RCRA 2021. Those slices are independent — do not expect a single survey to own the whole year.

So any control year-to-year change is “the economy’s waste dollars moved under old shares.” Any extra treatment movement is “shares caught up to the year.”

### What the control path already tells you

Under frozen 2017 weights, hazardous-waste total EF (`562HAZ`) falls roughly **cut in half from 2018 to 2024** (−48% vs 2018). Combustors / incinerators (`562213`) fall about **a third**. That drift is already in today’s production approach whenever the MUT advances and weights stay put. **HOLD does not mean stable waste-child EFs over time** — it means stable *shares* on moving dollars.

### What year-aligned weights add

Treatment after prior-weighted regen + chain:

- **`562HAZ`:** no wipe; 2022 paired Δ is **−16%** (equal-fill interim had been **+150%**). YoY path is smoother than equal-fill Flip evidence.
- **`562213`:** 2022→2023 treatment YoY is **+7%** (equal-fill AIES-level handoff had been **+319%**). Chain holds post-fill SAS 2022 industry mix through 2023 and moves 2024 by AIES-only ratios.

Panel-wide, treatment’s mean absolute year-over-year move among waste children is now ≈ **0.089** vs control ≈ **0.082** (fractional) — nearly matched. Max treatment \|YoY\| ≈ 0.39 (2021 `562HAZ` vs prior), not the equal-fill 3.2 spike.

### How to use this for HOLD vs Flip

| If you care about… | Reading |
|--------------------|---------|
| National / typical-sector EFs | Flip is a small move (by-year medians ≈ −0.01% to −0.4%). This year-to-year note is about **waste children**, not the whole economy. |
| Stable waste-child EFs year to year | Neither path is flat. HOLD still drifts with the MUT; Flip adds share updates and a few larger child-year jumps. |
| Honest alignment of shares to the model year | Flip is the better match: shares follow SAS/AIES, Economic Census, and RCRA/Biennial Report inputs for that year (with a documented fill when Census suppresses detail). HOLD keeps 2017 shares on 2018–2024 dollars by design. |
| Fear of “zeros / −100%” | Obsolete after suppression recovery (waste-N variance). Do not HOLD for that reason. |
| Remaining known shortcuts | Temporary Biennial Report shipper→receiver path for waste×waste flows until follow-on BR→FBS work; Economic Census who-buys frozen at 2017 for model years 2018–2021 — both already settled as OK for Flip. |

**Decision framing for Y2Y comparison:** HOLD vs Flip is not “calm vs wild economy-wide EFs.” It is whether production should **accept a few waste-child EF jumps when year-matched data arrive** (and when survey sources change, e.g. SAS → AIES) in exchange for ending the systematic 2017-share / nowcast-year mismatch. The tables below are the evidence for that tradeoff.

---

## Panels (waste-child N)

**Control arm** = bundled 2017 weights every year (MUT dollars still move).  
**Treatment arm** = year-aligned weights (`match_io` = rebuild shares for the model year), including Table 2/3 suppression recovery on SAS years.

### Panel-wide \|YoY\| summary (waste children only)

Values are **fractional** year-over-year changes in N (0.08 ≈ 8%). `vs_prior` = change from the previous calendar year; `vs_2018` = change from 2018.

| Arm | mean \|N_yoy_vs_prior\| | p95 \|N_yoy_vs_prior\| | max \|N_yoy_vs_prior\| | mean \|N_yoy_vs_2018\| | p95 \|vs_2018\| | max \|vs_2018\| |
|-----|------------------------:|-----------------------:|-----------------------:|----------------------:|----------------:|----------------:|
| Control | 0.082 | 0.165 | 0.254 | 0.172 | 0.372 | 0.480 |
| Treatment | 0.089 | 0.179 | 0.390 | 0.160 | 0.386 | 0.481 |

\*Treatment max \|vs_prior\| is **2021 `562HAZ`** (−39% vs 2020); the equal-fill **+319% `562213` 2022→2023** spike is retired under prior-weighted fill + chain.

### Extreme paths (both YoY bases)

**`562HAZ` N** (hazardous waste — total EF)

| Year | Control N | Ctrl vs prior | Ctrl vs 2018 | Treatment N | Tx vs prior | Tx vs 2018 |
|------|----------:|--------------:|-------------:|------------:|------------:|-----------:|
| 2018 | 1.977 | — | 0 | 1.546 | — | 0 |
| 2019 | 1.797 | −9% | −9% | 1.658 | +7% | +7% |
| 2020 | 1.761 | −2% | −11% | 1.539 | −7% | −0% |
| 2021 | 1.314 | −25% | −34% | 0.939 | −39% | −39% |
| 2022 | 1.216 | −7% | −38% | **1.018** | **+8%** | **−34%** |
| 2023 | 1.120 | −8% | −43% | 0.835 | −18% | −46% |
| 2024 | 1.027 | −8% | −48% | 0.802 | −4% | −48% |

**`562213` N** (solid waste combustors / incinerators — total EF)

| Year | Control N | Ctrl vs prior | Ctrl vs 2018 | Treatment N | Tx vs prior | Tx vs 2018 |
|------|----------:|--------------:|-------------:|------------:|------------:|-----------:|
| 2018 | 8.439 | — | 0 | 7.735 | — | 0 |
| 2019 | 7.878 | −7% | −7% | 7.800 | +1% | +1% |
| 2020 | 8.079 | +3% | −4% | 7.376 | −5% | −5% |
| 2021 | 7.057 | −13% | −16% | **5.622** | **−24%** | **−27%** |
| 2022 | 6.234 | −12% | −26% | 6.476 | +15% | −16% |
| 2023 | 6.602 | +6% | −22% | 6.898 | +7% | −11% |
| 2024 | 5.501 | −17% | −35% | 6.786 | −2% | −12% |

---

## Comparison note (acceptance bar item 3)

1. **Frozen-2017 control already moves.** Under fixed 2017 shares, waste-child total EFs still drift with MUT dollars (control mean absolute year-over-year ≈ 8%, max ≈ 25%; vs 2018 up to ~48% for `562HAZ` by 2024).
2. **Year-aligned weights** after prior-weighted fill + chain add **little extra YoY volatility** (treatment mean ≈ 9%). Equal-fill Flip interim spikes (+150% paired 2022 HAZ; +319% 562213 YoY at the SAS→AIES handoff) are **retired**.
3. **Vs both bases:** control and treatment paths now track more closely; remaining paired gaps (e.g. 2022 `562HAZ` −16%, 2021 `562213` −20%) sit on top of an already-moving control baseline — see waste-N variance for which share slice dominates each case.

**Stakeholder decision (acceptance bar item 4):** **FLIP** (2026-09-29). See [`flip_release_note.md`](flip_release_note.md) and [`production_gate.md`](production_gate.md). Evidence caches regenerated 2026-09-30 under prior-weighted fill + chain; **`v0_4_1`** snapshot SHA `0d26d14f…`.
