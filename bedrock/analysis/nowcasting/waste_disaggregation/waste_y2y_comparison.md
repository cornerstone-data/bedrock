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

**Short answer:** Holding 2017 shares fixed does **not** freeze waste-child EFs — under today’s approach they already drift ~8% year-over-year on average (up to ~25% in a bad year; up to ~50% vs 2018 by 2024 for hazardous waste). Year-aligned weights add **extra spikes on a few children in a few years**, but after the suppression fix those spikes are real structure updates (and one large switch when AIES replaces SAS — see below), not “the sector vanished.” Flip is choosing whether production should track year-matched waste structure on top of that already-moving dollar baseline.

### Two paths, same dollars

Both arms use the **same** MUT pin each year. The only intentional difference is the waste **percent shares** used to split BEA `562000` into seven Cornerstone children.

| Path | Weights | What still moves every year |
|------|---------|-----------------------------|
| **Control** (today’s production stance) | Always 2017 workbook shares | MUT / nowcast industry dollars |
| **Treatment** (Flip candidate) | Shares rebuilt for that year | Same MUT dollars **plus** the share slices below |

Share slices on the treatment arm (plain names):

- **Industry mix** — how large each waste child is among waste firms (SAS Table 3 expenses through 2022; AIES total expenses in 2023–2024).
- **Commodity mix** — how waste output is split across children (SAS Table 2 revenue).
- **Who-buys** — which customer classes buy waste services (Economic Census `ecnclcust`; frozen at 2017 for model years 2018–2021).
- **RCRA intersection** — waste-firm-to-waste-firm shipments (Biennial Report shipper→receiver path for now; follow-on BR→FBS work will replace the temporary bypass).

So any control year-to-year change is “the economy’s waste dollars moved under old shares.” Any extra treatment movement is “shares caught up to the year.”

### What the control path already tells you

Under frozen 2017 weights, hazardous-waste total EF (`562HAZ`) falls roughly **cut in half from 2018 to 2024** (−48% vs 2018). Combustors / incinerators (`562213`) fall about **a third**. That drift is already in today’s production approach whenever the MUT advances and weights stay put. **HOLD does not mean stable waste-child EFs over time** — it means stable *shares* on moving dollars.

### What year-aligned weights add

Treatment is choppier for a few children:

- **`562HAZ`:** after **suppression recovery** (Census hid some 6-digit SAS lines with flag `S` and a published zero; we fill those from the published NAICS-562 total so hazardous-waste shares are no longer forced to 0%), 2022 jumps up (treatment ≈ 3.0 vs a gently declining control ≈ 1.2), then settles back near control in 2023–2024. The jump lines up with those restored SAS shares plus refreshing how waste firms ship to each other (Biennial Report ≥2017) versus the workbook’s older 2012 RCRA pattern — see [`waste_n_variance.md`](waste_n_variance.md).
- **`562213`:** declines through 2022 under **SAS** (Census Service Annual Survey) shares, then **rebounds sharply in 2023** when **AIES** (Annual Integrated Economic Survey — successor survey for waste-firm expenses) becomes the industry-mix source (2.25 → 9.43, +319% year-over-year). By 2024 it stays elevated vs control. That is a survey-source change (SAS → AIES), not a wipe-to-zero.

Panel-wide, treatment’s average absolute year-over-year move among waste children is higher than control (~0.24 vs ~0.08 on a fractional scale, i.e. ~24% vs ~8%), but the gap is driven by those handful of spikes — not every child every year.

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
| Treatment | 0.242 | 0.646 | 3.192* | 0.196 | 0.403 | 0.968 |

\*Treatment max is dominated by **2022→2023 `562213`** rebound (2.25 → 9.43, +319%).

### Extreme paths (both YoY bases)

**`562HAZ` N** (hazardous waste — total EF)

| Year | Control N | Ctrl vs prior | Ctrl vs 2018 | Treatment N | Tx vs prior | Tx vs 2018 |
|------|----------:|--------------:|-------------:|------------:|------------:|-----------:|
| 2018 | 1.977 | — | 0 | 1.546 | — | 0 |
| 2019 | 1.797 | −9% | −9% | 1.658 | +7% | +7% |
| 2020 | 1.761 | −2% | −11% | 1.539 | −7% | −0% |
| 2021 | 1.314 | −25% | −34% | 0.939 | −39% | −39% |
| 2022 | 1.216 | −7% | −38% | **3.042** | **+224%** | **+97%** |
| 2023 | 1.120 | −8% | −43% | 1.069 | −65% | −31% |
| 2024 | 1.027 | −8% | −48% | 1.022 | −4% | −34% |

**`562213` N** (solid waste combustors / incinerators — total EF)

| Year | Control N | Ctrl vs prior | Ctrl vs 2018 | Treatment N | Tx vs prior | Tx vs 2018 |
|------|----------:|--------------:|-------------:|------------:|------------:|-----------:|
| 2018 | 8.439 | — | 0 | 7.735 | — | 0 |
| 2019 | 7.878 | −7% | −7% | 7.800 | +1% | +1% |
| 2020 | 8.079 | +3% | −4% | 7.376 | −5% | −5% |
| 2021 | 7.057 | −13% | −16% | **5.622** | **−24%** | **−27%** |
| 2022 | 6.234 | −12% | −26% | 2.249 | −60% | −71% |
| 2023 | 6.602 | +6% | −22% | 9.428 | +319% | +22% |
| 2024 | 5.501 | −17% | −35% | 9.013 | −4% | +17% |

---

## Comparison note (acceptance bar item 3)

1. **Frozen-2017 control already moves.** Under fixed 2017 shares, waste-child total EFs still drift with MUT dollars (control mean absolute year-over-year ≈ 8%, max ≈ 25%; vs 2018 up to ~48% for `562HAZ` by 2024).
2. **Year-aligned weights add child-specific volatility**, not a uniform amplification. After suppression recovery, treatment no longer shows **near-zero / −100% wipe** episodes; remaining spikes are **share/structure jumps** (2022 `562HAZ` under restored SAS shares + refreshed waste×waste shipments; 2022→2023 `562213` when AIES replaces SAS for industry mix) rather than “Census hid the cell → we treated it as $0.”
3. **Vs both bases:** control paths remain smoother cumulative declines; treatment paths still move more at a few children/years, but the pre-fix cliff-to-zero story is **obsolete**. Paired treatment-vs-control extremes in the by-year table (e.g. 2022 `562HAZ` +150%, 2021 `562213` −20%) sit **on top of** an already-moving control baseline — see waste-N variance for which share slice dominates each case.

**Stakeholder decision (acceptance bar item 4):** **FLIP** (2026-09-29). See [`flip_release_note.md`](flip_release_note.md) and [`production_gate.md`](production_gate.md). Branch snapshot / waterfall are done; **re-snapshot on the merge commit to `main`** remains per the release note.
