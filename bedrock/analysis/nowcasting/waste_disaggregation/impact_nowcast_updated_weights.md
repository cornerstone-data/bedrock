# Impact report — 2024 waste-weight pilot (control vs treatment)

**Status:** Complete (local paired EF comparison, 2026-09-22).  
**Plan:** `.cursor/plans/waste_disagg_three_phases_18d3c084.plan.md` (Decision 7 Pilot-only).

## Context (plain language)

Cornerstone’s nowcast model already **disaggregates** the single waste industry `562000` into seven child industries (collection, hazardous waste, landfills, combustors, remediation, materials recovery, and other). Today’s production path still uses **2017 authored weights** for how that split works.

This pilot asks: if we rebuild those weights for **2024** from newer source data, how much do national emission factors (EFs) move?

| Arm | Config | Waste weights | Electricity |
|-----|--------|---------------|-------------|
| Control | `2025_usa_cornerstone_v0_4_nowcast_2024.yaml` | Bundled **2017** CSVs | off |
| Treatment | `2025_usa_cornerstone_v0_4_nowcast_2024_waste_weights_match_io.yaml` | `match_io` → **2024** derive | off |

Same MUT year (2024), GHG year (2024), and electricity flags off. The only intentional difference is the waste-weight vintage.

**What changed in treatment weights (provenance):**

| Input | Control | Treatment |
|-------|---------|-----------|
| Industry mix (Use columns / VA / Make diagonal) | 2017 workbook | **AIES 2024 BASIC** total expenses by child |
| Commodity mix (Use row sum) | 2017 workbook | **SAS 2022** Table 2 revenue (carry) |
| Who-buys / FD (9 customer-class codes) | 2017 EC in CSV | **EC 2022** `ecnclcust` |
| Use intersection (waste×waste) | Workbook **2012** RCRA embedded in CSV | **RCRA 2021** BR shipper→receiver (temporary bypass of CRHW FBS) |

Treatment `ec_source_year=2022` (happy path — not an EC freeze). RCRA BR bypass is temporary for diagnostics; whether to extend stewi/CRHW is a follow-up.

**How this run was produced:** Google Sheets ADC was expired, so this report uses a **local paired EF comparison** (same math as diagnostics `N`/`D` vectors via `pull_efs_for_diagnostics`), not Drive sheets. Artifacts: `cache/impact_2024/`, figures below.

## Economy-wide results

Across ~400 comparable sectors, updating waste weights barely moves most EFs.

| Metric | Total EF (N) | Direct EF (D) |
|--------|--------------|---------------|
| Median % change (treatment vs control) | **−0.25%** | **0.0%** |
| 95th percentile of \|% change\| | **1.19%** | **0.0%*** |
| Share of sectors with \|N %\| > 1% | **9.1%** | — |
| Share of sectors with \|N %\| > 5% | **1.2%** | — |

\*Almost all non-waste sectors have **unchanged** direct EFs; D only moves where the waste disaggregation itself changes the B-matrix waste rows. The economy-wide D histogram therefore piles at zero even though waste children move a lot.

![N percent-diff histogram](figures/impact_2024_N_perc_diff_hist.png)

![D percent-diff histogram](figures/impact_2024_D_perc_diff_hist.png)

**Reading:** Total EFs (N) shift slightly for many sectors because waste sits in their supply chains; the typical move is a fraction of a percent. Direct EFs (D) are flat almost everywhere except the waste children themselves.

## Waste-sector results

These are the sectors the weight update is *designed* to affect.

| Sector | Name | N control | N treatment | N % Δ | D control | D treatment | D % Δ |
|--------|------|-----------|-------------|-------|-----------|-------------|-------|
| 562111 | Solid waste collection | 0.161 | 0.149 | **−7.7%** | 0.046 | 0.044 | −5.7% |
| 562HAZ | Hazardous waste | 1.241 | 1.296 | **+4.5%** | 0.065 | 0.097 | **+50.6%** |
| 562212 | Solid waste landfill | 7.937 | 8.644 | **+8.9%** | 7.749 | 8.528 | +10.1% |
| 562213 | Solid waste combustors | 5.439 | 8.909 | **+63.8%** | 5.314 | 8.791 | **+65.4%** |
| 562910 | Remediation services | 0.131 | 0.127 | −3.1% | 0.033 | 0.028 | −15.1% |
| 562920 | Materials recovery | 0.205 | 0.227 | **+10.8%** | 0.113 | 0.123 | +8.9% |
| 562OTH | Other waste management | 0.506 | 0.491 | −3.0% | 0.373 | 0.355 | −4.8% |

![Waste sector N and D percent changes](figures/impact_2024_waste_sectors_N_D_pct.png)

**Reading:** Combustors (`562213`) and landfills (`562212`) jump the most — consistent with a newer industry mix / intersection reallocating mass among waste children. Hazardous waste’s **direct** EF rises sharply even though its **total** EF rises only ~4.5%, which usually means the weight update changed how that child’s own emissions intensity is allocated more than its full supply-chain burden. Collection and “other” ease slightly.

Parent code `562000` is not a separate published EF sector after disaggregation (children replace it), so there is no parent row in the EF table.

## Top movers (economy-wide by \|N %\|)

The largest total-EF movers are **the waste children themselves**. Outside waste, the biggest \|N %\| moves are ~1–5% (e.g. life insurance `524113` ≈ −5.5%), i.e. second-order supply-chain effects — not a broad re-ranking of the economy.

| Rank | Sector | N % Δ | Notes |
|------|--------|-------|-------|
| 1 | 562213 | +63.8% | Waste combustors |
| 2 | 562920 | +10.8% | Materials recovery |
| 3 | 562212 | +8.9% | Landfills |
| 4 | 562111 | −7.7% | Collection |
| 5 | 524113 | −5.5% | Largest non-waste N mover in top 25 |
| 6–8 | 562HAZ / 562910 / 562OTH | +4.5% / −3.1% / −3.0% | Other waste children |

Full table: `cache/impact_2024/top25_N_perc_movers.csv`.

## Caveats

1. **Paired deltas are not “industry-mix only.”** Control intersection still embeds workbook **2012** RCRA; treatment uses **2021** BR shipper→receiver. EC who-buys also moves 2017→2022.
2. **RCRA path is temporary.** Intersection bypasses CRHW FBS; stewi/CRHW shipment-edge extension remains a follow-up decision.
3. **No Google Sheet this run** (ADC reauth blocked Drive writes). Numbers are from the same EF pull used by `generate_diagnostics`; re-dispatch to Drive after ADC refresh if a sheet archive is needed.
4. **Production configs are unchanged.** Canonical `nowcast_2024` still uses 2017 weights.

## Plain-language conclusion

Updating waste disaggregation weights for 2024 **does what we expect**: it substantially revises EFs **inside the waste industries** (especially combustors and landfills), while **almost all other sectors** see tiny total-EF shifts (median ≈ −0.25%, p95 \|Δ\| ≈ 1.2%). Direct EFs outside waste are essentially untouched.

For a nowcast production flip, the stakeholder choice is whether those waste-sector EF revisions (and the modest spillover into the rest of the economy) are acceptable given newer AIES / EC / RCRA inputs — not whether the update “breaks” national EFs broadly. It does not.

**Recommended next step (Decision 7):** stakeholder review of this 2024 pilot before authorizing 2018–2023 multi-year weights or a production default flip.

## Artifacts

| Path | Contents |
|------|----------|
| `cache/impact_2024/summary.json` | Stats + provenance |
| `cache/impact_2024/paired_control_vs_treatment.csv` | All-sector paired N/D |
| `cache/impact_2024/waste_sectors_control_vs_treatment.csv` | Waste children only |
| `cache/impact_2024/top25_N_perc_movers.csv` | Top \|N %\| movers |
| `figures/impact_2024_N_perc_diff_hist.png` | Economy-wide N histogram |
| `figures/impact_2024_D_perc_diff_hist.png` | Economy-wide D histogram |
| `figures/impact_2024_waste_sectors_N_D_pct.png` | Waste children bar chart |
| `scripts/run_waste_weight_impact_efs.py` | Reproducible local runner |
