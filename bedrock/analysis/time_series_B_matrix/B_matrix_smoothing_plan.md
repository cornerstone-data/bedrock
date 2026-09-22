# B matrix smoothing project plan

## Phase 1 Project objective

Identify sources of emissions and/or output changes that cause unjustified direct emissions intensity changes; prioritize remediation; test and implement remediation (smoothing) steps; test for acceptance.  

## Approach

### Prioritize drivers of rocky trends

Based on 
1. Emissions data sources or economic output sources driving the most change (top-down)
2. Sector direct EF internannual changes in real dollars coupled with justified fixes that can potentially reduce cumulatively (across all EFs) the most (bottom-up).

## Develop diagnostics to identify drivers

Create the diagnostics to identify and prioritize the drivers. 

Drivers found so far, what each is worth, and whether it has been shown to
be justified, are tracked in
[`B_driver_investigation.md`](B_driver_investigation.md). That tracker is the
bridge between the two halves of this plan: one row per driver, carrying the
diagnostic that found it, its potency, a `Justified?` verdict, and the issue
that owns the remediation.

⚠️ **A driver is not prioritised until it has a verdict, and not actioned until
it has an issue.** Ranking on potency alone would put effort into movements that
turn out to be real — the tracker holds three `justified` rows whose correct
action is to leave them alone, and two that are `out of scope` here and were
handed to Nowcasting Phase 2. Every `unresolved` row carries an issue so that
nothing measured here depends on this document being re-read to survive.

### Determine why the drivers change and how to fix them

`B = (E/x) @ Vnorm` 

There are 3 primary sources of change, `E`, `x`, and `Vnorm`. This project starts without bias toward any of those sources.

Was it `E`, `x`, or `Vnorm` that jumped the most out of tune with the others? Why the change - is there solid justification for the change? If not how do we remediate it? If so should another variable then be adjusted, and how?

Inflation is an expected and acceptable form of change, so inflation effects must be separated from the real change.

### Out of scope: `L`

`B = (E/x) @ Vnorm` has exactly three inputs and `L` is not one of them; it
enters only through `N = B @ L`. `L` comes from `A = U_norm @ V_norm`, the
nowcast's own IO product, so no emissions-side change can move it — and it
moves `N` two to four times as much as the factors do, and `Vnorm` is 3% to
13% of gross `B` movement depending on year. Diagnosed here,
remediated in Nowcast Phase 2. `Vnorm` falls on the same side of that line
for remediation, being the nowcast Make, even though it is a direct term in
`B`.

### Implement change and retest

1. Rerun key diagnostic(s) following the change. Was change effective? Did it have indirect effects causing other EF change to occur?

---

## Methods: which diagnostic answers which approach

Everything below comes from `B_change_diagnostics.py` in this folder. Run it
with no arguments; it writes 30 tables and 5 figures to `output/`. `--facility-data`
adds D14 and D15 and four more tables, and is the one option that needs a network. Figures for
the numbers quoted here are from the run of 2026-09-18 on FBS vintage
`v0.3.0_99655e9` and MUT vintage `v0.3.0_4276083`. **Treat the module and its
CSVs as the live source** — quoted numbers here go stale, the outputs do not.

⚠️ **Check the vintage, not just the numbers.** An earlier run of this note
resolved to a local-only FBS build that no ref reaches, and published a set of
findings off it. `--list-vintages` prints the provenance of every build on
disk; `resolve_span_vintage` now prefers one reachable from `origin/main` and
refuses to choose when several span-covering builds are present and none is.

| ID | Approach | Question it asks | Diagnostic | Output |
|---|---|---|---|---|
| **D1** | gate | does the decomposition close? | `total_gap == sum(divergence) + composition`; raises if the worst residual exceeds 0.01% of prior-year `E` | `identity_check.csv`, `identity_check_real.csv` |
| **D2a** | 1, top-down | which data source moves the most CO2e away from output? | divergence decomposition by `MetaSources` × `AttributionSources`, merged onto the attribution *role* | `divergence_by_source.png`, `divergence_by_source_real.png`, `divergence_by_attribution.csv`, `divergence_by_metasource.csv`, `divergence_by_class.csv`, `divergence_detail.csv` |
| **D2b** | 1, top-down | is a source's movement really a vintage relabelling? | the same decomposition keyed on the **raw** `AttributionSources` string, so EPA's mid-span table renumbering stays visible | `divergence_by_attribution_raw.csv` |
| **D3** | 1, top-down | is it `E`, `x` or `Vnorm` moving? | E by attribution class against x indexed, with the Make's cumulative reallocation on a second axis | `E_vs_x_indexed.png` |
| **D4** | 1, top-down | did this source ever track output at all? | `dlog(E)` on `dlog(x)` per source, emissions-weighted | `output_elasticity.csv`, `output_elasticity_real.csv`, `output_elasticity_by_metasource.csv`, `output_elasticity.png` |
| **D5** | 1, top-down | how much of the apparent move is just prices? | nominal vs constant-dollar counterfactual | `price_effect.csv` |
| **D6a** | 2, bottom-up | which EFs moved most, per year, in real dollars? | year-on-year change in `B`, ranked on `abs_delta_B_pct_of_N` — the change in the direct factor weighted by its share of that commodity's own `N` | `B_change_real.csv` |
| **D6b** | 2, bottom-up | how much of the `N` move is the factors, and how much is `L`? | the same table's `pct_change_N_L_held` / `pct_change_N_L_effect`: `N` recomputed with the current year's factors through the prior year's `L` | `B_change_real.csv` |
| **D7** | 2, bottom-up | which source sits behind a given commodity's EF? | `B` split by attribution | `B_by_attribution.csv` |
| **D8** | 1 → 2 bridge | which sector *and* source jointly? | divergence by sector, and by sector × source pair | `divergence_by_sector_stratum.csv`, `divergence_by_sector.csv`, `divergence_by_sector_real.csv` |
| **D9** | 1 → 2 bridge | does a cell drift or just oscillate? | gross movement against `oscillation = 1 - \|net\| / gross` per (sector, inventory table, attribution) cell | `sector_stratum_divergence.png`, `sector_stratum_span.csv` |
| **D10** | context | how much of `E` has its sector split derived from the IO tables? | share of emissions carried by each attribution route and class | `io_derived_share.csv`, `attribution_shares.csv` |
| **D11** | context | which sectors carry emissions with no gross output to divide by? | emissions whose sector has no `x`, so they cannot enter `E / x` | `emissions_without_output.csv` |
| **D12** | context | what are the levels behind a percentage? | `B`, `N` and `x`, nominal and real | `B_total.csv`, `B_total_real.csv`, `x.csv`, `x_real.csv` |
| **D13** | 1, top-down | how much of gross `B` movement is the Make rather than `E / x`? | hold `Vnorm` at the prior year and difference, output-weighted and unweighted. Both one-sided index directions are reported because they disagree materially; the headline is their average | `vnorm_share_of_B_movement.csv` |
| **D14** | 1 → 2 bridge | did we allocate a sector less fuel combustion than its facilities physically reported? | **GHGRP subpart C plus the subpart W combustion tables** (#927), crosswalked from facility NAICS to BEA detail, as an annual **lower bound** on each sector's table 3-11. Subpart W is not optional: onshore production, gathering and boosting and gas distribution report their combustion there, so a subpart C floor holds none of their fuel. A sector below the floor in *every* year is a constant offset — either a boundary definition difference or an allocation vector that cannot see the fuel at all; one that clears it in some years and breaches it in others has a volatility defect | `combustion_floor_test.csv`, `ghgrp_combustion_floor.csv`, `ghgrp_subpart_C.csv`, `ghgrp_subpart_W_combustion.csv` — ⚠️ **opt-in, `--facility-data`**: the only diagnostic here that reaches outside the repository, downloading GHGRP per year through `stewi` and the two subpart W views from Envirofacts. Covers the whole 2017-2024 span |
| **D15** | 1 → 2 bridge | how much of table 3-11 could rest on facilities that actually reported, and where would that move the split? | GHGRP subpart C first, then NEI combustion SCCs for the facilities below GHGRP's threshold, deduplicated on `FRS_ID`; sector from the **facility** NAICS, never from the process code. Splits `purchased` from `self_supplied` fuel, because fuel that never changed hands — byproduct gas, and the lease and plant fuel of an oil and gas operation (#927) — is never a purchase and no Use row can carry it | `facility_basis.csv`, `facility_combustion.csv` — ⚠️ **opt-in, `--facility-data`**; bounded by NEI, which trails GHGRP by a year |
| **D16** | 1 → 2 bridge | which sectors do their own facilities report more than we assign them, and can a vector fix it? | The GHGRP floor over **every subpart except electricity** against the sector’s **whole** assignment, `allocated` and `Direct` together — the one comparison that assumes nothing about which subpart answers which inventory table. Split on `Direct`: **restate** where it is immaterial, so a better table 3-11 vector closes it; **relocate** where it is not, which no vector can touch | `under_attributed_sectors.csv`, `ghgrp_facility_floor.csv` — ⚠️ **opt-in, `--facility-data`**. 20-25 sectors a year, 145-161 Mt, of which only 16-24 Mt is restatable; petroleum refineries alone is 96.7 Mt of the rest ([#962](https://github.com/cornerstone-data/bedrock/issues/962), [#953](https://github.com/cornerstone-data/bedrock/issues/953)) |

**Citing a diagnostic.** `D#` is the stable handle; the letter suffix marks a
second reading of the same table rather than a second run. Cite the ID in
[`B_driver_investigation.md`](B_driver_investigation.md) so a tracker row
carries the evidence that produced it, and add a row here before citing an ID
that does not yet exist. Where a diagnostic has a nominal and a real variant,
**the real one is the one to cite** — see D5 for why.

Findings from these diagnostics, including what a 5% gate actually selects
and how much of the movement each of `E`, `x` and `Vnorm` accounts for, are
in [`About_B_change_drivers.md`](About_B_change_drivers.md).

The two approaches rank in different units and will not agree, by design.
Approach 1 ranks in **Mt CO2e** — what moves the total. Approach 2 ranks in
**% of the factor** — what looks broken. A small commodity can top the
percentage list and be invisible in the mass list. Both are needed; neither
substitutes for the other.

### Acceptance test

"Test for acceptance" needs a number to move. The module already computes two
that work as before-and-after measures, both on real-dollar EFs:

- **median `abs_delta_B_pct_of_N`**, per year — currently 0.32% to 1.17%
  depending on year;
- **share of commodities over the chosen gate**, per year — a 5% gate on
  `abs_delta_B_pct_of_N` currently admits 5.2% to 22.5%.

Record both before any remediation, and re-run after. The second bullet of the
retest step — did the fix cause other EFs to move — is the same two numbers
plus a diff of `B_change_real.csv` against the pre-change copy, so an
improvement bought by displacing movement elsewhere is visible rather than
netted away.

