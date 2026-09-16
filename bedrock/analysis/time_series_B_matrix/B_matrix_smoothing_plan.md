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
[`B_driver_investigation.md`](B_driver_investigation.md).

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
with no arguments; it writes 28 tables and 3 figures to `output/`. Figures for
the numbers quoted here are from the run of 2026-09-15 on FBS vintage
`v0.3.0_796a6ca` and MUT vintage `v0.3.0_4276083`. **Treat the module and its
CSVs as the live source** — quoted numbers here go stale, the outputs do not.

| Approach | Question it asks | Diagnostic | Output |
|---|---|---|---|
| 1, top-down | which data source moves the most CO2e away from output? | divergence decomposition by `MetaSources` × `AttributionSources` | `divergence_by_source.png`, `divergence_by_source_real.png`, `divergence_by_attribution.csv`, `divergence_by_metasource.csv` |
| 1, top-down | is it `E`, `x` or `Vnorm` moving? | E by attribution class against x indexed, with the Make's reallocation on a second axis | `E_vs_x_indexed.png` |
| 1, top-down | did this source ever track output at all? | `dlog(E)` on `dlog(x)` per source, emissions-weighted | `output_elasticity.csv`, `output_elasticity_real.csv` |
| 1, top-down | how much of the apparent move is just prices? | nominal vs constant-dollar counterfactual | `price_effect.csv` |
| 2, bottom-up | which EFs moved most, per year, in real dollars? | year-on-year change in `B`, ranked on `abs_delta_B_pct_of_N` — the change in the direct factor weighted by its share of that commodity's own `N` | `B_change_real.csv` |
| 2, bottom-up | which source sits behind a given commodity's EF? | `B` split by attribution | `B_by_attribution.csv` |
| 1 → 2 bridge | which sector *and* source jointly? | divergence by sector × source pair | `divergence_by_sector_stratum.csv` |
| 1 → 2 bridge | does a cell drift or just oscillate? | gross movement against `oscillation = 1 - \|net\| / gross` per cell | `sector_stratum_divergence.png`, `sector_stratum_span.csv` |

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

- **median `abs_delta_B_pct_of_N`**, per year — currently 0.33% to 1.44%
  depending on year;
- **share of commodities over the chosen gate**, per year — a 5% gate on
  `abs_delta_B_pct_of_N` currently admits 5.2% to 22.5%.

Record both before any remediation, and re-run after. The second bullet of the
retest step — did the fix cause other EFs to move — is the same two numbers
plus a diff of `B_change_real.csv` against the pre-change copy, so an
improvement bought by displacing movement elsewhere is visible rather than
netted away.

