# B matrix smoothing project plan

## Phase 1 Project objective

Identify sources of emissions and/or output changes that cause unjustified direct emissions intensity changes; prioritize remediation; test and implement remediation (smoothing) steps; test for acceptance.  

## Approach

### Prioritize drivers of rocky trends

Based on 
1. Emissions data sources or economic output sources driving the most change (top-down)
2. Sector direct EF internannual changes over 5% in real dollars coupled with justified fixes that can potentially reduce cumulatively (across all EFs) the most (bottom-up).

## Develop diagnostics to identify drivers

Create the diagnostics to identify and prioritize the drivers. 

### Determine why the drivers change and how to fix them

1. Was it the numerator or denominator that jumped the most? Why the change - is there solid justification for the change? If not how do we reduce it? If so should the other side then be adjusted, and how?

### Implement change and retest

1. Rerun key diagnostic(s) following the change. Was change effective? Did it have indirect effects causing other EF change to occur?

---

## Methods: which diagnostic answers which approach

Everything below comes from `derive_B_time_series.py` in this folder. Run it
with no arguments; it writes 28 tables and 3 figures to `output/`. Figures for
the numbers quoted here are from the run of 2026-09-15 on FBS vintage
`v0.3.0_796a6ca` and MUT vintage `v0.3.0_4276083`. **Treat the module and its
CSVs as the live source** — quoted numbers here go stale, the outputs do not.

| Approach | Question it asks | Diagnostic | Output |
|---|---|---|---|
| 1, top-down | which data source moves the most CO2e away from output? | divergence decomposition by `MetaSources` × `AttributionSources` | `divergence_by_source.png`, `divergence_by_source_real.png`, `divergence_by_attribution.csv`, `divergence_by_metasource.csv` |
| 1, top-down | is it emissions moving, or output? | E by attribution class against x, indexed | `E_vs_x_indexed.png` |
| 1, top-down | did this source ever track output at all? | `dlog(E)` on `dlog(x)` per source, emissions-weighted | `output_elasticity.csv`, `output_elasticity_real.csv` |
| 1, top-down | how much of the apparent move is just prices? | nominal vs constant-dollar counterfactual | `price_effect.csv` |
| 2, bottom-up | which EFs moved most, per year, in real dollars? | year-on-year change in `B` | `B_change_real.csv` |
| 2, bottom-up | which source sits behind a given commodity's EF? | `B` split by attribution | `B_by_attribution.csv` |
| 1 → 2 bridge | which sector *and* source jointly? | divergence by sector × source pair | `divergence_by_sector_stratum.csv` |

The two approaches rank in different units and will not agree, by design.
Approach 1 ranks in **Mt CO2e** — what moves the total. Approach 2 ranks in
**% of the factor** — what looks broken. A small commodity can top the
percentage list and be invisible in the mass list. Both are needed; neither
substitutes for the other.

### Calibrating the 5% gate in approach 2

⚠️ **5% is not currently a prioritisation filter — it selects most of the
economy.** Measured on real-dollar commodity EFs:

| year | commodities | over 5% | over 10% | over 20% | median |
|---|---:|---:|---:|---:|---:|
| 2018 | 402 | 48.8% | 27.6% | 6.0% | 4.9% |
| 2019 | 402 | 48.3% | 23.6% | 7.5% | 4.8% |
| 2020 | 402 | 68.7% | 42.5% | 19.4% | 8.6% |
| 2021 | 400 | **85.5%** | 70.2% | 42.8% | 16.9% |
| 2022 | 401 | 71.6% | 47.4% | 23.2% | 9.5% |
| 2023 | 401 | 67.6% | 46.1% | 20.9% | 9.0% |
| 2024 | 400 | 64.5% | 38.5% | 11.0% | 7.6% |

The median commodity moves about as much as the gate, so in 2021 the gate
admits 342 of 400 commodities. Either raise it — 20% admits 6-43% depending on
year — or keep 5% as a *reporting* threshold and prioritise within it on
something else. The natural something else is impact-weighted: rank on
`|delta_B| × q`, the change in the factor times the commodity's output, so the
list answers "reduces cumulatively the most", which a percentage ranking does
not. `B_change_real.csv` carries `delta_B` and `B_from`; the weight has to be
joined from `q` or final demand.

⚠️ Sort `B_change_real.csv` on `abs_pct_change` but filter on `B_from` first.
A commodity whose factor rounds to zero posts a vast percentage off a
rounding-scale numerator.

### A third driver: the Make

The remediation step asks "was it the numerator or denominator that jumped".
For `B` that is a false dichotomy, because `B = (E / x) @ Vnorm` has three
moving parts and each year's `Vnorm` is built from that year's nowcast Make.
A commodity's factor can move with `E/x` perfectly flat, purely because the
industry-to-commodity mix moved.

Holding one side at a time and differencing, the Make's share of gross `B`
movement is:

| year | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 |
|---|---:|---:|---:|---:|---:|---:|---:|
| Make share of movement | 9.0% | 7.3% | 10.5% | 7.5% | 11.2% | **26.6%** | **21.6%** |

Small through 2022 and then not small: in 2023 a quarter of all `B` movement
is the Make, not emissions and not output. For 133 commodity-years — 4.7% of
the panel — the Make moved the factor **more than the intensity did**. Those
cannot be remediated on either the emissions or the output side, and chasing
them there would waste the effort. Worth adding as a third branch of the
remediation question, and worth a look at what changed in the 2023 Make.

### What "unjustified" can mean, concretely

The objective turns on separating justified change from unjustified. Four
mechanisms found so far that produce movement with nothing underneath it:

1. **Vintage relabelling.** EPA renumbered its soils tables mid-span —
   `T_5_17` to `T_5_18` for direct soils, `T_5_18` to `T_5_19` for indirect —
   and non-energy use moved `T_3_25b` to `T_3_25` for 2023. Keyed on the raw
   table number, 2019 showed −297 Mt against +255 Mt while the emissions ran
   flat at ~290 Mt. This class is definitionally unjustified. Already handled
   in the module via `ATTRIBUTION_ROLE_ALIAS`; `divergence_by_attribution_raw.csv`
   keeps it visible.
2. **Prices in a nominal denominator.** 73% of both the 2021 and 2022 E-vs-x
   gap is the price wedge between nominal and constant-dollar output. Any
   approach-1 ranking built on nominal `x` charges sources for inflation. Use
   the `*_real` tables.
3. **Attribution-vector churn.** A source attributed on a row of the Use table
   moves when the Use table moves, whether or not anything was emitted. About
   31% of E has its sector split derived from the IO tables.
   `output_elasticity.csv` measures which sources actually tracked output
   rather than assuming it from the attribution label.
4. **Make-mix churn.** The third driver above.

### Acceptance test

"Test for acceptance" needs a number to move. The module already computes two
that work as before-and-after measures, both on real-dollar EFs:

- **median absolute interannual EF change**, per year — currently 4.8% to
  16.9% depending on year;
- **share of commodities over the chosen gate**, per year — the table above.

Record both before any remediation, and re-run after. The second bullet of the
retest step — did the fix cause other EFs to move — is the same two numbers
plus a diff of `B_change_real.csv` against the pre-change copy, so an
improvement bought by displacing movement elsewhere is visible rather than
netted away.

