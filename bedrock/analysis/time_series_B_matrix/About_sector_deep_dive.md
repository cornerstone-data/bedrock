# Sector deep dive

How to explain the trend in a commodity's emission factor. The triage answers the
first four questions for every sector on the list, and you write up the fifth.

```
uv run python -m bedrock.analysis.time_series_B_matrix.sector_deep_dive_triage --above-iv-score 0.2
```

| Step | Question | What to read |
|---|---|---|
| 1 | Which sectors? | IV score, from [`IV_score.py`](IV_score.py) |
| 2 | Did emissions change? | The trend in `E`, and its largest sources |
| 3 | Did output change? | The trend in `q`, in constant 2017 dollars |
| 4 | Did the supply chain change? | The driving factor, `B` or `L`, and the largest `A` cells |
| 5 | Why? | Your explanation, from the sources and evidence |

The run writes two files to `output/sector_deep_dive_triage/`: a CSV with one row per
sector, and `*_draft.md`, with the tracker rows and log entries ready to paste. The
entries show `E` and `q` as curves with 2017 = 1, saved to `images/sector_trends/`,
and the CSV has each year's value.
`--iv-csv` chooses the `IV_score.csv`, and so the weights. `--codes` runs named
sectors.

## Reading the result

- **Driving factor mostly `B`:** look at `E` and `q`. `B` is roughly `E / q`, so
  they show whether emissions or output moved it.
- **Driving factor mostly `L`:** look at the largest `A` cells, which say whose
  purchases changed. Those cells add up to the `L` part exactly.
- **Both:** do both. A commodity near 50% has real movement from each.

`N_d`, `L_dom` and the driving factor are domestic, matching the IV score. `peak_year`
in the CSV is the year furthest from the trend, if you want one year to open.

## Recording it

- Write the explanation in
  [`About_sector_trend_explanations.md`](About_sector_trend_explanations.md), and say
  which vintages it was checked on. Keep it short and link to the evidence.
- Update only the status and links in the tracker,
  [`B_driver_investigation.md`](B_driver_investigation.md).

## Going further

Use these only when the steps above don't explain a sector. They need the tables from
[`B_change_diagnostics.py`](B_change_diagnostics.py), written by its `main` at the same
vintages. Those tables use the full `L` and `N`, so their `N` figures won't match the
triage. `divergence_by_sector_stratum.csv` is on nominal output; use
`divergence_detail_real.csv` beside real figures. The `D#` codes are listed under "Key to the Diagnostic column" in
[`About_sector_trend_explanations.md`](About_sector_trend_explanations.md).

### Direct path — output, emissions, intensity

For sector `s` = same code (or Vnorm parents if secondary product):

1. **Levels (D12):** `x_real`, sector E, commodity B for Y−1 and Y — `%Δx`,
   `%ΔE`, `%Δ(E/x)`.
2. **Flag A — E/x diverge:** `|%ΔE − %Δx|` large while B moves → intensity
   story; if both move together and B is flat, the N move is elsewhere (L).
3. **Flag B — oscillation (D9):** `sector_stratum_span.csv` for `s`; high
   `oscillation` (~1) = churn candidate; low = trend / likely real.
4. **Flag C — elasticity (D4):** `output_elasticity_real.csv` for the sector's
   dominant attribution roles — slope far from 1 means E does not track x.

### Direct path — inventory / attribution

1. **Stratum mass (D8):** `divergence_by_sector_stratum.csv` for `s` and year
   Y — rank MetaSources × AttributionSources by absolute divergence.
2. **Commodity B split (D7):** `B_by_attribution.csv` for the commodity.
3. **Class route (D2 / D10):** map AttributionSources → role/class (`Direct`,
   Use-after-redef, MECS, facility/hybrid). Labels like MECS can still ride
   **Use money** (tracker `petroleum_industrial` pattern).
4. **Activity set / GHGI lines:** year GHG method YAML activity sets for that
   MetaSources stem, and/or
   [`inventory_fbs_sources`](../emissions_source_crosswalk/inventory_fbs_sources.py).
5. **Optional facility (D14–D16):** combustion / table 3-11 family →
   `--facility-data`.

### Direct path — Vnorm / Make

D13 `vnorm_share_of_B_movement.csv`; if material,
[`supply_mix_diagnostic`](../nowcasting/supply_mix_diagnostic.py).

### Structure path — Use / A / L

When D6b says L dominates, or N moves with flat B:

1. [`nd_drivers`](../nowcasting/results/nd_drivers.py) for `code` and pair
   Y−1→Y: emissions vs structure effect, then top **A_d** cells.
2. Map large ΔA cells to **Use purchases** (row i bought by column j).
3. Context: [`a_influence`](../nowcasting/results/a_influence.py),
   [`L_flux_priority`](../nowcasting/L_flux_priority.py);
   dollar-basis notes in `About_the_L_dollar_basis.md` /
   `About_the_nd_driver_basis.md`.
