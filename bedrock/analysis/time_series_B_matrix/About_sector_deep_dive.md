# Sector deep-dive checklist (D/N year moves)

Entry path for a commodity-year the IV score or assessment flags. Dig tables
explain *why*; IV only names *which* code. Use real (2017$) tables. Pin the
same FBS/MUT vintages the IV run used (`--list-vintages`; assessment pins in
[`time_series_matrix_assessment.py`](time_series_matrix_assessment.py)).

Tracker: [`B_driver_investigation.md`](B_driver_investigation.md) · D# register:
[`B_matrix_smoothing_plan.md`](B_matrix_smoothing_plan.md) · IV:
[`IV_score.py`](IV_score.py) / `About_IV_score.md` (when on the branch that
ships it)

**Batch triage** (IV top-N × dig peak year → path B/L/both + top stratum)::

    uv run python -m bedrock.analysis.time_series_B_matrix.sector_deep_dive_triage --top 30

## Intake

- **Code** (BEA detail), **peak pair** (Y−1 → Y from IV `peak_year` or the
  visual jump), **signal** (B jump, N jump, or both).
- Open assessment CSV/HTML for that code: levels of **E (Mt)**, **B**, **N**
  (normalized and absolute) under `output/time_series_matrix_assessment/` or
  the module's assessment dir.
- Industry vs commodity: the primary pair shares the code; B can reshuffle via
  Vnorm, so industry E/x need not equal commodity B.

## Triage: B problem or L/A problem?

From dig [`B_change_real.csv`](B_change_diagnostics.py) (D6a/D6b) for
`(code, year=Y)`:

| Look at | If large | Branch |
|---|---|---|
| `abs_delta_B_pct_of_N` / `%ΔB` | yes | **Direct path** |
| `pct_change_N_L_effect` vs `pct_change_N_L_held` | L effect dominates | **Structure path** |
| Both | — | Run both; do not assume B explains N |

## Direct path — output, emissions, intensity

For sector `s` = same code (or Vnorm parents if secondary product):

1. **Levels (D12):** `x_real`, sector E, commodity B for Y−1 and Y — `%Δx`,
   `%ΔE`, `%Δ(E/x)`.
2. **Flag A — E/x diverge:** `|%ΔE − %Δx|` large while B moves → intensity
   story; if both move together and B is flat, the N move is elsewhere (L).
3. **Flag B — oscillation (D9):** `sector_stratum_span.csv` for `s`; high
   `oscillation` (~1) = churn candidate; low = trend / likely real.
4. **Flag C — elasticity (D4):** `output_elasticity_real.csv` for the sector's
   dominant attribution roles — slope far from 1 means E does not track x.

## Direct path — inventory / attribution

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

## Direct path — Vnorm / Make

D13 `vnorm_share_of_B_movement.csv`; if material,
[`supply_mix_diagnostic`](../nowcasting/supply_mix_diagnostic.py).

## Structure path — Use / A / L

When D6b says L dominates, or N moves with flat B:

1. [`nd_drivers`](../nowcasting/results/nd_drivers.py) for `code` and pair
   Y−1→Y: emissions vs structure effect, then top **A_d** cells.
2. Map large ΔA cells to **Use purchases** (row i bought by column j).
3. Context: [`a_influence`](../nowcasting/results/a_influence.py),
   [`L_flux_priority`](../nowcasting/L_flux_priority.py);
   dollar-basis notes in `About_the_L_dollar_basis.md` /
   `About_the_nd_driver_basis.md`.

## Verdict write-up

- Facts: `%Δx`, `%ΔE`, `%ΔB`, `%ΔN`, L-held vs L-effect shares
- Dominant stratum: MetaSources + AttributionSources + class/route
- Flags hit (A–C, Use-cell spike, facility floor breach)
- **Justified?** / not / out of scope / unresolved — tracker vocabulary
- Next lever: FBS method, attribution vector, nowcast Use/Make, or leave alone

Rebuild dig (+ assessment) at the IV pins before quoting levels. Tracker rows
and [`About_B_change_drivers.md`](About_B_change_drivers.md) figures may
predate the current pins; treat them as a D# map, not as evidence.
