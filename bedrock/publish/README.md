# publish

Publishing pipeline for the bedrock EEIO model. Exports the model to
shareable file formats, mirroring the shape of `useeior`'s
`writeModeltoXLSX` output.

## Status

- **XLSX**: implemented for `B`, `C` (trivial row-summer; see divergence
  note below), `D` (`= C @ B`), `q`, plus metadata sheets
  (`commodities_meta`, `industries_meta`, `final_demand_meta`,
  `value_added_meta`, `config_summary`, `model_info`).
- **Supply-chain factors**: not implemented. Upstream R
  counterpart:
  [cornerstone-data/supply-chain-factors](https://github.com/cornerstone-data/supply-chain-factors).
- **Additional matrices** (`V`, `U`, `U_d`, `A`, `A_d`, `L`, `L_d`,
  `M`, `M_d`, `N`, `N_d`, `Rho`, `Phi`, `Tau`, `x`): registered as
  placeholders in `excel/writer.py` (`getter = lambda: None`), so their
  sheets are simply omitted. Each entry carries an inline `TODO`
  pointing at the missing derivation.
- **Import-emissions matrices** (`A_m`, `M_m`, `N_m`): require real
  import emission factors (`B_imp`), which bedrock does not yet
  produce. Blocked on `B_imp`.

## Known divergence from useeior (B units)

In `useeior`, `B` is in `kg of physical gas / USD` and a real GWP
characterization matrix `C` (indicator x flow) produces `D = C @ B` in
`kgCO2e / USD`.

In bedrock (Cornerstone), `B` is **already in `kgCO2e / USD`**: AR6 GWPs
are applied upstream at FBS aggregation in
`bedrock.transform.allocation.derived`. The 7 rows are aggregated GHG
groups (`CO2`, `CH4`, `N2O`, `HFCs`, `PFCs`, `SF6`, `NF3`).

Consequences for the published workbook:

1. **`B` sheets are not numerically comparable** between bedrock and
   useeior. Row dimensions differ (7 vs ~2000), and bedrock values are
   per-row GWP-weighted CO2e while useeior values are physical mass.
2. **The bedrock `C` and `D` sheets are emitted for useeior-shape
   parity only.** `C` is a trivial `(1, 7)` row-summer (single
   `Greenhouse Gases` indicator, all ones); `D = C @ B` reduces to
   `B.sum(axis=0)`. They do *not* carry GWP characterization
   information.
3. The `model_info` sheet documents this with `b_units`,
   `b_characterized`, `gwp_set`, `c_kind`, and
   `divergence_from_useeior` fields so downstream consumers cannot
   silently misinterpret the workbook.

Resolution paths (TODO):

- **A.** Switch bedrock `B` to physical-mass units and ship a real GWP
  `C` from `bedrock.utils.emissions.gwp.GWP100_AR6_CEDA`.
- **B.** Emit a `B_phys` companion sheet alongside the CO2e `B`, with a
  real GWP `C` keyed to `B_phys`.

Until one of those lands, `useeior_D[Greenhouse Gases]` is the
like-for-like comparable quantity for `bedrock_B.sum(axis=0)`.

## Recommended publish workflow

1. Generate parquet snapshots on `main` for the target config:

   ```
   uv run python -m bedrock.utils.snapshots.generate_snapshots --config_name <cfg>
   ```

2. **(Pre-flight)** Confirm bedrock-at-HEAD reproduces the snapshot:

   ```
   uv run pytest -m eeio_integration bedrock/publish/__tests__/test_excel_vs_snapshot.py
   ```

   This step is logically independent of step 3 -- the test runs its own
   `write_model_to_xlsx` to a tmp directory and compares against the
   parquet snapshot.

   **The test is hard-coded to `2025_usa_cornerstone_full_model`.** If
   `<cfg>` differs, this step does NOT validate the artifact you are
   about to publish. (TODO: parameterize the test to validate arbitrary
   configs; tracked in
   [bedrock/publish/__tests__/test_excel_vs_snapshot.py](__tests__/test_excel_vs_snapshot.py).)

   **Skippable when** all three hold:
   - `<cfg>` is `2025_usa_cornerstone_full_model`, AND
   - you are publishing from a `main` SHA covered by the most recent
     green
     [`test_integration.yml`](../../.github/workflows/test_integration.yml)
     run (cron: 7am/4pm PT weekdays), AND
   - no derivation-affecting code has merged since that CI run.

   On a feature branch, after a fresh merge, or for any non-default
   `<cfg>`, run it locally.

3. Run the publish CLI for the same config:

   ```
   uv run python -m bedrock.publish.excel.cli --config_name <cfg>
   ```

   This writes `bedrock/publish/output/<git_sha>/<cfg>.xlsx`.

4. Inspect the workbook (especially `model_info`). Manual distribution
   for now; GCS upload is a TODO.
