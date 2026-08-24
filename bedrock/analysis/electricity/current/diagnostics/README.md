# Electricity disaggregation diagnostics

Diagnostics for **current** production flags
(`implement_electricity_disaggregation` / `implement_electricity_mixed_units`).
Conversion copy in some reports still describes the old Table 2.4 path — do not
paste into a deck until rewritten.

Scripts are grouped by analysis so each subpackage owns the question it answers.
Shared cache / path helpers stay at package root. All reports and figures still
write under [`output/`](output/).

```
diagnostics/
  paths.py, manifest.py, local_workbooks.py, manifest.yaml, local_data/
  bly_dispersion/       # BLy waterfalls from diagnostics sheets
  ef_comparison/        # N/D vs v0.3 footing + N-variance writeup
  full_trace/           # live model IO / E / D / N / BLy walkthrough
  year_alignment/       # BLy vs E under A/q year handling
  hh_vs_interindustry/  # F01000 BLy attribution vs interindustry (not D0 class MWh)
  probes/               # one-off sector probes
  output/               # reports/figures (gitignored)
  __tests__/
```

Run everything from the **repo root** with the project venv
(`.venv\Scripts\python.exe` on Windows, or `uv run` if available).

---

## Shared prerequisites

1. **Configs** (in repo):
   - `2025_usa_cornerstone_v0_3_electricity_footing` — footing
   - `2025_usa_cornerstone_v0_3_electricity_reallocation`
   - `2025_usa_cornerstone_v0_3_electricity_disaggregation`
   - `2025_usa_cornerstone_v0_3_electricity_mixed_units` — FINAL

2. **Sheet-based analyses** (`bly_dispersion`, `ef_comparison.plot_ef`) also need
   diagnostics workbooks (local Excel or live Google Sheets) — see below.
   [`manifest.yaml`](manifest.yaml) `sheet_id` values are placeholders until Phase D
   sheets are created.

3. **Live-model analyses** (`full_trace`, `hh_vs_interindustry`,
   `probes`, `ef_comparison.analyze_n_variance`, `year_alignment`) need a working
   Bedrock data / model environment for those configs (same as running
   Cornerstone transforms).

---

## 1. BLy dispersion waterfalls — `bly_dispersion/`

Chained incremental BLy dispersion and net-change charts for
PR2 (reallocation) → PR3 (3-way split) → PR4 (mixed units), vs Cornerstone
v0.3.1 electricity footing.

| Module | Role |
|---|---|
| `run_all` | Entry point: load cache → write waterfall PNGs |
| `import_local` | Seed parquet cache from downloaded `.xlsx` |
| `refresh_cache` | Seed cache from Google Sheets (`manifest.yaml`) |
| `bly`, `dispersion`, `net_change`, `waterfall` | Library helpers |

### Sheet inputs

Trigger [`.github/workflows/generate_diagnostics.yml`](../../../.github/workflows/generate_diagnostics.yml)
four times (`use_useeio_baseline` unchecked), one per config above.

When generating diagnostics, pass **`--baseline v0.3.1`** (or the raw SHA
`00524c3c…`). Do **not** use `--baseline v0.3` — that alias points at the
retired `v0_3_0` footing, not this suite.

Then either:

**A — local Excel (no Google API)**

Download each diagnostics workbook (**File → Download → Microsoft Excel**) into
[`local_data/`](local_data/) as:

| File | Config |
|---|---|
| `2025_usa_cornerstone_v0_3_electricity_footing.xlsx` | footing |
| `2025_usa_cornerstone_v0_3_electricity_reallocation.xlsx` | step 1 |
| `2025_usa_cornerstone_v0_3_electricity_disaggregation.xlsx` | step 2 |
| `2025_usa_cornerstone_v0_3_electricity_mixed_units.xlsx` | step 3 + FINAL |

```bash
python -m bedrock.analysis.electricity.current.diagnostics.bly_dispersion.run_all \
  --local-dir bedrock/analysis/electricity/current/diagnostics/local_data
```

Or two steps:

```bash
python -m bedrock.analysis.electricity.current.diagnostics.bly_dispersion.import_local
python -m bedrock.analysis.electricity.current.diagnostics.bly_dispersion.run_all
```

**B — live Google Sheets**

Replace placeholder `sheet_id` values in [`manifest.yaml`](manifest.yaml) once
Phase D sheets exist, then:

```bash
python -m bedrock.analysis.electricity.current.diagnostics.bly_dispersion.refresh_cache
python -m bedrock.analysis.electricity.current.diagnostics.bly_dispersion.run_all
# or: ...run_all --refresh
```

Shorthand: `python -m bedrock.analysis.electricity.current.diagnostics.bly_dispersion`
(same as `run_all`).

### Outputs

- `output/electricity_bly_dispersion_waterfall_mmt.png`
- `output/electricity_bly_dispersion_waterfall_pct.png`
- `output/electricity_bly_net_change_waterfall_mmt.png`
- `output/electricity_bly_net_change_waterfall_pct.png`

**Metrics:** dispersion bars = `Σ_sector |ΔBLy|`; net-change level bars = `Σ BLy_new`;
a signed “BLy change due to …” bar appears only when total U.S. BLy changes between steps.

---

## 2. EF comparison vs footing — `ef_comparison/`

Compares **each electricity step** to the **v0.3.1 electricity footing** workbook’s
absolute `N_new` / `D_new` (not to the previous step).

| Module | Role |
|---|---|
| `plot_ef` | Suite PNGs + 3-panel N/D histograms |
| `vs_footing_frames` | Frame builders for `plot_ef` |
| `analyze_n_variance` | Why sector total-EF (N) rises at 3-way / mixed units |

```bash
python -m bedrock.analysis.electricity.current.diagnostics.ef_comparison.plot_ef \
  --local-dir bedrock/analysis/electricity/current/diagnostics/local_data
```

Shorthand: `python -m bedrock.analysis.electricity.current.diagnostics.ef_comparison`

`plot_ef` seeds the cache with `REQUIRED_TABS + EF_TABS`. BLy-only import via
`bly_dispersion` still uses `REQUIRED_TABS` only.

```bash
# Live model — writes under output/ef/panel/
python -m bedrock.analysis.electricity.current.diagnostics.ef_comparison.analyze_n_variance
```

### Outputs (`output/ef/`)

| Path | Contents |
|---|---|
| `electricity_reallocation/` | Suite PNGs for that step vs v0.3 footing |
| `electricity_disaggregation/` | Suite PNGs for 3-way vs v0.3 footing |
| `electricity_mixed_units/` | Suite PNGs for mixed units vs v0.3 footing |
| `panel/ef_panels_vs_v0_3_N.png` | 3-panel N % hist |
| `panel/ef_panels_vs_v0_3_D.png` | 3-panel D % hist |
| `panel/n_variance_*.csv`, `n_variance_explained.md` | From `analyze_n_variance` |

Dropped sectors (e.g. mixed-units `221110` kg/MWh vs kg/USD) are footnoted on figures.

---

## 3. Full model trace — `full_trace/`

Live walkthrough of IO / E / D / N / BLy across the four configs.

| Module | Role |
|---|---|
| `full_trace` | Main markdown report |
| `decompose_d_n_step` | Detailed D/N step decomposition (appended into the report) |

```bash
python -m bedrock.analysis.electricity.current.diagnostics.full_trace.full_trace
# or: python -m bedrock.analysis.electricity.current.diagnostics.full_trace

python -m bedrock.analysis.electricity.current.diagnostics.full_trace.decompose_d_n_step
```

### Output

- `output/electricity_full_trace.md`

---

## 4. Year alignment (BLy vs E) — `year_alignment/`

Documents year handling for E / B / A / q / L / D / N under mixed units, and
probes a single-year 2017 attempt (blockers + proxies). Uses
`2025_usa_cornerstone_v0_3_electricity_mixed_units`.

```bash
python -m bedrock.analysis.electricity.current.diagnostics.year_alignment.year_alignment_bly_e
# or: python -m bedrock.analysis.electricity.current.diagnostics.year_alignment
```

### Outputs (`output/year_alignment/`)

- `bly_e_year_alignment.md` (+ JSON companion when written)

---

## 5. Household vs interindustry BLy — `hh_vs_interindustry/`

Final-demand attribution of F01000 BLy vs interindustry under mixed units
(slides 33–34). Not D0 class MWh.

| Module | Role |
|---|---|
| `hh_vs_interindustry` | F01000 BLy vs interindustry |

```bash
python -m bedrock.analysis.electricity.current.diagnostics.hh_vs_interindustry.hh_vs_interindustry
# or: python -m bedrock.analysis.electricity.current.diagnostics.hh_vs_interindustry
```

### Outputs (`output/hh_vs_interindustry/`)

- `hh_vs_interindustry_mwh_bly.md` / `.json`

---

## 6. Sector probes — `probes/`

| Module | Role |
|---|---|
| `probe_221200` | Why gas-distribution commodity D falls after co-production reallocation |

```bash
python -m bedrock.analysis.electricity.current.diagnostics.probes.probe_221200
# or: python -m bedrock.analysis.electricity.current.diagnostics.probes
```

Prints to stdout (no dedicated output file).

---

## Suggested order

1. Sheet cache + BLy waterfalls (`bly_dispersion`) and EF plots (`ef_comparison.plot_ef`)
2. Live full trace (`full_trace`) once models resolve
3. Targeted follow-ups: `hh_vs_interindustry`; `year_alignment`;
   `analyze_n_variance`; `probes` as needed

---

## Tests

```bash
python -m pytest bedrock/analysis/electricity/current/diagnostics/__tests__ -q
```
