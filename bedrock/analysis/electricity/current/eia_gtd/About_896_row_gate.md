# About the #896 electricity Use-row gate

Diagnostic for
[issue #896](https://github.com/cornerstone-data/bedrock/issues/896): decide
whether the 2023–24 electricity share collapse is a **target** problem
(`T016 − ΣY`) or an **allocation** problem (seeds / carry / GRAS), without
changing the production MUT or the G/T/D allocator.

How to read schemas / classification rules: this note.
Measured results: **Results** in
[`issue_896_electricity_row_shares.md`](issue_896_electricity_row_shares.md).

## Run

```bash
python -m bedrock.analysis.electricity.current.eia_gtd.electricity_row_896 \
    --mode bands|gate|stages|seed_status|aies_seam|all \
    --mut-vintage v0.3.0_4276083 --csv [--check] \
    [--anchor-span 2022-2023]
```

`--years` (default `2017-2024`) drives **bands** and **gate**.
`--anchor-span` (default `2022-2023`) drives **stages**, **seed_status** top-N,
and which industries **aies_seam** ranks. AIES **index** years stay pinned at
2022/2023 (SAS→AIES survey break) and are never retargeted by `--anchor-span`.

`--mut-vintage` pins the MUT loaders only. Gate **targets** still call
`interior_row_targets` → live Supply / FD extracts
(`download_sources_ok=True`). Missing T016 or auth failure raises; the known
2022 supply–use gap (~$10.4bn) is always written as `supply_use_gap_usd`, never
silently equated away.

`--check` fails only on arithmetic inconsistency (band-sum identity, share
denominators, NaNs), not on classification outcome.

## Modes and CSVs

| Mode | CSV | Schema |
|---|---|---|
| `bands` | `electricity_row_896_bands.csv` | `BandShareEffectRow` |
| `gate` | `electricity_row_896_gate_years.csv`, `…_gate_decisions.csv` | `GateYear`, `GateDecision` |
| `stages` | `electricity_row_896_stages.csv` | `StageCellMove` |
| `seed_status` | `electricity_row_896_seed_status.csv` | `SeedStatusRow` (`year_a`/`year_b`/`share_effect_bn`) |
| `aies_seam` | `electricity_row_896_aies_seam.csv` | index ratio + hold-2022 CF $bn |

## Band partition (claim map)

Each Use industry is in exactly one band:

| Band | Membership |
|---|---|
| `manufacturing` | `materials_seed` / `nonmaterial_seed` columns (BEA 31–33) |
| `services_transport` | `services_transport_industries()` |
| `agriculture` | `farm_industries()` |
| `mining` | `MINING_SEEDED` |
| `utilities` | `ELECTRIC` (`221100`, `S00101`, `S00202`) |
| `trade` | keys of `WHOLESALE` ∪ `RETAIL` (production seed set) |
| `trade_unseeded` | other `42` / `44RT` detail (e.g. `425000`) |
| `held_2017` | everything else |

`trade_diagnostic_bn = trade + trade_unseeded` is printed for context and is
**not** part of the band-sum `--check` identity.

## Classification rule (`GateDecision`)

1. `|Δrealized_usd| < $5bn` and `|Δrealized_share| < 0.15pp` → `stable`
2. `|Δrealized_usd| < $5bn` and `|Δrealized_share| ≥ 0.15pp` → shares only:
   `target_collapse` if `share_parallel` else `allocation`
3. `target_fraction_of_level ≥ 0.70` and `share_parallel` → `target_collapse`
4. `0.40 ≤ target_fraction_of_level < 0.70` and `share_parallel` → `ambiguous`
5. else → `allocation`

`share_parallel`: same sign and
`|Δtarget_share / Δrealized_share − 1| ≤ 0.30` when `|Δrealized_share| ≥ 0.05pp`;
else false.

Sensitivity classifications `classification_at_0_60` / `…_0_80` use level cuts
0.60 / 0.80; `stable` is unchanged.

**Target scheduling** uses 2022→23 and 2023→24 — those two spans are 82% and
136% share effect, against 31% for 2020→21. **Allocation diagnostics must not
inherit that window.** The cell-level `L`-flux ranking (`cf888a6`) puts six of
eight churn commodities' worst year in 2020–2022, and 52.9% of `441000`'s
electricity-coefficient movement outside these two spans. Follow-up allocation
work (#900 / #899) grades on **2017–2024**; the services electricity seam
triggers only on crisis spans after a post-target re-measure.

## Stages vintage caveat

`step3` / `post_ipf` are **re-assembled today** via
`assemble_use_seed(fitted=False|True)` ($M → USD). `post_gras` is the **pinned
MUT** Use cell. If the live assembly has drifted from the pin, the module prints
`STALE_ASSEMBLY_WARNING` (row-net and/or top-N max cell) and still writes the
CSV. Cell-level GRAS vs IPF is qualitative under warning; the hard gate remains
`GateDecision` on target vs realized.

## Phase 2T / #990 target attribution

One-shot dump (not a standing gate mode):

```bash
python -m bedrock.analysis.electricity.current.eia_gtd.target_attribution_221100 \
    --csv [--check]
```

Schemas / Fix-vs-Attributed decision:
[`About_990_target_attribution.md`](About_990_target_attribution.md).

## Unit tests

```bash
uv run pytest bedrock/analysis/electricity/current/eia_gtd/__tests__/test_electricity_row_896.py
uv run pytest bedrock/analysis/electricity/current/eia_gtd/__tests__/test_target_attribution_221100.py
```

Pure fixtures: claim-map partition, `GateDecision` classification, `--anchor-span`
parse; Phase 2T signed identity / Fix-vs-Attributed rules. No GCS / MUT I/O.
