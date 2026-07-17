# Methodology feature flags

Playbook for adding a methodology toggle on `USAConfig`, wiring it in the
pipeline, and producing an atomic config YAML so diagnostics can measure its
impact in isolation.

EF diagnostics against that config:
[`../validation/evaluate_feature_impact.md`](../validation/evaluate_feature_impact.md).

## When a flag is needed

Use a flag when a methodology choice must be selectable per config YAML
(atomic evaluation, release progression, or opt-in vs the canonical model).
Default every flag to `False` on `USAConfig` so existing configs keep their
behavior until they opt in.

## 1. Add the field on `USAConfig`

Edit [`usa_config.py`](usa_config.py):

1. Place the field under the matching `#####` / `###` section (schema, IO,
   GHG, inflation, etc.).
2. Declare `bool = False` with a trailing `# DRI: <owner>` comment
   (**DRI** = Directly Responsible Individual — the person accountable for
   that flag), or use `Field(default=False, description=...)` when the
   constraint needs prose next to the field.
3. Add a `@model_validator(mode='after')` only for real incompatibilities
   (see `_validate_ghg_flag_compatibility`, `_validate_margins_mutual_exclusivity`,
   `_validate_deflate_x_requires_use_e_for_x_in_b`). Do not add validators that
   only restate “this flag must be used with its own YAML.”

Unresolved keys in a config YAML fail strict Pydantic validation, so the
field must exist on `USAConfig` before any YAML references it.

## 2. Gate the behavior in transform / extract

Branch on the flag at the call site that implements the change:

```python
from bedrock.utils.config.usa_config import get_usa_config

if get_usa_config().my_feature_flag:
    ...
```

Keep the gate local to the change. Prefer reading the flag once into a local
(`usa = get_usa_config()` / `cfg = get_usa_config()`) when a function already
holds the config object.

## 3. Add an atomic config YAML

An **atomic config** flips a single methodology flag relative to a baseline
so diagnostics measure that change in isolation (definition also in
[`../snapshots/README.md`](../snapshots/README.md)).

Create `bedrock/utils/config/configs/<name>.yaml`:

- Set the flag to `true`.
- Set any hard prerequisites the validators or call site require (for
  example `load_E_from_flowsa: true` when the gated path loads E from
  flowsa).
- Set `snapshot_version_or_git_sha` to the baseline diagnostics should
  compare against — the value whose parquet EFs become `N_old` / `D_old`
  on the sheet. Pick the baseline that matches the evaluation question:
  - **Latest accepted Bedrock / Cornerstone model** — the SHA in
    [`../snapshots/.SNAPSHOT_KEY`](../snapshots/.SNAPSHOT_KEY) (must also
    appear on the `USAConfig.snapshot_version_or_git_sha` Literal). Use this
    when the question is “what does this feature change relative to the
    model users already receive?”
  - **A prior Bedrock / Cornerstone release** — a release SHA in that
    Literal (`# v0.2`, `# v0.3.0`, etc.).
  - **Legacy CEDA-US v0** — set `'v0'`, or omit the key because the field
    default is `'v0'`. Omitting the key does **not** select
    `.SNAPSHOT_KEY`.
- Omit unrelated keys; `USAConfig` fills defaults for everything else.

`'v0'` means the legacy CEDA-US baseline. A SHA from `.SNAPSHOT_KEY` or
`snapshots/releases.py` represents a Bedrock / Cornerstone model snapshot.
Both use the snapshot loader; do not label a Bedrock / Cornerstone snapshot
as “CEDA snapshot” in diagnostics titles or findings.

Config name = filename without `.yaml` (this is the `config_name` passed to
`generate_diagnostics`).

Do **not** enable the flag on the canonical snapshot config
(`2025_usa_cornerstone_v0_3`, see `CANONICAL_USA_CONFIG` in `usa_config.py`)
until the methodology is accepted for that release. Do **not** remove old
entries from the `snapshot_version_or_git_sha` Literal — atomic configs and
fixtures may still pin them.

## 4. Verify intended config surface

For **FBS method** YAML edits, use the `diff_methods` CLI described in
[`README.md`](README.md) (§ Atomic FBS change testing).

For **USA config** YAMLs, load both configs and confirm only the intended
fields differ (for example via
`USAConfig.model_validate` / `_load_usa_config_from_file_name` in a short
script or REPL). The diagnostics sheet’s `config_summary` tab records the
resolved flag set for a run.

## Examples (patterns only)

| Flag | Gate | Atomic-style config |
|---|---|---|
| `update_mecs_method` | `bedrock/transform/allocation/derived.py` | `configs/2025_usa_cornerstone_ghg_mecs.yaml` |
| `load_useeio_nowcast_A_matrix` | `derive_cornerstone_Aq_scaled()` in `bedrock/transform/eeio/derived_cornerstone.py` | `configs/2025_usa_cornerstone_A_useeio_nowcast.yaml` (A-only) / `configs/2025_usa_cornerstone_v0_2_A_useeio_nowcast.yaml` (full-model stack for EF runs) |

## Related

[`../validation/evaluate_feature_impact.md`](../validation/evaluate_feature_impact.md)
— dispatch `generate_diagnostics`, plot, and interpret impact.
