# Historical electricity analyses

Last commit of the original UGO / Table 8.3 / Table 2.4 production path:

`af4a82994d9302bb6efef33633f3b2eb251b31b7`
(`feat(eeio): freeze current electricity 3-way and mixed-units production`).

EIA-anchored G/T/D replaced that path in a later commit. Method discussion:
[Discussion #85](https://github.com/cornerstone-data/methods/discussions/85)
and [Discussion #88](https://github.com/cornerstone-data/methods/discussions/88).

The freeze under `original_elec_disagg_implementation/output/<config_stem>/` is
**read-only**. It records the original implementation for

- `2025_usa_cornerstone_v0_3_electricity_disaggregation`
- `2025_usa_cornerstone_v0_3_electricity_mixed_units` (also has `c_row.parquet`)

Do not run historical Python against current production. Compare live EIA
output to the freeze with
`bedrock.analysis.electricity.current.vs_original_elec_disagg.compare_to_original_elec_disagg`.
