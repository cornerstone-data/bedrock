# Pre-MECS Industrial-weight freeze

EIA-anchored G/T/D mixed units with **dollar** manufacturing weights inside
Industrial — the production path immediately before Table 7.7 purchased-kWh
shares. v0.3.1 footing stays a separate comparison.

Generate (needs a full mixed-units run). Also writes ``D.parquet``,
``class_generation_mwh.parquet``, and ``run_metadata.json`` for the comparison
deck. Pass ``--config 2025_usa_cornerstone_v0_3_electricity_disaggregation`` for
the monetary 3-way freeze.

```bash
python -m bedrock.analysis.electricity.historical.pre_mecs_industrial_weights.write_freeze
```

Compare live MECS output with
`python -m bedrock.analysis.electricity.current.vs_pre_mecs_industrial_weights`.
The compare script is not a CI gate.
