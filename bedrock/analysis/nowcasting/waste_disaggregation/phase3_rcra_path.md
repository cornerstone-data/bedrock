# Phase 3.2 — RCRA path record

**Locked for Phase 3 analysis:** Use-intersection shares come from Biennial
Report shipper→receiver rows via
`rcra_waste_flows.build_intersection_mass_from_br` /
`load_rcra_intersection_shares`.

Provenance token on every treatment derive:

```text
rcra_path=br_bypass
```

(plus BR row stats in `WeightDerivationProvenance.fallback_notes`).

**Phase 4** (separate PR) owns bedrock BR→FBS replacement and retiring this
bypass. Do not block Phase 3.3 multi-year impacts on FBS work.
