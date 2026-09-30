# RCRA path record

**Locked for analysis:** Use-intersection shares come from Biennial
Report shipper→receiver rows via
`rcra_waste_flows.build_intersection_mass_from_br` /
`load_rcra_intersection_shares`.

Provenance token on every treatment derive:

```text
rcra_path=br_bypass
```

(plus BR row stats in `WeightDerivationProvenance.fallback_notes`).

**Follow-on BR→FBS work** (separate PR) owns bedrock BR→FBS replacement and retiring this
bypass. Do not block multi-year impacts on FBS work.
