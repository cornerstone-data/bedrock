# Redefinition-ratio Step 7

Planning and reconstruction docs for production Step 7: `#572` / `plan.md`-style per-cell GO-ratio transform.

| File | Contents |
| --- | --- |
| [ratio-plan.md](ratio-plan.md) | Source-of-truth implementation plan (detail Step 7 ratio carry) |
| [summary-span-test-plan.md](summary-span-test-plan.md) | Code plan: summary 2018–2024 span test for PR #775 |
| [summary-span-test-report.md](summary-span-test-report.md) | Run artifact: rollup gate, 2017 round-trip, 2018–2024 L1 scores |
| [summary_redef_span_test.py](summary_redef_span_test.py) | Script: learn 2017 summary ratios; score frozen carry |
| [ratio-reconstruction-report.md](ratio-reconstruction-report.md) | 2017 reconstruction results (full-grid match on all five frames) |

```bash
uv run python -m bedrock.analysis.nowcasting.after_redef_MUTs.redefinitions_ratio_implementation.summary_redef_span_test
```

**Motivation:** [BEA-style_redefinitions_reconstruction_report.md](../BEA_style_redefinitions_implementation/BEA-style_redefinitions_reconstruction_report.md) — Chapter 9 + named rules do not reconstruct published 2017 Use/VA/Import/Margins without a large residual; production follows the ratio-carry acceptance story instead.
