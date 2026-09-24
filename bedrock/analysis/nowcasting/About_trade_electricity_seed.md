# Trade electricity seed (#899) — grade results

Production module: [`trade_electricity_seed.py`](trade_electricity_seed.py).
Grade CLI: [`trade_electricity_seed_grade.py`](trade_electricity_seed_grade.py).

```bash
python -m bedrock.analysis.nowcasting.trade_electricity_seed_grade \
    --csv --check --mut-vintage v0.3.0_4276083
```

Schemas and Slack bars: plan §2A.3 / Decision rule (Candidate A = uniform EPA
Table 2.3 commercial; Candidate B = QCEW payroll).

## Grade window 2017–2024 (MUT `v0.3.0_4276083`, run 2026-09-24)

| candidate | max_uniform_yoy_spread (pp) | physical_proxy_cv | idiosyncratic_flag_count | held_missing_qcew | eia_level_gap_pct_2017 | pass_slack_bars |
|---|---:|---:|---:|---:|---:|---|
| `uniform_eia_commercial` (A) | ~0 (sanity) | **0.786** | 0 | 0 | −64.3 | **False** |
| `qcew_payroll` (B) | **47.7** | **0.771** | **16** | 0 | −64.3 | **False** |

Bars: A needs sanity spread ≤ 0.01 pp, idio == 0, and `physical_proxy_cv` ≤ 0.35.
B needs spread ≤ 5 pp every span, idio == 0, CV ≤ 0.35, and no QCEW holds.

**Choose / wire:** neither candidate passes → **do not wire** into
`composed_seed`; keep trade×`221100` on carry. Local CSVs (gitignored):
`trade_electricity_grade_summary.csv`, `trade_electricity_grade_spans.csv`.

### Notes

- A’s uniformity bars pass by construction; it fails only on
  `physical_proxy_cv` (Step-3 post–column-control dump / QCEW payroll, pooled
  2018–22). A failed CV does **not** mean Table 2.3 is wrong — EPA commercial
  is a broader end-use class than `WHOLESALE∪RETAIL`.
- B fails uniformity and idiosyncratic cuts as well as CV — not a fallback win.
- `eia_level_gap_pct_2017` is informational (IO trade×221100 vs Table 2.3
  commercial $); not a wire gate. Gap is large/negative because commercial
  revenue includes much more than trade.
- Soft-prefer #995 before any future wire still applies (opaque-MUT race only);
  grade itself does not wait on #995.
