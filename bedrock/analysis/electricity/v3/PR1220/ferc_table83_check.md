# #1215 check: `p_gen` vs EIA Table 8.3 (FERC Form 1 purchased power)

**Ask ([#1215](https://github.com/cornerstone-data/bedrock/issues/1215)):** EIA-861 sales for resale is the first candidate for `p_gen`; check it against FERC purchased power (EIA Table 8.3).

**What Table 8.3 can and cannot do:** it reports **purchased-power expense dollars** for **major investor-owned utilities only**. It has no MWh, so it cannot produce a second ¢/kWh. The check is whether 861 sales-for-resale dollars — and the `p_gen` built from them — move with that IOU expense.

**Run:** `python -m bedrock.analysis.electricity.v3.PR1220.ferc_table83_check` → `output/ferc_table83_check.csv`.

## Result (2017–2024)

| | 2017 | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `p_gen`, ¢/kWh (861) | 4.11 | 4.36 | 4.22 | 3.83 | 5.68 | 6.29 | 5.27 | 5.00 |
| 861 sales-for-resale revenue, $bn | 142 | 158 | 154 | 140 | 199 | 216 | 168 | 148 |
| Table 8.3 purchased power, $bn | 49 | 53 | 50 | 50 | 62 | 80 | 68 | 64 |
| 861 / Table 8.3 | 2.90 | 2.98 | 3.07 | 2.77 | 3.23 | 2.70 | 2.47 | 2.32 |
| `p_gen` index (2017=100) | 100 | 106 | 103 | 93 | 138 | 153 | 128 | 122 |
| Table 8.3 PP index (2017=100) | 100 | 108 | 103 | 103 | 126 | 163 | 138 | 130 |

- Mean level ratio 861 revenue / Table 8.3 PP ≈ **2.8×** (expected: 861 is industry-wide including marketers and public power; 8.3 is major IOUs only).
- Correlation of year-on-year % changes: **`p_gen` vs Table 8.3 PP ≈ 0.76**; 861 revenue vs Table 8.3 PP ≈ **0.79**; `p_gen` vs Table 8.3 fuel+PP ≈ **0.79**.
- Both series rise through the 2021–22 wholesale spike and ease afterward. Table 8.3 PP peaks harder in 2022 (index 163 vs `p_gen` 153) and stays a bit higher by 2024 (130 vs 122).

## Verdict

The 861-based `p_gen` **tracks the same wholesale cycle** as FERC/Table 8.3 purchased-power expense. Levels differ by coverage, so Table 8.3 is a **movement check**, not a replacement price. Nothing here argues for switching `p_gen` off 861.
