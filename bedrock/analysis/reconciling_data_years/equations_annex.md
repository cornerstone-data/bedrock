# Additional equations used in Reconciling Data Years


## Solution 3a - USEEIO like adjustment


$$ x = x_{2023} \frac{\Pi_{$2017}}{\Pi_{$2023}} $$

$$ B_i = E \hat{x}^{-1} $$ 

$$ B = B_i V\hat{q}^{-1} $$



## Solution 3b - A deflation

$$ A_{2023} = \hat{\rho}^{-1} A_{2017} \hat{\rho} $$ 

$$ \rho = \frac{\Pi_{2017}}{\Pi_{2023}}  V\hat{q}^{-1}$$
- Deflates d and n to `LATEST_TARGET_YEAR` (2024) USD for cross-year comparison.

### Output statistics
- **`n_stats.csv`** — mean and median of `n` across sectors, per model per year.
- **`q_ec_correlation.csv`** — YoY Pearson r (Δq vs Δe_c) and `pct_same_sign` (fraction of sectors where q and e_c move in the same direction) per model per year-transition.
- **`q_ec_correlation_summary.csv`** — wide-format pivot of the above with per-model averages across all transitions.
- **`efs.csv`** — long-format record of d, n, q, e_c per model × year × sector.

### Figures
- **`trends_d_q_ec.png`** — time-series of e_c, q, d, and n indexed to first year = 100, one row per model (including model 1), y-axis shared within each column for direct cross-model comparison.
- **`n_yoy_distribution.png`** — signed YoY violin plot of `n` across models.



## Solution 4

$$ A_{s2023,2017$} = \hat{\rho}^{-1} A_{s2023} \hat{\rho} $$  

$$ \alpha_{s2023/s2017} = A_{s2023,s2017$} \circ A_{s2017,s2017$}^{-1} $$

$$ \alpha_{2023/2017} = O_{ds_c} \alpha__{s2023/s2017} O_{ds_c}' $$

$$ A_{2023,2017$} = \alpha \circ A_{2017} $$