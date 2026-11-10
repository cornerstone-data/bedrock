# Electric power's value added on a consolidated basis

Issue #1012. Parent #990, grandparent #896. Reproduce with `consolidated_basis_1012.py`.

#1012 asked whether BEA's value added for electric power, 66.4% of its output in 2017,
survives a comparison with FERC Form 1. The first comparison found a gap. It was on the
wrong basis. This file records the comparison on the right basis, what it shows, and what
it cannot show with the data in the repo.

## 1. Short answer

On a consolidated basis there is no gap that needs a defect to explain it. BEA's
intermediate purchases are $4-9bn above what FERC implies, out of $79bn. The value
added question stays open only because the repo has no financial statements for the
other 40% of the industry.

The fuel rows are a separate finding, and it does not depend on consolidation. They sit
below EIA's own fuel costs (section 5).

## 2. Why the first comparison was wrong

BEA's `221100` is every electric entity put together, with sales between them cancelled.
Its output is what reaches outside buyers. FERC Form 1 covers one group of those
entities, the major investor-owned utilities, and its revenue and expenses include sales
and purchases between entities: sales for resale, transmission service, purchased power.

The first comparison scaled BEA's totals by 67.6%, FERC's revenue over BEA's output. That
revenue is not output on BEA's basis. The weight should be the investor-owned share of
what reaches outside buyers.

## 3. The weight

EIA-861 reports revenue by utility, and by ownership type. Retail sales plus delivery
revenue is what reaches outside buyers. 2017, $bn:

| | |
|---|---:|
| All utilities, retail sales plus delivery | 391.9 |
| BEA gross output for `221100` | 389.4 |
| Investor-owned, retail sales plus delivery | 235.5 |
| **Investor-owned share** | **60.1%** |
| Investor-owned total revenue in EIA-861 | 269.5 |
| The same, in FERC Form 1 | 263.3 |

All utilities land within 0.6% of BEA's output, which confirms that is the right basis.
The investor-owned revenue agrees with FERC within 2.4%.

## 4. The comparison at that weight

**Intermediate purchases.** FERC's are its revenue less every value added line, less
purchased power, which is bought from other entities in the industry and so cancels.
FERC has no labor line, so investor-owned compensation is unobserved. It is taken at the
same 60.1% of BEA's, and, for comparison, at the 67.6% the issue used.

| 2017, $bn | At 60.1% | At 67.6% |
|---|---:|---:|
| BEA intermediate purchases, $131.0bn at the weight | 78.7 | 78.7 |
| FERC, net of purchased power | 74.5 | 70.0 |
| Gap, FERC less BEA | **-4.2** | **-8.7** |

In the same terms the gap was +30.5 as first filed, and -18.5 after netting purchased
power but keeping the 67.6% weight. Neither of those is on a consistent basis. The other
39.9% of output would carry the remaining $52-61bn of intermediate. That is plausible for independent producers
buying gas and coal, but nothing in the repo tests it.

**Non-labor value added.** Taxes plus operating surplus (operating income plus
depreciation). FERC's are exact lines, so this does not depend on the compensation
assumption.

| 2017, $bn | |
|---|---:|
| FERC, investor-owned | 103.3 |
| BEA, value added less compensation | 197.9 |
| Investor-owned share | 52.2% |

Investor-owned utilities make 60% of the output and 52% of the non-labor value added, so
the other groups would need a higher ratio. Independent producers, federal hydro and
public power plausibly do: little fuel, regulated or cost-based pricing, a large capital
base. That cannot be tested here. The repo has no expenses for public power, co-ops,
federal or independent producers.

## 5. The fuel rows

BEA's total intermediate of $131.0bn looks about right. Its mix does not. The model's
electric column, the three electric columns merged, buys these fuels. Delivered cost is
EIA-923's, for every power plant:

| 2017, $bn | Model | EIA-923 | Ratio |
|---|---:|---:|---:|
| Coal (`212100`) | 7.8 | 22.6 | 0.34 |
| Gas (`211000`) | 20.6 | 30.0 | 0.69 |
| Refined petroleum (`324110`) | 10.7 | 0.57 | 18.9 |

Coal runs 0.21-0.38 of EIA's across 2017-2024 and gas 0.56-0.87. Part of that is expected:
EIA's cost is delivered, so it includes rail freight and pipeline charges, which BEA books
on transport rows. A fair test adds those rows. That test is not done.

Oil is the other way, 14 to 20 times EIA's oil bill, and freight does not explain it.
That is #1226.

**Movement.** Relative to 2017, coal and gas follow EIA-923 through 2020 and in 2023.
Coal falls further in 2021-22 (0.43 and 0.48 against EIA's 0.63 and 0.77). Gas runs under
EIA in 2019 and 2021-22 and over it in 2024 (1.58 against 1.25). Oil follows BEA's price
on purpose (#922). The cause of the coal and gas gaps is not diagnosed.

**CO2.** The model's CO2 on the electric column is 0.99 to 1.10 times EIA-923 heat times
EPA factors, with a mean near 1.05. Receipts are not combustion, and only coal, gas and
oil are counted, so a small gap is expected. This is the consistency check #1002 left
open, and it agrees.

## 6. The split of compensation

The generation and delivery split gave delivery the same share of every value added line.
QCEW private payroll says delivery's share of compensation is 57-60%, not 66-74%. That is
fixed in #1225. It is a different question from this one, but it is the same data, so it
is noted here.

## 7. What is still open

- **Non-IOU financials.** Public power, co-ops, federal and independent producers are
  40% of output. Their value added is the unexplained part. EIA Form 412 covers public
  power, and is not in the repo.
- **Freight on the fuel rows.** Add the transport and margin rows to the fuel test.
- **Coal and gas movement in 2021-22.** The balance to gross output less value added is
  the first thing to look at.
- **The 2017 oil cell.** The model has $10.7bn. The seed code quotes $17.4bn for BEA's
  cell. Not reconciled (#1226).
- Do not treat `VAPRO` as suspect on this evidence. It is BEA's published series, pinned
  by `T18`.
