# What the v0.5 Supply does to the commodity emission factors

Code: [`supply_mix_diagnostic.py`](supply_mix_diagnostic.py). Run of
2026-09-28: v0.4's after-redefinition Make (MUT `v0.3.0_4276083`) against
v0.5's (`v0.3.0_92b7a8a`), 2017-2024, both under GHG FBS `v0.3.0_99655e9`.
Emissions are held fixed, so every difference is the tables.

## The measure

A commodity's direct factor is the market-share-weighted mean of its makers'
intensities, `B_commodity = (E / x) @ Vnorm`. Its move between the two builds
splits exactly into two parts, which `--check` asserts:

- **mix**: the market shares alone, `b_v0.4 @ (Vnorm_v0.5 − Vnorm_v0.4)`,
  every industry's intensity held at v0.4's;
- **intensity**: the industries' own output moving under a fixed `E`,
  `(b_v0.5 − b_v0.4) @ Vnorm_v0.5`.

Product mix (each industry's split of its output across commodities) and
market shares (each commodity's split across the industries making it) are
also reported, as half the L1 distance between the builds in percentage points.

Direct factors only. Market shares also enter `A`, and what that does to `N`
is the [L flux ranking](About_the_L_flux_priority.md)'s job.

## Market shares barely move the factors

| year | commodities whose market shares moved >0.1pp | output-weighted mean \|mix effect\| | commodities over 5% |
|---|---:|---:|---:|
| 2017 | 0 | 0.00% | 0 |
| 2018 | 34 | 0.03% | 3 |
| 2019 | 57 | 0.06% | 5 |
| 2020 | 113 | 0.15% | 5 |
| 2021 | 85 | 0.13% | 5 |
| 2022 | 183 | 0.38% | 10 |
| 2023 | 181 | 0.13% | 13 |
| 2024 | 198 | 0.23% | 20 |

2017 is the benchmark year and matches exactly, as it should. The commodities
over 5% are mostly small electronics (computers, environmental controls,
irradiation apparatus), where one maker with a very different intensity gains
or loses a fraction of a point. Petrochemicals `325110` is the largest
manufacturing mover in 2024: refineries take 4.0pp of its market, lowering its
factor 4.8%.

One to look at: in 2024 electric utilities (`221100`) take 1.7pp of natural
gas distribution's market, raising its factor 8.7%. It looks like a side effect
of rebasing `221100` output on EIA, which scales the utilities' secondary
products along with electricity.

## Industry output moves them several times more

v0.4 held each manufacturing industry's 2022 census level into 2023-24 on
BEA's annual movement. v0.5 chains those two years on census receipts from
AIES, the Annual Integrated Economic Survey (#1014). With emissions fixed,
lower output means a higher factor:

| industry | v0.5 output 2022 / 2023 / 2024 | v0.4 2024 | factor effect 2024 | BLS price index 2022→24 |
|---|---|---:|---:|---:|
| `331110` Iron and steel mills | $127bn / $110bn / $99bn | $125bn | +26% | −24.7% |
| `322130` Paperboard mills | $41bn / $37bn / $34bn | $41bn | +21% | −3.1% |
| `327310` Cement | $11.5bn / $10.2bn / $10.4bn | $12.1bn | +16% | +18.9% |
| `325110` Petrochemicals | $76bn / $76bn / $68bn | $77bn | +12% | −20.4% |
| `324110` Petroleum refineries | $806bn / $694bn / $646bn | $582bn | −6% | |

The price column is the BLS producer price index for the industry
(`PCU331110331110` and so on), annual average.

- **Steel and petrochemicals are plausible.** Deflated by their price index,
  v0.5 has real output up 3% (steel) and 11% (petrochemicals) over 2022-24.
  v0.4, which carried 2022 forward, implied 30% and 27%.
- **Cement does not.** Its price rose 19% and USGS reports production value
  (tonnes × mill unit value) rising from $12.7bn in 2022 to $13.6bn in 2024.
  v0.5 has it falling 10%.
- **Paperboard is unconfirmed.** Nominal −18% against prices −3% means real
  output −15%. Physical containerboard production has not been checked.

## Cement: receipts moved to ready-mix concrete

The model follows the census faithfully. AIES itself reports cement
receipts falling, and ready-mix concrete rising by more:

| NAICS | 2022 (Economic Census) | 2023 (AIES) | 2024 (AIES) | 2022→24 |
|---|---:|---:|---:|---:|
| `327310` Cement | $11.7bn | $10.4bn | $10.6bn | −9.6% |
| `327320` Ready-mix concrete | $43.0bn | $46.5bn | $51.3bn | +19.5% |
| together | $54.7bn | $56.9bn | $61.9bn | +13.2% |

Together the two grow 13%, which fits the cement price index and USGS. Apart,
cement falls while its price rises and its tonnage slips only 7% (USGS
production, 91.2 to 85.0 million tonnes). The likely reading is that
integrated cement-and-concrete companies report more of their receipts under
ready-mix in AIES than they did in the Economic Census. That reading is not
confirmed. Cement carries one of the highest direct factors in the model
(4.2 kg CO2e per dollar in 2024), so a 16% overstatement matters.

Sources: BLS Producer Price Index API; USGS Mineral Commodity Summaries 2026,
Cement.

## Reproduce

```bash
uv run python -m bedrock.analysis.nowcasting.supply_mix_diagnostic --check
uv run python -m bedrock.analysis.nowcasting.supply_mix_diagnostic \
    --base-mut v0.3.0_4276083 --new-mut v0.3.0_92b7a8a --years 2023 2024
```

Tables go to `output/supply_mix/`.
