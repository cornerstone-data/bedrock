# Electricity accounting residual

A second pathway for electric power in the nowcast tables. The BEA-based pathway stays available and unchanged behind the flag. This note records the design and how far it has been built. Background is in `About_electricity_modeling_record.md`.

## The pathways

| | BEA-based (current) | retail output only (#1198) | residual (this pathway) |
|---|---|---|---|
| flags on | none | `electric_output_on_retail_revenue` | that and `electricity_accounting_residual` |
| electric gross output | BEA's lines, two-leg rebase on `221100` | EIA retail revenue | EIA retail revenue |
| price | EIA retail for the carry, BEA's index for real output | EIA retail for both | EIA retail for both |
| electric value added | BEA's | BEA's | BEA's less the residual |
| electric non-fuel inputs | whatever output less value added leaves | squeezed further | follow a target |
| extra sector | none | none | none (the residual stays in buyers' `V00300`) |

## Definition

For all electric industries (`221100`, `S00101`, `S00202`) together:

```
non-fuel target      = 2017 non-fuel inputs x input prices x EIA kWh
electric value added = retail output - EIA-923 fuel - non-fuel target
residual             = BEA value added - electric value added, never negative
```

- **Input prices** are each commodity's own price factor, weighted by the electric columns' 2017 cells, leaving out the coal, gas and refined petroleum rows. Electricity bought by the industry takes EIA's retail price. The target holds the real input mix per kWh fixed, the same theta = 1 prior as the Step 3 price carry.
- **Across electric industries** the residual is split by value added.
- **Across buyers**, commercial and industrial classes split it by that year's bills above EIA revenue, and buyers within a class by that year's bills. Households and exports don't buy it, because household electricity follows EIA residential revenue.
- **The residual** has no inputs and no emissions. The fall in electric value added equals the sum of the rises in buyers' `V00300`, so total value added still equals final demand.

The industrial class is farms, mining, construction and manufacturing. The commercial class is every other intermediate buyer, so it includes government and transport, which is approximate.

An earlier design sold the residual to electric power. That doesn't work: the electric column adds up the same whether the difference is residual or electric value added, so real inputs stay squeezed.

## How it reaches the balance

The residual has no row or column in any table. The balance sees a transfer of value added that sums to zero (`value_added_transfers`): electric value added falls, and each buyer's rises by its share. Every target built on gross output less value added follows, so no balancing code changes.

Before the balance, the transfer goes on each industry's gross operating surplus (`V00300`), in `nowcast._reconcile_to_published_vapro`. The balance can then move that row, which is unlocked, so the released surplus includes the transfer but does not isolate it. This holds in 2017-2024 (2017 with `electric_output_on_eia_level` on).

### Why it stays in surplus (#1271)

Buyers' output is fixed, so whatever they pay for the residual comes out of their real inputs. Booking it as a residual commodity, with a row in each buyer's column, gives the same total factors (`N`), because the residual has no inputs and no emissions. It would not give the same `A`: buyers' columns would gain a residual coefficient, so `A` column sums, direct emissions per dollar of real inputs, and commodity-level or spend-based outputs would differ. Anyone adding a residual commodity later for reporting should check those.

A separate industry sold to final demand would leave buyers' inputs alone, but final demand is controlled to NIPA, and households and exports follow EIA revenue. So the residual stays a transfer in surplus, and there is no residual sector to overlap with the delivery margin in `42ELEC`. The unused code that added a residual row and column to the balanced tables was removed in #1273.

Balanced bills can't set the class split, because they come out of the balance this feeds. It uses Step 3's electricity cells, on column totals before the transfer. Commercial's share of the excess:

| | 2017 | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Step 3 bills (used) | 83% | 78% | 84% | 90% | 80% | 72% | 70% | 75% |
| balanced bills, `v1.0.0_5514c3b` | 83% | 80% | 83% | 83% | 84% | 75% | 65% | 68% |

Step 3's commercial bills run $23-29bn above balanced bills in 2023-24, before the interior fit, so the split leans commercial in those years.

## Choice of target

All electric industries, $bn. Non-fuel inputs are intermediate inputs less EIA-923 fuel.

| non-fuel inputs | 2017 | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| BEA's output and value added | 121.0 | 147.6 | 148.6 | 122.9 | 143.8 | 165.2 | 121.7 | 131.2 |
| current two-leg output | 121.0 | 133.6 | 126.1 | 116.6 | 94.1 | 118.4 | 114.5 | 140.8 |
| 2017 cells x input price x kWh (chosen) | 121.0 | 127.9 | 128.5 | 125.9 | 135.6 | 150.5 | 151.6 | 160.6 |
| 2017 x investor-owned non-fuel operating and maintenance cost | 121.0 | 145.3 | 149.2 | 135.3 | 135.8 | 136.5 | 158.7 | 163.4 |

The investor-owned series covers part of the industry and jumps 20% in 2018, when administrative costs nearly double and taxes fall, so it is a check rather than the target. A larger residual is acceptable where it controls non-fuel inputs (Wes, 2026-11-05).

## Results, balanced live

Against `v1.0.0_5514c3b`, $bn, with `carry_power_petroleum_on_price` and `reprice_oil_gas_extraction_seed` off. Every identity holds.

| | 2021 | 2022 | 2023 | 2024 |
|---|---:|---:|---:|---:|
| residual | 60.2 | 68.3 | 47.7 | 18.1 |
| non-fuel inputs, current | 94.1 | 118.8 | 114.9 | 141.2 |
| non-fuel inputs, this pathway | 135.6 | 150.9 | 151.9 | 160.9 |
| gas into electric, current | 51.1 | 69.4 | 39.8 | 43.6 |
| gas into electric, this pathway | 56.3 | 72.7 | 42.2 | 44.5 |
| EIA-923 gas bill | 58.8 | 85.7 | 45.1 | 37.6 |
| interior cells below zero, current and this pathway | 7 | 7 | 7 | 7 |
| gross change outside the electric columns | 66.5 | 58.5 | 61.3 | 28.2 |
| as a share of the interior | 0.37% | 0.29% | 0.30% | 0.13% |
| electric surplus after | 155.6 | 179.7 | 223.2 | 243.8 |

Buyers give up other inputs, because the residual they buy exceeds the electricity they lose. That is what restores electric power's inputs. Commodity rows stay fixed.

Other real estate (`531ORE`) is the largest buyer, at $7.2bn in 2022.

## Regrade with the time series procedures

The IV score and sector deep dive triage, spend weights, on three full builds for 2017-2024. All use GHG FBS `v1.0.0_79273ed`, so `E` doesn't respond to the new tables, and none includes #1201.

- **Current:** `v1.0.0_5514c3b`.
- **Flags off:** the same code with regenerated trade inputs (#1179, #1180), `v1.0.0_b17908ftrade`.
- **All flags:** `electric_output_on_retail_revenue`, `electricity_accounting_residual`, `carry_power_petroleum_on_price`, `reprice_oil_gas_extraction_seed` and `hold_electric_gas_on_eia`, on the same trade inputs, `v1.0.0_b17908f`.

Steps 6 and 7 pass their checks on all eight years with all flags on. They read the 402-sector tables, in which the residual sits in buyers' surplus, so they need no new dimension.

| tracker sector | IV, current | IV, flags off | IV, all flags | gross `N_d` movement, flags off | gross `N_d` movement, all flags | `B` share, flags off | `B` share, all flags |
|---|---:|---:|---:|---:|---:|---:|---:|
| `221100` electric power | 3.23 | 3.23 | 1.66 | 56.1 | 35.5 | 93% | 87% |
| `481000` air transportation | 0.40 | 0.41 | 0.41 | 55.0 | 54.3 | 82% | 83% |
| `211000` oil and gas extraction | 0.35 | 0.34 | 0.34 | 42.5 | 41.5 | 54% | 52% |
| `484000` truck transportation | 0.31 | 0.31 | 0.31 | 22.3 | 21.7 | 65% | 61% |
| `324110` petroleum refineries | 0.26 | 0.26 | 0.26 | 35.3 | 35.2 | 55% | 53% |
| `325110` petrochemicals | 0.25 | 0.25 | 0.24 | 84.0 | 84.0 | 60% | 59% |
| `531ORE` other real estate | 0.23 | 0.23 | 0.23 | 58.2 | 58.4 | 48% | 43% |
| `325190` other basic organic chemicals | 0.23 | 0.23 | 0.23 | 31.6 | 31.5 | 62% | 62% |
| `325211` plastics and resin | 0.21 | 0.21 | 0.21 | 59.3 | 58.9 | 55% | 54% |

Across all commodities, the median gross `N_d` movement goes from 47.8 to 47.3 between flags off and all flags, and its `B` part from 25.8 to 19.6. Movement falls for 60% of commodities. It falls most for electric power (-20.5), gas distribution (-9.9) and `S00600` (-7.0), and rises by at most 4.6.

Electric power's `N_d`, kg CO2e per constant 2017 dollar:

| | 2017 | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| flags off | 4.187 | 4.223 | 3.992 | 3.552 | 4.079 | 3.841 | 3.304 | 3.168 |
| all flags | 4.187 | 4.090 | 3.800 | 3.511 | 3.696 | 3.551 | 3.296 | 3.226 |

The 2021 rise falls from 14.8% to 5.3%. The 2017 to 2024 decline stays near 23-24%.

Trade alone leaves these grades almost unchanged. It does move the 2024 balanced tables, by $83bn in gross terms, so the 2024 pin needs a bump either way.

## Not yet measured

- The GHG FBS on these tables, including #1201. Emissions allocated through the Use table respond to it, so these factors are not final.
- The emission shift between buyers on this design. In `A`, electricity bought by trucks falls about 11% in 2022, and other real estate's `N_d` falls about 2%.
- Whether oil and gas into refineries, which falls $38bn after redefinitions in 2022, is right. Refineries' `N_d` barely moves, because their column total is fixed and other inputs replace it.

## How far it is built

| part | status |
|---|---|
| residual amounts, class split | done, unit tested |
| config flag and value-added transfer in Step 5 | done, off by default, on in the v1.0 configs |
| live balance 2017-2024 | done, published in `v1.0.0_e7715da` |
| Step 6 and Step 7 | run on the 402-sector tables, checks pass in all eight years. No residual row or column; the residual is in buyers' `V00300` |
| reporting tables with a residual row and column | not planned (#1271); the code was removed in #1273 |
| a residual sector in the taxonomy | not planned (#1271) |
| released schema stays at 405 | yes: `A` and `B` read the 402-sector tables, and the residual has no inputs or emissions |
| evaluation on the IV score and tracker sectors | done on `v1.0.0_79273ed`; repeat on the rebuilt FBS |
