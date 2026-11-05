# Electricity accounting residual

A second pathway for electric power in the nowcast tables. The first, BEA-based pathway stays available and unchanged. This note records the design and how far it has been built. Background is in `About_electricity_modeling_record.md`.

## The two pathways

| | BEA-based pathway (current) | residual pathway |
|---|---|---|
| electric gross output | BEA's lines, with the two-leg rebase on `221100` | EIA retail revenue, where that is lower |
| price | EIA retail price for the carry, BEA's index for real output | EIA retail price for both |
| extra sector | none | `S00700`, one commodity and one industry |
| inputs of electric power | as balanced | unchanged from the BEA-based pathway |
| other industries' inputs | as balanced | unchanged, except electricity and the residual |

The residual pathway is a layer applied after the BEA-based balance. It does not change the balance, so with the layer off the tables are exactly the BEA-based tables.

`electric_output_on_retail_revenue` (#1198) is a third option that lowers output inside the balance, with no residual. In 2022 it takes $36bn out of electric power's inputs, and refineries buy $4.5bn more oil and gas.

## Why the residual is sold to buyers

An earlier version sold the residual to electric power. That does not work. Electric power's column adds up the same way whether the difference is residual value added or electric value added, so its real inputs would stay squeezed.

The difference has to leave the electric column as lower output and lower value added, and the buyers' lost electricity has to be replaced. The residual is what replaces it:

| | change |
|---|---|
| electric industries | output and value added each fall by `delta`; inputs unchanged |
| intermediate buyers | electricity falls by `delta` in total, in proportion to each buyer's purchases; the residual rises by the same amount; column totals unchanged |
| households, exports | unchanged |
| residual industry | output `delta`, value added `delta`, no inputs, no emissions |
| everything else | unchanged |

`delta` for each electric industry is its output on the BEA-based pathway less its retail-based output, and is never negative.

Holding households out follows the evidence: household electricity follows EIA residential revenue, and our bills above EIA revenue are on intermediate buyers (`About_electricity_modeling_record.md`, section 6).

## Result on `v1.0.0_5514c3b`

`python -m bedrock.transform.iot.electricity_accounting_residual --check`. Every identity holds in all seven years.

| | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 |
|---|---:|---:|---:|---:|---:|---:|---:|
| residual, $bn | 4.7 | 2.8 | 2.0 | 18.7 | 36.2 | 10.7 | 2.1 |
| share of intermediate electricity | 1.7% | 1.0% | 0.8% | 6.3% | 10.3% | 3.2% | 0.6% |
| real output on EIA's price, 2017 = 1 | 1.036 | 1.020 | 0.993 | 1.017 | 1.051 | 1.040 | 1.063 |
| EIA kWh, 2017 = 1 | 1.036 | 1.024 | 0.998 | 1.022 | 1.055 | 1.041 | 1.068 |
| electric surplus, before, $bn | 173.4 | 175.6 | 180.1 | 215.7 | 247.7 | 270.7 | 261.8 |
| electric surplus, after, $bn | 168.7 | 172.8 | 178.1 | 197.0 | 211.4 | 260.0 | 259.8 |

Real output stays within 0.5% of kWh. It falls short where `delta` is held at zero for an industry whose retail-based output would be higher.

The federal column's surplus falls to about $1.2bn in 2021 and $1.3bn in 2022, from $7.8bn and $8.2bn. It stays positive, but this is the most exposed column.

## Effect on emissions

Total emissions don't change. They shift between buyers, because electric output falls while households keep their electricity. Relative to the BEA-based pathway, on a first-order calculation:

| | 2021 | 2022 | 2023 |
|---|---:|---:|---:|
| intermediate buyers, emissions from electricity | 0.974 | 0.956 | 0.986 |
| households and exports, emissions from electricity | 1.039 | 1.066 | 1.019 |

So the 2022 electricity share of an intermediate buyer's footprint falls 4.4%, and households' rises 6.6%.

## Open decisions

- **Reference output.** See "Controlling non-fuel inputs" below. A larger residual is acceptable if it controls non-fuel inputs (Wes, 2026-11-05).
- **Allocation across buyers.** It is proportional to electricity purchases. Commercial buyers carry most of the excess over EIA revenue, so a class-based allocation may fit better.
- **The 2017 level.** The benchmark's roughly $68bn of bills above retail revenue stays in electricity. Moving it would change the observed benchmark.

## Controlling non-fuel inputs

The layer above leaves electric power's inputs where the BEA-based pathway has them. Measuring `delta` from BEA's published output would make the residual larger, but it would not control non-fuel inputs: it inherits BEA's path. To control them, the residual is sized from a target instead:

```
electric value added = retail output - fuel - non-fuel target
residual             = BEA value added - electric value added
```

All electric industries together, $bn. Non-fuel inputs are intermediate inputs less EIA-923 fuel.

| non-fuel inputs | 2017 | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| BEA's output and value added | 121.0 | 147.6 | 148.6 | 122.9 | 143.8 | 165.2 | 121.7 | 131.2 |
| current two-leg output | 121.0 | 133.6 | 126.1 | 116.6 | 94.1 | 118.4 | 114.5 | 140.8 |
| 2017 cells x commodity price x EIA kWh | 121.0 | 127.9 | 128.5 | 125.9 | 135.6 | 150.5 | 151.6 | 160.6 |
| 2017 x investor-owned non-fuel operating and maintenance cost | 121.0 | 145.3 | 149.2 | 135.3 | 135.8 | 136.5 | 158.7 | 163.4 |

| residual | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 |
|---|---:|---:|---:|---:|---:|---:|---:|
| from BEA's output | 18.7 | 25.1 | 7.6 | 68.4 | 83.0 | 17.9 | -11.3 |
| from the price x kWh target | -0.9 | 5.1 | 10.6 | 60.2 | 68.3 | 47.7 | 18.1 |
| from the operating and maintenance target | 16.4 | 25.8 | 20.0 | 60.4 | 54.3 | 54.9 | 20.9 |

The price x kWh target is the proposed one. It holds the real input mix fixed per kWh, the same theta = 1 prior as the Step 3 price carry, and it doesn't depend on investor-owned utilities standing in for the whole industry. Their series jumps 20% in 2018, when administrative costs nearly double and taxes fall. A negative residual is set to zero.

This version is not a layer on the balanced tables. Electric power buys more than it does on the BEA-based pathway, and commodity rows are fixed, so:

- the electric industries' intermediate target moves inside the balance, by about $42bn in 2021, $32bn in 2022, $37bn in 2023 and $20bn in 2024 above the current build;
- buyers' other inputs give up the same amount, because the residual they buy exceeds the electricity they lose;
- gas into electric power has more room against EIA's bill. How much has to be measured.

The BEA-based pathway stays unchanged behind the flag either way.

## How far it is built

| part | status |
|---|---|
| `electricity_accounting_residual.py`: amounts, table split, identity checks, report | done, unit tested |
| config flag selecting the pathway | not yet. A flag that changed only the price would mislead |
| Step 6 and Step 7 tables with the pair | not yet |
| collapsing the pair out of `A` and `B`, so the released schema stays at 405 | not yet tested. The residual has no inputs and no emissions, so it should not change a factor beyond the shift above |
| empty pair in 2017 | handled by the split; downstream not yet |
| evaluation on the IV score and tracker sectors | not yet |
