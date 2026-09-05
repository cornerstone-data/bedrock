# The price carry and theta

Reference for the second of Step 3's three moves: how a 2017 input mix is
carried to a later year. The code is
[`nowcast_intermediate.py`](../../transform/iot/nowcast_intermediate.py). The
measurements and the decisions behind the shipped values are in
[`intermediate_estimation_plan.md`](intermediate_estimation_plan.md) §Inflation,
§Margins, §θ goes negative and §S2.

## What theta is

`theta` is a scalar exponent, one number per `base -> year` span. It is not a
price ratio, not a valuation bridge and not specific to any commodity or
industry. The same value applies to all 402 commodity rows and all 402 industry
columns.

`carry_shares` is the whole of it:

```
share[c,j](t)  =  share[c,j](2017) x deflator[c](t) ** theta
                  then each column j renormalised to sum to 1
```

`deflator[c]` is the commodity-specific term. `theta` sets how much of a
commodity's own price movement passes into its nominal cost share.

## What the deflator is

`commodity_deflator(year, base)` is the product of two legs.

| leg | value | source |
|---|---|---|
| price | `p_c(year) / p_c(base)` | `derive_industry_price_index`, from BEA `UGO304-A` gross-output price indexes, topped up from the summary quarterly series for the latest years |
| margin | `(1 + mu_c(year)) / (1 + mu_c(base))`, where `mu_c = T014 / (T013 + T015)` | Supply table valuation columns |

The price leg is a producer-price index on an industry axis, read
commodity-for-commodity. The margin leg converts it to a purchaser-price ratio,
which is what a cell of this block needs: the intermediate block of the Use SUT
is at purchaser value.

Passing `margins=False` returns the price leg alone. That is #497 as written and
the wrong deflator for a purchaser-valued cell; it is kept so the two can be
scored against each other.

## The valuation chain

BEA's Supply table gives the chain as `T016 = T013 + T014 + T015`. `T014` is the
margins alone, not a running subtotal.

```
BAS  = T013                     basic value
PRO  = T013 + T015              producer value  (basic + net taxes on products)
PUR  = PRO + T014 = T016        purchaser value (producer + trade and transport margins)
```

So `1 + mu_c` is the purchaser-to-producer wedge, and the margin leg is the
movement in that wedge between the two years.

The denominator of `mu_c` is producer value, not basic value. BEA gross output
is valued at producers' prices, so the price leg already carries the
product-tax layer and only the margin layer is missing. Taking the rate over
`T013` alone overstates it by the tax wedge: median 3.3%, and largest on the
commodities that carry the most intermediate dollars here (`315AL` apparel 1.372
against 1.793, `324` petroleum 0.246 against 0.289).

There is no producer-to-basic ratio anywhere in Step 3.

## The elasticity reading

Under CES the cost share of an input is `s_c ~ alpha_c * p_c ** (1 - sigma)`, so
carrying a nominal share on `p ** theta` is the same statement as

```
theta = 1 - sigma
```

where `sigma` is the elasticity of substitution between inputs.

| theta | sigma | what it assumes |
|---:|---:|---|
| 1 | 0 | Leontief. The physical input mix is fixed, so a nominal share moves one for one with its own price. This is #497 as written, kept as `THETA_497` |
| 0.75 | 0.25 | `THETA_OFF_SURGE`, the value fitted on spans that do not cross 2021-22 |
| 0 | 1 | Cobb-Douglas. Nominal shares are the stable object, which is a frozen `A` matrix. `THETA_ACROSS_SURGE` |
| -0.5 | 1.5 | what 2024 fits: the quantity response outruns the price effect, so the nominal share moves against the price |

`theta` is fitted by grid search over `-1.0` to `1.5` in steps of `0.25`
(`THETA_GRID` in
[`intermediate_structure_drift.py`](intermediate_structure_drift.py)). The grid
runs negative because 2023 and 2024 fit there; an earlier grid started at 0.0
and those two years pinned to the floor.

## What is specific to what

| object | indexed by | varies with |
|---|---|---|
| `theta` | nothing | the span only |
| price leg | commodity | commodity |
| margin leg | commodity | commodity |
| the carry as applied | commodity x industry | commodity only |

No industry-specific deflator and no industry-specific `theta` exist. Every
column receives the same row-wise factor and is then renormalised.

## Values in use

`default_theta(year, base=2017)` returns `THETA_OFF_SURGE` unless the span
crosses the 2021-22 price surge, in which case it returns
`THETA_ACROSS_SURGE`. A span crosses if `base <= 2021 and year >= 2022`. For the
build's seven target years that is 0.75 for 2018-2021 and 0.0 for 2022-2024.

The two values are fitted on 78 non-nested summary spans with a base of 2012 or
later, not on the seven the build runs. Fitted 0.755 and 0.141, rounded. The
choice of rule and the reasons for rounding 0.141 up to zero are in
[`intermediate_estimation_plan.md`](intermediate_estimation_plan.md) §S2.

## Approximations and limits

The price index is an industry index used on commodity rows. At BEA detail the
two code lists are the same 398 codes plus four each way, and the detail Make
table is near-diagonal, so an industry code's deflator is taken as that
commodity's deflator. The four commodity rows with no industry counterpart are
`UNPRICED_COMMODITIES`: `S00300` noncomparable imports, `S00401` scrap, `S00402`
used and secondhand goods, `S00900` rest of the world adjustment. Their factor
is held at 1.0. BEA publishes no deflator for any of them.

The margin rate is detail-observed only at benchmark years, because BEA
publishes the detail Supply table for 2007, 2012 and 2017 only. For other years
the level is taken from 2017 at detail and the movement from the summary parent:
`mu_c(t) = mu_c(2017) * mu_P(t) / mu_P(2017)`. Scored against the observed 2012
detail factor and weighted by 2017 intermediate dollars, that rule is 0.756pp
off; taking the parent's factor down unchanged is 1.010pp; applying no factor is
1.818pp.

The margin leg applies only to margin-receiving commodities. For a trade or
transport commodity `T014` is large and negative, because the margin is
allocated away onto the goods it carries, so `mu` runs to -0.94 for `42` and
-0.99 for `486` and `1 + mu` is a near-zero denominator. The factor is held at
exactly 1.0 unless `mu_c(base) > 0` and `mu_c(year) > -1`, which also covers the
rows where the rate is undefined because producer value is zero. Those rows
carry almost no intermediate dollars in a purchaser-priced Use table.

The margin rate reads one Supply vintage for every year, rather than the
year-pinned vintage `_load_usa_summary_sut` selects, because the leg is a ratio
of two years. The two vintages differ by a median 0.50pp on 2020 rates and
1.20pp on 2022 rates, and by 0.00pp on 2017.

`MARGIN_YEARS` is 1997-2024, the years the Supply vintage publishes. It is a
separate constraint from `INTERMEDIATE_YEARS`, which is bounded by gross output.
A year outside `MARGIN_YEARS` raises rather than returning a factor of 1.0.

At `theta = 0` the deflator is raised to the zero power, so both legs are inert.
The margin leg moves 0.21% of the 2019 block, 0.54% of the 2021 block and 0.000%
of the 2024 block.

## The carry does not set the level

Each column is renormalised before the column control is applied, so the block
total is set entirely by `T005 = GO_producer - VAPRO`. The built block's total is
the same at `theta = 1` and at the fitted `theta` to floating-point noise, under
a cent on a $21 trillion block. Level tables are a check on the control, not
evidence about the carry.
