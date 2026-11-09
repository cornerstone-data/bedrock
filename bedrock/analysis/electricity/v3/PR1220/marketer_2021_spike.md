# Watch item 3: 2021 retail power marketer revenue spike

**Claim in #1220 / #1215:** retail marketers’ sales-for-resale revenue is ~$67.5bn in 2021 against ~$30–40bn in nearby years, and that lifts annual `p_gen`.

**Data:** EIA-861 via PUDL (`core_eia861__yearly_operational_data_revenue` joined to `…_misc` for `entity_type`). “Retail Power Marketer” is EIA’s entity type, not a hand label.

## What jumped

| Year | RPM revenue, $bn | RPM volume, TWh | Implied ¢/kWh | All sales-for-resale `p_gen`, ¢/kWh |
|---|---:|---:|---:|---:|
| 2019 | 40.0 | 1,057 | 3.79 | 4.22 |
| 2020 | 36.6 | 1,105 | 3.31 | 3.83 |
| **2021** | **67.5** | **1,054** | **6.41** | **5.68** |
| 2022 | 32.5 | 585 | 5.55 | 6.29 |
| 2023 | 26.6 | 571 | 4.65 | 5.27 |

So 2021 is a **price** spike on roughly flat RPM MWh (+$31bn revenue, −5% volume). The 2022 RPM dollar drop is partly **reclassification**: Exelon Generation leaves the retail-marketer category after 2021 while **Wholesale** Power Marketer revenue jumps to $83bn that year. Combined retail+wholesale marketer revenue stays high through 2022 ($114bn / $115bn), then eases.

## Who drove 2020 → 2021 (+$31bn)

| Driver | Δ revenue, $bn | Notes |
|---|---:|---|
| Texas (all RPM) | +16.0 | Shell Energy North America +10.6; Tenaska +3.5 |
| Maryland (almost all Exelon Generation Co.) | +13.5 | Exelon alone +13.4 on MD-coded rows |
| Everyone else | ~+1.5 | Small |

**Texas / Winter Storm Uri.** Shell’s TX sales-for-resale went from ~3.0 ¢/kWh (2020) to ~7.1 ¢/kWh (2021) on slightly lower MWh; Tenaska’s implied price went to ~9.1 ¢. That matches Uri (Feb 2021): ERCOT wholesale cleared at extreme prices for days, so marketers’ wholesale **sales** dollars ballooned without a matching volume boom. TX sales-for-resale revenue across all entity types rose from $40bn (2020) to $71bn (2021).

**Exelon (MD).** Same pattern — volume flat/down, implied price 3.8 → 6.9 ¢ — in a Mid-Atlantic book. That is the broader 2021 wholesale/gas-price cycle, not Uri. Exelon’s disappearance from the RPM series in 2022 is why the *retail* line alone looks like a one-year blip; the dollars largely move to wholesale marketers.

## Effect on national `p_gen`

Dropping all Retail Power Marketer rows changes 2021 `p_gen` only from **5.68 → 5.36 ¢/kWh**. The spike is real and concentrated, but the national average still rises a lot from non-RPM sellers (IOUs, co-ops, wholesale marketers). So the marketer episode **amplifies** the 2021 `p_gen` jump; it does not create it alone. That is consistent with using a multi-year average upstack (#1221): Uri-year and high-gas-year wholesale clears are exactly the single-year noise you smooth.

## Bottom line

The $67.5bn figure is Retail Power Marketer **sales-for-resale revenue** in 2021. It is explained by **much higher wholesale prices on similar MWh**, mainly Texas Uri (Shell, Tenaska) plus Exelon’s Mid-Atlantic book — not by a sudden doubling of marketer sales volume. Treat it as real wholesale-cycle / Uri signal in the 861 price, not as a data error; trailing averages are the right production response, not rewriting the annual series.
