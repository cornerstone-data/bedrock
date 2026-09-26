# Evidence that Census, not BEA, has manufacturing right in 2023-24

Issue #1013, shipped as PR #1014
(`bedrock.transform.iot.aies_go_chaining`).

The chaining module replaces BEA's own 2023-24 movement for manufacturing detail
with Census receipts. That is only defensible if Census is the better
measurement, and #1013 filed **two** candidate mechanisms with opposite fixes:

1. BEA's nonbenchmark extrapolation lost its anchor when Census retired the
   Annual Survey of Manufactures — then Census is right.
2. AIES itself has a survey break against ASM and the Economic Census — then
   Census is the one that moved and BEA may be fine.

This file records the evidence that settles it as (1), **with the provenance of
each number marked**, because the argument leans on facts from outside the repo
and those should not be mistaken for measurements.

## How to read the provenance marks

| mark | meaning |
|---|---|
| 🟢 **measured in-repo** | reproducible from committed data with the command given below |
| 🟡 **measured from an external API** | pulled live this session; the route is given, the value is not committed |
| 🔵 **public record, not verified here** | widely reported and used as corroboration only; ⚠️ **no part of the verdict rests on it alone** |

⚠️ The distinction matters. The verdict for refineries rests on 🟢 and 🟡 only.
The aircraft and steel cases lean on 🔵 for the *interpretation* of a 🟢
measurement — the measurement is that BEA and Census disagree on direction; the
public record is what says which direction is right.

## 1. The disagreement itself — 🟢 measured in-repo

Census total receipts against our EC-adjusted panel, $bn. `RCPTOT` from the
Economic Census for 2017 and 2022, `RCPT_TOT_VAL` from AIES for 2023 and 2024.

| code | industry | 2017 | 2022 | 2023 | 2024 | 2023→24 |
|---|---|---:|---:|---:|---:|---:|
| `324110` | refineries — Census | 499.1 | 825.2 | 709.5 | 656.0 | **−7.5%** |
| | refineries — BEA | 494.7 | 823.3 | 666.6 | 593.0 | **−11.0%** |
| `336411` | aircraft — Census | 149.6 | 92.8 | 106.2 | 96.8 | **−8.8%** |
| | aircraft — BEA | 149.6 | 95.2 | 123.5 | 133.7 | **+8.2%** |
| `331110` | iron and steel — Census | 87.6 | 129.8 | 112.2 | 100.5 | **−10.4%** |
| | iron and steel — BEA | 88.1 | 127.8 | 127.7 | 125.6 | **−1.6%** |
| `336412` | aircraft engines — Census | 38.9 | 40.1 | 42.2 | 47.5 | **+12.6%** |
| | aircraft engines — BEA | 46.7 | 49.5 | 59.7 | 61.4 | **+2.9%** |

✅ **The two agree closely in 2017 and 2022 and diverge in 2023-24**, which is
what makes this a finding about the nonbenchmark years rather than a definitional
mismatch. `336412` sits 20% above Census even in 2017 — a standing wedge that is
correctly *preserved* by anchoring each industry on its own base-year ratio
rather than forcing the ratio to 1.

## 1b. BEA could not have used AIES 2024 — the strongest argument, and an a priori one

⚠️ **BEA extended its gross output series to 2024 before AIES 2024 existed.**
(Wes, 2026-09-25.)  Census's 2024 AIES release post-dates BEA's 2024 annual
update, so whatever BEA used for 2024 manufacturing detail, **it was not this
survey** — it could not have been.

✅ That converts mechanism (1) from an inference into near-certainty, and it does
so *without* relying on any of the industry evidence below.  The industry cases
answer "which series is closer to the truth"; this one answers the prior
question of "is there any reason to expect BEA's 2024 detail to incorporate
AIES", and the answer is no.

➡️ **It also makes a testable prediction**: BEA should revise its 2023-24 detail
*toward* AIES at the next annual update.  If a future vintage of `UGO305-A`
moves the 59 drifting industries back toward census, that is this module's
finding confirmed by BEA itself — and the module should then be retired rather
than kept.  ⚠️ Worth re-running the drift table against each new BEA vintage for
exactly that reason.

⚠️ **Verify the two dates before quoting this externally.**  The reasoning is
sound but the release calendar is not reproduced here: check BEA's GDP-by-Industry
release date for the 2024 annual update against Census's AIES 2024 release date.

## 2. Refineries — the case that needs no outside knowledge

🟡 **measured from EIA v2 this session.** Two series, annual, US total:

| | 2017 | 2020 | 2022 | 2023 | 2024 |
|---|---:|---:|---:|---:|---:|
| crude refinery net input, kb/d | 16,590 | 14,212 | 15,977 | 15,967 | **16,225** |
| refiner acquisition cost of crude, $/bbl | 50.68 | 39.75 | 95.29 | 77.67 | **76.64** |

Routes: `petroleum/pnp/inpt2` (product `Crude Oil`, process `Refinery Net Input`,
`duoarea=NUS`, annual) and `petroleum/pri/rac2` (composite, `duoarea=NUS`,
annual). ⚠️ `petroleum/pnp/wiup` is **weekly only** and 400s on an annual
request.

🟢 Combining with the receipts above gives revenue per barrel of crude input, and
the implied gross margin over crude:

| | revenue/bbl | crude cost | **margin** |
|---|---:|---:|---:|
| 2017 actual | $81.70 | $50.68 | **$31.02** |
| 2024 **on BEA** ($592.3bn) | $100.02 | $76.64 | **$23.37** |
| 2024 **on Census** ($656.0bn) | $110.78 | $76.64 | **$34.14** |

⚠️ **BEA's 2024 level implies refining margins 25% below 2017.** Crude runs were
*up* 1.6% on 2023 and crude cost was *down* 1.3%, so nothing in the physical data
supports a margin compression of that size. Census's implies margins slightly
above 2017.

✅ **This case is closed on 🟢 and 🟡 alone** — no outside knowledge is needed, and
it is the single strongest piece of evidence in the file.

## 3. Aircraft — 🟢 direction measured, 🔵 direction adjudicated

🟢 BEA has `336411` **growing 8.2%** in 2024 while Census has it **falling 8.8%**.
A $36.8bn disagreement, and the largest single overshoot in manufacturing.

🔵 **Public record, not verified here.** 2024 was the worst year Boeing has had
since the 737 MAX grounding:

- deliveries fell to roughly **348** from about **528** in 2023
- the Alaska Airlines door-plug blowout in **January 2024** triggered an FAA
  production cap on the 737 line
- an IAM machinists' strike from **September to November 2024** halted 737, 767
  and 777 production for about seven weeks

⚠️ These figures are from public reporting and company disclosure. They are
**not** reproduced from any source in this repository, and a reader who wants
them nailed down should check Boeing's 2024 Form 10-K and monthly Orders and
Deliveries disclosures directly.

✅ What the public record is doing here is narrow but decisive: it says US
commercial aircraft output **unambiguously fell** in 2024. No plausible reading
has it growing 8.2%. So Census has the sign right and BEA does not.

## 4. Iron and steel — 🟢 direction measured, 🔵 direction adjudicated

🟢 Census has `331110` falling **10.4%** in 2024; BEA has it falling **1.6%**. A
$25.1bn disagreement.

🔵 **Public record, not verified here.** US hot-rolled coil fell from roughly
$1,000/ton early in 2024 to roughly $700/ton by late in the year, while raw steel
production was approximately flat. Flat volume against a sharply lower price
gives a materially lower nominal revenue, which is Census's −10.4% rather than
BEA's −1.6%.

⚠️ Neither the price path nor the production figure is reproduced here. An
independent check would use AISI weekly raw steel production and a published HRC
index.

## 5. What the evidence does *not* establish

Stated so the file is not read as more than it is.

- ⚠️ **Only three industries were arbitrated**, out of 59 that drift more than
  10% by 2024. The module applies Census movement to **all** of manufacturing.
  The inference from three to all rests on the *pattern* — every one of them
  breaks at 2023 and none breaks at 2022 — not on having checked each one.
- ⚠️ **AIES is a new instrument.** Mechanism (2) is made unlikely by the three
  cases above, not impossible. If a fourth industry were found where independent
  data backs BEA, that would be worth more than any of these.
- ⚠️ **The manufacturing total is not in evidence either way.** BEA and Census
  agree on it to within half a point in every year, which is why the module holds
  BEA's total and moves only the distribution. Nothing here says the level is
  right; it says the two sources do not disagree about it.
- ⚠️ **Trade and transport are untested.** AIES covers them and they are staged,
  but no ratio test has been run, so the module is manufacturing-only.

## Reproduce

🟢 the in-repo measurements:

```
Census_AIES_Expenses_{2023,2024}_*.parquet   FlowName 'RCPT_TOT_VAL',
                                             NAICS on ActivityConsumedBy
Census_EC_Expenses_{2017,2022}_*.parquet     FlowName 'RCPTOT'
detail_gross_output_panel(ec_adjusted=True)  and ec_adjusted=False

uv run python -m bedrock.transform.iot.aies_go_chaining --report
```

🟡 the EIA series, which need `EIA_API_KEY` from the repo `.env`:

```
https://api.eia.gov/v2/petroleum/pnp/inpt2/data/?frequency=annual&data[0]=value
    &facets[duoarea][]=NUS          product 'Crude Oil', process 'Refinery Net Input'
https://api.eia.gov/v2/petroleum/pri/rac2/data/?frequency=annual&data[0]=value
    &facets[duoarea][]=NUS          composite
```

➡️ If these become load-bearing for anything beyond this one verdict, they should
be promoted to a committed FBA rather than left as a live API call — the same
argument that produced the static-export rule for GHGRP.
