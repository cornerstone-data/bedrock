# EIA-anchored G/T/D analysis

Results-deck tables of the EIA-anchored generation / transmission /
distribution method.

Built from `EIAPurchaserAllocation` (IO purchasers tagged with EIA end-use
class), not purchaser-price vs producer-price valuation.

```bash
python -m bedrock.analysis.electricity.current.eia_gtd
# optional: --config 2025_usa_cornerstone_v0_3_electricity_disaggregation
```

That sequence is `reset_usa_config` → `clear_all_publish_caches` →
`set_global_usa_config` → `derive_cornerstone_Aq_scaled` →
`get_reanchored_eia_purchaser_allocation()` (assert not `None`). Do **not**
use `get_2017_eia_purchaser_allocation`.

| Table | Identity |
|---|---|
| Class MWh vs class targets | `_class_mwh_targets(eia_year, alloc.egrid_mwh)` vs `alloc.mwh.groupby(alloc.end_use_class).sum()` |
| Leftover T&D | `electricity_purchases − gen_dollars` (equals `t_dollars + d_dollars`) |
| Nibble vs clipped | class totals below class target; `clipped` is purchaser-level only |
| Optional ¢/kWh | `electricity_purchases / (10 × MWh)` vs Table 2.4 — check only, not class targets |
| MECS vs dollar manufacturing | Dual-run `industrial_weights=mecs\|dollars` on the same electricity_purchases |

Markdown lands at `current/diagnostics/output/eia_gtd_purchaser_tables.md`.

## Why Table 7.7 stays FBA (not FBS)

No MECS Energy FBS exists in bedrock. Tables 2.2/3.2 stay FBA and enter GHG (and CAP/HAP) FBS build as attribution sources. Table 7.7 is also left as FBA: it only supplies manufacturing purchased-kWh weights inside the electricity (Generation/Transmission/Distribution) purchaser allocation. We do not build an FBS because that path would run `estimate_suppressed_mecs_energy` and a generic NAICS→BEA crosswalk, both wrong for table 7.7 for the following reasons:

A) `estimate_suppressed_mecs_energy` drops every row marked `D` or `Q` (i.e., treats them as “no more information than an industry that is not in the table”) and replaces `*` with 0.25, because in those tables `*` means “less than 0.5 Trillion Btu”. Table 7.7 is million kWh, not TBtu. Putting 0.25 on `*` would be a TBtu-scale guess in the wrong unit.

Dropping `Q`/`D` is worse: EIA still publishes the manufacturing total `31-33`, so a withheld 3-digit industry is recoverable. If we drop it, those kWh disappear from manufacturing shares and that NAICS gets a zero electricity weight even though the table identity tells us the amount.

What this path does instead:

- `*` → 0 (too small to publish, not 0.25 TBtu)
- `Q`/`D` at 3-digit NAICS → residual fill (below)
- other `Q`/`D` → 0

B) 3-digit `Q`/`D` residual fill: Table 7.7 is hierarchical: `31-33` is all manufacturing purchased electricity; under that sit 3-digit industries (`311` food, `331` primary metals, `337` furniture, …), then 6-digit rows. When one 3-digit cell is `Q` or `D`, EIA still prints 31-33, which means we can calculate the leftover:

`leftover = 31-33 − (sum of 3-digit industries that are published)`

If exactly one 3-digit industry is Q/D, that leftover is that industry’s kWh. If leftover is not ~0 and there is not exactly one Q/D 3-digit, it hard-errors instead of guessing. This is indeed the case for 2022 MECS, which is the survey this implementation uses for model years after 2017:

Live US Table 7.7 `Electricity total`:

| Survey | 3-digit Q/D | `31-33` − published 3-digit |
|---|---|---|
| **2018** | none | **0** million kWh |
| **2022** | **exactly one: `337` (furniture), marked `Q`** | **9,224** million kWh |

We need that leftover because manufacturing G/T/D MWh shares are normalized 7.7 kWh weights. Losing a whole 3-digit industry would reassign its electricity to every other manufacturer. Recovering it from `31-33` keeps the withheld industry in the manufacturing pool with the quantity EIA implied.

C) Mapping: Using a default `FBS` map (`BEA_2017_Detail`) would not apply the 3.1 subtraction (e.g. `331313` = `3313` − `331314`/`331315`/`331318`) and could attach parent rows like `31-33` unless they are explicitly excluded. GHG combustion already uses the Cornerstone 3.1 hand map, not that generic crosswalk, which means that table 7.7 must use that same hand map, which the usual FBS path would not provide.

## MECS NAICS/BEA vs GHG industrial combustion

### Summary of comparison

The treatment of table 7.7 here and MECS for GHG interpret the *same* MECS industry pool onto BEA the *same way*. On that, the methods align: same 3.1 hand map, same manufacturing vs `NON_MECS` split. We are not inventing a second NAICS story for electricity.

What *is* different is GHG’s table-specific mechanics (static 2018 survey year use, `Q`/`D`→0, split by fuel Use). Those would be the wrong analogue for purchased kWh.

The electricity method is: map with the Cornerstone 3.1 dictionaries (same); split multi-IO rows by Use of that row’s commodity (same concept, different commodity); use the MECS survey that matches the electricity year (different); read 7.7’s `31-33` identity instead of dropping a recoverable 3-digit cell (different).

### Comparison of MECS across NAICS when using table 7.7 for electricity vs. GHG MECS allocation of industrial combustion

**Same**

| What | Method |
|---|---|
| Hand map | Which NAICS belong to which BEA, including 3.1 subtraction |
| Industry pools | Manufacturing vs `NON_MECS` membership |

The hand map of MECS across NAICS is consistent between the GHG MECS allocation and the table 7.7 allocation. Both paths use `CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_*` (plus `NON_MECS_INDUSTRIES` for residual industrial). The use of the 2022 survey requires the `MECS_7_7_NAICS_OVERLAY`, which relabels four 2018 NAICS codes so 2022 Table 7.7 can be read.

**Different**

| What | Method (7.7 vs GHG MECS) |
|---|---|
| Survey Used | 7.7 follows EIA/eGRID year (2018 or 2022) vs 3.1 pinned 2018 |
| Suppression treatment | 3-digit Q/D leftover vs Q/D → 0 |
| Multi-IO (and residual) **split weights** | electricity purchases vs BEA fuel Use |

### Does it make sense to align 7.7 for electricity with GHG MECS?

**Survey year:** No, not by pinning 7.7 to 2018. G/T/D is tied to the EIA/eGRID year (2017 → MECS 2018; model year → MECS 2022). Using 2018 shares on a 2024 electricity allocation would freeze manufacturing kWh in the previous survey while class MWh and eGRID move, which would mix two survey years in one allocation.

**Suppression treatment:** Align only if you value cross-method identity over this table. `load_mecs_3_1` zeroes `Q`/`D`. On 7.7 that would set 2022 `337` to 0 and dump 9,224 million kWh onto every other manufacturer, even though `31-33` still implies furniture’s total. The leftover fill makes sense for this table.

**Split weights:** No. GHG does not have a generic “use BEA fuel Use” rule. It splits a MECS coal (or gas) total by BEA Use of that fuel. The parallel for 7.7 is BEA Use of electricity (purchaser electricity purchases on 221100), which is what this path already does. Splitting purchased kWh by coal/gas Use would assign furniture vs chemicals electricity with the wrong commodity. Residual `NON_MECS` is the same idea: leftover industrial electricity should follow electricity purchases, not fuel Use.

## Census electricity cost vs Table 7.7 kWh

Nowcasting maps annual Census “cost of purchased electricity” onto `221100`: `CSTELEC` (ASM 2018–2021, EC 2017/2022) and `EXPS_ELEC_VAL` (AIES 2023, SAS 2018–2022), NAICS-6, on the `nowcast` expense FBAs (`Census_ASM_Expenses`, `Census_EC_Expenses`, `Census_AIES_Expenses`, `Census_AIES_Service_Expenses`, `Census_SAS_Expenses`). That is the right dollar series for SUT nowcasts. It is the wrong manufacturing **MWh** weight here.

G/T/D Industrial manufacturing is a physical split: class MWh comes from Table 2.2 / eGRID, and 7.7 is purchased kWh. Cost = kWh × rate. Intra-manufacturing rates are not flat, so cost shares would move MWh toward high-price, low-intensity sectors. Implied ¢/kWh vs Table 2.4 is already a check only, not a class target, for the same reason.

Census cost would help later for years between MECS surveys, or for Commercial/service NAICS-6 where 7.7 does not apply. Those FBAs are not on this branch and are not wired into the allocator.
