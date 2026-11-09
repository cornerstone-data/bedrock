# PR #1221 review — split bills into generation and a buyer delivery margin

**PR:** https://github.com/cornerstone-data/bedrock/pull/1221
**Implements (plan stays open):** #902 · **Stacked on:** #1220 · **Wires in:** #1217 · **Project:** [Electricity v3](https://github.com/orgs/cornerstone-data/projects/29)
**Branch:** `electricity_delivery_split` → `electricity_generation_price`
**Scope:** new `electricity_delivery.py` + tests; `generation_price` becomes a three-year trailing pool; `resale_volume` already on #1220. Nothing in the live model reads the split yet. Older G/T/D allocator untouched.

## Verdict

**Approve as the method brick #1217 will wire in.** This is the #902 buyer split: one external `p_gen`, generation floored at the bill, residual delivery by buyer. It correctly takes the #902 decision (three-year trailing average + floor) rather than leaving the three options open. Annual series and class tables live in the [PR body](https://github.com/cornerstone-data/bedrock/pull/1221); this note does not reproduce them.

## What it does

Adds `bedrock/transform/eeio/electricity_delivery.py`:

1. **`delivery_split(year, bills)`** — for each buyer with a positive bill, builds kWh, generation dollars, and delivery dollars.
2. **Class kWh** — `kWh_i = bill_i / price_i`, then scale so each EIA sales class matches Table 2.2. Prices from #1219 (MECS×EIA manufacturing; EIA class otherwise).
3. **Generation** — `gen_i = kWh_i × p_gen`, never more than the bill; overflow buyers flagged `floored` with delivery zero.
4. **Own use** — electric columns stay in generation at the full bill (not end use).
5. **Exports** — kWh from EIA Table 2.14; same floor rule.
6. **Line losses** — `generation − end use − exports` (floored at zero), returned as MWh and as USD at `p_gen` for #1217’s `42ELEC` input.

Also **changes `generation_price(year)`** from #1220’s single-year sales-for-resale price to a **three-year trailing pool** of revenue and volume (`pooled_resale_price`). The single-year series remains as `annual_generation_price`. That matches the #902 decision and is why 2021–22 manufacturing “buyers below” largely disappear before bills are applied.

**Transportation class fix:** only rail and transit (`482000`, `485000`, `S00201`) stay in EIA transportation; the rest of NAICS 48–49 move to commercial (the old map put ~$14bn of bills on ~7 TWh).

## Relation to Electricity v3

#902’s plan is: unchanged bills; generation at one observed `p_gen`; delivery margin by buyer into `42ELEC`; no water-fill and no separate T vs D. #1219 and #1220 supply the two prices. **#1221 is the split itself** — the table #1217 will write onto the row (and the loss cell on the column).

It does not place `42ELEC` in the taxonomy (#1216) or change the live disaggregation pipeline (#1217). Released configs keep the old G/T/D allocator. So this PR is foundation, not the release cutover.

## What looks good

- Implements the decided trailing-average + floor rule instead of reopening the three #1220 options.
- Conservation identity is explicit: `gen + delivery = bill` for every buyer; class kWh match EIA by construction.
- Own-use and exports are named special cases, not silent fall-throughs.
- Losses are a separate quantity (not baked into buyer kWh), which matches #1217’s “`42ELEC` buys lost generation” design.
- Transportation class correction is the right fix for the 48–49 dump into a tiny EIA transportation total.
- Synthetic unit tests cover floor, own use, exports, class scaling, and the three-year pool.

## Watch items (non-blocking for merge)

1. **API rename for readers of #1220:** after this PR, `generation_price` means the trailing pool; the annual series is `annual_generation_price`. Fine and intended; just don’t compare #1220 PR-body annual ¢/kWh to post-#1221 `generation_price` without noticing.
2. **Own-use stays in generation** — documented simplification for #1217 to confirm when the column is split.
3. **Bills above EIA revenue** — excess lands in delivery (margins look rich; emissions no longer follow that excess). Named limit in the PR; not a merge blocker.
4. **Rail/transit** — only floored buyers in the reported real-data run (2017 and 2020, &lt; $0.1bn). Small; keep an eye when #1217 books them.
5. **`N` table in the PR body is an estimate** through today’s `L`, not a built #1217 model. Treat as directional until the wire-in lands.

## Bottom line

#1221 is the Electricity v3 buyer split on top of #1219/#1220 prices. Merge as foundation; leave taxonomy (#1216) and pipeline wire-in (#1217) for the next bricks.
