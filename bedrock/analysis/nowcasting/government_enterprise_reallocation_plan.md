# Reallocating government enterprises — embed in the nowcast, or apply after?

**Status: proposal, no decision taken.** Written 2026-08-20 off the Step 4a government findings in
[`output_estimation_plan.md`](output_estimation_plan.md) §"Government — do the enterprises".

Cornerstone's longer-term goal is to reallocate the government *enterprise* sectors into the private
industries and commodities they actually produce. Two of the five are already done — federal and
state/local electric utilities into `221100`, state/local passenger transit into `485000` — in
[`bea_v2017_industry__cornerstone_industry.py:21-24`](../../utils/taxonomy/mappings/bea_v2017_industry__cornerstone_industry.py#L21-L24).
The question here is whether the remaining reallocation belongs **inside** the nowcast build or
**after** it, and what that choice costs the reference case.

**Recommendation in one line:** never before the balance; build it as a **reallocation in BEA's
chapter-9 sense** — BEA's own method, applied to secondary products BEA left under the
industry-technology assumption — sited with Step 7, gated by config, with the un-reallocated BEA-code
MUT quartet kept as a stored deliverable. That makes "embedded" and "afterwards" the same module run
at two different times, and turns the question into a scheduling decision rather than an
architectural one.

### What this builds on

[`methods` discussion #3](https://github.com/cornerstone-data/methods/discussions/3) — *"Do we use the
Before or After Redefinitions tables?"* — **resolved on 2025-11-17 in favour of After Redefinitions**,
on the grounds that commodities have more homogeneous input structure there and Cornerstone runs a
commodity-based model. Three findings from that thread are load-bearing here:

- Redefinitions are **BEA's selective application of the commodity-technology assumption**: they move
  the secondary products whose inputs clearly differ, and leave the rest under industry-technology.
  This proposal is an argument that a specific set of secondary products is on the wrong side of that
  line — not an argument for restructuring the accounts.
- Redefinitions cut economy-wide co-production by **~41%** in the 2017 benchmark. Reproduced
  independently here at **40.8%** (§1), which is the cross-check that the measurements below are
  being taken in the same space as that analysis.
- ⚠️ **BEA's documented method does not reproduce BEA's own AR tables** ([@jvendries,
  2025-11-21](https://github.com/cornerstone-data/methods/discussions/3)), because of an undocumented
  "review and adjustment of reallocations" pass. The thread's conclusion — that an estimation method
  is acceptable and this should not block using AR — applies with equal force here, and it sets what
  validation is available (§4).

---

## 1. What the accounts actually look like

All figures are 2017, $ million, from the benchmark **Make table** (`load_2017_V_before_redef_usa` /
`..._after_redef_usa`) unless marked otherwise. Use-side figures come from `Use_SUT_detail`.

> ⚠️ **The SUT Supply table and the Make table disagree on enterprise industry output**, and the gap
> is not small: `S00203` is 292,843 in `Supply_detail` against 273,910 in the before-redef Make, and
> `S00102` is 15,545 against 9,205. Commodity output agrees exactly (`q(S00203)` = 107,059 in both).
> This is the SUT/MUT divergence `plan.md` §"Framework facts" warns about, and it has to be run down
> before any implementation — the two spaces disagree about *which industry* holds ~19bn. Everything
> below uses Make space, because that is where redefinitions and the deliverables live.

### The five enterprise industries and what they make

| industry | output `x` | own commodity | the private commodities it produces |
|---|---:|---:|---|
| `S00101` federal electric | 15,872 | **0** | `221100` 15,731 · `541511` 125 · `531ORE` 16 |
| `S00201` S&L transit | 16,928 | **0** | `485000` 16,911 · `482000` 17 |
| `S00202` S&L electric | 63,412 | **0** | `221100` 63,412 |
| `S00102` other federal | 9,205 | 725 (7.9%) | `452000` 3,070 · `5241XX` 2,752 · `722110` 1,349 · `531ORE` 671 · `541511` 376 · `446000` 58 |
| `S00203` other S&L | 273,910 | **102,952 (37.6%)** | `221300` 68,351 · `713200` 42,310 · `531ORE` 23,396 · `48A000` 13,601 · `531HST` 7,688 · `221200` 6,180 · `445000` 5,176 · `812900` 3,174 |

Total enterprise output is **379,327m — 1.10%** of the economy's 34,468,047m of industry output.
Of that, **275,650m (72.7%) is co-production of private commodities** and 103,677m is the two
enterprises' own primary commodities.

### ✅ Finding 1: this is two problems, not one

The industry taxonomy has five enterprise codes. **The commodity taxonomy has only two** — `S00102`
and `S00203`
([`v2017_commodity.py:396-407`](../../utils/taxonomy/bea/v2017_commodity.py#L396-L407) against
[`v2017_industry.py:397-407`](../../utils/taxonomy/bea/v2017_industry.py#L397-L407)). That asymmetry
splits the work:

1. **The co-production — 275,650m, 72.7%.** A redefinition in BEA's exact sense: move commodity `c`
   out of the enterprise industry into the industry for which `c` is primary, and reallocate its
   inputs and value added. BEA's chapter-9 method applies directly (§4).
2. **The primary production — 103,677m.** *Not* a redefinition. No industry has `S00203` as its
   primary product, so the receiver rule has no destination and BEA's method is silent (§5).

**Electric and transit needed only problem 1, in its easiest form** — one receiver each, at 99–100%
concentration, and no commodity row to move at all. That is why it could be a four-line change to a
correspondence map, with no transform code and no balance implications.

### ✅ Finding 2: it is the *state and local enterprises* BEA leaves alone — not government generally

The obvious hypothesis — that BEA holds an institutional boundary around government — is **wrong**,
and the 2017 before/after Make pair says so:

| group | co-production BR | co-production AR | reduction | industry output |
|---|---:|---:|---:|---:|
| Economy | 3,244,777 | 1,919,411 | **40.8%** | 34,468,047 |
| Government — **general** | 582,815 | 375,720 | **35.5%** | 3,298,529 |
| Government — **enterprises** | 275,650 | 272,021 | **1.3%** | 379,327 |

BEA redefines *general* government at close to the economy-wide rate: `GSLGE` gives up 47,086m of
`541700` R&D and 17,833m of `722A00` food service; `GSLGO` gives up 29,014m of `525000` funds and
trusts plus 12,470m of school and highway construction; `S00500`/`S00600` give up 36,747m of R&D
between them. This matches the qualitative sweep in discussion #3 ("to 541700 Scientific R&D: **LOTS**
of moves… and from gov").

The enterprises are the exception, and within them the pattern is sharper still:

| industry | moved out by BEA | as % of `x` |
|---|---:|---:|
| `S00102` other federal | 3,504 | 38.1% |
| `S00101` federal electric | 125 | 0.8% |
| `S00201` S&L transit | **0** | **0%** |
| `S00202` S&L electric | **0** | **0%** |
| `S00203` other S&L | **0** | **0%** |

So BEA redefined 38% of `S00102` — it is not squeamish about federal enterprises — and moved
**exactly zero** out of the three state and local enterprise sectors. The 68,351m of water and
42,310m of gambling inside `S00203` survived a process that redefines "trade activities in nontrade
industries" and "service activities in nonservice industries" as standing rules.

⚠️ **Why is not documented anywhere I can find**, and the branch holding the discussion-#3 analysis
(`4-BeforevAfterRedef`) no longer exists on the remote. Read literally, BEA's criterion is that the
secondary product's inputs differ from *the primary product of the industry producing it* — so the
implied BEA judgment is that running a water utility, a lottery and public housing are not
input-dissimilar from whatever `S00203`'s own 102,952m primary product is. **That judgment is what
this proposal disputes**, and stating it that way is the honest framing: the claim is narrow,
falsifiable, and rests on BEA's own criterion rather than on a preference for private sectors.

### Sizing the use side

| commodity | total use | composition |
|---|---:|---|
| `S00102` | 1,233 | 100% intermediate, concentrated in finance (`523900` 835, `522A00` 284) |
| `S00203` | **107,054** | `F01000` household PCE **72,509 (67.7%)**, intermediate 34,550 across ~338 buyers |

Two-thirds of the residual commodity is household consumption. `S00102`'s commodity is ~1.2bn and can
be treated as rounding.

---

## 2. Where it could go in the pipeline

| # | Where | Verdict |
|---|---|---|
| **A** | **Pre-balance** — reallocate the Step 1–4 seed, balance the reallocated table in Step 5 | ❌ disqualified, §3 |
| **B** | **Post-balance, in-pipeline** — a second reallocation pass at Step 7, before Step 8's schema collapse | ✅ **recommended home** |
| **C** | **Post-publication** — the same module against the finished MUT quartet off GCS | ✅ same code, later; the fallback |
| **D** | **In the correspondence** — extend `bea_v2017_industry__cornerstone_industry.py` as electric/transit did | ⚠️ industry side only, and silently; §6 |

Siting it **at Step 7 rather than after it** is the change from the first draft of this proposal.
Step 7 already exists to apply redefinitions in BEA detail space; this *is* a redefinition; it belongs
in the same pass, as a second, separately-labelled schedule applied after BEA's own.

## 3. Why pre-balance is disqualified

Not a preference — three independent blockers, each fatal alone.

1. **Every RAS target is denominated in BEA codes, and they are BR-equivalent.** Step 5's target set
   (T1–T17, [`nowcast_targets.py`](../../transform/iot/nowcast_targets.py)) comes from BEA's published
   summary SUT, `UGO305-A` detail gross output and NIPA aggregates. `GSLE` — which is `S00203` alone
   — is one of the 21 single-child summary groups whose `q` is *observed*, per the Step 4a
   correction. Reallocating first means either inventing controls BEA never published or discarding
   them. Discussion #3 makes the same point from the other side: detailed gross output and the SUT
   are published **only** in a before-redefinitions equivalent, which is precisely why the whole build
   is naturally BR until Step 7. Moving a reallocation upstream of the balance compounds that
   alignment problem instead of containing it. The mask layer has the same issue: its tiers are keyed
   on BEA sectors.

2. **It destroys the backbone test.** Steps 6 and 7 are validated by replaying the 2017 benchmark
   and diffing against the published before/after MUT pair — `plan.md` calls this "the single
   highest-value test in the project." A reallocated table cannot reproduce a published table that
   still carries `S00203`.

3. **Redefinition ratios don't exist for reallocated codes.** Step 7 derives per-cell ratios from
   BEA's 2017 before/after pair on BEA detail codes. Merge `S00203` into `221300` first and there is
   no ratio to apply to the merged cell.

Softer but real: the reallocation is a *modelling choice* and the balance is an *accounting
operation*. Mixing them means any future change to the reallocation forces a full re-RAS of every
year.

## 4. The method for the co-production — BEA's chapter 9, not ours

This is settled, and the first draft of this proposal was wrong to call it an open problem.

**The receiver rule** is already implemented. `compute_coproduction_ratios`
([`derived_gross_industry_output.py:164-226`](../../transform/iot/derived_gross_industry_output.py#L164-L226))
states it directly: *"The destination industry is the industry whose primary commodity is `c`. At
BEA's detail level the industry and commodity code namespaces are the same, so the destination
industry code equals the commodity code."* No judgment call — `221300` goes to `221300`.

**The input rule** is BEA's, from *Concepts and Methods*, ch. 9 "Reallocations" (PDF p.180-184):

> *"Reallocation is the means by which the inputs associated with the production of redefined
> secondary products are identified and reassigned from the producing industry to the industry for
> which the product is primary… the intermediate inputs **and value added** associated with these
> products are reallocated to the primary industry… Generally, reallocations are estimated using the
> input pattern for the industry to which they are being moved."*

BEA's own worked example (tables 9.1–9.3): industry B produces $100 of commodity B using $20 of A,
$30 of B and $50 of value added. Moving $10 of commodity B out of industry A reallocates
$2 / $3 / $5 — *"calculated using the direct requirements coefficients to industry B."*

Operationally, for an amount `m` of commodity `c` moving from enterprise industry `i` to receiver
`d`, with `a[·,d]` the receiver's direct requirements per dollar of output:

```
Make:   V[i,c] -= m                  V[d,c] += m
Use:    U[·,i] -= m·a[·,d]           U[·,d] += m·a[·,d]
VA:     VA[i]  -= m·(1 - Σa[·,d])    VA[d] += m·(1 - Σa[·,d])
```

Commodity output is unchanged; industry output moves by `m`; every column total is preserved.
That is the same zero-sum shape `adjust_gross_output`
([`derived_gross_industry_output.py:229`](../../transform/iot/derived_gross_industry_output.py#L229))
already applies to the output vector — this extends it to the input columns and VA, which Step 7
needs regardless.

### ⚠️ Two caveats BEA states about its own method

**Negative residuals are expected and are the diagnostic.** BEA: *"in some cases, it may be necessary
to adjust the inputs so the reallocation does not result in negative values for inputs in the use
table. Negative inputs generally indicate that the inputs as originally estimated did not include the
inputs for the secondary production."* So `S00203`'s column going negative on some input after the
strip is a documented, anticipated outcome with a documented response — adjust — not a reason to
stop. It is the measurement that tells you where the estimate is thin.

**Large redefinitions get bespoke treatment.** BEA: *"Detailed reallocations are specifically
prepared for large redefinitions… For smaller redefinitions, the reallocations may include only a few
inputs."* The mechanical coefficient method is what BEA applies to the small ones. Which of ours are
"large" is answered below, and the answer is only two.

### ⚠️ What validation is therefore available

Discussion #3 established that BEA's documented method **does not** reproduce BEA's AR tables, owing
to an undocumented review-and-adjustment pass. So the benchmark-replay standard used elsewhere in
`plan.md` does **not** transfer to this step, and the proposal should not pretend otherwise. What is
available instead:

- **Totals preservation**, exactly: commodity output unchanged, industry output moved by `m`, every
  column total intact. This is by construction, so it proves the code runs — label it as such.
- **Negative-residual census** on the stripped enterprise columns, per BEA's caveat above.
- **Reproduce BEA's own enterprise moves**: `S00102`'s 3,504m is a real BEA redefinition of exactly
  this kind. Running the method on it and comparing to the published AR table is the closest thing to
  a held-out test that exists here — small, but real.
- **EF diffs** against the un-reallocated tables (§7).

### Where the assumption is being extrapolated

Applying `d`'s coefficients to a volume much larger than `d` currently produces is where
commodity-technology strains, and it is where the negative residuals will concentrate. Ranking
receivers by how far the move extrapolates them:

| receiver | receiver's `x` | inbound from enterprises | extrapolation |
|---|---:|---:|---:|
| `221300` water, sewage | 15,138 | 68,351 | **4.5×** |
| `713200` gambling | 36,796 | 42,310 | **1.15×** |
| `485000` transit *(done)* | 78,300 | 16,911 | 0.22× |
| `221100` electric *(done)* | 389,427 | 79,143 | 0.20× |
| `48A000` other transport support | 141,645 | 13,601 | 0.10× |
| `221200` natural gas dist. | 69,552 | 6,180 | 0.09× |
| `812900` other personal services | 68,718 | 3,174 | 0.05× |
| `445000` food and beverage stores | 249,237 | 5,176 | 0.02× |
| `531ORE` / `531HST` / `5241XX` / `722110` | large | ≤ 24,067 | ≤ 0.02× |

**Exactly two receivers are exposed**, and they are the same two that BEA's "large redefinitions"
caveat would single out. Everything else sits at or below the 0.20–0.22× the already-completed
electricity and transit reallocations ran at, so the method carries them without argument. For
`221300` the private industry is a fifth the size of the government output moving into it — the
merged industry's technology would be substantially the government's, which is defensible (public
water *is* how water is produced in the US) but should be a stated decision rather than a side
effect. `713200` is a milder version of the same.

## 5. The part BEA's method does not cover

The 102,952m `S00203` residual commodity has **no receiver**, because no industry has it as a primary
product. By construction it is what *isn't* water, gambling or housing, so splitting it by the
industry's own co-production shares is circular and points the wrong way.

Two honest options:

- **Source a functional decomposition** from the Census Annual Survey of State and Local Government
  Finances, which reports utility and enterprise revenue by function.
  `output_estimation_plan.md` identifies this as the natural decomposition of `S00203` and records
  that **there is no government-finance source in `extract/` — this would be a new FBA.**
- **Keep `S00203` as a commodity** and reallocate only the co-production.

The second is a legitimate shippable partial: it captures 72.7% of enterprise output with a method
that needs no new data, and leaves a smaller, honestly-labelled government residual behind.

## 6. Protecting the reference case

The concern is right, and it is **already partly realised**. Today's electric/transit reallocation is
hardcoded in the correspondence, so Cornerstone-schema tables have no `S00101`/`S00201`/`S00202` and
there is no flag to get them back. The reference case survives only because the BEA-detail tables
exist upstream of Step 8.

That points at the invariant to protect, which is not "the published tables are un-reallocated":

> ✅ **The BEA-detail MUT quartet, both before and after redefinitions, is itself a stored, versioned
> deliverable — not an intermediate.** Everything downstream, including this reallocation, is a
> labelled transform on top of it.

Step 9 already stores per-year Make/Use/Import/Margins on GCS; the ask is that the *BEA-code* variant
be stored, not only the Cornerstone-schema one. With that in place the reference case cannot be lost,
because it is the input to everything else. It also answers the open item bl-young raised in
discussion #3 — *"a year × industry matrix of industry output before / after"* — with the same
artifact, since a stored BR and AR pair is that matrix.

Then make the reallocation selectable rather than implicit. The precedent is
`iot_before_or_after_redefinition`, which does exactly this job for a structurally identical "same
accounts, two presentations" choice — now a **settled** choice (`after`, per discussion #3) rather
than an open one. Siting the reallocation at Step 7 makes the parallel exact, since both are
redefinitions. Proposed:

```yaml
gov_enterprise_reallocation: none | electric_transit | coproduction | full
```

`electric_transit` stays the default, so today's behaviour is unchanged and named rather than hidden;
`coproduction` is §4 alone; `full` adds §5. This also retires option **D** — a correspondence dict
cannot express the commodity side and gives no seam for a config flag.

## 7. What still needs sourcing or sweeping

**⚠️ The `S00203` residual (§5)** is the only genuine data gap, and only if `full` is wanted.

**⚠️ The satellite tables have to move with the sectors.** `S00203` and `S00102` are hardcoded
allocation keys in at least
[`transportation_fuel_use/derived.py:178-188`](../../transform/allocation/transportation_fuel_use/derived.py#L178-L188)
and
[`co2/non_energy_fuels_transport.py:25-37`](../../transform/allocation/co2/non_energy_fuels_transport.py#L25-L37).
A reallocation that moves the money and leaves the emissions produces an EF error, not a
reclassification. This is the same open question bl-young flagged in discussion #3 — *"which of our
environmental datasets or assumptions might be most impacted"* — narrowed to five sector codes, so it
is a tractable sweep rather than an open research item.

**⚠️ The SUT/Make output discrepancy** flagged at the top of §1 — 19bn on `S00203` — must be resolved
first, since the reallocation is defined on industry output.

### A note on how much this actually changes the EFs

Worth measuring early, because it may reset expectations. The Make table already assigns 68,351m of
the `221300` commodity to `S00203`, so under the industry-technology assumption the market-share
blend already carries government's input structure into the `221300` **commodity** EF. Reallocation
does not add that — it is already there. What genuinely changes is (a) industry-level results, which
lose the enterprise sectors, (b) the blend for receivers that gain a large, dissimilar column — i.e.
exactly the two rows at the top of §4's extrapolation table, and (c) the `S00203` residual commodity,
which today has its own row and its own EF. **Hypothesis to test, not a conclusion:** run the
reallocation on 2017 and diff commodity EFs. If the movement concentrates in the residual and in
water/gambling, the case for shipping `coproduction` alone gets much stronger.

## 8. Proposed sequence

1. **Reserve the seam.** Land the `gov_enterprise_reallocation` config variable and make the
   BEA-detail MUT quartet an explicit Step 9 deliverable. Small, and it is the thing that protects
   the reference case. Do this during Phase 1.
2. **Resolve the SUT/Make discrepancy** on `S00203` and `S00102`.
3. **Build the `coproduction` schedule against published 2017** — receiver rule from
   `compute_coproduction_ratios`, input rule from §4, extending the existing redefinition machinery
   to input columns and VA. No nowcast dependency; it runs on tables that exist today. Validate on
   `S00102`'s 3,504m of real BEA moves first.
4. **Measure it** — negative-residual census on the stripped columns, totals-preservation invariants,
   and commodity/industry EF diffs vs. the un-reallocated 2017 tables.
5. **Decide on the residual** once (4) says what the 103bn is worth. If it matters, that justifies
   the Census government-finance FBA — which `output_estimation_plan.md` correctly declined to fund
   on *Step 4a* grounds. A different justification, not a reversal.
6. **Switch it on in-pipeline** at Step 7, after Phase 2's re-run rather than before — Phase 1's
   tables are a working series, and the Sept 2026 BEA annual update re-runs everything.

## 9. Decisions this needs

1. Is `221300` acceptable as a merged sector that is **82% government**, and `713200` at 54%? If not,
   those two keep separate public/private industries and the reallocation covers the rest — which
   costs 110bn of the 276bn and leaves the method unarguable everywhere it is applied.
2. `coproduction` only, or `full`? `coproduction` is shippable with no new data; `full` is gated on a
   Census extract that does not exist.
3. Is the BEA-detail MUT quartet a published deliverable, or an internal artifact? Everything in §6
   depends on the former.
4. Does the reallocated table become the **default** for the model build, or an alternate scenario?
   This is a `ModelRequirements.md` question, and probably belongs back in `methods` rather than here.
