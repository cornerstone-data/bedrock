# Differences between the SUT ad MUT tables

- **Framework.** `V00100`, `V00300` and `T005` are rows of both Use tables with
  different values — compensation by 3 million, gross operating surplus by 16.
  Nothing errors; you silently get the other number. The tax rows don't
  correspond at all: SUT splits `T00OTOP` (608,542), `T00TOP` (755,451) and
  `T00SUB` (59,876) where MUT carries one net `V00200` (1,304,104). `T018`,
  `VABAS`, `VAPRO` are SUT-only; `T006`, `T008` MUT-only; `F05000` (imports) is a
  MUT-only *column*.
- **Valuation.** Not one basis per table — the SUT Use table carries three, and
  "which valuation" only has an answer once you say which slice you mean.

  Its **cells**, intermediate and final demand alike, are at **purchaser** value.
  Total use `T019` equals the Supply table's purchaser total `T016` 37,094,434
  for all 402 commodities; basic `T013` is 36,398,867, and the 695,567 between
  them is margins and product taxes. Only 37 commodities agree with basic, being
  the ones that carry neither.

  Its **industry columns** are then totalled twice over those same cells —
  `T018` 33,772,568 at basic, `T005` + `VAPRO` 34,468,127 at producer — which is
  the number comparable to MUT `T008` 34,468,137. Value added comes both ways for
  the same reason: `VABAS` 18,916,542, `VAPRO` 19,612,109. Three identities hold
  cell for cell across the 402 industries, to BEA's rounding:

  ```
  VABAS = V00100 + T00OTOP + V00300
  VAPRO = VABAS  + T00TOP  - T00SUB
  T018  = T005   + VABAS            # intermediate at purchaser + VA at basic
  ```

  So `bea_matrix_column('230301')` gives you purchaser-valued commodity inputs,
  while `bea_matrix_row('V00100')` gives a value-added component that sits under
  `VABAS`. The `.valuation` a series reports is the **cells'** basis; read the
  matrix `note` before comparing a total.

  The **Supply** table splits the same way, in the opposite direction. Its
  commodity × industry cells are domestic output at **basic** value, and the
  trailing columns bridge from there — every identity below holding 402/402 per
  commodity:

  ```
  T013 = T007 + MCIF + MADJ       total supply, BASIC        36,398,867
  T014 = TRADE + TRANS            margins                             1
  T015 = MDTY  + TOP   + SUB      taxes less subsidies          695,565
  T016 = T013  + T014  + T015     total supply, PURCHASER    37,094,434
         T013  + T015             derived PRODUCER           37,094,432
  ```

  Producer value is not published but derives cleanly: `T013 + T015`, equivalently
  `T016 − T014` — taxes but not margins.

  Two traps in those columns. `T014` sums to **1** economy-wide, not because
  margins are negligible but because they net out: a trade margin is added to the
  good and subtracted from the trade commodity that earned it. Any check that
  aggregates first will therefore accept a margin error silently — which is how
  this table's note read `T013 + MDTY/TOP/SUB = T016` for a while, an identity
  that is right in total and wrong for all but 120 commodities. And `SUB` is
  stored **negative** here, so it is added, where the Use table carries `T00SUB`
  positive and subtracts it.
- **Redefinition** moves money *between cells while preserving every total*, so a
  totals check cannot tell you that you picked the wrong one. Of the 161,604
  intermediate cells, 5,740 differ, 553,635 million moves gross and the largest
  single cell shifts 42,893 — for a net of −7. This is the axis to get right
  before any cell-by-cell comparison.