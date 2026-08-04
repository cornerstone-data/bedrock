# Use-table intersection CSV: Bedrock vs useeior

`WasteDisaggregationDetail2017_Use.csv` is shared in spirit with
useeior’s `inst/extdata/disaggspecs/WasteDisaggregationDetail2017_Use.csv`.
All slices other than **Use table intersection** match. Intersection rows
differ on purpose.

## What changed in Bedrock

For rows whose `Note` is `Use table intersection` only, Bedrock swaps the
**values** in `IndustryCode` and `CommodityCode` relative to useeior.
Headers and `PercentUsed` are unchanged. Other Notes (column sum, row sum,
FD, VA, etc.) are identical to useeior.

| | useeior (upstream) | Bedrock (this file) |
|---|---|---|
| Example mass `0.0111` | `IndustryCode=562111`, `CommodityCode=562HAZ` | `IndustryCode=562HAZ`, `CommodityCode=562111` |
| Meaning of columns on intersection rows | CSV col 1 → Use **row** (commodity); CSV col 2 → Use **col** (industry) | CSV col 1 → **industry**; CSV col 2 → **commodity** |

Sentinel after Bedrock’s swap:

```csv
562HAZ/US,562111/US,0.0111,Use table intersection
```

Same percent in useeior is still authored as:

```csv
562111/US,562HAZ/US,1.11E-02,Use table intersection
```

## Why both are “correct” for their loaders

Use IO is **commodity × industry** (`U[com, ind]`).

### useeior

Intersection allocations are applied **directly** from CSV column order.
For `UseIntersection`, column 1 indexes the Use **row** and column 2 the
Use **col**—even though those columns are named `IndustryCode` /
`CommodityCode`:

```r
# useeior R/DisaggregateFunctions.R — UseIntersection branch of applyAllocation
} else if(vectorToDisagg == "UseIntersection") {
  ...
  allocPercentagesRowIndex <- 1  # IndustryCode column of CSV
  allocPercentagesColIndex <- 2  # CommodityCode column of CSV
}

# later:
rowAlloc <- allocPercentages[r, allocPercentagesRowIndex]
colAlloc <- allocPercentages[r, allocPercentagesColIndex]
...
manualAllocVector[rowAllocIndex, colAllocIndex] <- value
```

So upstream intersection rows are authored as **Use row / Use col** under
misleading industry/commodity headers. That matches RCRA workbook semantics
once plugged straight into `U`.

### Bedrock

Bedrock builds an intermediate industry×commodity weight table, then
applies with an explicit transpose into Use:

```python
# disagg_weights.py — main-standard pivot for use_intersection
use_intersection_piv = _pivot_and_align(
    use_intersection_df,
    "PercentUsed",
    "IndustryCode",   # → index  (industry)
    "CommodityCode",  # → columns (commodity)
    disagg_sectors,
    disagg_sectors,
)
```

```python
# waste_disaggregation.py — apply contract
# Use: index=commodities, columns=industries
# use_intersection: index=industry, columns=commodity
output.loc[com, ind] = orig_val * intersection_w.loc[ind, com]
```

If Bedrock kept useeior’s intersection authorship under that pivot + apply
path, every off-diagonal cell would be **transposed**. Swapping Industry ↔
Commodity **values** on intersection rows only makes on-disk codes match the
column names and the `loc[ind, com]` contract, so Bedrock and useeior land
the same mass in `U` (e.g. `U["562111", "562HAZ"] ≈ 0.0111 × orig`).

## What was not changed

- Make intersection CSV (diagonal-only; orientation moot).
- Non-intersection Use slices (column/row sums, FD, VA).
- Loader pivot args (`IndustryCode` → index, `CommodityCode` → columns).
- Electricity disaggregation (does not use this CSV).

## Practical note

Do **not** overwrite this Bedrock Use CSV with a verbatim copy of useeior’s
file without also changing the loader or apply path. The two files are
intentionally not byte-identical on intersection rows.
