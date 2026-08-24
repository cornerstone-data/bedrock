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
| Class MWh vs D0 | `_class_mwh_targets(eia_year, alloc.egrid_mwh)` vs `alloc.mwh.groupby(alloc.end_use_class).sum()` |
| Leftover T&D | `bill − gen_dollars` (equals `t_dollars + d_dollars`) |
| Nibble vs clipped | class totals below D0 target; `clipped` is purchaser-level only |
| Optional ¢/kWh | `bill / (10 × MWh)` vs Table 2.4 — check only, not D0 |

Markdown lands at `current/diagnostics/output/eia_gtd_purchaser_tables.md`.
