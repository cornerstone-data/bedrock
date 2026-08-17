# trade_data

Scratch workspace for international trade data source evaluation ([bedrock#527](https://github.com/cornerstone-data/bedrock/issues/527)) ahead of nowcast trade integration (#526 / #528).

Lives at `bedrock/analysis/nowcasting/trade_data/`. Notes and probes stay here until implemented under `bedrock/extract` / FBA–FBS.

**Targets are SUT columns**: Use `F04000` for exports, Supply `MCIF` / `MADJ` / `MDTY` for imports. `F05000` is MUT-only and is not a target — see the retarget note atop [`trade_data_source_options_527.md`](trade_data_source_options_527.md).

| File | Contents |
|---|---|
| [`plan_527_long.md`](plan_527_long.md) | Intended plan (with annual-summary IO note and 2017 `compare()` results) |
| [`trade_data_source_options_527.md`](trade_data_source_options_527.md) | Full options writeup + 2017 validation findings |
| [`probe_2017_trade_totals.py`](probe_2017_trade_totals.py) | 2017 national Census+BEA FBA totals vs the SUT targets and ITA (sanity; not the 2–3% bar) |
| [`score_2017_trade_detail.py`](score_2017_trade_detail.py) | 2017 nowcast `F04000` / `MCIF` scorecard (national %, Pearson, Jaccard, `MISS` holes) |

### Working decision

**Option 1 (Census goods + BEA services)** is the primary extract. **ITA** controls national goods+services totals. USEEIO concordances for Detail mapping; Use/MCIF for 2017 truth and specials. **BACI is out of scope.**

The #557 commodity bars are scored on the **nowcast columns** (`derive_initial_Y_pur` `F04000` after the `S00900` identity; `derive_initial_supply_bridge` `MCIF`), not on unmapped FBA totals and not on `TableMatch.ok()` for the full FD/bridge blocks. Scorecard and hole rules: [`transform/trade/README.md`](../../../transform/trade/README.md).

### Probes

Requires local 2017 `Census_USATrade`, `BEA_IEA`, and `BEA_ITA` FBAs, plus NIPA FD / Trade FBS inputs for the nowcast scorer.

```powershell
uv run python -m bedrock.analysis.nowcasting.trade_data.probe_2017_trade_totals
uv run python -m bedrock.analysis.nowcasting.trade_data.score_2017_trade_detail
```

Writes under `bedrock/analysis/nowcasting/trade_data/output/` (gitignored).
