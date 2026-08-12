# trade_data

Scratch workspace for international trade data source evaluation ([bedrock#527](https://github.com/cornerstone-data/bedrock/issues/527)) ahead of nowcast trade integration (#526 / #528).

Lives at `bedrock/analysis/nowcasting/trade_data/`. Notes and probes stay here until implemented under `bedrock/extract` / FBA–FBS.

**Targets are SUT columns**: Use `F04000` for exports, Supply `MCIF` / `MADJ` / `MDTY` for imports. `F05000` is MUT-only and is not a target — see the retarget note atop [`trade_data_source_options_527.md`](trade_data_source_options_527.md).

| File | Contents |
|---|---|
| [`plan_527_long.md`](plan_527_long.md) | Intended plan (with annual-summary IO note and 2017 `compare()` results) |
| [`trade_data_source_options_527.md`](trade_data_source_options_527.md) | Full options writeup + 2017 validation findings |
| [`probe_2017_trade_totals.py`](probe_2017_trade_totals.py) | 2017 national Census+BEA FBA totals vs the SUT targets and ITA |

### Working decision

**Option 1 (Census goods + BEA services)** is the primary extract. **ITA** controls national goods+services totals. USEEIO concordances for Detail mapping; Use/MCIF for 2017 truth and specials. **BACI is out of scope.**

### Probes

Requires local 2017 `Census_USATrade`, `BEA_IEA`, and `BEA_ITA` FBAs. Commodity-level scoring is `Trade_Exports_2017` / `Trade_Imports_2017` FBS vs Use `F04000` / Supply `MCIF`.

```powershell
uv run python -m bedrock.analysis.nowcasting.trade_data.probe_2017_trade_totals
```

Writes under `bedrock/analysis/nowcasting/trade_data/output/` (gitignored).
