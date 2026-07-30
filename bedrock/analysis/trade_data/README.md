# trade_data

Scratch workspace for international trade data source evaluation ([bedrock#527](https://github.com/cornerstone-data/bedrock/issues/527)) ahead of F04000/F05000 nowcast integration (#526 / #528).

Lives at `bedrock/analysis/trade_data/`. Notes and probes stay here until implemented under `bedrock/extract` / FBA–FBS.

| File | Contents |
|---|---|
| [`plan_527_long.md`](plan_527_long.md) | Intended plan (with annual-summary IO note and 2017 `compare()` results) |
| [`trade_data_source_options_527.md`](trade_data_source_options_527.md) | Full options writeup + 2017 validation findings |
| [`probe_2017_trade_totals.py`](probe_2017_trade_totals.py) | 2017 national Census+BEA totals vs Use / MCIF / ITA |
| [`probe_2017_trade_detail.py`](probe_2017_trade_detail.py) | 2017 BEA Detail vectors vs F040/F050 (Pearson / coverage) |
| [`probe_2017_trade_compare.py`](probe_2017_trade_compare.py) | Same Detail extract vs Use F040/F050 via `compare_NIPA_to_IOT.compare()` (matched vs unmatched) |

### Working decision

**Option 1 (Census goods + BEA services)** is the primary extract. **ITA** controls national goods+services totals. USEEIO concordances for Detail mapping; Use/MCIF for 2017 truth and specials. **BACI is out of scope.**

### Probes

Requires `Census` and `BEA` in `bedrock/extract/API_Keys.env`.

```powershell
uv run python -m bedrock.analysis.trade_data.probe_2017_trade_totals
uv run python -m bedrock.analysis.trade_data.probe_2017_trade_detail
uv run python -m bedrock.analysis.trade_data.probe_2017_trade_compare
```

Writes under `bedrock/analysis/trade_data/output/` (gitignored).
