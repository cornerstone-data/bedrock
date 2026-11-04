# GHG model analysis

Tools and notes on how the GHG model assigns emissions to sectors, and on what
facility-reported data could change.

| File | Use it to |
|---|---|
| [`pipeline_diagram.py`](pipeline_diagram.py) | Draw the GHG model as one picture, from sources to emission factors, as `images/ghg_pipeline.png`. Shows the v0.5 method |
| [`About_facility_emissions_basis.md`](About_facility_emissions_basis.md) | Read what happens if the sector split for industrial emissions comes from facilities that reported them. Dated findings |
| [`fbs_vs_ghgrp_totals.py`](fbs_vs_ghgrp_totals.py) | Compare a sector's emissions in a GHG FBS with what its facilities report to GHGRP |
| [`facility_data_coverage.py`](facility_data_coverage.py) | See how much of a sector the facility data covers |
| [`mode_policy_scorecard.py`](mode_policy_scorecard.py) | Compare the facility attribution mode policies (#1040) |
| [`nei_combustion_proxy_test.py`](nei_combustion_proxy_test.py) | Test NEI pollutants as stand-ins for combustion CO2 (#919) |

Each script's opening docstring has its run command.

These scripts still depend on
[`time_series_B_matrix`](../time_series_B_matrix/README.md).
`fbs_vs_ghgrp_totals.py` and `facility_data_coverage.py` import
`B_change_diagnostics.py` and read its span cache. Those two and
`mode_policy_scorecard.py` write to `time_series_B_matrix/output/`. The facility
diagnostics D14 to D16 also remain in `B_change_diagnostics.py`, behind its
`--facility-data` option.
