"""Electricity disaggregation diagnostics.

Analyses live in subpackages that group scripts with the questions they answer:

- ``bly_dispersion`` — chained BLy dispersion / net-change waterfalls vs v0.2
- ``ef_comparison`` — N/D percent-diff plots and N-variance decomposition vs footing
- ``full_trace`` — live model IO / E / D / N / BLy walkthrough across the config chain
- ``year_alignment`` — BLy vs E under A/q year handling
- ``hh_vs_interindustry`` — household vs intermediate generation MWh drivers
- ``probes`` — one-off sector probes (e.g. 221200)

Shared infrastructure (``paths``, ``manifest``, ``local_data``) stays at package root.
Outputs remain under ``output/`` (layout unchanged by reorganization).
"""
