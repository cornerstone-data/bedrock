"""Signal-vs-noise diagnosis for the A-matrix physical-effect residual.

Pipeline (run order):

Phase A  — derive + decompose
  derive_A_snapshots          A.1  per-year A snapshots for the two LMDI methods
  compute_lmdi_phys           A.2  cell-level Q_phys + LMDI aggregation to j / NAICS-3
  plot_lmdi_phys              A.3  top-NAICS-3 stacked-bar visual

Phase B  — internal consistency
  compute_consistency_tests   B.1  lag-1 autocorr, within-NAICS-3 coherence, magnitude/shape
  plot_consistency_tests      B.2  autocorr scatter + magnitude histograms
  extract_signal_clean_naics3 B.3  pass/fail per NAICS-3 against three thresholds

Phase C  — external validation
  validate_klems              correlate Phase A against BEA-BLS KLEMS productivity

See ``signal_noise_plan.md`` for the motivating question and threshold choices.
"""
