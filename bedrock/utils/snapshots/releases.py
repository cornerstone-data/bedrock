"""Reference-only map of release labels to snapshot SHAs.

Not imported by runtime code. Integration tests use ``.SNAPSHOT_KEY``;
diagnostics resolve baselines via ``USAConfig.snapshot_version_or_git_sha``.

Each entry's ``# config:`` comment is the stem passed to
``generate_snapshots --config_name`` when that snapshot was built. Confirm
via the `generate_snapshots workflow
<https://github.com/cornerstone-data/bedrock/actions/workflows/generate_snapshots.yml>`_.

Update the current release entry in Phase A when ``.SNAPSHOT_KEY`` changes.
Patch releases that leave ``.SNAPSHOT_KEY`` unchanged do not add entries here.
"""

# Release snapshots (GCS prefix or git SHA)
v0 = "v0"  # config: legacy GCS prefix (pre git-SHA snapshots)
v0_1 = "1bda811e0169436ae90fd356fbef512ce7518ccb"  # config: 2025_usa_cornerstone_full_model
v0_2 = "7372464249c434c9bebb172c065a4d0e3702176e"  # config: 2025_usa_cornerstone_full_model; matches .SNAPSHOT_KEY

# Intermediate snapshot SHAs (atomic configs, test fixtures — not release labels)
TEST_config_default = "2ebb51f7190c3a62b5d8b2420bff9b20f57282fc"  # config: 2025_usa_cornerstone_full_model
TEST_fbs_schema = "9fe22d9afdfdb6806397b2356eb3cf4c4c346744"  # config: 2025_usa_cornerstone_fbs_schema
