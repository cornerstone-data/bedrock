"""Release labels to snapshot SHAs.

Imported by ``bedrock.utils.validation.diagnostics_baseline`` so
``generate_diagnostics --baseline v0.2`` (etc.) can resolve labels to SHAs.
Integration tests still prefer ``.SNAPSHOT_KEY`` for “current snapshot”
identity; diagnostics runs select a baseline via CLI / workflow, which
overrides ``USAConfig.snapshot_version_or_git_sha`` for that process.

Each entry's ``# config:`` comment is the stem passed to
``generate_snapshots --config_name`` when that snapshot was built. Confirm
via the `generate_snapshots workflow
<https://github.com/cornerstone-data/bedrock/actions/workflows/generate_snapshots.yml>`_.

Update the current release entry in Phase A when ``.SNAPSHOT_KEY`` changes.
Patch releases that leave ``.SNAPSHOT_KEY`` unchanged do not add entries here.

``EF_DOLLAR_YEAR_BY_SNAPSHOT_KEY`` records the dollar year of ``B`` (and thus
``D`` / ``N``) intensities in each snapshot. Diagnostics rebase snapshot EFs
from that year to the live ``model_base_year``. Update the map in the same
Phase A change that adds a new snapshot key.
"""

# Release snapshots (GCS prefix or git SHA)
v0 = "v0"  # config: legacy GCS prefix (pre git-SHA snapshots)
v0_1 = "1bda811e0169436ae90fd356fbef512ce7518ccb"  # config: 2025_usa_cornerstone_v0_2
v0_2 = "7372464249c434c9bebb172c065a4d0e3702176e"  # config: 2025_usa_cornerstone_v0_2
v0_3_0_alpha = (
    "4d67c8f0f5721a30ce03f4d3eef85a82e7199032"  # config: 2025_usa_cornerstone_v0_2
)
v0_3_beta = (
    "5a90baf0272fe8841e40db8cd513885b34051e86"  # config: 2025_usa_cornerstone_v0_3
)
v0_3_0 = "c60bdf4308cb660eee80a246214901cff9122820"  # config: 2025_usa_cornerstone_v0_3; matches .SNAPSHOT_KEY

# Intermediate snapshot SHAs (atomic configs, test fixtures — not release labels)
TEST_config_default = (
    "2ebb51f7190c3a62b5d8b2420bff9b20f57282fc"  # config: 2025_usa_cornerstone_v0_2
)
TEST_fbs_schema = "9fe22d9afdfdb6806397b2356eb3cf4c4c346744"  # config: 2025_usa_cornerstone_fbs_schema

# Dollar year of B/D/N in each snapshotted model used as a diagnostics baseline.
EF_DOLLAR_YEAR_BY_SNAPSHOT_KEY: dict[str, int] = {
    v0: 2023,
    v0_1: 2023,
    v0_2: 2023,
    v0_3_0_alpha: 2023,
    v0_3_beta: 2024,
    v0_3_0: 2024,
    TEST_config_default: 2023,
    TEST_fbs_schema: 2023,
}


def ef_dollar_year_for_snapshot(key: str) -> int:
    """Return the EF denominator dollar year for a GCS snapshot key."""
    try:
        return EF_DOLLAR_YEAR_BY_SNAPSHOT_KEY[key]
    except KeyError as exc:
        known = ', '.join(sorted(EF_DOLLAR_YEAR_BY_SNAPSHOT_KEY))
        raise ValueError(
            f'No EF dollar year registered for snapshot key {key!r}. '
            f'Add it to EF_DOLLAR_YEAR_BY_SNAPSHOT_KEY in '
            f'bedrock.utils.snapshots.releases (known: {known}).'
        ) from exc
