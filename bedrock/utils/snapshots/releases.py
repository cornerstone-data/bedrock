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
The ``# Output change:`` comment above an entry names the work that made the
snapshot necessary, so a reader can tell releases apart without digging through
``git log --follow`` on ``.SNAPSHOT_KEY`` and the bump PR bodies. Entries marked
baseline-only exist for diagnostics comparison and were never pinned as the
current snapshot.

Update the current release entry in Phase A when ``.SNAPSHOT_KEY`` changes.
Patch releases that leave ``.SNAPSHOT_KEY`` unchanged do not add entries here.

``EF_DOLLAR_YEAR_BY_SNAPSHOT_KEY`` records the dollar year of ``B`` (and thus
``D`` / ``N``) intensities in each snapshot. Diagnostics rebase snapshot EFs
from that year to the live ``model_base_year``. Update the map in the same
Phase A change that adds a new snapshot key.
"""

# Release snapshots (GCS prefix or git SHA)
v0 = "v0"  # config: legacy GCS prefix (pre git-SHA snapshots)

# Baseline-only label, never pinned as .SNAPSHOT_KEY.
v0_1 = "1bda811e0169436ae90fd356fbef512ce7518ccb"  # config: 2025_usa_cornerstone_v0_2

# Output change: cornerstone FBS schema comparison retargeted to main (#307).
v0_2 = "7372464249c434c9bebb172c065a4d0e3702176e"  # config: 2025_usa_cornerstone_v0_2

# Output change: y_nab derived from Adom and scaled_q, replacing disaggregated
# BEA Y/trade with negatives clipped (#458).
v0_3_0_alpha = (
    "4d67c8f0f5721a30ce03f4d3eef85a82e7199032"  # config: 2025_usa_cornerstone_v0_2
)

# Baseline-only label, never pinned as .SNAPSHOT_KEY.
v0_3_beta = (
    "5a90baf0272fe8841e40db8cd513885b34051e86"  # config: 2025_usa_cornerstone_v0_3
)

# Output change: many-to-one industry x expand (#513).
v0_3_0 = "c60bdf4308cb660eee80a246214901cff9122820"  # config: 2025_usa_cornerstone_v0_3

# Output change: waste Use-intersection orientation fix (#563).
v0_3_1 = "00524c3c8ba122a7a5b7f2139ff7ea6de08947bb"  # config: 2025_usa_cornerstone_v0_3

# Output change: MECS Energy methodology in Cornerstone GHG (#688), ported to the
# nowcast GHG configs (#858). Moves manufacturing B; largest cell CO2 @ 325320.
v0_3_2 = "7d0cb92af43882ee9496b5932e1893bb9ffcbdd7"  # config: 2025_usa_cornerstone_v0_3

# Output change: detail IO read from the 2024 nowcast MUT (after redefinition,
# build v0.3.0_4276083) instead of published BEA 2017 detail; x and q are that
# Make's row and column sums, B is no longer year-scaled or inflated, and the
# GHG FBS is attributed on nowcast Use (#880).
v0_4_0 = "2fcbd68b3275cc8e409d4df5d1f28a3a8355c249"  # config: 2025_usa_cornerstone_v0_4

# Alias for the shipped v0.4.0 SHA (pre waste match_io Flip) — same as v0_4_0.
v0_4_0_pre_waste_match_io = v0_4_0

# Output change: waste Flip — match_io year-aligned shares, prior-weighted SAS
# Table 2/3 suppression recovery, fail-loud AIES on match_io, and AIES-only
# industry-mix chain after SAS→AIES share-seam grade ≥ 3 pp (#1052).
v0_4_1 = "0d26d14f61a86e5f2c9c4fd9b7b2d23e63f203a0"  # config: 2025_usa_cornerstone_v0_4

# Output change: nowcast build v0.3.0_3096818 (AIES-chained gross output, theta = 1,
# electricity on EIA, smoothed manufacturing expense seed; #1014, #1030, #1056,
# pinned by #1057), facility GHG attribution with the release FBS
# GHG_national_Cornerstone_nowcast_facilities_2024_v0.3.0_3a1dddc (#965, #1023,
# #1055, #1059; GHGRP sector floor and ceiling, still gas, hydrogen and LNG
# carve-outs, #1061), margin impacts by transaction (#1027), waste re-split (#1058).
v0_5_0 = "3a1dddc2030e5793b929888c1343cf5a14729f4c"  # config: 2025_usa_cornerstone_v0_5

# Output change: transportation GHG allocation (#1096) with VIUS construction
# business types mapped to BEA structure codes in
# GHG_national_Cornerstone_nowcast_facilities_transportation_2024_v0.5.0_82e24cd
# (#1103, #1105). Facilities rung is G5; transport is FINAL.
v0_6_0 = "e10c75f542b0c9b24692f6524a53073e091d54dc"  # config: 2025_usa_cornerstone_v0_6

# Output change: canonical flip to 2026_usa_cornerstone_v1_0 retained-activity
# FBS (#1147); twin bag vs e10c75f5 on asserted B/A/q/Y. Tagged v1.0.0-rc.
v1_0_0_rc = (
    "326ae5d8922ad69347c360314be541935a374a4b"  # config: 2026_usa_cornerstone_v1_0
)

# Output change: the v1.0 configs load the nowcast MUT v1.0.0_5514c3b (#1170):
# 2017-2024 rebuilt on the 2026 BEA annual update (#1166, #1167), the Step 6
# commodity identity (#1162), the rest-of-world adjustment row (#1171),
# interpolated suppressed BEA services trade cells (#1172) and Census trade on
# the 2024 product-to-NAICS concordance (#1175). The GHG and Energy FBS are
# rebuilt on that MUT, GHG_national_Cornerstone_{year}_v1.0.0_79273ed (#1189).
v1_0_0_pre_electricity_flags = (
    "7f0bdab6e7d12f17a9c2a61437a22af1df7bed30"  # config: 2026_usa_cornerstone_v1_0
)

# Output change: the v1.0 configs load the nowcast MUT v1.0.0_b17908f with the
# electricity flags on (#1205): electric output on EIA retail revenue (#1198),
# the electricity accounting residual and the hold on its gas (#1199), repriced
# petroleum and oil and gas seeds (#1187), and the trade fixes (#1179, #1180).
# GHG_national_Cornerstone_{year}_v1.0.0_edf6e0e is rebuilt on that MUT, with
# the non-energy use split from #1201.
v1_0_0_pre_406 = (
    "c9b69d94a20aecc8d858738eb87093e26e612dbe"  # config: 2026_usa_cornerstone_v1_0
)

# Output change: the v1.0 configs load the nowcast MUT v1.0.0_8365fa4, the
# 2017-2024 rebuild on GitHub Actions (#1286): gold left out of Census trade
# (#1275), Step 7 closing Use rows on same-sign changes (#1277), every Step 5
# flow-by-sector table built at the commit (#1278) and air freight on the AIES
# level (#1274). Electricity delivery is its own sector, 42ELEC (406, #1283).
# GHG_national_Cornerstone_{year}_v1.0.0_8365fa4 is rebuilt on that MUT.
v1_0_0 = "7533da280bfc9e943065306ba37630228046713d"  # config: 2026_usa_cornerstone_v1_0; matches .SNAPSHOT_KEY

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
    v0_3_1: 2024,
    v0_3_2: 2024,
    v0_4_0: 2024,
    v0_4_0_pre_waste_match_io: 2024,
    v0_4_1: 2024,
    v0_5_0: 2024,
    v0_6_0: 2024,
    v1_0_0_rc: 2024,
    v1_0_0_pre_electricity_flags: 2024,
    v1_0_0_pre_406: 2024,
    v1_0_0: 2024,
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
