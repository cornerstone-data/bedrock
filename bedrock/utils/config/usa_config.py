from __future__ import annotations

import os
import typing as ta

import pandas as pd
import yaml
from pydantic import BaseModel, Field, model_validator

CONFIG_DIR = os.path.join(os.path.dirname(__file__), 'configs')
USA_CONFIG_ENV_VAR = 'USA_CONFIG_FILE'
CANONICAL_USA_CONFIG = '2025_usa_cornerstone_v0_4'

# Stems with no yaml under configs/. Historical EF sheets / combine keys may
# still use these strings (e.g. CEDA_V0_BASELINE); load/run is not supported.
# Compare against frozen GCS snapshot key ``v0`` via snapshot_version_or_git_sha.
RETIRED_USA_CONFIG_STEMS: frozenset[str] = frozenset(
    {
        'v8_ceda_2025_usa',
        '2025_usa_ceda_ghg_from_flowsa',
    }
)

BEA_PUBLISHED_DETAIL_IO_YEARS: frozenset[int] = frozenset({2012, 2017})
NOWCAST_IO_YEARS: frozenset[int] = frozenset(
    {2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024}
)
NowcastDetailIoYear = ta.Literal[2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]

DIAGNOSTICS_CLI_OVERRIDE_KEYS: frozenset[str] = frozenset(
    {
        'diagnostics_baseline_source',
        'snapshot_version_or_git_sha',
        'useeio_baseline_xlsx_gs_uri',
        'useeio_baseline_xlsx_sha256',
        'useeio_model_version_label',
        'model_base_year',
        'usa_ghg_data_year',
    }
)


class USAConfig(BaseModel):
    #####
    # Model base settings
    #####
    model_base_year: ta.Literal[2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024] = 2023
    bea_io_level: ta.Literal['detail', 'summary'] = 'detail'
    bea_io_scheme: ta.Literal[2017, 2022] = 2017  # documentation purposes
    price_type: ta.Literal['producer', 'purchaser'] = 'producer'
    iot_before_or_after_redefinition: ta.Literal['before', 'after'] = 'after'

    #####
    # Data selection
    #####
    usa_detail_io_source: ta.Literal['bea_published', 'nowcast'] = 'bea_published'
    nowcast_mut_vintage: ta.Optional[str] = Field(
        default=None,
        description=(
            'Artifact build label for nowcast BEA-detail MUT tables on GCS '
            '(e.g. v0.3.0_16f96b1). When omitted and usa_detail_io_source is '
            'nowcast, loaders pick the most recently uploaded Make parquet for '
            'the configured year and redefinition stage.'
        ),
    )
    usa_base_io_data_year: ta.Literal[
        2012, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024
    ] = 2017  # BEA benchmark year (bea_published) or IO calendar year (nowcast)
    usa_io_data_year: ta.Literal[2017, 2022, 2023, 2024] = (
        2022  # CEDA's legacy USA IO data year
    )
    usa_ghg_data_year: ta.Literal[2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024] = 2023

    ipcc_ar_version: ta.Literal['AR5', 'AR6'] = 'AR6'

    #####
    # Methodology selection
    #####
    ### IO Methodology selection
    # "IO year adjustments" bucket: CEDA A/q scaling to usa_io_data_year with
    # summary dollar-year rebase, bedrock-derived industry inflation factors,
    # and gross output at usa_ghg_data_year as B's denominator x.
    apply_io_year_adjustments: bool = False
    use_E_data_year_for_x_in_B: bool = Field(
        default=False,
        description=(
            'Deprecated USEEIO-parity compatibility flag: GHG-year x in B '
            'without the rest of the IO-year adjustments. Superseded by '
            'apply_io_year_adjustments; kept for configs on the pre-v0.3 A '
            'footing until the USEEIO-recreation removal.'
        ),
    )
    deflate_x_to_detail_io_year_for_B: bool = Field(
        default=False,
        description=(
            'Deflate BEA gross-output industry x at usa_ghg_data_year to '
            'usa_detail_original_year chain dollars before E/x in B '
            '(derive_cornerstone_B_via_vnorm). Requires '
            'use_E_data_year_for_x_in_B to be true.'
        ),
    )
    implement_waste_disaggregation: bool = False  # DRI: jorge.vendries
    # Waste weight vintage: 2017 = bundled CSVs (default/production);
    # match_io = usa_base_io_data_year; int = explicit year (diagnostic OK).
    waste_weights_year: ta.Literal['match_io'] | int | None = (
        2017  # DRI: jorge.vendries
    )
    implement_electricity_reallocation: bool = False  # DRI: jorge.vendries
    implement_electricity_disaggregation: bool = False  # DRI: jorge.vendries
    implement_electricity_mixed_units: bool = False  # DRI: jorge.vendries
    implement_electricity_reaggregation: bool = False  # DRI: jorge.vendries
    # Issue #1008 — constrain 221100×F01000 in nowcast GRAS. F01000 is Tier 2,
    # so without this the balance uses residential electricity as a residual
    # sink: -$12.3bn in 2022 and +$5.2bn in 2023 against EIA's published
    # residential revenue. `eia_band` collars the level to EIA and wins the
    # full-span 2017-2024 grade on the ship arm (Step-5-weighted EIA miss
    # $2.18bn against $4.41bn for the other two modes; trade displacement
    # $0.08bn); see About_1008_pce_electricity_pin.md.
    # ON by default: graded with rebase_utility_gross_output_on_eia OFF, which
    # is how production ships, so the two are not co-enabled unattributed.
    constrain_electricity_pce_cell: bool = True  # DRI: jorge.vendries
    electricity_pce_constraint_mode: ta.Literal[
        'none', 'tier1_fixed', 'row_side_target', 'eia_band'
    ] = 'eia_band'  # DRI: jorge.vendries
    # Rebase 221100 gross output on EIA volume x published price, splitting the
    # 2017 base into output sold to ultimate customers (moved on EPA Table 2.3
    # revenue) and sales for resale between utilities (moved on Table 8.3
    # purchased power). BEA's UGO305-A implied price per kWh runs 14.7% over
    # EIA's published average in 2022 and 2.2% under it in 2024, having agreed
    # to 0.2% at the 2017 benchmark. Also releases the published summary Supply
    # cell for summary group 22 so the reduction is not handed to gas
    # distribution and water. See
    # bedrock.transform.iot.eia_utility_go_adjustment (#1009).
    # ON by default for v0.5 (Wes, 2026-09-28): with it off, Steps 3-7 ran on
    # BEA's gross output, whose 2022 spike and 2023 fall are the supply-side
    # collapse behind #896 (221100 intermediate 2022->23: -$67.7bn off, -$28.0bn
    # on). It also moves the 221100 price carry onto EIA's retail price (see
    # nowcast_intermediate.commodity_price_factor). The PCE constraint's
    # grading already covered this arm: eia_band wins it too ($2.52bn weighted
    # EIA miss against $4.06bn), see About_1008_pce_electricity_pin.md.
    rebase_utility_gross_output_on_eia: bool = True  # DRI: WesIngwersen
    # Chain manufacturing detail gross output for 2023-24 on AIES receipts
    # instead of BEA's own annual movement. BEA's detail stops tracking census
    # after the 2022 Economic Census: measured against each industry's own 2017
    # ratio to census, the median drift is 2.3% in 2022 but 7.9% in 2024, with
    # $387bn of gross misallocation netting to only -$35bn. Aircraft grows 8.2%
    # on BEA in the year Boeing's deliveries fell 528 -> 348, where census shows
    # -8.8%. Holds the manufacturing TOTAL rather than each summary group,
    # because $250bn of the $387bn sits between groups. See
    # bedrock.transform.iot.aies_go_chaining (#1013).
    # ON by default: 2024 is the release year, BEA's 2023-24 manufacturing
    # detail carries $387bn of mix error, and BEA states it could not use AIES
    # (SCB 2026-06 preview). Unlike the other flags here this is a correction we
    # believe rather than an option we are trialling, so it ships enabled.
    chain_manufacturing_on_aies: bool = True  # DRI: WesIngwersen
    # Chain the services/transport expense seed across the SAS -> AIES survey
    # change instead of indexing AIES years against a SAS 2017 base: 2023 takes
    # 2022's (SAS) index and 2024 moves it by AIES's own 2024/2023 relative
    # index, so every ratio stays inside one survey. Indexing AIES against SAS
    # (the 2026-08-25 decision, #742) broke at the seam on two counts: item
    # levels jump beyond their CVs (management consulting's electricity x3.1 at
    # an 11% CV, back down 19% in 2024), and AIES publishes neither SAS's
    # "All other operating expenses" nor a comparable one, so the relative
    # index's denominator changes composition. For electricity that put the
    # 2023 Step 3 row $63bn above its supply-side target, and the interior fit
    # cut every buyer ~16% to close it (trade -24%). See
    # bedrock.analysis.nowcasting.services_transport_expense_seed.
    chain_services_seed_across_aies: bool = True  # DRI: WesIngwersen
    # Smooth each manufacturing industry's share of the expense seed's survey
    # total across the survey changes (census 2017 -> ASM 2018-21 -> census
    # 2022 -> AIES): ASM shares benchmarked to both censuses, a 3-year centred
    # geometric mean on the ASM years, and AIES years chained on AIES's own
    # change from the 2022 census share. The total keeps its raw path (2020 dip,
    # 2022 price peak) and census years stay exact. Every kind but electricity.
    # The raw path passed each instrument switch and withheld-cell fill into the
    # Use table's fuel rows (#1053: 33641A fuel 1.9, 11.0, 2.3 $M 2017-19). See
    # bedrock.analysis.nowcasting.inputs_structure.smoothed_kind_block.
    smooth_manufacturing_expense_path: bool = True  # DRI: WesIngwersen
    # Move data processing and hosting's (518200) electricity on LBNL's
    # national data center electricity series instead of the services survey,
    # priced on EIA's commercial price, and hold the cell through the interior
    # fit and GRAS. The survey cell (Service Annual Survey 2020-22, AIES 2023-24)
    # has 518200 buying ~37% fewer kWh in 2024 than 2017 while LBNL has data
    # centers at 2.8x. Overrides the survey observation on this one cell. See
    # nowcast_intermediate.DATA_CENTER_ELECTRICITY_CSV and #1035.
    move_data_processing_electricity_on_lbnl: bool = True  # DRI: WesIngwersen
    # Hold electricity, utility gas, refined petroleum, coal and oil and gas
    # extraction at theta = 1 in the Step 3 price carry instead of at the
    # fitted two-regime default. theta = 1 freezes the REAL input mix; theta =
    # 0 freezes the nominal share, which asserts a real quantity cut equal to
    # the price rise. For commodities an industry cannot do without that cut
    # did not happen, and the model books it as structural change. With the
    # theta = 1 prior now the default this is a no-op in effect; it is kept as a
    # GUARD, so that a future evidenced departure below 1.0 for some other
    # commodity cannot silently drag these rows down with it. Against the
    # retired two-regime rule it was worth +13.7% on these rows at 2022.
    # MECS 2018->2022 fits 0.976 and 1.091 by two
    # independent routes on manufacturing, the most substitutable case, so 1.0
    # is a lower bound for the locked sectors this actually reaches. The
    # observed mask confines it to the $279.6bn no survey answered - 31%
    # government, 15% construction, 5% transport. See
    # bedrock.transform.iot.nowcast_intermediate.INDISPENSABLE_COMMODITIES
    # (#891, #997).
    # ON by default: the alternative is not a neutral prior but the single
    # setting that most manufactures structural change, chosen on a 0.587%
    # score difference measured on BEA's published summary panel - which the
    # fitting harness reads instead of our seed, and so can never see this.
    carry_indispensable_commodities_in_full: bool = True  # DRI: WesIngwersen
    # Restore the retired two-regime theta fitted on BEA's published summary
    # panel (0.75 off the 2021-22 price surge, 0.0 across it) instead of the
    # theta = 1 prior the build now uses. OFF by default: the fit's headline
    # predictor is 96.2% collinear with "the target year's panel incorporates
    # neither the 2022 Economic Census nor AIES 2023/24" - 75 of 78 spans are
    # classified identically - so its R2 0.613 cannot separate substitution from
    # a panel that stopped taking in source data, and all 30 surge-crossing
    # spans end inside that region. Energy prices also reversed after 2022
    # (petroleum 1.945 -> 1.431 against 2017) while the penalty for theta = 1
    # doubled, which no price mechanism predicts but BEA's own drift against
    # census does (2.3/6.1/7.9%, #1013). Keep it available: on the summary panel
    # taken at face value the retired rule scores better, so the choice should
    # be re-runnable rather than only argued. See
    # bedrock.transform.iot.nowcast_intermediate.default_theta (#699, #891, #997).
    use_fitted_summary_regime_theta: bool = False  # DRI: WesIngwersen
    # Move the census-held scrap cells (S00401) of the steel and aluminum
    # buyers from 2022 on the BLS PPIs (WPU1012 iron and steel scrap,
    # WPU102302 aluminum base scrap) for years after the 2022 Economic Census.
    # The census seed holds 2022 dollars past 2022, near the scrap price peak
    # (steel scrap 0.844 of 2022 by 2024), so without this the scrap share of
    # these buyers' inputs stays at the peak. Because the column total is
    # fixed, the other inputs' shares rise instead, which raises these buyers'
    # indirect EF. No effect through 2022. See
    # bedrock.transform.iot.nowcast_intermediate SCRAP_PPI_BY_BUYER (#768).
    carry_held_scrap_on_ppi: bool = True  # DRI: WesIngwersen
    # Set the paper buyers' scrap cells (S00401, wastepaper) from measured
    # quantity x price in every year 2018-2024: BEA's 2017 value times US
    # recovered paper consumption (FAOSTAT, tracking AF&PA) times the BLS
    # recyclable-paper PPI (WPU0912), both relative to 2017, as a share of the
    # year's intermediate total. The census measures metal scrap only, so
    # these cells are BEA's 2017 values; they were wrongly flagged
    # census-observed and held at 2017 dollars through 2024. Consumption stays
    # within 0.94-1.00 of 2017 while the price runs 0.45-1.08, so price alone
    # would book a price swing as a swing in the mill's scrap intensity. See
    # bedrock.transform.iot.nowcast_intermediate BENCHMARK_SCRAP_PPI_BY_BUYER.
    set_paper_scrap_from_recovered_paper: bool = True  # DRI: WesIngwersen
    scale_a_matrix_with_useeio_method: bool = False  # DRI: mo.li
    # USEEIO-parity margins (useeior Rho/CPI path); anchors the USEEIO-baseline
    # release-waterfall chain (v03_waterfall_useeio_g1_schema_ghg).
    useeio_margins: bool = False  # DRI: WesIngwersen
    cornerstone_industry_avg_margins: bool = False  # DRI: WesIngwersen
    # Build the margin-impact matrix (A_margin, behind M_margin and N_margin)
    # from the margin commodity each transaction actually paid, read from the
    # nowcast Margins table's per-margin-commodity columns, instead of spreading
    # each margin type (transport, wholesale, retail) over its margin
    # commodities by their share of total output (useeior's table average).
    # Totals per margin type are unchanged; only the split within a type moves.
    # No effect with usa_detail_io_source 'bea_published', whose Margins table
    # has the three type columns only. Uses the same margin filters as
    # cornerstone_industry_avg_margins / useeio_margins select (#836).
    margin_impacts_from_transaction_sectors: bool = True  # DRI: WesIngwersen
    # Exponent on the census materials index in the Step 3 materials and mining
    # seeds: seed = Use2017 * (census_mix_t / census_mix_2017) ** alpha. 1.0 is
    # the full census movement; below 1 pulls the mix toward BEA's benchmark.
    # On the 2012 -> 2017 benchmark holdout (manufacturing, common support,
    # impact-weighted) the full index is 66.4% worse than a frozen 2012 mix,
    # 0.75 is 28.5% worse and 0.5 is 10.6% worse; only 0.1 breaks even
    # (+0.5%). BEA's own cell movement runs 0.04x the census's (#988). These
    # are the figures `python -m
    # bedrock.analysis.nowcasting.ec2012_matfuel_concordance --check` prints
    # and asserts. Ships at 1.0, so this week's builds are unchanged; 0.8 is
    # an evaluation build only, see
    # configs/2025_usa_cornerstone_v0_4_materials_alpha_0_8.yaml.
    census_materials_index_alpha: float = 1.0  # DRI: WesIngwersen
    ### GHG Methodology selection
    # "GHG model allocation" bucket: Cornerstone GHG FBS (pre-built parquet at
    # usa_ghg_data_year) vs the legacy CEDA-methodology FBS (2023 only).
    use_cornerstone_ghg_model: bool = False
    # When True with usa_detail_io_source=nowcast, load the facility-attribution
    # GHG FBS stem (GHG_national_Cornerstone_nowcast_facilities_{year}) instead
    # of the MECS/Use nowcast stem. Ignored when the detail source is
    # bea_published.
    use_facility_ghg_attribution: bool = False

    #####
    # Diagnostics baseline (parquet snapshots vs USEEIO Excel on GCS)
    #####
    diagnostics_baseline_source: ta.Literal['gcs_snapshot', 'gcs_useeio_xlsx'] = (
        'gcs_snapshot'
    )
    useeio_baseline_xlsx_gs_uri: ta.Optional[str] = Field(
        default=None,
        description=(
            'gs://cornerstone-default/... URI for the USEEIO baseline workbook. '
            'Typically supplied via useeio_baseline_pin.json with '
            'generate_diagnostics --useeio_baseline_pin_json, or set in YAML.'
        ),
    )
    useeio_baseline_xlsx_sha256: ta.Optional[str] = Field(
        default=None,
        description=(
            'SHA-256 (64 hex chars) of the exact xlsx bytes at useeio_baseline_xlsx_gs_uri. '
            'In CI, use bedrock/utils/snapshots/useeio_baseline_pin.json with '
            'generate_diagnostics --useeio_baseline_pin_json. Required in GitHub Actions '
            "when diagnostics_baseline_source is 'gcs_useeio_xlsx'."
        ),
    )
    useeio_model_version_label: ta.Optional[str] = Field(
        default=None,
        description=(
            'Short label for config_summary / auditing. Typically set in useeio_baseline_pin.json.'
        ),
    )

    @model_validator(mode='after')
    def _validate_diagnostics_baseline(self) -> USAConfig:
        """USEEIO baseline needs a GCS URI; CI must pin the xlsx with SHA256."""
        if self.diagnostics_baseline_source == 'gcs_useeio_xlsx':
            if not self.useeio_baseline_xlsx_gs_uri:
                raise ValueError(
                    'useeio_baseline_xlsx_gs_uri is required when '
                    "diagnostics_baseline_source is 'gcs_useeio_xlsx'"
                )
            if os.environ.get('GITHUB_ACTIONS') == 'true':
                if not self.useeio_baseline_xlsx_sha256:
                    raise ValueError(
                        'useeio_baseline_xlsx_sha256 is required in GitHub Actions '
                        "when diagnostics_baseline_source is 'gcs_useeio_xlsx'"
                    )
        return self

    @model_validator(mode='after')
    def _validate_deflate_x_requires_use_e_for_x_in_b(self) -> USAConfig:
        if self.deflate_x_to_detail_io_year_for_B and not self.use_ghg_year_x_in_B:
            raise ValueError(
                'deflate_x_to_detail_io_year_for_B requires use_E_data_year_for_x_in_B '
                'or apply_io_year_adjustments to be true'
            )
        return self

    @model_validator(mode='after')
    def _validate_margins_mutual_exclusivity(self) -> USAConfig:
        if self.useeio_margins and self.cornerstone_industry_avg_margins:
            raise ValueError(
                'At most one margins flag may be true; got: '
                'useeio_margins, cornerstone_industry_avg_margins'
            )
        return self

    @model_validator(mode='after')
    def _validate_ghg_flag_compatibility(self) -> USAConfig:
        if (
            self.implement_electricity_reallocation
            and not self.implement_waste_disaggregation
        ):
            raise ValueError(
                'implement_electricity_reallocation requires '
                'implement_waste_disaggregation'
            )
        if self.implement_electricity_disaggregation and not (
            self.implement_waste_disaggregation
            and self.implement_electricity_reallocation
        ):
            raise ValueError(
                'implement_electricity_disaggregation requires '
                'implement_waste_disaggregation and implement_electricity_reallocation'
            )
        if self.implement_electricity_mixed_units and not (
            self.implement_electricity_disaggregation
        ):
            raise ValueError(
                'implement_electricity_mixed_units requires '
                'implement_electricity_disaggregation'
            )
        if self.implement_electricity_reaggregation and not (
            self.implement_electricity_disaggregation
        ):
            raise ValueError(
                'implement_electricity_reaggregation requires '
                'implement_electricity_disaggregation'
            )
        if (
            self.implement_electricity_reaggregation
            and self.implement_electricity_mixed_units
        ):
            raise ValueError(
                'implement_electricity_reaggregation is mutually exclusive with '
                'implement_electricity_mixed_units'
            )
        return self

    @model_validator(mode='after')
    def _validate_detail_io_source(self) -> USAConfig:
        if self.usa_detail_io_source == 'bea_published':
            if self.usa_base_io_data_year not in BEA_PUBLISHED_DETAIL_IO_YEARS:
                raise ValueError(
                    'usa_base_io_data_year must be 2012 or 2017 when '
                    "usa_detail_io_source is 'bea_published'; "
                    f'got {self.usa_base_io_data_year}'
                )
        elif self.usa_detail_io_source == 'nowcast':
            if self.usa_base_io_data_year not in NOWCAST_IO_YEARS:
                raise ValueError(
                    'usa_base_io_data_year must be 2017–2024 when '
                    "usa_detail_io_source is 'nowcast'; "
                    f'got {self.usa_base_io_data_year}'
                )
            if self.usa_base_io_data_year != self.model_base_year:
                raise ValueError(
                    'usa_base_io_data_year must equal model_base_year when '
                    "usa_detail_io_source is 'nowcast'; "
                    f'got usa_base_io_data_year={self.usa_base_io_data_year}, '
                    f'model_base_year={self.model_base_year}'
                )
            if self.apply_io_year_adjustments:
                raise ValueError(
                    'apply_io_year_adjustments is incompatible with '
                    "usa_detail_io_source 'nowcast'"
                )
            if self.usa_ghg_data_year != self.usa_base_io_data_year:
                raise ValueError(
                    'usa_ghg_data_year must equal usa_base_io_data_year when '
                    "usa_detail_io_source is 'nowcast' (x is the nowcast Make row "
                    'sum and exists for that year only, so E must match it); '
                    f'got usa_ghg_data_year={self.usa_ghg_data_year}, '
                    f'usa_base_io_data_year={self.usa_base_io_data_year}'
                )
            if self.deflate_x_to_detail_io_year_for_B:
                raise ValueError(
                    'deflate_x_to_detail_io_year_for_B is incompatible with '
                    "usa_detail_io_source 'nowcast' (no intermediate dollar year)"
                )
        return self

    #####
    # Baseline snapshot
    #####
    # The git SHA below is the baseline snapshots used for diagnostic comparison
    # generated on main; the config each was built from is recorded in
    # bedrock.utils.snapshots.releases.
    # Comments here carry the release label only; what changed in each snapshot is
    # recorded next to the matching constant in bedrock.utils.snapshots.releases.
    snapshot_version_or_git_sha: ta.Literal[
        'v0',
        '1bda811e0169436ae90fd356fbef512ce7518ccb',  # v0.1
        '2ebb51f7190c3a62b5d8b2420bff9b20f57282fc',  # test
        '9fe22d9afdfdb6806397b2356eb3cf4c4c346744',  # test: snapshot from 2025_usa_cornerstone_fbs_schema
        '7372464249c434c9bebb172c065a4d0e3702176e',  # v0.2
        '4d67c8f0f5721a30ce03f4d3eef85a82e7199032',  # v0.3.0-alpha (config: 2025_usa_cornerstone_v0_2)
        '5a90baf0272fe8841e40db8cd513885b34051e86',  # v0.3-beta (config: 2025_usa_cornerstone_v0_3)
        '9a47eaa1060e6900154c7b819934a8a1669461c3',  # v0.3.0 before #513 (industry-x expand fix)
        'c60bdf4308cb660eee80a246214901cff9122820',  # v0.3.0
        '00524c3c8ba122a7a5b7f2139ff7ea6de08947bb',  # v0.3.1
        '7d0cb92af43882ee9496b5932e1893bb9ffcbdd7',  # v0.3.2
        '2fcbd68b3275cc8e409d4df5d1f28a3a8355c249',  # v0.4.0 (current .SNAPSHOT_KEY)
    ] = 'v0'

    @property
    def usa_detail_original_year(self) -> NowcastDetailIoYear:
        if self.usa_detail_io_source == 'nowcast':
            return ta.cast(NowcastDetailIoYear, self.usa_base_io_data_year)
        return 2017

    @property
    def use_ghg_year_x_in_B(self) -> bool:
        """B's denominator x is gross output at ``usa_ghg_data_year``.

        Under ``usa_detail_io_source == 'nowcast'`` the B path reads x from the
        nowcast Make regardless of this flag.
        """
        return self.apply_io_year_adjustments or self.use_E_data_year_for_x_in_B

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable dictionary representation of the config.

        Nested BaseModel values are converted to plain dictionaries via
        model_dump(), so callers can safely pass this mapping to pandas or
        json libraries.
        """
        result: dict[str, object] = {}
        for field_name in self.model_fields:
            value = getattr(self, field_name)
            if isinstance(value, BaseModel):
                result[field_name] = value.model_dump()
            else:
                result[field_name] = value
        return result

    def to_dataframe(self, config_name: str) -> pd.DataFrame:
        config_dict = self.to_dict()
        config_dict_df = pd.DataFrame(
            [
                {'config_field': key, 'value': value}
                for key, value in config_dict.items()
            ]
        )
        summaries = pd.concat(
            [
                pd.DataFrame(
                    {'config_field': 'config_name', 'value': config_name}, index=[0]
                ),
                config_dict_df,
            ],
        )
        return summaries


_usa_config: ta.Optional[USAConfig] = None


def _normalize_usa_config_file_name(config_file_name: str) -> str:
    if not config_file_name.endswith('.yaml'):
        return f'{config_file_name}.yaml'
    return config_file_name


def _raise_if_retired_usa_config(config_file_name: str) -> None:
    stem = os.path.basename(
        _normalize_usa_config_file_name(config_file_name)
    ).removesuffix('.yaml')
    if stem in RETIRED_USA_CONFIG_STEMS:
        raise ValueError(
            f'USA config {stem!r} is retired and cannot be loaded. '
            "For CEDA v0 EF comparisons, pin snapshot_version_or_git_sha to 'v0' "
            'on a live Cornerstone config; do not regenerate the legacy model.'
        )


def _load_usa_config_from_file_name(config_file_name: str) -> USAConfig:
    assert config_file_name.endswith('.yaml'), 'config file name must end with .yaml'
    _raise_if_retired_usa_config(config_file_name)
    path = os.path.join(CONFIG_DIR, config_file_name)
    if not os.path.isfile(path):
        raise FileNotFoundError(
            f'USA config {config_file_name!r} not found under {CONFIG_DIR!r}'
        )
    with open(path) as f:
        data = yaml.safe_load(f)
    config = USAConfig.model_validate(data, strict=True)
    return config


def set_global_usa_config(
    config_file: str,
    *,
    diagnostics_cli_overrides: dict[str, object] | None = None,
) -> None:
    """Set the process-wide USA config from YAML.

    Args:
        config_file: Config stem or filename under ``configs/`` (``.yaml`` is
            appended if missing).
        diagnostics_cli_overrides: If set, merged onto the YAML-loaded dict
            before ``USAConfig`` validation. Keys must be a subset of
            ``DIAGNOSTICS_CLI_OVERRIDE_KEYS`` (diagnostics baseline source,
            snapshot key, USEEIO pin fields, model years). Used by
            ``generate_diagnostics`` so one run can change the comparison
            target and years without a forked config file.
    """
    global _usa_config
    config_file_env = os.environ.get(USA_CONFIG_ENV_VAR)

    if (_usa_config is not None) or (config_file_env is not None):
        raise ValueError('Global USA config already set')

    config_file = _normalize_usa_config_file_name(config_file)
    _raise_if_retired_usa_config(config_file)

    base = _load_usa_config_from_file_name(config_file)
    if diagnostics_cli_overrides:
        unknown = set(diagnostics_cli_overrides) - DIAGNOSTICS_CLI_OVERRIDE_KEYS
        if unknown:
            raise ValueError(
                f'Unknown diagnostics_cli_overrides keys: {sorted(unknown)}'
            )
        filtered = {
            k: v
            for k, v in diagnostics_cli_overrides.items()
            if k in DIAGNOSTICS_CLI_OVERRIDE_KEYS and v is not None
        }
        merged = base.model_dump(mode='python')
        merged.update(filtered)
        _usa_config = USAConfig.model_validate(merged, strict=True)
    else:
        _usa_config = base
    os.environ[USA_CONFIG_ENV_VAR] = config_file


def set_global_usa_config_object(config: USAConfig, *, source_label: str) -> None:
    """Install an already-validated ``USAConfig`` as the process-wide singleton.

    Used by analysis-only loaders (e.g. waste-disagg Phase 3 YAMLs outside
    ``CONFIG_DIR``). ``source_label`` is recorded in ``USA_CONFIG_ENV_VAR`` for
    the already-set guard / logging only — ``get_usa_config()`` must never
    re-resolve that label via ``_load_usa_config_from_file_name``.
    """
    global _usa_config
    config_file_env = os.environ.get(USA_CONFIG_ENV_VAR)

    if (_usa_config is not None) or (config_file_env is not None):
        raise ValueError('Global USA config already set')

    _usa_config = config
    os.environ[USA_CONFIG_ENV_VAR] = source_label


def get_usa_config() -> USAConfig:
    global _usa_config
    if _usa_config is None:
        env_usa_config_file = os.environ.get(USA_CONFIG_ENV_VAR)
        if env_usa_config_file:
            _usa_config = _load_usa_config_from_file_name(env_usa_config_file)
        else:
            set_global_usa_config(f'{CANONICAL_USA_CONFIG}.yaml')
    assert _usa_config is not None
    return _usa_config


def reset_usa_config(should_reset_env_var: bool = True) -> None:
    """Clear the process-wide USA config."""
    global _usa_config
    _usa_config = None
    if should_reset_env_var:
        os.environ.pop(USA_CONFIG_ENV_VAR, None)
