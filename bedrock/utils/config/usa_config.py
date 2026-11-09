from __future__ import annotations

import os
import typing as ta

import pandas as pd
import yaml
from pydantic import BaseModel, Field, model_validator

CONFIG_DIR = os.path.join(os.path.dirname(__file__), 'configs')
USA_CONFIG_ENV_VAR = 'USA_CONFIG_FILE'
CANONICAL_USA_CONFIG = '2026_usa_cornerstone_v1_0'

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

# (usa_detail_io_source, usa_ghg_data_year) pairs allowed when
# implement_electricity_disaggregation is on. Year GHG methods embed eGRID.
ELECTRICITY_DISAGG_SUPPORTED_IO_YEAR: frozenset[tuple[str, int]] = frozenset(
    {
        ('bea_published', 2023),
        ('bea_published', 2024),
        ('nowcast', 2024),
    }
)

DIAGNOSTICS_CLI_OVERRIDE_KEYS: frozenset[str] = frozenset(
    {
        'diagnostics_baseline_source',
        'snapshot_version_or_git_sha',
        'useeio_baseline_xlsx_gs_uri',
        'useeio_baseline_xlsx_sha256',
        'useeio_model_version_label',
        'useeio_baseline_price_index',
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
    cornerstone_ghg_fbs_filename: ta.Optional[str] = Field(
        default=None,
        description=(
            'Exact Cornerstone GHG FlowBySector parquet, version and git hash '
            'included (for example '
            'GHG_national_Cornerstone_nowcast_facilities_2024_v0.3.0_3a1dddc'
            '.parquet). When set, the GHG loader reads that file from the '
            'local FBS directory and, if it is absent, downloads that object '
            'from gs://cornerstone-default/transform/output_data/, even when '
            'the filename stem differs from the selected method stem. A newer '
            'upload of the same stem is not a substitute. When omitted, the '
            'loader uses the newest parquet for the stem the config selects.'
        ),
    )
    bea_price_index_vintage: ta.Literal['2025Q2', '2026Q2'] = Field(
        default='2025Q2',
        description=(
            'BEA GDP-by-industry release for the industry price index. '
            'derive_industry_price_index (nowcast price carry, '
            'apply_io_year_adjustments inflation) reads its UGO304-A/UGO305-A '
            'detail and TGO104-Q summary. Every other reader (margin '
            'inflation, the diagnostics EF dollar-year adjustment) goes '
            'through obtain_inflation_factors_from_reference_data: 2025Q2 keeps the '
            'Watershed-derived BEA_PriceIndex parquet, which is not the '
            'derived 2025Q2 index; later vintages load the published index '
            'CEDA pins (PUBLISHED_PRICE_INDEX_STEMS). Gross output, '
            'intermediate inputs and value added stay on gdp.BEA_DATA_VERSION.'
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
            '(derive_B_via_vnorm). Requires '
            'use_E_data_year_for_x_in_B to be true.'
        ),
    )
    implement_waste_disaggregation: bool = False  # DRI: jorge.vendries
    # Waste weight vintage: 2017 = bundled CSVs (default when unset / non-nowcast);
    # match_io = usa_base_io_data_year. Canonical nowcast v0.4/v0.5 YAMLs set
    # match_io after Flip; int = explicit year (diagnostic OK).
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
    # With the rebase above on, move all electric gross output (221100, S00101
    # and S00202) on EIA retail revenue, and price it on EIA's average retail
    # price (revenue over kWh), so real output follows kWh sold. BEA's series
    # moves generation on sales for resale and distribution on retail revenue,
    # which counts the wholesale cycle twice: 2017 to 2022 output rises $166.8bn
    # against $94.9bn of retail revenue, although transmission plus distribution
    # equals retail revenue less generation in 2017 ($256.4bn against $257.2bn).
    # Output and price change together: retail output over BEA's price index
    # would deepen the 2021 dip in real output, to 0.880 of 2017. BEA's value
    # added stays pinned, so the lower output comes out of intermediate inputs.
    # OFF until the nowcast rebuild that bumps the 2024 balanced table pin; on
    # before that, a rebuild no longer reproduces the pinned tables. See
    # bedrock/analysis/electricity/About_electricity_modeling_record.md.
    electric_output_on_retail_revenue: bool = False  # DRI: WesIngwersen
    # Second pathway for electric power; needs the flag above. Electric non-fuel
    # inputs follow 2017 cells x input prices x EIA kWh, and what is left of BEA's
    # electric value added moves to a residual commodity and industry (S00700)
    # sold to commercial and industrial buyers. Keeps electric inputs from being
    # squeezed by pinned value added. OFF, and not to be set in a build that
    # publishes tables: Steps 6 and 7 do not read the pair yet. See
    # bedrock/analysis/electricity/About_electricity_accounting_residual.md.
    electricity_accounting_residual: bool = False  # DRI: WesIngwersen
    # Hold electric power's total purchases of oil and gas extraction (211000)
    # on BEA's 2017 total times EIA-923's gas bill relative to 2017, through the
    # interior fit and GRAS. Column controls and the fixed gas row otherwise take
    # it to $69bn in 2022 against EIA's $86bn, and $55-64bn with the oil and gas
    # seed repriced. The total is held rather than each cell because BEA's 2017
    # cells put 69% in S00202, which would exceed that column's inputs by 2022.
    # Other buyers of the row give up the difference. OFF until the rebuild that
    # bumps the 2024 balanced table pin. See
    # nowcast_intermediate.electric_gas_cells.
    hold_electric_gas_on_eia: bool = False  # DRI: WesIngwersen
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
    # Fill six-digit NAICS codes AIES withholds in one year but publishes in
    # the other from their published parent: the parent total less its
    # published six-digit codes. Without it a withheld code drops out of the
    # chain, so 325110 falls back to BEA's movement in 2023 and 325190 chains
    # on ethanol and 325194 alone. Only matters when the chain above is on.
    # See bedrock.transform.iot.aies_go_chaining.withheld_fill (#1230).
    fill_withheld_aies_codes: bool = False  # DRI: WesIngwersen
    # Keep the industries in aies_go_chaining.BEA_HOLDS on BEA's own movement
    # in the AIES chain, because their census receipts disagree with every
    # other measure. Today that is 325190: AIES 325199 receipts rise 22% from
    # 2022 to 2024, while production is flat, its price falls 4%, employment
    # falls 1% and exports fall 20%. BEA's own output rises 0.9% (#1211).
    hold_aies_outliers_on_bea: bool = False  # DRI: WesIngwersen
    # Chain gas distribution (221200) gross output for 2023-24 on census
    # receipts (NAICS 221210), moving its level. From 2022 to 2024 census
    # receipts fall 14.5% and EIA gas sales 17%, while BEA's output falls 5%;
    # business purchases of gas distribution are output less household
    # purchases, so they kept rising after 2022 while EIA's fell. Releases the
    # utilities summary group. See aies_go_chaining.UTILITY_CHAINS (#1004).
    chain_gas_distribution_on_aies: bool = False  # DRI: WesIngwersen
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
    # Chain waste industry-mix shares across the SAS → AIES survey seam when
    # the share-seam grade triggers (≥ 3 pp max |Δ| among Cornerstone waste
    # children). 2023 holds post-fill SAS 2022 shares; 2024 moves by AIES's
    # own 2024/2023 child-share ratios then renormalises. Independent of
    # chain_services_seed_across_aies. On for Flip / match_io once the grade
    # has triggered (#1052).
    chain_waste_industry_mix_across_aies: bool = True  # DRI: jorge.vendries
    # Move data processing and hosting's (518200) electricity on LBNL's
    # national data center electricity series instead of the services survey,
    # priced on EIA's commercial price, and hold the cell through the interior
    # fit and GRAS. The survey cell (Service Annual Survey 2020-22, AIES 2023-24)
    # has 518200 buying ~37% fewer kWh in 2024 than 2017 while LBNL has data
    # centers at 2.8x. Overrides the survey observation on this one cell. See
    # nowcast_intermediate.DATA_CENTER_ELECTRICITY_CSV and #1035.
    move_data_processing_electricity_on_lbnl: bool = True  # DRI: WesIngwersen
    # Move the electric power columns' refined petroleum cell (324110 into
    # 221100/S00101/S00202) on BEA's refined petroleum price instead of EIA 923's
    # oil fuel bill in the Step 3 utilities seed. BEA's 2017 cell is $17.4bn of
    # purchases against $0.57bn of oil burned at power plants, so EIA's oil leg
    # moves a cell 30 times its size on a different concept: the seeded cell went
    # 1.97x, 0.95x, 0.43x of 2017 in 2018-2020 while BEA's annual summary 324 x
    # 22 went 1.58x, 1.53x, 0.94x. On the 2012 -> 2017 benchmark holdout the
    # price leg scores +17.2% (impact) and +18.5% (dollar) against +16.0% and
    # +18.9% for EIA's oil leg, so it keeps the seed's verdict. Coal, gas and
    # purchased power stay on EIA. See
    # bedrock.analysis.nowcasting.utilities_expense_seed.oil_leg (#922).
    # OFF until the next nowcast rebuild, which turns it on and bumps the 2024
    # balanced table pin in the same change; on before that, a rebuild no longer
    # reproduces the pinned tables (test_nowcast_balanced_sut_pin).
    carry_power_petroleum_on_price: bool = False  # DRI: WesIngwersen
    # In the Step 3 mining seed, keep oil and gas extraction's (211000)
    # purchases of its own product at their real 2017 share instead of the
    # census's, and move its refined petroleum (324110) as the census's real
    # 2017 -> 2022 change times each year's relative price. The 2017 census
    # books 54.8% of the column's materials as its own product against 26.4% in
    # 2022 (2012 and 2022 agree), and halving that cell lifted every other row
    # about 2x: petroleum read 1.92x of 2017, held at the 2022 nominal peak
    # through 2024. Against BEA's published summary 211 column, petroleum per
    # $100 of output misses by 0.18 (2018-2024 mean) against 1.08, and self-use
    # by 1.52 against 3.31 (first-order estimate). See
    # bedrock.analysis.nowcasting.inputs_structure.repriced_column (#922).
    # OFF until the next nowcast rebuild, which turns it on and bumps the 2024
    # balanced table pin in the same change; on before that, a rebuild no longer
    # reproduces the pinned tables (test_nowcast_balanced_sut_pin).
    reprice_oil_gas_extraction_seed: bool = False  # DRI: WesIngwersen
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
    useeio_baseline_price_index: ta.Literal[
        'useeior_v1.8.0', 'bea_price_index_vintage'
    ] = Field(
        default='useeior_v1.8.0',
        description=(
            'Price index that converts the USEEIO baseline EFs from their dollar '
            'year to model_base_year. useeior_v1.8.0 is the pinned useeior '
            'Detail_CPI_IO_17sch, so the baseline stays fixed across releases '
            "(#1102). bea_price_index_vintage reuses the run's BEA vintage, "
            'which moves the baseline whenever that vintage moves.'
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
    def _validate_electric_output_basis(self) -> USAConfig:
        if (
            self.electric_output_on_retail_revenue
            and not self.rebase_utility_gross_output_on_eia
        ):
            raise ValueError(
                'electric_output_on_retail_revenue requires '
                'rebase_utility_gross_output_on_eia'
            )
        if (
            self.electricity_accounting_residual
            and not self.electric_output_on_retail_revenue
        ):
            raise ValueError(
                'electricity_accounting_residual requires '
                'electric_output_on_retail_revenue'
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
        if self.implement_electricity_disaggregation:
            key = (self.usa_detail_io_source, self.usa_ghg_data_year)
            if key not in ELECTRICITY_DISAGG_SUPPORTED_IO_YEAR:
                supported = ', '.join(
                    f'{src}/{yr}'
                    for src, yr in sorted(ELECTRICITY_DISAGG_SUPPORTED_IO_YEAR)
                )
                raise ValueError(
                    'implement_electricity_disaggregation requires '
                    f'(usa_detail_io_source, usa_ghg_data_year) in {{{supported}}}; '
                    f'got usa_detail_io_source={self.usa_detail_io_source!r}, '
                    f'usa_ghg_data_year={self.usa_ghg_data_year}'
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
        '2fcbd68b3275cc8e409d4df5d1f28a3a8355c249',  # v0.4.0 (shipped; pre waste match_io Flip)
        '60c8a6b8568b3002a73cdf8569114b2250571e94',  # v0.4.1 interim equal-fill Flip snapshot
        '1ca4453a7882ffbc70527bdaab32fcc835b7f84d',  # v0.4.1 local prior-weighted snapshot
        '0d26d14f61a86e5f2c9c4fd9b7b2d23e63f203a0',  # v0.4.1 (CI Flip)
        '3a1dddc2030e5793b929888c1343cf5a14729f4c',  # v0.5.0
        'e10c75f542b0c9b24692f6524a53073e091d54dc',  # v0.6.0
        '326ae5d8922ad69347c360314be541935a374a4b',  # v1.0.0-rc
        '7f0bdab6e7d12f17a9c2a61437a22af1df7bed30',  # v1.0.0, before the electricity flags
        'c9b69d94a20aecc8d858738eb87093e26e612dbe',  # v1.0.0 (current .SNAPSHOT_KEY)
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

    Used by analysis-only loaders (e.g. waste-disagg analysis YAMLs outside
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
