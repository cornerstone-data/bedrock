
import pandas as pd

from bedrock.ceda_usa.extract.iot.constants import (
    PRICE_INDEX_DETAIL_NAME_TO_BEA_2017_INDUSTRY_MAPPING,
    PRICE_INDEX_SUMMARY_LINE_NUMBER_TO_BEA_2017_SUMMARY_MAPPING_NON_EMPTY,
)
from bedrock.ceda_usa.extract.iot.io_price_index import (
    SECTOR_NAME_COL,
    SECTOR_SUMMARY_CODE_COL,
    load_go_detail,
    load_pi_detail,
    load_pi_summary_quarterly,
)
from bedrock.ceda_usa.utils.taxonomy.bea.v2017_industry import BEA_2017_INDUSTRY_CODES
from bedrock.ceda_usa.utils.taxonomy.mappings.bea_v2017_industry__bea_v2017_summary import (
    load_bea_v2017_industry_to_bea_v2017_summary,
)
from bedrock.ceda_usa.utils.taxonomy.utils import assert_sets_equal

START_YEAR = 2012
END_YEAR = 2025
YEARS = list(range(START_YEAR, END_YEAR + 1))
SECTOR_CODE_COL = "sector_code"
DISAGGREGATED_CODES = [
    "562111",  # Solid waste collection
    "562HAZ",  # Hazardous waste collection treatment and disposal
    "562212",  # Solid waste landfilling
    "562213",  # Solid waste combustors and incinerators
    "562910",  # Remediation services
    "562920",  # Material separation/recovery facilities
    "562OTH",  # Other waste collection and treatment services
]
INFLATION_SECTORS = [
    c for c in BEA_2017_INDUSTRY_CODES + DISAGGREGATED_CODES + ["333914", "335220"]
]


def prepare_formatted_bea_price_index() -> pd.DataFrame:
    """Return the BEA detail price index table formatted for all configured years."""
    return _combine_pi_detail_summary(END_YEAR).loc[:, [str(year) for year in YEARS]]


def _combine_pi_detail_summary(end_year: int) -> pd.DataFrame:
    """Merge aggregated detail PI with the latest summary PI and add waste splits."""
    pi = pd.merge(
        _aggregate_detail_pi(),
        _map_pi_summary__detail(
            pi_summary=_aggregate_latest_pi_summary_quarterly__annual(end_year)
        ),
        on=SECTOR_CODE_COL,
        how="left",
    )
    YEAR_COLS = pi.columns.difference(
        pd.Index([SECTOR_CODE_COL, SECTOR_NAME_COL, SECTOR_SUMMARY_CODE_COL])
    )

    waste_pi = pi[pi[SECTOR_CODE_COL] == "562000"]
    assert waste_pi.shape[0] == 1, "found multiple aggregated waste rows"

    waste_disaggregated = pd.concat(
        [waste_pi.assign(sector_code=code) for code in DISAGGREGATED_CODES],
        ignore_index=True,
    )
    pi_final = pd.concat(
        [
            pi,
            waste_disaggregated,
        ],
        ignore_index=True,
    ).set_index(SECTOR_CODE_COL)

    return pi_final[YEAR_COLS]


def _aggregate_detail_pi() -> pd.DataFrame:
    """Load detail-level PI/GO tables, aggregate duplicates, and validate coverage."""
    pi_detail = _map_detail_table(load_pi_detail())
    go_detail = _map_detail_table(load_go_detail())

    duplicated_codes = (
        pi_detail[SECTOR_CODE_COL]
        .value_counts()
        .pipe(lambda ser: ser[ser > 1])
        .index.tolist()
    )
    print(f"handling duplicated codes {duplicated_codes}")

    pi_detail_reagg = pi_detail.loc[
        ~pi_detail[SECTOR_CODE_COL].isin(duplicated_codes),
        :,
    ]

    YEAR_COLS = pi_detail_reagg.columns.difference(
        pd.Index([SECTOR_CODE_COL, SECTOR_NAME_COL])
    )
    for code in duplicated_codes:
        pi = pi_detail[pi_detail[SECTOR_CODE_COL] == code].sort_values(
            by=SECTOR_NAME_COL
        )
        go = go_detail[go_detail[SECTOR_CODE_COL] == code].sort_values(
            by=SECTOR_NAME_COL
        )
        wgt_avg_pi = round(
            (pi[YEAR_COLS] * go[YEAR_COLS]).sum(axis=0) / go[YEAR_COLS].sum(axis=0), 3
        )
        pi_agg = wgt_avg_pi.to_frame().transpose().assign(sector_code=code)
        pi_detail_reagg = pd.concat([pi_detail_reagg, pi_agg], ignore_index=True)

    assert pi_detail_reagg[SECTOR_CODE_COL].is_unique, "found duplicate secotr codes"
    assert_sets_equal(
        expected=set(BEA_2017_INDUSTRY_CODES) | {"335220"},
        actual=set(pi_detail_reagg[SECTOR_CODE_COL]),
        if_fail="exception",
    )
    return pi_detail_reagg


def _aggregate_latest_pi_summary_quarterly__annual(end_year: int) -> pd.DataFrame:
    """Average quarterly summary PI into an annual column for the provided year."""
    pi_summary_quarterly = load_pi_summary_quarterly()
    pi_summary_quarterly.index = pi_summary_quarterly.index.map(
        PRICE_INDEX_SUMMARY_LINE_NUMBER_TO_BEA_2017_SUMMARY_MAPPING_NON_EMPTY
    )
    pi_summary_quarterly[SECTOR_SUMMARY_CODE_COL] = pi_summary_quarterly.index
    pi_summary = pi_summary_quarterly.copy().dropna().reset_index(drop=True)
    pi_summary[str(end_year)] = pi_summary[
        [col for col in pi_summary.columns if col.startswith(str(end_year))]
    ].mean(axis=1)
    return pi_summary[[SECTOR_NAME_COL, SECTOR_SUMMARY_CODE_COL, str(end_year)]]


def _map_pi_summary__detail(pi_summary: pd.DataFrame) -> pd.DataFrame:
    """Map BEA summary-level PI rows onto their corresponding detail sectors."""
    summary__detail_mapping = pd.DataFrame.from_dict(
        load_bea_v2017_industry_to_bea_v2017_summary(),
        orient="index",
        columns=[SECTOR_SUMMARY_CODE_COL],
    )
    summary__detail_mapping[SECTOR_CODE_COL] = summary__detail_mapping.index

    return pi_summary.merge(
        summary__detail_mapping,
        on=SECTOR_SUMMARY_CODE_COL,
        how="left",
    ).set_index(SECTOR_NAME_COL)


def _map_detail_table(df: pd.DataFrame) -> pd.DataFrame:
    """Attach BEA detail sector codes to the raw PI/GO tables via name mapping."""
    mapping = pd.DataFrame(
        list(PRICE_INDEX_DETAIL_NAME_TO_BEA_2017_INDUSTRY_MAPPING.items()),
        columns=[SECTOR_NAME_COL, SECTOR_CODE_COL],
    ).explode(SECTOR_CODE_COL)
    return df.merge(mapping, on=SECTOR_NAME_COL, how="left")
