from bedrock.utils.taxonomy.cornerstone.commodities import WASTE_DISAGG_COMMODITIES
from bedrock.utils.taxonomy.cornerstone.disagg_sectors import DISAGG_SECTORS
from bedrock.utils.taxonomy.cornerstone.industries import WASTE_DISAGG_INDUSTRIES


def test_disagg_sectors_waste_registry() -> None:
    waste = DISAGG_SECTORS["waste"]
    assert waste.name == "waste"
    assert waste.industry_aggregate_code == "562000"
    assert waste.commodity_aggregate_code == "562000"
    assert len(waste.industry_new_codes) == 7
    assert len(waste.commodity_new_codes) == 7
    assert waste.industry_new_codes == waste.commodity_new_codes


def test_waste_disagg_aliases_match_registry() -> None:
    waste = DISAGG_SECTORS["waste"]
    assert WASTE_DISAGG_COMMODITIES["562000"] == list(waste.commodity_new_codes)
    assert WASTE_DISAGG_INDUSTRIES["562000"] == list(waste.industry_new_codes)
