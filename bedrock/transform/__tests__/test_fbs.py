import pytest
from pandas.testing import assert_frame_equal

from bedrock.transform.flowbysector import FlowBySector, getFlowBySector
from bedrock.utils.validation.validation import compare_FBS


@pytest.mark.skip  # replaced by compare to remote test
def test_generate_fbs() -> None:
    y = 2022
    method = f'GHG_national_{y}_m1'
    FlowBySector.generateFlowBySector(method, download_sources_ok=False)
    fbs = getFlowBySector(method)

    assert len(fbs) > 0


METHODS = [
    pytest.param("GHG_national_CEDA_2023", id="GHG_national_CEDA_2023"),
]


@pytest.mark.eeio_integration
@pytest.mark.parametrize("method", METHODS)
def test_generate_fbs_compare_to_remote(method: str) -> None:
    # Download and load from GCS (local directory needs to be empty of this
    # method to force download)
    fbs_remote = getFlowBySector(method, download_FBS_if_missing=True)

    # Compare to newly generated version
    FlowBySector.generateFlowBySector(method, download_sources_ok=False)
    fbs = getFlowBySector(method)

    df_m = compare_FBS(fbs_remote, fbs, ignore_metasources=False)

    # Drop some columns that may have different dtypes and are not used
    skip_columns = ['ProducedBySectorType', 'ConsumedBySectorType']

    assert_frame_equal(
        fbs_remote.drop(columns=skip_columns),
        fbs.drop(columns=skip_columns),
        check_like=True,
    )

    assert len(df_m) == 0


if __name__ == "__main__":
    # test_generate_fbs()
    test_generate_fbs_compare_to_remote(method='GHG_national_CEDA_2023')
