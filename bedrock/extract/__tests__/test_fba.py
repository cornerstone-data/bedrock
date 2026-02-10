import pytest
from pandas.testing import assert_frame_equal

from bedrock.extract.flowbyactivity import getFlowByActivity
from bedrock.extract.generateflowbyactivity import generateFlowByActivity
from bedrock.utils.validation.validation import _compare_fba_values


@pytest.mark.eeio_integration
def test_get_fba_from_gcs() -> None:
    df = getFlowByActivity("EPA_GHGI_T_2_1", year=2022, download_FBA_if_missing=True)

    assert len(df) > 0


@pytest.mark.eeio_integration
def test_generateFBA() -> None:
    generateFlowByActivity(year=2022, source='EPA_GHGI')
    df = getFlowByActivity("EPA_GHGI_T_2_1", year=2022)

    assert len(df) > 0


@pytest.mark.skip
def test_generate_fba_compare_to_remote() -> None:
    y = 2022
    source = 'USDA_CoA_Cropland'

    # Download and load from GCS (local directory needs to be empty of this
    # FBA to force download)
    fba_remote = getFlowByActivity(
        year=y, datasource=source, download_FBA_if_missing=True
    )

    # Compare to newly generated version
    generateFlowByActivity(year=y, source=source)
    fba = getFlowByActivity(year=y, datasource=source)

    df_m = _compare_fba_values(fba_remote, fba)

    assert_frame_equal(fba_remote, fba)

    assert len(df_m) == 0


if __name__ == "__main__":
    test_generate_fba_compare_to_remote()
