import pytest

from bedrock.flowsa import getFlowByActivity


@pytest.mark.eeio_integration
def test_get_fba_from_gcs() -> None:
    df = getFlowByActivity("EPA_GHGI_T_2_1", year=2022, download_FBA_if_missing=True)

    assert len(df) > 0
