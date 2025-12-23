import pytest

from bedrock.extract.flowbyactivity import getFlowByActivity
from bedrock.extract.generateflowbyactivity import generateFlowByActivity


@pytest.mark.eeio_integration
def test_get_fba_from_gcs() -> None:
    df = getFlowByActivity("EPA_GHGI_T_2_1", year=2022, download_FBA_if_missing=True)

    assert len(df) > 0


@pytest.mark.eeio_integration
def test_generateFBA() -> None:
    generateFlowByActivity(year=2022, source='EPA_GHGI')
    df = getFlowByActivity("EPA_GHGI_T_2_1", year=2022)

    assert len(df) > 0
