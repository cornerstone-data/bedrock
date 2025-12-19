import pytest

from bedrock.transform.flowbysector import FlowBySector, getFlowBySector


@pytest.mark.eeio_integration
def test_generate_fbs() -> None:
    y = 2022
    FlowBySector.generateFlowBySector(f'GHG_national_{y}_m1', download_sources_ok=True)
    fbs = getFlowBySector(f'GHG_national_{y}_m1')
    assert len(fbs) > 0
