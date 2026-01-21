import pandas as pd
import pytest

from bedrock.transform.flowbysector import FlowBySector, getFlowBySector
from bedrock.utils.validation.validation import compare_FBS


@pytest.mark.eeio_integration
def test_generate_fbs() -> None:
    y = 2022
    method = f'GHG_national_{y}_m1'
    FlowBySector.generateFlowBySector(method, download_sources_ok=False)
    fbs = getFlowBySector(method)

    assert len(fbs) > 0


def test_generate_fbs_compare_to_remote() -> None:
    y = 2022
    method = f'GHG_national_{y}_m1'
    # If need to compare to EPA remote download directly via esupy
    # f = set_fb_meta(method, 'FlowBySector')
    # download_from_remote(f, PATHS)
    # fbs_remote = getFlowBySector(method)

    # Download and load from GCS (local directory needs to be empty of this
    # method to force download)
    fbs_remote = getFlowBySector(method, download_FBS_if_missing=True)

    # Compare to newly generated version
    FlowBySector.generateFlowBySector(method, download_sources_ok=False)
    fbs = getFlowBySector(method)

    df_m = compare_FBS(fbs_remote, fbs, ignore_metasources=False)
    pd.testing.assert_frame_equal(fbs_remote, fbs, check_like=True)

    assert len(df_m) == 0


if __name__ == "__main__":
    # test_generate_fbs()
    test_generate_fbs_compare_to_remote()
