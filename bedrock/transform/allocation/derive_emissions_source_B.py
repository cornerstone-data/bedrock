import pandas as pd

from bedrock.transform.allocation.derived import derive_E_usa_emissions_sources
from bedrock.transform.eeio.derived_2017 import (
    derive_2017_g_usa,
    derive_2017_Vnorm_scrap_corrected,
)
from bedrock.ceda_usa.utils.formulas import compute_B_ind_matrix, compute_B_matrix


def derive_emissions_source_B() -> pd.DataFrame:
    """
    Put the emissions_source level broken out E through the industry-based technology approach to get the emissions_source level broken out B.
    They are then transformed to commodites via VRnorm_scrap_corrected.
    """
    E_emissions_sources = derive_E_usa_emissions_sources()

    g = derive_2017_g_usa()
    Vnorm = derive_2017_Vnorm_scrap_corrected()
    Bi = compute_B_ind_matrix(E=E_emissions_sources, g=g)
    Bc = compute_B_matrix(B_ind=Bi, V_norm=Vnorm)

    Bc = compute_B_matrix(B_ind=Bi, V_norm=Vnorm)
    return Bc
