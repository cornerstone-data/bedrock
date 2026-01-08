import typing as ta

from bedrock.utils.taxonomy.bea.ceda_v5 import CEDA_V5_SECTOR
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTOR

# This BEA code is renamed in CEDA V7 from CEDA V5
# because BEA updated their sector classifications.
# All other codes remain the same.
CEDA_V7_TO_CEDA_V5_CODES: ta.Dict[CEDA_V7_SECTOR, CEDA_V5_SECTOR] = {"333914": "33391A"}

CEDA_V5_TO_CEDA_V7_CODES: ta.Dict[CEDA_V5_SECTOR, CEDA_V7_SECTOR] = {
    v: k for k, v in CEDA_V7_TO_CEDA_V5_CODES.items()
}
