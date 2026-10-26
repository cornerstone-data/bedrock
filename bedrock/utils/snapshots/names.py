import typing as ta

SnapshotName = ta.Literal[
    "E_USA_ES",  # ceda EmissionsSource x industries (AR6 kg CO2e)
    "E_USA_GAS",  # 7 GHG x industries (former E_USA_ES content)
    "E_USA_ES_LONG",  # source x gas x sector (kg + AR5/AR6 CO2e)
    "B_USA_non_finetuned",
    "Adom_USA",
    "Aimp_USA",
    "scaled_q_USA",
    "y_nab_USA",
    "ytot_USA",
    "exports_USA",
    "ydom_USA",
    "yimp_USA",
    "Phi",
    "A_margin",
]

SNAPSHOT_NAMES: ta.List[SnapshotName] = list[SnapshotName](ta.get_args(SnapshotName))
