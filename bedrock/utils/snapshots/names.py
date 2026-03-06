import typing as ta

SnapshotName = ta.Literal[
    "E_USA_ES",
    "B_USA_non_finetuned",
    "Adom_USA",
    "Aimp_USA",
    "scaled_q_USA",
    "y_nab_USA",
    "ytot_USA",
    "exports_USA",
    "ydom_USA",
    "yimp_USA",
]

SNAPSHOT_NAMES: ta.List[SnapshotName] = list[SnapshotName](ta.get_args(SnapshotName))
