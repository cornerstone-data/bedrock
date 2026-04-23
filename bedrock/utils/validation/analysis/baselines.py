"""Registered diagnostics baselines: friendly name → Google Sheet ID.

Shared, long-lived comparison sheets. Each entry points at a diagnostics
sheet comparing current Cornerstone outputs against a named baseline
(CEDA-US, USEEIO, etc.). Extend as new reference runs are produced.

For ad-hoc or per-PR sheets, pass ``--sheet-id`` directly or set the
``BEDROCK_DIAGNOSTICS_SHEET_ID`` environment variable instead.
"""

from __future__ import annotations

BASELINES: dict[str, str] = {
    "ceda": "1pCSgLD14lmrQg3OtfHvnK4lQrFtqiavrjCt-C3R_bSw",
    "useeio": "1WjEMhfJw_Z62sGffoLFSJoZtC112IDtjA9Ak7H8PUVQ",
}
