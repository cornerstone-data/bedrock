"""Output paths for CEDA electricity diagnostics decks."""

from __future__ import annotations

from pathlib import Path

from bedrock.analysis.electricity.current.diagnostics.paths import OUT_DIR, ensure_dirs

CEDA_OUT_DIR = OUT_DIR / 'ceda_electricity'
PNG_DIR = CEDA_OUT_DIR / 'png'


def ensure_ceda_dirs() -> Path:
    ensure_dirs()
    CEDA_OUT_DIR.mkdir(parents=True, exist_ok=True)
    PNG_DIR.mkdir(parents=True, exist_ok=True)
    return CEDA_OUT_DIR
