"""Output paths for the five-slide electricity comparison deck."""

from __future__ import annotations

from pathlib import Path

from bedrock.analysis.electricity.current.diagnostics.paths import OUT_DIR, ensure_dirs

DECK_OUT_DIR = OUT_DIR / 'deck'
CACHE_DIR = DECK_OUT_DIR / 'cache'


def ensure_deck_dirs() -> Path:
    ensure_dirs()
    DECK_OUT_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return DECK_OUT_DIR


def impl_cache_dir(impl_id: str, config_stem: str) -> Path:
    return CACHE_DIR / impl_id / config_stem
