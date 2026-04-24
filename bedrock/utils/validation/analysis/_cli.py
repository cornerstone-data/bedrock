"""Shared CLI options + path helpers for analysis plot commands.

Every script exposes the same flags (``--baseline``, ``--sheet-id``,
``--refresh``, ``--tag``, ``--out-dir``) via ``common_options``. BLy-only
flags used by ``ef_plots`` live in ``bly_plot_options`` so they stay out of
``common_options`` (for any future script that shares the common five but not
BLy). The sheet-id resolver picks from (in order): ``--sheet-id``,
``--baseline`` lookup in ``baselines.BASELINES``, then
``$BEDROCK_DIAGNOSTICS_SHEET_ID``.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path
from typing import Any, TypeVar

import click

from .baselines import BASELINES
from .bly_plots import DEFAULT_GROUP_SMALL_THRESHOLD

F = TypeVar("F", bound=Callable[..., Any])

_OUTPUT_ROOT = Path(__file__).resolve().parent / "output"
_SHEET_ID_ENV = "BEDROCK_DIAGNOSTICS_SHEET_ID"


def common_options(func: F) -> F:
    """Apply the shared click options to ``func``."""
    func = click.option(
        "--out-dir",
        type=click.Path(file_okay=False, path_type=Path),
        default=None,
        help="Override output directory. Defaults to analysis/output/<tag>/",
    )(func)
    func = click.option(
        "--tag",
        type=str,
        default=None,
        help=(
            "Label for output dir + figure titles. Defaults to --baseline "
            "name, else first 12 chars of --sheet-id."
        ),
    )(func)
    func = click.option(
        "--refresh",
        is_flag=True,
        default=False,
        help="Force re-fetch from Google Sheets, overwriting the parquet cache.",
    )(func)
    func = click.option(
        "--sheet-id",
        required=False,
        default=None,
        type=str,
        help=(
            "Google Sheet ID (string between /d/ and /edit in the sheet URL). "
            "Takes precedence over --baseline."
        ),
    )(func)
    func = click.option(
        "--baseline",
        required=False,
        default=None,
        type=click.Choice(list(BASELINES)),
        help=(
            "Registered baseline name — looked up in baselines.BASELINES. "
            f"Falls back to ${_SHEET_ID_ENV} when neither --baseline nor "
            "--sheet-id is given."
        ),
    )(func)
    return func


def bly_plot_options(func: F) -> F:
    """BLy figure options (compose with ``common_options`` on ``ef_plots``)."""
    return click.option(
        "--bly-group-small-threshold",
        type=float,
        default=DEFAULT_GROUP_SMALL_THRESHOLD,
        show_default=True,
        help=(
            "BLy stacked bar: roll sectors with |Δ Mt CO2e| below this into "
            "Other Increase / Other Decrease. Use 0 to show every sector."
        ),
    )(func)


def resolve_sheet_id(sheet_id: str | None, baseline: str | None) -> str:
    """Resolve a sheet id from (priority order) ``--sheet-id``, ``--baseline``, env var."""
    if sheet_id:
        return sheet_id
    if baseline:
        return BASELINES[baseline]
    from_env = os.environ.get(_SHEET_ID_ENV)
    if from_env:
        return from_env
    raise click.UsageError(
        f"One of --sheet-id, --baseline, or ${_SHEET_ID_ENV} is required."
    )


def resolve_output_dir(
    sheet_id: str,
    tag: str | None,
    out_dir: Path | None,
    baseline: str | None = None,
) -> tuple[str, Path]:
    """Return a (tag, output_dir) pair, creating the directory if needed."""
    resolved_tag = tag or baseline or sheet_id[:12]
    resolved_dir = out_dir if out_dir else _OUTPUT_ROOT / resolved_tag
    resolved_dir.mkdir(parents=True, exist_ok=True)
    return resolved_tag, resolved_dir
