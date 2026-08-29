"""Is a cached FBA/FBS parquet older than the data it was built from?

⚠️ **This trap has produced wrong published claims three times.**  The most
recent cost a whole line of investigation: Census publishes a residual code
``33641X`` carrying **121.0 bn USD** of aircraft exports, the extractor learned
to keep it in #720, and *nothing downstream noticed*.  The cached
``Census_USATrade_2017`` FBA predated the fix by thirteen days, the
``Trade_Exports_2017`` FBS was built on that FBA, the Step 3 plan quoted the
FBS, and a diagnostic quoted the plan.  Four artifacts agreed, which read as
four confirmations and was one stale input reflected four times.

**Two different staleness bugs, and only the second is obvious.**

``internal``
    An artifact embeds a source whose ``date_created`` is **newer than its
    own** -- it consumed something, then that something was rebuilt, and the
    artifact was not.  Rare, and usually a build-order mistake.

``superseded``
    An artifact embeds source ``X`` built at time ``t``, and a **newer parquet
    for X now exists on disk**.  The artifact is internally consistent and
    completely stale.  ✅ **This is the one that bites**, because every number
    downstream stays self-consistent and nothing errors.

⚠️ **Regenerating an FBA does NOT invalidate the FBS built on it.**  The FBS
cache key is derived from the *method* files, so an FBA rebuild leaves it
valid and the next run returns byte-identical numbers that look like
independent confirmation.  Delete the FBS parquet, rebuild, and check with
this module -- do not infer from "the numbers did not move" that they were
right.

Run from repo root::

    # every cached artifact
    uv run python -m bedrock.utils.validation.stale_artifacts

    # just the ones matching a name
    uv run python -m bedrock.utils.validation.stale_artifacts --name Trade_

    # exit non-zero if anything is stale (for CI or a pre-rebuild gate)
    uv run python -m bedrock.utils.validation.stale_artifacts --strict
"""

from __future__ import annotations

import json
import sys
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[2]

#: Where generated parquet metadata lands.  FBAs under ``extract``, FBS under
#: ``transform``; both write ``<name>_<version>_<githash>_metadata.json``.
OUTPUT_DIRS = (
    _ROOT / "extract" / "output_data",
    _ROOT / "transform" / "output_data",
)

#: Keys under which a metadata blob nests the metadata of what it consumed.
SOURCE_KEYS = ("primary_source_meta", "attribution_source_meta")

_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def _parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, _DATE_FORMAT)
    except ValueError:
        return None


def _walk_sources(meta: dict[str, Any]) -> Iterator[dict[str, Any]]:
    """Every nested source blob, at any depth.

    ⚠️ ``primary_source_meta`` sits **inside ``tool_meta``**, not beside it, and
    an FBS nests its FBAs one level further down again.  Looking only at the
    top level finds nothing and reports every artifact clean -- which is how
    the first version of this module passed on the very trade parquets it was
    written to catch.
    """
    containers = [meta]
    tool_meta = meta.get("tool_meta")
    if isinstance(tool_meta, dict):
        containers.append(tool_meta)

    for container in containers:
        for key in SOURCE_KEYS:
            nested = container.get(key) or {}
            if not isinstance(nested, dict):
                continue
            for blob in nested.values():
                if not isinstance(blob, dict):
                    continue
                yield blob
                yield from _walk_sources(blob)


def load_metadata() -> dict[str, dict[str, Any]]:
    """Every ``*_metadata.json`` on disk, keyed by its file stem."""
    found: dict[str, dict[str, Any]] = {}
    for directory in OUTPUT_DIRS:
        if not directory.exists():
            continue
        for path in directory.glob("*_metadata.json"):
            try:
                found[path.stem] = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
    return found


def newest_on_disk(metadata: dict[str, dict[str, Any]]) -> dict[str, datetime]:
    """Latest ``date_created`` per ``name_data`` across everything cached."""
    newest: dict[str, datetime] = {}
    for blob in metadata.values():
        name = blob.get("name_data")
        created = _parse_date(blob.get("date_created"))
        if not name or created is None:
            continue
        if name not in newest or created > newest[name]:
            newest[name] = created
    return newest


def find_stale(name_filter: str = "") -> list[dict[str, Any]]:
    """Artifacts built before something they depend on.

    Returns one row per (artifact, source) problem, with ``kind`` set to
    ``internal`` or ``superseded`` -- see the module docstring for why the
    second matters more.
    """
    metadata = load_metadata()
    newest = newest_on_disk(metadata)

    problems: list[dict[str, Any]] = []
    for blob in metadata.values():
        name = blob.get("name_data", "")
        if name_filter and name_filter not in name:
            continue
        built = _parse_date(blob.get("date_created"))
        if built is None:
            continue

        for source in _walk_sources(blob):
            source_name = source.get("name_data")
            embedded = _parse_date(source.get("date_created"))
            if not source_name or embedded is None:
                continue

            if embedded > built:
                problems.append(
                    {
                        "kind": "internal",
                        "artifact": name,
                        "artifact_built": built,
                        "source": source_name,
                        "source_built": embedded,
                        "detail": "consumed a source newer than itself",
                    }
                )

            available = newest.get(source_name)
            if available is not None and available > embedded:
                problems.append(
                    {
                        "kind": "superseded",
                        "artifact": name,
                        "artifact_built": built,
                        "source": source_name,
                        "source_built": embedded,
                        "detail": (
                            f"a newer {source_name} exists on disk "
                            f"({available:%Y-%m-%d %H:%M})"
                        ),
                    }
                )

    problems.sort(key=lambda row: (row["kind"], row["artifact"], row["source"]))
    return problems


def main() -> None:
    name_filter = ""
    if "--name" in sys.argv:
        index = sys.argv.index("--name")
        if index + 1 < len(sys.argv):
            name_filter = sys.argv[index + 1]

    problems = find_stale(name_filter)
    scope = f" matching {name_filter!r}" if name_filter else ""
    if not problems:
        print(f"No stale cached artifacts{scope}.")
        return

    superseded = [p for p in problems if p["kind"] == "superseded"]
    internal = [p for p in problems if p["kind"] == "internal"]
    print(
        f"{len(problems)} staleness problem(s){scope}: "
        f"{len(superseded)} superseded, {len(internal)} internal."
    )
    print()
    for row in problems:
        print(
            f"  [{row['kind']:11s}] {row['artifact']} "
            f"(built {row['artifact_built']:%Y-%m-%d %H:%M})"
        )
        print(f"      source {row['source']}: {row['detail']}")

    print()
    print(
        "Fix by DELETING the stale parquet and rebuilding -- regenerating an "
        "upstream FBA does not invalidate the FBS built on it, and the rerun "
        "will otherwise return byte-identical numbers."
    )
    if "--strict" in sys.argv:
        sys.exit(1)


if __name__ == "__main__":
    main()
