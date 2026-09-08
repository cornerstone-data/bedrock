"""Is a cached FBA/FBS parquet older than the data it was built from?

⚠️ **This trap has produced wrong published claims three times.**  The most
recent cost a whole line of investigation: Census publishes a residual code
``33641X`` carrying **121.0 bn USD** of aircraft exports, the extractor learned
to keep it in #720, and *nothing downstream noticed*.  The cached
``Census_USATrade_2017`` FBA predated the fix by thirteen days, the
``Trade_Exports_2017`` FBS was built on that FBA, the Step 3 plan quoted the
FBS, and a diagnostic quoted the plan.  Four artifacts agreed, which read as
four confirmations and was one stale input reflected four times.

**Three different staleness bugs, and only the second is obvious.**

``internal``
    An artifact embeds a source whose ``date_created`` is **newer than its
    own** -- it consumed something, then that something was rebuilt, and the
    artifact was not.  Rare, and usually a build-order mistake.

``superseded``
    An artifact embeds source ``X`` built at time ``t``, and a **newer parquet
    for X now exists on disk**.  The artifact is internally consistent and
    completely stale.  ✅ **This is the one that bites**, because every number
    downstream stays self-consistent and nothing errors.

``method``
    The artifact's own **method** has changed since it was built -- its yaml,
    anything that yaml ``!include``s, or the Python modules beside it.  Nothing
    upstream moved, so neither of the other two classes can see it.

    ⚠️ **This is the class that has no natural error.**  Editing a method yaml
    and re-running returns the previous parquet, silently: the FBS cache key is
    the *git hash*, so an uncommitted edit does not change it and a committed
    one only changes it at the next commit.  On 2026-09-07 that bit twice in
    one session -- a re-weighted export split returned the old numbers verbatim,
    and a second attempt rebuilt 2017 only, putting a **fake discontinuity**
    between 2017 and 2018 that reads exactly like a real seam in the series.

    The check compares the artifact's ``date_created`` against the newest of
    (a) the last commit touching its method files and (b) their working-tree
    mtimes.  The mtime half is what catches the uncommitted edit.

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

    # delete what is stale so the next run rebuilds it
    uv run python -m bedrock.utils.validation.stale_artifacts --delete
    uv run python -m bedrock.utils.validation.stale_artifacts --name Trade_ --delete

⚠️ ``--delete`` removes the parquet **and** its metadata json, which is the
only way the next run rebuilds rather than reloading.  It prints every path
before removing it and refuses to touch anything outside
:data:`OUTPUT_DIRS`.
"""

from __future__ import annotations

import functools
import json
import re
import subprocess
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

#: ``method_url`` points into a GitHub blob; everything after the commit sha is
#: the repo-relative path of the method file.
_BLOB = re.compile(r"/blob/[0-9a-f]{7,40}/(?P<path>.+?)(?:\?|#|$)")

#: yaml include directives name a sibling method file.
_INCLUDE = re.compile(r"!include:([A-Za-z0-9_./-]+\.yaml)")

#: ``!clean_function:trade.utilities name`` / ``!script_function:BEA_IEA name``
#: name a module by dotted suffix rather than by path.  Only the last component
#: is resolvable without importing, so that is what is matched against module
#: basenames.
_FUNCTION = re.compile(r"!(?:clean|script)_function:([A-Za-z0-9_.]+)")


@functools.cache
def _modules_by_name() -> dict[str, tuple[Path, ...]]:
    """Every ``bedrock`` module, keyed by basename, for resolving yaml tags."""
    found: dict[str, list[Path]] = {}
    for path in (_ROOT).rglob("*.py"):
        if "__pycache__" in path.parts or "__tests__" in path.parts:
            continue
        found.setdefault(path.stem, []).append(path)
    return {name: tuple(sorted(paths)) for name, paths in found.items()}


def _repo_root() -> Path:
    return _ROOT.parent


@functools.cache
def _tracked_change_times() -> dict[str, datetime]:
    """Last commit time per tracked path, from one ``git log`` pass.

    ⚠️ Deliberately not ``git log -- <path>`` per artifact: there are hundreds
    of artifacts and a handful of method files, and a per-path call would make
    the check slow enough that nobody runs it before a rebuild.
    """
    try:
        result = subprocess.run(
            ["git", "log", "--name-only", "--pretty=format:%x01%cI"],
            cwd=_repo_root(),
            capture_output=True,
            text=True,
            check=True,
            timeout=180,
        )
    except (OSError, subprocess.SubprocessError):
        return {}

    times: dict[str, datetime] = {}
    when: datetime | None = None
    for line in result.stdout.splitlines():
        if line.startswith(""):
            when = None
            stamp = line[1:].strip()
            try:
                when = datetime.fromisoformat(stamp).astimezone().replace(tzinfo=None)
            except ValueError:
                when = None
        elif line.strip() and when is not None and line not in times:
            # git log walks newest first, so the first sighting is the newest.
            times[line.strip()] = when
    return times


def method_files(meta: dict[str, Any]) -> list[Path]:
    """The files whose content decides what this artifact contains.

    The method's own yaml, every yaml it ``!include``s transitively, and the
    modules its ``!clean_function`` / ``!script_function`` tags name.

    ⚠️ **Not every ``.py`` beside the yaml.**  That was the first version and it
    flagged 985 of 995 artifacts -- an unrelated commit anywhere in
    ``transform/nipa`` marked every ``NIPA_VA_*`` parquet stale.  A check that
    says everything is stale targets nothing.  The tags name a module by dotted
    suffix, so only the last component is resolvable without importing; that is
    matched against module basenames, which can pull in a same-named module
    elsewhere but errs narrow rather than repo-wide.

    ⚠️ Returns empty for a method that lives in another repository.  Several
    FBAs are flowsa's, and their ``method_url`` points at ``USEPA/flowsa``;
    this check cannot see those and says so rather than guessing.
    """
    url = str((meta.get("tool_meta") or {}).get("method_url") or "")
    if "cornerstone-data/bedrock" not in url:
        return []
    match = _BLOB.search(url)
    if not match:
        return []
    start = _repo_root() / match.group("path")
    if not start.exists():
        return []

    seen: set[Path] = set()
    queue = [start]
    while queue:
        current = queue.pop()
        if current in seen or not current.exists():
            continue
        seen.add(current)
        try:
            text = current.read_text(encoding="utf-8")
        except OSError:
            continue
        for name in _INCLUDE.findall(text):
            queue.append(current.parent / Path(name).name)
        if current.suffix in {".yaml", ".yml"}:
            for dotted in _FUNCTION.findall(text):
                for path in _modules_by_name().get(dotted.split(".")[-1], ()):
                    queue.append(path)
    return sorted(seen)


def method_changed_at(paths: list[Path]) -> tuple[datetime | None, Path | None]:
    """Newest of (last commit, working-tree mtime) over *paths*, and which file.

    ⚠️ The mtime half is not redundant.  An **uncommitted** method edit leaves
    the git hash untouched, so the FBS cache key does not move and the rebuild
    silently returns the old parquet -- which is exactly the failure this class
    exists to catch.
    """
    tracked = _tracked_change_times()
    root = _repo_root()
    newest: datetime | None = None
    culprit: Path | None = None
    for path in paths:
        stamps = []
        try:
            relative = path.relative_to(root).as_posix()
        except ValueError:
            relative = ""
        if relative and relative in tracked:
            stamps.append(tracked[relative])
        try:
            stamps.append(datetime.fromtimestamp(path.stat().st_mtime))
        except OSError:
            pass
        for stamp in stamps:
            if newest is None or stamp > newest:
                newest, culprit = stamp, path
    return newest, culprit


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

        changed, culprit = method_changed_at(method_files(blob))
        if changed is not None and changed > built:
            problems.append(
                {
                    "kind": "method",
                    "artifact": name,
                    "artifact_built": built,
                    "source": culprit.name if culprit else "method",
                    "source_built": changed,
                    "detail": (
                        f"its method changed at {changed:%Y-%m-%d %H:%M} "
                        f"({culprit.name if culprit else '?'}), after this was built"
                    ),
                }
            )

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


#: ``--scope`` values.  Extracts and transforms cost very different things to
#: rebuild: an FBS reruns from parquets already on disk, while an FBA re-fetches
#: from an API or GCS.  Defaulting to ``transform`` keeps the cheap half
#: one command away and makes the expensive half deliberate.
SCOPES = {
    "transform": (_ROOT / "transform" / "output_data",),
    "extract": (_ROOT / "extract" / "output_data",),
    "all": OUTPUT_DIRS,
}


def delete_stale(
    problems: list[dict[str, Any]], scope: str = "transform"
) -> list[Path]:
    """Remove the parquet and metadata of every artifact named in *problems*.

    ⚠️ Both files, not just the parquet. The loader finds a cached artifact by
    globbing the output directory, so leaving the metadata behind is harmless
    but leaving the *parquet* behind means the rebuild silently reloads it.

    Refuses anything outside :data:`OUTPUT_DIRS`, so a malformed metadata blob
    cannot point this at the source tree.
    """
    names = {row["artifact"] for row in problems}
    removed: list[Path] = []
    for directory in SCOPES[scope]:
        if not directory.exists():
            continue
        for path in sorted(directory.iterdir()):
            if not path.is_file():
                continue
            if not any(path.name.startswith(f"{name}_v") for name in names):
                continue
            if path.suffix not in {".parquet", ".json"}:
                continue
            if directory.resolve() not in path.resolve().parents:
                continue
            print(f"  removing {path.relative_to(_ROOT.parent)}")
            path.unlink()
            removed.append(path)
    return removed


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
    changed = [p for p in problems if p["kind"] == "method"]
    print(
        f"{len(problems)} staleness problem(s){scope}: "
        f"{len(superseded)} superseded, {len(internal)} internal, "
        f"{len(changed)} method changed."
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
    if "--delete" in sys.argv:
        scope = "transform"
        if "--scope" in sys.argv:
            index = sys.argv.index("--scope")
            if index + 1 < len(sys.argv):
                scope = sys.argv[index + 1]
        if scope not in SCOPES:
            print(f"--scope must be one of {sorted(SCOPES)}")
            sys.exit(2)
        print()
        print(f"Deleting stale artifacts in scope {scope!r}:")
        removed = delete_stale(problems, scope)
        print()
        print(f"Deleted {len(removed)} file(s); the next run rebuilds them.")
    elif "--strict" in sys.argv:
        sys.exit(1)


if __name__ == "__main__":
    main()
