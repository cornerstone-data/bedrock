---
name: graphite-stacked-prs
description: Use this skill whenever the user wants to commit code changes to GitHub using Graphite in stacked PR format, create a new branch in a stack, or submit PRs via Graphite. Trigger when the user mentions "Graphite", "stacked PR", "gt submit", "gt create", "stack this", "commit and push with Graphite", or asks to create a PR on top of an existing one. Claude should autonomously generate commit messages, branch names, and determine stack position without asking the user.
---

# Graphite Stacked PRs Skill

Claude commits code and opens stacked PRs via the Graphite CLI (`gt`), generating commit messages, branch names, and stack position autonomously. Graphite manages stack comments automatically — never post or edit them.

---

## Workflow

Run these steps in order. Each step is mandatory.

### 1. Sandbox setup (once per session)

`gt` writes to `~/.local/share/graphite/` and `~/.config/graphite/`, neither of which are in Claude Code's sandbox write allowlist. Redirect via XDG vars and copy the auth config across:

```bash
mkdir -p "$TMPDIR/gt-config/graphite" "$TMPDIR/gt-data" \
  && cp -r ~/.config/graphite/* "$TMPDIR/gt-config/graphite/"
```

Prefix every `gt` call with the redirected vars:

```bash
XDG_DATA_HOME="$TMPDIR/gt-data" XDG_CONFIG_HOME="$TMPDIR/gt-config" gt <subcommand>
```

Skipping the auth-config copy makes `gt submit` fail with `Please authenticate your Graphite CLI`.

### 2. Detect author prefix

Branch names use `<author>__<short-description>` — double underscore, kebab-case description, 3–5 words max (e.g. `mo__fix-snapshot-generation`).

**Detect prefix from existing branches, NOT from `git config user.name`.** The real name often has no obvious mapping to the established prefix (e.g. "Mo Li" → `mo`, not `moli`). Past sessions created stuck stacks by skipping this step.

1. Run `git branch -a | head -50` and read the prefix off this author's own existing branches (substring before `__`).
2. Match by author identity, not string similarity. If `mo__decarb-…` exists alongside `user.name = "Mo Li"`, the prefix is `mo` — do not invent `moli`, `mo-li`, or `mo/`.
3. Only if the author has no existing branches, fall back to `git config user.name` (shortest plausible token, lowercased).
4. Separator is always `__`. A `/` separator is a signal that the prefix was fabricated.

Cache the verified prefix for the rest of the session.

### 3. Stage files explicitly

**Never use `gt create -a` or `git add -A`** — both can sweep in plan/scratch markdown files (`*_plan.md`, `plan_*.md`) and other working docs that must not be pushed. Stage by name:

```bash
git add <file1> <file2> ...
```

Review the staged list with `git status` before continuing.

### 4. Determine stack position

- Default: stack on top of the current branch. Run `gt log` to verify.
- If the user says "on top of #245" or "after the auth PR", `gt checkout` that branch first.
- If there's no existing stack, the base is `main`.

### 5. Create branch + commit

Use [Conventional Commits](https://www.conventionalcommits.org/) — derive type and scope from context. Subject under 72 chars, no trailing period.

```bash
XDG_DATA_HOME="$TMPDIR/gt-data" XDG_CONFIG_HOME="$TMPDIR/gt-config" \
  gt create -m "feat(scope): short subject" --name <author>__<short-description>
```

Examples: `feat(cache): add Redis caching layer`, `fix(snapshots): correct generation logic for edge cases`, `refactor(auth): simplify middleware token validation`.

### 6. Run CI checks

Reproduce every `.github/workflows/ci.yml` job locally before submitting. If any check fails, fix → `gt modify` to amend → re-run.

```bash
uv run black --check .     # format
uv run ruff check .        # lint
uv run mypy bedrock        # typecheck
uv run pytest              # unit tests
```

Repo-wide, not changed-files-only — that is what CI runs, and a scoped `mypy` can miss breakage in an importing module. Integration tests (`uv run pytest -v -m eeio_integration`) hit GCS and run in a separate workflow; run them when the change touches the model pipeline, snapshots, or IO.

Do not push code that fails any of these.

### 7. Submit PR

```bash
XDG_DATA_HOME="$TMPDIR/gt-data" XDG_CONFIG_HOME="$TMPDIR/gt-config" \
  gt submit --no-interactive --publish
```

`gt submit` derives the PR title from the commit subject. It does **not** accept `--title` or `--body` flags, and in `--no-interactive` mode (the only mode available without a TTY) it leaves the GitHub PR template in place rather than populating the body. Patch the body in step 8.

### 8. Patch PR body via REST API

Draft the body against the template below, verify the length budget, then patch it on:

```bash
TOKEN=$(echo -e "protocol=https\nhost=github.com" | git credential fill 2>/dev/null \
  | grep '^password=' | cut -d= -f2)
BODY=$(cat <<'EOF'
<the filled template>
EOF
)
sed '/^<details>/,$d' <<<"$BODY" | wc -w   # must be <= 150; over budget means cut, not reword
JSON=$(uv run python -c "import json,sys; print(json.dumps({'body': sys.stdin.read()}))" <<<"$BODY")
curl -sS -X PATCH \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept: application/vnd.github+json" \
  -d "$JSON" \
  https://api.github.com/repos/cornerstone-data/bedrock/pulls/<PR#>
```

`git credential fill` reuses the credentials git already uses for `git push`. `api.github.com` is in Claude Code's default network allowlist.

If `gt submit` later updates the same PR (e.g. after `gt modify`), the patched body is preserved — Graphite only touches the body when creating new PRs interactively.

#### Template

```markdown
cc:
Closes:

**Stack:** step <n> of <N> — <stack goal, 8 words max> (<prev> → this → <next>)
**Plan:** <link to the plan / ladder doc>

## What changed? Why?

<2–3 sentences, present tense. Mechanism, not archaeology. Backticks for paths, identifiers, flags.>

## Impact

<Exactly 1 sentence on this repo's blast-radius axis: which configs' emission factors move.>

## Review focus

- <Up to 3 bullets, 1 line each: what to check, and what is verbatim or mechanical and safe to skim.>

## Testing

<Exactly 1 sentence: suites plus lint/type. Don't invent tests.>

## Heads-up

<1–2 lines: stale cache, rerun order, sequencing.>

<details>
<summary>Evidence</summary>

<Before/after values, per-sector N/D deltas, tables, logs. Uncapped.>

</details>
```

#### Which sections to include

Always: `What changed? Why?`, `Impact`, `Testing`. The rest are gated — omit the heading entirely rather than writing "none":

| Section        | Include only if                                   |
| -------------- | ------------------------------------------------- |
| `**Stack:**`   | the stack holds more than one PR                  |
| `**Plan:**`    | a plan or ladder doc for this work exists in-repo |
| `Review focus` | something non-obvious needs a human's eyes        |
| `Heads-up`     | a human must _do_ something before or after merge |
| `Evidence`     | there are real numbers to show                    |

`cc:` stays blank unless the user names someone; `Closes:` unless an issue # is known.

`Impact` is the one repo-specific slot: here it names which configs' emission factors move and whether the snapshot default shifts. Two worked examples:

> Moves N and D under `2025_usa_cornerstone_v0_3`; `full_model` and `.SNAPSHOT_KEY` untouched.

> No output change — refactor only, no config or snapshot touched.

Another repo swaps that sentence for its own axis; the skeleton doesn't change.

#### Length

Caps are in sentences because sentences are countable: 2–3 for `What changed? Why?`, exactly 1 for `Impact` and `Testing`, at most 3 one-line bullets for `Review focus`, 1–2 lines for `Heads-up`.

Everything above `<details>` must total **≤150 words** — run the `wc -w` line above before patching (it anchors on `^<details>`, so an inline mention of the tag in prose doesn't truncate the count). Over budget, move detail into the fold; the fold is uncapped, so cutting is lossless. Don't reword to squeeze under.

#### Rules

- **Write for the reviewer's decision, not the author's discovery.** Say what to check and what's a verbatim copy worth skimming. Why the old code was wrong belongs in a code comment or the plan doc — link it, don't retell it.
- **No `## Also in this PR`.** Unrelated work is either one `Review focus` bullet or a sign the PR should split.
- **Evidence is numbers, not narration.** Test counts and lint status compress to one line; before/after values go in the fold.
- **No dated correction notes** and no "what changed since review" sections — the body states current truth.

---

## Key `gt` Commands

| Command                                | Purpose                                            |
| -------------------------------------- | -------------------------------------------------- |
| `gt log`                               | View current stack                                 |
| `gt create -m "msg" --name <branch>`   | Commit staged files and create branch in stack     |
| `gt modify -m "new msg"`               | Amend the current commit (message and/or contents) |
| `gt submit --no-interactive --publish` | Push branch and open PR                            |
| `gt sync`                              | Sync stack with remote                             |
| `gt restack`                           | Rebase stack after upstream changes                |
| `gt checkout <branch>`                 | Switch branches in the stack                       |

Prefer `gt` over plain `git` whenever an equivalent exists.

---

## Output Checklist

For every stacked-PR task, Claude must output:

1. ✅ `gt create` — commit message and branch name filled in
2. ✅ CI checks pass — black, ruff, mypy, pytest all clean (fix and `gt modify` if not)
3. ✅ `gt submit` — actually run, returning the PR URL
4. ✅ PR body — step 8 template patched onto the PR via REST, gated sections omitted, ≤150 words above the fold (`wc -w` verified)

Do NOT post stack comments — Graphite manages these automatically.
