---
name: graphite-stacked-prs
description: Use this skill whenever the user wants to commit code changes to GitHub using Graphite in stacked PR format, create a new branch in a stack, or submit PRs via Graphite. Trigger when the user mentions "Graphite", "stacked PR", "gt submit", "gt create", "stack this", "commit and push with Graphite", or asks to create a PR on top of an existing one. Claude should autonomously generate commit messages, branch names, and determine stack position without asking the user.
---

# Graphite Stacked PRs Skill

This skill governs how Claude helps commit code and create stacked PRs via Graphite CLI (`gt`).

Claude acts autonomously: it **generates the commit message, branch name, and determines stack position** based on context — no need to prompt for these.

---

**Note:** Stack comments are managed automatically by Graphite — Claude must never post or edit stack comments.

---

## Claude's Autonomous Decisions

### Branch naming
Use the format `<author>__<short-description>` with kebab-case for the description.

**Detecting the author prefix:** Run `git config user.name` to get the current user, then run `gt log` or `git branch` and look at existing branch prefixes (the part before `__`) to find how that person's name maps to a prefix. For example, if `git config user.name` returns "Mo Li" and you see branches like `mo__fix-something`, the prefix is `mo`. If you see `btobin__add-feature` for "Brian Tobin", the prefix is `btobin`.

Cache this value at the start of the workflow — never hardcode or guess a prefix format.

**Examples** (assuming author is `mo`):
- "fix the snapshot generation bug" → `mo__fix-snapshot-generation`
- "add Redis caching to the data loader" → `mo__add-redis-caching`
- "refactor auth middleware" → `mo__refactor-auth-middleware`

Keep branch names short (3–5 words max after the `<author>__` prefix).

### Commit messages
Use [Conventional Commits](https://www.conventionalcommits.org/) format. Derive type and scope from context:
- `feat(cache): add Redis caching layer`
- `fix(snapshots): correct generation logic for edge cases`
- `refactor(auth): simplify middleware token validation`

Subject line under 72 characters. No period at end.

### Stack position
- Default: stack on top of the current branch (`gt log` to verify).
- If the user says "on top of #245" or "after the auth PR", check out that branch first before running `gt create`.
- If there's no existing stack, the base is `main`.

---

## PR Body Template

Every PR description (the `--body` passed to `gt submit`) must follow this template. Claude fills in all content based on context.

```
cc:
Closes:

## What changed? Why?

<concise explanation of what was changed and the reason — 1 to 3 sentences, using inline code for file paths, flags, and identifiers>

## Testing

<brief description of how this was tested or will be tested>
```

**Rules:**
- `cc:` — leave blank unless user specifies someone to notify.
- `Closes:` — fill in related issue number if known (e.g., `Closes: #123`), otherwise leave blank.
- **What changed? Why?** — factual, specific, technical. Mention file paths, parameter names, flags changed. No fluff.
- **Testing** — a one-liner is fine (e.g., "will run snapshots generation"). Don't invent tests not described.
- Use inline backticks for code identifiers, file paths, and CLI flags.

**Example:**
```
cc:
Closes:

## What changed? Why?

Removed the `adhoc` input parameter from the GitHub workflow for generating snapshots and updated the script path from `bedrock/publish/snapshots/generate_snapshots.py` to `bedrock/utils/snapshots/generate_snapshots.py`. The `--adhoc` flag is no longer passed to the script execution.

## Testing

will run snapshots generation
```

---

## Full Workflow

### Creating a new stacked PR

```bash
# 1. Check current stack position
gt log

# 2. Create branch + commit (Claude fills in message and branch name)
gt create -a -m "fix(snapshots): correct generation logic" --name fix/snapshot-generation

# 3. Submit PR — Claude generates the full body using the template above
gt submit --title "Fix snapshots generation"
# Then paste the PR body Claude generated into the GitHub editor
```

### After PR is created

Steps after `gt submit`:
1. Edit the PR body on GitHub to match the filled-in template Claude provides (using the `cc / Closes / What changed / Testing` template).
2. **Do NOT post a stack comment** — Graphite automatically posts and maintains stack comments on all PRs when you use `gt submit`.

---

## Key Graphite CLI Commands

| Command | Purpose |
|---|---|
| `gt log` | View current stack |
| `gt create -a -m "msg" --name branch-name` | Stage all, commit, create branch in stack |
| `gt submit --title "T" --body "B"` | Open PR for current branch |
| `gt sync` | Sync stack with remote |
| `gt restack` | Rebase stack after upstream changes |
| `gt checkout <branch>` | Switch to a branch in the stack |
| `gt modify -m "new msg"` | Amend commit message |

**Always use `gt` commands — never plain `git` when a `gt` equivalent exists.**

---

## Claude Output Checklist

For every stacked PR task, Claude must output all of the following as ready-to-copy blocks:

1. ✅ `gt create` command — with commit message and branch name filled in
2. ✅ `gt submit` command — with PR title filled in
3. ✅ **PR body** — filled-in `cc / Closes / What changed? Why? / Testing` template, to paste into the GitHub PR description

**Note:** Do NOT post stack comments — Graphite manages these automatically.