---
name: graphite-stacked-prs
description: Use this skill whenever the user wants to commit code changes to GitHub using Graphite in stacked PR format, create a new branch in a stack, submit PRs via Graphite, or add/update the standardized stack comment on a PR. Trigger when the user mentions "Graphite", "stacked PR", "gt submit", "gt create", "stack this", "commit and push with Graphite", or asks to create a PR on top of an existing one. Also trigger when the user asks to update the stack comment on an open PR. Claude should autonomously generate commit messages, branch names, and determine stack position without asking the user.
---

# Graphite Stacked PRs Skill

This skill governs how Claude helps commit code, create stacked PRs via Graphite CLI (`gt`), and maintain a standardized standalone stack comment on every PR.

Claude acts autonomously: it **generates the commit message, branch name, and determines stack position** based on context — no need to prompt for these.

---

## Stack Comment Format

Every PR **must** have a standalone comment (not in the PR body) listing the full stack. This comment is updated as new PRs are added.

### Structure

Each PR entry is a single bullet with the `#number` (GitHub auto-links it and renders the PR title). Stack is listed **top to bottom** (newest PR first). The bottom entry is always `• `main``. Followed by the standard footer.

**Markers:**
- `👈` = points to the **current PR** in the stack (only one PR gets this).
- `(View in Graphite)` = hyperlinked, only on the current PR, after `👈`.

**Single PR stack (posted as comment on PR #245):**
```
• #245 👈 [(View in Graphite)](https://app.graphite.dev/github/pr/cornerstone-data/bedrock/245)
• `main`

This stack of pull requests is managed by [Graphite](https://graphite.dev). Learn more about [stacking](https://graphite.dev/docs/stacking).
```

**3-PR stack (posted as comment on PR #246):**
```
• #247
• #246 👈 [(View in Graphite)](https://app.graphite.dev/github/pr/cornerstone-data/bedrock/246)
• #245
• `main`

This stack of pull requests is managed by [Graphite](https://graphite.dev). Learn more about [stacking](https://graphite.dev/docs/stacking).
```

**Rules:**
- Comment is **standalone** — posted as a PR comment, NOT in the PR description body.
- Use `#number` only — GitHub auto-links it and renders the PR title.
- Only the **current PR** (the one this comment is posted on) gets `👈 (View in Graphite)`.
- The same stack comment is posted on ALL PRs in the stack, but with `👈` pointing to the respective current PR.
- Always end with `• `main`` as the last bullet.
- When a new PR is added on top, **edit this comment on ALL existing PRs** in the stack to add the new entry (keeping `👈` on the correct PR for each).

---

## Claude's Autonomous Decisions

### Branch naming
Derive from the commit intent using kebab-case with a `feat/`, `fix/`, `refactor/`, etc. prefix:
- "fix the snapshot generation bug" → `fix/snapshot-generation`
- "add Redis caching to the data loader" → `feat/redis-caching`
- "refactor auth middleware" → `refactor/auth-middleware`

Keep branch names short (3–5 words max).

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

### After PR is created — two separate GitHub actions

There are **two distinct outputs** that go in different places:

**① PR body** — the description visible when you open the PR. Uses the `cc / Closes / What changed / Testing` template. Set via `gt submit` or edited directly in GitHub.

**② Stack comment** — a separate standalone comment posted *after* the PR is open. Uses the bullet-list stack format. This is NOT part of the PR body.

Steps after `gt submit`:
1. Edit the PR body on GitHub to match the filled-in template Claude provides.
2. Post the stack comment Claude generates as a **new comment** on the PR.
3. On all **previous PRs** in the stack → **edit their existing stack comment** to prepend the new PR at the top.

### Updating an existing stack comment

When a new PR is added on top:
1. Claude generates the updated full stack comment with the new PR prepended.
2. Edit the stack comment on each existing PR in the stack.
3. Post the same updated comment on the new PR.

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
4. ✅ **Stack comment** — bullet-list format, to post as a standalone GitHub comment (separate from the body)
5. ✅ If stacking on top of existing PRs: the updated stack comment to edit onto all prior PRs in the stack