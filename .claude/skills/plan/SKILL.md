---
name: plan
description: Create a detailed implementation plan and save to .claude/plan/
disable-model-invocation: false
argument-hint: [task or feature description]
---

Create a comprehensive implementation plan for: $ARGUMENTS

## Instructions

1. Research the codebase to understand relevant context
2. Break down the work into phases and individual tasks
3. Identify files affected and dependencies between phases
4. Include a testing strategy
5. **Always save the plan to `.claude/plan/`** — create the directory if needed (`mkdir -p .claude/plan`)
6. Use a descriptive filename: `.claude/plan/<feature_name>.md`
