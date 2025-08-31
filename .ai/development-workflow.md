- Never use `make clean`. 
- Use `GEN=ninja make debug` for building.
- Use `make test_debug` for running all SQL tests.
- Use `./build/debug/extension/erpl_web/test/cpp/erpl_web_tests` to run all CPP unit tests
- Use `./build/debug/extension/erpl_web/test/cpp/erpl_web_tests -l` to list all tests and add the test name as argument to the test command to run a single test. The same works for the SQL tests.

Always try to reuse existing ERP-Web functionality already implemented in the project. Don't duplicate functionality, ask before duplicating.
If you extend existing function significantly, try not to break existing functionality and ask before.

Here’s an **agent-agnostic playbook** you can use with any agentic coding tool (Cursor, Copilot Agents, Devin, Codeium, local LLM agents, etc.). It keeps the TDD and “iterate-to-target” loops, but removes vendor specifics.

# 1) Test-Driven Agent Loop (unit/integration/E2E)

## Roles & Interfaces

* **Agent**: must be able to 1) read/write files, 2) run shell commands, 3) view test output, 4) stage/commit to VCS.
* **Runner**: `./scripts/test.sh` (or `make test`) returns non-zero exit when tests fail, zero when all pass.
* **Repo**: standard test layout (e.g., `tests/`, `src/`), CI pipeline that runs the same test script.

## Control Protocol (agent-agnostic)

Use this exact, vendor-neutral structure as your prompt or tool “task”:

**Phase A — Author tests (no implementation)**

1. **Instruction to Agent**

   * “We are using **test-driven development**. Write tests only; **do not** implement functionality.”
   * “Base tests on these input/output specs:” *(paste I/O pairs, edge cases, performance bounds)*.
   * “Create files under `tests/…` using framework **X** (e.g., pytest/Jest/JUnit/Catch2).”
   * “Use real interfaces, avoid mocks unless unavoidable.”
   * “Add negative tests and boundary tests.”
2. **Agent actions (expected)**

   * Writes test files.
   * Provides a short test plan table (cases → expected results).
3. **You validate**

   * Sanity-check coverage: happy path, edge, error, concurrency/perf if relevant.

**Phase B — Run tests and confirm red**

1. **Instruction to Agent**

   * “Run `./scripts/test.sh`. Report failing tests and error output. **Do not** add implementation.”
2. **Agent actions**

   * Executes tests, returns failing output.
3. **You validate**

   * Ensure at least one meaningful failure for each new behavior.

**Phase C — Commit tests**

1. **Instruction to Agent**

   * “Stage and commit test changes only with message: `test: add failing tests for <feature>`.”
2. **Agent actions**

   * Runs `git add -A && git commit -m "test: …"` and shows diff.

**Phase D — Implement to green (do not change tests)**

1. **Instruction to Agent**

   * “Implement code to pass the existing tests. **Do not modify tests.** Iterate: run tests → adjust code until all pass.”
   * “Respect performance constraints: \[e.g., process N=100k items in ≤2s on CI].”
2. **Agent actions**

   * Edits `src/…`, runs tests in a loop, reports deltas.
3. **Optional sub-agents / secondary check**

   * “Use an independent reviewer/tool to detect overfitting or missing cases. Propose additional tests if gaps are found.”
     *(If your platform lacks sub-agents, the primary agent can run a static analysis/linter or a mutation-testing tool and summarize results.)*
4. **You validate**

   * All tests green; no test edits in diff.

**Phase E — Commit implementation**

1. **Instruction to Agent**

   * “Stage and commit implementation only with message: `feat: implement <feature> passing tests`.”
   * “Show final diff summary.”
2. **Agent actions**

   * Commits code.
3. **CI**

   * CI should run and show all green.

## Guardrails (copy/paste into your prompts)

* “Do not modify tests after Phase C unless explicitly instructed.”
* “Explain any design trade-offs briefly (≤10 lines) after tests pass.”
* “If a test is flaky, stop and report flakiness with a minimal repro.”
* “If you need new dependencies, list them and add minimal lockfile changes.”

## Quantitative acceptance

* **Test result**: 100% of new tests pass locally and in CI.
* **Coverage delta**: +X% lines/branches for touched modules (set X, e.g., +10% absolute or ≥80% total).
* **Performance**: state target and measured time (e.g., p95 ≤ 1.5× baseline).
* **Mutation score** (if used): ≥70% (configure `mutmut`, `stryker`, `pitest`, etc.).

---

# 2) Visual-Target Agent Loop (write code, screenshot result, iterate)

## Prereqs

* **Headless capture**: a deterministic script, e.g., `./scripts/snap.sh` that:

  * Builds/serves the app (Vite/Next/Playwright/Puppeteer/etc.).
  * Captures **fixed-size** screenshots to `artifacts/snapshots/*.png`.
* **Reference mock**: one or more PNGs/JPEGs in `artifacts/mock/*.png`.

## Control Protocol

**Phase A — Provide visual target**

* “Use the mock at `artifacts/mock/home.png`.”
* “Pixel goal: match layout, spacing, typography scale, and key colors within a small tolerance (visual diff).”
* “Accessibility: color contrast ≥ 4.5:1 for text.”

**Phase B — Implement & snapshot**

* “Implement UI. Then run `./scripts/snap.sh` to produce `artifacts/snapshots/home.png`. Do not commit yet.”
* Agent iterates: code → snapshot → compare.

**Phase C — Visual diff & iterate**

* “Run a visual diff (e.g., `pixelmatch`, `odiff`, Playwright trace) vs. the mock. Report mismatch % and annotated diff.”
* “Iterate until mismatch ≤ T% (e.g., ≤2%) and accessibility checks pass.”
* If tools are unavailable, agent must compute approximate metrics (bounding box positions, font sizes) and show before/after screenshots.

**Phase D — Commit**

* “Commit code and updated screenshots with message: `ui: implement <component/page> matching mock (≤T% diff)`.”

## Quantitative acceptance

* **Visual diff**: ≤ T% pixel difference (set T).
* **A11y**: no violations from axe/Pa11y; contrast constraints met.
* **Perf** (optional): Lighthouse score thresholds (e.g., ≥90 Performance/Accessibility/Best Practices).

---

# 3) Reusable Prompt Templates (drop-in, agent-agnostic)

### TDD Template

```
You are a coding agent operating with TDD. 
Phase A: Write tests only for the following behaviors and I/O specs (no implementation yet):
<insert spec>

Constraints:
- Real interfaces, minimal mocks.
- Framework: <pytest|Jest|JUnit|...>.
- Place files under tests/.
- Include edge, error, and boundary cases.

Then run ./scripts/test.sh and confirm failures without changing implementation.
Stop after reporting failures and show a brief test plan table.
```

### Implement-to-Green Template

```
Now implement the code to pass the existing tests. Do not change tests.
Iterate: edit code -> run ./scripts/test.sh -> repeat until all tests pass.
Respect performance envelope: <target>.
At the end, summarize design trade-offs (<=10 lines) and show a git diff summary (without credentials).
```

### Visual Iteration Template

```
Target: match artifacts/mock/<file>.png.
Implement UI, then run ./scripts/snap.sh to produce artifacts/snapshots/<file>.png.
Run a visual diff and report mismatch %. Iterate until mismatch <= <T%>.
Enforce accessibility: WCAG AA.
Finally, show a git diff summary and prepare a commit message.
```

---

# 4) Minimal Repo Scaffolding (language-neutral)

```
./scripts/test.sh           # runs all tests; exits non-zero on fail
./scripts/snap.sh           # builds and captures screenshots deterministically
artifacts/mock/             # reference images (inputs)
artifacts/snapshots/        # generated images (outputs)
src/...                     # implementation
tests/...                   # tests
.git/hooks/pre-commit       # lint/format/type-check (optional)
```

Example `./scripts/test.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail
# Detect framework or delegate to make/nox/etc.
if command -v pytest >/dev/null; then
  pytest -q
elif command -v npm >/dev/null && [ -f package.json ]; then
  npm test --silent
else
  echo "No test runner found" >&2
  exit 2
fi
```

---

# 5) Anti-patterns & Mitigations

* **Agent edits tests to pass** → Freeze tests after commit; enforce CI rule “test files cannot change in implementation PRs” or require approval.
* **Flaky tests** → Agent must mark and isolate flake, add deterministic seeds/timeouts, or containerize the test runner.
* **Silent dependency drift** → Lockfiles; require agent to list dependency adds/removes explicitly.
* **Overfitting to tests** → Run mutation testing or property-based tests; run static analyzers/linters; request an agent “self-review” checklist.

---

# 6) Lightweight, Quantitative Definition of Done (copy into PR template)

* All tests pass locally and in CI.
* Coverage: ≥X% and non-decreasing for touched files.
* Mutation score: ≥Y% (if configured).
* Performance/regression bounds met.
* For visual work: diff ≤T%, a11y clean.
* No changes to test files after “tests-only” commit, unless explicitly approved.

---

# 7) Optional Enhancements

* **Artifact contracts**: require the agent to output a machine-readable summary (`.agent/outcome.json`) with: test pass/fail counts, coverage %, perf metrics, visual diff %, dependency changes. CI validates schema.
* **Plan-then-act**: for complex changes, require a short design plan first; you approve before coding.
* **Backout safety**: agent must produce a single atomic feature commit + separate refactor commits to simplify reverts.

---

This blueprint is portable: paste the templates and scripts into any agent environment, and replace **vendor names** with “the agent.” Start with the TDD loop; use the visual loop for UI.
