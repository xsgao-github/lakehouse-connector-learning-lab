# Workflow: Code Style, Typo \& Linting Reviewer

## Description
Audits modified files for typos, code style inconsistencies, and executes local formatting tools.

## Execution Steps
1. Identify modified files using `git status --short`.
2. Inspect changed files for:
- Spelling errors in comments, variable names, log output, and markdown documentation.
- Naming conventions: `camelCase` for variables/methods, `PascalCase` for classes, `UPPER\_SNAKE\_CASE` for constants.
- Do **NOT** check code logics and syntax.

3. Run automated formatting and pre-commit checks in the terminal:
- `pre-commit run --all-files` (or `mvn spotless:apply` for Java if configured).

4. Provide a summary report listing fixed typos, applied auto-formatting, and remaining manual recommendations.

