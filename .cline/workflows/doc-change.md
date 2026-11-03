# Workflow: Branch Change Documentation Generator

## Description
Analyzes git diff against main and generates or updates module documentation (e.g., `module-4.md`).

## Execution Steps
1. Run `git diff main...HEAD --name-status` to identify modified, added, or deleted files in the branch.
2. Inspect diff details on core files in `src/main/java/` and helper scripts in `scripts/`.
3. Extract key technical implementations, schema changes, and strategy updates.
4. Draft/update the corresponding `module-X.md` file using the standard project layout:
- **Overview & Objectives**: Core problem solved and key strategy introduced.
- **Technical Implementation Details**: Concise code snippets highlighting key logic.
- **Environment & Seeding Pipeline**: Updated helper scripts or Docker commands.
- **Execution & Verification**: Exact terminal commands to build and run (`mvn package`, `java -jar ...`).
- **Key Takeaways & Best Practices**: 2-3 engineering guidelines learned.



