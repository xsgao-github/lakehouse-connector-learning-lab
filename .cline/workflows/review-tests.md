# Workflow: Test Coverage & Edge-Case Auditor

## Description
Audits unit and integration test coverage for core reading strategies and generates test cases for unhandled edge cases.

## Execution Steps
1. Identify core classes modified in the current branch (e.g., `ParallelReadingStrategy.java`, `ReliableReadingStrategy.java`).
2. Locate or create corresponding test files under `src/test/java/`.
3. Audit test coverage for key operational edge cases:
- **Partition Skew:** Records concentrated in a single partition while others are empty.
- **Integer Truncation & Division:** Odd record limit divided across threads (e.g., limit 10 across 3 threads).
- **Poison Pills & Malformed Payloads:** Null records, missing CSV fields, corrupted headers.
- **Empty Poll Handling:** Partitions returning no records without causing infinite loops.
- **Resource Leakage:** Verify consumer closure and executor pool shutdown.

4. Implement missing JUnit 5 / AssertJ test methods directly into the test suite and verify execution via `mvn test`.
