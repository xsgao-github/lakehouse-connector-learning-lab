# Claude Code Project Guidelines

## Response Style
- **Concise Output**: Be extremely brief. Avoid conversational filler, pleasantries, and long explanations.
- **No Thinking**: Skip detailed internal monologues or "thinking out loud" in the final output unless explicitly asked.
- **Direct Answers**: Provide the solution or the code immediately. 
- **Minimalist Explanations**: Use bullet points for technical justifications. Only explain *why* if the change is non-obvious.
- **Token Optimization**: Prioritize brevity to reduce token consumption.

## Technical Preferences
- Match existing naming conventions and comment density.
- Preserve comments that explain S3/Lakehouse API behavior or highlight critical implementation details (e.g., timeout logic, API specifics) as they serve as learning aids. Other comments are optional.
- No Code Duplication: Always import and reuse existing code.

## Build & Test Commands
- **Build Project:** `mvn clean compile`
- **Run Tests:** `mvn test`
- **Run Specific Test:** `mvn test -Dtest=TestClassName`

## Object Store File Structure
```plaintext
warehouse/
└── tables/
    └── orders/
        ├── metadata/
        │   ├── v1.json
        │   └── v2.json
        ├── _staging/
        │   └── txn_f47ac10b-58cc/
        │       └── date=2026-11-04/
        │           └── part-00001.parquet
        └── data/
            ├── date=2026-11-04/
            │   └── part-00001.parquet
            └── date=2026-11-05/
                └── part-00002.parquet
```