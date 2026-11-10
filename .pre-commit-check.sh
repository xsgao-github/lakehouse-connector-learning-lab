#!/bin/bash

echo "--- 🛠️ Running Pre-commit Checks ---"

# 1. Check for compilation errors
echo "Checking compilation..."
mvn clean compile
if [ $? -ne 0 ]; then
    echo "❌ Compilation failed. Please fix errors before committing."
    exit 1
fi
echo "✅ Compilation successful."

# 2. Check for common formatting/style issues (basic check)
echo "Checking for trailing whitespace..."
if git diff --cached | grep -q '[[:space:]]$'; then
    echo "❌ Trailing whitespace found in staged files."
    exit 1
fi
echo "✅ No trailing whitespace found."

echo "----------------------------------------"
echo "🎉 All checks passed! Ready to commit."
exit 0
