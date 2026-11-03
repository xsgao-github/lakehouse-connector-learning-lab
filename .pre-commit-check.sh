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

# 3. Ensure the environment setup is runnable
echo "Verifying setup script syntax..."
python -m py_compile setup_env.py
if [ $? -ne 0 ]; then
    echo "❌ setup_env.py has syntax errors."
    exit 1
fi
echo "✅ setup_env.py syntax is correct."

echo "----------------------------------------"
echo "🎉 All checks passed! Ready to commit."
exit 0
