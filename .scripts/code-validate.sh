#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"

export GOLANGCI_LINT_CACHE="${REPO_ROOT}/.cache/golangci-lint"

echo "✔️ Running Linter..."
if ! golangci-lint run; then
    echo "❌ golangci-lint found issues"
    exit 1
fi
echo "✅ Linter passed"
