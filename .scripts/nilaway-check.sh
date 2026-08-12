#!/usr/bin/env bash

set -euo pipefail

readonly NILAWAY_VERSION="v0.0.0-20260808063849-8649a03c818a"

target="${1:-./...}"

echo "Running nilaway nil-panic analysis (go.uber.org/nilaway@${NILAWAY_VERSION})..."

report="$(go run "go.uber.org/nilaway/cmd/nilaway@${NILAWAY_VERSION}" "${target}" || true)"
if [[ -n "${report}" ]]; then
    echo "❌ nilaway found potential nil-panic site(s):"
    printf '%s\n' "${report}"
    exit 1
fi

echo "✅ Nilaway gate passed"
