#!/usr/bin/env bash

set -euo pipefail

readonly DEADCODE_VERSION="v0.46.0"

echo "Running deadcode analysis (golang.org/x/tools/cmd/deadcode@${DEADCODE_VERSION})..."
echo "This is a library, not a service: there is no main package, so a main-only-roots"
echo "pass would treat the entire exported API as unreachable and fail permanently."
echo "Only the -test-rooted pass runs — it flags functions nothing calls, including tests."

report="$(go run "golang.org/x/tools/cmd/deadcode@${DEADCODE_VERSION}" -test ./...)"
if [[ -n "${report}" ]]; then
    echo "❌ deadcode found unreachable function(s):"
    printf '%s\n' "${report}"
    exit 1
fi

echo "✅ Deadcode gate passed"
