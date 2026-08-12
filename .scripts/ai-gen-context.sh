#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

OUT_DIR=".ai-docs"
STRUCTURE_FILE="${OUT_DIR}/structure.txt"
SYMBOLS_FILE="${OUT_DIR}/symbols.tags"

FD_BIN="$(command -v fd || command -v fdfind)" || {
    echo "❌ fd/fdfind not found" >&2
    exit 1
}

mkdir -p "${OUT_DIR}"

echo "📂 Mapping project structure..."
"${FD_BIN}" -t d -H -E .git -E .cache -E "${OUT_DIR}" > "${STRUCTURE_FILE}"

echo "🏷️ Extracting Go symbols..."
ctags -R \
    --languages=Go \
    --kinds-go=fmit \
    --fields=+S \
    --exclude="${OUT_DIR}" \
    -f "${SYMBOLS_FILE}" . 2>/dev/null || echo "⚠️ Ctags finished with warnings (ignored)"

echo "✅ AI Context updated in ${OUT_DIR}/"
