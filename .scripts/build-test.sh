#!/usr/bin/env bash

set -euo pipefail

echo "📦 Building..."
if ! go build -v ./...; then
    echo "❌ Build failed"
    exit 1
fi
echo "✅ Build successful"

echo "🧪 Running tests..."
if ! go test -v -short -failfast ./...; then
    echo "❌ Tests failed"
    exit 1
fi
echo "✅ Tests passed"
