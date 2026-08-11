# One-time repo setup: activate git hooks in .hooks/
setup-hooks:
    git config core.hooksPath .hooks

# Run code validation scripts (lint, then deadcode)
validate:
    ./.scripts/code-validate.sh

# Run the build and test script
build-test:
    ./.scripts/build-test.sh

# Report dead/unreachable code (library has no main package, see script for what this checks)
deadcode:
    ./.scripts/deadcode-check.sh

# Auto-fix every lint finding the linters can repair themselves
format dir="./...":
    golangci-lint run --fix {{ dir }}

# Build the module. Pass a dir to narrow the build (default: ./...)
build dir="./...":
    go build -v {{ dir }}

# Run tests. Example: just test dir=./internal/...
test dir="./...":
    go test -v {{ dir }}

# Run tests through gotestsum with noisy passing output filtered out
test-short dir="./...":
    @gotestsum --format standard-quiet --no-color -- {{ dir }} 2>&1 | grep -vE "^(ok|[[:space:]]*\?|.*Redismq|PASS)"

# Shellcheck the git hooks and the shell scripts that back them
lint-sh:
    shellcheck .hooks/pre-commit .hooks/pre-push .scripts/code-validate.sh .scripts/deadcode-check.sh .scripts/nilaway-check.sh .scripts/build-test.sh

# Report every site where the root error never reaches the logs, or collides with the cause key
lint-dropped-err dir="./...":
    go run ./.tools/droppederr/main.go {{ dir }}

# Report every potential nil-panic site
lint-nilaway dir="./...":
    ./.scripts/nilaway-check.sh {{ dir }}
