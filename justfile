# One-time repo setup: activate git hooks in .hooks/
setup-hooks:
    git config core.hooksPath .hooks

# Run code validation (golangci-lint)
validate:
    ./.scripts/code-validate.sh

# Run the build and test script
build-test:
    ./.scripts/build-test.sh

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
    shellcheck .hooks/pre-commit .hooks/pre-push .scripts/code-validate.sh .scripts/nilaway-check.sh .scripts/build-test.sh .scripts/ai-gen-context.sh

# Report every potential nil-panic site
lint-nilaway dir="./...":
    ./.scripts/nilaway-check.sh {{ dir }}

# Update all AI context files
ai-update:
    @./.scripts/ai-gen-context.sh
