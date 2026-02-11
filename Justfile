# dynamo-kad task runner
# Install just: https://github.com/casey/just

# Build all crates
build:
    cargo build --workspace

# Run all tests
test:
    cargo test --workspace

# Run clippy with deny warnings
clippy:
    cargo clippy --workspace --all-targets -- -D warnings

# Format all code
fmt:
    cargo fmt --all

# Check formatting (CI mode)
fmt-check:
    cargo fmt --all -- --check

# Run all benchmarks
bench:
    cargo bench --workspace

# Full CI check: format + lint + test
check: fmt-check clippy test

# Run a node with the given config file
run config="config.yaml":
    cargo run -p dynamo-node -- {{config}}
