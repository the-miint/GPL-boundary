.PHONY: build test test-scripts clean fmt clippy check install-hooks

build:
	cargo build

test:
	cargo test

check: fmt clippy test test-scripts

# The shell scenario tests for scripts/ and the review tooling. They need git and
# bash only, not the Rust toolchain or the submodules.
test-scripts:
	bash tests/shell/check_review_loop.sh

fmt:
	cargo fmt -- --check

clippy:
	cargo clippy -- -D warnings

clean:
	cargo clean

install-hooks:
	cp scripts/pre-commit .git/hooks/pre-commit
	chmod +x .git/hooks/pre-commit
	@echo "Pre-commit hook installed."
