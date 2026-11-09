.PHONY: build test test-scripts clean fmt clippy check install-hooks

build:
	cargo build

test:
	cargo test

check: fmt clippy test test-scripts

# Shell tests. They need git, bash and the POSIX text tools, not the Rust
# toolchain or the submodules.
test-scripts:
	bash tests/shell/check_review_loop.sh
	bash tests/shell/check_reviewer_files.sh

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
