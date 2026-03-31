.PHONY: build test clean fmt clippy check install-hooks

build:
	cargo build

test:
	cargo test

check: fmt clippy test

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
