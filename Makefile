.PHONY: build test clean fmt clippy check

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
