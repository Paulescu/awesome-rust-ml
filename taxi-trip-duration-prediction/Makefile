format:
	@cargo fmt --quiet

lint:
	@rustup component add clippy 2> /dev/null
	@cargo clippy --all-targets --all-features -- -D warnings 

run:
	@cargo run --quiet