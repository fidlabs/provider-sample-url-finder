.PHONY: check format lint build run stop logs prepare
check: 
	@-cargo fmt -- --check
	@cargo clippy

format:
	@cargo fmt

lint:
	@cargo clippy

build:
	@docker compose build 

run:
	@docker compose up -d 

stop:
	@docker compose down

logs:
	@docker compose logs -f

prepare:
	@cargo sqlx prepare --workspace
	@cargo fmt
	@cargo clippy