# Makefile
include .env
export

.PHONY: check format lint build run stop logs prepare test
.PHONY: run-db stop-db clean-db clear-db logs-db exec-db
.PHONY: migration migrate-up migrate-down migrate-status init-dev init-dev-db

check:
	@cargo fmt -- --check
	@cargo clippy

format:
	@cargo fmt

lint:
	@cargo clippy

test:
	@SQLX_OFFLINE=true cargo test --workspace

# Docker app commands
build:
	@docker compose build
run:
	@docker compose up -d
stop:
	@docker compose down
logs:
	@docker compose logs -f

# Docker Postgres commands
run-db:
	@docker compose up -d postgres
stop-db:
	@docker compose down postgres
exec-db:
	@docker compose exec postgres psql -U postgres -d uf
clean-db: clear-db
clear-db:
	@docker compose down -v postgres
logs-db:
	@docker compose logs -f postgres

# Database migration commands
migration:
	@sqlx migrate add -r --source migrations $(name)
migrate-up:
	@sqlx migrate run --source migrations --database-url $(DATABASE_URL)
migrate-down:
	@sqlx migrate revert --source migrations --database-url $(DATABASE_URL)
migrate-status:
	@sqlx migrate info --source migrations --database-url $(DATABASE_URL)

# Init dev environment
init-dev: run-db
	@until pg_isready -h localhost -p 5434 -U pguser > /dev/null 2>&1; do sleep 1; done
	@$(MAKE) migrate-up init-dev-db
# Init dev db (creates DMOB table and seeds data)
init-dev-db:
	@chmod +x scripts/setup_dev_db.sh
	@./scripts/setup_dev_db.sh

prepare:
	@cargo sqlx prepare --workspace
	@cargo fmt
	@cargo clippy

stop-app:
	@docker compose down url_finder
