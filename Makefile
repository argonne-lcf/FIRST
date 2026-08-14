sync:
	uv sync --all-groups

mypy: sync
	uv run mypy

format: sync
	uv run ruff check --select I --fix .
	uv run ruff format .

format-check: sync
	uv run ruff check --select I .
	uv run ruff format --check .

lint: sync
	uv run ruff check .

lint-fix: sync
	uv run ruff check --fix .

test: sync
	uv run pytest

install-dev: sync
	pre-commit install

# The compose shortcuts below read COMPOSE_FILE / COMPOSE_PROJECT_NAME from .env.
# For local dev, .env should contain:
#   COMPOSE_FILE=deploy/compose.yaml:deploy/compose.dev.yaml
#   COMPOSE_PROJECT_NAME=first
# Append :deploy/compose.tunnel.yaml to opt into the SOCKS tunnel.

compose-build:
	docker compose build

db-up:
	docker compose up -d postgres redis

db-down:
	docker compose down postgres redis

db-reset:
	@echo "This wipes the DEV database and redis volumes (first_postgres-dev, first_redis-dev)."
	@read -p "Type 'yes' to continue: " c; [ "$$c" = "yes" ] || { echo "Aborted."; exit 1; }
	docker compose down postgres redis
	docker volume rm first_postgres-dev
	docker volume rm first_redis-dev
	docker compose up -d postgres redis migration
	docker compose restart inference-gateway controller-manager

compose-down:
	docker compose down

compose-up:
	docker compose up -d

watch-logs:
	docker compose logs inference-gateway -f --since=1m

monitor-redis:
	docker compose exec -it redis redis-cli monitor

attach-tunnel:
	docker compose logs --no-log-prefix --tail=10 tunnel
	@echo -----------------------------------------------------
	@echo Attaching to tunnel container: press CTRL-X to detach
	@echo -----------------------------------------------------
	docker compose attach --detach-keys="ctrl-x" tunnel

# --- Production ---------------------------------------------------------------

PROD_COMPOSE=COMPOSE_FILE=deploy/compose.yaml:deploy/compose.prod.yaml

# One-time: create the external prod data volumes. `docker compose down -v`
# refuses to delete external volumes, so prod data survives a full teardown.
prod-init:
	docker volume create postgres_data_prod
	docker volume create redis_data_prod
	docker volume create prometheus_data_prod
	docker volume create grafana_data_prod

prod-up:
	$(PROD_COMPOSE) docker compose up -d

prod-down:
	$(PROD_COMPOSE) docker compose down

# Migrations are manual in prod. Run after deploying a new image.
prod-migrate:
	$(PROD_COMPOSE) docker compose run --rm inference-gateway \
		.venv/bin/alembic -c packages/gateway/first_gateway/database/alembic.ini upgrade head

# --- Backups (safe to run against a live stack; see scripts/README.md) --------

db-backup:
	./scripts/pg-backup.sh

prom-snapshot:
	./scripts/prom-snapshot.sh
