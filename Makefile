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

test:
	uv run pytest

install-dev: sync
	pre-commit install

# To use the compose shortcuts below, add these to your .env file:
#  COMPOSE_FILE=deploy/compose.yaml:deploy/compose.dev.yaml
#  COMPOSE_PROJECT_NAME=first

compose-build:
	docker compose build

db-up:
	docker compose up -d postgres redis

db-down:
	docker compose down postgres redis

compose-down:
	docker compose down

compose-up:
	docker compose up -d

watch-logs:
	docker compose logs inference-gateway -f --since=1m

prod-up:
	COMPOSE_FILE=deploy/compose.yaml:deploy/compose.prod.yaml docker compose up -d

prod-down:
	COMPOSE_FILE=deploy/compose.yaml:deploy/compose.prod.yaml docker compose down
