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

dev-db-up:
	COMPOSE_FILE=deploy/compose.yaml:deploy/compose.dev.yaml docker compose up -d postgres redis

dev-db-down:
	COMPOSE_FILE=deploy/compose.yaml:deploy/compose.dev.yaml docker compose down postgres redis

dev-up:
	COMPOSE_FILE=deploy/compose.yaml:deploy/compose.dev.yaml docker compose up -d

dev-down:
	COMPOSE_FILE=deploy/compose.yaml:deploy/compose.dev.yaml docker compose down

prod-up:
	COMPOSE_FILE=deploy/compose.yaml:deploy/compose.prod.yaml docker compose up -d

prod-down:
	COMPOSE_FILE=deploy/compose.yaml:deploy/compose.prod.yaml docker compose down