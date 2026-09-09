PYTHON ?= python
UV ?= uv --cache-dir .cache/uv
COMPOSE ?= docker compose --profile models
PNPM ?= pnpm
RUN = $(UV) run --no-sync python

.PHONY: bootstrap build check check-plan diagnostics down lint lock logs migrate restart status sync test-contract test-frontend test-integration test-smoke test-unit typecheck up
bootstrap:
	$(PYTHON) scripts/bootstrap.py secrets
sync:
	$(UV) sync --frozen --all-packages
lock:
	$(UV) lock
check:
	$(RUN) scripts/verify_project.py
check-plan:
	$(RUN) scripts/verify_project.py --plan
lint:
	$(UV) run --no-sync ruff check apps packages scripts tests
	$(PNPM) --dir apps/frontend lint
typecheck:
	$(UV) run --no-sync mypy --explicit-package-bases packages apps/backend/src apps/agent-runtime/src apps/ingestion-worker/src apps/retrieval-ml/src apps/outbox-publisher/src scripts/export_openapi.py scripts/verify_project.py
	$(PNPM) --dir apps/frontend typecheck
test-unit: test-contract
test-contract:
	$(RUN) scripts/verify_project.py --lane contracts
test-integration:
	$(RUN) scripts/verify_project.py --lane integration
test-smoke:
	$(RUN) scripts/verify_project.py --lane smoke
test-frontend:
	$(RUN) scripts/verify_project.py --lane frontend
build:
	$(COMPOSE) build
migrate: bootstrap
	$(COMPOSE) run --rm --build db-migrate
up: bootstrap
	$(COMPOSE) up -d --build --wait --wait-timeout 900
down:
	$(COMPOSE) down
restart:
	$(COMPOSE) restart
status:
	$(COMPOSE) ps -a
logs:
	$(COMPOSE) logs --tail 100
diagnostics:
	$(COMPOSE) -f compose.yaml -f infra/compose/diagnostics.yaml up -d --wait --wait-timeout 180
